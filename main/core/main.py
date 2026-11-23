from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
import logging
import os

from core.db import init_db, SessionLocal
from core.version import APP_VERSION
from core.api.stock_pools import router as stock_pools_router
from core.api.corporate_actions import router as corporate_actions_router
from core.api.formulas import router as formulas_router
from core.api.strategies import router as strategies_router
from core.api.backtest import router as backtest_router
from core.api.live import router as live_router
from core.api.system import router as system_router
from core.api.status import router as status_router
from core.api.tdx import router as tdx_router
from core.api.incremental import router as incremental_router
from core.api.market_gate import router as market_gate_router
from core.api.capital import router as capital_router
from core.api.pairing import router as pairing_router
from core.api.investment_board import router as investment_board_router
from core.api.tdx_gate import router as tdx_gate_router
from core.api.kline_coverage import router as kline_coverage_router
from core.api.data_observability import router as data_observability_router
from core.api.live_position_scope import router as live_position_scope_router
from core.tq.gate import TdxTaskBlocked
from core.logging_config import setup_logging
from core.services.backtest_service import (
    recover_stale_running_backtests, recover_stale_running_batch_runs,
)
from core.services.live_service import (
    ENGINES, DecisionArchiveScheduler, recover_stale_running_live_sessions,
)
from core.engine.live.engine_watchdog import (
    EngineWatchdog, record_channel_decision,
)
from core.services.backup_service import BackupScheduler
from core.services.incremental_backtest import IncrementalBacktestScheduler
from core.services.auto_shutdown import arm as arm_auto_shutdown
from core import runtime


logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    # 记录本次进程的实际监听端口 / pid / 版本到 data/runtime/server.json：
    # 打包版端口动态（8888~8898），自愈父进程与第二次双击都靠它找到本实例。
    runtime.mark_serving(runtime.resolve_bound_port())
    init_db()
    # 源码模式运行期数据目录统一（一次性）：旧仓库根 data/ 下的体检游标等
    # 持久 runtime 文件迁到 main/data（与 DB 同目录），原文件留 .bak。打包/
    # TQ_APP_ROOT 模式新旧同目录，自动 no-op。失败只告警，不阻断启动。
    if os.environ.get("TQ_DATA_DIR_MIGRATION_DISABLED") == "1":
        logger.info("runtime 数据目录迁移已禁用（TQ_DATA_DIR_MIGRATION_DISABLED=1）")
    else:
        try:
            moved = runtime.migrate_legacy_data_files()
            if moved:
                logger.warning("runtime 数据目录迁移完成：%s", moved)
        except Exception:  # noqa: BLE001 — 迁移失败不得阻断启动
            logger.warning("runtime 数据目录迁移失败（忽略，不影响启动）",
                           exc_info=True)
    # P1-3：桥地址启动校验（不阻断）。live 腿端口与配置声明的现网端口（simulation
    # 腿，缺省 8790）不一致时记一条显式告警——历史默认给的是 8791 信用账户腿，
    # 实盘会持续连不上 QMT 桥。
    try:
        from core.config import iquant_live_port_mismatch
        _endpoint_warn = iquant_live_port_mismatch()
        if _endpoint_warn:
            logger.warning("%s", _endpoint_warn)
    except Exception:  # noqa: BLE001 — 校验失败不得阻断启动
        logger.warning("iquant_bridge live 端点校验异常（已忽略）", exc_info=True)
    # P2-10：告警待发队列恢复（未启用 notify 时零副作用）。上次进程遇到飞书限频
    # 落盘的告警在这里继续投递，不依赖下一条新告警触发发送线程。
    try:
        from core.services.notify_service import get_notifier
        get_notifier().start()
    except Exception:  # noqa: BLE001 — 告警恢复失败不得阻断启动
        logger.warning("notify 待发队列恢复异常（已忽略）", exc_info=True)
    # L5：启动版本校验（默认关闭）。只读比对 alembic_version 与代码 head，
    # 不执行任何迁移；开关关闭时无日志、零副作用。
    try:
        from core.services import upgrade_service
        upgrade_service.startup_check()
    except Exception:  # noqa: BLE001 — 版本校验失败不得阻断启动
        logger.warning("启动版本校验异常（已忽略）", exc_info=True)
    # 打包版启用自动退出：浏览器标签页全部关闭后自动退出进程，避免残留占用端口。
    # 自愈子进程（--serve）必须常驻，不做「关标签页即退出」，否则父进程会反复重启。
    if _is_frozen() and not runtime.auto_shutdown_suppressed():
        arm_auto_shutdown()
        logger.info("打包模式：已启用自动退出（全部标签页关闭后自动退出）")
    # #5: 清理上次进程崩溃/重启残留的僵尸 running 回测记录（同步内联执行，
    # 正常退出不残留 running；残留即崩溃）。统一标 failed + 附说明，避免前端
    # 永久显示"运行中"且无法重新发起（_BACKTEST_LOCK 是进程内锁，重启已释放）。
    with SessionLocal() as db:
        n = recover_stale_running_backtests(db)
        if n:
            logger.warning("启动恢复：%d 条僵尸 running 回测已标记 failed", n)
        # 批量任务 run（walk-forward / 参数微扰）同理：崩溃残留 running 会导致
        # 面板永久"运行中"且删除被 409 守卫拦截。
        n2 = recover_stale_running_batch_runs(db)
        if n2:
            logger.warning("启动恢复：%d 条僵尸 running 批量任务（walk-forward/参数微扰）已标记 failed", n2)
        # 实盘同类缺陷（2026-11-04 实证）：后端重启不会自动恢复引擎，但 DB 仍留
        # status=running → 工作台显示「运行中」而 bridge-status=not_running。
        # 冷启动时引擎注册表必为空，安全统一置 stopped（逐条日志留原因），
        # 由用户手动重新启动。
        if os.environ.get("TQ_LIVE_STALE_RECOVERY_DISABLED") == "1":
            # 测试进程专用开关：lifespan 走真实 SessionLocal，测试触发 lifespan
            # 会误把正在运行的实盘会话置 stopped（2026-11-19 09:45:27 实证）。
            logger.info(
                "僵尸实盘会话清理已禁用（TQ_LIVE_STALE_RECOVERY_DISABLED=1）")
        else:
            n3 = recover_stale_running_live_sessions(db)
            if n3:
                logger.warning("启动恢复：%d 个僵尸 running 实盘会话已置 stopped", n3)
        # L1 旧交接目录清理（2026-11-16）：把 ths_ui.work_dir 下非活跃会话的数字
        # 目录移入 {work_dir}/_archive/<日期>/（空目录删除）。当前 running/stopping
        # 会话目录与引擎注册表键一律保留（cursor 仍在被消费的目录绝不动）；
        # 失败只告警，不得阻断启动。
        if os.environ.get("TQ_HANDOFF_ARCHIVE_DISABLED") == "1":
            # 测试进程专用：conftest 把库切到临时文件后，这里读不到真实 running
            # 会话，会把正在消费的会话目录误判为陈旧并归档（cursor 断供）。
            logger.info("旧交接目录清理已禁用（TQ_HANDOFF_ARCHIVE_DISABLED=1）")
        else:
            try:
                from core.engine.live.file_handoff import archive_stale_session_dirs
                from core.services.live_service import ths_ui_config
                from core.models import LiveSession as _LiveSession
                active_ids = {
                    row.id for row in db.query(_LiveSession)
                    .filter(_LiveSession.status.in_(("running", "stopping"))).all()
                } | set(ENGINES.keys())
                archive_stale_session_dirs(ths_ui_config()["work_dir"], active_ids)
            except Exception:  # noqa: BLE001 — 清理失败只告警
                logger.warning("旧交接目录清理失败（忽略，不影响启动）", exc_info=True)
    # 进程内引擎存活监控（daemon 线程，30s 扫描）：只做「通道停摆 / 轮次卡住」
    # 的检测与告警（logger / SSE / live_decision_events），**不重启进程**——它随
    # 进程一起消失，没法救自己。进程级崩溃/无响应的自愈由 `--supervise` 父进程
    # （core/supervisor.py）负责，两者职责不重叠。
    watchdog = None
    try:
        # 通道类故障单次判定即落 live_decision_events（gate=channel_offline/
        # channel_recovered），几分钟闪断也能在库里查到（2026-11-13）。
        # unregister_fn：DB 已 stopped/stopping 而本进程引擎仍在跑（另一进程
        # 收到 stop 的错配，2026-11-18 实证）时，自愈把孤儿引擎移出注册表。
        # 仅异常分支触发；正常 stop 由路由同步置 _running=False，不会走到这里。
        watchdog = EngineWatchdog(
            lambda: ENGINES, persist_fn=record_channel_decision,
            unregister_fn=lambda sid: ENGINES.pop(sid, None))
        watchdog.start()
        logger.info("实盘引擎存活监控已启动（进程内，30s 扫描）")
    except Exception:  # noqa: BLE001 — 监控启动失败不得阻断主服务
        logger.exception("实盘引擎存活监控启动失败（主服务继续运行）")
        watchdog = None
    # 内嵌数据备份调度（daemon 线程，每日 backup.daily_at 默认 15:10）：
    # VACUUM INTO 一致性副本 + WAL 收敛；独立线程，不占用事件循环、不碰实盘锁。
    backup_scheduler = None
    if os.environ.get("TQ_BACKUP_SCHEDULER_DISABLED") == "1":
        logger.info("数据备份调度已禁用（TQ_BACKUP_SCHEDULER_DISABLED=1）")
    else:
        try:
            backup_scheduler = BackupScheduler()
            backup_scheduler.start()
            logger.info("数据备份调度已启动（进程内，每日定时）")
        except Exception:  # noqa: BLE001 — 备份调度失败不得阻断交易主服务
            logger.exception("数据备份调度启动失败（主服务继续运行）")
            backup_scheduler = None
    # 接缝续跑增量回测调度（daemon 线程，每日 15:10 续跑 + 每周双跑校验）：
    # 复用 DailyCloser 15:05 已取到的日线，不进 _BACKTEST_LOCK 队列。
    incr_scheduler = None
    if os.environ.get("TQ_INCREMENTAL_SCHEDULER_DISABLED") == "1":
        logger.info("增量回测调度已禁用（TQ_INCREMENTAL_SCHEDULER_DISABLED=1）")
    else:
        try:
            incr_scheduler = IncrementalBacktestScheduler()
            incr_scheduler.start()
            logger.info("增量回测调度已启动（进程内，每日续跑 + 每周双跑校验）")
        except Exception:  # noqa: BLE001 — 调度失败不得阻断交易主服务
            logger.exception("增量回测调度启动失败（主服务继续运行）")
            incr_scheduler = None
    # 决策事件过期归档调度（daemon 线程，每小时）：把超过
    # live.decision_archive_days（默认 7 天）仍 open 的非高危事件标为 archived，
    # 只移出「未处理」计数，不删除；<=0 时调度自身空转（maybe_run 直接返回）。
    archive_scheduler = None
    if os.environ.get("TQ_DECISION_ARCHIVE_SCHEDULER_DISABLED") == "1":
        logger.info("决策事件归档调度已禁用（TQ_DECISION_ARCHIVE_SCHEDULER_DISABLED=1）")
    else:
        try:
            archive_scheduler = DecisionArchiveScheduler()
            archive_scheduler.start()
            logger.info("决策事件归档调度已启动（进程内，每小时）")
        except Exception:  # noqa: BLE001 — 归档调度失败不得阻断交易主服务
            logger.exception("决策事件归档调度启动失败（主服务继续运行）")
            archive_scheduler = None
    yield
    if archive_scheduler is not None:
        archive_scheduler.stop()
    if incr_scheduler is not None:
        incr_scheduler.stop()
    if backup_scheduler is not None:
        backup_scheduler.stop()
    if watchdog is not None:
        watchdog.stop()
    # 正常退出清理 server.json（只删属于本进程的那份）；被强杀时由父进程/下次启动覆盖
    runtime.mark_stopped()


app = FastAPI(title="创懿私募基金量化投资平台", version=APP_VERSION, lifespan=lifespan)

app.include_router(stock_pools_router)
app.include_router(formulas_router)
app.include_router(strategies_router)
app.include_router(backtest_router)
app.include_router(live_router)
app.include_router(system_router)
app.include_router(status_router)
app.include_router(tdx_router)
app.include_router(incremental_router)
app.include_router(market_gate_router)
app.include_router(corporate_actions_router)
app.include_router(capital_router)
app.include_router(pairing_router)
app.include_router(investment_board_router)
app.include_router(tdx_gate_router)
app.include_router(kline_coverage_router)
app.include_router(data_observability_router)
app.include_router(live_position_scope_router)


@app.exception_handler(TdxTaskBlocked)
async def tdx_task_blocked_handler(request: Request, exc: TdxTaskBlocked):
    """后台重任务被 TQ 通道闸拒绝 → 明确原因 + 下次可用时刻（HTTP 200 + code 409）。

    不用长时间转圈或 30s 超时：闸在发起任何 TQ 调用前就拒绝，前端据此提示
    「实盘运行中，回测类任务 15:15 后可用」。
    """
    from core.api.response import err as _err

    return JSONResponse(status_code=200, content=_err(409, str(exc), exc.to_dict()))


@app.exception_handler(Exception)
async def uncaught_exception_handler(request: Request, exc: Exception):
    """#13：未捕获异常 → 统一 envelope + HTTP 500（非 FastAPI 默认空 body）。

    仅注册 Exception，不注册 HTTPException → HTTPException 走 FastAPI 默认 handler
    （pass-through：真实 HTTP 状态码 + {"detail":...}），SSE 404 / backtest 409 行为不变。
    不暴露 str(exc) 给前端（内部细节不外泄），仅记日志。
    """
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"code": 500, "message": "服务器内部错误", "data": None},
    )


def _health_payload() -> dict:
    """``/health`` 与 ``/api/health`` 共用的 JSON 载荷（字段/行为完全一致）。"""
    payload = runtime.health_payload()
    try:
        from core.services import business_health

        if business_health.probe_enabled():
            payload["business"] = business_health.snapshot()
    except Exception:  # noqa: BLE001 — 业务探针失败不得让 /health 500
        logger.warning("业务级心跳采集失败（已忽略）", exc_info=True)
    return payload


def _spa_index_file():
    """前端 SPA 的 index.html 路径；未构建（dev/测试环境）时返回 None。"""
    frozen = hasattr(_sys, "frozen") or bool(_sys.argv[0] and _sys.argv[0].endswith(".exe"))
    base = _Path(_sys.argv[0]).resolve().parent
    dist = (base / "_internal" / "web" / "dist") if frozen else (
        _Path(__file__).resolve().parent.parent.parent / "web" / "dist")
    index = dist / "index.html"
    return index if index.exists() else None


@app.get("/health")
async def health(request: Request):
    """轻量探活（无鉴权）：版本 / 运行时长 / 实际端口 / pid。

    自愈父进程（``--supervise``）、桌面壳与外部监控都靠它判断后端是否真的在服务；
    端口动态，以 ``data/runtime/server.json`` 记录的端口为准。

    与前端「健康总览」页同路径（``/health``），按 ``Accept`` 内容协商分流：
    浏览器地址栏访问（``Accept`` 含 ``text/html``）回退前端 SPA 的 index.html；
    探针 / curl / 程序调用（``Accept: */*``、``application/json`` 或缺省）仍返回
    原来的 JSON 探活，响应体、状态码、字段一律不变。稳定的纯 JSON 冗余入口是
    ``GET /api/health``（两者等价）。

    L6：``health.business_probe_enabled=true`` 时附加 ``business`` 业务心跳
    （最近 bar / 轮次推进 / watchdog 扫描）；默认关闭时响应体与旧行为逐字一致。
    """
    if "text/html" in (request.headers.get("accept") or "").lower():
        index = _spa_index_file()
        if index is not None:
            return FileResponse(index, media_type="text/html")
    return _health_payload()


@app.get("/api/health")
async def api_health():
    """纯 JSON 探活（``/health`` 的冗余稳定入口）：字段与 ``/health`` 完全一致。

    供外部监控 / 探针长期引用；不受 ``Accept`` 内容协商影响，永远返回 JSON。
    """
    return _health_payload()


@app.get("/debug-path")
async def debug_path():
    import sys
    from pathlib import Path
    base = Path(sys.argv[0]).resolve().parent
    dist = base / "_internal" / "web" / "dist"
    if not dist.exists():
        dist = Path(__file__).resolve().parent.parent.parent / "web" / "dist"
    return {
        "frozen": hasattr(sys, 'frozen'),
        "argv0": sys.argv[0],
        "base": str(base),
        "dist": str(dist),
        "exists": dist.exists(),
    }


from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as _StarletteHTTPException
from pathlib import Path as _Path
import sys as _sys

class SPAStaticFiles(StaticFiles):
    """SPA 前端托管：未知前端路由（如 /dashboard）回退 index.html，
    浏览器直接刷新不再返回 {"detail":"Not Found"}；/api 未知路径仍返回 JSON 404。"""

    async def get_response(self, path: str, scope):
        # Windows 下 Starlette 会把路径分隔符转成反斜杠 → 归一化后再判断
        normalized = path.replace("\\", "/").lstrip("/")
        served_index = False
        try:
            response = await super().get_response(path, scope)
        except _StarletteHTTPException as exc:
            if exc.status_code == 404 and not normalized.startswith("api/"):
                response = await super().get_response("index.html", scope)
                served_index = True
            else:
                raise
        # 入口 HTML 不缓存（否则改了文案/资源哈希后浏览器仍拿旧入口）；
        # 带内容哈希的 assets/* 长缓存 + immutable，内容变化必然换文件名。
        if served_index or normalized in ("", ".", "index.html"):
            response.headers["Cache-Control"] = "no-cache, must-revalidate, max-age=0"
        elif normalized.startswith("assets/"):
            response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response

def _is_frozen() -> bool:
    return hasattr(_sys, 'frozen') or (_sys.argv[0] and _sys.argv[0].endswith('.exe'))

if _is_frozen():
    _base = _Path(_sys.argv[0]).resolve().parent
    _dist = _base / "_internal" / "web" / "dist"
else:
    _dist = _Path(__file__).resolve().parent.parent.parent / "web" / "dist"
logger.info("Static dist path: %s (exists=%s)", _dist, _dist.exists())
if _dist.exists():
    app.mount("/", SPAStaticFiles(directory=str(_dist), html=True), name="static")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8888)
