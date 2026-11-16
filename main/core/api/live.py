import asyncio
import json
import logging
from datetime import date, datetime
from typing import Dict, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import SessionLocal, get_db
from core.models import LiveSession, PortfolioStrategy
from core.config import load_config
from core.engine.http_bridge_dispatcher import HttpBridgeDispatcher
from core.engine.live_engine import LiveEngine
try:
    from core.engine.ths_ui_dispatcher import ThsUiDispatcher
except Exception:  # pragma: no cover - 依赖缺失时仍可走 QMT 桥
    ThsUiDispatcher = None  # type: ignore
# 业务逻辑已下沉到 core.services.live_service（与 backtest_service 同一范式）。
# 路由层仅留 HTTP 壳：参数校验 + asyncio 任务调度 + 调 service + ok/err 翻译。
from core.services import live_service as svc

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/live", tags=["live"])

# C1（终审）：start 的 check-then-act 竞态闸。`if session_id in _ENGINES` 检查与
# `_ENGINES[session_id]=engine` 注册之间隔了 `await run_in_threadpool(svc.start_session)`
# （heartbeat/preflight/recover，秒级窗口）——并发两次 start 都见空注册表 → 双引擎
# 并发真实下单（同 session 重复下单 / 跨 session 击穿 B6）。启动稀有，临界区串行可接受。
_START_LOCK = asyncio.Lock()


async def _stop_and_finalize(engine, session_id: int) -> None:
    """stop 的收尾协程：先让引擎 stop() 收敛（含异常），再把 stopping → stopped。

    收尾用独立 DB session（请求 Session 已随响应关闭）；失败只记日志，
    由下次启动的 recover_stale_running_live_sessions / 僵尸抢占兜底。
    """
    try:
        await engine.stop()
    except Exception:  # noqa: BLE001
        logger.exception("engine.stop() failed (session %s)", session_id)
    finally:
        try:
            await run_in_threadpool(svc.finalize_stopped, session_id)
        except Exception:  # noqa: BLE001
            logger.exception("finalize_stopped failed (session %s)", session_id)


# ---------------------------------------------------------------------------
# 兼容 re-export：集成测试 test_live_engine_api.py 直接引用以下 live.py 符号
# （对齐 core.api.backtest 的 re-export 范式）。
# ---------------------------------------------------------------------------
# 进程注册表 re-export 为 service 持有的同一个 dict——
# live_api._ENGINES.clear() / `sid in live_api._ENGINES` 仍作用于同一对象。
_ENGINES: Dict[int, LiveEngine] = svc.ENGINES

from core.services.live_service import (  # noqa: E402
    bridge_config as _bridge_config,
    resolve_stock_codes as _resolve_stock_codes,
    serialize_order as _serialize_order,
    serialize_trade as _serialize_trade,
    aggregate_positions_from_trades as _aggregate_positions_from_trades,
    engine_virtual_positions as _engine_virtual_positions,
)


def _dispatcher_cls():
    """旧行为兼容：按 config.ths_ui.enabled 选通道（同花顺 UI / QMT 通道）。

    enabled=false（默认）时返回本模块的 HttpBridgeDispatcher（测试 monkeypatch 生效）；
    enabled=true 且依赖可用时返回 ThsUiDispatcher。新会话请用 session.engine 显式选择。
    """
    cfg = load_config()
    ths = cfg.get("ths_ui", {}) if isinstance(cfg, dict) else {}
    if isinstance(ths, dict) and ths.get("enabled") and ThsUiDispatcher is not None:
        return ThsUiDispatcher
    return HttpBridgeDispatcher


def _build_engine(session_id: int, db: Session, mode: str = "simulation",
                  dispatcher_cls=None) -> LiveEngine:
    """薄 wrapper：绑定 db_session_factory=SessionLocal + 当前配置选出的 dispatcher。

    dispatcher_cls 显式传入时优先（测试 monkeypatch 直调）；否则沿用旧的全局通道选择
    （ths_ui.enabled → 同花顺，否则 QMT 桥）。新会话请经 /sessions/{id}/start
    由 session 生效引擎决定通道。
    """
    return svc.build_engine(
        session_id, db, mode,
        db_session_factory=SessionLocal,
        dispatcher_cls=dispatcher_cls or _dispatcher_cls(),
    )


# ---------------------------------------------------------------------------
# 路由：session CRUD
# ---------------------------------------------------------------------------

@router.get("/sessions")
def list_sessions(db: Session = Depends(get_db)):
    return ok(svc.list_sessions(db))


@router.post("/sessions")
def create_session(data: dict, db: Session = Depends(get_db)):
    name = str(data.get("name") or "").strip()
    if not name:
        return err(400, "会话名称不能为空")
    mode = data.get("mode", "simulation")
    if mode not in ("simulation", "live"):
        return err(400, "mode 非法，仅支持 simulation(仿真) / live(实盘)")
    # A-6：未显式选引擎时用 tq_trade.default_engine（空/非法回退 ths_ui.enabled → ths
    # 否则 iquant），值域校验沿用下方白名单。
    engine = str(data.get("engine") or svc.default_engine())
    if engine not in svc.ENGINES_META:
        return err(400, "engine 非法，仅支持 %s" % " / ".join(svc.ENGINES_META))
    data["name"] = name
    data["engine"] = engine
    execution_mode = str(data.get("execution_mode") or "alert")
    if execution_mode not in ("alert", "auto"):
        return err(400, "execution_mode 非法，仅支持 alert(半自动) / auto(全自动)")
    data["execution_mode"] = execution_mode
    # 会话级实盘资金基准（账户总资产口径）：None=各组合用自身 initial_capital；
    # 提供时须 >0。继承持仓的市值由引擎从起账现金扣除，避免重复计入。
    live_capital = data.get("live_capital")
    if live_capital is not None:
        try:
            live_capital = float(live_capital)
        except (TypeError, ValueError):
            return err(400, "实盘资金基准须大于 0")
        if not live_capital > 0:
            return err(400, "实盘资金基准须大于 0")
    data["live_capital"] = live_capital
    # 外部信号注入（0011）：通达信「条件预警」输出文件路径；空串/缺省 → None（不启用，
    # 走 TQ 公式计算）。填写后该会话信号来源改为文件，停用 TQ 公式链路（二者互斥）。
    signal_file_path = data.get("signal_file_path")
    if signal_file_path is not None:
        signal_file_path = str(signal_file_path).strip() or None
    data["signal_file_path"] = signal_file_path
    # 14:50 提前收盘价（仅 1d）：开关默认 false；时点默认 14:51，限定 14:51–14:59
    # （须晚于 14:50 分钟 bar 闭合、早于收盘）。周线/月线不提前，回测忽略。
    data["advance_close_enabled"] = bool(data.get("advance_close_enabled"))
    advance_close_time = str(data.get("advance_close_time") or "14:51").strip()
    try:
        hh, mm = advance_close_time.split(":")
        minutes = int(hh) * 60 + int(mm)
    except (TypeError, ValueError):
        return err(400, "advance_close_time 须为 HH:MM 格式")
    if not (14 * 60 + 51 <= minutes <= 14 * 60 + 59):
        return err(400, "advance_close_time 仅支持 14:51–14:59（14:50 bar 闭合后、收盘前）")
    data["advance_close_time"] = advance_close_time
    # 错周期 bar 风控（nullable 三态）：null/缺省=跟随全局默认；true/false=会话覆盖。
    mismatch = data.get("intraday_mismatched_risk_enabled")
    if mismatch is None:
        data["intraday_mismatched_risk_enabled"] = None
    elif isinstance(mismatch, bool):
        pass
    elif isinstance(mismatch, int) and mismatch in (0, 1):
        data["intraday_mismatched_risk_enabled"] = bool(mismatch)
    else:
        return err(400,
                   "intraday_mismatched_risk_enabled 仅支持 null / true / false")
    # 组合关联预校验：重复/不存在的 portfolio_id 此前会由 FK/唯一约束抛
    # IntegrityError 变成 500，这里统一拦截成 400 并给出具体 id。
    pids = data.get("portfolio_ids") or []
    if not isinstance(pids, list):
        return err(400, "portfolio_ids 必须为数组")
    try:
        pids = [int(p) for p in pids]
    except (TypeError, ValueError):
        return err(400, "portfolio_ids 只能包含整数组合 id")
    if len(set(pids)) != len(pids):
        return err(400, "portfolio_ids 存在重复项")
    if pids:
        existing = {
            row.id for row in
            db.query(PortfolioStrategy.id)
            .filter(PortfolioStrategy.id.in_(pids)).all()
        }
        missing = [str(p) for p in pids if p not in existing]
        if missing:
            return err(400, "组合不存在：%s" % "、".join(missing))
    data["portfolio_ids"] = pids
    return ok(svc.create_session(db, data, mode))


@router.get("/sessions/{session_id}")
def get_session(session_id: int, db: Session = Depends(get_db)):
    engine = _ENGINES.get(session_id)
    data = svc.get_session(db, session_id, engine)
    if data is None:
        return err(404, "资源不存在")
    return ok(data)


@router.get("/sessions/{session_id}/signals")
def session_signals(
    session_id: int,
    strategy_id: Optional[int] = None,
    stock_code: Optional[str] = None,
    signal_type: Optional[str] = None,
    execution_mode: Optional[str] = None,
    alert_status: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
    db: Session = Depends(get_db),
):
    def _dt(value):
        return datetime.fromisoformat(value) if value else None

    try:
        data = svc.session_signals(
            db, session_id, strategy_id=strategy_id, stock_code=stock_code,
            signal_type=signal_type, execution_mode=execution_mode,
            alert_status=alert_status, start=_dt(start), end=_dt(end),
            limit=limit, offset=offset,
        )
    except ValueError:
        return err(400, "start/end 必须为 ISO 时间")
    if data is None:
        return err(404, "资源不存在")
    return ok(data)


@router.post("/sessions/{session_id}/start")
async def start_session(session_id: int, data: Optional[dict] = None,
                        db: Session = Depends(get_db)):
    data = data or {}
    ack_keyboard_risk = bool(data.get("ack_keyboard_risk"))
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return err(404, "资源不存在")
    # C1（终审）：整段「幂等检查 → threadpool 启动（秒级）→ 注册」持 _START_LOCK
    # 串行——并发的第二个 start 会等锁，进锁后见注册表已有引擎 → 幂等返回 running；
    # 不同 session 的并发 start 也被串行化，B6 资源互斥（M1 多会话并行：THS 通道
    # 键鼠独占，其余通道可并行）在锁内判定不再有竞态窗口。
    async with _START_LOCK:
        # 幂等：已在跑直接返回（service 检测 session_id in _ENGINES）
        if session_id in _ENGINES:
            return ok({"id": session.id, "status": "running"})

        # I-6（audit）：同步重活（建引擎 / heartbeat 通道探测 / preflight / recover 重放）
        # 经线程池执行——事件循环不被阻塞（TQ/桥探测失败要等超时，秒级冻结会拖垮 SSE）。
        # 先取齐纯值，避免 worker 线程访问 loop 线程加载的 ORM 实例属性。
        mode = session.mode
        engine, _session, error = await run_in_threadpool(
            lambda: svc.start_session(
                db, session_id, _ENGINES, mode,
                db_session_factory=SessionLocal,
                dispatcher_cls=None,
                ack_keyboard_risk=ack_keyboard_risk,
            ),
        )
        if error is not None:
            # 结构化 409：data.rule ∈ engine_exclusive/account_exclusive/
            # keyboard_exclusive，data.occupied_by 指认占用方，供前端引导。
            if len(error) >= 3:
                status, message, extra = error[0], error[1], error[2]
            else:
                status, message, extra = error[0], error[1], None
            return err(status, message, extra)
        if engine is None:
            # 幂等路径（service 在锁内再查一次注册表，理论上上方已拦，兜底）
            return ok({"id": session.id, "status": "running"})

        # 起 asyncio 循环任务（async 端点内直接在 FastAPI 事件循环建任务）
        asyncio.create_task(engine.start())
        # 注册先于 commit，闭合 B6 窗口（与重构前顺序一致）；注册仍在 _START_LOCK
        # 临界区内，check-then-act 窗口已闭合（C1）。
        _ENGINES[session_id] = engine
        session.status = "running"
        session.started_at = datetime.now()
        db.commit()
        return ok({
            "id": session.id,
            "status": "running",
            "account_key": getattr(session, "account_key", None),
            "account_key_source": getattr(session, "account_key_source", None),
            "warnings": list(getattr(engine, "start_warnings", []) or []),
        })


@router.post("/sessions/{session_id}/stop")
async def stop_session(session_id: int, db: Session = Depends(get_db)):
    """停止会话。幂等：已停止的会话重复调用仍返回 200（此前返回 404）。

    与 start 共用 _START_LOCK：start 的建引擎/心跳在 threadpool 里是秒级的，
    若不排队，stop 会在「引擎尚未注册、DB 仍是 stopped」的窗口里直接返回 ok，
    随后 start 完成注册并把引擎跑起来——用户的停止被静默丢弃。
    """
    async with _START_LOCK:
        session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
        if not session:
            return err(404, "资源不存在")
        if session_id not in _ENGINES and session.status not in ("running", "stopping"):
            return ok({"id": session_id, "status": "stopped"})
        engine = svc.stop_session(db, session_id, _ENGINES)
        # 先同步置 _running=False，缩小「已置 stopped 但旧循环仍在轮询」的窗口，
        # 避免用户立刻 start 后新旧引擎并存各下一笔委托。
        if engine is not None:
            if hasattr(engine, "_running"):
                engine._running = False
            # I-5：同步置拒单闸——在途 tick 的 _handle_bar 入口即刻不再发新单
            # （不等 engine.stop() 协程被调度）。
            if hasattr(engine, "_stop_requested"):
                engine._stop_requested = True
            # stopping 中间态（2026-11-16）：引擎 stop() 收敛（含异常）后才置
            # stopped 并释放 C1/C2 占用；期间另有会话启动会被 409 拦。
            asyncio.create_task(_stop_and_finalize(engine, session_id))
        return ok({"id": session_id, "status": session.status})


@router.post("/sessions/{session_id}/portfolios/{portfolio_id}/recover")
def recover_breaker(session_id: int, portfolio_id: int, db: Session = Depends(get_db)):
    ok_flag, message = svc.recover_breaker(db, session_id, portfolio_id, _ENGINES)
    if not ok_flag:
        return err(404, message)
    return ok({"portfolio_id": portfolio_id, "status": "active", "circuit_breaker_count": 0})


@router.get("/account-preview")
def account_preview(engine: Optional[str] = None, db: Session = Depends(get_db)):
    """资金基准预览（A7）：按引擎探测真实账户资产（只读，不下单、不改账）。

    返回 {total_asset, available, captured_at}；实时探测失败回退最近账户快照。
    """
    data, error = svc.account_preview_by_engine(db, engine or svc.default_engine())
    if error is not None:
        return err(error[0], error[1])
    return ok(data)


@router.post("/sessions/{session_id}/portfolios/{portfolio_id}/reset-baseline")
def reset_baseline(session_id: int, portfolio_id: int,
                   data: Optional[dict] = None,
                   db: Session = Depends(get_db)):
    """收盘后重置资金基准（按组合）+ 成套重锚风控锚点（A1d）。

    - 基准 = 真实账户总资产口径，只缩不放：> 真实总资产 → 400。
    - 盘中（09:15–11:30 / 13:00–15:00）禁用 → 403。
    - 不清零熔断累计次数、不解锁手动恢复、不写 live_cash_flows。
    """
    data = data or {}
    value = data.get("value")
    if value is None:
        value = data.get("baseline_value")
    ok_flag, error = svc.reset_baseline(
        db, session_id, portfolio_id, value,
        engines=_ENGINES, note=data.get("note"),
    )
    if not ok_flag:
        return err(error[0], error[1])
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    return ok({
        "session_id": session_id,
        "portfolio_id": portfolio_id,
        "baseline_value": (
            float(session.baseline_value)
            if session is not None and session.baseline_value is not None else None),
        "baseline_version": int(session.baseline_version or 0) if session else 0,
        "status": "reset",
    })


@router.get("/channel")
def channel_info(engine: Optional[str] = None):
    """交易引擎清单 + 当前生效通道与在线状态。

    engine（可选，session.engine）决定 active/label；不传沿用旧默认（ths_ui.enabled → 同花顺）。
    """
    return ok(svc.channel_status(engine_key=engine))


@router.post("/channel/test")
def channel_test(data: Optional[dict] = None):
    """只读连通性探测：读一次持仓 / ping 一次。不产生下单。"""
    data = data or {}
    channel = data.get("channel") or "ths_ui"
    if channel not in ("ths_ui", "iquant_bridge", "tdx_tq", "goldminer_bridge"):
        return err(400, "channel 非法，仅支持 goldminer_bridge / iquant_bridge / ths_ui / tdx_tq")
    return ok(svc.test_channel(channel, data.get("mode") or "simulation"))


@router.get("/sessions/{session_id}/bridge-status")
def bridge_status(session_id: int, db: Session = Depends(get_db)):
    """桥在线状态（§11 切片5 并入，本切片先放最小端点）。

    引擎运行中取 dispatcher.heartbeat()；未运行返回 unknown。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return err(404, "资源不存在")
    engine = _ENGINES.get(session_id)
    return ok(svc.bridge_status(session_id, session.status, engine))


# ---- B4b: 历史查询端点（orders / trades / positions）----

@router.get("/sessions/{session_id}/orders")
def session_orders(session_id: int, status: Optional[str] = None, db: Session = Depends(get_db)):
    """B4b：委托历史。可选 ?status= 过滤（submitted/filled/partial/rejected/canceled）。"""
    rows = svc.session_orders(db, session_id, status)
    if rows is None:
        return err(404, "资源不存在")
    return ok(rows)


@router.get("/sessions/{session_id}/trades")
def session_trades(
    session_id: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """B4b：成交历史，按 trade_time 倒序。

    可选 ?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD（Asia/Shanghai 自然日，闭区间）；
    缺省不过滤，返回与旧版一致的全量列表（向后兼容，未删任何成交记录）。
    """
    try:
        start = date.fromisoformat(start_date) if start_date else None
        end = date.fromisoformat(end_date) if end_date else None
    except ValueError:
        return err(400, "start_date/end_date 需为 YYYY-MM-DD")
    rows = svc.session_trades(db, session_id, start, end)
    if rows is None:
        return err(404, "资源不存在")
    return ok(rows)


@router.get("/sessions/{session_id}/positions")
def session_positions(session_id: int, db: Session = Depends(get_db)):
    """B4b：持仓分级。运行中读引擎内存态虚拟持仓（含未落库当日变动，source=strategy）
    + dispatcher 真实持仓差集（账户原有持仓，source=external，60s TTL 缓存）；
    停止后从 live_trades 重放聚合（与 recover 同口径：BUY 加、SELL 减，均价加权）。"""
    rows = svc.session_positions(db, session_id, _ENGINES.get(session_id))
    if rows is None:
        return err(404, "资源不存在")
    return ok(rows)


@router.get("/sessions/{session_id}/cash-flows")
def session_cash_flows(
    session_id: int,
    portfolio_id: Optional[int] = None,
    limit: int = 200,
    db: Session = Depends(get_db),
):
    """外部现金流台账（入金/出金/分红/其他）+ 各组合累计净流入（③）。

    虚拟现金随台账增减，但净值/收益率/回撤按「总市值 − 累计净流入」计算，
    与回测（无外部现金流）口径一致。
    """
    data = svc.list_cash_flows(db, session_id, portfolio_id=portfolio_id, limit=limit)
    if data is None:
        return err(404, "资源不存在")
    return ok(data)


@router.post("/sessions/{session_id}/cash-flows")
def create_cash_flow(session_id: int, data: dict, db: Session = Depends(get_db)):
    """登记一笔外部现金流（③）。运行中会话立即计入虚拟账本，停止时下次启动重放。

    body: portfolio_id / flow_type(deposit|withdraw|dividend|other) / amount /
          flow_date(可选 YYYY-MM-DD，默认今天) / note(可选)。
    deposit/dividend/withdraw 传正数即可（符号由类型决定）；other 按符号判方向。
    """
    raw_pid = data.get("portfolio_id", data.get("portfolio_strategy_id"))
    try:
        portfolio_id = int(raw_pid)
    except (TypeError, ValueError):
        return err(400, "portfolio_id 必填且为整数")
    row, error = svc.record_cash_flow(
        db, session_id, portfolio_id,
        str(data.get("flow_type") or ""),
        data.get("amount"),
        flow_date=data.get("flow_date"),
        note=data.get("note"),
        engine=_ENGINES.get(session_id),
    )
    if error == "资源不存在":
        return err(404, error)
    if error is not None:
        return err(400, error)
    return ok(row)


@router.get("/sessions/{session_id}/decisions")
def session_decisions(session_id: int, db: Session = Depends(get_db)):
    """决策闸门事件（调参可观测性）：聚合统计 summary + 原始事件 events。

    覆盖信号→成交全链路拦截点（止损/止盈/熔断/资金不足/超仓/收盘/去重/桥拒单…），
    与回测详情的 decision_summary/decisions 同口径。
    """
    data = svc.session_decisions(db, session_id)
    if data is None:
        return err(404, "资源不存在")
    return ok(data)


@router.delete("/sessions/{session_id}")
def delete_session(session_id: int, db: Session = Depends(get_db)):
    ok_flag, message = svc.delete_session(db, session_id, _ENGINES)
    if not ok_flag:
        status = 409 if message and "运行中" in message else 404
        return err(status, message)
    return ok()


# ---------------------------------------------------------------------------
# SSE 事件流（HTTP 传输层，留路由）
# ---------------------------------------------------------------------------

def _sse_line(ev: dict) -> str:
    """把引擎事件 dict 格式化为一条 SSE：event: <type>\ndata: <json>\n\n。

    data 去掉 type 字段（type 进 event 行，design §5.6.10）；ensure_ascii=False
    让中文信号名（如 signal_name）原样下发。
    """
    ev_type = ev.get("type", "message")
    data = {k: v for k, v in ev.items() if k != "type"}
    return "event: %s\ndata: %s\n\n" % (ev_type, json.dumps(data, ensure_ascii=False))


async def _heartbeat_stream(request: Request, session_id: int,
                            ping_interval: float = 30.0):
    """未运行 session 的 /stream：仅 ping 保活心跳（默认 30s，EventSource 自动重连）。

    D-5（前端 I4）：心跳期间引擎注册（session 在别处启动/页面迟到）→ 每次 ping 前
    先查注册表，命中则发一条 `event: reconnect` 再结束流——前端收到后关闭当前
    EventSource 重连，落到真事件流（否则心跳流一直空转，用户看不到实时事件）。
    _ENGINES 是进程级 dict，.get 原子读，线程安全够用。
    """
    while True:
        if await request.is_disconnected():
            break
        if _ENGINES.get(session_id) is not None:
            yield _sse_line({"type": "reconnect", "session_id": session_id})
            return
        yield _sse_line({"type": "ping", "time": datetime.now().isoformat()})
        await asyncio.sleep(ping_interval)


@router.get("/sessions/{session_id}/stream")
async def session_stream(session_id: int, request: Request, db: Session = Depends(get_db)):
    """实盘 session 实时事件流（SSE，design §5.6.10）。

    运行中：转发引擎 stream_events（signal/order/trade/position/risk 五类事件，
    空闲 30s 由引擎内置 ping 心跳）；未运行：仅 ping 保活。
    每个连接绑定一个 session（无鉴权，EventSource 断线自动重连）。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        # 用 HTTPException(404) 而非 body-code:EventSource 需真实 HTTP 错误码才触发 onerror,
        # body-code + HTTP 200 会让 EventSource 静默失败无提示。
        raise HTTPException(status_code=404, detail="session 不存在")

    engine = _ENGINES.get(session_id)

    async def event_generator():
        if engine is not None:
            agen = engine.stream_events()
            try:
                async for ev in agen:
                    if await request.is_disconnected():
                        break
                    yield _sse_line(ev)
            finally:
                await agen.aclose()
        else:
            async for line in _heartbeat_stream(request, session_id):
                yield line

    from fastapi.responses import StreamingResponse
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive"},
    )


@router.get("/watchdog-status")
def watchdog_status():
    """进程内引擎存活监控的只读状态。

    ``last_scan_at`` 长时间不前进（> 2 个扫描周期）即说明扫描线程自身卡死/
    已退出——这是外部（前端/运维）判断「监控自己死了」的唯一可读信号。
    watchdog 未启动时返回 running=false，不报 500。
    """
    from core.engine.live.engine_watchdog import current_watchdog
    watchdog = current_watchdog()
    if watchdog is None:
        return ok({
            "running": False,
            "last_scan_at": None,
            "last_scan_duration_ms": None,
            "scan_count": 0,
            "active_alarms": [],
        })
    return ok(watchdog.status())
