from fastapi import APIRouter

from core.api.response import err, ok
from core.services.system_service import get_config, update_config
from core.services import auto_shutdown, backup_service, health_service, upgrade_service

router = APIRouter(prefix="/api/system", tags=["system"])


@router.get("/health-overview")
def health_overview_route():
    """统一健康总览（批次21 L3）：Core / 通道 / 数据新鲜度 / 当日任务 / 磁盘 / 待处理告警。

    纯只读聚合：不新建轮询、不发交易、不改库；单项读取失败只在 problems 中降级提示。
    """
    return ok(health_service.overview())


@router.get("/configs")
def get_config_route():
    return ok(get_config())


@router.put("/configs")
def update_config_route(data: dict):
    update_config(data)
    return ok()


@router.get("/upgrade-check")
def upgrade_check_route():
    """L5 升级前只读清单：当前/目标 revision、待执行迁移、备份新鲜度、回滚提示。

    纯只读（不执行 upgrade）；开关关闭时只把 ``enabled=false`` 带回，不做额外副作用。
    """
    return ok(upgrade_service.preflight())


@router.post("/upgrade/backup")
def upgrade_backup_route(data: dict):
    """L5 升级前强制备份：需 ``upgrade.enabled=true`` + ``confirm=true``。"""
    result = upgrade_service.prepare_backup(confirm=bool(data.get("confirm")))
    if not result.get("ok"):
        return err(400, result.get("message") or "升级前备份未执行", result)
    return ok(result)


@router.get("/backup")
def get_backup_route():
    """备份状态：配置 + 上次自动/手动备份记录 + 今日是否已自动备份。"""
    return ok(backup_service.get_status())


@router.post("/backup")
def create_backup_route():
    """立即备份（手动）。失败/跳过返回明确错误，绝不假装成功。

    同步 def 由 FastAPI 线程池执行，VACUUM INTO 不阻塞事件循环。
    手动备份不受 backup.enabled 限制（关闭自动备份后仍可手动备份）。
    """
    result = backup_service.run_backup(backup_service.KIND_MANUAL)
    if result.get("status") != backup_service.STATUS_SUCCESS:
        return err(500, result.get("message") or "数据备份失败", result)
    return ok(result)


@router.get("/backup/records")
def list_backup_records_route(limit: int = 20):
    """最近备份记录（恢复入口的选择列表）。"""
    return ok(backup_service.list_records(limit))


@router.post("/backup/verify")
def verify_backup_route(data: dict):
    """只读校验备份文件（不触碰当前库）：完整性 + 表数 + 各表行数。"""
    result = _resolve_backup_file(data)
    if isinstance(result, dict):
        return err(400, result.get("message") or "备份文件无法解析", result)
    verify = backup_service.verify_backup(result)
    if not verify.get("ok"):
        return err(400, verify.get("message") or "备份文件校验未通过", verify)
    return ok(verify)


@router.post("/backup/restore")
def restore_backup_route(data: dict):
    """从备份恢复当前库：需显式 confirm=true 且 backup.restore_enabled=true。

    前置校验全部通过才会动手；恢复前先自动备份当前库（失败即中止）。
    """
    result = backup_service.restore_backup(
        record_id=data.get("record_id"),
        path=data.get("path") or None,
        confirm=bool(data.get("confirm")),
    )
    if not result.get("ok"):
        return err(400, result.get("message") or "数据恢复失败", result)
    return ok(result)


def _resolve_backup_file(data: dict):
    """API 入参 → 备份文件路径；解析失败返回 ``{"message": ...}``。"""
    record_id = data.get("record_id")
    if record_id not in (None, ""):
        row = backup_service.find_record(record_id)
        if row is None or not row.path:
            return {"message": "备份记录不存在或缺少文件路径：id=%s" % record_id}
        return row.path
    path = str(data.get("path") or "").strip()
    if not path:
        return {"message": "请指定备份记录 id 或备份文件路径"}
    return path


@router.get("/heartbeat")
def heartbeat_get_route():
    """前端标签页存活心跳（打包版自动退出用；页面打开时每 5s 发送一次）。

    前端经 navigator.sendBeacon 发送（固定为 POST），GET 保留供直接调用/测试。
    与 POST 拆成两个函数共用同一实现：同一函数挂两个方法会让 FastAPI 生成
    重复 operationId（heartbeat_route_..._post），启动时打 Duplicate Operation ID
    告警并污染自动生成 SDK。
    """
    auto_shutdown.heartbeat()
    return ok()


@router.post("/heartbeat")
def heartbeat_post_route():
    """前端标签页存活心跳（POST，sendBeacon 固定方法）。"""
    return heartbeat_get_route()


@router.post("/closing")
def closing_route():
    """前端标签页关闭信号（pagehide）。打包版：宽限期内无新心跳则退出进程。"""
    auto_shutdown.request_close()
    return ok()


@router.post("/shutdown")
def shutdown_route():
    """显式退出：打包版退出进程并释放端口；开发模式 no-op。"""
    auto_shutdown.shutdown_now()
    return ok()
