from fastapi import APIRouter

from core.api.response import err, ok
from core.services.system_service import get_config, update_config
from core.services import auto_shutdown, backup_service

router = APIRouter(prefix="/api/system", tags=["system"])


@router.get("/configs")
def get_config_route():
    return ok(get_config())


@router.put("/configs")
def update_config_route(data: dict):
    update_config(data)
    return ok()


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


@router.api_route("/heartbeat", methods=["GET", "POST"])
def heartbeat_route():
    """前端标签页存活心跳（打包版自动退出用；页面打开时每 5s 发送一次）。

    前端经 navigator.sendBeacon 发送（固定为 POST），GET 保留供直接调用/测试。
    """
    auto_shutdown.heartbeat()
    return ok()


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
