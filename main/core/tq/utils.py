import logging
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from core.config import load_config

logger = logging.getLogger(__name__)


class TDXConnectionError(Exception):
    """通达信通道不可用（未启动/未登录/tqcenter 缺失/初始化失败）。"""


class TDXLockTimeout(TDXConnectionError):
    """等待 TQ 全局锁超时（P2-7）。

    继承 TDXConnectionError：对上层语义仍是「本轮 TQ 通道不可用」——路由层
    可直接给 503，引擎轮询按「取数失败」跳过本轮，不需要额外分支。
    """


_tdx_lock = threading.Lock()
# 单例初始化专用锁：与 _tdx_lock 分开，避免「已持 _tdx_lock 的调用方再进
# get_tq()」时自锁死。tqcenter 非线程安全，initialize 必须串行。
_init_lock = threading.Lock()
_tq = None
_tq_initialized = False

# P2-7：锁获取超时默认值/进程内缓存。读 config.yaml 有磁盘 IO，TQ 调用是热路径，
# 只在首次获取时读一次；改配置需重启（config.yaml 说明同此）。
_LOCK_TIMEOUT_DEFAULT = 30.0
_lock_timeout_cache: Optional[float] = None


def _lock_timeout_seconds() -> float:
    """config.tq_lock_timeout_seconds → 秒；缺失/非法回落 30s，<=0 表示无限阻塞。"""
    global _lock_timeout_cache
    if _lock_timeout_cache is None:
        try:
            raw = load_config().get("tq_lock_timeout_seconds",
                                    _LOCK_TIMEOUT_DEFAULT)
            value = float(raw)
        except (TypeError, ValueError):
            value = _LOCK_TIMEOUT_DEFAULT
        _lock_timeout_cache = value if value > 0 else 0.0
    return _lock_timeout_cache


class _TDXLockGuard:
    """``with get_tdx_lock():`` 的超时门闩（P2-7）。

    语义：
    - 拿到锁 → 与直接用 ``threading.Lock`` 完全一致；
    - 等待超过 ``timeout`` → 抛 :class:`TDXLockTimeout`，**不进入 with 体**。
      因此不存在「锁已释放/没拿到但操作仍在跑」的并发漏洞——操作要么持锁执行，
      要么一次都不执行；真正的持锁者（可能卡在 TQ DLL 里）不受影响。
    - ``timeout <= 0`` → 退回旧行为（无限阻塞），用于需要长时间等锁的离线任务。

    兼容 Lock 接口（acquire/release/locked），既有调用点 ``with get_tdx_lock():``
    与 ``get_tdx_lock().acquire()`` 均无需改动。
    """

    __slots__ = ("_timeout",)

    def __init__(self, timeout: float) -> None:
        self._timeout = float(timeout)

    def acquire(self, blocking: bool = True, timeout: Optional[float] = None) -> bool:
        if not blocking:
            return _tdx_lock.acquire(False)
        t = self._timeout if timeout is None else float(timeout)
        if t <= 0:
            return _tdx_lock.acquire(True)
        return _tdx_lock.acquire(True, t)

    def release(self) -> None:
        _tdx_lock.release()

    def locked(self) -> bool:
        return _tdx_lock.locked()

    def __enter__(self):
        started = time.monotonic()
        ok = self.acquire()
        if not ok:
            waited = time.monotonic() - started
            logger.warning(
                "等待通达信锁超时 %.1fs（tq_lock_timeout_seconds=%s）：本轮跳过 TQ 调用",
                waited, self._timeout,
            )
            raise TDXLockTimeout(
                "等待通达信锁超时（%.1fs），TQ 通道正被其它任务占用" % self._timeout)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        self.release()
        return False


def get_tdx_lock(timeout: Optional[float] = None) -> _TDXLockGuard:
    """TQ 全局锁（可超时）。默认超时取 config.tq_lock_timeout_seconds（30s）。

    所有 ``with get_tdx_lock():`` 的调用点在持锁超时后都会抛 TDXLockTimeout：
    数据/公式/预警调用按「本轮无数据」跳过，下单通道在**未发出任何委托前**
    中断（不会产生半提交状态），由引擎的 180s 超时/失败回队兜底。
    """
    if timeout is None:
        timeout = _lock_timeout_seconds()
    return _TDXLockGuard(timeout)


def _tdx_path() -> str:
    """从 config.yaml 读 tdx_path；缺失回退默认。"""
    cfg = load_config()
    p = cfg.get("tdx_path", "") or "D:\\zd_zyb"
    return p


def inject_tqcenter_path(tdx_path: str) -> bool:
    p = Path(tdx_path) / "PYPlugins"
    for sub in ("sys", "user"):
        candidate = p / sub
        if candidate.exists():
            s = str(candidate)
            if s not in sys.path:
                sys.path.append(s)
    return True


def _import_tq():
    """导入 tqcenter.tq；失败统一转成 TDXConnectionError（而非裸 ModuleNotFoundError）。

    裸 ModuleNotFoundError 不会被路由层识别，会冒泡成"服务器内部错误"，
    用户无法知道是通达信路径配置问题。
    """
    try:
        from tqcenter import tq
    except ModuleNotFoundError as e:
        raise TDXConnectionError(
            "无法加载 tqcenter（未在通达信目录的 PYPlugins 下找到 tqcenter.py）。"
            f"请检查配置中的通达信路径是否正确，当前路径：{_tdx_path()}"
        ) from e
    return tq


def get_tq():
    """获取 tqcenter.tq 单例，首次调用惰性 initialize 连接。

    连接标识用本文件路径（utils.py），唯一标识本进程。
    需通达信（回测版 new_tdx64）已启动并登录行情。
    """
    global _tq, _tq_initialized
    if _tq is not None and _tq_initialized:
        return _tq
    # 首次导入 + initialize 串行：两个线程并发进 initialize 会对同一 DLL 句柄
    # 重复初始化（tqcenter 非线程安全）。快路径在上面，正常调用无锁开销。
    with _init_lock:
        if _tq is None:
            inject_tqcenter_path(_tdx_path())
            _tq = _import_tq()
        if not _tq_initialized:
            try:
                _tq.initialize(__file__)
            except TDXConnectionError:
                raise
            except Exception as e:
                raise TDXConnectionError(
                    f"连接通达信失败，请确认通达信已启动并登录：{e}") from e
            _tq_initialized = True
    return _tq


def init_tq(tdx_path: str):
    """显式初始化（兼容旧接口）；tdx_path 用于注入 sys.path。"""
    global _tq, _tq_initialized
    inject_tqcenter_path(tdx_path)
    if _tq is None:
        _tq = _import_tq()
    try:
        _tq.initialize(__file__)
    except TDXConnectionError:
        raise
    except Exception as e:
        raise TDXConnectionError(f"连接通达信失败，请确认通达信已启动并登录：{e}") from e
    _tq_initialized = True


def close_tq():
    global _tq, _tq_initialized
    if _tq is not None:
        try:
            _tq.close()
        except Exception:
            pass
        _tq = None
        _tq_initialized = False
