"""P2-7：TQ 全局锁（_tdx_lock）获取超时语义。

核心断言：超时抛 TDXLockTimeout 且 **不进入 with 体**——操作要么持锁执行、
要么一次都不执行，不存在「锁没拿到但 TQ 调用仍进行」的并发漏洞。
"""
import threading

import pytest

from core.tq import utils
from core.tq.utils import TDXLockTimeout, get_tdx_lock


def test_lock_timeout_raises_without_entering_body():
    holder_ready = threading.Event()
    release = threading.Event()

    def holder():
        with get_tdx_lock(timeout=1):
            holder_ready.set()
            release.wait(2)

    t = threading.Thread(target=holder)
    t.start()
    assert holder_ready.wait(1)

    ran = []
    with pytest.raises(TDXLockTimeout):
        with get_tdx_lock(timeout=0.05):
            ran.append(1)
    release.set()
    t.join(2)

    assert ran == []                    # 超时未进入 with 体


def test_lock_is_free_after_holder_releases():
    with get_tdx_lock(timeout=0.05):
        pass
    # 上一次释放后应能立即再取（证明 __exit__ 正确 release，无泄漏）
    with get_tdx_lock(timeout=0.05):
        assert utils._tdx_lock.locked()


def test_config_timeout_is_read_once(monkeypatch):
    monkeypatch.setattr(utils, "_lock_timeout_cache", None)
    monkeypatch.setattr(utils, "load_config",
                        lambda: {"tq_lock_timeout_seconds": 12})
    assert utils._lock_timeout_seconds() == 12.0
    # 第二次不再读配置（进程内缓存）
    monkeypatch.setattr(utils, "load_config",
                        lambda: {"tq_lock_timeout_seconds": 99})
    assert utils._lock_timeout_seconds() == 12.0


def test_invalid_config_falls_back_to_default(monkeypatch):
    monkeypatch.setattr(utils, "_lock_timeout_cache", None)
    monkeypatch.setattr(utils, "load_config",
                        lambda: {"tq_lock_timeout_seconds": "oops"})
    assert utils._lock_timeout_seconds() == utils._LOCK_TIMEOUT_DEFAULT
