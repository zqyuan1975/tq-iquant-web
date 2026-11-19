"""K1：回测协作式取消（2026-11-18）。

回测在请求线程内同步执行，无法安全强杀；采用「取消令牌 + 引擎循环检查」：

- 创建 record 后 ``register(record_ids)`` 登记一个 ``threading.Event``；
- 取消接口按 record_id 找到该 Event 并 ``set()``；
- 引擎每根 bar 前通过 ``cancel_check`` 检查，命中即中止并返回 canceled + 进度；
- 服务层把记录标 ``canceled``，**不落部分成交/快照**（避免半截结果被误当完整回测）。

多组合回测的所有子记录共享同一 Event：取消任一子记录即中止整批。

令牌仅在进程内有效；进程重启后残留 running 记录由启动清理逻辑标 failed。
"""
import threading
from typing import Dict, Iterable

_LOCK = threading.Lock()
_EVENTS: Dict[int, threading.Event] = {}


def register(record_ids: Iterable[int]) -> threading.Event:
    """为一批记录登记同一个取消令牌并返回（重复登记覆盖旧令牌）。"""
    event = threading.Event()
    with _LOCK:
        for rid in record_ids:
            _EVENTS[int(rid)] = event
    return event


def cancel(record_id: int) -> bool:
    """请求取消：命中本进程登记则置位并返回 True；未登记（已结束/跨进程）返回 False。"""
    with _LOCK:
        event = _EVENTS.get(int(record_id))
    if event is None:
        return False
    event.set()
    return True


def release(record_ids: Iterable[int]) -> None:
    """任务结束（完成/失败/取消）后摘除登记，避免令牌泄漏。"""
    with _LOCK:
        for rid in record_ids:
            _EVENTS.pop(int(rid), None)


def is_registered(record_id: int) -> bool:
    with _LOCK:
        return int(record_id) in _EVENTS
