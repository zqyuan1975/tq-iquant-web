"""通达信 7709 主站池：内置默认列表 + 配置覆盖、探活排序、失败隔离。

决策（设计稿 §11-1）：内置一份公开的默认主站地址，开箱即可用；配置
``live.minute_download.hosts`` 或环境变量 ``TQ_MINUTE_HOSTS`` 可覆盖。

策略：
- ``candidates()`` 按「未隔离优先、延迟升序、失败次数升序」给出候选；
- 单主站连续失败 ``failure_threshold`` 次 → 隔离 ``isolation_seconds``；
- 隔离到期自动解禁（半开：下次失败重新隔离）；
- 成功一次即清零连续失败并刷新延迟。
"""
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Tuple

#: 公开的通达信行情主站地址（可按部署环境替换；不来自任何外部工具的私有清单）。
DEFAULT_HOSTS: Tuple[Tuple[str, int], ...] = (
    ("119.147.212.81", 7709),
    ("119.147.212.83", 7709),
    ("14.215.128.18", 7709),
    ("115.238.56.198", 7709),
    ("124.71.187.122", 7709),
    ("111.229.247.189", 7709),
)

DEFAULT_FAILURE_THRESHOLD = 3
DEFAULT_ISOLATION_SECONDS = 60.0

#: 未探活过的延迟用该值排序（排在已探活节点之后）。
_UNKNOWN_LATENCY = float("inf")


@dataclass
class HostState:
    host: str
    port: int = 7709
    success: int = 0
    failure: int = 0
    consecutive_failures: int = 0
    latency_ms: Optional[float] = None
    isolated_until: float = 0.0
    last_error: str = ""

    @property
    def key(self) -> str:
        return "%s:%d" % (self.host, self.port)


class HostPool:
    """主站候选与健康度（线程安全）。"""

    def __init__(self, hosts: Optional[Iterable] = None,
                 failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
                 isolation_seconds: float = DEFAULT_ISOLATION_SECONDS,
                 clock=time.monotonic) -> None:
        self.failure_threshold = max(int(failure_threshold), 1)
        self.isolation_seconds = max(float(isolation_seconds), 0.0)
        self._clock = clock
        self._lock = threading.Lock()
        self._states: List[HostState] = []
        for item in (hosts if hosts is not None else DEFAULT_HOSTS):
            self._states.append(_to_state(item))

    def _find(self, host: str, port: int) -> Optional[HostState]:
        for st in self._states:
            if st.host == host and st.port == port:
                return st
        return None

    def _get(self, host: str, port: int) -> HostState:
        st = self._find(host, port)
        if st is None:
            st = HostState(host=str(host), port=int(port))
            self._states.append(st)
        return st

    def candidates(self) -> List[Tuple[str, int]]:
        now = self._clock()
        with self._lock:
            ordered = sorted(
                self._states,
                key=lambda s: (
                    1 if s.isolated_until > now else 0,
                    s.latency_ms if s.latency_ms is not None else _UNKNOWN_LATENCY,
                    s.consecutive_failures,
                ),
            )
            return [(s.host, s.port) for s in ordered]

    def available(self) -> List[Tuple[str, int]]:
        now = self._clock()
        with self._lock:
            return [(s.host, s.port) for s in self._states
                    if s.isolated_until <= now]

    def report_success(self, host: str, port: int,
                       latency_ms: Optional[float] = None) -> None:
        with self._lock:
            st = self._get(host, port)
            st.success += 1
            st.consecutive_failures = 0
            st.isolated_until = 0.0
            st.last_error = ""
            if latency_ms is not None and latency_ms >= 0:
                prev = st.latency_ms
                st.latency_ms = (float(latency_ms) if prev is None
                                 else prev * 0.7 + float(latency_ms) * 0.3)

    def report_failure(self, host: str, port: int,
                       error: str = "") -> None:
        with self._lock:
            st = self._get(host, port)
            st.failure += 1
            st.consecutive_failures += 1
            st.last_error = str(error)[:200]
            if st.consecutive_failures >= self.failure_threshold:
                st.isolated_until = self._clock() + self.isolation_seconds

    def size(self) -> int:
        return len(self._states)

    def snapshot(self) -> List[dict]:
        now = self._clock()
        with self._lock:
            return [{
                "host": s.key,
                "success": s.success,
                "failure": s.failure,
                "consecutive_failures": s.consecutive_failures,
                "latency_ms": (round(s.latency_ms, 1)
                               if s.latency_ms is not None else None),
                "isolated": s.isolated_until > now,
                "last_error": s.last_error,
            } for s in self._states]


def _to_state(item) -> HostState:
    if isinstance(item, HostState):
        return item
    if isinstance(item, dict):
        return HostState(host=str(item.get("host") or ""),
                         port=int(item.get("port") or 7709))
    if isinstance(item, str):
        text = item.strip()
        host, _, port = text.partition(":")
        return HostState(host=host, port=int(port) if port else 7709)
    if isinstance(item, Sequence) and len(item) >= 2:
        return HostState(host=str(item[0]), port=int(item[1]))
    raise ValueError("无法解析主站配置项: %r" % (item,))


def parse_hosts(value) -> Optional[List[HostState]]:
    """配置/环境变量 → 主站列表；空值返回 None（调用方回退内置默认）。"""
    if value is None:
        return None
    items = []
    if isinstance(value, str):
        items = [p.strip() for p in value.split(",") if p.strip()]
    elif isinstance(value, (list, tuple)):
        items = list(value)
    else:
        return None
    out = [_to_state(item) for item in items if item]
    return out or None


def load_host_pool(section: Optional[dict] = None, *,
                   failure_threshold: int = DEFAULT_FAILURE_THRESHOLD,
                   isolation_seconds: float = DEFAULT_ISOLATION_SECONDS,
                   clock=time.monotonic) -> HostPool:
    """按「环境变量 > 配置 > 内置默认」组装主站池。"""
    env = os.environ.get("TQ_MINUTE_HOSTS")
    configured = parse_hosts(env) or parse_hosts(
        (section or {}).get("hosts") if isinstance(section, dict) else None)
    hosts = configured if configured else list(DEFAULT_HOSTS)
    return HostPool(hosts, failure_threshold=failure_threshold,
                    isolation_seconds=isolation_seconds, clock=clock)


__all__ = [
    "DEFAULT_HOSTS", "HostPool", "HostState", "load_host_pool", "parse_hosts",
]
