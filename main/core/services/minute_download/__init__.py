"""内置分钟数据下载（通达信 7709 直连 → 平台自有存储 → 实盘优先读层）。

对外入口：
- :class:`MinuteDownloadService`：盘中循环下载器（start/stop/status）；
- :class:`StoreFirstMinuteDispatcher`：实盘优先读层（包装
  ``NativeMinuteDispatcher``，不改其语义）；
- :func:`build_service` / :func:`is_enabled` / :func:`store_first_enabled`：
  接线用的组装与开关读取（开关默认关闭）。

设计见 ``docs/plans/0022-内置分钟数据下载-设计.md``。
"""
from __future__ import annotations

from typing import Callable, Iterable, Optional

from .downloader import (
    PERIOD, SOURCE, DownloadConfig, MinuteDownloadService, RateLimiter,
    planned_missing_days,
)
from .hosts import HostPool, load_host_pool
from .protocol import BarRecord, TdxClient, normalize_code
from .store import MinuteStore, default_store_path
from .store_source import StoreFirstMinuteDispatcher

#: 默认关闭：任一开关开启才会改变现网行为。
DEFAULT_ENABLED = False
DEFAULT_STORE_FIRST = False


def is_enabled(section: Optional[dict]) -> bool:
    """下载器总开关 ``live.minute_download.enabled``（默认 false）。"""
    return bool((section or {}).get("enabled", DEFAULT_ENABLED))


def store_first_enabled(section: Optional[dict]) -> bool:
    """优先读层开关 ``live.minute_download.store_first``（默认 false）。"""
    return bool((section or {}).get("store_first", DEFAULT_STORE_FIRST))


def build_service(
    codes_provider: Optional[Callable[[], Iterable[str]]] = None,
    section: Optional[dict] = None,
    clock=None,
    request_fn=None,
    on_event: Optional[Callable[[dict], None]] = None,
) -> MinuteDownloadService:
    """按配置段组装下载器（不启动）。"""
    section = section if isinstance(section, dict) else {}
    cfg = DownloadConfig.from_section(section)
    store = MinuteStore(section.get("store_path") or None)
    pool = load_host_pool(
        section,
        failure_threshold=cfg.failure_threshold,
        isolation_seconds=cfg.isolation_seconds,
    )
    return MinuteDownloadService(
        store=store, config=cfg, host_pool=pool,
        codes_provider=codes_provider, request_fn=request_fn, clock=clock,
        on_event=on_event,
    )


__all__ = [
    "BarRecord", "DownloadConfig", "HostPool", "MinuteDownloadService",
    "MinuteStore", "PERIOD", "RateLimiter", "SOURCE",
    "StoreFirstMinuteDispatcher", "TdxClient", "build_service",
    "default_store_path", "is_enabled", "load_host_pool", "normalize_code",
    "planned_missing_days", "store_first_enabled",
]
