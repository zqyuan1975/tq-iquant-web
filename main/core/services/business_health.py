# -*- coding: utf-8 -*-
"""业务级心跳（批次22 L6）：给进程级探活补上「引擎是否真在推进」的判据。

进程级 ``/health`` 只能证明 FastAPI 还活着；引擎线程死锁/行情停摆时进程依旧
返回 200。本模块只读采集：

- **轮次推进**：``engine._last_tick_at``（``_tick_main`` 每轮刷新）；
- **最近 bar**：快照合成器 ``last_completed_at``，无合成器时取 ``_available_bar``
  （日线及以上周期的 00:00:00 不参与判定）；
- **watchdog 轮次**：``current_watchdog().status()`` 的最近扫描时间/扫描轮数/活跃告警。

交易时段内运行中会话停滞超过阈值 → ``ok=false``；盘外/未运行/预热宽限内不判。
纯只读：不写库、不触发交易、不重启任何进程。
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Callable, Dict, Optional

from core.config import load_config
from core.engine.live.snapshot_bars import default_in_session
from core.engine.live.timing import now_shanghai

logger = logging.getLogger(__name__)

DEFAULT_STALL_SECONDS = 180.0
DEFAULT_GRACE_SECONDS = 120.0

#: 日线及以上周期（bar_time 为 00:00:00，无分钟级新鲜度语义）
_LONG_PERIODS = frozenset({"1d", "1w", "1mon", "day", "week", "month"})


def _as_float(value, default: float) -> float:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return default
    return n if n > 0 else default


def probe_settings(cfg: Optional[dict] = None) -> dict:
    """``health.business_probe_enabled``（默认 false）+ 阈值/宽限。"""
    cfg = load_config() if cfg is None else cfg
    raw = cfg.get("health") if isinstance(cfg, dict) else None
    raw = raw if isinstance(raw, dict) else {}
    return {
        "enabled": bool(raw.get("business_probe_enabled", False)),
        "stall_seconds": _as_float(
            raw.get("business_stall_seconds"), DEFAULT_STALL_SECONDS),
        "grace_seconds": _as_float(
            raw.get("business_grace_seconds"), DEFAULT_GRACE_SECONDS),
    }


def probe_enabled(cfg: Optional[dict] = None) -> bool:
    return probe_settings(cfg)["enabled"]


def _last_bar_at(engine: object) -> Optional[datetime]:
    synth = getattr(engine, "_snapshot_synth", None)
    if synth is not None:
        return getattr(synth, "last_completed_at", None)
    bar = getattr(engine, "_available_bar", None)
    period = str(getattr(bar, "period", "") or "").lower()
    if period in _LONG_PERIODS:
        return None
    return getattr(bar, "bar_time", None)


def _iso(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def evaluate(sessions: Dict[int, object], now: datetime, *,
             stall_seconds: float = DEFAULT_STALL_SECONDS,
             grace_seconds: float = DEFAULT_GRACE_SECONDS,
             in_session_fn: Callable[[datetime], bool] = default_in_session) -> dict:
    """逐会话判定业务心跳；任何异常只降级该会话，不抛出。"""
    in_session = bool(in_session_fn(now))
    items = []
    problems = []
    stale_any = False

    for sid, engine in (sessions or {}).items():
        try:
            running = bool(getattr(engine, "_running", False))
            started = getattr(engine, "_started_at", None)
            tick = getattr(engine, "_last_tick_at", None)
            bar = _last_bar_at(engine)
            candidates = [v for v in (tick, bar) if v is not None]
            last = max(candidates) if candidates else started
            silence = ((now - last).total_seconds() if last is not None else None)
            warming = (started is not None
                       and (now - started).total_seconds() < grace_seconds)
            stale = bool(running and in_session and not warming
                         and silence is not None and silence > stall_seconds)
            if stale:
                stale_any = True
                problems.append(
                    "会话 %s 业务心跳停滞 %.0f 秒（最后活动 %s，阈值 %.0f 秒）"
                    % (sid, silence, last.strftime("%H:%M:%S"), stall_seconds))
            items.append({
                "session_id": int(sid),
                "running": running,
                "last_tick_at": _iso(tick),
                "last_bar_at": _iso(bar),
                "silence_seconds": round(silence, 1) if silence is not None else None,
                "warming": warming,
                "stale": stale,
            })
        except Exception as exc:  # noqa: BLE001 — 单个引擎异常不影响整体
            logger.warning("business-health: 会话 %s 采样失败（%s: %s）",
                           sid, type(exc).__name__, exc)
            items.append({"session_id": int(sid), "running": None,
                          "stale": False, "error": str(exc)})

    return {"in_session": in_session, "ok": not stale_any,
            "sessions": items, "problems": problems}


def _watchdog_state(now: datetime) -> dict:
    try:
        from core.engine.live.engine_watchdog import current_watchdog

        wd = current_watchdog()
        if wd is None:
            return {"running": False}
        status = wd.status()
    except Exception as exc:  # noqa: BLE001 — 依赖缺失视为未运行
        logger.info("business-health: 读取 watchdog 失败（%s: %s）",
                    type(exc).__name__, exc)
        return {"running": False}
    last = status.get("last_scan_at")
    stale = False
    if status.get("running") and last:
        try:
            silence = (now - datetime.fromisoformat(str(last))).total_seconds()
            stale = silence > float(status.get("interval_seconds") or 30.0) * 3
        except (ValueError, TypeError):
            stale = False
    return {
        "running": bool(status.get("running")),
        "last_scan_at": last,
        "scan_count": status.get("scan_count"),
        "active_alarms": len(status.get("active_alarms") or []),
        "stale": stale,
    }


def snapshot(now: Optional[datetime] = None,
             engine_provider: Optional[Callable[[], Dict[int, object]]] = None) -> dict:
    """业务心跳快照（供 ``/health`` 与 supervisor 判据）。只读、无副作用。"""
    settings = probe_settings()
    now = now or now_shanghai()
    if not settings["enabled"]:
        return {"enabled": False, "ok": True}
    if engine_provider is None:
        from core.services import live_service

        engine_provider = lambda: dict(live_service.ENGINES)  # noqa: E731
    result = evaluate(
        engine_provider() or {}, now,
        stall_seconds=settings["stall_seconds"],
        grace_seconds=settings["grace_seconds"])
    watchdog = _watchdog_state(now)
    problems = list(result["problems"])
    if watchdog.get("stale"):
        problems.append("引擎 watchdog 扫描超过 3 个周期未推进")
    return {
        "enabled": True,
        "ok": bool(result["ok"]) and not watchdog.get("stale"),
        "checked_at": now.isoformat(),
        "in_session": result["in_session"],
        "stall_seconds": settings["stall_seconds"],
        "sessions": result["sessions"],
        "watchdog": watchdog,
        "problems": problems,
    }
