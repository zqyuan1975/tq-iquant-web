# -*- coding: utf-8 -*-
"""保护窗内分钟合成快照兜底（``snapshot_fallback``）单元测试。

全部离线：不连主站、不读写业务库，只用注入时钟 + 纯内存桩。
"""
import time
from datetime import datetime, timedelta, timezone

import pytest

from core.services.minute_download.snapshot_fallback import SnapshotFallbackGuard
from core.tq import gate as g

_TZ = timezone(timedelta(hours=8))
_CFG = {
    "enabled": True,
    "pre_market_p2": ["08:00", "09:10"],
    "lunch_p2": ["11:30", "13:00"],
    "after_close_p2": ["15:15", "24:00"],
    "trading": [["09:10", "11:30"], ["13:00", "15:10"]],
    "p0_only": ["14:55", "15:10"],
    "p1_max_per_minute": 6000,
    "p2_max_per_minute": 6000,
    "p2_chunk_size": 60,
    "p3_max_per_minute": 30,
    "override_max_minutes": 120,
}


def _at(hh: int, mm: int, ss: int = 0, day: int = 23) -> datetime:
    """2026-11-23 是周一（交易日）。"""
    return datetime(2026, 11, day, hh, mm, ss, tzinfo=_TZ)


@pytest.fixture(autouse=True)
def _gated():
    g.reset_for_tests(config=dict(_CFG),
                      trading_day_fn=lambda d: d.weekday() < 5,
                      live_running_fn=lambda: True)
    g.clear_override()
    g.GATE.reset_state()
    yield
    g.clear_override()
    g.reset_for_tests(config=None, clock=None, trading_day_fn=None,
                      live_running_fn=None)
    g.GATE.reset_state()


class _Recorder:
    """记录兜底/正常两条取数路径的调用次数。"""

    def __init__(self, data=None, error=None):
        self.data = data if data is not None else {"600000": {"Now": 10.5}}
        self.error = error
        self.normal_calls = 0
        self.fallback_calls = 0

    def normal(self, codes):
        self.normal_calls += 1
        if self.error is not None:
            raise self.error
        return dict(self.data)

    def fallback(self, codes):
        self.fallback_calls += 1
        return dict(self.data)


def _store_state(*, age=5.0, lag=0, error_at=None, ok_at="2026-11-24 14:54:00"):
    return {
        "last_query_age_seconds": age,
        "last_lag_minutes": lag,
        "last_read_error_at": error_at,
        "last_ok_at": ok_at,
    }


def _guard(now_box, *, store=None, native=None, config=None, emit_event=None,
           recorder=None):
    rec = recorder or _Recorder()
    guard = SnapshotFallbackGuard(
        rec.normal,
        rec.fallback,
        store_state=(lambda: store.get("v")) if store else None,
        native_state=(lambda: native.get("v")) if native else None,
        clock=lambda: now_box["now"],
        emit_event=emit_event,
        config=config,
    )
    return guard, rec


def test_healthy_store_never_falls_back():
    now = {"now": _at(14, 56)}
    store = {"v": _store_state(lag=0)}
    guard, rec = _guard(now, store=store)

    with pytest.raises(g.TdxTaskBlocked) as ei:
        guard(["600000"])
    assert ei.value.reason == "healthy"
    assert rec.fallback_calls == 0
    assert rec.normal_calls == 0
    st = guard.status()
    assert st["triggered"] is False
    assert st["decisions"]["blocked_healthy"] == 1


def test_store_read_error_triggers_fallback():
    now = {"now": _at(14, 56)}
    store = {"v": _store_state(
        lag=0, error_at="2026-11-24 14:55:30",
        ok_at="2026-11-24 14:54:00")}
    guard, rec = _guard(now, store=store)

    assert guard(["600000"]) == {"600000": {"Now": 10.5}}
    assert rec.fallback_calls == 1
    st = guard.status()
    assert st["triggered"] is True
    assert st["reason"] == "store_read_error"


def test_store_stale_beyond_threshold_triggers_fallback():
    now = {"now": _at(14, 56)}
    store = {"v": _store_state(lag=5)}
    guard, rec = _guard(now, store=store)

    guard(["600000"])
    assert rec.fallback_calls == 1
    st = guard.status()
    assert st["reason"] == "store_stale"
    assert st["store_lag_minutes"] == 5


def test_empty_store_counts_as_stale():
    now = {"now": _at(14, 56)}
    store = {"v": _store_state(lag=None)}
    guard, rec = _guard(now, store=store)

    guard(["600000"])
    assert rec.fallback_calls == 1
    assert guard.status()["reason"] == "store_stale"


def test_stale_state_is_ignored_when_query_is_old():
    """轮询暂停很久后库状态不可信，不能仅凭旧状态触发兜底。"""
    now = {"now": _at(14, 56)}
    store = {"v": _store_state(age=600.0, lag=30)}
    guard, rec = _guard(now, store=store)

    with pytest.raises(g.TdxTaskBlocked) as ei:
        guard(["600000"])
    assert ei.value.reason == "healthy"
    assert rec.fallback_calls == 0


def test_native_stalled_triggers_fallback():
    now = {"now": _at(14, 56)}
    native = {"v": {"stalled": True}}
    guard, rec = _guard(now, native=native)

    guard(["600000"])
    assert rec.fallback_calls == 1
    assert guard.status()["reason"] == "native_stalled"


def test_multiple_triggers_are_aggregated():
    now = {"now": _at(14, 56)}
    store = {"v": _store_state(lag=5)}
    native = {"v": {"stalled": True}}
    guard, rec = _guard(now, store=store, native=native)

    guard(["600000"])
    assert guard.status()["reason"] == "store_stale,native_stalled"
    assert rec.fallback_calls == 1


def test_closing_auction_is_explicitly_blocked():
    """14:57 起为收盘集合竞价：宁可缺数据，也不合成假 bar。"""
    now = {"now": _at(14, 57, 30)}
    store = {"v": _store_state(lag=5)}
    guard, rec = _guard(now, store=store)

    with pytest.raises(g.TdxTaskBlocked) as ei:
        guard(["600000"])
    assert ei.value.reason == "closing_auction"
    assert rec.fallback_calls == 0
    assert guard.status()["window"]["state"] == "closing_auction"


def test_before_fallback_window_is_blocked():
    """保护窗已开、但兜底窗口（可配置）尚未开始：不触发。"""
    now = {"now": _at(14, 55, 30)}
    guard, rec = _guard(
        now, store={"v": _store_state(lag=5)},
        config={"protected_fallback_window_start": "14:56"})

    with pytest.raises(g.TdxTaskBlocked) as ei:
        guard(["600000"])
    assert ei.value.reason == "outside_fallback_window"
    assert rec.fallback_calls == 0


def test_outside_protected_window_uses_normal_path():
    now = {"now": _at(10, 30)}
    store = {"v": _store_state(lag=30)}
    guard, rec = _guard(now, store=store)

    assert guard(["600000"]) == {"600000": {"Now": 10.5}}
    assert rec.normal_calls == 1
    assert rec.fallback_calls == 0
    assert guard.status()["window"]["state"] == "outside"


def test_fallback_is_locally_rate_limited():
    now = {"now": _at(14, 56)}
    guard, rec = _guard(
        now, store={"v": _store_state(lag=5)},
        config={"fallback_min_interval_seconds": 60})

    assert guard(["600000"])
    with pytest.raises(g.TdxTaskBlocked) as ei:
        guard(["600000"])
    assert ei.value.reason == "rate_limited"
    assert rec.fallback_calls == 1
    assert guard.status()["decisions"]["blocked_rate_limited"] == 1


def test_fallback_disabled_by_config():
    now = {"now": _at(14, 56)}
    guard, rec = _guard(
        now, store={"v": _store_state(lag=5)},
        config={"protected_fallback_enabled": False})

    with pytest.raises(g.TdxTaskBlocked) as ei:
        guard(["600000"])
    assert ei.value.reason == "fallback_disabled"
    assert rec.fallback_calls == 0


def test_event_emitted_once_per_episode_with_context():
    now = {"now": _at(14, 56)}
    events = []
    guard, rec = _guard(
        now, store={"v": _store_state(lag=7)},
        config={"fallback_min_interval_seconds": 0.001,
                "fallback_event_cooldown_seconds": 600},
        emit_event=events.append)

    assert guard(["600000"])
    time.sleep(0.02)
    assert guard(["600000"])

    assert rec.fallback_calls == 2
    assert len(events) == 1, "同一 episode 内不重复刷事件"
    ev = events[0]
    assert ev["reason"] == "store_stale"
    assert ev["store_lag_minutes"] == 7
    assert ev["window"] == "p0_only"
    assert ev["window_state"] == "open"
    assert ev["fallback_window"] == ["14:55", "14:57"]
    assert ev["at"].startswith("2026-11-23 14:56")
    assert guard.status()["events"] == 1


def test_lock_timeout_is_counted_and_not_swallowed():
    now = {"now": _at(14, 56)}

    class _BlockedFallback(_Recorder):
        def fallback(self, codes):
            self.fallback_calls += 1
            raise g.TdxTaskBlocked("p3 denied", priority=g.P3,
                                   reason="p3_window_only")

    rec = _BlockedFallback()
    guard, _ = _guard(now, store={"v": _store_state(lag=5)}, recorder=rec)

    with pytest.raises(g.TdxTaskBlocked):
        guard(["600000"])
    st = guard.status()
    assert st["decisions"]["lock_timeout"] == 1
    assert st["active"] is False


def test_status_reports_thresholds_and_window():
    now = {"now": _at(14, 56)}
    guard, _ = _guard(now, store={"v": _store_state(lag=0)})

    st = guard.status()
    assert st["enabled"] is True
    assert st["window"] == {"start": "14:55", "end": "14:57", "state": "open"}
    assert st["triggered"] is False
    assert st["last_trigger_at"] is None
    assert st["thresholds"] == {
        "store_lag_minutes": 2,
        "min_interval_seconds": 5.0,
        "max_per_minute": 12,
    }
