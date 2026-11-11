"""P2-8：主循环分段耗时画像与饱和告警。

观测逻辑不改任何交易决策：只断言画像落值、阈值/节流与事件字段。
"""
import time
from datetime import datetime, timedelta
from types import SimpleNamespace

from core.engine.live_engine import LiveEngine


def _stub(interval=60):
    eng = LiveEngine.__new__(LiveEngine)
    eng.ctx = SimpleNamespace(session_id=1)
    eng._engine_key = "iquant"
    eng._poll_interval = interval
    eng._last_tick_profile = {}
    eng._tick_profile_total_ms = 0
    eng._tick_saturation_warned_at = None
    events = []
    eng._emit = lambda kind, payload: events.append((kind, payload))
    return eng, events


def test_record_tick_profile_under_threshold_has_no_event():
    eng, events = _stub()
    eng._record_tick_profile(datetime(2026, 8, 5, 10, 0), {"poll": 1000},
                             time.monotonic())
    assert events == []
    assert eng._last_tick_profile["poll"] == 1000
    assert eng._tick_profile_total_ms < 51000


def test_record_tick_profile_saturation_emits_once_per_5_minutes():
    now = datetime(2026, 8, 5, 10, 0)
    eng, events = _stub()
    eng._record_tick_profile(
        now, {"poll": 50000, "sweep_timeout": 5000}, time.monotonic() - 60)
    kind, payload = events[-1]
    assert kind == "risk"
    assert payload["gate"] == "loop_saturation"
    assert payload["rule"] == "tick_too_slow"
    assert payload["round_ms"] >= 51000
    assert "poll=" in payload["top_segments"]

    # 5 分钟内再度饱和：节流，不重复推送
    eng._record_tick_profile(
        now + timedelta(seconds=60), {"poll": 50000}, time.monotonic() - 60)
    assert len(events) == 1
