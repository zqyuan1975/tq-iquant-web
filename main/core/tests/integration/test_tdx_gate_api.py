"""TQ 通道闸接口：状态、二次确认、决策事件、紧急放开。"""
import os

import pytest

from core.tq import gate as g

_BASE_CFG = {
    "enabled": True,
    "pre_market_p2": ["08:00", "09:10"],
    "lunch_p2": ["11:30", "13:00"],
    "after_close_p2": ["15:15", "24:00"],
    "trading": [["09:10", "11:30"], ["13:00", "15:10"]],
    "p0_only": ["14:55", "15:10"],
    "p1_max_per_minute": 6000,
    "p2_max_per_minute": 6000,
    "p2_chunk_size": 60,
    "override_max_minutes": 120,
}


@pytest.fixture(autouse=True)
def _gated(monkeypatch):
    monkeypatch.delenv("TQ_GATE_FORCE_ALLOW", raising=False)
    from datetime import datetime, timedelta, timezone
    tz = timezone(timedelta(hours=8))
    g.reset_for_tests(config=dict(_BASE_CFG),
                      clock=lambda: datetime(2026, 11, 23, 10, 0, tzinfo=tz),
                      trading_day_fn=lambda d: d.weekday() < 5,
                      live_running_fn=lambda: True)
    g.clear_override()
    yield
    g.clear_override()
    g.reset_for_tests(config=None, clock=None, trading_day_fn=None,
                      live_running_fn=None)


def test_status_reports_trading_block(test_client):
    res = test_client.get("/api/system/tdx-gate").json()
    assert res["code"] == 0
    data = res["data"]
    assert data["enabled"] is True
    assert data["policy"]["window"] == "trading"
    assert data["policy"]["p2_allowed"] is False
    assert data["policy"]["p1_allowed"] is True


def test_override_requires_confirm_and_reason(test_client):
    res = test_client.post("/api/system/tdx-gate/override",
                           json={"reason": "紧急排查"}).json()
    assert res["code"] == 400
    res = test_client.post("/api/system/tdx-gate/override",
                           json={"confirm": True}).json()
    assert res["code"] == 400
    assert g.override_info() is None


def test_status_no_live_engine_does_not_block_p2(test_client):
    """刚安装无实盘：状态接口不得报交易时段拦截（window=no_live_engine）。"""
    from datetime import datetime, timedelta, timezone
    tz = timezone(timedelta(hours=8))
    g.reset_for_tests(config=dict(_BASE_CFG),
                      clock=lambda: datetime(2026, 11, 23, 10, 0, tzinfo=tz),
                      trading_day_fn=lambda d: d.weekday() < 5,
                      live_running_fn=lambda: False)
    res = test_client.get("/api/system/tdx-gate").json()
    data = res["data"]
    assert data["policy"]["live_running"] is False
    assert data["policy"]["window"] == "no_live_engine"
    assert data["policy"]["p2_allowed"] is True
    assert data["policy"]["p1_allowed"] is True
