# -*- coding: utf-8 -*-
"""市场多空状态卡服务：短时缓存 / 刷新绕过 / 取数失败报错 / 转折历史格式。"""
from datetime import date, datetime, timedelta

import polars as pl
import pytest

from core.engine.market_gate import MarketGateUnavailableError
from core.services import market_gate_service as svc


def _ohlcv(n=420, seed=3):
    import numpy as np
    rng = np.random.default_rng(seed)
    close = 4000.0 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
    open_ = close * (1 + rng.normal(0, 0.003, n))
    high = np.maximum(open_, close) * 1.004
    low = np.minimum(open_, close) * 0.996
    vol = rng.integers(1000, 100000, n).astype(float)
    return pl.DataFrame({
        "datetime": [datetime(2024, 1, 1) + timedelta(days=i) for i in range(n)],
        "Open": open_.tolist(), "High": high.tolist(),
        "Low": low.tolist(), "Close": close.tolist(), "Volume": vol.tolist(),
    })


@pytest.fixture(autouse=True)
def _clean_cache():
    svc.clear_cache()
    yield
    svc.clear_cache()


def test_cached_then_force_refresh(monkeypatch):
    calls = {"n": 0}
    df = _ohlcv()

    def fake(code, start, end, period="1d", **kwargs):
        calls["n"] += 1
        return df

    monkeypatch.setattr(svc, "build_gate_ohlcv", fake)
    first = svc.fetch_gate_status()
    second = svc.fetch_gate_status()
    assert calls["n"] == 1
    assert second["cached"] is True
    assert first["code"] == svc.DEFAULT_GATE_INDEX
    forced = svc.fetch_gate_status(force=True)
    assert calls["n"] == 2
    assert forced["cached"] is False


def test_payload_shape_and_transitions(monkeypatch):
    df = _ohlcv()
    monkeypatch.setattr(svc, "build_gate_ohlcv", lambda *a, **k: df)
    data = svc.fetch_gate_status(history=3)
    for key in ("code", "benchmark_name", "as_of", "direction", "direction_label",
                "strength", "since", "duration_bars", "transitions"):
        assert key in data
    assert data["direction"] in (-1, 0, 1, 2)
    assert 0 <= data["strength"] <= 100
    assert len(data["transitions"]) <= 3
    for item in data["transitions"]:
        assert set(item) == {"date", "state"}
        date.fromisoformat(item["date"])


def test_expected_latest_trading_day_respects_close_time():
    # 2026-11-18 是周三；15:00 前预期为前一交易日，15:00 后为当日
    assert svc.expected_latest_trading_day(
        datetime(2026, 11, 18, 10, 0)) == date(2026, 11, 17)
    assert svc.expected_latest_trading_day(
        datetime(2026, 11, 18, 15, 30)) == date(2026, 11, 18)


def test_expected_latest_trading_day_skips_weekend_and_holiday():
    # 周六 → 上周五
    assert svc.expected_latest_trading_day(
        datetime(2026, 11, 21, 10, 0)) == date(2026, 11, 20)
    # 国庆（10/1 休市）→ 9/30
    assert svc.expected_latest_trading_day(
        datetime(2026, 10, 1, 10, 0)) == date(2026, 9, 30)


def test_stale_flag_and_days(monkeypatch):
    df = _ohlcv()
    monkeypatch.setattr(svc, "build_gate_ohlcv", lambda *a, **k: df)
    data = svc.fetch_gate_status()
    as_of = date.fromisoformat(data["as_of"])

    svc.clear_cache()
    monkeypatch.setattr(
        svc, "expected_latest_trading_day", lambda now=None: as_of)
    fresh = svc.fetch_gate_status()
    assert fresh["stale"] is False
    assert fresh["stale_days"] == 0
    assert fresh["latest_trading_day"] == as_of.isoformat()

    svc.clear_cache()
    monkeypatch.setattr(
        svc, "expected_latest_trading_day",
        lambda now=None: as_of + timedelta(days=10))
    old = svc.fetch_gate_status()
    assert old["stale"] is True
    assert old["stale_days"] >= svc.GATE_STALE_TRADING_DAYS


def test_empty_ohlcv_raises(monkeypatch):
    monkeypatch.setattr(svc, "build_gate_ohlcv", lambda *a, **k: pl.DataFrame())
    with pytest.raises(MarketGateUnavailableError):
        svc.fetch_gate_status(force=True)


def test_short_history_yields_undetermined(monkeypatch):
    """历史不足（均线全预热区）→ 状态全 0「未定」，按放行处理而不是报错。"""
    monkeypatch.setattr(svc, "build_gate_ohlcv", lambda *a, **k: _ohlcv(n=3))
    data = svc.fetch_gate_status(force=True)
    assert data["direction"] == 0
    assert data["direction_label"] == "未定"
    assert data["duration_bars"] >= 1
