# -*- coding: utf-8 -*-
"""行业数据装配：窗口预热 / 不足 300 根 fail-open / 取数失败 / 缓存。"""
from datetime import date, datetime, timedelta

import polars as pl

from core.services import industry_data as ind


def _frame(n: int, start: date = date(2023, 1, 2)) -> pl.DataFrame:
    days = []
    d = start
    while len(days) < n:
        if d.weekday() < 5:
            days.append(datetime(d.year, d.month, d.day))
        d += timedelta(days=1)
    close = [10.0 + i * 0.02 for i in range(n)]
    return pl.DataFrame({
        "datetime": days,
        "Open": close,
        "High": [c + 0.1 for c in close],
        "Low": [c - 0.1 for c in close],
        "Close": close,
        "Volume": [1000.0] * n,
    })


def test_shift_trading_days_skips_weekends():
    # 2026-11-23(周一) 回退 1 个交易日 → 2026-11-20(周五)
    assert ind.shift_trading_days(date(2026, 11, 23), -1) == date(2026, 11, 20)
    # 300 个交易日的回退跨度 > 300 自然日（含周末）
    back = ind.shift_trading_days(date(2026, 11, 20), -ind.INDUSTRY_WARMUP_TRADING_DAYS)
    assert (date(2026, 11, 20) - back).days > 300


def test_extract_states_requires_min_bars():
    from core.engine.rating_core import compute_rating_frame

    rich = compute_rating_frame(_frame(320), min_bars=0)
    states = ind._extract_states(rich)
    assert len(states) == len(rich)  # 320 >= MIN_BARS → 逐日都有状态

    short = compute_rating_frame(_frame(120), min_bars=0)
    assert ind._extract_states(short) == {}  # 不足 300 根 → 不输出任何状态


def test_build_marks_short_and_missing_industries_unavailable(monkeypatch):
    monkeypatch.setattr(ind, "list_industry_codes", lambda: [
        {"code": "881002.SH", "name": "煤炭开采"},
        {"code": "881005.SH", "name": "焦炭加工"},
        {"code": "881007.SH", "name": "油气开采"},
    ])
    frames = {
        "881002.SH": _frame(340),
        "881005.SH": _frame(120),   # 不足 300 根
    }
    monkeypatch.setattr(ind, "fetch_industry_ohlcv",
                        lambda codes, start, end: frames)
    monkeypatch.setattr(ind, "fetch_membership",
                        lambda codes: {"600019.SH": "881002.SH"})

    tracker = ind._build(date(2024, 6, 3), date(2025, 6, 3), True)
    assert tracker.industry_count() == 1
    unavailable = tracker.unavailable_industries()
    assert "881005.SH" in unavailable and "300" in unavailable["881005.SH"]
    assert "881007.SH" in unavailable
    assert tracker.missing_for_stock("600019.SH", date(2025, 6, 2)) is None


def test_build_fetch_failure_is_fail_open(monkeypatch):
    monkeypatch.setattr(ind, "list_industry_codes",
                        lambda: [{"code": "881002.SH", "name": "煤炭开采"}])

    def _boom(codes, start, end):
        raise RuntimeError("tdx down")

    monkeypatch.setattr(ind, "fetch_industry_ohlcv", _boom)
    tracker = ind._build(date(2024, 6, 3), date(2025, 6, 3), True)
    assert tracker.available is False
    assert "tdx down" in tracker.error
    assert tracker.state_for_stock("600019.SH", date(2025, 6, 2)) is None


def test_empty_universe_is_fail_open(monkeypatch):
    monkeypatch.setattr(ind, "list_industry_codes", lambda: [])
    tracker = ind._build(date(2024, 6, 3), date(2025, 6, 3), True)
    assert tracker.available is False
    assert "行业名单为空" in tracker.error


def test_build_industry_tracker_caches(monkeypatch):
    ind.clear_cache()
    calls = {"n": 0}

    def _fake_build(start, end, include_membership):
        calls["n"] += 1
        from core.engine.industry_gate import IndustryTracker
        return IndustryTracker({}, {})

    monkeypatch.setattr(ind, "_build", _fake_build)
    a = ind.build_industry_tracker(date(2024, 6, 3), date(2025, 6, 3))
    b = ind.build_industry_tracker(date(2024, 6, 3), date(2025, 6, 3))
    assert a is b and calls["n"] == 1
    ind.build_industry_tracker(date(2024, 6, 3), date(2025, 6, 3), refresh=True)
    assert calls["n"] == 2
    ind.clear_cache()
