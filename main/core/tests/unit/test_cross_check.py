"""F4：收盘后抽样交叉校验单测（纯函数 + 引擎默认关闭/超阈值路径）。"""
from datetime import date
from types import SimpleNamespace

import pytest

from core.engine.live.cross_check import (
    compare_closes, format_report, pct_deviation, pick_sample, positive_price,
)

_TODAY = date(2026, 11, 18)


def test_pick_sample_deterministic_and_capped():
    codes = ["600%03d.SH" % i for i in range(12)]
    assert pick_sample(codes, 3) == ["600000.SH", "600004.SH", "600008.SH"]
    assert pick_sample(codes, 20) == sorted(codes)
    assert pick_sample([], 5) == []
    assert pick_sample(codes, 0) == []


def test_pct_deviation():
    assert pct_deviation(10.0, 10.0) == pytest.approx(0.0)
    assert pct_deviation(10.5, 10.0) == pytest.approx(5.0)
    assert pct_deviation(10.0, 0.0) == float("inf")
    assert pct_deviation(None, 10.0) == float("inf")


def test_compare_closes_within_threshold():
    report = compare_closes(
        {"600000.SH": 10.0}, {"600000.SH": 10.05}, 1.0)
    assert report["checked"] == 1
    assert report["deviations"] == []
    assert report["max_seen_pct"] == pytest.approx(0.4975)


def test_compare_closes_reports_deviation_and_missing():
    report = compare_closes(
        {"600000.SH": 10.0, "000001.SZ": 12.0},
        {"600000.SH": 10.5},
        1.0,
    )
    assert report["checked"] == 1
    assert report["missing"] == ["000001.SZ"]
    assert len(report["deviations"]) == 1
    dev = report["deviations"][0]
    assert dev["code"] == "600000.SH"
    assert dev["dev_pct"] == pytest.approx(4.7619)
    text = format_report(report)
    assert "600000.SH" in text and "4.762%" in text


def test_compare_closes_non_positive_price_is_deviation():
    report = compare_closes({"600000.SH": 10.0}, {"600000.SH": 0.0}, 1.0)
    assert report["deviations"][0]["dev_pct"] is None


def test_positive_price():
    assert positive_price("10.5") == pytest.approx(10.5)
    assert positive_price(0) is None
    assert positive_price(None) is None
    assert positive_price(float("nan")) is None


def test_engine_cross_check_disabled_noop(monkeypatch):
    from core.engine import live_engine

    monkeypatch.setattr(
        live_engine, "load_config",
        lambda: {"live": {"cross_check": {"enabled": False}}})
    engine = object.__new__(live_engine.LiveEngine)
    engine._cross_check_done_date = None
    calls = []
    engine._tq_daily_closes = lambda *a: calls.append("tq") or {}
    live_engine.LiveEngine._report_daily_cross_check(engine, _TODAY)
    assert calls == []
    assert engine._cross_check_done_date is None


def test_engine_cross_check_emits_event(monkeypatch):
    from core.engine import live_engine

    monkeypatch.setattr(
        live_engine, "load_config",
        lambda: {"live": {"cross_check": {
            "enabled": True, "sample_size": 5, "max_dev_pct": 1.0}}})
    monkeypatch.setattr(live_engine, "notify", lambda *a, **k: None)
    engine = object.__new__(live_engine.LiveEngine)
    engine._cross_check_done_date = None
    engine._bar_poller = SimpleNamespace(stock_codes=["600000.SH"])
    engine.ctx = SimpleNamespace(session_id=7)
    engine._engine_key = "tdx"
    engine._tq_daily_closes = lambda codes, today: {"600000.SH": 10.0}
    engine._live_daily_closes = lambda codes, today: (
        {"600000.SH": 10.5}, "snapshot_1m_close")
    emitted = []
    engine._emit_risk_safe = lambda payload, persist=False: emitted.append(
        (payload, persist))

    live_engine.LiveEngine._report_daily_cross_check(engine, _TODAY)

    assert engine._cross_check_done_date == _TODAY
    assert len(emitted) == 1
    payload, persist = emitted[0]
    assert persist is True
    assert payload["gate"] == "data_cross_check"
    assert payload["deviation_count"] == 1
    assert payload["secondary"] == "snapshot_1m_close"


def test_engine_cross_check_once_per_day(monkeypatch):
    from core.engine import live_engine

    monkeypatch.setattr(
        live_engine, "load_config",
        lambda: {"live": {"cross_check": {"enabled": True}}})
    engine = object.__new__(live_engine.LiveEngine)
    engine._cross_check_done_date = _TODAY
    calls = []
    engine._tq_daily_closes = lambda *a: calls.append(1) or {}
    live_engine.LiveEngine._report_daily_cross_check(engine, _TODAY)
    assert calls == []
