"""TQ 盘中 1m 探测器的纯逻辑单测（不触 TQ、不碰实盘）。"""
import logging
from datetime import datetime

from core.engine.live.tq_minute_probe import TqMinuteProbe

T0 = datetime(2026, 11, 20, 9, 31)
T1 = datetime(2026, 11, 20, 9, 32)


def _make(ts_box, codes=("600000.SH",), mono=None, interval=300.0, max_codes=5):
    mono = mono if mono is not None else [0.0]

    def fetch(want):
        return {c: ts_box["ts"] for c in want}

    return TqMinuteProbe(
        7, lambda: list(codes), fetch=fetch, interval_seconds=interval,
        max_codes=max_codes, clock=lambda: T1, monotonic=lambda: mono[0],
    ), mono


def test_baseline_then_advance(caplog):
    box = {"ts": T0}
    probe, mono = _make(box)
    with caplog.at_level(logging.INFO, logger="core.engine.live.tq_minute_probe"):
        ch = probe.tick()
        assert ch == {"600000.SH": (None, T0)}
        assert "基线" in caplog.text and "未切换" in caplog.text

        caplog.clear()
        mono[0] = 301.0
        box["ts"] = T1
        ch = probe.tick()
        assert ch == {"600000.SH": (T0, T1)}
        assert "有新数据" in caplog.text and "仅留痕" in caplog.text


def test_unchanged_and_throttle(caplog):
    box = {"ts": T0}
    probe, mono = _make(box)
    probe.tick()  # 基线
    caplog.clear()
    # 未到间隔：直接返回空（不取数、不打日志）
    assert probe.tick() == {}
    # 到间隔但时间未前进：无变化、无 INFO 刷屏
    mono[0] = 301.0
    with caplog.at_level(logging.INFO, logger="core.engine.live.tq_minute_probe"):
        assert probe.tick() == {}
        assert "有新数据" not in caplog.text


def test_fetch_error_warns_once(caplog):
    calls = {"n": 0}

    def boom(_codes):
        calls["n"] += 1
        raise RuntimeError("TQ offline")

    probe = TqMinuteProbe(7, lambda: ["600000.SH"], fetch=boom,
                          clock=lambda: T1, monotonic=lambda: 0.0)
    with caplog.at_level(logging.WARNING, logger="core.engine.live.tq_minute_probe"):
        assert probe.tick(force=True) == {}
        assert "探测失败" in caplog.text
        caplog.clear()
        assert probe.tick(force=True) == {}
        assert "探测失败" not in caplog.text  # 同类错误静默
    assert calls["n"] == 2


def test_no_data_logs_once(caplog):
    probe = TqMinuteProbe(7, lambda: ["600000.SH"], fetch=lambda _c: {},
                          clock=lambda: T1, monotonic=lambda: 0.0)
    with caplog.at_level(logging.INFO, logger="core.engine.live.tq_minute_probe"):
        probe.tick(force=True)
        assert "无可用数据" in caplog.text
        caplog.clear()
        probe.tick(force=True)
        assert "无可用数据" not in caplog.text


def test_max_codes_sorted_slice():
    seen = {}

    def fetch(codes):
        seen["codes"] = list(codes)
        return {}

    probe = TqMinuteProbe(
        7, lambda: ["300750.SZ", "600000.SH", "000001.SZ", "920718.BJ", "688318.SH", "601318.SH"],
        fetch=fetch, max_codes=5, clock=lambda: T1, monotonic=lambda: 0.0)
    probe.tick(force=True)
    expected = sorted(["300750.SZ", "600000.SH", "000001.SZ", "920718.BJ",
                       "688318.SH", "601318.SH"])[:5]
    assert seen["codes"] == expected


def test_empty_codes_no_call():
    called = {"n": 0}

    def fetch(codes):
        called["n"] += 1
        return {}

    probe = TqMinuteProbe(7, lambda: [], fetch=fetch, monotonic=lambda: 0.0,
                          clock=lambda: T1)
    assert probe.tick(force=True) == {}
    assert called["n"] == 0
