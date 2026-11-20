"""原生分钟数据源（``native_minute``）单元测试。

覆盖：未完成桶丢弃、判新只看标签前进（不看 mtime）、停滞回落快照合成、
标签恢复前进后切回原生（回落/恢复均留痕）。
"""
from datetime import datetime

from core.engine.bar_poller import BarPoller
from core.engine.live.native_minute import (
    MinuteSourceHealth,
    NativeMinuteDispatcher,
    closed_bars,
    latest_closed_bar,
)

STOCK = "000001.SZ"
NOW = datetime(2026, 11, 20, 10, 33, 31)


def _bar(hhmm, close=10.0):
    t = datetime(2026, 11, 20, *hhmm)
    return {
        "stime": t.strftime("%Y%m%d%H%M%S"),
        "open": close, "high": close, "low": close, "close": close,
        "volume": 100,
    }


def _stime_list(bars):
    return [b["stime"] for b in bars]


class _Fallback:
    """快照合成替身：返回一根已闭合的 10:32 bar。"""

    def __init__(self):
        self.calls = []

    def query_quotes_batch(self, codes, period="1m", count=10):
        self.calls.append((list(codes), period, count))
        return {c: [_bar((10, 32), 9.9)] for c in codes}


def _dispatcher(fetch, clock, health=None, fallback=None):
    health = health or MinuteSourceHealth(
        clock=clock, in_session=lambda dt: True)
    return NativeMinuteDispatcher(
        health=health, fallback=fallback, fetch_batch=fetch, clock=clock)


# --- 未完成桶 ---

def test_closed_bars_drops_unfinished_bucket():
    bars = [_bar((10, 32)), _bar((10, 33)), _bar((10, 34))]
    assert _stime_list(closed_bars(bars, NOW)) == [
        "20261120103200", "20261120103300"]
    assert latest_closed_bar(bars, NOW)["stime"] == "20261120103300"


def test_dispatcher_never_returns_forming_bar():
    fetched = {STOCK: [_bar((10, 32)), _bar((10, 33)), _bar((10, 34))]}
    calls = []

    def fetch(codes, period, count):
        calls.append((list(codes), period, count))
        return fetched

    d = _dispatcher(fetch, lambda: NOW)
    out = d.query_quotes_batch([STOCK], "1m", 10)
    assert _stime_list(out[STOCK]) == ["20261120103200", "20261120103300"]
    # 1m 多取 2 根，剔除未完成桶后仍能凑满 count
    assert calls[0][2] == 12


# --- 判新只看标签前进 ---

def test_bar_poller_triggers_only_on_label_advance():
    now = [NOW]
    bars = {STOCK: [_bar((10, 32)), _bar((10, 33)), _bar((10, 34))]}

    def fetch(codes, period, count):
        return bars

    d = _dispatcher(fetch, lambda: now[0])
    poller = BarPoller(d, [STOCK], period="1m", count=10)
    events = []
    poller.on_bar = events.append
    poller.poll()   # 建基线
    poller.poll()   # 标签未前进（mtime 即使变化也不应触发）
    assert events == []
    bars[STOCK] = [_bar((10, 33)), _bar((10, 34)), _bar((10, 35))]
    now[0] = datetime(2026, 11, 20, 10, 34, 31)
    poller.poll()
    assert len(events) == 1
    assert events[0].bar_time == datetime(2026, 11, 20, 10, 34)


# --- 停滞回落 / 恢复 ---

def test_stall_falls_back_then_recovers_on_label_advance():
    now = [datetime(2026, 11, 20, 10, 30, 0)]
    bars = {STOCK: [_bar((10, 30))]}

    def fetch(codes, period, count):
        return bars

    fallback = _Fallback()
    events = []
    health = MinuteSourceHealth(
        stall_timeout_seconds=60, clock=lambda: now[0],
        in_session=lambda dt: True)
    health.on_transition = events.append
    d = NativeMinuteDispatcher(
        health=health, fallback=fallback,
        fetch_batch=fetch, clock=lambda: now[0])

    out = d.query_quotes_batch([STOCK])
    assert out[STOCK][0]["stime"] == "20261120103000"
    assert health.in_fallback is False

    # 90s 未前进 → 回落快照合成，并留痕
    now[0] = datetime(2026, 11, 20, 10, 31, 30)
    out = d.query_quotes_batch([STOCK])
    assert fallback.calls[-1][1] == "1m"
    assert out[STOCK][0]["stime"] == "20261120103200"
    assert health.in_fallback is True
    assert events[-1]["rule"] == "fallback"
    assert events[-1]["lag_seconds"] >= 60

    # 标签前进 → 切回原生，并留痕
    bars[STOCK] = [_bar((10, 30)), _bar((10, 33))]
    now[0] = datetime(2026, 11, 20, 10, 33, 30)
    out = d.query_quotes_batch([STOCK])
    assert out[STOCK][-1]["stime"] == "20261120103300"
    assert health.in_fallback is False
    assert events[-1]["rule"] == "recovered"


def test_no_stall_outside_session():
    """午休/收盘不判停滞（11:30–13:00 不应把正常停写当故障）。"""
    now = [datetime(2026, 11, 20, 11, 31, 0)]
    health = MinuteSourceHealth(
        stall_timeout_seconds=60, clock=lambda: now[0],
        in_session=lambda dt: not (dt.hour == 11 and dt.minute >= 30))
    health.observe([datetime(2026, 11, 20, 11, 30)])
    now[0] = datetime(2026, 11, 20, 11, 45, 0)
    assert health.is_fallback() is False
