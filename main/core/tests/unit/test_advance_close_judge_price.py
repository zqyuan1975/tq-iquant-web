"""14:51 判定价来源：原生 1m 优先 + 逐只回落快照合成 + fail-closed。

规范 2026-11-23：判定价（signal bar 14:50 的 close）取价顺序为
原生 1m（该 code 标签 14:51:00）→ 快照合成 ``completed_bars`` → 不返回价格
（调用方重试/告警，绝不伪造）。四条回落条件逐 code 判定，不整批降级：

1. ``live.native_minute.enabled`` 未开（``_market_data._native_minute`` 为 None）；
2. 原生源 stalled / fallback 健康态；
3. 取回的最后一根标签 ≠ 14:51（不得用 14:50/14:52 顶替）；
4. 价格无效（0 / 负 / 缺失）。

判定价来源（``native_1m``/``snapshot_synth``/``none``）与执行价来源分别留档，
本文件只覆盖判定价一侧；滞后秒数记录原生最后一根标签落后 14:51 的差距。
"""
from datetime import date, datetime
from types import SimpleNamespace

from core.engine.live.daily_closer import DailyCloser
from core.tests.unit.test_live_engine import _db_factory

_TODAY = date(2026, 8, 5)  # 周三
_NOW = datetime(2026, 8, 5, 14, 52)
_CODES = ["600000.SH", "000001.SZ"]


def _bar(stime: str, close: str = "9.10", open_: str = "9.00") -> dict:
    return {"stime": stime, "open": open_, "high": "9.20",
            "low": "8.90", "close": close, "volume": "1000"}


class _Health:
    def __init__(self, fallback: bool):
        self.fallback = fallback

    def is_fallback(self, now=None):
        return self.fallback


class _Native:
    """NativeMinuteDispatcher 替身：只实现本链路用到的两处。"""

    def __init__(self, bars, fallback: bool = False):
        self.bars = bars
        self.minute_health = _Health(fallback)

    def query_quotes_batch(self, codes, period="1m", count=20):
        return {c: list(self.bars.get(c) or []) for c in codes}


class _Synth:
    """SnapshotBarSynthesizer 替身：completed_bars 回放既定 bar。"""

    def __init__(self, bars):
        self.bars = bars

    def completed_bars(self, codes=None, count=30):
        return {c: list(self.bars.get(c) or []) for c in (codes or [])}


def _mk_closer(native=None, synth=None):
    factory, _ = _db_factory()
    ctx = SimpleNamespace(
        session_id=1,
        portfolios=[],
        db_session_factory=factory,
        dispatcher=None,
        code_period_count={},
        clock=lambda: _NOW,
    )
    # 原生调度器挂在 BarPoller._dispatcher 上（LiveEngine 的实际接线位置）；
    # MarketDataService._native_minute 只是健康态对象，不能用来取数。
    market_data = SimpleNamespace(
        _bar_poller=SimpleNamespace(_dispatcher=native) if native is not None else None,
    )
    return DailyCloser(
        ctx=ctx,
        market_data=market_data,
        breaker=None,
        orders=None,
        on_bar=lambda *a, **k: None,
        calendar_provider=lambda: SimpleNamespace(is_trading_day=lambda d: True),
        emit=lambda *a, **k: None,
        set_bridge_offline=lambda: None,
        snapshot_synth=synth,
    )


def test_native_1m_preferred_when_label_matches_and_price_valid():
    """原生正常（标签 14:51、价格有效）→ 取原生收盘价，来源 native_1m、滞后 0。"""
    native = _Native({
        "600000.SH": [_bar("20260805145000", "9.00"), _bar("20260805145100", "9.11")],
        "000001.SZ": [_bar("20260805145100", "8.88")],
    })
    synth = _Synth({
        "600000.SH": [_bar("20260805145100", "1.00")],
    })
    closer = _mk_closer(native=native, synth=synth)

    prices, sources, lags = closer._advance_close_prices(_CODES, _TODAY)

    assert prices == {"600000.SH": 9.11, "000001.SZ": 8.88}
    assert sources == {"600000.SH": "native_1m", "000001.SZ": "native_1m"}
    assert lags == {"600000.SH": 0.0, "000001.SZ": 0.0}


def test_native_disabled_falls_back_to_snapshot_synth():
    """``live.native_minute.enabled`` 未开（无调度器）→ 逐只回落快照合成。"""
    synth = _Synth({
        "600000.SH": [_bar("20260805145000", "9.00"), _bar("20260805145100", "9.21")],
        "000001.SZ": [_bar("20260805145100", "8.77")],
    })
    closer = _mk_closer(native=None, synth=synth)

    prices, sources, lags = closer._advance_close_prices(_CODES, _TODAY)

    assert prices == {"600000.SH": 9.21, "000001.SZ": 8.77}
    assert sources == {"600000.SH": "snapshot_synth", "000001.SZ": "snapshot_synth"}
    assert lags == {"600000.SH": None, "000001.SZ": None}


def test_native_stalled_falls_back_to_snapshot_synth():
    """原生源处于 fallback/stalled 健康态 → 不采信原生，回落合成。"""
    native = _Native({
        "600000.SH": [_bar("20260805145100", "9.11")],
        "000001.SZ": [_bar("20260805145100", "8.88")],
    }, fallback=True)
    synth = _Synth({
        "600000.SH": [_bar("20260805145100", "9.31")],
        "000001.SZ": [_bar("20260805145100", "8.71")],
    })
    closer = _mk_closer(native=native, synth=synth)

    prices, sources, lags = closer._advance_close_prices(_CODES, _TODAY)

    assert prices == {"600000.SH": 9.31, "000001.SZ": 8.71}
    assert sources == {"600000.SH": "snapshot_synth", "000001.SZ": "snapshot_synth"}


def test_native_label_mismatch_falls_back_and_records_lag():
    """最后一根标签 ≠ 14:51（未追平）→ 该 code 回落，不得用 14:50 顶替。

    另一只已追平的原生 code 不受影响（逐只判定，不整批降级）。
    """
    native = _Native({
        "600000.SH": [_bar("20260805145000", "9.05")],  # 只到 14:50
        "000001.SZ": [_bar("20260805145100", "8.88")],
    })
    synth = _Synth({
        "600000.SH": [_bar("20260805145100", "9.26")],
        "000001.SZ": [_bar("20260805145100", "1.00")],
    })
    closer = _mk_closer(native=native, synth=synth)

    prices, sources, lags = closer._advance_close_prices(_CODES, _TODAY)

    assert prices["600000.SH"] == 9.26          # 回落合成，非 14:50 的 9.05
    assert sources["600000.SH"] == "snapshot_synth"
    assert lags["600000.SH"] == 60.0            # 落后 14:51 一分钟
    assert prices["000001.SZ"] == 8.88          # 已追平 → 原生
    assert sources["000001.SZ"] == "native_1m"


def test_native_invalid_price_falls_back_to_snapshot_synth():
    """原生价格无效（0 / 负 / 缺失）→ 该 code 回落合成。"""
    native = _Native({
        "600000.SH": [_bar("20260805145100", "0")],
        "000001.SZ": [{"stime": "20260805145100", "open": "9.0"}],  # 无 close
    })
    synth = _Synth({
        "600000.SH": [_bar("20260805145100", "9.41")],
        "000001.SZ": [_bar("20260805145100", "8.61")],
    })
    closer = _mk_closer(native=native, synth=synth)

    prices, sources, _lags = closer._advance_close_prices(_CODES, _TODAY)

    assert prices == {"600000.SH": 9.41, "000001.SZ": 8.61}
    assert sources == {"600000.SH": "snapshot_synth", "000001.SZ": "snapshot_synth"}


def test_no_source_available_is_fail_closed():
    """原生与合成都取不到 → 不返回价格（调用方重试/告警），来源标记 none。"""
    native = _Native({"600000.SH": [_bar("20260805145000", "9.05")]})
    closer = _mk_closer(native=native, synth=_Synth({}))

    prices, sources, lags = closer._advance_close_prices(_CODES, _TODAY)

    assert prices == {}
    assert sources == {"600000.SH": "none", "000001.SZ": "none"}
    assert lags["600000.SH"] == 60.0       # 仍留痕滞后，便于定位未追平
    assert lags["000001.SZ"] is None
