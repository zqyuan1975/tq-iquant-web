"""MarketDataService 周期聚合兜底测试（P1-6）。

TQ 本地分钟线盘中只到上一交易日 → 新鲜度闸会把 5m/15m/30m/1h 策略整日拦死。
这里验证：当日已合成 1m bar 可聚合出本周期 bar 兜底派发；无合成器时仍按旧语义
返回 stale（行为不变）。
"""
from datetime import datetime, timedelta
from types import SimpleNamespace

from core.engine.live.market_data import MarketDataService
from core.engine.live.snapshot_bars import (
    SnapshotBarSynthesizer,
    SnapshotQuoteDispatcher,
)

STOCK = "000001.SZ"
TODAY = datetime(2026, 11, 11, 9, 35)


def _bar(stime, o, h, l, c, v=100):
    return {"stime": stime, "open": o, "high": h, "low": l, "close": c,
            "volume": v}


class _StaleDispatcher:
    """原生分钟线只到上一交易日（复现实盘 TQ 本地分钟线口径）。"""

    def __init__(self, bars=None):
        self._bars = bars or {}
        self.calls = []

    def query_quote(self, code, period="1m", count=10):
        self.calls.append((code, period, count))
        return list(self._bars.get((code, period), []))


class _Poller:
    period = "1m"

    def __init__(self, codes, dispatcher):
        self.stock_codes = list(codes)
        self._dispatcher = dispatcher


class _Strategy:
    def __init__(self, sid=1, period="5m"):
        self.strategy_id = sid
        self.period = period
        self.stock_pool = None


def _service(dispatcher, poller_dispatcher, synth=None, on_bar=None,
             emit_risk=None):
    ctx = SimpleNamespace(
        portfolios=[SimpleNamespace(strategies=[_Strategy()])],
        dispatcher=dispatcher,
        code_period_count={},
    )
    poller = _Poller([STOCK], poller_dispatcher)
    return MarketDataService(
        ctx, poller,
        on_bar=on_bar or (lambda *a, **k: None),
        set_available_map=lambda m: None,
        emit_risk=emit_risk,
        snapshot_synth=synth,
    )


def _synth_with_today_5m():
    """合成器里预置 09:31–09:35 五根 1m → 可聚合出 09:35 的 5m bar。"""
    s = SnapshotBarSynthesizer(
        [STOCK], fetch_snapshot=lambda codes: {},
        in_session=lambda dt: True)
    bars = [{
        "label": datetime(2026, 11, 11, 9, 31) + timedelta(minutes=i),
        "open": 10.0 + i * 0.1,
        "high": 10.2 + i * 0.1,
        "low": 9.9 + i * 0.1,
        "close": 10.1 + i * 0.1,
        "volume": 100 + i,
    } for i in range(5)]
    with s._lock:
        s._completed[STOCK] = bars
    return s


def _stale_native_5m():
    return {
        (STOCK, "5m"): [
            _bar("20261110145500", 9.0, 9.2, 8.9, 9.1),
            _bar("20261110150000", 9.1, 9.3, 9.0, 9.2),
        ],
    }


def test_stale_native_dispatches_aggregated_5m_with_source_marker():
    events = []
    risks = []
    svc = _service(
        _StaleDispatcher(_stale_native_5m()),
        _StaleDispatcher(_stale_native_5m()),
        synth=_synth_with_today_5m(),
        on_bar=lambda *a, **k: events.append((a, k)),
        emit_risk=risks.append,
    )

    status = svc.dispatch_period_bar("5m", TODAY)

    assert status == "dispatched"
    assert len(events) == 1
    args, kwargs = events[0]
    bar_event = args[1]
    assert bar_event.period == "5m"
    assert bar_event.bar_time == TODAY
    # OHLCV 来自聚合：open=首根、close=末根、high/low=极值、volume=累加后统一为股
    assert float(bar_event.stocks[STOCK]["open"]) == 10.0
    assert float(bar_event.stocks[STOCK]["close"]) == 10.5
    assert float(bar_event.stocks[STOCK]["high"]) == 10.6
    assert float(bar_event.stocks[STOCK]["low"]) == 9.9
    assert bar_event.stocks[STOCK]["volume"] == 51000
    # 注入给公式的 bars：当日聚合 bar 在末位并带来源标记，历史原生 bar 仍在
    injected = kwargs["bars_by_code"][STOCK]
    assert injected[-1]["source"] == "aggregated_1m"
    assert injected[-1]["stime"] == "20261111093500"
    assert any(b["stime"].startswith("20261110") for b in injected)
    # 可观测事件：标明来源是聚合而非 TQ 原生
    fallback = [r for r in risks if r.get("gate") == "period_bar_fallback"]
    assert len(fallback) == 1
    assert fallback[0]["source"] == "aggregated_1m"
    assert fallback[0]["period"] == "5m"
    assert not any(r.get("gate") == "period_bar_freshness" for r in risks)


def test_without_synthesizer_keeps_stale_behavior():
    events = []
    risks = []
    svc = _service(
        _StaleDispatcher(_stale_native_5m()),
        _StaleDispatcher(_stale_native_5m()),
        on_bar=lambda *a, **k: events.append((a, k)),
        emit_risk=risks.append,
    )

    assert svc.dispatch_period_bar("5m", TODAY) == "stale"
    assert events == []
    assert any(r.get("gate") == "period_bar_freshness" for r in risks)
    assert not any(r.get("gate") == "period_bar_fallback" for r in risks)


def test_synthesizer_auto_discovered_from_poller_dispatcher():
    """无需改 live_engine：从 bar_poller._dispatcher.synthesizer 自动发现。"""
    events = []
    synth = _synth_with_today_5m()
    svc = _service(
        _StaleDispatcher(_stale_native_5m()),
        SnapshotQuoteDispatcher(synth),
        on_bar=lambda *a, **k: events.append((a, k)),
    )

    assert svc.dispatch_period_bar("5m", TODAY) == "dispatched"
    assert len(events) == 1


def test_incomplete_aggregated_bucket_stays_stale():
    """合成器只有 4 根 1m（桶不完整）→ 不派发，按原 stale 语义告警。"""
    synth = _synth_with_today_5m()
    with synth._lock:
        synth._completed[STOCK] = synth._completed[STOCK][:-1]
    risks = []
    svc = _service(
        _StaleDispatcher(_stale_native_5m()),
        _StaleDispatcher(_stale_native_5m()),
        synth=synth,
        emit_risk=risks.append,
    )

    assert svc.dispatch_period_bar("5m", TODAY) == "stale"
    assert any(r.get("gate") == "period_bar_freshness" for r in risks)


def test_empty_status_means_no_data_and_is_distinct_from_no_strategy():
    """P1#6（2026-11-16）：empty 只表示「有该周期策略但取不到 bar」（可补偿重试）。

    旧的 empty 同时表示「实例无该周期策略」——引擎据此无法区分，只对 stale 挂
    待补偿，TQ 偶发空返回时该 5m/15m/30m/1h 边界当日永久丢失。现在无策略返回
    no_strategy（不补偿），取不到数据返回 empty（与 stale 同等补偿）。
    """
    # 有 5m 策略，但任何 code 都拿不到 bar → empty（可补偿）
    svc = _service(_StaleDispatcher({}), _StaleDispatcher({}))
    assert svc.dispatch_period_bar("5m", TODAY) == "empty"

    # 实例只有 1m 策略 → 5m 边界直接 no_strategy（无事可做，不补偿）
    ctx = SimpleNamespace(
        portfolios=[SimpleNamespace(strategies=[_Strategy(period="1m")])],
        dispatcher=_StaleDispatcher({}),
        code_period_count={},
    )
    svc_m1 = MarketDataService(
        ctx, _Poller([STOCK], _StaleDispatcher({})),
        on_bar=lambda *a, **k: None,
        set_available_map=lambda m: None,
    )
    assert svc_m1.dispatch_period_bar("5m", TODAY) == "no_strategy"

