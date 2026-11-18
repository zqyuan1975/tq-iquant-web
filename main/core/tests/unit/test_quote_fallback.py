"""J5：行情取数降级链单测。"""
import pytest

from core.engine.http_bridge_dispatcher import BridgeUnavailableError
from core.engine.live.fallback_quote import FallbackQuoteDispatcher


class _Disp:
    def __init__(self, bars=None, error=None, batch=None, **attrs):
        self.bars = bars
        self.error = error
        self.batch = batch
        self.calls = []
        for key, value in attrs.items():
            setattr(self, key, value)

    def query_quote(self, code, period="1m", count=10):
        self.calls.append((code, period, count))
        if self.error is not None:
            raise self.error
        return list(self.bars or [])

    def query_quotes_batch(self, codes, period, count):
        if self.error is not None:
            raise self.error
        return dict(self.batch or {})


def _bar(close=10.0):
    return {"stime": "20261118100000", "open": 9.9, "high": 10.1,
            "low": 9.8, "close": close, "volume": 100}


def test_primary_success_tagged_with_source():
    primary = _Disp(bars=[_bar()])
    fallback = _Disp(bars=[_bar(11.0)])
    disp = FallbackQuoteDispatcher([("primary", primary), ("goldminer", fallback)])
    bars = disp.query_quote("600000.SH")
    assert bars[0]["close"] == pytest.approx(10.0)
    assert bars[0]["source"] == "primary"
    assert fallback.calls == []
    assert disp.last_source == "primary"


def test_fallback_on_bridge_error():
    primary = _Disp(error=BridgeUnavailableError("bridge down"))
    fallback = _Disp(bars=[_bar(11.0)])
    disp = FallbackQuoteDispatcher([("primary", primary), ("goldminer", fallback)])
    bars = disp.query_quote("600000.SH")
    assert bars[0]["source"] == "goldminer"
    assert disp.last_source == "goldminer"
    assert primary.calls and fallback.calls


def test_fallback_on_empty_result():
    primary = _Disp(bars=[])
    fallback = _Disp(bars=[_bar()])
    disp = FallbackQuoteDispatcher([("primary", primary), ("tq", fallback)])
    assert disp.query_quote("600000.SH")[0]["source"] == "tq"


def test_all_fail_prefers_bridge_unavailable():
    primary = _Disp(error=RuntimeError("boom"))
    fallback = _Disp(error=BridgeUnavailableError("down"))
    disp = FallbackQuoteDispatcher([("primary", primary), ("tq", fallback)])
    with pytest.raises(BridgeUnavailableError):
        disp.query_quote("600000.SH")


def test_all_empty_returns_empty_without_raising():
    disp = FallbackQuoteDispatcher([
        ("primary", _Disp(bars=[])), ("tq", _Disp(bars=[]))])
    assert disp.query_quote("600000.SH") == []


def test_batch_fills_missing_from_next_provider():
    primary = _Disp(batch={"600000.SH": [_bar()]}, supports_batch_quote=True)
    fallback = _Disp(bars=[_bar(11.0)])
    disp = FallbackQuoteDispatcher([("primary", primary), ("tq", fallback)])
    out = disp.query_quotes_batch(["600000.SH", "000001.SZ"], "1m", 10)
    assert out["600000.SH"][0]["source"] == "primary"
    assert out["000001.SZ"][0]["source"] == "tq"
    assert disp.last_source == "tq"


def test_capability_attributes():
    primary = _Disp(bars=[_bar()], supports_batch_quote=True,
                    supports_historical_quote=True,
                    bars_explicitly_completed=True)
    plain = _Disp(bars=[_bar()])
    disp = FallbackQuoteDispatcher([("primary", primary), ("tq", plain)])
    assert disp.supports_batch_quote is True
    # 历史补派/闭合语义以主源为准或取 AND（保守）
    assert disp.supports_historical_quote is True
    assert disp.bars_explicitly_completed is False
    assert disp.sources == ["primary", "tq"]


def test_single_provider_requires_at_least_one():
    with pytest.raises(ValueError):
        FallbackQuoteDispatcher([])


def test_build_quote_fallback_disabled_returns_primary(monkeypatch):
    from core.services import live_service

    monkeypatch.setattr(
        live_service, "load_config",
        lambda: {"live": {"quote_fallback": {"enabled": False}}})
    primary = object()
    assert live_service.build_quote_fallback(primary, "iquant") is primary


def test_build_quote_fallback_no_usable_secondary(monkeypatch):
    from core.services import live_service

    monkeypatch.setattr(
        live_service, "load_config",
        lambda: {"live": {"quote_fallback": {
            "enabled": True, "chain": ["iquant", "unknown"]}}})
    primary = object()
    # iquant 是主源自身（去重），unknown 不可构造 → 保持单源
    assert live_service.build_quote_fallback(primary, "iquant") is primary
