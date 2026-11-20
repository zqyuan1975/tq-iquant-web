"""错周期逐 1m 风控的会话三态解析（NULL 时按子策略周期自动推导）。"""
from types import SimpleNamespace

from core.services.live_service import resolve_mismatched_risk


def _sess(value):
    return SimpleNamespace(intraday_mismatched_risk_enabled=value)


def test_explicit_value_wins_over_periods():
    assert resolve_mismatched_risk(_sess(False), ["1m", "5m"]) == (
        False, "explicit")
    assert resolve_mismatched_risk(_sess(True), []) == (True, "explicit")


def test_null_with_minute_strategy_enables():
    assert resolve_mismatched_risk(_sess(None), ["1d", "5m"]) == (
        True, "auto_minute")


def test_null_without_minute_strategy_disables():
    assert resolve_mismatched_risk(_sess(None), []) == (False, "auto_no_minute")
    assert resolve_mismatched_risk(None, []) == (False, "auto_no_minute")
