"""错周期逐 1m 风控的会话三态解析（纯用户控制：NULL＝未设置＝关闭）。"""
from types import SimpleNamespace

from core.services.live_service import resolve_mismatched_risk


def _sess(value):
    return SimpleNamespace(intraday_mismatched_risk_enabled=value)


def test_explicit_value_wins():
    assert resolve_mismatched_risk(_sess(False)) == (False, "explicit")
    assert resolve_mismatched_risk(_sess(True)) == (True, "explicit")


def test_null_is_always_off_and_reported_as_unset():
    """NULL（未显式设置）一律关闭，不因存在分钟策略自动开启。"""
    assert resolve_mismatched_risk(_sess(None)) == (False, "unset")
    assert resolve_mismatched_risk(None) == (False, "unset")
