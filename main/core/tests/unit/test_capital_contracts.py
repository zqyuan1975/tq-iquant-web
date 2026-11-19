"""资金池合同层（0021 A1）单测：开关默认值 / 配置解析 / 版本字段。"""
from core.config import load_config
from core.engine.capital_contracts import (
    CapitalPoolSwitches,
    build_capital_pool_version,
)


def test_default_config_switches_all_off(tmp_path):
    cfg = load_config(str(tmp_path / "absent.yaml"))
    switches = CapitalPoolSwitches.from_config(cfg)
    assert switches.to_dict() == {
        "capital_pool_enabled": False,
        "pairing_enabled": False,
        "loan_model_enabled": False,
        "cash_outlet_enabled": False,
    }


def test_partial_config_keeps_missing_switches_off(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text("capital_pool:\n  enabled: true\n", encoding="utf-8")
    cfg = load_config(str(path))
    switches = CapitalPoolSwitches.from_config(cfg)
    assert switches.capital_pool_enabled is True
    assert switches.pairing_enabled is False
    assert switches.loan_model_enabled is False
    assert switches.cash_outlet_enabled is False


def test_switch_accepts_bool_and_string_true_only():
    s = CapitalPoolSwitches.from_config({"capital_pool": {
        "enabled": "true", "pairing_enabled": 1, "loan_model_enabled": None,
        "cash_outlet_enabled": "off",
    }})
    assert s.capital_pool_enabled is True
    assert s.pairing_enabled is False
    assert s.loan_model_enabled is False
    assert s.cash_outlet_enabled is False


def test_invalid_block_is_all_off():
    s = CapitalPoolSwitches.from_config({"capital_pool": "yes"})
    assert s.capital_pool_enabled is False


def test_version_payload_has_algo_and_defaults():
    v = build_capital_pool_version()
    assert v["quota_algo_version"]
    assert v["defaults"]["target_utilization"] == "0.95"
    assert v["defaults"]["max_holdings"] == 10
