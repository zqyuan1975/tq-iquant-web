"""层 2：equity_cap_mode 组合 JSON 配置（data/portfolio_settings.json）。

不新增表/迁移：mode 存 JSON 文件，缺省 cap（旧行为），支持随时改/清空。
"""
from core.services import portfolio_settings as ps


def test_default_mode_is_cap(tmp_path):
    assert ps.get_equity_cap_mode(1, tmp_path / "s.json") == "cap"


def test_set_get_and_clear(tmp_path):
    p = tmp_path / "s.json"
    ps.set_equity_cap_mode(7, "distribute", p)
    assert ps.get_equity_cap_mode(7, p) == "distribute"
    # 其它组合不受影响
    assert ps.get_equity_cap_mode(8, p) == "cap"
    # 显式 cap / None 都归一回默认值（删除键）
    ps.set_equity_cap_mode(7, "cap", p)
    assert ps.get_equity_cap_mode(7, p) == "cap"
    ps.set_equity_cap_mode(7, "distribute", p)
    ps.set_equity_cap_mode(7, None, p)
    assert ps.get_equity_cap_mode(7, p) == "cap"


def test_invalid_mode_falls_back_to_cap(tmp_path):
    p = tmp_path / "s.json"
    ps.set_equity_cap_mode(1, "bogus", p)
    assert ps.get_equity_cap_mode(1, p) == "cap"


def test_corrupt_file_falls_back_to_cap(tmp_path):
    p = tmp_path / "s.json"
    p.write_text("{not json", encoding="utf-8")
    assert ps.get_equity_cap_mode(1, p) == "cap"
    # 损坏文件仍可被后续写入修复
    ps.set_equity_cap_mode(1, "distribute", p)
    assert ps.get_equity_cap_mode(1, p) == "distribute"


def test_delete_portfolio_settings(tmp_path):
    p = tmp_path / "s.json"
    ps.set_equity_cap_mode(3, "distribute", p)
    ps.delete_portfolio_settings(3, p)
    assert ps.get_equity_cap_mode(3, p) == "cap"
    # 最后一个组合删除后文件也移除，避免残留空 {} 文件
    assert not p.exists()


def test_industry_defaults_all_off(tmp_path):
    s = ps.get_industry_settings(1, tmp_path / "s.json")
    assert s["industry_gate_mode"] == "off"
    assert s["industry_rank_enabled"] is False
    assert s["industry_weight_enabled"] is False
    assert s["industry_overbought_block"] is False
    assert s["industry_trail_tighten_enabled"] is False
    assert s["industry_bear_reduce_ratio"] == 0.0
    assert ps.industry_data_needed(s) is False


def test_apply_industry_overrides_run_level(tmp_path):
    p = tmp_path / "s.json"
    ps.set_industry_settings(5, {"industry_rank_enabled": True}, p)
    base = ps.get_industry_settings(5, p)
    # 未携带的键沿用组合设置；非法模式回落 off；未知键忽略；None=不覆盖
    merged = ps.apply_industry_overrides(base, {
        "industry_gate_mode": "bogus",
        "industry_weight_enabled": True,
        "industry_max_per_industry": None,
        "unknown_key": 1,
    })
    assert merged["industry_rank_enabled"] is True
    assert merged["industry_gate_mode"] == "off"
    assert merged["industry_weight_enabled"] is True
    assert merged["industry_max_per_industry"] == 2
    assert ps.industry_data_needed(merged) is True
    # 不写盘：组合设置保持原样
    assert ps.get_industry_settings(5, p)["industry_weight_enabled"] is False


def test_industry_bool_string_coerced(tmp_path):
    p = tmp_path / "s.json"
    ps.set_industry_settings(9, {"industry_rank_enabled": "false"}, p)
    assert ps.get_industry_settings(9, p)["industry_rank_enabled"] is False
