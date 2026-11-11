"""market_rules（涨跌停幅度）共享模块测试。

_limit_ratio_for 原属 backtest_engine，1.3.1 迁到 core.engine.market_rules 供
实盘 TdxTqDispatcher 涨跌停前置校验共用；backtest_engine 保留 re-export，
既有 import 路径不破。
"""
from decimal import Decimal

from core.engine.backtest_engine import _limit_ratio_for as from_backtest
from core.engine.market_rules import (
    _limit_ratio_for,
    limit_ratio_for,
    limit_rule_for,
    tick_size,
)
from core.engine.price_limit import resolve_limit_rule


def test_main_board_ten_percent():
    assert _limit_ratio_for("000001.SZ") == Decimal("0.10")
    assert _limit_ratio_for("600000.SH") == Decimal("0.10")


def test_gem_and_star_twenty_percent():
    assert _limit_ratio_for("300750.SZ") == Decimal("0.20")
    assert _limit_ratio_for("301236.SZ") == Decimal("0.20")
    assert _limit_ratio_for("688981.SH") == Decimal("0.20")


def test_bse_thirty_percent():
    assert _limit_ratio_for("430047.BJ") == Decimal("0.30")
    assert _limit_ratio_for("830799.BJ") == Decimal("0.30")
    # 92xxxx 为北交所新股段，同 30%（原实现漏判，按 9 开头误落 10%）
    assert _limit_ratio_for("920718.BJ") == Decimal("0.30")
    assert _limit_ratio_for("920000") == Decimal("0.30")


def test_plain_code_without_suffix():
    assert _limit_ratio_for("600000") == Decimal("0.10")
    assert _limit_ratio_for("300750") == Decimal("0.20")


def test_backtest_engine_reexport_kept():
    """迁移后 backtest_engine 既有导入路径不破（re-export 同一函数）。"""
    assert from_backtest is _limit_ratio_for


def test_limit_rule_for_st_main_board_is_five_percent():
    """回测与实盘同源：名称含 ST 的主板股票 5%。"""
    rule = limit_rule_for("000001.SZ", name="*ST某某")
    assert rule.ratio == Decimal("0.05")
    assert rule.source == "name_st"
    assert limit_ratio_for("600000.SH", name="ST甲") == Decimal("0.05")


def test_limit_rule_for_matches_live_resolver():
    """同一只票：回测入口与实盘 resolve_limit_rule 逐值一致（有名称时）。"""
    cases = [
        ("000001.SZ", "平安银行"),
        ("000001.SZ", "ST某某"),
        ("300750.SZ", "宁德时代"),
        ("688981.SH", "中芯国际"),
        ("830799.BJ", "艾融软件"),
    ]
    for code, name in cases:
        live = resolve_limit_rule(code, name=name)
        backtest = limit_rule_for(code, name=name)
        assert backtest.ratio == live.ratio, (code, name)
        assert backtest.board == live.board, (code, name)


def test_limit_rule_for_unresolved_ratio_falls_back_to_board_ratio():
    """ETF/转债/未知：resolve 给不出可靠幅度时回落板块常规幅度（保持回测旧口径）。"""
    for code in ("510300.SH", "113537.SH", "abcdef"):
        rule = limit_rule_for(code)
        assert rule.ratio == Decimal("0.10"), code
        assert rule.source.endswith("_board_fallback"), code
        assert rule.unlimited is False


def test_tick_size_etf_is_one_milli():
    """ETF（沪 5 开头 / 深 1 开头）最小价差 0.001。"""
    assert tick_size("510300.SH") == Decimal("0.001")
    assert tick_size("588000.SH") == Decimal("0.001")
    assert tick_size("159915.SZ") == Decimal("0.001")
    assert tick_size("159888.SZ") == Decimal("0.001")


def test_tick_size_stock_is_one_cent():
    """股票（沪 6 / 深 0、3 / 北 4、8）最小价差 0.01。"""
    assert tick_size("600000.SH") == Decimal("0.01")
    assert tick_size("000001.SZ") == Decimal("0.01")
    assert tick_size("300750.SZ") == Decimal("0.01")
    assert tick_size("688981.SH") == Decimal("0.01")
    assert tick_size("830799.BJ") == Decimal("0.01")


def test_tick_size_plain_code_without_suffix():
    assert tick_size("510300") == Decimal("0.001")
    assert tick_size("600000") == Decimal("0.01")
