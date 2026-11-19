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
    """ETF/未知：resolve 给不出可靠幅度时回落板块常规幅度（保持回测旧口径）。"""
    for code in ("510300.SH", "abcdef"):
        rule = limit_rule_for(code)
        assert rule.ratio == Decimal("0.10"), code
        assert rule.source.endswith("_board_fallback"), code
        assert rule.unlimited is False


def test_limit_rule_for_convertible_bond_is_twenty_percent():
    """可转债（2026-11-16 B2）：11/12 开头按 ±20%，不再回落 10%。

    依据：沪深转债自 2022-08 起统一 ±20%；旧口径 10% 会让「涨停买入/跌停卖出」
    被判未越板而成交。用途覆盖回测引擎与桥接 fallback（桥无 price_limit 时）。
    已知限制：转债上市首日 +57.3%/-43.3% 特例未建模（按 20% 偏窄，fail-safe）。
    """
    for code in ("113537.SH", "110059.SH", "123456.SZ", "127789.SZ"):
        rule = limit_rule_for(code)
        assert rule.ratio == Decimal("0.20"), code
        assert rule.unlimited is False
        assert rule.source.endswith("_bond_default_20pct"), code


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


# ---------------- 修复 5：交易日天数差（冷静期按交易日推进） ----------------

def test_trading_days_between_skips_weekends_and_holidays():
    from datetime import date
    from core.engine.market_rules import trading_days_between
    # 周五 → 下周一 = 1 个交易日（自然日 3 天）
    assert trading_days_between(date(2026, 11, 13), date(2026, 11, 16)) == 1
    # 同日 = 0（当日卖出当日，冷静期 N=1 时拦当日）
    assert trading_days_between(date(2026, 11, 13), date(2026, 11, 13)) == 0
    # 反向/缺失 = 0
    assert trading_days_between(date(2026, 11, 16), date(2026, 11, 13)) == 0
    assert trading_days_between(None, date(2026, 11, 13)) == 0
    # 中秋（2026-09-25 休市）：9/24 → 9/28 = 1（周五 9/25 休市 + 周末）
    assert trading_days_between(date(2026, 9, 24), date(2026, 9, 28)) == 1
    # 国庆（10/1-10/7 休市）：9/30 → 10/8 = 1
    assert trading_days_between(date(2026, 9, 30), date(2026, 10, 8)) == 1


# ---------------- 修复 7：名称缺失 → 幅度按默认推断的可观测性 ----------------

def test_limit_rule_for_name_missing_warns_once_per_code(caplog):
    import logging
    import core.engine.market_rules as mr
    code = "600519.SH"
    mr._name_unknown_warned.discard(code)      # 去重集合是进程级，测试内重置
    with caplog.at_level(logging.WARNING, logger="core.engine.market_rules"):
        r1 = limit_rule_for(code)              # 名称缺失但判定不变
        r2 = limit_rule_for(code)
    warns = [rec for rec in caplog.records if "证券名称缺失" in rec.getMessage()]
    assert len(warns) == 1, [r.getMessage() for r in warns]
    assert "600519.SH" in warns[0].getMessage()
    # 判定结果不变（幅度仍按板块默认，只补可观测性）
    assert r1.ratio == Decimal("0.10") and r2.ratio == r1.ratio

