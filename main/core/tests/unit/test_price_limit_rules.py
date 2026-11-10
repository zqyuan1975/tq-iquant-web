"""core.engine.price_limit 涨跌停推算规则单测（板块 + ST + 新股上市初期）。

判据表（2026-11 现行 A 股规则，明细见模块 docstring）：
- 主板 10%，ST/*ST 5%
- 创业板/科创板 20%（ST 不降档，仍 20%）
- 北交所 30%（ST 同样 30%）
- 新股：沪深（主板注册制/创业板/科创板）前 5 个交易日、北交所上市首日无涨跌幅限制
- ETF/转债/未知代码：仅凭代码不可定档 → ratio=None（fail-open，交柜台）
"""
from datetime import date, timedelta
from decimal import Decimal

from core.engine.price_limit import (
    BOARD_BOND,
    BOARD_BSE,
    BOARD_CHINEXT,
    BOARD_FUND,
    BOARD_MAIN,
    BOARD_STAR,
    BOARD_UNKNOWN,
    board_for,
    extract_name,
    is_st_name,
    resolve_limit_rule,
)

TODAY = date(2026, 11, 10)


def _rule(code, name=None, listed_bars=None, earliest=None, today=TODAY):
    return resolve_limit_rule(
        code, name=name, listed_bars=listed_bars,
        earliest_bar_date=earliest, today=today)


# ---------------- 板块判定 ----------------

def test_board_for_prefixes():
    assert board_for("600000.SH") == BOARD_MAIN
    assert board_for("601318.SH") == BOARD_MAIN
    assert board_for("603259.SH") == BOARD_MAIN
    assert board_for("605499.SH") == BOARD_MAIN
    assert board_for("000001.SZ") == BOARD_MAIN
    assert board_for("001979.SZ") == BOARD_MAIN
    assert board_for("002594.SZ") == BOARD_MAIN
    assert board_for("003816.SZ") == BOARD_MAIN
    assert board_for("300750.SZ") == BOARD_CHINEXT
    assert board_for("301236.SZ") == BOARD_CHINEXT
    assert board_for("688981.SH") == BOARD_STAR
    assert board_for("689009.SH") == BOARD_STAR
    assert board_for("430047.BJ") == BOARD_BSE
    assert board_for("830799.BJ") == BOARD_BSE
    assert board_for("920718.BJ") == BOARD_BSE
    assert board_for("510300.SH") == BOARD_FUND
    assert board_for("159915.SZ") == BOARD_FUND
    assert board_for("110059.SH") == BOARD_BOND
    assert board_for("123456.SZ") == BOARD_BOND
    assert board_for("") == BOARD_UNKNOWN
    assert board_for("abc") == BOARD_UNKNOWN


def test_board_for_without_suffix():
    assert board_for("600000") == BOARD_MAIN
    assert board_for("300750") == BOARD_CHINEXT


# ---------------- ST 名称标记 ----------------

def test_is_st_name():
    assert is_st_name("ST浦发")
    assert is_st_name("*ST海航")
    assert is_st_name("SST前锋")
    assert is_st_name("st 康美")
    assert not is_st_name("浦发银行")
    assert not is_st_name(None)
    assert not is_st_name("")


# ---------------- 常规幅度 ----------------

def test_regular_ratios_per_board():
    cases = [
        ("600000.SH", "浦发银行", Decimal("0.10")),
        ("000001.SZ", "平安银行", Decimal("0.10")),
        ("300750.SZ", "宁德时代", Decimal("0.20")),
        ("688981.SH", "中芯国际", Decimal("0.20")),
        ("830799.BJ", "艾融软件", Decimal("0.30")),
    ]
    for code, name, ratio in cases:
        rule = _rule(code, name=name)
        assert rule.ratio == ratio
        assert rule.unlimited is False
        assert rule.source == "board_default"
        assert rule.confidence == "high"


# ---------------- ST 幅度按板块分档 ----------------

def test_st_ratios_per_board():
    """主板 ST 5%；创业板/科创板 ST 仍 20%；北交所 ST 30%。"""
    cases = [
        ("600000.SH", "ST浦发", Decimal("0.05")),
        ("000001.SZ", "*ST平安", Decimal("0.05")),
        ("300750.SZ", "ST宁德", Decimal("0.20")),
        ("688981.SH", "*ST中芯", Decimal("0.20")),
        ("830799.BJ", "ST艾融", Decimal("0.30")),
    ]
    for code, name, ratio in cases:
        rule = _rule(code, name=name)
        assert rule.ratio == ratio
        assert rule.source == "name_st"
        assert rule.confidence == "high"


def test_name_unknown_falls_back_to_board_default():
    """名称取不到 → 板块默认幅度 + confidence=medium（主板 ST 会偏宽，柜台兜底）。"""
    rule = _rule("600000.SH", name=None)
    assert rule.ratio == Decimal("0.10")
    assert rule.source == "board_default_name_unknown"
    assert rule.confidence == "medium"


# ---------------- 新股上市初期无涨跌幅限制 ----------------

def test_new_listing_window_five_sessions_for_hs_boards():
    """沪深三板块：前 5 个交易日无限制（已交易 4 天 → 仍在窗口内，第 5 天结束）。"""
    earliest = TODAY - timedelta(days=4)
    for code in ("600001.SH", "300999.SZ", "688999.SH"):
        rule = _rule(code, name="新股", listed_bars=4, earliest=earliest)
        assert rule.ratio is None
        assert rule.unlimited is True
        assert rule.source == "new_listing_5_sessions"


def test_new_listing_window_ends_after_five_sessions():
    """已交易 5 天 → 第 6 个交易日起恢复常规幅度。"""
    rule = _rule("600001.SH", name="新股", listed_bars=5,
                 earliest=TODAY - timedelta(days=5))
    assert rule.ratio == Decimal("0.10")
    assert rule.source == "board_default"


def test_bse_new_listing_only_first_day():
    """北交所仅上市首日无限制（已交易 0 天 → 无限制；1 天 → 30%）。"""
    first_day = _rule("920718.BJ", name="新股", listed_bars=0,
                      earliest=TODAY - timedelta(days=2))
    assert first_day.ratio is None
    assert first_day.source == "new_listing_1_sessions"
    day2 = _rule("920718.BJ", name="新股", listed_bars=1,
                 earliest=TODAY - timedelta(days=3))
    assert day2.ratio == Decimal("0.30")


def test_new_listing_requires_recent_earliest_bar():
    """日线根数少但最早一根在 45 天前 → 视为本地数据残缺，不判新股。"""
    rule = _rule("600001.SH", name="老股", listed_bars=2,
                 earliest=TODAY - timedelta(days=100))
    assert rule.ratio == Decimal("0.10")
    assert rule.source == "board_default"


def test_new_listing_without_dates_is_not_assumed():
    """缺最早 bar 日期 / 今日 → 不能认定新股，退回板块默认（不瞎猜）。"""
    assert _rule("600001.SH", name="新股", listed_bars=1).ratio == Decimal("0.10")
    assert resolve_limit_rule(
        "600001.SH", name="新股", listed_bars=1,
        earliest_bar_date=TODAY - timedelta(days=1), today=None,
    ).ratio == Decimal("0.10")


def test_main_board_new_listing_confidence_low():
    """主板新股历史上首日 44% 限价 → 置信度降为 low（创业板/科创板 medium）。"""
    earliest = TODAY - timedelta(days=1)
    assert _rule("600001.SH", name="新股", listed_bars=1,
                 earliest=earliest).confidence == "low"
    assert _rule("300999.SZ", name="新股", listed_bars=1,
                 earliest=earliest).confidence == "medium"


# ---------------- 无法定档 → fail-open ----------------

def test_unresolved_boards_return_none_ratio():
    """ETF / 可转债 / 未知代码：仅凭代码不可定档 → ratio=None（放行交柜台）。"""
    for code, board in (("510300.SH", BOARD_FUND),
                        ("159915.SZ", BOARD_FUND),
                        ("113537.SH", BOARD_BOND),
                        ("123456.SZ", BOARD_BOND),
                        ("abc", BOARD_UNKNOWN)):
        rule = _rule(code, name="某标的")
        assert rule.ratio is None
        assert rule.unlimited is True
        assert rule.confidence == "low"
        assert rule.board == board


def test_label_contains_source_and_confidence():
    """label 供通道日志/拒单文案留痕（回放可区分「推的」与「确定的」）。"""
    rule = _rule("600000.SH", name="ST浦发")
    assert "board=main" in rule.label
    assert "ratio=0.05" in rule.label
    assert "source=name_st" in rule.label
    assert "confidence=high" in rule.label
    unlimited = _rule("510300.SH")
    assert "ratio=-" in unlimited.label


# ---------------- extract_name（TQ 批量信息返回形态各异）----------------

def test_extract_name_variants():
    assert extract_name({"600000.SH": {"Name": "浦发银行"}}, "600000.SH") == "浦发银行"
    assert extract_name({"StockName": "浦发银行"}, "600000.SH") == "浦发银行"
    assert extract_name({"证券名称": "浦发银行"}, "600000.SH") == "浦发银行"
    assert extract_name([{"名称": "浦发银行"}], "600000.SH") == "浦发银行"
    assert extract_name({"600000.SH": [{"Name": "浦发银行"}]}, "600000.SH") == "浦发银行"


def test_extract_name_does_not_mistake_error_code_for_name():
    """{"ErrorId": "0"} 这类返回不能把 "0" 当名称。"""
    assert extract_name({"ErrorId": "0"}) is None
    assert extract_name(None) is None
    assert extract_name({}) is None
