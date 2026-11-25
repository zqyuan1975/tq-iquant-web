# -*- coding: utf-8 -*-
"""公司行为内核 + 回测接入（J1/J2，2026-11-18）。

覆盖：
- 送转：股数/均价/最高价/T+1 桶等比调整，总成本不变；
- 分红：回测用比例口径（cash_pct × 前收 × 持股），实盘用每股口径（cash_per_share）；
- 顺延与幂等：周六除权日 → 周一生效且只生效一次；
- 停牌闸：停牌期间 OPEN/ADD 被拦（卖出一律放行）；
- 退市强平：开关打开时按最后可交易价生成 SELL；
- 未启用（timeline=None）时结果与改动前逐字一致。
"""
from datetime import date, datetime
from decimal import Decimal

import polars as pl

from core.engine.backtest_engine import BacktestEngine
from core.engine.corporate_actions import (
    CorporateActionEvent,
    CorporateActionTimeline,
    KIND_DELIST_CLOSE,
    KIND_DIVIDEND_CASH,
    KIND_SKIP_CASH,
    apply_day,
)
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from tq_iquant_shared.constants import SignalType, TradeType

STOCK = "000001.SZ"


def _portfolio(cash=Decimal("100000")):
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=cash, risk_manager=pm)
    ctx = StrategyContext(strategy_id=1, period="1d",
                          capital_ratio=Decimal("0.6"), max_positions=5)
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port


def _hold(port, qty=1000, price=Decimal("10")):
    ctx = port.strategies[0]
    pos = Position(STOCK)
    ctx.positions[STOCK] = pos
    pos.buy(qty, price, datetime(2026, 7, 1))
    pos.highest_price = price
    return pos


# ---------------- 内核：送转 / 分红 ----------------

def test_split_keeps_total_cost_and_scales_lots():
    port = _portfolio()
    pos = _hold(port)
    tl = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="split",
                             ex_date=date(2026, 7, 20),
                             share_ratio=Decimal("0.3")),
    ])
    actions = apply_day(port, tl, date(2026, 7, 20), mode="backtest",
                        last_action_date=date(2026, 7, 17))
    assert pos.quantity == 1300
    assert pos.avg_cost == Decimal("10") * 1000 / 1300
    assert pos.highest_price == Decimal("10") * 1000 / 1300
    assert sum(pos._lots.values()) == 1300
    assert any(a.kind == "split" and a.qty == 300 for a in actions)


def test_split_skipped_when_apply_split_false():
    """实盘：送转交给柜台镜面，内核不得手工改持仓数量。"""
    port = _portfolio()
    pos = _hold(port)
    tl = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="split",
                             ex_date=date(2026, 7, 20),
                             share_ratio=Decimal("0.3")),
    ])
    actions = apply_day(port, tl, date(2026, 7, 20), mode="live",
                        apply_split=False,
                        last_action_date=date(2026, 7, 17))
    assert pos.quantity == 1000
    assert any(a.kind == "split" and a.reason == "mirror" for a in actions)


def test_dividend_backtest_uses_pct_times_prev_close():
    port = _portfolio()
    cash0 = port.account.cash
    _hold(port)
    tl = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="dividend",
                             ex_date=date(2026, 7, 20),
                             cash_pct=Decimal("0.01")),
    ])
    actions = apply_day(port, tl, date(2026, 7, 20), mode="backtest",
                        price_resolver=lambda code: Decimal("10"),
                        last_action_date=date(2026, 7, 17))
    # 0.01 × 10 × 1000 = 100
    assert port.account.cash == cash0 + Decimal("100")
    assert [a.cash for a in actions if a.kind == KIND_DIVIDEND_CASH] == [Decimal("100")]


def test_dividend_live_uses_per_share_amount():
    port = _portfolio()
    cash0 = port.account.cash
    _hold(port)
    tl = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="dividend",
                             ex_date=date(2026, 7, 20),
                             cash_per_share=Decimal("0.35")),
    ])
    apply_day(port, tl, date(2026, 7, 20), mode="live",
              price_resolver=lambda code: Decimal("10"),
              last_action_date=date(2026, 7, 17))
    assert port.account.cash == cash0 + Decimal("350")


def test_dividend_backtest_raw_prefers_per_share():
    """J4 exact 档（不复权价空间）：优先按每股税后金额入账。"""
    port = _portfolio()
    cash0 = port.account.cash
    _hold(port)
    tl = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="dividend",
                             ex_date=date(2026, 7, 20),
                             cash_per_share=Decimal("0.35"),
                             cash_pct=Decimal("0.99")),
    ])
    apply_day(port, tl, date(2026, 7, 20), mode="backtest_raw",
              price_resolver=lambda code: Decimal("10"),
              last_action_date=date(2026, 7, 17))
    assert port.account.cash == cash0 + Decimal("350")


def test_dividend_backtest_raw_falls_back_to_pct():
    """J4 exact 档缺每股金额时回落比例口径（与 backtest 相同算式）。"""
    port = _portfolio()
    cash0 = port.account.cash
    _hold(port)
    tl = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="dividend",
                             ex_date=date(2026, 7, 20),
                             cash_pct=Decimal("0.01")),
    ])
    apply_day(port, tl, date(2026, 7, 20), mode="backtest_raw",
              price_resolver=lambda code: Decimal("10"),
              last_action_date=date(2026, 7, 17))
    assert port.account.cash == cash0 + Decimal("100")


def test_dividend_missing_field_is_skip_not_guess():
    port = _portfolio()
    cash0 = port.account.cash
    _hold(port)
    tl = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="dividend",
                             ex_date=date(2026, 7, 20),
                             cash_per_share=Decimal("0.35")),
    ])
    actions = apply_day(port, tl, date(2026, 7, 20), mode="backtest",
                        price_resolver=lambda code: Decimal("10"),
                        last_action_date=date(2026, 7, 17))
    assert port.account.cash == cash0
    assert any(a.kind == KIND_SKIP_CASH for a in actions)


def test_event_postponed_to_next_trading_day_and_only_once():
    """周六除权 → 周一才生效；连续两日调用只有第一次生效。"""
    port = _portfolio()
    cash0 = port.account.cash
    _hold(port)
    tl = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="dividend",
                             ex_date=date(2026, 7, 18),  # 周六
                             cash_per_share=Decimal("1")),
    ])
    apply_day(port, tl, date(2026, 7, 20), mode="live",
              last_action_date=date(2026, 7, 17))
    assert port.account.cash == cash0 + Decimal("1000")
    # 同一天再调用（last_action_date=周一）→ 无事件
    apply_day(port, tl, date(2026, 7, 20), mode="live",
              last_action_date=date(2026, 7, 20))
    assert port.account.cash == cash0 + Decimal("1000")


# ---------------- 回测接入 ----------------

def _klines():
    return {STOCK: {"1d": pl.DataFrame({
        "datetime": [datetime(2026, 7, 29), datetime(2026, 7, 30)],
        "Open": [Decimal("10"), Decimal("10.2")],
        "High": [Decimal("10.3"), Decimal("10.5")],
        "Low": [Decimal("9.9"), Decimal("10.0")],
        "Close": [Decimal("10.2"), Decimal("10.4")],
        "Volume": [1000, 1000],
    })}}


def _cache():
    return {
        (1, STOCK, datetime(2026, 7, 29)): [{"name": "open_sig", "value": 1}],
        (1, STOCK, datetime(2026, 7, 30)): [{"name": "open_sig", "value": 0}],
    }


def _ctx(port):
    ctx = port.strategies[0]
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN, "trigger_value": 1},
    ]
    return ctx


def test_suspended_code_blocks_open_in_backtest():
    port = _portfolio()
    _ctx(port)
    port.corporate_timeline = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="suspend",
                             ex_date=date(2026, 7, 29),
                             end_date=date(2026, 7, 30)),
    ])
    result = BacktestEngine().run(port, klines=_klines(), signal_cache=_cache())
    assert result["trades"] == []
    assert any(ev.get("gate") == "corporate_suspend" and ev.get("action") == "block"
               for ev in result["decisions"])


def test_without_timeline_behavior_unchanged():
    port = _portfolio()
    _ctx(port)
    result = BacktestEngine().run(port, klines=_klines(), signal_cache=_cache())
    assert len(result["trades"]) == 1
    assert not any(str(ev.get("gate", "")).startswith("corporate")
                   for ev in result["decisions"])


def test_delist_force_close_generates_sell_trade():
    port = _portfolio()
    _ctx(port)
    _hold(port)
    port.corporate_timeline = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="delist",
                             ex_date=date(2026, 7, 29)),
    ])
    port.delist_force_close_enabled = True
    result = BacktestEngine().run(port, klines=_klines(), signal_cache=_cache())
    sells = [t for t in result["trades"] if t.trade_type == TradeType.SELL]
    assert sells and sells[0].quantity == 1000
    assert any(ev.get("gate") == "corporate_action" for ev in result["decisions"])


def test_delist_notice_when_force_close_disabled():
    port = _portfolio()
    _ctx(port)
    pos = _hold(port)
    port.corporate_timeline = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="delist",
                             ex_date=date(2026, 7, 29)),
    ])
    result = BacktestEngine().run(port, klines=_klines(), signal_cache=_cache())
    assert not [t for t in result["trades"] if t.trade_type == TradeType.SELL]
    assert pos.quantity > 0 or pos.quantity == 1000
    assert any(ev.get("action") == "notice" for ev in result["decisions"]
               if ev.get("gate") == "corporate_action")


def test_reinvest_buys_whole_lots_at_close():
    port = _portfolio()
    _ctx(port)
    _hold(port)
    port.corporate_timeline = CorporateActionTimeline([
        CorporateActionEvent(code=STOCK, event_type="dividend",
                             ex_date=date(2026, 7, 29),
                             cash_pct=Decimal("0.5")),
    ])
    port.dividend_reinvest_enabled = True
    result = BacktestEngine().run(port, klines=_klines(), signal_cache=_cache())
    reinvest = [t for t in result["trades"]
                if t.signal_name == "corporate_action_reinvest"]
    assert reinvest, "应有分红再投资成交"
    assert reinvest[0].quantity % 100 == 0
    assert any(ev.get("action") == "apply" for ev in result["decisions"]
               if ev.get("gate") == "corporate_action")


def test_delist_action_kind_constant_is_stable():
    assert KIND_DELIST_CLOSE == "delist_close"


# ---------------- 除权除息闸（2026-11-22 口径切换） ----------------

def test_ex_dividend_day_blocks_open_but_not_close():
    port = _portfolio()
    _ctx(port)
    port.ex_dividend_codes = frozenset({STOCK})
    port.ex_dividend_data_ok = True
    result = BacktestEngine().run(port, klines=_klines(), signal_cache=_cache())
    assert result["trades"] == []
    assert any(ev.get("gate") == "corporate_ex_dividend" and ev.get("action") == "block"
               for ev in result["decisions"])

    # 卖出不受除权闸影响：持仓 + CLOSE 信号照常成交
    sell_port = _portfolio()
    sell_ctx = _ctx(sell_port)
    sell_ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.CLOSE, "trigger_value": 1},
    ]
    _hold(sell_port)
    sell_port.ex_dividend_codes = frozenset({STOCK})
    sell_port.ex_dividend_data_ok = True
    sell_result = BacktestEngine().run(
        sell_port, klines=_klines(), signal_cache=_cache())
    assert [t for t in sell_result["trades"] if t.trade_type == TradeType.SELL]


def test_ex_dividend_missing_data_fails_open_with_one_notice():
    """取不到事件表：不误拦（照常开仓），但每日至多一条不可用标记。"""
    port = _portfolio()
    _ctx(port)
    port.ex_dividend_codes = frozenset()
    port.ex_dividend_data_ok = False
    result = BacktestEngine().run(port, klines=_klines(), signal_cache=_cache())
    assert len(result["trades"]) == 1
    notices = [ev for ev in result["decisions"]
               if ev.get("gate") == "ex_dividend_data_unavailable"]
    assert len(notices) == 1
    assert notices[0]["action"] == "notice"


def test_ex_dividend_loader_sets_flags_once_per_day(db_session):
    """LiveEngine 日首加载：读库失败/表空 → ok=False；同一交易日只加载一次。"""
    from types import SimpleNamespace

    from core.engine.live_engine import LiveEngine
    from core.models import CorporateAction

    db_session.add(CorporateAction(code=STOCK, event_type="dividend",
                                   ex_date=date(2026, 6, 11),
                                   cash_per_share=Decimal("0.25")))
    db_session.commit()
    portfolio = SimpleNamespace(ex_dividend_codes=None,
                                ex_dividend_data_ok=None)
    engine = SimpleNamespace(_db_session_factory=lambda: db_session,
                             _ex_div_loaded=set(), portfolios=[portfolio])

    LiveEngine._maybe_load_ex_dividend_day(engine, datetime(2026, 6, 11, 9, 20))
    assert portfolio.ex_dividend_data_ok is True
    assert portfolio.ex_dividend_codes == frozenset({STOCK})

    # 同日再调：不会重读（置 None 后仍保持，证明走了幂等短路）
    portfolio.ex_dividend_codes = None
    LiveEngine._maybe_load_ex_dividend_day(engine, datetime(2026, 6, 11, 10, 0))
    assert portfolio.ex_dividend_codes is None


# ---------------- P1-3：沪/北送转新增股 T+1 落桶 ----------------

SH = "600000.SH"
BJ = "830799.BJ"


def _hold_code(port, code, qty=1000, price=Decimal("10")):
    ctx = port.strategies[0]
    pos = Position(code)
    ctx.positions[code] = pos
    pos.buy(qty, price, datetime(2026, 7, 1))
    pos.highest_price = price
    return pos


def _split_timeline(code, ex_date=date(2026, 7, 20), ratio=Decimal("0.3")):
    return CorporateActionTimeline([
        CorporateActionEvent(code=code, event_type="split",
                             ex_date=ex_date, share_ratio=ratio),
    ])


def test_sh_split_new_shares_not_sellable_on_ex_date():
    """沪市送转：原 1000 股除权日可卖，新增 300 股次一交易日才可卖。

    本系统 ``_lots`` 键为取得日、可卖判据为 ``d < query_date``，因此新增股记
    ``ex_date`` 桶；对用户口径即「ex_date+1 起可卖」。
    """
    port = _portfolio()
    pos = _hold_code(port, SH)
    next_day = date(2026, 7, 21)
    apply_day(port, _split_timeline(SH), date(2026, 7, 20),
              mode="backtest", last_action_date=date(2026, 7, 17),
              next_trade_day=lambda d: next_day if d == date(2026, 7, 20) else None)

    assert pos.quantity == 1300
    assert pos._lots == {date(2026, 7, 1): 1000, date(2026, 7, 20): 300}
    assert pos.available_shares_on(date(2026, 7, 20)) == 1000
    assert pos.available_shares_on(next_day) == 1300
    assert pos.avg_cost == Decimal("10") * 1000 / 1300
    assert pos.highest_price == Decimal("10") * 1000 / 1300


def test_bj_split_same_t_plus_one_rule():
    port = _portfolio()
    pos = _hold_code(port, BJ)
    next_day = date(2026, 7, 21)
    apply_day(port, _split_timeline(BJ), date(2026, 7, 20),
              mode="backtest", last_action_date=date(2026, 7, 17),
              next_trade_day=lambda d: next_day)

    assert pos._lots == {date(2026, 7, 1): 1000, date(2026, 7, 20): 300}
    assert pos.available_shares_on(date(2026, 7, 20)) == 1000
    assert pos.available_shares_on(next_day) == 1300


def test_sz_split_keeps_scaling_all_lots_same_day():
    """深市维持原口径：各桶等比放大、日期不变，除权日全部可卖。"""
    port = _portfolio()
    pos = _hold_code(port, STOCK)
    apply_day(port, _split_timeline(STOCK), date(2026, 7, 20),
              mode="backtest", last_action_date=date(2026, 7, 17),
              next_trade_day=lambda d: date(2026, 7, 21))

    assert pos.quantity == 1300
    assert pos._lots == {date(2026, 7, 1): 1300}
    assert pos.available_shares_on(date(2026, 7, 20)) == 1300


def test_sh_split_falls_back_when_next_trade_day_unknown():
    """取不到下一交易日 → 回落到原「等比放大」口径（不静默丢股）。"""
    port = _portfolio()
    pos = _hold_code(port, SH)
    apply_day(port, _split_timeline(SH), date(2026, 7, 20),
              mode="backtest", last_action_date=date(2026, 7, 17),
              next_trade_day=lambda d: None)

    assert pos.quantity == 1300
    assert sum(pos._lots.values()) == 1300
    assert pos.available_shares_on(date(2026, 7, 20)) == 1300


# ---------------- 除息基准调整（2026-11-25） ----------------
# 真实样本：600256.SH 2025-07-18 除息，每股派 0.622 元（前收 6.08 → 除息日
# 开 5.48 / 收 5.45，下跳幅度 = 派息额）。若成本与峰值不随同减，
# 收盘 5.45 相对成本 6.08 的 -10.36% 会被算成止损/移动止损回撤 → 凭空触发。
DIV_CODE = "600256.SH"
DIV_CPS = Decimal("0.622")
DIV_PREV_CLOSE = Decimal("6.08")
DIV_EX_DATE = date(2025, 7, 18)


def _hold_div(port, qty=1000):
    ctx = port.strategies[0]
    pos = Position(DIV_CODE)
    ctx.positions[DIV_CODE] = pos
    pos.buy(qty, DIV_PREV_CLOSE, datetime(2025, 7, 17))
    pos.highest_price = DIV_PREV_CLOSE
    return pos


def _div_timeline():
    return CorporateActionTimeline([
        CorporateActionEvent(code=DIV_CODE, event_type="dividend",
                             ex_date=DIV_EX_DATE,
                             cash_per_share=DIV_CPS),
    ])


def _div_klines():
    """真实日线（通达信 .day 原始价，不复权）。"""
    return {DIV_CODE: {"1d": pl.DataFrame({
        "datetime": [datetime(2025, 7, 16), datetime(2025, 7, 17),
                     datetime(2025, 7, 18), datetime(2025, 7, 21)],
        "Open": [Decimal("6.03"), Decimal("6.09"), Decimal("5.48"), Decimal("5.45")],
        "High": [Decimal("6.10"), Decimal("6.11"), Decimal("5.50"), Decimal("5.54")],
        "Low": [Decimal("6.02"), Decimal("6.02"), Decimal("5.43"), Decimal("5.45")],
        "Close": [Decimal("6.09"), Decimal("6.08"), Decimal("5.45"), Decimal("5.53")],
        "Volume": [1000, 1000, 1000, 1000],
    })}}


def _risk_ctx(port, *, stop=Decimal("0.05"), take=Decimal("0.99"),
              trail=Decimal("0")):
    port.strategies[0].strategy_risk = StrategyRiskManager(
        stop_loss_ratio=stop, take_profit_ratio=take,
        trailing_stop_ratio=trail)


def test_ex_dividend_no_false_stop_loss_backtest_raw():
    """不复权回测（exact）：除息日不因价格下跳触发固定止损。"""
    port = _portfolio()
    _risk_ctx(port)
    pos = _hold_div(port)
    port.corporate_action_mode = "backtest_raw"
    port.corporate_timeline = _div_timeline()

    result = BacktestEngine().run(
        port, klines=_div_klines(), signal_cache={})

    assert not [t for t in result["trades"] if t.trade_type == TradeType.SELL]
    # 成本按每股派息同减：6.08 - 0.622 = 5.458（峰值除息日同样下调到 5.458，
    # 之后被 2025-07-21 收盘 5.53 重新抬高）
    assert pos.avg_cost == Decimal("5.458")
    assert pos.highest_price == Decimal("5.53")
    # 现金分红照常入账（0.622 × 1000）
    assert port.account.cash == Decimal("100000") + Decimal("622.00")


def test_ex_dividend_no_false_trailing_stop_backtest_raw():
    """不复权回测（exact）：除息日不因价格下跳触发移动止损。"""
    port = _portfolio()
    _risk_ctx(port, stop=Decimal("0.99"), trail=Decimal("0.03"))
    pos = _hold_div(port)
    port.corporate_action_mode = "backtest_raw"
    port.corporate_timeline = _div_timeline()

    result = BacktestEngine().run(
        port, klines=_div_klines(), signal_cache={})

    assert not [t for t in result["trades"] if t.trade_type == TradeType.SELL]
    # 除息日收盘 5.45 相对下调后的峰值 5.458 仅 -0.15%，远小于 3%；
    # 若峰值仍是除息前的 6.08，回撤 10.36% 必然触发（这正是修复前的情形）。
    assert pos.highest_price == Decimal("5.53")


def test_ex_dividend_no_false_stop_loss_live():
    """实盘：柜台成本已摊薄（mirror_bridge），内核只调峰值且不二次扣减成本。"""
    from core.engine.event import BarEvent

    port = _portfolio()
    _risk_ctx(port)
    pos = _hold_div(port)
    # 模拟柜台合并均价：已按分红摊薄为 5.458（内核不得再减一次）
    pos.mirror_bridge(1000, 1000, "5.458", date(2025, 7, 17))

    apply_day(port, _div_timeline(), DIV_EX_DATE, mode="live",
              apply_split=False, price_resolver=lambda code: DIV_PREV_CLOSE,
              last_action_date=date(2025, 7, 17))

    assert pos.avg_cost == Decimal("5.458")   # 不被内核二次扣减
    assert pos.highest_price == Decimal("5.458")
    assert port.account.cash == Decimal("100000") + Decimal("622.00")

    bar = BarEvent(stocks={DIV_CODE: {
        "open": Decimal("5.48"), "high": Decimal("5.50"),
        "low": Decimal("5.43"), "close": Decimal("5.45"),
        "volume": 1000,
    }}, bar_time=datetime(2025, 7, 18, 15, 0), period="1d")
    orders = port.on_bar(bar, signal_cache={}, period="1d")
    assert not [o for o in orders if o.trade_type == TradeType.SELL]


def test_dividend_preserves_cost_plus_cash_book_value():
    """分红后口径正确：成本市值 + 现金合计不变（只是左口袋换右口袋）。"""
    port = _portfolio()
    pos = _hold_div(port)
    before = pos.avg_cost * pos.quantity + port.account.cash

    apply_day(port, _div_timeline(), DIV_EX_DATE, mode="backtest_raw",
              apply_split=False, price_resolver=lambda code: DIV_PREV_CLOSE,
              last_action_date=date(2025, 7, 17))

    after = pos.avg_cost * pos.quantity + port.account.cash
    assert after == before


def test_dividend_front_adjusted_mode_does_not_double_adjust():
    """前复权空间序列本身连续：不再下调成本/峰值（避免双重扣减）。"""
    port = _portfolio()
    pos = _hold_div(port)
    before_cost, before_high = pos.avg_cost, pos.highest_price

    apply_day(port, _div_timeline(), DIV_EX_DATE, mode="backtest",
              apply_split=False, price_resolver=lambda code: DIV_PREV_CLOSE,
              last_action_date=date(2025, 7, 17))

    assert pos.avg_cost == before_cost
    assert pos.highest_price == before_high


# ---------------- 峰值下调的强制落库 ----------------

def test_peak_adjust_force_overwrites_higher_persisted_peak(db_session):
    """除息下调必须覆盖库里更高的旧峰值，且不改 peak_source/peak_bar_time。"""
    import threading
    from types import SimpleNamespace

    from core.engine.live_engine import LiveEngine
    from core.models import LivePositionPeak

    engine = object.__new__(LiveEngine)
    engine.ctx = SimpleNamespace(session_id=7)
    engine._peak_buffer = {}
    engine._peak_rows = set()
    engine._peak_lock = threading.Lock()
    engine._db_session_factory = lambda: db_session

    db_session.add(LivePositionPeak(
        live_session_id=7, portfolio_strategy_id=1, stock_code=DIV_CODE,
        highest_price=Decimal("6.08"),
        peak_bar_time=datetime(2025, 7, 17, 15, 0), peak_source="bar"))
    db_session.commit()

    engine._note_peak_adjusted(1, DIV_CODE, Decimal("5.458"))
    assert engine._flush_position_peaks(db_session) is True
    db_session.commit()

    row = db_session.query(LivePositionPeak).one()
    assert row.highest_price == Decimal("5.458")
    assert row.peak_source == "bar"
    assert row.peak_bar_time == datetime(2025, 7, 17, 15, 0)


def test_peak_raise_after_adjust_still_forces_write(db_session):
    """下调待写期间又创新高：仍带强制标记，不被库里旧高值挡住。"""
    import threading
    from types import SimpleNamespace

    from core.engine.live_engine import LiveEngine
    from core.models import LivePositionPeak

    engine = object.__new__(LiveEngine)
    engine.ctx = SimpleNamespace(session_id=7)
    engine._peak_buffer = {}
    engine._peak_rows = set()
    engine._peak_lock = threading.Lock()
    engine._db_session_factory = lambda: db_session

    db_session.add(LivePositionPeak(
        live_session_id=7, portfolio_strategy_id=1, stock_code=DIV_CODE,
        highest_price=Decimal("6.08"), peak_source="bar"))
    db_session.commit()

    engine._note_peak_adjusted(1, DIV_CODE, Decimal("5.458"))
    engine._note_peak_raised(1, DIV_CODE, Decimal("5.70"),
                             datetime(2025, 7, 22, 15, 0))
    engine._flush_position_peaks(db_session)
    db_session.commit()

    row = db_session.query(LivePositionPeak).one()
    assert row.highest_price == Decimal("5.70")



