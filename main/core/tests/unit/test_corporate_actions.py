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

