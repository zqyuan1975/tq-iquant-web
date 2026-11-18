# -*- coding: utf-8 -*-
"""市场门控闸：多头放行 / 空头拦 OPEN+ADD / 卖出与风控不受影响 / None 旧行为。

fixture 模式复用 test_portfolio.py（Portfolio 最小构造 + Position 预置持仓）；
直接调 ``_signal_to_order`` 验证闸门语义，不跑完整 on_bar。
"""
from datetime import date, datetime
from decimal import Decimal

from core.engine.decision import DecisionRecorder
from core.engine.event import BarEvent, SignalEvent, TradeEvent
from core.engine.market_gate import MarketGateTracker
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.engine.position import Position
from tq_iquant_shared.constants import SignalType, TradeType

CODE = "600019.SH"


def _tracker(states):
    t = MarketGateTracker()
    t.feed_history(states, {d: 60 for d in states})
    return t


def _mk_portfolio(gate=None):
    """最小 Portfolio + StrategyContext（不跑 assemble）。

    gate 非 None 时设 p.current_market_gate = gate。返回 (port, ctx)。
    """
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1d",
        capital_ratio=Decimal("0.6"), max_positions=5,
    )
    ctx.formula_signals = []
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.1"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    port.current_market_gate = gate
    return port, ctx


def _held_ctx(port, ctx):
    """同款 ctx 预置一只持仓（ADD/CLOSE 用），avg_cost=10。"""
    pos = Position(CODE)
    pos.apply_trade(_buy("10", 1000, datetime(2025, 6, 16, 9, 30)))
    ctx.positions[CODE] = pos
    return port, ctx


def _bar(bar_date):
    return BarEvent(
        stocks={CODE: {
            "open": Decimal("10"), "high": Decimal("10.6"),
            "low": Decimal("9.4"), "close": Decimal("10.5"), "volume": 1000,
        }},
        bar_time=datetime(bar_date.year, bar_date.month, bar_date.day, 15, 0),
    )


def _sig(stype):
    return SignalEvent(strategy_id=1, stock_code=CODE,
                       signal_name="sig", signal_type=stype,
                       bar_time=datetime(2025, 6, 17, 15, 0))


def _buy(price, quantity, trade_time):
    return TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=CODE,
        trade_type=TradeType.BUY, price=Decimal(price), quantity=quantity,
        amount=Decimal(price) * quantity, commission=Decimal("0"),
        stamp_duty=Decimal("0"), trade_time=trade_time,
    )


def test_gate_blocks_open_in_bear():
    """空头状态（昨日收盘）→ OPEN 被拦，记 gate=market_gate 决策事件。"""
    p, ctx = _mk_portfolio(gate=_tracker({date(2025, 6, 16): -1}))
    rec = DecisionRecorder()
    p.set_recorder(rec)
    bar = _bar(date(2025, 6, 17))
    orders = p._signal_to_order(ctx, _sig(SignalType.OPEN), bar)
    assert orders == []
    events = rec.drain()
    assert any(ev.gate == "market_gate" and ev.action == "block" for ev in events)


def test_gate_allows_open_in_bull():
    """多头状态 → OPEN 放行（其余闸按既有 fixture 不触发）。"""
    p, ctx = _mk_portfolio(gate=_tracker({date(2025, 6, 16): 1}))
    bar = _bar(date(2025, 6, 17))
    orders = p._signal_to_order(ctx, _sig(SignalType.OPEN), bar)
    assert len(orders) == 1
    assert orders[0].trade_type == TradeType.BUY


def test_gate_blocks_add_in_bear():
    """空头状态 + 有持仓 → ADD 被拦（在 add_position_threshold 判断之前）。"""
    p, ctx = _held_ctx(*_mk_portfolio(gate=_tracker({date(2025, 6, 16): -1})))
    bar = _bar(date(2025, 6, 17))
    orders = p._signal_to_order(ctx, _sig(SignalType.ADD), bar)
    assert orders == []


def test_gate_right_shift_uses_prev_day():
    """右移一日：16 日空头、17 日多头 → 17 日的 OPEN 用 16 日状态，仍拦。"""
    p, ctx = _mk_portfolio(gate=_tracker({date(2025, 6, 16): -1,
                                          date(2025, 6, 17): 1}))
    bar = _bar(date(2025, 6, 17))
    assert p._signal_to_order(ctx, _sig(SignalType.OPEN), bar) == []


def test_gate_does_not_block_sell():
    """空头状态不影响 CLOSE 卖出（多空切换不强平存量持仓）。"""
    p, ctx = _held_ctx(*_mk_portfolio(gate=_tracker({date(2025, 6, 16): -1})))
    bar = _bar(date(2025, 6, 17))
    orders = p._signal_to_order(ctx, _sig(SignalType.CLOSE), bar)
    assert len(orders) == 1
    assert orders[0].trade_type == TradeType.SELL
    assert orders[0].quantity == 1000


def test_gate_none_unchanged():
    """tracker=None（门控关）→ OPEN 行为与旧逻辑一致，正常出单。"""
    p, ctx = _mk_portfolio(gate=None)
    bar = _bar(date(2025, 6, 17))
    orders = p._signal_to_order(ctx, _sig(SignalType.OPEN), bar)
    assert len(orders) == 1
