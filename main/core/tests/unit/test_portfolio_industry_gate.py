# -*- coding: utf-8 -*-
"""行业放行门控：relief / strict / 缺失 fail-open / 全关逐位不变。"""
from datetime import date, datetime
from decimal import Decimal

from core.engine.decision import DecisionRecorder
from core.engine.event import BarEvent, SignalEvent, TradeEvent
from core.engine.industry_gate import IndustryState, IndustryTracker
from core.engine.market_gate import MarketGateTracker
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from tq_iquant_shared.constants import SignalType, TradeType

CODE = "600019.SH"
IND = "881002.SH"


def _market(states):
    t = MarketGateTracker()
    t.feed_history(states, {d: 60 for d in states})
    return t


def _industry(direction, stock=CODE, ind=IND):
    return IndustryTracker(
        {ind: {date(2025, 6, 16): IndustryState(
            direction=direction, strength=70.0,
            bull_dur=10 if direction in (1, 2) else 0,
            bias_z=0.0, overbought=False)}},
        {stock: ind},
    )


def _mk_portfolio(market=None, industry=None, mode="off"):
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(strategy_id=1, period="1d",
                          capital_ratio=Decimal("0.6"), max_positions=5)
    ctx.formula_signals = []
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.1"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"))
    ctx.add_position_threshold = Decimal("-1")
    port.strategies.append(ctx)
    port.current_market_gate = market
    port.current_industry_gate = industry
    port.configure_industry({"industry_gate_mode": mode})
    return port, ctx


def _held(port, ctx):
    pos = Position(CODE)
    pos.apply_trade(TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=CODE,
        trade_type=TradeType.BUY, price=Decimal("10"), quantity=1000,
        amount=Decimal("10000"), commission=Decimal("0"),
        stamp_duty=Decimal("0"), trade_time=datetime(2025, 6, 16, 9, 30)))
    ctx.positions[CODE] = pos
    return port, ctx


def _bar():
    return BarEvent(
        stocks={CODE: {"open": Decimal("10"), "high": Decimal("10.6"),
                       "low": Decimal("9.4"), "close": Decimal("10.5"),
                       "volume": 1000}},
        bar_time=datetime(2025, 6, 17, 15, 0))


def _open():
    return SignalEvent(strategy_id=1, stock_code=CODE, signal_name="sig",
                       signal_type=SignalType.OPEN,
                       bar_time=datetime(2025, 6, 17, 15, 0))


def test_off_mode_unchanged():
    """全关：大盘空头仍拦、大盘多头仍放行（与接线前一致）。"""
    p, ctx = _mk_portfolio(market=_market({date(2025, 6, 16): -1}),
                           industry=_industry(1))
    assert p._signal_to_order(ctx, _open(), _bar()) == []
    assert p._gate_decision(_bar().bar_time, ctx, CODE) == "market"

    p2, ctx2 = _mk_portfolio(market=_market({date(2025, 6, 16): 1}),
                             industry=_industry(-1))
    assert len(p2._signal_to_order(ctx2, _open(), _bar())) == 1


def test_relief_allows_bull_industry_when_market_bear():
    p, ctx = _mk_portfolio(market=_market({date(2025, 6, 16): -1}),
                           industry=_industry(1), mode="industry_relief")
    assert p._gate_decision(_bar().bar_time, ctx, CODE) is None
    assert len(p._signal_to_order(ctx, _open(), _bar())) == 1


def test_relief_blocks_non_bull_industry_when_market_bear():
    p, ctx = _mk_portfolio(market=_market({date(2025, 6, 16): -1}),
                           industry=_industry(-1), mode="industry_relief")
    rec = DecisionRecorder()
    p.set_recorder(rec)
    assert p._signal_to_order(ctx, _open(), _bar()) == []
    assert p._gate_decision(_bar().bar_time, ctx, CODE) == "industry"
    assert any(e.gate == "industry_gate" and e.action == "block"
               for e in rec.drain())


def test_relief_ignores_industry_when_market_bull():
    p, ctx = _mk_portfolio(market=_market({date(2025, 6, 16): 1}),
                           industry=_industry(-1), mode="industry_relief")
    assert p._gate_decision(_bar().bar_time, ctx, CODE) is None


def test_strict_blocks_non_bull_even_when_market_bull():
    p, ctx = _mk_portfolio(market=_market({date(2025, 6, 16): 1}),
                           industry=_industry(0), mode="industry_strict")
    assert p._gate_decision(_bar().bar_time, ctx, CODE) == "industry"
    assert p._signal_to_order(ctx, _open(), _bar()) == []


def test_strict_industry_bull_does_not_override_market_bear():
    p, ctx = _mk_portfolio(market=_market({date(2025, 6, 16): -1}),
                           industry=_industry(1), mode="industry_strict")
    assert p._gate_decision(_bar().bar_time, ctx, CODE) == "market"


def test_missing_industry_is_fail_open_with_event():
    """未映射个股 → 放行（大盘多头）+ WARNING 事件；大盘空头仍按大盘拦。"""
    p, ctx = _mk_portfolio(market=_market({date(2025, 6, 16): 1}),
                           industry=IndustryTracker({}, {}),
                           mode="industry_strict")
    rec = DecisionRecorder()
    p.set_recorder(rec)
    assert p._gate_decision(_bar().bar_time, ctx, CODE) is None
    assert any(e.gate == "industry_gate_missing" for e in rec.drain())
    # 大盘空头时缺失行业不改变大盘结论
    p.current_market_gate = _market({date(2025, 6, 16): -1})
    assert p._gate_decision(_bar().bar_time, ctx, CODE) == "market"


def test_strategy_off_bypasses_industry_gate():
    p, ctx = _mk_portfolio(market=_market({date(2025, 6, 16): -1}),
                           industry=_industry(-1), mode="industry_strict")
    ctx.market_gate_mode = "off"
    assert p._gate_decision(_bar().bar_time, ctx, CODE) is None


def test_relief_applies_to_add():
    p, ctx = _held(*_mk_portfolio(
        market=_market({date(2025, 6, 16): -1}),
        industry=_industry(1), mode="industry_relief"))
    sig = SignalEvent(strategy_id=1, stock_code=CODE, signal_name="add",
                      signal_type=SignalType.ADD,
                      bar_time=datetime(2025, 6, 17, 15, 0))
    assert len(p._signal_to_order(ctx, sig, _bar())) == 1
