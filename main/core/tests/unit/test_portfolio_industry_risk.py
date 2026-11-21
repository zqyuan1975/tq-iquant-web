# -*- coding: utf-8 -*-
"""C 强度权重 / D 超买停开仓 / E 转空收紧移动止损 / F 行业空头减仓。"""
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

A = "600019.SH"
IND = "881002.SH"


def _ind(direction, strength=100.0, z=0.0, ob=False):
    return IndustryTracker(
        {IND: {date(2025, 6, 16): IndustryState(
            direction=direction, strength=strength,
            bull_dur=20 if direction in (1, 2) else 0,
            bias_z=z, overbought=ob)}},
        {A: IND},
    )


def _mk(ind=None, market=None, **switches):
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(strategy_id=1, period="1d",
                          capital_ratio=Decimal("0.9"), max_positions=5)
    ctx.formula_signals = []
    ctx.single_open_ratio = Decimal("0.5")
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.1"), take_profit_ratio=Decimal("0.5"),
        trailing_stop_ratio=Decimal("0.1"))
    ctx.get_signal = lambda bar, signal_cache=None: [
        SignalEvent(strategy_id=1, stock_code=A, signal_name="sig",
                    signal_type=SignalType.OPEN, bar_time=bar.bar_time)]
    port.strategies.append(ctx)
    port.max_holdings = 5
    port.current_industry_gate = ind
    port.current_market_gate = market
    port.configure_industry({"industry_weight_enabled": False,
                             "industry_overbought_block": False,
                             "industry_trail_tighten_enabled": False,
                             "industry_bear_reduce_ratio": 0.0})
    if switches:
        port.configure_industry(dict({"industry_weight_enabled": False}, **switches))
    return port, ctx


def _bar(close="10.5"):
    return BarEvent(
        stocks={A: {"open": Decimal(close), "high": Decimal(close),
                    "low": Decimal("9.4"), "close": Decimal(close),
                    "volume": 1000}},
        bar_time=datetime(2025, 6, 17, 15, 0),
    )


def _hold(port, ctx, avg="10", highest="10"):
    pos = Position(A)
    pos.apply_trade(TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=A, trade_type=TradeType.BUY,
        price=Decimal(avg), quantity=1000, amount=Decimal(avg) * 1000,
        commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=datetime(2025, 6, 16, 9, 30)))
    pos.highest_price = Decimal(highest)
    ctx.positions[A] = pos
    return pos


# ---------------- C ----------------

def test_strength_weight_scales_quantity():
    base_port, base_ctx = _mk(ind=_ind(1, 100.0))
    base_orders = base_port.on_bar(_bar())
    assert len(base_orders) == 1
    base_qty = base_orders[0].quantity

    port, ctx = _mk(ind=_ind(1, 100.0), industry_weight_enabled=True)
    orders = port.on_bar(_bar())
    assert len(orders) == 1
    # weight = 0.5 + 100/100 = 1.5 → 数量为基准 1.5 倍（整手取整后）
    assert orders[0].quantity > base_qty
    assert orders[0].quantity <= int(base_qty * Decimal("1.5")) + 100


def test_weight_neutral_when_industry_not_bull():
    port, ctx = _mk(ind=_ind(-1, 100.0), industry_weight_enabled=True)
    orders = port.on_bar(_bar())
    assert len(orders) == 1  # 非多头不放大（门控关闭时仍放行）
    base_port, _ = _mk(ind=_ind(-1, 100.0))
    assert orders[0].quantity == base_port.on_bar(_bar())[0].quantity


# ---------------- D ----------------

def test_overbought_blocks_open_not_existing_position():
    port, ctx = _mk(ind=_ind(1, 80.0, z=3.1, ob=True),
                    industry_overbought_block=True)
    rec = DecisionRecorder()
    port.set_recorder(rec)
    assert port.on_bar(_bar()) == []
    assert any(e.gate == "industry_overbought" for e in rec.drain())

    # 已有持仓不强平：超买不会产生卖出信号
    port2, ctx2 = _mk(ind=_ind(1, 80.0, z=3.1, ob=True),
                      industry_overbought_block=True)
    _hold(port2, ctx2)
    ctx2.get_signal = lambda bar, signal_cache=None: []
    assert port2.on_bar(_bar()) == []


def test_overbought_below_threshold_allows():
    port, ctx = _mk(ind=_ind(1, 80.0, z=2.0, ob=False),
                    industry_overbought_block=True)
    assert len(port.on_bar(_bar())) == 1


# ---------------- E ----------------

def test_industry_bear_tightens_trailing_stop():
    # 高 10 现价 9.84（回撤 1.6%）：原 10% 移动止损不触发
    port, ctx = _mk(ind=_ind(-1), industry_trail_tighten_enabled=True,
                    industry_trail_tighten_ratio=0.015)
    _hold(port, ctx, highest="10")
    ctx.get_signal = lambda bar, signal_cache=None: []
    signals = port._check_risks_locked(ctx, _bar("9.84"))
    assert any(s.signal_type == SignalType.TRAILING_STOP for s in signals)


def test_trailing_untouched_when_industry_not_bear():
    port, ctx = _mk(ind=_ind(1), industry_trail_tighten_enabled=True,
                    industry_trail_tighten_ratio=0.015)
    _hold(port, ctx, highest="10")
    ctx.get_signal = lambda bar, signal_cache=None: []
    signals = port._check_risks_locked(ctx, _bar("9.84"))
    assert not any(s.signal_type == SignalType.TRAILING_STOP for s in signals)


# ---------------- F ----------------

def _bear_market():
    t = MarketGateTracker()
    t.feed_history({date(2025, 6, 16): -1}, {})
    return t


def _bull_market():
    t = MarketGateTracker()
    t.feed_history({date(2025, 6, 16): 1}, {})
    return t


def test_industry_bear_reduce_uses_industry_ratio():
    port, ctx = _mk(ind=_ind(-1), market=_bull_market())
    port.industry_bear_reduce_ratio = Decimal("0.3")
    _hold(port, ctx)
    sigs = port._gate_bear_reduce_signals(ctx, _bar())
    assert len(sigs) == 1 and sigs[0].reduce_ratio == Decimal("0.3")
    # 同一空头段内不重复
    assert port._gate_bear_reduce_signals(ctx, _bar()) == []


def test_reduce_takes_max_not_sum():
    port, ctx = _mk(ind=_ind(-1), market=_bear_market())
    port.gate_bear_reduce_ratio = Decimal("0.2")
    port.industry_bear_reduce_ratio = Decimal("0.3")
    _hold(port, ctx)
    sigs = port._gate_bear_reduce_signals(ctx, _bar())
    assert len(sigs) == 1
    assert sigs[0].reduce_ratio == Decimal("0.3")  # max(0.2, 0.3)，不是 0.5

    port2, ctx2 = _mk(ind=_ind(-1), market=_bear_market())
    port2.gate_bear_reduce_ratio = Decimal("0.4")
    port2.industry_bear_reduce_ratio = Decimal("0.3")
    _hold(port2, ctx2)
    assert port2._gate_bear_reduce_signals(ctx2, _bar())[0].reduce_ratio == Decimal("0.4")


def test_reduce_episode_resets_after_industry_turns_bull():
    port, ctx = _mk(ind=_ind(-1), market=_bull_market())
    port.industry_bear_reduce_ratio = Decimal("0.3")
    _hold(port, ctx)
    assert len(port._gate_bear_reduce_signals(ctx, _bar())) == 1
    # 行业转多 → 清 episode；再转空重新计一次
    port.current_industry_gate = _ind(1)
    assert port._gate_bear_reduce_signals(ctx, _bar()) == []
    assert (1, A) not in port._gate_bear_episode
    port.current_industry_gate = _ind(-1)
    assert len(port._gate_bear_reduce_signals(ctx, _bar())) == 1
