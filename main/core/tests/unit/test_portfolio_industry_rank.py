# -*- coding: utf-8 -*-
"""B 跨策略 OPEN 排序 + 同行业持仓上限（默认关闭）。"""
from datetime import date, datetime
from decimal import Decimal

from core.engine.decision import DecisionRecorder
from core.engine.event import BarEvent, SignalEvent, TradeEvent
from core.engine.industry_gate import IndustryState, IndustryTracker
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from tq_iquant_shared.constants import SignalType, TradeType

A, B, C = "600019.SH", "600028.SH", "601857.SH"
I1, I2 = "881002.SH", "881005.SH"


def _mk(max_holdings=5, rank=True, cap=2, ind=None):
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(strategy_id=1, period="1d",
                          capital_ratio=Decimal("0.9"), max_positions=5)
    ctx.formula_signals = []
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.1"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"))
    port.strategies.append(ctx)
    port.max_holdings = max_holdings
    port.current_industry_gate = ind
    port.configure_industry({
        "industry_rank_enabled": rank,
        "industry_max_per_industry": cap,
    })
    return port, ctx


def _ind(bull_by_stock):
    states = {}
    membership = {}
    for stock, (industry, lead, strength) in bull_by_stock.items():
        membership[stock] = industry
        states.setdefault(industry, {})[date(2025, 6, 16)] = IndustryState(
            direction=1 if lead else 0, strength=strength, bull_dur=lead,
            bias_z=0.0, overbought=False)
    return IndustryTracker(states, membership)


def _bar(*codes):
    return BarEvent(
        stocks={c: {"open": Decimal("10"), "high": Decimal("10.6"),
                    "low": Decimal("9.4"), "close": Decimal("10.5"),
                    "volume": 1000} for c in codes},
        bar_time=datetime(2025, 6, 17, 15, 0))


def _signal(code):
    return SignalEvent(strategy_id=1, stock_code=code, signal_name="sig",
                       signal_type=SignalType.OPEN,
                       bar_time=datetime(2025, 6, 17, 15, 0))


def _run(port, ctx, codes):
    ctx.get_signal = lambda bar, signal_cache=None: [_signal(c) for c in codes]
    return port.on_bar(_bar(*codes))


def test_rank_disabled_keeps_original_path():
    ind = _ind({A: (I1, 30, 80.0), B: (I2, 5, 50.0)})
    port, ctx = _mk(max_holdings=1, rank=False, ind=ind)
    orders = _run(port, ctx, [A, B])
    # 原路径：max_holdings 只数已成交持仓（同 bar 两只都会过闸），且不做行业排序
    assert [o.stock_code for o in orders] == [A, B]


def test_rank_prefers_industry_lead_days():
    ind = _ind({A: (I1, 30, 80.0), B: (I2, 5, 50.0)})
    port, ctx = _mk(max_holdings=1, rank=True, ind=ind)
    orders = _run(port, ctx, [B, A])   # 顺序故意反过来
    assert len(orders) == 1
    assert orders[0].stock_code == A


def test_same_industry_cap_limits_new_opens():
    ind = _ind({A: (I1, 30, 80.0), B: (I1, 20, 70.0), C: (I1, 10, 60.0)})
    port, ctx = _mk(max_holdings=5, rank=True, cap=2, ind=ind)
    rec = DecisionRecorder()
    port.set_recorder(rec)
    orders = _run(port, ctx, [A, B, C])
    assert len(orders) == 2
    events = rec.drain()
    assert any(e.gate == "industry_cap" and e.stock_code == C for e in events)


def test_same_industry_cap_counts_existing_holdings():
    ind = _ind({A: (I1, 30, 80.0), B: (I1, 20, 70.0), C: (I1, 10, 60.0)})
    port, ctx = _mk(max_holdings=5, rank=True, cap=2, ind=ind)
    pos = Position(A)
    pos.apply_trade(TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=A, trade_type=TradeType.BUY,
        price=Decimal("10"), quantity=1000, amount=Decimal("10000"),
        commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=datetime(2025, 6, 16, 9, 30)))
    ctx.positions[A] = pos
    orders = _run(port, ctx, [B, C])
    assert [o.stock_code for o in orders] == [B]


def test_missing_industry_falls_back_without_crash():
    """行业轴可用但个股未映射 → 不排序、不占同行业额度，按原顺序出单。"""
    ind = IndustryTracker({}, {})
    port, ctx = _mk(max_holdings=2, rank=True, cap=2, ind=ind)
    orders = _run(port, ctx, [A, B])
    assert len(orders) == 2


def test_no_tracker_keeps_original_path():
    port, ctx = _mk(max_holdings=2, rank=True, cap=2, ind=None)
    orders = _run(port, ctx, [A, B])
    assert len(orders) == 2
