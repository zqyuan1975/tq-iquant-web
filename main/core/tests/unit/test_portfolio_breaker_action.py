"""D4 熔断处置动作单元测试（Portfolio 层，回测/实盘同源）。"""
from datetime import datetime
from decimal import Decimal

from core.engine.event import BarEvent
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from tq_iquant_shared.constants import TradeType


def _setup(action, quantity=1000):
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"),
        breaker_action=action)
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(strategy_id=1, period="1d",
                          capital_ratio=Decimal("0.6"), max_positions=5)
    ctx.formula_signals = []
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.1"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    pos = Position("000001.SZ")
    pos.apply_trade(_buy(quantity))
    ctx.positions["000001.SZ"] = pos
    port.strategies.append(ctx)
    bar = BarEvent(
        stocks={"000001.SZ": {
            "open": Decimal("10"), "high": Decimal("10.5"),
            "low": Decimal("9"), "close": Decimal("10"), "volume": 10000,
        }},
        bar_time=datetime(2026, 11, 18, 14, 30),
    )
    return port, pm, bar


def _buy(quantity):
    from core.engine.event import TradeEvent
    return TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code="000001.SZ",
        trade_type=TradeType.BUY, price=Decimal("10"), quantity=quantity,
        amount=Decimal("10") * quantity, commission=Decimal("0"),
        stamp_duty=Decimal("0"),
        trade_time=datetime(2026, 11, 17, 10, 0),
    )


def test_half_action_sells_half_position():
    port, pm, bar = _setup("half")
    pm.pending_disposal = "half"
    orders = port.on_bar(bar, signal_cache={})
    sells = [o for o in orders if o.trade_type == TradeType.SELL]
    assert len(sells) == 1
    assert sells[0].quantity == 500
    assert sells[0].signal_name == "breaker_half"
    assert pm.pending_disposal is None


def test_liquidate_action_sells_all():
    port, pm, bar = _setup("liquidate")
    pm.pending_disposal = "liquidate"
    orders = port.on_bar(bar, signal_cache={})
    sells = [o for o in orders if o.trade_type == TradeType.SELL]
    assert len(sells) == 1
    assert sells[0].quantity == 1000
    assert sells[0].signal_name == "breaker_liquidate"


def test_default_halt_does_not_sell():
    port, pm, bar = _setup("halt")
    orders = port.on_bar(bar, signal_cache={})
    assert [o for o in orders if o.trade_type == TradeType.SELL] == []


def test_half_skips_when_less_than_one_lot():
    port, pm, bar = _setup("half", quantity=100)
    pm.pending_disposal = "half"
    orders = port.on_bar(bar, signal_cache={})
    assert [o for o in orders if o.trade_type == TradeType.SELL] == []


def test_invalid_action_falls_back_to_halt():
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"),
                              breaker_action="explode")
    assert pm.breaker_action == "halt"
