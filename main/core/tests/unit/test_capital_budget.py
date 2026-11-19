"""策略额度接入 Portfolio（0021 B2）单测。

口径：``StrategyContext.capital_budget`` 默认 None → 行为逐位一致；
显式注入后，策略总市值（持仓市值 + 本单）不得超过额度：
超出缩量、剩余不足一手拦截、不跨策略借额度。
"""
from datetime import datetime
from decimal import Decimal

from tq_iquant_shared.constants import SignalType

from core.engine.decision import DecisionRecorder
from core.engine.event import BarEvent
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext

D = Decimal
BAR_TIME = datetime(2026, 7, 29)


def _bar(codes, price="10"):
    return BarEvent(
        stocks={
            code: {
                "open": D(price), "high": D(price), "low": D(price),
                "close": D(price), "volume": 100000,
            }
            for code in codes
        },
        bar_time=BAR_TIME,
        period="1d",
    )


def _risk():
    return StrategyRiskManager(
        stop_loss_ratio=D("0.5"), take_profit_ratio=D("0.9"),
        trailing_stop_ratio=D("0"),
    )


def _ctx(strategy_id, *, capital_ratio="0.6", budget=None, **kwargs):
    ctx = StrategyContext(
        strategy_id=strategy_id, period="1d",
        capital_ratio=D(capital_ratio), max_positions=5,
        single_open_ratio=D("1.0"),
        capital_budget=None if budget is None else D(budget),
        **kwargs,
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = _risk()
    return ctx


def _portfolio(*contexts):
    pm = PortfolioRiskManager(
        max_drawdown=D("0.9"), daily_loss_limit=D("0.9"))
    recorder = DecisionRecorder()
    port = Portfolio(
        portfolio_id=1, initial_capital=D("100000"),
        risk_manager=pm, recorder=recorder,
    )
    port.strategies.extend(contexts)
    return port, recorder


def _cache(pairs):
    return {
        (sid, code, BAR_TIME): [{"name": "open_sig", "value": 1}]
        for sid, code in pairs
    }


# ---------------- 默认关闭：逐位一致 ----------------
def test_budget_none_keeps_original_quantity():
    ctx = _ctx(1)
    port, recorder = _portfolio(ctx)

    orders = port.on_bar(_bar(["000001.SZ"]), signal_cache=_cache([(1, "000001.SZ")]))

    assert len(orders) == 1
    # 0.6 × 100000 × 1.0 / 10 = 6000 股（与未接入额度时一致）
    assert orders[0].quantity == 6000
    assert not [e for e in recorder.drain() if e.gate.startswith("capital_quota")]


# ---------------- 开启：超出缩量 ----------------
def test_budget_shrinks_order_to_entitlement():
    ctx = _ctx(1, budget="20000")
    port, recorder = _portfolio(ctx)

    orders = port.on_bar(_bar(["000001.SZ"]), signal_cache=_cache([(1, "000001.SZ")]))

    assert len(orders) == 1
    assert orders[0].quantity == 2000  # 20000 / 10
    events = [e for e in recorder.drain() if e.gate.startswith("capital_quota")]
    assert len(events) == 1
    assert events[0].action == "shrink"
    assert events[0].requested_qty == 6000
    assert events[0].actual_value == 2000.0


# ---------------- 开启：额度用尽 / 不足一手 ----------------
def test_budget_exhausted_blocks_new_position():
    ctx = _ctx(1, budget="10000")
    port, recorder = _portfolio(ctx)
    pos = Position("600000.SH")
    pos.buy(1000, D("10"), datetime(2026, 7, 28))
    ctx.positions["600000.SH"] = pos

    orders = port.on_bar(
        _bar(["000001.SZ", "600000.SH"]),
        signal_cache=_cache([(1, "000001.SZ")]),
    )

    assert orders == []
    events = [e for e in recorder.drain() if e.gate.startswith("capital_quota")]
    assert len(events) == 1
    assert events[0].action == "block"


def test_budget_below_one_lot_is_blocked():
    ctx = _ctx(1, budget="500")
    port, recorder = _portfolio(ctx)

    orders = port.on_bar(_bar(["000001.SZ"]), signal_cache=_cache([(1, "000001.SZ")]))

    assert orders == []
    events = [e for e in recorder.drain() if e.gate.startswith("capital_quota")]
    assert len(events) == 1
    assert events[0].gate.startswith("capital_quota")


# ---------------- 主从：各算各的额度 ----------------
def test_master_and_slave_use_their_own_budget():
    master = _ctx(1, capital_ratio="0.6", budget="30000")
    slave = _ctx(
        2, capital_ratio="0.4", budget="10000",
        role="slave", master_strategy_id=1,
    )
    port, _ = _portfolio(master, slave)
    # 从策略只能买主策略当前持有的同一只股票
    pos = Position("000002.SZ")
    pos.buy(1000, D("10"), datetime(2026, 7, 28))
    master.positions["000002.SZ"] = pos

    orders = port.on_bar(
        _bar(["000001.SZ", "000002.SZ"]),
        signal_cache=_cache([(1, "000001.SZ"), (2, "000002.SZ")]),
    )

    by_strategy = {o.strategy_id: o.quantity for o in orders}
    # 组合权益 = 100000 现金 + 10000 持仓 = 110000
    # master：0.6×110000=66000 → 6600 股，但额度 30000−10000=20000 → 2000 股
    # slave：0.4×110000=44000 → 4400 股，但只有自己的 10000 额度 → 1000 股
    assert by_strategy == {1: 2000, 2: 1000}
