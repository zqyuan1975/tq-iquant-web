"""层 1 容量模型：参与率预算 / 5 日累计 / sqrt 冲击 / 残量顺延。"""
from datetime import datetime
from decimal import Decimal

import polars as pl

from core.engine.backtest_engine import BacktestEngine
from core.engine.capacity import CapacityBudget, capacity_model_version
from core.engine.event import OrderEvent
from core.engine.execution_engine import SimulatedDispatcher
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from tq_iquant_shared.constants import SignalType, TradeType

STOCK = "000001.SZ"


def _budget(rows, **kwargs):
    df = pl.DataFrame({
        "datetime": [r[0] for r in rows],
        "Volume": [r[1] for r in rows],
    })
    return CapacityBudget({STOCK: {"1d": df}}, **kwargs)


def _order(quantity, bar_time):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=STOCK,
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=quantity, bar_time=bar_time,
    )


def test_small_cap_daily_participation():
    """中小盘单日参与率 0.5%：100 万股 bar 只能成交 5000 股。"""
    t = datetime(2026, 1, 5)
    budget = _budget([(t, 1_000_000)])
    allowed, why = budget.allow(STOCK, t, 1_000_000, Decimal("10"), 100_000, lot=100)
    assert allowed == 5_000
    assert why in ("bar", "day")


def test_large_cap_daily_participation():
    """超大市值（成交额 ≥ 50 亿）单日参与率 1%。"""
    t = datetime(2026, 1, 5)
    volume = 60_000_000
    budget = _budget([(t, volume)])
    allowed, why = budget.allow(STOCK, t, volume, Decimal("100"), 2_000_000, lot=100)
    assert allowed == 600_000
    assert why in ("bar", "day")


def test_same_bar_aggregation_across_strategies():
    """同一 bar 同票跨策略共享预算：第二笔订单只剩剩余额度。"""
    t = datetime(2026, 1, 5)
    budget = _budget([(t, 1_000_000)])
    first, _ = budget.allow(STOCK, t, 1_000_000, Decimal("10"), 100_000)
    assert first == 5_000
    budget.consume(STOCK, t, first)
    second, _ = budget.allow(STOCK, t, 1_000_000, Decimal("10"), 100_000)
    assert second == 0


def test_five_day_cumulative_participation():
    """5 日累计 1.5%：前 4 日已用 74% 时，第 5 日仅剩 1000 股。"""
    days = [datetime(2026, 1, d) for d in (5, 6, 7, 8, 9, 12)]
    rows = [(d, 1_000_000) for d in days]
    budget = _budget(rows)
    # 窗口（1/6~1/12）内先占用 74,000 股
    budget.consume(STOCK, days[1], 74_000)
    allowed, why = budget.allow(
        STOCK, days[-1], 1_000_000, Decimal("10"), 10_000, lot=1)
    assert allowed == 1_000
    assert why == "5d"


def test_capacity_model_version_tracks_k():
    """版本串：linear=v1；sqrt 带参与率与 k，k 变化即不可混比。"""
    assert capacity_model_version(
        "linear", daily_small=Decimal("0.005"), daily_large=Decimal("0.01"),
        five_day_small=Decimal("0.015"), five_day_large=Decimal("0.02"),
        impact_k=Decimal("0.10"),
    ) == "v1"
    v2 = capacity_model_version(
        "sqrt", daily_small=Decimal("0.005"), daily_large=Decimal("0.01"),
        five_day_small=Decimal("0.015"), five_day_large=Decimal("0.02"),
        impact_k=Decimal("0.10"),
    )
    assert v2.startswith("v2-sqrt-p0.5%/1%-5d1.5%/2%-k0.1")


def test_sqrt_impact_slippage_formula_and_cap():
    """sqrt 冲击 = 20bps + k×√参与率；> 5% 判不可成交（None）。"""
    dispatcher = SimulatedDispatcher(
        open_prices={STOCK: Decimal("10")},
        bar_volumes={STOCK: 1_000_000},
        slippage_impact_coef=Decimal("0.10"),
        impact_model="sqrt",
    )
    order = _order(10_000, datetime(2026, 1, 5))  # 参与率 1% → 0.2% + 1% = 1.2%
    trade = dispatcher.place_order(order)
    assert trade is not None
    assert trade.price == Decimal("10.12")
    assert trade.ref_price == Decimal("10")

    too_large = _order(1_000_000, datetime(2026, 1, 5))  # 参与率 100% → 10.2%
    assert dispatcher.place_order(too_large) is None


def test_linear_impact_unchanged_by_default():
    """impact_model 默认 linear：结果与旧口径一致（k×参与率）。"""
    dispatcher = SimulatedDispatcher(
        open_prices={STOCK: Decimal("10")},
        bar_volumes={STOCK: 1_000_000},
        slippage_impact_coef=Decimal("0.10"),
    )
    trade = dispatcher.place_order(_order(10_000, datetime(2026, 1, 5)))
    assert trade is not None
    assert trade.price == Decimal("10.01")


def _portfolio():
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("1000000"),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1d",
        capital_ratio=Decimal("1.0"), max_positions=1,
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.5"), take_profit_ratio=Decimal("0.9"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    # 测试用宽参与率：5% 单日 / 10% 五日，便于用少量 bar 观察切片
    port.capacity_model = "sqrt"
    port.capacity_daily_ratio_small = Decimal("0.05")
    port.capacity_5d_ratio_small = Decimal("0.10")
    port.capacity_slice_ttl_bars = 3
    return port, ctx


def _klines(volume):
    rows = [
        (datetime(2026, 7, 29), 10, 10.3, 9.9, 10.0, volume),
        (datetime(2026, 7, 30), 10.0, 10.3, 9.9, 10.0, volume),
        (datetime(2026, 7, 31), 10.0, 10.3, 9.9, 10.0, volume),
        (datetime(2026, 8, 3), 10.0, 10.3, 9.9, 10.0, volume),
        (datetime(2026, 8, 4), 10.0, 10.3, 9.9, 10.0, volume),
    ]
    df = pl.DataFrame({
        "datetime": [r[0] for r in rows],
        "Open": [Decimal(str(r[1])) for r in rows],
        "High": [Decimal(str(r[2])) for r in rows],
        "Low": [Decimal(str(r[3])) for r in rows],
        "Close": [Decimal(str(r[4])) for r in rows],
        "Volume": [r[5] for r in rows],
    })
    return {STOCK: {"1d": df}}


def test_engine_slices_order_and_carries_residual():
    """单笔 10,000 股被单日 5,000 股预算切成两笔，残量顺延到下一交易日。"""
    port, _ctx = _portfolio()
    klines = _klines(100_000)  # 5% × 10 万股 = 5,000 股/日
    cache = {(1, STOCK, datetime(2026, 7, 29)): [
        {"name": "open_sig", "value": 1}]}

    result = BacktestEngine().run(port, klines=klines, signal_cache=cache)

    buys = [t for t in result["trades"] if t.trade_type == TradeType.BUY]
    assert sum(t.quantity for t in buys) == 10_000
    assert [t.quantity for t in buys] == [5_000, 5_000]
    # 第一笔在 7/30 成交，残量在 7/31 成交
    assert [t.trade_time.date().isoformat() for t in buys] == [
        "2026-07-30", "2026-07-31"]
    assert any(d["gate"] == "capacity_slice" for d in result["decisions"])


def test_engine_expires_after_ttl_without_whole_reject():
    """整笔都放不下时顺延，达到 TTL 才作废（不是第一根 bar 就整单拒绝）。"""
    port, _ctx = _portfolio()
    # 0.5% 未覆盖，改用默认 0.5%：5% → 50 股（<1 手）→ 每次都放不下
    port.capacity_daily_ratio_small = Decimal("0.005")
    klines = _klines(10_000)  # 0.5% × 1 万股 = 50 股
    cache = {(1, STOCK, datetime(2026, 7, 29)): [
        {"name": "open_sig", "value": 1}]}

    result = BacktestEngine().run(port, klines=klines, signal_cache=cache)

    assert result["trades"] == []
    gates = [d["gate"] for d in result["decisions"]]
    assert "capacity_slice" in gates
    assert "capacity_slice_expired" in gates
