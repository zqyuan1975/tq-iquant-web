# -*- coding: utf-8 -*-
"""回测引擎门控接入：空头拦开仓 / 多头放行 / 日快照写 gate_state+gate_strength。

复用 test_backtest_engine.py 的最小 harness（3 根日线 + signal_cache）。
"""
from datetime import date, datetime
from decimal import Decimal

import polars as pl

from core.engine.backtest_engine import BacktestEngine
from core.engine.market_gate import MarketGateTracker
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from tq_iquant_shared.constants import SignalType

STOCK = "000001.SZ"


def _klines():
    df = pl.DataFrame({
        "datetime": [datetime(2026, 7, 29), datetime(2026, 7, 30), datetime(2026, 7, 31)],
        "Open": [Decimal("10"), Decimal("10.2"), Decimal("9.0")],
        "High": [Decimal("10.3"), Decimal("10.5"), Decimal("9.2")],
        "Low": [Decimal("9.9"), Decimal("8.9"), Decimal("8.8")],
        "Close": [Decimal("10.2"), Decimal("9.0"), Decimal("9.1")],
        "Volume": [1000, 1000, 1000],
    })
    return {STOCK: {"1d": df}}


def _portfolio():
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx = StrategyContext(strategy_id=1, period="1d",
                          capital_ratio=Decimal("0.6"), max_positions=5)
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN, "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port


def _cache():
    return {
        (1, STOCK, datetime(2026, 7, 29)): [{"name": "open_sig", "value": 1}],
        (1, STOCK, datetime(2026, 7, 30)): [{"name": "open_sig", "value": -1}],
        (1, STOCK, datetime(2026, 7, 31)): [{"name": "open_sig", "value": -1}],
    }


def _tracker(state):
    t = MarketGateTracker()
    t.feed_history({date(2026, 7, 28): state}, {date(2026, 7, 28): 70})
    return t


def test_gate_bear_blocks_open_and_snapshots_carry_state():
    """昨日空头 → 今日 OPEN 不开仓（0 笔成交）；快照带 gate_state=-1 / 强度。"""
    port = _portfolio()
    result = BacktestEngine().run(
        port, klines=_klines(), signal_cache=_cache(),
        gate_tracker=_tracker(-1),
    )
    assert result["trades"] == []
    snaps = result["snapshots"]
    assert len(snaps) == 3
    assert all(s["gate_state"] == -1 for s in snaps)
    assert all(s["gate_strength"] == 70 for s in snaps)
    assert any(ev.get("gate") == "market_gate" and ev.get("action") == "block"
               for ev in result["decisions"])


def test_gate_bull_allows_open():
    """昨日多头 → OPEN 正常成交（与无门控行为一致）。"""
    port = _portfolio()
    result = BacktestEngine().run(
        port, klines=_klines(), signal_cache=_cache(),
        gate_tracker=_tracker(1),
    )
    assert len(result["trades"]) >= 1
    assert result["trades"][0].stock_code == STOCK
    assert all(s["gate_state"] == 1 for s in result["snapshots"])


def test_no_gate_tracker_keeps_snapshots_clean():
    """未启用门控 → 快照不含 gate 键（旧口径逐字不变）。"""
    port = _portfolio()
    result = BacktestEngine().run(port, klines=_klines(), signal_cache=_cache())
    assert all("gate_state" not in s for s in result["snapshots"])
