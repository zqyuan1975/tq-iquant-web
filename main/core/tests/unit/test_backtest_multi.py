# -*- coding: utf-8 -*-
"""N1 多组合共享现金回测（BacktestEngine.run_multi）。

两个组合各自保留 Account，但资金审批用「Σcash − Σ在途BUY」的共享可用额；
成交时跨组合转账保持 Σ 现金守恒。默认单组合路径不调用本方法，故既有行为不变。
"""
from datetime import datetime
from decimal import Decimal

import polars as pl

from core.engine.backtest_engine import BacktestEngine
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from tq_iquant_shared.constants import SignalType

CODE = "000001.SZ"


def _klines(code=CODE):
    df = pl.DataFrame({
        "datetime": [datetime(2026, 7, 29), datetime(2026, 7, 30),
                     datetime(2026, 7, 31)],
        "Open": [Decimal("10"), Decimal("10"), Decimal("10")],
        "High": [Decimal("10.3"), Decimal("10.3"), Decimal("10.3")],
        "Low": [Decimal("9.9"), Decimal("9.9"), Decimal("9.9")],
        "Close": [Decimal("10.2"), Decimal("10.1"), Decimal("10.1")],
        "Volume": [100000, 100000, 100000],
    })
    return {code: {"1d": df}}


def _portfolio(pid, sid, capital, ratio="1", fixed=False, cash=None):
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.5"), daily_loss_limit=Decimal("0.5"))
    port = Portfolio(
        portfolio_id=pid, initial_capital=Decimal(capital), risk_manager=pm,
        position_sizing=("fixed" if fixed else "dynamic"))
    if cash is not None:
        port.account.cash = Decimal(cash)
    ctx = StrategyContext(
        strategy_id=sid, period="1d",
        capital_ratio=Decimal(ratio), max_positions=5,
        single_open_ratio=Decimal("1"),      # 单票开满策略资金（便于算整手股数）
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.5"), take_profit_ratio=Decimal("0.9"),
        trailing_stop_ratio=Decimal("0"))
    port.strategies.append(ctx)
    return port


def _cache(strategy_ids, code=CODE):
    out = {}
    for sid in strategy_ids:
        out[(sid, code, datetime(2026, 7, 29))] = [
            {"name": "open_sig", "value": 1}]
        out[(sid, code, datetime(2026, 7, 30))] = [
            {"name": "open_sig", "value": -1}]
        out[(sid, code, datetime(2026, 7, 31))] = [
            {"name": "open_sig", "value": -1}]
    return out


def test_each_portfolio_buys_with_own_virtual_cash():
    """多组合在同一条时间轴上各自成交：虚拟现金独立，互不划转（0010 §5.1）。"""
    p1 = _portfolio(1, 1, "10000")
    p2 = _portfolio(2, 2, "10000")
    klines = _klines()
    result = BacktestEngine().run_multi(
        [p1, p2], klines=klines, signal_cache=_cache([1, 2]))

    for pid in (1, 2):
        trades = result[pid]["trades"]
        assert len(trades) == 1, f"组合 {pid} 应有 1 笔成交"
        first = trades[0]
        # 10000 / 10 元，整手；同一组合不允许超买
        assert first.quantity % 100 == 0
        assert first.quantity >= 800
        assert first.price == Decimal("10")
    # 各组合成交后现金独立下降（互不补充）
    assert p1.account.cash < Decimal("3000")
    assert p2.account.cash < Decimal("3000")


def test_insufficient_own_cash_skips_order_and_records_event():
    """同一组合两笔买单抢同一份现金：先到者整笔成交，后到者低于 80% 门槛跳过。"""
    p1 = _portfolio(1, 1, "10000")
    klines = _klines()
    klines.update(_klines("600000.SH"))
    cache = _cache([1])
    for t in (datetime(2026, 7, 29), datetime(2026, 7, 30),
              datetime(2026, 7, 31)):
        cache[(1, "600000.SH", t)] = [{"name": "open_sig", "value": 1}]
    result = BacktestEngine().run_multi(
        [p1], klines=klines, signal_cache=cache)

    assert len(result[1]["trades"]) == 1
    decisions = result.get("_decisions") or []
    skips = [d for d in decisions if d.get("gate") == "insufficient_cash"]
    assert skips, "资金不足必须留下 insufficient_cash 决策事件"
    assert skips[0]["param_name"] == "available_cash"
    assert skips[0]["portfolio_id"] == 1
    assert skips[0]["final_qty"] == 0


def test_run_multi_returns_snapshots_and_evaluations_per_portfolio():
    """逐组合结果形状与 run() 同构（含 evaluations，供 service 直接落库）。"""
    p1 = _portfolio(1, 1, "10000")
    p1.strategies[0].formula_signals = []
    p1.account.cash = Decimal("10000")
    klines = _klines()
    result = BacktestEngine().run_multi(
        [p1], klines=klines, signal_cache={})
    res = result[1]
    assert len(res["snapshots"]) == 3
    assert isinstance(res["evaluations"], dict)
    assert "total_return" in res["evaluations"]


# ---------------- 0021 E2：会话池动态额度 ----------------
def _pool_case():
    """组合 1 闲置 10 万无信号；组合 2 初始 1 万但现金仅 7 千，信号买 900 股。"""
    p1 = _portfolio(1, 1, "100000", fixed=True)
    p1.strategies[0].formula_signals = []
    p2 = _portfolio(2, 2, "10000", fixed=True, cash="7000")
    cache = _cache([2])
    return p1, p2, cache


def test_capital_pool_disabled_matches_legacy_independent_cash():
    """enabled=False 与不传 plan 逐位一致（旧结果不变）。"""
    p1, p2, cache = _pool_case()
    base = BacktestEngine().run_multi(
        [p1, p2], klines=_klines(), signal_cache=cache)
    q1, q2, cache2 = _pool_case()
    off = BacktestEngine().run_multi(
        [q1, q2], klines=_klines(), signal_cache=cache2,
        capital_pool_plan={"enabled": False})

    for pid in (1, 2):
        assert base[pid]["trades"] == off[pid]["trades"]
        assert base[pid]["snapshots"] == off[pid]["snapshots"]
        assert "capital_pool" not in base[pid]
        assert "capital_pool" not in off[pid]


def test_capital_pool_borrows_idle_cash_and_keeps_cash_non_negative():
    """组合 2 自有现金不足，从组合 1 闲置现金拆借；Σ 现金守恒且无负现金。"""
    p1, p2, cache = _pool_case()
    cash_before = p1.account.cash + p2.account.cash
    result = BacktestEngine().run_multi(
        [p1, p2], klines=_klines(), signal_cache=cache,
        capital_pool_plan={"enabled": True, "max_share": "1.0"})

    trades = result[2]["trades"]
    assert len(trades) == 1
    assert trades[0].quantity >= 900
    assert p1.account.cash < Decimal("100000"), "出借方现金应减少"
    assert p1.account.cash >= 0
    assert p2.account.cash >= 0

    # Σ 现金 = 初始 Σ 现金 − 成交金额（费用另计，用成交额近似上界）
    spent = trades[0].price * trades[0].quantity
    assert p1.account.cash + p2.account.cash <= cash_before - spent + Decimal("100")

    pool = result[2]["capital_pool"]
    assert pool["version"] == "capital-pool-e2"
    assert Decimal(pool["borrowed_total"]) > 0
    assert Decimal(result[1]["capital_pool"]["lent_total"]) > 0
    snap_30 = next(s for s in result[2]["snapshots"]
                   if str(s["snap_date"]).startswith("2026-07-30"))
    assert snap_30["capital_pool"]["borrowed"] != "0"
    borrow_events = [d for d in result["_decisions"]
                     if d.get("gate") == "capital_pool_borrow"]
    assert borrow_events and borrow_events[0]["portfolio_id"] == 2


def test_capital_pool_increases_deployable_cash_vs_independent():
    """同一需求下，动态池能成交而独立现金因不足被拒（可复现对比）。"""
    p1, p2, cache = _pool_case()
    independent = BacktestEngine().run_multi(
        [p1, p2], klines=_klines(), signal_cache=cache)
    assert independent[2]["trades"] == []

    q1, q2, cache2 = _pool_case()
    pooled = BacktestEngine().run_multi(
        [q1, q2], klines=_klines(), signal_cache=cache2,
        capital_pool_plan={"enabled": True, "max_share": "1.0"})
    assert len(pooled[2]["trades"]) == 1
    assert q1.account.cash + q2.account.cash >= 0

