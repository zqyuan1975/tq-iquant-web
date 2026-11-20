# -*- coding: utf-8 -*-
"""E7 默认路径回归：会话池关闭时，多组合回测与旧行为逐位一致。"""
import hashlib
import json
from datetime import datetime
from decimal import Decimal

import polars as pl

from core.engine.backtest_engine import BacktestEngine
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.services.backtest_service import aggregate_run_group
from tq_iquant_shared.constants import SignalType

CODE = "000001.SZ"


def _klines():
    df = pl.DataFrame({
        "datetime": [datetime(2026, 7, 29), datetime(2026, 7, 30),
                     datetime(2026, 7, 31)],
        "Open": [Decimal("10"), Decimal("10"), Decimal("10")],
        "High": [Decimal("10.3"), Decimal("10.3"), Decimal("10.3")],
        "Low": [Decimal("9.9"), Decimal("9.9"), Decimal("9.9")],
        "Close": [Decimal("10.2"), Decimal("10.1"), Decimal("10.1")],
        "Volume": [100000, 100000, 100000],
    })
    return {CODE: {"1d": df}}


def _portfolio(pid, sid, capital):
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.5"), daily_loss_limit=Decimal("0.5"))
    port = Portfolio(
        portfolio_id=pid, initial_capital=Decimal(capital),
        risk_manager=pm, position_sizing="dynamic")
    ctx = StrategyContext(
        strategy_id=sid, period="1d",
        capital_ratio=Decimal("1"), max_positions=5,
        single_open_ratio=Decimal("1"))
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.5"), take_profit_ratio=Decimal("0.9"),
        trailing_stop_ratio=Decimal("0"))
    port.strategies.append(ctx)
    return port


def _cache():
    out = {}
    for sid in (1, 2):
        out[(sid, CODE, datetime(2026, 7, 29))] = [
            {"name": "open_sig", "value": 1}]
        out[(sid, CODE, datetime(2026, 7, 30))] = [
            {"name": "open_sig", "value": -1}]
        out[(sid, CODE, datetime(2026, 7, 31))] = [
            {"name": "open_sig", "value": -1}]
    return out


def _run(plan=None, omit=False):
    portfolios = [_portfolio(1, 1, "30000"), _portfolio(2, 2, "10000")]
    engine = BacktestEngine()
    kwargs = dict(klines=_klines(), signal_cache=_cache())
    if omit:
        return engine.run_multi(portfolios, **kwargs)
    return engine.run_multi(portfolios, capital_pool_plan=plan, **kwargs)


def _flat(obj):
    """交易/决策对象 → 稳定 dict（不依赖具体字段名）。"""
    if hasattr(obj, "__dict__"):
        return {k: str(v) for k, v in vars(obj).items()}
    return str(obj)


def _normalize(results):
    out = {}
    for pid, res in results.items():
        if not isinstance(pid, int):
            continue
        out[str(pid)] = {
            "trades": [_flat(t) for t in res.get("trades") or []],
            "snapshots": [
                {k: str(v) for k, v in snap.items()}
                for snap in res.get("snapshots") or []
            ],
            "evaluations": res.get("evaluations"),
            "strategy_evaluations": res.get("strategy_evaluations"),
            "pending_order_count": res.get("pending_order_count"),
            "has_capital_pool": "capital_pool" in res,
        }
    out["decisions"] = results.get("_decisions") or []
    out["execution_stats"] = results.get("execution_stats")
    return out


def _digest(payload) -> str:
    canonical = json.dumps(
        payload, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def test_disabled_plan_matches_absent_plan_bitwise():
    absent = _run(omit=True)
    disabled = _run(plan={"enabled": False})

    assert _digest(_normalize(absent)) == _digest(_normalize(disabled))
    for pid in (1, 2):
        assert "capital_pool" not in absent[pid]
        assert "capital_pool" not in disabled[pid]
        assert absent[pid]["trades"], "默认路径应仍能成交"
    gates = {ev.get("gate") for ev in (absent.get("_decisions") or [])}
    assert "capital_pool_borrow" not in gates


def test_disabled_plan_aggregate_hash_unchanged():
    absent = _run(omit=True)
    disabled = _run(plan={"enabled": False})
    initial_total = Decimal("40000")

    def aggregate_hash(results):
        per = {pid: res for pid, res in results.items()
               if isinstance(pid, int)}
        return _digest(aggregate_run_group(per, initial_total))

    assert aggregate_hash(absent) == aggregate_hash(disabled)


def test_single_run_path_has_no_pool_fields():
    result = BacktestEngine().run(
        _portfolio(1, 1, "10000"), klines=_klines(), signal_cache=_cache())
    assert "capital_pool" not in result
    assert result.get("trades")
