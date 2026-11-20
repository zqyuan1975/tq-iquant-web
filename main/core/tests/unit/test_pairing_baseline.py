# -*- coding: utf-8 -*-
"""E6 基线批跑器：四种资金口径齐全、参数哈希稳定、报告可复算。"""
from datetime import datetime
from decimal import Decimal

import polars as pl

from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.services.pairing_baseline import (
    BASELINE_KINDS, BaselineError, params_hash, recompute_totals, run_baselines,
)
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
        portfolio_id=pid, initial_capital=Decimal(capital), risk_manager=pm,
        position_sizing="dynamic")
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


def _factory(cap1="30000", cap2="10000"):
    def build():
        return [_portfolio(1, 1, cap1), _portfolio(2, 2, cap2)]
    return build


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


def test_all_four_baselines_present_and_members_counted_once():
    report = run_baselines(_factory(), _klines(), signal_cache=_cache())

    assert set(report["baselines"].keys()) == set(BASELINE_KINDS)
    assert report["version"] == "pairing-baseline-e6"
    for kind, payload in report["baselines"].items():
        assert len(payload["members"]) == 2, kind
        assert [m["portfolio_id"] for m in payload["members"]] == [1, 2]
        assert payload["total_capital"] == "40000", kind


def test_static_equal_and_ratio_split_capital():
    report = run_baselines(
        _factory(), _klines(), signal_cache=_cache(),
        weights={1: Decimal("3"), 2: Decimal("1")})

    equal = report["baselines"]["static_equal"]
    assert [m["capital"] for m in equal["members"]] == ["20000", "20000"]

    ratio = report["baselines"]["static_ratio"]
    assert [m["capital"] for m in ratio["members"]] == ["30000", "10000"]

    standalone = report["baselines"]["standalone"]
    assert [m["capital"] for m in standalone["members"]] == ["30000", "10000"]

    dynamic = report["baselines"]["dynamic_pool"]
    assert [m["capital"] for m in dynamic["members"]] == ["30000", "10000"]
    assert all(m["trades"] >= 1 for m in dynamic["members"])


def test_params_hash_is_stable_and_report_recomputable():
    first = run_baselines(_factory(), _klines(), signal_cache=_cache())
    second = run_baselines(_factory(), _klines(), signal_cache=_cache())

    assert first["params_hash"] == second["params_hash"]
    assert len(first["params_hash"]) == 64
    assert first["params_hash"] == params_hash(first["params"])

    # 同一份成员明细复算总额，与报告一致（报告可复算）
    recomputed = recompute_totals(first)
    for kind, payload in first["baselines"].items():
        assert recomputed[kind]["total_capital"] == payload["total_capital"]
        assert recomputed[kind]["total_equity"] == payload["total_equity"]
        assert recomputed[kind]["total_return"] == payload["total_return"]

    # 同参数重跑结果一致
    for kind in BASELINE_KINDS:
        assert (first["baselines"][kind]["members"]
                == second["baselines"][kind]["members"]), kind


def test_invalid_inputs_rejected():
    try:
        run_baselines(_factory(), _klines(), signal_cache=_cache(),
                      weights={1: Decimal("1")})
        raise AssertionError("缺少权重应报错")
    except BaselineError as exc:
        assert "权重" in str(exc)
    try:
        run_baselines(lambda: [], _klines(), signal_cache=_cache())
        raise AssertionError("空组合应报错")
    except BaselineError as exc:
        assert "为空" in str(exc)
