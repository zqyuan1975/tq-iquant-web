"""资金池 C5：收益率曲线与现金出口回测建模（影子，收益单列）。"""
from datetime import date, datetime, timedelta
from decimal import Decimal

import polars as pl
import pytest

from core.engine.backtest_engine import BacktestEngine
from core.engine.cash_outlet_model import (
    CASH_OUTLET_MODEL_VERSION,
    CashOutletPlan,
    CashOutletPlanError,
    YieldCurve,
    accrue,
    add_business_days,
    build_cash_yield_contribution,
    is_business_day,
)
from core.engine.event import SignalType
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext


PLAN = {
    "outlet_code": "GC001", "liquidity": "t0", "settlement_days": 0,
    "annual_yield": "0.0365", "min_amount": "1000",
}


def test_accrue_formula():
    # 100000 × 3.65% × 1 / 365 = 10
    assert accrue(Decimal("100000"), Decimal("0.0365"), 1) == Decimal("10")
    assert accrue(Decimal("100000"), Decimal("0.0365"), 30) == Decimal("300")
    assert accrue(Decimal("0"), Decimal("0.0365"), 10) == Decimal("0")
    assert accrue(Decimal("100"), Decimal("0.0365"), 0) == Decimal("0")


def test_business_day_helpers():
    assert is_business_day(date(2026, 11, 20))      # 周五
    assert not is_business_day(date(2026, 11, 21))  # 周六
    assert add_business_days(date(2026, 11, 20), 0) == date(2026, 11, 20)
    assert add_business_days(date(2026, 11, 20), 1) == date(2026, 11, 23)
    assert add_business_days(date(2026, 11, 20), 3) == date(2026, 11, 25)


def test_yield_curve_is_stepwise_and_never_extrapolates():
    curve = YieldCurve.from_raw([
        ("2026-01-01", "0.02"), ("2026-06-01", "0.03"),
    ])
    assert curve.rate_on(date(2025, 12, 31)) is None
    assert curve.rate_on(date(2026, 1, 1)) == Decimal("0.02")
    assert curve.rate_on(date(2026, 5, 31)) == Decimal("0.02")
    assert curve.rate_on(date(2026, 6, 1)) == Decimal("0.03")


def test_plan_validation():
    with pytest.raises(CashOutletPlanError):
        CashOutletPlan.from_dict({"liquidity": "t0", "annual_yield": "0.02"})
    with pytest.raises(CashOutletPlanError):
        CashOutletPlan.from_dict({"outlet_code": "X", "liquidity": "t7",
                                  "annual_yield": "0.02"})
    with pytest.raises(CashOutletPlanError):
        CashOutletPlan.from_dict({"outlet_code": "X", "liquidity": "t1",
                                  "settlement_days": 8,
                                  "annual_yield": "0.02"})
    with pytest.raises(CashOutletPlanError):
        CashOutletPlan.from_dict({"outlet_code": "X", "liquidity": "term",
                                  "term_days": 0, "annual_yield": "0.02"})
    with pytest.raises(CashOutletPlanError):
        CashOutletPlan.from_dict({"outlet_code": "X", "liquidity": "t0",
                                  "min_amount": "1000", "max_amount": "100",
                                  "annual_yield": "0.02"})
    with pytest.raises(CashOutletPlanError):
        CashOutletPlan.from_dict({"outlet_code": "X", "liquidity": "t0"})


def test_availability_by_liquidity():
    plan = CashOutletPlan.from_dict(PLAN)
    friday = date(2026, 11, 20)
    assert plan.available_after(friday) == friday

    t1 = CashOutletPlan.from_dict({**PLAN, "liquidity": "t1",
                                   "settlement_days": 1})
    assert t1.available_after(friday) == date(2026, 11, 23)

    term = CashOutletPlan.from_dict({**PLAN, "liquidity": "term",
                                     "term_days": 3, "settlement_days": 1})
    # 3 天到期 + 1 天结算 → 4 个工作日
    assert term.available_after(friday) == date(2026, 11, 26)


def _week(monday=date(2026, 11, 16), cash="100000"):
    """周一到周五 5 条日终快照。"""
    return [
        {"snap_date": monday + timedelta(days=i), "cash": Decimal(cash),
         "total_value": Decimal("1000000"),
         "market_value": Decimal("1000000") - Decimal(cash)}
        for i in range(5)
    ]


def test_contribution_daily_accrual_and_series():
    data = build_cash_yield_contribution(_week(), PLAN, include_series=True)
    assert data["version"] == CASH_OUTLET_MODEL_VERSION
    assert data["enabled"] is True
    assert data["days_accrued"] == 4          # 周二至周五
    assert data["total_interest"] == "40"     # 10/天 × 4
    assert len(data["series"]) == 4
    assert data["series"][0]["interest"] == "10"


def test_min_amount_and_max_amount():
    low = build_cash_yield_contribution(_week(cash="500"), PLAN)
    assert low["days_accrued"] == 0
    assert low["total_interest"] == "0"

    capped = build_cash_yield_contribution(
        _week(cash="1000000"), {**PLAN, "max_amount": "100000"})
    assert capped["total_interest"] == "40"   # 封顶 10 万，仍 10/天


def test_weekend_skipped():
    friday = date(2026, 11, 20)
    rows = [
        {"snap_date": friday, "cash": Decimal("100000")},
        {"snap_date": friday + timedelta(days=1), "cash": Decimal("100000")},
        {"snap_date": friday + timedelta(days=2), "cash": Decimal("100000")},
        {"snap_date": friday + timedelta(days=3), "cash": Decimal("100000")},
    ]
    data = build_cash_yield_contribution(rows, PLAN)
    assert data["days_accrued"] == 1          # 只有周一那一段计息
    assert data["total_interest"] == "10"


def test_curve_change_uses_previous_day_rate():
    rows = _week(monday=date(2026, 5, 25))
    curve = [("2026-01-01", "0.01"), ("2026-05-27", "0.02")]
    data = build_cash_yield_contribution(
        rows, {**PLAN, "yield_curve": curve})
    # 5-25 起 1% 用 2 天，5-27 起 2% 用 2 天：
    # 100000×1%/365×2 ≈ 5.479 + 100000×2%/365×2 ≈ 10.959
    assert data["days_accrued"] == 4
    assert Decimal(data["total_interest"]) > Decimal("16.4")
    assert Decimal(data["total_interest"]) < Decimal("16.5")


# ---------------------------------------------------------------------------
# 回测接入：只多一个单列字段，交易链路逐位不变
# ---------------------------------------------------------------------------

STOCK = "000001.SZ"


def _klines(rows):
    return {STOCK: {"1d": pl.DataFrame({
        "datetime": [r[0] for r in rows],
        "Open": [r[1] for r in rows],
        "High": [r[2] for r in rows],
        "Low": [r[3] for r in rows],
        "Close": [r[4] for r in rows],
        "Volume": [r[5] for r in rows],
    })}}


def _run(*, plan=None):
    rows = []
    for i in range(12):
        d = datetime(2026, 11, 16) + timedelta(days=i)
        price = Decimal("10.00") + Decimal(i % 3) / Decimal("10")
        rows.append((d, price, price + Decimal("0.2"),
                     price - Decimal("0.2"), price, 100000 + i * 100))
    cache = {
        (1, STOCK, datetime(2026, 11, 16) + timedelta(days=i)): [
            {"name": "open_sig", "value": -1},
            {"name": "close_sig", "value": -1},
        ]
        for i in range(12)
    }
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("1000000"),
                     risk_manager=pm)
    ctx = StrategyContext(strategy_id=1, period="1d",
                          capital_ratio=Decimal("0.1"), max_positions=1)
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
        {"signal_name": "close_sig", "signal_type": SignalType.CLOSE,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.5"), take_profit_ratio=Decimal("0.5"),
        trailing_stop_ratio=Decimal("0"))
    port.strategies.append(ctx)
    return BacktestEngine().run(port, klines=_klines(rows),
                                signal_cache=cache, cash_outlet_plan=plan)


def test_engine_default_path_unchanged_and_yield_separate():
    base = _run()
    with_plan = _run(plan=PLAN)
    assert base["cash_yield_contribution"] is None
    assert with_plan["trades"] == base["trades"]
    assert with_plan["snapshots"] == base["snapshots"]
    assert with_plan["evaluations"] == base["evaluations"]

    contribution = with_plan["cash_yield_contribution"]
    assert contribution["enabled"] is True
    assert Decimal(contribution["total_interest"]) > 0
    # 利息不进净值：快照 total_value 与未启用时完全相同
    assert [s["total_value"] for s in with_plan["snapshots"]] == \
           [s["total_value"] for s in base["snapshots"]]


def test_engine_invalid_plan_degrades_without_failing():
    result = _run(plan={"outlet_code": "BAD", "liquidity": "t9"})
    contribution = result["cash_yield_contribution"]
    assert contribution["enabled"] is False
    assert contribution["status"] == "invalid"
    assert result["trades"] is not None
