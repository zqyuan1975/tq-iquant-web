"""借款方资格 E1–E12（0021 B4）：只读判定与参数漂移。"""
import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import (
    Base,
    BacktestDailySnapshot,
    BacktestEvaluation,
    BacktestRecord,
    BacktestTrade,
    PortfolioStrategy,
    StockPool,
    StockPoolStock,
    Strategy,
    WalkForwardRun,
)
from core.services.backtest_service import build_params_snapshot
from core.services.pairing_eligibility import (
    E_CAPACITY_MISSING,
    E_DATA_QUALITY,
    E_DECISION_TIME_LEAK,
    E_EVALUATION_MISSING,
    E_GATE_COVERAGE_LOW,
    E_NO_BACKTEST,
    E_OOS_MISSING,
    E_PARAM_STALE,
    E_PORTFOLIO_INVALID,
    E_RISK_DEFAULTED,
    E_TRADE_SAMPLE_SMALL,
    E_WINDOW_SHORT,
    STATUS_ELIGIBLE,
    STATUS_INELIGIBLE,
    STATUS_STALE,
    STATUS_WATCH,
    evaluate_eligibility,
)
from core.services.pairing_eligibility import config_fingerprint


def _db():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed(db, *, status="completed", trading_days=800, trades=120,
          hold_days=300, bull=200, bear=100, oos_years=1, with_eval=True,
          quality=True, capacity=(), completed_at=None, pool_size=1,
          max_drawdown=Decimal("0.2")):
    pool = StockPool(code="TQCS", name="测试池")
    db.add(pool)
    db.flush()
    for i in range(pool_size):
        db.add(StockPoolStock(pool_id=pool.id, stock_code="60000%d.SH" % i))
    ps = PortfolioStrategy(
        name="组合", stock_pool_id=pool.id, initial_capital=Decimal("1000000"),
        max_holdings=10, max_drawdown=max_drawdown,
        daily_loss_limit=Decimal("0.05"))
    db.add(ps)
    db.flush()
    strategy = Strategy(portfolio_id=ps.id, name="主策略", formula_id=1,
                        period="1d", role="master")
    db.add(strategy)
    db.flush()

    rec = BacktestRecord(
        portfolio_strategy_id=ps.id, name="回测", start_date=date(2023, 1, 1),
        end_date=date(2026, 1, 1), status=status,
        completed_at=completed_at or datetime(2026, 2, 1))
    db.add(rec)
    db.flush()
    rec.params_snapshot = build_params_snapshot(
        ps, strategies=[strategy],
        stock_pool_codes={str(pool.id): sorted(
            "60000%d.SH" % i for i in range(pool_size))},
        market_gate_enabled=True)
    rec.validation_json = json.dumps({"capacity": list(capacity)})
    if quality is None:
        rec.data_quality_json = None
    elif quality is True:
        rec.data_quality_json = json.dumps(
            {"summary": {"coverage_ratio": 0.995}, "missing_stocks": []})
    else:
        rec.data_quality_json = json.dumps(
            {"summary": {"coverage_ratio": quality}, "missing_stocks": []})

    if with_eval:
        db.add(BacktestEvaluation(
            backtest_record_id=rec.id, target_type="portfolio", target_id=ps.id))
    if oos_years:
        db.add(WalkForwardRun(
            portfolio_strategy_id=ps.id, name="wf", start_date=date(2018, 1, 1),
            end_date=date(2026, 1, 1), train_years=3, test_years=oos_years,
            status="completed", completed_at=datetime(2026, 1, 20)))

    snaps = []
    day0 = date(2023, 1, 1)
    for i in range(trading_days):
        if i < bull:
            gate = 1
        elif i < bull + bear:
            gate = -1
        else:
            gate = 0
        snaps.append(BacktestDailySnapshot(
            backtest_record_id=rec.id, target_type="portfolio", target_id=ps.id,
            snap_date=day0 + timedelta(days=i),
            total_value=Decimal("1000000"), cash=Decimal("200000"),
            market_value=Decimal("800000"), gate_state=gate,
            positions_json=('[{"code":"600000.SH"}]' if i < hold_days else "[]"),
        ))
    db.bulk_save_objects(snaps)
    db.bulk_save_objects([
        BacktestTrade(
            backtest_record_id=rec.id, strategy_id=strategy.id,
            signal_name="open_sig", signal_type="OPEN", stock_code="600000.SH",
            trade_type="BUY", price=Decimal("10"), quantity=100,
            amount=Decimal("1000"), commission=Decimal("5"),
            stamp_duty=Decimal("0"), bar_time=datetime(2023, 1, 2, 15, 0))
        for _ in range(trades)
    ])
    db.commit()
    return db, ps, rec, strategy


def _codes(result):
    return {c["code"] for c in result["codes"]}


def test_eligible_when_all_checks_pass():
    db, ps, rec, _ = _seed(_db())
    result = evaluate_eligibility(db, ps.id)

    assert result["status"] == STATUS_ELIGIBLE
    assert result["codes"] == []
    assert len(result["checks"]) == 12
    assert result["fingerprint"]["frozen"] == result["fingerprint"]["current"]
    assert result["fingerprint"]["diff_fields"] == []


def test_e1_requires_completed_record():
    db, ps, rec, _ = _seed(_db(), status="failed")
    result = evaluate_eligibility(db, ps.id, record_id=rec.id)
    assert result["status"] == STATUS_INELIGIBLE
    assert E_NO_BACKTEST in _codes(result)


def test_e2_requires_evaluation_snapshot():
    db, ps, _, _ = _seed(_db(), with_eval=False)
    result = evaluate_eligibility(db, ps.id)
    assert E_EVALUATION_MISSING in _codes(result)


def test_e3_param_drift_is_stale_and_lists_fields():
    db, ps, rec, strategy = _seed(_db())
    strategy.max_positions = 7
    db.commit()

    result = evaluate_eligibility(db, ps.id)
    assert result["status"] == STATUS_STALE
    assert E_PARAM_STALE in _codes(result)
    assert "strategies" in result["fingerprint"]["diff_fields"]


def test_e3_missing_snapshot_is_stale():
    db, ps, rec, _ = _seed(_db())
    rec.params_snapshot = None
    db.commit()
    result = evaluate_eligibility(db, ps.id)
    assert result["status"] == STATUS_STALE
    assert E_PARAM_STALE in _codes(result)


def test_e3_pool_change_is_stale():
    db, ps, rec, _ = _seed(_db())
    db.add(StockPoolStock(pool_id=ps.stock_pool_id, stock_code="600009.SH"))
    db.commit()
    result = evaluate_eligibility(db, ps.id)
    assert result["status"] == STATUS_STALE
    assert "stock_pool_codes" in result["fingerprint"]["diff_fields"]


def test_e4_requires_three_year_window():
    db, ps, _, _ = _seed(_db(), trading_days=700)
    result = evaluate_eligibility(db, ps.id)
    assert E_WINDOW_SHORT in _codes(result)


def test_e5_requires_one_year_oos():
    db, ps, _, _ = _seed(_db(), oos_years=0)
    result = evaluate_eligibility(db, ps.id)
    assert E_OOS_MISSING in _codes(result)


def test_e6_requires_trade_sample():
    db, ps, _, _ = _seed(_db(), trades=50)
    result = evaluate_eligibility(db, ps.id)
    assert E_TRADE_SAMPLE_SMALL in _codes(result)


def test_e6_requires_holding_days():
    db, ps, _, _ = _seed(_db(), hold_days=100)
    result = evaluate_eligibility(db, ps.id)
    assert E_TRADE_SAMPLE_SMALL in _codes(result)


def test_e7_requires_bear_segment_coverage():
    db, ps, _, _ = _seed(_db(), bull=200, bear=0)
    result = evaluate_eligibility(db, ps.id)
    assert E_GATE_COVERAGE_LOW in _codes(result)


def test_e8_requires_non_empty_stock_pool():
    db, ps, _, _ = _seed(_db(), pool_size=0)
    result = evaluate_eligibility(db, ps.id)
    assert E_PORTFOLIO_INVALID in _codes(result)


def test_e9_capacity_missing_watch_on_l2_reject_on_l3():
    db, ps, rec, _ = _seed(_db())
    rec.validation_json = None
    db.commit()

    l2 = evaluate_eligibility(db, ps.id, scope="L2")
    assert l2["status"] == STATUS_WATCH
    assert E_CAPACITY_MISSING in _codes(l2)

    l3 = evaluate_eligibility(db, ps.id, scope="L3")
    assert l3["status"] == STATUS_INELIGIBLE
    assert E_CAPACITY_MISSING in _codes(l3)


def test_e9_capacity_warnings_reject_on_l3():
    db, ps, _, _ = _seed(_db(), capacity=["ADV 参与率过高"])
    result = evaluate_eligibility(db, ps.id, scope="L3")
    assert E_CAPACITY_MISSING in _codes(result)


def test_e10_risk_defaults_only_warn():
    # 风控列为 NOT NULL + 库默认值，「缺失」在库里不可表达；等价状态是风控阈值
    # 被配成 0（等于不约束）→ 视为用保守默认，只降级不拒绝。
    db, ps, _, _ = _seed(_db(), max_drawdown=Decimal("0"))
    result = evaluate_eligibility(db, ps.id)
    assert result["status"] == STATUS_WATCH
    assert E_RISK_DEFAULTED in _codes(result)


def test_e11_missing_quality_rejects():
    db, ps, _, _ = _seed(_db(), quality=None)
    result = evaluate_eligibility(db, ps.id)
    assert E_DATA_QUALITY in _codes(result)


def test_e11_low_coverage_rejects():
    db, ps, _, _ = _seed(_db(), quality=0.90)
    result = evaluate_eligibility(db, ps.id)
    assert E_DATA_QUALITY in _codes(result)


def test_e12_future_completion_rejects():
    future = datetime.now() + timedelta(days=2)
    db, ps, rec, _ = _seed(_db(), completed_at=future)
    result = evaluate_eligibility(db, ps.id, record_id=rec.id)
    assert E_DECISION_TIME_LEAK in _codes(result)


def test_lookup_error_for_unknown_portfolio():
    db = _db()
    with pytest.raises(LookupError):
        evaluate_eligibility(db, 999)


def test_fingerprint_is_deterministic():
    db, ps, rec, _ = _seed(_db())
    frozen = json.loads(rec.params_snapshot)
    first = config_fingerprint(ps, db, frozen)
    second = config_fingerprint(ps, db, frozen)
    assert first == second
    assert first["frozen"] == first["current"]
