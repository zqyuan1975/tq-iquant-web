"""搭配评估 H1–H8 + 软判据（0021 B5）测试。

全部只读：构造两个组合策略的回测记录（日快照/成交/评估/walk-forward），
验证硬约束、软判据、PENDING_C/D 标记与 GO/WATCH/NO_GO 判定。
"""
import json
from datetime import date, datetime, time, timedelta
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
from core.services.pairing_eval import (
    PAIRING_EVAL_VERSION,
    PENDING_C,
    PENDING_D,
    STATE_FAIL,
    STATE_PASS,
    STATE_PENDING,
    STATUS_GO,
    STATUS_NO_GO,
    STATUS_WATCH,
    evaluate_pairing,
)

DAYS = 800
DAY0 = date(2023, 1, 1)


def _db():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _alternating(start=Decimal("1000000"), days=DAYS, phase=0,
                 up="0.015", down="0.010"):
    """围绕起始净值震荡的序列（不复利）：回撤浅（<3%）且权益接近初始值，
    用于正常场景与 5% 现金需求判定。"""
    values = []
    for i in range(days):
        if (i + phase) % 2 == 0:
            value = start * (Decimal("1") + Decimal(up))
        else:
            value = start * (Decimal("1") - Decimal(down))
        values.append(value.quantize(Decimal("0.01")))
    return values


def _drop(start=Decimal("1000000"), days=DAYS, at=400, pct="0.10"):
    values, value, factor = [], start, Decimal(pct)
    for i in range(days):
        if i == at:
            value *= (Decimal("1") - factor)
        values.append(value.quantize(Decimal("0.01")))
    return values


def _make_side(db, *, name, code, values, phase, hold_days, turnover,
               max_drawdown=Decimal("0.20"), buy_amount=Decimal("60000")):
    pool = StockPool(code="POOL_%s" % name, name=name)
    db.add(pool)
    db.flush()
    db.add(StockPoolStock(pool_id=pool.id, stock_code=code))
    ps = PortfolioStrategy(
        name=name, stock_pool_id=pool.id, initial_capital=Decimal("1000000"),
        max_holdings=10, max_drawdown=max_drawdown,
        daily_loss_limit=Decimal("0.05"))
    db.add(ps)
    db.flush()
    strategy = Strategy(portfolio_id=ps.id, name="%s-主策略" % name,
                        formula_id=1, period="1d", role="master")
    db.add(strategy)
    db.flush()

    rec = BacktestRecord(
        portfolio_strategy_id=ps.id, name="%s-回测" % name,
        start_date=DAY0, end_date=DAY0 + timedelta(days=DAYS - 1),
        status="completed", completed_at=datetime(2026, 1, 1))
    db.add(rec)
    db.flush()
    rec.params_snapshot = build_params_snapshot(
        ps, strategies=[strategy],
        stock_pool_codes={str(pool.id): [code]})
    rec.validation_json = json.dumps({"capacity": []})
    rec.data_quality_json = json.dumps(
        {"summary": {"coverage_ratio": 0.995}, "missing_stocks": []})

    db.add(BacktestEvaluation(
        backtest_record_id=rec.id, target_type="portfolio", target_id=ps.id,
        annual_turnover=Decimal(str(turnover)),
        max_drawdown=Decimal("0.10")))
    db.add(WalkForwardRun(
        portfolio_strategy_id=ps.id, name="%s-wf" % name,
        start_date=DAY0, end_date=DAY0 + timedelta(days=DAYS - 1),
        train_years=3, test_years=1, status="completed",
        completed_at=datetime(2025, 1, 1),
        summary_json=json.dumps({"windows": [{
            "test": {"start": DAY0.isoformat(),
                     "end": (DAY0 + timedelta(days=DAYS - 1)).isoformat()}}]})))

    positions = json.dumps([{
        "stock_code": code, "quantity": 1000, "sellable_quantity": 1000,
        "market_value": "500000", "cost_value": "500000"}])
    db.bulk_save_objects([
        BacktestDailySnapshot(
            backtest_record_id=rec.id, target_type="portfolio",
            target_id=ps.id, snap_date=DAY0 + timedelta(days=i),
            total_value=value, cash=Decimal("200000"),
            market_value=value - Decimal("200000"),
            gate_state=(1 if i < 200 else (-1 if i < 300 else 0)),
            positions_json=positions)
        for i, value in enumerate(values)
    ])

    trades = []
    for k in range(150):
        buy_day = 2 * k + phase
        sell_day = buy_day + hold_days
        buy_at = datetime.combine(
            DAY0 + timedelta(days=buy_day), time(15, 0))
        trades.append(BacktestTrade(
            backtest_record_id=rec.id, strategy_id=strategy.id,
            signal_name="open_sig", signal_type="OPEN", stock_code=code,
            trade_type="BUY", price=Decimal("10"), quantity=6000,
            amount=buy_amount, commission=Decimal("5"),
            stamp_duty=Decimal("0"), transfer_fee=Decimal("0"),
            bar_time=buy_at))
        if sell_day < DAYS:
            sell_at = datetime.combine(
                DAY0 + timedelta(days=sell_day), time(15, 0))
            trades.append(BacktestTrade(
                backtest_record_id=rec.id, strategy_id=strategy.id,
                signal_name="close_sig", signal_type="CLOSE", stock_code=code,
                trade_type="SELL", price=Decimal("10"), quantity=6000,
                amount=buy_amount, commission=Decimal("5"),
                stamp_duty=Decimal("0"), transfer_fee=Decimal("0"),
                bar_time=sell_at))
    db.bulk_save_objects(trades)
    db.commit()
    return ps, rec, strategy


def _capacity_ok():
    return {
        "lender_participation": 0.004,
        "borrower_participation": 0.006,
        "same_code_participation": 0.008,
        "marginal_1x": 0.02,
        "marginal_2x": 0.01,
        "overlap": 0.05,
        "bid_probability": 0.20,
        "curve_decay_2x": 0.10,
    }


def _pair(db, *, lender_values=None, borrower_values=None,
          lender_code="600000.SH", borrower_code="600001.SH",
          lender_hold=10, borrower_hold=20,
          lender_turnover=10, borrower_turnover=20,
          lender_dd=Decimal("0.20"), borrower_dd=Decimal("0.20")):
    lender, lender_rec, _ = _make_side(
        db, name="出借方", code=lender_code,
        values=lender_values or _alternating(phase=0), phase=0,
        hold_days=lender_hold, turnover=lender_turnover,
        max_drawdown=lender_dd)
    borrower, borrower_rec, _ = _make_side(
        db, name="借款方", code=borrower_code,
        values=borrower_values or _alternating(phase=1), phase=1,
        hold_days=borrower_hold, turnover=borrower_turnover,
        max_drawdown=borrower_dd)
    return lender, borrower


def _check(result, code):
    return next(h for h in result["hard"] if h["code"] == code)


def _feature(result, name):
    return next(s for s in result["soft"] if s["feature"] == name)


def test_go_when_hard_pass_and_soft_clean():
    db = _db()
    lender, borrower = _pair(db)
    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())

    assert result["version"] == PAIRING_EVAL_VERSION
    assert result["status"] == STATUS_GO
    assert all(h["state"] == STATE_PASS for h in result["hard"])
    assert _feature(result, "return_corr")["state"] == STATE_PASS
    assert _feature(result, "same_code_signal_overlap")["state"] == STATE_PASS
    assert _feature(result, "cash_use_complement")["state"] == STATE_PASS
    assert result["common_days"] == DAYS - 1


def test_capacity_pending_blocks_go_until_c_layer():
    db = _db()
    lender, borrower = _pair(db)
    result = evaluate_pairing(db, lender.id, borrower.id)

    assert result["status"] == STATUS_WATCH
    assert _check(result, "H4")["state"] == STATE_PENDING
    assert _check(result, "H4")["detail"]["pending_code"] == PENDING_C
    assert _check(result, "H6")["detail"]["pending_code"] == PENDING_C
    assert any(s["pending_code"] == PENDING_D for s in result["soft"])


def test_h1_borrower_ineligible_is_no_go():
    db = _db()
    lender, borrower = _pair(db)
    record = db.query(BacktestRecord).filter_by(
        portfolio_strategy_id=borrower.id).first()
    record.status = "failed"
    db.commit()

    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())
    assert result["status"] == STATUS_NO_GO
    assert _check(result, "H1")["state"] == STATE_FAIL
    assert "H1" in result["triggers"]


def test_h2_short_common_oos_is_no_go():
    db = _db()
    lender, borrower = _pair(db)
    for ps_id in (lender.id, borrower.id):
        run = db.query(WalkForwardRun).filter_by(
            portfolio_strategy_id=ps_id).first()
        run.summary_json = json.dumps({"windows": [{
            "test": {"start": DAY0.isoformat(),
                     "end": (DAY0 + timedelta(days=100)).isoformat()}}]})
    db.commit()

    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())
    assert result["status"] == STATUS_NO_GO
    assert _check(result, "H2")["state"] == STATE_FAIL
    assert result["common_days"] == 100


def test_h3_position_overlap_high_is_no_go():
    db = _db()
    lender, borrower = _pair(db, borrower_code="600000.SH")
    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())
    assert result["status"] == STATUS_NO_GO
    assert _check(result, "H3")["state"] == STATE_FAIL
    assert result["metrics"]["weighted_code_overlap"] == pytest.approx(1.0)


def test_h4_capacity_conflict_is_no_go():
    db = _db()
    lender, borrower = _pair(db)
    cap = _capacity_ok()
    cap["lender_participation"] = 0.02
    result = evaluate_pairing(db, lender.id, borrower.id, capacity=cap)
    assert result["status"] == STATUS_NO_GO
    assert _check(result, "H4")["state"] == STATE_FAIL


def test_h5_same_day_cash_demand_is_no_go():
    db = _db()
    lender, borrower = _pair(db)
    # 借款方改在偶数日买入（与出借方同日）
    trade_days = {
        row.bar_time.date()
        for row in db.query(BacktestTrade).filter_by(
            backtest_record_id=db.query(BacktestRecord).filter_by(
                portfolio_strategy_id=borrower.id).first().id,
            trade_type="BUY").all()
    }
    for row in db.query(BacktestTrade).filter(
            BacktestTrade.backtest_record_id == db.query(BacktestRecord)
            .filter_by(portfolio_strategy_id=borrower.id)
            .first().id, BacktestTrade.trade_type == "BUY").all():
        row.bar_time = datetime.combine(
            row.bar_time.date() - timedelta(days=1), time(15, 0))
    db.commit()
    assert trade_days  # 原买入日非空

    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())
    assert result["status"] == STATUS_NO_GO
    assert _check(result, "H5")["state"] == STATE_FAIL


def test_h6_negative_marginal_is_no_go():
    db = _db()
    lender, borrower = _pair(db)
    cap = _capacity_ok()
    cap["marginal_2x"] = -0.01
    result = evaluate_pairing(db, lender.id, borrower.id, capacity=cap)
    assert result["status"] == STATUS_NO_GO
    assert _check(result, "H6")["state"] == STATE_FAIL


def test_h7_individual_drawdown_breach_is_no_go():
    db = _db()
    lender, borrower = _pair(
        db, lender_values=_drop(pct="0.30"), borrower_values=_alternating(phase=1))
    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())
    assert result["status"] == STATUS_NO_GO
    assert _check(result, "H7")["state"] == STATE_FAIL


def test_h8_combined_drawdown_over_limit_is_no_go():
    db = _db()
    lender, borrower = _pair(
        db, lender_values=_drop(pct="0.04"),
        borrower_values=_drop(pct="0.029"),
        lender_dd=Decimal("0.10"), borrower_dd=Decimal("0.03"))
    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())
    assert _check(result, "H7")["state"] == STATE_PASS
    assert _check(result, "H8")["state"] == STATE_FAIL
    assert result["status"] == STATUS_NO_GO


def test_two_soft_failures_are_no_go():
    db = _db()
    same = _drop(pct="0.10")
    lender, borrower = _pair(
        db, lender_values=same, borrower_values=list(same))
    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())
    assert _check(result, "H8")["state"] == STATE_PASS
    assert _feature(result, "return_corr")["state"] == STATE_FAIL
    assert _feature(result, "drawdown_overlap")["state"] == STATE_FAIL
    assert result["status"] == STATUS_NO_GO


def test_one_soft_failure_is_watch():
    db = _db()
    same = _alternating(phase=0)
    lender, borrower = _pair(
        db, lender_values=same, borrower_values=list(same))
    result = evaluate_pairing(db, lender.id, borrower.id,
                              capacity=_capacity_ok())
    assert _feature(result, "return_corr")["state"] == STATE_FAIL
    assert result["counts"]["soft_fail"] == 1
    assert result["status"] == STATUS_WATCH


def test_same_portfolio_is_rejected():
    db = _db()
    lender, _ = _pair(db)
    with pytest.raises(LookupError):
        evaluate_pairing(db, lender.id, lender.id)
    with pytest.raises(LookupError):
        evaluate_pairing(db, lender.id, 9999)
