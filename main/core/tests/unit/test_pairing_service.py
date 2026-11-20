"""L2 拼盘草稿/评估/批准（0021 B6）测试。"""
import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import (
    Base, BacktestRecord, BacktestTrade, CapitalPairingPlan,
    PortfolioStrategy, Strategy,
)
from core.services import pairing_service
from core.services.backtest_service import (
    _portfolio_strategies, build_params_snapshot, pool_composition,
)
from core.services.pairing_service import (
    BINDING_VERSION,
    PLAN_APPROVED,
    PLAN_DRAFT,
    PLAN_SUSPENDED,
    PairingError,
    create_plan,
    evaluate_plan,
    get_plan,
    list_plans,
    approve_plan,
    delete_plan,
    build_loan_model_plan,
    link_backtest,
)
from core.tests.unit.test_pairing_eligibility import _db, _seed


def _pair(db):
    db, ps, rec, master = _seed(db)
    slave = Strategy(portfolio_id=ps.id, name="搭配策略", formula_id=1,
                     period="1d", role="slave")
    db.add(slave)
    db.flush()
    db.bulk_save_objects([
        BacktestTrade(
            backtest_record_id=rec.id, strategy_id=slave.id,
            signal_name="open_sig", signal_type="OPEN",
            stock_code="600001.SH", trade_type="BUY",
            price=Decimal("10"), quantity=100, amount=Decimal("1000"),
            commission=Decimal("5"), stamp_duty=Decimal("0"),
            bar_time=datetime(2023, 1, 3, 15, 0) + timedelta(days=i))
        for i in range(60)
    ] + [
        BacktestTrade(
            backtest_record_id=rec.id, strategy_id=slave.id,
            signal_name="close_sig", signal_type="CLOSE",
            stock_code="600001.SH", trade_type="SELL",
            price=Decimal("10"), quantity=100, amount=Decimal("1000"),
            commission=Decimal("5"), stamp_duty=Decimal("0"),
            bar_time=datetime(2023, 2, 3, 15, 0) + timedelta(days=i))
        for i in range(60)
    ])
    rec.params_snapshot = build_params_snapshot(
        ps, strategies=_portfolio_strategies(ps, db),
        stock_pool_codes=pool_composition(ps, db), market_gate_enabled=True)
    db.commit()
    return db, ps, rec, master, slave


def _payload(ps, master, slave, **over):
    data = {
        "name": "L2 测试拼盘", "portfolio_strategy_id": ps.id,
        "lender_strategy_id": master.id, "borrower_strategy_ids": [slave.id],
        "principal": 100000, "scope": "L2", "loan_term_days": 365,
        "recall_notice_days": 2, "release_delay_days": 0, "max_share": 1.0,
        "created_by": "tester",
    }
    data.update(over)
    return data


def test_create_plan_and_detail():
    db, ps, rec, master, slave = _pair(_db())
    plan = create_plan(db, _payload(ps, master, slave))

    assert plan["status"] == PLAN_DRAFT
    assert plan["scope"] == "L2"
    assert plan["eval"]["status"] == "GO"
    assert plan["edge"]["lender_id"] == master.id
    assert plan["edge"]["borrower_id"] == slave.id
    binding = plan["binding"]
    assert binding["binding_version"] == BINDING_VERSION
    assert binding["loan"]["principal"] == "100000"
    assert plan["loan_model_plan"] == {
        "enabled": True, "lender_strategy_id": master.id,
        "borrower_strategy_id": slave.id, "principal": "100000.00",
        "due_days": 365, "pairing_version": plan["pairing_version"],
        "borrowers": [{
            "portfolio_strategy_id": ps.id, "strategy_id": slave.id,
            "borrower_record_id": rec.id, "max_share": "1.0000",
        }]}

    listed = list_plans(db)
    assert len(listed) == 1 and listed[0]["id"] == plan["id"]
    assert get_plan(db, plan["id"])["name"] == "L2 测试拼盘"


def test_binding_snapshot_freezes_requote_and_limits():
    db, ps, rec, master, slave = _pair(_db())
    plan = create_plan(db, _payload(ps, master, slave))
    lender = plan["binding"]["lender"]
    for field in ("strategy_id", "period", "risk_timing",
                  "requote_enabled", "requote_max_attempts",
                  "requote_price_mode", "requote_max_slippage_ticks",
                  "max_order_amount", "split_order_enabled",
                  "split_order_threshold"):
        assert field in lender


def test_drift_after_period_change_marks_no_go_and_suspends():
    db, ps, rec, master, slave = _pair(_db())
    plan = create_plan(db, _payload(ps, master, slave))
    approve_plan(db, plan["id"])
    assert get_plan(db, plan["id"])["status"] == PLAN_APPROVED

    slave.period = "5m"
    db.commit()
    result = evaluate_plan(db, plan["id"])

    assert result["status"] == "NO_GO"
    assert any("borrower.period" in reason for reason in result["reasons"])
    assert get_plan(db, plan["id"])["status"] == PLAN_SUSPENDED


def test_approve_requires_go():
    db, ps, rec, master, slave = _pair(_db())
    plan = create_plan(db, _payload(ps, master, slave))
    slave.period = "5m"
    db.commit()
    evaluate_plan(db, plan["id"])

    with pytest.raises(PairingError):
        approve_plan(db, plan["id"])


def test_only_one_borrower_in_stage_b():
    db, ps, rec, master, slave = _pair(_db())
    with pytest.raises(PairingError):
        create_plan(db, _payload(ps, master, slave,
                                 borrower_strategy_ids=[slave.id, master.id]))


def test_cross_portfolio_borrower_rejected():
    db, ps, rec, master, slave = _pair(_db())
    other_ps = type(ps)(
        name="别的组合", stock_pool_id=ps.stock_pool_id,
        initial_capital=Decimal("1000000"), max_holdings=10,
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    db.add(other_ps)
    db.flush()
    outsider = Strategy(portfolio_id=other_ps.id, name="外部", formula_id=1,
                        period="1d", role="master")
    db.add(outsider)
    db.commit()

    with pytest.raises(PairingError):
        create_plan(db, _payload(ps, master, outsider))


def test_l3_not_open_in_stage_b():
    db, ps, rec, master, slave = _pair(_db())
    with pytest.raises(PairingError):
        create_plan(db, _payload(ps, master, slave, scope="L3"))


def test_delete_only_draft():
    db, ps, rec, master, slave = _pair(_db())
    plan = create_plan(db, _payload(ps, master, slave))
    approve_plan(db, plan["id"])
    with pytest.raises(PairingError):
        delete_plan(db, plan["id"])

    db2, ps2, rec2, master2, slave2 = _pair(_db())
    draft = create_plan(db2, _payload(ps2, master2, slave2))
    assert delete_plan(db2, draft["id"]) == {"deleted": draft["id"]}
    assert db2.query(CapitalPairingPlan).count() == 0


def test_link_backtest_sets_record():
    db, ps, rec, master, slave = _pair(_db())
    plan = create_plan(db, _payload(ps, master, slave))
    link_backtest(db, plan["id"], 4321)
    detail = get_plan(db, plan["id"])
    assert detail["backtest_record_id"] == 4321
    assert detail["evidence"]["backtest_record_id"] == 4321


def test_loan_model_plan_rejects_missing_plan():
    db = _db()
    with pytest.raises(LookupError):
        build_loan_model_plan(db, 9999)


# ---------------- 0021 E4：L3 跨组合 2–3 借款方 / 队列 TTL ----------------
def _l3_case(db):
    db, ps, rec, master, slave = _pair(db)
    ps2 = PortfolioStrategy(
        name="组合二", stock_pool_id=ps.stock_pool_id,
        initial_capital=Decimal("1000000"), max_holdings=10,
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    db.add(ps2)
    db.flush()
    s2 = Strategy(portfolio_id=ps2.id, name="借款二", formula_id=1,
                  period="1d", role="slave")
    s3 = Strategy(portfolio_id=ps2.id, name="借款三", formula_id=1,
                  period="1d", role="slave")
    db.add_all([s2, s3])
    db.commit()
    return db, ps, ps2, master, slave, s2, s3


def _l3_payload(ps, master, pairs, **over):
    data = {
        "name": "L3 跨组合拼盘", "portfolio_strategy_id": ps.id,
        "lender_strategy_id": master.id, "scope": "L3",
        "borrowers": [
            {"portfolio_strategy_id": pid, "strategy_id": sid,
             "max_share": share}
            for pid, sid, share in pairs
        ],
        "principal": 300000, "loan_term_days": 365,
        "recall_notice_days": 2, "created_by": "tester",
    }
    data.update(over)
    return data


def test_l3_creates_three_borrower_edges_across_portfolios():
    db, ps, ps2, master, slave, s2, s3 = _l3_case(_db())
    plan = create_plan(db, _l3_payload(ps, master, [
        (ps.id, slave.id, 1.0), (ps2.id, s2.id, 0.6), (ps2.id, s3.id, 0.45),
    ]))

    assert plan["scope"] == "L3"
    assert plan["borrower_count"] == 3
    assert len(plan["edges"]) == 3
    shares = {e["borrower_id"]: e["max_share"] for e in plan["edges"]}
    assert shares[slave.id] == "1.0000"
    assert shares[s2.id] == "0.6000"
    assert shares[s3.id] == "0.4500"
    assert {b["portfolio_strategy_id"]
            for b in plan["loan_model_plan"]["borrowers"]} == {ps.id, ps2.id}


def test_l3_fourth_borrower_queues_with_ttl():
    db, ps, ps2, master, slave, s2, s3 = _l3_case(_db())
    s4 = Strategy(portfolio_id=ps2.id, name="借款四", formula_id=1,
                  period="1d", role="slave")
    db.add(s4)
    db.commit()

    queued = create_plan(db, _l3_payload(ps, master, [
        (ps.id, slave.id, 1.0), (ps2.id, s2.id, 0.6),
        (ps2.id, s3.id, 0.45), (ps2.id, s4.id, 1.0),
    ]))

    assert queued["status"] == "QUEUED"
    assert queued["borrower_count"] == 0
    assert queued["queue"]["policy"] == "queue"
    assert queued["queue"]["requested"] == 4
    assert queued["queue"]["position"] == 1
    assert queued["queue"]["expires_at"] > queued["queue"]["enqueued_at"]


def test_l3_fourth_borrower_rejected_when_policy_reject():
    db, ps, ps2, master, slave, s2, s3 = _l3_case(_db())
    s4 = Strategy(portfolio_id=ps2.id, name="借款四", formula_id=1,
                  period="1d", role="slave")
    db.add(s4)
    db.commit()

    with pytest.raises(PairingError, match="BORROWER_LIMIT_REACHED"):
        create_plan(db, _l3_payload(ps, master, [
            (ps.id, slave.id, 1.0), (ps2.id, s2.id, 0.6),
            (ps2.id, s3.id, 0.45), (ps2.id, s4.id, 1.0),
        ], queue_policy="reject"))


def test_l3_queued_plan_expires_after_ttl():
    db, ps, ps2, master, slave, s2, s3 = _l3_case(_db())
    s4 = Strategy(portfolio_id=ps2.id, name="借款四", formula_id=1,
                  period="1d", role="slave")
    db.add(s4)
    db.commit()
    queued = create_plan(db, _l3_payload(ps, master, [
        (ps.id, slave.id, 1.0), (ps2.id, s2.id, 0.6),
        (ps2.id, s3.id, 0.45), (ps2.id, s4.id, 1.0),
    ]))

    far_future = datetime.now() + timedelta(days=60)
    rows = list_plans(db, now=far_future)
    row = next(p for p in rows if p["id"] == queued["id"])
    assert row["status"] == "EXPIRED"
    assert row["queue"] is None

