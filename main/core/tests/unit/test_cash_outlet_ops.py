"""G4 现金出口执行台账（P1/P2/P3、限额、回流与短款追溯）测试。"""
from datetime import date, datetime
from decimal import Decimal

import pytest

from core.models import CashOutlet
from core.services import cash_outlet_service as svc
from core.tests.unit.test_cash_outlet import _outlet
from core.tests.unit.test_pairing_eligibility import _db


NOW = datetime(2026, 11, 20, 14, 30)


def _seed_outlet(db, **over):
    outlet = _outlet(**over)
    outlet.id = None
    db.add(outlet)
    db.commit()
    db.refresh(outlet)
    return outlet


def test_p1_plan_confirm_and_return():
    db = _db()
    outlet = _seed_outlet(db)

    op = svc.plan_outlet_op(
        db, outlet, amount="10000", mode="P1", now=NOW)
    assert op["status"] == svc.OP_PENDING
    assert op["mode"] == "P1"
    assert op["auto"] is False

    confirmed = svc.confirm_op(db, op["id"], approved_by="ops", now=NOW)
    assert confirmed["status"] == svc.OP_EXECUTED
    assert confirmed["approved_by"] == "ops"

    returned = svc.record_return(
        db, op["id"], principal_returned="10000", yield_amount="25.50",
        returned_at=NOW)
    assert returned["status"] == svc.OP_RETURNED
    assert returned["yield_amount"] == "25.50"
    assert returned["shortfall_amount"] == "0.00"

    summary = svc.ops_summary(db)
    assert summary["yield_contribution"] == "25.50"
    assert summary["shortfall_total"] == "0"
    assert summary["returned_principal"] == "10000.00"


def test_shortfall_requires_reason_and_is_traceable():
    db = _db()
    outlet = _seed_outlet(db)
    op = svc.plan_outlet_op(db, outlet, amount="5000", mode="P1", now=NOW)
    svc.confirm_op(db, op["id"], approved_by="ops", now=NOW)

    with pytest.raises(svc.OutletOpsError, match="shortfall_reason"):
        svc.record_return(db, op["id"], principal_returned="4800")

    returned = svc.record_return(
        db, op["id"], principal_returned="4800",
        shortfall_reason="通道延迟，T+1 补回", returned_at=NOW)
    assert returned["shortfall_amount"] == "200.00"
    recon = svc.reconcile_ops(db, today=date(2026, 11, 21))
    assert recon["shortfalls"][0]["shortfall_reason"] == "通道延迟，T+1 补回"
    assert recon["ok"] is True


def test_p3_auto_executes_only_verified_short_term_outlet():
    db = _db()
    outlet = _seed_outlet(db, code="GC001-P3")
    op = svc.plan_outlet_op(db, outlet, amount="20000", mode="P3", now=NOW)
    assert op["auto"] is True
    assert op["status"] == svc.OP_EXECUTED
    assert op["approved_by"] == "auto:p3"

    unverified = _seed_outlet(db, code="GC002", verified=False)
    with pytest.raises(svc.OutletOpsError):
        svc.plan_outlet_op(db, unverified, amount="20000", mode="P3", now=NOW)


def test_daily_limit_accumulates_across_ops():
    db = _db()
    outlet = _seed_outlet(db, max_daily_amount=Decimal("5000"))
    first = svc.plan_outlet_op(db, outlet, amount="4000", mode="P1", now=NOW)
    # created_at 由模型默认取真实时钟（now 注入只影响业务时间字段，不覆盖它），
    # 「单日累计」按 created_at 归日；这里显式对齐到 NOW，避免用例只在运行当天
    # 恰好等于 NOW 时才生效的时间依赖。
    row = db.get(svc.CashOutletOp, first["id"])
    row.created_at = NOW
    db.commit()
    with pytest.raises(svc.OutletOpsError, match="超过单日上限"):
        svc.plan_outlet_op(db, outlet, amount="2000", mode="P1", now=NOW)


def test_idempotency_key_returns_same_op():
    db = _db()
    outlet = _seed_outlet(db)
    first = svc.plan_outlet_op(
        db, outlet, amount="3000", mode="P1",
        idempotency_key="op-key-1", now=NOW)
    second = svc.plan_outlet_op(
        db, outlet, amount="3000", mode="P1",
        idempotency_key="op-key-1", now=NOW)
    assert second["id"] == first["id"]
    assert second["deduplicated"] is True
    assert db.query(svc.CashOutletOp).count() == 1


def test_mode_gating_and_invalid_amount():
    assert svc.mode_allows("P3", "P1") is True
    assert svc.mode_allows("P2", "P3") is False
    assert svc.mode_allows("OFF", "P1") is False
    with pytest.raises(svc.OutletOpsError):
        svc.normalize_mode("P9")
    db = _db()
    outlet = _seed_outlet(db)
    with pytest.raises(svc.OutletOpsError):
        svc.plan_outlet_op(db, outlet, amount="0", mode="P1", now=NOW)


def test_confirm_requires_pending_and_reject_is_terminal():
    db = _db()
    outlet = _seed_outlet(db)
    op = svc.plan_outlet_op(db, outlet, amount="1000", mode="P1", now=NOW)
    rejected = svc.reject_op(db, op["id"], reason="额度另有安排")
    assert rejected["status"] == svc.OP_REJECTED
    with pytest.raises(svc.OutletOpsError):
        svc.confirm_op(db, op["id"], approved_by="ops", now=NOW)
