"""G3 拼盘健康指标与自动暂停（0020 §5.5 / 0021 G3）测试。"""
from datetime import date, datetime

import pytest

from core.models import CapitalPairing, CapitalPairingPlan
from core.services import capital_health
from core.services.capital_health import (
    STATUS_ACTIVE,
    STATUS_INSUFFICIENT,
    STATUS_RETIRED,
    STATUS_SUSPENDED,
    STATUS_WATCH,
    HealthError,
    advance_streaks,
    evaluate_health,
    health_history,
    record_health,
    resume_plan,
    retire_plan,
)
from core.services.pairing_service import approve_plan, create_plan
from core.tests.unit.test_pairing_eligibility import _db
from core.tests.unit.test_pairing_service import _pair, _payload


NORMAL = {
    "corr_20d": 0.1, "corr_60d": 0.2, "drawdown_excess_pp": 0.5,
    "adv_participation": 0.005, "grab_ratio_month": 0.01,
    "recall_per_year": 4, "shortfall_ratio": 0.005,
    "shortfall_quarter_count": 0, "borrower_fill_rate": 0.98,
    "lender_recovery_fill_rate": 0.95, "params_hash_match": True,
    "reconcile_failed": False,
}


def _healthy_plan(db):
    db, ps, rec, master, slave = _pair(db)
    plan = create_plan(db, _payload(ps, master, slave))
    approve_plan(db, plan["id"])
    return db, plan["id"]


# ---------------------------------------------------------------------------
# 纯函数：阈值与连续计数
# ---------------------------------------------------------------------------

def test_all_normal_is_active():
    result = evaluate_health(NORMAL, streaks={})
    assert result["status"] == STATUS_ACTIVE
    assert result["counts"]["normal"] == 10
    assert result["reasons"] == []


def test_missing_observations_is_insufficient():
    result = evaluate_health({}, streaks={})
    assert result["status"] == STATUS_INSUFFICIENT
    assert result["counts"]["skipped"] == 10


def test_correlation_watch_then_suspend_after_five_days():
    watch = evaluate_health({"corr_60d": 0.75}, streaks={"corr_high": 0})
    assert watch["status"] == STATUS_WATCH
    four = evaluate_health({"corr_60d": 0.85}, streaks={"corr_high": 4})
    assert four["status"] == STATUS_WATCH
    five = evaluate_health({"corr_60d": 0.85}, streaks={"corr_high": 5})
    assert five["status"] == STATUS_SUSPENDED
    assert "correlation" in five["reasons"]


def test_drawdown_over_three_pp_suspends_immediately():
    assert evaluate_health(
        {"drawdown_excess_pp": 2.5}, {})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"drawdown_excess_pp": 3.5}, {})["status"] == STATUS_SUSPENDED
    assert evaluate_health(
        {"drawdown_limit_breached": True}, {})["status"] == STATUS_SUSPENDED


def test_adv_and_grab_streak_thresholds():
    assert evaluate_health(
        {"adv_participation": 0.012}, {})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"adv_participation": 0.02}, {"adv_high": 4})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"adv_participation": 0.02}, {"adv_high": 5})["status"] == STATUS_SUSPENDED
    assert evaluate_health(
        {"grab_ratio_month": 0.12}, {"grab_high_months": 1})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"grab_ratio_month": 0.12},
        {"grab_high_months": 2})["status"] == STATUS_SUSPENDED


def test_recall_shortfall_and_fill_rates():
    assert evaluate_health(
        {"recall_per_year": 18}, {})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"recall_per_year": 30}, {})["status"] == STATUS_SUSPENDED
    assert evaluate_health(
        {"shortfall_ratio": 0.03}, {})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"shortfall_ratio": 0.06}, {})["status"] == STATUS_SUSPENDED
    assert evaluate_health(
        {"shortfall_quarter_count": 3}, {})["status"] == STATUS_SUSPENDED
    assert evaluate_health(
        {"borrower_fill_rate": 0.92}, {})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"borrower_fill_rate": 0.85},
        {"borrower_fill_low": 5})["status"] == STATUS_SUSPENDED
    assert evaluate_health(
        {"lender_recovery_fill_rate": 0.85}, {})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"lender_recovery_fill_rate": 0.75},
        {"lender_fill_low": 5})["status"] == STATUS_SUSPENDED


def test_params_mismatch_and_reconcile_are_immediate_or_streak_based():
    assert evaluate_health(
        {"params_hash_match": False}, {})["status"] == STATUS_SUSPENDED
    assert evaluate_health(
        {"reconcile_failed": True},
        {"reconcile_failed": 1})["status"] == STATUS_WATCH
    assert evaluate_health(
        {"reconcile_failed": True},
        {"reconcile_failed": 2})["status"] == STATUS_SUSPENDED


def test_advance_streaks_increments_resets_and_keeps_missing():
    first = advance_streaks(
        {"corr_60d": 0.9, "adv_participation": 0.02,
         "borrower_fill_rate": 0.85, "lender_recovery_fill_rate": 0.7,
         "reconcile_failed": True, "grab_ratio_month": 0.12},
        previous={}, as_of=date(2026, 11, 20))
    assert first["corr_high"] == 1
    assert first["adv_high"] == 1
    assert first["borrower_fill_low"] == 1
    assert first["lender_fill_low"] == 1
    assert first["reconcile_failed"] == 1
    assert first["grab_high_months"] == 1

    second = advance_streaks(
        {"corr_60d": 0.1, "grab_ratio_month": 0.01},
        previous=first, as_of=date(2026, 11, 23),
        prev_as_of=date(2026, 11, 20))
    # 正常观测重置连续计数；缺观测沿用旧值
    assert second["corr_high"] == 0
    assert second["adv_high"] == 1
    assert second["borrower_fill_low"] == 1
    assert second["grab_high_months"] == 0


# ---------------------------------------------------------------------------
# 落库与状态机
# ---------------------------------------------------------------------------

def test_suspend_then_manual_resume_requires_five_normal_days():
    db, plan_id = _healthy_plan(_db())

    for day in range(1, 6):
        row = record_health(
            db, plan_id, NORMAL, as_of=date(2026, 11, day))
        assert row["status"] == STATUS_ACTIVE
    assert row["normal_streak"] == 5
    assert db.get(CapitalPairingPlan, plan_id).status == "APPROVED"

    bad = dict(NORMAL, shortfall_ratio=0.06)
    row = record_health(db, plan_id, bad, as_of=date(2026, 11, 6))
    assert row["status"] == STATUS_SUSPENDED
    assert db.get(CapitalPairingPlan, plan_id).status == "SUSPENDED"
    edge = db.query(CapitalPairing).filter(
        CapitalPairing.pairing_plan_id == plan_id).first()
    assert edge.status == "SUSPENDED"

    with pytest.raises(HealthError, match="连续正常交易日 0"):
        resume_plan(db, plan_id, reason="测试提前恢复")

    # 暂停后即使恢复为正常也不自动回升，必须人工确认
    for day in range(7, 12):
        row = record_health(db, plan_id, NORMAL, as_of=date(2026, 11, day))
        assert row["status"] == STATUS_SUSPENDED
        assert row["held_for_manual_resume"] is False
        assert db.get(CapitalPairingPlan, plan_id).status == "SUSPENDED"
    assert row["normal_streak"] == 5

    data = resume_plan(db, plan_id, reason="指标已连续 5 日正常",
                       valid_until="2026-12-31", operator="ops")
    assert db.get(CapitalPairingPlan, plan_id).status == "APPROVED"
    assert edge.status == "ACTIVE"
    assert data["resume"]["reason"] == "指标已连续 5 日正常"
    assert data["resume"]["valid_until"] == "2026-12-31"
    assert data["resume"]["normal_streak"] == 5


def test_watch_requires_manual_resume_too():
    db, plan_id = _healthy_plan(_db())
    row = record_health(db, plan_id, {"corr_60d": 0.6},
                        as_of=date(2026, 11, 20))
    assert row["status"] == STATUS_WATCH
    assert db.get(CapitalPairingPlan, plan_id).status == "WATCH"
    for day in range(21, 26):
        row = record_health(db, plan_id, NORMAL, as_of=date(2026, 11, day))
        assert row["status"] == STATUS_WATCH
    assert row["normal_streak"] == 5
    resume_plan(db, plan_id, reason="观察期结束")
    assert db.get(CapitalPairingPlan, plan_id).status == "APPROVED"


def test_insufficient_observation_keeps_status_and_streak():
    db, plan_id = _healthy_plan(_db())
    record_health(db, plan_id, dict(NORMAL, shortfall_ratio=0.06),
                  as_of=date(2026, 11, 20))
    row = record_health(db, plan_id, {}, as_of=date(2026, 11, 21))
    assert row["status"] == STATUS_SUSPENDED
    assert row["normal_streak"] == 0
    assert db.get(CapitalPairingPlan, plan_id).status == "SUSPENDED"


def test_draft_plan_cannot_be_evaluated():
    db, ps, rec, master, slave = _pair(_db())
    plan = create_plan(db, _payload(ps, master, slave))
    with pytest.raises(HealthError):
        record_health(db, plan["id"], NORMAL)


def test_retire_is_permanent_and_keeps_history():
    db, plan_id = _healthy_plan(_db())
    record_health(db, plan_id, NORMAL, as_of=date(2026, 11, 20))
    data = retire_plan(db, plan_id, reason="替换为新方案", operator="ops")
    assert data["plan_status"] == STATUS_RETIRED
    plan = db.get(CapitalPairingPlan, plan_id)
    assert plan.status == STATUS_RETIRED
    assert plan.effective_to is not None
    assert all(e.status == STATUS_RETIRED for e in db.query(CapitalPairing).filter(
        CapitalPairing.pairing_plan_id == plan_id).all())
    assert health_history(db, plan_id)["retire_reason"]["reason"] == "替换为新方案"
    with pytest.raises(HealthError):
        record_health(db, plan_id, NORMAL)
    with pytest.raises(HealthError):
        retire_plan(db, plan_id, reason="重复退役")
