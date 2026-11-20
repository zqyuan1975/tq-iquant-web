"""F2 实盘继承矩阵 + live_haircut 单元测试。"""
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import (
    Base, BacktestRunGroup, CapitalPairingPlan, LiveSession, PortfolioStrategy,
    Strategy,
)
from core.services import capital_inheritance as ci
from core.services import live_service
from core.services.pairing_service import binding_snapshot


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _portfolio(db):
    row = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(row)
    db.commit()
    return row


def _strategy(db, ps_id, name="s1", **kw):
    kw.setdefault("role", "主")
    row = Strategy(
        portfolio_id=ps_id, name=name, formula_id=1, period="1d", **kw)
    db.add(row)
    db.commit()
    return row


def _binding(lender, borrower, loan=None):
    return {
        "lender": binding_snapshot(lender),
        "borrower": binding_snapshot(borrower),
        "loan": loan or {
            "principal": "100000", "loan_term_days": 30,
            "recall_notice_days": 2, "release_delay_days": 0,
        },
    }


# ---------------------------------------------------------------------------
# 纯函数：矩阵 / 折扣 / 比对
# ---------------------------------------------------------------------------

def test_matrix_covers_inherit_fields_and_modes():
    rows = {row["key"]: row for row in ci.INHERITANCE_MATRIX}
    for field in ci.INHERIT_BINDING_FIELDS:
        assert rows[field]["mode"] == "inherit"
    assert {rows[key]["mode"] for key in
            ("principal", "loan_term_days", "execution_mode")} == {"confirm"}
    haircut_keys = {row["key"] for row in ci.INHERITANCE_MATRIX
                    if row["mode"] == "haircut"}
    assert haircut_keys == {spec["key"] for spec in ci.LIVE_HAIRCUT_FACTORS}
    assert len(haircut_keys) == 5


def test_live_haircut_defaults_are_explainable_and_multiplicative():
    out = ci.build_live_haircut()
    assert out["version"] == ci.LIVE_HAIRCUT_VERSION
    assert [f["key"] for f in out["factors"]] == [
        "t_plus_1", "real_available", "in_flight", "trade_backfill",
        "bridge_latency"]
    assert out["composite"] == 0.907911
    assert all(f["reason"] for f in out["factors"])
    assert out["warnings"] == []


def test_live_haircut_override_invalid_and_unknown_warn():
    out = ci.build_live_haircut({
        "t_plus_1": 0.5, "real_available": "x", "nope": 1,
    })
    factors = {f["key"]: f for f in out["factors"]}
    assert factors["t_plus_1"]["factor"] == 0.5
    assert factors["t_plus_1"]["overridden"] is True
    # 非法值回退默认
    assert factors["real_available"]["factor"] == 0.98
    assert factors["real_available"]["overridden"] is False
    codes = {w["code"] for w in out["warnings"]}
    assert codes == {"live_haircut_invalid", "live_haircut_unknown"}
    assert out["composite"] < 0.907911


def test_build_report_match_drift_and_missing():
    lender_plan = {"strategy_id": 1, "name": "L", "role": "主",
                   "formula_id": 1, "period": "1d", "risk_timing": "next_day"}
    borrower_plan = {"strategy_id": 2, "name": "B", "role": "从",
                     "formula_id": 1, "period": "1d", "risk_timing": "next_day"}
    snapshots = {
        1: dict(lender_plan, period="1d"),
        2: dict(borrower_plan, period="1w"),  # 周期漂移
    }
    report = ci.build_report(
        {"lender": lender_plan, "borrower": borrower_plan,
         "loan": {"principal": "100000"}},
        snapshots, pairing_version="v1")

    assert report["enabled"] is True
    assert report["drift_fields"] == ["borrowers[0].period"]
    assert report["drift_count"] == 1
    statuses = {(i["side"], i["field"]): i["status"] for i in report["items"]}
    assert statuses[("lender", "period")] == "match"
    assert statuses[("borrowers[0]", "period")] == "drift"
    # 计划缺失字段 → 告警；未提供的策略 → strategy_missing
    codes = [w["code"] for w in report["warnings"]]
    assert "inheritance_field_missing" in codes  # formula_id 之外的空字段
    assert "strategy_missing" not in codes
    miss = ci.build_report(
        {"lender": lender_plan, "borrower": borrower_plan}, {})
    assert any(w["code"] == "strategy_missing" for w in miss["warnings"])


def test_build_report_missing_plan_field_warns():
    lender = {"strategy_id": 1, "name": "L"}
    report = ci.build_report(
        {"lender": lender, "borrower": lender, "loan": {}}, {1: lender})
    missing = [w for w in report["warnings"]
               if w["code"] == "inheritance_field_missing"]
    assert missing
    assert any(w["side"] == "loan" for w in missing)


# ---------------------------------------------------------------------------
# live_service：会话绑定 → 只读报告
# ---------------------------------------------------------------------------

def _seed_bound(db, *, run_group=False):
    ps = _portfolio(db)
    lender = _strategy(db, ps.id, "lender", role="主")
    borrower = _strategy(db, ps.id, "borrower", role="从")
    plan = CapitalPairingPlan(
        name="拼盘", portfolio_strategy_id=ps.id,
        lender_strategy_id=lender.id,
        binding_json=json.dumps(_binding(lender, borrower)))
    db.add(plan)
    db.commit()
    session = LiveSession(name="s", mode="simulation", status="stopped",
                          pairing_plan_id=None if run_group else plan.id)
    if run_group:
        group = BacktestRunGroup(name="rg", status="completed",
                                 pairing_plan_id=plan.id)
        db.add(group)
        db.commit()
        session.backtest_run_group_id = group.id
    db.add(session)
    db.commit()
    return session, plan, lender, borrower


def test_session_report_unbound_disabled():
    db = _db()
    session = LiveSession(name="s", mode="simulation", status="stopped")
    db.add(session)
    db.commit()
    out = live_service.capital_inheritance_report(db, session)
    assert out == {"enabled": False, "reason": "session_not_bound"}


def test_session_report_bound_plan_matches():
    db = _db()
    session, plan, _, _ = _seed_bound(db)
    out = live_service.capital_inheritance_report(db, session)
    assert out["enabled"] is True and out["found"] is True
    assert out["source"] == "pairing_plan"
    assert out["plan_id"] == plan.id
    assert out["drift_count"] == 0
    assert len(out["matrix"]) == len(ci.INHERITANCE_MATRIX)
    assert out["haircut"]["composite"] == 0.907911


def test_session_report_resolves_plan_via_run_group():
    db = _db()
    session, plan, _, _ = _seed_bound(db, run_group=True)
    out = live_service.capital_inheritance_report(db, session)
    assert out["source"] == "backtest_run_group"
    assert out["plan_id"] == plan.id
    assert out["run_group_id"] == session.backtest_run_group_id


def test_session_report_missing_plan_returns_warning():
    db = _db()
    session = LiveSession(name="s", mode="simulation", status="stopped",
                          pairing_plan_id=99999)
    db.add(session)
    db.commit()
    out = live_service.capital_inheritance_report(db, session)
    assert out["enabled"] is True and out["found"] is False
    assert out["warnings"] == [{"code": "pairing_plan_missing"}]


def test_session_detail_exposes_inheritance_and_drift_on_param_change():
    db = _db()
    session, _, _, borrower = _seed_bound(db)
    detail = live_service.get_session(db, session.id)
    assert detail["capital_inheritance"]["drift_count"] == 0

    borrower.period = "1w"
    db.commit()
    detail = live_service.get_session(db, session.id)
    assert "borrowers[0].period" in (
        detail["capital_inheritance"]["drift_fields"])
