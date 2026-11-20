"""F3 影子引擎单元测试：只记录「计划 vs 实际」，订单路径零变化。"""
import json
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.engine.capital_shadow import SHADOW_VERSION, build_shadow_payload
from core.engine.live_engine import LiveEngine
from core.models import Base, LiveCapitalShadow


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def test_payload_plan_vs_actual_and_haircut():
    payload = build_shadow_payload(
        portfolio_id=3,
        bar_time=datetime(2026, 11, 20, 10, 0),
        loan_plan={
            "plan_id": 7, "lender_strategy_id": 1,
            "borrowers": [{"strategy_id": 2}],
            "principal": "100000", "due_days": 30,
            "recall_notice_days": 2,
        },
        members=[{"strategy_id": 1, "role": "主"}],
        cash="40000", total_value="55000",
        haircut={"composite": 0.907911},
        drift_fields=["borrowers[0].period"],
    )
    assert payload["version"] == SHADOW_VERSION
    assert payload["plan"]["principal"] == "100000.00"
    assert payload["plan"]["usable_principal"] == "90791.10"
    assert payload["actual"]["cash"] == "40000.00"
    assert payload["actual"]["positions_value"] == "15000.00"
    assert payload["actual"]["loan_ledger"] == "g1_not_enabled"
    assert payload["gap"] == "50791.10"
    assert payload["recall"]["due_days"] == 30
    assert payload["recall"]["notice_days"] == 2
    assert payload["orders_unchanged"] is True
    codes = [w["code"] for w in payload["warnings"]]
    assert codes == ["param_drift"]


def test_payload_handles_missing_plan_values():
    payload = build_shadow_payload(
        portfolio_id=1, bar_time=None, loan_plan=None, members=None,
        cash=None, total_value=None, haircut=None)
    assert payload["plan"]["principal"] is None
    assert payload["plan"]["usable_principal"] is None
    assert payload["actual"]["cash"] is None
    assert payload["gap"] is None
    assert payload["warnings"] == []


def _engine_stub(shadow):
    return SimpleNamespace(session_id=11, _capital_shadow=shadow)


def _portfolio_stub(cash="1000", total="1800", ids=(1, 2)):
    return SimpleNamespace(
        portfolio_id=9,
        strategies=[SimpleNamespace(strategy_id=i) for i in ids],
        account=SimpleNamespace(cash=cash),
        total_value=lambda bar: total,
    )


_SHADOW = {
    "plan_id": 7,
    "loan_plan": {
        "plan_id": 7, "principal": "50000", "due_days": 30,
        "borrowers": [{"strategy_id": 2}],
    },
    "members": [{"strategy_id": 2, "role": "从"}],
    "haircut": {"composite": 0.9},
    "drift_fields": [],
    "warning_codes": [],
}


def test_disabled_shadow_does_not_touch_db():
    db = _db()
    engine = _engine_stub(None)
    LiveEngine._maybe_record_capital_shadow(
        engine, db, _portfolio_stub(), SimpleNamespace(bar_time=None))
    assert db.query(LiveCapitalShadow).count() == 0


def test_enabled_shadow_writes_one_row():
    db = _db()
    engine = _engine_stub(dict(_SHADOW))
    LiveEngine._maybe_record_capital_shadow(
        engine, db, _portfolio_stub(), SimpleNamespace(bar_time=None))
    rows = db.query(LiveCapitalShadow).all()
    assert len(rows) == 1
    row = rows[0]
    assert row.live_session_id == 11 and row.portfolio_id == 9
    assert row.version == SHADOW_VERSION
    payload = json.loads(row.payload_json)
    assert payload["plan"]["plan_id"] == 7
    assert payload["actual"]["cash"] == "1000.00"
    assert payload["plan"]["usable_principal"] == "45000.00"
    assert payload["orders_unchanged"] is True


def test_shadow_skips_non_member_portfolio():
    db = _db()
    engine = _engine_stub(dict(_SHADOW))
    LiveEngine._maybe_record_capital_shadow(
        engine, db, _portfolio_stub(ids=(8, 9)), SimpleNamespace(bar_time=None))
    assert db.query(LiveCapitalShadow).count() == 0


def test_shadow_failure_is_swallowed():
    db = _db()
    engine = _engine_stub(dict(_SHADOW))

    def boom(bar):
        raise RuntimeError("valuation failed")

    portfolio = _portfolio_stub()
    portfolio.total_value = boom
    # 不抛出：影子失败不得影响交易主链路
    LiveEngine._maybe_record_capital_shadow(
        engine, db, portfolio, SimpleNamespace(bar_time=None))
    assert db.query(LiveCapitalShadow).count() == 0
