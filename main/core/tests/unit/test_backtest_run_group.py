"""多组合回测批（0021 E3）：拼盘绑定、聚合冻结、组级取消。"""
from datetime import date

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.models import Base, BacktestRecord, BacktestRunGroup
from core.services import backtest_cancel
from core.services import backtest_service as svc


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    db = Session()
    try:
        yield db
    finally:
        db.close()
        engine.dispose()


def _group(db, status="running", pairing_plan_id=None):
    g = BacktestRunGroup(
        name="拼盘批", status=status, pairing_plan_id=pairing_plan_id)
    db.add(g)
    db.commit()
    db.refresh(g)
    return g


def _record(db, group_id, status="running"):
    r = BacktestRecord(
        portfolio_strategy_id=1, name="成员",
        start_date=date(2026, 1, 1), end_date=date(2026, 2, 1),
        status=status, run_group_id=group_id,
    )
    db.add(r)
    db.commit()
    db.refresh(r)
    return r


def test_serialize_group_exposes_pairing_and_frozen_aggregate(db_session):
    g = _group(db_session, pairing_plan_id=42)
    g.aggregate_json = '{"dates": ["2026-01-01"], "values": ["100000"]}'
    db_session.commit()

    payload = svc._serialize_run_group(g)
    assert payload["pairing_plan_id"] == 42
    assert payload["aggregate_frozen"] is True


def test_group_cancel_sets_shared_token_for_all_members(db_session):
    g = _group(db_session)
    r1 = _record(db_session, g.id)
    r2 = _record(db_session, g.id)
    event = backtest_cancel.register([r1.id, r2.id])
    try:
        result = svc.cancel_run_group(db_session, g.id)
        assert result["running_members"] == [r1.id, r2.id]
        assert result["cancel_requested"] == [r1.id, r2.id]
        assert result["all_batch"] is True
        assert event.is_set()
    finally:
        backtest_cancel.release([r1.id, r2.id])


def test_group_cancel_without_running_members_is_noop(db_session):
    g = _group(db_session, status="completed")
    _record(db_session, g.id, status="completed")

    result = svc.cancel_run_group(db_session, g.id)
    assert result["running_members"] == []
    assert result["cancel_requested"] == []


def test_group_cancel_missing_group_returns_none(db_session):
    assert svc.cancel_run_group(db_session, 99999) is None
