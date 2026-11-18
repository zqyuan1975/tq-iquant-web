"""D3 待生效风控参数队列单元测试。"""
from decimal import Decimal

import pytest

from core.models import (
    Formula, LiveSession, LiveSessionPortfolio, PortfolioStrategy, StockPool,
    Strategy,
)
from core.services import live_service


def _seed(db):
    pool = StockPool(code="P1", name="pool")
    db.add(pool)
    db.flush()
    formula = Formula(name="f1", content="CLOSE")
    db.add(formula)
    db.flush()
    ps = PortfolioStrategy(name="组合", stock_pool_id=pool.id,
                           max_drawdown=Decimal("0.20"),
                           daily_loss_limit=Decimal("0.05"))
    db.add(ps)
    db.flush()
    strat = Strategy(portfolio_id=ps.id, name="s1", formula_id=formula.id,
                     period="1m", role="master",
                     stop_loss_ratio=Decimal("0.05"))
    db.add(strat)
    session = LiveSession(name="会话", mode="live", status="stopped")
    db.add(session)
    db.flush()
    db.add(LiveSessionPortfolio(session_id=session.id,
                                portfolio_strategy_id=ps.id))
    db.commit()
    return session, ps, strat


def test_stopped_session_applies_immediately(db_session):
    session, ps, _ = _seed(db_session)
    ok, data = live_service.enqueue_pending_change(
        db_session, session.id,
        {"portfolio_strategy_id": ps.id, "field": "max_drawdown",
         "value": "0.12"}, {})
    assert ok is True
    assert data["status"] == "applied"
    assert data["effective_at"] == "now"
    db_session.refresh(ps)
    assert Decimal(str(ps.max_drawdown)) == Decimal("0.12")


def test_running_session_queues_until_start(db_session):
    session, ps, _ = _seed(db_session)
    engines = {session.id: object()}
    ok, data = live_service.enqueue_pending_change(
        db_session, session.id,
        {"portfolio_strategy_id": ps.id, "field": "daily_loss_limit",
         "value": "0.03"}, engines)
    assert ok is True
    assert data["status"] == "pending"
    db_session.refresh(ps)
    assert Decimal(str(ps.daily_loss_limit)) == Decimal("0.05")

    rows = live_service.list_pending_changes(db_session, session.id)
    assert len(rows) == 1 and rows[0]["status"] == "pending"

    applied = live_service.apply_pending_changes(db_session, session.id)
    assert applied == 1
    db_session.refresh(ps)
    assert Decimal(str(ps.daily_loss_limit)) == Decimal("0.03")
    rows = live_service.list_pending_changes(db_session, session.id)
    assert rows[0]["status"] == "applied"


def test_strategy_level_field(db_session):
    session, ps, strat = _seed(db_session)
    engines = {session.id: object()}
    ok, data = live_service.enqueue_pending_change(
        db_session, session.id,
        {"portfolio_strategy_id": ps.id, "strategy_id": strat.id,
         "field": "stop_loss_ratio", "value": "0.08"}, engines)
    assert ok is True and data["status"] == "pending"
    live_service.apply_pending_changes(db_session, session.id)
    db_session.refresh(strat)
    assert Decimal(str(strat.stop_loss_ratio)) == Decimal("0.08")


def test_duplicate_pending_updates_single_row(db_session):
    session, ps, _ = _seed(db_session)
    engines = {session.id: object()}
    for value in ("0.12", "0.10"):
        ok, _ = live_service.enqueue_pending_change(
            db_session, session.id,
            {"portfolio_strategy_id": ps.id, "field": "max_drawdown",
             "value": value}, engines)
        assert ok is True
    rows = live_service.list_pending_changes(db_session, session.id)
    assert len(rows) == 1
    assert rows[0]["new_value"] == "0.10"


def test_invalid_field_and_value(db_session):
    session, ps, _ = _seed(db_session)
    ok, error = live_service.enqueue_pending_change(
        db_session, session.id,
        {"portfolio_strategy_id": ps.id, "field": "unknown",
         "value": "0.1"}, {})
    assert ok is False and error[0] == 400
    ok, error = live_service.enqueue_pending_change(
        db_session, session.id,
        {"portfolio_strategy_id": ps.id, "field": "max_drawdown",
         "value": "1.5"}, {})
    assert ok is False and error[0] == 400


def test_discard_pending(db_session):
    session, ps, _ = _seed(db_session)
    engines = {session.id: object()}
    ok, data = live_service.enqueue_pending_change(
        db_session, session.id,
        {"portfolio_strategy_id": ps.id, "field": "max_drawdown",
         "value": "0.10"}, engines)
    assert ok is True
    ok, result = live_service.discard_pending_change(
        db_session, session.id, data["id"], engines)
    assert ok is True and result["status"] == "discarded"
    db_session.refresh(ps)
    assert Decimal(str(ps.max_drawdown)) == Decimal("0.20")
    assert live_service.apply_pending_changes(db_session, session.id) == 0
