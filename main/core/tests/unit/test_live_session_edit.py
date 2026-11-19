"""B1 + A4 会话配置编辑 / 组合增删 单元测试。"""
import json
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from datetime import date

from core.models import (
    Base, BacktestRecord, LiveSession, LiveSessionPendingChange,
    LiveSessionPortfolio, PortfolioStrategy,
)
from core.services import live_service


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _portfolio(db, name="p1"):
    p = PortfolioStrategy(name=name, stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    return p


def _session(db, pids, engine="tdx", status="stopped"):
    data = {
        "name": "s1", "engine": engine, "execution_mode": "alert",
        "portfolio_ids": pids, "capital_pool_mode": "off",
    }
    return live_service.create_session(db, data, "simulation")


def _fake_engine():
    return SimpleNamespace(
        bridge_online=True, pending_orders_count=0,
        last_backfill_time=None, reconcile_mismatches=[], _calendar=None,
    )


def test_stopped_session_edit_applies_immediately_and_syncs_portfolios():
    db = _db()
    p1, p2 = _portfolio(db, "p1"), _portfolio(db, "p2")
    sid = _session(db, [p1.id])["id"]

    ok, result = live_service.update_session(
        db, sid,
        {"name": "renamed", "engine": "iquant", "execution_mode": "auto",
         "portfolio_ids": [p2.id], "capital_pool_mode": "equal"},
        {},
    )
    assert ok is True
    assert result["effective_at"] == "now"
    assert set(result["applied"]) == {
        "name", "engine", "execution_mode", "portfolio_ids", "capital_pool_mode"}
    session = db.query(LiveSession).filter_by(id=sid).first()
    assert (session.name, session.engine, session.execution_mode,
            session.capital_pool_mode) == ("renamed", "iquant", "auto", "equal")
    assert result["session"]["portfolio_ids"] == [p2.id]


def test_running_session_edit_queues_except_name():
    db = _db()
    p1, p2 = _portfolio(db, "p1"), _portfolio(db, "p2")
    sid = _session(db, [p1.id])["id"]
    engines = {sid: _fake_engine()}

    ok, result = live_service.update_session(
        db, sid,
        {"name": "renamed", "engine": "iquant", "portfolio_ids": [p2.id]},
        engines,
    )
    assert ok is True
    assert result["effective_at"] == "next_start"
    assert result["applied"] == ["name"]
    assert set(result["pending"]) == {"engine", "portfolio_ids"}
    session = db.query(LiveSession).filter_by(id=sid).first()
    assert session.name == "renamed"
    assert session.engine == "tdx"  # 运行中不改
    rows = db.query(LiveSessionPendingChange).filter_by(
        live_session_id=sid, status="pending").all()
    assert {r.field for r in rows} == {"engine", "portfolio_ids"}
    assert json.loads(next(r for r in rows if r.field == "engine").new_value) == "iquant"


def test_pending_session_changes_applied_before_next_start():
    db = _db()
    p1, p2 = _portfolio(db, "p1"), _portfolio(db, "p2")
    sid = _session(db, [p1.id])["id"]
    live_service.update_session(
        db, sid, {"engine": "iquant", "portfolio_ids": [p2.id]},
        {sid: _fake_engine()},
    )
    applied = live_service.apply_pending_changes(db, sid)
    assert applied == 2
    session = db.query(LiveSession).filter_by(id=sid).first()
    assert session.engine == "iquant"
    assert [l.portfolio_strategy_id for l in
            db.query(LiveSessionPortfolio)
            .filter_by(session_id=sid).all()] == [p2.id]
    assert db.query(LiveSessionPendingChange).filter_by(
        live_session_id=sid, status="pending").count() == 0


def test_pending_session_change_dedupes_same_field():
    db = _db()
    p1 = _portfolio(db)
    sid = _session(db, [p1.id])["id"]
    engines = {sid: _fake_engine()}
    live_service.update_session(db, sid, {"engine": "iquant"}, engines)
    live_service.update_session(db, sid, {"engine": "goldminer"}, engines)
    rows = db.query(LiveSessionPendingChange).filter_by(
        live_session_id=sid, field="engine", status="pending").all()
    assert len(rows) == 1
    assert json.loads(rows[0].new_value) == "goldminer"


def test_backtest_record_binding_is_editable_and_applies_while_running():
    db = _db()
    p1 = _portfolio(db)
    sid = _session(db, [p1.id])["id"]
    rec = BacktestRecord(
        portfolio_strategy_id=p1.id, name="回测基准", status="completed",
        start_date=date(2026, 1, 1), end_date=date(2026, 11, 16),
    )
    db.add(rec)
    db.commit()

    # 运行中也立即生效（纯观测字段，不进下次启动队列）
    session = db.query(LiveSession).filter_by(id=sid).first()
    session.status = "running"
    db.commit()
    ok, result = live_service.update_session(
        db, sid, {"backtest_record_id": rec.id}, {sid: _fake_engine()})
    assert ok is True
    assert result["effective_at"] == "now"
    assert "backtest_record_id" in result["applied"]
    assert result["session"]["backtest_record_id"] == rec.id
    assert db.query(LiveSession).filter_by(id=sid).first().backtest_record_id == rec.id

    # 置空 = 回到按组合自动关联
    ok, result = live_service.update_session(
        db, sid, {"backtest_record_id": None}, {sid: _fake_engine()})
    assert ok is True and result["session"]["backtest_record_id"] is None

    ok, result = live_service.update_session(
        db, sid, {"backtest_record_id": 99999}, {})
    assert ok is False and result[0] == 400


def test_update_session_validation_errors():
    db = _db()
    p1 = _portfolio(db)
    sid = _session(db, [p1.id])["id"]

    ok, result = live_service.update_session(db, 99999, {"name": "x"}, {})
    assert ok is False and result[0] == 404

    for body in ({"name": "  "},
                 {"engine": "nope"},
                 {"execution_mode": "nope"},
                 {"capital_pool_mode": "nope"},
                 {"portfolio_ids": "x"},
                 {"portfolio_ids": [p1.id, p1.id]},
                 {"portfolio_ids": [99999]},
                 {"advance_close_time": "15:30"},
                 {"unknown": 1}):
        ok, result = live_service.update_session(db, sid, body, {})
        assert ok is False and result[0] == 400, body


def test_update_session_signal_path_blank_becomes_none():
    db = _db()
    p1 = _portfolio(db)
    sid = _session(db, [p1.id])["id"]
    ok, _ = live_service.update_session(db, sid, {"signal_file_path": "  "}, {})
    assert ok is True
    assert db.query(LiveSession).filter_by(id=sid).first().signal_file_path is None
