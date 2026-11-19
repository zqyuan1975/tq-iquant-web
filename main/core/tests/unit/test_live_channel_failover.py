"""B2 主备通道：健康探活 / 人工确认切换 / 主通道离线建议事件。"""
from datetime import datetime
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.engine.live_engine import LiveEngine
from core.models import (
    Base, LiveDecisionEvent, LiveSession, LiveSessionPendingChange,
    PortfolioStrategy,
)
from core.services import live_service


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _session(db, engine="tdx", backup=None, failover=False, status="stopped"):
    p = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    data = {
        "name": "s1", "engine": engine, "execution_mode": "alert",
        "portfolio_ids": [p.id],
    }
    sid = live_service.create_session(db, data, "simulation")["id"]
    session = db.query(LiveSession).filter_by(id=sid).first()
    session.backup_engine = backup
    session.failover_enabled = failover
    db.commit()
    return sid


def _fake_engine():
    return SimpleNamespace(
        bridge_online=True, pending_orders_count=0,
        last_backfill_time=None, reconcile_mismatches=[], _calendar=None,
    )


def test_channel_health_reports_primary_and_backup(monkeypatch):
    db = _db()
    sid = _session(db, engine="tdx", backup="iquant")

    def fake_status(engine: str = None, engine_key: str = None, engines=None):
        online = engine_key == "iquant"
        return {
            "label": {"tdx": "通达信", "iquant": "QMT"}.get(engine_key, engine_key),
            "online": online, "online_source": "probe", "engine_running": False,
            "configured": True, "detail": "x", "stale": False, "checked_at": None,
        }

    monkeypatch.setattr(live_service, "channel_status", fake_status)
    data = live_service.session_channel_health(db, sid, {})
    assert data["primary"]["engine"] == "tdx"
    assert data["primary"]["online"] is False
    assert data["backup"]["engine"] == "iquant"
    assert data["backup"]["online"] is True
    assert data["failover_enabled"] is False

    assert live_service.session_channel_health(db, 99999, {}) is None


def test_switch_stopped_session_applies_immediately_and_records_event():
    db = _db()
    sid = _session(db, engine="tdx", backup="iquant")
    ok, result = live_service.switch_session_channel(db, sid, None, {})
    assert ok is True
    assert (result["from_engine"], result["to_engine"],
            result["effective_at"]) == ("tdx", "iquant", "now")
    assert db.query(LiveSession).filter_by(id=sid).first().engine == "iquant"
    ev = db.query(LiveDecisionEvent).filter_by(
        live_session_id=sid, gate="channel_failover").one()
    assert ev.action == "switch" and ev.layer == "live_gate"


def test_switch_running_session_queues_for_next_start():
    db = _db()
    sid = _session(db, engine="tdx", backup="iquant")
    db.query(LiveSession).filter_by(id=sid).first().status = "running"
    db.commit()
    ok, result = live_service.switch_session_channel(
        db, sid, None, {sid: _fake_engine()})
    assert ok is True and result["effective_at"] == "next_start"
    assert db.query(LiveSession).filter_by(id=sid).first().engine == "tdx"
    row = db.query(LiveSessionPendingChange).filter_by(
        live_session_id=sid, field="engine", status="pending").one()
    assert row.portfolio_strategy_id is None
    assert row.new_value == '"iquant"'
    ev = db.query(LiveDecisionEvent).filter_by(
        live_session_id=sid, gate="channel_failover").one()
    assert ev.action == "switch_pending"


def test_switch_validation_errors():
    db = _db()
    sid = _session(db, engine="tdx", backup=None)
    ok, result = live_service.switch_session_channel(db, sid, None, {})
    assert ok is False and result[0] == 400  # 未配置备用
    ok, result = live_service.switch_session_channel(db, sid, "nope", {})
    assert ok is False and result[0] == 400  # 非法目标
    ok, result = live_service.switch_session_channel(db, sid, "tdx", {})
    assert ok is False and result[0] == 400  # 与主通道相同
    assert live_service.switch_session_channel(db, 99999, "iquant", {})[0] is False


def test_update_session_validates_backup_engine():
    db = _db()
    sid = _session(db, engine="tdx")
    ok, result = live_service.update_session(
        db, sid, {"backup_engine": "nope"}, {})
    assert ok is False and result[0] == 400
    ok, result = live_service.update_session(
        db, sid, {"backup_engine": "tdx"}, {})
    assert ok is False and result[0] == 400
    ok, result = live_service.update_session(
        db, sid, {"backup_engine": "iquant", "failover_enabled": True}, {})
    assert ok is True
    session = db.query(LiveSession).filter_by(id=sid).first()
    assert session.backup_engine == "iquant" and session.failover_enabled is True


def test_engine_advises_failover_once_per_day():
    db = _db()
    engine = SimpleNamespace(
        session_id=1, backup_engine="iquant", _failover_enabled=True,
        _failover_advised_day=None, _db_session_factory=lambda: db,
    )
    LiveEngine._maybe_advise_failover(engine, datetime(2026, 11, 18, 10, 0))
    LiveEngine._maybe_advise_failover(engine, datetime(2026, 11, 18, 10, 1))
    LiveEngine._maybe_advise_failover(engine, datetime(2026, 11, 19, 10, 0))
    rows = db.query(LiveDecisionEvent).filter_by(
        live_session_id=1, gate="channel_failover").all()
    assert len(rows) == 2
    assert "iquant" in rows[0].message


def test_engine_failover_advice_disabled_by_default():
    db = _db()
    engine = SimpleNamespace(
        session_id=1, backup_engine="iquant", _failover_enabled=False,
        _failover_advised_day=None, _db_session_factory=lambda: db,
    )
    LiveEngine._maybe_advise_failover(engine, datetime(2026, 11, 18, 10, 0))
    assert db.query(LiveDecisionEvent).filter_by(
        gate="channel_failover").count() == 0
