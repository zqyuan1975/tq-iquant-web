"""B3 通道级守护：检查清单 / 一键重新探活。"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import (
    Base, LiveSession, LiveSessionPendingChange, PortfolioStrategy,
)
from core.services import live_service


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _session(db, engine="tdx", backup=None, status="stopped"):
    p = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    sid = live_service.create_session(db, {
        "name": "s1", "engine": engine, "execution_mode": "alert",
        "portfolio_ids": [p.id],
    }, "simulation")["id"]
    session = db.query(LiveSession).filter_by(id=sid).first()
    session.backup_engine = backup
    session.status = status
    db.commit()
    return sid


def _fake_status(online_map):
    def fake(engine: str = None, engine_key: str = None, engines=None,
             probe_deadline=None):
        return {
            "label": engine_key,
            "online": online_map.get(engine_key, False),
            "online_source": "probe", "engine_running": False,
            "configured": True, "detail": "", "stale": False,
            "checked_at": None,
        }
    return fake


def test_guard_lists_checks_and_actions(monkeypatch):
    db = _db()
    sid = _session(db, engine="tdx", backup="iquant")
    monkeypatch.setattr(live_service, "channel_status",
                        _fake_status({"tdx": False, "iquant": True}))
    data = live_service.session_channel_guard(db, sid, {})
    checks = {c["key"]: c for c in data["checks"]}
    assert data["guard_mode"] == "advisory"
    assert checks["client_online"]["ok"] is False
    assert checks["backup_ready"]["ok"] is True
    assert checks["pending_changes"]["ok"] is True
    assert data["ready_to_switch"] is True
    assert any("通达信" in a for a in data["actions"])

    assert live_service.session_channel_guard(db, 99999, {}) is None


def test_guard_skips_backup_check_for_stopped_session(monkeypatch):
    """停止态不判备用通道：未配置/离线都不再制造假告警（2026-11-19）。"""
    db = _db()
    sid = _session(db, engine="tdx", backup=None, status="stopped")
    monkeypatch.setattr(live_service, "channel_status",
                        _fake_status({"tdx": True}))
    data = live_service.session_channel_guard(db, sid, {})
    checks = {c["key"]: c for c in data["checks"]}
    assert checks["backup_ready"]["ok"] is True
    assert "未运行" in checks["backup_ready"]["detail"]


def test_guard_flags_running_session_without_engine(monkeypatch):
    db = _db()
    sid = _session(db, engine="tdx", status="running")
    monkeypatch.setattr(live_service, "channel_status",
                        _fake_status({"tdx": True}))
    data = live_service.session_channel_guard(db, sid, {})
    checks = {c["key"]: c for c in data["checks"]}
    assert checks["engine_registered"]["ok"] is False
    assert any("重新启动" in a for a in data["actions"])


def test_guard_counts_pending_session_changes(monkeypatch):
    db = _db()
    sid = _session(db, engine="tdx")
    db.add(LiveSessionPendingChange(
        live_session_id=sid, portfolio_strategy_id=None,
        field="engine", new_value='"iquant"', status="pending"))
    db.commit()
    monkeypatch.setattr(live_service, "channel_status",
                        _fake_status({"tdx": True}))
    data = live_service.session_channel_guard(db, sid, {})
    checks = {c["key"]: c for c in data["checks"]}
    assert checks["pending_changes"]["ok"] is False
    assert "1 项" in checks["pending_changes"]["detail"]


def test_reconnect_online(monkeypatch):
    db = _db()
    sid = _session(db, engine="tdx")
    monkeypatch.setattr(live_service, "_force_channel_probe",
                        lambda key: (True, False, "ok"))
    ok, data = live_service.reconnect_session_channel(db, sid, {})
    assert ok is True
    assert data["online"] is True and data["manual_action"] == ""


def test_reconnect_offline_returns_manual_hint(monkeypatch):
    db = _db()
    sid = _session(db, engine="ths")
    monkeypatch.setattr(live_service, "_force_channel_probe",
                        lambda key: (False, False, "ok"))
    ok, data = live_service.reconnect_session_channel(db, sid, {})
    assert ok is True
    assert data["online"] is False
    assert "同花顺" in data["manual_action"]
    assert live_service.reconnect_session_channel(db, 99999, {})[0] is False
