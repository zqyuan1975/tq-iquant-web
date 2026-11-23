"""数据与口径观测汇总 + 自动审计触发（P0-1，2026-11-23）。"""
from datetime import date

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.db import get_db
from core.main import app
from core.models import (
    BacktestRecord, Base, CorporateActionDiagnostic, DIAG_UNVERIFIED_EVENT,
)
from core.services import data_observability as obs


def _add_diag(db, code="600211.SH", day=date(2026, 6, 11)):
    db.add(CorporateActionDiagnostic(
        diagnostic_type=DIAG_UNVERIFIED_EVENT, code=code, break_date=day,
        source="tq:close_raw_vs_front"))
    db.commit()


def _add_record(db, name, snapshot):
    import json
    raw = snapshot if isinstance(snapshot, str) else (
        json.dumps(snapshot) if snapshot is not None else None)
    rec = BacktestRecord(
        portfolio_strategy_id=1, name=name,
        start_date=date(2026, 1, 1), end_date=date(2026, 2, 1),
        status="completed", params_snapshot=raw)
    db.add(rec)
    db.commit()
    return rec


def test_summary_counts_unverified_and_stale(db_session):
    _add_diag(db_session, "600211.SH")
    _add_diag(db_session, "600212.SH", date(2026, 7, 1))
    _add_record(db_session, "ok", {
        "dividend_mode": "exact",
        "price_caliber": {"version": "price_caliber_v2"},
    })
    _add_record(db_session, "stale-mode", {
        "dividend_mode": "front", "price_caliber_version": "price_caliber_v2",
    })
    _add_record(db_session, "stale-version", {"dividend_mode": "exact"})
    _add_record(db_session, "missing", None)

    out = obs.observability_summary(db_session)

    assert out["corporate_actions"]["unverified"] == 2
    assert out["corporate_actions"]["codes"] == ["600211.SH", "600212.SH"]
    assert out["corporate_actions"]["last_audit"] is None
    assert out["caliber"]["records"] == 4
    assert out["caliber"]["stale"] == 3
    assert sum(out["caliber"]["reasons"].values()) == 3
    assert "kline_coverage" in out


def test_summary_empty_db(db_session):
    out = obs.observability_summary(db_session)

    assert out["corporate_actions"]["unverified"] == 0
    assert out["caliber"] == {"records": 0, "stale": 0, "reasons": {}}


def test_auto_audit_disabled_by_env(monkeypatch):
    monkeypatch.setenv("TQ_AUTO_CA_AUDIT_DISABLED", "1")

    assert obs.auto_audit_enabled() is False
    assert obs.trigger_auto_audit(["600211.SH"], reason="test") is False


def test_trigger_auto_audit_dispatches_thread(monkeypatch):
    monkeypatch.delenv("TQ_AUTO_CA_AUDIT_DISABLED", raising=False)
    monkeypatch.setattr(obs, "auto_audit_enabled", lambda: True)
    calls = {}
    monkeypatch.setattr(obs, "run_auto_audit",
                        lambda codes, **kw: calls.update(codes=list(codes), **kw))

    assert obs.trigger_auto_audit(["600211.SH"], reason="backtest_start") is True
    # 等待后台线程（短超时，避免用例依赖调度）
    import time
    for _ in range(50):
        if calls:
            break
        time.sleep(0.01)
    assert calls.get("codes") == ["600211.SH"]
    assert calls.get("reason") == "backtest_start"


@pytest.fixture
def client_and_session():
    """内存 SQLite（StaticPool，跨线程可用）+ TestClient，覆盖 get_db。"""
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False},
        poolclass=StaticPool)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c, Session
    app.dependency_overrides.clear()


def test_summary_api_ok(client_and_session):
    client, Session = client_and_session
    with Session() as db:
        _add_diag(db, "600211.SH")

    resp = client.get("/api/data-observability/summary")

    assert resp.status_code == 200
    body = resp.json()
    assert body["code"] == 0
    assert body["data"]["corporate_actions"]["unverified"] == 1


def test_run_auto_audit_skips_when_disabled(monkeypatch):
    monkeypatch.setattr(
        "core.services.corporate_action_audit.audit_config",
        lambda: {"enabled": False, "lookback_days": 250})

    out = obs.run_auto_audit(["600211.SH"], reason="test")

    assert out["skipped"] is True
    assert "enabled=false" in out["reason"]
