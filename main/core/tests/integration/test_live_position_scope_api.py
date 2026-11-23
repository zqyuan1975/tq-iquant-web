"""按标的授权代卖 API：放开/收回 + 审计事件（只做留痕，不改全局 sell_scope）。"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.main import app
from core.db import get_db
from core.models import Base, LiveDecisionEvent, LivePositionScope, LiveSession


@pytest.fixture
def client(tmp_path):
    engine = create_engine(
        "sqlite:///%s" % (tmp_path / "scope.db").as_posix(),
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c, Session
    app.dependency_overrides.clear()


def _seed_session(Session, sid=1):
    db = Session()
    db.add(LiveSession(id=sid, name="会话#1", mode="simulation",
                       status="running", sell_scope=None))
    db.commit()
    db.close()


def test_grant_and_revoke_writes_audit(client):
    c, Session = client
    _seed_session(Session)

    # 默认无记录 → 不代卖
    body = c.get("/api/live-position-scope/1").json()
    assert body["code"] == 0
    assert body["data"]["enabled_codes"] == []

    # 放开（代码大小写归一）
    body = c.put("/api/live-position-scope/1/002745.sz",
                 json={"enabled": True}).json()
    assert body["code"] == 0
    assert body["data"]["code"] == "002745.SZ"
    assert body["data"]["enabled"] is True
    assert body["data"]["changed"] is True
    assert body["data"]["effective_at"] == "next_tick"

    body = c.get("/api/live-position-scope/1").json()
    assert body["data"]["enabled_codes"] == ["002745.SZ"]

    # 收回：保留行、enabled=0
    body = c.put("/api/live-position-scope/1/002745.SZ",
                 json={"enabled": False}).json()
    assert body["data"]["enabled"] is False
    assert body["data"]["changed"] is True

    body = c.get("/api/live-position-scope/1").json()
    assert body["data"]["enabled_codes"] == []
    assert body["data"]["items"][0]["enabled"] is False
    assert body["data"]["items"][0]["updated_by"] == "无鉴权，无法识别操作者"

    # 审计：两条事件（grant/revoke），已置 acked 不计入待处理
    db = Session()
    rows = (db.query(LiveDecisionEvent)
            .filter(LiveDecisionEvent.live_session_id == 1)
            .order_by(LiveDecisionEvent.id).all())
    assert [r.action for r in rows] == ["grant", "revoke"]
    assert all(r.gate == "sell_scope_auth" for r in rows)
    assert all(r.ack_status == "acked" for r in rows)
    assert all("002745.SZ" in (r.message or "") for r in rows)
    assert all("无鉴权，无法识别操作者" in (r.message or "") for r in rows)
    db.close()

    # 重复收回：幂等，changed=False
    body = c.put("/api/live-position-scope/1/002745.SZ",
                 json={"enabled": False}).json()
    assert body["data"]["changed"] is False


def test_scope_api_validates_input(client):
    c, Session = client
    _seed_session(Session)
    assert c.get("/api/live-position-scope/999").json()["code"] == 404
    assert c.put("/api/live-position-scope/999/002745.SZ",
                 json={"enabled": True}).json()["code"] == 404
    assert c.put("/api/live-position-scope/1/002745.SZ",
                 json={}).json()["code"] == 400
    assert c.put("/api/live-position-scope/1/%20",
                 json={"enabled": True}).json()["code"] in (400, 404)
