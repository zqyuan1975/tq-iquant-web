"""C1 引擎独占 / C2 账户独占 / 会话上限 / 建会话软校验（A1、2026-11-16）。

全部用两行会话数据 + mock，不启动真实引擎、不连交易通道。
"""
from datetime import datetime, timedelta

from core.models import LiveSession
from core.services import live_service as svc
from core.services.live_service import (
    MAX_ACTIVE_LIVE_SESSIONS,
    _check_account_conflict,
    _check_channel_conflict,
    _occupied_sessions,
    create_session,
    resolve_account_key,
    start_session,
)


def _mk(db, name="s", *, engine="tdx", status="stopped", account_key=None,
        started_at=None, stopped_at=None):
    row = LiveSession(
        name=name, mode="simulation", engine=engine, status=status,
        execution_mode="alert", account_key=account_key,
        started_at=started_at, stopped_at=stopped_at,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


# ---------------------------------------------------------------------------
# C2 账户键解析三态：桥 /ping → config → 降级 engine:<key>
# ---------------------------------------------------------------------------

def test_resolve_account_key_bridge_ping(monkeypatch):
    dispatcher = type("D", (), {"last_ping": {"account": "410038214257"}})()
    monkeypatch.setattr(svc, "load_config", lambda: {})
    assert resolve_account_key("iquant", dispatcher) == (
        "iquant:410038214257", "bridge")


def test_resolve_account_key_config_fallback(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: {
        "iquant_bridge": {"account_id": "410038214257"},
    })
    assert resolve_account_key("iquant", None) == (
        "iquant:410038214257", "config")


def test_resolve_account_key_degraded(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: {})
    monkeypatch.setattr(svc, "tq_trade_config", lambda: {})
    assert resolve_account_key("tdx") == ("engine:tdx", "degraded")
    assert resolve_account_key("goldminer", None) == (
        "engine:goldminer", "degraded")


# ---------------------------------------------------------------------------
# C1 引擎独占 409 结构
# ---------------------------------------------------------------------------

def test_engine_conflict_returns_409_with_occupied_by(db_session):
    peer = _mk(db_session, "peer", engine="tdx", status="running",
               started_at=datetime.now())
    me = _mk(db_session, "me", engine="tdx")
    err = _check_channel_conflict(db_session, me, {})
    assert err is not None
    assert err[0] == 409
    assert err[2]["rule"] == "engine_exclusive"
    assert err[2]["occupied_by"]["session_id"] == peer.id


# ---------------------------------------------------------------------------
# C2 混合引擎同账户互斥
# ---------------------------------------------------------------------------

def test_account_conflict_across_engines(db_session):
    peer = _mk(db_session, "peer", engine="tdx", status="running",
               account_key="shared:8888", started_at=datetime.now())
    me = _mk(db_session, "me", engine="ths")
    err = _check_account_conflict(db_session, me, {}, "shared:8888")
    assert err is not None
    assert err[0] == 409
    assert err[2]["rule"] == "account_exclusive"
    assert err[2]["occupied_by"]["session_id"] == peer.id


def test_account_conflict_ignores_different_account(db_session):
    _mk(db_session, "peer", engine="tdx", status="running",
        account_key="tdx:111", started_at=datetime.now())
    me = _mk(db_session, "me", engine="ths")
    assert _check_account_conflict(db_session, me, {}, "ths:222") is None


# ---------------------------------------------------------------------------
# stopping 计入占用 / 120s 僵尸释放
# ---------------------------------------------------------------------------

def test_stopping_session_counts_as_occupied(db_session):
    peer = _mk(db_session, "peer", engine="tdx", status="stopping",
               started_at=datetime.now(), stopped_at=datetime.now())
    me = _mk(db_session, "me", engine="tdx")
    assert peer.id in _occupied_sessions(db_session, {}, me.id)


def test_stale_stopping_released_after_120s(db_session):
    peer = _mk(db_session, "peer", engine="tdx", status="stopping",
               started_at=datetime.now() - timedelta(seconds=200),
               stopped_at=datetime.now() - timedelta(seconds=121))
    me = _mk(db_session, "me", engine="tdx")
    occupied = _occupied_sessions(db_session, {}, me.id)
    assert peer.id not in occupied
    db_session.refresh(peer)
    assert peer.status == "stopped"


# ---------------------------------------------------------------------------
# A1 会话总量硬上限
# ---------------------------------------------------------------------------

def test_start_session_rejects_fifth_active_session(db_session):
    for idx, eng in enumerate(("tdx", "ths", "iquant", "goldminer")):
        _mk(db_session, "p%d" % idx, engine=eng, status="running",
            started_at=datetime.now())
    me = _mk(db_session, "me", engine="tdx")
    engine, session, err = start_session(
        db_session, me.id, {}, "simulation")
    assert engine is None and session is not None and err is not None
    assert err[0] == 409
    assert err[2]["rule"] == "session_limit"
    assert err[2]["limit"] == MAX_ACTIVE_LIVE_SESSIONS
    assert len(err[2]["occupied_by"]) == MAX_ACTIVE_LIVE_SESSIONS


def test_start_session_limit_not_triggered_at_three(db_session):
    for idx, eng in enumerate(("tdx", "ths", "iquant")):
        _mk(db_session, "p%d" % idx, engine=eng, status="running",
            started_at=datetime.now())
    me = _mk(db_session, "me", engine="goldminer")
    # 未到上限：应继续进入 C1 校验（此处无同引擎占用，返回的下一个门是空池 503）
    engine, session, err = start_session(
        db_session, me.id, {}, "simulation")
    assert engine is None
    assert err is not None and err[0] != 409


# ---------------------------------------------------------------------------
# A1 建会话软校验（warnings，不阻断）
# ---------------------------------------------------------------------------

def test_create_session_returns_warning_when_engine_occupied(db_session):
    _mk(db_session, "peer", engine="tdx", status="running",
        started_at=datetime.now())
    out = create_session(db_session, {
        "name": "new", "engine": "tdx", "portfolio_ids": [],
    }, "simulation")
    assert out["warnings"]
    assert "启动前需先停止" in out["warnings"][0]
    assert out["status"] == "stopped"


def test_create_session_no_warning_when_engine_free(db_session):
    out = create_session(db_session, {
        "name": "new", "engine": "iquant", "portfolio_ids": [],
    }, "simulation")
    assert out["warnings"] == []
