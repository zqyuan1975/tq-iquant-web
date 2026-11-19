"""C3 键鼠互斥（tdx/ths）：软拦 ack 真校验 + 硬拦（A2，2026-11-16）。

约束：无 ack → 409；带 ack → 放行且留痕；硬拦 → ack 也不放行。
"""
from datetime import datetime

from core.models import LiveDecisionEvent, LiveSession
from core.services import live_service as svc
from core.services.live_service import _check_keyboard_conflict


def _mk(db, name, *, engine, status="stopped"):
    row = LiveSession(
        name=name, mode="simulation", engine=engine, status=status,
        execution_mode="alert",
        started_at=datetime.now() if status in ("running", "stopping") else None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def test_soft_block_without_ack_returns_409(db_session, monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: {"tq_trade": {}})
    peer = _mk(db_session, "peer", engine="tdx", status="running")
    me = _mk(db_session, "me", engine="ths")
    err = _check_keyboard_conflict(db_session, me, {}, False)
    assert err is not None
    assert err[0] == 409
    assert err[2]["rule"] == "keyboard_exclusive"
    assert err[2]["requires_ack"] is True
    assert err[2]["hard_block"] is False
    assert err[2]["occupied_by"]["session_id"] == peer.id


def test_soft_block_with_ack_passes_and_records_event(db_session, monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: {"tq_trade": {}})
    peer = _mk(db_session, "peer", engine="tdx", status="running")
    me = _mk(db_session, "me", engine="ths")
    assert _check_keyboard_conflict(db_session, me, {}, True) is None
    events = (
        db_session.query(LiveDecisionEvent)
        .filter(LiveDecisionEvent.live_session_id == me.id)
        .all()
    )
    acked = [e for e in events if e.gate == "keyboard_exclusive" and e.action == "ack"]
    assert len(acked) == 1
    assert str(peer.id) in (acked[0].message or "") or acked[0].actual_value == float(peer.id)


def test_hard_block_rejects_even_with_ack(db_session, monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: {
        "tq_trade": {"keyboard_conflict_hard_block": True},
    })
    _mk(db_session, "peer", engine="tdx", status="running")
    me = _mk(db_session, "me", engine="ths")
    err = _check_keyboard_conflict(db_session, me, {}, True)
    assert err is not None
    assert err[0] == 409
    assert err[2]["hard_block"] is True
    assert err[2]["requires_ack"] is False


def test_no_peer_engine_returns_none(db_session, monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: {"tq_trade": {}})
    me = _mk(db_session, "me", engine="ths")
    assert _check_keyboard_conflict(db_session, me, {}, False) is None


def test_non_keyboard_engine_skips(db_session):
    _mk(db_session, "peer", engine="ths", status="running")
    me = _mk(db_session, "me", engine="iquant")
    # iquant 不参与键鼠互斥，不需要 ack
    assert _check_keyboard_conflict(db_session, me, {}, False) is None
