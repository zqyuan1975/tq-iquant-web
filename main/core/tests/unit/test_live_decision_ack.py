"""M4 决策事件确认/关闭单元测试。"""
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, LiveDecisionEvent, LiveSession
from core.services import live_service


def _db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed(db, n=3):
    db.add(LiveSession(id=1, name="s1", mode="simulation", status="stopped",
                       engine="tdx"))
    for i in range(n):
        db.add(LiveDecisionEvent(
            live_session_id=1, gate="max_positions_full", layer="portfolio_risk",
            action="block", bar_time=datetime(2026, 11, 18, 9, 30 + i),
            message="第 %d 条" % (i + 1)))
    db.commit()


def test_session_decisions_ack_summary_and_filter():
    db = _db()
    _seed(db)

    data = live_service.session_decisions(db, 1)
    assert data["ack_summary"] == {"open": 3, "acked": 0}
    assert len(data["events"]) == 3
    assert all(e["ack_status"] == "open" for e in data["events"])

    assert live_service.session_decisions(db, 1, ack_status="acked")["events"] == []
    assert len(live_service.session_decisions(db, 1, ack_status="open")["events"]) == 3


def test_ack_single_event_with_note():
    db = _db()
    _seed(db)
    event = db.query(LiveDecisionEvent).order_by(LiveDecisionEvent.id).first()

    result = live_service.ack_decision(db, 1, event.id, note="已人工核对")
    assert result["ack_status"] == "acked"
    assert result["ack_note"] == "已人工核对"
    assert result["ack_at"]

    data = live_service.session_decisions(db, 1)
    assert data["ack_summary"] == {"open": 2, "acked": 1}
    # 重复确认幂等
    again = live_service.ack_decision(db, 1, event.id, note="补充备注")
    assert again["ack_note"] == "补充备注"
    assert live_service.session_decisions(db, 1)["ack_summary"] == {
        "open": 2, "acked": 1}


def test_ack_all_day_end_defaults_note():
    db = _db()
    _seed(db)
    first = db.query(LiveDecisionEvent).order_by(LiveDecisionEvent.id).first()
    live_service.ack_decision(db, 1, first.id, note="单条处理")

    result = live_service.ack_all_decisions(db, 1, day_end=True)
    assert result["acked"] == 2
    assert result["note"] == "日终清零"
    data = live_service.session_decisions(db, 1)
    assert data["ack_summary"] == {"open": 0, "acked": 3}
    # 已确认的不被覆盖
    assert data["events"][0]["ack_note"] == "单条处理"
    # 再跑一次不重复确认
    assert live_service.ack_all_decisions(db, 1, day_end=True)["acked"] == 0


def test_ack_missing_session_or_event_returns_none():
    db = _db()
    _seed(db)
    assert live_service.ack_decision(db, 999, 1) is None
    assert live_service.ack_decision(db, 1, 9999) is None
    assert live_service.ack_all_decisions(db, 999) is None
