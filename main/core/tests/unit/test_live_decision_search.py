"""L4 关键事件检索单元测试（跨会话只读查询）。"""
from datetime import date, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, LiveDecisionEvent, LiveSession
from core.services import live_service


def _db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed(db):
    db.add_all([
        LiveSession(id=1, name="会话一", mode="simulation", status="stopped",
                    engine="tdx"),
        LiveSession(id=2, name="会话二", mode="simulation", status="stopped",
                    engine="ths"),
    ])
    db.add_all([
        LiveDecisionEvent(
            live_session_id=1, gate="stop_loss", layer="strategy_risk",
            action="trigger", stock_code="600000", param_name="stop_loss_ratio",
            message="止损触发", created_at=datetime(2026, 11, 17, 10, 0),
            bar_time=datetime(2026, 11, 17, 9, 58)),
        LiveDecisionEvent(
            live_session_id=1, gate="max_positions_full", layer="portfolio_risk",
            action="block", message="持仓已满", created_at=datetime(2026, 11, 18, 9, 40),
            bar_time=datetime(2026, 11, 18, 9, 39)),
        LiveDecisionEvent(
            live_session_id=2, gate="stop_loss", layer="strategy_risk",
            action="trigger", stock_code="000001", message="止损触发 2",
            created_at=datetime(2026, 11, 18, 14, 0),
            bar_time=datetime(2026, 11, 18, 13, 59), ack_status="acked",
            ack_note="已核对", ack_at=datetime(2026, 11, 18, 14, 5)),
        # 历史脏行（bar_time 早于 2000 年）不参与检索
        LiveDecisionEvent(
            live_session_id=2, gate="stop_loss", layer="strategy_risk",
            action="trigger", message="脏行",
            created_at=datetime(2026, 11, 18, 15, 0),
            bar_time=datetime(1970, 8, 23, 20, 5, 6)),
    ])
    db.commit()


def test_search_without_filter_orders_desc_and_skips_dirty_bar_time():
    db = _db()
    _seed(db)

    data = live_service.search_decision_events(db)
    assert data["total"] == 3
    assert [i["message"] for i in data["items"]] == [
        "止损触发 2", "持仓已满", "止损触发"]
    assert data["items"][0]["session_id"] == 2
    assert data["items"][0]["session_name"] == "会话二"


def test_search_filters_session_gate_and_keyword():
    db = _db()
    _seed(db)

    assert live_service.search_decision_events(db, session_id=1)["total"] == 2
    assert live_service.search_decision_events(db, gate="stop_loss")["total"] == 2
    assert live_service.search_decision_events(
        db, layer="portfolio_risk")["total"] == 1
    assert live_service.search_decision_events(db, action="block")["total"] == 1
    assert live_service.search_decision_events(db, keyword="600000")["total"] == 1
    assert live_service.search_decision_events(db, keyword="止损")["total"] == 2


def test_search_filters_ack_status():
    db = _db()
    _seed(db)

    open_rows = live_service.search_decision_events(db, ack_status="open")
    assert open_rows["total"] == 2
    assert all(i["ack_status"] == "open" for i in open_rows["items"])
    acked = live_service.search_decision_events(db, ack_status="acked")
    assert acked["total"] == 1
    assert acked["items"][0]["ack_note"] == "已核对"


def test_search_date_range_inclusive_and_paging():
    db = _db()
    _seed(db)

    one_day = live_service.search_decision_events(
        db, start=date(2026, 11, 18), end=date(2026, 11, 18))
    assert one_day["total"] == 2
    both = live_service.search_decision_events(
        db, start=date(2026, 11, 17), end=date(2026, 11, 18))
    assert both["total"] == 3

    page = live_service.search_decision_events(db, limit=1, offset=1)
    assert page["limit"] == 1 and page["offset"] == 1
    assert page["total"] == 3
    assert len(page["items"]) == 1


def test_search_clamps_limit_and_offset():
    db = _db()
    _seed(db)

    assert live_service.search_decision_events(db, limit=0)["limit"] == 1
    assert live_service.search_decision_events(db, limit=5000)["limit"] == 1000
    assert live_service.search_decision_events(db, offset=-5)["offset"] == 0
