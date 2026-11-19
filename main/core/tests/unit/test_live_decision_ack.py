"""决策事件确认/批量确认/过期归档单元测试。"""
from datetime import datetime, timedelta, timezone

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import (
    Base, LiveDecisionAckLog, LiveDecisionEvent, LiveSession,
)
from core.services import live_service


def _db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _seed(db, n=3, gate="max_positions_full", created_at=None):
    db.add(LiveSession(id=1, name="s1", mode="simulation", status="stopped",
                       engine="tdx"))
    for i in range(n):
        db.add(LiveDecisionEvent(
            live_session_id=1, gate=gate, layer="portfolio_risk",
            action="block", bar_time=datetime(2026, 11, 18, 9, 30 + i),
            message="第 %d 条" % (i + 1), created_at=created_at))
    db.commit()


def test_session_decisions_ack_summary_and_filter():
    db = _db()
    _seed(db)

    data = live_service.session_decisions(db, 1)
    assert data["ack_summary"] == {"open": 3, "acked": 0, "archived": 0}
    assert len(data["events"]) == 3
    assert all(e["ack_status"] == "open" for e in data["events"])

    assert live_service.session_decisions(db, 1, ack_status="acked")["events"] == []
    assert len(live_service.session_decisions(db, 1, ack_status="open")["events"]) == 3
    assert live_service.session_decisions(db, 1, ack_status="archived")["events"] == []


def test_ack_single_event_with_note_and_audit_log():
    db = _db()
    _seed(db)
    event = db.query(LiveDecisionEvent).order_by(LiveDecisionEvent.id).first()

    result = live_service.ack_decision(db, 1, event.id, note="已人工核对",
                                       operator="ops:ouy")
    assert result["ack_status"] == "acked"
    assert result["ack_note"] == "已人工核对"
    assert result["ack_by"] == "ops:ouy"
    assert result["ack_at"]

    data = live_service.session_decisions(db, 1)
    assert data["ack_summary"] == {"open": 2, "acked": 1, "archived": 0}
    # 重复确认幂等，且每次操作都留审计
    again = live_service.ack_decision(db, 1, event.id, note="补充备注")
    assert again["ack_note"] == "补充备注"
    assert live_service.session_decisions(db, 1)["ack_summary"] == {
        "open": 2, "acked": 1, "archived": 0}
    logs = db.query(LiveDecisionAckLog).filter(
        LiveDecisionAckLog.event_id == event.id).order_by(
            LiveDecisionAckLog.id).all()
    assert len(logs) == 2
    assert logs[0].from_status == "open" and logs[0].to_status == "acked"
    # 重复确认如实记录 acked → acked，不伪装成首次处理
    assert logs[1].from_status == "acked" and logs[1].to_status == "acked"
    assert logs[0].reason == "已人工核对"


def test_batch_ack_requires_filters_and_note():
    db = _db()
    _seed(db)
    try:
        live_service.batch_ack_decisions(db, 1, note="全选")
    except ValueError as exc:
        assert "筛选条件" in str(exc)
    else:  # pragma: no cover - 必须拒绝「全选所有」
        raise AssertionError("无筛选条件的批量确认必须被拒绝")

    try:
        live_service.batch_ack_decisions(db, 1, action="block", note="只按动作")
    except ValueError as exc:
        assert "筛选条件" in str(exc)
    else:  # pragma: no cover - action 不能单独作为筛选
        raise AssertionError("仅 action 的批量确认必须被拒绝")

    try:
        live_service.batch_ack_decisions(
            db, 1, gate="max_positions_full", note="  ")
    except ValueError as exc:
        assert "原因" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("缺原因必须被拒绝")

    assert live_service.batch_ack_decisions(db, 999, gate="x", note="x") is None


def test_batch_ack_excludes_high_risk_and_writes_audit():
    db = _db()
    _seed(db, n=2)
    db.add(LiveDecisionEvent(
        live_session_id=1, gate="cash_diff", layer="account_risk",
        action="alert", bar_time=datetime(2026, 11, 18, 10, 0),
        message="现金不一致"))
    db.commit()

    preview = live_service.preview_batch_ack(db, 1, layer="portfolio_risk")
    assert preview["matched"] == 2
    assert preview["by_gate"] == {"max_positions_full": 2}

    preview_all = live_service.preview_batch_ack(db, 1, keyword="第")
    assert preview_all["matched"] == 2
    assert preview_all["high_risk_excluded"] == 0
    cash_preview = live_service.preview_batch_ack(db, 1, gate="cash_diff")
    assert cash_preview["matched"] == 0
    assert cash_preview["high_risk_excluded"] == 1

    # 预览数不一致 → 409 语义（conflict），不确认任何事件
    conflict = live_service.batch_ack_decisions(
        db, 1, layer="portfolio_risk", note="批量核对", expected_count=99)
    assert conflict["conflict"] is True
    assert conflict["matched"] == 2

    result = live_service.batch_ack_decisions(
        db, 1, layer="portfolio_risk", note="批量核对", operator="ops:a",
        expected_count=2)
    assert result["acked"] == 2
    assert result["batch_id"]
    data = live_service.session_decisions(db, 1)
    assert data["ack_summary"] == {"open": 1, "acked": 2, "archived": 0}
    # 高危事件保持 open，绝不被批量确认
    cash = [e for e in data["events"] if e["gate"] == "cash_diff"][0]
    assert cash["ack_status"] == "open"
    assert all(e["ack_by"] == "ops:a" for e in data["events"]
               if e["gate"] == "max_positions_full")

    logs = live_service.list_decision_ack_logs(db, 1)
    summary = [x for x in logs["items"] if x["event_id"] is None]
    assert len(summary) == 1
    assert summary[0]["affected_count"] == 2
    assert summary[0]["batch_id"] == result["batch_id"]
    assert summary[0]["operator"] == "ops:a"
    assert "layer" in summary[0]["filters_json"]
    assert logs["total"] == 3  # 1 汇总 + 2 逐条
    # 高危事件不产生审计行
    assert all(x["event_id"] != cash["id"] for x in logs["items"])


def test_archive_stale_decisions_moves_out_of_open_but_keeps_record():
    db = _db()
    old = datetime(2026, 11, 1, 9, 30)
    _seed(db, n=2, created_at=old)
    db.add(LiveDecisionEvent(
        live_session_id=1, gate="cash_diff", layer="account_risk",
        action="alert", bar_time=datetime(2026, 11, 1, 10, 0),
        message="旧的高危现金差异", created_at=old))
    db.add(LiveDecisionEvent(
        live_session_id=1, gate="max_positions_full", layer="portfolio_risk",
        action="block", bar_time=datetime(2026, 11, 19, 9, 30),
        message="新事件", created_at=datetime(2026, 11, 19, 9, 30)))
    db.commit()

    result = live_service.archive_stale_decisions(
        db, days=7, now=datetime(2026, 11, 19, 12, 0))
    assert result["archived"] == 2  # 旧的非高危两条；高危与新事件不动

    data = live_service.session_decisions(db, 1)
    assert data["ack_summary"] == {"open": 2, "acked": 0, "archived": 2}
    archived = [e for e in data["events"] if e["ack_status"] == "archived"]
    assert len(archived) == 2
    assert all(e["archived_at"] and e["archive_reason"] for e in archived)
    # 记录没有删除，可按 archived 回看
    only_archived = live_service.session_decisions(db, 1, ack_status="archived")
    assert len(only_archived["events"]) == 2
    # 高危事件不自动归档，仍留在未处理
    assert [e["gate"] for e in data["events"]
            if e["ack_status"] == "open"].count("cash_diff") == 1

    # 计数口径一致性：健康总览（ack_summary.open）== 事件检索 open 总数
    search_open = live_service.search_decision_events(db, ack_status="open")
    assert search_open["total"] == data["ack_summary"]["open"]
    archived_search = live_service.search_decision_events(
        db, ack_status="archived")
    assert archived_search["total"] == 2

    # 审计：一行汇总 + 逐条
    logs = live_service.list_decision_ack_logs(db, 1)
    archive_logs = [x for x in logs["items"] if x["action"] == "archive"]
    assert len(archive_logs) == 3
    assert any(x["event_id"] is None and x["affected_count"] == 2
               for x in archive_logs)

    # 再跑不重复归档；days<=0 关闭
    again = live_service.archive_stale_decisions(
        db, days=7, now=datetime(2026, 11, 19, 12, 0))
    assert again["archived"] == 0
    off = live_service.archive_stale_decisions(db, days=0)
    assert off["archived"] == 0


def test_archive_uses_default_seven_day_window():
    db = _db()
    _seed(db, n=1, created_at=(
        datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=8)))
    result = live_service.archive_stale_decisions(db)
    assert result["archived"] == 1
    assert result["days"] == 7


def test_ack_missing_session_or_event_returns_none():
    db = _db()
    _seed(db)
    assert live_service.ack_decision(db, 999, 1) is None
    assert live_service.ack_decision(db, 1, 9999) is None
    assert live_service.preview_batch_ack(db, 999, gate="x") is None
    assert live_service.list_decision_ack_logs(db, 999) is None
