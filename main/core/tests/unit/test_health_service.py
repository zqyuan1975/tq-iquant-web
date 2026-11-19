# -*- coding: utf-8 -*-
"""统一健康总览（批次21 L3）单测：只读聚合，不依赖真实通道/真实库。

锁定的口径：
1. 无会话时 level=ok，六类信息齐全；
2. 今日下单/成交数、未处理告警/待放行/待审批计数按会话聚合；
3. 运行中但引擎未注册 / 通道离线 → level=error；
4. 数据缺失/磁盘不足/备份未跑 → level=warn，且 problems 里给出可读原因；
5. 子项读取抛错只降级，不拖垮总览；API 永远返回 code=0。
"""
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import (
    Base,
    LiveApprovalQueue,
    LiveOrder,
    LiveSession,
    LiveTrade,
)
from core.services import health_service as hs

NOW = datetime(2026, 11, 18, 10, 0, 0)


@pytest.fixture
def session_factory(tmp_path):
    engine = create_engine(f"sqlite:///{(tmp_path / 'health.db').as_posix()}")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


@pytest.fixture
def patched(monkeypatch):
    """把通道/备份/通知全部替换为确定性桩，避免真实探活。"""
    monkeypatch.setattr(hs.live_service, "ENGINES", {})
    monkeypatch.setattr(
        hs.live_service, "bridge_status",
        lambda sid, status, engine: {"online": True, "status": status})
    monkeypatch.setattr(
        hs.live_service, "session_channel_health",
        lambda db, sid, engines, probe_deadline=None: {
            "primary": {"configured": True, "online": True, "engine": "tdx"},
            "backup": None})
    monkeypatch.setattr(
        hs.live_service, "session_channel_guard",
        lambda db, sid, engines, probe_deadline=None, health=None: {"checks": [
            {"key": "client_online", "ok": True, "label": "客户端/桥在线"}]})
    monkeypatch.setattr(
        hs.live_service, "session_data_health",
        lambda db, sid, engines, auto_fill=True, **kw: {
            "available": True,
            "health": {"healthy": True, "periods": {
                "1d": {"stale": 0, "empty": 0, "total": 3}}}})
    monkeypatch.setattr(
        hs.live_service, "session_decisions",
        lambda db, sid, ack_status=None: {
            "ack_summary": {"open": 0, "acked": 0}})
    monkeypatch.setattr(
        hs.live_service, "channel_status",
        lambda engine_key=None, **kw: {"engines": ["tdx"], "active": "tdx"})
    monkeypatch.setattr(hs.backup_service, "get_status", lambda: {
        "enabled": False, "daily_at": "15:10", "auto_done_today": True,
        "last_auto": None, "last_manual": None})
    monkeypatch.setattr(hs.backup_service, "backup_settings",
                        lambda cfg=None: {"min_free_mb": 500.0})
    monkeypatch.setattr(hs.backup_service, "database_path",
                        lambda: __import__("pathlib").Path("."))
    monkeypatch.setattr(hs.backup_service, "free_disk_mb", lambda p: 10000.0)
    monkeypatch.setattr(hs.notify_service, "from_config",
                        lambda cfg=None: hs.notify_service.NotifyConfig())
    monkeypatch.setattr(hs, "_watchdog_status",
                        lambda: {"running": True, "active_alarms": []})
    return monkeypatch


def _session(db, name="会话A", status="stopped", engine="tdx"):
    row = LiveSession(name=name, mode="simulation", engine=engine,
                      status=status)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _add_activity(db, sid):
    db.add(LiveOrder(live_session_id=sid, portfolio_strategy_id=1,
                     strategy_id=1, stock_code="600000", trade_type="BUY",
                     order_type="limit", quantity=100, status="submitted",
                     created_at=datetime(2026, 11, 18, 9, 31)))
    db.add(LiveOrder(live_session_id=sid, portfolio_strategy_id=1,
                     strategy_id=1, stock_code="600001", trade_type="BUY",
                     order_type="limit", quantity=100, status="deferred",
                     created_at=datetime(2026, 11, 18, 9, 32)))
    db.add(LiveTrade(live_session_id=sid, portfolio_strategy_id=1,
                     strategy_id=1, stock_code="600000", trade_type="BUY",
                     price=10, quantity=100, amount=1000, commission=5,
                     stamp_duty=0, trade_time=datetime(2026, 11, 18, 9, 35)))
    db.add(LiveApprovalQueue(
        live_session_id=sid, portfolio_strategy_id=1, strategy_id=1,
        stock_code="600002", trade_type="BUY", quantity=100,
        status="pending", expires_at=datetime(2026, 11, 18, 15, 0)))
    db.commit()


def test_overview_empty_is_ok(session_factory, patched):
    res = hs.overview(session_factory=session_factory, now=NOW)
    assert res["level"] == "ok" and res["problems"] == []
    assert res["sessions"] == []
    for key in ("core", "channels", "tasks", "backup", "disk", "notify",
                "alert_totals", "watchdog"):
        assert key in res
    assert res["tasks"]["trade_date"] == "2026-11-18"
    assert res["disk"]["ok"] is True


def test_overview_counts_today_alerts_and_warns(session_factory, patched):
    db = session_factory()
    sess = _session(db)
    _add_activity(db, sess.id)
    db.close()
    patched.setattr(hs.live_service, "session_decisions",
                    lambda db, sid, ack_status=None: {
                        "ack_summary": {"open": 3, "acked": 1}})

    res = hs.overview(session_factory=session_factory, now=NOW)
    item = res["sessions"][0]
    assert item["today"] == {"orders": 2, "trades": 1}
    assert item["alerts"] == {"open_decisions": 3, "deferred_orders": 1,
                              "pending_approvals": 1}
    assert item["channel_guard"]["checks"] == [
        {"key": "client_online", "label": "客户端/桥在线", "ok": True,
         "detail": ""}]
    assert res["alert_totals"] == {"open_decisions": 3, "deferred_orders": 1,
                                  "pending_approvals": 1}
    assert res["level"] == "warn"
    texts = " ".join(p["text"] for p in res["problems"])
    assert "未处理告警" in texts and "顺延" in texts and "待审批" in texts


def test_running_without_engine_and_offline_bridge_is_error(
        session_factory, patched):
    db = session_factory()
    _session(db, status="running")
    db.close()
    patched.setattr(hs.live_service, "bridge_status",
                    lambda sid, status, engine: {"online": False,
                                                 "status": status})

    res = hs.overview(session_factory=session_factory, now=NOW)
    item = res["sessions"][0]
    assert item["registered"] is False
    assert res["level"] == "error"
    texts = " ".join(p["text"] for p in res["problems"])
    assert "引擎未注册" in texts and "通道离线" in texts


def test_data_stale_disk_low_backup_missing_are_warns(session_factory, patched):
    db = session_factory()
    sess = _session(db, status="running")
    sid = sess.id
    db.close()
    patched.setattr(hs.live_service, "ENGINES", {sid: object()})
    patched.setattr(hs.live_service, "session_data_health",
                    lambda db, sid, engines, auto_fill=True, **kw: {
                        "available": True,
                        "health": {"healthy": False, "periods": {
                            "1d": {"stale": 2, "empty": 1, "total": 3}}}})
    patched.setattr(hs.backup_service, "free_disk_mb", lambda p: 10.0)
    patched.setattr(hs.backup_service, "get_status", lambda: {
        "enabled": True, "daily_at": "15:10", "auto_done_today": False,
        "last_auto": {"status": "failed", "message": "磁盘不足"},
        "last_manual": None})

    res = hs.overview(session_factory=session_factory, now=NOW)
    assert res["level"] == "warn"
    texts = " ".join(p["text"] for p in res["problems"])
    assert "数据缺失/陈旧 3 项" in texts
    assert "磁盘剩余" in texts and "今日自动备份尚未完成" in texts
    assert "自动备份失败" in texts


def test_subitem_failure_degrades_without_raising(session_factory, patched):
    db = session_factory()
    _session(db)
    db.close()

    def _boom(*a, **kw):
        raise RuntimeError("通道探活炸了")

    patched.setattr(hs.live_service, "session_channel_health", _boom)
    patched.setattr(hs.live_service, "session_data_health", _boom)
    patched.setattr(hs.live_service, "session_decisions", _boom)

    res = hs.overview(session_factory=session_factory, now=NOW)
    item = res["sessions"][0]
    assert item["channel"]["primary"] is None
    assert item["data"]["available"] is False
    assert item["alerts"]["open_decisions"] == 0
    assert res["level"] in ("ok", "warn")


def test_health_overview_api(monkeypatch):
    import asyncio

    from httpx import ASGITransport, AsyncClient

    from core.api import system as system_api
    from core.main import app

    monkeypatch.setattr(system_api.health_service, "overview",
                        lambda: {"level": "ok", "sessions": []})

    async def _call():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            return await c.get("/api/system/health-overview")

    body = asyncio.run(_call()).json()
    assert body["code"] == 0 and body["data"]["level"] == "ok"


def test_truncated_data_health_marks_session_incomplete(session_factory, patched):
    """体检超预算截断：会话标记未完成、原因带已校验只数，顶层 partial 标明。"""
    db = session_factory()
    _session(db)
    db.close()
    patched.setattr(hs.live_service, "session_data_health",
                    lambda db, sid, engines, auto_fill=True, **kw: {
                        "available": True, "truncated": True,
                        "codes_total": 496, "codes_checked": 30,
                        "health": {"healthy": False, "periods": {
                            "1d": {"stale": 0, "empty": 0, "total": 30}}}})

    res = hs.overview(session_factory=session_factory, now=NOW)
    item = res["sessions"][0]
    assert item["data"]["incomplete"] is True
    assert item["data"]["truncated"] is True
    assert "30/496" in item["data"]["reason"]
    assert res["partial"] is True
    assert res["incomplete_sessions"] == [
        {"session_id": item["id"], "name": item["name"],
         "reason": item["data"]["reason"]}]
    assert "数据体检未完成" in " ".join(p["text"] for p in res["problems"])


def test_overview_total_budget_degrades_remaining_sessions(
        session_factory, patched, monkeypatch):
    """整体预算耗尽后，后续会话不再发起体检，只降级标注原因。"""
    db = session_factory()
    _session(db, name="会话A")
    _session(db, name="会话B")
    db.close()
    clock = {"t": 1000.0}
    monkeypatch.setattr(hs, "_monotonic", lambda: clock["t"])
    monkeypatch.setattr(hs, "_OVERVIEW_TOTAL_BUDGET_SECONDS", 5.0)
    monkeypatch.setattr(hs, "_OVERVIEW_DATA_HEALTH_BUDGET_SECONDS", 3.0)
    calls = []

    def _slow(db, sid, engines, auto_fill=True, **kw):
        calls.append(sid)
        clock["t"] += 6.0  # 一次会话就吃掉整个总预算
        return {"available": True, "truncated": True, "codes_total": 100,
                "codes_checked": 30,
                "health": {"healthy": False, "periods": {}}}

    patched.setattr(hs.live_service, "session_data_health", _slow)

    res = hs.overview(session_factory=session_factory, now=NOW)
    assert len(calls) == 1  # 第二个会话未再发起体检
    first, second = res["sessions"]
    assert second["data"]["incomplete"] is True
    assert second["data"]["reason"] == hs.REASON_OVERVIEW_BUDGET
    assert second["data"]["codes_total"] is None
    assert len(res["incomplete_sessions"]) == 2
    assert res["partial"] is True
    assert first["data"]["incomplete"] is True  # 第一个虽截断但已返回部分结果


def test_overview_hard_deadline_stops_probe_and_data_of_later_sessions(
        session_factory, patched, monkeypatch):
    """硬截止：整体预算被前一个会话吃满后，后续会话零探活预算且跳过体检。"""
    db = session_factory()
    _session(db, name="会话A")
    _session(db, name="会话B")
    db.close()
    clock = {"t": 1000.0}
    monkeypatch.setattr(hs, "_monotonic", lambda: clock["t"])
    monkeypatch.setattr(hs, "_OVERVIEW_TOTAL_BUDGET_SECONDS", 5.0)
    seen = []

    def _health(db, sid, engines, probe_deadline=None):
        now = clock["t"]
        seen.append((sid, probe_deadline, now))
        if len(seen) == 1:
            clock["t"] = 1005.0  # 第一个会话吃满整个总预算
        return {
            "primary": {"configured": True, "online": False, "engine": "x",
                        "probe_degraded": True,
                        "probe_note": "探活超时，沿用上次结果"},
            "backup": None,
        }

    patched.setattr(hs.live_service, "session_channel_health", _health)

    res = hs.overview(session_factory=session_factory, now=NOW)

    assert len(seen) == 2
    assert seen[0][1] <= 1005.0          # 第一个会话的探活截止不越过总截止
    assert seen[1][1] <= seen[1][2]      # 第二个会话只剩零预算（deadline == now）
    assert res["sessions"][1]["data"]["reason"] == hs.REASON_OVERVIEW_BUDGET
    assert res["partial"] is True
    assert res["elapsed_seconds"] == 5.0


def test_overview_global_channel_probe_bounded_by_tail_budget(
        session_factory, patched, monkeypatch):
    """全局通道卡只能占用收尾预留的 0.5s，慢桥不得把总览拖过硬截止。"""
    db = session_factory()
    _session(db)
    db.close()
    clock = {"t": 1000.0}
    monkeypatch.setattr(hs, "_monotonic", lambda: clock["t"])
    monkeypatch.setattr(hs, "_OVERVIEW_TOTAL_BUDGET_SECONDS", 11.5)
    seen = {}

    def _status(engine_key=None, probe_deadline=None):
        seen["now"] = clock["t"]
        seen["deadline"] = probe_deadline
        clock["t"] += 0.5  # 探活用满收尾预留
        return {"engines": ["tdx"], "active": "tdx"}

    patched.setattr(hs.live_service, "channel_status", _status)

    res = hs.overview(session_factory=session_factory, now=NOW)

    assert seen["now"] == 1000.0
    assert seen["deadline"] == 1000.0 + 11.5 + 0.5
    assert res["elapsed_seconds"] == 0.5  # 收尾探活计入实际耗时


def test_in_progress_data_health_marks_incomplete(session_factory, patched):
    """同一会话体检已在跑：总览降级为未完成，不重复取数。"""
    db = session_factory()
    _session(db)
    db.close()
    called = []

    def _inflight(db, sid, engines, auto_fill=True, **kw):
        called.append(sid)
        return {"session_id": sid, "available": False, "in_progress": True,
                "reason": "同一会话已有数据体检在执行，请稍后刷新"}

    patched.setattr(hs.live_service, "session_data_health", _inflight)

    res = hs.overview(session_factory=session_factory, now=NOW)
    item = res["sessions"][0]
    assert len(called) == 1
    assert item["data"]["in_progress"] is True
    assert item["data"]["incomplete"] is True
    assert "已有数据体检在执行" in item["data"]["reason"]
    assert res["partial"] is True


def test_overview_probes_channel_once_and_guard_reuses_result(
        session_factory, patched):
    """同一轮总览内同一会话只探一次：guard 复用 channel-health 的结果。"""
    db = session_factory()
    _session(db, engine="goldminer")
    db.close()
    calls = {"health": 0, "deadlines": [], "guard": []}

    def _health(db, sid, engines, probe_deadline=None):
        calls["health"] += 1
        calls["deadlines"].append(probe_deadline)
        return {
            "primary": {"configured": True, "online": True,
                        "engine": "goldminer"},
            "backup": None,
        }

    def _guard(db, sid, engines, probe_deadline=None, health=None):
        calls["guard"].append(health)
        return {"checks": [{"key": "client_online", "ok": True,
                            "label": "客户端/桥在线"}]}

    patched.setattr(hs.live_service, "session_channel_health", _health)
    patched.setattr(hs.live_service, "session_channel_guard", _guard)

    hs.overview(session_factory=session_factory, now=NOW)

    assert calls["health"] == 1                       # 只真实探活一次
    assert calls["guard"][0] is not None               # guard 拿到同一结果
    assert calls["guard"][0]["primary"]["engine"] == "goldminer"
    assert calls["deadlines"][0] is not None           # 探活有独立时间预算


def test_overview_surfaces_probe_degradation(session_factory, patched):
    """探活超时沿用上次结果时，总览如实标注（不能静默当成离线/正常）。"""
    db = session_factory()
    _session(db, engine="goldminer")
    db.close()

    def _health(db, sid, engines, probe_deadline=None):
        return {
            "primary": {"configured": True, "online": True,
                        "engine": "goldminer", "probe_degraded": True,
                        "probe_note": "探活超时，沿用上次结果"},
            "backup": None,
        }

    patched.setattr(hs.live_service, "session_channel_health", _health)

    res = hs.overview(session_factory=session_factory, now=NOW)
    item = res["sessions"][0]
    assert item["channel"]["probe_degraded"] is True
    assert item["channel"]["probe_note"] == "探活超时，沿用上次结果"
    assert any("探活超时，沿用上次结果" in p["text"] for p in res["problems"])
