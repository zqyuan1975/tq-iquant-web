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
        lambda db, sid, engines: {
            "primary": {"configured": True, "online": True, "engine": "tdx"},
            "backup": None})
    monkeypatch.setattr(
        hs.live_service, "session_channel_guard",
        lambda db, sid, engines: {"checks": [
            {"key": "client_online", "ok": True, "label": "客户端/桥在线"}]})
    monkeypatch.setattr(
        hs.live_service, "session_data_health",
        lambda db, sid, engines, auto_fill=True: {
            "available": True,
            "health": {"healthy": True, "periods": {
                "1d": {"stale": 0, "empty": 0, "total": 3}}}})
    monkeypatch.setattr(
        hs.live_service, "session_decisions",
        lambda db, sid, ack_status=None: {
            "ack_summary": {"open": 0, "acked": 0}})
    monkeypatch.setattr(
        hs.live_service, "channel_status",
        lambda engine_key=None: {"engines": ["tdx"], "active": "tdx"})
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
                    lambda db, sid, engines, auto_fill=True: {
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
