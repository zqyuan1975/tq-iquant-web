# -*- coding: utf-8 -*-
"""统一健康总览（批次21 L3）：只读聚合，不改任何交易/落库语义。

一次返回运维需要一眼看完的六类信息：

- **Core**：版本 / 运行时长（与 ``/api/status`` 同源）；
- **通道**：全局通道清单 + 每个会话的主/备通道健康与守护检查结论；
- **数据新鲜度**：每会话缺失/陈旧 bar 计数（复用 ``live_service.session_data_health``，
  只读模式，不触发自动补齐）；
- **当日任务**：备份是否已跑（``backup_service``）+ 今日下单/成交数；
- **磁盘/备份**：剩余空间与保护阈值、最近自动/手动备份记录；
- **待处理告警**：未确认决策事件、待放行顺延委托、待审批单、watchdog 告警。

边界：**不新建轮询线程、不发交易、不改库**；任何子项读取失败都降级为
``available=false`` 并写一条 WARNING，总览接口本身永远返回成功。
"""
from __future__ import annotations

import logging
from datetime import datetime
from time import monotonic as _monotonic
from typing import Any, Dict, List, Optional

from core.api.status import get_status
from core.db import SessionLocal
from core.engine.live.timing import now_shanghai
from core.models import (
    LiveApprovalQueue,
    LiveOrder,
    LiveSession,
    LiveTrade,
)
from core.services import backup_service, live_service
from core.services import notify_service

logger = logging.getLogger(__name__)

#: 单会话数据只读体检（不补齐）：等下用 live_service 内部同一套检查口径。
LEVEL_OK = "ok"
LEVEL_WARN = "warn"
LEVEL_ERROR = "error"

# 健康总览整体时间预算：多会话逐个聚合时，不能让某个会话的体检把接口拖过百秒
# （此前未限流，实测 200s+ 仍在跑，且请求超时后线程不可取消）。超预算的会话
# 直接降级为「未完成」并标注原因，已拿到的部分照常返回。
_OVERVIEW_TOTAL_BUDGET_SECONDS = 12.0
# 单会话体检预算（与启动体检 30 只/3s 对齐）：总览是告警级信息，宁可标注
# 「未校验完整」也不允许扫全池。
_OVERVIEW_DATA_HEALTH_MAX_CODES = 30
_OVERVIEW_DATA_HEALTH_BUDGET_SECONDS = 2.0
# 降级原因（前端/日志可直接展示）。
REASON_OVERVIEW_BUDGET = "健康总览整体时间预算已耗尽，未做数据体检"
REASON_DATA_TRUNCATED = "体检超出预算，仅校验部分标的"


def _safe(label: str, fn, default=None):
    """读取子项；失败降级为 default 并写一条 WARNING（总览不因单项失败整体失败）。"""
    try:
        return fn()
    except Exception as exc:  # noqa: BLE001
        logger.warning("health-overview: 读取 %s 失败（%s: %s）",
                       label, type(exc).__name__, exc)
        return default


def _channel_ok(primary: Optional[dict]) -> bool:
    """主通道是否可接受：未配置直接判失败；在线未知（None）不算失败。"""
    primary = primary or {}
    if not primary.get("configured", True):
        return False
    return primary.get("online") is not False


def _session_item(db, session: LiveSession, now: datetime,
                  data_budget: Optional[float] = None) -> dict:
    """单会话聚合；``data_budget`` 是本会话体检可用的秒数（None=默认预算）。

    体检预算耗尽时不再调用取数，直接标注「未完成 + 原因」，保证总览整体时长可控。
    """
    sid = int(session.id)
    engine = live_service.ENGINES.get(sid)

    bridge = _safe("bridge_status(%s)" % sid,
                   lambda: live_service.bridge_status(sid, session.status, engine),
                   {"online": None, "status": "unknown"})
    health = _safe("channel_health(%s)" % sid,
                   lambda: live_service.session_channel_health(
                       db, sid, live_service.ENGINES))
    primary = (health or {}).get("primary")
    backup = (health or {}).get("backup")

    guard = _safe("channel_guard(%s)" % sid,
                  lambda: live_service.session_channel_guard(
                      db, sid, live_service.ENGINES))
    checks = (guard or {}).get("checks") or []
    failed = [c.get("label") for c in checks if not c.get("ok")]

    if data_budget is not None and data_budget <= 0:
        data_health = {"available": False, "incomplete": True,
                       "skipped": True, "reason": REASON_OVERVIEW_BUDGET}
    else:
        budget = (_OVERVIEW_DATA_HEALTH_BUDGET_SECONDS if data_budget is None
                  else max(0.1, min(_OVERVIEW_DATA_HEALTH_BUDGET_SECONDS,
                                    data_budget)))
        data_health = _safe("data_health(%s)" % sid,
                            lambda: live_service.session_data_health(
                                db, sid, live_service.ENGINES, auto_fill=False,
                                max_codes=_OVERVIEW_DATA_HEALTH_MAX_CODES,
                                time_budget_seconds=budget))
    data = {"available": False, "incomplete": False}
    if data_health:
        raw = data_health.get("health") or {}
        periods = {}
        for period, stats in (raw.get("periods") or {}).items():
            periods[period] = {
                "stale": int(stats.get("stale") or 0),
                "empty": int(stats.get("empty") or 0),
                "total": int(stats.get("total") or 0),
            }
        in_progress = bool(data_health.get("in_progress"))
        skipped = bool(data_health.get("skipped"))
        truncated = bool(data_health.get("truncated") or raw.get("truncated"))
        reason = data_health.get("reason")
        if truncated and not skipped and not in_progress:
            reason = "%s（已校验 %s/%s 只，其余未校验）" % (
                REASON_DATA_TRUNCATED,
                data_health.get("codes_checked", raw.get("checked_codes")),
                data_health.get("codes_total"))
        data = {
            "available": bool(data_health.get("available", True)),
            "healthy": bool(raw.get("healthy")),
            "unavailable": bool(raw.get("unavailable")),
            "reason": reason,
            "codes_total": data_health.get("codes_total"),
            "codes_checked": data_health.get("codes_checked",
                                             raw.get("checked_codes")),
            "periods": periods,
            "bad_total": sum(p["stale"] + p["empty"] for p in periods.values()),
            "truncated": truncated,
            "in_progress": in_progress,
            "incomplete": bool(in_progress or skipped or truncated),
        }

    start = datetime(now.year, now.month, now.day)
    orders_today = db.query(LiveOrder).filter(
        LiveOrder.live_session_id == sid,
        LiveOrder.created_at >= start).count()
    trades_today = db.query(LiveTrade).filter(
        LiveTrade.live_session_id == sid,
        LiveTrade.trade_time >= start).count()

    decisions = _safe("decisions(%s)" % sid,
                      lambda: live_service.session_decisions(db, sid)) or {}
    open_decisions = int((decisions.get("ack_summary") or {}).get("open") or 0)

    deferred = db.query(LiveOrder).filter(
        LiveOrder.live_session_id == sid,
        LiveOrder.status == "deferred").count()
    pending_approvals = db.query(LiveApprovalQueue).filter(
        LiveApprovalQueue.live_session_id == sid,
        LiveApprovalQueue.status == "pending").count()

    return {
        "id": sid,
        "name": session.name,
        "mode": session.mode,
        "status": session.status,
        "engine": getattr(session, "engine", None),
        "backup_engine": getattr(session, "backup_engine", None),
        "registered": engine is not None,
        "bridge": bridge,
        "channel": {"primary": primary, "backup": backup},
        "channel_ok": _channel_ok(primary),
        "channel_guard": {
            "available": guard is not None,
            "ok": bool(checks) and not failed,
            "failed": failed,
            # 逐项结果（5 项）供健康总览/故障向导展示，避免只看到 problems 里
            # 被截断的前 4 项（2026-11-19）。
            "checks": [
                {
                    "key": c.get("key"),
                    "label": c.get("label"),
                    "ok": bool(c.get("ok")),
                    "detail": c.get("detail") or "",
                }
                for c in checks
            ],
        },
        "data": data,
        "today": {"orders": orders_today, "trades": trades_today},
        "alerts": {
            "open_decisions": open_decisions,
            "deferred_orders": deferred,
            "pending_approvals": pending_approvals,
        },
    }


def _watchdog_status() -> dict:
    try:
        from core.engine.live.engine_watchdog import current_watchdog
    except Exception:  # noqa: BLE001 — 依赖缺失视为未运行
        return {"running": False, "active_alarms": []}
    wd = _safe("watchdog", current_watchdog)
    if wd is None:
        return {"running": False, "active_alarms": []}
    return _safe("watchdog.status", wd.status,
                 {"running": False, "active_alarms": []})


def _notify_state() -> dict:
    cfg = _safe("notify_config", notify_service.from_config,
                notify_service.NotifyConfig())
    channels = []
    if cfg.feishu_usable:
        channels.append("feishu")
    if cfg.wecom_usable:
        channels.append("wecom")
    if cfg.email_usable:
        channels.append("email")
    if cfg.sound_usable:
        channels.append("sound")
    return {"enabled": bool(cfg.enabled), "usable": bool(cfg.usable),
            "channels": channels, "min_level": cfg.min_level}


def _disk_state() -> dict:
    settings = _safe("backup_settings", backup_service.backup_settings, {}) or {}
    db_path = backup_service.database_path()
    base = db_path.parent if db_path.parent.exists() else db_path
    free_mb = _safe("free_disk_mb", lambda: backup_service.free_disk_mb(base), None)
    min_free = float(settings.get("min_free_mb") or 0)
    return {
        "free_mb": round(free_mb, 1) if free_mb is not None else None,
        "min_free_mb": min_free,
        "ok": (free_mb is not None and free_mb >= min_free),
        "path": str(base),
    }


def overview(session_factory=None, now: Optional[datetime] = None) -> dict:
    """聚合健康总览（只读）。任何子项异常都不会让本函数抛错。"""
    now = now or now_shanghai()
    factory = session_factory or SessionLocal
    core = _safe("status", get_status, {})
    core_data = (core or {}).get("data", {}).get("core", {}) if core else {}

    # 整体预算：逐会话聚合（通道探测各 1s 封顶，体检按剩余预算分配），
    # 预算耗尽后剩余会话不再体检，只标明「未完成 + 原因」。
    deadline = _monotonic() + _OVERVIEW_TOTAL_BUDGET_SECONDS
    db = factory()
    try:
        sessions = db.query(LiveSession).order_by(LiveSession.id).all()
        items = []
        for s in sessions:
            items.append(_session_item(db, s, now,
                                       data_budget=deadline - _monotonic()))
    finally:
        db.close()

    incomplete_sessions = [
        {"session_id": it["id"], "name": it["name"],
         "reason": (it.get("data") or {}).get("reason") or "数据体检未完成"}
        for it in items
        if (it.get("data") or {}).get("incomplete")
    ]

    backup_status = _safe("backup_status", backup_service.get_status, {}) or {}
    watchdog = _watchdog_status()
    notify = _notify_state()
    disk = _disk_state()
    channels = _safe("channel_status", live_service.channel_status, {}) or {}

    totals = {"open_decisions": 0, "deferred_orders": 0,
              "pending_approvals": 0}
    for item in items:
        for key in totals:
            totals[key] += int(item["alerts"][key])

    problems: List[Dict[str, str]] = []

    def _add(level: str, text: str) -> None:
        problems.append({"level": level, "text": text})

    for item in items:
        running = item["status"] in ("running", "stopping")
        if running and not item["registered"]:
            _add(LEVEL_ERROR, "会话「%s」状态为 %s 但引擎未注册"
                 % (item["name"], item["status"]))
        bridge_online = (item["bridge"] or {}).get("online")
        if running and bridge_online is False:
            _add(LEVEL_ERROR, "会话「%s」交易通道离线" % item["name"])
        if running and not item["channel_ok"]:
            _add(LEVEL_ERROR, "会话「%s」主通道不可用" % item["name"])
        if item["channel_guard"]["failed"]:
            _add(LEVEL_WARN, "会话「%s」通道检查未通过：%s" % (
                item["name"], "、".join(str(x) for x in
                                       item["channel_guard"]["failed"][:4])))
        data = item["data"]
        if data.get("unavailable"):
            _add(LEVEL_WARN, "会话「%s」数据体检不可用：%s"
                 % (item["name"], data.get("reason") or "行情通道不可用"))
        elif data.get("incomplete"):
            _add(LEVEL_WARN, "会话「%s」数据体检未完成：%s"
                 % (item["name"], data.get("reason") or "未校验完整"))
        elif data.get("available") and not data.get("healthy"):
            _add(LEVEL_WARN, "会话「%s」数据缺失/陈旧 %d 项"
                 % (item["name"], int(data.get("bad_total") or 0)))
        if item["alerts"]["open_decisions"]:
            _add(LEVEL_WARN, "会话「%s」有 %d 条未处理告警"
                 % (item["name"], item["alerts"]["open_decisions"]))
        if item["alerts"]["deferred_orders"]:
            _add(LEVEL_WARN, "会话「%s」有 %d 笔待放行顺延委托"
                 % (item["name"], item["alerts"]["deferred_orders"]))
        if item["alerts"]["pending_approvals"]:
            _add(LEVEL_WARN, "会话「%s」有 %d 笔待审批委托"
                 % (item["name"], item["alerts"]["pending_approvals"]))

    if not disk["ok"]:
        _add(LEVEL_WARN, "磁盘剩余 %s MB 低于保护阈值 %s MB"
             % (disk["free_mb"], disk["min_free_mb"]))
    if backup_status.get("enabled") and not backup_status.get("auto_done_today"):
        _add(LEVEL_WARN, "今日自动备份尚未完成（计划 %s）"
             % (backup_status.get("daily_at") or "—"))
    last_auto = backup_status.get("last_auto") or {}
    if last_auto.get("status") == backup_service.STATUS_FAILED:
        _add(LEVEL_WARN, "最近一次自动备份失败：%s"
             % (last_auto.get("message") or "未知原因"))
    if watchdog.get("active_alarms"):
        _add(LEVEL_WARN, "引擎 watchdog 有 %d 条活跃告警"
             % len(watchdog["active_alarms"]))
    running_count = sum(1 for s in items if s["status"] in ("running", "stopping"))
    if running_count and not watchdog.get("running"):
        _add(LEVEL_WARN, "存在运行中会话但 watchdog 未运行")

    level = LEVEL_OK
    if any(p["level"] == LEVEL_ERROR for p in problems):
        level = LEVEL_ERROR
    elif problems:
        level = LEVEL_WARN

    return {
        "generated_at": now.isoformat(),
        "level": level,
        "problems": problems,
        "partial": bool(incomplete_sessions),
        "incomplete_sessions": incomplete_sessions,
        "core": {
            "online": True,
            "version": core_data.get("version"),
            "uptime": core_data.get("uptime"),
        },
        "watchdog": {
            "running": bool(watchdog.get("running")),
            "last_scan_at": watchdog.get("last_scan_at"),
            "active_alarms": watchdog.get("active_alarms") or [],
        },
        "channels": channels,
        "sessions": items,
        "tasks": {
            "trade_date": now.date().isoformat(),
            "running_sessions": running_count,
            "backup_at": backup_status.get("daily_at"),
            "backup_done_today": bool(backup_status.get("auto_done_today")),
        },
        "backup": backup_status,
        "disk": disk,
        "notify": notify,
        "alert_totals": totals,
    }
