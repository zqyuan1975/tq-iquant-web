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
# 11.5s 是**硬截止**：逐会话聚合的总时长按此分配，留 0.5s 给收尾（备份/磁盘/汇总/
# 序列化），保证首次打开稳定 < 12s（此前 12.0s 只是软预算，实测尖峰 14.63s）。
_OVERVIEW_TOTAL_BUDGET_SECONDS = 11.5
# 单会话体检预算（与启动体检 30 只/3s 对齐）：总览是告警级信息，宁可标注
# 「未校验完整」也不允许扫全池。1.2s ≈ 单只取价硬超时（2s 预算的 60%），
# 3 个会话合计 3.6s，把更多时间留给慢桥探活（掘金 /ping 3–5s）。
_OVERVIEW_DATA_HEALTH_MAX_CODES = 30
_OVERVIEW_DATA_HEALTH_BUDGET_SECONDS = 1.2
# 单会话通道探活预算上限：与 HTTP 桥探活硬超时（6s）对齐；实际值由剩余时间
# 按会话分摊，窗口类探针毫秒级、几乎不占预算。
_OVERVIEW_PROBE_BUDGET_SECONDS = 6.0
# 剩余时间低于此值就不再为本会话探活/体检，直接返回已有结果（硬截止兜底）。
_OVERVIEW_PROBE_FLOOR_SECONDS = 0.5
# 降级原因（前端/日志可直接展示）。
REASON_OVERVIEW_BUDGET = "健康总览整体时间预算已耗尽，未做数据体检"
REASON_DATA_TRUNCATED = "体检为抽样校验（受抽样上限与时间预算限制，未覆盖的标的不计缺失）"
# 问题 → 影响（向导/总览统一展示：原因看 text，影响看这里，动作看 fix/observe）。
_PROBLEM_IMPACTS = {
    "engine_unregistered": "会话显示运行中但后端没有引擎实例，不会产生新信号或下单",
    "bridge_offline": "下单与回执通道中断，策略信号无法执行",
    "channel_unavailable": "行情/交易通道不可用，实盘无法正常取数与下单",
    "channel_guard": "通道就绪度不足，启动或主备切换可能失败",
    "probe_degraded": "通道状态可能滞后，本项为观测提示、非故障",
    "data_unavailable": "无法判断数据新鲜度，选股可能使用旧数据",
    "data_incomplete": "本次抽样未覆盖全部标的，属未验完、非数据缺失",
    "data_missing": "缺失标的不会进入选股/信号，回测与实盘结果会偏差",
    "data_unverified": "本批标的未验成（查询异常/超时），不代表本地缺数据",
    "open_decisions": "告警堆积会掩盖新问题，盘后难以核对",
    "deferred_orders": "顺延委托未放行，可能错过交易时点",
    "pending_approvals": "待审批单积压，超时后会过期被拒",
    "disk_low": "磁盘不足会自动跳过备份，故障时缺少恢复点",
    "backup_pending": "今日无备份，故障时缺少恢复点",
    "backup_failed": "最近备份不可用，故障时缺少恢复点",
    "watchdog_alarms": "引擎异常未被处置，可能持续停滞",
    "watchdog_down": "引擎卡死不会被自动检测与告警",
}


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
                  data_budget: Optional[float] = None,
                  probe_deadline: Optional[float] = None) -> dict:
    """单会话聚合；``data_budget`` 是本会话体检可用的秒数（None=默认预算）。

    体检预算耗尽时不再调用取数，直接标注「未完成 + 原因」，保证总览整体时长可控。

    ``probe_deadline`` 是本会话通道探活的时间预算（单调时钟时点）。同一轮总览里
    channel-health 只算**一次**，guard 直接复用该结果（``health=``），避免同会话
    两次真实探活（此前 s2/s3 各多花 3.5s / 5.5s）。
    """
    sid = int(session.id)
    engine = live_service.ENGINES.get(sid)
    # 未运行会话没有引擎在跑：不探数据体检（省预算），也不产生「未完成/缺失」条目。
    running = session.status in ("running", "stopping")

    bridge = _safe("bridge_status(%s)" % sid,
                   lambda: live_service.bridge_status(sid, session.status, engine),
                   {"online": None, "status": "unknown"})
    health = _safe("channel_health(%s)" % sid,
                   lambda: live_service.session_channel_health(
                       db, sid, live_service.ENGINES, probe_deadline))
    primary = (health or {}).get("primary")
    backup = (health or {}).get("backup")

    guard = _safe("channel_guard(%s)" % sid,
                  lambda: live_service.session_channel_guard(
                      db, sid, live_service.ENGINES,
                      probe_deadline=probe_deadline, health=health))
    checks = (guard or {}).get("checks") or []
    failed = [c.get("label") for c in checks if not c.get("ok")]

    if not running:
        # 停止态：没有引擎在跑，数据新鲜度不适用（不是「有缺失/陈旧」）。
        data = {"available": False, "healthy": False, "unavailable": False,
                "reason": "", "codes_total": None, "codes_checked": None,
                "periods": {}, "bad_total": 0, "data_missing_count": 0,
                "status_counts": {k: 0 for k in
                                  ("ok", "stale", "empty", "error", "timeout")},
                "truncated": False, "in_progress": False,
                "incomplete": False, "not_running": True}
        data_health = None
    elif data_budget is not None and data_budget <= 0:
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
            # 明确这是「抽样没覆盖到」，不是「数据缺失」；未校验部分不补下载。
            reason = "%s（已校验 %s/%s 只，其余未校验、不计缺失，可稍后重试）" % (
                REASON_DATA_TRUNCATED,
                data_health.get("codes_checked", raw.get("checked_codes")),
                data_health.get("codes_total"))
        status_counts = raw.get("status_counts") or {}
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
            # 与数据体检同一口径：stale/empty 才是真缺失，error/timeout/未抽查不算。
            "data_missing_count": int(raw.get("data_missing_count")
                                      or sum(p["stale"] + p["empty"]
                                             for p in periods.values())),
            "status_counts": {k: int(status_counts.get(k) or 0) for k in
                              ("ok", "stale", "empty", "error", "timeout")},
            "truncated": truncated,
            "in_progress": in_progress,
            "incomplete": bool(in_progress or skipped or truncated),
            "not_running": False,
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
        "channel": {
            "primary": primary, "backup": backup,
            # 探活降级（超时/预算耗尽沿用上次结果）——与「通道离线」区分开：
            # 前者是本次没探成，后者是探到了确实不在。
            "probe_degraded": bool((primary or {}).get("probe_degraded")),
            "probe_note": (primary or {}).get("probe_note") or "",
        },
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

    # 整体预算：逐会话聚合（通道探活按通道超时封顶，体检按剩余预算分摊）。
    # 硬截止：剩余时间不足 0.5s 的会话不再探活/体检，直接返回已有结果并标 partial，
    # 保证任何单个会话/通道都不能把整体拖过 12s。
    started_at = _monotonic()
    deadline = started_at + _OVERVIEW_TOTAL_BUDGET_SECONDS
    db = factory()
    try:
        sessions = db.query(LiveSession).order_by(LiveSession.id).all()
        items = []
        total = len(sessions)
        for idx, s in enumerate(sessions):
            remaining = deadline - _monotonic()
            sessions_left = max(1, total - idx)
            if remaining <= _OVERVIEW_PROBE_FLOOR_SECONDS:
                # 预算已尽：不探活、不体检，沿用缓存值（探活内部会直接返回上次结果）。
                items.append(_session_item(
                    db, s, now, data_budget=0.0,
                    probe_deadline=_monotonic()))
                continue
            # 先给本会话留出体检预算，剩余才给探活；再按剩余会话数分摊，
            # 避免前面的慢桥把后面的会话挤成「未完成」。
            reserve = min(_OVERVIEW_DATA_HEALTH_BUDGET_SECONDS,
                          remaining / sessions_left)
            probe_budget = max(
                _OVERVIEW_PROBE_FLOOR_SECONDS,
                min(_OVERVIEW_PROBE_BUDGET_SECONDS, remaining - reserve))
            data_budget = max(
                0.0, min(_OVERVIEW_DATA_HEALTH_BUDGET_SECONDS,
                         remaining - probe_budget))
            items.append(_session_item(
                db, s, now, data_budget=data_budget,
                probe_deadline=_monotonic() + probe_budget))
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
    # 全局通道卡：逐会话聚合后再探，只能占用收尾预留的 0.5s 额度——超出即沿用
    # 缓存/上次已知状态并标注降级，避免默认通道是慢桥时把整体拖过硬截止。
    channels = _safe(
        "channel_status",
        lambda: live_service.channel_status(
            probe_deadline=deadline + _OVERVIEW_PROBE_FLOOR_SECONDS),
        {}) or {}

    totals = {"open_decisions": 0, "deferred_orders": 0,
              "pending_approvals": 0}
    for item in items:
        for key in totals:
            totals[key] += int(item["alerts"][key])

    problems: List[Dict[str, Any]] = []

    def _add(level: str, text: str, *, kind: str = None,
             session_id: int = None, fix: dict = None,
             observe: str = None) -> None:
        """登记一条总览问题。

        ``fix`` 必须是本平台内**真能执行并让该条消失**的动作
        （{"label","to"}，to 为站内路由）；做不到的（需在通达信/客户端等外部
        软件操作、或进程级不可控）一律给 ``observe`` 写清外部处理方式，
        前端会把它们与「可修复故障」分开呈现，避免向导里放摆设按钮。
        """
        problems.append({
            "level": level, "text": text, "kind": kind,
            "session_id": session_id,
            "impact": _PROBLEM_IMPACTS.get(kind),
            "fix": fix, "observe": observe,
        })

    for item in items:
        running = item["status"] in ("running", "stopping")
        sid = item["id"]
        if running and not item["registered"]:
            _add(LEVEL_ERROR, "会话「%s」状态为 %s 但引擎未注册"
                 % (item["name"], item["status"]),
                 kind="engine_unregistered", session_id=sid,
                 fix={"label": "停止后重新启动该会话", "to": "/live"})
        bridge_online = (item["bridge"] or {}).get("online")
        if running and bridge_online is False:
            _add(LEVEL_ERROR, "会话「%s」交易通道离线" % item["name"],
                 kind="bridge_offline", session_id=sid,
                 observe="在对应交易客户端/桥进程确认已登录并监听端口；"
                         "外部进程平台不代启，恢复后该条自动消失")
        if running and not item["channel_ok"]:
            _add(LEVEL_ERROR, "会话「%s」主通道不可用" % item["name"],
                 kind="channel_unavailable", session_id=sid,
                 observe="核对系统配置中的通道地址，并在客户端完成登录；"
                         "改配置后需停止并重新启动会话（配置待生效项会提示）")
        data = item["data"]
        # 未运行会话没有引擎/取数需求：通道检查与数据体检不产生「需关注」条目
        # （2026-11-23：02/03 已 stopped 仍报「客户端/桥在线」「体检未完成 0/150」）。
        guard = item["channel_guard"]
        if running and guard["failed"]:
            failed = [str(x) for x in guard["failed"][:4]]
            only_restart = all(
                c.get("key") in ("pending_changes", "engine_registered")
                for c in (guard.get("checks") or [])
                if c.get("ok") is False)
            _add(LEVEL_WARN, "会话「%s」通道检查未通过：%s"
                 % (item["name"], "、".join(failed)),
                 kind="channel_guard", session_id=sid,
                 fix=({"label": "停止后重新启动会话（应用待生效配置）",
                       "to": "/live"} if only_restart else None),
                 observe=(None if only_restart else
                          "按通道检查明细逐项核对；客户端在线/备通道属外部或配置项，"
                          "修好后重跑通道检查即可消失"))
        chan = item.get("channel") or {}
        if running and chan.get("probe_degraded") and chan.get("probe_note"):
            # 探活没探成 ≠ 通道离线：如实标注，避免被误读为故障。
            _add(LEVEL_WARN, "会话「%s」通道探活降级：%s"
                 % (item["name"], chan.get("probe_note")),
                 kind="probe_degraded", session_id=sid,
                 observe="探活超时后沿用上次已知状态，稍后自动重试；无需操作")
        if running and data.get("unavailable"):
            _add(LEVEL_WARN, "会话「%s」数据体检不可用：%s"
                 % (item["name"], data.get("reason") or "行情通道不可用"),
                 kind="data_unavailable", session_id=sid,
                 observe="先修复行情通道（客户端/桥在线）后在实盘工作台重跑数据体检")
        elif running and data.get("incomplete"):
            _add(LEVEL_WARN, "会话「%s」数据体检未完成：%s"
                 % (item["name"], data.get("reason") or "未校验完整"),
                 kind="data_incomplete", session_id=sid,
                 observe="抽样校验未覆盖全部标的（受抽样上限与时间预算限制），"
                         "未校验部分不计缺失、无需补下载；如需全量核对请在通达信客户端人工核验")
        elif running and data.get("available") and not data.get("healthy"):
            bad = int(data.get("bad_total") or 0)
            counts = data.get("status_counts") or {}
            err = int(counts.get("error") or 0)
            timeout_count = int(counts.get("timeout") or 0)
            if bad:
                _add(LEVEL_WARN, "会话「%s」数据缺失/陈旧 %d 项"
                     % (item["name"], bad),
                     kind="data_missing", session_id=sid,
                     observe="在通达信客户端补下载对应周期数据（外部操作），"
                             "完成后重跑数据体检该条消失")
            elif err or timeout_count:
                # 零缺失时不写「缺失」：查不到/超时只是没验成，不是数据缺失。
                _add(LEVEL_WARN, "会话「%s」数据体检查询异常 %d 只、"
                     "取价超时 %d 只（未计入缺失）"
                     % (item["name"], err, timeout_count),
                     kind="data_unverified", session_id=sid,
                     fix={"label": "重跑数据体检", "to": "/live"})
        # 未运行会话不进入「需关注」：告警/顺延/待审批计数仍在会话表与关键事件里
        # 如实可见（同一份事实），但停止态没有可执行的部署修复动作，不作为故障项。
        if running and item["alerts"]["open_decisions"]:
            _add(LEVEL_WARN, "会话「%s」有 %d 条未处理告警"
                 % (item["name"], item["alerts"]["open_decisions"]),
                 kind="open_decisions", session_id=sid,
                 fix={"label": "去决策页签批量确认", "to": "/live?tab=decisions"})
        if running and item["alerts"]["deferred_orders"]:
            _add(LEVEL_WARN, "会话「%s」有 %d 笔待放行顺延委托"
                 % (item["name"], item["alerts"]["deferred_orders"]),
                 kind="deferred_orders", session_id=sid,
                 fix={"label": "去委托页签处理顺延", "to": "/live?tab=orders"})
        if running and item["alerts"]["pending_approvals"]:
            _add(LEVEL_WARN, "会话「%s」有 %d 笔待审批委托"
                 % (item["name"], item["alerts"]["pending_approvals"]),
                 kind="pending_approvals", session_id=sid,
                 fix={"label": "前往审批", "to": "/live?tab=approvals"})

    if not disk["ok"]:
        _add(LEVEL_WARN, "磁盘剩余 %s MB 低于保护阈值 %s MB"
             % (disk["free_mb"], disk["min_free_mb"]),
             kind="disk_low",
             observe="清理磁盘后可自动恢复；磁盘不足时自动备份会跳过（外部操作）")
    if backup_status.get("enabled") and not backup_status.get("auto_done_today"):
        _add(LEVEL_WARN, "今日自动备份尚未完成（计划 %s）"
             % (backup_status.get("daily_at") or "—"),
             kind="backup_pending",
             fix={"label": "去系统页手动备份", "to": "/system"})
    last_auto = backup_status.get("last_auto") or {}
    if last_auto.get("status") == backup_service.STATUS_FAILED:
        _add(LEVEL_WARN, "最近一次自动备份失败：%s"
             % (last_auto.get("message") or "未知原因"),
             kind="backup_failed",
             fix={"label": "去系统页手动备份", "to": "/system"})
    if watchdog.get("active_alarms"):
        _add(LEVEL_WARN, "引擎 watchdog 有 %d 条活跃告警"
             % len(watchdog["active_alarms"]),
             kind="watchdog_alarms",
             observe="在实盘工作台查看会话告警；持续告警通常需停止并重新启动会话")
    running_count = sum(1 for s in items if s["status"] in ("running", "stopping"))
    if running_count and not watchdog.get("running"):
        _add(LEVEL_WARN, "存在运行中会话但 watchdog 未运行",
             kind="watchdog_down",
             observe="watchdog 随 Core 进程启动；需在服务器侧重启 Core（平台不代启进程）")

    level = LEVEL_OK
    if any(p["level"] == LEVEL_ERROR for p in problems):
        level = LEVEL_ERROR
    elif problems:
        level = LEVEL_WARN

    return {
        "generated_at": now.isoformat(),
        # 便于现场核对硬截止是否生效（耗时应在 budget_seconds 内）。
        "budget_seconds": _OVERVIEW_TOTAL_BUDGET_SECONDS,
        "elapsed_seconds": round(_monotonic() - started_at, 2),
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
