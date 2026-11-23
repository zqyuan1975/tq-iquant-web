"""实盘引擎存活监控（进程内）+ 通道级停滞探针。

两层信号：

- **引擎层**：``engine._last_tick_at``（``_tick_main`` 每轮起始刷新）+ 
  ``live_account_snapshots`` 最新 ``captured_at``（deals 池节拍落库）。抓
  「进程还在、引擎却不动」（线程池饿死/死锁）。
- **通道级**（按 ``dispatcher.channel`` 注册探针）：
  - ``tdx_tq``：行情新鲜度停滞（优先用快照合成器最近一根已闭合 1m bar；
    日线/周线/月线 bar 的 ``00:00:00`` 不参与判定）/ ``_bridge_online=False``；
  - ``ths_ui``：文件交接层 orders 积压未被 cursor 消费、cursor 长时间不推进、
    results 回执未消费（**不受交易时段限制**）；
  - ``goldminer_bridge``：``engine._bridge_online=False`` 持续超过阈值。

**不覆盖进程级崩溃**——本线程是进程内 daemon，进程消失后一并消失，**不会也
不可能重启自己**。进程级崩溃/无响应的自愈由随包分发的 ``--supervise`` 父进程
（``core/supervisor.py``：探活 ``/health`` + 退避重启）负责；本类只做上面的通道
与轮次停摆检测并告警，两者职责不重叠。

阈值（config.yaml ``live.*``，非法/非正回退默认）：
``heartbeat_timeout_seconds``=90、``execution_stall_seconds``=180、
``bridge_timeout_seconds``=90、``market_data_stall_seconds``=180。

告警节流：同一 (会话, 规则) 一轮停滞只报一次，持续停滞每 ``repeat_seconds``
（默认 300s）最多重复一次；恢复后发 ``{rule}_recovered`` 事件并重置。
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from datetime import time as _dtime
from typing import Callable, Dict, Optional

from sqlalchemy import func

from core.config import load_config
from core.db import SessionLocal
from core.models import LiveAccountSnapshot, LiveDecisionEvent, LiveSession
from core.runtime import process_tag
from core.services.notify_service import notify

from .snapshot_bars import default_in_session
from .timing import now_shanghai
from .win_dialog_scope import client_window_present

logger = logging.getLogger(__name__)

# 进程内当前启动的 watchdog（只读接口用；未启动/已停止为 None）
_ACTIVE_WATCHDOG = None


def current_watchdog():
    """返回当前进程内启动的 EngineWatchdog（无则 None）。"""
    return _ACTIVE_WATCHDOG

DEFAULT_HEARTBEAT_TIMEOUT_SECONDS = 90.0
DEFAULT_EXECUTION_STALL_SECONDS = 180.0
DEFAULT_BRIDGE_TIMEOUT_SECONDS = 90.0
DEFAULT_MARKET_STALL_SECONDS = 180.0
DEFAULT_SCAN_INTERVAL_SECONDS = 30.0
DEFAULT_GRACE_SECONDS = 120.0
DEFAULT_REPEAT_SECONDS = 300.0

#: 规则 → 恢复事件名（``xxx_recovered``）
_RECOVERY_EVENT = {
    "engine_stale": "engine_recovered",
    "execution_stalled": "execution_recovered",
    "bridge_unreachable": "bridge_recovered",
    "market_data_stalled": "market_data_recovered",
    "client_offline": "client_recovered",
}

#: 通道类规则（首次判定落 ``live_decision_events``）；``engine_stale`` 属引擎层，
#: 不是通道在线态，不冒充 ``channel_offline``（2026-11-13）。
_CHANNEL_RULES = frozenset({
    "market_data_stalled", "bridge_unreachable", "execution_stalled",
    "client_offline",
})

#: 日线及以上周期（bar_time 为 00:00:00，不具备分钟级行情新鲜度语义）
_LONG_PERIODS = frozenset({"1d", "1w", "1mon", "day", "week", "month"})

#: DB 状态判定「会话不应在跑」的取值（正常 stop 路由会同步置 _running=False，
#: 因此引擎仍在跑即为进程/状态错配；见 _check_owner_mismatch）。
_NOT_RUNNING_STATUSES = frozenset({"stopped", "stopping"})

#: 「同代」容差：DB ``started_at`` 与引擎 ``_started_at`` 相差超过该值即视为
#: 不同一次启动（DB 时间在 create_task 前写、引擎时间在协程内写，正常仅差数秒）。
_GENERATION_TOLERANCE_SECONDS = 120.0

#: 连续竞价边界（用于把「墙钟静默」折算成「有效交易静默」）
_AM_OPEN = _dtime(9, 30)
_LUNCH_START = _dtime(11, 30)
_LUNCH_END = _dtime(13, 0)
#: 盘前判定窗口（含集合竞价）：此时段无行情，只能看客户端是否还在
_PRE_OPEN_START = _dtime(8, 55)


def in_pre_open_window(now: datetime) -> bool:
    """盘前窗口 08:55–09:30（工作日）：只判客户端在线，不判行情。

    2026-11-13 早盘事故落在 08:54–09:30，旧实现 ``_probe_tdx`` 在该时段直接
    ``return None``，退出完全无声。盘后（15:00 之后）仍不判，避免噪音。
    """
    if now.weekday() >= 5:
        return False
    return _PRE_OPEN_START <= now.time() < _AM_OPEN


def record_channel_decision(session_id: int, channel: str, gate: str,
                            action: str, seconds: float, basis: str,
                            message: str) -> None:
    """通道在线态决策事件落库（best-effort，复用 ``live_decision_events``）。

    字段：``gate=channel_offline|channel_recovered``、``layer=live_gate``、
    ``action=trigger|recover``、``param_name=channel:<通道>``、``actual_value``=
    已持续秒数、``message`` 含会话/规则/依据。失败只记日志，**绝不抛回扫描线程**。
    """
    try:
        db = SessionLocal()
        try:
            db.add(LiveDecisionEvent(
                live_session_id=int(session_id),
                gate=gate,
                layer="live_gate",
                action=action,
                param_name=("channel:%s" % channel)[:40],
                actual_value=float(seconds),
                message=(message or "")[:200],
            ))
            db.commit()
        finally:
            db.close()
    except Exception:  # noqa: BLE001 — 观测链路失败不影响监控/交易
        logger.exception(
            "record channel decision failed (session %s, gate %s)",
            session_id, gate)


def _minute_dt(day, minutes: int) -> datetime:
    """当日 00:00 + ``minutes`` 分钟（支持 1440=次日 00:00）。"""
    return datetime.combine(day, _dtime(0, 0)) + timedelta(minutes=int(minutes))


def _hm_minutes(raw) -> int:
    """``'HH:MM'`` → 当日分钟数；兼容 YAML 把未加引号的 ``08:00`` 解析成整数。"""
    if isinstance(raw, int):
        return raw
    hh, _, mm = str(raw).strip().partition(":")
    return int(hh) * 60 + int(mm or 0)


def _protected_windows() -> list:
    """TQ 通道闸的 ``p0_only`` 窗口（分钟区间列表）；闸关闭/配置非法返回空。

    收盘保护窗（默认 14:55–15:10）内闸对 P1/P2 全禁、P0 只做收盘收口，快照采样
    被拒，行情基准停在同一根属预期行为。窗口一律从 ``core.tq.gate.gate_config()``
    读取（不硬编码），跟随 config.yaml 的 ``tq_gate.p0_only``。
    """
    try:
        from core.tq.gate import gate_config
        cfg = gate_config()
        if not isinstance(cfg, dict) or not cfg.get("enabled", True):
            return []
        raw = cfg.get("p0_only")
        if isinstance(raw, (list, tuple)) and len(raw) == 2:
            return [(_hm_minutes(raw[0]), _hm_minutes(raw[1]))]
    except Exception:  # noqa: BLE001 — 闸配置不可用不臆造扣减
        pass
    return []


def _tradable_seconds(start: datetime, end: datetime) -> float:
    """start→end 的有效交易时长：扣除午休 11:30–13:00 与收盘保护窗；隔夜从开盘起算。

    午休/隔夜不是行情停滞（13:00 开盘首根 pm 1m bar 要到 13:01 才闭合，
    09:30 开盘首根同样如此），直接用墙钟相减会在每天开盘后必报假警
    （2026-11-05 13:00:02 实证：last bar=11:41:18，静默 4724 秒）。

    收盘保护窗（``tq_gate.p0_only``，默认 14:55–15:10）同样不是停滞：闸对
    P1/P2 全禁、快照采样被拒，last completed bar 停在同一根是预期行为
    （2026-11-23 14:58 实证：误报「行情停滞 180s」）。
    """
    if start >= end:
        return 0.0
    if start.date() != end.date():
        start = datetime.combine(end.date(), _AM_OPEN)
        if start >= end:
            return 0.0
    total = (end - start).total_seconds()
    day = end.date()
    quiet = 0.0
    lunch_s = datetime.combine(day, _LUNCH_START)
    lunch_e = datetime.combine(day, _LUNCH_END)
    quiet += max(0.0, (min(end, lunch_e) - max(start, lunch_s)).total_seconds())
    for start_min, end_min in _protected_windows():
        win_s = _minute_dt(day, start_min)
        win_e = _minute_dt(day, end_min)
        quiet += max(0.0, (min(end, win_e) - max(start, win_s)).total_seconds())
    return total - quiet


def config_seconds(key: str, default: float, cfg: Optional[dict] = None) -> float:
    """读 ``live.<key>`` 秒数；缺省/非法/非正数回退 ``default``。"""
    cfg = load_config() if cfg is None else cfg
    raw = (cfg.get("live") or {}).get(key) if isinstance(cfg, dict) else None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return float(default)
    return value if value > 0 else float(default)


def heartbeat_timeout_seconds(cfg: Optional[dict] = None) -> float:
    return config_seconds(
        "heartbeat_timeout_seconds", DEFAULT_HEARTBEAT_TIMEOUT_SECONDS, cfg)


def latest_snapshot_at(session_id: int) -> Optional[datetime]:
    """该会话 ``live_account_snapshots`` 最新 ``captured_at``；无记录/失败 → None。"""
    db = SessionLocal()
    try:
        return db.query(func.max(LiveAccountSnapshot.captured_at)).filter(
            LiveAccountSnapshot.live_session_id == session_id).scalar()
    finally:
        db.close()


def live_session_status(session_id: int) -> Optional[str]:
    """该会话在 ``live_sessions.status`` 的当前值；无记录/失败 → None。

    用于「DB 说已停、本进程引擎还在跑」的错配自检（2026-11-18 实证：stop
    打到了没有引擎的进程，DB 置 stopped 后另一进程快照仍每 5s 写入）。
    """
    db = SessionLocal()
    try:
        row = db.query(LiveSession.status).filter(
            LiveSession.id == session_id).first()
        return str(row[0]) if row else None
    finally:
        db.close()


def live_session_started_at(session_id: int) -> Optional[datetime]:
    """该会话 ``live_sessions.started_at``；无记录/为空/失败 → None。

    错配自愈的「同代」判据：DB 行与引擎实例必须是同一次 start 产生的，
    否则 ``stop`` 说的是上一代/另一代会话，不能据此杀当前引擎。
    """
    db = SessionLocal()
    try:
        row = db.query(LiveSession.started_at).filter(
            LiveSession.id == session_id).first()
        return row[0] if row else None
    finally:
        db.close()


@dataclass
class StallSignal:
    """通道探针命中结果。"""

    rule: str
    seconds: float
    message: str
    detail: dict = field(default_factory=dict)


class EngineWatchdog:
    """进程内引擎卡死 + 通道停滞检测（daemon 线程）。"""

    def __init__(
        self,
        sessions_provider: Callable[[], Dict[int, object]],
        *,
        timeout_seconds: Optional[float] = None,
        execution_stall_seconds: Optional[float] = None,
        bridge_timeout_seconds: Optional[float] = None,
        market_stall_seconds: Optional[float] = None,
        interval_seconds: float = DEFAULT_SCAN_INTERVAL_SECONDS,
        grace_seconds: float = DEFAULT_GRACE_SECONDS,
        repeat_seconds: float = DEFAULT_REPEAT_SECONDS,
        now_fn: Callable[[], datetime] = now_shanghai,
        mono_fn: Callable[[], float] = time.monotonic,
        snapshot_fn: Callable[[int], Optional[datetime]] = latest_snapshot_at,
        session_status_fn: Callable[[int], Optional[str]] = live_session_status,
        session_started_at_fn: Callable[[int], Optional[datetime]] = (
            live_session_started_at),
        unregister_fn: Optional[Callable[[int], None]] = None,
        in_session_fn: Callable[[datetime], bool] = default_in_session,
        client_online_fn: Callable[[str], bool] = client_window_present,
        persist_fn: Optional[Callable[..., None]] = None,
    ):
        self._sessions_provider = sessions_provider
        self._timeout = (heartbeat_timeout_seconds() if timeout_seconds is None
                         else float(timeout_seconds))
        self._exec_stall = (
            config_seconds("execution_stall_seconds", DEFAULT_EXECUTION_STALL_SECONDS)
            if execution_stall_seconds is None else float(execution_stall_seconds))
        self._bridge_timeout = (
            config_seconds("bridge_timeout_seconds", DEFAULT_BRIDGE_TIMEOUT_SECONDS)
            if bridge_timeout_seconds is None else float(bridge_timeout_seconds))
        self._market_stall = (
            config_seconds("market_data_stall_seconds", DEFAULT_MARKET_STALL_SECONDS)
            if market_stall_seconds is None else float(market_stall_seconds))
        self._interval = max(float(interval_seconds), 1.0)
        self._grace = max(float(grace_seconds), 0.0)
        self._repeat = max(float(repeat_seconds), 0.0)
        self._now = now_fn
        self._mono = mono_fn
        self._snapshot = snapshot_fn
        # 会话 DB 状态探针（错配自检用；单测可注入，避免读真实库）
        self._session_status = session_status_fn
        # 会话 DB 启动时刻（错配自愈的「同代」判据；单测可注入）
        self._session_started_at = session_started_at_fn
        # 错配自愈：把本进程的孤儿引擎移出注册表（生产注入 ENGINES.pop）
        self._unregister = unregister_fn
        self._in_session = in_session_fn
        # 客户端窗口/进程探针（tdx/ths）：毫秒级 ctypes，不初始化 TQ
        self._client_online = client_online_fn
        # 通道事件落库（None=不落库：单测默认不写真实 dev.db；生产由 main.py 注入）
        self._persist = persist_fn
        # (session_id, rule) -> {"alerted": bool, "last_alert_mono": float}
        self._states: Dict[tuple, dict] = {}
        # 探针用的持续态：cursor 未推进起点 / 桥离线起点 / 回执积压起点
        self._probe_state: Dict[tuple, float] = {}
        # 进程错配态：session_id -> {"alerted", "first_mono", "last_log_mono"}
        self._owner_mismatch: Dict[int, dict] = {}
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        # 自身可观测性（只读）：最近一次扫描时间/耗时/轮数 + 当轮统计
        self._scan_count = 0
        self._last_scan_at: Optional[datetime] = None
        self._last_scan_duration_ms: Optional[float] = None
        self._scan_stats: Dict[str, int] = {
            "sessions": 0, "rules_checked": 0, "alarms": 0}

    # ---------------------------------------------------------------- 生命周期
    def start(self) -> None:
        global _ACTIVE_WATCHDOG
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                _ACTIVE_WATCHDOG = self
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._run, name="engine-watchdog", daemon=True)
            self._thread.start()
            _ACTIVE_WATCHDOG = self

    def stop(self, timeout: float = 2.0) -> None:
        global _ACTIVE_WATCHDOG
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._thread = None
        if _ACTIVE_WATCHDOG is self:
            _ACTIVE_WATCHDOG = None

    def status(self) -> dict:
        """只读状态快照（供 /api/live/watchdog-status）。

        ``last_scan_at`` 停止前进 = 扫描线程自身卡死/退出，这是外部可判据。
        """
        active = [
            {"session_id": sid, "rule": rule, "channel": state.get("channel")}
            for (sid, rule), state in list(self._states.items())
            if state.get("alerted")
        ]
        thread = self._thread
        return {
            "running": thread is not None and thread.is_alive(),
            "last_scan_at": (self._last_scan_at.isoformat()
                             if self._last_scan_at else None),
            "last_scan_duration_ms": self._last_scan_duration_ms,
            "scan_count": self._scan_count,
            "active_alarms": active,
            "owner_mismatch": [
                {"session_id": sid, "db_status": state.get("db_status")}
                for sid, state in list(self._owner_mismatch.items())
                if state.get("alerted")
            ],
            "interval_seconds": self._interval,
            "thresholds": {
                "heartbeat_timeout_seconds": self._timeout,
                "execution_stall_seconds": self._exec_stall,
                "bridge_timeout_seconds": self._bridge_timeout,
                "market_data_stall_seconds": self._market_stall,
            },
        }

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self.scan()
            except Exception:  # noqa: BLE001 — 监控自身绝不能拖垮进程
                logger.exception("engine watchdog scan error")

    # ---------------------------------------------------------------- 检测
    def scan(self) -> None:
        """扫一轮：引擎卡死 + 各通道探针；恢复发对应 recovered 事件。"""
        t0 = time.perf_counter()
        now = self._now()
        self._scan_stats = {"sessions": 0, "rules_checked": 0, "alarms": 0}
        sessions = dict(self._sessions_provider() or {})
        # 用 list() 快照迭代：错配自愈会在扫描过程中把引擎移出注册表
        for session_id, engine in list(sessions.items()):
            try:
                self._scan_one(now, session_id, engine)
            except Exception:  # noqa: BLE001 — 单会话异常不影响其它会话
                logger.exception(
                    "engine watchdog session scan error (session %s)", session_id)
        # 引擎已从注册表移除的会话：清掉错配态（停止/重启都算收敛，不发 recovered）
        for session_id in list(self._owner_mismatch):
            if session_id not in sessions:
                self._owner_mismatch.pop(session_id, None)
        self._scan_count += 1
        self._last_scan_at = now
        self._last_scan_duration_ms = (time.perf_counter() - t0) * 1000.0
        # DEBUG 心跳：默认级别不刷屏，排查时临时调级别即可看到扫描线程在转
        logger.debug(
            "engine watchdog scan: sessions=%d rules_checked=%d alarms=%d duration_ms=%.1f",
            self._scan_stats["sessions"], self._scan_stats["rules_checked"],
            self._scan_stats["alarms"], self._last_scan_duration_ms)

    def _scan_one(self, now: datetime, session_id: int, engine: object) -> None:
        if not getattr(engine, "_running", False):
            self._clear_session(session_id)
            self._owner_mismatch.pop(session_id, None)
            return
        started = getattr(engine, "_started_at", None)
        if started is not None and (now - started).total_seconds() < self._grace:
            return  # 预热宽限

        self._scan_stats["sessions"] += 1
        channel = self._channel(engine)
        # 进程/会话状态错配（DB 已 stopped/stopping 但本进程引擎仍在跑）：
        # 不受交易时段限制——盘后照样会继续写快照，必须能报出来。
        # 自愈已收敛该引擎时本轮不再跑探针（避免对正在停的会话发通道告警）。
        if self._check_owner_mismatch(session_id, engine, channel):
            return
        # 引擎层卡死：仅交易时段判定（无 bar/无快照在盘外是正常）
        if self._in_session(now):
            self._scan_stats["rules_checked"] += 1
            self._check_engine_heartbeat(now, session_id, engine, started, channel)

        # 通道级探针：ths 积压不受时段限制；tdx/bridge 由探针内部各自判定
        self._scan_stats["rules_checked"] += 1
        signal = self._probe(channel, now, session_id, engine)
        if signal is None:
            self._recover_if_alerted(engine, session_id, channel, channel_probe=True)
        else:
            self._maybe_alert(engine, session_id, channel, signal)

    @staticmethod
    def _channel(engine: object) -> str:
        dispatcher = getattr(engine, "_dispatcher", None)
        return str(getattr(dispatcher, "channel", "") or "unknown")

    # ------------------------------------------------------------ 进程错配自检
    def _check_owner_mismatch(self, session_id: int, engine: object,
                              channel: str) -> bool:
        """DB 已 stopped/stopping，但本进程引擎仍在跑 → 进程/状态错配。

        正常 stop 一定由持有引擎的进程处理：路由同步置 ``_running=False``，
        因此本检查只在异常分支命中（预热宽限已在 ``_scan_one`` 扣掉）。命中
        即 ERROR + SSE risk + 落 ``engine_owner_mismatch`` 决策事件，再由
        ``_heal_owner_mismatch`` 收敛（本进程自愈）。

        返回 True 表示引擎已被自愈移出注册表，调用方应跳过本轮其它探针。
        「自愈」必须证据充分：注册表实例一致 + 与 DB 同代 + 无任何新鲜活动；
        任一不满足都只告警、不杀引擎（宁可漏杀不可错杀，2026-11-19 错杀事故）。
        """
        # 过期快照保护：``stop_session`` 先 pop 注册表再 commit，正常 stop 的
        # 瞬间可能被扫到「快照里有引擎、DB 已 stopping」。此时注册表里已无该
        # 引擎（或已换成重启后的新引擎），不是错配，直接跳过。
        try:
            if self._sessions_provider().get(session_id) is not engine:
                return False
        except Exception:  # noqa: BLE001 — 注册表不可读时不构成错配判据
            return False
        try:
            status = self._session_status(session_id)
        except Exception:  # noqa: BLE001 — DB 抖动不应当成错配
            logger.warning(
                "engine watchdog: 读取会话状态失败 (session %s)", session_id,
                exc_info=True)
            return False
        if status is None:
            # 会话行不存在（删除/尚未落库）：不做错配判定
            self._owner_mismatch.pop(session_id, None)
            return False
        if status not in _NOT_RUNNING_STATUSES:
            self._recover_owner_mismatch(session_id, engine, channel, status)
            return False
        # 自愈前置：先判「这真的是一个该杀的孤儿引擎吗」。证据不足时只告警，
        # 保留 _owner_mismatch 态（后续扫描可重判，活动停掉后才可能变成充分）。
        sufficient, evidence = self._orphan_evidence(session_id, engine)
        mono = self._mono()
        state = self._owner_mismatch.get(session_id)
        if state is None:
            state = {"alerted": False, "first_mono": mono,
                     "last_log_mono": 0.0, "db_status": status}
            self._owner_mismatch[session_id] = state
        state["db_status"] = status
        state["evidence"] = evidence
        if state["alerted"] and mono - state["last_log_mono"] < self._repeat:
            return False
        first = not state["alerted"]
        state["alerted"] = True
        state["last_log_mono"] = mono
        seconds = max(mono - state.get("first_mono", mono), 0.0)
        self._scan_stats["alarms"] += 1
        message = (
            "进程/会话状态错配：会话 %s 在 DB 已 %s，但本进程引擎仍在运行"
            "（%s，已持续 %.0f 秒）——快照/成交回填会继续写入。"
            "请确认是否有另一个后端进程持有该引擎（stop 打到了没有引擎的进程）"
            "；%s"
            % (session_id, status, process_tag(), seconds,
               ("自愈证据充分（%s）" % evidence) if sufficient
               else ("证据不足，跳过自愈（%s）" % evidence))
        )
        logger.error("%s", message)
        self._safe_emit(engine, "risk", {
            "session_id": session_id,
            "engine": channel,
            "rule": "engine_owner_mismatch",
            "seconds": int(seconds),
            "db_status": status,
            "self_heal": bool(self._unregister is not None and sufficient),
            "evidence": evidence,
            "message": message,
        })
        if first:
            self._persist_owner_event(
                session_id, channel, "engine_owner_mismatch", "trigger",
                seconds, "db_status=%s;%s" % (status, evidence), message)
        # 自愈默认关闭：只有生产注入 unregister_fn（真实的 ENGINES 注册表）
        # 才收敛停机；未注入（单测/只读诊断）保持「只告警不动引擎」。
        # 证据不充分一律不杀：DB 一行 stopped 不足以认定引擎该停。
        if self._unregister is not None and sufficient:
            self._heal_owner_mismatch(session_id, engine, status)
            return True
        return False

    def _orphan_evidence(self, session_id: int,
                         engine: object) -> tuple:
        """判定「DB 已停、本进程引擎仍在跑」是否足以自愈；返回 (是否充分, 依据)。

        三条判据缺一不可：

        1. 注册表里就是这个实例（调用方已查一次，这里复核，防扫描中换引擎）；
        2. 引擎 ``_started_at`` 与 DB ``started_at`` 同代（差 ≤ 120s）；
        3. 引擎**没有任何新鲜活动**（快照与 ``_last_tick_at`` 均超过心跳阈值）。

        第 3 条是关键：快照/心跳新鲜 = 引擎健在，此时 DB 的 stopped 更可能是
        写错了或 stop 落到了别的进程，杀它就是把健在的交易引擎掐掉。
        读不到判据（DB 抖动/字段为空）也按证据不足处理，只告警。
        """
        try:
            if self._sessions_provider().get(session_id) is not engine:
                return False, "注册表实例已不是本引擎（可能已重启）"
        except Exception:  # noqa: BLE001 — 注册表不可读即证据不足
            return False, "注册表不可读"
        engine_started = getattr(engine, "_started_at", None)
        try:
            db_started = self._session_started_at(session_id)
        except Exception:  # noqa: BLE001 — DB 抖动即证据不足
            logger.warning(
                "engine watchdog: 读取会话 started_at 失败 (session %s)",
                session_id, exc_info=True)
            return False, "DB started_at 读取失败"
        if engine_started is None or db_started is None:
            return False, "缺少启动时刻（engine=%s, db=%s）" % (
                engine_started, db_started)
        diff = abs((engine_started - db_started).total_seconds())
        if diff > _GENERATION_TOLERANCE_SECONDS:
            return False, "started_at 不同代（相差 %.0f 秒）" % diff
        now = self._now()
        try:
            snap = self._snapshot(session_id)
        except Exception:  # noqa: BLE001 — 读不到快照即无法确认孤儿
            logger.warning(
                "engine watchdog: 读取账户快照时间失败 (session %s)",
                session_id, exc_info=True)
            return False, "快照时间读取失败"
        for label, ts in (("快照", snap),
                          ("心跳", getattr(engine, "_last_tick_at", None))):
            if ts is None:
                continue
            age = (now - ts).total_seconds()
            if age <= self._timeout:
                return False, "引擎仍有新鲜活动（%s 距now %.0fs ≤ %.0fs）" % (
                    label, age, self._timeout)
        return True, ("注册表实例一致、started_at 同代（差 %.1fs）、"
                      "快照/心跳均静默超过 %.0fs" % (diff, self._timeout))

    def _heal_owner_mismatch(self, session_id: int, engine: object,
                             status: str) -> None:
        """把「DB 已停、本进程仍在跑」的孤儿引擎收敛停机并移出注册表。

        只会在异常分支触发（status ∈ stopped/stopping 且引擎已过预热宽限）：
        正常 stop 已由路由同步置 ``_running=False``，不会走到这里；会话重启
        会重新构建并注册新引擎，不受影响。先同步置标志（循环在下一拍退出），
        再尽力调度 ``engine.stop()`` 做清理，最后移出注册表，避免本进程继续
        占用该会话、也避免 watchdog 反复告警。
        """
        errors = []
        try:
            if hasattr(engine, "_running"):
                engine._running = False
            if hasattr(engine, "_stop_requested"):
                engine._stop_requested = True
        except Exception as exc:  # noqa: BLE001 — 自愈尽力而为，需留痕
            errors.append("flags:%s" % exc)
        loop = getattr(engine, "_loop_ref", None)
        try:
            if loop is not None and loop.is_running():
                asyncio.run_coroutine_threadsafe(engine.stop(), loop)
        except Exception as exc:  # noqa: BLE001
            errors.append("stop:%s" % exc)
        if self._unregister is not None:
            try:
                self._unregister(session_id)
            except Exception as exc:  # noqa: BLE001
                errors.append("unregister:%s" % exc)
        if errors:
            logger.error(
                "错配自愈部分失败：会话 %s（DB=%s，%s，%s）",
                session_id, status, process_tag(), "; ".join(errors))
        else:
            logger.error(
                "错配自愈：会话 %s（DB=%s）已置 _running=False、调度 engine.stop() "
                "并移出本进程注册表（%s）", session_id, status, process_tag())

    def _recover_owner_mismatch(self, session_id: int, engine: object,
                                channel: str, status: str) -> None:
        state = self._owner_mismatch.pop(session_id, None)
        if not state or not state.get("alerted"):
            return
        seconds = max(self._mono() - state.get("first_mono", self._mono()), 0.0)
        message = (
            "进程/会话状态一致恢复：会话 %s DB=%s，本进程引擎在运行（%s）"
            % (session_id, status, process_tag()))
        logger.info("%s", message)
        self._safe_emit(engine, "engine_owner_recovered", {
            "session_id": session_id,
            "engine": channel,
            "rule": "engine_owner_recovered",
            "db_status": status,
            "message": message,
        })
        self._persist_owner_event(
            session_id, channel, "engine_owner_recovered", "recover",
            seconds, "db_status=%s" % status, message)

    def _persist_owner_event(self, session_id: int, channel: str, gate: str,
                             action: str, seconds: float, basis: str,
                             message: str) -> None:
        """错配事件落 ``live_decision_events``（复用通道事件写入路径）。"""
        if self._persist is None:
            return
        try:
            self._persist(
                session_id=session_id, channel=channel, gate=gate,
                action=action, seconds=seconds, basis=basis,
                message=message)
        except Exception:  # noqa: BLE001 — 观测链路失败不影响监控/交易
            logger.exception(
                "engine watchdog persist owner event failed (session %s, %s)",
                session_id, gate)

    # ---------------------------------------------------------------- 引擎层
    def _check_engine_heartbeat(self, now: datetime, session_id: int, engine: object,
                                started: Optional[datetime], channel: str) -> None:
        last = self._last_activity(session_id, engine, started)
        if last is None:
            return
        silence = (now - last).total_seconds()
        if silence <= self._timeout:
            self._recover_rule(engine, session_id, channel, "engine_stale")
            return
        self._maybe_alert(engine, session_id, channel, StallSignal(
            rule="engine_stale",
            seconds=silence,
            message=(
                "实盘引擎卡死：会话 %s 已静默 %.0f 秒（最后活动 %s，阈值 %.0f 秒）"
                % (session_id, silence, last.strftime("%H:%M:%S"), self._timeout)),
            detail={"last_activity": last.strftime("%Y-%m-%d %H:%M:%S"),
                    "threshold": int(self._timeout)},
        ))

    def _last_activity(self, session_id: int, engine: object,
                       started: Optional[datetime]) -> Optional[datetime]:
        tick = getattr(engine, "_last_tick_at", None)
        snap = None
        try:
            snap = self._snapshot(session_id)
        except Exception:  # noqa: BLE001 — DB 抖动不应误判，另一信号仍可用
            logger.warning(
                "engine watchdog: 读取账户快照时间失败 (session %s)", session_id,
                exc_info=True)
        candidates = [v for v in (tick, snap) if v is not None]
        return max(candidates) if candidates else started

    # ---------------------------------------------------------------- 通道探针
    def _probe(self, channel: str, now: datetime, session_id: int,
               engine: object) -> Optional[StallSignal]:
        has_synth = getattr(engine, "_snapshot_synth", None) is not None
        if channel == "ths_ui":
            # THS 先判执行面（积压/游标）；无异常再看行情新鲜度（THS 行情也走快照合成）
            return (self._probe_ths(now, session_id, engine)
                    or (self._probe_tdx(now, session_id, engine)
                        if has_synth else None))
        if channel == "tdx_tq":
            return self._probe_tdx(now, session_id, engine)
        if channel in ("goldminer_bridge", "iquant_bridge"):
            # 掘金行情由平台走 TQ 快照合成（不是桥 /quote）：桥心跳正常但快照停摆
            # 也必须能报 → 先判行情新鲜度，再看桥不可达（两者任一命中即告警）。
            if has_synth:
                return (self._probe_tdx(now, session_id, engine)
                        or self._probe_bridge(now, session_id, engine))
            return self._probe_bridge(now, session_id, engine)
        if has_synth:
            # 其它挂了快照合成器的会话（alert 监听等）：按行情新鲜度监控
            return self._probe_tdx(now, session_id, engine)
        return None  # 未知通道：仅引擎层监控

    def _uptime_basis(self, now: datetime, engine: object) -> Optional[datetime]:
        """启动时刻基准；同日起步的会话取 max(启动时刻, 今日 09:30)。

        开盘前启动（如 09:15/09:20）时，09:30 前一根已闭合 1m bar 都还没有，
        直接用启动时刻算「行情静默」会在每天 09:30 必报一次假警（首根 bar 要到
        09:31 才闭合）。把基准抬到开盘时刻，只有开盘后真停滞才告警。
        """
        started = getattr(engine, "_started_at", None)
        if started is None:
            return None
        if started.date() == now.date():
            floor = datetime.combine(now.date(), _AM_OPEN)
            if started < floor:
                return floor
        return started

    def _probe_tdx_client(self, now: datetime, session_id: int,
                          engine: object) -> Optional[StallSignal]:
        """通达信客户端窗口/进程是否还在（毫秒级 ctypes，不初始化 TQ）。

        这是**不依赖心跳侧证**的在线判据：客户端退出 = 行情与下单同时不可用，
        盘前同样成立。探测本身异常按「在线」处理（不误判离线）。
        持续离线起点用单调时钟记在 ``_probe_state``，恢复即清。
        """
        try:
            online = bool(self._client_online("tdx"))
        except Exception:  # noqa: BLE001 — 探针异常不等于客户端掉了
            logger.warning(
                "engine watchdog: tdx 客户端探测失败 (session %s)", session_id,
                exc_info=True)
            online = True
        key = (session_id, "client_offline")
        if online:
            self._probe_state.pop(key, None)
            return None
        since = self._probe_state.setdefault(key, self._mono())
        seconds = max(self._mono() - since, 0.0)
        return StallSignal(
            rule="client_offline", seconds=seconds,
            message=("通达信客户端不在线：会话 %s 未检测到通达信窗口/进程"
                     "（客户端可能已退出；已持续约 %.0f 秒）"
                     % (session_id, seconds)),
            detail={"basis": "client_window", "engine": "tdx_tq"})

    def _probe_tdx(self, now: datetime, session_id: int,
                   engine: object) -> Optional[StallSignal]:
        """客户端在线 + 行情新鲜度 / 行情链路离线。

        时段口径（2026-11-13）：连续竞价判行情；盘前 08:55–09:30 只判客户端
        （盘前没有行情，不能用「合成停摆」推断）；盘后/午休/非交易日不判。

        新鲜度基准优先取快照合成器「最近一根已闭合 1m bar」（tdx/ths/alert/掘金
        的盘中数据源）。**策略周期 bar（1d/1w/1mon）的 00:00:00 不参与判定**——
        直接与墙钟相减会在每天开盘后必报假警（2026-11-05 14:31 实证：
        last bar=00:00:00、静默 52278 秒，实际 5s 账户采样与 1m bar 一直在跑）。
        """
        in_session = self._in_session(now)
        if not in_session and not in_pre_open_window(now):
            return None  # 盘后/午休/非交易日不判，避免噪音
        # 客户端是否还在（不依赖心跳侧证；盘前同样有效）
        client = self._probe_tdx_client(now, session_id, engine)
        if client is not None:
            return client
        if not in_session:
            return None  # 盘前客户端在：无行情可判，不臆造停滞
        if not getattr(engine, "_bridge_online", True):
            started = getattr(engine, "_started_at", None)
            last = getattr(engine, "_last_tick_at", None) or started
            seconds = (_tradable_seconds(last, now) if last
                       else self._market_stall)
            return StallSignal(
                rule="market_data_stalled", seconds=seconds,
                message=("行情链路不可用（bridge_online=False）：会话 %s，"
                         "已持续约 %.0f 秒" % (session_id, seconds)),
                detail={"reason": "market_offline"})
        basis = None
        basis_name = ""
        synth = getattr(engine, "_snapshot_synth", None)
        if synth is not None:
            basis = getattr(synth, "last_completed_at", None)
            basis_name = "snapshot_1m"
            if basis is None:
                # 合成器在跑但尚无已闭合 1m bar：以启动时刻计（宽限已在外层扣），
                # 同日起步抬到开盘时刻，消掉 09:30 的固定假警。
                basis = self._uptime_basis(now, engine)
                basis_name = "uptime"
        else:
            bar = getattr(engine, "_available_bar", None)
            period = str(getattr(bar, "period", "") or "").lower()
            if period in _LONG_PERIODS:
                # 日线及以上：bar_time 为 00:00:00，无分钟级新鲜度语义 → 不判定，
                # 由引擎层心跳/桥探针兜底（不臆造告警）。
                return None
            basis = getattr(bar, "bar_time", None)
            basis_name = "bar_%s" % (period or "1m")
            if basis is None:
                basis = self._uptime_basis(now, engine)
                basis_name = "uptime"
        if basis is None:
            return None
        # 午休/隔夜不计入停滞（否则 13:00 与 09:30 开盘首分钟必报假警）
        silence = _tradable_seconds(basis, now)
        if silence <= self._market_stall:
            return None
        return StallSignal(
            rule="market_data_stalled", seconds=silence,
            message=("行情停滞：会话 %s 最后行情基准=%s（%s），已 %.0f 秒未推进"
                     "（阈值 %.0f 秒）"
                     % (session_id, basis.strftime("%H:%M:%S"), basis_name, silence,
                        self._market_stall)),
            detail={"last_bar_time": basis.strftime("%Y-%m-%d %H:%M:%S"),
                    "basis": basis_name,
                    "threshold": int(self._market_stall)})

    def _probe_ths(self, now: datetime, session_id: int,
                   engine: object) -> Optional[StallSignal]:
        """ths：orders 积压未消费 / cursor 不推进 / results 回执未消费。

        **不受交易时段限制**——任何时段积压都是异常。
        """
        handoff = getattr(engine, "_file_handoff", None)
        if handoff is None:
            return None
        try:
            lines = self._read_lines(handoff.orders_path)
        except OSError:
            return None
        total = len(lines)
        offset = self._cursor_offset(handoff.cursor_path, total)
        backlog = max(total - offset, 0)
        mono = self._mono()
        cursor_key = (session_id, "ths_cursor")
        if backlog > 0:
            since = self._probe_state.setdefault(cursor_key, mono)
            no_advance = mono - since
        else:
            self._probe_state.pop(cursor_key, None)
            no_advance = 0.0
        oldest_wait = self._oldest_pending_seconds(lines, offset, now)
        # 回执积压（results 新增但引擎未 execution_receipts 弹出）
        dispatcher = getattr(engine, "_dispatcher", None)
        pending_receipts = int(getattr(dispatcher, "pending_receipt_count", 0) or 0)
        receipt_key = (session_id, "ths_receipts")
        if pending_receipts > 0:
            since = self._probe_state.setdefault(receipt_key, mono)
            receipts_wait = mono - since
        else:
            self._probe_state.pop(receipt_key, None)
            receipts_wait = 0.0
        stalled = backlog > 0 and (
            (oldest_wait is not None and oldest_wait > self._exec_stall)
            or no_advance > self._exec_stall)
        if stalled:
            seconds = max(oldest_wait or 0.0, no_advance)
            return StallSignal(
                rule="execution_stalled", seconds=seconds,
                message=("ths 执行停滞：会话 %s orders 积压 %d 行未消费，最老待消费"
                         " %.0f 秒，cursor=%d/%d，%.0f 秒无推进"
                         % (session_id, backlog, oldest_wait or 0.0, offset, total,
                            no_advance)),
                detail={"engine": "ths", "backlog": backlog,
                        "oldest_pending_seconds": int(oldest_wait or 0),
                        "cursor_offset": offset, "orders_total": total})
        if pending_receipts > 0 and receipts_wait > self._exec_stall:
            return StallSignal(
                rule="execution_stalled", seconds=receipts_wait,
                message=("ths 回执积压：会话 %s results 回执 %d 条未消费，已 %.0f 秒"
                         "（cursor=%d/%d）"
                         % (session_id, pending_receipts, receipts_wait, offset, total)),
                detail={"engine": "ths", "backlog": pending_receipts,
                        "reason": "results_unconsumed",
                        "cursor_offset": offset, "orders_total": total})
        return None

    def _probe_bridge(self, now: datetime, session_id: int,
                      engine: object) -> Optional[StallSignal]:
        """HTTP 桥：引擎在跑但心跳已标离线并持续超过阈值。"""
        if not self._in_session(now):
            return None
        if getattr(engine, "_bridge_online", True):
            self._probe_state.pop((session_id, "bridge_offline"), None)
            return None
        mono = self._mono()
        key = (session_id, "bridge_offline")
        since = self._probe_state.setdefault(key, mono)
        wait = mono - since
        if wait <= self._bridge_timeout:
            return None
        return StallSignal(
            rule="bridge_unreachable", seconds=wait,
            message=("交易桥不可达：会话 %s 引擎运行中但 bridge_online=False，"
                     "已持续 %.0f 秒（阈值 %.0f 秒）"
                     % (session_id, wait, self._bridge_timeout)),
            detail={"engine": self._channel(engine), "threshold": int(self._bridge_timeout)})

    # ---------------------------------------------------------------- 告警/恢复
    def _maybe_alert(self, engine: object, session_id: int, channel: str,
                     signal: StallSignal) -> None:
        key = (session_id, signal.rule)
        state = self._states.setdefault(
            key, {"alerted": False, "last_alert_mono": 0.0})
        mono = self._mono()
        if state["alerted"] and mono - state["last_alert_mono"] < self._repeat:
            return
        first = not state["alerted"]
        state["alerted"] = True
        state["last_alert_mono"] = mono
        state["channel"] = channel
        if first:
            # 离线起点（首次判定时刻），恢复行据此算出「断了多久」
            state["first_mono"] = mono
        self._scan_stats["alarms"] += 1
        payload = {
            "session_id": session_id,
            "engine": channel,
            "rule": signal.rule,
            "seconds": int(signal.seconds),
            "count": int(signal.detail.get("backlog", 1) or 1),
            "message": signal.message,
        }
        payload.update(signal.detail)
        logger.error("%s", signal.message)
        self._safe_emit(engine, "risk", payload)
        # 告警外发（旁路）：只在「进入告警」时发一次（300s 重复周期不再刷屏），
        # notify 内部另有静默期去重。失败/未启用都不影响监控与交易。
        if first:
            notify(
                "error", "通道离线/停滞",
                signal.message,
                gate="channel_%s" % signal.rule,
                code=channel,
                trade_date=self._now().date(),
                fields={"会话": session_id, "通道": channel,
                        "规则": signal.rule,
                        "持续": "%d 秒" % int(signal.seconds)},
            )
        # 单次判定即留痕（不等 300s 重复周期），几分钟闪断也能在库里查到
        if first and signal.rule in _CHANNEL_RULES:
            self._persist_channel_event(
                session_id, channel, signal.rule, online=False,
                seconds=signal.seconds,
                message=("通道离线：%s（会话 %s，判据=%s，已持续 %d 秒）%s"
                         % (channel, session_id, signal.rule,
                            int(signal.seconds), signal.message)))

    def _persist_channel_event(self, session_id: int, channel: str, rule: str,
                               *, online: bool, seconds: float = 0.0,
                               message: str = "") -> None:
        """落一条 ``channel_offline``/``channel_recovered``（失败只记日志）。"""
        if self._persist is None:
            return
        try:
            self._persist(
                session_id=session_id,
                channel=channel,
                gate="channel_recovered" if online else "channel_offline",
                action="recover" if online else "trigger",
                seconds=seconds,
                basis=rule,
                message=message,
            )
        except Exception:  # noqa: BLE001 — 观测链路失败不影响监控/交易
            logger.exception(
                "engine watchdog persist channel event failed (session %s, %s)",
                session_id, rule)

    def _recover_rule(self, engine: object, session_id: int, channel: str,
                      rule: str) -> None:
        key = (session_id, rule)
        state = self._states.pop(key, None)
        if not state or not state.get("alerted"):
            return
        since = state.get("first_mono")
        seconds = max(self._mono() - since, 0.0) if since is not None else 0.0
        name = "%s（%s）" % (session_id, channel)
        message = "实盘监控恢复：会话 %s 的 %s 已恢复正常" % (name, rule)
        logger.info("%s", message)
        self._safe_emit(engine, _RECOVERY_EVENT.get(rule, rule + "_recovered"), {
            "session_id": session_id,
            "engine": channel,
            "rule": _RECOVERY_EVENT.get(rule, rule + "_recovered"),
            "message": message,
        })
        if rule in _CHANNEL_RULES:
            self._persist_channel_event(
                session_id, channel, rule, online=True, seconds=seconds,
                message=("通道恢复：%s（会话 %s，判据=%s，已持续 %d 秒）"
                         % (channel, session_id, rule, int(seconds))))
        # 告警外发（旁路）：通道恢复（warn 级别，默认 min_level 下可见）
        notify(
            "warn", "通道恢复", message,
            gate="channel_%s_recovered" % rule, code=channel,
            trade_date=self._now().date(),
            fields={"会话": session_id, "通道": channel, "规则": rule,
                    "持续": "%d 秒" % int(seconds)},
        )

    def _recover_if_alerted(self, engine: object, session_id: int, channel: str,
                            channel_probe: bool) -> None:
        """通道探针未命中：清该会话所有通道类规则的告警态并各发一次恢复。"""
        for rule in ("execution_stalled", "bridge_unreachable",
                     "market_data_stalled", "client_offline"):
            self._recover_rule(engine, session_id, channel, rule)

    def clear_session(self, session_id: int) -> None:
        """会话停止（从 ENGINES 移除）后清该会话监控态。

        否则 ``status().active_alarms`` 会把停止前的告警永久挂着（2026-11-13
        实证：会话 5 停止后 ``client_offline`` 红灯不灭，直到重新启动会话）。
        只清内存态，不写库、不发 SSE（停止不是「恢复」，不应伪造 recovered）。
        """
        self._clear_session(session_id)

    def _clear_session(self, session_id: int) -> None:
        for key in [k for k in self._states if k[0] == session_id]:
            self._states.pop(key, None)
        for key in [k for k in self._probe_state if k[0] == session_id]:
            self._probe_state.pop(key, None)

    @staticmethod
    def _safe_emit(engine: object, event_type: str, payload: dict) -> None:
        emit = getattr(engine, "_emit", None)
        if emit is None:
            return
        try:
            emit(event_type, payload)
        except Exception:  # noqa: BLE001 — SSE 队列异常不影响监控与交易
            logger.exception("engine watchdog emit error (%s)", event_type)

    # ---------------------------------------------------------------- 文件读取
    @staticmethod
    def _read_lines(path: str) -> list:
        with open(path, "r", encoding="utf-8") as f:
            return f.readlines()

    @staticmethod
    def _cursor_offset(path: str, total: int) -> int:
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            offset = max(int(data.get("orders_offset") or 0), 0)
        except (OSError, ValueError, TypeError):
            return 0
        return 0 if offset > total else offset

    @staticmethod
    def _oldest_pending_seconds(lines: list, offset: int,
                                now: datetime) -> Optional[float]:
        for raw in lines[offset:]:
            text = raw.strip()
            if not text:
                continue
            try:
                entry = json.loads(text)
            except (ValueError, TypeError):
                continue
            ts = entry.get("ts")
            if not ts:
                continue
            try:
                dt = datetime.fromisoformat(str(ts))
            except ValueError:
                continue
            return max((now - dt).total_seconds(), 0.0)
        return None
