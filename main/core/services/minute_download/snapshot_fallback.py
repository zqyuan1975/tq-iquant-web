"""保护窗内「分钟数据合成」只读兜底（P3，最低优先级）。

背景
----
实盘 1m 的三级回落是「自有库 → 原生 ``.lc1`` → 快照合成」。14:55–15:10 是
收盘保护窗（``tq_gate.p0_only``），通道闸只放行 P0，快照取数（默认 P1）被
拒绝；若此时自有库异常且原生停滞，1m 会彻底无源。

本模块只在「快照合成采样」这一条**只读**路径上补一个受控兜底：

- 仅在保护窗内、且窗口收窄到 ``14:55:00–14:57:00`` 才可能触发；
- 触发条件必须窄：库读失败 / 库 lag 超阈值（含库空）/ 原生停滞，三者之一；
- 走独立的最低优先级 ``P3``（让行于 P0/P1/P2），并再叠一层本地限频；
- 只读：只调快照批量取数，不写库、不下单、不改引擎状态；
- 可观测：每次进入兜底 episode 落一条 WARNING + risk 事件（原因 / 库 lag /
  窗口状态），计数进入会话详情 ``minute_download.fallback``，绝不静默降级。

边界（刻意留空的时间段）
------------------------
- ``14:57:00–15:00:00`` **禁止兜底**。沪深两市 14:57 起进入收盘集合竞价，
  快照返回的是集合竞价指示价/量，无法代表连续竞价的 1m OHLCV；用它合成会
  产出假 bar，污染信号与风控。宁可缺这 3 分钟的兜底，也不制造假数据。
- ``15:00`` 后 ``default_in_session`` 已停止采样；``15:10`` 后保护窗结束、
  P1 恢复，均不需要兜底。
"""
from __future__ import annotations

import logging
import threading
import time
from collections import deque
from datetime import datetime, time as dt_time
from typing import Callable, Dict, List, Optional

from core.tq.gate import (
    P3, TdxTaskBlocked, evaluate_policy, now_shanghai,
)
from core.tq.utils import TDXLockTimeout

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK_WINDOW_START = "14:55"
DEFAULT_FALLBACK_WINDOW_END = "14:57"

_DEFAULT_CONFIG = {
    "protected_fallback_enabled": True,
    "protected_fallback_window_start": DEFAULT_FALLBACK_WINDOW_START,
    "protected_fallback_window_end": DEFAULT_FALLBACK_WINDOW_END,
    "store_lag_threshold_minutes": 2,
    "fallback_min_interval_seconds": 5.0,
    "fallback_max_per_minute": 12,
    "fallback_event_cooldown_seconds": 60.0,
    "fallback_state_stale_seconds": 120.0,
}


def _parse_hhmm(value, default: dt_time) -> dt_time:
    text = str(value or "").strip()
    for fmt in ("%H:%M", "%H%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return default


def _positive_float(value, default: float) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if v > 0 else default


def _positive_int(value, default: int) -> int:
    try:
        v = int(value)
    except (TypeError, ValueError):
        return default
    return v if v > 0 else default


class SnapshotFallbackGuard:
    """快照取数守卫：正常走 P1，保护窗内按窄条件走 P3 只读兜底。

    作为 ``SnapshotBarSynthesizer(fetch_snapshot=guard)`` 的可调用对象使用。
    """

    def __init__(
        self,
        fetch_normal: Callable[[List[str]], dict],
        fetch_fallback: Optional[Callable[[List[str]], dict]] = None,
        *,
        store_state: Optional[Callable[[], Optional[dict]]] = None,
        native_state: Optional[Callable[[], Optional[dict]]] = None,
        clock: Callable[[], datetime] = now_shanghai,
        emit_event: Optional[Callable[[dict], None]] = None,
        config: Optional[dict] = None,
    ) -> None:
        cfg = dict(_DEFAULT_CONFIG)
        if isinstance(config, dict):
            cfg.update({k: v for k, v in config.items() if v is not None})
        self._fetch_normal = fetch_normal
        self._fetch_fallback = fetch_fallback or fetch_normal
        self._store_state = store_state
        self._native_state = native_state
        self._clock = clock
        self.emit_event = emit_event

        self._enabled = bool(cfg.get("protected_fallback_enabled", True))
        self._window_start = _parse_hhmm(
            cfg.get("protected_fallback_window_start"),
            dt_time(14, 55))
        self._window_end = _parse_hhmm(
            cfg.get("protected_fallback_window_end"),
            dt_time(14, 57))
        self._lag_threshold = _positive_int(
            cfg.get("store_lag_threshold_minutes"), 2)
        self._min_interval = _positive_float(
            cfg.get("fallback_min_interval_seconds"), 5.0)
        self._max_per_minute = _positive_int(
            cfg.get("fallback_max_per_minute"), 12)
        self._event_cooldown = _positive_float(
            cfg.get("fallback_event_cooldown_seconds"), 60.0)
        self._state_stale = _positive_float(
            cfg.get("fallback_state_stale_seconds"), 120.0)

        self._lock = threading.Lock()
        self._calls: deque = deque()
        self._last_call_mono: Optional[float] = None
        self._stats = {
            "normal": 0, "allowed": 0, "success": 0,
            "blocked_healthy": 0, "blocked_boundary": 0,
            "blocked_rate_limited": 0, "lock_timeout": 0, "errors": 0,
        }
        self._triggered = False
        self._active = False
        self._last_reason = ""
        self._last_emitted_reason = ""
        self._last_trigger_at: Optional[str] = None
        self._last_success_at: Optional[str] = None
        self._last_lag: Optional[int] = None
        self._last_window_state = ""
        self._last_gate_window = ""
        self._events = 0
        self._last_event_mono = 0.0

    # ------------------------------------------------------------------
    # 取数入口
    # ------------------------------------------------------------------

    def __call__(self, codes) -> dict:
        code_list = [str(c) for c in (codes or [])]
        now = self._clock()
        policy = evaluate_policy(now)
        if not policy.get("protected"):
            self._bump("normal")
            return self._fetch_normal(code_list)

        window_state = self._window_state(now)
        if not self._enabled:
            self._note_blocked("fallback_disabled", policy, window_state)
            raise self._blocked("保护窗兜底已在配置中关闭", "fallback_disabled",
                                policy)
        if window_state != "open":
            self._note_blocked(window_state, policy, window_state)
            reason = ("closing_auction" if window_state == "closing_auction"
                      else "outside_fallback_window")
            message = (
                "保护窗兜底已关闭：14:57 起为收盘集合竞价，快照不能代表连续"
                "竞价 1m，拒绝兜底" if reason == "closing_auction"
                else "保护窗兜底仅允许 %s–%s"
                % (self._window_start.strftime("%H:%M"),
                   self._window_end.strftime("%H:%M")))
            raise self._blocked(message, reason, policy)

        triggers, lag = self._triggers()
        if not triggers:
            self._note_blocked("healthy", policy, window_state)
            raise self._blocked(
                "分钟数据源正常，保护窗内不触发快照兜底", "healthy", policy)

        if not self._take_token():
            self._note_blocked("rate_limited", policy, window_state)
            raise self._blocked(
                "保护窗兜底触发本地限频（%ss/%s 每分钟）"
                % (self._min_interval, self._max_per_minute),
                "rate_limited", policy)

        reason_text = ",".join(triggers)
        self._note_trigger(reason_text, lag, policy, window_state)
        try:
            data = self._fetch_fallback(code_list)
        except (TDXLockTimeout, TdxTaskBlocked) as exc:
            self._bump("lock_timeout")
            self._clear_active()
            raise exc
        except Exception:
            self._bump("errors")
            self._clear_active()
            raise
        self._bump("success")
        with self._lock:
            self._last_success_at = now.strftime("%Y-%m-%d %H:%M:%S")
            self._active = False
        return data

    # ------------------------------------------------------------------
    # 判定
    # ------------------------------------------------------------------

    def _window_state(self, now: datetime) -> str:
        t = now.time()
        if t < self._window_start:
            return "before_window"
        if t < self._window_end:
            return "open"
        return "closing_auction"

    def _triggers(self) -> tuple[List[str], Optional[int]]:
        reasons: List[str] = []
        lag: Optional[int] = None
        store = self._safe_call(self._store_state)
        if store:
            try:
                age = store.get("last_query_age_seconds")
                age = None if age is None else float(age)
            except (TypeError, ValueError):
                age = None
            # 只有近期真正查过库，状态才可信；刚启动/轮询暂停时不据此触发。
            if age is not None and age <= self._state_stale:
                lag = store.get("last_lag_minutes")
                last_error_at = str(store.get("last_read_error_at") or "")
                last_ok_at = str(store.get("last_ok_at") or "")
                if last_error_at and (
                        not last_ok_at or last_error_at >= last_ok_at):
                    reasons.append("store_read_error")
                if lag is None or int(lag) > self._lag_threshold:
                    reasons.append("store_stale")
        native = self._safe_call(self._native_state)
        if native and native.get("stalled") is True:
            reasons.append("native_stalled")
        return reasons, lag

    def _take_token(self) -> bool:
        now = time.monotonic()
        with self._lock:
            while self._calls and now - self._calls[0] >= 60.0:
                self._calls.popleft()
            if len(self._calls) >= self._max_per_minute:
                return False
            if (self._last_call_mono is not None
                    and now - self._last_call_mono < self._min_interval):
                return False
            self._calls.append(now)
            self._last_call_mono = now
            return True

    # ------------------------------------------------------------------
    # 可观测
    # ------------------------------------------------------------------

    def _note_trigger(self, reason: str, lag: Optional[int],
                      policy: Dict, window_state: str) -> None:
        now = self._clock()
        now_text = now.strftime("%Y-%m-%d %H:%M:%S")
        should_emit = False
        with self._lock:
            self._bump_locked("allowed")
            self._triggered = True
            self._active = True
            self._last_reason = reason
            self._last_lag = lag
            self._last_trigger_at = now_text
            self._last_window_state = window_state
            self._last_gate_window = str(policy.get("window") or "")
            mono = time.monotonic()
            if (reason != self._last_emitted_reason
                    or mono - self._last_event_mono >= self._event_cooldown):
                self._last_emitted_reason = reason
                self._last_event_mono = mono
                self._events += 1
                should_emit = True
        if not should_emit:
            return
        message = (
            "保护窗内分钟数据兜底触发（原因=%s，库 lag=%s 分钟，窗口=%s，"
            "允许时段=%s–%s）"
            % (reason,
               "未知" if lag is None else lag,
               policy.get("window") or "—",
               self._window_start.strftime("%H:%M"),
               self._window_end.strftime("%H:%M")))
        logger.warning("%s", message)
        payload = {
            "gate": "minute_fallback",
            "rule": "protected_window_fallback",
            "message": message,
            "reason": reason,
            "store_lag_minutes": lag,
            "window": policy.get("window"),
            "window_state": window_state,
            "fallback_window": [self._window_start.strftime("%H:%M"),
                                self._window_end.strftime("%H:%M")],
            "at": now_text,
        }
        if self.emit_event is not None:
            try:
                self.emit_event(payload)
            except Exception:  # noqa: BLE001 — 留痕失败不影响取数
                logger.warning("保护窗兜底事件投递失败", exc_info=True)

    def _note_blocked(self, reason: str, policy: Dict, window_state: str) -> None:
        key = {
            "healthy": "blocked_healthy",
            "rate_limited": "blocked_rate_limited",
        }.get(reason, "blocked_boundary")
        with self._lock:
            self._bump_locked(key)
            self._active = False
            self._last_window_state = window_state
            self._last_gate_window = str(policy.get("window") or "")

    def _blocked(self, message: str, reason: str, policy: Dict) -> TdxTaskBlocked:
        return TdxTaskBlocked(
            message, priority=P3, reason=reason,
            window=str(policy.get("window") or ""))

    def _bump(self, key: str) -> None:
        with self._lock:
            self._bump_locked(key)

    def _bump_locked(self, key: str) -> None:
        self._stats[key] = self._stats.get(key, 0) + 1

    def _clear_active(self) -> None:
        with self._lock:
            self._active = False

    @staticmethod
    def _safe_call(fn) -> Optional[dict]:
        if fn is None:
            return None
        try:
            value = fn()
        except Exception:  # noqa: BLE001 — 观测失败不影响取数主链路
            logger.debug("兜底状态读取失败", exc_info=True)
            return None
        return value if isinstance(value, dict) else None

    def status(self) -> Dict:
        now = self._clock()
        try:
            policy = evaluate_policy(now)
        except Exception:  # noqa: BLE001
            policy = {}
        with self._lock:
            return {
                "enabled": self._enabled,
                "window": {
                    "start": self._window_start.strftime("%H:%M"),
                    "end": self._window_end.strftime("%H:%M"),
                    "state": (self._window_state(now)
                              if policy.get("protected") else "outside"),
                },
                "triggered": self._triggered,
                "active": self._active,
                "reason": self._last_reason,
                "store_lag_minutes": self._last_lag,
                "window_state": (
                    self._last_window_state
                    or str(policy.get("window") or "")),
                "gate_window": (
                    self._last_gate_window
                    or str(policy.get("window") or "")),
                "last_trigger_at": self._last_trigger_at,
                "last_success_at": self._last_success_at,
                "events": self._events,
                "decisions": dict(self._stats),
                "thresholds": {
                    "store_lag_minutes": self._lag_threshold,
                    "min_interval_seconds": self._min_interval,
                    "max_per_minute": self._max_per_minute,
                },
            }


__all__ = [
    "SnapshotFallbackGuard",
    "DEFAULT_FALLBACK_WINDOW_START",
    "DEFAULT_FALLBACK_WINDOW_END",
]
