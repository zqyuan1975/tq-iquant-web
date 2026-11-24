# -*- coding: utf-8 -*-
"""投资看板自动刷新调度（进程内 daemon 线程）。

用户诉求：交易日自动刷新「行业指数 + 成分股」，日线与 30m 两层都要覆盖，
让看板在时点之后读到最新快照，不必手动点刷新。

数据分层与时点（默认值，均可在 config 改）：
- 日线（1d）：09:05 盘前 + 16:00 盘后，口径「行业 + 成分股」。
- 30m：11:35 午休 + 16:00 盘后，口径「行业」（成分股 30m 仍按抽屉 lazy）。
  30m 的 10:00–11:00、13:30–14:30 标签都落在 TQ 闸的交易时段，P2 会被拒；
  因此不按标签实时刷新，而是在放行窗一次性补齐（午休补 4 根、盘后补全天 8 根），
  数据与手动刷新完全同源同精度，只是盘中快照滞后。

行为约定（与平台既有调度同风格）：
- 时点走 ``config.investment_board.auto_refresh``；``enabled`` 默认 true，
  环境变量 ``TQ_BOARD_AUTO_REFRESH_DISABLED=1`` 可整体关闭。
- 仅交易日执行（周末硬规则 + 内置休市表，与 TQ 通道闸同口径）。
- 进程在时点之后启动时，补跑当天已过的**最近一个**时点一次；不补历史日期。
- 幂等：同一交易日同一时点同一周期已有 run（trigger=``auto:HHMM``）即不再发起，
  进程重启同样生效；快照本身按 (as_of, period, code) upsert，不产生重复行。
- 截断续跑：run 结果为 partial/failed 时，在下一个 P2 放行窗自动补跑一次
  （trigger=``auto:HHMMr``，冷却默认 5 分钟，每时点最多补一次），避免 09:05
  被 09:10 交易时段截断后停在 partial 无人处理。
- 尊重 TQ 通道闸：``p2_allowed`` 为假时不发起，每分钟轮询，等闸放开后补跑。
- 与手动刷新共用 ``board_service.REFRESH_LOCK``，避免并发全量刷新。
- 失败可见：run 落 failed/partial（前端「上次刷新」可见）+ logger + notify 外发。
- 只写 ``board_*`` 表，不触碰实盘取数、下单、风控、回测主链。
"""
from __future__ import annotations

import logging
import threading
from datetime import date, datetime, time as dtime, timedelta
from typing import Callable, Dict, List, Optional

from sqlalchemy import select

from core.models.investment_board import BoardRun
from core.services import board_service as svc

logger = logging.getLogger(__name__)

DEFAULT_TIMES = ("09:05", "16:00")
DEFAULT_TIMES_30M = ("11:35", "16:00")
DEFAULT_PERIOD = "1d"
DEFAULT_SCOPE = "all"
DEFAULT_SCOPE_30M = svc.SCOPE_INDUSTRIES
_VALID_SCOPES = (svc.SCOPE_INDUSTRIES, svc.SCOPE_STOCKS, "all")


def _hm_minutes(raw) -> Optional[int]:
    """'HH:MM' → 当日分钟数；兼容 YAML 把未加引号的 ``09:05`` 解析成 545。"""
    if isinstance(raw, int) and not isinstance(raw, bool):
        return raw if 0 <= raw < 24 * 60 else None
    text = str(raw or "").strip()
    hh, sep, mm = text.partition(":")
    if not sep:
        return None
    try:
        h, m = int(hh), int(mm or 0)
    except ValueError:
        return None
    if not (0 <= h < 24 and 0 <= m < 60):
        return None
    return h * 60 + m


def _slot_label(minutes: int) -> str:
    h, m = divmod(minutes, 60)
    return f"{h:02d}{m:02d}"


def _slots(raw_times, default) -> List[dict]:
    times = raw_times
    if not isinstance(times, (list, tuple)) or not times:
        times = list(default)
    slots: List[dict] = []
    seen = set()
    for item in times:
        minutes = _hm_minutes(item)
        if minutes is None or minutes in seen:
            continue
        seen.add(minutes)
        slots.append({"minutes": minutes, "trigger": "auto:" + _slot_label(minutes)})
    slots.sort(key=lambda s: s["minutes"])
    return slots


def settings(cfg: Optional[dict]) -> dict:
    """解析 auto_refresh 配置；非法值回落到安全默认，不抛异常。"""
    raw = ((cfg or {}).get("investment_board") or {}).get("auto_refresh") or {}
    period = str(raw.get("period") or DEFAULT_PERIOD).strip()
    if period not in svc.BOARD_PERIODS:
        period = DEFAULT_PERIOD
    scope = str(raw.get("scope") or DEFAULT_SCOPE).strip()
    if scope not in _VALID_SCOPES:
        scope = DEFAULT_SCOPE
    scope_30m = str(raw.get("scope_30m") or DEFAULT_SCOPE_30M).strip()
    if scope_30m not in _VALID_SCOPES:
        scope_30m = DEFAULT_SCOPE_30M

    tracks: List[dict] = []
    slots = _slots(raw.get("times"), DEFAULT_TIMES)
    if slots:
        tracks.append({
            "period": period, "scope": scope, "slots": slots,
            # 盘前时点取「上一交易日」日线，交给 execute_run 的默认口径
            "as_of_today": False,
        })
    slots_30m = _slots(raw.get("times_30m"), DEFAULT_TIMES_30M)
    # period 配成 30m 时不再重复挂第二条 30m 轨
    if slots_30m and period != "30m":
        tracks.append({
            "period": "30m", "scope": scope_30m, "slots": slots_30m,
            # 盘中/盘后都要当日 as_of，否则 15:00 前会被截断到上一交易日
            "as_of_today": True,
        })
    try:
        cooldown = float(raw.get("retry_cooldown_minutes", 5) or 5)
    except (TypeError, ValueError):
        cooldown = 5.0
    return {
        "enabled": bool(raw.get("enabled", True)),
        "tracks": tracks,
        "retry_partial": bool(raw.get("retry_partial", True)),
        "retry_cooldown_minutes": max(0.0, cooldown),
    }


# ---------------------------------------------------------------- 默认真实依赖

def _default_cfg_provider() -> dict:
    from core.config import load_config
    return load_config()


def _default_now() -> datetime:
    from core.tq.gate import now_shanghai
    return now_shanghai()


def _default_is_trading_day(d: date) -> bool:
    from core.tq.gate import is_trading_day
    return is_trading_day(d)


def _default_policy() -> dict:
    from core.tq.gate import evaluate_policy
    return evaluate_policy()


def _default_session_factory():
    from core.db import SessionLocal
    return SessionLocal()


def _default_notify(level: str, title: str, message: str) -> None:
    try:
        from core.services.notify_service import notify
        notify(level, title, message)
    except Exception:  # noqa: BLE001 — 告警外发失败不得影响调度
        logger.exception("投资看板自动刷新：告警外发失败（已记录日志）")


def _clear_query_cache() -> None:
    try:
        from core.api.investment_board import clear_query_cache
        clear_query_cache()
    except Exception:  # noqa: BLE001 — 缓存清理失败不影响刷新结果
        logger.debug("投资看板自动刷新：清查询缓存失败（忽略）", exc_info=True)


def _default_run(trigger: str, period: str, scope: str,
                 as_of: Optional[date]) -> dict:
    """真实刷新：建 run → 清缓存 → P2 优先级执行（与手动刷新同链路）。"""
    from core.db import SessionLocal
    from core.tq.gate import P2, tdx_priority

    db = SessionLocal()
    try:
        run = svc.create_run(db, period, scope, trigger=trigger)
        run_id = run.id
    finally:
        db.close()
    _clear_query_cache()
    svc.clear_benchmark_cache()
    with tdx_priority(P2):
        return svc.execute_run(run_id, period, scope, as_of=as_of)


# ---------------------------------------------------------------- 调度器

class BoardAutoRefreshScheduler:
    """每日定时刷新看板快照（daemon 线程，每分钟轮询一次）。"""

    def __init__(
        self,
        *,
        cfg_provider: Optional[Callable[[], dict]] = None,
        session_factory: Optional[Callable[[], object]] = None,
        run_fn: Optional[Callable[[str, str, str, Optional[date]], dict]] = None,
        notify_fn: Optional[Callable[[str, str, str], None]] = None,
        policy_fn: Optional[Callable[[], dict]] = None,
        is_trading_day_fn: Optional[Callable[[date], bool]] = None,
        now_fn: Optional[Callable[[], datetime]] = None,
        interval_seconds: float = 60.0,
    ) -> None:
        self._cfg_provider = cfg_provider or _default_cfg_provider
        self._session_factory = session_factory
        self._run_fn = run_fn or _default_run
        self._notify_fn = notify_fn or _default_notify
        self._policy_fn = policy_fn or _default_policy
        self._is_trading_day = is_trading_day_fn or _default_is_trading_day
        self._now = now_fn or _default_now
        self._interval = max(5.0, float(interval_seconds))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._blocked_noted: set = set()

    # ------------------------------------------------------------ 生命周期

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="board-auto-refresh", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._thread = None

    def _loop(self) -> None:
        # 启动即检查一次：进程在时点之后启动时补跑当天最近一个时点
        while not self._stop.is_set():
            try:
                self.maybe_run()
            except Exception:  # noqa: BLE001 — 单轮异常不得终止调度
                logger.exception("投资看板自动刷新：调度轮询异常")
            if self._stop.wait(self._interval):
                break

    # ------------------------------------------------------------ 单轮判定

    def maybe_run(self) -> List[dict]:
        """检查所有轨道；返回本轮实际执行的 run 结果（可能为空）。"""
        cfg = settings(self._cfg_provider())
        if not cfg["enabled"] or not cfg["tracks"]:
            return []
        now = self._now()
        if now.tzinfo is not None:
            now = now.replace(tzinfo=None)
        if not self._is_trading_day(now.date()):
            return []
        results: List[dict] = []
        for track in cfg["tracks"]:
            try:
                result = self._run_track(track, cfg, now)
            except Exception:  # noqa: BLE001 — 单轨异常不影响另一轨
                logger.exception("投资看板自动刷新：轨道 %s 执行异常", track["period"])
                result = None
            if result is not None:
                results.append(result)
        return results

    def _run_track(self, track: dict, cfg: dict,
                   now: datetime) -> Optional[dict]:
        current = now.hour * 60 + now.minute
        due = [s for s in track["slots"] if s["minutes"] <= current]
        if not due:
            return None
        slot = due[-1]
        base = slot["trigger"]
        period = track["period"]

        attempts = self._runs_today(now.date(), period, base)
        if not attempts:
            trigger = base
        else:
            latest = attempts[-1]
            if latest["status"] == "ok":
                return None
            if not cfg["retry_partial"]:
                return None
            if any(a["trigger"] == base + "r" for a in attempts):
                return None  # 每个时点最多补跑一次
            last_at = latest["finished_at"] or latest["started_at"]
            if last_at is not None:
                waited = (now - last_at).total_seconds() / 60.0
                if waited < cfg["retry_cooldown_minutes"]:
                    return None
            trigger = base + "r"

        policy = self._policy_fn() or {}
        if not policy.get("p2_allowed"):
            key = (now.date(), trigger)
            if key not in self._blocked_noted:
                self._blocked_noted.add(key)
                logger.info(
                    "投资看板自动刷新：%s 时点遇 TQ 闸（%s），等待放行后补跑",
                    trigger, policy.get("window"))
            return None

        if not svc.REFRESH_LOCK.acquire(blocking=False):
            logger.info("投资看板自动刷新：已有刷新任务运行，稍后重试")
            return None
        as_of = now.date() if track.get("as_of_today") else None
        try:
            logger.info("投资看板自动刷新开始：trigger=%s period=%s scope=%s as_of=%s",
                        trigger, period, track["scope"], as_of)
            result = self._run_fn(trigger, period, track["scope"], as_of) or {}
            status = str(result.get("status") or "unknown")
            logger.info(
                "投资看板自动刷新完成：trigger=%s status=%s as_of=%s",
                trigger, status, result.get("as_of"))
            self._report(trigger, period, track["scope"], status, result)
            return result
        finally:
            svc.REFRESH_LOCK.release()

    def _report(self, trigger: str, period: str, scope: str,
                status: str, result: dict) -> None:
        """失败/补跑后仍不完整要可见：日志 + 告警，不静默。"""
        if status == "failed":
            error = str(result.get("error") or "未知错误")
            logger.error("投资看板自动刷新失败：trigger=%s error=%s", trigger, error)
            self._notify_fn(
                "warn", "投资看板自动刷新失败",
                f"{trigger} 刷新 {period}（{scope}）失败：{error}"[:500])
            return
        if status == "partial":
            stats = result.get("stats") or {}
            skipped = int((stats.get("industries") or {}).get("skipped") or 0) + \
                int((stats.get("stocks") or {}).get("skipped") or 0)
            if trigger.endswith("r"):
                logger.warning(
                    "投资看板自动刷新补跑后仍有缺漏：trigger=%s skipped=%s",
                    trigger, skipped)
                self._notify_fn(
                    "warn", "投资看板自动刷新仍有缺漏",
                    f"{trigger} 刷新 {period}（{scope}）补跑后仍跳过 {skipped} 项，"
                    f"详见 board_runs"[:500])
            else:
                logger.warning(
                    "投资看板自动刷新部分失败：trigger=%s skipped=%s（将在放行窗补跑一次）",
                    trigger, skipped)

    def _runs_today(self, day: date, period: str, base_trigger: str) -> List[dict]:
        """当天该时点（含补跑）的 run 记录，按开始时间升序。"""
        factory = self._session_factory or _default_session_factory
        db = factory()
        try:
            start = datetime.combine(day, dtime.min)
            end = start + timedelta(days=1)
            rows = db.execute(
                select(BoardRun.trigger, BoardRun.status,
                       BoardRun.started_at, BoardRun.finished_at)
                .where(
                    BoardRun.period == period,
                    BoardRun.trigger.in_((base_trigger, base_trigger + "r")),
                    BoardRun.started_at >= start,
                    BoardRun.started_at < end,
                ).order_by(BoardRun.started_at)
            ).all()
            return [{"trigger": t, "status": s,
                     "started_at": sa, "finished_at": fa}
                    for t, s, sa, fa in rows]
        except Exception:  # noqa: BLE001 — 读失败按未执行处理，交给 run 自身兜底
            logger.exception("投资看板自动刷新：读取当日 run 记录失败")
            return []
        finally:
            try:
                db.close()
            except Exception:  # noqa: BLE001
                pass
