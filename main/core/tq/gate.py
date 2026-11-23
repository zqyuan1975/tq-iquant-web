"""TQ 通道优先级闸（P0/P1/P2 + 交易时间窗）。

背景
----
通达信 TQ 一台电脑只能开一个实例，实盘与回测/刷新类任务必然共用同一通道。
原实现是一把进程级 ``threading.Lock``：无公平性、无优先级、无时段概念。
实测（2026-11-19 13:34–13:36、2026-11-20 12:40–12:45）后台重任务持锁期间，
实盘快照/成交回填/下单全部 ``TDXLockTimeout``，通道被判离线并暂停交易。

分级
----
- P0 实盘主链：行情轮询与快照、信号计算、下单/撤单、成交回填、账户快照、
  风控检查。任何时刻可拿通道，且等待中的 P1/P2 必须在**当前单次调用结束**
  后让出（优先级准入，不是排队等锁）。
- P1 实盘辅助：门控快照、市场多空、行业评级刷新、自选同步、分钟源探测、
  指标预热。交易时段可跑但受每分钟配额约束；14:55–15:10 保护窗口全禁。
- P2 后台重任务：回测/滚动/参数扰动/稳健性对比、行业与成分股全量刷新、
  增量重建、公司行为回填。交易时段直接拒绝（``TdxTaskBlocked``，立即返回，
  不再空转 30s）；盘前 08:00–09:10、午休 11:30–13:00、15:15 后至次日 08:00
  放开；非交易日全天放开。

前提：以上时段限制**只在有实盘引擎运行时生效**（判据为进程级
``live_service.ENGINES`` 非空）。刚安装/纯回测场景没有实盘在跑，P1/P2 不因
时段被拒（``window=no_live_engine``），只保留配额与分片。

时间窗全部来自 ``config.yaml`` 的 ``tq_gate`` 段（见 ``DEFAULT_GATE_CONFIG``），
不在代码里硬编码。闸在进程内，改代码或放开/收紧默认值需重启 Core。
"""
from __future__ import annotations

import logging
import os
import threading
import time
from contextlib import contextmanager
from datetime import date, datetime, time as _time, timedelta, timezone
from typing import Callable, Dict, Iterator, Optional, Tuple

from core.config import load_config

logger = logging.getLogger(__name__)

# 优先级：数值越小越优先（P0 实盘主链）
P0 = 0
P1 = 1
P2 = 2
PRIORITY_NAMES = {P0: "P0", P1: "P1", P2: "P2"}

_SH_TZ = timezone(timedelta(hours=8))
_SHANGHAI = "Asia/Shanghai"

DEFAULT_GATE_CONFIG: Dict = {
    # 总开关：关闭时退化为原「互斥锁 + 30s 超时」，不做分级/时段/配额。
    "enabled": True,
    # 盘前放开 P2（集合竞价 09:15 之前留出通道给实盘预热）
    "pre_market_p2": ["08:00", "09:10"],
    # 午休放开 P2（交易所不受理委托，实盘只做低频快照）
    "lunch_p2": ["11:30", "13:00"],
    # 收盘后放开 P2（至次日 00:00；00:00–08:00 同样放开，见 _SECOND_HALF_OPEN）
    "after_close_p2": ["15:15", "24:00"],
    # 交易时段：只允许 P0 + 限额 P1
    "trading": [["09:10", "11:30"], ["13:00", "15:10"]],
    # 收盘保护窗口：仅 P0（15:05 日线轮次实测 52–64s，留缓冲）
    "p0_only": ["14:55", "15:10"],
    # 交易时段 P1/P2 每分钟最大放行次数（令牌桶，防单类任务长期占满通道）
    "p1_max_per_minute": 600,
    "p2_max_per_minute": 600,
    # P2 取数分片：每次 TQ 调用最多取多少只票，片间放锁让 P0 插队
    "p2_chunk_size": 60,
    # 手工紧急放开的最长有效期（分钟）
    "override_max_minutes": 120,
}


class TDXConnectionError(Exception):
    """通达信通道不可用（未启动/未登录/tqcenter 缺失/初始化失败）。"""


class TDXLockTimeout(TDXConnectionError):
    """等待 TQ 通道超时（原 P2-7 语义，保留）。

    继承 TDXConnectionError：对上层语义仍是「本轮 TQ 通道不可用」——路由层
    可直接给 503，引擎轮询按「取数失败」跳过本轮，不需要额外分支。
    """


class TdxTaskBlocked(TDXConnectionError):
    """任务被 TQ 通道闸的时段策略拒绝（**未发起任何 TQ 调用**）。

    与 :class:`TDXLockTimeout` 的区别：这是「按规则不允许现在跑」，应立即把
    明确原因与下次可用时间返回给调用方（API 409），而不是等待超时。
    """

    def __init__(
        self,
        message: str,
        *,
        priority: int,
        reason: str,
        window: str = "",
        next_available_at: Optional[datetime] = None,
    ) -> None:
        super().__init__(message)
        self.priority = priority
        self.reason = reason
        self.window = window
        self.next_available_at = next_available_at

    def to_dict(self) -> Dict:
        return {
            "priority": PRIORITY_NAMES.get(self.priority, str(self.priority)),
            "reason": self.reason,
            "window": self.window,
            "next_available_at": (
                self.next_available_at.isoformat(timespec="seconds")
                if self.next_available_at else None
            ),
            "message": str(self),
        }


# ---------------------------------------------------------------- 配置/时钟

_cfg_cache: Optional[Dict] = None
_cfg_injected = False
_clock: Optional[Callable[[], datetime]] = None
_trading_day_fn: Optional[Callable[[date], bool]] = None
_live_running_fn: Optional[Callable[[], bool]] = None
_policy_cache: Optional[Tuple[str, Dict]] = None


def _live_running() -> bool:
    """当前进程是否存在运行中的实盘引擎。

    时段限制只为「给实盘让路」而存在：没有实盘在跑时（如刚安装、纯回测使用），
    P1/P2 不应因交易时段被拒。注册表是进程级 dict（``live_service.ENGINES``），
    启动/停止会话会即时反映到下一次 ``evaluate_policy``（缓存键含该标记）。
    无法判定时按「有实盘」处理（保守：维持时段限制，不放开重任务）。
    """
    if _live_running_fn is not None:
        return _live_running_fn()
    try:
        from core.services import live_service
        return bool(live_service.ENGINES)
    except Exception:  # noqa: BLE001 — 导入/判定异常不放开闸
        return True


def gate_config(force_reload: bool = False) -> Dict:
    """``config.yaml`` 的 ``tq_gate`` 段（缺键回退 ``DEFAULT_GATE_CONFIG``）。

    缓存一次：TQ 调用是热路径；改配置需重启 Core（与 tq_lock_timeout_seconds 同）。
    ``TQ_GATE_FORCE_ALLOW=1`` 时强制关闭闸（测试进程用：避免用例受墙上时钟的
    交易时段影响）；``reset_for_tests(config=...)`` 注入的配置不受该环境变量影响。
    """
    global _cfg_cache
    if _cfg_cache is None or force_reload:
        merged = dict(DEFAULT_GATE_CONFIG)
        try:
            raw = load_config().get("tq_gate") or {}
        except Exception:  # noqa: BLE001 — 配置损坏按默认值运行，绝不阻断通道
            raw = {}
        if isinstance(raw, dict):
            merged.update(raw)
        _cfg_cache = merged
    if not _cfg_injected and os.environ.get("TQ_GATE_FORCE_ALLOW") == "1":
        return {**_cfg_cache, "enabled": False}
    return _cfg_cache


def now_shanghai() -> datetime:
    if _clock is not None:
        return _clock()
    return datetime.now(_SH_TZ)


def reset_for_tests(
    config: Optional[Dict] = None,
    clock: Optional[Callable[[], datetime]] = None,
    trading_day_fn: Optional[Callable[[date], bool]] = None,
    live_running_fn: Optional[Callable[[], bool]] = None,
) -> None:
    """测试注入：重置配置缓存/时钟/交易日/实盘运行判定（生产代码不调用）。"""
    global _cfg_cache, _cfg_injected, _clock, _trading_day_fn, _live_running_fn
    global _policy_cache
    _cfg_cache = dict(config) if config is not None else None
    _cfg_injected = config is not None
    _clock = clock
    _trading_day_fn = trading_day_fn
    _live_running_fn = live_running_fn
    _policy_cache = None


def _trading_day(d: date) -> bool:
    """交易日判定：周末硬规则 + 内置官方休市表；未知年份 fail-open。"""
    if _trading_day_fn is not None:
        return _trading_day_fn(d)
    if d.weekday() >= 5:
        return False
    try:
        # 延迟导入，避免 core.tq → core.engine 的包级循环依赖
        from core.engine.live.calendar import BUILTIN_CLOSURES
    except Exception:  # noqa: BLE001 — 日历表不可用时按工作日放行（fail-open）
        return True
    closed = BUILTIN_CLOSURES.get(d.year)
    if closed is None:
        return True
    return d not in closed


def _hm_minutes(raw) -> int:
    """'HH:MM' → 当日分钟数；兼容 YAML 把未加引号的 ``08:00`` 解析成 480 的情况。"""
    if isinstance(raw, int):
        return raw
    text = str(raw).strip()
    hh, _, mm = text.partition(":")
    h = int(hh)
    m = int(mm or 0)
    return h * 60 + m


def _in_window(now: datetime, window) -> bool:
    """``["HH:MM","HH:MM"]`` 左闭右开；支持跨零点（如 00:00–08:00 不跨界，24:00 表示末尾）。"""
    start, end = _hm_minutes(window[0]), _hm_minutes(window[1])
    cur = now.hour * 60 + now.minute
    return start <= cur < end


def _windows(cfg: Dict, key: str):
    raw = cfg.get(key) or []
    if key == "trading":
        return [w for w in raw
                if isinstance(w, (list, tuple)) and len(w) == 2]
    return [raw] if isinstance(raw, (list, tuple)) and len(raw) == 2 else []


def _override_active(now: Optional[datetime] = None) -> bool:
    with _override_lock:
        return _override is not None and float(_override["expires_at"]) > time.time()


# ---------------------------------------------------------------- 时段策略


def evaluate_policy(now: Optional[datetime] = None) -> Dict:
    """当前时刻的闸策略快照（供闸准入、状态接口、拒绝文案共用）。

    生产路径按分钟缓存（时段判定只到分钟）；注入时钟（测试）时不缓存。
    """
    global _policy_cache
    if now is None:
        now = now_shanghai()
    live_running = _live_running()
    if _clock is None:
        key = now.strftime("%Y%m%d%H%M") + (":1" if live_running else ":0")
        if _policy_cache is not None and _policy_cache[0] == key:
            return _policy_cache[1]
    cfg = gate_config()
    trading_day = _trading_day(now.date())
    override = _override_active(now)

    protected = trading_day and any(_in_window(now, w) for w in _windows(cfg, "p0_only"))
    p2_open = (not trading_day) or override or any(
        _in_window(now, w) for w in (
            _windows(cfg, "pre_market_p2")
            + _windows(cfg, "lunch_p2")
            + _windows(cfg, "after_close_p2")
            # 「15:15 后至次日 08:00」的凌晨段
            + [["00:00", "08:00"]]
        )
    )
    # 保护窗口优先于 P2 放开窗口（两者当前不重叠，防未来配置重叠时误放行）
    if protected:
        p2_open = False
    p1_open = not protected

    if not live_running:
        # 无实盘引擎：时段限制整体不生效（限制只为给实盘让路而存在），
        # 仅保留配额与分片。避免「没实盘却禁回测/刷新」。
        protected = False
        p2_open = True
        p1_open = True

    if not live_running:
        window = "no_live_engine"
    elif not trading_day:
        window = "non_trading_day"
    elif protected:
        window = "p0_only"
    elif any(_in_window(now, w) for w in _windows(cfg, "pre_market_p2")):
        window = "pre_market"
    elif any(_in_window(now, w) for w in _windows(cfg, "lunch_p2")):
        window = "lunch"
    elif any(_in_window(now, w) for w in _windows(cfg, "after_close_p2")) \
            or _in_window(now, ["00:00", "08:00"]):
        window = "after_close"
    elif any(_in_window(now, w) for w in _windows(cfg, "trading")):
        window = "trading"
    else:
        # 09:10–11:30/13:00–15:10 之外的未标注间隙（如 15:10–15:15）：
        # 按交易口径（P2 不放行）取安全侧，避免收盘收口被后台任务干扰。
        window = "trading_buffer"

    policy = {
        "now": now.isoformat(timespec="seconds"),
        "trading_day": trading_day,
        # 是否有运行中的实盘引擎：false 时 window=no_live_engine，P1/P2 全放行
        "live_running": live_running,
        "window": window,
        "override": override,
        "protected": protected,
        "p1_allowed": p1_open,
        "p2_allowed": p2_open,
        # 配额与分片始终保留（即使无实盘/放开窗口）：保护 TQ 通道本身
        "p2_quota_active": trading_day,
        "p1_quota_active": trading_day,
        "next_p2_open_at": None if p2_open else _next_p2_open(now, cfg),
    }
    if _clock is None:
        _policy_cache = (now.strftime("%Y%m%d%H%M")
                         + (":1" if live_running else ":0"), policy)
    return policy


def _protected_end(now: datetime, cfg: Dict) -> datetime:
    """当前保护窗口的结束时刻（P1 的 ``next_available_at``）。"""
    for w in _windows(cfg, "p0_only"):
        if _in_window(now, w):
            minutes = _hm_minutes(w[1])
            hh, mm = divmod(minutes, 60)
            return datetime.combine(now.date(), _time(hh % 24, mm), tzinfo=now.tzinfo)
    return now


def _next_p2_open(now: datetime, cfg: Dict) -> Optional[datetime]:
    """从 ``now`` 向后找第一个 P2 放行时刻（最多 8 天；用于拒绝文案/状态）。"""
    candidates = []
    for offset in range(0, 9):
        day = (now + timedelta(days=offset)).date()
        if not _trading_day(day):
            candidates.append(datetime.combine(day, _time(0, 0), tzinfo=now.tzinfo))
            continue
        for hm in ("00:00", "08:00", "11:30", "15:15"):
            h, m = int(hm[:2]), int(hm[3:])
            candidates.append(datetime.combine(day, _time(h, m), tzinfo=now.tzinfo))
    for cand in sorted(candidates):
        if cand > now:
            return cand
    return None


# ---------------------------------------------------------------- 令牌桶


class _TokenBucket:
    """每分钟 ``per_minute`` 次的平滑令牌桶（容量 = 1 秒的量，允许小突发）。"""

    def __init__(self, per_minute: int) -> None:
        rate = max(0.0, float(per_minute)) / 60.0
        self._rate = rate
        self._capacity = max(1.0, rate)
        self._tokens = self._capacity
        self._updated = time.monotonic()

    def try_take(self) -> float:
        """取一个令牌：成功返回 0，失败返回距下次可取的秒数。"""
        if self._rate <= 0:
            return 0.0  # 0 表示不限额（配置非法时宁可不挡）
        now = time.monotonic()
        self._tokens = min(self._capacity, self._tokens + (now - self._updated) * self._rate)
        self._updated = now
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return 0.0
        return (1.0 - self._tokens) / self._rate


# ---------------------------------------------------------------- 线程级默认优先级

_local = threading.local()


def current_priority(default: int = P1) -> int:
    """当前线程的默认优先级（由 :func:`tdx_priority` 上下文管理器设置）。"""
    return getattr(_local, "priority", default)


@contextmanager
def tdx_priority(priority) -> Iterator[int]:
    """在 with 块内把本线程的 TQ 调用默认优先级设为 ``priority``。

    用于后台重任务入口（回测/滚动/扰动/增量重建/看板刷新/公司行为回填），
    块内所有经 ``get_tdx_lock()`` 的调用自动落到该级别；退出时恢复原值。
    """
    level = parse_priority(priority)
    previous = getattr(_local, "priority", None)
    _local.priority = level
    try:
        yield level
    finally:
        if previous is None:
            try:
                del _local.priority
            except AttributeError:
                pass
        else:
            _local.priority = previous


def parse_priority(value) -> int:
    """'P0'/'P1'/'P2'（大小写不敏感）或 0/1/2 → int；非法值按 P1。"""
    if isinstance(value, int) and value in (P0, P1, P2):
        return value
    text = str(value or "").strip().upper()
    return {"P0": P0, "P1": P1, "P2": P2}.get(text, P1)


# ---------------------------------------------------------------- 紧急放开

_override_lock = threading.Lock()
_override: Optional[Dict] = None


def set_override(minutes: int, reason: str, operator: str = "") -> Dict:
    """手工紧急放开时段限制（配额仍生效）。返回放开信息（供 API/事件落库）。"""
    cfg = gate_config()
    cap = int(cfg.get("override_max_minutes") or 120)
    mins = max(1, min(int(minutes or 30), cap))
    info = {
        "minutes": mins,
        "reason": str(reason or "").strip() or "未填写原因",
        "operator": str(operator or "").strip(),
        "started_at": now_shanghai().isoformat(timespec="seconds"),
        "expires_at": time.time() + mins * 60,
        "expires_at_local": (now_shanghai() + timedelta(minutes=mins)).isoformat(timespec="seconds"),
    }
    global _override, _policy_cache
    with _override_lock:
        _override = info
        _policy_cache = None
    logger.warning("TQ 通道闸被手工放开 %s 分钟（操作者=%s，原因=%s）",
                   mins, info["operator"] or "—", info["reason"])
    return dict(info)


def clear_override() -> bool:
    global _override, _policy_cache
    with _override_lock:
        existed = _override is not None
        _override = None
        _policy_cache = None
    if existed:
        logger.warning("TQ 通道闸手工放开已解除")
    return existed


def override_info() -> Optional[Dict]:
    with _override_lock:
        if _override is None:
            return None
        info = dict(_override)
    return {
        "minutes": info["minutes"],
        "reason": info["reason"],
        "operator": info["operator"],
        "started_at": info["started_at"],
        "expires_at": info["expires_at_local"],
        "active": float(info["expires_at"]) > time.time(),
    }


# ---------------------------------------------------------------- 闸本体


class TdxChannelGate:
    """优先级感知的 TQ 通道闸（可重入？否——与原 ``threading.Lock`` 一致）。

    准入规则（owner 为空时）：
    1. 时段策略拒绝 → 立即抛 :class:`TdxTaskBlocked`（不等待）；
    2. 有更高优先级在等 → 让行；
    3. 本级别配额已用尽 → 等到有令牌（受 timeout 约束）。
    """

    def __init__(self) -> None:
        self._cv = threading.Condition(threading.Lock())
        self._owner: Optional[int] = None
        self._waiters: Dict[int, int] = {P0: 0, P1: 0, P2: 0}
        self._buckets = {P1: _TokenBucket(600), P2: _TokenBucket(600)}
        self._bucket_cfg: Dict[int, int] = {P1: 600, P2: 600}
        self._stats = {
            "granted": {P0: 0, P1: 0, P2: 0},
            "blocked": {P0: 0, P1: 0, P2: 0},
            "timeouts": {P0: 0, P1: 0, P2: 0},
            "last_wait_ms": {P0: 0.0, P1: 0.0, P2: 0.0},
            "max_wait_ms": {P0: 0.0, P1: 0.0, P2: 0.0},
            "low_priority_granted": 0,
            "low_priority_yielded_to_p0": 0,
        }

    # -- 配置同步 ------------------------------------------------------
    def _sync_buckets(self, cfg: Dict) -> None:
        for level, key in ((P1, "p1_max_per_minute"), (P2, "p2_max_per_minute")):
            try:
                per_min = int(cfg.get(key) or 600)
            except (TypeError, ValueError):
                per_min = 600
            if per_min != self._bucket_cfg[level]:
                self._bucket_cfg[level] = per_min
                self._buckets[level] = _TokenBucket(per_min)

    # -- 准入 ----------------------------------------------------------
    def _check_policy(self, priority: int, cfg: Dict, now: datetime) -> Optional[Dict]:
        """返回策略快照；被拒绝时抛 TdxTaskBlocked。"""
        policy = evaluate_policy(now)
        if not cfg.get("enabled", True):
            return policy
        if priority == P0:
            return policy
        if priority == P1 and not policy["p1_allowed"]:
            nxt = _protected_end(now, cfg)
            raise TdxTaskBlocked(
                "实盘收盘保护窗口（%s）内暂停实盘辅助 TQ 任务；%s 后恢复"
                % ("–".join(cfg.get("p0_only") or ["14:55", "15:10"]),
                   nxt.strftime("%H:%M")),
                priority=priority, reason="protected_window",
                window=policy["window"], next_available_at=nxt)
        if priority == P2 and not policy["p2_allowed"]:
            nxt = policy["next_p2_open_at"]
            raise TdxTaskBlocked(
                "实盘运行中，回测类后台任务被 TQ 通道闸拒绝；%s 后自动可用"
                % (nxt.strftime("%m-%d %H:%M") if nxt else "收盘后"),
                priority=priority, reason="trading_window",
                window=policy["window"], next_available_at=nxt)
        return policy

    def _higher_waiting(self, priority: int) -> bool:
        return any(count > 0 for level, count in self._waiters.items() if level < priority)

    def acquire(
        self,
        priority: Optional[int] = None,
        blocking: bool = True,
        timeout: Optional[float] = None,
        default_timeout: float = 30.0,
    ) -> bool:
        cfg = gate_config()
        level = current_priority(P1) if priority is None else parse_priority(priority)
        self._sync_buckets(cfg)
        timeout = default_timeout if timeout is None else float(timeout)
        deadline = None if timeout <= 0 else time.monotonic() + timeout
        started = time.monotonic()

        with self._cv:
            me = threading.get_ident()
            if self._owner == me:
                raise RuntimeError(
                    "TQ 通道闸不可重入：同线程重复获取（原 _tdx_lock 亦如此）")
            self._waiters[level] += 1
            try:
                while True:
                    policy = self._check_policy(level, cfg, now_shanghai())
                    if self._owner is None:
                        if self._higher_waiting(level):
                            # 更高优先级在等：本级别让行（并把本次让出计入观测）
                            if level != P0:
                                self._stats["low_priority_yielded_to_p0"] += 1
                        else:
                            quota_wait = 0.0
                            if level in (P1, P2) and cfg.get("enabled", True):
                                quota_active = (
                                    policy["p1_quota_active"] if level == P1
                                    else policy["p2_quota_active"])
                                if quota_active:
                                    quota_wait = self._buckets[level].try_take()
                            if quota_wait <= 0.0:
                                self._owner = me
                                self._stats["granted"][level] += 1
                                waited_ms = (time.monotonic() - started) * 1000.0
                                self._stats["last_wait_ms"][level] = round(waited_ms, 1)
                                self._stats["max_wait_ms"][level] = max(
                                    self._stats["max_wait_ms"][level], round(waited_ms, 1))
                                if level != P0:
                                    self._stats["low_priority_granted"] += 1
                                return True
                    if not blocking:
                        return False
                    remaining = None if deadline is None else deadline - time.monotonic()
                    if remaining is not None and remaining <= 0:
                        self._stats["timeouts"][level] += 1
                        return False
                    self._cv.wait(min(remaining, 0.05) if remaining is not None else 0.05)
            except TdxTaskBlocked:
                self._stats["blocked"][level] += 1
                raise
            finally:
                self._waiters[level] -= 1

    def release(self) -> None:
        with self._cv:
            if self._owner is None:
                raise RuntimeError("TQ 通道闸 release 但无人持有（调用方未 acquire）")
            self._owner = None
            self._cv.notify_all()

    def locked(self) -> bool:
        with self._cv:
            return self._owner is not None

    def reset_state(self) -> None:
        """清空持有者/等待者/统计（仅测试用；生产不调用）。"""
        cfg = gate_config()
        with self._cv:
            self._owner = None
            for level in (P0, P1, P2):
                self._waiters[level] = 0
                for key in ("granted", "blocked", "timeouts"):
                    self._stats[key][level] = 0
                self._stats["last_wait_ms"][level] = 0.0
                self._stats["max_wait_ms"][level] = 0.0
            self._stats["low_priority_granted"] = 0
            self._stats["low_priority_yielded_to_p0"] = 0
        self._sync_buckets(cfg)

    def stats(self) -> Dict:
        with self._cv:
            return {
                "granted": {PRIORITY_NAMES[k]: v for k, v in self._stats["granted"].items()},
                "blocked": {PRIORITY_NAMES[k]: v for k, v in self._stats["blocked"].items()},
                "timeouts": {PRIORITY_NAMES[k]: v for k, v in self._stats["timeouts"].items()},
                "last_wait_ms": {PRIORITY_NAMES[k]: v for k, v in self._stats["last_wait_ms"].items()},
                "max_wait_ms": {PRIORITY_NAMES[k]: v for k, v in self._stats["max_wait_ms"].items()},
                "waiters": {PRIORITY_NAMES[k]: v for k, v in self._waiters.items()},
                "low_priority_granted": self._stats["low_priority_granted"],
                "low_priority_yielded_to_p0": self._stats["low_priority_yielded_to_p0"],
            }


GATE = TdxChannelGate()


def status() -> Dict:
    """闸状态（供 ``GET /api/system/tdx-gate`` 与前端置灰提示）。"""
    cfg = gate_config()
    policy = evaluate_policy()
    return {
        "enabled": bool(cfg.get("enabled", True)),
        "config": {
            "pre_market_p2": cfg.get("pre_market_p2"),
            "lunch_p2": cfg.get("lunch_p2"),
            "after_close_p2": cfg.get("after_close_p2"),
            "trading": cfg.get("trading"),
            "p0_only": cfg.get("p0_only"),
            "p1_max_per_minute": cfg.get("p1_max_per_minute"),
            "p2_max_per_minute": cfg.get("p2_max_per_minute"),
            "p2_chunk_size": cfg.get("p2_chunk_size"),
        },
        "policy": policy,
        "override": override_info(),
        "channel_held": GATE.locked(),
        "stats": GATE.stats(),
    }


# 别名：utils 统一出口用 gate_status，避免与通用 status 名字冲突
gate_status = status
