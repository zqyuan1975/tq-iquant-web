"""实盘优先读取层：自有分钟存储优先，取不到/落后即原样委托内层。

**不改** ``NativeMinuteDispatcher`` 的任何既有语义——本类只做包装：

- ``query_quotes_batch`` 先查自有存储（只取 ``label <= now`` 的已闭合 bar）；
- 只有当该票最新已闭合标签不落后于「当前期望闭合桶」超过
  ``frontier_max_lag`` 根时才采用存储结果；
- 其余情况（空、落后、存储异常、非 1m 周期）整批/逐票**原样委托 inner**，
  inner 的日志、回落事件、健康状态都不受影响；
- ``minute_health`` 直接暴露 inner 的健康态，避免两套口径打架。

判定口径与 ``NativeMinuteDispatcher`` 一致：标签=分钟桶终点，只看标签
<= 当前墙钟的 bar；非交易时段不要求「新鲜」（取上一时段收盘数据即可）。
"""
from __future__ import annotations

import logging
import re
import threading
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional

from core.engine.bar_poller import parse_bar_time
from core.engine.live.native_minute import closed_bars
from core.engine.live.snapshot_bars import default_in_session
from core.engine.live.timing import now_shanghai

from .store import MinuteStore

logger = logging.getLogger(__name__)

PERIOD_1M = "1m"


class StoreFirstMinuteDispatcher:
    """``NativeMinuteDispatcher`` 的优先读包装（默认关闭，可无残留回退）。"""

    supports_batch_quote = True
    bars_explicitly_completed = True

    def __init__(
        self,
        inner,
        store: MinuteStore,
        clock: Callable[[], datetime] = now_shanghai,
        frontier_max_lag: int = 1,
        in_session: Callable[[datetime], bool] = default_in_session,
        pool_codes_provider: Optional[Callable[[], Optional[Iterable[str]]]] = None,
    ) -> None:
        self._inner = inner
        self._store = store
        self._clock = clock
        self.frontier_max_lag = max(int(frontier_max_lag), 0)
        self._in_session = in_session
        # 下载池标的提供者（返回 6 位代码集合）；用于识别「引擎在轮询、但下载池
        # 未覆盖」的票——这类票只能走 inner/快照，必须可观测而不是静默。
        self._pool_codes_provider = pool_codes_provider
        self._coverage_lock = threading.Lock()
        self._coverage: dict = {
            "requested": 0, "in_pool": 0, "outside_pool": 0,
            "outside_codes": [], "pool_known": False,
        }
        self._coverage_key: Optional[tuple] = None
        # 实际生效层级计数：store=自有库命中，inner=(原生/快照)委托。只做观测。
        self._routes = {"requests": 0, "store": 0, "inner": 0}
        # 供保护窗兜底守卫判断的只读状态（库读失败 / 落后 / 空）。
        self._state_lock = threading.Lock()
        self._state: dict = {
            "last_query_at": None,
            "last_lag_minutes": None,
            "last_read_error_at": None,
            "last_ok_at": None,
            "read_errors_total": 0,
            "codes": 0,
            "fresh": 0,
            "stale": 0,
            "last_error": "",
        }

    @property
    def minute_health(self):
        """健康口径完全沿用内层（不新造第二套判定）。"""
        return getattr(self._inner, "minute_health", None)

    def query_quotes_batch(
        self, codes, period: str = PERIOD_1M, count: int = 10,
    ) -> Dict[str, list]:
        want = [str(c) for c in (codes or [])]
        if str(period) == PERIOD_1M and want:
            self._note_coverage(want)
        if str(period) != PERIOD_1M or not want:
            return self._delegate(want, period, count)
        n = max(int(count or 10), 2)
        now = self._clock()
        out: Dict[str, list] = {}
        stale: List[str] = []
        lags: List[int] = []
        read_error = False
        for code in want:
            bars, failed = self._read_store(code, n, now)
            read_error = read_error or failed
            lag = self._lag_minutes(bars, now)
            if lag is not None:
                lags.append(lag)
            if bars and self._is_fresh(bars, now, lag=lag):
                out[code] = bars
            else:
                stale.append(code)
        self._note_query(now, len(want), len(want) - len(stale),
                         len(stale), lags, read_error)
        if stale:
            try:
                delegated = self._inner.query_quotes_batch(stale, period, count)
            except Exception:
                # 存储没命中又不该吞掉内层异常：按原样抛出，保持行为可观测。
                raise
            out.update(delegated or {})
        with self._coverage_lock:
            self._routes["requests"] += len(want)
            self._routes["store"] += len(want) - len(stale)
            self._routes["inner"] += len(stale)
        return {code: out.get(code, []) for code in want}

    def query_quote(self, code: str, period: str = PERIOD_1M,
                    count: int = 10) -> list:
        return self.query_quotes_batch([code], period, count).get(str(code), [])

    # ------------------------------------------------------------------
    # 内部
    # ------------------------------------------------------------------

    def _read_store(self, code: str, count: int,
                    now: datetime) -> tuple[List[dict], bool]:
        """读存储并只保留已闭合 bar；任何异常都按「未命中」处理。

        返回 ``(bars, failed)``：``failed`` 供保护窗兜底守卫识别「库读失败」。
        """
        try:
            raw = self._store.recent_bars(code, PERIOD_1M, count + 2)
        except Exception:
            logger.warning("分钟存储读取失败 code=%s（回落内层）", code,
                           exc_info=True)
            self._note_read_error()
            return [], True
        return closed_bars(raw or [], now)[-count:], False

    def _lag_minutes(self, bars: List[dict], now: datetime) -> Optional[int]:
        """最新已闭合 bar 相对当前分钟桶的落后分钟数；无 bar → None。"""
        if not bars:
            return None
        labels = [t for t in (parse_bar_time(b) for b in bars) if t is not None]
        if not labels:
            return None
        if not self._in_session(now):
            return 0
        expected = now.replace(second=0, microsecond=0)
        return max(0, int((expected - max(labels)).total_seconds() // 60))

    def _is_fresh(self, bars: List[dict], now: datetime,
                  lag: Optional[int] = None) -> bool:
        if not bars:
            return False
        if lag is None:
            lag = self._lag_minutes(bars, now)
        if lag is None:
            return False
        # 非交易时段（午休/收盘/非交易日）不要求新鲜：上一时段数据即最优。
        if not self._in_session(now):
            return True
        return lag <= self.frontier_max_lag

    def _delegate(self, codes: List[str], period: str,
                  count: int) -> Dict[str, list]:
        return self._inner.query_quotes_batch(codes, period, count)

    # ------------------------------------------------------------------
    # 只读状态（保护窗兜底守卫据此判断「库读失败 / 落后 / 空」）
    # ------------------------------------------------------------------

    def _note_read_error(self) -> None:
        now_text = self._clock().strftime("%Y-%m-%d %H:%M:%S")
        with self._state_lock:
            self._state["last_read_error_at"] = now_text
            self._state["read_errors_total"] += 1
            self._state["last_error"] = "read_failed"

    def _note_query(self, now: datetime, codes: int, fresh: int, stale: int,
                    lags: List[int], read_error: bool) -> None:
        now_text = now.strftime("%Y-%m-%d %H:%M:%S")
        with self._state_lock:
            state = self._state
            state["last_query_at"] = now
            state["codes"] = codes
            state["fresh"] = fresh
            state["stale"] = stale
            # 有数据取最大 lag（最差票）；整批无数据 → None（等同「库空」）。
            state["last_lag_minutes"] = max(lags) if lags else None
            if read_error:
                state["last_error"] = state["last_error"] or "read_failed"
            else:
                state["last_ok_at"] = now_text
                state["last_error"] = ""

    def store_state(self) -> dict:
        """只读快照：最近一次库查询的 lag / 失败 / 空覆盖情况。"""
        now = self._clock()
        with self._state_lock:
            state = dict(self._state)
        last_query = state.get("last_query_at")
        age: Optional[float] = None
        if isinstance(last_query, datetime):
            try:
                age = max(0.0, (now - last_query).total_seconds())
            except TypeError:  # naive/aware 混用时不猜测
                age = None
        return {
            "last_query_at": (
                last_query.strftime("%Y-%m-%d %H:%M:%S")
                if isinstance(last_query, datetime) else None),
            "last_query_age_seconds": (
                round(age, 3) if age is not None else None),
            "last_lag_minutes": state.get("last_lag_minutes"),
            "last_read_error_at": state.get("last_read_error_at"),
            "last_ok_at": state.get("last_ok_at"),
            "read_errors_total": state.get("read_errors_total", 0),
            "codes": state.get("codes", 0),
            "fresh": state.get("fresh", 0),
            "stale": state.get("stale", 0),
            "last_error": state.get("last_error", ""),
        }

    # ------------------------------------------------------------------
    # 覆盖缺口观测（下载池之外、只能走 inner/快照的票）
    # ------------------------------------------------------------------

    @staticmethod
    def _code6(code: str) -> str:
        return re.sub(r"\D", "", str(code or ""))[:6]

    def _note_coverage(self, want: List[str]) -> None:
        """记录本轮请求中「不在下载池」的票数；仅在集合变化时打一行日志。"""
        provider = self._pool_codes_provider
        if provider is None:
            return
        try:
            raw = provider()
        except Exception:  # noqa: BLE001 — 覆盖观测失败不影响行情主链路
            logger.debug("下载池标的读取失败（覆盖观测跳过）", exc_info=True)
            return
        if raw is None:
            return
        pool = {self._code6(c) for c in raw if self._code6(c)}
        outside = sorted({self._code6(c) for c in want
                          if self._code6(c) and self._code6(c) not in pool})
        key = tuple(outside)
        with self._coverage_lock:
            self._coverage = {
                "requested": len(want),
                "in_pool": len(want) - len(outside),
                "outside_pool": len(outside),
                "outside_codes": outside[:50],
                "pool_known": True,
            }
            changed = key != self._coverage_key
            self._coverage_key = key
        if changed and outside:
            logger.warning(
                "分钟数据覆盖缺口：%d 只引擎轮询标的不在下载池，将走原生/快照"
                "（示例 %s）", len(outside), outside[:10])

    def coverage_status(self) -> dict:
        """覆盖缺口只读快照（供会话详情/工作台展示）。"""
        with self._coverage_lock:
            return dict(self._coverage)

    def route_status(self) -> dict:
        """实际生效层级累计计数：store（自有库）/ inner（原生或快照）。"""
        with self._coverage_lock:
            return dict(self._routes)

    def status(self) -> dict:
        """优先读层运行态：健康沿用 inner + 覆盖缺口 + 生效层级计数。"""
        return {
            "store": str(self._store.path),
            "frontier_max_lag": self.frontier_max_lag,
            "health": (self.minute_health.status()
                       if hasattr(self.minute_health, "status") else None),
            "coverage": self.coverage_status(),
            "routes": self.route_status(),
            "freshness": self.store_state(),
        }


__all__ = ["StoreFirstMinuteDispatcher"]
