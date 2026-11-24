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
        for code in want:
            bars = self._read_store(code, n, now)
            if bars and self._is_fresh(bars, now):
                out[code] = bars
            else:
                stale.append(code)
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

    def _read_store(self, code: str, count: int, now: datetime) -> List[dict]:
        """读存储并只保留已闭合 bar；任何异常都按「未命中」处理。"""
        try:
            raw = self._store.recent_bars(code, PERIOD_1M, count + 2)
        except Exception:
            logger.warning("分钟存储读取失败 code=%s（回落内层）", code,
                           exc_info=True)
            return []
        return closed_bars(raw or [], now)[-count:]

    def _is_fresh(self, bars: List[dict], now: datetime) -> bool:
        if not bars:
            return False
        labels = [t for t in (parse_bar_time(b) for b in bars) if t is not None]
        if not labels:
            return False
        # 非交易时段（午休/收盘/非交易日）不要求新鲜：上一时段数据即最优。
        if not self._in_session(now):
            return True
        expected = now.replace(second=0, microsecond=0)
        lag_minutes = int(
            (expected - max(labels)).total_seconds() // 60)
        return lag_minutes <= self.frontier_max_lag

    def _delegate(self, codes: List[str], period: str,
                  count: int) -> Dict[str, list]:
        return self._inner.query_quotes_batch(codes, period, count)

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
        }


__all__ = ["StoreFirstMinuteDispatcher"]
