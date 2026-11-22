"""原生分钟数据源（实盘盘中 1m/5m/…）— 可选开关 ``live.native_minute.enabled``。

背景（2026-11-20 实测）：盘中 1m/5m 本地原生文件（``.lc1``/``.lc5``）由外部
「数据下载工具」15–30 秒一轮写入，TQ ``get_market_data`` 直接读到的就是盘中
最新数据；此前实盘 1m 走快照合成，与回测（原生 K 线）口径不同。

默认关闭（沿用快照合成，行为与接入前一致）。开启后：

- 1m 读原生 ``.lc1``、5m 读原生 ``.lc5``，15m/30m/1h 走原生周期（TDX 由
  ``.lc1`` 聚合），与回测同源，不走快照合成；
- **只保留标签时间 <= 当前墙钟的已闭合 bar**（标签=区间终点）；最后一根
  正在形成的桶丢弃，不进入信号与风控；
- **判新只看 bar 标签前进**，不看 mtime（下载工具对无变化文件会跳过写盘）；
- 交易时段内超过 ``stall_timeout_seconds`` 未前进（工具未开/被关/清单未更新）
  → 自动回落快照合成，WARNING + risk 事件留痕（原因/时刻/滞后）；恢复前进时
  同样留痕并切回原生；
- 午休与非交易时段不判停滞，避免 11:30–13:00 误报。

时间语义：全模块 naive datetime，按 Asia/Shanghai 处理（与引擎其余部分一致）。
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, time
from typing import Callable, Dict, List, Optional

from ..bar_poller import parse_bar_time
from .snapshot_bars import default_in_session
from .timing import now_shanghai

logger = logging.getLogger("core.engine.live.native_minute")

#: 原生分钟数据源支持的周期（其余周期不拦，交回调用方/兜底）。
NATIVE_PERIODS = ("1m", "5m", "15m", "30m", "1h")

_AM_OPEN = time(9, 30)
_AM_CLOSE = time(11, 30)
_PM_OPEN = time(13, 0)

#: 停滞判定的最短阈值：外部工具 15–30s 一轮，阈值过小会在正常写入间隙误报。
_MIN_STALL_SECONDS = 60.0


def closed_bars(bars: List[dict], now: datetime) -> List[dict]:
    """只保留标签时间 ``<= now`` 的已闭合 bar（按 stime 升序、同标签去重）。

    标签语义 = 分钟/桶终点：标签大于墙钟说明该桶仍在形成，必须丢弃
    （不得进入信号与风控）。``now`` 为 naive 上海时间。
    """
    seen: Dict[datetime, dict] = {}
    for bar in bars or []:
        bt = parse_bar_time(bar)
        if bt is None or bt > now:
            continue
        seen[bt] = bar
    return [seen[t] for t in sorted(seen)]


def latest_closed_bar(bars: List[dict], now: datetime) -> Optional[dict]:
    """``bars`` 中标签 ``<= now`` 的最新一根；没有闭合 bar → None。"""
    closed = closed_bars(bars, now)
    return closed[-1] if closed else None


def fetch_native_bars(
    codes: List[str], period: str = "1m", count: int = 10,
) -> Dict[str, List[dict]]:
    """原生 K 线批量取数（TQ 本地文件，前复权，与回测同源）。

    直接复用 ``TdxTqDispatcher`` 的代码规整与 ``raw_to_bars`` 转换，避免
    与主通道的取数口径分叉；异常向上抛，由调用方决定是否触发回落。
    """
    from core.tq.data import TQData

    from ..tdx_tq_dispatcher import normalize_symbol, raw_to_bars

    pairs = []
    seen = set()
    for code in codes or []:
        code6 = re.sub(r"\D", "", str(code or ""))[:6]
        if len(code6) != 6:
            continue
        tq_code = normalize_symbol(code)
        if tq_code in seen:
            continue
        seen.add(tq_code)
        pairs.append((str(code), tq_code))
    if not pairs:
        return {}
    raw = TQData().get_history_raw(
        [tq for _, tq in pairs], [period], dividend_type="none",
        count=max(int(count or 10), 2), priority="P0")
    converted = raw_to_bars(raw, period)
    return {orig: converted.get(tq, []) for orig, tq in pairs}


class MinuteSourceHealth:
    """原生分钟数据「前进 / 停滞」状态机（回落判据的唯一来源）。

    - 前进判据：本轮观测到的最大已闭合标签 > ``last_label``；
    - 停滞：交易时段内距 ``last_advanced_at`` 超过 ``stall_timeout_seconds``；
    - 回落期间即使标签不变也保持回落，直到标签真正前进才恢复；
    - ``on_transition`` 回调用于 risk 事件留痕（回落/恢复都不能静默）。
    """

    def __init__(
        self,
        stall_timeout_seconds: float = 180.0,
        clock: Callable[[], datetime] = now_shanghai,
        in_session: Callable[[datetime], bool] = default_in_session,
    ) -> None:
        self.stall_timeout_seconds = max(
            float(stall_timeout_seconds or 0), _MIN_STALL_SECONDS)
        self._clock = clock
        self._in_session = in_session
        self.started_at = clock()
        self.last_label: Optional[datetime] = None
        self.last_advanced_at: Optional[datetime] = None
        self.in_fallback = False
        self.fallback_since: Optional[datetime] = None
        self.reason = ""
        self.on_transition: Optional[Callable[[dict], None]] = None

    # --- 观测 ---

    def observe(self, labels: List[Optional[datetime]], now: Optional[datetime] = None) -> None:
        """上报本轮已闭合 bar 的标签集合；标签前进则刷新前进时刻并尝试恢复。"""
        now = now or self._clock()
        known = [t for t in (labels or []) if t is not None]
        max_label = max(known) if known else None
        if max_label is not None and (
            self.last_label is None or max_label > self.last_label
        ):
            self.last_label = max_label
            # 以标签时刻作为「最后前进」起算点：会话中途启动时若工具已停，
            # 直接按真实数据时刻判定停滞，不会把启动时刻误当成前进时刻。
            self.last_advanced_at = max_label
            if self.in_fallback:
                self._transition(False, now, "native_recovered")
        self._evaluate(now)

    def _evaluate(self, now: datetime) -> None:
        """按墙钟判定停滞；非交易时段（午休/收盘）不判。"""
        if not self._in_session(now) or self.in_fallback:
            return
        ref = self._reference(now)
        if (now - ref).total_seconds() > self.stall_timeout_seconds:
            self._transition(True, now, "no_advance")

    def _session_start(self, now: datetime) -> datetime:
        """当前交易时段起点时刻（09:30 / 13:00）。"""
        start = _AM_OPEN if now.time() < _AM_CLOSE else _PM_OPEN
        return datetime.combine(now.date(), start)

    def _session_anchor(self, now: datetime) -> datetime:
        """本交易时段起点（09:30 / 13:00），但不早于会话启动时刻。"""
        return max(self._session_start(now), self.started_at)

    def _reference(self, now: datetime) -> datetime:
        """停滞/滞后参考点：最后前进时刻；跨时段时以下一时段起点为下限。

        上午最后一根标签（如 11:29；TDX 原生无 11:30 根）在午休后仍会被 13:00
        首轮取回；若直接拿它作参考，会把整段午休算成停滞并误报回落。此时改用
        下午开盘作参考（13:00 只是桶锚点、不是 bar 标签，下午首根为 13:01）。
        同一时段内的判定不变（仍按标签时刻起算，可识别中途停写）。
        """
        ref = self.last_advanced_at or self._session_anchor(now)
        if ref < self._session_start(now):
            return self._session_anchor(now)
        return ref

    def _lag_seconds(self, now: datetime) -> int:
        return max(int((now - self._reference(now)).total_seconds()), 0)

    # --- 状态 ---

    def is_fallback(self, now: Optional[datetime] = None) -> bool:
        """当前是否应回落快照合成（会先按墙钟刷新停滞判定）。"""
        self._evaluate(now or self._clock())
        return self.in_fallback

    def status(self, now: Optional[datetime] = None) -> dict:
        """工作台/会话详情展示用的只读状态（含最后前进时刻与滞后秒数）。"""
        now = now or self._clock()
        self._evaluate(now)
        return {
            "enabled": True,
            "mode": "fallback" if self.in_fallback else "native",
            "last_advance_at": (
                self.last_advanced_at.strftime("%Y-%m-%d %H:%M:%S")
                if self.last_advanced_at is not None else None),
            "last_label": (
                self.last_label.strftime("%Y-%m-%d %H:%M")
                if self.last_label is not None else None),
            "lag_seconds": self._lag_seconds(now),
            "stalled": self.in_fallback,
            "reason": self.reason,
            "stall_timeout_seconds": int(self.stall_timeout_seconds),
        }

    # --- 回落 / 恢复留痕 ---

    def _transition(self, to_fallback: bool, now: datetime, reason: str) -> None:
        self.in_fallback = to_fallback
        self.reason = reason
        self.fallback_since = now if to_fallback else None
        lag = self._lag_seconds(now)
        last_at = (
            self.last_advanced_at.strftime("%H:%M:%S")
            if self.last_advanced_at is not None else "从未前进")
        if to_fallback:
            message = (
                "原生分钟数据停止前进（最后前进 %s，已滞后 %d 秒），"
                "自动回落快照合成" % (last_at, lag))
            logger.warning("%s（reason=%s, at=%s）", message, reason, now)
        else:
            message = (
                "原生分钟数据恢复前进（最后标签 %s），切回原生取数"
                % (self.last_label.strftime("%H:%M") if self.last_label else "—"))
            logger.info("%s（at=%s）", message, now)
        payload = {
            "gate": "native_minute",
            "rule": "fallback" if to_fallback else "recovered",
            "message": message,
            "reason": reason,
            "last_advance_at": (
                self.last_advanced_at.strftime("%Y-%m-%d %H:%M:%S")
                if self.last_advanced_at is not None else None),
            "lag_seconds": lag,
            "at": now.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if self.on_transition is None:
            return
        try:
            self.on_transition(payload)
        except Exception:  # noqa: BLE001 — 留痕失败不影响行情主链路
            logger.warning("原生分钟状态事件投递失败", exc_info=True)


class NativeMinuteDispatcher:
    """BarPoller 行情适配器：读原生分钟文件，停滞时回落快照合成。

    与 ``SnapshotQuoteDispatcher`` 同构（``query_quotes_batch`` /
    ``query_quote``）。``bars_explicitly_completed=True``：返回窗口内只含
    ``label <= now`` 的闭合 bar，BarPoller 无需再等下一根确认。

    ``minute_health`` 供 ``MarketDataService`` 做周期派发决策（回落期间
    强制走快照聚合），也供会话详情接口暴露给工作台。
    """

    supports_batch_quote = True
    bars_explicitly_completed = True

    def __init__(
        self,
        health: MinuteSourceHealth,
        fallback=None,
        fetch_batch: Optional[Callable[..., Dict[str, List[dict]]]] = None,
        clock: Callable[[], datetime] = now_shanghai,
    ) -> None:
        self.minute_health = health
        self._fallback = fallback
        self._fetch_batch = fetch_batch or fetch_native_bars
        self._clock = clock

    def query_quotes_batch(
        self, codes, period: str = "1m", count: int = 10,
    ) -> Dict[str, list]:
        key = str(period)
        want = [str(c) for c in (codes or [])]
        n = max(int(count or 10), 2)
        if key not in NATIVE_PERIODS:
            return self._fallback_batch(want, key, n)
        now = self._clock()
        # 回落期间仍每轮尝试原生取数：只有真正读到更大的标签才能恢复；
        # 返回数据仍用快照合成（回落不静默、也不因一次空拉就永久放弃原生）。
        # 1m 多取 2 根：剔除未完成桶后仍能凑满 count
        fetch_n = n + 2 if key == "1m" else n
        try:
            raw = self._fetch_batch(want, key, fetch_n)
        except Exception:  # noqa: BLE001 — 单轮失败不切换，等停滞阈值判定
            logger.warning("原生分钟取数失败 period=%s（下一轮重试）", key,
                           exc_info=True)
            raw = {}
        out = {
            code: closed_bars(raw.get(code) or [], now)[-n:]
            for code in want
        }
        labels = [
            parse_bar_time(bar) for bars in out.values() for bar in bars
        ]
        self.minute_health.observe(labels, now)
        if self.minute_health.is_fallback(now):
            return self._fallback_batch(want, key, n)
        return out

    def query_quote(self, code: str, period: str = "1m", count: int = 10) -> list:
        return self.query_quotes_batch([code], period, count).get(str(code), [])

    def _fallback_batch(self, codes, period: str, count: int) -> Dict[str, list]:
        if self._fallback is None:
            return {}
        return self._fallback.query_quotes_batch(codes, period, count)
