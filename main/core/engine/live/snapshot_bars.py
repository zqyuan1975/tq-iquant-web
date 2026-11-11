"""快照合成 1m bar——实盘盘中数据源（TQ 分钟 K 线盘中不可得的补偿）。

实测（2026-11-03 盘中）+ 手册 refresh_kline 说明：TQ 盘中 ``get_market_data``
的 1m/5m 分钟线只到上一交易日（``refresh_cache(force=True)`` 亦无效），
而 ``get_market_snapshot_batch`` 快照实时、``get_market_data`` 1d 含当日实时
日线。→ 盘中 bar 流改由快照按分钟聚合合成，BarPoller 完成判定零改动复用
（``SnapshotQuoteDispatcher`` 与 ``query_quotes_batch`` 同构）。

节拍与线程：引擎 ``_tick_deals``（5s，独立 deals 线程池）每轮 ``sample()``
一次采样；60s 主循环 ``poll()`` 经适配器取已完成 bar 触发。内部状态由
``threading.Lock`` 保护（两池真并发）。

OHLCV 语义：
- stime = 分钟结束时刻（与 TDX 1m K 线一致：10:50 分钟 → 10:51:00）；
- open/close = 分钟内首/末采样 Now，high/low = 采样极值（5s 节拍下
  close 偏差 ≤ 采样间隔，极值可能漏掉采样间尖峰——1m 级信号/风控可接受）；
- volume = 快照当日总量差分（Volume 为日累计口径），回退异常 clamp 为 0；
- 无成交分钟仍产 bar（volume=0、价格持平）——保持 bar 连续性，风控不漏判。

时段门控：仅 A 股连续竞价时段（09:30-11:30 / 13:00-15:00，Asia/Shanghai，
周末硬规则）采样合成；午休/集合竞价不产假 bar。门控关闭时 ``sample()``
仍执行分钟滚动（跨午休/隔日补完成上一根 forming bar）。

时间语义：全模块 naive datetime，按 Asia/Shanghai 处理（与引擎其余部分一致）。
"""
from __future__ import annotations

import logging
import threading
from datetime import date, datetime, time, timedelta
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

_AM_OPEN = time(9, 30)
_AM_CLOSE = time(11, 30)
_PM_OPEN = time(13, 0)
_PM_CLOSE = time(15, 0)

# 每 code 保留的已完成 1m bar 上限。1h 聚合需要连续 60 根，取 130 留两个完整
# 1h 桶余量（count 窗口仍由调用方截断，1m 取数行为不变；每会话内存约
# code 数 × 130 个小 dict，量级可忽略）。
_MAX_COMPLETED_BARS = 130

#: 需要用快照合成 1m 聚合的周期 → 每个周期内含的 1m 根数。
#: 键与实盘周期口径一致（``1h``，不是回测的 ``60m``）。
AGGREGATED_PERIOD_MINUTES = {"5m": 5, "15m": 15, "30m": 30, "1h": 60}

FetchSnapshot = Callable[[List[str]], dict]
SessionGate = Callable[[datetime], bool]


def default_in_session(now: datetime) -> bool:
    """A 股连续竞价时段判定（周末硬规则；节假日 fail-open 同 TradingCalendar）。"""
    if now.weekday() >= 5:
        return False
    t = now.time()
    return (_AM_OPEN <= t < _AM_CLOSE) or (_PM_OPEN <= t < _PM_CLOSE)


def _minute_end(now: datetime) -> datetime:
    """采样时刻所属分钟的结束时刻（bar stime 语义）。"""
    return now.replace(second=0, microsecond=0) + timedelta(minutes=1)


def _f(v) -> Optional[float]:
    """快照字段（str/num）→ float；非法 → None。"""
    if v is None:
        return None
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def _to_bar_dict(b: dict) -> dict:
    return {
        "stime": b["label"].strftime("%Y%m%d%H%M%S"),
        "open": b["open"],
        "high": b["high"],
        "low": b["low"],
        "close": b["close"],
        "volume": int(b["volume"]),
    }


def _bucket_end(label: datetime, minutes: int) -> Optional[datetime]:
    """1m bar（label=分钟终点）所属 ``minutes`` 分钟桶的终点；跨时段/非法 → None。

    桶按**交易时段对齐**（与 TDX 原生分钟线一致）：上午从 09:30、下午从 13:00
    起算，5m→09:35…11:30 / 13:05…15:00；30m→10:00/10:30/11:00/11:30…；
    1h→10:30/11:30/14:00/15:00。桶终点超出收盘 → None（不产出跨界桶）。
    """
    start = label - timedelta(minutes=1)
    t = start.time()
    if _AM_OPEN <= t < _AM_CLOSE:
        session_open, session_close = _AM_OPEN, _AM_CLOSE
    elif _PM_OPEN <= t < _PM_CLOSE:
        session_open, session_close = _PM_OPEN, _PM_CLOSE
    else:
        return None
    anchor = start.replace(
        hour=session_open.hour, minute=session_open.minute, second=0, microsecond=0)
    offset = int((start - anchor).total_seconds() // 60)
    if offset < 0:
        return None
    end = anchor + timedelta(minutes=(offset // minutes + 1) * minutes)
    close_at = start.replace(
        hour=session_close.hour, minute=session_close.minute, second=0, microsecond=0)
    return end if end <= close_at else None


def _aggregate_completed(
    bars: List[dict], minutes: int, period: str, count: int,
) -> List[dict]:
    """已完成 1m bar → ``minutes`` 分钟聚合 bar（只输出**完整桶**）。

    完整性判据：桶内 1m bar 数量 == ``minutes``，且标签从「桶起点+1min」到桶终点
    逐分钟连续。缺任何一分钟（采样停摆/取数失败/开盘中途启动）整桶丢弃——绝不
    用残缺数据冒充周期 bar（与新鲜度闸 fail-safe 同口径）。

    OHLCV：open=首根 open、close=末根 close、high/low=极值、volume=累加。
    输出带 ``source=aggregated_1m`` / ``period`` 标记，便于事后区分「聚合」与
    「TQ 原生」。
    """
    buckets: Dict[datetime, List[dict]] = {}
    for b in bars:
        end = _bucket_end(b["label"], minutes)
        if end is None:
            continue
        buckets.setdefault(end, []).append(b)
    out: List[dict] = []
    for end in sorted(buckets):
        group = sorted(buckets[end], key=lambda x: x["label"])
        if len(group) != minutes:
            continue
        first = end - timedelta(minutes=minutes)
        expected = [first + timedelta(minutes=i + 1) for i in range(minutes)]
        if [g["label"] for g in group] != expected:
            continue
        out.append({
            "stime": end.strftime("%Y%m%d%H%M%S"),
            "open": group[0]["open"],
            "high": max(g["high"] for g in group),
            "low": min(g["low"] for g in group),
            "close": group[-1]["close"],
            "volume": int(sum(int(g.get("volume") or 0) for g in group)),
            "source": "aggregated_1m",
            "period": period,
        })
    return out[-int(count or 10):]


class SnapshotBarSynthesizer:
    """实时快照 → 1m bar 合成器。

    Usage:
        synth = SnapshotBarSynthesizer(codes, fetch_snapshot=TQData().get_market_snapshot_batch 包装)
        # deals 循环每 5s：synth.sample(now)
        # 主循环 BarPoller 经 SnapshotQuoteDispatcher 读 completed_bars
    """

    def __init__(
        self,
        codes: List[str],
        fetch_snapshot: FetchSnapshot,
        in_session: SessionGate = default_in_session,
    ):
        self._codes = list(codes)
        self._fetch = fetch_snapshot
        self._in_session = in_session
        self._lock = threading.Lock()
        self._reset_all_unlocked()

    # --- 公开接口 ---

    def sample(self, now: datetime) -> None:
        """采样一轮：先滚动完成到期 forming bar（无论门控），门控开放时
        拉快照并入 forming。快照拉取失败仅告警跳过，不破坏既有状态。"""
        with self._lock:
            self._rollover_unlocked(now)
            if not self._in_session(now):
                return
            try:
                snaps = self._fetch(self._codes) or {}
            except Exception as e:  # noqa: BLE001
                logger.warning("快照拉取失败，跳过本采样：%s", e)
                return
            # 跨日：重置差分基线与 forming（已完成 bar 保留——BarPoller
            # 基线跨日连续，次日不回放昨日 bar）
            if self._last_date is not None and now.date() != self._last_date:
                self._forming = {c: None for c in self._codes}
                self._last_volume = {c: None for c in self._codes}
                logger.info("快照合成跨交易日重置差分基线（%s → %s）",
                            self._last_date, now.date())
            self._last_date = now.date()
            for code in self._codes:
                # 单只股票的字段异常不得打断整轮采样（否则后续 code 本分钟全部丢失）
                try:
                    snap = snaps.get(code)
                    if not isinstance(snap, dict):
                        continue
                    price = _f(snap.get("Now"))
                    vol = _f(snap.get("Volume"))
                    if price is None or price <= 0:
                        continue
                    last_vol = self._last_volume.get(code)
                    # Volume 由数值变 None（快照字段缺失）：本分钟量 clamp 到 0，
                    # 并保留原差分基线（下次有量时把缺口累计回来），避免
                    # int(None - last) 抛 TypeError 中断整轮采样。
                    if vol is None or last_vol is None:
                        delta = 0
                    else:
                        delta = max(0, int(vol - last_vol))
                    if vol is not None:
                        self._last_volume[code] = vol
                    self._update_forming_unlocked(code, now, price, delta)
                except Exception as e:  # noqa: BLE001
                    logger.warning("快照合成跳过 %s：%s", code, e)

    def completed_bars(
        self, codes: Optional[List[str]] = None, count: int = 10,
    ) -> Dict[str, List[dict]]:
        """已完成（分钟已结束）的合成 bar，{code: [bar, ...]}，与
        ``query_quotes_batch`` 返回同构；bar 按时间升序，取末 ``count`` 根。"""
        want = list(codes) if codes else list(self._codes)
        with self._lock:
            out: Dict[str, List[dict]] = {}
            for code in want:
                bars = self._completed.get(code, [])[-int(count or 10):]
                out[code] = [_to_bar_dict(b) for b in bars]
            return out

    def aggregated_bars(
        self, codes: Optional[List[str]] = None, period: str = "5m",
        count: int = 10,
    ) -> Dict[str, List[dict]]:
        """已完成 1m bar → 5m/15m/30m/1h 聚合 bar（TQ 原生分钟线过期时的兜底）。

        数据全部来自本进程内存（``_completed``），不触 TQ；只输出完整桶
        （见 ``_aggregate_completed``）。返回与 ``query_quotes_batch`` 同构：
        ``{code: [bar, ...]}`` 升序，取末 ``count`` 根；不支持的周期返回空。
        聚合 bar 带 ``source=aggregated_1m``，供调用方在事件/日志中标注来源。
        """
        key = str(period or "")
        minutes = AGGREGATED_PERIOD_MINUTES.get(key)
        if not minutes:
            return {}
        want = list(codes) if codes else list(self._codes)
        n = max(int(count or 10), 1)
        with self._lock:
            return {
                code: _aggregate_completed(
                    self._completed.get(code) or [], minutes, key, n)
                for code in want
            }

    def latest_price(self, code: str) -> Optional[Tuple[float, datetime]]:
        """最近一次快照采样：(价格, 采样所属分钟终点)；无样本 → None。

        forming（本分钟最新样本）优先，回退最近一根已完成 bar 的 close。
        与 ``completed_bars`` 的区别：这里是**采样价**而非闭合 bar，供提交前
        价格刷新这类「只要盘口最新价」的场景使用——TQ 本地 1m 盘中只到上一
        交易日（见模块 docstring），不能用它冒充当日实时价。
        """
        c = str(code or "").strip()
        if not c:
            return None
        with self._lock:
            f = self._forming.get(c)
            if f is not None:
                return float(f["close"]), f["label"]
            bars = self._completed.get(c) or []
            if bars:
                return float(bars[-1]["close"]), bars[-1]["label"]
        return None

    def open_at(self, codes: Optional[List[str]], start: datetime) -> Dict[str, float]:
        """指定分钟区间（``start`` 起、1 分钟）的开盘价，``{code: open}``。

        bar 标签 = 分钟终点（``start + 1min``）。同时查 forming 与 completed：
        该分钟刚开始时首个采样即建 forming（open 一经建立不再变化），
        ``start+1min`` 时刻起由 ``_rollover_unlocked`` 移入 completed。
        取不到的 code 不返回（调用方 fail-closed，绝不伪造价格）。
        """
        label = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
        want = list(codes) if codes else list(self._codes)
        out: Dict[str, float] = {}
        with self._lock:
            for code in want:
                f = self._forming.get(code)
                if f is not None and f["label"] == label:
                    out[code] = f["open"]
                    continue
                for b in reversed(self._completed.get(code, [])):
                    if b["label"] == label:
                        out[code] = b["open"]
                        break
                    if b["label"] < label:
                        break
        return out

    def add_codes(self, codes: List[str]) -> None:
        """动态扩池（账户对齐：外部持仓导入后纳入快照合成/风控驱动）。

        新代码从零起算（无历史回放）：首个采样建基线，其后正常聚合。
        已存在的代码忽略。"""
        with self._lock:
            for code in codes:
                c = str(code or "").strip()
                if not c or c in self._codes:
                    continue
                self._codes.append(c)
                self._completed[c] = []
                self._forming[c] = None
                self._last_volume[c] = None

    def reset(self) -> None:
        """清空全部状态（E8 离线恢复 / 会话重开时与 poller.reset_baseline 对齐）。"""
        with self._lock:
            self._reset_all_unlocked()

    # --- 内部 ---

    def _reset_all_unlocked(self) -> None:
        self._completed: Dict[str, List[dict]] = {c: [] for c in self._codes}
        self._forming: Dict[str, Optional[dict]] = {c: None for c in self._codes}
        self._last_volume: Dict[str, Optional[float]] = {c: None for c in self._codes}
        self._last_date: Optional[date] = None
        # 最近一根「已闭合」1m bar 的分钟终点（跨 code 取最大）。watchdog 用它
        # 作为行情新鲜度基准：采样/快照拉取一旦停摆，该值不再推进。
        self._last_completed_at: Optional[datetime] = None

    @property
    def last_completed_at(self) -> Optional[datetime]:
        """最近一根已闭合 1m bar 的分钟终点（Asia/Shanghai naive）。"""
        with self._lock:
            return self._last_completed_at

    def _rollover_unlocked(self, now: datetime) -> None:
        """forming bar 的分钟已结束 → 移入 completed（门控无关，跨午休/隔日补完成）。"""
        for code in self._codes:
            f = self._forming.get(code)
            if f is not None and f["label"] <= now:
                lst = self._completed.setdefault(code, [])
                lst.append(f)
                if len(lst) > _MAX_COMPLETED_BARS:
                    del lst[:len(lst) - _MAX_COMPLETED_BARS]
                self._last_completed_at = f["label"]
                self._forming[code] = None

    def _update_forming_unlocked(
        self, code: str, now: datetime, price: float, delta: int,
    ) -> None:
        label = _minute_end(now)
        f = self._forming.get(code)
        if f is None or f["label"] != label:
            self._forming[code] = {
                "label": label, "open": price, "high": price,
                "low": price, "close": price, "volume": delta,
            }
            return
        f["high"] = max(f["high"], price)
        f["low"] = min(f["low"], price)
        f["close"] = price
        f["volume"] += delta


class SnapshotQuoteDispatcher:
    """BarPoller 行情适配器：把合成器已完成 bar 以 ``query_quotes_batch``
    同构格式暴露（``supports_batch_quote=True``）。1m 返回原生合成 bar；
    5m/15m/30m/1h 返回由 1m 聚合的 bar（``source=aggregated_1m``）；其余
    周期返回空 → BarPoller 视为无数据，与桥离线路径一致。"""

    supports_batch_quote = True
    #: 本适配器返回的每一根都已是闭合 bar：``completed_bars`` 只吐
    #: ``_rollover_unlocked`` 移入 ``_completed`` 的 bar，而该迁移的判定条件是
    #: 采样时刻 ``now >= f["label"]``（label = 分钟区间终点）。forming bar 永不
    #: 出现在返回窗口 → BarPoller 无需再「等下一根」把最新一根当进行中。
    bars_explicitly_completed = True

    def __init__(self, synthesizer: SnapshotBarSynthesizer):
        self._synth = synthesizer

    @property
    def synthesizer(self) -> SnapshotBarSynthesizer:
        """底层合成器（MarketDataService 自动发现聚合兜底能力用）。"""
        return self._synth

    def query_quotes_batch(
        self, codes, period: str = "1m", count: int = 10,
    ) -> Dict[str, list]:
        key = str(period)
        want = [str(c) for c in (codes or [])]
        n = max(int(count or 10), 2)
        if key == "1m":
            return self._synth.completed_bars(want, count=n)
        if key in AGGREGATED_PERIOD_MINUTES:
            return self._synth.aggregated_bars(want, period=key, count=n)
        return {}

    def query_quote(self, code: str, period: str = "1m", count: int = 10) -> list:
        return self.query_quotes_batch([code], period, count).get(str(code), [])
