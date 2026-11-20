"""TQ 盘中 1m 主备探测（只探测 + 留痕，不切换数据源）。

背景（2026-11-20 实测）：TQ 的 ``get_market_data(period='1m')`` 实际读本地
``vipdoc\\*\\minline\\*.lc1``；``refresh_kline`` 只刷新客户端内存缓存，不下载、不补缺。
实盘 tdx/ths/alert/goldminer 的 1m 仍由快照合成（``SnapshotBarSynthesizer``）。

本模块只做一件事：周期性读取 TQ 1m 的最新 bar 时间戳，与上一轮基线比较：

- 首次观测 → INFO 基线（记录 TQ 1m 当前停在哪个时刻）；
- 时间戳前进 → INFO 记录「有新数据」+ 滞后秒数，供后续判断能否切主；
- 时间戳不变 → DEBUG 心跳（不刷屏）；
- 全部无数据 → INFO 一次（本地 ``.lc1`` 可能尚未落盘/非交易时段）；
- 取数异常 → WARNING 一次（随后同类错误静默，恢复后重新告警）。

行为边界：只读 TQ、不写文件/数据库、不参与 BarPoller/Dispatcher 装配，不影响信号、
风控与下单；异常自吞，绝不向上抛。日志统一带 ``[tq-1m-probe]`` 前缀便于 grep 与统计。
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta
from typing import Callable, Dict, Iterable, List, Optional, Tuple

logger = logging.getLogger(__name__)

#: 探测间隔下限：TQ 1m 是本地文件/缓存读取，5 分钟一轮足够判断“是否盘中前进”。
DEFAULT_INTERVAL_SECONDS = 300.0
#: 每轮最多探测的样本数（TQ 批量 240 根 ×30 只实测 0.4s，5 只约 0.1s）。
DEFAULT_MAX_CODES = 5
#: 取数回溯天数（count 模式实际只取末几根，这里仅约束 start_time）。
_LOOKBACK_DAYS = 10

FetchLatest = Callable[[List[str]], Dict[str, datetime]]


def fetch_latest_1m(codes: List[str], now: Optional[datetime] = None) -> Dict[str, datetime]:
    """取每只代码 TQ 1m 的最新 bar 时间（naive，Asia/Shanghai）。

    与回测/实盘同一条 TQ 取数路径（``TQData.get_history_raw``，内部持 TDX 锁）。
    无数据的代码不出现在返回里（退市/停牌/本地无文件均由调用方按“缺失”处理）。
    """
    from core.tq.data import TQData

    ref = now or datetime.now()
    start = (ref - timedelta(days=_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    raw = TQData().get_history_raw(
        list(codes), ["1m"], start=start, end="", dividend_type="none", count=2)
    close = (raw.get("1m") or {}).get("Close")
    out: Dict[str, datetime] = {}
    if close is None:
        return out
    columns = getattr(close, "columns", [])
    for code in codes:
        if code not in columns:
            continue
        try:
            series = close[code].dropna()
            if len(series) == 0:
                continue
            ts = series.index[-1]
            out[code] = ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts
        except Exception:  # noqa: BLE001 —— 单只异常不拖累整轮
            continue
    return out


class TqMinuteProbe:
    """TQ 1m 最新时间戳探测器（无副作用，可单测注入 fetch/clock/monotonic）。"""

    def __init__(
        self,
        session_id: int,
        codes_provider: Callable[[], Iterable[str]],
        fetch: Optional[FetchLatest] = None,
        interval_seconds: float = DEFAULT_INTERVAL_SECONDS,
        max_codes: int = DEFAULT_MAX_CODES,
        clock: Optional[Callable[[], datetime]] = None,
        monotonic: Optional[Callable[[], float]] = None,
    ):
        self._session_id = session_id
        self._codes_provider = codes_provider
        self._fetch = fetch or fetch_latest_1m
        self._interval = max(30.0, float(interval_seconds or DEFAULT_INTERVAL_SECONDS))
        self._max_codes = max(1, int(max_codes or DEFAULT_MAX_CODES))
        self._clock = clock or datetime.now
        self._monotonic = monotonic or time.monotonic
        self._last_run: Optional[float] = None
        self._last_stime: Dict[str, datetime] = {}
        self._baseline_logged = False
        self._no_data_logged = False
        self._error_logged = False

    # --- 调度 ---

    def due(self) -> bool:
        """距上一轮是否已到探测间隔（未跑过 → True）。"""
        if self._last_run is None:
            return True
        return (self._monotonic() - self._last_run) >= self._interval

    # --- 单轮 ---

    def tick(self, force: bool = False) -> Dict[str, Tuple[Optional[datetime], Optional[datetime]]]:
        """执行一轮探测；返回 {code: (旧时间, 新时间)}（仅含时间前进/回退的代码）。

        节流：未到间隔且 ``force=False`` 时直接返回空。取数异常只告警一次。
        """
        if not force and not self.due():
            return {}
        self._last_run = self._monotonic()

        codes = self._pick_codes()
        if not codes:
            return {}
        try:
            latest = self._fetch(codes) or {}
        except Exception as exc:  # noqa: BLE001
            if not self._error_logged:
                logger.warning(
                    "[tq-1m-probe] session=%s TQ 1m 探测失败（同类错误后续静默）: %s",
                    self._session_id, exc)
                self._error_logged = True
            return {}
        if self._error_logged:
            logger.info("[tq-1m-probe] session=%s TQ 1m 探测恢复", self._session_id)
            self._error_logged = False

        changed: Dict[str, Tuple[Optional[datetime], Optional[datetime]]] = {}
        for code in codes:
            cur = latest.get(code)
            if cur is None:
                continue
            prev = self._last_stime.get(code)
            if prev is None:
                self._last_stime[code] = cur
                changed[code] = (None, cur)
                continue
            if cur == prev:
                continue
            self._last_stime[code] = cur
            changed[code] = (prev, cur)
            if cur < prev:
                logger.warning(
                    "[tq-1m-probe] session=%s TQ 1m 时间回退 %s: %s → %s（本地文件被重写？）",
                    self._session_id, code, prev, cur)

        self._log_round(codes, latest, changed)
        return changed

    # --- 内部 ---

    def _pick_codes(self) -> List[str]:
        try:
            raw = list(self._codes_provider() or [])
        except Exception:  # noqa: BLE001
            return []
        return sorted(raw)[: self._max_codes]

    def _log_round(
        self,
        codes: List[str],
        latest: Dict[str, datetime],
        changed: Dict[str, Tuple[Optional[datetime], Optional[datetime]]],
    ) -> None:
        advances = {c: v for c, v in changed.items() if v[0] is not None and v[1] > v[0]}
        if advances:
            sample = sorted(advances.items())[0]
            code, (prev, cur) = sample
            lag = (self._clock() - cur).total_seconds()
            logger.info(
                "[tq-1m-probe] session=%s TQ 1m 有新数据: %d/%d 只；样本 %s %s → %s，"
                "滞后 %.0fs（仅探测 TQ 1m 进度，不参与 1m 选源）",
                self._session_id, len(advances), len(codes), code, prev, cur, lag)
            self._baseline_logged = True

        if not self._baseline_logged and latest:
            newest = max(latest.values())
            lag = (self._clock() - newest).total_seconds()
            logger.info(
                "[tq-1m-probe] session=%s 基线: TQ 1m 最新 %s（%d/%d 只有数据），"
                "滞后 %.0fs；本探测只记录 TQ 1m 最新时间戳，不参与 1m 选源",
                self._session_id, newest, len(latest), len(codes), lag)
            self._baseline_logged = True

        if not latest and not self._no_data_logged:
            logger.info(
                "[tq-1m-probe] session=%s TQ 1m 无可用数据（%d 只样本均无 bar，"
                "本地 vipdoc 可能尚未落盘）",
                self._session_id, len(codes))
            self._no_data_logged = True
        elif latest:
            self._no_data_logged = False
