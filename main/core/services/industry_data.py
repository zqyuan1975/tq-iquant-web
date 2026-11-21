# -*- coding: utf-8 -*-
"""行业日线决策数据装配（回测 / 实盘共用，2026-11-22）。

职责：把 881 二级行业的**日线**历史算成逐日评级状态轴，并把个股映射到行业，
产出 ``IndustryTracker`` 供决策层（``Portfolio._gate_blocks`` / on_bar 排序）读取。

- 取数窗口：``start`` 前推 **300 个交易日**（评级内核 MA288 预热 + MIN_BARS=300），
  回测传运行区间起点，实盘传「今日 − 500 自然日」即可覆盖同一预热长度。
- 缓存：进程内按 (start, end, include_membership) 缓存，默认 300s；回测一次
  运行内所有组合共享同一份 tracker，避免逐组合重复取数。
- 缺失：整批取数失败 → tracker.error；单行业取数失败/不足 300 根 → 该行业不进
  状态轴、记入 ``unavailable``；个股无法映射 → 查询返回 None。以上一律由决策层
  fail-open 放行，并留 WARNING 与决策事件。
"""
from __future__ import annotations

import logging
import re
import threading
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

import polars as pl

from core.engine.industry_gate import IndustryState, IndustryTracker
from core.engine.rating_core import MIN_BARS, compute_rating_frame
from core.tq.data import TQData
from core.tq.utils import get_tdx_lock, get_tq

logger = logging.getLogger(__name__)

#: 预热：start 前推的交易日数（与 MIN_BARS=300 对齐）
INDUSTRY_WARMUP_TRADING_DAYS = 300
#: 实盘取数窗口：自然日（≈ 300 交易日 + 缓冲）
INDUSTRY_LIVE_SPAN_DAYS = 500

_INDUSTRY_CODE_RE = re.compile(r"^881\d{3}(\.SH)?$")
_FIELDS = ("Open", "High", "Low", "Close", "Volume")

_CACHE_TTL_SECONDS = 300
_CACHE_MAX_ENTRIES = 8
_cache: dict = {}
_build_lock = threading.Lock()


# ---------------- 行业名单与映射 ----------------

def _normalize_industry_code(raw) -> Optional[str]:
    code = str(raw or "").strip().upper()
    if not code:
        return None
    if not code.endswith(".SH"):
        code = f"{code}.SH"
    return code if _INDUSTRY_CODE_RE.match(code) else None


def list_industry_codes() -> List[dict]:
    """通达信二级行业名单 ``[{"code": "881002.SH", "name": "煤炭开采"}]``。"""
    raw = TQData().get_industry_list() or []
    seen: Dict[str, str] = {}
    for item in raw:
        code = _normalize_industry_code(item.get("code"))
        if code and code not in seen:
            seen[code] = str(item.get("name") or code)
    return [{"code": c, "name": n} for c, n in seen.items()]


def _sector_stocks(code: str) -> List[str]:
    """单行业成分股代码（兼容 block_type 3/0/1，与投资看板同口径）。"""
    for block_type in (3, 0, 1):
        try:
            with get_tdx_lock():
                raw = get_tq().get_stock_list_in_sector(
                    code, block_type=block_type, list_type=1) or []
        except Exception:  # noqa: BLE001
            logger.warning("行业成分股取数失败 %s block_type=%s",
                           code, block_type, exc_info=True)
            continue
        if raw:
            return [str(s.get("Code")) for s in raw if s.get("Code")]
    return []


def fetch_membership(industry_codes: List[str]) -> Dict[str, str]:
    """个股 → 行业反查表（同票多归属时以先出现者为准，实测 0 多归属）。"""
    out: Dict[str, str] = {}
    for code in industry_codes:
        for stock in _sector_stocks(code):
            out.setdefault(stock, code)
    return out


# ---------------- 交易日回退 ----------------

def _prev_trading_day(day: date) -> date:
    from core.engine.live.calendar import BUILTIN_CLOSURES

    d = day - timedelta(days=1)
    for _ in range(30):
        if d.weekday() < 5 and d not in BUILTIN_CLOSURES.get(
                d.year, frozenset()):
            return d
        d -= timedelta(days=1)
    return d


def shift_trading_days(day: date, steps: int) -> date:
    """按交易日平移（负=回退）；仅用于取数窗口预热。"""
    d = day
    step = -1 if steps < 0 else 1
    for _ in range(abs(int(steps))):
        d = _prev_trading_day(d) if step < 0 else _next_trading_day(d)
    return d


def _next_trading_day(day: date) -> date:
    from core.engine.live.calendar import BUILTIN_CLOSURES

    d = day + timedelta(days=1)
    for _ in range(30):
        if d.weekday() < 5 and d not in BUILTIN_CLOSURES.get(
                d.year, frozenset()):
            return d
        d += timedelta(days=1)
    return d


# ---------------- 行情取数 ----------------

def _is_nan(v) -> bool:
    try:
        return v != v
    except Exception:  # noqa: BLE001
        return True


def fetch_industry_ohlcv(codes: List[str], start: date, end: date) -> Dict[str, pl.DataFrame]:
    """批量取行业日线（前复权），返回 ``{code: polars.DataFrame}``。"""
    codes = [c for c in codes if c]
    if not codes:
        return {}
    raw = TQData().get_history_raw(
        stocks=codes, periods=["1d"],
        start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"),
        dividend_type="front", count=-1,
    )
    frame = (raw or {}).get("1d") or {}
    close_df = frame.get("Close")
    if close_df is None or getattr(close_df, "empty", True):
        return {}
    index = list(close_df.index)
    out: Dict[str, pl.DataFrame] = {}
    for code in codes:
        if code not in close_df.columns:
            continue
        cols: Dict[str, list] = {}
        ok = True
        for name in _FIELDS:
            col = frame.get(name)
            if col is None or code not in getattr(col, "columns", []):
                ok = False
                break
            cols[name] = [None if _is_nan(v) else float(v)
                          for v in col[code].to_list()]
        if not ok:
            continue
        df = pl.DataFrame({"datetime": index, **cols}).drop_nulls()
        if df.height:
            out[code] = df.sort("datetime")
    return out


def _as_date(value) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _extract_states(frame) -> Dict[date, IndustryState]:
    """评级明细 → {交易日: IndustryState}；根数 < MIN_BARS 的日子不输出状态。

    与看板「不足 300 根不输出方向」一致：预热期没有任何状态 → 查询返回 None
    → 决策层 fail-open 放行。
    """
    out: Dict[date, IndustryState] = {}
    for row in frame.itertuples(index=False):
        day = _as_date(getattr(row, "date", None))
        if day is None:
            continue
        try:
            bars = int(row.bars)
        except (TypeError, ValueError):
            continue
        if bars < MIN_BARS:
            continue
        direction = int(row.direction)
        z = getattr(row, "bias_z", None)
        bias_z = None if z is None or _is_nan(z) else float(z)
        strength = getattr(row, "cur_str", None)
        out[day] = IndustryState(
            direction=direction,
            strength=0.0 if strength is None or _is_nan(strength) else float(strength),
            bull_dur=max(0, int(getattr(row, "bull_dur", 0) or 0)),
            bias_z=bias_z,
            overbought=bool(getattr(row, "overbought", False)),
        )
    return out


# ---------------- 装配 ----------------

def _build(start: date, end: date, include_membership: bool) -> IndustryTracker:
    t0 = time.monotonic()
    fetch_start = shift_trading_days(start, -INDUSTRY_WARMUP_TRADING_DAYS)
    industries = list_industry_codes()
    if not industries:
        logger.warning("行业决策数据：行业名单为空，行业门控将 fail-open 放行")
        return IndustryTracker({}, {}, error="行业名单为空（通达信行业板块不可用）")
    codes = [item["code"] for item in industries]
    try:
        frames = fetch_industry_ohlcv(codes, fetch_start, end)
    except Exception as exc:  # noqa: BLE001
        logger.warning("行业决策数据：行业日线取数失败，行业门控将 fail-open 放行",
                       exc_info=True)
        return IndustryTracker({}, {}, error=f"行业日线取数失败：{exc}")

    states: Dict[str, Dict[date, IndustryState]] = {}
    unavailable: Dict[str, str] = {}
    for code in codes:
        df = frames.get(code)
        if df is None or df.height == 0:
            unavailable[code] = "未取到日线"
            continue
        frame = compute_rating_frame(df, min_bars=0)
        if frame.empty:
            unavailable[code] = "行情不足，无法计算"
            continue
        days = _extract_states(frame)
        if not days:
            unavailable[code] = f"有效根数 < {MIN_BARS}"
            continue
        states[code] = days

    membership: Dict[str, str] = {}
    if include_membership:
        membership = fetch_membership(codes)
        if not membership:
            logger.warning("行业决策数据：成分股映射为空，个股将无法定位行业"
                           "（门控 fail-open）")

    tracker = IndustryTracker(states, membership, unavailable=unavailable)
    span = tracker.span()
    logger.info(
        "行业决策数据就绪：行业 %d / 不可用 %d / 个股映射 %d / 区间 %s~%s / 耗时 %.1fs",
        tracker.industry_count(), len(unavailable), tracker.stock_count(),
        span[0] if span else "-", span[1] if span else "-",
        time.monotonic() - t0,
    )
    if unavailable:
        logger.info("行业决策数据：不可用行业 %d 个（fail-open）：%s",
                    len(unavailable),
                    ",".join(sorted(unavailable)[:10]))
    return tracker


def build_industry_tracker(
    start: date,
    end: date,
    include_membership: bool = True,
    refresh: bool = False,
) -> IndustryTracker:
    """构造（或命中缓存返回）行业状态轴。永不抛异常——失败返回 error tracker。"""
    key = (start.isoformat(), end.isoformat(), bool(include_membership))
    now = time.monotonic()
    hit = _cache.get(key)
    if hit is not None and not refresh and now - hit[0] <= _CACHE_TTL_SECONDS:
        return hit[1]
    with _build_lock:
        hit = _cache.get(key)
        if hit is not None and not refresh and time.monotonic() - hit[0] <= _CACHE_TTL_SECONDS:
            return hit[1]
        try:
            tracker = _build(start, end, include_membership)
        except Exception as exc:  # noqa: BLE001
            logger.warning("行业决策数据装配异常，行业门控将 fail-open 放行",
                           exc_info=True)
            tracker = IndustryTracker({}, {}, error=f"行业数据装配异常：{exc}")
        _cache[key] = (time.monotonic(), tracker)
        if len(_cache) > _CACHE_MAX_ENTRIES:
            oldest = min(_cache, key=lambda k: _cache[k][0])
            _cache.pop(oldest, None)
        return tracker


def clear_cache() -> None:
    with _build_lock:
        _cache.clear()
