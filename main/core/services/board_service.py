# -*- coding: utf-8 -*-
"""投资看板服务（只读）。

职责：构建 128 个 881 二级行业 与 其成分股 的评级快照，落 ``board_*`` 表；
不写交易/门控/策略任何表，不 import 交易与实盘引擎。

公式唯一实现来源：``core.engine.rating_core.compute_rating_frame``
（tqgate_py_v2：方向取 ENH、空头强度含偏离分_空、不足 300 根不输出方向）。
行情走本项目 TQ 通道（``build_gate_ohlcv`` 同源的 ``get_history_raw``），
不读行业评级系统的 parquet / min30_timeline。
"""
from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, time as dtime, timedelta
from typing import Dict, Iterable, List, Optional, Tuple

import polars as pl
from sqlalchemy import select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from core.engine.market_rules import trading_days_between
from core.engine.rating_core import (
    BIAS_Z_K,
    BIAS_Z_WATCH,
    BIAS_Z_WINDOW,
    MIN_BARS,
    RATING_FORMULA_VERSION,
    compute_rating_frame,
)
from core.models.investment_board import (
    BoardIndustry,
    BoardIndustrySnapshot,
    BoardRun,
    BoardStockSnapshot,
)
from core.services.market_gate_service import (
    DEFAULT_GATE_INDEX,
    expected_latest_trading_day,
)
from core.tq.data import TQData
from core.tq.utils import get_tdx_lock, get_tq

logger = logging.getLogger(__name__)

BOARD_PERIODS = ("1d", "30m")
SCOPE_INDUSTRIES = "industries"
SCOPE_STOCKS = "stocks"

#: 通达信二级行业代码：严格 881 + 3 位数字（881999 之类混入项一律剔除）
_INDUSTRY_CODE_RE = re.compile(r"^881\d{3}(\.SH)?$")
EXPECTED_INDUSTRY_COUNT = 128

FETCH_BARS = 320
_FETCH_SPAN_DAYS = {"1d": 670, "30m": 170}
_FETCH_WORKERS = 8
#: 30m 个股只算「日线由空转多」的行业，避免全市场 30m 取数
_STOCK_30M_MAX_INDUSTRIES = 20

_DIRECTION_LABELS = {-1: "空头", 0: "未定", 1: "多头", 2: "多头"}
_LEVEL_LABELS = {
    "strong_bull": "强多", "medium_bull": "中多", "weak_bull": "弱多",
    "strong_bear": "强空", "medium_bear": "中空", "weak_bear": "弱空",
    "none": "无信号",
}

_INTRADAY_SEQ = [dtime(10, 0), dtime(10, 30), dtime(11, 0), dtime(11, 30),
                 dtime(13, 30), dtime(14, 0), dtime(14, 30), dtime(15, 0)]


# ---------------- 名单 ----------------

def _normalize_industry_code(raw: str) -> Optional[str]:
    code = str(raw or "").strip().upper()
    if not code:
        return None
    if not code.endswith(".SH"):
        code = f"{code}.SH"
    return code if _INDUSTRY_CODE_RE.match(code) else None


def refresh_universe(db: Session) -> dict:
    """拉取通达信二级行业名单并落库（严格 128 校验，只增/失活，不删行）。"""
    raw = TQData().get_industry_list() or []
    seen: Dict[str, str] = {}
    dropped: List[str] = []
    for item in raw:
        code = _normalize_industry_code(item.get("code"))
        if not code:
            dropped.append(str(item.get("code")))
            continue
        if code not in seen:
            seen[code] = str(item.get("name") or code)
    today = date.today()
    existing = {r.code: r for r in db.execute(select(BoardIndustry)).scalars()}
    added = 0
    for code, name in seen.items():
        row = existing.get(code)
        if row is None:
            db.add(BoardIndustry(code=code, name=name, active=1,
                                 first_seen=today, last_seen=today,
                                 source="tdx_industry"))
            added += 1
        else:
            row.name = name
            row.active = 1
            row.last_seen = today
            if row.first_seen is None:
                row.first_seen = today
    deactivated = 0
    for code, row in existing.items():
        if code not in seen and row.active:
            row.active = 0
            deactivated += 1
    db.commit()
    info = {
        "count": len(seen),
        "expected": EXPECTED_INDUSTRY_COUNT,
        "ok": len(seen) == EXPECTED_INDUSTRY_COUNT,
        "added": added,
        "deactivated": deactivated,
        "dropped": dropped[:10],
        "refreshed_at": datetime.now().isoformat(timespec="seconds"),
    }
    if not info["ok"]:
        logger.warning("投资看板：行业名单数量 %s ≠ %s（已按实际名单继续）",
                       len(seen), EXPECTED_INDUSTRY_COUNT)
    return info


def list_universe(db: Session) -> List[BoardIndustry]:
    """有效行业名单；表为空时先同步一次。"""
    rows = list(db.execute(
        select(BoardIndustry).where(BoardIndustry.active == 1)
        .order_by(BoardIndustry.code)).scalars())
    if not rows:
        refresh_universe(db)
        rows = list(db.execute(
            select(BoardIndustry).where(BoardIndustry.active == 1)
            .order_by(BoardIndustry.code)).scalars())
    return rows


def universe_status(db: Session) -> dict:
    rows = list(db.execute(
        select(BoardIndustry).where(BoardIndustry.active == 1)).scalars())
    codes = [r.code for r in rows]
    return {
        "count": len(codes),
        "expected": EXPECTED_INDUSTRY_COUNT,
        "ok": len(codes) == EXPECTED_INDUSTRY_COUNT,
        "codes": codes,
    }


# ---------------- 行情取数与闭合 bar ----------------

def _fetch_many(codes: Iterable[str], period: str,
                end: date) -> Dict[str, pl.DataFrame]:
    """批量取数（单周期多标的），按标的返回 polars OHLCV，规则同 build_gate_ohlcv。"""
    codes = [c for c in codes if c]
    if not codes:
        return {}
    span = _FETCH_SPAN_DAYS.get(period, 670)
    start = end - timedelta(days=span)
    try:
        raw = TQData().get_history_raw(
            stocks=codes, periods=[period],
            start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"),
            dividend_type="front", count=-1,
        )
    except Exception:
        logger.exception("投资看板：批量取数失败 period=%s codes=%s",
                         period, len(codes))
        return {}
    frame = (raw or {}).get(period) or {}
    names = ("Open", "High", "Low", "Close", "Volume")
    for name in names:
        if frame.get(name) is None or getattr(frame[name], "empty", True):
            return {}
    close_df = frame["Close"]
    index = list(close_df.index)
    out: Dict[str, pl.DataFrame] = {}
    for code in codes:
        if code not in close_df.columns:
            continue
        cols: Dict[str, list] = {}
        ok = True
        for name in names:
            col = frame[name]
            if code not in col.columns:
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


def _is_nan(v) -> bool:
    try:
        return v != v
    except Exception:  # noqa: BLE001
        return True


def _truncate_as_of(df: pl.DataFrame, as_of: date) -> pl.DataFrame:
    limit = datetime.combine(as_of + timedelta(days=1), dtime(0, 0))
    return df.filter(pl.col("datetime") < pl.lit(limit))


def _closed_bars(df: pl.DataFrame, period: str,
                 now: datetime) -> Tuple[pl.DataFrame, Optional[str], Optional[str]]:
    """只保留已闭合 bar，并给出 ``bar_closed_at`` / ``effective_from``。

    日线：15:00 后当日 bar 才视为闭合，否则剔除当日。
    30m：通达信 30m 时间戳为 bar 结束时刻，闭合后 +2 分钟才可用；
    ``effective_from`` = 下一根 bar（收盘后下一根生效）。
    """
    if df.height == 0:
        return df, None, None
    if period == "1d":
        closed_today = (now.hour, now.minute) >= (15, 10)
        cutoff = now.date() if closed_today else now.date() - timedelta(days=1)
        kept = df.filter(
            pl.col("datetime").cast(pl.Date) <= pl.lit(cutoff))
        if kept.height == 0:
            return kept, None, None
        last_day = kept["datetime"].max().date()
        closed_at = f"{last_day.isoformat()} 15:00"
        effective = _next_trading_day(last_day).isoformat()
        return kept, closed_at, effective

    # 30m：bar 时间戳为结束时刻，需 >= +2 分钟
    def _usable(ts) -> bool:
        return ts + timedelta(minutes=2) <= now

    rows = [ts for ts in df["datetime"].to_list() if _usable(ts)]
    if not rows:
        return df.head(0), None, None
    kept = df.filter(pl.col("datetime").is_in(rows))
    last = kept["datetime"].max()
    return kept, last.isoformat(sep=" "), _next_bar_label(last).isoformat(sep=" ")


def _next_trading_day(day: date) -> date:
    from core.engine.live.calendar import BUILTIN_CLOSURES

    d = day + timedelta(days=1)
    for _ in range(30):
        if d.weekday() < 5 and d not in BUILTIN_CLOSURES.get(
                d.year, frozenset()):
            return d
        d += timedelta(days=1)
    return d


def _next_bar_label(ts: datetime) -> datetime:
    t = ts.time()
    if t in _INTRADAY_SEQ:
        i = _INTRADAY_SEQ.index(t)
        if i + 1 < len(_INTRADAY_SEQ):
            return datetime.combine(ts.date(), _INTRADAY_SEQ[i + 1])
    return datetime.combine(_next_trading_day(ts.date()), dtime(10, 0))


def _prev_trading_day(day: date) -> date:
    from core.engine.live.calendar import BUILTIN_CLOSURES

    d = day - timedelta(days=1)
    for _ in range(30):
        if d.weekday() < 5 and d not in BUILTIN_CLOSURES.get(
                d.year, frozenset()):
            return d
        d -= timedelta(days=1)
    return d


def _turn_bar_time(as_of: date, period: str, bull_dur: Optional[int],
                   bar_closed_at: Optional[str]) -> Optional[str]:
    """本轮「由空转多」那根 bar 的时间。

    由 as_of / 最新 bar 往回推 bull_dur-1 根：日线按交易日回退，
    30m 按 8 根/交易日 的 bar 序列回退，返回该 bar 的收盘时间。
    """
    if as_of is None or not bull_dur or int(bull_dur) <= 0:
        return None
    steps = int(bull_dur) - 1
    if period != "30m":
        d = as_of
        for _ in range(steps):
            d = _prev_trading_day(d)
        return d.isoformat()
    if not bar_closed_at:
        return None
    try:
        ts = datetime.fromisoformat(str(bar_closed_at).replace(" ", "T"))
    except ValueError:
        return None
    idx = len(_INTRADAY_SEQ) - 1
    if ts.time() in _INTRADAY_SEQ:
        idx = _INTRADAY_SEQ.index(ts.time())
    d, rem = ts.date(), steps
    while rem > 0:
        if idx >= rem:
            idx -= rem
            rem = 0
        else:
            rem -= idx + 1
            d = _prev_trading_day(d)
            idx = len(_INTRADAY_SEQ) - 1
    return datetime.combine(d, _INTRADAY_SEQ[idx]).isoformat(sep=" ")[:16]


# ---------------- 快照计算 ----------------

def _stale_flag(period: str, as_of: date, now: datetime) -> int:
    latest = expected_latest_trading_day(now)
    if as_of >= latest:
        return 0
    return 1 if trading_days_between(as_of, latest) > 0 else 0


def _frame_for(df: pl.DataFrame, period: str, as_of: date,
               now: datetime):
    """单标的：截断 → 闭合过滤 → 评级 frame（不足 300 根返回 None）。"""
    if df is None or df.height == 0:
        return None
    df = _truncate_as_of(df, as_of)
    df, closed_at, effective = _closed_bars(df, period, now)
    frame = compute_rating_frame(df, min_bars=MIN_BARS)
    if frame.empty:
        return None
    return frame, closed_at, effective


def _snapshot_values(frame, period: str, as_of: date, now: datetime,
                     run_id: Optional[int], name: str) -> dict:
    last = frame.iloc[-1]
    return {
        "as_of": as_of,
        "period": period,
        "industry_name": name,
        "direction": int(last["direction"]),
        "strength": _f(last["cur_str"]),
        "level": str(last["level"]),
        "bull_dur": int(last["bull_dur"]),
        "bear_dur": int(last["bear_dur"]),
        "bias": _f(last["bias"]),
        "bias_z": _f(last["bias_z"]),
        "overbought": 1 if bool(last["overbought"]) else 0,
        "oversold": 1 if bool(last["oversold"]) else 0,
        "jd": _f(last["jd"]),
        "jd_filter": int(last["jd_filter"]),
        "channel_filter": int(last["channel_filter"]),
        "up_str": _f(last["up_str"]),
        "dn_str": _f(last["dn_str"]),
        "bars": int(last["bars"]),
        "stale": _stale_flag(period, as_of, now),
        "computed_at": datetime.now(),
        "run_id": run_id,
    }


def _f(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def _upsert_industry_snapshots(db: Session, rows: List[dict]) -> None:
    if not rows:
        return
    stmt = sqlite_insert(BoardIndustrySnapshot).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["as_of", "period", "industry_code"],
        set_={c: stmt.excluded[c] for c in (
            "industry_name", "direction", "strength", "level", "bull_dur",
            "bear_dur", "bias", "bias_z", "overbought", "oversold", "jd",
            "jd_filter", "channel_filter", "up_str", "dn_str", "bars",
            "stale", "bar_closed_at", "effective_from", "computed_at",
            "run_id",
        )},
    )
    db.execute(stmt)


def _upsert_stock_snapshots(db: Session, rows: List[dict]) -> None:
    if not rows:
        return
    stmt = sqlite_insert(BoardStockSnapshot).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=["as_of", "period", "industry_code", "stock_code"],
        set_={c: stmt.excluded[c] for c in (
            "stock_name", "direction", "strength", "level", "bull_dur",
            "bear_dur", "lead_days", "bias", "bias_z", "overbought",
            "oversold", "bars", "stale", "bar_closed_at", "effective_from",
            "computed_at", "run_id",
        )},
    )
    db.execute(stmt)


def _benchmark_frame(as_of: date, now: datetime,
                     code: str = DEFAULT_GATE_INDEX):
    frames = _fetch_many([code], "1d", as_of)
    got = _frame_for(frames.get(code), "1d", as_of, now)
    if not got:
        return None
    frame, _, _ = got
    return frame.iloc[-1]


def compute_industries(db: Session, period: str, as_of: date,
                       trigger: str = "manual", run_id: Optional[int] = None,
                       codes: Optional[List[str]] = None) -> dict:
    """计算并写入行业快照；返回统计（含基准 bull_dur）。"""
    now = datetime.now()
    universe = {r.code: r.name for r in list_universe(db)}
    target = [c for c in (codes or list(universe)) if c in universe]
    bench = _benchmark_frame(as_of, now)
    bench_bull = int(bench["bull_dur"]) if bench is not None else None
    t0 = time.monotonic()
    frames = _fetch_many(target, period, as_of)
    rows: List[dict] = []
    skipped: List[str] = []
    for code in target:
        got = _frame_for(frames.get(code), period, as_of, now)
        if not got:
            skipped.append(code)
            continue
        frame, closed_at, effective = got
        vals = _snapshot_values(frame, period, as_of, now, run_id,
                                universe[code])
        vals.update({
            "industry_code": code,
            "bar_closed_at": closed_at,
            "effective_from": effective,
        })
        rows.append(vals)
    _upsert_industry_snapshots(db, rows)
    db.commit()
    return {
        "total": len(target),
        "ok": len(rows),
        "skipped": len(skipped),
        "skipped_codes": skipped[:20],
        "benchmark_bull_dur": bench_bull,
        "benchmark": None if bench is None else {
            "as_of": as_of.isoformat(),
            "direction": int(bench["direction"]),
            "strength": _f(bench["cur_str"]),
            "bull_dur": int(bench["bull_dur"]),
        },
        "duration_sec": round(time.monotonic() - t0, 1),
    }


def _sector_stocks(code: str) -> List[dict]:
    """行业成分股（通达信行业板块）。兼容 block_type 3/0/1。"""
    for block_type in (3, 0, 1):
        try:
            with get_tdx_lock():
                raw = get_tq().get_stock_list_in_sector(
                    code, block_type=block_type, list_type=1) or []
        except Exception:
            logger.warning("投资看板：成分股取数失败 %s block_type=%s",
                           code, block_type, exc_info=True)
            continue
        if raw:
            return [{"stock_code": str(s.get("Code") or ""),
                     "stock_name": str(s.get("Name") or "")}
                    for s in raw if s.get("Code")]
    return []


def _industry_bull_map(db: Session, period: str, as_of: date) -> Dict[str, int]:
    rows = db.execute(
        select(BoardIndustrySnapshot.industry_code, BoardIndustrySnapshot.bull_dur)
        .where(BoardIndustrySnapshot.as_of == as_of,
               BoardIndustrySnapshot.period == period)).all()
    return {code: int(bull or 0) for code, bull in rows}


def compute_stocks(db: Session, period: str, as_of: date,
                   trigger: str = "manual", run_id: Optional[int] = None,
                   industry_codes: Optional[List[str]] = None) -> dict:
    """计算并写入行业成分股快照（``lead_days`` = 个股 bull_dur − 行业 bull_dur）。"""
    now = datetime.now()
    ind_bull = _industry_bull_map(db, period, as_of)
    if industry_codes is None:
        base = select(BoardIndustrySnapshot.industry_code).where(
            BoardIndustrySnapshot.as_of == as_of,
            BoardIndustrySnapshot.period == period,
            BoardIndustrySnapshot.direction.in_((1, 2)))
        if period == "30m":
            base = base.where(BoardIndustrySnapshot.bull_dur == 1)
        industry_codes = [c for (c,) in db.execute(base).all()]
        if period == "30m":
            industry_codes = industry_codes[:_STOCK_30M_MAX_INDUSTRIES]
    target = list(dict.fromkeys(industry_codes or []))
    t0 = time.monotonic()
    rows: List[dict] = []
    skipped = 0
    total = 0

    def _one_industry(code: str) -> List[dict]:
        stocks = _sector_stocks(code)
        return [{"industry_code": code, **s} for s in stocks]

    membership: List[dict] = []
    if target:
        with ThreadPoolExecutor(max_workers=min(_FETCH_WORKERS, len(target))) as ex:
            for fut in as_completed([ex.submit(_one_industry, c) for c in target]):
                try:
                    membership.extend(fut.result())
                except Exception:  # noqa: BLE001
                    logger.warning("投资看板：成分股列表失败", exc_info=True)

    by_industry: Dict[str, List[dict]] = {}
    for m in membership:
        by_industry.setdefault(m["industry_code"], []).append(m)

    for ind_code, members in by_industry.items():
        codes = [m["stock_code"] for m in members]
        total += len(codes)
        frames = _fetch_many(codes, period, as_of)
        ind_b = ind_bull.get(ind_code)
        for m in members:
            got = _frame_for(frames.get(m["stock_code"]), period, as_of, now)
            if not got:
                skipped += 1
                continue
            frame, closed_at, effective = got
            last = frame.iloc[-1]
            stock_bull = int(last["bull_dur"])
            lead = _lead_days(stock_bull, ind_b)
            rows.append({
                "as_of": as_of,
                "period": period,
                "industry_code": ind_code,
                "stock_code": m["stock_code"],
                "stock_name": m["stock_name"],
                "direction": int(last["direction"]),
                "strength": _f(last["cur_str"]),
                "level": str(last["level"]),
                "bull_dur": stock_bull,
                "bear_dur": int(last["bear_dur"]),
                "lead_days": lead,
                "bias": _f(last["bias"]),
                "bias_z": _f(last["bias_z"]),
                "overbought": 1 if bool(last["overbought"]) else 0,
                "oversold": 1 if bool(last["oversold"]) else 0,
                "bars": int(last["bars"]),
                "stale": _stale_flag(period, as_of, now),
                "bar_closed_at": closed_at,
                "effective_from": effective,
                "computed_at": datetime.now(),
                "run_id": run_id,
            })
        _upsert_stock_snapshots(db, rows)
        rows = []
        db.commit()
    db.commit()
    return {
        "industries": len(by_industry),
        "total": total,
        "ok": total - skipped,
        "skipped": skipped,
        "duration_sec": round(time.monotonic() - t0, 1),
    }


def ensure_industry_stock_snapshot(db: Session, code: str, period: str,
                                   as_of: Optional[date] = None) -> str:
    """按需补齐单个行业的成分股快照（仅 30m）。

    30m 全市场个股取数成本高，默认只在用户打开行业抽屉时补齐：
    行业须在 30m 层级「由空转多」（bull_dur == 1）；已有快照直接复用。
    返回 ``ready`` / ``not_reversal`` / ``no_industry``。
    """
    if period != "30m":
        return "ready"
    day = as_of or latest_as_of(db, period)
    if day is None:
        return "no_industry"
    ind = db.execute(
        select(BoardIndustrySnapshot.bull_dur).where(
            BoardIndustrySnapshot.as_of == day,
            BoardIndustrySnapshot.period == period,
            BoardIndustrySnapshot.industry_code == code)
    ).first()
    if ind is None:
        return "no_industry"
    if int(ind[0] or 0) != 1:
        return "not_reversal"
    exists = db.execute(
        select(BoardStockSnapshot.id).where(
            BoardStockSnapshot.as_of == day,
            BoardStockSnapshot.period == period,
            BoardStockSnapshot.industry_code == code).limit(1)
    ).first()
    if exists is not None:
        return "ready"
    compute_stocks(db, period, day, trigger="lazy", industry_codes=[code])
    return "ready"


# ---------------- 刷新任务 ----------------

def create_run(db: Session, period: str, scope: str,
               trigger: str) -> BoardRun:
    run = BoardRun(period=period, scope=scope, trigger=trigger,
                   status="running", started_at=datetime.now(),
                   total=0, ok=0, skipped=0)
    db.add(run)
    db.commit()
    return run


def execute_run(run_id: int, period: str, scope: str,
                as_of: Optional[date] = None) -> dict:
    """执行一次刷新（独立 session，供后台线程/同步调用）。"""
    from core.db import SessionLocal

    db = SessionLocal()
    try:
        run = db.get(BoardRun, run_id)
        if run is None:
            return {"status": "failed", "error": "run 不存在"}
        try:
            universe_info = refresh_universe(db)
        except Exception as exc:  # noqa: BLE001
            logger.exception("投资看板：名单同步失败")
            universe_info = {"ok": False, "error": str(exc)}
        target_day = as_of or expected_latest_trading_day()
        stats: dict = {"universe": universe_info}
        status = "ok"
        try:
            if scope in (SCOPE_INDUSTRIES, "all"):
                stats["industries"] = compute_industries(
                    db, period, target_day, trigger=run.trigger, run_id=run_id)
            if scope in (SCOPE_STOCKS, "all"):
                stats["stocks"] = compute_stocks(
                    db, period, target_day, trigger=run.trigger, run_id=run_id)
        except Exception as exc:  # noqa: BLE001
            logger.exception("投资看板：刷新失败 run=%s", run_id)
            status = "failed"
            run.error = str(exc)
        if status == "ok":
            ind = stats.get("industries") or {}
            stk = stats.get("stocks") or {}
            if (ind.get("skipped") or 0) > 0 or (stk.get("skipped") or 0) > 0:
                status = "partial"
            run.total = int(ind.get("total") or 0) + int(stk.get("total") or 0)
            run.ok = int(ind.get("ok") or 0) + int(stk.get("ok") or 0)
            run.skipped = int(ind.get("skipped") or 0) + int(stk.get("skipped") or 0)
        run.status = status
        run.as_of = target_day
        run.finished_at = datetime.now()
        run.stats_json = json.dumps(stats, ensure_ascii=False)
        db.commit()
        return {"status": status, "run_id": run_id, "as_of": target_day.isoformat(),
                "stats": stats}
    finally:
        db.close()


def serialize_run(run: BoardRun) -> dict:
    stats = None
    if run.stats_json:
        try:
            stats = json.loads(run.stats_json)
        except ValueError:
            stats = None
    return {
        "id": run.id, "period": run.period, "scope": run.scope,
        "as_of": run.as_of.isoformat() if run.as_of else None,
        "trigger": run.trigger, "status": run.status,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
        "total": run.total, "ok": run.ok, "skipped": run.skipped,
        "error": run.error, "stats": stats,
    }


def latest_run(db: Session, period: str) -> Optional[BoardRun]:
    return db.execute(
        select(BoardRun).where(BoardRun.period == period)
        .order_by(BoardRun.id.desc()).limit(1)).scalars().first()


_BENCH_CACHE_TTL_SECONDS = 300
_bench_cache: dict = {}


def _benchmark_bull(code: str, day: date) -> Optional[int]:
    """当前选定基准的日线 bull_dur（已闭合 bar）。

    榜单领先天数以页面选定的基准为准，不再固定 000001.SH；
    300s 进程内缓存，避免同一页面多次请求重复取基准历史。
    """
    key = (code, day)
    now = time.monotonic()
    hit = _bench_cache.get(key)
    if hit is not None and now - hit[0] <= _BENCH_CACHE_TTL_SECONDS:
        return hit[1]
    bench = _benchmark_frame(day, datetime.now(), code)
    val = int(bench["bull_dur"]) if bench is not None else None
    _bench_cache[key] = (now, val)
    return val


def clear_benchmark_cache() -> None:
    _bench_cache.clear()


def benchmark_bull_dur(db: Session, period: str,
                       benchmark: Optional[str] = None,
                       as_of: Optional[date] = None) -> Optional[int]:
    """选定基准的 bull_dur；未指定时沿用门控默认基准。"""
    day = resolve_as_of(db, period, as_of)
    if day is None:
        return None
    return _benchmark_bull((benchmark or DEFAULT_GATE_INDEX).strip().upper(), day)


# ---------------- 查询与序列化 ----------------

def _lead_days(own_bull: Optional[int], ref_bull: Optional[int]) -> Optional[int]:
    """领先天数 = 自身多头持续 − 参照多头持续。

    自身尚未转多 → None；参照（大盘/行业）仍在空头（bull_dur=0）时，
    领先天数即自身已持续的多头天数（即「比参照早几天转多」）。
    """
    if not own_bull or int(own_bull) <= 0:
        return None
    return int(own_bull) - int(ref_bull or 0)


def _bias_watch(bias_z, overbought, oversold) -> tuple:
    """由既有 bias_z 派生观察档标记（2.0 <= |z| < 2.5），不改表结构。"""
    z = _f(bias_z)
    if z is None:
        return False, False
    ob = (not bool(overbought)) and BIAS_Z_WATCH <= z < BIAS_Z_K
    os_ = (not bool(oversold)) and -BIAS_Z_K < z <= -BIAS_Z_WATCH
    return ob, os_


def serialize_industry(row: BoardIndustrySnapshot,
                       bench_bull: Optional[int] = None,
                       rank: Optional[int] = None,
                       turn_at: Optional[str] = None) -> dict:
    lead = _lead_days(row.bull_dur, bench_bull)
    ob_watch, os_watch = _bias_watch(row.bias_z, row.overbought, row.oversold)
    return {
        "rank": rank,
        "code": row.industry_code,
        "name": row.industry_name,
        "as_of": row.as_of.isoformat() if row.as_of else None,
        "period": row.period,
        "direction": row.direction,
        "direction_label": _DIRECTION_LABELS.get(row.direction, "未定"),
        "strength": _f(row.strength),
        "level": row.level,
        "level_label": _LEVEL_LABELS.get(row.level or "none", "无信号"),
        "bull_dur": row.bull_dur,
        "bear_dur": row.bear_dur,
        "lead_days": lead,
        "turned_at": turn_at,
        "bias": _f(row.bias),
        "bias_z": _f(row.bias_z),
        "overbought": bool(row.overbought),
        "oversold": bool(row.oversold),
        "overbought_watch": ob_watch,
        "oversold_watch": os_watch,
        "jd": _f(row.jd),
        "jd_filter": row.jd_filter,
        "channel_filter": row.channel_filter,
        "up_str": _f(row.up_str),
        "dn_str": _f(row.dn_str),
        "bars": row.bars,
        "stale": bool(row.stale),
        "bar_closed_at": row.bar_closed_at,
        "effective_from": row.effective_from,
        "computed_at": (row.computed_at.isoformat()
                        if row.computed_at else None),
    }


def serialize_stock(row: BoardStockSnapshot,
                    rank: Optional[int] = None) -> dict:
    ob_watch, os_watch = _bias_watch(row.bias_z, row.overbought, row.oversold)
    return {
        "rank": rank,
        "code": row.stock_code,
        "name": row.stock_name,
        "industry_code": row.industry_code,
        "as_of": row.as_of.isoformat() if row.as_of else None,
        "period": row.period,
        "direction": row.direction,
        "direction_label": _DIRECTION_LABELS.get(row.direction, "未定"),
        "strength": _f(row.strength),
        "level": row.level,
        "level_label": _LEVEL_LABELS.get(row.level or "none", "无信号"),
        "bull_dur": row.bull_dur,
        "bear_dur": row.bear_dur,
        "lead_days": row.lead_days,
        "bias": _f(row.bias),
        "bias_z": _f(row.bias_z),
        "overbought": bool(row.overbought),
        "oversold": bool(row.oversold),
        "overbought_watch": ob_watch,
        "oversold_watch": os_watch,
        "bars": row.bars,
        "stale": bool(row.stale),
        "bar_closed_at": row.bar_closed_at,
        "effective_from": row.effective_from,
        "computed_at": (row.computed_at.isoformat()
                        if row.computed_at else None),
    }


def latest_as_of(db: Session, period: str) -> Optional[date]:
    return db.execute(
        select(BoardIndustrySnapshot.as_of)
        .where(BoardIndustrySnapshot.period == period)
        .order_by(BoardIndustrySnapshot.as_of.desc()).limit(1)
    ).scalars().first()


def resolve_as_of(db: Session, period: str,
                  as_of: Optional[date]) -> Optional[date]:
    return as_of or latest_as_of(db, period)


_INDUSTRY_SORTS = {
    "bull_dur": BoardIndustrySnapshot.bull_dur,
    "strength": BoardIndustrySnapshot.strength,
    "lead_days": BoardIndustrySnapshot.bull_dur,
    "bias_z": BoardIndustrySnapshot.bias_z,
    "code": BoardIndustrySnapshot.industry_code,
    "name": BoardIndustrySnapshot.industry_name,
    "direction": BoardIndustrySnapshot.direction,
}

_STOCK_SORTS = {
    "lead_days": BoardStockSnapshot.lead_days,
    "bull_dur": BoardStockSnapshot.bull_dur,
    "strength": BoardStockSnapshot.strength,
    "bias_z": BoardStockSnapshot.bias_z,
    "code": BoardStockSnapshot.stock_code,
}


def query_industries(db: Session, period: str, as_of: Optional[date] = None,
                     sort: str = "bull_dur", order: str = "desc",
                     level: Optional[str] = None,
                     limit: int = 200,
                     benchmark: Optional[str] = None) -> dict:
    day = resolve_as_of(db, period, as_of)
    if day is None:
        return {"as_of": None, "rows": [], "benchmark_bull_dur": None}
    q = select(BoardIndustrySnapshot).where(
        BoardIndustrySnapshot.period == period,
        BoardIndustrySnapshot.as_of == day)
    if level:
        q = q.where(BoardIndustrySnapshot.level == level)
    col = _INDUSTRY_SORTS.get(sort, BoardIndustrySnapshot.bull_dur)
    q = q.order_by(col.desc() if order != "asc" else col.asc())
    limit = max(1, min(int(limit or 200), 200))
    rows = list(db.execute(q.limit(limit)).scalars())
    bench = benchmark_bull_dur(db, period, benchmark, day)
    return {
        "as_of": day.isoformat(),
        "benchmark_bull_dur": bench,
        "rows": [serialize_industry(
            r, bench, i + 1,
            _turn_bar_time(r.as_of, period, r.bull_dur, r.bar_closed_at))
            for i, r in enumerate(rows)],
    }


def query_industry(db: Session, code: str, period: str,
                   as_of: Optional[date] = None,
                   benchmark: Optional[str] = None) -> Optional[dict]:
    day = resolve_as_of(db, period, as_of)
    if day is None:
        return None
    row = db.execute(
        select(BoardIndustrySnapshot).where(
            BoardIndustrySnapshot.period == period,
            BoardIndustrySnapshot.as_of == day,
            BoardIndustrySnapshot.industry_code == code)
    ).scalars().first()
    if row is None:
        return None
    return serialize_industry(
        row, benchmark_bull_dur(db, period, benchmark, day), None,
        _turn_bar_time(row.as_of, period, row.bull_dur, row.bar_closed_at))


def query_stocks(db: Session, industry_code: str, period: str,
                 as_of: Optional[date] = None, sort: str = "lead_days",
                 order: str = "desc", limit: int = 50) -> dict:
    day = resolve_as_of(db, period, as_of)
    if day is None:
        return {"as_of": None, "rows": []}
    q = select(BoardStockSnapshot).where(
        BoardStockSnapshot.period == period,
        BoardStockSnapshot.as_of == day,
        BoardStockSnapshot.industry_code == industry_code)
    col = _STOCK_SORTS.get(sort, BoardStockSnapshot.lead_days)
    q = q.order_by(col.desc() if order != "asc" else col.asc())
    limit = max(1, min(int(limit or 50), 500))
    rows = list(db.execute(q.limit(limit)).scalars())
    return {"as_of": day.isoformat(),
            "rows": [serialize_stock(r, i + 1) for i, r in enumerate(rows)]}


def query_leaders(db: Session, period: str, as_of: Optional[date] = None,
                  limit: int = 10,
                  benchmark: Optional[str] = None) -> dict:
    data = query_industries(db, period, as_of, sort="bull_dur",
                            order="desc", limit=200, benchmark=benchmark)
    rows = data["rows"]
    sustained = [r for r in rows if (r["bull_dur"] or 0) > 0][:limit]
    reversal = sorted(
        [r for r in rows if r["bull_dur"] == 1],
        key=lambda r: r["strength"] or 0, reverse=True)[:limit]
    excess = sorted(
        [r for r in rows if (r["lead_days"] or 0) > 0],
        key=lambda r: r["lead_days"], reverse=True)[:limit]
    return {"as_of": data["as_of"],
            "benchmark_bull_dur": data.get("benchmark_bull_dur"),
            "sustained": sustained, "reversal": reversal, "excess": excess}


def level_counts(db: Session, period: str,
                 as_of: Optional[date] = None) -> dict:
    day = resolve_as_of(db, period, as_of)
    if day is None:
        return {}
    rows = db.execute(
        select(BoardIndustrySnapshot.level).where(
            BoardIndustrySnapshot.period == period,
            BoardIndustrySnapshot.as_of == day)).scalars()
    out: Dict[str, int] = {}
    for lv in rows:
        key = lv or "none"
        out[key] = out.get(key, 0) + 1
    return out


def watch_counts(db: Session, period: str,
                 as_of: Optional[date] = None) -> dict:
    """观察档（2.0 <= |z| < 2.5）行业数，用于榜单折叠计数。"""
    day = resolve_as_of(db, period, as_of)
    if day is None:
        return {"overbought": 0, "oversold": 0}
    rows = db.execute(
        select(BoardIndustrySnapshot.bias_z,
               BoardIndustrySnapshot.overbought,
               BoardIndustrySnapshot.oversold).where(
            BoardIndustrySnapshot.period == period,
            BoardIndustrySnapshot.as_of == day)).all()
    ob = os_ = 0
    for bias_z, overbought, oversold in rows:
        w_ob, w_os = _bias_watch(bias_z, overbought, oversold)
        ob += int(w_ob)
        os_ += int(w_os)
    return {"overbought": ob, "oversold": os_}


def meta(db: Session) -> dict:
    return {
        "formula_version": RATING_FORMULA_VERSION,
        "direction_basis": "m_enh",
        "min_bars": MIN_BARS,
        "bias_z": {"window": BIAS_Z_WINDOW, "k": BIAS_Z_K,
                   "watch": BIAS_Z_WATCH,
                   "include_today": False, "symmetric": True},
        "periods": list(BOARD_PERIODS),
        "universe": universe_status(db),
        "benchmark": DEFAULT_GATE_INDEX,
        "level_labels": _LEVEL_LABELS,
        "caliber_note": (
            "强度/方向/超买超卖全部由本项目 rating_core 产出（tqgate_py_v2）；"
            "超买超卖主档 k=2.5，2.0≤|z|<2.5 仅作轻度提示；"
            "行业评级系统仅提供榜单形态与 30m 切换的产品化参考。"
        ),
    }
