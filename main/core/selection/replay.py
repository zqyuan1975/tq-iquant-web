# -*- coding: utf-8 -*-
"""历史回放内核：按 ``as_of`` 从平台内置仓做 PIT 重建，并交付历史池。

口径（不读外部目录，只用内置仓 + 平台 TQ P2 封装）：

- **财报**：``selection_wh_fin.series_blob`` 自带 ``announce_time``，逐只逐期
  截断到 ``announce_time <= as_of``；同一 ``tag_time`` 有多个版本时取截止日
  前最后一次披露。期数、审计意见、同比/趋势/CAGR 全部基于截断后的序列。
- **市值/股息率**：``get_gpjy_value_by_date`` 的 GP16/GP20/GP21 按「年 + 月日」
  取，落 ``selection_gp_snapshots`` 缓存（含股息率），重复回放不再打 TQ。
- **分红**：``selection_wh_dividend.rows_blob`` 按除权日 <= as_of 过滤（源头为
  ``get_divid_factors``）；``dp3/dp_annual`` 优先用当日 GP21（可重建，
  且不受分红序列 2024 年后不完整的影响）。
- **K 线**：``selection_wh_daily`` 按 ``trade_date <= as_of`` 取区间，再按宽库
  同口径（近 252 交易日）重算收盘/波动率/最大回撤。

不可 PIT 重建的字段（PE/PB/流通市值/一致预期/ST/退市标记）一律留空，并在
快照 ``quality.pit_unavailable`` 显式列出，绝不用今日值冒充。批量回放以
``(run_id, phase=as_of, batch_key=方案/阶段)`` 记录进度，可按快照幂等续跑。
"""
from __future__ import annotations

import argparse
import copy
import json
import math
import sys
import time
import traceback
import zlib
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from sqlalchemy import func, text
from sqlalchemy.exc import IntegrityError

from core.models.selection import (
    SelectionGpSnapshot, SelectionPoolInterval, SelectionRun, SelectionScheme,
    SelectionSchemeVersion, SelectionSnapshot, SelectionSnapshotStock,
    SelectionWarehouseBuildProgress, SelectionWarehouseDividend,
    SelectionWarehouseFin, SelectionWarehouseMeta, SelectionWarehouseStock,
)
from core.selection import engine, sector_pools
from core.services import selection_run_service

#: 首批回放的 11 个季末（决策日；所有截断以此为上限）
REPLAY_AS_OFS: Tuple[date, ...] = (
    date(2024, 4, 30), date(2024, 6, 28), date(2024, 9, 30),
    date(2024, 12, 31), date(2025, 3, 31), date(2025, 6, 30),
    date(2025, 9, 30), date(2025, 12, 31), date(2026, 3, 31),
    date(2026, 6, 30), date(2026, 9, 30),
)
GP_FIELDS: Tuple[str, ...] = ("GP16", "GP20", "GP21")
GP_MIN_COVERAGE = 0.6        # 缓存覆盖率达到该比例即视为「已取过」，不再打 TQ
PROGRESS_RUN_ID = "replay"
_CTX_CACHE: Dict[tuple, dict] = {}
_CTX_ORDER: List[tuple] = []
_CTX_MAX = 2


class ReplayError(engine.EngineError):
    """回放前置不满足（数据缺失 / TQ 锁被占）。"""


# ------------------------------------------------------------------ helpers
def _cutoff(as_of: date) -> int:
    return int(as_of.strftime("%Y%m%d"))


def _num(value: Any) -> Optional[float]:
    return engine._num(value)


def _wh_key(db) -> tuple:
    meta = engine.warehouse_meta(db)
    if meta is None:
        raise ReplayError("尚未导入宽库，无法回放")
    return (meta.as_of, meta.imported_at, meta.stock_count)


# ------------------------------------------------------------------ 财报截断
def truncate_series(series: dict, as_of: date) -> dict:
    """把一条财报序列截断到 ``announce_time <= as_of``。

    - 同一 ``tag_time`` 多版本：取截止日前最后一次披露（重述按当时可见版本）；
    - 截止日前未披露的期次整期剔除；
    - 返回的新 dict 只含保留下来的期次，其余键原样保留。
    """
    tags = list(series.get("tag_time") or [])
    anns = list(series.get("announce_time") or [])
    cutoff = _cutoff(as_of)
    picked: Dict[str, Tuple[int, int]] = {}
    for idx, (tag, ann) in enumerate(zip(tags, anns)):
        try:
            ann_int = int(ann)
        except (TypeError, ValueError):
            continue
        if ann_int > cutoff:
            continue
        key = str(tag)
        prev = picked.get(key)
        if prev is None or ann_int >= prev[0]:
            picked[key] = (ann_int, idx)
    order = [idx for _, idx in sorted(picked.values(), key=lambda p: p[1])]
    out: Dict[str, Any] = {}
    for key, value in series.items():
        if isinstance(value, list) and len(value) == len(tags):
            out[key] = [value[i] for i in order]
        else:
            out[key] = value
    return out


def _load_series(db, as_of: date) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    rows = db.query(SelectionWarehouseFin.code,
                    SelectionWarehouseFin.series_blob).all()
    for code, blob in rows:
        try:
            series = engine._json_blob(blob)
        except Exception:
            continue
        if series:
            out[code] = truncate_series(series, as_of)
    return out


# ------------------------------------------------------------------ 分红
def _load_dividends(db, as_of: date) -> Dict[str, list]:
    """除权日 <= as_of 的分红行（``[ex_date, year, bonus]``，bonus 元/股）。"""
    cutoff = _cutoff(as_of)
    out: Dict[str, list] = {}
    rows = db.query(SelectionWarehouseDividend.code,
                    SelectionWarehouseDividend.rows_blob).all()
    for code, blob in rows:
        try:
            data = engine._json_blob(blob)
        except Exception:
            continue
        kept = []
        for item in data or []:
            if not isinstance(item, (list, tuple)) or len(item) < 3:
                continue
            try:
                ex = int(item[0])
            except (TypeError, ValueError):
                continue
            if ex <= cutoff:
                kept.append([item[0], item[1], item[2]])
        if kept:
            out[code] = kept
    return out


# ------------------------------------------------------------------ K 线派生
def _load_metrics(db, as_of: date) -> Dict[str, dict]:
    """按宽库同口径重算 close / 波动率 / 近 252 日最大回撤（不复权收盘）。"""
    sql = text("""
        SELECT code, close FROM (
            SELECT code, close,
                   ROW_NUMBER() OVER (
                       PARTITION BY code ORDER BY trade_date DESC) AS rn
            FROM selection_wh_daily
            WHERE trade_date <= :d AND close IS NOT NULL
        ) t WHERE rn <= 252 ORDER BY code, rn DESC
    """)
    out: Dict[str, dict] = {}
    code_now: Optional[str] = None
    closes: List[float] = []
    for code, close in db.execute(sql, {"d": as_of}):
        if code != code_now:
            if code_now and closes:
                out[code_now] = _metrics_from_closes(closes)
            code_now = code
            closes = []
        if close is not None:
            closes.append(float(close))
    if code_now and closes:
        out[code_now] = _metrics_from_closes(closes)
    return out


def _metrics_from_closes(closes: List[float]) -> dict:
    window = closes[-252:]
    latest = window[-1]
    volatility = None
    drawdown = None
    if len(window) >= 2:
        returns = [window[i] / window[i - 1] - 1.0
                   for i in range(1, len(window)) if window[i - 1]]
        if returns:
            mean = sum(returns) / len(returns)
            var = sum((r - mean) ** 2 for r in returns) / len(returns)
            volatility = round(math.sqrt(var) * math.sqrt(252) * 100, 4)
        peak = window[0]
        worst = 0.0
        for value in window:
            peak = max(peak, value)
            if peak:
                worst = min(worst, value / peak - 1.0)
        drawdown = round(worst * 100, 4)
    return {"close": latest, "volatility_pct": volatility,
            "max_drawdown_pct": drawdown}


# ------------------------------------------------------------------ GP 快照
def _gp_map(db, as_of: date) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    rows = (db.query(SelectionGpSnapshot)
            .filter(SelectionGpSnapshot.as_of == as_of).all())
    for row in rows:
        if row.total_mv_wan is None and row.div_yield_pct is None:
            continue
        out[row.code] = {
            "GP16": float(row.total_mv_wan) if row.total_mv_wan is not None else None,
            "GP20": float(row.pledge_ratio) if row.pledge_ratio is not None else None,
            "GP21": float(row.div_yield_pct) if row.div_yield_pct is not None else None,
        }
    return out


def _universe_codes(db) -> List[str]:
    return [code for (code,) in
            db.query(SelectionWarehouseStock.code)
            .order_by(SelectionWarehouseStock.code).all()]


def ensure_gp(db, as_of: date, codes: Optional[Sequence[str]] = None,
              priority: str = "P2", force: bool = False) -> Dict[str, Any]:
    """取 as_of 当日 GP16/GP20/GP21 并缓存到 ``selection_gp_snapshots``。

    覆盖率达标即视为已取过（幂等续跑）；TQ 抢不到锁时抛 ``ReplayError``，
    由批处理记录失败原因并保留断点。
    """
    codes = list(codes or _universe_codes(db))
    existing = {row.code: row for row in
                db.query(SelectionGpSnapshot)
                .filter(SelectionGpSnapshot.as_of == as_of).all()}
    covered = sum(1 for row in existing.values()
                  if row.total_mv_wan is not None)
    if not force and codes and covered >= len(codes) * GP_MIN_COVERAGE:
        return {"as_of": as_of.isoformat(), "status": "cached",
                "covered": covered, "universe": len(codes),
                "fetched": 0, "source": "selection_gp_snapshots"}

    from core.tq.data import TQData

    tq = TQData()
    started = time.time()
    try:
        frame = tq.get_gpjy_value_by_date(
            codes, as_of.year, "%02d%02d" % (as_of.month, as_of.day),
            list(GP_FIELDS), priority=priority)
    except Exception as exc:  # TdxTaskBlocked / 连接异常
        raise ReplayError(
            "GP 取数失败（%s，%s）：%s" % (as_of.isoformat(),
                                         type(exc).__name__, exc)) from None
    fetched: Dict[str, Dict[str, float]] = {}
    if frame is not None and not getattr(frame, "empty", True):
        code_col = next((c for c in ("Code", "code", "CODE")
                         if c in frame.columns), None)
        if code_col is None:
            raise ReplayError("GP 返回缺少代码列：%s" % list(frame.columns))
        for _, row in frame.iterrows():
            code = str(row.get(code_col) or "")
            if not code:
                continue
            vals: Dict[str, float] = {}
            for field in GP_FIELDS:
                val = _num(row.get(field))
                if val not in (None, 0.0):
                    vals[field] = val
            if vals:
                # 同一代码多行时保留非零值（平台对部分票返回全 0 占位行）
                merged = fetched.setdefault(code, {})
                for key, val in vals.items():
                    if key not in merged or abs(val) > abs(merged[key]):
                        merged[key] = val

    for code, vals in fetched.items():
        row = existing.get(code)
        if row is None:
            row = SelectionGpSnapshot(code=code, as_of=as_of,
                                      source="tq:GP16")
            db.add(row)
            existing[code] = row
        if vals.get("GP16") is not None:
            row.total_mv_wan = vals["GP16"]
        if vals.get("GP20") is not None:
            row.pledge_ratio = vals["GP20"]
        if vals.get("GP21") is not None:
            row.div_yield_pct = vals["GP21"]
    db.commit()
    return {"as_of": as_of.isoformat(), "status": "fetched",
            "covered": sum(1 for row in existing.values()
                           if row.total_mv_wan is not None),
            "universe": len(codes), "fetched": len(fetched),
            "with_yield": sum(1 for vals in fetched.values()
                              if vals.get("GP21") is not None),
            "elapsed_sec": round(time.time() - started, 2),
            "source": "tq:get_gpjy_value_by_date"}


# ------------------------------------------------------------------ 上下文
def build_context(db, as_of: date) -> dict:
    """重建 as_of 时点的 records（同一 as_of 可跨方案复用）。"""
    meta = engine.warehouse_meta(db)
    if meta is None:
        raise ReplayError("尚未导入宽库，无法回放")
    started = time.time()
    series = _load_series(db, as_of)
    dividends = _load_dividends(db, as_of)
    metrics = _load_metrics(db, as_of)
    gp = _gp_map(db, as_of)
    universe = _universe_codes(db)
    if not series:
        raise ReplayError("内置仓没有财报序列，无法重建 %s" % as_of.isoformat())

    recs = engine._build_records(db, meta, pit={
        "series": series, "dividends": dividends,
        "metrics": metrics, "gp": gp})
    periods = sorted(len(s.get("tag_time") or []) for s in series.values())
    div_rows = sum(len(v) for v in dividends.values())
    counts = {
        "universe": len(universe),
        "fin_series": len(series),
        "fin_periods_min": periods[0] if periods else 0,
        "fin_periods_median": periods[len(periods) // 2] if periods else 0,
        "fin_periods_max": periods[-1] if periods else 0,
        "dividend_codes": len(dividends),
        "dividend_rows": div_rows,
        "gp_codes": len(gp),
        "gp_yield_codes": sum(1 for vals in gp.values()
                              if vals.get("GP21") is not None),
        "kline_codes": len(metrics),
        "kline_missing": len(universe) - len(metrics),
        "elapsed_sec": round(time.time() - started, 2),
    }
    warnings: List[str] = []
    if len(gp) < len(universe) * GP_MIN_COVERAGE:
        warnings.append(
            "GP16 覆盖率 %d/%d 偏低：市值/股息率类因子覆盖不足，先跑 replay --gp-only"
            % (len(gp), len(universe)))
    if len(metrics) < len(universe) * 0.9:
        warnings.append("日线覆盖 %d/%d：%d 只票缺少 as_of 前日线，K 线因子留空"
                        % (len(metrics), len(universe), len(universe) - len(metrics)))
    quality = {
        "mode": "replay",
        "as_of": as_of.isoformat(),
        "warehouse_as_of": meta.as_of.isoformat(),
        "fin_rule": "announce_time <= as_of 逐期截断；同 tag_time 取截止日前最后一次披露",
        "market_cap_source": "get_gpjy_value_by_date GP16（万元）→ 亿元；按年 + 月日取",
        "dividend_source": "selection_wh_dividend 除权日 <= as_of（源头 get_divid_factors）；dp3/dp_annual 优先当日 GP21",
        "kline_source": "selection_wh_daily 不复权收盘；trade_date <= as_of 后取近 252 个交易日",
        "counts": counts,
        "warnings": warnings,
        "pit_unavailable": [
            {"field": "pe", "reason": "没有历史 PE 快照，不用导入日 PE 冒充"},
            {"field": "pb", "reason": "没有历史 PB 快照，不用导入日 PB 冒充"},
            {"field": "ltsz", "reason": "没有历史流通市值；规模因子用 GP16 总市值代理"},
            {"field": "g_fwd_2y/g_rev_2y/neg_peg_fwd",
             "reason": "GO 一致预期没有历史序列；成长池强制走 historical 轨"},
            {"field": "is_st", "reason": "ST 标记只有当前值，回放置否并在此披露"},
            {"field": "is_quit", "reason": "退市标记只有当前值，回放置否并在此披露"},
        ],
    }
    return {"as_of": as_of, "records": recs, "quality": quality}


def context(db, as_of: date, use_cache: bool = True) -> dict:
    """取 as_of 的 PIT 重建结果（带 LRU 缓存，供一次批量内复用）。"""
    key = (_wh_key(db), as_of)
    if use_cache and key in _CTX_CACHE:
        return _CTX_CACHE[key]
    ctx = build_context(db, as_of)
    if use_cache:
        _CTX_CACHE[key] = ctx
        _CTX_ORDER.append(key)
        while len(_CTX_ORDER) > _CTX_MAX:
            _CTX_CACHE.pop(_CTX_ORDER.pop(0), None)
    return ctx


def copy_records(ctx: dict) -> List[dict]:
    return copy.deepcopy(ctx["records"])


def clear_cache() -> None:
    _CTX_CACHE.clear()
    _CTX_ORDER.clear()


# ------------------------------------------------------------------ 进度
def _record_progress(db, phase: str, batch_key: str, status: str, *,
                     total: Optional[int] = None, done: Optional[int] = None,
                     error: Optional[str] = None,
                     payload: Optional[dict] = None,
                     started: bool = False) -> None:
    row = (db.query(SelectionWarehouseBuildProgress)
           .filter_by(run_id=PROGRESS_RUN_ID, phase=phase,
                      batch_key=batch_key).first())
    if row is None:
        row = SelectionWarehouseBuildProgress(
            run_id=PROGRESS_RUN_ID, mode="replay", phase=phase,
            batch_key=batch_key, status="pending")
        db.add(row)
        db.flush()
    row.status = status
    row.mode = "replay"
    if total is not None:
        row.total = total
    if done is not None:
        row.done = done
    row.error = (str(error)[:500] if error else None)
    if payload is not None:
        row.payload_json = json.dumps(payload, ensure_ascii=False, default=str)
    if started:
        row.started_at = datetime.now()
    row.updated_at = datetime.now()
    db.commit()


def status(db) -> dict:
    rows = (db.query(SelectionWarehouseBuildProgress)
            .filter(SelectionWarehouseBuildProgress.run_id == PROGRESS_RUN_ID)
            .order_by(SelectionWarehouseBuildProgress.phase,
                      SelectionWarehouseBuildProgress.batch_key).all())
    phases: Dict[str, dict] = {}
    for row in rows:
        item = phases.setdefault(row.phase, {
            "as_of": row.phase, "batches": 0, "done": 0, "failed": 0,
            "running": 0, "pending": 0, "errors": [], "counts": None})
        item["batches"] += 1
        if row.status in ("done", "skipped"):
            item["done"] += 1
        elif row.status == "failed":
            item["failed"] += 1
            if row.error:
                item["errors"].append("%s: %s" % (row.batch_key, row.error))
        else:
            item[row.status] = item.get(row.status, 0) + 1
        if row.batch_key == "PIT" and row.payload_json:
            try:
                item["counts"] = json.loads(row.payload_json)
            except Exception:
                pass
    return {"run_id": PROGRESS_RUN_ID,
            "phases": [phases[key] for key in sorted(phases)],
            "finished": [key for key in sorted(phases)
                         if phases[key]["failed"] == 0
                         and phases[key].get("pending", 0) == 0
                         and phases[key].get("running", 0) == 0]}


# ------------------------------------------------------------------ 选股交付
def _active_versions(db) -> List[Tuple[SelectionScheme, SelectionSchemeVersion]]:
    out = []
    schemes = (db.query(SelectionScheme)
               .filter(SelectionScheme.status == "active")
               .order_by(SelectionScheme.id).all())
    for scheme in schemes:
        version = (db.query(SelectionSchemeVersion)
                   .filter_by(scheme_id=scheme.id, status="active")
                   .order_by(SelectionSchemeVersion.version.desc()).first())
        if version is not None:
            out.append((scheme, version))
    return out


def _snapshot_at(db, version_id: int, as_of: date) -> Optional[SelectionSnapshot]:
    return (db.query(SelectionSnapshot)
            .filter(SelectionSnapshot.scheme_version_id == version_id,
                    SelectionSnapshot.as_of == as_of)
            .order_by(SelectionSnapshot.id.desc()).first())


def run_as_of(db, as_of: date, *, publish_version: bool = True,
              force: bool = False) -> dict:
    """单个 as_of 的完整回放：GP → PIT 重建 → 选股 → 冻结/交付 → 区间。"""
    phase = as_of.isoformat()
    started = time.time()
    summary: Dict[str, Any] = {
        "as_of": phase, "gp": None, "pit": None, "runs": [],
        "snapshots": 0, "published": 0, "intervals": None,
        "skipped": [], "errors": [], "elapsed_sec": None}

    _record_progress(db, phase, "GP", "running", started=True)
    try:
        gp = ensure_gp(db, as_of)
        summary["gp"] = gp
        _record_progress(db, phase, "GP", "done",
                         total=gp.get("universe"), done=gp.get("covered"),
                         payload=gp)
    except Exception as exc:
        _record_progress(db, phase, "GP", "failed", error=str(exc))
        summary["errors"].append("GP: %s" % exc)
        summary["elapsed_sec"] = round(time.time() - started, 2)
        return summary

    _record_progress(db, phase, "PIT", "running", started=True)
    try:
        ctx = context(db, as_of)
        summary["pit"] = ctx["quality"]["counts"] | {
            "warnings": ctx["quality"]["warnings"]}
        _record_progress(db, phase, "PIT", "done",
                         total=ctx["quality"]["counts"]["universe"],
                         done=len(ctx["records"]),
                         payload=ctx["quality"]["counts"])
    except Exception as exc:
        _record_progress(db, phase, "PIT", "failed", error=str(exc))
        summary["errors"].append("PIT: %s" % exc)
        summary["elapsed_sec"] = round(time.time() - started, 2)
        return summary

    for scheme, version in _active_versions(db):
        key = scheme.code
        existing = _snapshot_at(db, version.id, as_of)
        if existing is not None and not force:
            if not existing.frozen:
                selection_run_service.freeze(db, existing.id)
            _record_progress(db, phase, key, "skipped", done=1,
                             payload={"reason": "已有快照",
                                      "snapshot_id": existing.id})
            summary["runs"].append({
                "pool_key": scheme.pool_key, "scheme": key,
                "snapshot_id": existing.id, "run_status": "existing",
                "selected": db.query(func.count(SelectionSnapshotStock.id))
                .filter_by(snapshot_id=existing.id, selected=True).scalar()})
            continue
        _record_progress(db, phase, key, "running", started=True)
        try:
            res = selection_run_service.run_schemes(
                db, scheme_ids=[scheme.id], as_of=as_of)
        except Exception as exc:
            _record_progress(db, phase, key, "failed", error=str(exc))
            summary["errors"].append("%s: %s" % (key, exc))
            continue
        runs = res.get("runs") or []
        skipped = res.get("skipped") or []
        if skipped:
            reason = skipped[0].get("error") or "skipped"
            _record_progress(db, phase, key, "failed", error=reason,
                             payload={"status": "skipped"})
            summary["skipped"].append({"pool_key": scheme.pool_key,
                                       "scheme": key, "reason": reason})
            continue
        if not runs:
            _record_progress(db, phase, key, "failed", error="没有运行记录")
            continue
        run = runs[0]
        snap = _snapshot_at(db, version.id, as_of)
        if run["status"] != "succeeded" or snap is None:
            reason = run.get("error") or run["status"]
            _record_progress(db, phase, key, "failed", error=reason)
            summary["skipped"].append({"pool_key": scheme.pool_key,
                                       "scheme": key, "reason": reason})
            continue
        if not snap.frozen:
            selection_run_service.freeze(db, snap.id)
        _record_progress(db, phase, key, "done", done=1,
                         payload={"snapshot_id": snap.id,
                                  "selected": run.get("stock_count") or 0})
        summary["runs"].append({
            "pool_key": scheme.pool_key, "scheme": key,
            "snapshot_id": snap.id, "selected": run.get("stock_count") or 0,
            "run_status": run["status"]})
    db.commit()

    # 交付：每期版本池（不写 stock_pool_memberships 序列，序列走 intervals）
    if publish_version:
        snaps = _snapshots_at(db, as_of)
        for snap in snaps:
            if snap.published_pool_id:
                continue
            try:
                published = selection_run_service.publish(
                    db, snap.id, series=False)
                summary["published"] += 1
                for item in summary["runs"]:
                    if item["snapshot_id"] == snap.id:
                        item["version_pool_id"] = published["pool_id"]
                        item["published_members"] = published["stock_count"]
            except Exception as exc:
                summary["errors"].append("publish snapshot %s: %s"
                                         % (snap.id, exc))
        db.commit()
    summary["snapshots"] = len(_snapshots_at(db, as_of))
    summary["elapsed_sec"] = round(time.time() - started, 2)
    return summary


def _snapshots_at(db, as_of: date) -> List[SelectionSnapshot]:
    versions = [version.id for _, version in _active_versions(db)]
    if not versions:
        return []
    return (db.query(SelectionSnapshot)
            .filter(SelectionSnapshot.as_of == as_of,
                    SelectionSnapshot.scheme_version_id.in_(versions))
            .order_by(SelectionSnapshot.id).all())


# ------------------------------------------------------------------ 成员区间
def series_pool_code(pool_key: str) -> str:
    """方案 pool_key → 序列池代码 ``SEL_{TAG}``。"""
    return "SEL_" + pool_key.upper().replace(":", "_")


def pool_key_for_code(db, pool_code: Optional[str]) -> Optional[str]:
    """把 ``SEL_{TAG}`` 池代码反解成方案 pool_key（用于按日期解析历史成分）。"""
    if not pool_code or not pool_code.startswith("SEL_"):
        return None
    keys = [key for (key,) in
            db.query(SelectionScheme.pool_key)
            .filter(SelectionScheme.status == "active").distinct().all()]
    for key in keys:
        if series_pool_code(key) == pool_code:
            return key
    return None


def _pool_observations(db, pool_key: str) -> List[Tuple[date, int, Set[str]]]:
    """池在各观察日的成员集合（同一天取最新快照，只取当日后的最新一期）。

    只认**已冻结**快照：冻结 = 已交付，未冻结的草稿不能定义历史成分（否则
    别人正在编辑的当期快照会让历史区间抖动）。
    """
    rows = (db.query(SelectionSnapshot.id, SelectionSnapshot.as_of)
            .join(SelectionSchemeVersion,
                  SelectionSchemeVersion.id == SelectionSnapshot.scheme_version_id)
            .join(SelectionScheme,
                  SelectionScheme.id == SelectionSchemeVersion.scheme_id)
            .filter(SelectionScheme.pool_key == pool_key,
                    SelectionSnapshot.frozen.is_(True),
                    SelectionSnapshot.as_of >= REPLAY_AS_OFS[0])
            .order_by(SelectionSnapshot.as_of, SelectionSnapshot.id).all())
    latest: Dict[date, int] = {}
    for snapshot_id, as_of in rows:
        latest[as_of] = snapshot_id
    if not latest:
        return []
    snap_ids = list(latest.values())
    members: Dict[int, Set[str]] = {sid: set() for sid in snap_ids}
    codes = (db.query(SelectionSnapshotStock.snapshot_id,
                      SelectionSnapshotStock.code)
             .filter(SelectionSnapshotStock.snapshot_id.in_(snap_ids),
                     SelectionSnapshotStock.selected.is_(True)).all())
    for snapshot_id, code in codes:
        members[snapshot_id].add(code)
    return [(as_of, latest[as_of], members[latest[as_of]])
            for as_of in sorted(latest)]


def build_intervals(observations: Sequence[Tuple[date, int, Set[str]]],
                    open_tail: bool = True) -> List[dict]:
    """把「各观察日成员集合」合并成成员区间。

    - 连续观察日都在成员内 → 合并成一段，区间到「首次缺席观察日的前一天」；
    - 最后一段若仍延续到最后一个观察日：``open_tail`` 为真写开口（当期），
      否则以最后观察日封闭（历史池没有当期交付，不伪造开口）。
    """
    if not observations:
        return []
    by_code: Dict[str, List[int]] = {}
    for idx, (_as_of, _sid, codes) in enumerate(observations):
        for code in codes:
            by_code.setdefault(code, []).append(idx)
    out: List[dict] = []
    last_idx = len(observations) - 1
    for code, idxs in sorted(by_code.items()):
        start = prev = idxs[0]
        runs: List[Tuple[int, int]] = []
        for idx in idxs[1:]:
            if idx == prev + 1:
                prev = idx
                continue
            runs.append((start, prev))
            start = prev = idx
        runs.append((start, prev))
        for begin, end in runs:
            from_day = observations[begin][0]
            if end == last_idx and open_tail:
                to_day = None
                source = "snapshot"
            else:
                if end == last_idx:
                    to_day = observations[end][0]
                else:
                    to_day = observations[end + 1][0] - timedelta(days=1)
                source = "backfill"
            out.append({
                "code": code, "effective_from": from_day,
                "effective_to": to_day, "source": source,
                "snapshot_id": observations[begin][1],
                "note": "replay:%s~%s" % (
                    from_day.isoformat(),
                    to_day.isoformat() if to_day else "open")})
    return out


def check_overlaps(pool_key: str, intervals: Sequence[dict],
                   existing: Sequence[SelectionPoolInterval] = ()) -> int:
    """同池同票区间重叠检测；发现即抛错并定位到池/代码/日期。"""
    checks: Dict[str, List[Tuple[date, Optional[date], str]]] = {}
    for item in intervals:
        checks.setdefault(item["code"], []).append(
            (item["effective_from"], item["effective_to"], "new"))
    for row in existing:
        checks.setdefault(row.code, []).append(
            (row.effective_from, row.effective_to, row.source or "db"))
    anomalies = 0
    for code, spans in checks.items():
        spans.sort(key=lambda s: s[0])
        for (f1, t1, s1), (f2, t2, s2) in zip(spans, spans[1:]):
            if t1 is None or f2 <= t1:
                anomalies += 1
                raise ReplayError(
                    "成员区间重叠：池=%s 代码=%s 区间[%s~%s](%s) 与 [%s~%s](%s)"
                    % (pool_key, code, f1.isoformat(),
                       t1.isoformat() if t1 else "open", s1,
                       f2.isoformat(), t2.isoformat() if t2 else "open", s2))
    return anomalies


def ensure_series_pools(db, pool_keys: Optional[Sequence[str]] = None) -> List[dict]:
    """确保每个方案都有序列池 ``SEL_{TAG}``，当前成分取最新一期**已冻结**快照。

    历史成分由 ``selection_pool_intervals`` 承载；这里只补池行与当前成分，
    让回测/实盘的选择器能看到池并按日期解析（幂等：重复执行只刷新当前成分）。
    """
    from core.models.stock_pool import StockPool
    from core.models.stock_pool_stock import StockPoolStock

    schemes = (db.query(SelectionScheme)
               .filter(SelectionScheme.status == "active")
               .order_by(SelectionScheme.id).all())
    out: List[dict] = []
    for scheme in schemes:
        if pool_keys and scheme.pool_key not in pool_keys:
            continue
        snap = (db.query(SelectionSnapshot)
                .join(SelectionSchemeVersion,
                      SelectionSchemeVersion.id ==
                      SelectionSnapshot.scheme_version_id)
                .filter(SelectionSchemeVersion.scheme_id == scheme.id,
                        SelectionSnapshot.frozen.is_(True),
                        SelectionSnapshot.as_of >= REPLAY_AS_OFS[0])
                .order_by(SelectionSnapshot.as_of.desc(),
                          SelectionSnapshot.id.desc()).first())
        if snap is None:
            out.append({"pool_key": scheme.pool_key,
                        "pool_code": series_pool_code(scheme.pool_key),
                        "status": "no_snapshot"})
            continue
        code = series_pool_code(scheme.pool_key)
        pool = db.query(StockPool).filter_by(code=code).first()
        if pool is None:
            pool = StockPool(code=code, name="%s选股序列" % scheme.name)
            db.add(pool)
            db.flush()
        current = [code_ for (code_,) in
                   db.query(SelectionSnapshotStock.code)
                   .filter_by(snapshot_id=snap.id, selected=True)
                   .order_by(SelectionSnapshotStock.rank.asc()).all()]
        db.query(StockPoolStock).filter_by(pool_id=pool.id).delete(
            synchronize_session=False)
        for stock_code in current:
            db.add(StockPoolStock(pool_id=pool.id, stock_code=stock_code))
        pool.synced_at = datetime.now()
        db.commit()
        out.append({"pool_key": scheme.pool_key, "pool_code": code,
                    "pool_id": pool.id, "snapshot_id": snap.id,
                    "as_of": snap.as_of.isoformat(),
                    "members": len(current), "status": "ok"})
    return out


def rebuild_pool_intervals(db, pool_key: str, *, write: bool = True) -> dict:
    """按该池全部快照重算成员区间并落 ``selection_pool_intervals``。"""
    obs = _pool_observations(db, pool_key)
    if not obs:
        return {"pool_key": pool_key, "observations": 0, "intervals": 0,
                "members_avg": 0, "written": 0, "anomalies": 0}
    open_tail = obs[-1][0] >= max(REPLAY_AS_OFS)
    intervals = build_intervals(obs, open_tail=open_tail)
    replay_notes = SelectionPoolInterval.note.like("replay:%")
    existing = (db.query(SelectionPoolInterval)
                .filter(SelectionPoolInterval.pool_key == pool_key,
                        ~replay_notes).all())
    # 先与其它来源（live/snapshot）对账，再与自身校验
    anomalies = check_overlaps(pool_key, intervals, existing) if existing else 0
    anomalies += check_overlaps(pool_key, intervals)
    written = 0
    if write:
        (db.query(SelectionPoolInterval)
         .filter(SelectionPoolInterval.pool_key == pool_key,
                 replay_notes).delete(synchronize_session=False))
        db.flush()
        for item in intervals:
            db.add(SelectionPoolInterval(
                pool_key=pool_key, code=item["code"],
                effective_from=item["effective_from"],
                effective_to=item["effective_to"], source=item["source"],
                snapshot_id=item["snapshot_id"], note=item["note"]))
            written += 1
        try:
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise ReplayError(
                "成员区间写入冲突（池=%s）：%s" % (pool_key, exc)) from None
    members_avg = sum(len(codes) for _, _, codes in obs) / len(obs)
    return {"pool_key": pool_key, "observations": len(obs),
            "intervals": len(intervals),
            "members_avg": round(members_avg, 1),
            "written": written, "anomalies": anomalies,
            "open_tail": open_tail,
            "first_as_of": obs[0][0].isoformat(),
            "last_as_of": obs[-1][0].isoformat()}


def rebuild_intervals(db, pools: Optional[Sequence[str]] = None,
                      write: bool = True) -> List[dict]:
    if pools is None:
        pools = [key for (key,) in
                 db.query(SelectionScheme.pool_key)
                 .filter(SelectionScheme.status == "active")
                 .distinct().all()]
    if write:
        ensure_series_pools(db, pools)
    return [rebuild_pool_intervals(db, key, write=write) for key in pools]


# ------------------------------------------------------------------ 批量
def run_batch(as_ofs: Optional[Sequence[date]] = None, *,
              publish_version: bool = True, force: bool = False) -> List[dict]:
    from core.db import SessionLocal

    results: List[dict] = []
    for as_of in (as_ofs or REPLAY_AS_OFS):
        db = SessionLocal()
        try:
            results.append(run_as_of(db, as_of,
                                     publish_version=publish_version,
                                     force=force))
        except Exception as exc:
            traceback.print_exc()
            results.append({"as_of": as_of.isoformat(),
                            "errors": ["%s: %s" % (type(exc).__name__, exc)]})
        finally:
            db.close()
    return results


def _parse_dates(value: str) -> List[date]:
    out = []
    for part in (value or "").replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        out.append(datetime.strptime(part, "%Y-%m-%d").date())
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="条件选股历史回放（PIT 重建）")
    parser.add_argument("--as-ofs", default="",
                        help="逗号分隔的决策日；缺省为 11 个季末")
    parser.add_argument("--gp-only", action="store_true",
                        help="只补 GP16/GP20/GP21 缓存")
    parser.add_argument("--no-version", action="store_true",
                        help="不交付每期版本池")
    parser.add_argument("--intervals-only", action="store_true",
                        help="只重算成员区间")
    parser.add_argument("--force", action="store_true",
                        help="已有快照也重算")
    parser.add_argument("--status", action="store_true", help="查看进度")
    args = parser.parse_args(argv)

    from core.db import SessionLocal

    as_ofs = _parse_dates(args.as_ofs) or list(REPLAY_AS_OFS)
    db = SessionLocal()
    try:
        if args.status:
            print(json.dumps(status(db), ensure_ascii=False, indent=2))
            return 0
        if args.intervals_only:
            print(json.dumps(rebuild_intervals(db), ensure_ascii=False,
                             indent=2))
            return 0
        if args.gp_only:
            for as_of in as_ofs:
                print(json.dumps(ensure_gp(db, as_of), ensure_ascii=False))
            return 0
    finally:
        db.close()

    results = run_batch(as_ofs, publish_version=not args.no_version,
                        force=args.force)
    for item in results:
        print(json.dumps(item, ensure_ascii=False))

    db = SessionLocal()
    try:
        intervals = rebuild_intervals(db)
        print(json.dumps({"intervals": intervals}, ensure_ascii=False))
        print(json.dumps({"status": status(db)}, ensure_ascii=False))
    finally:
        db.close()
    return 0 if all(not item.get("errors") for item in results) else 1


if __name__ == "__main__":
    sys.exit(main())
