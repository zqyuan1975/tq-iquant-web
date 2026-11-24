# -*- coding: utf-8 -*-
"""内置宽库构建（阶段二）· 落库层。

职责边界：

* **只写**：``selection_wh_stocks/fin/dividend/daily``、``selection_wh_meta``、
  ``selection_wh_build_progress``；
* **不做取数**（口径层负责），**不做阶段判断**（编排层负责）；
* 每个批次一个事务：数据行 + progress 行一起提交，失败批次整体回滚；
* 快照可见性只由 ``selection_wh_meta.as_of`` 的最终切换决定——在切换事务
  提交前，已有 as_of 的读者不会看到新批次（新增行只补缺口/追加未来日线，
  不改写旧 as_of 已覆盖代码的既有值）。
"""
from __future__ import annotations

import json
import zlib
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

from sqlalchemy import func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from core.models.selection import (
    SelectionSectorMembership,
    SelectionWarehouseBuildProgress,
    SelectionWarehouseDaily,
    SelectionWarehouseDividend,
    SelectionWarehouseFin,
    SelectionWarehouseMeta,
    SelectionWarehouseStock,
)

META_ID = 1
BATCH_PROGRESS = 300
BATCH_ROWS = 300


def encode_json(obj: Any) -> bytes:
    return zlib.compress(
        json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def decode_json(blob: Optional[bytes]) -> Any:
    if not blob:
        return None
    try:
        return json.loads(zlib.decompress(blob).decode("utf-8"))
    except (zlib.error, ValueError, UnicodeDecodeError):
        return None


def _chunks(seq: Sequence[Any], size: int) -> Iterable[Sequence[Any]]:
    for i in range(0, len(seq), size):
        yield seq[i:i + size]


# ------------------------------------------------------------------ progress
def record_progress(db, *, run_id: str, mode: str, phase: str, batch_key: str,
                    status: str, total: Optional[int] = None,
                    done: Optional[int] = None, error: Optional[str] = None,
                    payload: Optional[Dict[str, Any]] = None) -> None:
    """upsert 批次进度（与数据同事务；调用方负责 commit）。"""
    row = (db.query(SelectionWarehouseBuildProgress)
           .filter_by(run_id=run_id, phase=phase, batch_key=batch_key).first())
    now = datetime.now()
    if row is None:
        row = SelectionWarehouseBuildProgress(
            run_id=run_id, mode=mode, phase=phase, batch_key=batch_key,
            started_at=now)
        db.add(row)
    row.status = status
    row.total = total if total is not None else row.total
    row.done = done if done is not None else row.done
    row.error = (error or "")[:500] or None
    if payload is not None:
        row.payload_json = json.dumps(payload, ensure_ascii=False)
    row.updated_at = now
    db.flush()


def done_batches(db, run_id: str) -> set:
    rows = (db.query(SelectionWarehouseBuildProgress.phase,
                     SelectionWarehouseBuildProgress.batch_key)
            .filter(SelectionWarehouseBuildProgress.run_id == run_id,
                    SelectionWarehouseBuildProgress.status == "done").all())
    return {(phase, key) for phase, key in rows}


def plan_batches(db, *, run_id: str, mode: str, phase: str,
                 batches: Sequence[tuple]) -> None:
    """预登记全部批次为 pending，让状态接口能显示「已完成/总批次」。

    已 done 的批次保持不动（续跑不重做）；failed 批次重置为 pending 以便重试。
    """
    keys = {(phase, key) for key, _ in batches}
    existing = {
        row.batch_key: row
        for row in (db.query(SelectionWarehouseBuildProgress)
                    .filter(SelectionWarehouseBuildProgress.run_id == run_id,
                            SelectionWarehouseBuildProgress.phase == phase).all())
    }
    for key, total in batches:
        row = existing.get(key)
        if row is not None and row.status == "done":
            continue
        record_progress(db, run_id=run_id, mode=mode, phase=phase,
                        batch_key=key, status="pending", total=total, done=0)
    # 清理本次计划之外的历史批次（例如宇宙变化导致分片数减少）
    for key, row in existing.items():
        if (phase, key) not in keys and row.status != "done":
            db.delete(row)


def latest_run_id(db) -> Optional[str]:
    row = (db.query(SelectionWarehouseBuildProgress.run_id)
           .order_by(SelectionWarehouseBuildProgress.updated_at.desc()).first())
    return row[0] if row else None


def run_progress(db, run_id: str) -> List[SelectionWarehouseBuildProgress]:
    return (db.query(SelectionWarehouseBuildProgress)
            .filter(SelectionWarehouseBuildProgress.run_id == run_id)
            .order_by(SelectionWarehouseBuildProgress.phase,
                      SelectionWarehouseBuildProgress.batch_key).all())


# ------------------------------------------------------------------ existing
def existing_codes(db) -> set:
    return {code for (code,) in db.query(SelectionWarehouseStock.code).all()}


def fin_codes(db) -> set:
    return {code for (code,) in db.query(SelectionWarehouseFin.code).all()}


def dividend_codes(db) -> set:
    return {code for (code,) in db.query(SelectionWarehouseDividend.code).all()}


def daily_watermarks(db) -> Dict[str, date]:
    rows = (db.query(SelectionWarehouseDaily.code,
                     func.max(SelectionWarehouseDaily.trade_date))
            .group_by(SelectionWarehouseDaily.code).all())
    out: Dict[str, date] = {}
    for code, last in rows:
        if last is not None:
            out[code] = last if isinstance(last, date) else date.fromisoformat(str(last))
    return out


def daily_bounds(db) -> Dict[str, Any]:
    row = (db.query(func.min(SelectionWarehouseDaily.trade_date),
                    func.max(SelectionWarehouseDaily.trade_date),
                    func.count()).first())
    return {
        "min_date": row[0].isoformat() if row and row[0] else None,
        "max_date": row[1].isoformat() if row and row[1] else None,
        "rows": int(row[2] or 0) if row else 0,
    }


def table_counts(db) -> Dict[str, int]:
    return {
        "stock_count": db.query(SelectionWarehouseStock).count(),
        "fin_count": db.query(SelectionWarehouseFin).count(),
        "dividend_count": db.query(SelectionWarehouseDividend).count(),
        "daily_count": db.query(SelectionWarehouseDaily).count(),
    }


# ------------------------------------------------------------------ upserts
def upsert_stocks(db, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    added = 0
    for batch in _chunks(rows, 200):
        payload = []
        for row in batch:
            payload.append({
                "code": row["code"],
                "name": row.get("name"),
                "exchange": row.get("exchange"),
                "sector_code": row.get("sector_code"),
                "sector_name": row.get("sector_name"),
                "is_financial": bool(row.get("is_financial", False)),
                "more_blob": encode_json(row["more"]) if row.get("more") else None,
                "info_blob": encode_json(row["info"]) if row.get("info") else None,
                "go_blob": encode_json(row["go"]) if row.get("go") else None,
                "gp_blob": encode_json(row["gp"]) if row.get("gp") else None,
                "fin_periods": int(row.get("fin_periods", 0) or 0),
                "fin_last_tag": row.get("fin_last_tag"),
            })
        stmt = sqlite_insert(SelectionWarehouseStock).values(payload)
        stmt = stmt.on_conflict_do_update(
            index_elements=["code"],
            set_={
                "name": func.coalesce(stmt.excluded.name,
                                      SelectionWarehouseStock.name),
                "exchange": func.coalesce(stmt.excluded.exchange,
                                          SelectionWarehouseStock.exchange),
                "sector_code": func.coalesce(stmt.excluded.sector_code,
                                             SelectionWarehouseStock.sector_code),
                "sector_name": func.coalesce(stmt.excluded.sector_name,
                                             SelectionWarehouseStock.sector_name),
                "is_financial": stmt.excluded.is_financial,
                "more_blob": func.coalesce(stmt.excluded.more_blob,
                                           SelectionWarehouseStock.more_blob),
                "info_blob": func.coalesce(stmt.excluded.info_blob,
                                           SelectionWarehouseStock.info_blob),
                "go_blob": func.coalesce(stmt.excluded.go_blob,
                                         SelectionWarehouseStock.go_blob),
                "gp_blob": func.coalesce(stmt.excluded.gp_blob,
                                         SelectionWarehouseStock.gp_blob),
            })
        db.execute(stmt)
        added += len(payload)
    return added


def upsert_fin(db, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    for batch in _chunks(rows, BATCH_ROWS):
        payload = [{
            "code": row["code"],
            "n_periods": int(row["n_periods"]),
            "last_tag_time": row.get("last_tag_time"),
            "last_announce_time": row.get("last_announce_time"),
            "series_blob": encode_json(row["series"]),
        } for row in batch]
        stmt = sqlite_insert(SelectionWarehouseFin).values(payload)
        stmt = stmt.on_conflict_do_update(
            index_elements=["code"],
            set_={
                "n_periods": stmt.excluded.n_periods,
                "last_tag_time": stmt.excluded.last_tag_time,
                "last_announce_time": stmt.excluded.last_announce_time,
                "series_blob": stmt.excluded.series_blob,
            })
        db.execute(stmt)
    return len(rows)


def upsert_dividend(db, rows: Sequence[Dict[str, Any]]) -> int:
    if not rows:
        return 0
    for batch in _chunks(rows, BATCH_ROWS):
        payload = [{
            "code": row["code"],
            "n_events": int(row["n_events"]),
            "last_ex_date": row.get("last_ex_date"),
            "rows_blob": encode_json(row["rows"]),
        } for row in batch]
        stmt = sqlite_insert(SelectionWarehouseDividend).values(payload)
        stmt = stmt.on_conflict_do_update(
            index_elements=["code"],
            set_={
                "n_events": stmt.excluded.n_events,
                "last_ex_date": stmt.excluded.last_ex_date,
                "rows_blob": stmt.excluded.rows_blob,
            })
        db.execute(stmt)
    return len(rows)


def insert_daily(db, rows: Sequence[Dict[str, Any]]) -> int:
    """按 (code, trade_date) 幂等插入；重复行忽略，返回**新增**行数。"""
    if not rows:
        return 0
    added = 0
    for batch in _chunks(rows, BATCH_ROWS):
        payload = [{
            "code": row["code"],
            "trade_date": row["trade_date"],
            "open": row.get("open"),
            "high": row.get("high"),
            "low": row.get("low"),
            "close": row["close"],
            "volume": row.get("volume"),
            "amount": row.get("amount"),
            "source": row.get("source", "tq:history"),
        } for row in batch]
        stmt = (sqlite_insert(SelectionWarehouseDaily).values(payload)
                .on_conflict_do_nothing(index_elements=["code", "trade_date"]))
        result = db.execute(stmt)
        added += max(int(result.rowcount or 0), 0)
    return added


# ------------------------------------------------------------------ sector
def refresh_sector_membership(db, as_of: date) -> int:
    """按当前股票行业名刷新 ``as_of`` 归属（只补缺失，幂等）。"""
    from core.selection import sector_pools

    return sector_pools.refresh_membership(db, as_of)


# ------------------------------------------------------------------ metrics
def recompute_metrics(db, as_of: date) -> int:
    """用 ``selection_wh_daily`` 回填每票 close / 波动率 / 近 252 日最大回撤。

    只更新有日线数据的票；日线不足 2 根时只写 close。波动率口径与导入仓
    一致：近 252 个交易日收益率标准差 × sqrt(252) × 100。
    """
    import math

    codes = [code for (code,) in db.query(SelectionWarehouseStock.code)
             .order_by(SelectionWarehouseStock.code).all()]
    updated = 0
    for batch in _chunks(codes, 300):
        rows = (db.query(SelectionWarehouseDaily.code,
                         SelectionWarehouseDaily.close)
                .filter(SelectionWarehouseDaily.code.in_(batch),
                        SelectionWarehouseDaily.trade_date <= as_of)
                .order_by(SelectionWarehouseDaily.code,
                          SelectionWarehouseDaily.trade_date).all())
        by_code: Dict[str, List[float]] = {}
        for code, close in rows:
            if close is None:
                continue
            by_code.setdefault(code, []).append(float(close))
        for code, closes in by_code.items():
            window = closes[-252:]
            latest = window[-1]
            volatility = None
            drawdown = None
            if len(window) >= 2:
                returns = [
                    window[i] / window[i - 1] - 1.0
                    for i in range(1, len(window)) if window[i - 1]
                ]
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
            db.execute(update(SelectionWarehouseStock)
                       .where(SelectionWarehouseStock.code == code)
                       .values(close=latest, volatility_pct=volatility,
                               max_drawdown_pct=drawdown))
            updated += 1
    return updated


# ------------------------------------------------------------------ publish
def publish_meta(db, *, as_of: date, source_path: str,
                 counts: Dict[str, Any]) -> None:
    """单事务切换 as_of：调用方 commit 后新快照才对读者可见。"""
    meta = db.get(SelectionWarehouseMeta, META_ID)
    now = datetime.now()
    payload = {
        "builtin": True,
        "daily": daily_bounds(db),
        "generated_at": now.isoformat(timespec="seconds"),
        **counts,
    }
    if meta is None:
        meta = SelectionWarehouseMeta(id=META_ID, as_of=as_of,
                                      source_path=source_path, imported_at=now)
        db.add(meta)
    meta.as_of = as_of
    meta.source_path = source_path
    meta.imported_at = now
    meta.stock_count = int(counts.get("stock_count", 0))
    meta.fin_count = int(counts.get("fin_count", 0))
    meta.dividend_count = int(counts.get("dividend_count", 0))
    meta.counts_json = json.dumps(payload, ensure_ascii=False)
    db.flush()


def meta_row(db) -> Optional[SelectionWarehouseMeta]:
    return db.get(SelectionWarehouseMeta, META_ID)
