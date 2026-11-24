# -*- coding: utf-8 -*-
"""条件选股运行 / 快照 / 冻结 / 发布到股票池。

链路：启用版本 → 引擎计算（只读导入仓）→ SelectionRun + SelectionSnapshot
（明细逐票留行）→ 冻结（不可再变）→ 发布为平台股票池（当前成分 + PIT 成员区间）。
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from sqlalchemy import func

from core.models.selection import (
    SelectionFactorValidation, SelectionRun, SelectionScheme,
    SelectionSchemeVersion, SelectionSnapshot, SelectionSnapshotStock,
    SelectionWarehouseDividend, SelectionWarehouseFin, SelectionWarehouseMeta,
    SelectionWarehouseStock,
)
from core.models.stock_pool import StockPool
from core.models.stock_pool_membership import StockPoolMembership
from core.models.stock_pool_stock import StockPoolStock
from core.selection import engine
from core.selection.field_dict import FIELD_SET_VERSION
from core.services import selection_service


class RunError(ValueError):
    """运行/快照操作不合法（API 层转 400）。"""


def _now() -> datetime:
    return datetime.now()


def _active_version(db, scheme: SelectionScheme) -> Optional[SelectionSchemeVersion]:
    return (db.query(SelectionSchemeVersion)
            .filter_by(scheme_id=scheme.id, status="active")
            .order_by(SelectionSchemeVersion.version.desc())
            .first())


def _version_scheme(db, version_id: int):
    version = db.get(SelectionSchemeVersion, version_id)
    scheme = db.get(SelectionScheme, version.scheme_id) if version else None
    return version, scheme


def serialize_run(db, run: SelectionRun,
                  snapshot: Optional[SelectionSnapshot] = None) -> Dict[str, Any]:
    version, scheme = _version_scheme(db, run.scheme_version_id)
    return {
        "id": run.id,
        "scheme_version_id": run.scheme_version_id,
        "scheme_id": scheme.id if scheme else None,
        "scheme_code": scheme.code if scheme else None,
        "scheme_name": scheme.name if scheme else None,
        "pool_key": scheme.pool_key if scheme else None,
        "version": version.version if version else None,
        "as_of": run.as_of.isoformat() if run.as_of else None,
        "status": run.status,
        "stock_count": run.stock_count or 0,
        "params_hash": run.params_hash,
        "error": run.error,
        "started_at": run.started_at.isoformat(timespec="seconds") if run.started_at else None,
        "finished_at": run.finished_at.isoformat(timespec="seconds") if run.finished_at else None,
        "created_at": run.created_at.isoformat(timespec="seconds") if run.created_at else None,
        "snapshot_id": snapshot.id if snapshot else None,
        "frozen": bool(snapshot.frozen) if snapshot else False,
        "published_pool_id": snapshot.published_pool_id if snapshot else None,
    }


def serialize_snapshot(db, snap: SelectionSnapshot,
                       run: Optional[SelectionRun] = None) -> Dict[str, Any]:
    run = run or db.get(SelectionRun, snap.run_id)
    version, scheme = _version_scheme(db, snap.scheme_version_id)
    stats = json.loads(snap.stats_json) if snap.stats_json else {}
    quality = json.loads(snap.quality_json) if snap.quality_json else {}
    pool = db.get(StockPool, snap.published_pool_id) if snap.published_pool_id else None
    return {
        "id": snap.id,
        "run_id": snap.run_id,
        "scheme_version_id": snap.scheme_version_id,
        "scheme_id": scheme.id if scheme else None,
        "scheme_code": scheme.code if scheme else None,
        "scheme_name": scheme.name if scheme else None,
        "pool_key": scheme.pool_key if scheme else None,
        "version": version.version if version else None,
        "as_of": snap.as_of.isoformat() if snap.as_of else None,
        "fingerprint": snap.fingerprint,
        "field_set_version": snap.field_set_version,
        "spec_hash": snap.spec_hash,
        "config_fingerprint": snap.config_fingerprint,
        "frozen": bool(snap.frozen),
        "published_pool_id": snap.published_pool_id,
        "published_pool_code": pool.code if pool else None,
        "published_pool_name": pool.name if pool else None,
        "stock_count": run.stock_count if run else None,
        "stats": stats,
        "quality": quality,
        "created_at": snap.created_at.isoformat(timespec="seconds") if snap.created_at else None,
    }


def run_schemes(db, scheme_ids: Optional[List[int]] = None,
                as_of: Optional[date] = None,
                note: str = "") -> Dict[str, Any]:
    """对指定方案（默认全部启用方案）跑一次选股并落库。"""
    meta = engine.warehouse_meta(db)
    target_as_of = as_of or (meta.as_of if meta else None)
    if target_as_of is None:
        raise RunError("尚未导入宽库，无法运行")

    query = db.query(SelectionScheme)
    if scheme_ids:
        query = query.filter(SelectionScheme.id.in_(scheme_ids))
    else:
        query = query.filter(SelectionScheme.status == "active")
    schemes = query.order_by(SelectionScheme.id).all()
    if not schemes:
        raise RunError("没有可运行的方案")

    results = []
    skipped: List[Dict[str, Any]] = []
    for scheme in schemes:
        version = _active_version(db, scheme)
        started = _now()
        if version is None:
            skipped.append({
                "scheme_id": scheme.id, "scheme_code": scheme.code,
                "scheme_name": scheme.name, "pool_key": scheme.pool_key,
                "status": "skipped", "error": "方案没有启用版本",
                "as_of": target_as_of.isoformat(),
            })
            continue

        spec = json.loads(version.spec_json)
        wh_hash = meta.imported_at.isoformat() if meta and meta.imported_at else ""
        params_hash = hashlib.sha256(
            ("%s|%s|%s" % (version.spec_hash, target_as_of, wh_hash)).encode()
        ).hexdigest()[:16]
        run = SelectionRun(
            scheme_version_id=version.id, as_of=target_as_of, status="running",
            params_hash=params_hash, started_at=started, stock_count=0)
        db.add(run)
        db.commit()
        db.refresh(run)

        try:
            result = engine.compute(db, spec, target_as_of)
        except engine.EngineError as exc:
            run.status = "skipped"
            run.error = str(exc)
            run.finished_at = _now()
            db.commit()
            results.append(serialize_run(db, run))
            continue
        except Exception as exc:  # pragma: no cover - 防御性
            db.rollback()
            run = db.get(SelectionRun, run.id)
            run.status = "failed"
            run.error = "计算失败：%s" % exc
            run.finished_at = _now()
            db.commit()
            results.append(serialize_run(db, run))
            continue

        stats = result["stats"]
        # 把「本轮用到的因子」的稳定性验证明细一并落到质量披露：说明每个权重
        # 是否有历史证据支撑，避免只给分数不给依据。
        used_factors = set(spec.get("weights") or {}) | set(
            spec.get("weights_hist") or {})
        validation = factor_validation(db)
        if validation.get("available"):
            result["quality"]["factor_validation"] = {
                "as_of": validation.get("as_of"),
                "n_periods": validation.get("n_periods"),
                "horizon_days": validation.get("horizon_days"),
                "factors": {
                    key: value for key, value in
                    (validation.get("factors") or {}).items()
                    if key in used_factors},
                "unvalidated_used": sorted(
                    key for key in used_factors
                    if key in (validation.get("unvalidated") or {})),
            }
        else:
            result["quality"]["factor_validation"] = {
                "available": False, "reason": validation.get("reason")}
        selected = [row for row in result["rows"] if row["selected"]]
        fingerprint = hashlib.sha256(
            ("%s|%s|%s|%s|%s" % (target_as_of, version.spec_hash,
                                 version.field_fingerprint, wh_hash,
                                 FIELD_SET_VERSION)).encode()
        ).hexdigest()[:16]
        snap = SelectionSnapshot(
            run_id=run.id, scheme_version_id=version.id, as_of=target_as_of,
            fingerprint=fingerprint, field_set_version=FIELD_SET_VERSION,
            spec_hash=version.spec_hash,
            config_fingerprint="%s|%s" % (version.spec_hash, wh_hash),
            stats_json=json.dumps(stats, ensure_ascii=False),
            quality_json=json.dumps(result["quality"], ensure_ascii=False),
            frozen=False)
        db.add(snap)
        db.flush()
        for row in result["rows"]:
            db.add(SelectionSnapshotStock(
                snapshot_id=snap.id, code=row["code"], rank=row.get("rank"),
                score=row.get("score"), selected=bool(row["selected"]),
                pool_key=result["pool_key"],
                exclude_reason=row.get("exclude_reason") or row.get("gate"),
                factor_json=json.dumps(row, ensure_ascii=False),
            ))
        run.status = "succeeded"
        run.stock_count = len(selected)
        run.finished_at = _now()
        db.commit()
        results.append(serialize_run(db, run, snap))

    return {"as_of": target_as_of.isoformat(),
            "warehouse_as_of": meta.as_of.isoformat() if meta else None,
            "runs": results, "skipped": skipped}


def run_detail(db, run_id: int, include_stocks: bool = True,
               selected_only: bool = False,
               limit: int = 500) -> Dict[str, Any]:
    run = db.get(SelectionRun, run_id)
    if run is None:
        raise LookupError("运行记录不存在：%s" % run_id)
    snap = (db.query(SelectionSnapshot)
            .filter_by(run_id=run.id).first())
    data = serialize_run(db, run, snap)
    if snap is not None:
        data["stats"] = json.loads(snap.stats_json) if snap.stats_json else {}
        data["quality"] = json.loads(snap.quality_json) if snap.quality_json else {}
        if include_stocks:
            detail = snapshot_detail(db, snap.id, include_stocks=True,
                                     selected_only=selected_only, limit=limit)
            data["stocks"] = detail.get("stocks", [])
    return data


def list_runs(db, limit: int = 50,
              scheme_id: Optional[int] = None) -> List[Dict[str, Any]]:
    q = db.query(SelectionRun, SelectionSnapshot).outerjoin(
        SelectionSnapshot, SelectionSnapshot.run_id == SelectionRun.id)
    if scheme_id:
        version_ids = [v.id for v in db.query(SelectionSchemeVersion)
                       .filter_by(scheme_id=scheme_id).all()]
        q = q.filter(SelectionRun.scheme_version_id.in_(version_ids or [-1]))
    pairs = q.order_by(SelectionRun.id.desc()).limit(limit).all()
    return [serialize_run(db, run, snap) for run, snap in pairs]


def _stock_payload(row: SelectionSnapshotStock) -> Dict[str, Any]:
    try:
        detail = json.loads(row.factor_json) if row.factor_json else {}
    except Exception:
        detail = {}
    return {
        "code": row.code,
        "rank": row.rank,
        "score": float(row.score) if row.score is not None else None,
        "selected": bool(row.selected),
        "pool_key": row.pool_key,
        "exclude_reason": row.exclude_reason,
        "name": detail.get("name"),
        "sector": detail.get("sector"),
        "track": detail.get("track"),
        "coverage": detail.get("coverage"),
        "factors": detail.get("factors") or {},
        "pcts": detail.get("pcts") or {},
        "raw_pcts": detail.get("raw_pcts") or {},
        "explain": detail.get("explain") or [],
    }


def snapshot_detail(db, snapshot_id: int, include_stocks: bool = True,
                    selected_only: bool = False,
                    limit: int = 500) -> Dict[str, Any]:
    snap = db.get(SelectionSnapshot, snapshot_id)
    if snap is None:
        raise LookupError("快照不存在：%s" % snapshot_id)
    data = serialize_snapshot(db, snap)
    if include_stocks:
        q = db.query(SelectionSnapshotStock).filter_by(snapshot_id=snapshot_id)
        if selected_only:
            q = q.filter(SelectionSnapshotStock.selected.is_(True))
        q = q.order_by(SelectionSnapshotStock.selected.desc(),
                       SelectionSnapshotStock.rank.is_(None),
                       SelectionSnapshotStock.rank.asc(),
                       SelectionSnapshotStock.score.desc())
        rows = q.limit(max(1, min(limit, 5000))).all()
        data["stocks"] = [_stock_payload(r) for r in rows]
    return data


def list_snapshots(db, limit: int = 50) -> List[Dict[str, Any]]:
    snaps = (db.query(SelectionSnapshot)
             .order_by(SelectionSnapshot.id.desc()).limit(limit).all())
    out = []
    for snap in snaps:
        run = db.get(SelectionRun, snap.run_id)
        out.append(serialize_snapshot(db, snap, run))
    return out


def freeze(db, snapshot_id: int) -> Dict[str, Any]:
    snap = db.get(SelectionSnapshot, snapshot_id)
    if snap is None:
        raise LookupError("快照不存在：%s" % snapshot_id)
    if not snap.frozen:
        snap.frozen = True
        db.commit()
        db.refresh(snap)
    return serialize_snapshot(db, snap)


def publish(db, snapshot_id: int, code: Optional[str] = None,
            name: Optional[str] = None,
            series: bool = False) -> Dict[str, Any]:
    """发布快照为平台股票池。

    两种池（都不改表结构，均为 StockPool 行）：
    * ``series=False``（默认）：**版本池** ``SEL_{POOL}_{YYYYMMDD}``——成分固定为
      本快照命中集，实盘会话挂它即「挂池版本」，盘中不会漂移；
    * ``series=True``：**序列池** ``SEL_{POOL}``——同一风格跨日累积 PIT 成员区间
      （新进开区间、移出关区间），回测选它即可按日期动态换池。
    """
    snap = db.get(SelectionSnapshot, snapshot_id)
    if snap is None:
        raise LookupError("快照不存在：%s" % snapshot_id)
    if not snap.frozen:
        raise RunError("快照未冻结，先冻结再发布")
    version, scheme = _version_scheme(db, snap.scheme_version_id)
    pool_key = scheme.pool_key if scheme else "selection"
    if series:
        pool_code = (code or "SEL_%s" % pool_key.upper()).strip()
        pool_name = (name or "%s选股序列" % (
            scheme.name if scheme else "条件")).strip()
    else:
        pool_code = (code or "SEL_%s_%s" % (
            pool_key.upper(), snap.as_of.strftime("%Y%m%d"))).strip()
        pool_name = (name or "%s选股 %s" % (
            scheme.name if scheme else "条件", snap.as_of.isoformat())).strip()

    selected = (db.query(SelectionSnapshotStock)
                .filter_by(snapshot_id=snap.id, selected=True)
                .order_by(SelectionSnapshotStock.rank.asc()).all())
    if not selected:
        raise RunError("该快照没有命中股票，无法发布")

    pool = db.query(StockPool).filter_by(code=pool_code).first()
    if pool is None:
        pool = StockPool(code=pool_code, name=pool_name)
        db.add(pool)
        db.flush()
    else:
        pool.name = pool_name
        # fetch：把已删除的旧成分从 identity map 里同步移除，避免重新插入
        # 同 PK 行时触发 identity map 冲突（SQLite 会复用自增 id）。
        db.query(StockPoolStock).filter_by(pool_id=pool.id).delete(
            synchronize_session="fetch")
        db.flush()
    pool.synced_at = _now()

    codes = [row.code for row in selected]
    for row in selected:
        try:
            detail = json.loads(row.factor_json) if row.factor_json else {}
        except Exception:
            detail = {}
        db.add(StockPoolStock(pool_id=pool.id, stock_code=row.code,
                              stock_name=detail.get("name")))

    # PIT 成员区间：先关闭本次未再命中的开口区间，再为新增标的开区间
    open_rows = (db.query(StockPoolMembership)
                 .filter_by(pool_id=pool.id)
                 .filter(StockPoolMembership.effective_to.is_(None)).all())
    wanted = set(codes)
    for member in open_rows:
        if member.stock_code not in wanted:
            member.effective_to = snap.as_of - timedelta(days=1)
    existing = {
        (m.stock_code, m.effective_from)
        for m in db.query(StockPoolMembership)
        .filter(StockPoolMembership.pool_id == pool.id,
                StockPoolMembership.stock_code.in_(codes)).all()
    } if codes else set()
    for code in codes:
        if (code, snap.as_of) in existing:
            continue
        db.add(StockPoolMembership(
            pool_id=pool.id, stock_code=code,
            effective_from=snap.as_of,
            note="selection snapshot #%s" % snap.id))

    snap.published_pool_id = pool.id
    db.commit()
    db.refresh(snap)
    return {
        "snapshot_id": snap.id,
        "pool_id": pool.id,
        "pool_code": pool.code,
        "pool_name": pool.name,
        "stock_count": len(codes),
        "kind": "series" if series else "version",
    }


def deliver(db, as_of: Optional[date] = None) -> Dict[str, Any]:
    """T 盘后一次交付：运行 → 冻结 → 发版本池 → 发序列池。

    顺序刻意「先序列、后版本」：``SelectionSnapshot.published_pool_id`` 最终指向
    版本池（实盘挂接对象），序列池只供回测按日期取成分。
    """
    result = run_schemes(db, as_of=as_of)
    delivered = []
    for run in result["runs"]:
        if run["status"] != "succeeded" or not run["snapshot_id"]:
            continue
        snap_id = run["snapshot_id"]
        freeze(db, snap_id)
        series_pub = publish(db, snap_id, series=True)
        version_pub = publish(db, snap_id)
        delivered.append({
            "run_id": run["id"],
            "snapshot_id": snap_id,
            "pool_key": run["pool_key"],
            "scheme_name": run["scheme_name"],
            "stock_count": run["stock_count"],
            "version_pool": version_pub,
            "series_pool": series_pub,
        })
    return {"as_of": result["as_of"], "runs": result["runs"],
            "skipped": result["skipped"], "delivered": delivered}


def warehouse_status(db) -> Dict[str, Any]:
    meta = db.query(SelectionWarehouseMeta).first()
    wide = None
    try:
        from core.selection import wide_db
        wide = wide_db.probe()
    except Exception as exc:  # pragma: no cover - 只读探针失败不阻塞
        wide = {"ready": False, "error": str(exc)}
    if meta is None:
        return {"ready": False, "message": "尚未导入宽库", "wide_db": wide}
    counts = {}
    if meta.counts_json:
        try:
            counts = json.loads(meta.counts_json)
        except Exception:
            counts = {}
    stock_rows = db.query(func.count(SelectionWarehouseStock.code)).scalar() or 0
    fin_rows = db.query(func.count(SelectionWarehouseFin.code)).scalar() or 0
    div_rows = db.query(func.count(SelectionWarehouseDividend.code)).scalar() or 0
    stale_days = (date.today() - meta.as_of).days
    return {
        "ready": True,
        "as_of": meta.as_of.isoformat(),
        "imported_at": meta.imported_at.isoformat(timespec="seconds")
        if meta.imported_at else None,
        "source_path": meta.source_path,
        "source_size_mb": float(meta.source_size_mb) if meta.source_size_mb else None,
        "stale_days": stale_days,
        "stock_count": stock_rows,
        "fin_count": fin_rows,
        "dividend_count": div_rows,
        "counts": counts,
        "wide_db": wide,
    }


def factor_validation(db) -> Dict[str, Any]:
    """因子稳定性摘要（导入时回算并冻结）。"""
    row = (db.query(SelectionFactorValidation)
           .order_by(SelectionFactorValidation.id.desc()).first())
    if row is None:
        return {"available": False,
                "reason": "尚未导入宽库或导入时未计算验证"}
    try:
        payload = json.loads(row.payload_json)
    except Exception:
        payload = {"available": False, "reason": "验证摘要损坏"}
    payload.setdefault("available", True)
    payload["as_of"] = row.as_of.isoformat()
    payload["horizon_days"] = row.horizon_days
    payload["step_days"] = row.step_days
    payload["n_periods"] = row.n_periods
    payload["computed_at"] = (row.computed_at.isoformat(timespec="seconds")
                              if row.computed_at else None)
    return payload
