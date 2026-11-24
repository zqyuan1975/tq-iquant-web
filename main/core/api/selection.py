# -*- coding: utf-8 -*-
"""条件选股 API（A1）。

只做「方案 / 版本 / 元数据 / 宽库状态」四类读接口与两类写接口；
选股执行、快照冻结、发布到股票池在后续批次（依赖 D3 字段集定稿）。
"""

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.models.selection import SelectionScheme
from core.selection import field_dict, wide_db
from core.selection.field_dict import UnregisteredField
from core.services import selection_service as svc

router = APIRouter(prefix="/api/selection", tags=["selection"])


class SchemeReq(BaseModel):
    code: str
    name: str
    pool_key: str
    description: str = ""


class SchemeUpdateReq(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None


class VersionReq(BaseModel):
    spec: dict
    note: str = ""
    uncalibrated: bool = True


def _bad(exc: Exception):
    return err(400, str(exc))


@router.get("/meta")
def selection_meta():
    """字段字典与池枚举：前端筛选面板直接消费，避免硬编码。"""
    fields = [{
        "key": s.key, "source": s.source, "column": s.column, "unit": s.unit,
        "zero_is_missing": s.zero_is_missing, "pit_safe": s.pit_safe,
        "pools": list(s.pools), "note": s.note,
    } for s in field_dict.FIELD_REGISTRY.values()]
    return ok({
        "field_set_version": field_dict.FIELD_SET_VERSION,
        "pools": list(svc.POOL_KEYS),
        "industry_prefix": svc.INDUSTRY_PREFIX,
        "fields": fields,
    })


@router.get("/wide-db/status")
def wide_db_status():
    """外部宽库探针（A0-2）：不可用只报 ready=false，不抛 500。"""
    return ok(wide_db.probe())


@router.get("/schemes")
def list_schemes(include_archived: bool = False, db: Session = Depends(get_db)):
    return ok(svc.list_schemes(db, include_archived=include_archived))


@router.post("/schemes")
def create_scheme(req: SchemeReq, db: Session = Depends(get_db)):
    try:
        scheme = svc.create_scheme(db, req.code, req.name, req.pool_key,
                                   req.description)
    except (svc.SpecError, UnregisteredField) as exc:
        return _bad(exc)
    except IntegrityError:
        db.rollback()
        return err(409, "方案 code 已存在：%s" % req.code)
    return ok({"id": scheme.id, "code": scheme.code, "name": scheme.name,
               "pool_key": scheme.pool_key, "status": scheme.status})


@router.post("/schemes/seed")
def seed_schemes(db: Session = Depends(get_db)):
    """幂等种子：四风格池各建一个空方案（不含版本）。"""
    return ok({"created": svc.seed_default_schemes(db)})


@router.get("/schemes/{scheme_id}")
def get_scheme(scheme_id: int, db: Session = Depends(get_db)):
    scheme = db.get(SelectionScheme, scheme_id)
    if scheme is None:
        return err(404, "方案不存在：%s" % scheme_id)
    versions = svc.list_versions(db, scheme_id)
    return ok({
        "id": scheme.id, "code": scheme.code, "name": scheme.name,
        "pool_key": scheme.pool_key, "description": scheme.description,
        "status": scheme.status,
        "versions": [svc.version_payload(v) for v in versions],
    })


@router.put("/schemes/{scheme_id}")
def update_scheme(scheme_id: int, req: SchemeUpdateReq,
                  db: Session = Depends(get_db)):
    try:
        scheme = svc.update_scheme(
            db, scheme_id,
            name=req.name, description=req.description, status=req.status)
    except LookupError as exc:
        return err(404, str(exc))
    except svc.SpecError as exc:
        return _bad(exc)
    return ok({"id": scheme.id, "name": scheme.name,
               "pool_key": scheme.pool_key, "status": scheme.status})


@router.get("/schemes/{scheme_id}/versions")
def list_versions(scheme_id: int, db: Session = Depends(get_db)):
    return ok([svc.version_payload(v) for v in svc.list_versions(db, scheme_id)])


@router.post("/schemes/{scheme_id}/versions")
def create_version(scheme_id: int, req: VersionReq,
                   db: Session = Depends(get_db)):
    try:
        version = svc.create_version(db, scheme_id, req.spec,
                                     note=req.note,
                                     uncalibrated=req.uncalibrated)
    except LookupError as exc:
        return err(404, str(exc))
    except (svc.SpecError, UnregisteredField) as exc:
        return _bad(exc)
    return ok(svc.version_payload(version))


@router.post("/schemes/{scheme_id}/versions/{version_id}/activate")
def activate_version(scheme_id: int, version_id: int,
                     db: Session = Depends(get_db)):
    try:
        version = svc.activate_version(db, scheme_id, version_id)
    except LookupError as exc:
        return err(404, str(exc))
    return ok(svc.version_payload(version))


# --- GP16 历史总市值（D2 口径）-------------------------------------------

class GpIngestReq(BaseModel):
    day: date
    codes: Optional[List[str]] = None   # 省略 = 全市场（TQ market=5）


@router.post("/gp/ingest")
def gp_ingest(req: GpIngestReq, db: Session = Depends(get_db)):
    """按日取 GP16 并落库；P2 优先级，分片在 TQData 内完成。"""
    from core.services import selection_gp as gp
    from core.tq.data import TQData

    codes = req.codes
    if not codes:
        codes = TQData().get_all_stocks(market="5")
    try:
        result = gp.ingest_gp16(db, codes, req.day, priority="P2")
    except Exception as exc:  # TQ 不可用/分片失败：如实报错，不落半批
        db.rollback()
        return err(502, "GP16 取数失败：%s" % exc)
    return ok(result)


@router.get("/gp/snapshots")
def gp_snapshots(start: Optional[date] = None, end: Optional[date] = None,
                 codes: Optional[str] = None, db: Session = Depends(get_db)):
    from core.services import selection_gp as gp

    code_list = [c for c in (codes or "").split(",") if c.strip()] or None
    return ok(gp.list_gp16(db, start=start, end=end, codes=code_list))


@router.get("/gp/audit")
def gp_audit(day: date = Query(...), threshold_pct: float = 5.0,
            db: Session = Depends(get_db)):
    """缺口分类（停牌/真缺失）+ 除权断点核对；只报告，不改数。"""
    from core.services import selection_gp as gp

    return ok(gp.audit_day(db, day, threshold_pct=threshold_pct))
