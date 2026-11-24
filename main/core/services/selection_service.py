# -*- coding: utf-8 -*-
"""条件选股方案与版本服务（A1）。

四条不可妥协的规则：
1. 方案可改名/停用，**版本不可变**——改参数只能新建版本，历史版本永不改写；
2. 权重必须非负且和为 1（±1e-6），**不做自动归一**（静默改参数会让指纹失真）；
3. 字段必须已在 ``field_dict`` 登记，未登记一律拒绝；
4. 新版本默认 ``uncalibrated=True``——经验权重未经回测，前端必须显示「未校准」。
"""

from __future__ import annotations

import hashlib
import json
from typing import Dict, List, Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from core.models.selection import SelectionScheme, SelectionSchemeVersion
from core.selection.field_dict import (
    FIELD_SET_VERSION, field_set_fingerprint, get_field)

POOL_KEYS = ("growth", "quality", "value", "dividend")
INDUSTRY_PREFIX = "industry:"
WEIGHT_TOLERANCE = 1e-6
SCHEME_STATUSES = ("active", "archived")
VERSION_STATUSES = ("draft", "active", "archived")


class SpecError(ValueError):
    """方案参数不合法（API 层转 400）。"""


def _dump(spec: dict) -> str:
    return json.dumps(spec, ensure_ascii=False, sort_keys=True)


def spec_fingerprint(spec: dict) -> str:
    return hashlib.sha256(_dump(spec).encode("utf-8")).hexdigest()[:16]


def validate_spec(spec: dict) -> dict:
    """校验参数树；只校验不改写，返回原对象。"""
    if not isinstance(spec, dict):
        raise SpecError("spec 必须是对象")
    pool = str(spec.get("pool_key") or "").strip()
    if not (pool in POOL_KEYS or pool.startswith(INDUSTRY_PREFIX)):
        raise SpecError("pool_key 非法：%r" % pool)

    weights = spec.get("weights")
    if not isinstance(weights, dict) or not weights:
        raise SpecError("weights 不能为空")
    total = 0.0
    for key, weight in weights.items():
        get_field(key)  # 未登记字段直接抛 UnregisteredField
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise SpecError("权重必须是数字：%s=%r" % (key, weight))
        if weight < 0:
            raise SpecError("权重不能为负：%s=%r" % (key, weight))
        total += float(weight)
    if abs(total - 1.0) > WEIGHT_TOLERANCE:
        raise SpecError("权重和必须为 1，实际 %.6f" % total)

    top_n = spec.get("top_n", 200)
    if isinstance(top_n, bool) or not isinstance(top_n, int) or not 1 <= top_n <= 1000:
        raise SpecError("top_n 必须是 1..1000 的整数")

    fields = spec.get("fields")
    if fields is not None:
        if not isinstance(fields, list):
            raise SpecError("fields 必须是数组")
        for key in fields:
            get_field(key)
    return spec


def create_scheme(db: Session, code: str, name: str, pool_key: str,
                  description: str = "") -> SelectionScheme:
    code = (code or "").strip()
    name = (name or "").strip()
    if not code or not name:
        raise SpecError("code 与 name 不能为空")
    if not (pool_key in POOL_KEYS or str(pool_key or "").startswith(INDUSTRY_PREFIX)):
        raise SpecError("pool_key 非法：%r" % pool_key)
    scheme = SelectionScheme(code=code, name=name, pool_key=pool_key,
                             description=description or None, status="active")
    db.add(scheme)
    db.commit()
    db.refresh(scheme)
    return scheme


def update_scheme(db: Session, scheme_id: int, **changes) -> SelectionScheme:
    scheme = db.get(SelectionScheme, scheme_id)
    if scheme is None:
        raise LookupError("方案不存在：%s" % scheme_id)
    if "status" in changes and changes["status"] not in SCHEME_STATUSES:
        raise SpecError("status 非法：%r" % changes["status"])
    for field in ("name", "description", "status"):
        if field in changes and changes[field] is not None:
            setattr(scheme, field, changes[field])
    db.commit()
    db.refresh(scheme)
    return scheme


def list_schemes(db: Session, include_archived: bool = False) -> List[dict]:
    query = db.query(SelectionScheme)
    if not include_archived:
        query = query.filter(SelectionScheme.status == "active")
    rows = query.order_by(SelectionScheme.pool_key, SelectionScheme.code).all()
    out = []
    for scheme in rows:
        latest = (db.query(SelectionSchemeVersion)
                  .filter(SelectionSchemeVersion.scheme_id == scheme.id)
                  .order_by(SelectionSchemeVersion.version.desc()).first())
        out.append({
            "id": scheme.id, "code": scheme.code, "name": scheme.name,
            "pool_key": scheme.pool_key, "description": scheme.description,
            "status": scheme.status,
            "latest_version": latest.version if latest else None,
            "latest_status": latest.status if latest else None,
        })
    return out


def list_versions(db: Session, scheme_id: int) -> List[SelectionSchemeVersion]:
    return (db.query(SelectionSchemeVersion)
            .filter(SelectionSchemeVersion.scheme_id == scheme_id)
            .order_by(SelectionSchemeVersion.version.desc()).all())


def create_version(db: Session, scheme_id: int, spec: dict,
                   note: str = "", uncalibrated: bool = True
                   ) -> SelectionSchemeVersion:
    scheme = db.get(SelectionScheme, scheme_id)
    if scheme is None:
        raise LookupError("方案不存在：%s" % scheme_id)
    validate_spec(spec)
    fields = spec.get("fields") or list(spec["weights"].keys())
    latest = (db.query(func.max(SelectionSchemeVersion.version))
              .filter(SelectionSchemeVersion.scheme_id == scheme_id).scalar())
    version = SelectionSchemeVersion(
        scheme_id=scheme_id,
        version=(latest or 0) + 1,
        spec_json=_dump(spec),
        spec_hash=spec_fingerprint(spec),
        field_set_version=FIELD_SET_VERSION,
        field_fingerprint=field_set_fingerprint(fields),
        weight_sum=sum(float(w) for w in spec["weights"].values()),
        uncalibrated=uncalibrated,
        status="draft",
        note=note or None,
    )
    db.add(version)
    db.commit()
    db.refresh(version)
    return version


def activate_version(db: Session, scheme_id: int,
                     version_id: int) -> SelectionSchemeVersion:
    version = db.get(SelectionSchemeVersion, version_id)
    if version is None or version.scheme_id != scheme_id:
        raise LookupError("版本不存在：%s" % version_id)
    (db.query(SelectionSchemeVersion)
     .filter(SelectionSchemeVersion.scheme_id == scheme_id,
             SelectionSchemeVersion.id != version_id,
             SelectionSchemeVersion.status == "active")
     .update({"status": "archived"}, synchronize_session=False))
    version.status = "active"
    db.commit()
    db.refresh(version)
    return version


def version_payload(version: SelectionSchemeVersion) -> Dict:
    return {
        "id": version.id,
        "scheme_id": version.scheme_id,
        "version": version.version,
        "spec": json.loads(version.spec_json),
        "spec_hash": version.spec_hash,
        "field_set_version": version.field_set_version,
        "field_fingerprint": version.field_fingerprint,
        "weight_sum": float(version.weight_sum) if version.weight_sum is not None else None,
        "uncalibrated": bool(version.uncalibrated),
        "status": version.status,
        "note": version.note,
        "created_at": version.created_at.isoformat() if version.created_at else None,
    }


def seed_default_schemes(db: Session) -> int:
    """幂等种子：四风格池各建一个方案（不建版本，参数由用户填）。"""
    created = 0
    for pool in POOL_KEYS:
        if db.query(SelectionScheme).filter_by(code=pool).first():
            continue
        db.add(SelectionScheme(
            code=pool, name={"growth": "成长", "quality": "质量",
                             "value": "价值", "dividend": "红利"}[pool],
            pool_key=pool, description="内置模板（权重未校准，需先建版本）",
            status="active"))
        created += 1
    if created:
        db.commit()
    return created
