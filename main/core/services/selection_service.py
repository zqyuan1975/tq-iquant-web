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
from core.selection.engine import DEFAULT_PE_MODE, PE_MODES
from core.selection.field_dict import (
    FIELD_SET_VERSION, assert_scorable, field_set_fingerprint, get_field)

POOL_KEYS = ("growth", "quality", "value", "dividend")
INDUSTRY_PREFIX = "industry:"
WEIGHT_TOLERANCE = 1e-6
SCHEME_STATUSES = ("active", "archived")
VERSION_STATUSES = ("draft", "active", "archived")
TRACKS = ("both", "forward", "historical")

#: D3 拍板：首发默认权重直接采用外部方案 v4 的 STYLE_WEIGHTS（未校准），
#: 全部字段都落在 field_dict.SCORABLE_FIELDS 内。用户可改权重，但只能改这些字段。
DEFAULT_GATES = {
    "min_float_mv": 20.0,       # 流通市值下限（亿元）
    "max_pe": 500.0,            # PE 上限
    "min_div_streak": 3,        # 红利：连续分红年数
    "max_payout": 100.0,        # 红利：分红率上限 %
    "min_fcf_cover": 1.0,       # 红利：自由现金流覆盖
    "max_roic_trend_drop": -5.0,  # 价值否决：ROIC 趋势下限
}

DEFAULT_SPECS: Dict[str, dict] = {
    "dividend": {
        "pool_key": "dividend",
        "top_n": 100,
        "weights": {"dp3": .25, "ep": .20, "cfp": .15, "roic_adj": .15,
                    "dps_growth": .10, "payout_stab": .10, "size": .05},
    },
    "growth": {
        "pool_key": "growth",
        "track": "both",
        "top_n": 100,
        "weights": {"g_fwd_2y": .35, "neg_peg_fwd": .30, "g_rev_2y": .15,
                    "accel": .10, "roic_trend": .10},
        "weights_hist": {"np_yoy": .35, "neg_peg_hist": .30, "rev_yoy": .20,
                         "g_hist_3y": .10, "roic_trend": .05},
    },
    "quality": {
        "pool_key": "quality",
        "top_n": 100,
        "weights": {"roic_adj": .25, "roe_adj": .20, "gm": .15,
                    "earn_qual": .15, "solvency": .15, "govern": .10},
    },
    "value": {
        "pool_key": "value",
        "top_n": 100,
        "weights": {"ep": .30, "bp": .20, "cfp": .20, "dp3": .15,
                    "sp": .10, "size": .05},
    },
}


class SpecError(ValueError):
    """方案参数不合法（API 层转 400）。"""


def _dump(spec: dict) -> str:
    return json.dumps(spec, ensure_ascii=False, sort_keys=True)


def spec_fingerprint(spec: dict) -> str:
    return hashlib.sha256(_dump(spec).encode("utf-8")).hexdigest()[:16]


def default_spec(pool_key: str) -> dict:
    """返回带默认闸门的参数模板（深拷贝，调用方可直接改）。"""
    if pool_key.startswith(INDUSTRY_PREFIX):
        from core.selection import sector_pools
        return sector_pools.default_spec(pool_key)
    base = DEFAULT_SPECS.get(pool_key)
    if base is None:
        raise SpecError("暂不支持该池的默认参数：%s" % pool_key)
    spec = json.loads(json.dumps(base))
    spec["gates"] = dict(DEFAULT_GATES)
    #: 默认做行业+市值中性化（版本参数的一部分，可随版本回退）
    spec["neutralize"] = True
    #: PE 处理口径：strict=现值（PE<=0 全挡）；loss_discount=亏损股进候选但降权
    spec["pe_mode"] = DEFAULT_PE_MODE
    return spec


def _check_weights(weights: dict, label: str) -> float:
    if not isinstance(weights, dict) or not weights:
        raise SpecError("%s 不能为空" % label)
    total = 0.0
    for key, weight in weights.items():
        get_field(key)          # 未登记字段直接抛 UnregisteredField
        assert_scorable([key])  # D3：未接入打分的字段不许进权重
        if isinstance(weight, bool) or not isinstance(weight, (int, float)):
            raise SpecError("%s 权重必须是数字：%s=%r" % (label, key, weight))
        if weight < 0:
            raise SpecError("%s 权重不能为负：%s=%r" % (label, key, weight))
        total += float(weight)
    if abs(total - 1.0) > WEIGHT_TOLERANCE:
        raise SpecError("%s 权重和必须为 1，实际 %.6f" % (label, total))
    return total


def _check_gates(gates: dict) -> None:
    if not isinstance(gates, dict):
        raise SpecError("gates 必须是对象")
    limits = {
        "min_float_mv": (0.0, 100000.0),
        "max_pe": (0.0, 10000.0),
        "min_div_streak": (0, 30),
        "max_payout": (0.0, 1000.0),
        "min_fcf_cover": (-100.0, 100.0),
        "max_roic_trend_drop": (-100.0, 0.0),
    }
    unknown = sorted(set(gates) - set(limits))
    if unknown:
        raise SpecError("未知闸门：%s" % ", ".join(unknown))
    for key, value in gates.items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise SpecError("闸门必须是数字：%s=%r" % (key, value))
        low, high = limits[key]
        if not low <= float(value) <= high:
            raise SpecError("闸门 %s 超出范围 %.2f..%.2f" % (key, low, high))


def validate_spec(spec: dict) -> dict:
    """校验参数树；只校验不改写，返回原对象。"""
    if not isinstance(spec, dict):
        raise SpecError("spec 必须是对象")
    pool = str(spec.get("pool_key") or "").strip()
    if not (pool in POOL_KEYS or pool.startswith(INDUSTRY_PREFIX)):
        raise SpecError("pool_key 非法：%r" % pool)

    industry = pool.startswith(INDUSTRY_PREFIX)
    industry_def: Dict = {}
    if industry:
        from core.selection import sector_pools
        try:
            industry_def = sector_pools.get_pool(pool)
        except sector_pools.SectorPoolError as exc:
            raise SpecError(str(exc)) from None
    # 金融地产只分组、L2 标签池沿用个股 L1 权重 → 都不要求自带权重
    group_only = bool(spec.get("group_only")
                      or (industry and industry_def.get("group_only")))
    cross_pool = bool(spec.get("cross_pool")
                      or (industry and industry_def.get("cross_pool")))
    if not (group_only or cross_pool):
        _check_weights(spec.get("weights"), "weights")
    elif spec.get("weights"):
        _check_weights(spec["weights"], "weights")

    supports_track = pool == "growth" or bool(industry and industry_def.get("track"))
    track = spec.get("track", "both")
    if supports_track:
        if track not in TRACKS:
            raise SpecError("track 必须是 %s 之一" % "/".join(TRACKS))
        # 只有 both 需要历史轨权重；forward/historical 单轨时 weights 即该轨权重
        if track == "both":
            _check_weights(spec.get("weights_hist"), "weights_hist")
        elif spec.get("weights_hist"):
            _check_weights(spec["weights_hist"], "weights_hist")
    elif "track" in spec:
        raise SpecError("只有成长池/科技行业池支持 track")

    top_n = spec.get("top_n", 100)
    if isinstance(top_n, bool) or not isinstance(top_n, int) or not 1 <= top_n <= 1000:
        raise SpecError("top_n 必须是 1..1000 的整数")

    if "neutralize" in spec and not isinstance(spec["neutralize"], bool):
        raise SpecError("neutralize 必须是布尔值")

    pe_mode = spec.get("pe_mode", DEFAULT_PE_MODE)
    if pe_mode not in PE_MODES:
        raise SpecError("pe_mode 必须是 %s 之一" % "/".join(PE_MODES))

    gates = spec.get("gates", {})
    _check_gates(gates)

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
    if spec.get("weights_hist"):
        fields = list(fields) + list(spec["weights_hist"].keys())
    fields = sorted(set(fields))
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


def seed_industry_schemes(db: Session) -> int:
    """幂等种子：9 个 L1 主池 + 14 个 L2 标签池，各建方案与 v1 版本。

    金融地产 v1 只分组不打分（权重为空），L2 池沿用个股所属 L1 权重，
    两者都在 spec 里显式标记，避免被误当成「权重缺失」。
    """
    from core.selection import sector_pools

    created = 0
    for item in sector_pools.pools_payload():
        scheme = db.query(SelectionScheme).filter_by(code=item["code"]).first()
        if scheme is None:
            desc = "行业%s池：%s" % (
                "标签" if item["kind"] == "l2" else "主",
                sector_pools.get_pool(item["key"]).get("description") or "")
            scheme = SelectionScheme(
                code=item["code"], name="行业·" + item["label"],
                pool_key=item["key"], description=desc[:300],
                status="active")
            db.add(scheme)
            db.flush()
            created += 1
        if db.query(SelectionSchemeVersion).filter_by(scheme_id=scheme.id).first():
            continue
        spec = sector_pools.default_spec(item["key"])
        validate_spec(spec)
        fields = sorted(set(spec.get("weights") or {})
                        | set(spec.get("weights_hist") or {}))
        db.add(SelectionSchemeVersion(
            scheme_id=scheme.id, version=1,
            spec_json=_dump(spec), spec_hash=spec_fingerprint(spec),
            field_set_version=FIELD_SET_VERSION,
            field_fingerprint=field_set_fingerprint(fields),
            weight_sum=sum(float(w) for w in (spec.get("weights") or {}).values()),
            uncalibrated=True, status="active",
            note="行业池内置 v1（未校准）",
        ))
    db.commit()
    return created


def seed_default_schemes(db: Session) -> int:
    """幂等种子：四风格池各建一个方案，并各建 v1 默认参数版本（启用）。

    v1 权重来自外部方案 v4 的经验值，统一标 ``uncalibrated=True``；
    用户改参数只能新建版本，v1 永远不会被覆盖。
    """
    created = 0
    names = {"growth": "成长", "quality": "质量",
             "value": "价值", "dividend": "红利"}
    for pool in POOL_KEYS:
        scheme = db.query(SelectionScheme).filter_by(code=pool).first()
        if scheme is None:
            scheme = SelectionScheme(
                code=pool, name=names[pool], pool_key=pool,
                description="内置模板（首发权重未校准）", status="active")
            db.add(scheme)
            db.flush()
            created += 1
        if db.query(SelectionSchemeVersion).filter_by(scheme_id=scheme.id).first():
            continue
        spec = default_spec(pool)
        validate_spec(spec)
        fields = sorted(set(spec["weights"]) | set(spec.get("weights_hist") or {}))
        db.add(SelectionSchemeVersion(
            scheme_id=scheme.id, version=1,
            spec_json=_dump(spec), spec_hash=spec_fingerprint(spec),
            field_set_version=FIELD_SET_VERSION,
            field_fingerprint=field_set_fingerprint(fields),
            weight_sum=sum(float(w) for w in spec["weights"].values()),
            uncalibrated=True, status="active",
            note="内置 v1（未校准）",
        ))
    db.commit()
    return created
