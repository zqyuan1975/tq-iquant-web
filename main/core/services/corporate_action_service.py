"""公司行为手工台账服务（2026-11-18，J1/J2）。

只做「校验 + CRUD」，不做自动抓取。事件表为空 / 组合开关关闭时，回测与实盘
的执行路径与改动前逐字一致（见 ``core/engine/corporate_actions.py``）。

校验口径（与方案 §2.3 一致）：
- ``event_type`` 必须在 ``EVENT_TYPES``；
- ``code`` 必须带交易所后缀（``600000.SH``），与全仓口径一致；
- ``dividend`` 至少给 ``cash_per_share``（实盘口径）或 ``cash_pct``（回测口径）之一；
- ``split`` 必须给 ``share_ratio``（0.3 = 10 送 3）；
- ``suspend`` 的 ``end_date`` 不得早于 ``ex_date``；
- ``(code, event_type, ex_date)`` 唯一 → 重复录入报冲突（409）。
"""
from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Iterable, Optional

from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from core.engine.corporate_actions import EVENT_TYPES


class CorporateActionError(ValueError):
    """校验/业务错误基类。"""


class CorporateActionValidationError(CorporateActionError):
    """字段不合法（400 / 批量导入 422）。"""


class CorporateActionNotFound(CorporateActionError):
    """id 不存在（404）。"""


class CorporateActionConflict(CorporateActionError):
    """(code, event_type, ex_date) 重复（409）。"""


_FIELDS = (
    "code", "event_type", "ex_date", "end_date", "cash_per_share", "cash_pct",
    "share_ratio", "rights_price", "note",
)


def _dec(value) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001 — 非法数字统一转业务错误
        raise CorporateActionValidationError("金额/比例字段不是合法数字：%r" % (value,))


def _as_date(value) -> Optional[date]:
    if value is None or value == "":
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError:
        raise CorporateActionValidationError("日期格式应为 YYYY-MM-DD：%r" % (value,))


def normalize_payload(payload: dict, base: Optional[dict] = None) -> dict:
    """规范化并校验一条事件；``base`` 用于 PUT 的部分字段合并。"""
    from core.models import CorporateAction as _Model  # noqa: F401 — 仅确保模型已注册

    merged = dict(base or {})
    for key in _FIELDS:
        if key in payload:
            merged[key] = payload[key]

    code = str(merged.get("code") or "").strip().upper()
    if not code or "." not in code:
        raise CorporateActionValidationError("code 必须带交易所后缀（如 600000.SH）")
    event_type = str(merged.get("event_type") or "").strip()
    if event_type not in EVENT_TYPES:
        raise CorporateActionValidationError(
            "event_type 必须是 %s 之一" % "/".join(EVENT_TYPES))
    ex_date = _as_date(merged.get("ex_date"))
    if ex_date is None:
        raise CorporateActionValidationError("ex_date 不能为空")
    end_date = _as_date(merged.get("end_date"))
    cash_per_share = _dec(merged.get("cash_per_share"))
    cash_pct = _dec(merged.get("cash_pct"))
    share_ratio = _dec(merged.get("share_ratio"))
    rights_price = _dec(merged.get("rights_price"))
    note = merged.get("note")
    if note is not None:
        note = str(note)[:200]

    if event_type == "dividend" and cash_per_share is None and cash_pct is None:
        raise CorporateActionValidationError(
            "dividend 至少提供 cash_per_share（实盘口径）或 cash_pct（回测口径）之一")
    if event_type == "split" and (share_ratio is None or share_ratio <= 0):
        raise CorporateActionValidationError("split 必须提供正数 share_ratio")
    if event_type == "suspend" and end_date is not None and end_date < ex_date:
        raise CorporateActionValidationError("suspend 的 end_date 不得早于 ex_date")

    return {
        "code": code,
        "event_type": event_type,
        "ex_date": ex_date,
        "end_date": end_date,
        "cash_per_share": cash_per_share,
        "cash_pct": cash_pct,
        "share_ratio": share_ratio,
        "rights_price": rights_price,
        "note": note,
    }


def _serialize(row) -> dict:
    return {
        "id": row.id,
        "code": row.code,
        "event_type": row.event_type,
        "ex_date": row.ex_date.isoformat() if row.ex_date else None,
        "end_date": row.end_date.isoformat() if row.end_date else None,
        "cash_per_share": float(row.cash_per_share) if row.cash_per_share is not None else None,
        "cash_pct": float(row.cash_pct) if row.cash_pct is not None else None,
        "share_ratio": float(row.share_ratio) if row.share_ratio is not None else None,
        "rights_price": float(row.rights_price) if row.rights_price is not None else None,
        "note": row.note,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def list_actions(db: Session, *, code: Optional[str] = None,
                 event_type: Optional[str] = None,
                 start: Optional[date] = None, end: Optional[date] = None,
                 page: int = 1, page_size: int = 50) -> dict:
    from core.models import CorporateAction

    query = db.query(CorporateAction)
    if code:
        query = query.filter(CorporateAction.code == code.strip().upper())
    if event_type:
        query = query.filter(CorporateAction.event_type == event_type)
    if start is not None:
        query = query.filter(CorporateAction.ex_date >= start)
    if end is not None:
        query = query.filter(CorporateAction.ex_date <= end)
    total = query.count()
    page = max(1, int(page or 1))
    page_size = max(1, min(500, int(page_size or 50)))
    rows = (query.order_by(CorporateAction.ex_date.desc(), CorporateAction.id.desc())
            .offset((page - 1) * page_size).limit(page_size).all())
    return {"total": total, "page": page, "page_size": page_size,
            "items": [_serialize(r) for r in rows]}


def _apply(row, fields: dict) -> None:
    for key, value in fields.items():
        setattr(row, key, value)


def create_action(db: Session, payload: dict):
    from core.models import CorporateAction

    fields = normalize_payload(payload)
    row = CorporateAction(**fields)
    db.add(row)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise CorporateActionConflict(
            "%s %s %s 已存在" % (fields["code"], fields["event_type"],
                                 fields["ex_date"]))
    db.refresh(row)
    return _serialize(row)


def batch_create(db: Session, items: Iterable[dict]) -> dict:
    """批量导入：逐条校验后**单事务**提交，任一条非法则整批回滚（422）。"""
    from core.models import CorporateAction

    errors = []
    normalized = []
    for idx, item in enumerate(items or [], start=1):
        try:
            normalized.append(normalize_payload(item))
        except CorporateActionValidationError as exc:
            errors.append({"row": idx, "reason": str(exc)})
    if errors:
        raise CorporateActionValidationError(errors)
    for fields in normalized:
        db.add(CorporateAction(**fields))
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise CorporateActionConflict("批量导入中存在重复的 (code, event_type, ex_date)")
    return {"created": len(normalized)}


def update_action(db: Session, action_id: int, payload: dict) -> dict:
    from core.models import CorporateAction

    row = db.query(CorporateAction).filter(CorporateAction.id == action_id).first()
    if row is None:
        raise CorporateActionNotFound("事件 %s 不存在" % action_id)
    base = {key: getattr(row, key) for key in _FIELDS}
    fields = normalize_payload(payload, base=base)
    _apply(row, fields)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        raise CorporateActionConflict(
            "%s %s %s 已存在" % (fields["code"], fields["event_type"],
                                 fields["ex_date"]))
    db.refresh(row)
    return _serialize(row)


def delete_action(db: Session, action_id: int) -> None:
    from core.models import CorporateAction

    row = db.query(CorporateAction).filter(CorporateAction.id == action_id).first()
    if row is None:
        raise CorporateActionNotFound("事件 %s 不存在" % action_id)
    db.delete(row)
    db.commit()
