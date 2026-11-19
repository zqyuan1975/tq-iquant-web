"""公司行为手工台账 API（2026-11-18，J1/J2）。

基础路径 ``/api/corporate-actions``；统一 envelope（``ok``/``err``，code 与
HTTP 语义对齐但 HTTP 仍 200，与全仓其它路由一致）。

错误码：400 字段非法 / 404 不存在 / 409 重复 / 422 批量导入有非法行
（返回 ``data.errors[]`` 带行号与原因，整批回滚）。
"""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.services.corporate_action_service import (
    CorporateActionConflict,
    CorporateActionNotFound,
    CorporateActionValidationError,
    batch_create,
    create_action,
    delete_action,
    list_actions,
    update_action,
)

router = APIRouter(prefix="/api/corporate-actions", tags=["corporate_actions"])


class CorporateActionReq(BaseModel):
    code: Optional[str] = None
    event_type: Optional[str] = None
    ex_date: Optional[str] = None
    end_date: Optional[str] = None
    cash_per_share: Optional[float] = None
    cash_pct: Optional[float] = None
    share_ratio: Optional[float] = None
    rights_price: Optional[float] = None
    note: Optional[str] = None


class BatchReq(BaseModel):
    items: List[CorporateActionReq]


@router.get("")
def list_corporate_actions(code: Optional[str] = None,
                           event_type: Optional[str] = None,
                           start: Optional[str] = None,
                           end: Optional[str] = None,
                           page: int = 1,
                           page_size: int = 50,
                           db: Session = Depends(get_db)):
    try:
        start_d = date.fromisoformat(start) if start else None
        end_d = date.fromisoformat(end) if end else None
    except ValueError:
        return err(400, "start/end 日期格式应为 YYYY-MM-DD")
    return ok(list_actions(db, code=code, event_type=event_type, start=start_d,
                           end=end_d, page=page, page_size=page_size))


def _payload(req: CorporateActionReq) -> dict:
    # exclude_unset：PUT 语义下未传字段保持原值；POST 由 normalize_payload 兜底
    return req.model_dump(exclude_unset=True)


@router.post("")
def create_corporate_action(req: CorporateActionReq, db: Session = Depends(get_db)):
    try:
        return ok(create_action(db, _payload(req)))
    except CorporateActionValidationError as exc:
        return err(400, str(exc))
    except CorporateActionConflict as exc:
        return err(409, str(exc))


@router.post("/batch")
def batch_create_corporate_actions(req: BatchReq, db: Session = Depends(get_db)):
    try:
        return ok(batch_create(db, [_payload(item) for item in req.items]))
    except CorporateActionValidationError as exc:
        detail = exc.args[0] if exc.args else str(exc)
        if isinstance(detail, list):
            return err(422, "批量导入存在非法行，整批已回滚", {"errors": detail})
        return err(400, str(exc))
    except CorporateActionConflict as exc:
        return err(409, str(exc))


@router.put("/{action_id}")
def update_corporate_action(action_id: int, req: CorporateActionReq,
                            db: Session = Depends(get_db)):
    try:
        return ok(update_action(db, action_id, _payload(req)))
    except CorporateActionValidationError as exc:
        return err(400, str(exc))
    except CorporateActionNotFound as exc:
        return err(404, str(exc))
    except CorporateActionConflict as exc:
        return err(409, str(exc))


@router.delete("/{action_id}")
def delete_corporate_action(action_id: int, db: Session = Depends(get_db)):
    try:
        delete_action(db, action_id)
        return ok({"deleted": action_id})
    except CorporateActionNotFound as exc:
        return err(404, str(exc))
