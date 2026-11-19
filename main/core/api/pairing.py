"""拼盘接口（0021 B4 资格 + B6 拼盘表/L2 草稿）。

- ``GET  /api/pairing/eligibility/{portfolio_strategy_id}``：资格只读判定；
- ``POST /api/pairing/plans``：新建 L2 拼盘草稿（含冻结评估）；
- ``GET  /api/pairing/plans`` / ``GET /api/pairing/plans/{id}``：列表/详情；
- ``POST /api/pairing/plans/{id}/evaluate``：按当前策略行重算（漂移即失效）；
- ``POST /api/pairing/plans/{id}/approve``：仅评估 GO 可批准；
- ``DELETE /api/pairing/plans/{id}``：仅草稿/归档可删；
- ``GET  /api/pairing/plans/{id}/loan-model-plan``：给回测用的 B3 计划。

开关关闭（``capital_pool.enabled`` 或 ``pairing_enabled``，默认）一律返回
``enabled=false`` 且不读写业务表；拼盘操作不触碰实盘会话与交易链路。
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.config import load_config
from core.db import get_db
from core.engine.capital_contracts import CapitalPoolSwitches, STATUS_DISABLED
from core.services import pairing_service
from core.services.pairing_eligibility import evaluate_eligibility

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pairing", tags=["capital-pairing"])


def _disabled_reasons() -> Optional[list]:
    switches = CapitalPoolSwitches.from_config(load_config() or {})
    if not switches.capital_pool_enabled:
        return ["资金池开关未启用（capital_pool.enabled=false）"]
    if not switches.pairing_enabled:
        return ["拼盘开关未启用（capital_pool.pairing_enabled=false）"]
    return None


def _disabled() -> dict:
    return ok({"enabled": False, "status": STATUS_DISABLED,
               "reasons": _disabled_reasons() or []})


class PairingPlanRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    portfolio_strategy_id: int = Field(..., ge=1)
    lender_strategy_id: int = Field(..., ge=1)
    borrower_strategy_ids: list[int] = Field(..., min_length=1)
    principal: float = Field(..., gt=0)
    scope: str = "L2"
    loan_term_days: int = 365
    recall_notice_days: int = 2
    release_delay_days: int = 0
    max_share: float = 1.0
    pool_scope: Optional[str] = None
    ownership_policy: Optional[str] = None
    no_cap_reserve_policy: Optional[str] = None
    gate_hysteresis_days: Optional[int] = None
    created_by: Optional[str] = None


@router.get("/eligibility/{portfolio_strategy_id}")
def pairing_eligibility(
    portfolio_strategy_id: int,
    record_id: Optional[int] = Query(None, ge=1, description="指定回测记录；缺省取最近已完成"),
    scope: str = Query("L2", description="L2=同组合拼盘（默认）/ L3=跨组合"),
    db: Session = Depends(get_db),
):
    if _disabled_reasons():
        return _disabled()
    try:
        return ok(evaluate_eligibility(
            db, portfolio_strategy_id, record_id=record_id, scope=scope))
    except LookupError as exc:
        return err(404, str(exc))


@router.post("/plans")
def create_pairing_plan(payload: PairingPlanRequest,
                        db: Session = Depends(get_db)):
    if _disabled_reasons():
        return _disabled()
    try:
        data = pairing_service.create_plan(db, payload.model_dump())
    except LookupError as exc:
        return err(404, str(exc))
    except pairing_service.PairingError as exc:
        return err(422, str(exc))
    return ok(data)


@router.get("/plans")
def list_pairing_plans(scope: Optional[str] = Query(None),
                       status: Optional[str] = Query(None),
                       db: Session = Depends(get_db)):
    if _disabled_reasons():
        return _disabled()
    return ok(pairing_service.list_plans(db, scope=scope, status=status))


@router.get("/plans/{plan_id}")
def get_pairing_plan(plan_id: int, db: Session = Depends(get_db)):
    if _disabled_reasons():
        return _disabled()
    try:
        return ok(pairing_service.get_plan(db, plan_id))
    except LookupError as exc:
        return err(404, str(exc))


@router.post("/plans/{plan_id}/evaluate")
def evaluate_pairing_plan(plan_id: int, db: Session = Depends(get_db)):
    if _disabled_reasons():
        return _disabled()
    try:
        return ok(pairing_service.evaluate_plan(db, plan_id))
    except LookupError as exc:
        return err(404, str(exc))


@router.post("/plans/{plan_id}/approve")
def approve_pairing_plan(plan_id: int, db: Session = Depends(get_db)):
    if _disabled_reasons():
        return _disabled()
    try:
        return ok(pairing_service.approve_plan(db, plan_id))
    except LookupError as exc:
        return err(404, str(exc))
    except pairing_service.PairingError as exc:
        return err(409, str(exc))


@router.delete("/plans/{plan_id}")
def delete_pairing_plan(plan_id: int, db: Session = Depends(get_db)):
    if _disabled_reasons():
        return _disabled()
    try:
        return ok(pairing_service.delete_plan(db, plan_id))
    except LookupError as exc:
        return err(404, str(exc))
    except pairing_service.PairingError as exc:
        return err(409, str(exc))


@router.get("/plans/{plan_id}/loan-model-plan")
def pairing_loan_model_plan(plan_id: int, db: Session = Depends(get_db)):
    if _disabled_reasons():
        return _disabled()
    try:
        return ok(pairing_service.build_loan_model_plan(db, plan_id))
    except LookupError as exc:
        return err(404, str(exc))
    except pairing_service.PairingError as exc:
        return err(422, str(exc))
