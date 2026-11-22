"""接缝续跑式增量回测 API。

- GET  /api/incremental/summary：前端拿 checkpoint + 逐日对照行 + 最近一次双跑校验。
- POST /api/incremental/validate：手动触发双跑校验（从接缝日全量重算对账）。

校验走服务内 `_INCR_LOCK` 串行化，**不占用** 回测的 `_BACKTEST_LOCK` 队列。
"""
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.services import incremental_backtest as svc
from core.tq.gate import P2, TdxTaskBlocked, tdx_priority

router = APIRouter(prefix="/api/incremental", tags=["incremental"])


class ValidateRequest(BaseModel):
    session_id: int
    portfolio_strategy_id: int
    from_date: Optional[date] = None
    to_date: Optional[date] = None


class RebuildRequest(BaseModel):
    session_id: int
    portfolio_strategy_id: int


@router.get("/summary")
def get_summary(
    session_id: int,
    portfolio_strategy_id: int,
    db: Session = Depends(get_db),
):
    try:
        return ok(svc.summary(db, session_id, portfolio_strategy_id))
    except Exception as exc:  # noqa: BLE001 — 统一 envelope，前端可读
        return err(500, "读取增量回测汇总失败：%s" % exc)


@router.post("/rebuild")
def post_rebuild(body: RebuildRequest, db: Session = Depends(get_db)):
    """K3：一键重建接缝（丢弃 checkpoint 后从接缝日全量重建）。"""
    try:
        with tdx_priority(P2):
            result = svc.run_incremental(
                db, body.session_id, body.portfolio_strategy_id,
                force_rebuild=True)
    except TdxTaskBlocked:
        raise  # 交给全局处理器：返回 409 + 下次可用时刻
    except Exception as exc:  # noqa: BLE001
        return err(500, "重建接缝失败：%s" % exc)
    if result.get("status") == "error":
        return err(422, result.get("reason") or "重建接缝失败", result)
    return ok(result)


@router.post("/validate")
def post_validate(body: ValidateRequest, db: Session = Depends(get_db)):
    try:
        with tdx_priority(P2):
            result = svc.run_validation(
                db, body.session_id, body.portfolio_strategy_id,
                from_date=body.from_date, to_date=body.to_date)
    except TdxTaskBlocked:
        raise
    except Exception as exc:  # noqa: BLE001
        return err(500, "双跑校验失败：%s" % exc)
    if result.get("status") == "error":
        return err(422, result.get("reason") or "双跑校验失败", result)
    return ok(result)
