"""拼盘资格只读接口（0021 B4）。

``GET /api/pairing/eligibility/{portfolio_strategy_id}?record_id=&scope=L2``

- 开关关闭（``capital_pool.enabled`` 或 ``pairing_enabled``，默认）→ 返回
  ``enabled=false`` + ``status=disabled``，不计算；
- 组合不存在 → 404（envelope code=404）；
- 只读：不写表、不触发回测、不改交易链路。
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.config import load_config
from core.db import get_db
from core.engine.capital_contracts import CapitalPoolSwitches, STATUS_DISABLED
from core.services.pairing_eligibility import evaluate_eligibility

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pairing", tags=["capital-pairing"])


@router.get("/eligibility/{portfolio_strategy_id}")
def pairing_eligibility(
    portfolio_strategy_id: int,
    record_id: Optional[int] = Query(None, ge=1, description="指定回测记录；缺省取最近已完成"),
    scope: str = Query("L2", description="L2=同组合拼盘（默认）/ L3=跨组合"),
    db: Session = Depends(get_db),
):
    switches = CapitalPoolSwitches.from_config(load_config() or {})
    if not switches.capital_pool_enabled or not switches.pairing_enabled:
        return ok({
            "enabled": False,
            "status": STATUS_DISABLED,
            "reasons": [
                "资金池开关未启用（capital_pool.enabled=false）"
                if not switches.capital_pool_enabled
                else "拼盘开关未启用（capital_pool.pairing_enabled=false）",
            ],
        })
    try:
        return ok(evaluate_eligibility(
            db, portfolio_strategy_id, record_id=record_id, scope=scope))
    except LookupError as exc:
        return err(404, str(exc))
