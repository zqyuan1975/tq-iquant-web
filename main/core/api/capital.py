"""资金池额度推导只读接口（0021 A4）。

单一只读接口：``GET /api/capital/derive?portfolio_id=N``。

- 开关关闭（``capital_pool.enabled=false``，默认）→ 返回 ``enabled=false`` +
  ``status=disabled``，前端据此显示「未启用」，不计算、不落库；
- 组合不存在 → 404；权益缺失等无法确定 → 422（附推导结果便于定位原因）；
- 不提供任何写操作，也不改变交易链路。

推导口径与 ``core.engine.capital_quota`` 同源（纯函数），本模块只做 DB 只读装配。
"""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.config import load_config
from core.db import get_db
from core.engine.capital_contracts import (
    CapitalPoolSwitches,
    STATUS_DISABLED,
    STATUS_NOT_DERIVABLE,
    build_capital_pool_version,
)
from core.engine.capital_quota import (
    derive_portfolio_quota,
    portfolio_quota_input_from_orm,
)
from core.models import PortfolioStrategy, Strategy

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/capital", tags=["capital"])


def _disabled_payload(reasons) -> dict:
    return {
        "enabled": False,
        "status": STATUS_DISABLED,
        "reasons": list(reasons),
        "version": build_capital_pool_version(),
    }


@router.get("/derive")
def capital_derive(
    portfolio_id: int = Query(..., ge=1, description="组合 ID"),
    equity: Optional[float] = Query(
        None, gt=0, description="当前总权益；缺省用组合 initial_capital（仅按配置推导）"),
    gross_exposure: Optional[float] = Query(
        None, ge=0, description="当前毛敞口；提供时计算可用缺口 headroom"),
    db: Session = Depends(get_db),
):
    switches = CapitalPoolSwitches.from_config(load_config() or {})
    if not switches.capital_pool_enabled:
        return ok(_disabled_payload(["资金池开关未启用（capital_pool.enabled=false）"]))

    ps = db.get(PortfolioStrategy, portfolio_id)
    if ps is None:
        return err(404, f"组合不存在：{portfolio_id}")
    strategies = (
        db.query(Strategy).filter(Strategy.portfolio_id == portfolio_id).all()
    )

    cfg = portfolio_quota_input_from_orm(
        ps, strategies, equity=equity, gross_exposure=gross_exposure,
    )
    try:
        result = derive_portfolio_quota(cfg, enabled=True)
    except Exception:  # noqa: BLE001 — 只读接口，不因推导异常影响服务
        logger.exception("额度推导失败 portfolio_id=%s", portfolio_id)
        return err(500, "额度推导失败，请查看服务日志")

    data = result.to_dict()
    data["enabled"] = True
    if result.status == STATUS_NOT_DERIVABLE:
        return err(422, "；".join(result.reasons) or "无法推导额度", data)
    return ok(data)
