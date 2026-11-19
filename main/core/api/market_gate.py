"""市场多空状态接口 — 备选股池页「市场多空状态」区。

单一只读接口：``GET /api/market-gate/status``。
口径与回测/实盘门控同源（core/engine/market_gate.py），不落库、可复算；
``refresh=true`` 绕过进程内 5 分钟缓存（前端刷新按钮）。
"""
import logging

from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.engine.market_gate import MarketGateUnavailableError
from core.services.market_gate_service import (
    DEFAULT_GATE_INDEX,
    fetch_gate_overview,
    fetch_gate_status,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/market-gate", tags=["market-gate"])


@router.get("/status")
def market_gate_status(
    code: str = Query(DEFAULT_GATE_INDEX, description="基准指数代码，如 000985.CSI"),
    period: str = Query("1d", description="K 线周期，默认日线"),
    count: int = Query(500, ge=100, le=3000, description="取数根数（含 MA288 预热）"),
    history: int = Query(5, ge=1, le=20, description="返回最近 N 次转折"),
    refresh: bool = Query(False, description="true=绕过缓存，强制重新取数"),
):
    try:
        data = fetch_gate_status(
            benchmark_index=code, period=period, count=count,
            history=history, force=refresh,
        )
    except MarketGateUnavailableError as exc:
        # 可操作的配置/数据问题（指数未下载、历史不足）→ 400，前端原样提示
        return err(400, str(exc))
    except Exception as exc:  # noqa: BLE001
        logger.exception("市场多空状态计算失败 code=%s", code)
        return err(500, f"市场多空状态计算失败：{exc}")
    return ok(data)


@router.get("/overview")
def market_gate_overview(db: Session = Depends(get_db)):
    """E5 统一市场状态面板：两套机制作用域/优先级 + 组合与策略配置（只读）。"""
    try:
        return ok(fetch_gate_overview(db))
    except Exception as exc:  # noqa: BLE001
        logger.exception("市场状态面板聚合失败")
        return err(500, f"市场状态面板聚合失败：{exc}")
