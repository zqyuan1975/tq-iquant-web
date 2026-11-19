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
from core.engine.capital_reserve import (
    build_utilization_points,
    summarize_utilization,
)
from core.models import (
    BacktestDailySnapshot,
    BacktestRecord,
    LivePortfolioDaily,
    PortfolioStrategy,
    Strategy,
)

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


def _backtest_rows(db: Session, portfolio_id: int,
                   record_id: Optional[int]):
    """回测日快照 → 利用率行（只读）；返回 (rows, 实际 record_id)。"""
    if record_id is not None:
        rec = db.get(BacktestRecord, record_id)
        if rec is None or rec.portfolio_strategy_id != portfolio_id:
            raise LookupError(f"回测记录不属于该组合：{record_id}")
    else:
        rec = (
            db.query(BacktestRecord)
            .filter(BacktestRecord.portfolio_strategy_id == portfolio_id,
                    BacktestRecord.status == "completed")
            .order_by(BacktestRecord.id.desc())
            .first()
        )
        if rec is None:
            return [], None
    snaps = (
        db.query(BacktestDailySnapshot)
        .filter(BacktestDailySnapshot.backtest_record_id == rec.id,
                BacktestDailySnapshot.target_type == "portfolio",
                BacktestDailySnapshot.target_id == portfolio_id)
        .order_by(BacktestDailySnapshot.snap_date)
        .all()
    )
    return [
        {"snap_date": s.snap_date, "total_value": s.total_value,
         "market_value": s.market_value, "gate_state": s.gate_state}
        for s in snaps
    ], rec.id


def _live_rows(db: Session, portfolio_id: int, session_id: Optional[int]):
    """实盘组合日序列 → 利用率行（只读）；返回 (rows, 实际 session_id)。"""
    q = db.query(LivePortfolioDaily).filter(
        LivePortfolioDaily.portfolio_strategy_id == portfolio_id)
    if session_id is not None:
        q = q.filter(LivePortfolioDaily.live_session_id == session_id)
    else:
        latest = (
            db.query(LivePortfolioDaily)
            .filter(LivePortfolioDaily.portfolio_strategy_id == portfolio_id)
            .order_by(LivePortfolioDaily.trade_date.desc())
            .first()
        )
        if latest is None:
            return [], None
        q = q.filter(LivePortfolioDaily.live_session_id == latest.live_session_id)
    rows = q.order_by(LivePortfolioDaily.trade_date).all()
    return [
        {"snap_date": r.trade_date, "total_value": r.total_value,
         "market_value": r.market_value, "gate_state": None}
        for r in rows
    ], (rows[0].live_session_id if rows else None)


@router.get("/utilization")
def capital_utilization(
    portfolio_id: int = Query(..., ge=1, description="组合 ID"),
    source: str = Query("backtest", pattern="^(backtest|live)$",
                        description="数据来源：backtest|live"),
    record_id: Optional[int] = Query(None, ge=1, description="回测记录 ID"),
    session_id: Optional[int] = Query(None, ge=1, description="实盘会话 ID"),
    window: int = Query(252, ge=20, le=2000, description="回看窗口（交易日）"),
    db: Session = Depends(get_db),
):
    """历史利用率统计（0021 C2，只读）。

    分位口径与 C1 预留共用同一套过滤（剔除启动 20 日、权益跳变、门控关闭），
    返回原始序列供界面复算；样本不足只降级为 ``no_history``，不返回 422。
    不写任何业务表。
    """
    switches = CapitalPoolSwitches.from_config(load_config() or {})
    if not switches.capital_pool_enabled:
        return ok(_disabled_payload(["资金池开关未启用（capital_pool.enabled=false）"]))

    ps = db.get(PortfolioStrategy, portfolio_id)
    if ps is None:
        return err(404, f"组合不存在：{portfolio_id}")

    try:
        if source == "backtest":
            rows, used = _backtest_rows(db, portfolio_id, record_id)
        else:
            rows, used = _live_rows(db, portfolio_id, session_id)
    except LookupError as exc:
        return err(404, str(exc))

    points = build_utilization_points(rows)
    stats = summarize_utilization(points, window=window, include_series=True)
    data = stats.to_dict()
    data.update({
        "enabled": True,
        "source": source,
        "portfolio_id": portfolio_id,
        "record_id": used if source == "backtest" else None,
        "session_id": used if source == "live" else None,
    })
    return ok(data)
