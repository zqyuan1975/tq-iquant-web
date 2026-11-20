"""资金池额度推导只读接口（0021 A4）。

单一只读接口：``GET /api/capital/derive?portfolio_id=N``。

- 开关关闭（``capital_pool.enabled=false``，默认）→ 返回 ``enabled=false`` +
  ``status=disabled``，前端据此显示「未启用」，不计算、不落库；
- 组合不存在 → 404；权益缺失等无法确定 → 422（附推导结果便于定位原因）；
- 不提供任何写操作，也不改变交易链路。

推导口径与 ``core.engine.capital_quota`` 同源（纯函数），本模块只做 DB 只读装配。
"""
import logging
from datetime import date, datetime
from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
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
    CashOutlet,
    LivePortfolioDaily,
    LiveSession,
    PortfolioStrategy,
    Strategy,
)
from core.services import cash_outlet_service

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


# ---------------------------------------------------------------------------
# 现金出口白名单（0021 C4，只读影子：登记 + C1–C8 判据，不下单）
# ---------------------------------------------------------------------------

class CashOutletRequest(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    category: Optional[str] = None
    channel: Optional[str] = None
    channel_order_enabled: Optional[bool] = None
    yield_source: Optional[str] = None
    yield_freq: Optional[str] = None
    yield_history_days: Optional[int] = None
    liquidity: Optional[str] = None
    term_days: Optional[int] = None
    settlement_max_days: Optional[int] = None
    holiday_postpone: Optional[bool] = None
    min_amount: Optional[str] = None
    max_single_amount: Optional[str] = None
    max_daily_amount: Optional[str] = None
    min_increment: Optional[str] = None
    redeemable_early: Optional[bool] = None
    early_redeem_penalty: Optional[str] = None
    auto_mature_return: Optional[bool] = None
    fast_redeem_limit: Optional[str] = None
    risk_level: Optional[str] = None
    principal_guaranteed: Optional[bool] = None
    arrival_buffer_days: Optional[int] = None
    inflow_certainty: Optional[str] = None
    status: Optional[str] = None
    notes: Optional[str] = None


class CashOutletEvaluateRequest(BaseModel):
    need_date: Optional[date] = None
    required_amount: Optional[str] = None


class CashOutletVerifyRequest(BaseModel):
    amount: str
    result: str
    verified_at: Optional[datetime] = None
    valid_days: int = 365


def _outlet_switches() -> CapitalPoolSwitches:
    return CapitalPoolSwitches.from_config(load_config() or {})


def _outlet_disabled():
    return ok(_disabled_payload(
        ["现金出口开关未启用（capital_pool.cash_outlet_enabled=false）"]))


def _get_outlet(db: Session, outlet_id: int) -> CashOutlet:
    outlet = db.get(CashOutlet, outlet_id)
    if outlet is None:
        raise LookupError(f"现金出口不存在：{outlet_id}")
    return outlet


@router.get("/outlets")
def list_outlets(
    category: Optional[str] = Query(None, description="repo|etf_money|other"),
    status: Optional[str] = Query(None, description="active|archived"),
    db: Session = Depends(get_db),
):
    """现金出口白名单（只读列表）。"""
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    q = db.query(CashOutlet)
    if category:
        q = q.filter(CashOutlet.category == category)
    if status:
        q = q.filter(CashOutlet.status == status)
    rows = q.order_by(CashOutlet.code).all()
    return ok({"enabled": True, "items": [
        cash_outlet_service.serialize_outlet(o) for o in rows]})


@router.get("/outlets/{outlet_id}")
def get_outlet(outlet_id: int, db: Session = Depends(get_db)):
    """单个现金出口配置（只读）。"""
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        outlet = _get_outlet(db, outlet_id)
    except LookupError as exc:
        return err(404, str(exc))
    return ok({"enabled": True,
               "item": cash_outlet_service.serialize_outlet(outlet)})


@router.post("/outlets")
def create_outlet(payload: CashOutletRequest, db: Session = Depends(get_db)):
    """登记一个出口（仅配置，不下单）。"""
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        outlet = cash_outlet_service.create_outlet(
            db, payload.model_dump(exclude_unset=True))
    except ValueError as exc:
        return err(422, str(exc))
    return ok({"enabled": True,
               "item": cash_outlet_service.serialize_outlet(outlet)})


@router.put("/outlets/{outlet_id}")
def update_outlet(outlet_id: int, payload: CashOutletRequest,
                  db: Session = Depends(get_db)):
    """更新出口配置（仅配置，不影响已有验证证据以外的状态）。"""
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        outlet = _get_outlet(db, outlet_id)
    except LookupError as exc:
        return err(404, str(exc))
    outlet = cash_outlet_service.update_outlet(
        db, outlet, payload.model_dump(exclude_unset=True))
    return ok({"enabled": True,
               "item": cash_outlet_service.serialize_outlet(outlet)})


@router.post("/outlets/{outlet_id}/evaluate")
def evaluate_outlet(outlet_id: int, payload: CashOutletEvaluateRequest,
                    db: Session = Depends(get_db)):
    """按 C1–C8 判定出口是否可借（只读，可复现）。"""
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        outlet = _get_outlet(db, outlet_id)
    except LookupError as exc:
        return err(404, str(exc))
    result = cash_outlet_service.evaluate_outlet(
        outlet,
        need_date=payload.need_date,
        required_amount=payload.required_amount,
    )
    result["enabled"] = True
    return ok(result)


@router.post("/outlets/{outlet_id}/verify")
def verify_outlet(outlet_id: int, payload: CashOutletVerifyRequest,
                  db: Session = Depends(get_db)):
    """登记 C8 通道验证证据（只记录小额实盘结果，不发起交易）。"""
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        outlet = _get_outlet(db, outlet_id)
    except LookupError as exc:
        return err(404, str(exc))
    try:
        amount = Decimal(str(payload.amount))
    except Exception:  # noqa: BLE001
        return err(422, f"验证金额非法：{payload.amount}")
    if not amount.is_finite() or amount <= 0:
        return err(422, f"验证金额必须 >0：{payload.amount}")
    outlet = cash_outlet_service.verify_outlet(
        db, outlet, amount=amount, result=payload.result,
        verified_at=payload.verified_at, valid_days=payload.valid_days)
    return ok({"enabled": True,
               "item": cash_outlet_service.serialize_outlet(outlet)})


# ---------------------------------------------------------------------------
# G4 现金出口执行台账（P1 人工 / P2 半自动 / P3 自动）
# ---------------------------------------------------------------------------
class CashOutletOpRequest(BaseModel):
    outlet_id: int
    amount: str
    mode: str = "P1"
    live_session_id: Optional[int] = None
    need_date: Optional[date] = None
    idempotency_key: Optional[str] = None
    notes: Optional[str] = None


class CashOutletOpConfirmRequest(BaseModel):
    approved_by: str


class CashOutletOpRejectRequest(BaseModel):
    reason: str


class CashOutletOpReturnRequest(BaseModel):
    principal_returned: str
    yield_amount: str = "0"
    shortfall_reason: Optional[str] = None
    returned_at: Optional[datetime] = None


def _outlet_ops_guard(session_id: Optional[int], mode: str) -> Optional[str]:
    """校验会话分级开关；返回错误信息或 None。"""
    if session_id is None:
        return None if mode.upper() in ("P1", "P2") else \
            "P3 自动模式必须绑定实盘会话（会话级 cash_outlet_mode=P3）"
    return None


@router.get("/outlet-ops")
def list_cash_outlet_ops(
    outlet_id: Optional[int] = Query(None, ge=1),
    live_session_id: Optional[int] = Query(None, ge=1),
    status: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=500),
    db: Session = Depends(get_db),
):
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    return ok(cash_outlet_service.list_ops(
        db, outlet_id=outlet_id, live_session_id=live_session_id,
        status=status, limit=limit))


@router.get("/outlet-ops/reconcile")
def reconcile_cash_outlet_ops(db: Session = Depends(get_db)):
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    return ok(cash_outlet_service.reconcile_ops(db))


@router.post("/outlet-ops")
def create_cash_outlet_op(payload: CashOutletOpRequest,
                          db: Session = Depends(get_db)):
    """登记出口计划：P1/P2 待人工确认，P3 满足硬条件才自动放行。

    只写台账，不划转、不下单、不改变组合现金与策略归因。
    """
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        mode = cash_outlet_service.normalize_mode(payload.mode)
    except cash_outlet_service.OutletOpsError as exc:
        return err(422, str(exc))
    guard = _outlet_ops_guard(payload.live_session_id, mode)
    if guard:
        return err(422, guard)
    if payload.live_session_id is not None:
        session = db.get(LiveSession, payload.live_session_id)
        if session is None:
            return err(404, f"实盘会话不存在：{payload.live_session_id}")
        if not cash_outlet_service.mode_allows(
                getattr(session, "cash_outlet_mode", "OFF"), mode):
            return err(
                422,
                "会话分级开关不允许 %s（当前 %s）"
                % (mode, getattr(session, "cash_outlet_mode", "OFF")))
    try:
        outlet = _get_outlet(db, payload.outlet_id)
        return ok(cash_outlet_service.plan_outlet_op(
            db, outlet, amount=payload.amount, mode=mode,
            live_session_id=payload.live_session_id,
            need_date=payload.need_date,
            idempotency_key=payload.idempotency_key,
            notes=payload.notes))
    except LookupError as exc:
        return err(404, str(exc))
    except cash_outlet_service.OutletOpsError as exc:
        return err(422, str(exc))


@router.post("/outlet-ops/{op_id}/confirm")
def confirm_cash_outlet_op(op_id: int, payload: CashOutletOpConfirmRequest,
                           db: Session = Depends(get_db)):
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        return ok(cash_outlet_service.confirm_op(
            db, op_id, approved_by=payload.approved_by))
    except LookupError as exc:
        return err(404, str(exc))
    except cash_outlet_service.OutletOpsError as exc:
        return err(409, str(exc))


@router.post("/outlet-ops/{op_id}/reject")
def reject_cash_outlet_op(op_id: int, payload: CashOutletOpRejectRequest,
                          db: Session = Depends(get_db)):
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        return ok(cash_outlet_service.reject_op(
            db, op_id, reason=payload.reason))
    except LookupError as exc:
        return err(404, str(exc))
    except cash_outlet_service.OutletOpsError as exc:
        return err(409, str(exc))


@router.post("/outlet-ops/{op_id}/return")
def return_cash_outlet_op(op_id: int, payload: CashOutletOpReturnRequest,
                          db: Session = Depends(get_db)):
    """登记回流；出口收益只写台账（yield_contribution），不进策略归因。"""
    if not _outlet_switches().cash_outlet_enabled:
        return _outlet_disabled()
    try:
        return ok(cash_outlet_service.record_return(
            db, op_id, principal_returned=payload.principal_returned,
            yield_amount=payload.yield_amount,
            shortfall_reason=payload.shortfall_reason,
            returned_at=payload.returned_at))
    except LookupError as exc:
        return err(404, str(exc))
    except cash_outlet_service.OutletOpsError as exc:
        return err(409, str(exc))
