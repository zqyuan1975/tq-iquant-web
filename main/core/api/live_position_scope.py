"""按标的授权代卖（sell_scope=platform_only 下的逐票放行）。

背景：``tq_trade.live_sell_scope`` / ``live_sessions.sell_scope`` 默认
``platform_only``——存量/外部仓仍完整参与风控计算（止损/移动止损/止盈照常
触发并落事件），只在下单量收窄层被夹到 ``platform_quantity``，为 0 则跳过。

本模块提供不切全局 ``full`` 的替代方案：对单个标的显式「放开/收回代卖授权」。
- 默认无记录 = 不代卖（与改动前逐字一致）；
- 有启用记录 → ``_scope_sell_quantity`` 按整仓可卖量放行，T+1、涨跌停、审批、
  时点等既有闸门照旧；
- 不提供「一键全部授权」，也不改动 ``live_sell_scope`` / ``sell_scope`` 的值。

变更写审计事件（``live_decision_events``，gate=sell_scope_auth）：
谁、何时、放开/收回哪只；事件置 acked 以免计入「待处理」健康计数。
"""
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.models import LiveDecisionEvent, LivePositionScope, LiveSession

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/live-position-scope", tags=["live-position-scope"])

# 系统无鉴权：操作者固定留痕，不伪造身份（与 /live/sessions/{id}/stop 审计同口径）。
_OPERATOR = "无鉴权，无法识别操作者"


def _fmt_dt(value) -> Optional[str]:
    return value.strftime("%Y-%m-%d %H:%M:%S") if value else None


def _get_session(db: Session, session_id: int):
    return db.query(LiveSession).filter(LiveSession.id == session_id).first()


@router.get("/{session_id}")
def list_position_scopes(session_id: int, db: Session = Depends(get_db)):
    """该会话的代卖授权清单（含已收回行，enabled 区分；只读）。"""
    if _get_session(db, session_id) is None:
        return err(404, "资源不存在")
    rows = (db.query(LivePositionScope)
            .filter(LivePositionScope.session_id == session_id)
            .order_by(LivePositionScope.code).all())
    return ok({
        "session_id": session_id,
        "enabled_codes": [r.code for r in rows if r.enabled],
        "items": [{
            "code": r.code,
            "enabled": bool(r.enabled),
            "updated_at": _fmt_dt(r.updated_at),
            "updated_by": r.updated_by,
        } for r in rows],
    })


@router.put("/{session_id}/{code}")
def set_position_scope(session_id: int, code: str, data: dict,
                       db: Session = Depends(get_db)):
    """放开/收回单个标的的代卖授权。

    - body: ``{"enabled": true|false}``；true 才会让该票的卖单按整仓放行。
    - 下一轮 tick 生效（引擎每轮重读启用集合）。
    - 写审计事件：操作者/时间/代码/放开或收回。
    """
    if _get_session(db, session_id) is None:
        return err(404, "资源不存在")
    code = str(code or "").strip().upper()
    if not code:
        return err(400, "股票代码不能为空")
    if "enabled" not in data:
        return err(400, "缺少 enabled（true=放开代卖 / false=收回）")
    enabled = bool(data.get("enabled"))
    now = datetime.now()

    row = (db.query(LivePositionScope)
           .filter(LivePositionScope.session_id == session_id,
                   LivePositionScope.code == code).first())
    old = bool(row.enabled) if row is not None else False
    if row is None:
        row = LivePositionScope(session_id=session_id, code=code,
                                enabled=enabled, updated_by=_OPERATOR)
        db.add(row)
    else:
        row.enabled = enabled
        row.updated_by = _OPERATOR
        row.updated_at = now

    action = "grant" if enabled else "revoke"
    event = LiveDecisionEvent(
        live_session_id=session_id,
        gate="sell_scope_auth",
        layer="live_gate",
        action=action,
        stock_code=code,
        bar_time=now,
        message="代卖授权%s：%s（%s；操作者=%s）" % (
            "放开" if enabled else "收回", code,
            now.strftime("%Y-%m-%d %H:%M:%S"), _OPERATOR),
        # 审计事件：直接置已确认，避免计入「待处理」健康计数
        ack_status="acked",
        ack_at=now,
        ack_by=_OPERATOR,
        ack_note="代卖授权变更（审计，无需处理）",
    )
    db.add(event)
    db.commit()
    logger.warning("会话 #%s 代卖授权%s：%s（%s）", session_id,
                   "放开" if enabled else "收回", code, _OPERATOR)
    return ok({
        "session_id": session_id,
        "code": code,
        "enabled": enabled,
        "changed": old != enabled,
        "updated_by": _OPERATOR,
        "effective_at": "next_tick",
        "message": ("已放开代卖授权：%s（下一轮 tick 生效；T+1/涨跌停/审批/"
                    "时点等闸门照旧）" % code if enabled
                    else "已收回代卖授权：%s（下一轮 tick 恢复为仅卖平台仓）" % code),
    })
