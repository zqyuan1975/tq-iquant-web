"""TQ 通道闸（tq_gate）状态与手工紧急放开接口。

- ``GET  /api/system/tdx-gate``：当前时段窗口、P0/P1/P2 是否放行、下次 P2 可用时刻、
  闸统计（各级放行/拦截/等待耗时）——前端据此置灰回测类按钮并说明原因。
- ``POST /api/system/tdx-gate/override``：手工紧急放开时段限制（二次确认 + 原因 +
  有效期上限）。必须显式 ``confirm=true``；每次放开/解除都落一条决策事件，
  避免被当作日常绕过手段。
- ``POST /api/system/tdx-gate/override/clear``：立即解除。

只读接口与放开接口都不启停任何进程；放开在进程内生效、到期自动失效。
"""
from typing import Optional

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.models import LiveDecisionEvent, LiveSession
from core.tq import gate

router = APIRouter(prefix="/api/system/tdx-gate", tags=["system"])

_OVERRIDE_GATE = "tq_gate_override"


@router.get("")
def get_gate_status_route():
    return ok(gate.status())


@router.post("/override")
def post_gate_override_route(data: dict, db: Session = Depends(get_db)):
    """手工紧急放开时段限制（P2/P1 在交易时段可跑）。

    二次确认：必须 ``confirm=true``；``reason`` 必填；有效期受
    ``tq_gate.override_max_minutes`` 上限约束。配额（每分钟放行次数）仍然生效。
    """
    if not bool(data.get("confirm")):
        return err(400, "紧急放开需二次确认（confirm=true）")
    reason = str(data.get("reason") or "").strip()
    if not reason:
        return err(400, "紧急放开必须填写原因")
    try:
        minutes = int(data.get("minutes") or 30)
    except (TypeError, ValueError):
        return err(400, "minutes 必须为整数分钟")
    info = gate.set_override(
        minutes, reason, operator=str(data.get("operator") or ""))
    event_id = _write_event(
        db, action="override",
        message="TQ 通道闸手工放开 %s 分钟：%s" % (info["minutes"], reason))
    return ok({"override": info, "decision_event_id": event_id})


@router.post("/override/clear")
def post_gate_override_clear_route(data: Optional[dict] = None,
                                   db: Session = Depends(get_db)):
    data = data or {}
    reason = str(data.get("reason") or "手工解除").strip()
    existed = gate.clear_override()
    event_id = _write_event(
        db, action="override_clear",
        message="TQ 通道闸手工放开已解除：%s" % reason)
    return ok({"cleared": existed, "decision_event_id": event_id})


def _write_event(db: Session, action: str, message: str) -> Optional[int]:
    """落一条决策事件（优先运行中会话，其次最近会话；无会话则只记日志）。"""
    session = (
        db.query(LiveSession)
        .filter(LiveSession.status.in_(("running", "stopping")))
        .order_by(LiveSession.id.desc())
        .first()
    )
    if session is None:
        session = db.query(LiveSession).order_by(LiveSession.id.desc()).first()
    if session is None:
        gate.logger.warning("TQ 通道闸事件（无实盘会话可挂载）：%s", message)
        return None
    event = LiveDecisionEvent(
        live_session_id=session.id,
        gate=_OVERRIDE_GATE,
        layer="live_gate",
        action=action,
        message=message[:200],
    )
    db.add(event)
    db.commit()
    db.refresh(event)
    return event.id
