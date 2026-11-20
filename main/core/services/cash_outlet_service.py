"""现金出口白名单与 C1–C8 判据（0021 C4，只读影子，不下单）。

口径（0020 §13.2）：

- C1 通道可下单；C2 收益率数据；C3 流动性（T+0 / T+1 / 到期 ≤7 天）；
  C4 限额；C5 赎回/提前支取；C6 风险 R1/R2；C7 回流确定性；C8 通道验证；
- 未验证或验证过期一律 ``OUTLET_NOT_VERIFIED``，不给「历史收益高就能借」开口；
- 本模块只登记与评估配置，不产生委托、划转、收益，也不接实盘链路。
"""
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from core.models import CashOutlet, CashOutletOp

logger = logging.getLogger(__name__)

OUTLET_VERSION = "cash-outlet-c4"
STATUS_READY = "READY"
STATUS_BLOCKED = "BLOCKED"

CHANNEL_UNSUPPORTED = "CHANNEL_UNSUPPORTED"
YIELD_DATA_MISSING = "YIELD_DATA_MISSING"
LIQUIDITY_MISMATCH = "LIQUIDITY_MISMATCH"
LIMIT_TOO_SMALL = "LIMIT_TOO_SMALL"
REDEMPTION_RESTRICTED = "REDEMPTION_RESTRICTED"
RISK_LEVEL_HIGH = "RISK_LEVEL_HIGH"
SETTLEMENT_UNCERTAIN = "SETTLEMENT_UNCERTAIN"
OUTLET_NOT_VERIFIED = "OUTLET_NOT_VERIFIED"

ALL_CODES = (
    CHANNEL_UNSUPPORTED, YIELD_DATA_MISSING, LIQUIDITY_MISMATCH,
    LIMIT_TOO_SMALL, REDEMPTION_RESTRICTED, RISK_LEVEL_HIGH,
    SETTLEMENT_UNCERTAIN, OUTLET_NOT_VERIFIED,
)

ALLOWED_CHANNELS = ("tq", "qmt", "gj")   # manual 只能人工，不算可编程通道
ALLOWED_RISK_LEVELS = ("R1", "R2")
MAX_TERM_DAYS = 7
MIN_YIELD_HISTORY_DAYS = 60
VERIFY_VALID_DAYS = 365


def _dec(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:  # noqa: BLE001 — 脏配置按缺失处理
        return None


def _num_str(value: Any) -> Optional[str]:
    d = _dec(value)
    return format(d, "f") if d is not None else None


def _check(code: str, name: str, state: str, detail: str) -> Dict[str, Any]:
    return {"code": code, "name": name, "state": state, "detail": detail}


def evaluate_outlet(
    outlet: CashOutlet,
    *,
    need_date: Optional[date] = None,
    required_amount: Any = None,
    now: Optional[datetime] = None,
) -> Dict[str, Any]:
    """按 C1–C8 判定一个出口能否出借（只读，可复现）。

    ``need_date`` 给定时额外校验 C7「到期+缓冲 ≤ 借款方下次用钱日」；
    ``required_amount`` 给定时校验 C4 限额与递增单位。缺失的参数只跳过对应
    检查项，不把「没给金额」当成通过。
    """
    now = now or datetime.now()
    today = now.date()
    checks: List[Dict[str, Any]] = []
    codes: List[str] = []
    warnings: List[str] = []

    def fail(code: str, name: str, detail: str) -> None:
        codes.append(code)
        checks.append(_check(code, name, "fail", detail))

    # C1 通道可下单
    channel = (outlet.channel or "manual").lower()
    if not outlet.channel_order_enabled or channel not in ALLOWED_CHANNELS:
        fail(CHANNEL_UNSUPPORTED, "C1 通道可下单",
             "通道 %s 未开放可编程下单（仅接受 %s）"
             % (channel, "/".join(ALLOWED_CHANNELS)))
    else:
        checks.append(_check("C1", "C1 通道可下单", "pass",
                             "通道 %s 已开启下单" % channel))

    # C2 收益率数据
    yield_backtestable = False
    if not outlet.yield_source or not outlet.yield_freq:
        fail(YIELD_DATA_MISSING, "C2 收益率数据",
             "缺少收益率来源或频率，无法计息")
    elif (outlet.yield_history_days or 0) < MIN_YIELD_HISTORY_DAYS:
        warnings.append("not_backtestable")
        checks.append(_check(
            "C2", "C2 收益率数据", "warn",
            "历史 %s 日 < %d 日，只能用当前报价，标注不可回测"
            % (outlet.yield_history_days or 0, MIN_YIELD_HISTORY_DAYS)))
    else:
        yield_backtestable = True
        checks.append(_check(
            "C2", "C2 收益率数据", "pass",
            "日频序列 %s 日，可回测" % outlet.yield_history_days))

    # C3 流动性
    liquidity = (outlet.liquidity or "").lower()
    settle = int(outlet.settlement_max_days or 0)
    term = int(outlet.term_days or 0)
    if liquidity in ("t0", "t1"):
        if settle > 1:
            fail(LIQUIDITY_MISMATCH, "C3 流动性",
                 "%s 品种结算 %d 天，超过 1 天" % (liquidity, settle))
        else:
            checks.append(_check("C3", "C3 流动性", "pass",
                                 "%s，最晚 %d 天可用" % (liquidity, settle)))
    elif liquidity == "term":
        if term <= 0 or term > MAX_TERM_DAYS or settle > MAX_TERM_DAYS:
            fail(LIQUIDITY_MISMATCH, "C3 流动性",
                 "到期 %d 天/结算 %d 天，首期只接受 ≤%d 天"
                 % (term, settle, MAX_TERM_DAYS))
        else:
            checks.append(_check("C3", "C3 流动性", "pass",
                                 "到期 %d 天，结算 %d 天" % (term, settle)))
    else:
        fail(LIQUIDITY_MISMATCH, "C3 流动性",
             "流动性口径 %r 不受支持" % (outlet.liquidity,))

    # C4 限额
    min_amount = _dec(outlet.min_amount)
    max_single = _dec(outlet.max_single_amount)
    max_daily = _dec(outlet.max_daily_amount)
    increment = _dec(outlet.min_increment)
    limit_bad = None
    if min_amount is not None and max_single is not None \
            and min_amount > max_single:
        limit_bad = "最小金额 %s > 单笔上限 %s" % (min_amount, max_single)
    elif required_amount is not None:
        amount = _dec(required_amount)
        if amount is None or amount <= 0:
            limit_bad = "申请金额非法：%r" % (required_amount,)
        elif min_amount is not None and amount < min_amount:
            limit_bad = "申请金额 %s < 最小金额 %s" % (amount, min_amount)
        elif max_single is not None and amount > max_single:
            limit_bad = "申请金额 %s > 单笔上限 %s" % (amount, max_single)
        elif max_daily is not None and amount > max_daily:
            limit_bad = "申请金额 %s > 单日上限 %s" % (amount, max_daily)
        elif increment is not None and increment > 0 and min_amount is not None \
                and (amount - min_amount) % increment != 0:
            limit_bad = "申请金额不满足递增单位 %s" % increment
    elif increment is not None and increment <= 0:
        limit_bad = "递增单位必须 >0"
    if limit_bad:
        fail(LIMIT_TOO_SMALL, "C4 限额", limit_bad)
    else:
        checks.append(_check("C4", "C4 限额", "pass",
                             "限额可配置且能事前校验"))

    # C5 赎回/提前支取
    if not outlet.redeemable_early and not outlet.auto_mature_return:
        fail(REDEMPTION_RESTRICTED, "C5 赎回",
             "不可提前赎回且无到期自动回款，退出不确定")
    else:
        checks.append(_check(
            "C5", "C5 赎回", "pass",
            "可提前赎回" if outlet.redeemable_early else "到期自动回款"))

    # C6 风险等级
    risk = (outlet.risk_level or "").upper()
    if risk not in ALLOWED_RISK_LEVELS:
        fail(RISK_LEVEL_HIGH, "C6 风险等级",
             "风险等级 %r 不在 %s" % (outlet.risk_level,
                                     "/".join(ALLOWED_RISK_LEVELS)))
    else:
        checks.append(_check("C6", "C6 风险等级", "pass", "风险等级 %s" % risk))

    # C7 回流确定性
    buffer_days = int(outlet.arrival_buffer_days or 0)
    arrival_days = max(settle, term if liquidity == "term" else 0) + buffer_days
    if need_date is not None:
        arrival = today + timedelta(days=arrival_days)
        if arrival > need_date:
            fail(SETTLEMENT_UNCERTAIN, "C7 回流确定性",
                 "预计 %s 可用，晚于用钱日 %s" % (arrival, need_date))
        else:
            checks.append(_check(
                "C7", "C7 回流确定性", "pass",
                "预计 %s 可用，早于用钱日 %s" % (arrival, need_date)))
    elif (outlet.inflow_certainty or "").lower() == "probable":
        warnings.append("inflow_probable")
        checks.append(_check("C7", "C7 回流确定性", "warn",
                             "回流仅「可能」，不能作为确定额度"))
    else:
        checks.append(_check("C7", "C7 回流确定性", "pass",
                             "预计最晚 %d 天可用" % arrival_days))

    # C8 通道验证
    expires = outlet.verify_expires_at
    if not outlet.verified or not outlet.verified_at or expires is None \
            or expires < today:
        fail(OUTLET_NOT_VERIFIED, "C8 通道验证",
             "未验证或验证已过期（有效期 %d 天）" % VERIFY_VALID_DAYS)
    else:
        checks.append(_check(
            "C8", "C8 通道验证", "pass",
            "已验证，有效期至 %s" % expires))

    return {
        "version": OUTLET_VERSION,
        "outlet_id": outlet.id,
        "code": outlet.code,
        "name": outlet.name,
        "status": STATUS_BLOCKED if codes else STATUS_READY,
        "codes": codes,
        "checks": checks,
        "warnings": warnings,
        "yield_backtestable": yield_backtestable,
        "needed_amount": _num_str(required_amount),
        "need_date": need_date.isoformat() if need_date else None,
        "decided_at": now.isoformat(),
    }


def serialize_outlet(outlet: CashOutlet) -> Dict[str, Any]:
    """出口配置的可读字段（金额统一为字符串，避免浮点误差）。"""
    return {
        "id": outlet.id,
        "code": outlet.code,
        "name": outlet.name,
        "category": outlet.category,
        "channel": outlet.channel,
        "channel_order_enabled": bool(outlet.channel_order_enabled),
        "yield_source": outlet.yield_source,
        "yield_freq": outlet.yield_freq,
        "yield_history_days": outlet.yield_history_days,
        "liquidity": outlet.liquidity,
        "term_days": outlet.term_days,
        "settlement_max_days": outlet.settlement_max_days,
        "holiday_postpone": bool(outlet.holiday_postpone),
        "min_amount": _num_str(outlet.min_amount),
        "max_single_amount": _num_str(outlet.max_single_amount),
        "max_daily_amount": _num_str(outlet.max_daily_amount),
        "min_increment": _num_str(outlet.min_increment),
        "redeemable_early": bool(outlet.redeemable_early),
        "early_redeem_penalty": _num_str(outlet.early_redeem_penalty),
        "auto_mature_return": bool(outlet.auto_mature_return),
        "fast_redeem_limit": _num_str(outlet.fast_redeem_limit),
        "risk_level": outlet.risk_level,
        "principal_guaranteed": bool(outlet.principal_guaranteed),
        "arrival_buffer_days": outlet.arrival_buffer_days,
        "inflow_certainty": outlet.inflow_certainty,
        "verified": bool(outlet.verified),
        "verified_at": outlet.verified_at.isoformat()
        if outlet.verified_at else None,
        "verify_amount": _num_str(outlet.verify_amount),
        "verify_result": outlet.verify_result,
        "verify_expires_at": outlet.verify_expires_at.isoformat()
        if outlet.verify_expires_at else None,
        "status": outlet.status,
        "notes": outlet.notes,
        "created_at": outlet.created_at.isoformat()
        if outlet.created_at else None,
        "updated_at": outlet.updated_at.isoformat()
        if outlet.updated_at else None,
    }


def create_outlet(db: Session, payload: Dict[str, Any]) -> CashOutlet:
    code = (payload.get("code") or "").strip()
    if not code:
        raise ValueError("出口代码不能为空")
    if db.query(CashOutlet).filter(CashOutlet.code == code).first():
        raise ValueError("出口代码已存在：%s" % code)
    outlet = CashOutlet(code=code, name=(payload.get("name") or code).strip())
    _apply(db, outlet, payload, creating=True)
    db.add(outlet)
    db.commit()
    db.refresh(outlet)
    return outlet


def update_outlet(db: Session, outlet: CashOutlet,
                  payload: Dict[str, Any]) -> CashOutlet:
    _apply(db, outlet, payload, creating=False)
    db.commit()
    db.refresh(outlet)
    return outlet


_FIELDS = (
    "name", "category", "channel", "channel_order_enabled", "yield_source",
    "yield_freq", "yield_history_days", "liquidity", "term_days",
    "settlement_max_days", "holiday_postpone", "min_amount",
    "max_single_amount", "max_daily_amount", "min_increment",
    "redeemable_early", "early_redeem_penalty", "auto_mature_return",
    "fast_redeem_limit", "risk_level", "principal_guaranteed",
    "arrival_buffer_days", "inflow_certainty", "status", "notes",
)


def _apply(db: Session, outlet: CashOutlet, payload: Dict[str, Any],
           *, creating: bool) -> None:
    for field in _FIELDS:
        if field not in payload:
            continue
        value = payload[field]
        if value is not None and field in (
                "min_amount", "max_single_amount", "max_daily_amount",
                "min_increment", "early_redeem_penalty", "fast_redeem_limit"):
            value = _dec(value)
        setattr(outlet, field, value)
    if creating:
        outlet.name = outlet.name or outlet.code


def verify_outlet(db: Session, outlet: CashOutlet, *,
                  amount: Any, result: str,
                  verified_at: Optional[datetime] = None,
                  valid_days: int = VERIFY_VALID_DAYS) -> CashOutlet:
    """登记 C8 小额实盘验证证据（只记录，不发起任何交易）。"""
    at = verified_at or datetime.now()
    outlet.verified = True
    outlet.verified_at = at
    outlet.verify_amount = _dec(amount)
    outlet.verify_result = (result or "")[:200]
    outlet.verify_expires_at = (at + timedelta(days=max(1, int(valid_days)))).date()
    db.commit()
    db.refresh(outlet)
    return outlet


# ---------------------------------------------------------------------------
# G4 现金出口执行台账（人工 → 半自动 → 自动）
# ---------------------------------------------------------------------------
OUTLET_OPS_VERSION = "cash-outlet-ops-g4"
MODE_OFF = "OFF"
MODE_P1 = "P1"
MODE_P2 = "P2"
MODE_P3 = "P3"
MODE_ORDER = {MODE_OFF: 0, MODE_P1: 1, MODE_P2: 2, MODE_P3: 3}

OP_PENDING = "PENDING_CONFIRM"
OP_EXECUTED = "EXECUTED"
OP_RETURNED = "RETURNED"
OP_REJECTED = "REJECTED"
OP_CANCELLED = "CANCELLED"
OP_ACTIVE = (OP_PENDING, OP_EXECUTED)
OP_STATUSES = (OP_PENDING, OP_EXECUTED, OP_RETURNED, OP_REJECTED, OP_CANCELLED)


class OutletOpsError(ValueError):
    """现金出口操作被拒；``codes`` 保留 C1–C8 原因码，便于追溯。"""

    def __init__(self, message: str, codes: Optional[List[str]] = None):
        super().__init__(message)
        self.codes = list(codes or [])


def normalize_mode(value: Any) -> str:
    mode = str(value or MODE_OFF).upper()
    if mode not in MODE_ORDER:
        raise OutletOpsError("模式非法：%r（仅 OFF/P1/P2/P3）" % value)
    return mode


def mode_allows(session_mode: Any, requested: Any) -> bool:
    """会话分级开关是否允许该操作模式（OFF 一律不允许）。"""
    try:
        return MODE_ORDER.get(normalize_mode(session_mode), 0) >= \
            MODE_ORDER[normalize_mode(requested)]
    except OutletOpsError:
        return False


def p3_eligible(outlet: CashOutlet, now: Optional[datetime] = None) -> tuple:
    """P3 自动放行的硬条件；返回 (是否满足, 原因列表)。"""
    now = now or datetime.now()
    result = evaluate_outlet(outlet, now=now)
    reasons = list(result["codes"])
    channel = (outlet.channel or "manual").lower()
    liquidity = (outlet.liquidity or "").lower()
    term = int(outlet.term_days or 0)
    settle = int(outlet.settlement_max_days or 0)
    if channel not in ALLOWED_CHANNELS:
        reasons.append(CHANNEL_UNSUPPORTED)
    if (outlet.risk_level or "").upper() not in ALLOWED_RISK_LEVELS:
        reasons.append(RISK_LEVEL_HIGH)
    if liquidity == "term" and (term <= 0 or term > MAX_TERM_DAYS
                                or settle > MAX_TERM_DAYS):
        reasons.append(LIQUIDITY_MISMATCH)
    if liquidity not in ("t0", "t1", "term"):
        reasons.append(LIQUIDITY_MISMATCH)
    if (outlet.inflow_certainty or "").lower() == "probable":
        reasons.append(SETTLEMENT_UNCERTAIN)
    return (not reasons), sorted(set(reasons))


def _dec_amount(value: Any) -> Decimal:
    amount = _dec(value)
    if amount is None or not amount.is_finite() or amount <= 0:
        raise OutletOpsError("金额非法：%r" % (value,))
    return amount


def _serialize_op(op: CashOutletOp) -> Dict[str, Any]:
    return {
        "id": op.id,
        "version": OUTLET_OPS_VERSION,
        "live_session_id": op.live_session_id,
        "outlet_id": op.outlet_id,
        "mode": op.mode,
        "amount": _num_str(op.amount),
        "status": op.status,
        "idempotency_key": op.idempotency_key,
        "executed_at": op.executed_at.isoformat(sep=" ") if op.executed_at else None,
        "expected_return_date": op.expected_return_date.isoformat()
        if op.expected_return_date else None,
        "returned_at": op.returned_at.isoformat(sep=" ") if op.returned_at else None,
        "principal_returned": _num_str(op.principal_returned),
        "yield_amount": _num_str(op.yield_amount) or "0",
        "shortfall_amount": _num_str(op.shortfall_amount) or "0",
        "shortfall_reason": op.shortfall_reason,
        "approved_by": op.approved_by,
        "approved_at": op.approved_at.isoformat(sep=" ") if op.approved_at else None,
        "notes": op.notes,
        "created_at": op.created_at.isoformat(sep=" ") if op.created_at else None,
    }


def _daily_planned(db: Session, outlet_id: int, day: date) -> Decimal:
    rows = (db.query(CashOutletOp)
            .filter(CashOutletOp.outlet_id == outlet_id,
                    CashOutletOp.status.in_(OP_ACTIVE))
            .all())
    total = Decimal("0")
    for row in rows:
        stamp = row.executed_at or row.created_at
        if stamp is not None and stamp.date() == day:
            total += row.amount or Decimal("0")
    return total


def plan_outlet_op(db: Session, outlet: CashOutlet, *, amount: Any,
                   mode: Any = MODE_P1,
                   live_session_id: Optional[int] = None,
                   need_date: Optional[date] = None,
                   idempotency_key: Optional[str] = None,
                   notes: Optional[str] = None,
                   now: Optional[datetime] = None) -> Dict[str, Any]:
    """登记一笔出口计划；P3 满足硬条件才自动放行，否则转人工确认。

    只写 ``cash_outlet_ops``，不划转、不下单、不改组合现金/净值。
    """
    now = now or datetime.now()
    mode = normalize_mode(mode)
    if mode == MODE_OFF:
        raise OutletOpsError("模式 OFF 不允许登记现金出口操作")
    amount = _dec_amount(amount)

    key = (idempotency_key or "").strip() or None
    if key:
        existing = (db.query(CashOutletOp)
                    .filter(CashOutletOp.idempotency_key == key).first())
        if existing is not None:
            data = _serialize_op(existing)
            data["deduplicated"] = True
            return data

    result = evaluate_outlet(
        outlet, need_date=need_date, required_amount=amount, now=now)
    if result["status"] == STATUS_BLOCKED:
        raise OutletOpsError(
            "出口 %s 未通过 C1–C8：%s" % (outlet.code, "；".join(result["codes"])),
            result["codes"])

    max_daily = _dec(outlet.max_daily_amount)
    if max_daily is not None:
        planned = _daily_planned(db, outlet.id, now.date())
        if planned + amount > max_daily:
            raise OutletOpsError(
                "超过单日上限：已计划 %s + 本次 %s > %s"
                % (planned, amount, max_daily), [LIMIT_TOO_SMALL])

    settle = int(outlet.settlement_max_days or 0)
    term = int(outlet.term_days or 0) \
        if (outlet.liquidity or "").lower() == "term" else 0
    arrival = max(settle, term) + int(outlet.arrival_buffer_days or 0)
    expected = now.date() + timedelta(days=arrival)

    auto = False
    auto_reasons: List[str] = []
    if mode == MODE_P3:
        auto, auto_reasons = p3_eligible(outlet, now=now)

    op = CashOutletOp(
        live_session_id=live_session_id, outlet_id=outlet.id, mode=mode,
        amount=amount,
        status=OP_EXECUTED if auto else OP_PENDING,
        idempotency_key=key,
        executed_at=now if auto else None,
        expected_return_date=expected,
        approved_by="auto:p3" if auto else None,
        approved_at=now if auto else None,
        notes=(notes or "") if auto else (
            (notes or "") if not auto_reasons else
            ((notes + "；") if notes else "")
            + "P3 条件不满足，转人工确认：" + "/".join(auto_reasons)),
    )
    db.add(op)
    db.commit()
    db.refresh(op)
    data = _serialize_op(op)
    data["auto"] = auto
    data["auto_blocked_reasons"] = auto_reasons
    data["evaluation"] = result
    return data


def confirm_op(db: Session, op_id: int, *, approved_by: str,
               now: Optional[datetime] = None) -> Dict[str, Any]:
    """人工确认待投放计划；确认只代表台账放行，不产生真实下单。"""
    op = db.get(CashOutletOp, op_id)
    if op is None:
        raise LookupError("出口操作不存在：%s" % op_id)
    if op.status != OP_PENDING:
        raise OutletOpsError("仅待确认状态可确认，当前 %s" % op.status)
    if not (approved_by or "").strip():
        raise OutletOpsError("确认必须填写操作人")
    now = now or datetime.now()
    op.status = OP_EXECUTED
    op.executed_at = now
    op.approved_by = approved_by.strip()
    op.approved_at = now
    db.commit()
    db.refresh(op)
    return _serialize_op(op)


def reject_op(db: Session, op_id: int, *, reason: str,
              now: Optional[datetime] = None) -> Dict[str, Any]:
    op = db.get(CashOutletOp, op_id)
    if op is None:
        raise LookupError("出口操作不存在：%s" % op_id)
    if op.status != OP_PENDING:
        raise OutletOpsError("仅待确认状态可驳回，当前 %s" % op.status)
    if not (reason or "").strip():
        raise OutletOpsError("驳回必须填写理由")
    op.status = OP_REJECTED
    op.notes = ((op.notes + "；") if op.notes else "") + "驳回：" + reason.strip()
    db.commit()
    db.refresh(op)
    return _serialize_op(op)


def record_return(db: Session, op_id: int, *, principal_returned: Any,
                  yield_amount: Any = 0,
                  shortfall_reason: Optional[str] = None,
                  returned_at: Optional[datetime] = None) -> Dict[str, Any]:
    """登记回流：收益只写本表，短款必须带原因（可追溯）。"""
    op = db.get(CashOutletOp, op_id)
    if op is None:
        raise LookupError("出口操作不存在：%s" % op_id)
    if op.status != OP_EXECUTED:
        raise OutletOpsError("仅已投放状态可登记回流，当前 %s" % op.status)
    principal = _dec(principal_returned)
    if principal is None or not principal.is_finite() or principal < 0:
        raise OutletOpsError("回流本金非法：%r" % (principal_returned,))
    gain = _dec(yield_amount) or Decimal("0")
    if not gain.is_finite() or gain < 0:
        raise OutletOpsError("出口收益非法：%r" % (yield_amount,))
    shortfall = (op.amount or Decimal("0")) - principal
    if shortfall > 0 and not (shortfall_reason or "").strip():
        raise OutletOpsError(
            "存在短款 %s，必须填写 shortfall_reason" % shortfall)
    now = returned_at or datetime.now()
    op.status = OP_RETURNED
    op.returned_at = now
    op.principal_returned = principal
    op.yield_amount = gain
    op.shortfall_amount = max(Decimal("0"), shortfall)
    op.shortfall_reason = (shortfall_reason or "").strip() or None
    db.commit()
    db.refresh(op)
    return _serialize_op(op)


def list_ops(db: Session, *, outlet_id: Optional[int] = None,
             live_session_id: Optional[int] = None,
             status: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    q = db.query(CashOutletOp)
    if outlet_id is not None:
        q = q.filter(CashOutletOp.outlet_id == outlet_id)
    if live_session_id is not None:
        q = q.filter(CashOutletOp.live_session_id == live_session_id)
    if status:
        q = q.filter(CashOutletOp.status == status)
    rows = q.order_by(CashOutletOp.id.desc()).limit(max(1, int(limit))).all()
    summary = ops_summary(db, outlet_id=outlet_id,
                          live_session_id=live_session_id)
    return {"version": OUTLET_OPS_VERSION, "items": [
        _serialize_op(op) for op in rows], "summary": summary}


def ops_summary(db: Session, *, outlet_id: Optional[int] = None,
                live_session_id: Optional[int] = None) -> Dict[str, Any]:
    q = db.query(CashOutletOp)
    if outlet_id is not None:
        q = q.filter(CashOutletOp.outlet_id == outlet_id)
    if live_session_id is not None:
        q = q.filter(CashOutletOp.live_session_id == live_session_id)
    rows = q.all()
    outstanding = sum((op.amount or Decimal("0") for op in rows
                       if op.status == OP_EXECUTED), Decimal("0"))
    returned = sum((op.principal_returned or Decimal("0") for op in rows
                    if op.status == OP_RETURNED), Decimal("0"))
    yield_total = sum((op.yield_amount or Decimal("0") for op in rows
                       if op.status == OP_RETURNED), Decimal("0"))
    shortfall = sum((op.shortfall_amount or Decimal("0") for op in rows
                     if op.status == OP_RETURNED), Decimal("0"))
    return {
        "version": OUTLET_OPS_VERSION,
        # 出口收益单列，绝不并入策略/组合归因
        "outstanding_principal": _num_str(outstanding) or "0",
        "returned_principal": _num_str(returned) or "0",
        "yield_contribution": _num_str(yield_total) or "0",
        "shortfall_total": _num_str(shortfall) or "0",
        "pending_count": sum(1 for op in rows if op.status == OP_PENDING),
        "executed_count": sum(1 for op in rows if op.status == OP_EXECUTED),
        "returned_count": sum(1 for op in rows if op.status == OP_RETURNED),
    }


def reconcile_ops(db: Session, *, today: Optional[date] = None) -> Dict[str, Any]:
    """台账对账：逾期未回、短款未说明、金额异常，全部给出可追溯 id。"""
    today = today or datetime.now().date()
    rows = db.query(CashOutletOp).all()
    overdue = []
    shortfalls = []
    anomalies = []
    for op in rows:
        if op.status == OP_EXECUTED and op.expected_return_date \
                and op.expected_return_date < today:
            overdue.append({
                "id": op.id, "outlet_id": op.outlet_id,
                "amount": _num_str(op.amount),
                "expected_return_date": op.expected_return_date.isoformat(),
            })
        if op.status == OP_RETURNED and (op.shortfall_amount or 0) > 0:
            shortfalls.append({
                "id": op.id, "outlet_id": op.outlet_id,
                "shortfall_amount": _num_str(op.shortfall_amount),
                "shortfall_reason": op.shortfall_reason,
            })
        if (op.amount or Decimal("0")) <= 0:
            anomalies.append({"id": op.id, "reason": "amount<=0"})
        if op.status == OP_RETURNED and (op.shortfall_amount or 0) > 0 \
                and not op.shortfall_reason:
            anomalies.append({"id": op.id, "reason": "shortfall_without_reason"})
    return {
        "version": OUTLET_OPS_VERSION,
        "ok": not overdue and not anomalies,
        "overdue": overdue,
        "shortfalls": shortfalls,
        "anomalies": anomalies,
        "checked_at": datetime.now().isoformat(sep=" ", timespec="seconds"),
    }
