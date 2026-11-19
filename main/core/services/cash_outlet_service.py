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

from core.models import CashOutlet

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
