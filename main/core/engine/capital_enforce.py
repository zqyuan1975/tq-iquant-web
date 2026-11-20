"""G1 实盘资金强制：拼盘借据 → 组合可用资金（0021）。

设计口径：
- 纯计算 + 内存 ``LoanBook``：无 IO、无时钟、不读配置；时间由调用方传入。
- 默认关闭：``LiveEngine`` 只有显式传入配置且会话开关为 True 才生效；
  关闭时 ``_buy_available_cash / _capital_share`` 的返回值与改动前逐字一致。
- 借款方组合可用现金 +credit、贷出方组合 −debit（两者都按借据未归还余额
  实时计算）；借据一经释放（kill switch / 归还）净额立即为 0。
- 恒等式：``Σ credit == Σ lot.outstanding == lent_total``，且
  ``LoanBook.check_invariants()`` 为空；构建失败直接返回 None（不半强制）。

v1 限制：组合级强制。L2 同组合内的策略级腾挪（借出策略减仓、借入策略加仓）
留待 G2 对账按策略口径接入；G1 先保证跨组合（L3）与组合级恒等式。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, List, Optional

from core.engine.capital_contracts import dec_str
from core.engine.capital_loan import (
    LoanError,
    LoanBook,
    Owner,
    REASON_MANUAL,
    SCOPE_PORTFOLIO,
)

ENFORCE_VERSION = "capital-enforce-g1"

_CENT = Decimal("0.01")


def _dec(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        out = Decimal(str(value))
    except (TypeError, ValueError):
        return None
    return out if out.is_finite() else None


def _shares(borrowers: List[dict]) -> List[Decimal]:
    """借款方权重：max_share ∈ (0,1] 优先，否则等权；归一化到合计 1。"""
    weights: List[Decimal] = []
    for item in borrowers:
        raw = _dec(item.get("max_share"))
        weights.append(raw if raw is not None and raw > 0 else Decimal(0))
    if not any(w > 0 for w in weights):
        weights = [Decimal(1)] * len(borrowers)
    total = sum(weights, Decimal(0))
    return [w / total for w in weights]


def build_enforce_config(
    *,
    loan_plan: Optional[dict],
    haircut: Optional[dict],
    strategy_portfolio: Dict[int, int],
    at: Optional[datetime] = None,
) -> Optional[dict]:
    """把拼盘借款计划落成可强制的额度配置；不可强制时返回 None。

    ``strategy_portfolio``：会话内 strategy_id → portfolio_id（多组合会话的
    唯一映射来源）；不在映射里的借款方跳过并告警，贷出方缺失则整份配置作废。
    """
    plan = loan_plan if isinstance(loan_plan, dict) else {}
    if not plan.get("enabled", True):
        return None
    principal = _dec(plan.get("principal"))
    if principal is None or principal <= 0:
        return None
    lender_sid = plan.get("lender_strategy_id")
    try:
        lender_sid = int(lender_sid)
    except (TypeError, ValueError):
        return None
    lender_pid = strategy_portfolio.get(lender_sid)
    if lender_pid is None:
        return None

    composite = _dec((haircut or {}).get("composite"))
    if composite is None or not (0 < composite <= 1):
        composite = Decimal(0)
    usable = (principal * composite).quantize(_CENT, rounding=ROUND_DOWN)
    if usable <= 0:
        return None

    borrowers = [
        b for b in (plan.get("borrowers") or [])
        if isinstance(b, dict) and _dec(b.get("strategy_id")) is not None
    ]
    if not borrowers:
        return None

    warnings: List[str] = []
    valid: List[dict] = []
    for item in borrowers:
        sid = int(_dec(item.get("strategy_id")))
        pid = strategy_portfolio.get(sid)
        if pid is None:
            warnings.append("enforce_borrower_not_in_session:%s" % sid)
            continue
        valid.append({
            "strategy_id": sid,
            "portfolio_id": pid,
            "max_share": item.get("max_share"),
        })
    if not valid:
        return None
    weights = _shares(valid)

    due_days = plan.get("due_days")
    try:
        due_days = max(int(due_days), 1)
    except (TypeError, ValueError):
        due_days = 7
    created_at = at or datetime.now()
    due_at = created_at + timedelta(days=due_days)

    book = LoanBook()
    lender = Owner("strategy", lender_sid)
    book.deposit_lendable(lender, usable)
    lent = Decimal(0)
    remaining = usable
    for idx, (item, weight) in enumerate(zip(valid, weights)):
        if idx == len(valid) - 1:
            amount = remaining
        else:
            amount = (usable * weight).quantize(_CENT, rounding=ROUND_DOWN)
            remaining -= amount
        if amount <= 0:
            warnings.append("enforce_credit_too_small:%s" % item["strategy_id"])
            continue
        borrower = Owner("strategy", item["strategy_id"])
        try:
            book.grant_credit(borrower, amount)
            book.lend(
                lender=lender,
                borrower=borrower,
                principal=amount,
                scope=SCOPE_PORTFOLIO,
                created_at=created_at,
                due_at=due_at,
                reason=REASON_MANUAL,
            )
        except LoanError as exc:
            warnings.append("enforce_loan_rejected:%s:%s"
                            % (item["strategy_id"], exc.code))
            continue
        lent += amount

    if lent <= 0:
        return None
    violations = book.check_invariants()
    if violations:
        # 账本自检不通过绝不进入强制路径（宁可退回现有行为）
        return None

    return {
        "version": ENFORCE_VERSION,
        "plan_id": plan.get("plan_id"),
        "lender_strategy_id": lender_sid,
        "lender_portfolio_id": lender_pid,
        "principal": principal,
        "haircut_composite": composite,
        "usable_principal": usable,
        "lent_total": lent,
        "due_at": due_at,
        "strategy_portfolio": dict(strategy_portfolio),
        "book": book,
        "warnings": warnings,
    }


def _net_by_portfolio(config: dict) -> Dict[int, Decimal]:
    cfg = config or {}
    book = cfg.get("book")
    mapping = cfg.get("strategy_portfolio") or {}
    out: Dict[int, Decimal] = {}
    if book is None:
        return out
    for lot in book.open_lots():
        amount = lot.outstanding
        if amount <= 0:
            continue
        borrower_pid = mapping.get(lot.borrower_id)
        lender_pid = mapping.get(lot.lender_id)
        if borrower_pid is not None:
            out[borrower_pid] = out.get(borrower_pid, Decimal(0)) + amount
        if lender_pid is not None:
            out[lender_pid] = out.get(lender_pid, Decimal(0)) - amount
    return out


def portfolio_delta(config: Optional[dict], portfolio_id: int) -> Decimal:
    """该组合的借据净额：借入为正、贷出为负；无借据为 0。"""
    return _net_by_portfolio(config or {}).get(portfolio_id, Decimal(0))


def strategy_delta(config: Optional[dict], strategy_id: int) -> Decimal:
    """策略口径净额（G2 对账用；G1 不下发到策略资金）。"""
    book = (config or {}).get("book")
    if book is None:
        return Decimal(0)
    borrower = sum(
        (lot.outstanding for lot in book.open_lots()
         if lot.borrower_id == strategy_id), Decimal(0))
    lender = sum(
        (lot.outstanding for lot in book.open_lots()
         if lot.lender_id == strategy_id), Decimal(0))
    return borrower - lender


def release_all(config: Optional[dict], at: datetime) -> int:
    """kill switch：全额归还所有未结借据（只回收不新增），返回释放笔数。"""
    book = (config or {}).get("book")
    if book is None or not isinstance(at, datetime):
        return 0
    released = 0
    for lot in list(book.open_lots()):
        if lot.outstanding <= 0:
            continue
        try:
            book.repay(lot.loan_id, lot.outstanding, at=at)
        except LoanError:
            continue
        released += 1
    return released


def snapshot(config: Optional[dict]) -> Dict[str, Any]:
    """只读摘要（事件/测试用）：金额统一字符串。"""
    cfg = config or {}
    book = cfg.get("book")
    return {
        "version": cfg.get("version"),
        "plan_id": cfg.get("plan_id"),
        "lender_strategy_id": cfg.get("lender_strategy_id"),
        "lender_portfolio_id": cfg.get("lender_portfolio_id"),
        "haircut_composite": dec_str(cfg.get("haircut_composite")),
        "usable_principal": dec_str(cfg.get("usable_principal")),
        "lent_total": dec_str(cfg.get("lent_total")),
        "outstanding": dec_str(book.total_outstanding()) if book else "0",
        "warnings": list(cfg.get("warnings") or []),
    }
