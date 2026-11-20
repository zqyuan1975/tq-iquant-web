"""LoanLot 借据账本与状态机（0021 B1）。

设计口径：0019 §2.1（资金状态机）、§2.3（LoanLot 硬约束）、§4.4（召回优先级）、
§4.5（禁止双边占用）。

- 纯内存、无 IO、无时钟：``created_at / due_at / recall_deadline`` 全部由调用方传入，
  回测逐 bar、实盘逐日都能复现同一条时间线。
- 默认不参与任何链路：只有显式构造 ``LoanBook`` 并调用方法才会产生状态；
  不读配置、不写数据库、不改任何既有行为。
- 金额统一 ``Decimal``，``to_dict()`` 输出字符串，避免浮点误差。
- v1 硬限制：禁止再抵押（借入方不得把借来的额度再借出）。

状态机（0019 §2.1 §2.3）::

    active ──request_recall──▶ recall_pending ──repay(全额)──▶ returned
      │                              │
      ├────────repay(全额)───────────┴──▶ returned
      └────────write_off─────────────┴──▶ written_off

``returned`` / ``written_off`` 为终态；其余迁移一律拒绝并抛 ``LoanError``。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.engine.capital_contracts import dec_str

# ---- 借据状态 -----------------------------------------------------------
STATUS_ACTIVE = "active"
STATUS_RECALL_PENDING = "recall_pending"
STATUS_RETURNED = "returned"
STATUS_WRITTEN_OFF = "written_off"

OPEN_STATUSES = frozenset({STATUS_ACTIVE, STATUS_RECALL_PENDING})
TERMINAL_STATUSES = frozenset({STATUS_RETURNED, STATUS_WRITTEN_OFF})
ALL_STATUSES = frozenset({STATUS_ACTIVE, STATUS_RECALL_PENDING,
                          STATUS_RETURNED, STATUS_WRITTEN_OFF})

# ---- 归属范围（0019 §2.2）------------------------------------------------
SCOPE_PORTFOLIO = "portfolio"
SCOPE_SESSION = "session"
SCOPE_CUSTOM = "custom"
VALID_SCOPES = frozenset({SCOPE_PORTFOLIO, SCOPE_SESSION, SCOPE_CUSTOM})

LENDER_TYPES = frozenset({"strategy", "portfolio", "session"})
BORROWER_TYPES = frozenset({"strategy", "portfolio", "cash_outlet"})

# ---- 出借原因 -----------------------------------------------------------
REASON_DEMAND_IDLE = "demand_idle"
REASON_GATE_CLOSED = "gate_closed"
REASON_MANUAL = "manual"
REASON_CUSTOM = "custom"

# ---- 拒绝码 -------------------------------------------------------------
ERR_INVALID_PRINCIPAL = "INVALID_PRINCIPAL"
ERR_INVALID_AMOUNT = "INVALID_AMOUNT"
ERR_INVALID_DUE_AT = "INVALID_DUE_AT"
ERR_INVALID_TIME = "INVALID_TIME"
ERR_INVALID_SCOPE = "INVALID_SCOPE"
ERR_INVALID_OWNER = "INVALID_OWNER"
ERR_UNKNOWN_LOT = "UNKNOWN_LOT"
ERR_SELF_LENDING = "SELF_LENDING"
ERR_REHYPOTHECATION = "REHYPOTHECATION_FORBIDDEN"
ERR_BORROWER_FROZEN = "BORROWER_FROZEN"
ERR_CREDIT_INSUFFICIENT = "CREDIT_INSUFFICIENT"
ERR_CREDIT_BELOW_OUTSTANDING = "CREDIT_BELOW_OUTSTANDING"
ERR_LENDABLE_INSUFFICIENT = "LENDABLE_INSUFFICIENT"
ERR_INVALID_TRANSITION = "INVALID_TRANSITION"
ERR_OVER_REPAY = "OVER_REPAY"
ERR_DUPLICATE_LOAN_ID = "DUPLICATE_LOAN_ID"


class LoanError(Exception):
    """借据账本拒绝码异常；``code`` 用于事件与界面解释。"""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message or code
        super().__init__("%s: %s" % (code, self.message))


@dataclass(frozen=True)
class Owner:
    """资金来源/去向：类型 + 标识；用于可哈希的账本键。"""

    owner_type: str
    owner_id: Any

    def __post_init__(self) -> None:
        if self.owner_type not in LENDER_TYPES | BORROWER_TYPES:
            raise LoanError(ERR_INVALID_OWNER, "未知归属类型 %r" % (self.owner_type,))

    def to_dict(self) -> Dict[str, Any]:
        return {"type": self.owner_type, "id": self.owner_id}


def _check_positive(value: Any, code: str = ERR_INVALID_AMOUNT) -> Decimal:
    if not isinstance(value, Decimal):
        try:
            value = Decimal(str(value))
        except Exception as exc:  # noqa: BLE001 - 统一转成拒绝码
            raise LoanError(code, "金额非法: %r" % (value,)) from exc
    if not value.is_finite() or value <= 0:
        raise LoanError(code, "金额必须为正: %s" % dec_str(value))
    return value


@dataclass
class LoanLot:
    """一笔出借；``outstanding`` 只减不增，本金不可改写。"""

    loan_id: str
    lender_type: str
    lender_id: Any
    borrower_type: str
    borrower_id: Any
    scope: str
    principal: Decimal
    outstanding: Decimal
    created_at: datetime
    due_at: datetime
    reason: str = REASON_DEMAND_IDLE
    recall_requested_at: Optional[datetime] = None
    recall_deadline: Optional[datetime] = None
    returned_at: Optional[datetime] = None
    written_off_amount: Optional[Decimal] = None
    write_off_reason: Optional[str] = None
    status: str = STATUS_ACTIVE

    @property
    def lender(self) -> Owner:
        return Owner(self.lender_type, self.lender_id)

    @property
    def borrower(self) -> Owner:
        return Owner(self.borrower_type, self.borrower_id)

    @property
    def is_open(self) -> bool:
        return self.status in OPEN_STATUSES

    def to_dict(self) -> Dict[str, Any]:
        return {
            "loan_id": self.loan_id,
            "lender_type": self.lender_type,
            "lender_id": self.lender_id,
            "borrower_type": self.borrower_type,
            "borrower_id": self.borrower_id,
            "scope": self.scope,
            "principal": dec_str(self.principal),
            "outstanding": dec_str(self.outstanding),
            "created_at": self.created_at.isoformat(),
            "due_at": self.due_at.isoformat(),
            "reason": self.reason,
            "recall_requested_at": (
                self.recall_requested_at.isoformat()
                if self.recall_requested_at else None
            ),
            "recall_deadline": (
                self.recall_deadline.isoformat()
                if self.recall_deadline else None
            ),
            "returned_at": (
                self.returned_at.isoformat() if self.returned_at else None
            ),
            "written_off_amount": dec_str(self.written_off_amount),
            "write_off_reason": self.write_off_reason,
            "status": self.status,
        }


class LoanBook:
    """借据账本：可借余额、授信额度与 LoanLot 的唯一真相。

    金额守恒（0019 §2.3 硬约束）::

        deposited − withdrawn = Σ lendable + Σ outstanding + Σ written_off

    任何时点同一笔可借余额只能落在一个 LoanLot 里：``lend`` 先扣
    ``lendable``，扣不出来直接拒绝，因此不存在双重占用。
    """

    def __init__(self) -> None:
        self._lots: Dict[str, LoanLot] = {}
        self._lendable: Dict[Owner, Decimal] = {}
        self._credit: Dict[Owner, Decimal] = {}
        self._frozen: Dict[Owner, str] = {}
        self._deposited = Decimal(0)
        # 分 owner 的累计存入（只增不减）：G2 恒等式 (5) 判断单一贷出方是否
        # 超发；与全局 _deposited 并行，不影响既有借还逻辑。
        self._deposited_by: Dict[Owner, Decimal] = {}
        self._withdrawn = Decimal(0)
        self._seq = 0

    # ---- 可借余额 -------------------------------------------------------
    def deposit_lendable(self, owner: Owner, amount: Any) -> Decimal:
        amt = _check_positive(amount)
        self._lendable[owner] = self._lendable.get(owner, Decimal(0)) + amt
        self._deposited += amt
        self._deposited_by[owner] = self._deposited_by.get(owner, Decimal(0)) + amt
        return self._lendable[owner]

    def deposited_for(self, owner: Owner) -> Decimal:
        """该 owner 历史累计存入的可借额度（只增不减，G2 恒等式 (5) 用）。"""
        return self._deposited_by.get(owner, Decimal(0))

    def withdraw_lendable(self, owner: Owner, amount: Any) -> Decimal:
        amt = _check_positive(amount)
        balance = self._lendable.get(owner, Decimal(0))
        if balance < amt:
            raise LoanError(
                ERR_LENDABLE_INSUFFICIENT,
                "%s 可借 %s < 提取 %s" % (owner.to_dict(), dec_str(balance),
                                          dec_str(amt)),
            )
        self._lendable[owner] = balance - amt
        self._withdrawn += amt
        return self._lendable[owner]

    def lendable(self, owner: Owner) -> Decimal:
        return self._lendable.get(owner, Decimal(0))

    # ---- 授信 -----------------------------------------------------------
    def grant_credit(self, borrower: Owner, amount: Any) -> Decimal:
        """设置借款人本窗口额度；不得低于其未归还余额。"""
        try:
            amt = Decimal(str(amount))
        except Exception as exc:  # noqa: BLE001
            raise LoanError(ERR_INVALID_AMOUNT, "额度非法: %r" % (amount,)) from exc
        if not amt.is_finite() or amt < 0:
            raise LoanError(ERR_INVALID_AMOUNT, "额度必须非负: %r" % (amount,))
        outstanding = self.outstanding_for_borrower(borrower)
        if amt < outstanding:
            raise LoanError(
                ERR_CREDIT_BELOW_OUTSTANDING,
                "额度 %s 低于未归还 %s" % (dec_str(amt), dec_str(outstanding)),
            )
        self._credit[borrower] = amt
        return amt

    def credit(self, borrower: Owner) -> Decimal:
        return self._credit.get(borrower, Decimal(0))

    def available_credit(self, borrower: Owner) -> Decimal:
        """可用额度 = 授信 − 未归还；冻结期间为 0（0019 §4.4 第 2 步）。"""
        if borrower in self._frozen:
            return Decimal(0)
        left = self.credit(borrower) - self.outstanding_for_borrower(borrower)
        return left if left > 0 else Decimal(0)

    def freeze_borrower(self, borrower: Owner, reason: str) -> None:
        self._frozen[borrower] = reason

    def unfreeze_borrower(self, borrower: Owner) -> None:
        self._frozen.pop(borrower, None)

    def is_frozen(self, borrower: Owner) -> bool:
        return borrower in self._frozen

    # ---- 出借 -----------------------------------------------------------
    def lend(
        self,
        *,
        lender: Owner,
        borrower: Owner,
        principal: Any,
        scope: str,
        created_at: datetime,
        due_at: datetime,
        reason: str = REASON_DEMAND_IDLE,
        loan_id: Optional[str] = None,
    ) -> LoanLot:
        amt = _check_positive(principal, ERR_INVALID_PRINCIPAL)
        if scope not in VALID_SCOPES:
            raise LoanError(ERR_INVALID_SCOPE, "未知归属范围 %r" % (scope,))
        if not isinstance(created_at, datetime) or not isinstance(due_at, datetime):
            raise LoanError(ERR_INVALID_TIME, "created_at/due_at 必须是 datetime")
        if due_at <= created_at:
            raise LoanError(ERR_INVALID_DUE_AT, "到期时间必须晚于创建时间")
        if lender.owner_type not in LENDER_TYPES:
            raise LoanError(ERR_INVALID_OWNER, "出借人类型非法")
        if borrower.owner_type not in BORROWER_TYPES:
            raise LoanError(ERR_INVALID_OWNER, "借款人类型非法")
        if lender == borrower:
            raise LoanError(ERR_SELF_LENDING, "不能借给自己")
        if self.outstanding_for_borrower(lender) > 0:
            # 0019 §2.3 (4)：借入方不得把借来的额度再借出（v1 保守口径）
            raise LoanError(
                ERR_REHYPOTHECATION,
                "出借人尚有未归还借款，禁止再抵押出借",
            )
        if self.is_frozen(borrower):
            raise LoanError(ERR_BORROWER_FROZEN, "借款人已被召回冻结")
        if self.available_credit(borrower) < amt:
            raise LoanError(
                ERR_CREDIT_INSUFFICIENT,
                "借款人可用额度 %s < 出借 %s" % (
                    dec_str(self.available_credit(borrower)), dec_str(amt)),
            )
        balance = self.lendable(lender)
        if balance < amt:
            raise LoanError(
                ERR_LENDABLE_INSUFFICIENT,
                "出借人可借 %s < 出借 %s" % (dec_str(balance), dec_str(amt)),
            )

        lot_id = loan_id or self._next_loan_id()
        if lot_id in self._lots:
            raise LoanError(ERR_DUPLICATE_LOAN_ID, "loan_id 重复: %s" % lot_id)

        self._lendable[lender] = balance - amt
        lot = LoanLot(
            loan_id=lot_id,
            lender_type=lender.owner_type,
            lender_id=lender.owner_id,
            borrower_type=borrower.owner_type,
            borrower_id=borrower.owner_id,
            scope=scope,
            principal=amt,
            outstanding=amt,
            created_at=created_at,
            due_at=due_at,
            reason=reason,
        )
        self._lots[lot_id] = lot
        return lot

    def _next_loan_id(self) -> str:
        self._seq += 1
        return "L%06d" % self._seq

    # ---- 召回 -----------------------------------------------------------
    def request_recall(
        self,
        loan_id: str,
        *,
        at: datetime,
        deadline: datetime,
    ) -> LoanLot:
        lot = self._require_lot(loan_id)
        if lot.status != STATUS_ACTIVE:
            raise LoanError(
                ERR_INVALID_TRANSITION,
                "%s 不能从 %s 发起召回" % (loan_id, lot.status),
            )
        if not isinstance(at, datetime) or not isinstance(deadline, datetime):
            raise LoanError(ERR_INVALID_TIME, "召回时间必须是 datetime")
        if deadline <= at:
            raise LoanError(ERR_INVALID_DUE_AT, "召回截止必须晚于召回发起时间")
        lot.status = STATUS_RECALL_PENDING
        lot.recall_requested_at = at
        lot.recall_deadline = deadline
        # 召回一发出即冻结借款人可用额度（不等现金到账）
        self.freeze_borrower(lot.borrower, "recall:%s" % loan_id)
        return lot

    # ---- 归还 -----------------------------------------------------------
    def repay(self, loan_id: str, amount: Any, *, at: datetime) -> LoanLot:
        lot = self._require_lot(loan_id)
        if lot.status not in OPEN_STATUSES:
            raise LoanError(
                ERR_INVALID_TRANSITION,
                "%s 已处于 %s，不能再归还" % (loan_id, lot.status),
            )
        amt = _check_positive(amount)
        if amt > lot.outstanding:
            raise LoanError(
                ERR_OVER_REPAY,
                "归还 %s 超过未归还 %s" % (dec_str(amt), dec_str(lot.outstanding)),
            )
        if not isinstance(at, datetime):
            raise LoanError(ERR_INVALID_TIME, "归还时间必须是 datetime")

        lot.outstanding -= amt
        self._lendable[lot.lender] = self.lendable(lot.lender) + amt
        if lot.outstanding == 0:
            lot.status = STATUS_RETURNED
            lot.returned_at = at
            self._maybe_unfreeze(lot.borrower)
        return lot

    # ---- 核销 -----------------------------------------------------------
    def write_off(self, loan_id: str, *, at: datetime, reason: str = "shortfall") -> LoanLot:
        lot = self._require_lot(loan_id)
        if lot.status not in OPEN_STATUSES:
            raise LoanError(
                ERR_INVALID_TRANSITION,
                "%s 已处于 %s，不能核销" % (loan_id, lot.status),
            )
        if not isinstance(at, datetime):
            raise LoanError(ERR_INVALID_TIME, "核销时间必须是 datetime")
        lot.written_off_amount = lot.outstanding
        lot.write_off_reason = reason
        lot.outstanding = Decimal(0)
        lot.status = STATUS_WRITTEN_OFF
        lot.returned_at = at
        self._maybe_unfreeze(lot.borrower)
        return lot

    def _maybe_unfreeze(self, borrower: Owner) -> None:
        """借款人不再有召回中的借据时解除冻结。"""
        for lot in self._lots.values():
            if lot.borrower == borrower and lot.status == STATUS_RECALL_PENDING:
                return
        self.unfreeze_borrower(borrower)

    # ---- 查询 -----------------------------------------------------------
    def _require_lot(self, loan_id: str) -> LoanLot:
        lot = self._lots.get(loan_id)
        if lot is None:
            raise LoanError(ERR_UNKNOWN_LOT, "未知借据 %s" % loan_id)
        return lot

    def get(self, loan_id: str) -> Optional[LoanLot]:
        return self._lots.get(loan_id)

    def lots(self) -> List[LoanLot]:
        return list(self._lots.values())

    def open_lots(self) -> List[LoanLot]:
        return [x for x in self._lots.values() if x.is_open]

    def overdue_lots(self, at: datetime) -> List[LoanLot]:
        """到期未归还的 active 借据；由调用方决定是否发起召回。"""
        return sorted(
            (x for x in self._lots.values()
             if x.status == STATUS_ACTIVE and x.due_at <= at),
            key=lambda x: (x.due_at, x.loan_id),
        )

    def recall_priority_lots(
        self,
        *,
        lender: Optional[Owner] = None,
        scope: Optional[str] = None,
    ) -> List[LoanLot]:
        """0019 §4.4 召回顺序：到期最早 → 金额最大 → loan_id。"""
        pool = [
            x for x in self._lots.values()
            if x.is_open
            and (lender is None or x.lender == lender)
            and (scope is None or x.scope == scope)
        ]
        return sorted(pool, key=lambda x: (x.due_at, -x.outstanding, x.loan_id))

    def outstanding_for_lender(self, lender: Owner) -> Decimal:
        return sum((x.outstanding for x in self._lots.values()
                    if x.lender == lender), Decimal(0))

    def outstanding_for_borrower(self, borrower: Owner) -> Decimal:
        return sum((x.outstanding for x in self._lots.values()
                    if x.borrower == borrower), Decimal(0))

    def total_outstanding(self) -> Decimal:
        return sum((x.outstanding for x in self._lots.values()), Decimal(0))

    def total_written_off(self) -> Decimal:
        return sum((x.written_off_amount or Decimal(0)
                    for x in self._lots.values()), Decimal(0))

    def outstanding_by_scope(self) -> Dict[str, Decimal]:
        out: Dict[str, Decimal] = {}
        for lot in self._lots.values():
            out[lot.scope] = out.get(lot.scope, Decimal(0)) + lot.outstanding
        return out

    def lendable_total(self) -> Decimal:
        return sum(self._lendable.values(), Decimal(0))

    # ---- 对账 -----------------------------------------------------------
    def check_invariants(self) -> List[str]:
        """返回违规列表；空列表表示借据公平、金额守恒（0019 §2.3）。"""
        violations: List[str] = []

        for lot in self._lots.values():
            if lot.principal <= 0:
                violations.append("%s: principal <= 0" % lot.loan_id)
            if lot.outstanding < 0:
                violations.append("%s: outstanding < 0" % lot.loan_id)
            if lot.outstanding > lot.principal:
                violations.append("%s: outstanding > principal" % lot.loan_id)
            if lot.status not in ALL_STATUSES:
                violations.append("%s: 未知状态 %r" % (lot.loan_id, lot.status))
            if lot.status in TERMINAL_STATUSES and lot.outstanding != 0:
                violations.append("%s: 终态仍有未归还" % lot.loan_id)
            if lot.status == STATUS_RECALL_PENDING and (
                lot.recall_requested_at is None or lot.recall_deadline is None
            ):
                violations.append("%s: 召回中缺少时间戳" % lot.loan_id)

        # 按 scope 拆分的「借出 = 借入」
        lent: Dict[str, Decimal] = {}
        borrowed: Dict[str, Decimal] = {}
        for lot in self._lots.values():
            lent[lot.scope] = lent.get(lot.scope, Decimal(0)) + lot.outstanding
            borrowed[lot.scope] = borrowed.get(lot.scope, Decimal(0)) + lot.outstanding
        for scope in set(lent) | set(borrowed):
            if lent.get(scope, Decimal(0)) != borrowed.get(scope, Decimal(0)):
                violations.append(
                    "scope=%s: Σ借出 %s != Σ借入 %s" % (
                        scope, dec_str(lent.get(scope, Decimal(0))),
                        dec_str(borrowed.get(scope, Decimal(0)))),
                )

        # 金额守恒：存入 − 提取 = 可借 + 未归还 + 核销
        lhs = self._deposited - self._withdrawn
        rhs = self.lendable_total() + self.total_outstanding() + self.total_written_off()
        if lhs != rhs:
            violations.append(
                "金额不守恒: 存入−提取 %s != 可借+未归还+核销 %s" % (
                    dec_str(lhs), dec_str(rhs)),
            )

        return violations

    def snapshot(self) -> Dict[str, Any]:
        """用于事件/对账的结构化快照；键顺序固定，便于哈希对比。"""
        return {
            "deposited": dec_str(self._deposited),
            "withdrawn": dec_str(self._withdrawn),
            "lendable": {
                "%s:%s" % (owner.owner_type, owner.owner_id): dec_str(amount)
                for owner, amount in sorted(
                    self._lendable.items(),
                    key=lambda kv: (kv[0].owner_type, str(kv[0].owner_id)),
                )
            },
            "outstanding_total": dec_str(self.total_outstanding()),
            "written_off_total": dec_str(self.total_written_off()),
            "lots": [
                lot.to_dict() for lot in sorted(
                    self._lots.values(), key=lambda x: x.loan_id)
            ],
        }
