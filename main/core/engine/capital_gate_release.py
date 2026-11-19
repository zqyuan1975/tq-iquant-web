"""门控释放/召回状态机（0021 D2，口径 0019 §4）。

纯内存、无 IO、无时钟：交易日历与当日 ``now`` 由调用方传入，逐日推进即可
复现同一条时间线。本模块**不记账**——资金账本仍是
:mod:`core.engine.capital_loan` 的 ``LoanBook``；这里只维护每个出借人
（策略/组合/会话）的门控状态、待释放额、召回冻结与 ``reopen_reserve``，
并通过事件告诉调用方「何时把多少额度 deposit 进 LoanBook / 何时发起召回」。

时间线（``release_confirm_days=1 / release_delay_days=1 /
reopen_confirm_days=1 / recall_notice_days=1``，0019 §4.2）::

    T 收盘        基准转空              → 右移，T+1 才可见
    T+1 首 bar    CLOSED 确认            → PENDING_RELEASE，pending 不可借
    T+2 首 bar    释放延迟结束           → 事件 release_amount，可登记借据
    T+11 首 bar   OPEN 确认              → REOPENING，立即召回该出借人全部借据
    T+12 首 bar   召回截止               → 未归还记 recall_shortfall，继续冻结

四条硬规则（0019 §4.5）：

1. ``PENDING_RELEASE`` 不计入可借，延迟期内谁都不能用；
2. ``PENDING_RECALL`` 不计入可借，召回期只能归还；
3. 召回一发出借款人额度立即冻结（由 ``LoanBook.freeze_borrower`` 保证）；
4. 禁止再抵押（由 ``LoanBook.lend`` 保证），本模块不重复实现。

``UNKNOWN``（门控不可判定）：交易侧维持 fail-open，但**资金上不释放、
不发起新出借、不解除召回**。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from decimal import Decimal
from typing import Dict, List, Optional, Sequence

from core.engine.capital_contracts import dec_str
from core.engine.capital_loan import (
    STATUS_ACTIVE,
    LoanBook,
    LoanError,
    Owner,
)

GATE_RELEASE_VERSION = "gate-release-d2"

# ---- 门控状态（0019 §4.1） ----------------------------------------------
GATE_OPEN = "OPEN"
GATE_CLOSED = "CLOSED"
GATE_UNKNOWN = "UNKNOWN"
GATE_STATUSES = frozenset({GATE_OPEN, GATE_CLOSED, GATE_UNKNOWN})

# ---- 额度状态 -----------------------------------------------------------
STATUS_IDLE = "IDLE"                        # 无可释放/无召回进行
STATUS_PENDING_RELEASE = "PENDING_RELEASE"  # 已确认闲置，延迟期不可借
STATUS_RELEASED = "RELEASED"                # 已发出释放事件（金额已可入账）
STATUS_REOPENING = "REOPENING"              # 召回进行中，reopen_reserve 受保护

# ---- 原因码（事件/界面解释） --------------------------------------------
R_GATE_CLOSED_CONFIRMED = "gate_closed_confirmed"
R_RELEASE_DELAYED = "release_delayed"
R_RELEASE_DONE = "release_done"
R_RELEASE_HELD_UNKNOWN = "release_held_unknown"
R_RELEASE_CANCELLED = "release_cancelled_gate_reopened"
R_RECALL_ISSUED = "recall_issued"
R_RECALL_COMPLETE = "recall_complete"
R_RECALL_SHORTFALL = "recall_shortfall"
R_GATE_UNKNOWN = "gate_unknown_no_release"
R_NO_CHANGE = "no_change"

# ---- 拒绝码 -------------------------------------------------------------
ERR_DAY_UNKNOWN = "DAY_NOT_IN_TRADING_DAYS"
ERR_DAY_NOT_INCREASING = "DAY_NOT_INCREASING"
ERR_INVALID_GATE = "INVALID_GATE_STATUS"
ERR_MISSING_LOAN_BOOK = "MISSING_LOAN_BOOK"

_FIRST_BAR = time(9, 30)


class GateReleaseError(Exception):
    """状态机拒绝码异常。"""

    def __init__(self, code: str, message: str = "") -> None:
        self.code = code
        self.message = message or code
        super().__init__("%s: %s" % (code, self.message))


def resolve_gate_status(mode: Optional[str] = None,
                        portfolio_gate_enabled: bool = False,
                        tracker_state: Optional[int] = None) -> str:
    """策略级门控状态（0019 §4.1 逐行实现，不另写简化版）。

    ``mode``：``inherit``（默认）/ ``off`` / ``on``；``tracker_state`` 为
    ``MarketGateTracker.state_for(d)`` 的右移值（-1/0/1/2），``None`` 表示
    取不到门控（未接线或取数失败）。
    """
    mode = (mode or "inherit").lower()
    if mode == "off":
        return GATE_OPEN
    if tracker_state is None:
        if mode == "inherit" and not portfolio_gate_enabled:
            return GATE_OPEN
        return GATE_UNKNOWN
    if mode == "on" or portfolio_gate_enabled:
        return GATE_CLOSED if int(tracker_state) == -1 else GATE_OPEN
    return GATE_OPEN


@dataclass
class ReleaseState:
    """单个出借人的门控释放/召回状态（只读快照经 ``to_dict`` 暴露）。"""

    owner: Owner
    gate: str = GATE_OPEN
    streak_closed: int = 0
    streak_open: int = 0
    status: str = STATUS_IDLE
    pending_amount: Decimal = Decimal(0)
    release_index: Optional[int] = None
    reopen_reserve: Decimal = Decimal(0)
    recall_deadline_index: Optional[int] = None
    shortfall_amount: Decimal = Decimal(0)
    recalled_loan_ids: List[str] = field(default_factory=list)
    last_day: Optional[date] = None
    last_reason: str = R_NO_CHANGE

    def to_dict(self) -> Dict[str, object]:
        return {
            "owner": self.owner.to_dict(),
            "gate": self.gate,
            "status": self.status,
            "pending_amount": dec_str(self.pending_amount),
            "reopen_reserve": dec_str(self.reopen_reserve),
            "shortfall_amount": dec_str(self.shortfall_amount),
            "recalled_loan_ids": list(self.recalled_loan_ids),
            "last_day": self.last_day.isoformat() if self.last_day else None,
            "last_reason": self.last_reason,
        }


class GateReleaseBook:
    """逐日出借人门控账本：释放延迟、恢复召回、原策略优先。

    ``trading_days`` 为升序交易日序列（T+1/T+2 按交易日计，不按自然日）。
    ``observe`` 必须按日期严格递增调用；同一天重复调用直接拒绝，保证
    「某天只用当天及之前的状态」。
    """

    def __init__(self, trading_days: Sequence[date], *,
                 release_confirm_days: int = 1,
                 release_delay_days: int = 1,
                 reopen_confirm_days: int = 1,
                 recall_notice_days: int = 1) -> None:
        if not trading_days:
            raise ValueError("trading_days 不能为空")
        self.trading_days = list(trading_days)
        self._index = {d: i for i, d in enumerate(self.trading_days)}
        self.release_confirm_days = max(1, int(release_confirm_days))
        self.release_delay_days = max(0, int(release_delay_days))
        self.reopen_confirm_days = max(1, int(reopen_confirm_days))
        self.recall_notice_days = max(1, int(recall_notice_days))
        self._states: Dict[Owner, ReleaseState] = {}

    # ---- 查询 -----------------------------------------------------------
    def state(self, owner: Owner) -> ReleaseState:
        return self._states.setdefault(owner, ReleaseState(owner=owner))

    def pending_release(self, owner: Owner) -> Decimal:
        return self.state(owner).pending_amount

    def reopen_reserve(self, owner: Owner) -> Decimal:
        return self.state(owner).reopen_reserve

    def new_lending_blocked(self, owner: Owner) -> bool:
        """UNKNOWN 或召回进行中：不得新增出借（0019 §4.1/§4.5）。"""
        st = self.state(owner)
        return st.gate == GATE_UNKNOWN or st.status == STATUS_REOPENING

    def lendable_after_reserve(self, owner: Owner, book: LoanBook) -> Decimal:
        """可实际借出的额度 = 账本可借 − reopen_reserve（原策略优先）。"""
        left = book.lendable(owner) - self.reopen_reserve(owner)
        return left if left > 0 else Decimal(0)

    def snapshot(self) -> Dict[str, Dict[str, object]]:
        return {
            "%s:%s" % (owner.owner_type, owner.owner_id): st.to_dict()
            for owner, st in sorted(self._states.items(),
                                    key=lambda kv: (kv[0].owner_type,
                                                    str(kv[0].owner_id)))
        }

    # ---- 逐日推进 -------------------------------------------------------
    def observe(self, owner: Owner, day: date, gate_status: str, *,
                idle_amount=0, loan_book: Optional[LoanBook] = None,
                now: Optional[datetime] = None,
                scope: Optional[str] = None) -> Dict[str, object]:
        """推进一天，返回当日事件。

        - ``idle_amount``：当前判定为闲置、可释放的额度（仅 CLOSED 时使用）；
        - ``loan_book``：恢复召回时必须提供，借据状态机在账本里；
        - ``now``：召回发起时间（缺省 ``day`` 09:30），写进借据时间戳。
        """
        if gate_status not in GATE_STATUSES:
            raise GateReleaseError(ERR_INVALID_GATE, "未知门控状态 %r" % (gate_status,))
        idx = self._index.get(day)
        if idx is None:
            raise GateReleaseError(ERR_DAY_UNKNOWN, "非交易日 %s" % (day,))
        st = self.state(owner)
        if st.last_day is not None and day <= st.last_day:
            raise GateReleaseError(
                ERR_DAY_NOT_INCREASING,
                "%s 观测日必须严格递增：%s <= %s" % (owner.to_dict(), day, st.last_day))
        st.last_day = day
        st.gate = gate_status
        if gate_status == GATE_CLOSED:
            st.streak_closed += 1
            st.streak_open = 0
        elif gate_status == GATE_OPEN:
            st.streak_open += 1
            st.streak_closed = 0
        else:
            st.streak_closed = 0
            st.streak_open = 0

        idle = _dec_nonneg(idle_amount)
        event = {
            "owner": owner.to_dict(),
            "day": day.isoformat(),
            "gate": gate_status,
            "status": st.status,
            "release_amount": None,
            "recalled": [],
            "shortfall": "0",
            "reasons": [],
        }
        reasons: List[str] = event["reasons"]

        if gate_status == GATE_CLOSED:
            self._on_closed(st, idx, idle, event, reasons)
        elif gate_status == GATE_OPEN:
            self._on_open(st, idx, loan_book, now, day, scope, event, reasons)
        else:
            reasons.append(R_GATE_UNKNOWN)
            if st.status == STATUS_PENDING_RELEASE:
                reasons.append(R_RELEASE_HELD_UNKNOWN)

        st.last_reason = reasons[-1] if reasons else R_NO_CHANGE
        if not reasons:
            reasons.append(R_NO_CHANGE)
        event["status"] = st.status
        return event

    # ---- CLOSED：确认 → 延迟 → 释放 --------------------------------------
    def _on_closed(self, st: ReleaseState, idx: int, idle: Decimal,
                   event: Dict[str, object], reasons: List[str]) -> None:
        if st.status == STATUS_REOPENING:
            # 召回中再次关闭：只保留原策略预留，不重复释放
            reasons.append(R_NO_CHANGE)
            return
        if st.status != STATUS_PENDING_RELEASE:
            if st.status == STATUS_RELEASED and idle <= 0:
                # 已释放且当日无新增闲置：不重复进入待释放
                reasons.append(R_NO_CHANGE)
                return
            if st.streak_closed < self.release_confirm_days:
                reasons.append(R_NO_CHANGE)
                return
            st.status = STATUS_PENDING_RELEASE
            st.pending_amount = idle
            st.release_index = idx + self.release_delay_days
            reasons.append(R_GATE_CLOSED_CONFIRMED)
            return
        # 已确认待释放：闲置额只减不增，避免把已不再闲置的钱借出
        if idle < st.pending_amount:
            st.pending_amount = idle
        if st.release_index is None or idx < st.release_index:
            reasons.append(R_RELEASE_DELAYED)
            return
        amount = st.pending_amount
        st.pending_amount = Decimal(0)
        st.release_index = None
        st.status = STATUS_RELEASED
        if amount > 0:
            event["release_amount"] = dec_str(amount)
            reasons.append(R_RELEASE_DONE)
        else:
            reasons.append(R_NO_CHANGE)

    # ---- OPEN：取消未释放 / 发起召回 / 完成恢复 ---------------------------
    def _on_open(self, st: ReleaseState, idx: int,
                 loan_book: Optional[LoanBook], now: Optional[datetime],
                 day: date, scope: Optional[str],
                 event: Dict[str, object], reasons: List[str]) -> None:
        if st.status == STATUS_REOPENING:
            if loan_book is None:
                raise GateReleaseError(ERR_MISSING_LOAN_BOOK, "召回进行中需要 loan_book")
            remaining = [x for x in loan_book.open_lots() if x.lender == st.owner]
            if not remaining:
                st.status = STATUS_IDLE
                st.reopen_reserve = Decimal(0)
                st.recall_deadline_index = None
                st.recalled_loan_ids = []
                reasons.append(R_RECALL_COMPLETE)
                return
            overdue = [x for x in remaining
                       if st.recall_deadline_index is not None
                       and idx > st.recall_deadline_index]
            if overdue:
                st.shortfall_amount = sum(
                    (x.outstanding for x in overdue), Decimal(0))
                event["shortfall"] = dec_str(st.shortfall_amount)
                reasons.append(R_RECALL_SHORTFALL)
            else:
                reasons.append(R_NO_CHANGE)
            return
        if st.streak_open < self.reopen_confirm_days:
            reasons.append(R_NO_CHANGE)
            return
        if st.status == STATUS_IDLE:
            # 本机未观测到关闭段：普通 OPEN 日不触发召回（召回由到期/风险/
            # 手动等其它入口负责，本机只管门控驱动的那一段）
            reasons.append(R_NO_CHANGE)
            return
        if loan_book is None:
            # 观测到关闭段后恢复：必须核对在借借据，不能静默跳过召回
            raise GateReleaseError(ERR_MISSING_LOAN_BOOK, "恢复判定需要 loan_book")
        open_lots = [x for x in loan_book.open_lots() if x.lender == st.owner]
        if st.status == STATUS_PENDING_RELEASE:
            # 未释放的部分直接作废，额度回到原策略名下（本模块不记账）
            st.status = STATUS_IDLE
            st.pending_amount = Decimal(0)
            st.release_index = None
            reasons.append(R_RELEASE_CANCELLED)
        if not open_lots:
            if st.status == STATUS_RELEASED:
                st.status = STATUS_IDLE
            reasons.append(R_NO_CHANGE)
            return
        # 恢复确认：按 0019 §4.4 顺序立即召回该出借人的全部在借借据
        recalled = self._issue_recalls(st, idx, loan_book, now, day, scope)
        st.status = STATUS_REOPENING
        st.reopen_reserve = sum((x.outstanding for x in recalled), Decimal(0))
        st.recalled_loan_ids = [x.loan_id for x in recalled]
        st.recall_deadline_index = idx + self.recall_notice_days
        st.shortfall_amount = Decimal(0)
        event["recalled"] = list(st.recalled_loan_ids)
        reasons.append(R_RECALL_ISSUED)

    def _issue_recalls(self, st: ReleaseState, idx: int, book: LoanBook,
                       now: Optional[datetime], day: date,
                       scope: Optional[str]):
        deadline_index = min(idx + self.recall_notice_days,
                             len(self.trading_days) - 1)
        deadline = datetime.combine(self.trading_days[deadline_index], _FIRST_BAR)
        at = now or datetime.combine(day, _FIRST_BAR)
        out = []
        for lot in book.recall_priority_lots(lender=st.owner, scope=scope):
            if lot.status != STATUS_ACTIVE:
                continue
            try:
                book.request_recall(lot.loan_id, at=at, deadline=deadline)
            except LoanError:
                continue
            out.append(lot)
        return out


def _dec_nonneg(value) -> Decimal:
    try:
        amount = value if isinstance(value, Decimal) else Decimal(str(value or 0))
    except Exception:  # noqa: BLE001 - 非法输入保守按 0
        return Decimal(0)
    return amount if amount > 0 and amount.is_finite() else Decimal(0)
