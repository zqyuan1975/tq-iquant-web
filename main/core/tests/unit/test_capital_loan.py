"""LoanLot 借据账本与状态机（0021 B1）单测。

覆盖：金额守恒、禁止双重占用、状态迁移穷举、召回冻结与解冻、
禁止再抵押、终态拒绝、召回优先级、核销与逾期。
"""
from datetime import datetime, timedelta
from decimal import Decimal

import pytest

from core.engine.capital_loan import (
    ERR_BORROWER_FROZEN,
    ERR_CREDIT_BELOW_OUTSTANDING,
    ERR_CREDIT_INSUFFICIENT,
    ERR_INVALID_DUE_AT,
    ERR_INVALID_PRINCIPAL,
    ERR_INVALID_SCOPE,
    ERR_INVALID_TRANSITION,
    ERR_LENDABLE_INSUFFICIENT,
    ERR_OVER_REPAY,
    ERR_REHYPOTHECATION,
    ERR_SELF_LENDING,
    SCOPE_PORTFOLIO,
    SCOPE_SESSION,
    STATUS_ACTIVE,
    STATUS_RECALL_PENDING,
    STATUS_RETURNED,
    STATUS_WRITTEN_OFF,
    LoanBook,
    LoanError,
    Owner,
)

D = Decimal
T0 = datetime(2026, 11, 20, 9, 30)


def _book(lendable="1000000", credit="1000000"):
    book = LoanBook()
    lender = Owner("portfolio", 1)
    borrower = Owner("portfolio", 2)
    book.deposit_lendable(lender, D(lendable))
    book.grant_credit(borrower, D(credit))
    return book, lender, borrower


def _lend(book, lender, borrower, amount="100000", **kwargs):
    params = dict(
        lender=lender,
        borrower=borrower,
        principal=D(amount),
        scope=SCOPE_PORTFOLIO,
        created_at=T0,
        due_at=T0 + timedelta(days=1),
    )
    params.update(kwargs)
    return book.lend(**params)


# ---------------- 基本借还 / 守恒 ----------------
def test_lend_and_repay_keep_books_balanced():
    book, lender, borrower = _book()

    lot = _lend(book, lender, borrower)
    assert lot.status == STATUS_ACTIVE
    assert lot.loan_id == "L000001"
    assert book.lendable(lender) == D("900000")
    assert book.outstanding_for_borrower(borrower) == D("100000")
    assert book.available_credit(borrower) == D("900000")
    assert book.check_invariants() == []

    book.repay(lot.loan_id, D("40000"), at=T0 + timedelta(hours=1))
    assert lot.status == STATUS_ACTIVE
    assert lot.outstanding == D("60000")
    assert book.lendable(lender) == D("940000")
    assert book.check_invariants() == []

    book.repay(lot.loan_id, D("60000"), at=T0 + timedelta(hours=2))
    assert lot.status == STATUS_RETURNED
    assert lot.returned_at == T0 + timedelta(hours=2)
    assert book.lendable(lender) == D("1000000")
    assert book.total_outstanding() == D(0)
    assert book.check_invariants() == []


def test_same_balance_cannot_be_occupied_twice():
    book, lender, borrower = _book(credit="2000000")
    _lend(book, lender, borrower, "600000")

    with pytest.raises(LoanError) as err:
        _lend(book, lender, borrower, "600000")
    assert err.value.code == ERR_LENDABLE_INSUFFICIENT

    assert book.outstanding_for_borrower(borrower) == D("600000")
    assert book.check_invariants() == []


# ---------------- 授信约束 ----------------
def test_credit_caps_borrower_even_when_lender_has_cash():
    book, lender, borrower = _book(credit="150000")

    with pytest.raises(LoanError) as err:
        _lend(book, lender, borrower, "200000")
    assert err.value.code == ERR_CREDIT_INSUFFICIENT

    book.grant_credit(borrower, D("200000"))
    _lend(book, lender, borrower, "200000")

    with pytest.raises(LoanError) as err:
        book.grant_credit(borrower, D("199999"))
    assert err.value.code == ERR_CREDIT_BELOW_OUTSTANDING


# ---------------- 非法输入 ----------------
@pytest.mark.parametrize(
    "kwargs, code",
    [
        ({"amount": "0"}, ERR_INVALID_PRINCIPAL),
        ({"amount": "-1"}, ERR_INVALID_PRINCIPAL),
        ({"scope": "global"}, ERR_INVALID_SCOPE),
        ({"due_at": T0}, ERR_INVALID_DUE_AT),
    ],
)
def test_invalid_lend_inputs_rejected(kwargs, code):
    book, lender, borrower = _book()
    with pytest.raises(LoanError) as err:
        _lend(book, lender, borrower, **kwargs)
    assert err.value.code == code
    assert book.check_invariants() == []


def test_self_lending_rejected():
    book, lender, _ = _book()
    with pytest.raises(LoanError) as err:
        _lend(book, lender, lender)
    assert err.value.code == ERR_SELF_LENDING


def test_over_repay_rejected():
    book, lender, borrower = _book()
    lot = _lend(book, lender, borrower)
    with pytest.raises(LoanError) as err:
        book.repay(lot.loan_id, D("100001"), at=T0 + timedelta(hours=1))
    assert err.value.code == ERR_OVER_REPAY
    assert lot.outstanding == D("100000")


def test_withdraw_cannot_exceed_lendable():
    book, lender, _ = _book()
    with pytest.raises(LoanError) as err:
        book.withdraw_lendable(lender, D("1000001"))
    assert err.value.code == ERR_LENDABLE_INSUFFICIENT
    assert book.lendable(lender) == D("1000000")


# ---------------- 召回与冻结 ----------------
def test_recall_freezes_borrower_until_fully_repaid():
    book, lender, borrower = _book(credit="2000000")
    lot = _lend(book, lender, borrower)
    other_lender = Owner("portfolio", 3)
    book.deposit_lendable(other_lender, D("1000000"))

    book.request_recall(
        lot.loan_id, at=T0 + timedelta(hours=2),
        deadline=T0 + timedelta(days=1),
    )
    assert lot.status == STATUS_RECALL_PENDING
    assert lot.recall_requested_at == T0 + timedelta(hours=2)
    assert book.is_frozen(borrower)
    assert book.available_credit(borrower) == D(0)

    with pytest.raises(LoanError) as err:
        _lend(book, other_lender, borrower, "10000")
    assert err.value.code == ERR_BORROWER_FROZEN

    book.repay(lot.loan_id, D("50000"), at=T0 + timedelta(hours=3))
    assert book.is_frozen(borrower)
    assert lot.status == STATUS_RECALL_PENDING

    book.repay(lot.loan_id, D("50000"), at=T0 + timedelta(hours=4))
    assert lot.status == STATUS_RETURNED
    assert not book.is_frozen(borrower)
    assert book.available_credit(borrower) == D("2000000")
    assert book.check_invariants() == []


def test_write_off_stays_conserved_and_keeps_lender_short():
    book, lender, borrower = _book()
    lot = _lend(book, lender, borrower)

    book.write_off(lot.loan_id, at=T0 + timedelta(days=3), reason="shortfall")
    assert lot.status == STATUS_WRITTEN_OFF
    assert lot.written_off_amount == D("100000")
    assert lot.write_off_reason == "shortfall"
    assert lot.outstanding == D(0)
    assert book.lendable(lender) == D("900000")
    assert book.total_written_off() == D("100000")
    assert not book.is_frozen(borrower)
    assert book.check_invariants() == []


# ---------------- 禁止再抵押 ----------------
def test_borrower_cannot_relend_while_indebted():
    book, lender, borrower = _book()
    _lend(book, lender, borrower)

    downstream = Owner("portfolio", 9)
    book.deposit_lendable(borrower, D("500000"))
    book.grant_credit(downstream, D("500000"))

    with pytest.raises(LoanError) as err:
        _lend(book, borrower, downstream)
    assert err.value.code == ERR_REHYPOTHECATION


# ---------------- 召回优先级 / 逾期 ----------------
def test_recall_priority_orders_by_due_then_amount_then_id():
    book, lender, _ = _book(credit="3000000")
    borrower = Owner("portfolio", 2)
    a = _lend(book, lender, borrower, "100000",
              due_at=T0 + timedelta(days=1))
    b = _lend(book, lender, borrower, "300000",
              due_at=T0 + timedelta(days=2))
    c = _lend(book, lender, borrower, "200000",
              due_at=T0 + timedelta(days=2))
    d = _lend(book, lender, borrower, "400000",
              due_at=T0 + timedelta(days=2))

    ordered = [x.loan_id for x in book.recall_priority_lots(lender=lender)]
    assert ordered == [a.loan_id, d.loan_id, b.loan_id, c.loan_id]

    session_only = book.recall_priority_lots(scope=SCOPE_SESSION)
    assert session_only == []


def test_overdue_lots_only_returns_active_past_due():
    book, lender, borrower = _book()
    lot = _lend(book, lender, borrower)
    assert book.overdue_lots(T0) == []
    assert [x.loan_id for x in book.overdue_lots(T0 + timedelta(days=1))] == [
        lot.loan_id
    ]


# ---------------- 状态迁移穷举 ----------------
_ACTIONS = ("request_recall", "repay", "write_off")
_EXPECTED = {
    STATUS_ACTIVE: {"request_recall": True, "repay": True, "write_off": True},
    STATUS_RECALL_PENDING: {"request_recall": False, "repay": True,
                            "write_off": True},
    STATUS_RETURNED: {"request_recall": False, "repay": False,
                      "write_off": False},
    STATUS_WRITTEN_OFF: {"request_recall": False, "repay": False,
                         "write_off": False},
}


def _lot_in_status(status):
    book, lender, borrower = _book()
    lot = _lend(book, lender, borrower)
    if status == STATUS_RECALL_PENDING:
        book.request_recall(
            lot.loan_id, at=T0 + timedelta(hours=1),
            deadline=T0 + timedelta(days=2),
        )
    elif status == STATUS_RETURNED:
        book.repay(lot.loan_id, D("100000"), at=T0 + timedelta(hours=1))
    elif status == STATUS_WRITTEN_OFF:
        book.write_off(lot.loan_id, at=T0 + timedelta(hours=1))
    assert lot.status == status
    return book, lot


def _apply(book, lot, action):
    at = T0 + timedelta(hours=3)
    if action == "request_recall":
        return book.request_recall(
            lot.loan_id, at=at, deadline=T0 + timedelta(days=2))
    if action == "repay":
        return book.repay(lot.loan_id, D("100000"), at=at)
    return book.write_off(lot.loan_id, at=at)


@pytest.mark.parametrize("status", sorted(_EXPECTED))
@pytest.mark.parametrize("action", _ACTIONS)
def test_state_transition_matrix(status, action):
    book, lot = _lot_in_status(status)
    allowed = _EXPECTED[status][action]

    if allowed:
        _apply(book, lot, action)
        assert book.check_invariants() == []
    else:
        with pytest.raises(LoanError) as err:
            _apply(book, lot, action)
        assert err.value.code == ERR_INVALID_TRANSITION
        assert lot.status == status


# ---------------- 快照 ----------------
def test_snapshot_is_deterministic():
    def build():
        book, lender, borrower = _book()
        _lend(book, lender, borrower)
        return book.snapshot()

    assert build() == build()
    snap = build()
    assert snap["outstanding_total"] == "100000"
    assert snap["lendable"]["portfolio:1"] == "900000"
    assert snap["lots"][0]["loan_id"] == "L000001"
