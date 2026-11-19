# -*- coding: utf-8 -*-
"""门控释放/召回状态机（0021 D2）测试。

纯状态机：合成交易日历 + 借据账本，验证 T+1/T+2 时间线、恢复召回、
原策略优先（reopen_reserve）、不双占、UNKNOWN 不释放与无前视。
"""
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest

from core.engine.capital_gate_release import (
    GATE_CLOSED,
    GATE_OPEN,
    GATE_UNKNOWN,
    R_GATE_CLOSED_CONFIRMED,
    R_RECALL_COMPLETE,
    R_RECALL_ISSUED,
    R_RECALL_SHORTFALL,
    R_RELEASE_CANCELLED,
    R_RELEASE_DELAYED,
    R_RELEASE_DONE,
    R_RELEASE_HELD_UNKNOWN,
    STATUS_IDLE,
    STATUS_PENDING_RELEASE,
    STATUS_RELEASED,
    STATUS_REOPENING,
    GateReleaseBook,
    GateReleaseError,
    resolve_gate_status,
)
from core.engine.capital_loan import (
    STATUS_RECALL_PENDING,
    STATUS_RETURNED,
    LoanBook,
    Owner,
)

DAYS = [date(2025, 1, 2) + timedelta(days=i) for i in range(12)]
OWNER = Owner("strategy", 1)
B1 = Owner("strategy", 2)
B2 = Owner("strategy", 3)


def _book():
    book = LoanBook()
    book.deposit_lendable(OWNER, Decimal("1000"))
    book.grant_credit(B1, Decimal("600"))
    book.grant_credit(B2, Decimal("600"))
    return book


def _lend(book, borrower, amount, due_day):
    return book.lend(
        lender=OWNER, borrower=borrower, principal=Decimal(amount),
        scope="portfolio",
        created_at=datetime(2025, 1, 2, 9, 30),
        due_at=datetime.combine(due_day, datetime.min.time()))


# ---------------- 门控状态解析 ----------------

def test_resolve_gate_status_matches_0019():
    assert resolve_gate_status("off", True, -1) == GATE_OPEN
    assert resolve_gate_status("inherit", False, -1) == GATE_OPEN   # 组合未启用
    assert resolve_gate_status("inherit", True, -1) == GATE_CLOSED
    assert resolve_gate_status("inherit", True, 1) == GATE_OPEN
    assert resolve_gate_status("on", False, -1) == GATE_CLOSED      # 强制启用
    assert resolve_gate_status("inherit", True, None) == GATE_UNKNOWN
    assert resolve_gate_status("on", False, None) == GATE_UNKNOWN


# ---------------- 释放时间线 ----------------

def test_release_t_plus_1_pending_t_plus_2_lendable():
    rel = GateReleaseBook(DAYS)
    day1 = rel.observe(OWNER, DAYS[1], GATE_CLOSED, idle_amount=100)
    assert day1["status"] == STATUS_PENDING_RELEASE
    assert R_GATE_CLOSED_CONFIRMED in day1["reasons"]
    assert rel.pending_release(OWNER) == Decimal("100")
    assert day1["release_amount"] is None          # 延迟期内不可借

    day2 = rel.observe(OWNER, DAYS[2], GATE_CLOSED, idle_amount=100)
    assert day2["status"] == STATUS_RELEASED
    assert day2["release_amount"] == "100"
    assert R_RELEASE_DONE in day2["reasons"]
    assert rel.pending_release(OWNER) == Decimal("0")

    day3 = rel.observe(OWNER, DAYS[3], GATE_CLOSED, idle_amount=0)
    assert day3["release_amount"] is None          # 不重复释放（不双占）
    assert rel.snapshot()["strategy:1"]["status"] == STATUS_RELEASED


def test_release_pending_shrinks_with_idle():
    rel = GateReleaseBook(DAYS, release_delay_days=2)
    rel.observe(OWNER, DAYS[0], GATE_CLOSED, idle_amount=100)
    rel.observe(OWNER, DAYS[1], GATE_CLOSED, idle_amount=60)
    event = rel.observe(OWNER, DAYS[2], GATE_CLOSED, idle_amount=60)
    assert event["release_amount"] == "60"


def test_release_cancelled_when_gate_reopens_before_delay():
    rel = GateReleaseBook(DAYS)
    rel.observe(OWNER, DAYS[0], GATE_CLOSED, idle_amount=100)
    event = rel.observe(OWNER, DAYS[1], GATE_OPEN, loan_book=_book())
    assert event["status"] == STATUS_IDLE
    assert R_RELEASE_CANCELLED in event["reasons"]
    assert rel.pending_release(OWNER) == Decimal("0")


def test_release_held_when_gate_unknown():
    rel = GateReleaseBook(DAYS)
    rel.observe(OWNER, DAYS[0], GATE_CLOSED, idle_amount=100)
    held = rel.observe(OWNER, DAYS[1], GATE_UNKNOWN)
    assert held["release_amount"] is None
    assert R_RELEASE_HELD_UNKNOWN in held["reasons"]
    assert rel.new_lending_blocked(OWNER) is True
    resumed = rel.observe(OWNER, DAYS[2], GATE_CLOSED, idle_amount=100)
    assert resumed["release_amount"] == "100"


# ---------------- 恢复召回 ----------------

def _closed_with_loans():
    book = _book()
    late = _lend(book, B1, "300", DAYS[10])
    early = _lend(book, B2, "200", DAYS[8])
    rel = GateReleaseBook(DAYS)
    rel.observe(OWNER, DAYS[0], GATE_OPEN, loan_book=book)
    rel.observe(OWNER, DAYS[1], GATE_CLOSED, idle_amount=0, loan_book=book)
    return book, rel, late, early


def test_reopen_recalls_all_lots_and_freezes_borrowers():
    book, rel, late, early = _closed_with_loans()
    event = rel.observe(OWNER, DAYS[2], GATE_OPEN, loan_book=book)

    assert event["status"] == STATUS_REOPENING
    assert R_RECALL_ISSUED in event["reasons"]
    # 0019 §4.4 顺序：到期最早 → 金额最大 → loan_id
    assert event["recalled"] == [early.loan_id, late.loan_id]
    assert all(lot.status == STATUS_RECALL_PENDING
               for lot in (late, early))
    assert book.is_frozen(B1) and book.is_frozen(B2)
    assert rel.reopen_reserve(OWNER) == Decimal("500")
    assert rel.new_lending_blocked(OWNER) is True
    # 召回期间可借余额全部进入预留，谁都借不走
    assert rel.lendable_after_reserve(OWNER, book) == Decimal("0")


def test_reopen_completes_after_repay():
    book, rel, late, early = _closed_with_loans()
    rel.observe(OWNER, DAYS[2], GATE_OPEN, loan_book=book)
    book.repay(late.loan_id, Decimal("300"), at=datetime(2025, 1, 4, 9, 30))
    book.repay(early.loan_id, Decimal("200"), at=datetime(2025, 1, 4, 9, 30))
    assert late.status == STATUS_RETURNED
    assert not book.is_frozen(B1)

    event = rel.observe(OWNER, DAYS[3], GATE_OPEN, loan_book=book)
    assert event["status"] == STATUS_IDLE
    assert R_RECALL_COMPLETE in event["reasons"]
    assert rel.reopen_reserve(OWNER) == Decimal("0")
    assert rel.new_lending_blocked(OWNER) is False
    assert rel.lendable_after_reserve(OWNER, book) == Decimal("1000")


def test_recall_shortfall_after_deadline():
    book, rel, late, early = _closed_with_loans()
    rel.observe(OWNER, DAYS[2], GATE_OPEN, loan_book=book)
    # 召回截止 = DAYS[3]；DAYS[4] 仍未归还 → shortfall
    event = rel.observe(OWNER, DAYS[4], GATE_OPEN, loan_book=book)
    assert R_RECALL_SHORTFALL in event["reasons"]
    assert event["shortfall"] == "500"
    assert event["status"] == STATUS_REOPENING
    assert rel.new_lending_blocked(OWNER) is True


def test_reopen_without_loan_book_is_rejected():
    rel = GateReleaseBook(DAYS)
    rel.observe(OWNER, DAYS[0], GATE_CLOSED, idle_amount=0)
    rel.observe(OWNER, DAYS[1], GATE_CLOSED, idle_amount=0)
    with pytest.raises(GateReleaseError) as exc:
        rel.observe(OWNER, DAYS[2], GATE_OPEN)
    assert exc.value.code == "MISSING_LOAN_BOOK"


# ---------------- 无前视 / 输入约束 ----------------

def test_no_lookahead_prefix_matches_full_run():
    book = _book()

    def run(days):
        rel = GateReleaseBook(DAYS)
        out = []
        for day in days:
            gate = GATE_CLOSED if day in DAYS[1:3] else GATE_OPEN
            out.append(rel.observe(OWNER, day, gate, idle_amount=100,
                                   loan_book=book))
        return out

    prefix = run(DAYS[:4])
    full = run(DAYS)
    for i, event in enumerate(prefix):
        assert event["status"] == full[i]["status"]
        assert event["release_amount"] == full[i]["release_amount"]


def test_observe_rejects_non_trading_day_and_backwards():
    rel = GateReleaseBook(DAYS)
    with pytest.raises(GateReleaseError) as exc:
        rel.observe(OWNER, date(2030, 1, 1), GATE_OPEN)
    assert exc.value.code == "DAY_NOT_IN_TRADING_DAYS"
    rel.observe(OWNER, DAYS[3], GATE_OPEN)
    with pytest.raises(GateReleaseError) as exc:
        rel.observe(OWNER, DAYS[3], GATE_CLOSED)
    assert exc.value.code == "DAY_NOT_INCREASING"
    with pytest.raises(GateReleaseError) as exc:
        rel.observe(OWNER, DAYS[4], "SIDEWAYS")
    assert exc.value.code == "INVALID_GATE_STATUS"


def test_pending_release_not_counted_as_lendable():
    rel = GateReleaseBook(DAYS, release_delay_days=2)
    book = _book()
    rel.observe(OWNER, DAYS[0], GATE_CLOSED, idle_amount=100, loan_book=book)
    # 延迟期：pending 不在账本可借里，也没有释放事件
    assert rel.pending_release(OWNER) == Decimal("100")
    assert book.lendable(OWNER) == Decimal("1000")
    assert R_RELEASE_DELAYED in rel.observe(
        OWNER, DAYS[1], GATE_CLOSED, idle_amount=100, loan_book=book)["reasons"]
