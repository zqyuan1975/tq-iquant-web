"""G2 资金池对账恒等式 (1)–(10) 单元测试（0021）。"""
from decimal import Decimal
from types import SimpleNamespace

from core.engine.capital_loan import Owner
from core.engine.capital_reconcile import reconcile


def _ports():
    return [
        {"portfolio_id": 1, "cash": Decimal("1000"), "total_value": Decimal("2000"),
         "positions": {"600000": Decimal("600"), "000001": Decimal("400")},
         "cash_floor": Decimal("100"), "reserved_buy": Decimal("0")},
        {"portfolio_id": 2, "cash": Decimal("800"), "total_value": Decimal("1000"),
         "positions": {"600000": Decimal("200")},
         "cash_floor": Decimal("0"), "reserved_buy": Decimal("50")},
    ]


def _book(lots=None, deposited=Decimal("900")):
    lots = lots or []
    return SimpleNamespace(
        check_invariants=lambda: [],
        total_outstanding=lambda: sum(
            (x.outstanding for x in lots), Decimal(0)),
        open_lots=lambda: list(lots),
        deposited_for=lambda owner: deposited,
    )


def _by_id(report, idx):
    return next(it for it in report["items"] if it["id"] == idx)


def test_all_skipped_is_ok():
    report = reconcile(portfolios=_ports())
    assert report["ok"] is True
    assert report["block_new_buys"] is False
    assert _by_id(report, 1)["status"] == "skipped"
    assert _by_id(report, 4)["status"] == "skipped"
    assert _by_id(report, 9)["status"] == "skipped"


def test_identity_1_cash_total():
    ok = reconcile(portfolios=_ports(), expected_cash_total="1800")
    assert _by_id(ok, 1)["status"] == "ok"
    bad = reconcile(portfolios=_ports(), expected_cash_total="1700")
    assert _by_id(bad, 1)["status"] == "fail"
    assert bad["failed"] == [1]
    # 非阻断项
    assert bad["block_new_buys"] is False


def test_identity_2_negative_free_blocks_buys():
    ports = _ports()
    ports[0]["cash_floor"] = Decimal("2000")   # 1800 − 2000 = −200
    report = reconcile(portfolios=ports, loan_book=_book())
    assert _by_id(report, 2)["status"] == "fail"
    assert report["blocking"] == [2]
    assert report["block_new_buys"] is True


def test_identity_3_budget_and_identity_6_negative_cash():
    ports = _ports()
    ports[0]["budget"] = Decimal("700")   # 预算 700 < 自由额度 1700
    ports[1]["budget"] = Decimal("100")
    ports[1]["used"] = Decimal("80")      # 80+50 在途 > 100
    ports[1]["cash"] = Decimal("-1")
    report = reconcile(portfolios=ports, loan_book=_book())
    assert _by_id(report, 3)["status"] == "fail"
    assert _by_id(report, 6)["status"] == "fail"
    assert set(report["blocking"]) == {3, 6}


def test_identity_4_and_5_loan_book():
    lot = SimpleNamespace(outstanding=Decimal("600"), lender_id=11,
                          lender=Owner("strategy", 11), status="active")
    book = _book([lot], deposited=Decimal("500"))
    report = reconcile(portfolios=_ports(), loan_book=book)
    assert _by_id(report, 4)["status"] == "ok"
    assert _by_id(report, 5)["status"] == "fail"   # 600 > 500

    book_ok = _book([lot], deposited=Decimal("600"))
    assert _by_id(
        reconcile(portfolios=_ports(), loan_book=book_ok), 5)["status"] == "ok"

    bad = _book([lot])
    bad.check_invariants = lambda: ["scope mismatch"]
    report = reconcile(portfolios=_ports(), loan_book=bad)
    assert _by_id(report, 4)["status"] == "fail"
    assert report["block_new_buys"] is True       # (4) 属阻断项


def test_identity_7_and_8_real_side():
    ports = _ports()
    report = reconcile(portfolios=ports, real_total_asset="3100")
    assert _by_id(report, 7)["status"] == "fail"  # 3000 vs 3100 超 0.5%

    ok = reconcile(portfolios=ports, real_total_asset="3000")
    assert _by_id(ok, 7)["status"] == "ok"

    quotas = {1: Decimal("1500"), 2: Decimal("600")}
    report = reconcile(portfolios=ports, real_available="2000",
                       real_quotas=quotas)
    assert _by_id(report, 8)["status"] == "fail"  # 2100 > 1960

    ok = reconcile(portfolios=ports, real_available="2000",
                   real_quotas={1: Decimal("1500"), 2: Decimal("400")})
    assert _by_id(ok, 8)["status"] == "ok"


def test_identity_9_cash_rollforward():
    ports = _ports()
    report = reconcile(
        portfolios=ports,
        opening_cash={1: "900", 2: "700"},
        trade_cash_delta={1: "50", 2: "100"},
        external_flow_delta={1: "50", 2: "0"},
    )
    assert _by_id(report, 9)["status"] == "ok"

    bad = reconcile(
        portfolios=ports,
        opening_cash={1: "900", 2: "700"},
        trade_cash_delta={1: "0", 2: "100"},
        external_flow_delta={1: "0", 2: "0"},
    )
    assert _by_id(bad, 9)["status"] == "fail"


def test_identity_10_concentration():
    ports = _ports()
    # 600000 合计 800 / 总市值 3000 = 26.7% > 25%
    report = reconcile(portfolios=ports, concentration_limit="0.25")
    assert _by_id(report, 10)["status"] == "fail"
    ok = reconcile(portfolios=ports, concentration_limit="0.30")
    assert _by_id(ok, 10)["status"] == "ok"
