"""资金池 C3：容量曲线斜率 / ADV 参与率重叠（纯函数）。"""
from decimal import Decimal

from core.engine.capacity import (
    CAPACITY_CURVE_VERSION,
    STATUS_CURVE_OK,
    STATUS_CURVE_PENDING,
    CapacityCurvePoint,
    compute_capacity_curve,
    compute_participation,
)


def test_linear_curve_slope_and_marginals():
    curve = compute_capacity_curve([
        (1, Decimal("0.20")), (1.5, Decimal("0.16")), (2, Decimal("0.12")),
    ])
    assert curve.status == STATUS_CURVE_OK
    assert curve.points == 3
    assert curve.slope == Decimal("-0.08")
    assert curve.marginal_1x == Decimal("-0.08")
    assert curve.marginal_2x == Decimal("-0.08")
    assert curve.return_1x == Decimal("0.20")
    assert curve.return_2x == Decimal("0.12")
    assert curve.curve_decay_2x == Decimal("0.4")
    assert curve.version == CAPACITY_CURVE_VERSION


def test_concave_curve_marginal_declines_faster():
    curve = compute_capacity_curve([
        CapacityCurvePoint(Decimal("1"), Decimal("0.30")),
        CapacityCurvePoint(Decimal("1.5"), Decimal("0.24")),
        CapacityCurvePoint(Decimal("2"), Decimal("0.14")),
    ])
    assert curve.status == STATUS_CURVE_OK
    assert curve.slope == Decimal("-0.16")
    # 1x 单侧差分 -0.12；2x 单侧差分 -0.20（边际收益加速下滑）
    assert curve.marginal_1x == Decimal("-0.12")
    assert curve.marginal_2x == Decimal("-0.20")
    assert curve.marginal_2x < curve.marginal_1x


def test_interpolates_endpoints_without_extrapolation():
    curve = compute_capacity_curve([
        (Decimal("0.5"), Decimal("0.25")),
        (Decimal("1.5"), Decimal("0.15")),
        (Decimal("2.5"), Decimal("0.05")),
    ])
    assert curve.status == STATUS_CURVE_OK
    # 1x/2x 落在区间内 → 线性插值 0.20 / 0.10；超出范围不外推
    assert curve.return_1x == Decimal("0.20")
    assert curve.return_2x == Decimal("0.10")
    assert curve.marginal_1x == Decimal("-0.10")
    assert curve.marginal_2x == Decimal("-0.10")
    out = compute_capacity_curve([
        (Decimal("3"), Decimal("0.1")), (Decimal("3.5"), Decimal("0.08")),
        (Decimal("4"), Decimal("0.06")),
    ])
    assert out.status == STATUS_CURVE_OK
    assert out.return_1x is None and out.return_2x is None
    assert out.curve_decay_2x is None


def test_insufficient_points_return_pending_data():
    curve = compute_capacity_curve([(1, Decimal("0.2")), (1.5, Decimal("0.1"))])
    assert curve.status == STATUS_CURVE_PENDING
    assert curve.slope is None and curve.marginal_1x is None
    assert "PENDING_DATA" in curve.reasons[0]


def test_duplicate_scale_and_large_gap_rejected():
    dup = compute_capacity_curve([(1, Decimal("0.2")), (1, Decimal("0.1")),
                                  (2, Decimal("0.05"))])
    assert dup.status == STATUS_CURVE_PENDING
    assert "重复" in dup.reasons[0]

    gap = compute_capacity_curve([(Decimal("1"), Decimal("0.2")),
                                  (Decimal("3"), Decimal("0.1")),
                                  (Decimal("4"), Decimal("0.05"))])
    assert gap.status == STATUS_CURVE_PENDING
    assert "间隔" in gap.reasons[0]

    ok = compute_capacity_curve(
        [(Decimal("1"), Decimal("0.2")), (Decimal("3"), Decimal("0.1")),
         (Decimal("4"), Decimal("0.05"))], max_gap=None)
    assert ok.status == STATUS_CURVE_OK


def test_missing_or_nonfinite_points_are_pending_not_guessed():
    curve = compute_capacity_curve([(1, Decimal("0.2")), (1.5, None),
                                    (2, Decimal("0.1"))])
    assert curve.status == STATUS_CURVE_PENDING
    assert curve.marginal_1x is None

    nan = compute_capacity_curve([(1, Decimal("0.2")),
                                  (Decimal("NaN"), Decimal("0.1")),
                                  (2, Decimal("0.05"))])
    assert nan.status == STATUS_CURVE_PENDING

    bad = compute_capacity_curve([(1, Decimal("0.2")), 42, (2, Decimal("0.1"))])
    assert bad.status == STATUS_CURVE_PENDING


def test_negative_base_return_has_no_decay_ratio():
    curve = compute_capacity_curve([
        (1, Decimal("-0.05")), (1.5, Decimal("-0.10")),
        (2, Decimal("-0.20")),
    ])
    assert curve.status == STATUS_CURVE_OK
    assert curve.curve_decay_2x is None
    assert curve.slope == Decimal("-0.15")


def test_participation_matches_pairing_eval_keys():
    part = compute_participation(
        adv_by_code={"600000.SH": Decimal("1000"),
                     "000001.SZ": Decimal("500"),
                     "300750.SZ": Decimal("0")},
        lender_amount_by_code={"600000.SH": Decimal("10")},
        borrower_amount_by_code={"600000.SH": Decimal("5"),
                                 "000001.SZ": Decimal("20")},
    )
    assert part.status == STATUS_CURVE_OK
    assert part.lender_participation == Decimal("10") / Decimal("1500")
    assert part.borrower_participation == Decimal("25") / Decimal("1500")
    assert part.same_code_participation == Decimal("0.015")
    assert part.overlap == Decimal("0.005")
    assert part.bid_probability == Decimal("0.75")  # 0.015 / 0.02
    # pairing_eval 读取的键必须齐全
    data = part.to_dict()
    for key in ("lender_participation", "borrower_participation",
                "same_code_participation", "overlap", "bid_probability"):
        assert key in data


def test_participation_edges():
    no_common = compute_participation(
        adv_by_code={"A": 1000, "B": 1000},
        lender_amount_by_code={"A": 10},
        borrower_amount_by_code={"B": 10},
    )
    assert no_common.status == "ok_no_common"
    assert no_common.same_code_participation is None
    assert no_common.bid_probability is None

    missing = compute_participation(
        adv_by_code={},
        lender_amount_by_code={"A": 10},
        borrower_amount_by_code={"A": 5},
    )
    assert missing.status == STATUS_CURVE_PENDING
    assert missing.lender_participation is None

    clipped = compute_participation(
        adv_by_code={"A": 1000},
        lender_amount_by_code={"A": 20},
        borrower_amount_by_code={"A": 10},
    )
    assert clipped.same_code_participation == Decimal("0.03")
    assert clipped.bid_probability == Decimal("1")


def test_no_future_volume_dependency():
    """同一批历史点重复计算必须完全一致（纯函数，不取行情、无时钟）。"""
    points = [(1, Decimal("0.2")), (1.5, Decimal("0.15")),
              (2, Decimal("0.1"))]
    a = compute_capacity_curve(points)
    b = compute_capacity_curve(list(reversed(points)))
    assert a.to_dict() == b.to_dict()
