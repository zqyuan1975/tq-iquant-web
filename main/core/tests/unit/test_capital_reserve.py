"""无单仓上限预留 + 可外借额度（0021 C1）单测。"""
from decimal import Decimal

from core.engine.capital_reserve import (
    STATUS_DEGRADED,
    STATUS_DISABLED,
    STATUS_NOT_DERIVABLE,
    STATUS_NO_HISTORY,
    STATUS_OK,
    UtilizationPoint,
    derive_lendable,
    derive_no_cap_reserve,
)

D = Decimal


def _linear_points(n=262, start=D("0.5"), step=D("0.001"),
                   startup=20, startup_u=D("0.1")):
    pts = [UtilizationPoint(utilization=startup_u, equity=D("1000000"))
           for _ in range(startup)]
    pts += [UtilizationPoint(utilization=start + step * i, equity=D("1000000"))
            for i in range(n)]
    return pts


# ---------------- 分位数预留 ----------------
def test_p95_reserve_uses_percentile_and_jump_not_mean():
    """252 个窗口样本：p95=0.74845，但当前 0.761 + 5 日跃升 0.005 更大 → 0.766。"""
    r = derive_no_cap_reserve(_linear_points())

    assert r.allowed is True
    assert r.status == STATUS_OK
    assert r.samples == 252
    assert r.u_reserve == D("0.766")
    assert r.u_reserve > r.u_p50  # 明确不是均值口径
    assert r.delta_u_p95_5d == D("0.005")


def test_drawdown_worst_dominates_quiet_percentile():
    """回撤 >3% 当日占用 88%，即使平时都在 50% 也要按 88% 预留。"""
    pts = [UtilizationPoint(utilization=D("0.5"), equity=D("1000000"))
           for _ in range(252)]
    pts[100] = UtilizationPoint(
        utilization=D("0.88"), equity=D("1000000"), drawdown=D("0.06"))
    r = derive_no_cap_reserve(pts, exclude_first_days=0)

    assert r.u_dd_worst == D("0.88")
    assert r.u_reserve == D("0.88")


def test_small_sample_adds_margin_and_stays_below_target():
    pts = [UtilizationPoint(utilization=D("0.5"), equity=D("1000000"))
           for _ in range(100)]
    r = derive_no_cap_reserve(pts, exclude_first_days=0)

    assert r.status == STATUS_DEGRADED
    assert "small_sample_margin" in r.degraded
    assert r.u_reserve == D("0.55")


def test_insufficient_history_refuses_lending():
    """样本 <60：拒绝外借，u_reserve 取目标利用率（等价不预留）。"""
    pts = [UtilizationPoint(utilization=D("0.5"), equity=D("1000000"))
           for _ in range(59)]
    r = derive_no_cap_reserve(pts, exclude_first_days=0)

    assert r.status == STATUS_NO_HISTORY
    assert r.allowed is False
    assert r.u_reserve == D("0.95")
    assert "拒绝外借" in "".join(r.reasons)


def test_gate_closed_days_excluded_by_default():
    pts = [UtilizationPoint(utilization=D("0.5"), equity=D("1000000"))
           for _ in range(251)]
    pts.append(UtilizationPoint(
        utilization=D("0.99"), equity=D("1000000"), gate_closed=True))

    excluded = derive_no_cap_reserve(pts, exclude_first_days=0)
    assert excluded.excluded["gate_closed"] == 1
    assert excluded.u_max == D("0.5")

    included = derive_no_cap_reserve(
        pts, exclude_first_days=0, include_gate_closed=True)
    assert included.u_max == D("0.99")
    assert included.u_reserve == D("0.95")  # 目标利用率封顶


def test_equity_jump_point_excluded_as_capital_flow_noise():
    pts = [UtilizationPoint(utilization=D("0.5"), equity=D("1000000"))
           for _ in range(251)]
    pts.append(UtilizationPoint(utilization=D("0.99"), equity=D("2000000")))

    r = derive_no_cap_reserve(pts, exclude_first_days=0)
    assert r.excluded["equity_jump"] == 1
    assert r.u_max == D("0.5")


def test_policy_max_uses_historical_peak_with_jump_guard():
    r = derive_no_cap_reserve(_linear_points(), policy="max")
    assert r.u_max == D("0.761")
    assert r.u_reserve == D("0.766")  # max(历史峰值, 当前+跃升)


def test_policy_manual_requires_value():
    pts = _linear_points()
    missing = derive_no_cap_reserve(pts, policy="manual")
    assert missing.status == STATUS_NOT_DERIVABLE
    assert missing.allowed is False

    given = derive_no_cap_reserve(
        pts, policy="manual", manual_reserve=D("0.8"))
    assert given.status == STATUS_OK
    assert given.u_reserve == D("0.8")


def test_unknown_policy_is_not_derivable():
    r = derive_no_cap_reserve(_linear_points(), policy="mean")
    assert r.status == STATUS_NOT_DERIVABLE
    assert r.allowed is False


def test_clamp_target_utilization():
    pts = _linear_points(n=262, start=D("0.90"), step=D("0.001"))
    r = derive_no_cap_reserve(pts, target_utilization=D("0.95"))
    assert r.u_reserve == D("0.95")


# ---------------- 可外借额度 ----------------
def test_lendable_subtracts_protect_and_floor():
    r = derive_lendable(
        cash=D("1000000"), equity=D("2000000"), in_flight_buy=D("100000"),
        own_need_cash=D("200000"), min_cash_reserve_pct=D("0.03"),
        enabled=True,
    )
    assert r.status == STATUS_OK
    assert r.cash_floor == D("60000")
    assert r.protect == D("360000")
    assert r.lendable == D("640000")


def test_insufficient_cash_reserves_instead_of_lending():
    r = derive_lendable(
        cash=D("300000"), equity=D("2000000"), in_flight_buy=D("100000"),
        own_need_cash=D("200000"), min_cash_reserve_pct=D("0.03"),
        enabled=True,
    )
    assert r.lendable == D("0")
    assert "不足则预留" in "".join(r.reasons)


def test_min_lend_threshold_blocks_small_surplus():
    r = derive_lendable(
        cash=D("1000000"), equity=D("2000000"), in_flight_buy=D("0"),
        own_need_cash=D("200000"), min_cash_reserve_pct=D("0.03"),
        min_lend_unit=D("2000000"), enabled=True,
    )
    assert r.lendable == D("0")
    assert r.threshold == D("2000000")


def test_missing_in_flight_is_not_derivable():
    r = derive_lendable(
        cash=D("1000000"), equity=D("2000000"), in_flight_buy=None,
        own_need_cash=D("0"), enabled=True,
    )
    assert r.status == STATUS_NOT_DERIVABLE
    assert r.lendable is None


def test_missing_min_cash_reserve_defaults_to_three_percent():
    r = derive_lendable(
        cash=D("1000000"), equity=D("2000000"), in_flight_buy=D("0"),
        own_need_cash=D("0"), enabled=True,
    )
    assert r.status == STATUS_DEGRADED
    assert "min_cash_reserve_pct_defaulted" in r.degraded
    assert r.cash_floor == D("60000")


def test_reserve_not_allowed_blocks_lending():
    pts = [UtilizationPoint(utilization=D("0.5"), equity=D("1000000"))
           for _ in range(30)]
    reserve = derive_no_cap_reserve(pts, exclude_first_days=0)
    assert reserve.allowed is False

    r = derive_lendable(
        cash=D("1000000"), equity=D("2000000"), in_flight_buy=D("0"),
        own_need_cash=D("0"), reserve=reserve, enabled=True,
    )
    assert r.lendable == D("0")
    assert r.status == STATUS_NO_HISTORY


def test_reserve_converts_to_cash_need_via_gross_exposure():
    pts = [UtilizationPoint(utilization=D("0.5"), equity=D("1000000"))
           for _ in range(252)]
    reserve = derive_no_cap_reserve(pts, exclude_first_days=0)
    assert reserve.u_reserve == D("0.5")

    r = derive_lendable(
        cash=D("2000000"), equity=D("2000000"), in_flight_buy=D("0"),
        reserve=reserve, gross_exposure=D("1000000"),
        min_cash_reserve_pct=D("0.03"), enabled=True,
    )
    assert r.own_need_cash == D("0")  # 已用 100 万 = 50% 预留，无需额外留现金
    assert r.lendable == D("1940000")


def test_lendable_disabled_by_default():
    r = derive_lendable(
        cash=D("1000000"), equity=D("2000000"), in_flight_buy=D("0"),
        own_need_cash=D("0"),
    )
    assert r.status == STATUS_DISABLED
    assert r.lendable is None
