"""无单仓上限时的保守预留与会话可外借额度（0021 C1，纯计算）。

依据 docs/plans/0019 §3 与 0021 C1：

- ``u_reserve = min(T_p, max(u_p95, u_dd_worst, u_current + Δu_p95_5d))``；
- 样本不足（< ``min_samples``，默认 60）**拒绝外借**，不拿均值或默认值蒙；
- 只有「预留后仍有富余」才登记可外借额度，避免行情到来时被迫召回。

本模块先于 C2/C3 落地，仅提供纯函数，不接入任何交易链路、不被引擎默认调用。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, List, Optional, Sequence

from core.engine.capital_contracts import (
    DEFAULT_MIN_CASH_RESERVE_PCT,
    STATUS_DEGRADED,
    STATUS_DISABLED,
    STATUS_NOT_DERIVABLE,
    STATUS_OK,
    dec_str as _dec_str,
    to_decimal as _to_decimal,
)

STATUS_NO_HISTORY = "no_history"

DEFAULT_MIN_SAMPLES = 60
DEFAULT_WINDOW = 252
DEFAULT_LEND_MIN_AMOUNT_PCT = Decimal("0.05")
DEFAULT_EXCLUDE_FIRST_DAYS = 20
DEFAULT_EQUITY_JUMP_THRESHOLD = Decimal("0.20")
DD_WORST_THRESHOLD = Decimal("0.03")
SMALL_SAMPLE_MARGIN = Decimal("0.05")
POLICIES = ("p90", "p95", "max", "manual")


def _percentile(values: Sequence[Decimal], p: Decimal) -> Decimal:
    """线性插值分位数（p ∈ [0,1]），与 Excel PERCENTILE.INC 口径一致。"""
    if not values:
        return Decimal(0)
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = Decimal(len(ordered) - 1) * p
    lo = int(pos)
    hi = min(lo + 1, len(ordered) - 1)
    frac = pos - Decimal(lo)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * frac


@dataclass(frozen=True)
class UtilizationPoint:
    """单日利用率样本。

    - ``utilization``：持仓市值 / 总资产（0019 §3.2 的 u_t）；
    - ``equity``：当日总资产，用于剔除增资/撤资造成的 >20% 跳变；
    - ``drawdown``：当日回撤（正数，0.05 表示回撤 5%），用于回撤期最坏占用；
    - ``gate_closed``：当日门控关闭，默认口径下剔除。
    """

    utilization: Decimal
    equity: Optional[Decimal] = None
    drawdown: Optional[Decimal] = None
    gate_closed: bool = False


@dataclass(frozen=True)
class ReserveResult:
    status: str
    policy: str
    u_reserve: Optional[Decimal]
    u_p50: Optional[Decimal]
    u_p90: Optional[Decimal]
    u_p95: Optional[Decimal]
    u_max: Optional[Decimal]
    u_dd_worst: Optional[Decimal]
    delta_u_p95_5d: Optional[Decimal]
    u_current: Optional[Decimal]
    samples: int
    excluded: Dict[str, int] = field(default_factory=dict)
    allowed: bool = False
    degraded: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "policy": self.policy,
            "u_reserve": _dec_str(self.u_reserve),
            "u_p50": _dec_str(self.u_p50),
            "u_p90": _dec_str(self.u_p90),
            "u_p95": _dec_str(self.u_p95),
            "u_max": _dec_str(self.u_max),
            "u_dd_worst": _dec_str(self.u_dd_worst),
            "delta_u_p95_5d": _dec_str(self.delta_u_p95_5d),
            "u_current": _dec_str(self.u_current),
            "samples": self.samples,
            "excluded": dict(self.excluded),
            "allowed": self.allowed,
            "degraded": list(self.degraded),
            "reasons": list(self.reasons),
        }


def _filter_points(
    points: Sequence[UtilizationPoint],
    *,
    window: int,
    include_gate_closed: bool,
    exclude_first_days: int,
    equity_jump_threshold: Decimal,
) -> tuple[List[UtilizationPoint], Dict[str, int]]:
    excluded = {"startup_days": 0, "equity_jump": 0, "gate_closed": 0,
                "invalid": 0}
    valid: List[UtilizationPoint] = []
    for p in points:
        u = _to_decimal(p.utilization)
        if u is None or u < 0:
            excluded["invalid"] += 1
            continue
        valid.append(UtilizationPoint(
            utilization=u,
            equity=_to_decimal(p.equity),
            drawdown=_to_decimal(p.drawdown),
            gate_closed=bool(p.gate_closed),
        ))

    if exclude_first_days > 0 and len(valid) > exclude_first_days:
        excluded["startup_days"] = exclude_first_days
        valid = valid[exclude_first_days:]

    kept: List[UtilizationPoint] = []
    prev_equity: Optional[Decimal] = None
    for p in valid:
        if (prev_equity is not None and p.equity is not None and prev_equity > 0):
            change = abs(p.equity / prev_equity - 1)
            if change > equity_jump_threshold:
                excluded["equity_jump"] += 1
                prev_equity = p.equity
                continue
        if p.gate_closed and not include_gate_closed:
            excluded["gate_closed"] += 1
            prev_equity = p.equity or prev_equity
            continue
        kept.append(p)
        if p.equity is not None:
            prev_equity = p.equity

    if window > 0 and len(kept) > window:
        excluded["window_trimmed"] = len(kept) - window
        kept = kept[-window:]
    return kept, excluded


def derive_no_cap_reserve(
    points: Sequence[UtilizationPoint],
    *,
    policy: str = "p95",
    window: int = DEFAULT_WINDOW,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    target_utilization: Any = Decimal("0.95"),
    current_utilization: Any = None,
    manual_reserve: Any = None,
    include_gate_closed: bool = False,
    exclude_first_days: int = DEFAULT_EXCLUDE_FIRST_DAYS,
    equity_jump_threshold: Any = DEFAULT_EQUITY_JUMP_THRESHOLD,
) -> ReserveResult:
    """无单仓上限时的利用率预留（0019 §3.3 / §3.5）。

    返回 ``allowed=False`` 表示当期不得外借；此时 ``u_reserve`` 取目标利用率，
    调用方应把可外借额度置 0。
    """
    policy = (policy or "p95").strip().lower()
    if policy not in POLICIES:
        return ReserveResult(
            status=STATUS_NOT_DERIVABLE, policy=policy, u_reserve=None,
            u_p50=None, u_p90=None, u_p95=None, u_max=None,
            u_dd_worst=None, delta_u_p95_5d=None, u_current=None,
            samples=0, reasons=["policy 非法，仅支持 p90/p95/max/manual"],
        )

    target = _to_decimal(target_utilization)
    if target is None:
        target = Decimal("0.95")
    if target < 0:
        target = Decimal(0)
    if target > 1:
        target = Decimal(1)

    kept, excluded = _filter_points(
        points,
        window=window,
        include_gate_closed=include_gate_closed,
        exclude_first_days=exclude_first_days,
        equity_jump_threshold=_to_decimal(equity_jump_threshold)
        or DEFAULT_EQUITY_JUMP_THRESHOLD,
    )
    n = len(kept)
    us = [p.utilization for p in kept]

    if n < min_samples:
        return ReserveResult(
            status=STATUS_NO_HISTORY, policy=policy, u_reserve=target,
            u_p50=_percentile(us, Decimal("0.5")) if us else None,
            u_p90=_percentile(us, Decimal("0.9")) if us else None,
            u_p95=_percentile(us, Decimal("0.95")) if us else None,
            u_max=max(us) if us else None,
            u_dd_worst=None, delta_u_p95_5d=None, u_current=None,
            samples=n, excluded=excluded, allowed=False,
            degraded=["no_history"],
            reasons=[
                "有效样本 %d < 最小样本 %d，拒绝外借（Protect=cash，"
                "u_reserve=目标利用率 %.2f）" % (n, min_samples, float(target)),
            ],
        )

    if policy == "manual":
        manual = _to_decimal(manual_reserve)
        if manual is None:
            return ReserveResult(
                status=STATUS_NOT_DERIVABLE, policy=policy, u_reserve=None,
                u_p50=None, u_p90=None, u_p95=None, u_max=None,
                u_dd_worst=None, delta_u_p95_5d=None, u_current=None,
                samples=n, excluded=excluded,
                reasons=["policy=manual 但未提供 no_cap_reserve_manual"],
            )
        base = manual
    else:
        u_p50 = _percentile(us, Decimal("0.5"))
        u_p90 = _percentile(us, Decimal("0.9"))
        u_p95 = _percentile(us, Decimal("0.95"))
        u_max = max(us)
        u_dd_worst = max(
            (p.utilization for p in kept
             if p.drawdown is not None and p.drawdown > DD_WORST_THRESHOLD),
            default=Decimal(0),
        )
        jumps = [us[i] - us[i - 5] for i in range(5, n)]
        delta = _percentile(jumps, Decimal("0.95")) if jumps else Decimal(0)
        delta = max(Decimal(0), delta)
        current = _to_decimal(current_utilization)
        if current is None:
            current = us[-1]
        current = max(Decimal(0), current)

        degraded: List[str] = []
        reasons: List[str] = []
        if policy == "p90":
            base = max(u_p90, current + delta)
        elif policy == "max":
            base = max(u_max, current + delta)
        else:
            base = max(u_p95, u_dd_worst, current + delta)

        if min_samples <= n < window:
            base = base + SMALL_SAMPLE_MARGIN
            degraded.append("small_sample_margin")
            reasons.append("样本 %d < 窗口 %d，分位值加 %.2f 安全边际"
                           % (n, window, float(SMALL_SAMPLE_MARGIN)))

        u_reserve = max(Decimal(0), min(target, base))
        reasons.append(
            "预留取 max(分位全量/回撤最坏/当前+5日跃升) 并受目标利用率封顶；"
            "不用均值：均值会把低占用与行情占用混在一起，行情到来时只能召回")
        return ReserveResult(
            status=STATUS_DEGRADED if degraded else STATUS_OK,
            policy=policy, u_reserve=u_reserve, u_p50=u_p50, u_p90=u_p90,
            u_p95=u_p95, u_max=u_max, u_dd_worst=u_dd_worst,
            delta_u_p95_5d=delta, u_current=current, samples=n,
            excluded=excluded, allowed=True, degraded=degraded, reasons=reasons,
        )

    u_reserve = max(Decimal(0), min(target, base))
    return ReserveResult(
        status=STATUS_OK, policy=policy, u_reserve=u_reserve,
        u_p50=_percentile(us, Decimal("0.5")),
        u_p90=_percentile(us, Decimal("0.9")),
        u_p95=_percentile(us, Decimal("0.95")),
        u_max=max(us), u_dd_worst=None, delta_u_p95_5d=None,
        u_current=_to_decimal(current_utilization), samples=n,
        excluded=excluded, allowed=True,
        reasons=["manual 口径：使用用户指定比例，风险由用户承担"],
    )


@dataclass(frozen=True)
class LendableResult:
    status: str
    cash: Optional[Decimal]
    equity: Optional[Decimal]
    own_need_cash: Optional[Decimal]
    cash_floor: Optional[Decimal]
    in_flight_buy: Optional[Decimal]
    protect: Optional[Decimal]
    lendable: Optional[Decimal]
    threshold: Optional[Decimal]
    degraded: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "cash": _dec_str(self.cash),
            "equity": _dec_str(self.equity),
            "own_need_cash": _dec_str(self.own_need_cash),
            "cash_floor": _dec_str(self.cash_floor),
            "in_flight_buy": _dec_str(self.in_flight_buy),
            "protect": _dec_str(self.protect),
            "lendable": _dec_str(self.lendable),
            "threshold": _dec_str(self.threshold),
            "degraded": list(self.degraded),
            "reasons": list(self.reasons),
        }


def derive_lendable(
    *,
    cash: Any,
    equity: Any,
    in_flight_buy: Any,
    own_need_cash: Any = None,
    reserve: Optional[ReserveResult] = None,
    gross_exposure: Any = None,
    min_cash_reserve_pct: Any = None,
    min_lend_unit: Any = 0,
    lend_min_amount_pct: Any = DEFAULT_LEND_MIN_AMOUNT_PCT,
    enabled: bool = False,
) -> LendableResult:
    """可外借额度（0019 §3.6）：不足则预留、富余才外借。"""
    if not enabled:
        return LendableResult(
            status=STATUS_DISABLED, cash=None, equity=None,
            own_need_cash=None, cash_floor=None, in_flight_buy=None,
            protect=None, lendable=None, threshold=None,
            reasons=["推导器未启用（enabled=False），不做任何计算"],
        )

    cash_d = _to_decimal(cash)
    equity_d = _to_decimal(equity)
    in_flight = _to_decimal(in_flight_buy)
    if cash_d is None or equity_d is None or equity_d <= 0 or in_flight is None:
        return LendableResult(
            status=STATUS_NOT_DERIVABLE, cash=cash_d, equity=equity_d,
            own_need_cash=None, cash_floor=None, in_flight_buy=in_flight,
            protect=None, lendable=None, threshold=None,
            reasons=["现金/权益/在途买入承诺存在缺失，无法判断可外借额度"],
        )

    degraded: List[str] = []
    reasons: List[str] = []

    if reserve is not None and not reserve.allowed:
        return LendableResult(
            status=reserve.status, cash=cash_d, equity=equity_d,
            own_need_cash=None, cash_floor=None, in_flight_buy=in_flight,
            protect=None, lendable=Decimal(0), threshold=None,
            degraded=list(reserve.degraded),
            reasons=["预留口径未通过（%s），当期不外借" % reserve.status]
            + list(reserve.reasons),
        )

    need = _to_decimal(own_need_cash)
    if need is None:
        if reserve is None or reserve.u_reserve is None:
            return LendableResult(
                status=STATUS_NOT_DERIVABLE, cash=cash_d, equity=equity_d,
                own_need_cash=None, cash_floor=None, in_flight_buy=in_flight,
                protect=None, lendable=None, threshold=None,
                reasons=["缺少自有需求金额（own_need_cash）且无可用预留口径"],
            )
        expo = _to_decimal(gross_exposure)
        if expo is None:
            return LendableResult(
                status=STATUS_NOT_DERIVABLE, cash=cash_d, equity=equity_d,
                own_need_cash=None, cash_floor=None, in_flight_buy=in_flight,
                protect=None, lendable=None, threshold=None,
                reasons=["缺少当前毛敞口，无法把预留利用率换算成现金需求"],
            )
        need = max(Decimal(0), reserve.u_reserve * equity_d - expo)
    need = max(Decimal(0), need)

    floor_pct = _to_decimal(min_cash_reserve_pct)
    if floor_pct is None:
        floor_pct = DEFAULT_MIN_CASH_RESERVE_PCT
        degraded.append("min_cash_reserve_pct_defaulted")
        reasons.append("min_cash_reserve_pct 缺失，按 3% 保守预留")
    floor_pct = max(Decimal(0), floor_pct)
    floor = floor_pct * equity_d

    protect = need + floor + in_flight
    lendable = max(Decimal(0), cash_d - protect)

    unit = _to_decimal(min_lend_unit) or Decimal(0)
    pct = _to_decimal(lend_min_amount_pct)
    if pct is None:
        pct = DEFAULT_LEND_MIN_AMOUNT_PCT
    threshold = max(unit, max(Decimal(0), pct) * cash_d)

    if lendable < threshold:
        reasons.append(
            "可外借 %s 低于最小出借阈值 %s（不足则预留），不外借"
            % (_dec_str(lendable), _dec_str(threshold)))
        lendable = Decimal(0)
    else:
        reasons.append("富余 %s ≥ 阈值 %s，才允许登记 LoanLot"
                       % (_dec_str(lendable), _dec_str(threshold)))

    return LendableResult(
        status=STATUS_DEGRADED if degraded else STATUS_OK,
        cash=cash_d, equity=equity_d, own_need_cash=need, cash_floor=floor,
        in_flight_buy=in_flight, protect=protect, lendable=lendable,
        threshold=threshold, degraded=degraded, reasons=reasons,
    )
