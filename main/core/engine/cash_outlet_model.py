"""收益率曲线与现金出口回测建模（0021 C5，影子口径）。

只估「闲置现金放在出口里的利息」与「可用时点」，**不证明通道可执行**；
执行可行性由 C8 实盘验证。收益永远单列在 ``cash_yield_contribution``，
不并入组合净值、不改变策略指标与成交链路——默认不传 plan 时逐位不变。
"""
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence, Tuple

CASH_OUTLET_MODEL_VERSION = "cash-outlet-model-c5"
DEFAULT_DAY_COUNT = 365
SUPPORTED_LIQUIDITY = ("t0", "t1", "term")


class CashOutletPlanError(ValueError):
    """plan 结构非法；调用方应降级为 ``enabled=False``，不要中断回测。"""


def _dec(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return d if d.is_finite() else None


def _fmt(value: Decimal) -> str:
    """金额/收益率字符串：去掉尾零，避免 ``40.0000`` 这类噪声。"""
    return format(value.normalize(), "f")


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def is_business_day(value: date) -> bool:
    """周一至周五；无节假日日历（口径限制，见模块注释）。"""
    return value.weekday() < 5


def add_business_days(start: date, days: int) -> date:
    """从 ``start`` 起前进 ``days`` 个工作日（0=当天，1=下一工作日）。"""
    if days <= 0:
        return start
    cur = start
    remaining = days
    while remaining > 0:
        cur += timedelta(days=1)
        if is_business_day(cur):
            remaining -= 1
    return cur


@dataclass(frozen=True)
class YieldCurve:
    """阶梯收益率曲线：``points`` 为 (生效日, 年化收益率)，按日期升序。"""

    points: Tuple[Tuple[date, Decimal], ...]

    @classmethod
    def from_raw(cls, raw: Any) -> "YieldCurve":
        if isinstance(raw, YieldCurve):
            return raw
        if raw is None:
            return cls(())
        items: List[Tuple[date, Decimal]] = []
        if isinstance(raw, dict):
            raw = [{"date": k, "annual_yield": v} for k, v in raw.items()]
        for item in raw:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                d, rate = _as_date(item[0]), _dec(item[1])
            elif isinstance(item, dict):
                d = _as_date(item.get("date"))
                rate = _dec(item.get("annual_yield", item.get("rate")))
            else:
                raise CashOutletPlanError("收益率曲线点结构非法：%r" % (item,))
            if d is None or rate is None:
                raise CashOutletPlanError("收益率曲线点缺少日期或利率：%r" % (item,))
            items.append((d, rate))
        items.sort(key=lambda p: p[0])
        return cls(tuple(items))

    def rate_on(self, day: date) -> Optional[Decimal]:
        """取「最近一个生效日 ≤ day」的利率；早于首个点返回 None（不外推）。"""
        rate = None
        for start, value in self.points:
            if start > day:
                break
            rate = value
        return rate


@dataclass(frozen=True)
class CashOutletPlan:
    outlet_code: str
    liquidity: str
    settlement_days: int
    day_count: int
    curve: YieldCurve
    min_amount: Decimal
    max_amount: Optional[Decimal] = None
    term_days: int = 0

    @classmethod
    def from_dict(cls, raw: Any) -> "CashOutletPlan":
        if isinstance(raw, CashOutletPlan):
            return raw
        if not isinstance(raw, dict):
            raise CashOutletPlanError("plan 必须是对象")
        code = str(raw.get("outlet_code") or "").strip()
        if not code:
            raise CashOutletPlanError("缺少 outlet_code")
        liquidity = str(raw.get("liquidity") or "").lower()
        if liquidity not in SUPPORTED_LIQUIDITY:
            raise CashOutletPlanError("liquidity 只能是 %s"
                                      % "/".join(SUPPORTED_LIQUIDITY))
        settlement = raw.get("settlement_days")
        settlement_days = 0 if settlement is None else int(settlement)
        if settlement_days < 0 or settlement_days > 7:
            raise CashOutletPlanError("settlement_days 必须在 0–7")
        term_days = int(raw.get("term_days") or 0)
        if liquidity == "term" and not (0 < term_days <= 7):
            raise CashOutletPlanError("term 品种的 term_days 必须在 1–7")
        min_amount = _dec(raw.get("min_amount")) or Decimal(0)
        max_amount = _dec(raw.get("max_amount"))
        if min_amount < 0 or (max_amount is not None and max_amount <= 0):
            raise CashOutletPlanError("金额区间非法")
        if max_amount is not None and max_amount < min_amount:
            raise CashOutletPlanError("max_amount < min_amount")

        curve = YieldCurve.from_raw(raw.get("yield_curve"))
        if not curve.points:
            annual = _dec(raw.get("annual_yield"))
            if annual is None:
                raise CashOutletPlanError("缺少 yield_curve 或 annual_yield")
            curve = YieldCurve(((date(1900, 1, 1), annual),))
        day_count = int(raw.get("day_count") or DEFAULT_DAY_COUNT)
        if day_count <= 0:
            raise CashOutletPlanError("day_count 必须 >0")

        return cls(
            outlet_code=code, liquidity=liquidity,
            settlement_days=settlement_days, day_count=day_count,
            curve=curve, min_amount=min_amount, max_amount=max_amount,
            term_days=term_days,
        )

    def available_after(self, deploy_day: date) -> date:
        """部署日 → 最早可用日：t0 当天，t1 下一工作日，term 到期+结算。"""
        if self.liquidity == "t0":
            return deploy_day
        extra = self.term_days + self.settlement_days \
            if self.liquidity == "term" else max(1, self.settlement_days)
        return add_business_days(deploy_day, extra)


def accrue(principal: Decimal, annual_yield: Decimal, days: int,
           day_count: int = DEFAULT_DAY_COUNT) -> Decimal:
    """单利计息：``principal × 年化 × days / day_count``。"""
    if principal <= 0 or days <= 0:
        return Decimal(0)
    return principal * annual_yield * Decimal(days) / Decimal(day_count)


def build_cash_yield_contribution(
    snapshots: Sequence[Dict[str, Any]],
    plan: Any,
    *,
    include_series: bool = False,
    max_series: int = 200,
) -> Dict[str, Any]:
    """按日快照估算闲置现金利息（只读，不改进现金与净值）。

    口径：
    - 用「上一日快照的现金」作为当日可部署额度，封顶 ``max_amount``；
      低于 ``min_amount`` 当日不计息；
    - 只有「上一日部署、当日已可用」（t0 当天、t1 次一工作日、term 到期）
      才计息；周末不计息（无节假日日历）；
    - 利率取「上一日」的阶梯利率，早于曲线首点不计息，不外推。
    """
    parsed = CashOutletPlan.from_dict(plan)
    rows = sorted(
        (s for s in snapshots if _as_date(s.get("snap_date")) is not None),
        key=lambda s: _as_date(s.get("snap_date")))

    total = Decimal(0)
    days = 0
    series: List[Dict[str, Any]] = []
    for prev, cur in zip(rows, rows[1:]):
        prev_day = _as_date(prev.get("snap_date"))
        cur_day = _as_date(cur.get("snap_date"))
        if cur_day <= prev_day or not is_business_day(cur_day):
            continue
        if parsed.available_after(prev_day) > cur_day:
            continue
        cash = _dec(prev.get("cash")) or Decimal(0)
        if cash < parsed.min_amount or cash <= 0:
            continue
        principal = min(cash, parsed.max_amount) \
            if parsed.max_amount is not None else cash
        rate = parsed.curve.rate_on(prev_day)
        if rate is None:
            continue
        interest = accrue(principal, rate, 1, parsed.day_count)
        total += interest
        days += 1
        if include_series and len(series) < max_series:
            series.append({
                "date": cur_day.isoformat(),
                "idle_cash": _fmt(principal),
                "annual_yield": _fmt(rate),
                "interest": _fmt(interest),
            })

    data: Dict[str, Any] = {
        "version": CASH_OUTLET_MODEL_VERSION,
        "enabled": True,
        "outlet_code": parsed.outlet_code,
        "liquidity": parsed.liquidity,
        "settlement_days": parsed.settlement_days,
        "term_days": parsed.term_days,
        "day_count": parsed.day_count,
        "min_amount": _fmt(parsed.min_amount),
        "max_amount": (_fmt(parsed.max_amount)
                       if parsed.max_amount is not None else None),
        "total_interest": _fmt(total),
        "days_accrued": days,
        "notes": [
            "利息单列，不并入净值/成交/策略指标",
            "周末不计息；无节假日日历",
            "只估收益与可用时点，不代表通道可执行",
        ],
    }
    if include_series:
        data["series"] = series
    return data
