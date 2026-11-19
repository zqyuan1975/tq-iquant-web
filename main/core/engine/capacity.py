"""容量预算（层 1）：单笔参与率 + 5 日累计 + 同日同票跨策略聚合。

设计口径（保守值，均可由组合/run 参数覆盖）：

- 单日参与率：中小盘 0.5%、超大市值 1%（当日成交额 ≥ 50 亿视作超大市值代理）；
- 5 日累计：中小盘 1.5%、超大市值 2%（含当日已累计成交量，因果）；
- 三重预算取最小：本 bar 成交量、当日累计成交量、最近 5 个交易日累计成交量；
- 三个账本共用**同一个实例**：多组合回测里同一 code 同一天的所有策略订单
  自动聚合，避免「每个策略各占 1%」变相放大到 N%。

成交量口径：``klines[code][period]["Volume"]`` 与引擎 ``price_index`` 同源
（TQ Volume，单位=股）。逐日序列按「1d 优先、否则最细周期」懒加载，只有真正
下过单的 code 才建索引，避免全池预展开的内存开销。
"""

from bisect import bisect_right
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple


# 取数优先级：日线优先（天然是「当日成交量」）；没有日线时按最细周期逐日累加。
_SERIES_PERIODS = ("1d", "1w", "1mon", "1h", "60m", "30m", "15m", "5m", "1m")


class CapacityBudget:
    def __init__(
        self,
        klines: Optional[dict] = None,
        *,
        daily_small: Decimal = Decimal("0.005"),
        daily_large: Decimal = Decimal("0.01"),
        five_day_small: Decimal = Decimal("0.015"),
        five_day_large: Decimal = Decimal("0.02"),
        large_turnover: Decimal = Decimal("5000000000"),
        window_days: int = 5,
        ttl_bars: int = 3,
    ):
        self._klines = klines or {}
        self.daily_small = daily_small
        self.daily_large = daily_large
        self.five_day_small = five_day_small
        self.five_day_large = five_day_large
        self.large_turnover = large_turnover
        self.window_days = max(1, int(window_days))
        self.ttl_bars = max(1, int(ttl_bars))
        # code -> (交易日列表, {交易日: (时间戳列表, 累计成交量列表)})
        self._series: Dict[str, tuple] = {}
        # 本 bar 已成交（code, bar_time）→ 股数；同日跨策略聚合
        self._used_bar: Dict[tuple, int] = {}
        # 当日已成交（code, date）→ 股数；5 日窗口用
        self._day_used: Dict[tuple, int] = {}

    # ---------- 逐日成交量索引（懒加载） ----------

    def _load_series(self, code: str) -> Tuple[list, dict]:
        cached = self._series.get(code)
        if cached is not None:
            return cached
        periods = self._klines.get(code) or {}
        rows = []
        for period in _SERIES_PERIODS:
            df = periods.get(period)
            if df is None or "datetime" not in df.columns or "Volume" not in df.columns:
                continue
            rows = list(zip(df["datetime"].to_list(), df["Volume"].to_list()))
            break
        by_day: Dict[date, list] = {}
        for ts, volume in rows:
            if ts is None:
                continue
            day = ts.date() if hasattr(ts, "date") else ts
            by_day.setdefault(day, []).append((ts, int(volume or 0)))
        days = sorted(by_day)
        detail: Dict[date, tuple] = {}
        for day in days:
            items = sorted(by_day[day], key=lambda item: item[0])
            times = [item[0] for item in items]
            cumulative = []
            total = 0
            for _, volume in items:
                total += volume
                cumulative.append(total)
            detail[day] = (times, cumulative)
        value = (days, detail)
        self._series[code] = value
        return value

    def _day_volume_so_far(self, code: str, bar_time) -> int:
        """当日截至 bar_time 的累计成交量（日线即当日总量；分钟线为因果累计）。"""
        days, detail = self._load_series(code)
        if not days:
            return 0
        day = bar_time.date() if hasattr(bar_time, "date") else bar_time
        entry = detail.get(day)
        if entry is None:
            return 0
        times, cumulative = entry
        idx = bisect_right(times, bar_time) - 1
        return cumulative[idx] if idx >= 0 else 0

    def _window(self, code: str, bar_time) -> list:
        """最近 window_days 个交易日（含当日，按 code 自身的数据日历）。"""
        days, _ = self._load_series(code)
        if not days:
            return []
        day = bar_time.date() if hasattr(bar_time, "date") else bar_time
        idx = bisect_right(days, day) - 1
        if idx < 0:
            return []
        return days[max(0, idx - self.window_days + 1): idx + 1]

    def _window_volume(self, code: str, bar_time) -> int:
        """5 日窗口成交量：历史日取全日量，当日取截至当前 bar 的累计量（无前视）。"""
        days, detail = self._load_series(code)
        total = 0
        today = bar_time.date() if hasattr(bar_time, "date") else bar_time
        for day in self._window(code, bar_time):
            entry = detail.get(day)
            if entry is None:
                continue
            if day == today:
                times, cumulative = entry
                idx = bisect_right(times, bar_time) - 1
                total += cumulative[idx] if idx >= 0 else 0
            else:
                total += entry[1][-1]
        return total

    def _window_used(self, code: str, bar_time) -> int:
        return sum(
            self._day_used.get((code, day), 0)
            for day in self._window(code, bar_time)
        )

    # ---------- 预算 ----------

    def _is_large(self, bar_close, bar_volume: int) -> bool:
        if bar_close is None:
            return False
        try:
            turnover = Decimal(str(bar_close)) * Decimal(int(bar_volume))
        except Exception:  # noqa: BLE001 — 脏数据不阻断回测，按中小盘保守处理
            return False
        return turnover >= self.large_turnover

    def allow(self, code: str, bar_time, bar_volume, bar_close,
              requested: int, lot: int = 1) -> Tuple[int, str]:
        """返回 (可成交股数, 约束来源)；来源 ∈ bar/day/5d/no_volume/none。

        lot>1 时按手向下取整（BUY 路径）；SELL 传 lot=1 允许零股。
        """
        if requested <= 0:
            return 0, "none"
        if bar_volume is None or int(bar_volume) <= 0:
            return 0, "no_volume"
        volume = int(bar_volume)
        large = self._is_large(bar_close, volume)
        daily = self.daily_large if large else self.daily_small
        five_day = self.five_day_large if large else self.five_day_small
        day = bar_time.date() if hasattr(bar_time, "date") else bar_time

        bar_budget = int(daily * Decimal(volume)) - self._used_bar.get(
            (code, bar_time), 0)
        day_budget = int(daily * Decimal(self._day_volume_so_far(code, bar_time))) \
            - self._day_used.get((code, day), 0)
        five_budget = int(five_day * Decimal(self._window_volume(code, bar_time))) \
            - self._window_used(code, bar_time)

        allowed, reason = min(
            ((bar_budget, "bar"), (day_budget, "day"), (five_budget, "5d")),
            key=lambda item: item[0],
        )
        allowed = max(0, min(int(requested), int(allowed)))
        if lot > 1:
            allowed = allowed // lot * lot
        return allowed, reason

    def consume(self, code: str, bar_time, quantity: int) -> None:
        """成交后记账（以实际成交量为准，含资金/敞口二次缩量）。"""
        qty = int(quantity or 0)
        if qty <= 0:
            return
        key = (code, bar_time)
        self._used_bar[key] = self._used_bar.get(key, 0) + qty
        day = bar_time.date() if hasattr(bar_time, "date") else bar_time
        day_key = (code, day)
        self._day_used[day_key] = self._day_used.get(day_key, 0) + qty


def capacity_model_version(model: str, *, daily_small: Decimal, daily_large: Decimal,
                           five_day_small: Decimal, five_day_large: Decimal,
                           impact_k: Decimal) -> str:
    """写进 ``params_snapshot.capacity_model_version`` 的可读版本串。

    linear（旧口径）固定 ``v1``；sqrt 形如
    ``v2-sqrt-p0.5%/1%-5d1.5%/2%-k0.10``，k 变化即新旧结果不可直接混比。
    """
    if model != "sqrt":
        return "v1"
    def pct(value: Decimal) -> str:
        return ("%s" % (Decimal(str(value)) * 100).normalize()) + "%"
    return "v2-sqrt-p%s/%s-5d%s/%s-k%s" % (
        pct(daily_small), pct(daily_large),
        pct(five_day_small), pct(five_day_large),
        Decimal(str(impact_k)).normalize(),
    )


# ---------------------------------------------------------------------------
# C3：容量曲线 / 斜率 / ADV 参与率重叠
# ---------------------------------------------------------------------------
#
# 口径（0019 §4 / 0020 H4-H6）：
# - 曲线点必须由调用方用「截至该规模、仅历史数据」的联合回测结果构造，
#   本模块只做纯计算，不取行情、不引入未来成交量；
# - 边际收益是「目标规模下的斜率」，不是全期平均收益；
# - 数据不足返回 ``PENDING_DATA``，所有斜率/衰减置 None，绝不猜测。

CAPACITY_CURVE_VERSION = "capacity-curve-c3"
STATUS_CURVE_OK = "ok"
STATUS_CURVE_PENDING = "PENDING_DATA"
DEFAULT_MIN_CURVE_POINTS = 3
DEFAULT_MAX_CURVE_GAP = Decimal("1.0")  # 相邻规模点最大间隔（倍数）


@dataclass(frozen=True)
class CapacityCurvePoint:
    """一个规模点：``scale`` 为目标规模倍数（1.0=当前规模），``net_return`` 为其净收益。"""

    scale: Decimal
    net_return: Decimal


@dataclass(frozen=True)
class CapacityCurve:
    status: str
    points: int
    slope: Optional[Decimal] = None
    marginal_1x: Optional[Decimal] = None
    marginal_2x: Optional[Decimal] = None
    return_1x: Optional[Decimal] = None
    return_2x: Optional[Decimal] = None
    curve_decay_2x: Optional[Decimal] = None
    reasons: List[str] = field(default_factory=list)
    version: str = CAPACITY_CURVE_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "status": self.status,
            "points": self.points,
            "slope": _dec(self.slope),
            "marginal_1x": _dec(self.marginal_1x),
            "marginal_2x": _dec(self.marginal_2x),
            "return_1x": _dec(self.return_1x),
            "return_2x": _dec(self.return_2x),
            "curve_decay_2x": _dec(self.curve_decay_2x),
            "reasons": list(self.reasons),
        }


def _dec(value: Any) -> Optional[str]:
    if value is None:
        return None
    return format(Decimal(str(value)), "f")


def _num(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except Exception:  # noqa: BLE001 — 非数值视为缺失
        return None
    return d if d.is_finite() else None


def _linear_interp(points: Sequence[CapacityCurvePoint],
                   x: Decimal) -> Optional[Decimal]:
    """在已按 scale 升序的点上线性插值收益；超出范围返回 None（不外推）。"""
    if not points:
        return None
    if x < points[0].scale or x > points[-1].scale:
        return None
    for left, right in zip(points, points[1:]):
        if left.scale <= x <= right.scale:
            if right.scale == left.scale:
                return left.net_return
            ratio = (x - left.scale) / (right.scale - left.scale)
            return left.net_return + ratio * (right.net_return - left.net_return)
    return points[-1].net_return


def _central_slope(points: Sequence[CapacityCurvePoint],
                   x: Decimal) -> Optional[Decimal]:
    """在 x 处取中心差分斜率；端点退化为单侧差分。"""
    exact = [i for i, p in enumerate(points) if p.scale == x]
    if exact:
        i = exact[0]
        if i == 0 and len(points) > 1:
            left, right = points[0], points[1]
        elif i == len(points) - 1 and i > 0:
            left, right = points[-2], points[-1]
        elif 0 < i < len(points) - 1:
            left, right = points[i - 1], points[i + 1]
        else:
            return None
    else:
        for left, right in zip(points, points[1:]):
            if left.scale < x < right.scale:
                break
        else:
            return None
    span = right.scale - left.scale
    if span == 0:
        return None
    return (right.net_return - left.net_return) / span


def compute_capacity_curve(
    points: Sequence[Any],
    *,
    min_points: int = DEFAULT_MIN_CURVE_POINTS,
    max_gap: Any = DEFAULT_MAX_CURVE_GAP,
) -> CapacityCurve:
    """容量曲线斜率与边际收益（C3）。

    ``points`` 支持 ``CapacityCurvePoint`` 或 ``(scale, net_return)`` 二元组；
    只使用调用方给出的历史点，按 scale 升序后计算：

    - ``slope``：首尾整体斜率（每增加 1 倍规模，净收益的变化）；
    - ``marginal_1x`` / ``marginal_2x``：1x/2x 处中心差分斜率（目标规模的边际收益）；
    - ``curve_decay_2x``：``1 - return(2x)/return(1x)``，1x 收益 ≤0 时置 None。

    任一点缺失/非有限、规模重复、点数不足或相邻间隔超过 ``max_gap`` 时
    返回 ``PENDING_DATA``，所有数值为 None。
    """
    parsed: List[CapacityCurvePoint] = []
    for item in points or []:
        if isinstance(item, CapacityCurvePoint):
            scale, ret = item.scale, item.net_return
        else:
            try:
                scale, ret = item[0], item[1]
            except Exception:  # noqa: BLE001 — 结构错误按数据不足处理
                return CapacityCurve(STATUS_CURVE_PENDING, 0,
                                     reasons=["points 结构非法"])
        s, r = _num(scale), _num(ret)
        if s is None or r is None:
            return CapacityCurve(STATUS_CURVE_PENDING, len(parsed),
                                 reasons=["存在缺失/非有限点，不能猜测"])
        parsed.append(CapacityCurvePoint(scale=s, net_return=r))

    parsed.sort(key=lambda p: p.scale)
    if len(parsed) < max(2, int(min_points)):
        return CapacityCurve(
            STATUS_CURVE_PENDING, len(parsed),
            reasons=["有效规模点 %d < 最少 %d，返回 PENDING_DATA"
                     % (len(parsed), max(2, int(min_points)))])
    for left, right in zip(parsed, parsed[1:]):
        if right.scale == left.scale:
            return CapacityCurve(STATUS_CURVE_PENDING, len(parsed),
                                 reasons=["规模点重复：%s" % _dec(left.scale)])
        if max_gap is not None and right.scale - left.scale > _num(max_gap):
            return CapacityCurve(STATUS_CURVE_PENDING, len(parsed),
                                 reasons=["相邻规模点间隔过大（>%s）"
                                          % _dec(max_gap)])

    total_span = parsed[-1].scale - parsed[0].scale
    slope = ((parsed[-1].net_return - parsed[0].net_return) / total_span
             if total_span else None)
    m1 = _central_slope(parsed, Decimal("1"))
    m2 = _central_slope(parsed, Decimal("2"))
    r1 = _linear_interp(parsed, Decimal("1"))
    r2 = _linear_interp(parsed, Decimal("2"))
    decay = (Decimal(1) - r2 / r1) if (r1 is not None and r2 is not None
                                       and r1 > 0) else None
    reasons = ["斜率与边际收益仅用调用方提供的规模点计算，不读取未来成交量"]
    return CapacityCurve(
        STATUS_CURVE_OK, len(parsed),
        slope=slope, marginal_1x=m1, marginal_2x=m2,
        return_1x=r1, return_2x=r2, curve_decay_2x=decay, reasons=reasons,
    )


@dataclass(frozen=True)
class CapacityParticipation:
    """ADV 参与率与同票重叠（C3）；缺失 ADV 时 status=PENDING_DATA。"""

    status: str
    lender_participation: Optional[Decimal] = None
    borrower_participation: Optional[Decimal] = None
    same_code_participation: Optional[Decimal] = None
    overlap: Optional[Decimal] = None
    bid_probability: Optional[Decimal] = None
    reasons: List[str] = field(default_factory=list)
    version: str = CAPACITY_CURVE_VERSION

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "status": self.status,
            "lender_participation": _dec(self.lender_participation),
            "borrower_participation": _dec(self.borrower_participation),
            "same_code_participation": _dec(self.same_code_participation),
            "overlap": _dec(self.overlap),
            "bid_probability": _dec(self.bid_probability),
            "reasons": list(self.reasons),
        }


def compute_participation(
    *,
    adv_by_code: Mapping[str, Any],
    lender_amount_by_code: Mapping[str, Any],
    borrower_amount_by_code: Mapping[str, Any],
    contested_ratio: Any = Decimal("0.02"),
) -> CapacityParticipation:
    """ADV 参与率与同票重叠（C3）。

    - ``lender/borrower_participation``：各自日均下单额 / 对应 ADV 之和；
    - ``same_code_participation``：双方共同持仓票的合计额 / 这些票的 ADV 之和（H4 用）；
    - ``overlap``：共同票上较小一方的占用 / 共同票 ADV 之和（重叠程度）；
    - ``bid_probability``：同票参与率 / ``contested_ratio``（2% 视为完全争抢），
      封顶 1；这是待实盘校准的启发式，评估侧只作软判据。

    ADV 缺失或分母 ≤0 时返回 ``PENDING_DATA``，不把缺失当 0 参与率。
    """
    codes = set(lender_amount_by_code) | set(borrower_amount_by_code)
    if not codes:
        return CapacityParticipation(STATUS_CURVE_PENDING,
                                     reasons=["无下单金额数据"])

    adv: Dict[str, Decimal] = {}
    for code in codes:
        value = _num(adv_by_code.get(code))
        if value is not None and value > 0:
            adv[code] = value
    if not adv:
        return CapacityParticipation(
            STATUS_CURVE_PENDING,
            reasons=["ADV 全部缺失/为 0，无法计算参与率"])

    def amount(mapping: Mapping[str, Any], code: str) -> Decimal:
        return _num(mapping.get(code)) or Decimal(0)

    total_adv = sum(adv.values())
    common = [c for c in codes
              if c in adv
              and amount(lender_amount_by_code, c) > 0
              and amount(borrower_amount_by_code, c) > 0]
    common_adv = sum(adv[c] for c in common)
    lender_common = sum(amount(lender_amount_by_code, c) for c in common)
    borrower_common = sum(amount(borrower_amount_by_code, c) for c in common)
    same_code = ((lender_common + borrower_common) / common_adv
                 if common_adv > 0 else None)
    overlap = (sum(min(amount(lender_amount_by_code, c),
                       amount(borrower_amount_by_code, c)) for c in common)
               / common_adv if common_adv > 0 else None)
    cap = _num(contested_ratio) or Decimal("0.02")
    bid = (min(Decimal(1), same_code / cap)
           if same_code is not None and cap > 0 else None)

    return CapacityParticipation(
        STATUS_CURVE_OK if common else "ok_no_common",
        lender_participation=(sum(amount(lender_amount_by_code, c)
                                  for c in adv) / total_adv),
        borrower_participation=(sum(amount(borrower_amount_by_code, c)
                                    for c in adv) / total_adv),
        same_code_participation=same_code,
        overlap=overlap,
        bid_probability=bid,
        reasons=["参与率分母为对应 ADV 之和；缺失 ADV 的票被排除并单独计数"],
    )
