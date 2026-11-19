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
from datetime import date, datetime
from decimal import Decimal
from typing import Dict, Optional, Tuple


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
