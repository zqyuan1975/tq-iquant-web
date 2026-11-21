# -*- coding: utf-8 -*-
"""行业方向门控与排序的只读时间轴（2026-11-22 行业层参与门控决策）。

数据来源：881 二级行业**日线**经 ``rating_core.compute_rating_frame`` 计算，
逐日得到 direction / strength（cur_str）/ bull_dur / bias_z / overbought。

查询语义与市场门控一致：**右移一日**——取严格早于查询日的最近一条记录，
即「昨日收盘的行业状态管今日开仓」，绝不使用当日收盘信息。

缺失语义：行业未知、该日无记录、历史不足 300 根、整批取数失败，一律返回
``None``；由决策层 fail-open 放行并记录 WARNING + 决策事件（与大盘门控一致）。
"""
from __future__ import annotations

import bisect
from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, Mapping, Optional

#: 两种行业门控模式（均默认关闭；关闭时决策层不读本模块）
INDUSTRY_RELIEF = "industry_relief"
INDUSTRY_STRICT = "industry_strict"
INDUSTRY_MODES = (INDUSTRY_RELIEF, INDUSTRY_STRICT)


@dataclass(frozen=True)
class IndustryState:
    """单个行业某一交易日的评级快照（日线口径）。"""

    direction: int          # -1 空头 / 0 未定 / 1,2 多头
    strength: float         # cur_str（当前方向那一侧的总强度）
    bull_dur: int           # 多头持续天数（空头/未定时为 0）
    bias_z: Optional[float]  # 乖离率 250 日滚动 z（不足为 None）
    overbought: bool        # z >= 2.5

    @property
    def is_bull(self) -> bool:
        return self.direction in (1, 2)

    @property
    def is_bear(self) -> bool:
        return self.direction == -1


class IndustryTracker:
    """逐行业逐日的只读状态轴 + 个股→行业映射。

    线程约束：回测单线程 / 实盘主池串行调用；构造后不再修改。
    """

    def __init__(
        self,
        states: Mapping[str, Mapping[date, IndustryState]],
        membership: Mapping[str, str],
        unavailable: Optional[Mapping[str, str]] = None,
        error: Optional[str] = None,
    ):
        self._states: Dict[str, Dict[date, IndustryState]] = {
            str(code): dict(days or {}) for code, days in (states or {}).items()
        }
        self._days: Dict[str, list] = {
            code: sorted(days) for code, days in self._states.items()
        }
        self._membership: Dict[str, str] = {
            str(stock): str(ind) for stock, ind in (membership or {}).items()
        }
        # 个股 → 行业 的反查缓存（映射本身是静态的，缺失时也缓存 None 以免重复查）
        self._unavailable: Dict[str, str] = dict(unavailable or {})
        self.error = error

    # ---------------- 基础属性 ----------------

    @property
    def available(self) -> bool:
        return bool(self._states)

    def industry_of(self, stock_code: str) -> Optional[str]:
        return self._membership.get(stock_code)

    def industry_count(self) -> int:
        return len(self._states)

    def stock_count(self) -> int:
        return len(self._membership)

    def span(self) -> Optional[tuple]:
        starts = [v[0] for v in self._days.values() if v]
        ends = [v[-1] for v in self._days.values() if v]
        if not starts:
            return None
        return (min(starts), max(ends))

    def unavailable_industries(self) -> Dict[str, str]:
        return dict(self._unavailable)

    # ---------------- 查询（右移一日） ----------------

    def state_on(self, industry_code: str, day: date) -> Optional[IndustryState]:
        """行业在 day 的生效状态 = 严格早于 day 的最近一条日线记录。"""
        days = self._days.get(industry_code)
        if not days:
            return None
        i = bisect.bisect_left(days, day)
        if i <= 0:
            return None
        return self._states[industry_code][days[i - 1]]

    def state_for_stock(self, stock_code: str, day: date) -> Optional[IndustryState]:
        ind = self.industry_of(stock_code)
        if ind is None:
            return None
        return self.state_on(ind, day)

    def bull_dur_for_stock(self, stock_code: str, day: date) -> int:
        """个股所属行业的领先天数（多头持续天数）；缺失/非多头 → 0。"""
        state = self.state_for_stock(stock_code, day)
        if state is None or not state.is_bull:
            return 0
        return int(state.bull_dur or 0)

    def strength_for_stock(self, stock_code: str, day: date) -> float:
        state = self.state_for_stock(stock_code, day)
        return float(state.strength) if state is not None else 0.0

    def is_bull_for_stock(self, stock_code: str, day: date) -> Optional[bool]:
        """True 多头 / False 非多头 / None 数据缺失（调用方 fail-open）。"""
        state = self.state_for_stock(stock_code, day)
        return None if state is None else state.is_bull

    def missing_for_stock(self, stock_code: str, day: date) -> Optional[str]:
        """数据缺失原因（None = 数据可用）。用于 WARNING 与决策事件留痕。"""
        if self.error:
            return f"行业数据不可用：{self.error}"
        ind = self.industry_of(stock_code)
        if ind is None:
            return "个股未映射到 881 二级行业"
        reason = self._unavailable.get(ind)
        if reason:
            return f"行业 {ind} 不可用（{reason}）"
        if self.state_on(ind, day) is None:
            return f"行业 {ind} 在 {day} 无历史状态"
        return None

    def summary(self) -> dict:
        span = self.span()
        return {
            "available": self.available,
            "industries": self.industry_count(),
            "stocks": self.stock_count(),
            "span": [str(span[0]), str(span[1])] if span else None,
            "unavailable": len(self._unavailable),
            "error": self.error,
        }
