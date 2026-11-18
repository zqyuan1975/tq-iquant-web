"""时点股票池判定内核（J7，2026-11-18）。

回测/实盘共用同一份判定：给定 ``stock_pool_memberships`` 的生效区间，回答
「该交易日这只票在不在池里」。**默认不启用**——组合 ``pool_pit_enabled=0`` 时
commodity 不构建本对象、``Portfolio.pool_timeline`` 为 None，闸门第一行返回，
行为与改动前逐字一致。

口径：
  * 区间两端**含**（``effective_from <= day <= effective_to``；``effective_to=None``
    表示仍在池内）；
  * ``covered(day)`` = 该日是否存在任何有效区间。为 False 时调用方 **fail-open
    放行**（回落当前 ``stock_pool_stocks``），并在组合内记一条
    ``pool_pit_uncovered`` 事件——绝不因为「历史没录」把整段回测变成零交易；
  * 只拦 OPEN/ADD，卖出与风控不受影响（由 ``Portfolio._signal_to_order`` 保证）。
"""

import hashlib
import json
from datetime import date
from typing import Dict, Iterable, List, Optional, Tuple

POOL_TIMELINE_VERSION = "manual_memberships_v1"


def _norm_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _row_get(row, name):
    if isinstance(row, dict):
        return row.get(name)
    return getattr(row, name, None)


class PoolTimeline:
    """时点成分索引：code -> 有序区间列表（按 from 升序，互不重叠由维护方保证）。"""

    def __init__(self, intervals: Dict[str, List[Tuple[date, Optional[date]]]]):
        self._intervals = {
            code: tuple(sorted(items, key=lambda p: p[0]))
            for code, items in intervals.items() if items
        }
        self._starts = {
            code: [p[0] for p in items] for code, items in self._intervals.items()
        }
        self._covered_cache: Dict[date, bool] = {}

    def __len__(self) -> int:
        return sum(len(v) for v in self._intervals.values())

    @property
    def is_empty(self) -> bool:
        return not self._intervals

    @property
    def codes(self) -> Tuple[str, ...]:
        return tuple(sorted(self._intervals))

    def contains(self, code: str, day: date) -> bool:
        if day is None:
            return False
        for start, end in self._intervals.get(code, ()):
            if start <= day and (end is None or day <= end):
                return True
        return False

    def covered(self, day: date) -> bool:
        """该交易日是否有任何有效成分记录（用于 fail-open 判定）。"""
        if day is None or self.is_empty:
            return False
        hit = self._covered_cache.get(day)
        if hit is not None:
            return hit
        covered = False
        for items in self._intervals.values():
            for start, end in items:
                if start <= day and (end is None or day <= end):
                    covered = True
                    break
            if covered:
                break
        self._covered_cache[day] = covered
        return covered

    def codes_on(self, day: date) -> Tuple[str, ...]:
        return tuple(code for code in self.codes if self.contains(code, day))

    def digest(self) -> str:
        """事件集摘要（进 params_snapshot 与增量指纹）：区间任何变动都改值。"""
        payload = [
            [code, star.isoformat(), end.isoformat() if end else None]
            for code, items in sorted(self._intervals.items())
            for star, end in items
        ]
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def build_pool_timeline(rows: Iterable) -> PoolTimeline:
    """从 ORM 行 / dict 列表构建（字段：stock_code / effective_from / effective_to）。"""
    grouped: Dict[str, List[Tuple[date, Optional[date]]]] = {}
    for row in rows or []:
        code = (_row_get(row, "stock_code") or "").strip()
        start = _norm_date(_row_get(row, "effective_from"))
        if not code or start is None:
            continue
        end = _norm_date(_row_get(row, "effective_to"))
        if end is not None and end < start:
            continue
        grouped.setdefault(code, []).append((start, end))
    return PoolTimeline(grouped)
