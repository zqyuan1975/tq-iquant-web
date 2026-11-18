"""F3：缺失分钟段时间点 → 连续段压缩（日终缺失清单）。

引擎在缺口补派/周期边界放弃时把「仍缺」的分钟时间点登记为当日缺失；日终把
这些散点压成连续区间输出，便于人工判断是否影响决策。

纯函数、无副作用，便于单测。
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Iterable, List


def compress_missing_segments(minutes: Iterable[datetime]) -> List[dict]:
    """把缺失分钟时间点合并为连续区间。

    - 去重并排序，秒/微秒截断到分钟；
    - 相邻（间隔 ≤1 分钟）的时间点合并为一段；
    - 午休/跨时段/断线造成的空档自然断开为多段；
    - 返回 [{"start": iso, "end": iso, "minutes": n}, ...]。
    """
    points = sorted({
        m.replace(second=0, microsecond=0)
        for m in (minutes or [])
        if isinstance(m, datetime)
    })
    if not points:
        return []
    out: List[dict] = []
    start = prev = points[0]
    for cur in points[1:]:
        if cur - prev <= timedelta(minutes=1):
            prev = cur
            continue
        out.append(_segment(start, prev))
        start = prev = cur
    out.append(_segment(start, prev))
    return out


def _segment(start: datetime, end: datetime) -> dict:
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "minutes": int((end - start).total_seconds() // 60) + 1,
    }
