"""市场多空状态（门控状态卡）服务 — 备选股池页「市场多空状态」区。

口径与回测/实盘门控完全一致：同一 ``compute_gate_states`` 内核、
同一 ``MarketGateTracker`` 右移一日语义。本模块只做「取数 → 算状态 → 组装
展示字段」，不落库、不缓存历史（状态卡天然可复算，落库反而引入一致性负担）。

进程内短时缓存（2026-11-18）：同一 ``(code, period)`` 在 ``_CACHE_TTL_SECONDS``
内复用上次结果，避免用户每进一次备选股池页就触发一次全量 TQ 取数
（TQ 取数会短暂冻结通达信客户端）。缓存不跨自然日（23:59 后强制重算的意思是
缓存键不含日期，但 TTL 只有数分钟，跨日场景由 TTL 自然过期覆盖）。
"""
import logging
import time
from datetime import date, timedelta
from typing import Dict, Optional, Tuple

from core.engine.crisis_alpha import CRISIS_BENCHMARKS
from core.engine.market_gate import (
    GATE_WARMUP_DAYS,
    MarketGateUnavailableError,
    compute_gate_states,
)
from core.services.backtest_service import build_gate_ohlcv

logger = logging.getLogger(__name__)

# 门控默认基准（与组合层新建默认一致）：中证全指
DEFAULT_GATE_INDEX = "000985.CSI"

# 缓存 TTL：5 分钟。刷新按钮走 ``force=True`` 绕过缓存（用户显式要求最新）。
_CACHE_TTL_SECONDS = 300
_cache: Dict[Tuple[str, str], Tuple[float, dict]] = {}

_DIRECTION_LABELS = {-1: "空头", 0: "未定", 1: "多头", 2: "多头"}

_BENCHMARK_NAMES = {spec["code"]: spec["name"] for spec in CRISIS_BENCHMARKS}


def _transitions(days: list, states: dict, limit: int) -> list:
    """最近 limit 次状态转折（新→旧），元素 {date, state}。"""
    out = []
    prev = None
    for d in days:
        cur = states[d]
        if prev is not None and cur != prev:
            out.append({"date": d.isoformat(), "state": cur})
        prev = cur
    return list(reversed(out[-limit:])) if limit > 0 else []


def _compute(code: str, period: str, count: int, history: int) -> dict:
    """实算一次状态卡（不走缓存）。取数失败抛 MarketGateUnavailableError。"""
    # 日历天数按「根数 × 1.6 + 预热」估算，保证 MA288 有足够前置样本
    span_days = max(GATE_WARMUP_DAYS + 60, int(count * 1.6) + 30)
    end = date.today()
    start = end - timedelta(days=span_days)
    ohlcv = build_gate_ohlcv(code, start, end, period=period)
    if ohlcv.is_empty():
        raise MarketGateUnavailableError(
            f"门控基准 {code} 无法取数（无 {period} K 线）。"
            f"请确认该指数已在通达信中下载。"
        )
    states, strengths = compute_gate_states(ohlcv)
    if not states:
        raise MarketGateUnavailableError(
            f"门控基准 {code} 历史不足，无法判定多空状态（区间 {start}~{end}）。"
            f"请先在通达信中补齐该指数日线数据。"
        )
    days = sorted(states)
    as_of = days[-1]
    direction = int(states[as_of])
    # 当前状态的起始日与持续交易日数：向前回溯到最近一次状态变化。
    # 状态序列已做 carry-forward（0 只可能出现在最前端预热区），故等值回溯即可。
    idx = len(days) - 1
    while idx > 0 and states[days[idx - 1]] == direction:
        idx -= 1
    since = days[idx]
    duration = len(days) - idx
    return {
        "code": code,
        "benchmark_name": _BENCHMARK_NAMES.get(code, code),
        "period": period,
        "as_of": as_of.isoformat(),
        "direction": direction,
        "direction_label": _DIRECTION_LABELS.get(direction, "未定"),
        "strength": int(strengths.get(as_of) or 0),
        "since": since.isoformat() if hasattr(since, "isoformat") else str(since),
        "duration_bars": int(duration),
        "transitions": _transitions(days, states, history),
        "bars": len(days),
    }


def fetch_gate_status(benchmark_index: Optional[str] = None,
                      period: str = "1d",
                      count: int = 500,
                      history: int = 5,
                      force: bool = False) -> dict:
    """市场多空状态卡数据（带 5 分钟进程内缓存）。

    ``force=True``（前端刷新按钮）跳过缓存直取最新，并回填缓存。
    取数/公式无输出一律抛 MarketGateUnavailableError（路由层转 400）。
    """
    code = (benchmark_index or DEFAULT_GATE_INDEX).strip() or DEFAULT_GATE_INDEX
    key = (code, period)
    now = time.monotonic()
    if not force:
        hit = _cache.get(key)
        if hit is not None and now - hit[0] <= _CACHE_TTL_SECONDS:
            data = dict(hit[1])
            # count/history 只影响展示深度，命中缓存时按本次参数重新截取转折列表
            data["transitions"] = data["transitions"][:max(0, history)]
            data["cached"] = True
            return data
    data = _compute(code, period, count, history)
    _cache[key] = (now, data)
    out = dict(data)
    out["cached"] = False
    return out


def clear_cache() -> None:
    """清空缓存（测试/基准切换用）。"""
    _cache.clear()
