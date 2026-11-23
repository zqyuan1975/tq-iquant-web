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
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

from core.engine.crisis_alpha import BENCHMARK_EXTRA_NAMES, CRISIS_BENCHMARKS
from core.engine.market_gate import (
    GATE_WARMUP_DAYS,
    MarketGateUnavailableError,
    compute_gate_states,
    parse_gate_indices,
)
from core.engine.market_rules import trading_days_between
from core.services.backtest_service import build_gate_ohlcv
from core.tq.gate import TDXLockTimeout, TdxTaskBlocked
from core.tq.utils import TDXConnectionError

logger = logging.getLogger(__name__)

# 门控默认基准（与组合层新建默认一致）：上证指数
# （通达信可自动下载日线；中证全指需手工下载，缺数据会让状态卡算不出来）
DEFAULT_GATE_INDEX = "000001.SH"

# E4：状态数据落后超过 N 个交易日 → 标记陈旧（前端标黄，提示可能不准）。
GATE_STALE_TRADING_DAYS = 3

# 缓存 TTL：5 分钟。刷新按钮走 ``force=True`` 绕过缓存（用户显式要求最新）。
_CACHE_TTL_SECONDS = 300
_cache: Dict[Tuple[str, str], Tuple[float, dict]] = {}

_DIRECTION_LABELS = {-1: "空头", 0: "未定", 1: "多头", 2: "多头"}

_BENCHMARK_NAMES = {spec["code"]: spec["name"] for spec in CRISIS_BENCHMARKS}
_BENCHMARK_NAMES.update(BENCHMARK_EXTRA_NAMES)


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


def _last_trading_day_on_or_before(day: date) -> date:
    """往前找最近的交易日（周末 + 内置休市表；表未收录年份按仅周末）。"""
    from core.engine.live.calendar import BUILTIN_CLOSURES

    d = day
    for _ in range(3700):
        if d.weekday() < 5 and d not in BUILTIN_CLOSURES.get(
                d.year, frozenset()):
            return d
        d -= timedelta(days=1)
    return d


def expected_latest_trading_day(now: Optional[datetime] = None) -> date:
    """应有完整日线落定的最近交易日。

    15:00 前今天尚未收盘，预期最新完整日线为「今天之前」的最后一个交易日；
    15:00 后（含）今天的日线应已可算，若非交易日则前移。
    """
    now = now or datetime.now()
    today = now.date()
    anchor = (today if (now.hour, now.minute) >= (15, 0)
              else today - timedelta(days=1))
    return _last_trading_day_on_or_before(anchor)


def _unavailable(message: str, reason: str) -> MarketGateUnavailableError:
    """构造带原因码的不可用异常（reason: gate_blocked/timeout/channel_unavailable/no_data）。"""
    exc = MarketGateUnavailableError(message)
    exc.reason = reason  # noqa: B010 — 附加属性，路由层据此区分文案
    return exc


def _compute(code: str, period: str, count: int, history: int) -> dict:
    """实算一次状态卡（不走缓存）。取数失败抛 MarketGateUnavailableError。

    取数层区分三种失败原因（闸拒绝 / 超时 / 真无数据），对外文案随原因变化，
    不再把「通道闸挡住了」误报成「请在通达信下载指数日线」。
    """
    # 日历天数按「根数 × 1.6 + 预热」估算，保证 MA288 有足够前置样本
    span_days = max(GATE_WARMUP_DAYS + 60, int(count * 1.6) + 30)
    end = date.today()
    start = end - timedelta(days=span_days)
    try:
        ohlcv = build_gate_ohlcv(
            code, start, end, period=period, raise_gate_errors=True)
    except TdxTaskBlocked as exc:
        raise _unavailable(
            f"门控基准 {code} 取数被 TQ 通道闸拒绝（{exc.reason}"
            f"{'，' + exc.window if exc.window else ''}）：{exc}",
            "gate_blocked") from exc
    except TDXLockTimeout as exc:
        raise _unavailable(
            f"门控基准 {code} 取数等待 TQ 通道超时：{exc}", "timeout") from exc
    except TDXConnectionError as exc:
        raise _unavailable(
            f"门控基准 {code} 取数失败：TQ 通道不可用（{exc}）",
            "channel_unavailable") from exc
    if ohlcv.is_empty():
        raise _unavailable(
            f"门控基准 {code} 无法取数（无 {period} K 线）。"
            f"请确认该指数已在通达信中下载。",
            "no_data")
    states, strengths = compute_gate_states(ohlcv)
    if not states:
        raise _unavailable(
            f"门控基准 {code} 历史不足，无法判定多空状态（区间 {start}~{end}）。"
            f"请先在通达信中补齐该指数日线数据。",
            "no_data")
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
    # E4：数据新鲜度 —— 与「应已落定的最近交易日」比对，超过 N 个交易日标黄。
    latest = expected_latest_trading_day()
    stale_days = trading_days_between(as_of, latest)
    if latest <= as_of:
        stale_days = 0
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
        "latest_trading_day": latest.isoformat(),
        "stale_days": int(stale_days),
        "stale": bool(stale_days >= GATE_STALE_TRADING_DAYS),
        # 新增字段（只增不改）：来源与数据时间，供前端区分实时取数/实盘快照
        "source": "live_fetch",
        "data_time": as_of.isoformat(),
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


def fetch_gate_status_from_snapshot(db, benchmark_index: Optional[str] = None,
                                    period: str = "1d",
                                    history: int = 5) -> Optional[dict]:
    """最近一次成功落库的实盘门控快照 → 状态卡结构（无可用快照返回 None）。

    看板优先走此路径：读 ``live_gate_snapshots``（实盘每日门控落库），**不触发
    TQ 取数**，因此不会被 14:55–15:10 保护窗或回测重任务挡住。返回结构与
    ``fetch_gate_status`` 兼容（只新增字段，不删/不改名），未知的明细字段
    （since/duration_bars/bars/transitions）置空。
    """
    from core.models import LiveGateSnapshot

    row = (
        db.query(LiveGateSnapshot)
        .filter(LiveGateSnapshot.direction.isnot(None),
                LiveGateSnapshot.as_of.isnot(None))
        .order_by(LiveGateSnapshot.snap_date.desc(),
                  LiveGateSnapshot.updated_at.desc(),
                  LiveGateSnapshot.id.desc())
        .first()
    )
    if row is None:
        return None
    codes = [c.strip() for c in str(row.benchmark_index or "").split(",")
             if c.strip()]
    code = codes[0] if codes else (benchmark_index or DEFAULT_GATE_INDEX)
    latest = expected_latest_trading_day()
    stale_days = trading_days_between(row.as_of, latest)
    if latest <= row.as_of:
        stale_days = 0
    direction = int(row.direction)
    return {
        "code": code,
        "benchmark_name": _BENCHMARK_NAMES.get(code, code),
        "period": period,
        "as_of": row.as_of.isoformat(),
        "direction": direction,
        "direction_label": _DIRECTION_LABELS.get(direction, "未定"),
        "strength": int(row.strength or 0),
        # 快照只存当日生效方向/强度/基准与数据截至日，明细字段置空（不伪造）
        "since": None,
        "duration_bars": None,
        "transitions": [],
        "bars": None,
        "latest_trading_day": latest.isoformat(),
        "stale_days": int(stale_days),
        "stale": bool(stale_days >= GATE_STALE_TRADING_DAYS),
        "cached": True,
        # 新增字段（只增不改）：来源、数据时间与快照元信息
        "source": "live_snapshot",
        "data_time": row.as_of.isoformat(),
        "snapshot_at": (row.updated_at.isoformat(timespec="seconds")
                        if row.updated_at else None),
        "snapshot_date": (row.snap_date.isoformat() if row.snap_date else None),
        "snapshot_session_id": row.live_session_id,
        "snapshot_portfolio_id": row.portfolio_strategy_id,
        "requested_code": benchmark_index or DEFAULT_GATE_INDEX,
        "code_matched": bool((benchmark_index or DEFAULT_GATE_INDEX) in codes),
    }


# E5：两套市场状态机制的作用域与优先级（只读说明，前端原样渲染）
GATE_SCOPE_NOTES = (
    {
        "key": "market_gate",
        "name": "市场多空门控",
        "scope": "组合级（每个组合独立配置基准/投票指数）",
        "effect": "空头状态只拦开仓/加仓（OPEN/ADD），持仓不强平；可选按比例减仓一次",
        "timing": "右移一日（昨日收盘状态管今日开仓）；基准取数失败 fail-open 放行",
    },
    {
        "key": "regime_filter",
        "name": "市场状态闸（B5）",
        "scope": "策略级（基准周线 bull / 其它）",
        "effect": "bull 态用 trend_* 覆盖移动止损与最大持仓；只改风控参数，不拦开仓",
        "timing": "每日 on_bar 前注入当前状态；未启用时全部回落原参数",
    },
    {
        "key": "precedence",
        "name": "优先级",
        "scope": "两套机制并行，不互斥",
        "effect": "门控先决定能否开仓（策略 off 可豁免）；市场状态闸只调整已开仓位的止损/槽位",
        "timing": "先门控（开仓闸）→ 后风控参数（状态闸）",
    },
)


def fetch_gate_overview(db) -> dict:
    """E5 统一市场状态面板数据（只读，不改任何判定）。

    返回两套机制的静态作用域说明 + 各组合/子策略的当前配置，供前端一屏看清
    market_gate 与 regime_filter 各自作用域、优先级与生效配置。
    """
    from core.models import PortfolioStrategy, Strategy

    strategies = db.query(Strategy).order_by(Strategy.portfolio_id, Strategy.id).all()
    by_portfolio: Dict[int, list] = {}
    for s in strategies:
        by_portfolio.setdefault(s.portfolio_id, []).append(s)

    portfolios = []
    for p in db.query(PortfolioStrategy).order_by(PortfolioStrategy.id).all():
        benchmark = getattr(p, "benchmark_index", None) or DEFAULT_GATE_INDEX
        indices = parse_gate_indices(getattr(p, "market_gate_indices", None))
        rows = by_portfolio.get(p.id, [])
        force_on = any(
            (getattr(s, "market_gate_mode", None) or "inherit") == "on"
            for s in rows
        )
        portfolios.append({
            "id": p.id,
            "name": p.name,
            "market_gate_enabled": bool(getattr(p, "market_gate_enabled", False)),
            "effective_enabled": bool(
                getattr(p, "market_gate_enabled", False)) or force_on,
            "benchmark_index": benchmark,
            "indices": indices or [benchmark],
            "vote_mode": getattr(p, "market_gate_vote_mode", None) or "any",
            "strength_threshold": int(
                getattr(p, "market_gate_strength_threshold", 0) or 0),
            "bear_reduce_ratio": float(
                getattr(p, "market_gate_bear_reduce_ratio", 0) or 0),
            "engine": getattr(p, "market_gate_engine", None) or "python",
            "formula_name": getattr(p, "market_gate_formula_name", None) or "",
            "strategies": [{
                "id": s.id,
                "name": s.name,
                "regime_filter_enabled": bool(
                    getattr(s, "regime_filter_enabled", False)),
                "trend_trailing_stop_ratio": (
                    float(s.trend_trailing_stop_ratio)
                    if getattr(s, "trend_trailing_stop_ratio", None) is not None
                    else None),
                "trend_max_positions": getattr(s, "trend_max_positions", None),
                "market_gate_mode": (
                    getattr(s, "market_gate_mode", None) or "inherit"),
            } for s in rows],
        })
    return {"scopes": list(GATE_SCOPE_NOTES), "portfolios": portfolios}
