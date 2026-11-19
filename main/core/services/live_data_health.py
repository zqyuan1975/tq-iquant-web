"""F2：实盘启动前数据体检 + 非盘中自动补齐（只读观测，不改交易语义）。

背景：实盘取数依赖通达信本地已下载数据。取数为空 → 周期新鲜度闸拦截 → 只记告警，
用户往往不知道是「数据没下载」。本模块提供：

- ``check_data_health``：逐 code × period 用现有 ``dispatcher.query_quote`` 读最新
  几根 bar，判定 ok / stale / empty / error（不落库、不下单、不预热引擎缓存）；
- ``auto_fill_missing``：**仅非盘中**（非交易日，或 <09:15 / >15:10）对缺失项用更大
  count 重新读一次（唤醒桥端/TQ 已落盘数据）；盘中一律不触发，只报告；
- ``run_health_check``：体检 + 可选补齐的组合入口。

现实边界（必须如实告知用户）：通达信 TQ 通道 ``query_quote`` 只读本地已有数据
（``TdxTqDispatcher.query_quote`` → ``TQData.get_history_raw``），平台**无法程序化
触发通达信下载**；这里的「自动补齐」是重新读取，读不到仍列入缺失清单，需人工在
客户端补下载。QMT 桥的落盘由桥侧负责。
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Dict, List, Optional

from core.engine.bar_poller import parse_bar_time
from core.engine.live.timing import now_shanghai

logger = logging.getLogger(__name__)

DATA_HEALTH_VERSION = "live_data_health_v1"

# 「盘中」窗口（含盘前集合竞价）：此窗口内不做自动补齐，避免与实盘取数抢通道。
_INTRADAY_START = time(9, 15)
_INTRADAY_END = time(15, 10)
# 体检读取根数：够判定最新 bar 日期即可，不拉长历史。
_CHECK_COUNT = 3
# 非盘中补齐读取根数：覆盖常见公式窗口，唤醒桥端本地已落盘数据。
_WARM_COUNT = 240
# 周期新鲜度容差（天）：周/月线的最新 bar 日期天然早于「上一交易日」。
_PERIOD_TOLERANCE_DAYS = {"1w": 7, "1mon": 31}
# 缺失清单最多返回的代码数（防止响应过大；总数另给 count）。
_MAX_CODES = 50


def _fallback_is_trading_day(d: date) -> bool:
    """无日历时退化为「工作日即可交易日」（与 fail-open 口径一致）。"""
    return d.weekday() < 5


def prev_trading_day(d: date, calendar=None) -> date:
    """返回严格早于 d 的最近一个交易日（最多回看 60 天）。"""
    cur = d - timedelta(days=1)
    for _ in range(60):
        is_td = (calendar.is_trading_day(cur) if calendar is not None
                 else _fallback_is_trading_day(cur))
        if is_td:
            return cur
        cur -= timedelta(days=1)
    return d - timedelta(days=1)


def is_intraday(now: datetime, calendar=None) -> bool:
    """是否处于盘中（非交易日恒 False；交易日 09:15–15:10 为 True）。"""
    today = now.date()
    is_td = (calendar.is_trading_day(today) if calendar is not None
             else _fallback_is_trading_day(today))
    if not is_td:
        return False
    return _INTRADAY_START <= now.time() <= _INTRADAY_END


def expected_bar_date(period: str, now: datetime, calendar=None) -> date:
    """该周期「最新一根已完成 bar」的期望日期下限。

    - 非交易日 / 盘前：上一交易日；
    - 分钟周期（1m/5m/…）：交易日 09:31 后期望当日；
    - 1d：交易日 15:05（日线落定）后期望当日，否则上一交易日；
    - 1w/1mon：用上一交易日 + 容差（周/月线本身日期更早）。
    """
    today = now.date()
    is_td = (calendar.is_trading_day(today) if calendar is not None
             else _fallback_is_trading_day(today))
    last = prev_trading_day(today, calendar)
    if not is_td:
        return last
    if period in ("1w", "1mon"):
        return last
    if period == "1d":
        return today if now.time() >= time(15, 5) else last
    return today if now.time() >= time(9, 31) else last


def _bar_stimes(bars) -> List[datetime]:
    out: List[datetime] = []
    for bar in bars or []:
        if not isinstance(bar, dict):
            continue
        bt = parse_bar_time(bar)
        if bt is not None:
            out.append(bt)
    return out


def classify_bar_freshness(period: str, bars, expected_date: date) -> dict:
    """单 code 单周期新鲜度：ok / stale / empty（纯函数，便于单测）。"""
    stimes = _bar_stimes(bars)
    if not stimes:
        return {"status": "empty", "last_bar": None}
    last = max(stimes)
    tolerance = _PERIOD_TOLERANCE_DAYS.get(period, 0)
    status = "ok" if last.date() >= expected_date - timedelta(days=tolerance) else "stale"
    return {"status": status, "last_bar": last.isoformat()}


def check_data_health(dispatcher, codes, periods, *, now: datetime = None,
                      calendar=None) -> dict:
    """逐 code × period 读取最新 bar 并汇总健康度（只读）。

    返回结构（可直接 JSON 序列化）：
    {version, checked_at, intraday, expected_dates, healthy, unavailable,
     periods: {p: {total, ok, stale, empty, error, stale_codes, empty_codes,
                   error_codes, stale_count, empty_count, error_count}},
     missing_codes: [...], missing_count}
    """
    now = now or now_shanghai()
    codes = [str(c) for c in (codes or []) if str(c)]
    periods = [str(p) for p in (periods or []) if str(p)]
    intraday = is_intraday(now, calendar)
    expected = {p: expected_bar_date(p, now, calendar) for p in periods}
    per_period: Dict[str, dict] = {}
    all_missing: List[str] = []
    total_checks = 0
    total_errors = 0
    for period in periods:
        stats = {"total": 0, "ok": 0, "stale": 0, "empty": 0, "error": 0,
                 "stale_codes": [], "empty_codes": [], "error_codes": []}
        for code in codes:
            if not code:
                continue
            stats["total"] += 1
            total_checks += 1
            try:
                bars = dispatcher.query_quote(code, period=period, count=_CHECK_COUNT)
            except Exception as exc:  # noqa: BLE001 — 桥离线/通道异常都不阻断体检
                stats["error"] += 1
                total_errors += 1
                if len(stats["error_codes"]) < _MAX_CODES:
                    stats["error_codes"].append(code)
                logger.debug("数据体检取数失败 code=%s period=%s: %s", code, period, exc)
                continue
            verdict = classify_bar_freshness(period, bars, expected[period])
            stats[verdict["status"]] += 1
            if verdict["status"] != "ok" and len(stats["%s_codes" % verdict["status"]]) < _MAX_CODES:
                stats["%s_codes" % verdict["status"]].append(code)
        stats["stale_count"] = stats["stale"]
        stats["empty_count"] = stats["empty"]
        stats["error_count"] = stats["error"]
        per_period[period] = stats
        for key in ("stale_codes", "empty_codes", "error_codes"):
            for code in stats[key]:
                if code not in all_missing:
                    all_missing.append(code)
    healthy = all(
        stats["stale"] == 0 and stats["empty"] == 0 and stats["error"] == 0
        for stats in per_period.values()
    )
    return {
        "version": DATA_HEALTH_VERSION,
        "checked_at": now.isoformat(),
        "intraday": intraday,
        "expected_dates": {p: d.isoformat() for p, d in expected.items()},
        "healthy": healthy,
        "unavailable": bool(total_checks) and total_errors == total_checks,
        "periods": per_period,
        "missing_codes": all_missing[:_MAX_CODES],
        "missing_count": len(all_missing),
    }


def auto_fill_missing(dispatcher, health: dict, codes, periods, *,
                      now: datetime = None, calendar=None) -> dict:
    """非盘中自动补齐：对缺失 (code, period) 用更大 count 重读一次。

    盘中禁止自动补齐（只报告）；非交易日 / 盘前盘后允许。重读只唤醒桥端/TQ
    已落盘数据，不触发通达信下载（平台无该能力）。
    返回 {triggered, reason, requested, warmed, failed, after?}；
    ``after`` 是补齐后的新体检结果（仅在 triggered 时附带）。
    """
    now = now or now_shanghai()
    if is_intraday(now, calendar):
        return {"triggered": False, "reason": "intraday", "requested": 0,
                "warmed": 0, "failed": 0}
    pairs: List[tuple] = []
    for period in periods:
        stats = (health.get("periods") or {}).get(period) or {}
        for key in ("stale_codes", "empty_codes", "error_codes"):
            for code in stats.get(key) or []:
                if (code, period) not in pairs:
                    pairs.append((code, period))
    if not pairs:
        return {"triggered": False, "reason": "healthy_or_no_missing",
                "requested": 0, "warmed": 0, "failed": 0}
    warmed = 0
    failed = 0
    for code, period in pairs:
        try:
            bars = dispatcher.query_quote(code, period=period, count=_WARM_COUNT)
            if bars:
                warmed += 1
            else:
                failed += 1
        except Exception:  # noqa: BLE001 — 补齐失败只计数，不阻断
            failed += 1
            logger.debug("数据补齐失败 code=%s period=%s", code, period, exc_info=True)
    after = check_data_health(dispatcher, codes, periods, now=now, calendar=calendar)
    return {"triggered": True, "reason": "off_hours", "requested": len(pairs),
            "warmed": warmed, "failed": failed, "after": after}


def run_health_check(dispatcher, codes, periods, *, auto_fill: bool = True,
                     now: datetime = None, calendar=None) -> dict:
    """体检 + 可选非盘中补齐；返回 {health, auto_fill}（health 为补齐后结果）。"""
    now = now or now_shanghai()
    health = check_data_health(dispatcher, codes, periods, now=now, calendar=calendar)
    result = {"health": health, "auto_fill": None}
    if auto_fill and not health.get("healthy"):
        fill = auto_fill_missing(dispatcher, health, codes, periods,
                                 now=now, calendar=calendar)
        after = fill.pop("after", None)
        result["auto_fill"] = fill
        if after is not None:
            result["health"] = after
    return result


def health_summary_text(health: dict) -> str:
    """体检结论 → 一句话（启动告警/通知复用）。"""
    if health.get("unavailable"):
        return "数据体检失败：行情通道不可用，无法读取本地数据"
    parts: List[str] = []
    for period, stats in (health.get("periods") or {}).items():
        bad = int(stats.get("stale", 0)) + int(stats.get("empty", 0))
        if bad:
            parts.append("%s 缺失/陈旧 %d 只" % (period, bad))
    if not parts:
        return "数据体检通过"
    shown = "、".join((health.get("missing_codes") or [])[:10])
    return "数据体检异常：%s（%s%s）；请确认通达信已下载对应周期数据" % (
        "，".join(parts), shown,
        " 等" if int(health.get("missing_count", 0)) > 10 else "",
    )
