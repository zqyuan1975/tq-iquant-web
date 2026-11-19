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

import inspect
import logging
import time as _time
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
# 单只取价慢调用阈值（秒）：超过即写 WARNING 并记入 slow_quotes，便于定位哪只票/
# 哪条通道在拖（2026-11-19 实测掘金桥单只 /quote 18–26s）。
_SLOW_QUOTE_SECONDS = 5.0
# slow_quotes 最多保留条数（避免响应体膨胀）。
_MAX_SLOW_QUOTES = 20
# 单只取价硬超时（秒）：预算只在「每只之前」检查，在飞的那次拦不住，实测掘金桥
# 出现过单只 12.67s 把 2s 预算拖穿。这里给 httpx 请求本身加硬超时，超时即放弃
# 该只并计入未校验（``timeout_codes``），保证单会话体检不超过约 2×预算。
# 取值理由：默认 2s ≈ 一次正常桥应答 + 一次排队余量（QMT /quote 实测 0.31s，
# 掘金正常 5.9s）；带预算调用时再压到预算的 60%（2s 预算 → 1.2s），保证单只
# 超时**严格低于**整体预算，不会一只就吃满预算。桥慢（掘金 5.9s）时该只会被
# 误杀为「未校验/超时」而不是「数据缺失」——如实标注，不误报缺失、不阻断接口。
_QUOTE_HARD_TIMEOUT_SECONDS = 2.0
# 单只超时占整体预算的上限比例：留出余量给下一只/收尾，避免一只吃满预算。
_QUOTE_TIMEOUT_BUDGET_RATIO = 0.6
# 是否支持 per-call timeout 的能力缓存（按 dispatcher 类型）。
_TIMEOUT_SUPPORT: Dict[type, bool] = {}


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


def _supports_call_timeout(dispatcher) -> bool:
    """dispatcher.query_quote 是否接受 per-call ``timeout``（按类型缓存）。

    只有 HTTP 桥实现支持；tdx/ths 的同步实现与测试替身不支持，退化为不带超时
    （这两类都是本地调用，不经过可能挂几十秒的网络）。
    """
    cls = type(dispatcher)
    hit = _TIMEOUT_SUPPORT.get(cls)
    if hit is None:
        try:
            sig = inspect.signature(dispatcher.query_quote)
            hit = ("timeout" in sig.parameters
                   or any(p.kind == p.VAR_KEYWORD for p in sig.parameters.values()))
        except (TypeError, ValueError):
            hit = False
        _TIMEOUT_SUPPORT[cls] = hit
    return hit


def _query_quote(dispatcher, code: str, period: str, timeout: float,
                 count: int = _CHECK_COUNT):
    """带硬超时的单只取价（不支持 per-call timeout 的实现原样调用）。"""
    if _supports_call_timeout(dispatcher):
        return dispatcher.query_quote(code, period=period,
                                      count=count, timeout=timeout)
    return dispatcher.query_quote(code, period=period, count=count)


def _is_timeout_error(exc: BaseException) -> bool:
    """判断是否为取价超时（桥侧把 httpx 超时包成 BridgeUnavailableError）。"""
    cause = getattr(exc, "__cause__", None)
    for err in (exc, cause):
        if err is None:
            continue
        if "timeout" in type(err).__name__.lower():
            return True
        text = str(err).lower()
        if "timed out" in text or "timeout" in text:
            return True
    return False


def check_data_health(dispatcher, codes, periods, *, now: datetime = None,
                      calendar=None, max_codes: Optional[int] = None,
                      time_budget_seconds: Optional[float] = None,
                      quote_timeout_seconds: Optional[float] = None,
                      session_id: Optional[int] = None) -> dict:
    """逐 code × period 读取最新 bar 并汇总健康度（只读）。

    ``max_codes`` / ``time_budget_seconds`` 用于启动与健康总览链路：体检只是告警，
    不允许串行遍历全池把调用方长时间卡住（掘金桥单只 /quote 实测 8–26s）。
    超出预算时提前收尾，并置 ``truncated=True`` 且 ``healthy=False``。

    ``session_id`` 只用于慢调用留痕：单只取价超过 ``_SLOW_QUOTE_SECONDS`` 时写
    WARNING（会话/标的/周期/耗时），并在结果 ``slow_quotes`` 里保留最近若干条。

    ``quote_timeout_seconds`` 是**单只取价硬超时**（默认 2s）：时间预算只在每只
    之前检查，在飞的那次调用拦不住；给桥请求本身加超时后，超时的标的记为
    ``timeout_codes`` / 未校验，单会话最坏耗时约为 2×预算。

    返回结构（可直接 JSON 序列化）：
    {version, checked_at, intraday, expected_dates, healthy, unavailable,
     truncated, requested_codes, checked_codes,
     timeout_count, timeout_codes, slow_quotes, slow_quote_count,
     periods: {p: {total, ok, stale, empty, error, stale_codes, empty_codes,
                   error_codes, stale_count, empty_count, error_count}},
     missing_codes: [...], missing_count}
    """
    now = now or now_shanghai()
    quote_timeout = (float(quote_timeout_seconds)
                     if quote_timeout_seconds is not None
                     else _QUOTE_HARD_TIMEOUT_SECONDS)
    if time_budget_seconds is not None and time_budget_seconds > 0:
        # 单只超时必须**严格低于**整体预算，否则「在飞的那只」仍会把调用方拖穿。
        quote_timeout = min(quote_timeout, max(
            0.2, float(time_budget_seconds) * _QUOTE_TIMEOUT_BUDGET_RATIO))
    requested_codes = [str(c) for c in (codes or []) if str(c)]
    codes = requested_codes
    periods = [str(p) for p in (periods or []) if str(p)]
    truncated = False
    if max_codes is not None and max_codes > 0 and len(codes) > max_codes:
        codes = codes[:max_codes]
        truncated = True
    deadline = None
    if time_budget_seconds is not None and time_budget_seconds > 0:
        deadline = _time.monotonic() + float(time_budget_seconds)
    intraday = is_intraday(now, calendar)
    expected = {p: expected_bar_date(p, now, calendar) for p in periods}
    per_period: Dict[str, dict] = {}
    all_missing: List[str] = []
    checked_codes: set = set()
    total_checks = 0
    total_errors = 0
    budget_exhausted = False
    slow_quotes: List[dict] = []
    timeout_codes: List[str] = []
    timeout_count = 0
    for period in periods:
        stats = {"total": 0, "ok": 0, "stale": 0, "empty": 0, "error": 0,
                 "timeout": 0,
                 "stale_codes": [], "empty_codes": [], "error_codes": []}
        for code in codes:
            if not code:
                continue
            if deadline is not None and _time.monotonic() >= deadline:
                budget_exhausted = True
                break
            stats["total"] += 1
            total_checks += 1
            call_timeout = quote_timeout
            if deadline is not None:
                call_timeout = min(quote_timeout,
                                   max(0.05, deadline - _time.monotonic()))
            quote_started = _time.monotonic()
            try:
                bars = _query_quote(dispatcher, code, period, call_timeout)
            except Exception as exc:  # noqa: BLE001 — 桥离线/通道异常都不阻断体检
                if _is_timeout_error(exc):
                    # 硬超时：放弃该只并计入未校验，**不**计入 error/missing，
                    # 避免把「来不及验」误报成「数据缺失」（补齐也没有意义）。
                    stats["timeout"] += 1
                    timeout_count += 1
                    if len(timeout_codes) < _MAX_CODES:
                        timeout_codes.append(code)
                    logger.warning(
                        "数据体检单只取价超时：会话 %s 标的 %s 周期 %s 硬超时 %.2fs，"
                        "本次放弃该只并计入未校验（不判为数据缺失）",
                        session_id, code, period, call_timeout)
                else:
                    stats["error"] += 1
                    total_errors += 1
                    if len(stats["error_codes"]) < _MAX_CODES:
                        stats["error_codes"].append(code)
                    logger.debug("数据体检取数失败 code=%s period=%s: %s",
                                 code, period, exc)
                continue
            finally:
                elapsed = _time.monotonic() - quote_started
                if elapsed >= _SLOW_QUOTE_SECONDS:
                    logger.warning(
                        "数据体检单只取价偏慢：会话 %s 标的 %s 周期 %s 耗时 %.2fs"
                        "（桥/通道排队或数据源慢，超出阈值 %.1fs）",
                        session_id, code, period, elapsed, _SLOW_QUOTE_SECONDS)
                    if len(slow_quotes) < _MAX_SLOW_QUOTES:
                        slow_quotes.append({
                            "code": code, "period": period,
                            "elapsed_seconds": round(elapsed, 2),
                        })
            checked_codes.add(code)
            verdict = classify_bar_freshness(period, bars, expected[period])
            stats[verdict["status"]] += 1
            if verdict["status"] != "ok" and len(stats["%s_codes" % verdict["status"]]) < _MAX_CODES:
                stats["%s_codes" % verdict["status"]].append(code)
        stats["stale_count"] = stats["stale"]
        stats["empty_count"] = stats["empty"]
        stats["error_count"] = stats["error"]
        stats["timeout_count"] = stats["timeout"]
        per_period[period] = stats
        for key in ("stale_codes", "empty_codes", "error_codes"):
            for code in stats[key]:
                if code not in all_missing:
                    all_missing.append(code)
        if budget_exhausted:
            break
    truncated = truncated or budget_exhausted
    healthy = (not truncated) and all(
        stats["stale"] == 0 and stats["empty"] == 0 and stats["error"] == 0
        and stats["timeout"] == 0
        for stats in per_period.values()
    )
    return {
        "version": DATA_HEALTH_VERSION,
        "checked_at": now.isoformat(),
        "intraday": intraday,
        "expected_dates": {p: d.isoformat() for p, d in expected.items()},
        "healthy": healthy,
        "unavailable": bool(total_checks) and total_errors == total_checks,
        "truncated": truncated,
        "requested_codes": len(requested_codes),
        "checked_codes": len(checked_codes),
        "timeout_count": timeout_count,
        "timeout_codes": timeout_codes,
        "slow_quotes": slow_quotes,
        "slow_quote_count": len(slow_quotes),
        "periods": per_period,
        "missing_codes": all_missing[:_MAX_CODES],
        "missing_count": len(all_missing),
    }


def auto_fill_missing(dispatcher, health: dict, codes, periods, *,
                      now: datetime = None, calendar=None,
                      max_pairs: Optional[int] = None,
                      time_budget_seconds: Optional[float] = None,
                      quote_timeout_seconds: Optional[float] = None,
                      session_id: Optional[int] = None) -> dict:
    """非盘中自动补齐：对缺失 (code, period) 用更大 count 重读一次。

    盘中禁止自动补齐（只报告）；非交易日 / 盘前盘后允许。重读只唤醒桥端/TQ
    已落盘数据，不触发通达信下载（平台无该能力）。
    ``max_pairs`` / ``time_budget_seconds`` 用于启动链路限流，超预算即收尾。
    返回 {triggered, reason, requested, warmed, failed, truncated?, after?}；
    ``after`` 是补齐后的新体检结果（仅在 triggered 且未超预算时附带）。
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
    requested_pairs = len(pairs)
    truncated = False
    if max_pairs is not None and max_pairs > 0 and len(pairs) > max_pairs:
        pairs = pairs[:max_pairs]
        truncated = True
    deadline = None
    if time_budget_seconds is not None and time_budget_seconds > 0:
        deadline = _time.monotonic() + float(time_budget_seconds)
    warmed = 0
    failed = 0
    call_timeout = (float(quote_timeout_seconds)
                    if quote_timeout_seconds is not None
                    else _QUOTE_HARD_TIMEOUT_SECONDS)
    for code, period in pairs:
        if deadline is not None and _time.monotonic() >= deadline:
            truncated = True
            break
        this_timeout = call_timeout
        if deadline is not None:
            this_timeout = min(call_timeout,
                               max(0.05, deadline - _time.monotonic()))
        quote_started = _time.monotonic()
        try:
            bars = _query_quote(dispatcher, code, period, this_timeout,
                                count=_WARM_COUNT)
            if bars:
                warmed += 1
            else:
                failed += 1
        except Exception:  # noqa: BLE001 — 补齐失败只计数，不阻断
            failed += 1
            logger.debug("数据补齐失败 code=%s period=%s", code, period, exc_info=True)
        finally:
            elapsed = _time.monotonic() - quote_started
            if elapsed >= _SLOW_QUOTE_SECONDS:
                logger.warning(
                    "数据补齐单只取价偏慢：会话 %s 标的 %s 周期 %s 耗时 %.2fs",
                    session_id, code, period, elapsed)
    if truncated:
        return {"triggered": True, "reason": "budget_exhausted",
                "requested": requested_pairs, "warmed": warmed, "failed": failed,
                "truncated": True}
    after = check_data_health(dispatcher, codes, periods, now=now, calendar=calendar,
                              quote_timeout_seconds=quote_timeout_seconds,
                              session_id=session_id)
    return {"triggered": True, "reason": "off_hours", "requested": requested_pairs,
            "warmed": warmed, "failed": failed, "after": after}


def run_health_check(dispatcher, codes, periods, *, auto_fill: bool = True,
                     now: datetime = None, calendar=None,
                     max_codes: Optional[int] = None,
                     time_budget_seconds: Optional[float] = None,
                     quote_timeout_seconds: Optional[float] = None,
                     session_id: Optional[int] = None) -> dict:
    """体检 + 可选非盘中补齐；返回 {health, auto_fill}（health 为补齐后结果）。

    启动链路与健康总览都应传 ``max_codes`` / ``time_budget_seconds``，保证体检
    不会把调用方卡在同步取数上；超预算时跳过补齐，直接返回部分结果。
    ``quote_timeout_seconds`` 透传给单只取价硬超时（不传则用默认 2s）。
    ``session_id`` 仅用于慢调用留痕。
    """
    now = now or now_shanghai()
    health = check_data_health(dispatcher, codes, periods, now=now, calendar=calendar,
                               max_codes=max_codes,
                               time_budget_seconds=time_budget_seconds,
                               quote_timeout_seconds=quote_timeout_seconds,
                               session_id=session_id)
    result = {"health": health, "auto_fill": None}
    if health.get("truncated"):
        result["auto_fill"] = {"triggered": False, "reason": "budget_exhausted",
                               "requested": 0, "warmed": 0, "failed": 0}
        return result
    if auto_fill and not health.get("healthy"):
        fill = auto_fill_missing(dispatcher, health, codes, periods,
                                 now=now, calendar=calendar,
                                 max_pairs=max_codes,
                                 time_budget_seconds=time_budget_seconds,
                                 quote_timeout_seconds=quote_timeout_seconds,
                                 session_id=session_id)
        after = fill.pop("after", None)
        result["auto_fill"] = fill
        if after is not None:
            result["health"] = after
    return result


def health_summary_text(health: dict) -> str:
    """体检结论 → 一句话（启动告警/通知复用）。"""
    if health.get("unavailable"):
        return "数据体检失败：行情通道不可用，无法读取本地数据"
    if health.get("truncated"):
        return "数据体检未完成：仅抽查 %s/%s 只（已到启动等待预算，未阻塞启动）；未抽查部分未校验" % (
            health.get("checked_codes", 0), health.get("requested_codes", 0),
        )
    parts: List[str] = []
    for period, stats in (health.get("periods") or {}).items():
        bad = int(stats.get("stale", 0)) + int(stats.get("empty", 0))
        if bad:
            parts.append("%s 缺失/陈旧 %d 只" % (period, bad))
    timeout_count = int(health.get("timeout_count", 0) or 0)
    if not parts:
        if timeout_count:
            return "数据体检部分未校验：%d 只取价超时（不影响启动）" % timeout_count
        return "数据体检通过"
    shown = "、".join((health.get("missing_codes") or [])[:10])
    text = "数据体检异常：%s（%s%s）；请确认通达信已下载对应周期数据" % (
        "，".join(parts), shown,
        " 等" if int(health.get("missing_count", 0)) > 10 else "",
    )
    if timeout_count:
        text += "；另有 %d 只取价超时未校验" % timeout_count
    return text
