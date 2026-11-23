"""周期-执行语义（唯一判定入口）。

规范见 `2026-11-23/period-execution-spec/outputs/周期-执行语义规范.md`。

三条不变式：
1. 信号在策略自身周期 bar 上判定（周期末根必须是完整 bar）；
2. 执行在下一根同级别 bar 开盘；
3. 回测与实盘同一套定义。

信号判定维度只有三个互斥取值（不要新增第四套开关）：
- ``period_close``     周期末根收盘后判定（默认；可下单）
- ``dynamic``          周期内逐日滚动判定（仅 1w/1mon；仅预警，不下单）
- ``pre_close_1450``   14:50 实时价代理日线收盘（仅 1d；当日尾盘可下单）

``pre_close_1450`` 仍由会话级 ``advance_close_enabled`` 表达（未显式设置=关闭、
显式 true/false 永远优先），本模块只做取值归一与互斥校验。
"""
from __future__ import annotations

import logging
from datetime import date
from typing import Optional

logger = logging.getLogger(__name__)

PERIOD_CLOSE = "period_close"
DYNAMIC = "dynamic"
PRE_CLOSE_1450 = "pre_close_1450"

SIGNAL_MODES = (PERIOD_CLOSE, DYNAMIC, PRE_CLOSE_1450)
#: 动态 K 线只对周线/月线开放。
DYNAMIC_PERIODS = ("1w", "1mon")
#: 14:50 提前收盘价只对日线开放。
PRE_CLOSE_PERIODS = ("1d",)
#: 分钟周期：每根已闭合 bar 自身就是周期末根。
MINUTE_PERIODS = ("1m", "5m", "15m", "30m", "1h")


def normalize_mode(raw) -> str:
    """配置值归一：只接受 ``period_close`` / ``dynamic``。

    ``pre_close_1450`` 不允许通过全局配置生效（否则会把「未显式设置=关闭」
    变成隐式开启）；出现时视为非法，回退 ``period_close`` 并告警。
    """
    text = str(raw or "").strip().lower()
    if text == DYNAMIC:
        return DYNAMIC
    if text == PRE_CLOSE_1450:
        logger.warning(
            "live.period_signal_mode=%s 不允许作为全局默认（14:50 仅能由会话级 "
            "advance_close_enabled 显式开启），已回退 %s", raw, PERIOD_CLOSE)
        return PERIOD_CLOSE
    return PERIOD_CLOSE


def period_signal_mode(cfg: Optional[dict] = None) -> str:
    """读取 ``live.period_signal_mode``（缺键/异常/非法 → ``period_close``）。"""
    if cfg is None:
        try:
            from core.config import load_config
            cfg = load_config() or {}
        except Exception:  # noqa: BLE001 — 配置不可读时按最安全默认
            cfg = {}
    live_cfg = (cfg.get("live") or {}) if isinstance(cfg, dict) else {}
    return normalize_mode(live_cfg.get("period_signal_mode"))


def raw_configured_mode(cfg: Optional[dict] = None) -> str:
    """读取 ``live.period_signal_mode`` 的原始取值（不做归一，供严格校验用）。

    配置不可读时返回空串（按默认 ``period_close`` 处理）。
    """
    if cfg is None:
        try:
            from core.config import load_config
            cfg = load_config() or {}
        except Exception:  # noqa: BLE001 — 配置不可读时按最安全默认
            cfg = {}
    live_cfg = (cfg.get("live") or {}) if isinstance(cfg, dict) else {}
    return str(live_cfg.get("period_signal_mode") or "").strip().lower()


def validate_configured_mode(raw) -> Optional[str]:
    """严格校验全局 ``live.period_signal_mode``；合法返回 None，否则错误文案。

    与 ``normalize_mode`` 的「静默回退」不同：这里用于保存/启动入口，非法取值
    必须让调用方拒绝并把原因暴露给用户，而不是悄悄按 ``period_close`` 跑。
    """
    text = str(raw or "").strip().lower()
    if text in ("", PERIOD_CLOSE, DYNAMIC):
        return None
    if text == PRE_CLOSE_1450:
        return ("period_signal_mode 不允许取 pre_close_1450"
                "（14:50 只能由会话级 advance_close_enabled 显式开启），"
                "收到 %s" % raw)
    return ("未知的 period_signal_mode 取值：%s（可选 %s / %s / 留空）"
            % (raw, PERIOD_CLOSE, DYNAMIC))


def validate_session_mode(raw, advance_close_enabled: bool) -> Optional[str]:
    """会话启动时的组合校验：非法取值、或 dynamic 与 14:50 并用 → 错误文案。"""
    msg = validate_configured_mode(raw)
    if msg:
        return msg
    if normalize_mode(raw) == DYNAMIC and advance_close_enabled:
        return ("period_signal_mode=dynamic 与 advance_close_enabled=true 互斥："
                "动态 K 线只做预警不下单，不能与 14:50 提前收盘价（当日下单）同时开启")
    return None


def resolve_strategy_mode(period: str, configured: str,
                          advance_close_enabled: bool = False) -> str:
    """单策略的生效取值（互斥）。

    - ``1d`` + 会话 14:50 开关 → ``pre_close_1450``；
    - ``1w``/``1mon`` + 全局 ``dynamic`` → ``dynamic``；
    - 其余 → ``period_close``。
    """
    if period in PRE_CLOSE_PERIODS and advance_close_enabled:
        return PRE_CLOSE_1450
    if period in DYNAMIC_PERIODS and normalize_mode(configured) == DYNAMIC:
        return DYNAMIC
    return PERIOD_CLOSE


def is_dynamic(period: str, configured: str) -> bool:
    """该周期是否处于动态 K 线模式（仅 1w/1mon）。"""
    return period in DYNAMIC_PERIODS and normalize_mode(configured) == DYNAMIC


def is_period_end_day(calendar, period: str, day: date) -> bool:
    """周期末交易日判定（交易日历版，回测经周期 bar 的 Date 天然满足）。

    - ``1d``   ：每个交易日；
    - ``1w``   ：本周最后一个交易日（周五休市前移、整周无交易日=False）；
    - ``1mon`` ：本月最后一个交易日；
    - 分钟周期 ：每根已闭合 bar 即周期末根，恒 True。
    """
    if period == "1d":
        return bool(calendar.is_trading_day(day))
    if period == "1w":
        return bool(calendar.is_last_trading_day_of_week(day))
    if period == "1mon":
        return bool(calendar.is_last_trading_day_of_month(day))
    if period in MINUTE_PERIODS:
        return True
    return True


def validate_mode(period: str, mode: str) -> Optional[str]:
    """互斥/适用性校验；合法返回 None，否则返回错误文案。"""
    if mode not in SIGNAL_MODES:
        return "未知信号判定模式：%s" % mode
    if mode == DYNAMIC and period not in DYNAMIC_PERIODS:
        return "dynamic 仅适用于 1w/1mon（当前 period=%s）" % period
    if mode == PRE_CLOSE_1450 and period not in PRE_CLOSE_PERIODS:
        return "pre_close_1450 仅适用于 1d（当前 period=%s）" % period
    return None
