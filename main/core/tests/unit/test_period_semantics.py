"""周期-执行语义（规范 1/2/3）单测：三取值互斥 + 周期末交易日判定。

只用纯函数，不连 DB / 不碰实盘。
"""
from datetime import date
from decimal import Decimal

from core.engine import period_semantics as ps
from core.engine.strategy_context import StrategyContext


class _FakeCalendar:
    """只实现 is_period_end_day 用到的三个查询。"""

    def __init__(self, trading_days, last_week_days, last_month_days):
        self.trading_days = set(trading_days)
        self.last_week_days = set(last_week_days)
        self.last_month_days = set(last_month_days)

    def is_trading_day(self, day):
        return day in self.trading_days

    def is_last_trading_day_of_week(self, day):
        return day in self.last_week_days

    def is_last_trading_day_of_month(self, day):
        return day in self.last_month_days


def test_normalize_mode_rejects_global_pre_close():
    # pre_close_1450 不能作为全局默认，必须回退 period_close
    assert ps.normalize_mode("pre_close_1450") == ps.PERIOD_CLOSE
    assert ps.normalize_mode("DYNAMIC") == ps.DYNAMIC
    assert ps.normalize_mode(None) == ps.PERIOD_CLOSE
    assert ps.normalize_mode("garbage") == ps.PERIOD_CLOSE


def test_resolve_strategy_mode_mutual_exclusion():
    # 1d + 会话 14:50 → pre_close_1450（与 dynamic 不可叠加）
    assert ps.resolve_strategy_mode("1d", ps.DYNAMIC, True) == ps.PRE_CLOSE_1450
    # 1d 无 14:50 → 即便全局 dynamic 也只回落 period_close
    assert ps.resolve_strategy_mode("1d", ps.DYNAMIC, False) == ps.PERIOD_CLOSE
    # 1w/1mon + dynamic → dynamic
    assert ps.resolve_strategy_mode("1w", ps.DYNAMIC, True) == ps.DYNAMIC
    assert ps.resolve_strategy_mode("1mon", ps.DYNAMIC, False) == ps.DYNAMIC
    # 默认
    assert ps.resolve_strategy_mode("1w", ps.PERIOD_CLOSE) == ps.PERIOD_CLOSE


def test_validate_mode_applicability():
    assert ps.validate_mode("1w", ps.DYNAMIC) is None
    assert ps.validate_mode("1d", ps.DYNAMIC) is not None
    assert ps.validate_mode("1d", ps.PRE_CLOSE_1450) is None
    assert ps.validate_mode("1w", ps.PRE_CLOSE_1450) is not None
    assert ps.validate_mode("1d", "bad") is not None


def test_validate_configured_mode_strict():
    # 空/period_close/dynamic 合法
    assert ps.validate_configured_mode("") is None
    assert ps.validate_configured_mode(None) is None
    assert ps.validate_configured_mode(ps.PERIOD_CLOSE) is None
    assert ps.validate_configured_mode(ps.DYNAMIC) is None
    # pre_close_1450 与未知取值显式报错（不再静默回退）
    assert ps.validate_configured_mode(ps.PRE_CLOSE_1450) is not None
    assert ps.validate_configured_mode("dynamik") is not None


def test_validate_session_mode_dynamic_with_advance_close_rejected():
    # dynamic + 14:50 提前收盘价互斥 → 可见错误
    assert ps.validate_session_mode(ps.DYNAMIC, True) is not None
    # dynamic 无 14:50 合法
    assert ps.validate_session_mode(ps.DYNAMIC, False) is None
    # period_close / 空配 + 14:50 合法
    assert ps.validate_session_mode(ps.PERIOD_CLOSE, True) is None
    assert ps.validate_session_mode("", True) is None
    # 非法取值在会话启动也拒绝
    assert ps.validate_session_mode("garbage", False) is not None



def test_is_period_end_day_daily():
    cal = _FakeCalendar([date(2026, 11, 23)], [], [])
    assert ps.is_period_end_day(cal, "1d", date(2026, 11, 23)) is True
    assert ps.is_period_end_day(cal, "1d", date(2026, 11, 22)) is False


def test_is_period_end_day_week_friday_holiday_and_empty_week():
    # 2026-11-20 周五为本周末交易日；11-19 周四休市（也不在交易日集合）
    cal = _FakeCalendar(
        [date(2026, 11, 16), date(2026, 11, 17), date(2026, 11, 18),
         date(2026, 11, 20)],
        [date(2026, 11, 20)],
        [],
    )
    assert ps.is_period_end_day(cal, "1w", date(2026, 11, 20)) is True
    assert ps.is_period_end_day(cal, "1w", date(2026, 11, 19)) is False
    assert ps.is_period_end_day(cal, "1w", date(2026, 11, 18)) is False
    # 整周无交易日（国庆/春节整周休市）：本周任何一天都不是周期末
    empty = _FakeCalendar([], [], [])
    for d in (date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5)):
        assert ps.is_period_end_day(empty, "1w", d) is False


def test_is_period_end_day_week_friday_moved_to_thursday():
    # 周五休市 → 周四成为本周最后一个交易日
    cal = _FakeCalendar(
        [date(2026, 11, 16), date(2026, 11, 17), date(2026, 11, 18),
         date(2026, 11, 19)],
        [date(2026, 11, 19)],
        [],
    )
    assert ps.is_period_end_day(cal, "1w", date(2026, 11, 19)) is True
    assert ps.is_period_end_day(cal, "1w", date(2026, 11, 20)) is False


def test_is_period_end_day_month_and_year_end():
    cal = _FakeCalendar(
        [date(2026, 12, 30), date(2026, 12, 31)],
        [],
        [date(2026, 12, 31)],
    )
    assert ps.is_period_end_day(cal, "1mon", date(2026, 12, 31)) is True
    assert ps.is_period_end_day(cal, "1mon", date(2026, 12, 30)) is False
    # 跨年：次年 1 月首个交易日前，12-31 是本年末
    cal2 = _FakeCalendar(
        [date(2027, 1, 4)],
        [],
        [date(2027, 1, 4)],
    )
    assert ps.is_period_end_day(cal2, "1mon", date(2026, 12, 31)) is False
    assert ps.is_period_end_day(cal2, "1mon", date(2027, 1, 4)) is True


def test_is_period_end_day_minute_periods_always_true():
    cal = _FakeCalendar([], [], [])
    for p in ps.MINUTE_PERIODS:
        assert ps.is_period_end_day(cal, p, date(2026, 11, 23)) is True


def test_strategy_context_reversal_disposition_defaults_to_close_next_open():
    ctx = StrategyContext(
        strategy_id=1, period="1d", capital_ratio=Decimal("0.2"),
        max_positions=5,
    )
    assert ctx.reversal_disposition == "close_next_open"

    ctx2 = StrategyContext(
        strategy_id=2, period="1d", capital_ratio=Decimal("0.2"),
        max_positions=5, reversal_disposition="hold",
    )
    assert ctx2.reversal_disposition == "hold"
