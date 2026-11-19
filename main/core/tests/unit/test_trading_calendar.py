"""交易日历与交易总开关单元测试。

权威交易日历来自桥 xtdata.get_trading_dates（/calendar 端点）。设计原则：
- 周末（周六/周日）永远非交易日（A 股硬规则，不依赖数据）。
- 工作日是否交易日由桥返回的权威集合判定（节假日）。
- 桥离线/无数据时 fail-open：工作日默认交易日（绝不因日历过期误挡真实交易日），
  仅记录一次告警。资金安全靠 is_trading_allowed 的时段门 + 15:00 收盘守卫兜底。
"""
from datetime import date, datetime

import pytest

from core.engine.trading_calendar import TradingCalendar
from core.engine.live.calendar import BUILTIN_CLOSURES, BUILTIN_HALF_DAYS


# 2026 年部分交易日（真实日历，用于断言；桥 provider 一般返回整年）
_2026_TRADING = [
    date(2026, 1, 5),    # 周一
    date(2026, 1, 6),    # 周二
    date(2026, 10, 9),    # 国庆后
    date(2026, 12, 31),
]


def _provider(dates):
    """构造一个返回指定交易日列表的 provider。"""
    def _p():
        return list(dates)
    return _p


def test_weekend_is_never_trading_day():
    cal = TradingCalendar(_provider(_2026_TRADING))
    # 2026-08-22 周六、08-23 周日
    assert cal.is_trading_day(date(2026, 8, 22)) is False
    assert cal.is_trading_day(date(2026, 8, 23)) is False


def test_weekday_in_calendar_is_trading_day():
    cal = TradingCalendar(_provider([date(2026, 8, 24)]))  # 周一
    assert cal.is_trading_day(date(2026, 8, 24)) is True


def test_provider_datetime_items_coerced_to_date():
    """provider 返回 datetime（date 子类）时须归一成 date，否则集合查找永不命中。"""
    cal = TradingCalendar(_provider([datetime(2026, 8, 24, 0, 0), date(2026, 8, 26)]))
    assert cal.is_trading_day(date(2026, 8, 24)) is True
    # 08-25 在数据源覆盖范围内（08-24..08-26）却不在集合 → 非交易日
    assert cal.is_trading_day(date(2026, 8, 25)) is False


def test_weekday_holiday_not_in_calendar_is_non_trading():
    # 工作日但不在交易日集合（如国庆假期内的调休工作日）→ 非交易日
    cal = TradingCalendar(_provider([date(2026, 10, 9)]))
    assert cal.is_trading_day(date(2026, 10, 1)) is False  # 国庆


def test_provider_failure_fail_open_on_weekday(caplog):
    """桥离线：工作日默认交易日（fail-open/内置表），并记录一次告警（不静默）。"""
    caplog.set_level("WARNING", logger="core.engine.live.calendar")
    def boom():
        raise RuntimeError("bridge offline")
    cal = TradingCalendar(boom)
    # 周一，无数据 → 放行（不误挡）
    assert cal.is_trading_day(date(2026, 8, 24)) is True
    # 周末仍硬判非交易日（不依赖 provider）
    assert cal.is_trading_day(date(2026, 8, 22)) is False
    # 告警可见：异常原文 + 退化来源进日志
    assert any("bridge offline" in rec.message for rec in caplog.records)
    assert any("交易日历" in rec.message for rec in caplog.records)
    assert cal.status()["degraded"] is True


def test_provider_returns_empty_fail_open_on_weekday():
    """provider 返回空（桥 404/老桥无 /calendar）：工作日 fail-open。"""
    cal = TradingCalendar(_provider([]))
    assert cal.is_trading_day(date(2026, 8, 24)) is True
    assert cal.is_trading_day(date(2026, 8, 22)) is False


def test_last_trading_day_of_week_and_month():
    """周期末识别：周线认本周最后一个交易日、月线认本月最后一个交易日。"""
    # 覆盖 08-24..08-31；集合内 08-28（周五）休市、08-31（周一）为 8 月最后交易日
    cal = TradingCalendar(_provider([
        date(2026, 8, 24), date(2026, 8, 26),
        date(2026, 8, 27), date(2026, 8, 31),
    ]))
    # 周五休市 → 08-27（周四）成为本周最后一个交易日
    assert cal.is_last_trading_day_of_week(date(2026, 8, 27)) is True
    assert cal.is_last_trading_day_of_week(date(2026, 8, 26)) is False
    # 月末：08-31 是 8 月最后一个交易日；08-27 不是
    assert cal.is_last_trading_day_of_month(date(2026, 8, 31)) is True
    assert cal.is_last_trading_day_of_month(date(2026, 8, 27)) is False
    # 非交易日直接返回 False（不会把周末当周期末）
    assert cal.is_last_trading_day_of_week(date(2026, 8, 29)) is False


def test_calendar_caches_per_year_single_provider_call():
    """同一自然年只拉一次日历（缓存）。"""
    calls = {"n": 0}
    def counting_provider():
        calls["n"] += 1
        return [date(2026, 8, 24), date(2026, 8, 25)]
    cal = TradingCalendar(counting_provider)
    cal.is_trading_day(date(2026, 8, 24))
    cal.is_trading_day(date(2026, 8, 25))
    cal.is_trading_day(date(2026, 8, 24))
    assert calls["n"] == 1


# ---------------- is_trading_allowed 总开关 ----------------

def test_trading_allowed_during_session():
    cal = TradingCalendar(_provider([date(2026, 8, 24)]))
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 9, 30)) is True
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 10, 0)) is True
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 14, 59)) is True


def test_trading_blocked_before_open():
    cal = TradingCalendar(_provider([date(2026, 8, 24)]))
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 9, 0)) is False
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 9, 29)) is False


def test_trading_blocked_at_and_after_close():
    cal = TradingCalendar(_provider([date(2026, 8, 24)]))
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 15, 0)) is False
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 15, 30)) is False


def test_trading_blocked_during_lunch_break():
    """P2-5：11:30–13:00 午休禁报（11:30 边界 bar 的单不再排进 TDX/被 QMT 拒）。"""
    cal = TradingCalendar(_provider([date(2026, 8, 24)]))
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 11, 29)) is True
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 11, 30)) is False
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 12, 0)) is False
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 12, 59)) is False
    # 13:00 起恢复正常派发
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 13, 0)) is True
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 14, 0)) is True


def test_is_lunch_break_left_closed_right_open():
    """午休判定左闭右开（11:30 起、13:00 止），与 is_trading_allowed 同一组常量。"""
    cal = TradingCalendar(_provider([date(2026, 8, 24)]))
    assert cal.is_lunch_break(datetime(2026, 8, 24, 11, 29, 59)) is False
    assert cal.is_lunch_break(datetime(2026, 8, 24, 11, 30)) is True
    assert cal.is_lunch_break(datetime(2026, 8, 24, 12, 0)) is True
    assert cal.is_lunch_break(datetime(2026, 8, 24, 12, 59, 59)) is True
    assert cal.is_lunch_break(datetime(2026, 8, 24, 13, 0)) is False
    # 与下单总闸口径互为镜像：午休内禁报，11:29 与 13:00 允许
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 11, 29, 59)) is True
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 11, 30)) is False
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 13, 0)) is True


def test_lunch_end_at_is_same_day_1300():
    """暂停状态回报的「下次恢复」= 当日 13:00:00。"""
    cal = TradingCalendar(_provider([date(2026, 8, 24)]))
    assert cal.lunch_end_at(datetime(2026, 8, 24, 11, 45, 12, 500000)) == \
        datetime(2026, 8, 24, 13, 0)


def test_trading_blocked_on_weekend_even_at_session_time():
    # 周六 10:00 —— 时段对但非交易日，禁止
    cal = TradingCalendar(_provider([]))
    assert cal.is_trading_allowed(datetime(2026, 8, 22, 10, 0)) is False


def test_trading_blocked_on_holiday_at_session_time():
    cal = TradingCalendar(_provider([date(2026, 10, 9)]))
    assert cal.is_trading_allowed(datetime(2026, 10, 1, 10, 0)) is False  # 国庆


def test_trading_allowed_fail_open_weekday_when_provider_down():
    """桥离线时工作日交易时段内仍放行（fail-open）——不误挡真实交易日。"""
    def boom():
        raise RuntimeError("offline")
    cal = TradingCalendar(boom)
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 10, 0)) is True


# ---------------- 2026-11-05 修复：日历格式 / 多源回退 / 退化可见 ----------------

def test_iso_dashed_date_strings_are_parsed():
    """掘金桥 /calendar 返回 'YYYY-MM-DD'：必须解析（原实现只认 'YYYYMMDD' → 全丢）。"""
    cal = TradingCalendar(_provider(["2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05"]))
    assert cal.is_trading_day(date(2026, 11, 2)) is True
    assert cal.is_trading_day(date(2026, 10, 1)) is False  # 国庆（内置表拦下）
    assert cal.status()["degraded"] is False
    assert cal.status()["years"]["2026"]["source"] == "primary"
    assert cal.status()["years"]["2026"]["trading_days"] == 4


def test_other_date_formats_are_parsed():
    cal = TradingCalendar(_provider(["2026/01/05", "2026-01-06 00:00:00", "20260107"]))
    assert cal.is_trading_day(date(2026, 1, 5)) is True
    assert cal.is_trading_day(date(2026, 1, 6)) is True
    assert cal.is_trading_day(date(2026, 1, 7)) is True


def test_primary_empty_falls_back_to_secondary():
    cal = TradingCalendar(
        _provider([]),
        fallback_providers=[("goldminer_bridge", _provider(["2026-11-02"]))],
    )
    assert cal.is_trading_day(date(2026, 11, 2)) is True
    assert cal.status()["degraded"] is False
    assert cal.status()["years"]["2026"]["source"] == "goldminer_bridge"


def test_year_aware_fallback_provider_receives_year():
    seen = []

    def gm(year):
        seen.append(year)
        return ["2026-01-05"]

    cal = TradingCalendar(_provider([]), fallback_providers=[("goldminer_bridge", gm)])
    cal.is_trading_day(date(2026, 1, 5))
    assert seen == [2026]


def test_all_sources_empty_use_builtin_table_and_warn_once(caplog):
    """全部源为空 → 内置官方休市表 + 一条 WARNING + 一条 risk 事件（不刷屏）。"""
    caplog.set_level("WARNING", logger="core.engine.live.calendar")
    events = []
    cal = TradingCalendar(_provider([]), fallback_providers=[("goldminer_bridge", _provider([]))])
    cal.on_degraded = events.append

    assert cal.is_trading_day(date(2026, 10, 1)) is False  # 国庆（内置表休市）
    assert cal.is_trading_day(date(2026, 11, 6)) is True   # 普通工作日放行
    assert cal.status()["degraded"] is True
    assert cal.status()["years"]["2026"]["source"] == "builtin"
    assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1
    assert len(events) == 1
    assert events[0]["rule"] == "trading_calendar_degraded"

    # 同年重复判定不再新增告警/事件
    cal.is_trading_day(date(2026, 12, 1))
    assert len([r for r in caplog.records if r.levelname == "WARNING"]) == 1
    assert len(events) == 1


def test_uncovered_year_fail_open_with_warning(caplog):
    """内置表未收录年份（无权威数据）→ fail-open（工作日放行）+ WARNING。"""
    caplog.set_level("WARNING", logger="core.engine.live.calendar")
    cal = TradingCalendar(_provider([]))
    assert cal.is_trading_day(date(2030, 1, 7)) is True  # 周一
    assert cal.status()["years"]["2030"]["source"] == "fail_open"
    assert any("fail-open" in r.message for r in caplog.records)


def test_date_beyond_provider_coverage_not_misjudged_as_closed():
    """桥日历只给到"今天"：覆盖范围外的工作日不得因"不在集合"被判休市。"""
    cal = TradingCalendar(
        _provider(["2026-11-02", "2026-11-03", "2026-11-04", "2026-11-05"]))
    assert cal.is_trading_day(date(2026, 11, 6)) is True   # 覆盖外普通工作日
    assert cal.is_trading_day(date(2026, 12, 31)) is True
    assert cal.is_trading_day(date(2026, 10, 1)) is False  # 覆盖外但内置表是节假日


def test_empty_result_not_cached_forever():
    """退化结果不被永久缓存：退避窗后重试，源恢复即自动升级回权威集合。"""
    now = [1000.0]
    calls = {"n": 0}
    state = {"ok": False}

    def prov():
        calls["n"] += 1
        return ["2026-08-24"] if state["ok"] else []

    cal = TradingCalendar(prov, retry_after_seconds=60.0, clock=lambda: now[0])
    assert cal.is_trading_day(date(2026, 8, 24)) is True
    assert cal.status()["degraded"] is True

    cal.is_trading_day(date(2026, 8, 25))  # 退避窗内 → 不再打源
    assert calls["n"] == 1

    state["ok"] = True
    now[0] += 61.0
    assert cal.is_trading_day(date(2026, 8, 24)) is True
    assert calls["n"] == 2
    assert cal.status()["degraded"] is False
    assert cal.status()["years"]["2026"]["source"] == "primary"


def test_builtin_2026_matches_official_schedule():
    """内置表与上交所 2026 年休市安排一致（抽查节假日与复市日）。"""
    closed = BUILTIN_CLOSURES[2026]
    for d in (date(2026, 1, 1), date(2026, 2, 17), date(2026, 4, 6),
              date(2026, 5, 5), date(2026, 6, 19), date(2026, 9, 25),
              date(2026, 10, 7)):
        assert d in closed
    for d in (date(2026, 1, 5), date(2026, 2, 24), date(2026, 4, 7),
              date(2026, 5, 6), date(2026, 6, 22), date(2026, 9, 28),
              date(2026, 10, 8)):
        assert d not in closed


# ---------------- 半日市（P2-10）----------------

_HALF_DAY = date(2026, 12, 31)  # 周四（合成半日市，仅用于验证机制）


def _half_day_cal():
    return TradingCalendar(
        _provider([date(2026, 12, 30), _HALF_DAY]),
        half_day_provider=lambda year: ["2026-12-31"],
    )


def test_half_day_morning_allowed_afternoon_blocked():
    """半日市：09:30 可交易，11:30 起（含原下午时段）不可交易。"""
    cal = _half_day_cal()
    assert cal.is_half_day(_HALF_DAY) is True
    assert cal.is_trading_allowed(datetime(2026, 12, 31, 9, 30)) is True
    assert cal.is_trading_allowed(datetime(2026, 12, 31, 10, 45)) is True
    assert cal.is_trading_allowed(datetime(2026, 12, 31, 11, 29)) is True
    # 11:30 边界（左闭右开）与整个下午一律禁报
    assert cal.is_trading_allowed(datetime(2026, 12, 31, 11, 30)) is False
    assert cal.is_trading_allowed(datetime(2026, 12, 31, 13, 30)) is False
    assert cal.is_trading_allowed(datetime(2026, 12, 31, 14, 59)) is False
    assert cal.session_close_time(_HALF_DAY).isoformat() == "11:30:00"
    assert cal.status()["half_days"]["2026"] == 1


def test_normal_day_unaffected_by_half_day_source():
    """同一日历下，非半日市日期的时段判定保持原样（09:30–15:00 含午休）。"""
    cal = _half_day_cal()
    normal = date(2026, 12, 30)
    assert cal.is_half_day(normal) is False
    assert cal.is_trading_allowed(datetime(2026, 12, 30, 9, 30)) is True
    assert cal.is_trading_allowed(datetime(2026, 12, 30, 13, 30)) is True
    assert cal.is_trading_allowed(datetime(2026, 12, 30, 14, 59)) is True
    assert cal.is_trading_allowed(datetime(2026, 12, 30, 11, 45)) is False  # 午休
    assert cal.session_close_time(normal).isoformat() == "15:00:00"


def test_half_day_source_unavailable_fail_open_warn_once(caplog):
    """半日市数据不可用：fail-open（按全天交易）+ 每年一条 WARNING/risk 事件。"""
    caplog.set_level("WARNING", logger="core.engine.live.calendar")
    events = []
    cal = TradingCalendar(_provider([date(2030, 1, 7)]))  # 2030 无内置半日市表
    cal.on_degraded = events.append

    assert cal.is_half_day(date(2030, 1, 7)) is False
    # 不因半日市数据缺失拒绝交易
    assert cal.is_trading_allowed(datetime(2030, 1, 7, 13, 30)) is True
    half_warns = [r for r in caplog.records
                  if r.levelname == "WARNING" and "半日市" in r.message]
    assert len(half_warns) == 1
    assert [e["rule"] for e in events] == ["half_day_calendar_degraded"]

    # 同年重复判定不再新增告警（退避窗内不重复打源）
    cal.is_trading_allowed(datetime(2030, 1, 8, 13, 30))
    assert len([r for r in caplog.records
                if r.levelname == "WARNING" and "半日市" in r.message]) == 1
    assert len(events) == 1


def test_builtin_half_day_table_covers_2026_2027_without_warning(caplog):
    """已核对年份（2026/2027）收录空集合 = 确认无半日市，不触发退化告警。"""
    caplog.set_level("WARNING", logger="core.engine.live.calendar")
    assert BUILTIN_HALF_DAYS[2026] == frozenset()
    assert BUILTIN_HALF_DAYS[2027] == frozenset()
    cal = TradingCalendar(_provider([date(2026, 8, 24)]))
    assert cal.is_half_day(date(2026, 8, 24)) is False
    assert cal.is_trading_allowed(datetime(2026, 8, 24, 14, 0)) is True
    assert not [r for r in caplog.records if "半日市" in r.message]


# ---------------- 2027 年休市表 ----------------

def test_builtin_2027_matches_official_schedule():
    """2027 年内置休市表与国办发明电〔2026〕10号一致（工作日休市逐项抽查）。"""
    closed = BUILTIN_CLOSURES[2027]
    for d in (date(2027, 1, 1),                                   # 元旦
              date(2027, 2, 5), date(2027, 2, 8), date(2027, 2, 12),  # 春节
              date(2027, 4, 5),                                   # 清明
              date(2027, 5, 3), date(2027, 5, 5),                 # 劳动节
              date(2027, 6, 9),                                   # 端午
              date(2027, 9, 15),                                  # 中秋
              date(2027, 10, 1), date(2027, 10, 7)):              # 国庆
        assert d in closed
    # 复市日/节前最后一个交易日不是休市
    for d in (date(2027, 1, 4), date(2027, 2, 4), date(2027, 2, 15),
              date(2027, 4, 6), date(2027, 5, 6), date(2027, 6, 10),
              date(2027, 9, 16), date(2027, 10, 8)):
        assert d not in closed
    # 周末不重复收录（由周末硬规则覆盖）
    for d in closed:
        assert d.weekday() < 5


def test_2027_holiday_closed_and_normal_workday_open():
    """2027 休市日判不可交易；普通工作日/节前最后一个交易日判可交易。"""
    cal = TradingCalendar(_provider([]))  # 全部源为空 → 内置表
    assert cal.is_trading_day(date(2027, 2, 8)) is False      # 春节（周一）
    assert cal.is_trading_day(date(2027, 10, 4)) is False     # 国庆（周一）
    assert cal.is_trading_day(date(2027, 6, 9)) is False      # 端午（周三）
    assert cal.is_trading_allowed(datetime(2027, 2, 8, 10, 0)) is False
    # 春节前最后一个交易日（2027-02-04 周四）正常交易
    assert cal.is_trading_day(date(2027, 2, 4)) is True
    assert cal.is_trading_allowed(datetime(2027, 2, 4, 10, 0)) is True
    assert cal.is_trading_day(date(2027, 10, 8)) is True       # 国庆后复市


def test_2027_weekend_makeup_workdays_still_closed():
    """2027 调休上班日全部是周末（1/31、2/14、5/8、9/26、10/9）：
    国务院"上班"不等于交易所开市，A 股周末硬休市。"""
    cal = TradingCalendar(_provider(["2027-01-31", "2027-02-14", "2027-05-08",
                                     "2027-09-26", "2027-10-09"]))
    for d in (date(2027, 1, 31), date(2027, 2, 14), date(2027, 5, 8),
              date(2027, 9, 26), date(2027, 10, 9)):
        assert d.weekday() >= 5
        assert cal.is_trading_day(d) is False
        assert cal.is_trading_allowed(datetime(d.year, d.month, d.day, 10, 0)) is False
