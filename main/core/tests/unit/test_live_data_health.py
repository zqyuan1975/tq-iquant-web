"""F2 数据体检：周期新鲜度判定 / 盘中不补齐 / 非盘中重读 / 桥离线降级。"""
from datetime import date, datetime

from core.services import live_data_health as h


class _Cal:
    def __init__(self, holidays=()):
        self.holidays = set(holidays)

    def is_trading_day(self, d: date) -> bool:
        return d.weekday() < 5 and d not in self.holidays


class _Disp:
    def __init__(self, bars_by_code=None, fail=False):
        self.bars_by_code = bars_by_code or {}
        self.fail = fail
        self.calls = []

    def query_quote(self, code, period="1d", count=1):
        self.calls.append((code, period, count))
        if self.fail:
            raise RuntimeError("bridge offline")
        return self.bars_by_code.get(code, [])


def _bars(day: date, hhmmss="150100"):
    return [{"stime": day.strftime("%Y%m%d") + hhmmss}]


# 2026-11-18 为周三
_NOW_OFF = datetime(2026, 11, 18, 20, 0)
_NOW_IN = datetime(2026, 11, 18, 10, 0)
_CAL = _Cal()


def test_classify_bar_freshness():
    assert h.classify_bar_freshness("1d", [], date(2026, 11, 18))["status"] == "empty"
    stale = h.classify_bar_freshness("1d", _bars(date(2026, 11, 17)),
                                     date(2026, 11, 18))
    assert stale["status"] == "stale"
    assert stale["last_bar"].startswith("2026-11-17")
    ok = h.classify_bar_freshness("1d", _bars(date(2026, 11, 18)),
                                  date(2026, 11, 18))
    assert ok["status"] == "ok"
    # 周线容差：上一交易日之前的周五仍算 ok
    weekly = h.classify_bar_freshness("1w", _bars(date(2026, 11, 13)),
                                      date(2026, 11, 18))
    assert weekly["status"] == "ok"


def test_expected_bar_date_cutoffs():
    # 盘中 10:00：分钟周期期望当日，日线仍期望上一交易日
    assert h.expected_bar_date("5m", _NOW_IN, _CAL) == date(2026, 11, 18)
    assert h.expected_bar_date("1d", _NOW_IN, _CAL) == date(2026, 11, 17)
    # 收盘后 20:00：日线期望当日
    assert h.expected_bar_date("1d", _NOW_OFF, _CAL) == date(2026, 11, 18)
    # 周末：期望最近交易日（周五）
    weekend = datetime(2026, 11, 21, 10, 0)
    assert h.expected_bar_date("5m", weekend, _CAL) == date(2026, 11, 20)


def test_is_intraday():
    assert h.is_intraday(_NOW_IN, _CAL) is True
    assert h.is_intraday(datetime(2026, 11, 18, 8, 0), _CAL) is False
    assert h.is_intraday(datetime(2026, 11, 21, 10, 0), _CAL) is False
    assert h.is_intraday(_NOW_IN, _Cal(holidays=[date(2026, 11, 18)])) is False


def test_check_data_health_ok_and_missing():
    disp = _Disp({
        "600000.SH": _bars(date(2026, 11, 18)),
        "000001.SZ": _bars(date(2026, 11, 17)),
        # 600519.SH 无数据 → empty
    })
    health = h.check_data_health(disp, ["600000.SH", "000001.SZ", "600519.SH"],
                                 ["1d"], now=_NOW_OFF, calendar=_CAL)
    p = health["periods"]["1d"]
    assert p["total"] == 3
    assert p["ok"] == 1
    assert p["stale"] == 1
    assert p["empty"] == 1
    assert health["healthy"] is False
    assert health["unavailable"] is False
    assert set(health["missing_codes"]) == {"000001.SZ", "600519.SH"}
    assert health["missing_count"] == 2


def test_check_data_health_bridge_offline():
    health = h.check_data_health(_Disp(fail=True), ["600000.SH"], ["1d"],
                                 now=_NOW_OFF, calendar=_CAL)
    assert health["unavailable"] is True
    assert health["healthy"] is False
    assert health["periods"]["1d"]["error"] == 1


def test_auto_fill_skipped_intraday():
    disp = _Disp({})
    health = h.check_data_health(disp, ["600000.SH"], ["1d"],
                                 now=_NOW_IN, calendar=_CAL)
    disp.calls.clear()
    fill = h.auto_fill_missing(disp, health, ["600000.SH"], ["1d"],
                               now=_NOW_IN, calendar=_CAL)
    assert fill["triggered"] is False
    assert fill["reason"] == "intraday"
    assert disp.calls == []  # 盘中不触发任何补齐读取


def test_auto_fill_off_hours_warms_and_rechecks():
    # 第一次读空 → 补齐读（count=240）有数据 → after 健康
    class _WarmDisp(_Disp):
        def __init__(self):
            super().__init__()
            self.warmed = False

        def query_quote(self, code, period="1d", count=1):
            self.calls.append((code, period, count))
            if count >= h._WARM_COUNT:
                self.warmed = True
            return _bars(date(2026, 11, 18)) if self.warmed else []

    disp = _WarmDisp()
    health = h.check_data_health(disp, ["600000.SH"], ["1d"],
                                 now=_NOW_OFF, calendar=_CAL)
    fill = h.auto_fill_missing(disp, health, ["600000.SH"], ["1d"],
                               now=_NOW_OFF, calendar=_CAL)
    assert fill["triggered"] is True
    assert fill["requested"] == 1
    assert fill["warmed"] == 1
    assert fill["after"]["healthy"] is True
    assert any(c[2] == h._WARM_COUNT for c in disp.calls)


def test_run_health_check_healthy_skips_fill():
    disp = _Disp({"600000.SH": _bars(date(2026, 11, 18))})
    out = h.run_health_check(disp, ["600000.SH"], ["1d"],
                             auto_fill=True, now=_NOW_OFF, calendar=_CAL)
    assert out["health"]["healthy"] is True
    assert out["auto_fill"] is None
    assert h.health_summary_text(out["health"]) == "数据体检通过"
