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


def test_check_data_health_max_codes_truncates_without_scanning_pool():
    # 启动链路必须限流：全池 496 只逐个 /quote（掘金桥单只 ~8s）会卡住启动
    codes = ["600000.SH", "000001.SZ", "600519.SH"]
    disp = _Disp({c: _bars(date(2026, 11, 18)) for c in codes})
    health = h.check_data_health(disp, codes, ["1d"], now=_NOW_OFF, calendar=_CAL,
                                 max_codes=2)
    assert health["truncated"] is True
    assert health["requested_codes"] == 3
    assert health["checked_codes"] == 2
    assert len(disp.calls) == 2  # 未读取第 3 只
    assert health["healthy"] is False  # 未全量校验，不能报「通过」
    assert "未完成" in h.health_summary_text(health)


def test_check_data_health_time_budget_stops_early(monkeypatch):
    clock = {"t": 1000.0}
    monkeypatch.setattr(h._time, "monotonic", lambda: clock["t"])

    class _SlowDisp(_Disp):
        def query_quote(self, code, period="1d", count=1):
            self.calls.append((code, period, count))
            clock["t"] += 10.0
            return _bars(date(2026, 11, 18))

    disp = _SlowDisp()
    health = h.check_data_health(disp, ["A", "B", "C"], ["1d"], now=_NOW_OFF,
                                 calendar=_CAL, time_budget_seconds=3)
    assert health["truncated"] is True
    assert health["checked_codes"] == 1
    assert len(disp.calls) == 1


def test_run_health_check_truncated_skips_fill():
    # 超预算后不再补齐（否则同等规模的串行读取会继续拖住启动链）
    disp = _Disp({})
    out = h.run_health_check(disp, ["A", "B"], ["1d"], auto_fill=True,
                             now=_NOW_OFF, calendar=_CAL, max_codes=1)
    assert out["health"]["truncated"] is True
    assert out["auto_fill"]["reason"] == "budget_exhausted"
    assert len(disp.calls) == 1


def test_check_data_health_logs_slow_quote(monkeypatch, caplog):
    """单只取价超阈值要留痕：日志带会话/标的/周期/耗时，结果里带 slow_quotes。"""
    monkeypatch.setattr(h, "_SLOW_QUOTE_SECONDS", 0.0)
    disp = _Disp({c: _bars(date(2026, 11, 18)) for c in ("600000.SH", "000001.SZ")})
    with caplog.at_level("WARNING", logger=h.logger.name):
        health = h.check_data_health(disp, ["600000.SH", "000001.SZ"], ["1d"],
                                     now=_NOW_OFF, calendar=_CAL, session_id=7)
    assert health["slow_quote_count"] == 2
    assert {q["code"] for q in health["slow_quotes"]} == {"600000.SH", "000001.SZ"}
    assert "会话 7" in caplog.text and "600000.SH" in caplog.text


def test_check_data_health_no_slow_quote_below_threshold():
    disp = _Disp({"600000.SH": _bars(date(2026, 11, 18))})
    health = h.check_data_health(disp, ["600000.SH"], ["1d"],
                                 now=_NOW_OFF, calendar=_CAL, session_id=7)
    assert health["slow_quote_count"] == 0


class _TimeoutDisp:
    """支持 per-call timeout 的桥替身：slow_codes 里的标的一律超时。"""

    def __init__(self, slow_codes=(), bars_by_code=None):
        self.slow_codes = set(slow_codes)
        self.bars_by_code = bars_by_code or {}
        self.calls = []

    def query_quote(self, code, period="1d", count=1, timeout=None):
        self.calls.append((code, period, timeout))
        if code in self.slow_codes:
            raise TimeoutError("timed out")
        return self.bars_by_code.get(code, [])


def test_check_data_health_hard_timeout_marks_unverified_not_missing():
    """单只硬超时：放弃该只、计入未校验，**不**误报为数据缺失。"""
    codes = ["600000.SH", "000001.SZ", "600519.SH"]
    disp = _TimeoutDisp(
        slow_codes={"000001.SZ"},
        bars_by_code={c: _bars(date(2026, 11, 18)) for c in codes})
    health = h.check_data_health(disp, codes, ["1d"], now=_NOW_OFF,
                                 calendar=_CAL, time_budget_seconds=2.0)
    assert health["timeout_count"] == 1
    assert health["timeout_codes"] == ["000001.SZ"]
    assert health["checked_codes"] == 2          # 超时那只未校验
    assert "000001.SZ" not in health["missing_codes"]   # 不当成缺失
    assert health["periods"]["1d"]["timeout"] == 1
    assert health["periods"]["1d"]["error"] == 0        # 也不当成通道错误
    assert health["healthy"] is False
    assert "未校验" in h.health_summary_text(health)
    # 单只硬超时必须严格低于整体预算（2s 预算 → ≤ 1.2s）
    assert all(t is not None and t < 2.0 for _c, _p, t in disp.calls)


def test_check_data_health_hard_timeout_defaults_below_no_budget():
    """不带预算时仍走默认单只硬超时（2s），保证在飞调用可被放弃。"""
    disp = _TimeoutDisp(bars_by_code={"A": _bars(date(2026, 11, 18))})
    h.check_data_health(disp, ["A"], ["1d"], now=_NOW_OFF, calendar=_CAL)
    assert disp.calls[0][2] == h._QUOTE_HARD_TIMEOUT_SECONDS


def test_check_data_health_dispatch_without_timeout_support_still_works():
    """tdx/ths 等同步实现不接受 per-call timeout：退化为原调用，不抛 TypeError。"""
    disp = _Disp({"A": _bars(date(2026, 11, 18))})
    health = h.check_data_health(disp, ["A"], ["1d"], now=_NOW_OFF,
                                 calendar=_CAL, time_budget_seconds=2.0)
    assert health["periods"]["1d"]["ok"] == 1
    assert health["timeout_count"] == 0


def test_is_timeout_error_detects_wrapped_bridge_error():
    """桥把 httpx 超时包成 BridgeUnavailableError（cause 保留）也要识别。"""

    class _BridgeUnavailable(RuntimeError):
        pass

    err = _BridgeUnavailable("bridge request failed: ")
    err.__cause__ = TimeoutError()
    assert h._is_timeout_error(err) is True
    assert h._is_timeout_error(RuntimeError("bridge offline")) is False
