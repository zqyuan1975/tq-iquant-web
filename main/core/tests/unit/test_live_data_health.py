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
    # 首次读空 → 体检内预热重读（count=240）仍空 → 补齐再读（count=240）有数据 → after 健康
    class _WarmDisp(_Disp):
        def __init__(self):
            super().__init__()
            self.warm_reads = 0

        def query_quote(self, code, period="1d", count=1):
            self.calls.append((code, period, count))
            if count >= h._WARM_COUNT:
                self.warm_reads += 1
                # 第 1 次是体检内的预热重读（仍空），第 2 次是 auto_fill 的补齐
                return _bars(date(2026, 11, 18)) if self.warm_reads >= 2 else []
            return []

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
    # 超预算/抽样截断后不再做第二遍补齐（否则同等规模的串行读取会继续拖住启动）；
    # 但缺失嫌疑标的的预热重读已在体检内完成，不算「跳过重读」。
    disp = _Disp({})
    out = h.run_health_check(disp, ["A", "B"], ["1d"], auto_fill=True,
                             now=_NOW_OFF, calendar=_CAL, max_codes=1)
    assert out["health"]["truncated"] is True
    assert out["health"]["truncated_reason"] == "sample"
    assert out["health"]["warm_retry"]["attempted"] == 1  # 有嫌疑就重读，与截断无关
    assert out["auto_fill"]["reason"] == "truncated"
    # 1 次 3 根判定 + 1 次 240 根预热重读
    assert [c[2] for c in disp.calls] == [h._CHECK_COUNT, h._WARM_COUNT]


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
    assert health["data_missing_count"] == 0            # 超时不计入「真缺失」
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


class _ShallowBarDisp:
    """3 根读空、240 根才有数据：复现 2026-11-20 09:07 的误报场景。"""

    def __init__(self, warm_codes=(), fail_codes=()):
        self.warm_codes = set(warm_codes)
        self.fail_codes = set(fail_codes)
        self.calls = []

    def query_quote(self, code, period="1d", count=1):
        self.calls.append((code, period, count))
        if code in self.fail_codes:
            raise RuntimeError("query failed")
        if count >= h._WARM_COUNT and code in self.warm_codes:
            return _bars(date(2026, 11, 18))
        return []


def test_sample_truncation_does_not_false_report_missing():
    """抽样截断/未验 ≠ 缺失：3 根读空但 240 根能读到的，不得计缺失或提示补下载。"""
    codes = ["600000.SH", "000001.SZ", "600519.SH"]
    disp = _ShallowBarDisp(warm_codes=codes)
    health = h.check_data_health(disp, codes, ["1d"], now=_NOW_OFF,
                                 calendar=_CAL, max_codes=2,
                                 time_budget_seconds=30.0)
    assert health["truncated"] is True
    assert health["truncated_reason"] == "sample"
    assert health["checked_codes"] == 2
    assert health["unchecked_count"] == 1          # 共 3 只、已验 2 只
    assert health["data_missing_count"] == 0
    assert health["status_counts"]["ok"] == 2
    assert health["warm_retry"]["resolved"] == 2
    assert health["checks"][0]["last_bar"].startswith("2026-11-18")
    assert health["checks"][0]["reason_code"] == "ok"
    text = h.health_summary_text(health)
    assert "未完成" in text and "已验 2/3" in text
    assert "补下载" not in text


def test_intraday_skips_warm_retry_and_marks_reason():
    """盘中不做预热重读（避免抢实盘通道），结果明确标注跳过原因。"""
    disp = _ShallowBarDisp(warm_codes=["600000.SH"])
    health = h.check_data_health(disp, ["600000.SH"], ["1d"], now=_NOW_IN,
                                 calendar=_CAL)
    assert health["intraday"] is True
    assert health["warm_retry"]["enabled"] is False
    assert health["warm_retry"]["skipped_reason"] == "intraday"
    assert health["warm_retry"]["attempted"] == 0
    assert health["periods"]["1d"]["empty"] == 1
    assert all(c[2] == h._CHECK_COUNT for c in disp.calls)  # 只做过 3 根那次


def test_query_error_is_not_counted_as_real_missing():
    """查询异常与真缺失分开：error 只进 missing 历史字段，不进 data_missing。"""
    disp = _ShallowBarDisp(fail_codes=["600000.SH"])  # 600519.SH 真无 bar
    health = h.check_data_health(disp, ["600000.SH", "600519.SH"], ["1d"],
                                 now=_NOW_OFF, calendar=_CAL,
                                 time_budget_seconds=30.0)
    assert health["status_counts"]["error"] == 1
    assert health["status_counts"]["empty"] == 1
    assert health["data_missing_count"] == 1
    assert health["data_missing_codes"] == ["600519.SH"]
    assert "600000.SH" not in health["data_missing_codes"]
    # 历史字段口径不变（stale+empty+error），仅新增字段区分
    assert health["missing_count"] == 2
    by_code = {(c["code"], c["period"]): c for c in health["checks"]}
    assert by_code[("600000.SH", "1d")]["reason_code"] == "query_error"
    assert by_code[("600519.SH", "1d")]["reason_code"] == "no_bars"
    text = h.health_summary_text(health)
    assert "查询异常 1 只" in text and "无 bar 1 只" in text


def test_health_summary_text_branches():
    """文案分支：未完成/超时/异常/无 bar 各自触发，补下载只在有真缺失时出现。"""
    base = {"version": h.DATA_HEALTH_VERSION, "periods": {}}
    truncated = dict(base, truncated=True, truncated_reason="budget",
                     checked_codes=30, requested_codes=251, status_counts={})
    assert "预算耗尽" in h.health_summary_text(truncated)
    assert "已验 30/251" in h.health_summary_text(truncated)
    assert "补下载" not in h.health_summary_text(truncated)

    timeout_only = dict(base, status_counts={"ok": 2, "timeout": 1},
                        timeout_count=1)
    text = h.health_summary_text(timeout_only)
    assert "取价超时 1 只" in text and "不算缺失" in text
    assert "补下载" not in text

    error_only = dict(base, status_counts={"ok": 2, "error": 1})
    text = h.health_summary_text(error_only)
    assert "查询异常 1 只" in text
    assert "补下载" not in text

    empty_only = dict(base, status_counts={"ok": 2, "empty": 1},
                      data_missing_codes=["600519.SH"], data_missing_count=1)
    text = h.health_summary_text(empty_only)
    assert "无 bar 1 只" in text and "600519.SH" in text
    assert "补下载" in text


# ---------------- N3：轮换抽样游标 ----------------

def test_rotate_sample_covers_pool_over_rounds(tmp_path, monkeypatch):
    monkeypatch.setattr(h, "_cursor_path", lambda: tmp_path / "cursor.json")
    codes = [f"60000{i}.SH" for i in range(7)]

    r1 = h.rotate_sample(codes, 3, key="s1")
    r2 = h.rotate_sample(codes, 3, key="s1")
    r3 = h.rotate_sample(codes, 3, key="s1")

    assert r1["sample"] == codes[:3]
    assert r2["sample"] == codes[3:6]
    # 第三轮回绕：从 6 开始取 3 只，第 2 只回到 0
    assert r3["sample"] == [codes[6], codes[0], codes[1]]
    assert r3["wrapped"] is True


def test_rotate_sample_limit_and_scope_unchanged(tmp_path, monkeypatch):
    """单次上限不变；不同 key（会话）各自独立游标。"""
    monkeypatch.setattr(h, "_cursor_path", lambda: tmp_path / "cursor.json")
    codes = [f"S{i}" for i in range(5)]

    a = h.rotate_sample(codes, 5, key="a")
    b = h.rotate_sample(codes, 2, key="b")

    assert a["sample"] == codes  # 全池一次取完
    assert b["sample"] == codes[:2]
    assert a["total"] == b["total"] == 5


def test_rotate_sample_empty_and_zero_limit(tmp_path, monkeypatch):
    monkeypatch.setattr(h, "_cursor_path", lambda: tmp_path / "cursor.json")

    assert h.rotate_sample([], 10)["sample"] == []
    assert h.rotate_sample(["A"], 0)["sample"] == []


def test_check_data_health_rotates_sample_across_pool(tmp_path, monkeypatch):
    """两轮体检拼接后可覆盖全池（不再永远只查前 30 只）。"""
    monkeypatch.setattr(h, "_cursor_path", lambda: tmp_path / "cursor.json")
    codes = [f"60000{i}.SH" for i in range(6)]
    disp = _Disp({c: _bars(date(2026, 11, 18)) for c in codes})

    first = h.check_data_health(disp, codes, ["1d"], now=_NOW_OFF, calendar=_CAL,
                                max_codes=3, session_id=9)
    second = h.check_data_health(disp, codes, ["1d"], now=_NOW_OFF, calendar=_CAL,
                                 max_codes=3, session_id=9)

    checked = {c for c, _p, _n in disp.calls}
    assert checked == set(codes)
    assert first["sample_rotation"]["start"] == 0
    assert second["sample_rotation"]["start"] == 3
    assert first["checked_codes"] == second["checked_codes"] == 3


