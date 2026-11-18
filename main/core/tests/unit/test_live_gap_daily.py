"""F3：日终缺失分钟段清单（压缩 / 登记 / 日终报告只输出一次）。"""
from datetime import date, datetime, timedelta
from types import SimpleNamespace

from core.engine.live.daily_closer import DailyCloser
from core.engine.live.gap_report import compress_missing_segments
from core.engine.live_engine import LiveEngine


def _t(h, m):
    return datetime(2026, 8, 25, h, m)


def test_compress_empty_and_single():
    assert compress_missing_segments([]) == []
    segs = compress_missing_segments([_t(9, 31)])
    assert segs == [{
        "start": "2026-08-25T09:31:00",
        "end": "2026-08-25T09:31:00",
        "minutes": 1,
    }]


def test_compress_merges_adjacent_and_splits_lunch():
    minutes = [_t(11, 28), _t(11, 29), _t(11, 30), _t(13, 1), _t(13, 2)]
    segs = compress_missing_segments(minutes)
    assert [s["minutes"] for s in segs] == [3, 2]
    assert segs[0]["start"] == "2026-08-25T11:28:00"
    assert segs[0]["end"] == "2026-08-25T11:30:00"
    assert segs[1]["start"] == "2026-08-25T13:01:00"


def test_compress_dedup_and_seconds_truncation():
    minutes = [
        datetime(2026, 8, 25, 9, 31, 42),
        datetime(2026, 8, 25, 9, 31, 7),
        datetime(2026, 8, 25, 9, 33, 0),
    ]
    segs = compress_missing_segments(minutes)
    assert len(segs) == 2
    assert segs[0]["minutes"] == 1
    assert segs[1]["start"] == "2026-08-25T09:33:00"


def _report_stub(events):
    stub = LiveEngine.__new__(LiveEngine)
    stub.ctx = SimpleNamespace(session_id=7)
    stub._engine_key = "tdx"
    stub._day_missing_minutes = {}
    stub._emit_risk_safe = lambda payload, persist=False: events.append(
        (payload, persist))
    return stub


def test_record_missing_minutes_buckets_by_day():
    stub = _report_stub([])
    LiveEngine._record_missing_minutes(stub, [_t(10, 0), _t(10, 1)])
    LiveEngine._record_missing_minutes(
        stub, [datetime(2026, 8, 26, 9, 31)])
    assert set(stub._day_missing_minutes) == {date(2026, 8, 25), date(2026, 8, 26)}
    assert len(stub._day_missing_minutes[date(2026, 8, 25)]) == 2


def test_report_daily_gap_emits_once_and_clears(monkeypatch):
    events = []
    stub = _report_stub(events)
    calls = []
    monkeypatch.setattr("core.engine.live_engine.notify",
                        lambda *a, **k: calls.append((a, k)) or True)
    LiveEngine._record_missing_minutes(
        stub, [_t(10, 0), _t(10, 1), _t(10, 2), _t(11, 5)])
    LiveEngine._report_daily_gap(stub, date(2026, 8, 25))

    assert len(events) == 1
    payload, persist = events[0]
    assert persist is True
    assert payload["gate"] == "bar_gap_daily"
    assert payload["action"] == "missing_report"
    assert payload["missing_minutes"] == 4
    assert payload["segment_count"] == 2
    assert payload["segments"][0]["start"].endswith("10:00:00")
    assert len(calls) == 1
    assert date(2026, 8, 25) not in stub._day_missing_minutes

    # 幂等：同日再调用不再输出
    LiveEngine._report_daily_gap(stub, date(2026, 8, 25))
    assert len(events) == 1


def test_gap_event_records_missing_minutes():
    """_emit_gap_event（经 _audit_gap_recovery partial）把仍缺分钟登记到当日。"""
    stub = _report_stub([])
    stub._gap_recovery_plan = {
        "gap_start": datetime(2026, 8, 25, 9, 55),
        "gap_end": datetime(2026, 8, 25, 10, 5),
        "reason": "bridge_recovered",
    }
    stub._emit = lambda kind, payload: None
    replayed = [SimpleNamespace(bar_time=datetime(2026, 8, 25, 10, 0)
                                + timedelta(minutes=i))
                for i in range(6)]
    LiveEngine._audit_gap_recovery(stub, datetime(2026, 8, 25, 10, 5), replayed)
    missing = stub._day_missing_minutes[date(2026, 8, 25)]
    assert [m.strftime("%H:%M") for m in sorted(missing)] == [
        "09:56", "09:57", "09:58", "09:59"]


def test_daily_closer_callback_invoked_and_failure_swallowed():
    closer = DailyCloser.__new__(DailyCloser)
    closer._ctx = SimpleNamespace(session_id=7)
    calls = []
    closer._on_daily_gap_report = lambda d: calls.append(d)
    DailyCloser._report_daily_gap(closer, date(2026, 8, 25))
    assert calls == [date(2026, 8, 25)]

    closer._on_daily_gap_report = lambda d: (_ for _ in ()).throw(RuntimeError("x"))
    DailyCloser._report_daily_gap(closer, date(2026, 8, 25))  # 不抛
