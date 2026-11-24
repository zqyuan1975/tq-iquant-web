# -*- coding: utf-8 -*-
"""内置分钟数据下载（``minute_download``）单元测试。

覆盖：
- 7709 协议：请求字节布局、响应帧（普通/压缩）、逐条校验与去重；
- 存储：幂等 upsert、区间读取、5m 聚合口径；
- 下载器：缺日计划、写入 + 台账登记、主站失败切换、限频间隔；
- 优先读层：store 新鲜命中、落后/非 1m/异常回落 inner。

全部离线：不连主站、不读写 vipdoc、不碰真实业务库。
"""
import struct
import logging
import threading
import time as walltime
import zlib
from datetime import date, datetime, time, timedelta

import pytest

from core.services import kline_coverage_ledger as ledger
from core.services.minute_download import protocol
from core.services.minute_download.downloader import (
    SOURCE, DownloadConfig, MinuteDownloadService, RateLimiter,
    planned_missing_days,
)
from core.services.minute_download.hosts import HostPool, load_host_pool
from core.services.minute_download.store import MinuteStore
from core.services.minute_download.store_source import StoreFirstMinuteDispatcher

DAY = date(2026, 11, 20)  # 周五


# ---------------------------------------------------------------------------
# 协议
# ---------------------------------------------------------------------------

def _enc_varint(value):
    """编码一个「类 utf-8」有符号变长整数（与主站 K 线记录一致）。"""
    negative = value < 0
    rest = abs(int(value))
    first = rest & 0x3F
    rest >>= 6
    out = bytearray()
    if negative:
        first |= 0x40
    if rest:
        out.append(first | 0x80)
        while True:
            chunk = rest & 0x7F
            rest >>= 7
            if rest:
                out.append(chunk | 0x80)
            else:
                out.append(chunk)
                break
    else:
        out.append(first)
    return bytes(out)


def _records(seq):
    """按主站口径编码若干条 1m 记录（价格前向差分 + 变长整数）。"""
    out = bytearray()
    base = 0
    for day, hh, mm, close in seq:
        close_i = int(round(close * 1000))
        open_i = close_i
        high_i = close_i + 10
        low_i = close_i - 10
        raw_day = ((day.year - 2004) << 11) | (day.month * 100 + day.day)
        out += struct.pack("<HH", raw_day, hh * 60 + mm)
        out += _enc_varint(open_i - base)
        out += _enc_varint(close_i - open_i)
        out += _enc_varint(high_i - open_i)
        out += _enc_varint(low_i - open_i)
        out += struct.pack("<II", 100, 1000)
        base = close_i
    return bytes(out)


def _body(seq, count=None, tail=b""):
    n = len(seq) if count is None else count
    return struct.pack("<H", n) + _records(seq) + tail


def _frame(body, compress=False):
    # 真实主站帧头 16B：<IIIHH>，长度字段在 [12:14]/[14:16]（u16 LE）。
    if compress:
        payload = zlib.compress(body)
        return b"\x00" * 12 + struct.pack("<HH", len(payload), len(body)) + payload
    return b"\x00" * 12 + struct.pack("<HH", len(body), len(body)) + body


def _reader(data):
    pos = [0]

    def recv(n):
        chunk = data[pos[0]:pos[0] + n]
        pos[0] += n
        if len(chunk) < n:
            raise protocol.ProtocolError("short")
        return chunk

    return recv


def test_setup_and_heartbeat_packets():
    # 实测：第三段握手会把连接切到另一种模式（K 线只回 2 字节废帧）。
    assert len(protocol.SETUP_PACKETS) == 2
    assert protocol.SETUP_PACKETS[0].hex() == "0c0218930001030003000d0001"
    assert protocol.SETUP_PACKETS[1].hex() == "0c0218940001030003000d0002"
    assert protocol.encode_heartbeat(1).hex() == (
        "0c0c186c0001080008004e04010075c73301")


def test_encode_bars_request_layout():
    req = protocol.encode_bars_request(1, "600000", start=800, count=800)
    assert len(req) == 38
    fields = struct.unpack("<HIHHHH6sHHHHIIH", req)
    assert fields[0] == 0x010C
    assert fields[1] == 0x01016408
    assert fields[2:5] == (0x1C, 0x1C, 0x052D)
    market, code, category, one, start, count = fields[5:11]
    assert (market, code, category, one, start, count) == (
        1, b"600000", protocol.CATEGORY_1MIN, 1, 800, 800)
    assert fields[11:] == (0, 0, 0)


def test_encode_bars_request_rejects_bad_params():
    with pytest.raises(ValueError):
        protocol.encode_bars_request(1, "60000")
    with pytest.raises(ValueError):
        protocol.encode_bars_request(1, "600000", count=801)
    with pytest.raises(ValueError):
        protocol.encode_bars_request(1, "600000", start=65536)


def test_normalize_code_markets():
    assert protocol.normalize_code("600000.SH") == (1, "600000")
    assert protocol.normalize_code("000001") == (0, "000001")
    assert protocol.normalize_code("300750.SZ") == (0, "300750")
    assert protocol.normalize_code("830799.BJ") == (2, "830799")
    assert protocol.normalize_code("920001") == (2, "920001")
    assert protocol.normalize_code("430047") == (2, "430047")
    with pytest.raises(ValueError):
        protocol.normalize_code("abc")


def test_parse_bars_body_sorts_and_last_wins():
    body = _body([
        (DAY, 9, 32, 10.2),
        (DAY, 9, 31, 10.0),
        (DAY, 9, 31, 10.5),
    ])
    records, dropped = protocol.parse_bars_body(body)
    assert dropped == 0
    assert [r.stime for r in records] == ["20261120093100", "20261120093200"]
    assert records[0].close == pytest.approx(10.5)
    assert records[0].high == pytest.approx(10.51)
    assert records[0].low == pytest.approx(10.49)


def test_parse_bars_body_drops_invalid_and_truncated():
    body = _body([
        (DAY, 9, 20, 10.0),   # 非竞价时段
        (DAY, 9, 31, 0.0),    # 价格非法
        (DAY, 9, 33, 10.3),   # 合法
    ], count=4, tail=b"\x00" * 10)  # 第 4 条截断
    records, dropped = protocol.parse_bars_body(body)
    assert [r.stime for r in records] == ["20261120093300"]
    assert dropped == 3


def test_read_response_frame_plain_and_zlib():
    body = _body([(DAY, 9, 31, 10.0)])
    # 主站明文帧 zip == unzip；压缩帧 zip 为压缩后长度。
    assert protocol.read_response_frame(_reader(_frame(body))) == body
    assert protocol.read_response_frame(
        _reader(_frame(body, compress=True))) == body
    with pytest.raises(protocol.ProtocolError):
        protocol.read_response_frame(_reader(b"\x00\x00"))


def test_records_to_bars_keeps_share_volume():
    rec = protocol.BarRecord(day=DAY, minute=time(9, 31), open=10.0,
                             high=10.1, low=9.9, close=10.0, amount=1000.0,
                             volume=123)
    bar = protocol.records_to_bars([rec])[0]
    assert bar["volume"] == 123
    assert bar["stime"] == "20261120093100"


# ---------------------------------------------------------------------------
# 存储
# ---------------------------------------------------------------------------

def _bar(hh, mm, close=10.0, volume=100):
    return {
        "stime": "20261120%02d%02d00" % (hh, mm),
        "open": close, "high": close, "low": close, "close": close,
        "volume": volume, "amount": 1000.0,
    }


def test_store_upsert_idempotent_and_reads(tmp_path):
    store = MinuteStore(tmp_path / "minute.sqlite3")
    assert store.upsert_bars("600000", "1m", [_bar(9, 31, 10.0),
                                             _bar(9, 32, 10.1)]) == 2
    assert store.upsert_bars("600000", "1m", [_bar(9, 31, 10.5)]) == 1
    assert store.count("600000") == 2
    assert store.latest_bar_time("600000") == "2026-11-20 09:32"
    bars = store.recent_bars("600000", "1m", 5)
    assert bars[0]["close"] == pytest.approx(10.5)
    assert bars[0]["stime"] == "20261120093100"
    assert len(store.bars_between("600000", "1m",
                                  "2026-11-20", "2026-11-20")) == 2


def test_store_aggregate_5m_complete_buckets(tmp_path):
    store = MinuteStore(tmp_path / "minute.sqlite3")
    bars = [_bar(9, m, 10.0 + m / 100.0) for m in range(31, 41)]
    store.upsert_bars("600000", "1m", bars)
    out = store.aggregate_5m("600000", "2026-11-20", "2026-11-20")
    assert [b["stime"] for b in out] == ["20261120093500", "20261120094000"]
    assert out[0]["open"] == pytest.approx(10.31)
    assert out[0]["close"] == pytest.approx(10.35)
    assert out[0]["volume"] == 500


# ---------------------------------------------------------------------------
# 下载器
# ---------------------------------------------------------------------------

def _rec(day, hh, mm, close=10.0):
    return protocol.BarRecord(day=day, minute=time(hh, mm), open=close,
                              high=close, low=close, close=close,
                              amount=0.0, volume=10)


def _service(tmp_path, request_fn, codes=("600000",), now=None, **cfg):
    config = DownloadConfig(max_req_per_second=0, **cfg)
    return MinuteDownloadService(
        store=MinuteStore(tmp_path / "minute.sqlite3"),
        config=config,
        host_pool=HostPool([("127.0.0.1", 7709)]),
        codes_provider=lambda: list(codes),
        ledger_db_path=tmp_path / "ledger.sqlite3",
        request_fn=request_fn,
        clock=(lambda: now) if now else None,
    )


def test_planned_missing_days_from_ledger(tmp_path):
    db = tmp_path / "ledger.sqlite3"
    days = [date(2026, 11, 16), date(2026, 11, 17)]
    ledger.record_coverage("600000", "1m", ledger.FIELD_ALL,
                           [(d, d) for d in days], source=SOURCE, db_path=db)
    missing = planned_missing_days("600000", date(2026, 11, 16),
                                   date(2026, 11, 20), db_path=db)
    assert missing == [date(2026, 11, 18), date(2026, 11, 19),
                       date(2026, 11, 20)]


def test_run_once_writes_bars_and_defers_today_coverage(tmp_path):
    now = datetime(2026, 11, 20, 10, 10)

    def req(host, port, market, code, start, count):
        return [_rec(DAY, 9, 31), _rec(DAY, 9, 32)]

    svc = _service(tmp_path, req, now=now)
    summary = svc.run_once()
    assert summary["codes"] == 1
    assert summary["bars"] == 2
    assert svc.store.count("600000") == 2
    # 盘中不登记当日覆盖，避免把半日数据当成整日完整。
    assert planned_missing_days("600000", DAY, DAY,
                                db_path=tmp_path / "ledger.sqlite3") == [DAY]


def test_run_once_backfills_missing_days_and_records_coverage(tmp_path):
    today = date(2026, 11, 24)
    yesterday = date(2026, 11, 23)
    window_start = today - timedelta(days=30)
    db = tmp_path / "ledger.sqlite3"
    target = [date(2026, 11, 19), date(2026, 11, 20)]
    covered = [d for d in ledger.trading_days(window_start, yesterday)
               if d not in target]
    ledger.record_coverage("600000", "1m", ledger.FIELD_ALL,
                           [(d, d) for d in covered], source=SOURCE, db_path=db)
    assert planned_missing_days("600000", window_start, yesterday,
                                db_path=db) == target

    def req(host, port, market, code, start, count):
        if start > 0:
            return []
        return [_rec(target[0], 14, 59), _rec(target[1], 14, 59)]

    svc = _service(tmp_path, req, now=datetime(2026, 11, 24, 10, 0))
    summary = svc.run_once()
    assert summary["bars"] == 2
    assert planned_missing_days("600000", window_start, yesterday,
                                db_path=db) == []


def test_query_rotates_host_on_failure(tmp_path):
    seen = []

    def req(host, port, market, code, start, count):
        seen.append(host)
        if host == "bad":
            raise OSError("boom")
        return [_rec(DAY, 9, 31)]

    config = DownloadConfig(max_req_per_second=0, retry_times=0,
                            retry_backoff_seconds=0)
    pool = HostPool([("bad", 1), ("good", 2)], failure_threshold=3)
    svc = MinuteDownloadService(
        store=MinuteStore(tmp_path / "minute.sqlite3"), config=config,
        host_pool=pool, codes_provider=lambda: ["600000"],
        ledger_db_path=tmp_path / "ledger.sqlite3", request_fn=req,
        clock=lambda: datetime(2026, 11, 20, 10, 10))
    summary = svc.run_once()
    assert summary["failures"] == 0
    assert summary["bars"] == 1
    assert seen[:2] == ["bad", "good"]
    assert pool.snapshot()[0]["failure"] == 1


def test_rate_limiter_spacing():
    now = [0.0]
    sleeps = []

    def sleep(seconds):
        sleeps.append(seconds)
        now[0] += seconds

    limiter = RateLimiter(4.0, clock=lambda: now[0], sleep=sleep)
    for _ in range(3):
        limiter.acquire()
    assert [s for s in sleeps if s > 0] == pytest.approx([0.25, 0.25])


def test_empty_codes_short_circuits(tmp_path):
    svc = _service(tmp_path, lambda *a: pytest.fail("不应发请求"), codes=(),
                   now=datetime(2026, 11, 20, 10, 10))
    summary = svc.run_once()
    assert summary == {"codes": 0, "bars": 0, "requests": 0,
                       "failures": 0, "empty_days": 0}


# ---------------------------------------------------------------------------
# 并发池
# ---------------------------------------------------------------------------

def test_config_connections_default_and_clamp():
    cfg = DownloadConfig.from_section({})
    assert cfg.connections == 4
    assert cfg.max_req_per_second == 12.0
    assert DownloadConfig.from_section({"connections": 0}).connections == 1
    assert DownloadConfig.from_section({"connections": 99}).connections == 16
    assert DownloadConfig.from_section(
        {"connections": 2, "max_req_per_second": 6}).max_req_per_second == 6


def test_pool_dedupes_codes_before_dispatch(tmp_path):
    calls = []
    lock = threading.Lock()

    def req(host, port, market, code, start, count):
        with lock:
            calls.append((code, start))
        return [_rec(DAY, 9, 31), _rec(DAY, 9, 32)]

    svc = _service(tmp_path, req,
                   codes=("600000", "600000.SH", " 600000 ", "000001.SZ"),
                   now=datetime(2026, 11, 20, 10, 10), connections=4)
    summary = svc.run_once()
    assert summary["codes"] == 2
    assert summary["bars"] == 4
    # 每只票只提交一次，且只拉第一页（无缺日）
    assert sorted(calls) == [("000001", 0), ("600000", 0)]
    svc.stop(timeout=1)


def test_pool_isolates_single_code_failure_and_rotates_host(tmp_path):
    seen = []
    lock = threading.Lock()

    def req(host, port, market, code, start, count):
        with lock:
            seen.append((host, code))
        if code == "000002":
            raise OSError("boom")
        return [_rec(DAY, 9, 31)]

    config = DownloadConfig(max_req_per_second=0, retry_times=0,
                            retry_backoff_seconds=0, connections=4)
    pool = HostPool([("bad", 1), ("good", 2)], failure_threshold=3)
    svc = MinuteDownloadService(
        store=MinuteStore(tmp_path / "minute.sqlite3"), config=config,
        host_pool=pool, codes_provider=lambda: ["600000", "000001", "000002"],
        ledger_db_path=tmp_path / "ledger.sqlite3", request_fn=req,
        clock=lambda: datetime(2026, 11, 20, 10, 10))
    summary = svc.run_once()
    assert summary["failures"] == 1
    assert summary["bars"] == 2
    assert svc.store.count("600000") == 1
    assert svc.store.count("000001") == 1
    assert svc.store.count("000002") == 0
    # 失败票按 HostPool 顺序切到第二主站重试，其余票不受影响
    assert ("bad", "000002") in seen
    assert ("good", "000002") in seen
    svc.stop(timeout=1)


def test_pool_runs_requests_concurrently(tmp_path):
    active = [0]
    peak = [0]
    lock = threading.Lock()

    def req(host, port, market, code, start, count):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        walltime.sleep(0.03)
        with lock:
            active[0] -= 1
        return [_rec(DAY, 9, 31)]

    codes = ["%06d" % i for i in range(8)]
    svc = _service(tmp_path, req, codes=codes,
                   now=datetime(2026, 11, 20, 10, 10), connections=4)
    summary = svc.run_once()
    assert summary["bars"] == 8
    assert peak[0] >= 2, "并发池应让请求重叠执行"
    svc.stop(timeout=1)


def test_pool_concurrent_writes_do_not_conflict(tmp_path):
    """4 连接并发写同一个 SQLite：store 内部单写者，行数必须完全正确。"""
    def req(host, port, market, code, start, count):
        return [_rec(DAY, 9, 31), _rec(DAY, 9, 32)]

    codes = ["%06d" % i for i in range(20)]
    svc = _service(tmp_path, req, codes=codes,
                   now=datetime(2026, 11, 20, 10, 10), connections=4)
    summary = svc.run_once()
    assert summary["failures"] == 0
    assert summary["bars"] == 40
    for code in codes:
        assert svc.store.count(code) == 2
    svc.stop(timeout=1)


def test_pool_rate_limit_is_global_not_per_connection(tmp_path):
    """12 req/s 是全局总速率：8 只票（4 连接）仍只产生 7 次间隔。"""
    now = [0.0]
    sleeps = []
    lock = threading.Lock()

    def sleep(seconds):
        with lock:
            sleeps.append(seconds)
            now[0] += seconds

    def req(host, port, market, code, start, count):
        return [_rec(DAY, 9, 31)]

    codes = ["%06d" % i for i in range(8)]
    svc = _service(tmp_path, req, codes=codes,
                   now=datetime(2026, 11, 20, 10, 10), connections=4)
    svc._limiter = RateLimiter(12.0, clock=lambda: now[0], sleep=sleep)
    summary = svc.run_once()
    assert summary["bars"] == 8
    positive = [s for s in sleeps if s > 0]
    assert len(positive) == 7
    assert positive == pytest.approx([1.0 / 12.0] * 7)
    svc.stop(timeout=1)


def test_connections_one_keeps_sequential_order(tmp_path):
    order = []
    lock = threading.Lock()

    def req(host, port, market, code, start, count):
        with lock:
            order.append(code)
        return [_rec(DAY, 9, 31)]

    codes = ("600000", "000001", "300750")
    svc = _service(tmp_path, req, codes=codes,
                   now=datetime(2026, 11, 20, 10, 10), connections=1)
    summary = svc.run_once()
    assert summary["bars"] == 3
    assert order == list(codes)
    svc.stop(timeout=1)


# ---------------------------------------------------------------------------
# 交易时段门控
# ---------------------------------------------------------------------------

DLOGGER = "core.services.minute_download.downloader"


def _gate_service(tmp_path, box, calls=None, **cfg):
    """带可变时钟的下载器：``box["now"]`` 可随时改写（不依赖真实时间）。"""
    calls = calls if calls is not None else []

    def req(host, port, market, code, start, count):
        calls.append(code)
        return [_rec(DAY, 9, 31)]

    svc = _service(tmp_path, req, now=datetime(2026, 11, 20, 10, 0), **cfg)
    svc._clock = lambda: box["now"]
    return svc, calls


def test_window_config_defaults_and_override():
    cfg = DownloadConfig.from_section({})
    assert (cfg.window_start, cfg.window_end) == ("09:15", "15:05")
    cfg = DownloadConfig.from_section(
        {"window_start": "09:30", "window_end": "14:00"})
    assert (cfg.window_start, cfg.window_end) == ("09:30", "14:00")


@pytest.mark.parametrize("hh,mm,expected_state", [
    (9, 14, "before_window"),
    (9, 15, "active"),
    (15, 4, "active"),
    (15, 5, "active"),
    (15, 6, "after_window"),
])
def test_gate_window_boundaries(tmp_path, hh, mm, expected_state):
    box = {"now": datetime(2026, 11, 20, hh, mm)}
    svc, calls = _gate_service(tmp_path, box)
    summary = svc.run_once()
    assert svc.status()["gate"]["state"] == expected_state
    if expected_state == "active":
        assert summary["requests"] == 1
        assert svc.store.count("600000") == 1
        assert calls == ["600000"]
    else:
        assert summary["requests"] == 0
        assert summary["codes"] == 0
        assert svc.store.count("600000") == 0
        assert calls == []


def test_gate_outside_window_does_not_touch_store(tmp_path):
    box = {"now": datetime(2026, 11, 20, 20, 0)}
    svc, calls = _gate_service(tmp_path, box)
    for _ in range(3):
        summary = svc.run_once()
        assert summary == {"codes": 0, "bars": 0, "requests": 0,
                           "failures": 0, "empty_days": 0}
    assert calls == []
    assert svc.store.count("600000") == 0
    assert svc.status()["rounds"] == 3


def test_gate_logs_only_on_transition(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger=DLOGGER)
    box = {"now": datetime(2026, 11, 20, 20, 0)}
    svc, _ = _gate_service(tmp_path, box)
    svc.run_once()
    svc.run_once()
    left = [r for r in caplog.records if "离开交易时段" in r.getMessage()]
    assert len(left) == 1  # 连续两轮窗口外只打一行

    box["now"] = datetime(2026, 11, 20, 10, 0)
    svc.run_once()
    entered = [r for r in caplog.records if "已进入交易时段" in r.getMessage()]
    assert len(entered) == 1

    box["now"] = datetime(2026, 11, 20, 20, 0)
    svc.run_once()
    left = [r for r in caplog.records if "离开交易时段" in r.getMessage()]
    assert len(left) == 2


def test_gate_cross_day_waits_for_next_open(tmp_path):
    box = {"now": datetime(2026, 11, 19, 23, 59)}  # 周四深夜
    svc, calls = _gate_service(tmp_path, box)
    assert svc.run_once()["requests"] == 0
    assert svc.status()["gate"]["state"] == "after_window"

    box["now"] = datetime(2026, 11, 20, 9, 15)  # 次日开盘窗口边界
    assert svc.run_once()["requests"] == 1
    assert svc.status()["gate"]["state"] == "active"
    assert calls == ["600000"]


@pytest.mark.parametrize("day", [
    date(2026, 11, 21),  # 周六
    date(2026, 10, 1),   # 周四，内置休市表（国庆）
])
def test_gate_skips_non_trading_day(tmp_path, day, caplog):
    caplog.set_level(logging.INFO, logger=DLOGGER)
    box = {"now": datetime(day.year, day.month, day.day, 10, 0)}
    svc, calls = _gate_service(tmp_path, box)
    for _ in range(2):
        assert svc.run_once()["requests"] == 0
    assert calls == []
    assert svc.store.count("600000") == 0
    assert svc.status()["gate"]["state"] == "non_trading_day"
    assert len([r for r in caplog.records
                if "非交易日" in r.getMessage()]) == 1


def test_gate_falls_back_to_weekday_when_calendar_missing(tmp_path, caplog):
    caplog.set_level(logging.INFO, logger=DLOGGER)
    box = {"now": datetime(2028, 1, 3, 10, 0)}  # 周一，内置休市表未收录 2028
    svc, calls = _gate_service(tmp_path, box)
    assert svc.run_once()["requests"] == 1
    assert svc.status()["gate"]["calendar"] == "fallback"
    warns = [r for r in caplog.records
             if r.levelno == logging.WARNING and "交易日历不可用" in r.getMessage()]
    assert len(warns) == 1

    # 再跑一轮：仍在退化态，不重复告警
    svc.run_once()
    warns = [r for r in caplog.records
             if r.levelno == logging.WARNING and "交易日历不可用" in r.getMessage()]
    assert len(warns) == 1
    assert len(calls) == 2


def test_gate_uses_configured_window(tmp_path):
    box = {"now": datetime(2026, 11, 20, 9, 20)}
    svc, calls = _gate_service(
        tmp_path, box, window_start="09:30", window_end="14:00")
    assert svc.run_once()["requests"] == 0
    box["now"] = datetime(2026, 11, 20, 9, 30)
    assert svc.run_once()["requests"] == 1
    box["now"] = datetime(2026, 11, 20, 14, 0)  # 结束边界仍算窗口内
    assert svc.run_once()["requests"] == 1
    box["now"] = datetime(2026, 11, 20, 14, 1)
    assert svc.run_once()["requests"] == 0
    assert len(calls) == 2
    assert svc.status()["gate"]["window"] == ["09:30", "14:00"]


# ---------------------------------------------------------------------------
# 优先读层
# ---------------------------------------------------------------------------

class _Inner:
    minute_health = object()

    def __init__(self):
        self.calls = []

    def query_quotes_batch(self, codes, period="1m", count=10):
        self.calls.append((list(codes), period, count))
        return {c: [{
            "stime": "20261120200000", "open": 1.0, "high": 1.0,
            "low": 1.0, "close": 1.0, "volume": 0, "amount": 0,
        }] for c in codes}


def _store_with(tmp_path, minutes):
    store = MinuteStore(tmp_path / "minute.sqlite3")
    store.upsert_bars("600000", "1m",
                      [_bar(10, m, 10.0 + m / 100.0) for m in minutes])
    return store


def test_store_first_hits_when_fresh(tmp_path):
    store = _store_with(tmp_path, [8, 9, 10])
    inner = _Inner()
    dispatcher = StoreFirstMinuteDispatcher(
        inner, store, clock=lambda: datetime(2026, 11, 20, 10, 10, 20))
    out = dispatcher.query_quotes_batch(["600000"], "1m", 3)
    assert [b["stime"] for b in out["600000"]] == [
        "20261120100800", "20261120100900", "20261120101000"]
    assert inner.calls == []
    assert dispatcher.minute_health is _Inner.minute_health


def test_store_first_falls_back_when_stale(tmp_path):
    store = _store_with(tmp_path, [5])
    inner = _Inner()
    dispatcher = StoreFirstMinuteDispatcher(
        inner, store, clock=lambda: datetime(2026, 11, 20, 10, 10, 20))
    out = dispatcher.query_quotes_batch(["600000"], "1m", 3)
    assert out["600000"][0]["stime"] == "20261120200000"
    assert inner.calls == [(["600000"], "1m", 3)]


def test_store_first_serves_outside_session(tmp_path):
    store = _store_with(tmp_path, [28, 29, 30])
    inner = _Inner()
    dispatcher = StoreFirstMinuteDispatcher(
        inner, store, clock=lambda: datetime(2026, 11, 20, 12, 0))
    out = dispatcher.query_quotes_batch(["600000"], "1m", 3)
    assert out["600000"][-1]["stime"] == "20261120103000"
    assert inner.calls == []


def test_store_first_delegates_other_period_and_missing(tmp_path):
    store = _store_with(tmp_path, [10])
    inner = _Inner()
    dispatcher = StoreFirstMinuteDispatcher(
        inner, store, clock=lambda: datetime(2026, 11, 20, 10, 10, 20))
    dispatcher.query_quotes_batch(["600000"], "5m", 3)
    dispatcher.query_quotes_batch(["000001"], "1m", 3)
    assert inner.calls == [
        (["600000"], "5m", 3), (["000001"], "1m", 3)]


def test_store_first_propagates_inner_error(tmp_path):
    class _Boom(_Inner):
        def query_quotes_batch(self, codes, period="1m", count=10):
            raise RuntimeError("inner down")

    dispatcher = StoreFirstMinuteDispatcher(
        _Boom(), MinuteStore(tmp_path / "minute.sqlite3"),
        clock=lambda: datetime(2026, 11, 20, 10, 10, 20))
    with pytest.raises(RuntimeError):
        dispatcher.query_quotes_batch(["600000"], "1m", 3)


# ---------------------------------------------------------------------------
# 主站池
# ---------------------------------------------------------------------------

def test_host_pool_isolation_and_latency_order():
    now = [0.0]
    pool = HostPool([("a", 1), ("b", 2)], failure_threshold=2,
                    isolation_seconds=10, clock=lambda: now[0])
    pool.report_failure("a", 1, "x")
    pool.report_failure("a", 1, "x")
    assert ("a", 1) not in pool.available()
    assert pool.candidates()[0] == ("b", 2)
    now[0] = 11.0
    assert ("a", 1) in pool.available()

    pool.report_success("a", 1, 50.0)
    pool.report_success("b", 2, 200.0)
    assert pool.candidates()[0] == ("a", 1)


def test_load_host_pool_config_override(monkeypatch):
    monkeypatch.delenv("TQ_MINUTE_HOSTS", raising=False)
    pool = load_host_pool({"hosts": ["1.2.3.4:7709"]})
    assert pool.size() == 1
    assert pool.candidates() == [("1.2.3.4", 7709)]


# ---------------------------------------------------------------------------
# 三级回落链（StoreFirst → Native → 快照合成）
# ---------------------------------------------------------------------------

_STALE_BAR = {
    "stime": "20261120095000", "open": 1.0, "high": 1.0, "low": 1.0,
    "close": 1.0, "volume": 0, "amount": 0,
}


def _snapshot_chain(now_box, *, with_sample=True):
    """构造真实的 Native(health) → 快照合成 链，返回 (native, health)。"""
    from core.engine.live.native_minute import (
        MinuteSourceHealth, NativeMinuteDispatcher,
    )
    from core.engine.live.snapshot_bars import (
        SnapshotBarSynthesizer, SnapshotQuoteDispatcher,
    )

    synth = SnapshotBarSynthesizer(
        ["600000"],
        fetch_snapshot=lambda codes: {
            "600000": {"Now": 10.5, "Volume": 1000}},
    )
    if with_sample:
        # 10:00:10 建 forming(标签 10:01)；10:01:05 收线入 completed。
        synth.sample(datetime(2026, 11, 20, 10, 0, 10))
        synth.sample(datetime(2026, 11, 20, 10, 1, 5))
    health = MinuteSourceHealth(
        stall_timeout_seconds=60, clock=lambda: now_box["now"])
    native = NativeMinuteDispatcher(
        health,
        fallback=SnapshotQuoteDispatcher(synth),
        fetch_batch=lambda codes, period, count: {"600000": [_STALE_BAR]},
        clock=lambda: now_box["now"],
    )
    return native, health


def test_chain_store_miss_native_stalled_returns_snapshot_bar(
        tmp_path, caplog):
    now_box = {"now": datetime(2026, 11, 20, 10, 5, 0)}
    native, health = _snapshot_chain(now_box)
    dispatcher = StoreFirstMinuteDispatcher(
        native, MinuteStore(tmp_path / "minute.sqlite3"),
        clock=lambda: now_box["now"])

    out = dispatcher.query_quotes_batch(["600000"], "1m", 3)

    # 原生停滞（.lc1 停在 09:50）→ 返回的必须是快照合成 bar（标签 10:01）。
    assert [b["stime"] for b in out["600000"]] == ["20261120100100"]
    assert out["600000"][0]["close"] == pytest.approx(10.5)
    # minute_health 必须反映「实际生效的是回落层」，不能误报为原生/存储正常。
    st = dispatcher.minute_health.status()
    assert st["mode"] == "fallback" and st["stalled"] is True
    assert st["reason"] == "no_advance"
    # 包装层透传的就是 inner（原生）的健康对象，没有另造一套口径。
    assert dispatcher.minute_health is native.minute_health
    assert dispatcher.status()["health"]["mode"] == "fallback"
    # 实际生效层级：存储未命中 → 全部委托 inner（本轮回落到快照合成）。
    assert dispatcher.status()["routes"] == {
        "requests": 1, "store": 0, "inner": 1}


def test_chain_store_error_falls_back_to_native_then_snapshot(
        tmp_path, caplog):
    now_box = {"now": datetime(2026, 11, 20, 10, 5, 0)}
    native, _ = _snapshot_chain(now_box)
    store = MinuteStore(tmp_path / "minute.sqlite3")

    def _boom(*_a, **_k):
        raise RuntimeError("sqlite down")

    store.recent_bars = _boom  # type: ignore[assignment]
    dispatcher = StoreFirstMinuteDispatcher(
        native, store, clock=lambda: now_box["now"])

    out = dispatcher.query_quotes_batch(["600000"], "1m", 3)
    assert out["600000"][0]["stime"] == "20261120100100"
    assert any("分钟存储读取失败" in r.getMessage() for r in caplog.records)


def test_chain_inner_fetch_error_falls_back_to_snapshot(tmp_path):
    """inner（原生）取数抛错且已停滞 → 仍返回快照合成 bar（实盘不因 7709/原生失败停摆）。"""
    now_box = {"now": datetime(2026, 11, 20, 10, 20, 0)}

    def _raise(*_a, **_k):
        raise RuntimeError("native lc1 unavailable")

    from core.engine.live.native_minute import (
        MinuteSourceHealth, NativeMinuteDispatcher,
    )
    from core.engine.live.snapshot_bars import (
        SnapshotBarSynthesizer, SnapshotQuoteDispatcher,
    )

    synth = SnapshotBarSynthesizer(
        ["600000"],
        fetch_snapshot=lambda codes: {"600000": {"Now": 10.5, "Volume": 1000}})
    synth.sample(datetime(2026, 11, 20, 10, 0, 10))
    synth.sample(datetime(2026, 11, 20, 10, 1, 5))
    health = MinuteSourceHealth(
        stall_timeout_seconds=60, clock=lambda: now_box["now"])
    health.started_at = datetime(2026, 11, 20, 10, 0, 0)  # 会话早已启动
    native = NativeMinuteDispatcher(
        health, fallback=SnapshotQuoteDispatcher(synth),
        fetch_batch=_raise, clock=lambda: now_box["now"])
    dispatcher = StoreFirstMinuteDispatcher(
        native, MinuteStore(tmp_path / "minute.sqlite3"),
        clock=lambda: now_box["now"])

    out = dispatcher.query_quotes_batch(["600000"], "1m", 3)
    assert out["600000"][0]["stime"] == "20261120100100"
    assert dispatcher.minute_health.status()["mode"] == "fallback"


def test_chain_all_three_fail_returns_empty_and_poller_does_not_raise(
        tmp_path):
    """存储空 + 原生取数抛错 + 快照无已完成 bar：整链返回空，引擎不空转报单。"""
    from core.engine.bar_poller import BarPoller
    from core.engine.live.native_minute import (
        MinuteSourceHealth, NativeMinuteDispatcher,
    )
    from core.engine.live.snapshot_bars import (
        SnapshotBarSynthesizer, SnapshotQuoteDispatcher,
    )

    now_box = {"now": datetime(2026, 11, 20, 10, 10, 0)}

    def _raise(*_a, **_k):
        raise RuntimeError("native lc1 unavailable")

    synth = SnapshotBarSynthesizer(  # 从未采样 → 无 completed bar
        ["600000"], fetch_snapshot=lambda codes: {})
    health = MinuteSourceHealth(
        stall_timeout_seconds=60, clock=lambda: now_box["now"])
    native = NativeMinuteDispatcher(
        health, fallback=SnapshotQuoteDispatcher(synth),
        fetch_batch=_raise, clock=lambda: now_box["now"])
    dispatcher = StoreFirstMinuteDispatcher(
        native, MinuteStore(tmp_path / "minute.sqlite3"),
        clock=lambda: now_box["now"])

    poller = BarPoller(dispatcher, ["600000"], period="1m", count=10)
    assert poller.poll() == []       # 无异常、无 BarEvent
    assert poller.poll() == []       # 连续空转也不报错，等数据恢复
    # 快照也无数据时返回空列表（不是 None/异常），下游按「无新 bar」处理。
    assert dispatcher.query_quotes_batch(["600000"], "1m", 3) == {
        "600000": []}


# ---------------------------------------------------------------------------
# 覆盖缺口（下载池之外 → 走 inner/快照，必须可观测）
# ---------------------------------------------------------------------------

def test_coverage_gap_reported_and_logged_once(tmp_path, caplog):
    pool = {"codes": ["600000.SH"]}
    dispatcher = StoreFirstMinuteDispatcher(
        _Inner(), MinuteStore(tmp_path / "minute.sqlite3"),
        clock=lambda: datetime(2026, 11, 20, 10, 10, 20),
        pool_codes_provider=lambda: pool["codes"])

    dispatcher.query_quotes_batch(["600000.SH", "000001.SZ"], "1m", 3)
    cov = dispatcher.coverage_status()
    assert cov["pool_known"] is True
    assert cov["requested"] == 2 and cov["in_pool"] == 1
    assert cov["outside_pool"] == 1
    assert cov["outside_codes"] == ["000001"]

    # 同一缺口集合重复请求：不再刷日志。
    dispatcher.query_quotes_batch(["600000.SH", "000001.SZ"], "1m", 3)
    hits = [r for r in caplog.records if "覆盖缺口" in r.getMessage()]
    assert len(hits) == 1


def test_coverage_silent_when_pool_unknown(tmp_path, caplog):
    dispatcher = StoreFirstMinuteDispatcher(
        _Inner(), MinuteStore(tmp_path / "minute.sqlite3"),
        clock=lambda: datetime(2026, 11, 20, 10, 10, 20),
        pool_codes_provider=lambda: None)
    dispatcher.query_quotes_batch(["600000"], "1m", 3)
    assert dispatcher.coverage_status()["pool_known"] is False
    assert not [r for r in caplog.records if "覆盖缺口" in r.getMessage()]


# ---------------------------------------------------------------------------
# 线程监督（alive / stalled / 异常退出日志 / 会话 tick 重建）
# ---------------------------------------------------------------------------

class _AliveThread:
    def is_alive(self):
        return True


def _status_service(tmp_path, now_box, interval=60.0):
    svc = MinuteDownloadService(
        MinuteStore(tmp_path / "minute.sqlite3"),
        DownloadConfig(interval_seconds=interval),
        request_fn=lambda *_a, **_k: b"",
        clock=lambda: now_box["now"],
    )
    return svc


def test_status_exposes_alive_and_stalled(tmp_path):
    now_box = {"now": datetime(2026, 11, 20, 10, 10, 0)}
    svc = _status_service(tmp_path, now_box)
    svc._started_at = datetime(2026, 11, 20, 10, 0, 0)
    svc._last_round_at = "2026-11-20 10:00:00"
    svc._thread = _AliveThread()

    st = svc.status()
    assert st["alive"] is True and st["stalled"] is True   # 600s > 3×60s
    assert st["last_round_age_seconds"] == 600

    svc._last_round_at = "2026-11-20 10:09:30"
    st = svc.status()
    assert st["alive"] is True and st["stalled"] is False

    svc._thread = None
    st = svc.status()
    assert st["alive"] is False and st["stalled"] is False
    assert svc.pool_codes == []


def test_thread_abnormal_exit_logs_error(tmp_path, caplog):
    now_box = {"now": datetime(2026, 11, 20, 10, 10, 0)}
    svc = _status_service(tmp_path, now_box, interval=0.0)
    svc.run_once = lambda: {}  # type: ignore[assignment]

    def _die(_seconds):
        raise RuntimeError("boom")

    svc._wait = _die  # type: ignore[assignment]
    with caplog.at_level(logging.ERROR):
        assert svc.start() is True
        svc._thread.join(2.0)
    st = svc.status()
    assert st["alive"] is False
    assert "boom" in (st["thread_error"] or "")
    assert any("异常终止" in r.getMessage() for r in caplog.records)


def test_supervise_rebuilds_and_caps_attempts(monkeypatch):
    import core.services.live_service as ls

    class _FakeService:
        def __init__(self):
            self.starts = 0
            self.running = False

        def start(self):
            self.starts += 1
            return True

        def status(self):
            return {"alive": self.running}

    svc = _FakeService()
    monkeypatch.setitem(ls.MINUTE_DOWNLOADERS, 7, svc)
    monkeypatch.setitem(
        ls._MINUTE_SUPERVISION, 7, {"attempts": [], "gave_up": False})
    now = [1000.0]
    monkeypatch.setattr(ls.time, "monotonic", lambda: now[0])

    for _ in range(4):
        ls.supervise_minute_download(7)

    assert svc.starts == 3          # 10 分钟内最多重建 3 次
    assert ls._MINUTE_SUPERVISION[7]["gave_up"] is True

    st = ls._minute_download_status(7)
    assert st["enabled"] is True
    assert st["supervision"]["gave_up"] is True
    assert st["supervision"]["max_attempts"] == 3


def test_supervise_noop_when_thread_alive(monkeypatch):
    import core.services.live_service as ls

    class _FakeService:
        running = True

        def __init__(self):
            self.starts = 0

        def start(self):
            self.starts += 1
            return True

    svc = _FakeService()
    monkeypatch.setitem(ls.MINUTE_DOWNLOADERS, 8, svc)
    monkeypatch.setitem(
        ls._MINUTE_SUPERVISION, 8, {"attempts": [], "gave_up": False})
    ls.supervise_minute_download(8)
    assert svc.starts == 0
