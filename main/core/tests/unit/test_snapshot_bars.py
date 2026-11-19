"""SnapshotBarSynthesizer 单元测试。

TQ 盘中 K 线缓存只到上一交易日（实测+手册 refresh_kline 限制），
实盘 bar 流改由 get_market_snapshot_batch 实时快照按分钟聚合合成。
"""
from datetime import datetime, timedelta

import pytest

from core.engine.bar_poller import BarPoller
from core.engine.live.snapshot_bars import (
    SnapshotBarSynthesizer,
    SnapshotQuoteDispatcher,
    default_in_session,
)


D1 = "20261103"


def _fetcher(snaps):
    """snaps: {code: {"Now": "10.0", "Volume": "100"}} → fetch_snapshot 闭包。"""
    def fetch(codes):
        return {c: dict(snaps[c]) for c in codes if c in snaps}
    return fetch


def _mk(snaps, codes=("000001.SZ",), gate=None):
    s = SnapshotBarSynthesizer(
        list(codes),
        fetch_snapshot=_fetcher(snaps),
        in_session=gate or (lambda dt: True),
    )
    s._test_snaps = snaps      # 测试钩子：直接改快照表模拟下一次采样
    return s


# --- 合成：分钟边界与 OHLCV ---

def test_first_sample_only_forming_no_completed():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}})
    s.sample(datetime(2026, 11, 3, 10, 50, 30))
    assert s.completed_bars(["000001.SZ"]) == {"000001.SZ": []}


def test_minute_rollover_completes_bar_ohlc_volume_delta():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}})
    s.sample(datetime(2026, 11, 3, 10, 50, 30))          # 基线采样
    s._test_snaps["000001.SZ"] = {"Now": "10.5", "Volume": "1100"}
    s.sample(datetime(2026, 11, 3, 10, 50, 40))
    s._test_snaps["000001.SZ"] = {"Now": "9.9", "Volume": "1200"}
    s.sample(datetime(2026, 11, 3, 10, 50, 58))
    s._test_snaps["000001.SZ"] = {"Now": "10.2", "Volume": "1600"}
    s.sample(datetime(2026, 11, 3, 10, 51, 10))          # 跨分钟 → 10:50 bar 完成

    bars = s.completed_bars(["000001.SZ"])["000001.SZ"]
    assert len(bars) == 1
    b = bars[0]
    # stime = 分钟结束时刻（10:50 分钟 → 10:51:00）
    assert b["stime"] == D1 + "105100"
    assert b["open"] == pytest.approx(10.0)
    assert b["high"] == pytest.approx(10.5)
    assert b["low"] == pytest.approx(9.9)
    assert b["close"] == pytest.approx(9.9)     # 分钟内末采样
    assert b["volume"] == 20000                 # 200 手 → 出口统一为股（1200-1000）


def test_open_at_reads_forming_and_completed_by_minute_end_label():
    """B 执行价：open_at(start) 取 start+1min 标签 bar 的 open（forming/completed 均可）。"""
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}})
    s.sample(datetime(2026, 11, 3, 14, 52, 5))
    # 14:52 分钟 bar 仍在 forming（label=14:53:00）→ 可取 open
    assert s.open_at(["000001.SZ"], datetime(2026, 11, 3, 14, 52)) == {
        "000001.SZ": pytest.approx(10.0)}
    # 14:51 起始 bar 从未采样 → 取不到（调用方 fail-closed）
    assert s.open_at(["000001.SZ"], datetime(2026, 11, 3, 14, 51)) == {}
    # 跨到 14:53 后该 bar 移入 completed，open 不变
    s.sample(datetime(2026, 11, 3, 14, 53, 1))
    assert s.open_at(["000001.SZ"], datetime(2026, 11, 3, 14, 52)) == {
        "000001.SZ": pytest.approx(10.0)}


def test_zero_volume_minute_still_produces_bar():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}})
    s.sample(datetime(2026, 11, 3, 10, 50, 30))
    s.sample(datetime(2026, 11, 3, 10, 51, 10))   # 无成交：Volume 不变
    bars = s.completed_bars(["000001.SZ"])["000001.SZ"]
    assert len(bars) == 1
    assert bars[0]["volume"] == 0
    assert bars[0]["close"] == pytest.approx(10.0)


def test_volume_decrease_clamped_to_zero():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}})
    s.sample(datetime(2026, 11, 3, 10, 50, 30))
    s._test_snaps["000001.SZ"] = {"Now": "10.1", "Volume": "900"}   # 异常回退
    s.sample(datetime(2026, 11, 3, 10, 51, 10))
    assert s.completed_bars(["000001.SZ"])["000001.SZ"][0]["volume"] == 0


def test_volume_none_midway_does_not_break_other_codes():
    """P1（2026-11-09）：单只 Volume 由数值变 None 不得抛 TypeError 中断整轮采样。

    旧实现 int(None - last) 抛错 → for 循环中断，其后股票本分钟全部丢样本
    （极端情况整日无完成 bar → deferred 永远不放行）。
    """
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"},
             "600000.SH": {"Now": "20.0", "Volume": "2000"}},
            codes=("000001.SZ", "600000.SH"))
    s.sample(datetime(2026, 11, 3, 10, 50, 30))

    s._test_snaps["000001.SZ"] = {"Now": "10.1", "Volume": None}
    s._test_snaps["600000.SH"] = {"Now": "20.5", "Volume": "2300"}
    s.sample(datetime(2026, 11, 3, 10, 50, 40))          # 不得抛异常
    s.sample(datetime(2026, 11, 3, 10, 51, 10))          # 跨分钟 → 两根都完成

    bars = s.completed_bars(["000001.SZ", "600000.SH"])
    assert len(bars["600000.SH"]) == 1                   # 其它股票照常采样
    assert bars["600000.SH"][0]["volume"] == 30000
    assert bars["600000.SH"][0]["close"] == pytest.approx(20.5)
    assert len(bars["000001.SZ"]) == 1                   # 量缺失只 clamp 到 0
    assert bars["000001.SZ"][0]["volume"] == 0
    assert bars["000001.SZ"][0]["close"] == pytest.approx(10.1)


def test_fetch_failure_keeps_state_and_rollover():
    snaps = {"000001.SZ": {"Now": "10.0", "Volume": "1000"}}
    calls = {"n": 0}

    def flaky(codes):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("snapshot boom")
        return {c: dict(snaps[c]) for c in codes}

    s = SnapshotBarSynthesizer(["000001.SZ"], fetch_snapshot=flaky,
                               in_session=lambda dt: True)
    s.sample(datetime(2026, 11, 3, 10, 50, 30))
    s.sample(datetime(2026, 11, 3, 10, 51, 5))    # fetch 失败 → 仅滚动完成
    bars = s.completed_bars(["000001.SZ"])["000001.SZ"]
    assert len(bars) == 1 and bars[0]["close"] == pytest.approx(10.0)

    snaps["000001.SZ"] = {"Now": "10.2", "Volume": "1600"}
    s.sample(datetime(2026, 11, 3, 10, 51, 30))
    s.sample(datetime(2026, 11, 3, 10, 52, 10))   # 10:51 bar 完成
    bars = s.completed_bars(["000001.SZ"])["000001.SZ"]
    assert bars[-1]["stime"] == D1 + "105200"
    assert bars[-1]["volume"] == 60000            # 600 手（1600-1000，故障未破坏基线）


def test_multi_minute_accumulates_and_count_window():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "0"}})
    vols = [0, 600, 1300, 1900]
    for i, v in enumerate(vols):
        s._test_snaps["000001.SZ"] = {"Now": str(10.0 + i), "Volume": str(v)}
        s.sample(datetime(2026, 11, 3, 10, 50 + i, 30))
    # 完成 bar：105100(vol0) 105200(600) 105300(700)；count=2 取末两根
    bars = s.completed_bars(["000001.SZ"], count=2)["000001.SZ"]
    assert [b["stime"][-6:] for b in bars] == ["105200", "105300"]
    assert bars[0]["volume"] == 60000 and bars[1]["volume"] == 70000


def test_completed_bars_filters_unknown_codes():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}})
    s.sample(datetime(2026, 11, 3, 10, 50, 30))
    s.sample(datetime(2026, 11, 3, 10, 51, 10))
    assert s.completed_bars(["999999.SZ"]) == {"999999.SZ": []}


def test_cross_day_resets_volume_baseline():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "500000"}})
    s.sample(datetime(2026, 11, 3, 14, 59, 50))
    s._test_snaps["000001.SZ"] = {"Now": "10.1", "Volume": "300"}
    s.sample(datetime(2026, 11, 4, 9, 30, 10))
    bars = s.completed_bars(["000001.SZ"])["000001.SZ"]
    # 昨日 14:59 bar 完成（stime=昨日 15:00）
    assert bars[0]["stime"] == "20261103150000"
    s._test_snaps["000001.SZ"] = {"Now": "10.2", "Volume": "500"}
    s.sample(datetime(2026, 11, 4, 9, 31, 10))    # 200 记入 09:31 forming
    s.sample(datetime(2026, 11, 4, 9, 32, 10))    # 滚动完成 09:31 bar
    bars = s.completed_bars(["000001.SZ"])["000001.SZ"]
    assert bars[-1]["stime"] == "20261104093200"
    assert bars[-1]["volume"] == 20000            # 新交易日差分基线已重置（200 手）


def test_reset_clears_state():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}})
    s.sample(datetime(2026, 11, 3, 10, 50, 30))
    s.sample(datetime(2026, 11, 3, 10, 51, 10))
    assert len(s.completed_bars(["000001.SZ"])["000001.SZ"]) == 1
    s.reset()
    assert s.completed_bars(["000001.SZ"]) == {"000001.SZ": []}
    s.sample(datetime(2026, 11, 3, 10, 52, 10))   # 重置后首采样仅建基线
    assert s.completed_bars(["000001.SZ"]) == {"000001.SZ": []}


# --- 交易时段门控 ---

@pytest.mark.parametrize("dt,expected", [
    (datetime(2026, 11, 3, 9, 15), False),    # 集合竞价前
    (datetime(2026, 11, 3, 9, 30), True),
    (datetime(2026, 11, 3, 10, 0), True),
    (datetime(2026, 11, 3, 11, 29, 59), True),
    (datetime(2026, 11, 3, 11, 30), False),   # 午休
    (datetime(2026, 11, 3, 12, 30), False),
    (datetime(2026, 11, 3, 13, 0), True),
    (datetime(2026, 11, 3, 14, 59, 59), True),
    (datetime(2026, 11, 3, 15, 0), False),
    (datetime(2026, 11, 7, 10, 0), False),    # 周六
])
def test_default_in_session(dt, expected):
    assert default_in_session(dt) is expected


def test_out_of_session_sample_ignored_but_rollover_completes():
    def gate(dt):
        # 午休关闭（模拟默认门控拦截采样数据）
        return not (dt.hour == 12)

    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}}, gate=gate)
    s.sample(datetime(2026, 11, 3, 11, 29, 50))   # 盘中 → 建 forming
    s._test_snaps["000001.SZ"] = {"Now": "10.9", "Volume": "9999"}
    s.sample(datetime(2026, 11, 3, 12, 0, 0))     # 午休 → 样本忽略，但 11:29 bar 完成
    bars = s.completed_bars(["000001.SZ"])["000001.SZ"]
    assert len(bars) == 1
    assert bars[0]["stime"] == D1 + "113000"
    assert bars[0]["close"] == pytest.approx(10.0)   # 12:00 的 10.9 未混入


# --- BarPoller 适配器 ---

def test_adapter_batch_and_single_and_period_guard():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "1000"}})
    s.sample(datetime(2026, 11, 3, 10, 50, 30))
    s.sample(datetime(2026, 11, 3, 10, 51, 10))
    ada = SnapshotQuoteDispatcher(s)
    assert ada.supports_batch_quote is True
    # 显式闭合声明：completed_bars 只吐已闭合 bar，BarPoller 可立即处理最新一根
    assert ada.bars_explicitly_completed is True
    out = ada.query_quotes_batch(["000001.SZ", "999999.SZ"], "1m", 10)
    assert set(out) == {"000001.SZ", "999999.SZ"}
    assert len(out["000001.SZ"]) == 1
    assert out["999999.SZ"] == []
    # 5m 现由 1m 聚合服务（桶未满 → 该 code 空列表，而非整体空 dict）
    assert ada.query_quotes_batch(["000001.SZ"], "5m", 10) == {"000001.SZ": []}
    assert ada.query_quotes_batch(["000001.SZ"], "2m", 10) == {}
    one = ada.query_quote("000001.SZ", period="1m", count=10)
    assert len(one) == 1 and one[0]["stime"] == D1 + "105100"


def test_bar_poller_triggers_synthesized_bar_end_to_end():
    """闭合声明下：分钟一结束（合成器 rollover）即触发，不再白等下一根。

    原用例断言「10:50 bar 已 rollover 但仍是最新 → 不触发，要等 10:52 出现」，
    这正是本轮要消除的 ~60s 延迟（把「已闭合」误当「进行中」）。合成器只在
    采样时刻 >= 分钟终点才 rollover，且完成列表永不含 forming，因此立即处理
    最新一根仍是「只用已闭合数据」。
    """
    snaps = {"000001.SZ": {"Now": "10.0", "Volume": "1000"}}
    s = SnapshotBarSynthesizer(["000001.SZ"], fetch_snapshot=_fetcher(snaps),
                               in_session=lambda dt: True)
    s._test_snaps = snaps
    poller = BarPoller(SnapshotQuoteDispatcher(s), ["000001.SZ"],
                       period="1m", count=10)

    s.sample(datetime(2026, 11, 3, 10, 50, 30))
    assert poller.poll() == []                    # 无闭合 bar：未建基线

    s._test_snaps["000001.SZ"] = {"Now": "10.2", "Volume": "1600"}
    s.sample(datetime(2026, 11, 3, 10, 51, 10))   # 10:51 label 闭合（close 10.0）
    assert poller.poll() == []                    # 首根闭合 bar 建基线，不回放

    s._test_snaps["000001.SZ"] = {"Now": "10.3", "Volume": "2000"}
    s.sample(datetime(2026, 11, 3, 10, 52, 10))   # 10:52 label 闭合（close 10.2）
    events = poller.poll()
    assert len(events) == 1                       # 本轮立即触发，不再等下一根
    ev = events[0]
    assert ev.bar_time.strftime("%Y%m%d%H%M%S") == D1 + "105200"
    assert float(ev.stocks["000001.SZ"]["close"]) == pytest.approx(10.2)

    assert poller.poll() == []                    # 窗口重复 → 不重复触发


# --- 5m/15m/30m/1h 聚合（TQ 原生分钟线过期时的兜底）---

def _inject_completed(s, code, start, n, base=10.0):
    """直接注入已完成 1m bar（label=分钟终点），便于构造多分钟桶。"""
    bars = [{
        "label": start + timedelta(minutes=i),
        "open": base + i * 0.1,
        "high": base + 0.2 + i * 0.1,
        "low": base - 0.1 + i * 0.1,
        "close": base + 0.1 + i * 0.1,
        "volume": 100 + i,
    } for i in range(n)]
    with s._lock:
        s._completed[code] = bars
    return bars


def test_aggregated_bars_5m_ohlcv_and_source_marker():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "0"}})
    _inject_completed(s, "000001.SZ", datetime(2026, 11, 3, 9, 31), 5)
    bars = s.aggregated_bars(["000001.SZ"], period="5m", count=10)["000001.SZ"]
    assert len(bars) == 1
    b = bars[0]
    assert b["stime"] == D1 + "093500"           # 桶终点（09:30–09:35）
    assert b["open"] == pytest.approx(10.0)      # 首根 open
    assert b["close"] == pytest.approx(10.5)     # 末根 close
    assert b["high"] == pytest.approx(10.6)      # 极值
    assert b["low"] == pytest.approx(9.9)
    assert b["volume"] == 51000                  # 累加 510 手 → 出口统一为股
    assert b["source"] == "aggregated_1m"
    assert b["period"] == "5m"


def test_aggregated_bars_skips_incomplete_bucket():
    """不足一个完整桶（4 根）不输出——绝不用残缺数据冒充周期 bar。"""
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "0"}})
    _inject_completed(s, "000001.SZ", datetime(2026, 11, 3, 9, 31), 4)
    assert s.aggregated_bars(["000001.SZ"], period="5m")["000001.SZ"] == []


def test_aggregated_bars_drops_bucket_with_middle_gap():
    """中间缺一分钟（标签不连续）整桶丢弃。"""
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "0"}})
    bars = _inject_completed(s, "000001.SZ", datetime(2026, 11, 3, 9, 31), 5)
    with s._lock:
        s._completed["000001.SZ"] = [b for b in bars if b["label"].minute != 33]
    assert s.aggregated_bars(["000001.SZ"], period="5m")["000001.SZ"] == []


def test_aggregated_bars_1h_from_real_sampling_pipeline():
    """真实采样 61 分钟 → 1h 桶（09:31–10:30）可聚合（需保留 ≥60 根 1m）。"""
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "0"}})
    base = datetime(2026, 11, 3, 9, 30, 30)
    for i in range(61):
        s.sample(base + timedelta(minutes=i))
    bars = s.aggregated_bars(["000001.SZ"], period="1h")["000001.SZ"]
    assert len(bars) == 1
    assert bars[0]["stime"] == D1 + "103000"
    assert bars[0]["source"] == "aggregated_1m"


def test_aggregated_bars_afternoon_session_alignment():
    """下午从 13:00 起算：13:01–13:05 归 13:05 桶，不与上午连桶。"""
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "0"}})
    _inject_completed(s, "000001.SZ", datetime(2026, 11, 3, 13, 1), 5)
    bars = s.aggregated_bars(["000001.SZ"], period="5m")["000001.SZ"]
    assert len(bars) == 1 and bars[0]["stime"] == D1 + "130500"


def test_aggregated_bars_unsupported_or_unknown_codes():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "0"}})
    assert s.aggregated_bars(["000001.SZ"], period="2m") == {}
    assert s.aggregated_bars(["999999.SZ"], period="5m") == {"999999.SZ": []}


def test_adapter_serves_aggregated_periods_and_exposes_synthesizer():
    s = _mk({"000001.SZ": {"Now": "10.0", "Volume": "0"}})
    _inject_completed(s, "000001.SZ", datetime(2026, 11, 3, 9, 31), 5)
    ada = SnapshotQuoteDispatcher(s)
    assert ada.synthesizer is s
    out = ada.query_quotes_batch(["000001.SZ"], "5m", 10)
    assert out["000001.SZ"][0]["stime"] == D1 + "093500"
    assert out["000001.SZ"][0]["source"] == "aggregated_1m"
