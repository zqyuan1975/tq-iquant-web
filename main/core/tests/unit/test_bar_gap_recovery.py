"""P2-6：桥离线恢复的 bar 缺口补派 / 缺口审计。

覆盖三层：
1. BarPoller 层：支持历史回补的行情源（桥 /quote）在恢复首轮按放大 count 取回
   缺口 bar，且只触发「水位之后」的 bar（幂等：同一批 bar 二次 poll 不再触发）；
   快照合成源（supports_historical_quote=False）不放大窗口。
2. 引擎处置计划：同日 + 窗口内 → replay；跨日 / 窗口外 → skip；快照源 →
   unrecoverable（客观取不到）。
3. 审计事件：补齐 → action=replay；只补到部分 → action=partial，带缺口首末。
"""
from datetime import datetime, timedelta
from types import SimpleNamespace

from core.engine.bar_poller import BarPoller
from core.engine.live_engine import LiveEngine, _expected_minute_labels


def _bar(ts: datetime, close: float = 10.0) -> dict:
    return {
        "stime": ts.strftime("%Y%m%d%H%M%S"),
        "open": close, "high": close, "low": close,
        "close": close, "volume": 100,
    }


class _HistDispatcher:
    """模拟 QMT/掘金桥 /quote：count 窗口内的历史 bar 可取。"""

    supports_historical_quote = True

    def __init__(self, bars):
        self.bars = list(bars)
        self.counts = []

    def query_quote(self, code, period="1m", count=10):
        self.counts.append(count)
        return self.bars[-count:]


class _SnapshotDispatcher:
    """模拟快照合成源：窗口固定，不能按 count 回补。"""

    supports_historical_quote = False

    def __init__(self, bars):
        self.bars = list(bars)
        self.counts = []

    def query_quote(self, code, period="1m", count=10):
        self.counts.append(count)
        return self.bars[-10:]


def _poller(dispatcher):
    p = BarPoller(dispatcher, ["600000.SH"], period="1m", count=10)
    got = []
    p.on_bar = got.append
    return p, got


def test_backfill_replays_gap_bars_once_and_is_idempotent():
    """断线 10 分钟：恢复首轮补派全部缺口 bar；再 poll 不重复触发。"""
    base = [datetime(2026, 8, 25, 9, 41) + timedelta(minutes=i)
            for i in range(11)]          # 09:41–09:51（09:51 为进行中）
    disp = _HistDispatcher([_bar(t) for t in base])
    p, got = _poller(disp)
    assert p.poll() == []                 # 首轮只建基线
    assert p.last_completed_stime == datetime(2026, 8, 25, 9, 50)

    # 离线期间漏掉 09:51–10:00，恢复时数据已到 10:01（10:01 进行中）
    gap = [datetime(2026, 8, 25, 9, 51) + timedelta(minutes=i)
           for i in range(11)]          # 09:51–10:01
    disp.bars.extend(_bar(t) for t in gap)

    triggered = p.poll(backfill_count=20)
    times = [b.bar_time for b in triggered]
    assert times == [datetime(2026, 8, 25, 9, 51) + timedelta(minutes=i)
                     for i in range(10)]
    assert disp.counts[-1] == 20          # 恢复首轮确实放大了拉取根数

    # 同一批数据再 poll：幂等，不重复触发
    assert p.poll(backfill_count=20) == []
    assert p.last_completed_stime == datetime(2026, 8, 25, 10, 0)


def test_backfill_window_not_widened_for_snapshot_source():
    """快照合成源固定窗口：count_override 不上浮（错过的分钟客观不存在）。"""
    bars = [_bar(datetime(2026, 8, 25, 9, 30 + i)) for i in range(10)]
    disp = _SnapshotDispatcher(bars)
    p, _ = _poller(disp)
    p.poll()
    p.poll(backfill_count=240)
    assert disp.counts[-1] == 10


def test_plan_gap_recovery_replay_same_day(monkeypatch):
    poller = SimpleNamespace(
        last_completed_stime=datetime(2026, 8, 25, 9, 55),
        supports_backfill=True,
    )
    stub = SimpleNamespace(_bar_poller=poller, _offline_since=None,
                           _quote_window_open=lambda now: True)
    plan = LiveEngine._plan_gap_recovery(stub, datetime(2026, 8, 25, 10, 25))
    assert plan["action"] == "replay"
    assert plan["gap_start"] == datetime(2026, 8, 25, 9, 55)
    # 排除正在形成的 10:25：gap_end = (now − 1s) 截到分钟 = 最近一根已闭合 bar
    assert plan["gap_end"] == datetime(2026, 8, 25, 10, 24)
    assert plan["count"] == 35            # 30 分钟缺口 +5 余量


def test_plan_gap_recovery_cross_day_skips():
    poller = SimpleNamespace(
        last_completed_stime=datetime(2026, 8, 24, 14, 59),
        supports_backfill=True,
    )
    stub = SimpleNamespace(_bar_poller=poller, _offline_since=None,
                           _quote_window_open=lambda now: True)
    plan = LiveEngine._plan_gap_recovery(stub, datetime(2026, 8, 25, 9, 40))
    assert plan["action"] == "skip"
    assert plan["reason"] == "cross_day"


def test_plan_gap_recovery_snapshot_source_unrecoverable():
    poller = SimpleNamespace(
        last_completed_stime=datetime(2026, 8, 25, 10, 0),
        supports_backfill=False,
    )
    stub = SimpleNamespace(_bar_poller=poller, _offline_since=None,
                           _quote_window_open=lambda now: True)
    plan = LiveEngine._plan_gap_recovery(stub, datetime(2026, 8, 25, 10, 3))
    assert plan["action"] == "unrecoverable"
    assert plan["reason"] == "snapshot_source"


def test_expected_minute_labels_skips_lunch_break():
    labels = _expected_minute_labels(
        datetime(2026, 8, 25, 11, 25), datetime(2026, 8, 25, 13, 5))
    assert [t.strftime("%H:%M") for t in labels] == [
        "11:26", "11:27", "11:28", "11:29", "11:30",
        "13:01", "13:02", "13:03", "13:04", "13:05",
    ]


def test_expected_minute_labels_tdx_form_ends_at_1129():
    """TDX 原生/快照源：上午最后一根是 11:29，11:30 不计为应有根。"""
    labels = _expected_minute_labels(
        datetime(2026, 8, 25, 11, 25), datetime(2026, 8, 25, 11, 29),
        has_1130_bar=False)
    assert [t.strftime("%H:%M") for t in labels] == [
        "11:26", "11:27", "11:28", "11:29",
    ]


def test_expected_minute_labels_bridge_form_keeps_1130():
    """QMT/掘金桥源：上午确有 11:30 这根，缺它仍是真缺口。"""
    labels = _expected_minute_labels(
        datetime(2026, 8, 25, 11, 25), datetime(2026, 8, 25, 11, 30),
        has_1130_bar=True)
    assert [t.strftime("%H:%M") for t in labels] == [
        "11:26", "11:27", "11:28", "11:29", "11:30",
    ]


def _audit_stub(plan):
    events = []
    stub = LiveEngine.__new__(LiveEngine)
    stub._gap_recovery_plan = plan
    stub.ctx = SimpleNamespace(session_id=7)
    stub._engine_key = "iquant"
    stub._emit = lambda kind, payload: events.append((kind, payload))
    return stub, events


def test_audit_tdx_form_no_missing_at_1129_close():
    """快照/原生源：补到 11:29 即完整，不报 11:30 缺失。"""
    plan = {"gap_start": datetime(2026, 8, 25, 11, 20),
            "gap_end": datetime(2026, 8, 25, 11, 29),
            "reason": "snapshot_source", "has_1130_bar": False}
    stub, events = _audit_stub(plan)
    replayed = [SimpleNamespace(bar_time=datetime(2026, 8, 25, 11, 20)
                                + timedelta(minutes=i))
                for i in range(1, 10)]        # 11:21–11:29
    LiveEngine._audit_gap_recovery(
        stub, datetime(2026, 8, 25, 11, 29), replayed)

    _, payload = events[-1]
    assert payload["action"] == "replay"
    assert payload["gap_bars"] == 9
    assert payload["missing_bars"] == 0


def test_audit_bridge_form_reports_missing_1130():
    """桥源：同样只补到 11:29 → 11:30 缺失，action=partial。"""
    plan = {"gap_start": datetime(2026, 8, 25, 11, 20),
            "gap_end": datetime(2026, 8, 25, 11, 30),
            "reason": "bridge_recovered", "has_1130_bar": True}
    stub, events = _audit_stub(plan)
    replayed = [SimpleNamespace(bar_time=datetime(2026, 8, 25, 11, 20)
                                + timedelta(minutes=i))
                for i in range(1, 10)]        # 11:21–11:29
    LiveEngine._audit_gap_recovery(
        stub, datetime(2026, 8, 25, 11, 30), replayed)

    _, payload = events[-1]
    assert payload["action"] == "partial"
    assert payload["gap_bars"] == 10
    assert payload["missing_bars"] == 1
    assert payload["missing_first"] == "2026-08-25T11:30:00"


def test_audit_partial_emits_missing_bounds():
    """补派只覆盖部分（桥 count 上限截断）→ action=partial + 缺口首末。"""
    events = []
    stub = LiveEngine.__new__(LiveEngine)
    stub._gap_recovery_plan = {
        "gap_start": datetime(2026, 8, 25, 9, 55),
        "gap_end": datetime(2026, 8, 25, 10, 5),
        "reason": "bridge_recovered",
    }
    stub.ctx = SimpleNamespace(session_id=7)
    stub._engine_key = "iquant"
    stub._emit = lambda kind, payload: events.append((kind, payload))
    replayed = [SimpleNamespace(bar_time=datetime(2026, 8, 25, 10, 0)
                                + timedelta(minutes=i))
                for i in range(6)]        # 只补回 10:00–10:05
    LiveEngine._audit_gap_recovery(stub, datetime(2026, 8, 25, 10, 5), replayed)

    kind, payload = events[-1]
    assert kind == "risk"
    assert payload["gate"] == "bar_gap"
    assert payload["action"] == "partial"
    assert payload["gap_bars"] == 10
    assert payload["replayed_bars"] == 6
    assert payload["missing_bars"] == 4
    assert payload["missing_first"] == "2026-08-25T09:56:00"
    assert payload["missing_last"] == "2026-08-25T09:59:00"
