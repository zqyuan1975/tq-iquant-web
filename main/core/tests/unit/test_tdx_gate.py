"""TQ 通道闸（core/tq/gate.py）：分级抢占、时间窗、配额、分片。

这些用例不连真实通达信、不启任何进程：只用进程内线程 + 注入时钟模拟。
"""
import threading
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from core.tq import gate as g
from core.tq.utils import TDXLockTimeout, get_tdx_lock

_TZ = timezone(timedelta(hours=8))
_BASE_CFG = {
    "enabled": True,
    "pre_market_p2": ["08:00", "09:10"],
    "lunch_p2": ["11:30", "13:00"],
    "after_close_p2": ["15:15", "24:00"],
    "trading": [["09:10", "11:30"], ["13:00", "15:10"]],
    "p0_only": ["14:55", "15:10"],
    "p1_max_per_minute": 6000,   # 配额放大，避免用例受令牌桶抖动影响
    "p2_max_per_minute": 6000,
    "p2_chunk_size": 60,
    "override_max_minutes": 120,
}


def _at(hh: int, mm: int, day: int = 23) -> datetime:
    """2026-11-23 是周一（交易日）。"""
    return datetime(2026, 11, day, hh, mm, tzinfo=_TZ)


@pytest.fixture(autouse=True)
def _gated(monkeypatch):
    monkeypatch.delenv("TQ_GATE_FORCE_ALLOW", raising=False)
    g.reset_for_tests(config=dict(_BASE_CFG),
                      clock=lambda: _at(10, 0),
                      trading_day_fn=lambda d: d.weekday() < 5,
                      live_running_fn=lambda: True)
    g.clear_override()
    g.GATE.reset_state()
    yield
    g.clear_override()
    g.reset_for_tests(config=None, clock=None, trading_day_fn=None,
                      live_running_fn=None)
    g.GATE.reset_state()


def _use_clock(dt: datetime, live_running: bool = True):
    """注入时钟；默认假定有实盘在跑（时段限制生效）。"""
    g.reset_for_tests(config=dict(_BASE_CFG), clock=lambda: dt,
                      trading_day_fn=lambda d: d.weekday() < 5,
                      live_running_fn=lambda: live_running)


# ------------------------------------------------------------------ 时间窗

def test_trading_window_blocks_p2_immediately_with_next_open():
    _use_clock(_at(10, 0))
    t0 = time.monotonic()
    with pytest.raises(g.TdxTaskBlocked) as ei:
        g.GATE.acquire(g.P2, timeout=30)
    elapsed = time.monotonic() - t0
    assert elapsed < 0.2, "交易时段 P2 必须立即拒绝，不得转圈等 30s"
    assert ei.value.reason == "trading_window"
    # 10:00 之后的第一个 P2 放开点是午休 11:30
    assert ei.value.next_available_at == _at(11, 30)
    assert "11:30" in str(ei.value)
    # 被拒绝时不占用通道
    assert g.GATE.locked() is False


def test_trading_window_allows_p1_and_p0():
    _use_clock(_at(10, 0))
    assert g.GATE.acquire(g.P1, timeout=1)
    g.GATE.release()
    assert g.GATE.acquire(g.P0, timeout=1)
    g.GATE.release()


def test_protected_window_blocks_p1_and_p2_but_not_p0():
    _use_clock(_at(15, 0))   # 14:55–15:10 保护窗口内（15:05 收盘轮次）
    with pytest.raises(g.TdxTaskBlocked) as ei1:
        g.GATE.acquire(g.P1, timeout=1)
    assert ei1.value.reason == "protected_window"
    with pytest.raises(g.TdxTaskBlocked):
        g.GATE.acquire(g.P2, timeout=1)
    assert g.GATE.acquire(g.P0, timeout=1)
    g.GATE.release()


def test_protected_window_allows_p3_readonly_fallback():
    """保护窗内 P3 是唯一新增放行级：供分钟合成只读兜底使用。"""
    _use_clock(_at(15, 0))
    policy = g.evaluate_policy()
    assert policy["protected"] is True
    assert policy["p3_allowed"] is True
    assert g.GATE.acquire(g.P3, timeout=1)
    g.GATE.release()
    # P1/P2 的收紧不受影响。
    with pytest.raises(g.TdxTaskBlocked):
        g.GATE.acquire(g.P1, timeout=1)
    with pytest.raises(g.TdxTaskBlocked):
        g.GATE.acquire(g.P2, timeout=1)


def test_p3_blocked_outside_protected_window():
    """非保护窗有 P1 正路可走：P3 立即拒绝，避免被其它任务借用。"""
    _use_clock(_at(10, 0))
    assert g.evaluate_policy()["p3_allowed"] is False
    with pytest.raises(g.TdxTaskBlocked) as ei:
        g.GATE.acquire(g.P3, timeout=1)
    assert ei.value.reason == "p3_window_only"


def test_p3_yields_to_waiting_p0():
    """P3 最低优先级：P0 等待时 P3 不得插队，必须让 P0 先拿到通道。"""
    _use_clock(_at(15, 0))
    assert g.GATE.acquire(g.P3, timeout=1)
    p0_result = {}
    p3_result = {}

    def p0_worker():
        p0_result["ok"] = g.GATE.acquire(g.P0, timeout=2)
        time.sleep(0.1)
        g.GATE.release()

    def p3_worker():
        p3_result["ok"] = g.GATE.acquire(g.P3, timeout=0.2)

    t0 = threading.Thread(target=p0_worker)
    t0.start()
    time.sleep(0.05)   # 确保 P0 已在等待
    t3 = threading.Thread(target=p3_worker)
    t3.start()
    t3.join(2)
    assert p3_result.get("ok") is False, "P3 不得插队 P0"
    g.GATE.release()
    t0.join(2)
    assert p0_result.get("ok") is True


def test_parse_priority_accepts_p3():
    assert g.parse_priority("P3") == g.P3
    assert g.parse_priority(3) == g.P3
    assert g.parse_priority("p3") == g.P3


def test_lunch_break_opens_p2():
    _use_clock(_at(12, 0))
    assert g.GATE.acquire(g.P2, timeout=1)
    g.GATE.release()


def test_after_close_opens_p2_at_1515():
    _use_clock(_at(15, 14))
    with pytest.raises(g.TdxTaskBlocked):
        g.GATE.acquire(g.P2, timeout=1)
    _use_clock(_at(15, 15))
    assert g.GATE.acquire(g.P2, timeout=1)
    g.GATE.release()


def test_non_trading_day_opens_everything():
    _use_clock(_at(10, 0, day=21))   # 2026-11-21 周六
    policy = g.evaluate_policy()
    assert policy["trading_day"] is False
    assert policy["window"] == "non_trading_day"
    assert g.GATE.acquire(g.P2, timeout=1)
    g.GATE.release()


def test_weekend_builtin_trading_day_rule():
    # 不注入 trading_day_fn：走内置休市表 + 周末硬规则，fail-open
    g.reset_for_tests(config=dict(_BASE_CFG),
                      clock=lambda: _at(10, 0, day=21))
    assert g.evaluate_policy()["trading_day"] is False


def test_gate_disabled_falls_back_to_plain_mutex():
    g.reset_for_tests(config=dict(_BASE_CFG, enabled=False),
                      clock=lambda: _at(10, 0))
    assert g.GATE.acquire(g.P2, timeout=1)   # 交易时段也放行（旧行为）
    g.GATE.release()


# ------------------------------------------------------------------ 抢占

def test_p0_preempts_p2_between_chunks():
    """P2 连续短调用（模拟分片取数）时，P0 只需等当前一片结束，而非整段任务。"""
    _use_clock(_at(10, 0))   # 交易时段 P2 会被拒；改用盘前 P2 放行窗口
    _use_clock(_at(8, 30))
    chunk_hold = 0.03
    stop = threading.Event()
    p2_done = threading.Event()

    def p2_worker():
        while not stop.is_set():
            g.GATE.acquire(g.P2, timeout=5)
            time.sleep(chunk_hold)      # 模拟一次 TQ 调用
            g.GATE.release()
        p2_done.set()

    t = threading.Thread(target=p2_worker, daemon=True)
    t.start()
    time.sleep(0.05)                    # 确保 P2 已进入循环

    t0 = time.monotonic()
    assert g.GATE.acquire(g.P0, timeout=5)
    p0_wait = time.monotonic() - t0
    g.GATE.release()

    stop.set()
    assert p2_done.wait(2)

    # P0 的等待上限 ≈ 一次 P2 调用耗时 + 调度抖动（远小于 30s）
    assert p0_wait < chunk_hold * 4, "P0 等待 %.3fs 超过预期" % p0_wait
    stats = g.GATE.stats()
    assert stats["low_priority_yielded_to_p0"] > 0, "P2 未观察到让出行为"


def test_p0_does_not_starve_behind_long_p2_hold_after_timeout():
    """P2 单次调用卡住（模拟 TQ DLL 卡死）时，P0 仍按 30s 超时语义报错而非永久挂起。"""
    _use_clock(_at(8, 30))
    holder_ready = threading.Event()
    release = threading.Event()

    def p2_holder():
        g.GATE.acquire(g.P2, timeout=5)
        holder_ready.set()
        release.wait(5)
        g.GATE.release()

    t = threading.Thread(target=p2_holder, daemon=True)
    t.start()
    assert holder_ready.wait(1)

    t0 = time.monotonic()
    with pytest.raises(TDXLockTimeout):
        with get_tdx_lock(timeout=0.1, priority="P0"):
            pass
    assert 0.05 < time.monotonic() - t0 < 1.0
    release.set()
    t.join(2)


def test_p1_yields_to_waiting_p0():
    _use_clock(_at(10, 0))
    holder_ready = threading.Event()
    p0_acquired = threading.Event()

    def p2_like_p1():
        g.GATE.acquire(g.P1, timeout=2)
        holder_ready.set()
        time.sleep(0.05)
        g.GATE.release()
        # 第二次尝试：此时 P0 已在等 → 必须让出（拿不到，超时返回 False）
        ok = g.GATE.acquire(g.P1, timeout=0.1)
        if ok:
            g.GATE.release()

    def p0():
        assert holder_ready.wait(1)
        assert g.GATE.acquire(g.P0, timeout=2)
        p0_acquired.set()
        time.sleep(0.2)   # 持有期间 P1 的第二次尝试应超时
        g.GATE.release()

    t1 = threading.Thread(target=p2_like_p1)
    t2 = threading.Thread(target=p0)
    t1.start(); t2.start()
    assert p0_acquired.wait(2)
    t1.join(2); t2.join(2)


# ------------------------------------------------------------------ 配额

def test_p1_quota_exhausts_and_recovers():
    cfg = dict(_BASE_CFG, p1_max_per_minute=600)   # 600/min = 10/s，容量 10
    g.reset_for_tests(config=cfg, clock=lambda: _at(10, 0),
                      trading_day_fn=lambda d: True)
    g.GATE.reset_state()
    # 容量 10 个令牌：连取 10 次应全部成功
    for _ in range(10):
        assert g.GATE.acquire(g.P1, timeout=0.1)
        g.GATE.release()
    # 第 11 次立即超时（令牌耗尽，等待 0.05s 不够补一个）
    t0 = time.monotonic()
    assert g.GATE.acquire(g.P1, timeout=0.05) is False
    assert time.monotonic() - t0 < 0.5
    # 0.2s 后补了 2 个令牌（10/s）
    time.sleep(0.25)
    assert g.GATE.acquire(g.P1, timeout=0.5)
    g.GATE.release()


# ------------------------------------------------------------------ 紧急放开

def test_override_opens_trading_window_and_expires():
    _use_clock(_at(10, 0))
    info = g.set_override(1, "测试紧急放开", operator="pytest")
    assert info["minutes"] == 1
    assert g.evaluate_policy()["p2_allowed"] is True
    assert g.GATE.acquire(g.P2, timeout=1)
    g.GATE.release()
    assert g.override_info()["active"] is True
    assert g.clear_override() is True
    with pytest.raises(g.TdxTaskBlocked):
        g.GATE.acquire(g.P2, timeout=1)


def test_override_minutes_capped_by_config():
    _use_clock(_at(10, 0))
    info = g.set_override(9999, "超长", operator="pytest")
    assert info["minutes"] == _BASE_CFG["override_max_minutes"]
    g.clear_override()


def test_status_reports_window_and_permissions():
    _use_clock(_at(15, 0))
    st = g.status()
    assert st["enabled"] is True
    assert st["policy"]["window"] == "p0_only"
    assert st["policy"]["p1_allowed"] is False
    assert st["policy"]["p2_allowed"] is False
    assert st["policy"]["next_p2_open_at"] is not None


# ------------------------------------------------------------------ 线程级默认级

def test_tdx_priority_sets_thread_default_and_restores():
    _use_clock(_at(10, 0))
    assert g.current_priority() == g.P1
    with g.tdx_priority(g.P2):
        assert g.current_priority() == g.P2
        with pytest.raises(g.TdxTaskBlocked):
            with get_tdx_lock(timeout=1):   # 未显式传优先级 → 线程默认 P2 → 被拒
                pass
    assert g.current_priority() == g.P1


def test_p0_explicit_bypasses_thread_default_p2():
    _use_clock(_at(10, 0))
    with g.tdx_priority(g.P2):
        with get_tdx_lock(timeout=1, priority="P0"):
            assert g.GATE.locked()
    assert not g.GATE.locked()


# ------------------------------------------------------------------ P2 分片取数

def test_get_history_raw_chunks_for_p2_and_merges(monkeypatch):
    from core.tq import data as data_mod

    g.reset_for_tests(config=dict(_BASE_CFG, p2_chunk_size=3),
                      clock=lambda: _at(8, 30),   # 盘前 P2 放行
                      trading_day_fn=lambda d: True)
    td = data_mod.TQData()
    calls = []

    def fake_load(stocks, periods, start, end, dividend_type, count):
        calls.append(list(stocks))
        cols = {c: [float(i + 1), float(i + 2)] for i, c in enumerate(stocks)}
        df = pd.DataFrame(cols, index=pd.to_datetime(["2026-11-20", "2026-11-21"]))
        td.skipped_stocks = []
        return {p: {"Close": df.copy()} for p in periods}

    monkeypatch.setattr(td, "_load_history_raw", fake_load)
    stocks = ["600000.SH", "600001.SH", "600002.SH", "600003.SH", "600004.SH"]
    raw = td.get_history_raw(stocks, ["1d"], priority="P2")

    assert [len(c) for c in calls] == [3, 2], "应分成 3+2 两片，每片一次独立加锁"
    close = raw["1d"]["Close"]
    assert list(close.columns) == stocks
    assert close.loc["2026-11-20"].tolist() == [1.0, 2.0, 3.0, 1.0, 2.0]


def test_get_history_raw_not_chunked_for_p0(monkeypatch):
    from core.tq import data as data_mod

    g.reset_for_tests(config=dict(_BASE_CFG, p2_chunk_size=3),
                      clock=lambda: _at(10, 0),
                      trading_day_fn=lambda d: True)
    td = data_mod.TQData()
    calls = []

    def fake_load(stocks, periods, start, end, dividend_type, count):
        calls.append(list(stocks))
        td.skipped_stocks = []
        return {"1d": {}}

    monkeypatch.setattr(td, "_load_history_raw", fake_load)
    td.get_history_raw(["a", "b", "c", "d"], ["1d"], priority="P0")
    assert calls == [["a", "b", "c", "d"]], "P0 不切片，一次调用"


# ------------------------------------------- 实盘主链/重任务的分级回归（新增）
def test_smoke_formula_outputs_takes_channel_at_p0(monkeypatch):
    """会话启动公式预检（_smoke_formula_outputs）按 P0 取通道：
    14:55–15:10 保护窗内启动会话不能被 P1 规则拒掉。"""
    from types import SimpleNamespace

    from core.services import live_service as ls

    seen = []

    class FakeFormula:
        def compute(self, *a, **kw):
            level = g.current_priority()
            seen.append(level)
            # 真闸：保护窗内若仍是 P1，会直接抛 TdxTaskBlocked
            assert g.GATE.acquire(level, timeout=1)
            g.GATE.release()
            return {"600000.SH": {"open_sig": [{"Date": "20260810", "Value": 1}],
                                  "ErrorId": 0}}

    monkeypatch.setattr(ls, "TQFormula", lambda: FakeFormula())
    monkeypatch.setattr(ls, "resolve_stock_codes", lambda db, pid: ["600000.SH"])
    monkeypatch.setattr(ls, "_session_strategies", lambda db, sid: [
        (1, SimpleNamespace(strategy_id=1, formula_id=1, period="1d",
                            formula_count=200, name="S1")),
    ])

    class _Q:
        def __init__(self, value):
            self.value = value

        def filter_by(self, **_):
            return self

        def first(self):
            return self.value

        def all(self):
            return self.value

    class _DB:
        def query(self, model):
            if model is ls.Formula:
                return _Q(SimpleNamespace(id=1, name="F", formula_count=200))
            return _Q([SimpleNamespace(signal_name="open_sig")])

    _use_clock(_at(15, 0))
    with pytest.raises(g.TdxTaskBlocked):
        g.GATE.acquire(g.P1, timeout=1)

    assert ls._smoke_formula_outputs(_DB(), 1) is None
    assert seen == [g.P0]


def test_signal_replay_endpoint_is_p2(monkeypatch):
    """POST /api/backtest/signal-replay 走 P2：交易时段立即拒绝并带下次可用时间，
    15:15 后放行（服务层实际触发取数时才过闸，故用 fake 服务内真过闸来验证）。"""
    from datetime import date
    from types import SimpleNamespace

    from core.api import backtest as bt_api

    levels = []
    runs = []

    def fake_svc(db, ps_id, start, end, dividend_mode="front", session_id=None):
        level = g.current_priority()
        levels.append(level)
        with get_tdx_lock(priority=level):   # 真闸：10:00 的 P2 会在此被拒
            runs.append(1)
        return ({"rows": []}, None)

    monkeypatch.setattr(bt_api, "_svc_replay_signals", fake_svc)
    req = SimpleNamespace(portfolio_strategy_id=1, start_date=date(2026, 10, 1),
                          end_date=date(2026, 10, 23), dividend_mode="none",
                          session_id=None)

    _use_clock(_at(10, 0))
    with pytest.raises(g.TdxTaskBlocked) as exc_info:
        bt_api.signal_replay(req, db=None)
    assert exc_info.value.reason == "trading_window"
    assert exc_info.value.next_available_at.strftime("%H:%M") == "11:30"
    assert levels == [g.P2]
    assert runs == [], "被拒时不得进入重任务主体"

    _use_clock(_at(15, 15))
    resp = bt_api.signal_replay(req, db=None)
    assert resp.get("code") == 0
    assert levels == [g.P2, g.P2]
    assert runs == [1]


# ------------------------------------------- 无实盘引擎时不按时段限制（新增）
def test_no_live_engine_opens_p2_during_trading():
    """刚安装/纯回测：没有实盘引擎时，交易时段也不拒 P2。"""
    _use_clock(_at(10, 0), live_running=False)
    p = g.evaluate_policy()
    assert p["live_running"] is False
    assert p["window"] == "no_live_engine"
    assert p["p1_allowed"] is True and p["p2_allowed"] is True
    assert p["next_p2_open_at"] is None
    t0 = time.monotonic()
    assert g.GATE.acquire(g.P2, timeout=1)
    g.GATE.release()
    assert time.monotonic() - t0 < 0.2


def test_no_live_engine_opens_p1_in_protected_window():
    _use_clock(_at(15, 0), live_running=False)
    assert g.evaluate_policy()["protected"] is False
    assert g.GATE.acquire(g.P1, timeout=1)
    g.GATE.release()


def test_live_engine_restores_time_windows():
    """有实盘时回到原时段限制（10:00 拒 P2 / 15:00 拒 P1），文案指向实盘。"""
    _use_clock(_at(10, 0), live_running=True)
    assert g.evaluate_policy()["live_running"] is True
    with pytest.raises(g.TdxTaskBlocked) as ei:
        g.GATE.acquire(g.P2, timeout=1)
    assert "实盘运行中" in str(ei.value)
    _use_clock(_at(15, 0), live_running=True)
    with pytest.raises(g.TdxTaskBlocked):
        g.GATE.acquire(g.P1, timeout=1)


def test_status_exposes_live_running():
    _use_clock(_at(10, 0), live_running=False)
    st = g.status()
    assert st["policy"]["live_running"] is False
    assert st["policy"]["p2_allowed"] is True


def test_default_live_running_reads_engine_registry(monkeypatch):
    """生产默认判据：进程级引擎注册表非空即视为有实盘。"""
    from core.services import live_service

    monkeypatch.setattr(live_service, "ENGINES", {})
    g.reset_for_tests(config=dict(_BASE_CFG), clock=lambda: _at(10, 0),
                      trading_day_fn=lambda d: d.weekday() < 5)
    assert g.evaluate_policy()["live_running"] is False

    monkeypatch.setattr(live_service, "ENGINES", {1: object()})
    g.reset_for_tests(config=dict(_BASE_CFG), clock=lambda: _at(10, 0),
                      trading_day_fn=lambda d: d.weekday() < 5)
    assert g.evaluate_policy()["live_running"] is True
