# -*- coding: utf-8 -*-
"""E3：组合级门控维度扩展（多指数投票 / 强度分档）单元测试。"""
from datetime import date, timedelta
from types import SimpleNamespace

from core.engine.market_gate import (
    CompositeGateTracker,
    MarketGateTracker,
    parse_gate_indices,
)

D0 = date(2024, 1, 2)
D1 = D0 + timedelta(days=1)
D2 = D0 + timedelta(days=2)
D3 = D0 + timedelta(days=3)
D4 = D0 + timedelta(days=4)


def _tracker(states: dict, strengths: dict) -> MarketGateTracker:
    t = MarketGateTracker()
    t.feed_history(states, strengths)
    return t


def test_parse_gate_indices_accepts_json_and_csv():
    assert parse_gate_indices(None) == []
    assert parse_gate_indices("") == []
    assert parse_gate_indices("000300.SH, 000985.CSI,000300.SH") == [
        "000300.SH", "000985.CSI"]
    assert parse_gate_indices('["000300.SH", "000905.SH"]') == [
        "000300.SH", "000905.SH"]
    # 以 "[" 开头但 JSON 非法 → 视为无有效配置
    assert parse_gate_indices('[not-json') == []


def test_vote_mode_any_blocks_on_single_bear():
    bear = _tracker({D0: -1}, {D0: 70})
    bull = _tracker({D0: 1}, {D0: 60})
    tracker = CompositeGateTracker([bear, bull], vote_mode="any")
    assert tracker.blocks(D1) is True
    assert tracker.state_for(D1) == -1
    assert tracker.strength_for(D1) == 70
    assert tracker.blocks(D0) is False  # 右移一日：D0 当日无更早状态


def test_vote_mode_majority_and_all():
    bear = _tracker({D0: -1}, {D0: 70})
    bull = _tracker({D0: 1}, {D0: 60})
    majority = CompositeGateTracker([bear, bull], vote_mode="majority")
    all_mode = CompositeGateTracker([bear, bull], vote_mode="all")
    # 1/2 空头：过半才拦 → False；全部才拦 → False
    assert majority.blocks(D1) is False
    assert all_mode.blocks(D1) is False
    # 2/2 空头 → 两种模式都拦
    bear2 = _tracker({D0: -1}, {D0: 55})
    for mode in ("majority", "all"):
        t = CompositeGateTracker([bear, bear2], vote_mode=mode)
        assert t.blocks(D1) is True
        assert t.strength_for(D1) == 70  # 取空头票最大强度


def test_strength_threshold_gates_bearish_vote():
    weak_bear = _tracker({D0: -1}, {D0: 40})
    assert CompositeGateTracker([weak_bear], strength_threshold=0).blocks(D1) is True
    assert CompositeGateTracker([weak_bear], strength_threshold=50).blocks(D1) is False
    assert CompositeGateTracker([weak_bear], strength_threshold=40).blocks(D1) is True
    # 阈值不改变多头方向
    bull = _tracker({D0: 2}, {D0: 20})
    t = CompositeGateTracker([bull], strength_threshold=90)
    assert t.state_for(D1) == 1
    assert t.blocks(D1) is False


def test_composite_state_span_and_switches():
    a = _tracker({D0: -1, D3: 1}, {D0: 80, D3: 60})
    b = _tracker({D1: 1, D3: 1, D4: 1}, {D1: 50, D3: 50, D4: 50})
    tracker = CompositeGateTracker([a, b], vote_mode="any")
    assert tracker.span() == (D0, D4)
    # 右移一日：D3 当日仍用 D1 前状态；D4 起才切多头
    assert [tracker.state_for(d) for d in (D1, D2, D3, D4)] == [-1, -1, -1, 1]
    assert tracker.switches() == 1


def test_single_index_defaults_match_plain_tracker():
    states = {D0: -1, D2: 1, D3: 2}
    strengths = {D0: 80, D2: 40, D3: 90}
    plain = _tracker(states, strengths)
    composite = CompositeGateTracker([_tracker(states, strengths)], vote_mode="any",
                                     strength_threshold=0)
    for d in (D0, D1, D2, D3, D4):
        assert composite.blocks(d) == plain.blocks(d)
        assert (composite.state_for(d) == 1) == (plain.state_for(d) in (1, 2))


def test_gate_enabled_for_strategy_force_on():
    from core.engine.market_gate import gate_enabled_for

    ps = SimpleNamespace(market_gate_enabled=False)
    assert gate_enabled_for(ps) is False
    assert gate_enabled_for(ps, [SimpleNamespace(market_gate_mode="inherit")]) is False
    assert gate_enabled_for(ps, [SimpleNamespace(market_gate_mode="on")]) is True
    assert gate_enabled_for(ps, [SimpleNamespace(market_gate_mode="on")],
                            requested=False) is False
    assert gate_enabled_for(SimpleNamespace(market_gate_enabled=True),
                            [SimpleNamespace(market_gate_mode="off")]) is True


def test_build_gate_tracker_dispatches_composite(monkeypatch):
    from core.services import backtest_service as bt

    calls: list = []

    def fake_single(code, engine, formula_name, start, end, **kwargs):
        calls.append(code)
        t = MarketGateTracker()
        t.feed_history({D0: -1 if code.endswith("SH") else 1}, {D0: 60})
        return t

    monkeypatch.setattr(bt, "build_single_gate_tracker", fake_single)
    ps = SimpleNamespace(
        market_gate_enabled=True,
        benchmark_index="000985.CSI",
        market_gate_indices='["000300.SH", "000905.SH"]',
        market_gate_engine="python",
        market_gate_formula_name=None,
        market_gate_vote_mode="any",
        market_gate_strength_threshold=0,
    )
    tracker = bt.build_gate_tracker(ps, D0, D4)
    assert isinstance(tracker, CompositeGateTracker)
    assert calls == ["000300.SH", "000905.SH"]
    assert tracker.blocks(D1) is True

    # 单指数 + any + 0 门槛 → 原 MarketGateTracker（旧路径不变）
    calls.clear()
    ps.market_gate_indices = None
    ps.benchmark_index = "000985.CSI"
    single = bt.build_gate_tracker(ps, D0, D4)
    assert isinstance(single, MarketGateTracker)
    assert calls == ["000985.CSI"]
