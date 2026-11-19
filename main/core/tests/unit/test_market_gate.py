# -*- coding: utf-8 -*-
"""市场门控：Tracker 右移语义 + Python M 状态机 + tdx 输出解析。"""
from datetime import date, timedelta

import polars as pl
import pytest

from core.engine.market_gate import (
    MarketGateTracker,
    build_gate_timeline,
    compute_gate_states,
    gate_off_overlap,
    parse_tdx_gate_outputs,
)


# ---------------- Tracker ----------------

def _tr(states, strengths=None):
    t = MarketGateTracker()
    t.feed_history(states, strengths or {d: 50 for d in states})
    return t


def test_state_for_right_shift():
    """右移：state_for(d) 取严格早于 d 的最近状态日。"""
    t = _tr({date(2025, 6, 13): 1, date(2025, 6, 16): -1})
    assert t.state_for(date(2025, 6, 16)) == 1    # 16 日当日开仓用 13 日状态
    assert t.state_for(date(2025, 6, 17)) == -1   # 17 日用 16 日状态
    assert t.state_for(date(2025, 6, 13)) == 0    # 严格早于：13 日无前状态
    assert t.state_for(date(2025, 1, 1)) == 0     # 未定期放行


def test_blocks_only_bear():
    t = _tr({date(2025, 6, 13): 1, date(2025, 6, 16): -1})
    assert not t.blocks(date(2025, 6, 16))   # 多头
    assert t.blocks(date(2025, 6, 17))       # 空头
    assert not t.blocks(date(2025, 1, 1))    # 未定期


def test_switches_and_span():
    t = _tr({date(2025, 1, 2): 1, date(2025, 3, 4): -1, date(2025, 5, 6): 1})
    assert t.switches() == 2
    assert t.span() == (date(2025, 1, 2), date(2025, 5, 6))


def test_empty_tracker():
    t = MarketGateTracker()
    assert t.state_for(date(2025, 1, 1)) == 0
    assert t.blocks(date(2025, 1, 1)) is False
    assert t.span() is None


# ---------------- compute_gate_states（Python M 状态机） ----------------

def _df(closes, volumes=None):
    """最小合成行情：横向盘后拉高/砸破，驱动状态翻转。"""
    rows = []
    for i, c in enumerate(closes):
        rows.append({
            "datetime": date(2024, 1, 1) + timedelta(days=i),
            "Open": c, "High": c * 1.01, "Low": c * 0.99,
            "Close": float(c), "Volume": float(volumes[i] if volumes else 1000),
        })
    return pl.DataFrame(rows)


def test_gate_warmup_is_zero_state():
    """预热不足（<288 bars）→ 全 0 状态（未定期放行）。"""
    df = _df([100.0] * 100)
    states, strengths = compute_gate_states(df)
    assert len(states) == 100
    assert all(v == 0 for v in states.values())
    assert all(v == 0 for v in strengths.values())


def test_gate_bullish_rally():
    """288 日横盘后强势拉升 60 日：C 突破 AH 且 AH 走高（D 条件）→ 多头。"""
    n_flat, n_up = 300, 60
    closes = [100.0] * n_flat + [100.0 * (1 + 0.01 * i) for i in range(n_up)]
    df = _df(closes)
    states, strengths = compute_gate_states(df)
    tail = [states[d] for d in sorted(states)[-10:]]
    assert all(v in (1, 2) for v in tail)          # 多头
    assert any(v > 0 for v in strengths.values())  # 强度非零


def test_gate_bearish_break():
    """288 日横盘后跌破 AL（MA26×0.994）→ E 条件 → 空头。"""
    n_flat, n_dn = 300, 30
    closes = [100.0] * n_flat + [100.0 * (1 - 0.01 * i) for i in range(n_dn)]
    df = _df(closes)
    states, _ = compute_gate_states(df)
    tail = [states[d] for d in sorted(states)[-10:]]
    assert all(v == -1 for v in tail)


def test_gate_carry_forward():
    """M_NOW=0 的日子（既不 M_IN 也不 M_OUT）沿用前值（T_M 机制）。"""
    n_flat, n_up, n_hold = 300, 20, 40
    closes = ([100.0] * n_flat
              + [100.0 * (1 + 0.01 * i) for i in range(n_up)]   # 拉升入多
              + [122.0] * n_hold)                                # 高位横盘（AH 走高停 → 无 M_IN）
    df = _df(closes)
    states, _ = compute_gate_states(df)
    ds = sorted(states)
    # 拉升段确认多头后，横盘段 M_NOW 多数日为 0 → carry-forward 保持多头
    hold_tail = [states[d] for d in ds[-10:]]
    assert all(v in (1, 2) for v in hold_tail)


def test_gate_strength_range():
    df = _df([100.0] * 300 + [100.0 * (1 + 0.01 * i) for i in range(60)])
    _, strengths = compute_gate_states(df)
    assert all(0 <= v <= 100 for v in strengths.values())


# ---------------- parse_tdx_gate_outputs ----------------

def _tdx_raw(longs, shorts, strs):
    series = {}
    for name, vals in (("多头强度", longs), ("空头强度", shorts), ("强度线", strs)):
        series[name] = {"date": [d.isoformat() for d in sorted(
            {date(2025, 1, 1) + timedelta(days=i)
             for i in range(len(vals))})],
            "value": vals}
    return {"ErrorId": "0", "000985.CSI": series}


def test_parse_tdx_up_down():
    raw = _tdx_raw([10, 0, 0], [0, 20, 0], [10, 20, 0])
    states, strengths = parse_tdx_gate_outputs(raw, "000985.CSI")
    ds = sorted(states)
    # 第 3 日两方向输出均 0（无信号）→ 解析器 carry-forward 前一方向（设计 §2 契约）
    assert [states[d] for d in ds] == [1, -1, -1]
    assert [strengths[d] for d in ds] == [10, 20, 0]


def test_parse_tdx_missing_line_raises():
    raw = {"ErrorId": "0", "000985.CSI": {"多头强度": {"date": [], "value": []}}}
    with pytest.raises(ValueError):
        parse_tdx_gate_outputs(raw, "000985.CSI")


# ---------------- D1 门控时间轴与关闭重合 ----------------

D0 = date(2025, 1, 2)


def _days(n):
    return [D0 + timedelta(days=i) for i in range(n)]


def _seq(spec, n=10):
    """spec: {index: state}；未列出的日期记 0（未定）。"""
    return {d: spec.get(i, 0) for i, d in enumerate(_days(n))}


def test_build_gate_timeline_from_pairs_and_mapping():
    days = _days(5)
    tl = build_gate_timeline([(days[0], -1), (days[1], 1), (days[2], None)])
    assert tl.days == (days[0], days[1])
    assert tl.closed_on(days[0]) and not tl.closed_on(days[1])
    assert tl.state_on(date(2024, 12, 31)) == 0          # 轴外日期按未定
    tl_map = build_gate_timeline({days[0]: -1, days[1]: 2})
    assert tl_map.days == (days[0], days[1])
    assert build_gate_timeline([]) is None
    assert build_gate_timeline([(days[0], None)]) is None


def test_gate_timeline_runs_and_coverage():
    tl = build_gate_timeline(_seq({1: -1, 2: -1, 4: -1, 5: 1}, n=6))
    days = _days(6)
    assert tl.runs() == [(days[1], days[2]), (days[4], days[4])]
    assert tl.coverage() == {"days": 6, "closed": 3, "open": 1, "neutral": 2}
    summary = tl.to_dict()
    assert summary["first_day"] == days[0].isoformat()
    assert [r["days"] for r in summary["closed_runs"]] == [2, 1]


def test_gate_timeline_has_no_lookahead():
    """某日判定只依赖当天记录：删掉后续日期不影响已有日期的状态。"""
    full = build_gate_timeline(_seq({2: -1, 5: 1}, n=8))
    prefix = build_gate_timeline(_seq({2: -1}, n=3))
    for day in prefix.days:
        assert full.state_on(day) == prefix.state_on(day)
        assert full.closed_on(day) == prefix.closed_on(day)
    # 关闭区间的结束日不能被未来的恢复日提前影响到
    assert full.runs() == [(_days(8)[2], _days(8)[2])]


def test_gate_off_overlap_jaccard_on_common_days():
    lender = build_gate_timeline(_seq({0: -1, 1: -1, 2: 1}, n=5))
    borrower = build_gate_timeline(_seq({1: -1, 3: -1}, n=5))
    metric = gate_off_overlap(lender, borrower)
    assert metric["both_closed"] == 1          # 仅第 1 日同日关闭
    assert metric["either_closed"] == 3        # 0/1/3
    assert metric["overlap"] == pytest.approx(1 / 3)
    assert metric["common_days"] == 5

    same = gate_off_overlap(lender, lender)
    assert same["overlap"] == pytest.approx(1.0)


def test_gate_off_overlap_zero_and_disjoint_days():
    lender = build_gate_timeline(_seq({0: 1, 1: 2}, n=3))
    borrower = build_gate_timeline(_seq({0: 0, 1: 0, 2: 0}, n=3))
    assert gate_off_overlap(lender, borrower)["overlap"] == 0.0
    other = build_gate_timeline({date(2024, 5, 1): -1})
    assert gate_off_overlap(lender, other) is None   # 无共同交易日
