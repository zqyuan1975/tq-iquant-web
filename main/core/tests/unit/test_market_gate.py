# -*- coding: utf-8 -*-
"""市场门控：Tracker 右移语义 + Python M 状态机 + tdx 输出解析。"""
from datetime import date, timedelta

import polars as pl
import pytest

from core.engine.market_gate import (
    MarketGateTracker,
    compute_gate_states,
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
