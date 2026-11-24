# -*- coding: utf-8 -*-
"""回测单笔金额上限：参数一致性校验与带数字告警（2026-11-24）。

只验证诊断文案与成因判定，不触发回测、不改任何参数。
"""
from types import SimpleNamespace

from core.services.backtest_service import (
    build_amount_cap_precheck, build_amount_cap_warning,
)


def _s(moa=500000, ratio=0.05, capital=1.0, name="s1"):
    return SimpleNamespace(max_order_amount=moa, single_open_ratio=ratio,
                           capital_ratio=capital, name=name)


def test_compounding_crossing_reports_threshold_date_and_advice():
    snaps = [
        {"snap_date": "2017-07-27", "total_value": 9_500_000},
        {"snap_date": "2017-07-28", "total_value": 10_128_075.9},
    ]
    text = build_amount_cap_warning(
        [_s()], {"buy_orders": 4268, "amount_cap_hits": 3389}, snaps)

    assert text is not None
    assert "3389/4268" in text and "79.4%" in text
    # 阈值权益 = 500000 / 5%（capital_ratio=1）
    assert "10,000,000 元" in text
    assert "2017-07-28" in text and "10,128,076" in text
    assert "复利增长" in text
    assert "equity_cap 设为 ≤ 10,000,000 元" in text
    # 按区间最高权益反推的单仓比例：500000 / 10128075.9 ≈ 4.94%
    assert "4.94%" in text


def test_abnormal_order_amount_when_equity_never_crosses():
    snaps = [{"snap_date": "2025-01-02", "total_value": 800_000}]
    text = build_amount_cap_warning(
        [_s()], {"buy_orders": 1500, "amount_cap_hits": 130}, snaps)

    assert text is not None
    assert "订单金额本身异常" in text
    assert "800,000" in text and "未越过阈值" in text
    assert "max_order_amount" in text
    # 不是容量错配，不应建议调 equity_cap
    assert "equity_cap" not in text


def test_fixed_sizing_reports_parameter_overrun():
    text = build_amount_cap_warning(
        [_s(moa=30000, ratio=0.05)],
        {"buy_orders": 100, "amount_cap_hits": 10},
        [{"snap_date": "2026-01-02", "total_value": 2_000_000}],
        position_sizing="fixed", initial_capital=1_000_000)

    assert text is not None
    assert "fixed 口径" in text
    assert "50,000" in text and "30,000" in text
    assert "3.00%" in text  # 30000 / 1000000


def test_cap_inconsistency_warns_even_without_actual_hits():
    text = build_amount_cap_warning(
        [_s()], {"buy_orders": 100, "amount_cap_hits": 0},
        [{"snap_date": "2026-01-02", "total_value": 1_000_000}],
        equity_cap=12_000_000)

    assert text is not None
    assert "未实际触发缩量" in text
    assert "参数不一致" in text
    assert "600,000" in text  # 1200 万 × 5%


def test_no_warning_when_cap_rarely_hit_and_equity_below_threshold():
    snaps = [{"snap_date": "2026-01-02", "total_value": 1_000_000}]
    assert build_amount_cap_warning(
        [_s()], {"buy_orders": 1000, "amount_cap_hits": 10}, snaps) is None
    # 无上限配置 / 无买入 → 静默
    assert build_amount_cap_warning([], {"buy_orders": 100,
                                         "amount_cap_hits": 90}) is None


def test_precheck_catches_equity_cap_and_starting_equity():
    text = build_amount_cap_precheck([_s()], "dynamic", 12_000_000, 1_000_000)
    assert text is not None
    assert "600,000" in text and "10,000,000" in text
    assert "建议" in text

    start_over = build_amount_cap_precheck(
        [_s()], "dynamic", None, 12_000_000)
    assert start_over is not None and "起始权益" in start_over

    assert build_amount_cap_precheck(
        [_s()], "dynamic", 5_000_000, 1_000_000) is None
