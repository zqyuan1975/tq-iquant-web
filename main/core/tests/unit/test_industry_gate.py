# -*- coding: utf-8 -*-
"""行业状态轴：右移一日 / 缺失 fail-open 标记 / 领先天数与强度查询。"""
from datetime import date

from core.engine.industry_gate import IndustryState, IndustryTracker


def _state(direction, strength=50.0, bull_dur=0, bias_z=0.0, ob=False):
    return IndustryState(direction=direction, strength=strength,
                         bull_dur=bull_dur, bias_z=bias_z, overbought=ob)


def _tracker():
    states = {
        "881002.SH": {
            date(2025, 6, 16): _state(-1, 20.0, 0),
            date(2025, 6, 17): _state(1, 70.0, 1),
            date(2025, 6, 18): _state(1, 80.0, 2),
        },
        "881005.SH": {
            date(2025, 6, 16): _state(1, 60.0, 9),
            date(2025, 6, 17): _state(1, 65.0, 10),
        },
    }
    membership = {"600019.SH": "881002.SH", "000001.SZ": "881005.SH"}
    return IndustryTracker(states, membership)


def test_state_on_right_shifts_one_day():
    t = _tracker()
    # 6/18 生效状态 = 6/17 的记录（昨日收盘管今日开仓）
    assert t.state_on("881002.SH", date(2025, 6, 18)).direction == 1
    # 6/17 生效状态 = 6/16（空头）
    assert t.state_on("881002.SH", date(2025, 6, 17)).direction == -1
    # 无更早记录 → None
    assert t.state_on("881002.SH", date(2025, 6, 16)) is None


def test_state_for_stock_and_bull_lead():
    t = _tracker()
    assert t.industry_of("600019.SH") == "881002.SH"
    # 6/19 取 6/18 的多头状态：bull_dur=2
    assert t.bull_dur_for_stock("600019.SH", date(2025, 6, 19)) == 2
    assert t.strength_for_stock("600019.SH", date(2025, 6, 19)) == 80.0
    assert t.is_bull_for_stock("600019.SH", date(2025, 6, 19)) is True
    # 6/17 取 6/16 空头：多空判定 False、领先天数 0
    assert t.is_bull_for_stock("600019.SH", date(2025, 6, 17)) is False
    assert t.bull_dur_for_stock("600019.SH", date(2025, 6, 17)) == 0


def test_missing_is_fail_open_none_and_reason():
    t = _tracker()
    # 未知个股 → None + 明确原因
    assert t.state_for_stock("999999.SH", date(2025, 6, 19)) is None
    assert "未映射" in t.missing_for_stock("999999.SH", date(2025, 6, 19))
    # 映射到行业但该日之前无历史
    assert t.state_for_stock("000001.SZ", date(2025, 6, 16)) is None
    assert t.state_for_stock("000001.SZ", date(2025, 6, 18)).direction == 1
    assert "无历史状态" in t.missing_for_stock("000001.SZ", date(2025, 6, 16))


def test_unavailable_industry_reported():
    t = IndustryTracker({}, {"600019.SH": "881002.SH"},
                        unavailable={"881002.SH": "有效根数 < 300"})
    assert t.available is False
    assert t.state_for_stock("600019.SH", date(2025, 6, 19)) is None
    reason = t.missing_for_stock("600019.SH", date(2025, 6, 19))
    assert "881002.SH" in reason and "300" in reason


def test_error_tracker_reason():
    t = IndustryTracker({}, {}, error="行业日线取数失败：boom")
    assert t.available is False
    assert t.missing_for_stock("600019.SH", date(2025, 6, 19)) == "行业数据不可用：行业日线取数失败：boom"
    assert t.span() is None


def test_summary_and_span():
    t = _tracker()
    assert t.industry_count() == 2
    assert t.stock_count() == 2
    assert t.span() == (date(2025, 6, 16), date(2025, 6, 18))
    assert t.summary()["available"] is True
