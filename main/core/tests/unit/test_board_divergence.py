# -*- coding: utf-8 -*-
"""G 大盘-行业背离提示：只读映射，不进决策。"""
from core.services.board_service import divergence_hint


def test_divergence_market_bear_industry_bull():
    code, label = divergence_hint(1, -1)
    assert code == "market_bear_industry_bull"
    assert "大盘空头" in label and "行业多头" in label


def test_divergence_market_bull_industry_bear():
    code, label = divergence_hint(-1, 2)
    assert code == "market_bull_industry_bear"
    assert "大盘多头" in label and "行业空头" in label


def test_divergence_none_when_aligned_or_unknown():
    assert divergence_hint(1, 1) == (None, None)
    assert divergence_hint(-1, -1) == (None, None)
    assert divergence_hint(0, -1) == (None, None)
    assert divergence_hint(1, None) == (None, None)
