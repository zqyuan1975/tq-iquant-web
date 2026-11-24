# -*- coding: utf-8 -*-
"""字段字典骨架（A0-3）：登记校验、缺失语义、字段集指纹。"""
import pandas as pd
import pytest

from core.selection.field_dict import (
    FIELD_SET_VERSION,
    UnregisteredField,
    field_set_fingerprint,
    get_field,
    missing_mask,
    require_fields,
    validate_columns,
)


def test_registered_field_carries_contract():
    mv = get_field("total_mv_wan")
    assert mv.source == "gp" and mv.column == "GP16"
    assert mv.unit == "万元" and mv.pit_safe is True
    pe = get_field("pe_ttm")
    assert pe.pit_safe is False, "只有当前值的字段不得进入历史快照"


def test_unknown_field_rejected():
    with pytest.raises(UnregisteredField):
        get_field("not_exists_field")


def test_source_mismatch_rejected():
    with pytest.raises(ValueError, match="来源"):
        require_fields(["pe_ttm"], source="fn")


def test_pool_scope_rejected():
    # bp 只登记在价值池
    require_fields(["bp"], pool="value")
    with pytest.raises(ValueError, match="价值|value|不适用"):
        require_fields(["bp"], pool="growth")


def test_validate_columns_allows_known_and_rejects_unknown():
    validate_columns(["FN314", "FN329"], "fn")
    with pytest.raises(UnregisteredField, match="FN999"):
        validate_columns(["FN999"], "fn")


def test_missing_mask_honours_zero_semantics():
    df = pd.DataFrame({
        "total_shares": [0.0, 100.0, None],
        "volatility": [0.0, 20.0, None],
    })
    mask = missing_mask(df, ["total_shares", "volatility"])
    # total_shares 的 0 视为缺失（TQ 填 0）
    assert mask["total_shares"].tolist() == [True, False, True]
    # volatility 允许真实 0（无波动），只把 NaN 视为缺失
    assert mask["volatility"].tolist() == [False, False, True]


def test_fingerprint_stable_and_sensitive():
    a = field_set_fingerprint(["pe_ttm", "pb_mrq"])
    b = field_set_fingerprint(["pb_mrq", "pe_ttm", "pe_ttm"])
    assert a == b, "顺序与重复不影响指纹"
    assert a != field_set_fingerprint(["pe_ttm"])
    assert a != field_set_fingerprint(["pe_ttm", "pb_mrq"], version="v2")
    assert len(a) == 16
    assert FIELD_SET_VERSION.startswith("draft")
