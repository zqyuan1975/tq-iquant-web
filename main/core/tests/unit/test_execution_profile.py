# -*- coding: utf-8 -*-
"""N1 回测执行档位（execution_profile）：legacy 逐字不变 / realistic 展开 / 快照与指纹。

档位是「会改历史结果」的开关集合，因此必须能被 params_snapshot 冻结、被增量指纹
识别（任一开关变化即判废旧接缝）。
"""
from decimal import Decimal

import pytest

from core.engine.execution_profile import (
    DEFAULT_VOLUME_LIMIT_RATIO,
    EXECUTION_PROFILE_VERSION,
    PROFILE_LEGACY,
    PROFILE_REALISTIC,
    expand_profile,
    fingerprint,
    normalize_profile,
    snapshot_payload,
)


def test_legacy_default_is_no_new_constraint():
    flags = expand_profile(None)
    assert flags["profile"] == PROFILE_LEGACY
    assert flags["volume_limit_enabled"] is False
    assert flags["adaptive_slippage_enabled"] is False
    assert flags["daily_close_peak_enabled"] is False
    assert flags["partial_fill_enabled"] is False
    assert flags["first_bar_no_fill"] is False
    # 涨跌停校验是既有行为（legacy 也开），不属于「新增约束」
    assert flags["price_limit_check_enabled"] is True
    assert flags["volume_limit_ratio"] is None


def test_realistic_enables_all_flags_and_ratio():
    flags = expand_profile(PROFILE_REALISTIC)
    assert flags["profile"] == PROFILE_REALISTIC
    assert flags["volume_limit_enabled"] is True
    assert flags["adaptive_slippage_enabled"] is True
    assert flags["daily_close_peak_enabled"] is True
    assert flags["partial_fill_enabled"] is True
    assert flags["first_bar_no_fill"] is True
    assert flags["volume_limit_ratio"] == DEFAULT_VOLUME_LIMIT_RATIO


def test_overrides_none_is_ignored_and_false_is_honoured():
    flags = expand_profile(PROFILE_REALISTIC, {
        "partial_fill_enabled": None,     # 不覆盖
        "first_bar_no_fill": False,       # 显式关闭
        "unknown_key": True,              # 未知键忽略
    })
    assert flags["partial_fill_enabled"] is True
    assert flags["first_bar_no_fill"] is False
    assert "unknown_key" not in flags


def test_legacy_can_enable_single_flag():
    flags = expand_profile(PROFILE_LEGACY, {"partial_fill_enabled": True})
    assert flags["partial_fill_enabled"] is True
    assert flags["volume_limit_enabled"] is False


def test_normalize_profile_rejects_unknown():
    with pytest.raises(ValueError):
        normalize_profile("aggressive")
    assert normalize_profile("") == PROFILE_LEGACY
    assert normalize_profile("REALISTIC") == PROFILE_REALISTIC


def test_snapshot_payload_json_safe_and_versioned():
    payload = snapshot_payload(expand_profile(PROFILE_REALISTIC))
    assert payload["profile"] == PROFILE_REALISTIC
    assert payload["version"] == EXECUTION_PROFILE_VERSION
    # Decimal 必须序列化成字符串（写 params_snapshot 的 JSON 安全要求）
    assert payload["volume_limit_ratio"] == str(DEFAULT_VOLUME_LIMIT_RATIO)
    assert isinstance(payload["volume_limit_ratio"], str)


def test_fingerprint_changes_with_profile_and_flags():
    legacy = fingerprint(expand_profile(PROFILE_LEGACY))
    realistic = fingerprint(expand_profile(PROFILE_REALISTIC))
    assert legacy != realistic
    tweaked = fingerprint(expand_profile(PROFILE_LEGACY, {
        "partial_fill_enabled": True}))
    assert tweaked != legacy
    # 同输入 → 稳定
    assert legacy == fingerprint(expand_profile(None))


def test_fingerprint_ratio_participates():
    on = expand_profile(PROFILE_REALISTIC)
    off = expand_profile(PROFILE_LEGACY)
    assert Decimal("0.10") in (DEFAULT_VOLUME_LIMIT_RATIO,)
    assert fingerprint(on) != fingerprint(off)
