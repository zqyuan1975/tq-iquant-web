"""回测执行档位（N1，2026-11-18）。

把「成交假设」集中成两档，避免用户自己在十几个开关里拼装：

  * ``legacy``（默认）：与接线前逐字一致——不新增任何约束。
  * ``realistic``：涨跌停校验、成交量上限 10%、自适应滑点、日终抬峰、成交量不足
    部分成交（残量回队）、每交易日首根 bar 不买入。

run 级可显式覆盖单项（``overrides``），最终**展开后的每个开关都写进
``params_snapshot.execution``**（含 profile 名与版本），并进增量
``schema_fingerprint`` —— 档位或任一开关变化即判废旧接缝。
"""

from decimal import Decimal
from typing import Optional

EXECUTION_PROFILE_VERSION = "execution_profile_v1"

PROFILE_LEGACY = "legacy"
PROFILE_REALISTIC = "realistic"
PROFILES = (PROFILE_LEGACY, PROFILE_REALISTIC)

# 键顺序固定，便于快照/指纹逐字段比对。
_FLAG_KEYS = (
    "price_limit_check_enabled",
    "volume_limit_enabled",
    "adaptive_slippage_enabled",
    "daily_close_peak_enabled",
    "partial_fill_enabled",
    "first_bar_no_fill",
)

_LEGACY = {
    "price_limit_check_enabled": True,
    "volume_limit_enabled": False,
    "adaptive_slippage_enabled": False,
    "daily_close_peak_enabled": False,
    "partial_fill_enabled": False,
    "first_bar_no_fill": False,
}

_REALISTIC = {
    "price_limit_check_enabled": True,
    "volume_limit_enabled": True,
    "adaptive_slippage_enabled": True,
    "daily_close_peak_enabled": True,
    "partial_fill_enabled": True,
    "first_bar_no_fill": True,
}

DEFAULT_VOLUME_LIMIT_RATIO = Decimal("0.10")


def normalize_profile(name: Optional[str]) -> str:
    value = (name or PROFILE_LEGACY).strip().lower()
    if value not in PROFILES:
        raise ValueError("execution_profile 仅支持 legacy / realistic")
    return value


def expand_profile(name: Optional[str] = None,
                   overrides: Optional[dict] = None) -> dict:
    """返回展开后的开关字典（含 ``profile`` / ``volume_limit_ratio``）。

    ``overrides`` 里值为 None 的键视为「不覆盖」；未知键忽略（防前端多传）。
    """
    profile = normalize_profile(name)
    flags = dict(_REALISTIC if profile == PROFILE_REALISTIC else _LEGACY)
    for key, value in (overrides or {}).items():
        if key in _FLAG_KEYS and value is not None:
            flags[key] = bool(value)
    flags["profile"] = profile
    flags["volume_limit_ratio"] = (
        DEFAULT_VOLUME_LIMIT_RATIO if flags["volume_limit_enabled"] else None)
    flags["version"] = EXECUTION_PROFILE_VERSION
    return flags


def snapshot_payload(flags: dict) -> dict:
    """JSON 安全形态（Decimal → 字符串），写 ``params_snapshot.execution``。"""
    payload = {key: flags.get(key) for key in _FLAG_KEYS}
    payload["profile"] = flags.get("profile", PROFILE_LEGACY)
    payload["version"] = flags.get("version", EXECUTION_PROFILE_VERSION)
    ratio = flags.get("volume_limit_ratio")
    payload["volume_limit_ratio"] = str(ratio) if ratio is not None else None
    return payload


def fingerprint(flags: dict) -> str:
    """增量指纹分项：profile + 展开后的每个开关（不含 version 之外的派生值）。"""
    payload = snapshot_payload(flags)
    return "|".join([
        payload.get("version") or "",
        payload.get("profile") or "",
        *[("%s=%s" % (k, int(bool(payload.get(k))))) for k in _FLAG_KEYS],
        "ratio=%s" % (payload.get("volume_limit_ratio") or ""),
    ])
