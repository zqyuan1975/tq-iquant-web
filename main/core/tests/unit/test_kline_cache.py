"""J6：回测 K 线 parquet 缓存单测。"""
import os
from datetime import datetime
from decimal import Decimal

import pandas as pd
import pytest

from core.tq.kline_cache import FIELDS, KlineCache

_NOW = datetime(2026, 11, 18, 15, 30)


def _raw(codes, timestamps, values):
    """构造 TQ 原始结构：{field: DataFrame(index=时间戳, columns=代码)}。"""
    idx = pd.to_datetime(timestamps)
    return {
        field: pd.DataFrame(
            {code: values[field] for code in codes}, index=idx)
        for field in FIELDS
    }


def _sample_raw():
    return _raw(
        ["600000.SH", "000001.SZ"],
        ["2026-11-16 00:00:00", "2026-11-17 00:00:00"],
        {
            "Open": [Decimal("10.10"), Decimal("10.20")],
            "High": [Decimal("10.50"), Decimal("10.60")],
            "Low": [Decimal("9.90"), Decimal("10.00")],
            "Close": [Decimal("10.30"), float("nan")],
            "Volume": ["1000", "2000"],
            "Amount": ["1030000", "2060000"],
        },
    )


def _cache(tmp_path, *, ttl=300):
    return KlineCache(str(tmp_path), today_ttl_seconds=ttl, clock=lambda: _NOW)


def test_store_then_load_roundtrip(tmp_path):
    cache = _cache(tmp_path)
    raw = _sample_raw()
    cache.store_period(raw, ["600000.SH"], "1d", "20260101", "20260110", "front")

    loaded = cache.load_period(
        ["600000.SH"], "1d", "20260101", "20260110", "front")
    assert loaded is not None
    assert set(loaded) == set(FIELDS)
    assert list(loaded["Close"].columns) == ["600000.SH"]
    assert [str(v) for v in loaded["Close"]["600000.SH"].tolist()] == ["10.30", "nan"]
    assert [str(v) for v in loaded["Volume"]["600000.SH"].tolist()] == ["1000", "2000"]
    assert loaded["Close"].index[0] == pd.Timestamp("2026-11-16")


def test_closed_interval_never_expires(tmp_path):
    cache = _cache(tmp_path, ttl=1)
    cache.store_period(_sample_raw(), ["600000.SH"], "1d",
                       "20260101", "20260110", "front")
    path = cache._path("600000.SH", "1d", "20260101", "20260110", "front")
    os.utime(path, (0, 0))  # 极旧 mtime
    assert cache.load_period(
        ["600000.SH"], "1d", "20260101", "20260110", "front") is not None


def test_today_range_expires_by_ttl(tmp_path):
    cache = _cache(tmp_path, ttl=300)
    cache.store_period(_sample_raw(), ["600000.SH"], "1d",
                       "20260101", "20261118", "front")
    path = cache._path("600000.SH", "1d", "20260101", "20261118", "front")
    assert cache.load_period(
        ["600000.SH"], "1d", "20260101", "20261118", "front") is not None
    os.utime(path, (0, 0))
    assert cache.load_period(
        ["600000.SH"], "1d", "20260101", "20261118", "front") is None


def test_partial_stocks_is_miss(tmp_path):
    cache = _cache(tmp_path)
    cache.store_period(_sample_raw(), ["600000.SH"], "1d",
                       "20260101", "20260110", "front")
    assert cache.load_period(
        ["600000.SH", "000001.SZ"], "1d", "20260101", "20260110", "front") is None


def test_corrupted_file_falls_back(tmp_path):
    cache = _cache(tmp_path)
    cache.store_period(_sample_raw(), ["600000.SH"], "1d",
                       "20260101", "20260110", "front")
    path = cache._path("600000.SH", "1d", "20260101", "20260110", "front")
    path.write_bytes(b"not parquet")
    assert cache.load_period(
        ["600000.SH"], "1d", "20260101", "20260110", "front") is None


def test_dividend_and_range_are_part_of_key(tmp_path):
    cache = _cache(tmp_path)
    raw = _sample_raw()
    cache.store_period(raw, ["600000.SH"], "1d", "20260101", "20260110", "front")
    assert cache.load_period(
        ["600000.SH"], "1d", "20260101", "20260110", "back") is None
    assert cache.load_period(
        ["600000.SH"], "1d", "20260102", "20260110", "front") is None


def test_from_config_disabled_returns_none():
    assert KlineCache.from_config(
        {"backtest": {"kline_cache": {"enabled": False}}}) is None
    assert KlineCache.from_config({}) is None


def test_from_config_enabled(tmp_path):
    cache = KlineCache.from_config({
        "backtest": {"kline_cache": {
            "enabled": True, "dir": str(tmp_path), "today_ttl_seconds": 60}},
    })
    assert isinstance(cache, KlineCache)
    assert cache.today_ttl_seconds == pytest.approx(60.0)
