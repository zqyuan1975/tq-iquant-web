"""桥方向归一单测：两腿（仿真/实盘）必须一致。

背景（2026-11-06 真机实证）：QMT 的 DEAL/ORDER 对象 ``m_nDirection`` 对股票
恒为 48（EEntrustBS），用它判买卖会把卖单当买单（601827 卖出 100 股返回
direction=48）。真正区分股票买卖的是 ``m_nOffsetFlag``：48=买，49-54=卖。
"""
import importlib.util
import pathlib

import pytest

_BRIDGE_DIR = pathlib.Path(__file__).resolve().parents[1]


def _load(name):
    spec = importlib.util.spec_from_file_location(
        name, str(_BRIDGE_DIR / (name + ".py")))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("name", ["iquant_bridge", "iquant_bridge_live"])
def test_row_side_prefers_offset_flag(name):
    mod = _load(name)
    assert mod._row_side(48, 48) == "BUY"      # 开仓/买入
    assert mod._row_side(49, 48) == "SELL"     # 平仓/卖出（direction 同为 48）
    for off in (50, 51, 52, 53, 54):
        assert mod._row_side(off, 48) == "SELL"


@pytest.mark.parametrize("name", ["iquant_bridge", "iquant_bridge_live"])
def test_row_side_never_defaults_to_buy(name):
    mod = _load(name)
    assert mod._row_side(None, 48) is None     # 旧桥形态：不可判定
    assert mod._row_side(None, 999) is None
    assert mod._row_side(999, None) is None


@pytest.mark.parametrize("name", ["iquant_bridge", "iquant_bridge_live"])
def test_row_side_direction_fallback_is_strict(name):
    mod = _load(name)
    assert mod._row_side(None, 23) == "BUY"    # opType 股票买入
    assert mod._row_side(None, 24) == "SELL"   # opType 股票卖出
    assert mod._row_side(None, 0) == "BUY"
    assert mod._row_side(None, 1) == "SELL"


# ---------------- /quote 复权参数（2026-11-12） ----------------
_BOTH = ["iquant_bridge", "iquant_bridge_live"]
_BAR = {"stime": "20261112103000", "open": 10.0, "high": 10.2,
        "low": 9.9, "close": 10.1, "volume": 1000, "amount": 10000.0}


class _Vals:
    def __init__(self, rows):
        self._rows = rows

    def tolist(self):
        return self._rows


class _Cols:
    def __init__(self, rows):
        self.values = _Vals(rows)


class FakeFrame:
    """最小 DataFrame 替身：只实现桥代码用到的 columns/index/len/tail/取值。"""

    def __init__(self, rows):
        self._rows = list(rows)
        self.columns = list(self._rows[0].keys()) if self._rows else []
        self.index = list(range(len(self._rows)))

    def __len__(self):
        return len(self._rows)

    def tail(self, n):
        return FakeFrame(self._rows[-n:])

    def __getitem__(self, key):
        if isinstance(key, str):
            return [r.get(key) for r in self._rows]
        return _Cols([[r.get(k) for k in key] for r in self._rows])


def _ctx_fn(calls, rows_by_mode):
    """按 dividend_type 返回不同行的假 get_market_data_ex；未定义的模式返回空表。"""

    def fn(fields, codes, period="1m", **kw):
        calls.append(dict(kw, period=period))
        rows = rows_by_mode.get(kw.get("dividend_type"))
        return {} if rows is None else {codes[0]: FakeFrame(rows)}

    return fn


def _quote_mod(name, rows_by_mode):
    mod = _load(name)
    calls = []
    mod.get_market_data_ex = _ctx_fn(calls, rows_by_mode)
    mod._has_data.add(("000001.SZ", "1m"))       # 关掉空读重试，测试不睡
    return mod, calls


@pytest.mark.parametrize("name", _BOTH)
def test_quote_without_dividend_param_keeps_historical_follow(name):
    mod, calls = _quote_mod(name, {"follow": [_BAR]})
    out = mod.get_quote({"code": "000001.SZ", "period": "1m", "count": "5"})
    assert out["ok"] is True and out["data"]["000001.SZ"]
    assert calls[0]["dividend_type"] == "follow", "缺省必须仍是历史 follow 口径"
    assert out["dividend_type"] == "default"


@pytest.mark.parametrize("name", _BOTH)
def test_quote_forwards_explicit_dividend_type(name):
    mod, calls = _quote_mod(name, {"front": [_BAR], "follow": [_BAR]})
    out = mod.get_quote({"code": "000001.SZ", "period": "1m",
                         "dividend_type": "front"})
    assert calls[0]["dividend_type"] == "front"
    assert out["dividend_type"] == "front"
    # 复权模式进缓存键：再取缺省口径必须重新取数，不能复用前复权结果
    mod.get_quote({"code": "000001.SZ", "period": "1m"})
    assert len(calls) == 2 and calls[1]["dividend_type"] == "follow"


@pytest.mark.parametrize("name", _BOTH)
def test_quote_empty_adjusted_table_falls_back_and_marks(name):
    mod, calls = _quote_mod(name, {"follow": [_BAR]})   # back -> 空表
    out = mod.get_quote({"code": "000001.SZ", "period": "1m",
                         "dividend_type": "back"})
    assert out["ok"] is True and out["data"]["000001.SZ"]
    assert out["dividend_fallback"] is True
    assert out["dividend_type"] == "default"
    assert out["dividend_type_requested"] == "back"
    assert len(calls) == 2, "显式模式空表后只降级一次"


@pytest.mark.parametrize("name", _BOTH)
def test_quote_unknown_dividend_type_ignored_but_reported(name):
    mod, calls = _quote_mod(name, {"follow": [_BAR]})
    out = mod.get_quote({"code": "000001.SZ", "period": "1m",
                         "dividend_type": "week"})
    assert calls[0]["dividend_type"] == "follow"
    assert out["dividend_type"] == "default"
    assert "dividend_note" in out


# ---------------- /price_limit 除权日前收（2026-11-12） ----------------
def _daily(*closes):
    return [{"stime": "202611%s" % ("11" if i == 0 else "12"),
             "open": c, "high": c, "low": c, "close": c,
             "volume": 100, "amount": 1000.0}
            for i, c in enumerate(closes)]


def _limit_mod(name, detail, rows_by_mode):
    mod = _load(name)
    mod.get_instrument_detail = lambda code: detail
    mod.get_market_data_ex = _ctx_fn([], rows_by_mode)
    mod._price_limit_cache.clear()
    return mod


@pytest.mark.parametrize("name", _BOTH)
def test_price_limit_exchange_limits_untouched(name):
    mod = _limit_mod(name, {"PreClose": 10.0, "UpStopPrice": 11.0,
                            "DownStopPrice": 9.0}, {})
    out = mod.get_price_limit({"code": "000001.SZ"})
    assert out["data"] == {"code": "000001.SZ", "pre_close": 10.0,
                           "up_limit": 11.0, "down_limit": 9.0}
    assert out["source"] == "instrument_detail"
    assert out["pre_close_source"] == "exchange_limit"
    assert out["ex_rights"] is None


@pytest.mark.parametrize("name", _BOTH)
def test_price_limit_non_exrights_unchanged(name):
    mod = _limit_mod(name, {"PreClose": 10.0},
                     {"none": _daily(9.8, 10.0), "front": _daily(9.8, 10.0)})
    out = mod.get_price_limit({"code": "000001.SZ"})
    assert out["data"]["pre_close"] == 10.0
    assert out["ex_rights"] is False
    assert out["pre_close_source"] == "instrument_detail"
    assert "notice" not in out


@pytest.mark.parametrize("name", _BOTH)
def test_price_limit_exrights_uses_reference_price(name):
    # 前复权昨收 9.5 != 不复权昨收 10.0 → 判定今日除权，前收改用参考价
    mod = _limit_mod(name, {"PreClose": 10.0},
                     {"none": _daily(9.9, 10.0), "front": _daily(9.4, 9.5)})
    out = mod.get_price_limit({"code": "000001.SZ"})
    assert out["data"]["pre_close"] == 9.5
    assert out["ex_rights"] is True
    assert out["pre_close_source"] == "front_adjusted_daily_close"
    assert "ex-rights" in out["notice"]


@pytest.mark.parametrize("name", _BOTH)
def test_price_limit_exrights_unavailable_fails_open_and_labels(name):
    mod = _load(name)
    mod.get_instrument_detail = lambda code: None
    mod._price_limit_cache.clear()
    out = mod.get_price_limit({"code": "000001.SZ"})
    assert out["data"] == {"code": "000001.SZ", "pre_close": None,
                           "up_limit": None, "down_limit": None}
    assert out["pre_close_source"] == "unavailable"
    assert out["ex_rights"] is None
    assert "unadjusted" in out["notice"]

