"""掘金桥方向归一单测：未知方向必须「不猜」。

背景：``side`` 缺失/无法识别时，旧实现 ``_is_buy`` 返回 False → direction=49（卖），
与 QMT 桥「未知方向不认领」的口径相反。实盘系统里方向猜错比丢单更危险，本文件
锁定「未知 → side="" 且不写 direction」的行为，防未来回退。

main venv 无 gm SDK（项目规则禁止把 gm 装进平台 venv），故先注入空壳
``gm`` / ``gm.api`` / ``gm.enum`` 再按文件路径加载桥模块。
"""
import importlib.util
import pathlib
import sys
import types

import pytest

_BRIDGE_DIR = pathlib.Path(__file__).resolve().parents[1]

_GM_API_NAMES = ("set_token", "set_account_id", "order_volume", "order_cancel", "run",
                 "get_cash", "get_position", "get_execution_reports", "get_orders",
                 "get_trading_dates", "history_n", "get_instruments", "MODE_LIVE")
_GM_ENUM_NAMES = ("OrderSide_Buy", "OrderSide_Sell", "OrderType_Limit",
                  "PositionEffect_Open", "PositionEffect_Close")


def _stub_gm():
    gm = types.ModuleType("gm")
    api = types.ModuleType("gm.api")
    enum = types.ModuleType("gm.enum")
    for n in _GM_API_NAMES:
        setattr(api, n, 0 if n == "MODE_LIVE" else (lambda *a, **k: None))
    for n in _GM_ENUM_NAMES:
        setattr(enum, n, n)
    gm.api = api
    gm.enum = enum
    sys.modules.setdefault("gm", gm)
    sys.modules.setdefault("gm.api", api)
    sys.modules.setdefault("gm.enum", enum)


def _load_bridge():
    _stub_gm()
    path = _BRIDGE_DIR / "goldminer_bridge.py"
    spec = importlib.util.spec_from_file_location("goldminer_bridge_under_test",
                                                  str(path))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def bridge():
    return _load_bridge()


# ---------------- _side_of：三分结果 ----------------

def test_side_of_known_values(bridge):
    assert bridge._side_of(1) == "buy"
    assert bridge._side_of("1") == "buy"
    assert bridge._side_of("OrderSide_Buy") == "buy"
    assert bridge._side_of(2) == "sell"
    assert bridge._side_of("2") == "sell"
    assert bridge._side_of("OrderSide_Sell") == "sell"


def test_side_of_unknown_never_guesses_sell(bridge):
    for v in (None, 0, "", "garbage", "OrderSide_Unknown", 99):
        assert bridge._side_of(v) == "", "side=%r 必须判未知（空串）" % (v,)


# ---------------- /orders：未知方向不写 direction ----------------

def _state(side, **kw):
    st = {"symbol": "SHSE.600000", "side": side, "price": 10.0, "volume": 100,
          "filled_volume": 0, "filled_amount": 0.0, "status_text": "New",
          "reason": "", "created_at": None}
    st.update(kw)
    return st


def test_order_row_omits_direction_when_side_unknown(bridge):
    for side in (None, 0, "", "garbage"):
        row = bridge._order_row("c-unknown", _state(side), None)
        assert "direction" not in row, "side=%r 不得写 direction" % (side,)
        assert row["side"] == ""


def test_order_row_known_side_maps_to_qmt_direction(bridge):
    buy = bridge._order_row("c-buy", _state(1), None)
    assert buy["direction"] == 48
    assert buy["side"] == "buy"
    sell = bridge._order_row("c-sell", _state("OrderSide_Sell"), None)
    assert sell["direction"] == 49
    assert sell["side"] == "sell"


# ---------------- /deals：未知方向不写 direction ----------------

def test_query_deals_unknown_side_omits_direction(bridge, monkeypatch):
    class _Rep(object):
        exec_type = 15
        side = None
        symbol = "SHSE.600000"
        cl_ord_id = "d-unknown"
        exec_id = "t-1"
        price = 10.0
        volume = 100
        filled_amount = 1000.0
        commission = 0.5
        created_at = "2026-11-07 10:00:00"

    monkeypatch.setattr(bridge, "_today_reports", lambda: [_Rep()])
    res = bridge.query_deals({})
    assert res["ok"] is True
    row = res["data"][0]
    assert "direction" not in row
    assert row["side"] == ""


# ---------------- /order：参数缺失/非法直接拒，不默认 buy/sell、不送 0 ----------------

@pytest.mark.parametrize("payload, keyword", [
    ({"code": "", "op": "buy", "volume": 100, "price": 10.0}, "code"),
    ({"code": "600000.SH", "volume": 100, "price": 10.0}, "op"),
    ({"code": "600000.SH", "op": "short", "volume": 100, "price": 10.0}, "op"),
    ({"code": "600000.SH", "op": "buy", "volume": 0, "price": 10.0}, "volume"),
    ({"code": "600000.SH", "op": "buy", "price": 10.0}, "volume"),
    ({"code": "600000.SH", "op": "buy", "volume": 100, "price": 0}, "price"),
    ({"code": "600000.SH", "op": "buy", "volume": 100}, "price"),
])
def test_place_order_rejects_missing_or_invalid_fields(bridge, payload, keyword):
    res = bridge.place_order(payload)
    assert res["ok"] is False
    assert res["data"]["status"] == "Invalid"
    assert keyword in res["error"]
    assert bridge._ledger_all() == [], "非法参数不得进台账"


# ---------------- /quote 复权参数（2026-11-12） ----------------
import datetime as _dt


def _bar_row(close=10.0):
    return {"eob": _dt.datetime(2026, 11, 12, 10, 30), "open": close,
            "high": close, "low": close, "close": close, "volume": 100}


def _gm_fake(calls, rows_by_adjust):
    """按 adjust 数值返回不同行的假 _gm_call；未定义的模式返回空表。"""

    def fake(fn, *args, **kwargs):
        calls.append(kwargs)
        return rows_by_adjust.get(kwargs.get("adjust"), [])

    return fake


def _quote(bridge, monkeypatch, rows_by_adjust, params):
    calls = []
    bridge._QUOTE_CACHE.clear()
    monkeypatch.setattr(bridge, "_gm_call", _gm_fake(calls, rows_by_adjust))
    return bridge.get_quote(params), calls


def test_quote_without_dividend_param_keeps_adjust_zero(bridge, monkeypatch):
    out, calls = _quote(bridge, monkeypatch, {0: [_bar_row()]},
                        {"code": "600000.SH", "period": "1m"})
    assert out["ok"] is True and out["data"]["600000.SH"]
    assert calls[0]["adjust"] == 0, "缺省必须仍是历史 adjust=0（不复权）"
    assert out["dividend_type"] == "none"
    assert out["meta"]["dividend_type"] == "none", "meta 与 QMT 桥同形态"
    assert "dividend_type_requested" not in out


def test_quote_forwards_explicit_dividend_type(bridge, monkeypatch):
    out, calls = _quote(bridge, monkeypatch,
                        {1: [_bar_row()], 2: [_bar_row()]},
                        {"code": "600000.SH", "period": "1m",
                         "dividend_type": "front"})
    assert calls[0]["adjust"] == 1
    assert out["dividend_type"] == "front"
    assert out["meta"]["dividend_type"] == "front"
    out2, calls2 = _quote(bridge, monkeypatch, {2: [_bar_row()]},
                          {"code": "600000.SH", "period": "1m",
                           "dividend_type": "back"})
    assert calls2[0]["adjust"] == 2
    assert out2["dividend_type"] == "back"
    assert out2["meta"]["dividend_type"] == "back"


def test_quote_empty_adjusted_table_falls_back_and_marks(bridge, monkeypatch):
    out, calls = _quote(bridge, monkeypatch, {0: [_bar_row()]},
                        {"code": "600000.SH", "period": "1m",
                         "dividend_type": "front"})
    assert out["ok"] is True and out["data"]["600000.SH"]
    assert [c["adjust"] for c in calls] == [1, 0]
    assert out["dividend_fallback"] is True
    assert out["dividend_type"] == "none"
    assert out["meta"]["dividend_type"] == "none"
    assert out["dividend_type_requested"] == "front"


def test_quote_unknown_dividend_type_ignored_but_reported(bridge, monkeypatch):
    out, calls = _quote(bridge, monkeypatch, {0: [_bar_row()]},
                        {"code": "600000.SH", "period": "1m",
                         "dividend_type": "week"})
    assert calls[0]["adjust"] == 0, "非法值必须按 none（adjust=0）处理"
    assert out["dividend_type"] == "none"
    assert out["meta"]["dividend_type"] == "none"
    assert "unknown dividend_type" in out["dividend_note"]
    assert "using 'none'" in out["dividend_note"]


# ---------------- /price_limit 除权日前收（2026-11-12） ----------------
def _daily_rows(close):
    """两根历史日线，末根即前收（日期都不是今天，避免被当成未收盘 bar）。"""
    today = _dt.date.today()
    return [{"eob": _dt.datetime.combine(today - _dt.timedelta(days=2),
                                         _dt.time(15, 0)), "close": close - 0.1},
            {"eob": _dt.datetime.combine(today - _dt.timedelta(days=1),
                                         _dt.time(15, 0)), "close": close}]


def _limit(bridge, monkeypatch, info, rows_by_adjust):
    bridge._PRICE_LIMIT_CACHE.clear()
    monkeypatch.setattr(bridge, "_read_instrument", lambda sym: info)
    monkeypatch.setattr(bridge, "_gm_call",
                        _gm_fake([], rows_by_adjust))
    return bridge.get_price_limit({"code": "600000.SH"})


def test_price_limit_instrument_info_untouched(bridge, monkeypatch):
    out = _limit(bridge, monkeypatch,
                 {"pre_close": 10.0, "upper_limit": 11.0, "lower_limit": 9.0},
                 {})
    assert out["data"] == {"code": "600000.SH", "pre_close": 10.0,
                           "up_limit": 11.0, "down_limit": 9.0}
    assert out["source"] == "instrument_info"
    assert out["pre_close_source"] == "instrument_info"
    assert out["ex_rights"] is None


def test_price_limit_non_exrights_unchanged(bridge, monkeypatch):
    out = _limit(bridge, monkeypatch, None,
                 {0: _daily_rows(10.0), 1: _daily_rows(10.0)})
    assert out["data"]["pre_close"] == 10.0
    assert out["source"] == "daily_close", "非除权日的旧 source 必须保持"
    assert out["ex_rights"] is False
    assert "notice" not in out


def test_price_limit_exrights_uses_reference_price(bridge, monkeypatch):
    out = _limit(bridge, monkeypatch, None,
                 {0: _daily_rows(10.0), 1: _daily_rows(9.5)})
    assert out["data"]["pre_close"] == 9.5
    assert out["ex_rights"] is True
    assert out["pre_close_source"] == "front_adjusted_daily_close"
    assert "除权日推断" in out["notice"]


def test_price_limit_exrights_unavailable_fails_open_and_labels(bridge,
                                                                monkeypatch):
    out = _limit(bridge, monkeypatch, None, {})
    assert out["data"]["pre_close"] is None
    assert out["source"] == "unavailable"
    assert out["pre_close_source"] == "unavailable"
    assert "除权信息不可得" in out["notice"]

