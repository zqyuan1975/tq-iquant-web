"""掘金桥（live/bridge/goldminer_bridge.py）单元测试 —— 纯 mock，不触网/不碰 gm。

覆盖四处缺陷：/order 真实结果与拒单、/orders 拒单回显与 remark、/cancel、幂等，
以及日历入参防御、卖出平仓语义与 /shutdown 自行退出（确认口令 + 延迟退出）。
"""
import datetime
import importlib.util
import json
import sys
import threading
import time
import types
import urllib.request
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace

import pytest

BRIDGE_PATH = (Path(__file__).resolve().parents[4]
               / "live" / "bridge" / "goldminer_bridge.py")


def _fake_order(cl_ord_id="cl-1", status=10, volume=100, price=7.52,
                symbol="SHSE.601398", side=1, reason=""):
    return SimpleNamespace(
        cl_ord_id=cl_ord_id, order_id="", ex_ord_id="", symbol=symbol,
        side=side, position_effect=1, price=price, volume=volume,
        filled_volume=0, filled_vwap=0, filled_amount=0,
        status=status, ord_rej_reason=0, ord_rej_reason_detail=reason,
        created_at=None,
    )


@pytest.fixture()
def bridge(monkeypatch):
    gm = types.ModuleType("gm")
    gm.__path__ = []
    api = types.ModuleType("gm.api")
    for name in ("set_token", "set_account_id", "run", "get_cash",
                 "get_position", "get_execution_reports", "get_orders",
                 "get_trading_dates", "history_n", "get_instruments"):
        setattr(api, name, lambda *a, **k: None)
    api.order_volume = lambda *a, **k: []
    api.order_cancel = lambda *a, **k: None
    api.MODE_LIVE = 1
    en = types.ModuleType("gm.enum")
    en.OrderSide_Buy = 1
    en.OrderSide_Sell = 2
    en.OrderType_Limit = 1
    en.PositionEffect_Open = 1
    en.PositionEffect_Close = 2
    sys.modules.update({"gm": gm, "gm.api": api, "gm.enum": en})

    spec = importlib.util.spec_from_file_location("gm_bridge_ut", str(BRIDGE_PATH))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.CONFIRM_REJECT = False

    yield mod

    for k in ("gm", "gm.api", "gm.enum", "gm_bridge_ut"):
        sys.modules.pop(k, None)


def test_order_returns_cl_ord_id_and_position_effect(bridge):
    calls = []

    def fake_order_volume(**kw):
        calls.append(kw)
        return [_fake_order(cl_ord_id="cl-%d" % len(calls),
                            side=kw["side"], volume=kw["volume"],
                            price=kw["price"], symbol=kw["symbol"])]

    bridge.order_volume = fake_order_volume

    buy = bridge.place_order({"code": "601398.SH", "op": "buy", "volume": 100,
                              "price": 7.52, "order_id": "oid-buy",
                              "remark": "rm-buy"})
    assert buy["ok"] is True
    assert buy["data"]["cl_ord_id"] == "cl-1"
    assert buy["data"]["accepted"] is True
    assert buy["data"]["rejected"] is False
    # A 股买入=开仓
    assert calls[-1]["position_effect"] == bridge.PositionEffect_Open

    sell = bridge.place_order({"code": "601398.SH", "op": "sell", "volume": 100,
                               "price": 7.52, "order_id": "oid-sell",
                               "remark": "rm-sell"})
    assert sell["ok"] is True
    # A 股卖出=平仓（缺陷 5）
    assert calls[-1]["position_effect"] == bridge.PositionEffect_Close


def test_order_rejected_by_broker_returns_ok_false_with_reason(bridge):
    reason = "[GMBROKER] 委托价不正确: 委托价超出涨跌停范围"
    bridge.order_volume = lambda **kw: [
        _fake_order(cl_ord_id="cl-rej", status=8, reason=reason)]

    res = bridge.place_order({"code": "601398.SH", "op": "buy", "volume": 100,
                              "price": 7.52, "order_id": "oid-rej"})
    assert res["ok"] is False
    assert reason in res["error"]
    assert res["data"]["cl_ord_id"] == "cl-rej"
    assert res["data"]["rejected"] is True
    assert res["data"]["accepted"] is False
    assert res["data"]["status"] == "Rejected"


def test_order_idempotent_replay_does_not_resubmit(bridge):
    calls = []

    def fake_order_volume(**kw):
        calls.append(kw)
        return [_fake_order(cl_ord_id="cl-once")]

    bridge.order_volume = fake_order_volume
    payload = {"code": "600888.SH", "op": "sell", "volume": 900,
               "price": 8.1, "order_id": "oid-dup", "remark": "rm-dup"}
    first = bridge.place_order(dict(payload))
    second = bridge.place_order(dict(payload))
    assert len(calls) == 1
    assert second["ok"] is True
    assert second["data"]["cl_ord_id"] == "cl-once"
    assert second["data"]["remark"] == "rm-dup"
    assert first["data"]["cl_ord_id"] == second["data"]["cl_ord_id"]


def test_orders_exposes_rejected_with_remark(bridge):
    bridge.order_volume = lambda **kw: [
        _fake_order(cl_ord_id="cl-rej2", status=8,
                    reason="[GMBROKER] 委托价不正确: 委托价超出涨跌停范围")]
    bridge.place_order({"code": "601398.SH", "op": "buy", "volume": 100,
                        "price": 7.52, "order_id": "oid-rej2", "remark": "rm-rej2"})

    # 查询源完全为空（柜台拒单未进回报）——台账必须兜住
    bridge.get_orders = lambda *a, **k: []
    bridge.get_execution_reports = lambda *a, **k: []

    rows = bridge.query_orders({})["data"]
    assert len(rows) == 1
    row = rows[0]
    assert row["cl_ord_id"] == "cl-rej2"
    assert row["status"] == 57            # 废单（平台 order_machine 判 rejected）
    assert row["status_text"] == "Rejected"
    assert row["remark"] == "rm-rej2"
    assert "涨跌停" in row["reason"]

    filtered = bridge.query_orders({"order_id": "oid-rej2"})["data"]
    assert len(filtered) == 1
    assert bridge.query_orders({"order_id": "no-such"})["data"] == []


def test_orders_merges_gm_query_and_ledger(bridge):
    bridge.order_volume = lambda **kw: [_fake_order(cl_ord_id="cl-gm", status=1)]
    bridge.get_execution_reports = lambda *a, **k: []
    bridge.place_order({"code": "601398.SH", "op": "buy", "volume": 100,
                        "price": 7.52, "order_id": "oid-gm", "remark": "rm-gm"})
    # 回查时 gm 已推进到「已成」，remark 由台账回填
    bridge.get_orders = lambda *a, **k: [_fake_order(cl_ord_id="cl-gm", status=3)]

    rows = bridge.query_orders({})["data"]
    assert len(rows) == 1
    assert rows[0]["status"] == 56      # 已成
    assert rows[0]["remark"] == "rm-gm"


def test_cancel_rejects_terminal_and_calls_gm(bridge):
    bridge.order_volume = lambda **kw: [_fake_order(cl_ord_id="cl-new", status=1)]
    res = bridge.place_order({"code": "601398.SH", "op": "buy", "volume": 100,
                              "price": 7.52, "order_id": "oid-new"})
    assert res["ok"] is True

    cancel_calls = []
    bridge.order_cancel = lambda orders: cancel_calls.append(orders)
    bridge.get_orders = lambda *a, **k: [_fake_order(cl_ord_id="cl-new", status=5)]

    ok = bridge.cancel_order({"order_id": "oid-new"})
    assert ok["ok"] is True
    assert cancel_calls and cancel_calls[0][0]["cl_ord_id"] == "cl-new"
    assert ok["data"]["cl_ord_id"] == "cl-new"
    assert ok["data"]["status"] == "Canceled"
    assert ok["data"]["verified"] is True

    # 已终态委托不再调 gm 撤单
    cancel_calls.clear()
    again = bridge.cancel_order({"cl_ord_id": "cl-new"})
    assert again["ok"] is False
    assert cancel_calls == []


def test_cancel_unknown_id_returns_gm_error(bridge):
    def raising(orders):
        raise RuntimeError("status=102 message=未知委托")

    bridge.order_cancel = raising
    res = bridge.cancel_order({"cl_ord_id": "cl-unknown"})
    assert res["ok"] is False
    assert "未知委托" in res["error"]


def test_calendar_empty_end_defaults_to_today(bridge):
    captured = {}

    def fake_dates(**kw):
        captured.update(kw)
        return ["2026-11-05"]

    bridge.get_trading_dates = fake_dates
    res = bridge.get_calendar({})
    assert res["ok"] is True
    assert captured["end_date"] != ""
    import datetime
    assert captured["end_date"] == datetime.date.today().strftime("%Y-%m-%d")

    bad = bridge.get_calendar({"end": "2026-13-99"})
    assert bad["ok"] is False
    assert "invalid date" in bad["error"]


def test_deals_reads_int_exec_type_and_side(bridge):
    bridge.get_execution_reports = lambda *a, **k: [SimpleNamespace(
        cl_ord_id="cl-deal", exec_id="ex-1", symbol="SHSE.600000",
        side=1, exec_type=15, price=10.5, volume=100, amount=1050.0,
        commission=1.0, created_at=None)]
    rows = bridge.query_deals({})["data"]
    assert len(rows) == 1
    assert rows[0]["direction"] == 48     # 买入
    assert rows[0]["order_ref"] == "cl-deal"


def test_ping_endpoints_includes_shutdown(bridge):
    assert "/shutdown" in bridge.ENDPOINTS


def test_http_routing_ping_lists_shutdown_and_rejects_without_confirm(
        bridge, monkeypatch):
    """真实 HTTP 层：GET /ping 回显端点清单；POST /shutdown 缺 confirm 不退出。"""
    exited = []
    monkeypatch.setattr(bridge.os, "_exit", lambda code: exited.append(code))

    server = ThreadingHTTPServer(("127.0.0.1", 0), bridge._Handler)
    server.daemon_threads = True
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    base = "http://127.0.0.1:%d" % server.server_address[1]
    try:
        with urllib.request.urlopen(base + "/ping", timeout=3) as r:
            ping = json.loads(r.read().decode("utf-8"))
        assert ping["ok"] is True
        assert "/shutdown" in ping["endpoints"]

        req = urllib.request.Request(
            base + "/shutdown", data=b"{}", method="POST",
            headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=3) as r:
            res = json.loads(r.read().decode("utf-8"))
        assert res["ok"] is False
        assert "confirm" in res["error"]
        assert exited == []
    finally:
        server.shutdown()
        server.server_close()
        t.join(timeout=2)


def test_shutdown_requires_explicit_confirm(bridge, monkeypatch):
    exited = []
    monkeypatch.setattr(bridge.os, "_exit", lambda code: exited.append(code))
    for payload in ({}, {"confirm": False}, {"confirm": 0}, {"no": "confirm"}):
        res = bridge.request_shutdown(payload)
        assert res["ok"] is False
        assert "confirm" in res["error"]
    assert exited == []


def test_shutdown_confirmed_returns_ok_then_exits(bridge, monkeypatch):
    done = threading.Event()
    codes = []

    def fake_exit(code):
        codes.append(code)
        done.set()

    monkeypatch.setattr(bridge.os, "_exit", fake_exit)
    monkeypatch.setattr(bridge, "SHUTDOWN_DELAY_SECONDS", 0)

    res = bridge.request_shutdown({"confirm": True})
    assert res == {"ok": True, "data": {"shutdown": True}}
    assert done.wait(timeout=3) is True
    assert codes == [0]


# ---------------- gm 健康可见性（/ping 增量字段 + on_error 接管） ----------------

def test_ping_includes_gm_health_without_calling_gm(bridge, monkeypatch):
    """/ping 必须含 gm 健康字段，且一次 gm 调用都不许发（不能把 GIL 阻塞带回心跳）。"""
    calls = []

    def _boom(*a, **k):
        calls.append((a, k))
        raise AssertionError("ping 不得调用 gm")

    for name in ("get_cash", "get_position", "get_orders", "get_execution_reports",
                 "get_trading_dates", "history_n", "order_volume", "order_cancel"):
        monkeypatch.setattr(bridge, name, _boom)

    body = bridge.ping_payload()
    assert calls == []
    # 既有字段一字不动
    assert body["ok"] is True
    assert body["service"] == "goldminer-bridge"
    assert body["port"] == bridge.PORT
    assert body["account"] == bridge.ACCOUNT
    assert body["dry_run"] is bridge.DRY_RUN
    assert body["endpoints"] == list(bridge.ENDPOINTS)
    assert len(body["endpoints"]) == 11
    # 增量健康字段
    for key in ("gm_ok_at", "gm_err_at", "gm_err_code", "gm_err_info",
                "gm_err_count", "gm_last_errors"):
        assert key in body
    assert body["gm_ok_at"] is None
    assert body["gm_err_count"] == 0
    assert body["gm_last_errors"] == []


def test_gm_call_success_updates_gm_ok_at(bridge, monkeypatch):
    monkeypatch.setattr(bridge, "get_cash",
                        lambda **kw: SimpleNamespace(available=1.0, nav=2.0))
    assert bridge.ping_payload()["gm_ok_at"] is None

    bridge._gm_call(bridge.get_cash, account_id=bridge.ACCOUNT)

    body = bridge.ping_payload()
    assert body["gm_ok_at"] is not None
    assert body["gm_err_count"] == 0


def test_gm_call_failure_updates_err_fields_visible_in_ping(bridge, monkeypatch):
    class FakeGmError(Exception):
        status = 4001

        def __str__(self):
            return "交易服务未连接"

    def _raise(**kw):
        raise FakeGmError("boom")

    monkeypatch.setattr(bridge, "get_cash", _raise)

    with pytest.raises(FakeGmError):
        bridge._gm_call(bridge.get_cash, account_id=bridge.ACCOUNT)

    body = bridge.ping_payload()
    assert body["gm_err_at"] is not None
    assert body["gm_err_code"] == "4001"
    assert "交易服务未连接" in body["gm_err_info"]
    assert body["gm_err_count"] == 1
    assert body["gm_last_errors"][-1]["code"] == "4001"


def test_on_error_records_ring_but_throttles_print(bridge, capsys):
    """on_error：不抛异常、环形缓冲全量留痕、同码 60s 内只打印一次。"""
    bridge.on_error(None, "1201", "实时行情服务连接断开")
    bridge.on_error(None, "1201", "实时行情服务连接断开")
    bridge.on_error(None, "4001", "其它错误")

    body = bridge.ping_payload()
    assert body["gm_err_count"] == 3
    assert [x["code"] for x in body["gm_last_errors"]] == ["1201", "1201", "4001"]
    assert body["gm_err_code"] == "4001"

    out = capsys.readouterr().out
    assert out.count("code=1201") == 1          # 同类节流
    assert "WARNING" in out                     # 连接类记 warning
    assert out.count("code=4001") == 1
    assert "[ERROR]" in out


def test_on_error_never_raises(bridge):
    """回调内异常必须被吞掉（SDK 会把异常当编码错误再回调一次）。"""
    bridge.on_error(None, None, None)           # 异常入参也不能抛
    assert bridge.ping_payload()["gm_err_count"] == 1


def test_gm_err_ring_capped_at_20(bridge):
    for i in range(25):
        bridge.on_error(None, "1200", "err-%d" % i)
    ring = bridge.ping_payload()["gm_last_errors"]
    assert len(ring) == 20
    assert ring[-1]["info"] == "err-24"


# ---------------- /price_limit（前收 + 涨跌停，缺字段回 null） ----------------
def test_price_limit_reads_instrument_info(bridge):
    seen = {}

    def fake(*a, **kw):
        seen.update(kw)
        return [SimpleNamespace(pre_close=10.0, upper_limit=11.0, lower_limit=9.0)]

    bridge.get_instruments = fake
    res = bridge.get_price_limit({"code": "600000.SH"})
    assert res["ok"] is True
    assert res["data"] == {"code": "600000.SH", "pre_close": 10.0,
                           "up_limit": 11.0, "down_limit": 9.0}
    assert res["source"] == "instrument_info"
    # 必须走 get_instruments、显式 fields、且不过滤 ST/停牌（否则空列表）
    assert seen["symbols"] == ["SHSE.600000"]
    assert seen["fields"] == "symbol,pre_close,upper_limit,lower_limit"
    assert seen["skip_st"] is False and seen["skip_suspended"] is False


def test_price_limit_falls_back_to_prev_daily_close(bridge):
    """合约信息缺前收 → 日线倒数第二根收盘（末根是当日未收盘 bar）。"""
    import datetime

    bridge.get_instruments = lambda **kw: []
    today = datetime.datetime.combine(datetime.date.today(), datetime.time())
    prev = today - datetime.timedelta(days=1)
    bridge.history_n = lambda **kw: [
        {"eob": prev, "close": 8.8},
        {"eob": today, "close": 9.1},
    ]
    res = bridge.get_price_limit({"code": "000001.SZ"})
    assert res["data"]["pre_close"] == 8.8
    assert res["data"]["up_limit"] is None
    assert res["data"]["down_limit"] is None
    assert res["source"] == "daily_close"


def test_price_limit_unavailable_returns_nulls(bridge):
    """gm 全挂 → ok:true + 字段全 null（平台 fail-open，不猜涨跌幅）。"""
    def boom(*a, **k):
        raise RuntimeError("gm down")

    bridge.get_instruments = boom
    bridge.history_n = boom
    res = bridge.get_price_limit({"code": "600000.SH"})
    assert res["ok"] is True
    assert res["data"]["pre_close"] is None
    assert res["source"] == "unavailable"
    assert bridge.get_price_limit({})["ok"] is False


# ---------------- /cancel 结局区分（canceled / rejected / not_found） ----------------
def test_cancel_result_canceled_after_verify(bridge):
    bridge.order_volume = lambda **kw: [_fake_order(cl_ord_id="cl-a", status=1)]
    bridge.place_order({"code": "601398.SH", "op": "buy", "volume": 100,
                        "price": 7.52, "order_id": "oid-a"})
    bridge.order_cancel = lambda orders: None
    bridge.get_orders = lambda *a, **k: [_fake_order(cl_ord_id="cl-a", status=5)]
    res = bridge.cancel_order({"cl_ord_id": "cl-a"})
    assert res["ok"] is True
    assert res["data"]["result"] == "canceled"
    assert res["data"]["verified"] is True


def test_cancel_result_rejected_when_filled(bridge):
    """撤单请求发出后复查到 Filled → ok:false + result=rejected（绝不误报已撤）。"""
    bridge.order_volume = lambda **kw: [_fake_order(cl_ord_id="cl-b", status=1)]
    bridge.place_order({"code": "601398.SH", "op": "buy", "volume": 100,
                        "price": 7.52, "order_id": "oid-b"})
    bridge.order_cancel = lambda orders: None
    bridge.get_orders = lambda *a, **k: [_fake_order(cl_ord_id="cl-b", status=3)]
    res = bridge.cancel_order({"cl_ord_id": "cl-b"})
    assert res["ok"] is False
    assert res["data"]["result"] == "rejected"


def test_cancel_result_not_found_when_broker_has_no_order(bridge):
    """gm 撤单报错且柜台当日委托确实没有该单 → result=not_found。"""
    def raising(orders):
        raise RuntimeError("status=102 message=未知委托")

    bridge.order_cancel = raising
    bridge.get_orders = lambda *a, **k: []
    res = bridge.cancel_order({"cl_ord_id": "cl-missing"})
    assert res["ok"] is False
    assert res["data"]["result"] == "not_found"
    assert "未知委托" in res["error"]





def _quote_fake(bridge, rows, calls):
    """替换 _gm_call：记录调用并返回指定 bars（不触网/不碰 gm）。"""
    def fake_gm_call(fn, **kw):
        calls.append(kw)
        return rows
    bridge._gm_call = fake_gm_call


def test_get_quote_cache_flag_and_55s_ttl(bridge):
    """非空结果：首次 cached=false，55s 内 cached=true 且不打云端，过期后重查。"""
    calls = []
    bars = [{"eob": datetime.datetime(2026, 11, 10, 11, 29), "open": 10.0,
             "high": 10.1, "low": 9.9, "close": 10.05, "volume": 1000}]
    _quote_fake(bridge, bars, calls)
    q = {"code": "600000.SH", "period": "1m", "count": 3}

    r1 = bridge.get_quote(q)
    assert r1["ok"] is True and r1["cached"] is False
    assert r1["data"]["600000.SH"][0]["stime"] == "20261110112900"
    assert len(calls) == 1

    r2 = bridge.get_quote(q)
    assert r2["ok"] is True and r2["cached"] is True
    assert r2["data"] == r1["data"]
    assert len(calls) == 1  # 55s 内不再打云端

    ck = ("600000.SH", "1m", 3, None)   # 缓存键含复权模式（缺省=None）
    ts, rows, _meta = bridge._QUOTE_CACHE[ck]
    bridge._QUOTE_CACHE[ck] = (ts - 56, rows, _meta)  # 推到 56s 前 → 过期
    r3 = bridge.get_quote(q)
    assert r3["cached"] is False
    assert len(calls) == 2
    # 换复权模式必须免命中同一缓存（front 与默认 adjust=0 的结果不同）
    r4 = bridge.get_quote(dict(q, dividend_type="front"))
    assert r4["cached"] is False
    assert len(calls) == 3
    assert r4["meta"]["dividend_type"] == "front"


def test_get_quote_empty_result_cached_for_10s(bridge):
    """空结果缓存 10s：结构保持 data[code]=[]，10s 内 cached=true 不再打云端。"""
    calls = []
    _quote_fake(bridge, [], calls)
    q = {"code": "600000.SH", "period": "1m", "count": 3}
    ck = ("600000.SH", "1m", 3, None)   # 缓存键含复权模式（缺省=None）

    r1 = bridge.get_quote(q)
    assert r1["ok"] is True and r1["cached"] is False
    assert r1["data"]["600000.SH"] == []  # 保留原结构，不伪造数据行
    assert len(calls) == 1

    r2 = bridge.get_quote(q)
    assert r2["ok"] is True and r2["cached"] is True
    assert r2["data"]["600000.SH"] == []
    assert len(calls) == 1

    ts, rows, _meta = bridge._QUOTE_CACHE[ck]
    bridge._QUOTE_CACHE[ck] = (ts - 9, rows, _meta)   # 9s 前：仍在 10s TTL 内
    assert bridge.get_quote(q)["cached"] is True
    assert len(calls) == 1

    bridge._QUOTE_CACHE[ck] = (ts - 11, rows, _meta)  # 11s 前：过期重查
    r3 = bridge.get_quote(q)
    assert r3["cached"] is False
    assert len(calls) == 2


def test_get_quote_error_not_cached(bridge):
    """云端异常不缓存：每次请求都重试，且不写入缓存键。"""
    calls = []

    def fake_gm_call(fn, **kw):
        calls.append(kw)
        raise RuntimeError("gm down")

    bridge._gm_call = fake_gm_call
    q = {"code": "600000.SH", "period": "1m", "count": 3}

    for _ in range(2):
        r = bridge.get_quote(q)
        assert r["ok"] is False and "quote:" in r["error"]
    assert len(calls) == 2
    assert ("600000.SH", "1m", 3) not in bridge._QUOTE_CACHE


def _handler_stub(bridge, write_error=None, path="/deals"):
    """绕过 socket 构造 _Handler：send_* 记账，wfile.write 可按需抛错。"""
    h = bridge._Handler.__new__(bridge._Handler)
    h.path = path
    h.request_version = "HTTP/1.1"
    h._headers_buffer = []
    h._sent = []

    def send_response(code, msg=None):
        h._sent.append(code)

    h.send_response = send_response
    h.send_header = lambda k, v: None
    h.end_headers = lambda: None

    class _W:
        def write(self, b):
            if write_error is not None:
                raise write_error
            return len(b)

    h.wfile = _W()
    return h


def test_is_client_gone_classification(bridge):
    """10054/10053/10038 与 BrokenPipe 判为客户端断开；其它异常不算。"""
    assert bridge.is_client_gone(ConnectionResetError(10054, "reset"))
    assert bridge.is_client_gone(BrokenPipeError())
    e = OSError("aborted")
    e.winerror = 10053
    assert bridge.is_client_gone(e)
    e2 = OSError("not socket")
    e2.winerror = 10038
    assert bridge.is_client_gone(e2)
    assert not bridge.is_client_gone(ValueError("bad"))
    assert not bridge.is_client_gone(RuntimeError("handler boom"))


def test_respond_client_closed_silent_single_line(bridge, capsys):
    """写回包遇 10054：不抛异常、返回 False、只打一行 client closed early。"""
    h = _handler_stub(bridge, write_error=ConnectionResetError(10054, "reset"))
    assert bridge._Handler._respond(h, {"ok": True}) is False
    assert h._write_failed is True
    out = capsys.readouterr().out
    assert "client closed early path=/deals" in out
    assert out.count("\n") == 1          # 单行，无 traceback
    assert "Traceback" not in out


def test_do_get_no_second_respond_after_write_failure(bridge):
    """回包写失败后不再走 500 二次写；处理阶段的对端断开同样不再写。"""
    bridge.query_deals = lambda q: {"ok": True, "data": []}
    h = _handler_stub(bridge, write_error=ConnectionResetError(10054, "reset"))
    assert h.do_GET() is False
    assert h._sent == [200]              # 只尝试写了一次

    def boom(q):
        raise ConnectionResetError(10054, "reset")

    bridge.query_deals = boom
    h2 = _handler_stub(bridge)
    assert h2.do_GET() is False
    assert h2._sent == []                # 对端已断：连 500 都不写

    h3 = _handler_stub(bridge)
    h3._write_failed = True
    bridge.query_deals = lambda q: (_ for _ in ()).throw(RuntimeError("boom"))
    assert h3.do_GET() is False
    assert h3._sent == []                # 已写失败过：放弃 500 兜底

