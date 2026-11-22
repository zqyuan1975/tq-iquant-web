"""HttpBridgeDispatcher 单测（0009 切片2）— httpx MockTransport 模拟 QMT 桥。

验证 Core 侧 dispatcher 与桥的 HTTP 映射正确：place_order→POST /order、
query_positions→GET /positions、桥离线抛 BridgeUnavailableError、幂等 order_id 透传。
"""
import json
from datetime import datetime
from decimal import Decimal

import httpx
import pytest

from core.engine.event import OrderEvent, TradeEvent
from core.engine.execution_engine import OrderDispatcher
from core.engine.http_bridge_dispatcher import (
    BridgeUnavailableError,
    BridgeOrderRejected,
    BridgeOrderIdError,
    HttpBridgeDispatcher,
)
from tq_iquant_shared.constants import SignalType, TradeType


def _make_order(**kw):
    defaults = dict(
        strategy_id=1,
        portfolio_id=1,
        stock_code="600000.SH",
        trade_type=TradeType.BUY,
        signal_type=SignalType.OPEN,
        quantity=100,
        price=Decimal("9.3"),
        bar_time=datetime(2026, 8, 5, 10, 0),
        signal_name="open_sig",
    )
    defaults.update(kw)
    return OrderEvent(**defaults)


class _Recorder:
    """MockTransport 辅助：记录每个请求 + 可选抛错。"""

    def __init__(self, respond=None, fail=False):
        self.requests = []
        self._respond = respond
        self._fail = fail

    def handler(self, request):
        self.requests.append(request)
        if self._fail:
            raise httpx.ConnectError("connection refused")
        if self._respond is not None:
            return self._respond(request)
        path = request.url.path
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path in ("/positions", "/account", "/orders", "/deals"):
            return httpx.Response(200, json={"ok": True, "data": []})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})


def _make_dispatcher(rec):
    client = httpx.Client(transport=httpx.MockTransport(rec.handler))
    return HttpBridgeDispatcher(base_url="http://127.0.0.1:8790", client=client), rec


def _last_json(rec, i=-1):
    return json.loads(rec.requests[i].content)


# ---------------- place_order 映射 ----------------
def _confirmed_responder(ref="cl-95514a12", status="filled", filled=100):
    """桥 ok:true 带 cl_ord_id + /orders 回读到该委托（默认全部成交）。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": True, "cl_ord_id": ref})
        if request.url.path == "/orders":
            return httpx.Response(200, json={"ok": True, "data": [{
                "cl_ord_id": ref, "status": status, "filled_volume": filled,
            }]})
        return httpx.Response(404)
    return respond


def test_place_order_maps_to_http():
    disp, rec = _make_dispatcher(_Recorder(respond=_confirmed_responder()))
    order = _make_order()
    trade = disp.place_order(order)

    order_req = [r for r in rec.requests if r.url.path == "/order"][-1]
    assert order_req.method == "POST"
    payload = json.loads(order_req.content)
    assert payload["code"] == "600000.SH"
    assert payload["op"] == "buy"
    assert payload["volume"] == 100
    assert payload["price"] == 9.3
    assert isinstance(trade, TradeEvent)
    assert trade.strategy_id == 1
    assert trade.stock_code == "600000.SH"
    assert trade.quantity == 100
    assert trade.signal_name == "open_sig"


def test_place_order_sell_op():
    disp, rec = _make_dispatcher(_Recorder())
    order = _make_order(trade_type=TradeType.SELL)
    disp.place_order(order)
    assert _last_json(rec)["op"] == "sell"


def test_place_order_sends_prtype_14():
    """0009 切片4：place_order payload 含 pr_type=14（对手价）。"""
    disp, rec = _make_dispatcher(_Recorder())
    disp.place_order(_make_order())
    assert _last_json(rec)["pr_type"] == 14


# ---------------- price_type（P0-3） ----------------
def test_place_order_default_price_type_is_opponent():
    """默认（配置缺省/opponent）→ 下发的 price_type 仍是旧语义对手价。"""
    disp, rec = _make_dispatcher(_Recorder())
    disp.place_order(_make_order())
    payload = _last_json(rec)
    assert payload["price_type"] == "opponent"
    assert payload["pr_type"] == 14


def test_place_order_price_type_from_config(monkeypatch):
    """config.iquant_bridge.price_type=limit → payload 带 limit 且 price=审批价。"""
    import core.engine.http_bridge_dispatcher as hbd

    monkeypatch.setattr(hbd, "load_config",
                        lambda: {"iquant_bridge": {"price_type": "limit"}})
    disp, rec = _make_dispatcher(_Recorder())
    disp.place_order(_make_order(price=Decimal("9.87")))
    payload = _last_json(rec)
    assert payload["price_type"] == "limit"
    assert payload["price"] == 9.87


def test_place_order_price_type_explicit_overrides_config(monkeypatch):
    import core.engine.http_bridge_dispatcher as hbd

    monkeypatch.setattr(hbd, "load_config",
                        lambda: {"iquant_bridge": {"price_type": "limit"}})
    rec = _Recorder()
    client = httpx.Client(transport=httpx.MockTransport(rec.handler))
    disp = HttpBridgeDispatcher(
        base_url="http://127.0.0.1:8790", client=client, price_type="market_best5")
    disp.place_order(_make_order())
    assert _last_json(rec)["price_type"] == "market_best5"


def test_place_order_goldminer_channel_omits_price_type():
    """掘金桥自身已走限价：payload 不得新增字段（保持零变化）。"""
    rec = _Recorder()
    client = httpx.Client(transport=httpx.MockTransport(rec.handler))
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:7801", client=client,
                                channel="goldminer_bridge")
    disp.place_order(_make_order())
    assert "price_type" not in _last_json(rec)


def test_place_order_unknown_price_type_falls_back_to_opponent(monkeypatch):
    import core.engine.http_bridge_dispatcher as hbd

    monkeypatch.setattr(hbd, "load_config",
                        lambda: {"iquant_bridge": {"price_type": "best5x"}})
    disp, rec = _make_dispatcher(_Recorder())
    disp.place_order(_make_order())
    assert _last_json(rec)["price_type"] == "opponent"


def test_place_order_sends_remark_oid_prefix():
    """remark = order_id(order) 前 20 位，桥作为 passorder userOrderId 写 m_strRemark。

    这是 Core↔桥订单精确匹配的根基：回填时 Core 按 remark 全局唯一认领本单，
    避免 代码+方向+数量 模糊匹配撞到跨会话遗留的同代码同向同量旧单。
    """
    disp, rec = _make_dispatcher(_Recorder())
    order = _make_order()
    disp.place_order(order)
    oid = HttpBridgeDispatcher.order_id(order)
    assert len(oid) == 32
    payload = _last_json(rec)
    assert payload["remark"] == oid[:20]
    assert len(payload["remark"]) == 20


def test_place_order_price_none_sends_zero():
    disp, rec = _make_dispatcher(_Recorder())
    order = _make_order(price=None)
    disp.place_order(order)
    assert _last_json(rec)["price"] == 0


# ---------------- 查询映射 ----------------
def test_query_positions_maps_to_http():
    disp, rec = _make_dispatcher(_Recorder())
    rows = disp.query_positions()
    assert rec.requests[-1].method == "GET"
    assert rec.requests[-1].url.path == "/positions"
    assert rows == []


def test_query_account_maps_to_http():
    disp, rec = _make_dispatcher(_Recorder())
    disp.query_account()
    assert rec.requests[-1].url.path == "/account"


def test_query_orders_maps_to_http():
    disp, rec = _make_dispatcher(_Recorder())
    disp.query_orders()
    assert rec.requests[-1].url.path == "/orders"


def test_query_quote_maps_to_http():
    """0009 切片3：query_quote → GET /quote?code=&period=&count=，返回该 code 的 bar 列表。"""
    bars = [
        {"stime": "20260805100800", "open": 9.30, "high": 9.35, "low": 9.28, "close": 9.32, "volume": 12000},
        {"stime": "20260805100900", "open": 9.32, "high": 9.40, "low": 9.31, "close": 9.38, "volume": 8000},
    ]

    def respond(request):
        if request.url.path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {"600000.SH": bars}})
        return httpx.Response(404)

    disp, rec = _make_dispatcher(_Recorder(respond=respond))
    rows = disp.query_quote("600000.SH", period="1m", count=10)
    assert rec.requests[-1].url.path == "/quote"
    assert "code=600000.SH" in str(rec.requests[-1].url)
    assert "period=1m" in str(rec.requests[-1].url)
    # 口径决定（2026-11-22）：信号取数显式不复权（旧桥忽略该参数、默认也是不复权）
    assert "dividend_type=none" in str(rec.requests[-1].url)
    assert rows == bars


def test_query_quote_empty_when_no_data():
    def respond(request):
        if request.url.path == "/quote":
            return httpx.Response(200, json={"ok": False, "error": "no data"})
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    assert disp.query_quote("600000.SH") == []


# ---------------- 桥离线 ----------------
def test_bridge_offline_raises_on_place():
    disp, _ = _make_dispatcher(_Recorder(fail=True))
    with pytest.raises(BridgeUnavailableError):
        disp.place_order(_make_order())


def test_bridge_offline_heartbeat_false():
    disp, _ = _make_dispatcher(_Recorder(fail=True))
    assert disp.heartbeat() is False


def test_heartbeat_true_when_online():
    disp, _ = _make_dispatcher(_Recorder())
    assert disp.heartbeat() is True
    assert disp.last_probe_error is None


def test_heartbeat_probe_error_refused():
    """连接被拒 = 端口无监听（启动闸据此给「桥进程未启动」）。"""
    disp, _ = _make_dispatcher(_Recorder(fail=True))
    assert disp.heartbeat() is False
    assert disp.last_probe_error == "refused"


def test_heartbeat_probe_error_timeout():
    """端口可连但超时 = 已监听但无响应（常见于桥卡在 SDK 查询）。"""
    def handler(request):
        raise httpx.ReadTimeout("timed out")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:7801", client=client)
    assert disp.heartbeat() is False
    assert disp.last_probe_error == "timeout"


def test_heartbeat_probe_error_protocol_on_non_200():
    def handler(request):
        return httpx.Response(404, json={"ok": False})
    client = httpx.Client(transport=httpx.MockTransport(handler))
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:7801", client=client)
    assert disp.heartbeat() is False
    assert disp.last_probe_error == "protocol"


def test_heartbeat_probe_error_protocol_on_non_json():
    def handler(request):
        return httpx.Response(200, text="<html>not json</html>")
    client = httpx.Client(transport=httpx.MockTransport(handler))
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:7801", client=client)
    assert disp.heartbeat() is False
    assert disp.last_probe_error == "protocol"


# ---------------- 下单超时/连接中断 → 在途（不得判拒单）----------------
def test_place_order_read_timeout_returns_none_as_inflight(caplog):
    """桥响应慢导致 /order 读超时（请求已发出）→ 返回 None（在途），不判拒单。

    原行为：任何异常都抛 BridgeUnavailableError → 引擎标 rejected 并移出在途集合，
    真实已发出的委托从此无 ref、无成交回填、无法撤（虚拟账本与柜台脱节）。
    修正：超时/连接中断按「已发出未确认」处理，引擎保持 submitted 等 /orders 认领。
    """
    import logging

    def handler(request):
        if request.url.path == "/order":
            raise httpx.ReadTimeout("timed out")
        return httpx.Response(200, json={"ok": True})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:7801", client=client)
    order = _make_order()
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        trade = disp.place_order(order)

    assert trade is None
    oid = HttpBridgeDispatcher.order_id(order)
    assert disp.last_unconfirmed_order_id == oid
    assert "在途" in (disp.last_unconfirmed_message or "")
    assert any("in-flight" in r.message for r in caplog.records), \
        "超时转在途必须留可回读线索（warning）"


# ---------------- 桥 HTTP 超时：读 15s / 建连 3s ----------------
def test_bridge_client_timeout_defaults(monkeypatch):
    """默认读/写/池 15s、建连 3s（掘金桥 /ping 实测 5~8s，10s 易误判断链）。"""
    from core.engine import http_bridge_dispatcher as mod
    monkeypatch.setattr(mod, "load_config", lambda: {"live": {}})
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:8790")
    t = disp._client.timeout
    assert t.read == 15.0
    assert t.write == 15.0
    assert t.pool == 15.0
    assert t.connect == 3.0
    disp._client.close()


def test_bridge_client_timeout_from_config(monkeypatch):
    """config.live.bridge_read/connect_timeout_seconds 可调（不改代码）。"""
    from core.engine import http_bridge_dispatcher as mod
    monkeypatch.setattr(mod, "load_config", lambda: {
        "live": {"bridge_read_timeout_seconds": 20,
                 "bridge_connect_timeout_seconds": 2},
    })
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:8790")
    t = disp._client.timeout
    assert t.read == 20.0
    assert t.connect == 2.0
    disp._client.close()


def test_bridge_client_timeout_explicit_override_beats_config(monkeypatch):
    """显式 timeout（通道级 request_timeout_seconds）优先；建连不跟着读超时放大。"""
    from core.engine import http_bridge_dispatcher as mod
    monkeypatch.setattr(mod, "load_config", lambda: {
        "live": {"bridge_read_timeout_seconds": 15,
                 "bridge_connect_timeout_seconds": 3},
    })
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:8790", timeout=30.0)
    t = disp._client.timeout
    assert t.read == 30.0
    assert t.connect == 3.0
    disp._client.close()


def test_bridge_client_connect_timeout_never_exceeds_read():
    """读超时配得比建连还小时，建连被夹到读超时（不留悬空连接）。"""
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:8790", timeout=2.0)
    t = disp._client.timeout
    assert t.connect == 2.0
    assert t.read == 2.0
    disp._client.close()


def test_place_order_connection_interrupted_returns_none_as_inflight():
    """连接中断（对端未发完整响应就断开）→ 同样按在途返回 None，不判拒单。"""
    def handler(request):
        if request.url.path == "/order":
            raise httpx.RemoteProtocolError(
                "peer closed connection without sending complete message body")
        return httpx.Response(200, json={"ok": True})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:7801", client=client)
    order = _make_order()

    assert disp.place_order(order) is None
    assert disp.last_unconfirmed_order_id == HttpBridgeDispatcher.order_id(order)
    assert "在途" in (disp.last_unconfirmed_message or "")


def test_place_order_ok_true_confirmed_filled_returns_trade():
    """(a) ok:true + 带 cl_ord_id + /orders 回读到 filled → 才可构造成交回报。"""
    disp, _ = _make_dispatcher(_Recorder(respond=_confirmed_responder()))
    trade = disp.place_order(_make_order())
    assert isinstance(trade, TradeEvent)
    assert trade.amount == Decimal("9.3") * 100
    assert disp.last_unconfirmed_order_id is None


def test_place_order_ok_true_without_ref_stays_unconfirmed(caplog):
    """(b) ok:true 但不含可识别委托标识 → 绝不成交，只能待确认（返回 None）。

    掘金桥对柜台拒单也回 {"ok":true}（如价格超涨跌停），且拒单不回显到
    /orders、/deals；若直接构造成交即伪造持仓/现金。无标识时降级为在途，
    日志带原文片段（bridge_order_unconfirmed）。
    """
    import logging

    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": True,
                                             "result": "委托被拒绝: 委托价超出涨跌停范围"})
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    order = _make_order()
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        trade = disp.place_order(order)

    assert trade is None
    assert disp.last_unconfirmed_order_id == HttpBridgeDispatcher.order_id(order)
    assert "在途" in (disp.last_unconfirmed_message or "")
    assert "涨跌停" in (disp.last_unconfirmed_message or ""), "线索须带应答原文片段"
    assert any("bridge_order_unconfirmed" in r.message for r in caplog.records)


def test_place_order_ok_true_ref_not_in_orders_stays_unconfirmed():
    """(b) ok:true + ref，但 /orders 回读不到该委托 → 待确认，不成交。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": True, "cl_ord_id": "cl-1"})
        if request.url.path == "/orders":
            return httpx.Response(200, json={"ok": True, "data": []})
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    trade = disp.place_order(_make_order())
    assert trade is None
    assert disp.last_unconfirmed_order_id
    assert "未回读" in (disp.last_unconfirmed_message or "")


def test_place_order_ok_true_ref_pending_stays_unconfirmed():
    """(b) ok:true + ref，回读到但状态未成交（挂单/未知状态码）→ 待确认，不成交。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": True, "cl_ord_id": "cl-1"})
        if request.url.path == "/orders":
            return httpx.Response(200, json={"ok": True, "data": [{
                "cl_ord_id": "cl-1", "status": 11, "filled_volume": 0,
            }]})
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    trade = disp.place_order(_make_order())
    assert trade is None
    assert disp.last_unconfirmed_order_id
    assert "未确认全部成交" in (disp.last_unconfirmed_message or "")


def test_place_order_ok_true_query_failure_stays_unconfirmed():
    """(b) ok:true + ref，但 /orders 回查超时 → 待确认（不判拒单、不成交）。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": True, "cl_ord_id": "cl-1"})
        if request.url.path == "/orders":
            raise httpx.ReadTimeout("timed out")
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    trade = disp.place_order(_make_order())
    assert trade is None
    assert disp.last_unconfirmed_order_id
    assert "回查失败" in (disp.last_unconfirmed_message or "")


def test_place_order_business_error_still_rejected():
    """明确收到桥的拒绝应答（{ok:false,error}）→ 仍判拒单（BridgeOrderRejected）。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": False, "error": "重复委托"})
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    with pytest.raises(BridgeOrderRejected, match="重复委托"):
        disp.place_order(_make_order())


def test_place_order_connect_error_still_unavailable():
    """连接被拒（请求从未发出）→ 仍是桥不可用，不按在途处理。"""
    disp, _ = _make_dispatcher(_Recorder(fail=True))
    with pytest.raises(BridgeUnavailableError):
        disp.place_order(_make_order())
    assert disp.last_unconfirmed_order_id is None


# ---------------- 桥返回 ok:false 但属于连接层瞬时故障 → 在途（不判拒单）--------
def _ok_false_responder(**body):
    """桥 HTTP 200 + ok:false（可带 error/data/result）。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json=dict(body))
        return httpx.Response(404)
    return respond


@pytest.mark.parametrize("error_text", [
    "order_volume raised: SdkError: 交易服务未连接",
    "order_volume returned empty list (SDK/网关未受理，无 cl_ord_id)",
    "[WinError 10061] 由于目标计算机积极拒绝，无法连接。",
    "HTTPConnectionPool(host='127.0.0.1', port=7801): Read timed out.",
    "query_orders: 交易服务不可用",
])
def test_place_order_transient_bridge_error_returns_none_as_inflight(caplog, error_text):
    """桥返回 ok:false，但错误文案属于连接层瞬时故障 → 返回 None（在途），不判拒单。

    掘金桥在 gm 交易服务断连窗口内会回 ok:false + "order_volume raised: ..." /
    "returned empty list"；这是连接层故障而非柜台明确拒单，判 rejected 会让真实
    委托脱跟踪（无 ref、无成交回填、无法撤）。正确语义是保持 submitted，由 180s
    超窗 + grace 兜底。
    """
    import logging

    disp, _ = _make_dispatcher(_Recorder(respond=_ok_false_responder(
        ok=False, error=error_text,
        data={"cl_ord_id": None, "accepted": False, "rejected": False,
              "status": "Error", "reason": error_text},
    )))
    order = _make_order()
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        trade = disp.place_order(order)

    assert trade is None, "瞬时故障不得构造成交回报"
    assert disp.last_unconfirmed_order_id == HttpBridgeDispatcher.order_id(order)
    assert disp.last_order_rejection is None, "瞬时故障不得留下拒单原文"
    assert "在途" in (disp.last_unconfirmed_message or "")
    assert any("bridge_order_transient" in r.message for r in caplog.records), \
        "瞬时故障转在途必须有 warning 级线索"


def test_place_order_transient_warning_is_throttled(caplog):
    """同类瞬时故障告警按 60s 窗口节流，避免断连窗口内逐单刷屏。"""
    import logging

    clock = _Clock()
    disp, _ = _make_dispatcher(_Recorder(respond=_ok_false_responder(
        ok=False, error="order_volume raised: 交易服务未连接",
    )))
    disp._now = clock
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        disp.place_order(_make_order(quantity=100))   # t=0 → 首报
        disp.place_order(_make_order(quantity=200))   # t=0 → 窗口内，抑制
        clock.t = 61.0
        disp.place_order(_make_order(quantity=300))   # t=61 → 再报

    warns = [r for r in caplog.records if "bridge_order_transient" in r.message]
    assert len(warns) == 2


def test_place_order_broker_rejected_flag_still_rejected():
    """柜台明确拒单（data.rejected=true + reason）→ 仍抛 BridgeOrderRejected。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={
                "ok": False,
                "error": "委托价超出涨跌停范围",
                "data": {"cl_ord_id": "cl-1", "accepted": False, "rejected": True,
                         "status": "Rejected", "reason": "委托价超出涨跌停范围"},
            })
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    with pytest.raises(BridgeOrderRejected, match="涨跌停"):
        disp.place_order(_make_order())
    assert disp.last_unconfirmed_order_id is None
    assert disp.last_order_rejection == "委托价超出涨跌停范围"


@pytest.mark.parametrize("error_text", [
    "委托价超出涨跌停范围",
    "可用资金不足",
    "委托数量非法",
    "order_volume raised: 委托数量必须为 100 的整数倍",
])
def test_place_order_business_rejection_text_still_rejected(error_text):
    """业务校验类文案（即使包在 order_volume raised 里）→ 仍是明确拒单。"""
    disp, _ = _make_dispatcher(_Recorder(
        respond=_ok_false_responder(ok=False, error=error_text)))
    with pytest.raises(BridgeOrderRejected):
        disp.place_order(_make_order())


def test_place_order_unknown_error_conservative_rejected():
    """未知错误文案 → 保守判「明确拒单」（宁可人工确认，不挂成在途 10 分钟）。"""
    disp, _ = _make_dispatcher(_Recorder(
        respond=_ok_false_responder(ok=False, error="bridge: something odd happened")))
    with pytest.raises(BridgeOrderRejected, match="something odd"):
        disp.place_order(_make_order())
    assert disp.last_unconfirmed_order_id is None


@pytest.mark.parametrize("error_text,payload,expected", [
    # 桥两条实测瞬时故障文案
    ("order_volume raised: SdkError: not connected", {}, "transient"),
    ("order_volume returned empty list (SDK/网关未受理，无 cl_ord_id)", {}, "transient"),
    # 桥内部查询失败
    ("query_positions: 交易服务断开", {}, "transient"),
    ("query_orders: timed out", {}, "transient"),
    # 连接层
    ("[WinError 10061] 由于目标计算机积极拒绝，无法连接。", {}, "transient"),
    ("[WinError 10054] 远程主机强迫关闭了一个现有的连接。", {}, "transient"),
    ("HTTPSConnectionPool: Read timed out.", {}, "transient"),
    ("Connection refused", {}, "transient"),
    # 柜台明确拒单
    ("委托价超出涨跌停范围", {}, "rejected"),
    ("可用资金不足", {}, "rejected"),
    ("", {"rejected": True, "reason": "废单"}, "rejected"),
    ("", {"data": {"rejected": True, "reason": "涨跌停"}}, "rejected"),
    ("重复委托", {}, "rejected"),
    # 未知 → 保守拒单
    ("bridge: something odd happened", {}, "rejected"),
    ("", {}, "rejected"),
])
def test_classify_bridge_error_table(error_text, payload, expected):
    """分类纯函数表驱动：模式 → rejected / transient。"""
    assert HttpBridgeDispatcher._classify_bridge_error(error_text, payload) == expected


# ---------------- 慢响应告警（阈值 + endpoint 去重）----------------
class _Clock:
    """可控单调时钟：handler 内推进，模拟桥单次调用耗时。"""

    def __init__(self, t=0.0):
        self.t = t

    def __call__(self):
        return self.t


def _slow_dispatcher(clock, advance=20.0, **kwargs):
    def handler(request):
        clock.t += advance
        path = request.url.path
        if path in ("/positions", "/account", "/orders", "/deals"):
            return httpx.Response(200, json={"ok": True, "data": []})
        return httpx.Response(200, json={"ok": True})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    return HttpBridgeDispatcher(
        base_url="http://127.0.0.1:7801", client=client, now_fn=clock,
        slow_threshold=15.0, **kwargs,
    )


def test_slow_response_warns_with_endpoint_and_elapsed(caplog):
    """单次调用耗时超慢阈值 → WARNING 带 endpoint + 耗时（非 errors）。"""
    import logging

    clock = _Clock()
    disp = _slow_dispatcher(clock)
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        disp.query_positions()

    slow = [r for r in caplog.records if "slow" in r.message]
    assert len(slow) == 1
    assert "/positions" in slow[0].message
    assert "20.00s" in slow[0].message or "20.0" in slow[0].message
    assert disp.last_slow[0] == "/positions"


def test_slow_response_warning_deduped_per_window(caplog):
    """慢告警按会话窗口去重：窗口内不重复刷（端点可不同，也合并成一条）。"""
    import logging

    clock = _Clock()
    disp = _slow_dispatcher(clock, slow_warn_interval=300.0)
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        disp.query_positions()      # t=20 → 首报
        disp.query_positions()      # t=40 → 窗口内，抑制
        clock.t = 300.0
        disp.query_positions()      # t=320 → 窗口期满，再报一次

    slow = [r for r in caplog.records if "slow" in r.message]
    assert len(slow) == 2


def test_slow_window_merges_different_endpoints_into_one_event():
    """同一窗口内 4 个不同端点各慢一次 → 只回调 1 条（首条及时，其余累计计数）。"""
    clock = _Clock()
    costs = {"/ping": 16.0, "/account": 19.3, "/positions": 17.0, "/orders": 18.0}

    def handler(request):
        clock.t += costs[request.url.path]
        return httpx.Response(200, json={"ok": True, "data": []})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    calls = []
    disp = HttpBridgeDispatcher(
        base_url="http://127.0.0.1:7801", client=client, now_fn=clock,
        slow_threshold=15.0, slow_warn_interval=300.0,
        on_slow=lambda e, s, summary: calls.append((e, s, summary)),
    )
    for path in ("/ping", "/account", "/positions", "/orders"):
        disp._do_request("GET", path)

    assert len(calls) == 1, "同一窗口内不得按端点各报一条"
    assert calls[0][0] == "/ping"
    assert calls[0][2] is None  # 首条无上一窗口汇总
    assert disp._slow_window_count == 3  # 窗口内后续 3 次已累计（首条已单独上报）


def test_slow_window_summary_carried_to_next_event():
    """窗口过期后的首条慢请求带上上一窗口汇总：N 次 + 最慢端点/耗时。"""
    clock = _Clock()
    costs = {"/ping": 16.0, "/account": 19.3, "/positions": 17.0, "/orders": 18.0}

    def handler(request):
        clock.t += costs[request.url.path]
        return httpx.Response(200, json={"ok": True, "data": []})

    client = httpx.Client(transport=httpx.MockTransport(handler))
    calls = []
    disp = HttpBridgeDispatcher(
        base_url="http://127.0.0.1:7801", client=client, now_fn=clock,
        slow_threshold=15.0, slow_warn_interval=300.0,
        on_slow=lambda e, s, summary: calls.append((e, s, summary)),
    )
    for path in ("/ping", "/account", "/positions", "/orders"):
        disp._do_request("GET", path)

    clock.t = 400.0  # 跨过 300s 窗口
    disp._do_request("GET", "/positions")

    assert len(calls) == 2
    summary = calls[1][2]
    assert summary == {
        "count": 3,
        "slowest_endpoint": "/account",
        "slowest_elapsed_seconds": 19.3,
        "window_seconds": 300.0,
    }


def test_slow_response_invokes_on_slow_callback():
    """超慢阈值时的回调（引擎据此发 risk 事件）；窗口内只回调一次。"""
    clock = _Clock()
    calls = []
    disp = _slow_dispatcher(
        clock, slow_warn_interval=300.0,
        on_slow=lambda endpoint, elapsed, summary: calls.append((endpoint, elapsed))
    )
    disp.query_positions()
    disp.query_positions()
    assert calls == [("/positions", 20.0)]


def test_fast_response_no_slow_alert(caplog):
    """未超阈值（含默认配置下的正常响应）→ 不告警、不回调。"""
    import logging

    clock = _Clock()
    calls = []
    disp = _slow_dispatcher(clock, advance=5.0,
                            on_slow=lambda e, s, summary: calls.append((e, s)))
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        disp.query_positions()
    assert not [r for r in caplog.records if "slow" in r.message]
    assert calls == []


# ---------------- 幂等 order_id 透传 ----------------
def test_idempotent_order_id_passthrough():
    disp, rec = _make_dispatcher(_Recorder())
    order = _make_order()
    disp.place_order(order)
    disp.place_order(order)                       # 同订单重试
    assert len(rec.requests) == 2
    assert _last_json(rec, -1)["order_id"] == _last_json(rec, -2)["order_id"]
    # 不同订单 → 不同 order_id
    disp.place_order(_make_order(quantity=200))
    assert len(rec.requests) == 3
    assert _last_json(rec, -1)["order_id"] != _last_json(rec, -2)["order_id"]
    # order_id 是确定性 hash 串
    assert len(_last_json(rec)["order_id"]) == 32


# ---------------- #24：桥返回非 JSON 不静默吞 ----------------
# ---------------- #4：桥业务拒单回显 ----------------
def test_place_order_bridge_rejection_raises_with_error():
    """#4：桥 {ok:false, error:...}（白名单/限额/重复）→ 抛 BridgeOrderRejected 回显原因。

    原行为：返回 None 被上层当笼统拒单，真实原因（如 volume 超限）丢在桥里查不到。
    """
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={
                "ok": False, "error": "volume 33900 exceeds max 100000",
            })
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    with pytest.raises(BridgeOrderRejected, match="volume 33900 exceeds max 100000"):
        disp.place_order(_make_order())


def test_place_order_ok_false_no_error_returns_none():
    """桥 {ok:false} 但无 error（异常路径）→ 维持返回 None（#24 非 JSON 语义，不当业务拒单）。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": False})
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    assert disp.place_order(_make_order()) is None


def test_place_order_non_json_body_warns_and_returns_none(caplog):
    """#24：桥 /order 返回非 JSON（如 HTML 错误页/坏网关）→ 告警 + 返回 None。

    原行为：r.json() 失败静默 data={} → data.get("ok") None → return None，
    被当业务拒绝（券商拒单），真实桥故障无痕迹。加 warning 让"非 JSON"可见。
    """
    import logging

    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(
                200, text="<html>502 Bad Gateway</html>",
                headers={"content-type": "text/html"},
            )
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        trade = disp.place_order(_make_order())
    assert trade is None
    assert any("non-JSON" in r.message for r in caplog.records), \
        "桥返回非 JSON 应触发 warning，不应静默"


def test_query_positions_non_json_body_warns_and_returns_empty(caplog):
    """#24：/positions 返回非 JSON → 告警 + 返回 []（不当无持仓）。

    原行为：返回 [] 被上层当"无持仓"，掩盖桥故障。加 warning 可见。
    """
    import logging

    def respond(request):
        if request.url.path == "/positions":
            return httpx.Response(
                200, text="<html>error</html>",
                headers={"content-type": "text/html"},
            )
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        rows = disp.query_positions()
    assert rows == []
    assert any("non-JSON" in r.message for r in caplog.records), \
        "桥返回非 JSON 应触发 warning，不应静默"


def test_query_quote_non_json_body_warns_and_returns_empty(caplog):
    """#24：/quote 返回非 JSON → 告警 + 返回 []（不当无行情）。

    原行为：返回 [] 让 BarPoller 拉不到 bar 静默不触发。加 warning 可见。
    """
    import logging

    def respond(request):
        if request.url.path == "/quote":
            return httpx.Response(
                200, text="<html>504 Gateway Timeout</html>",
                headers={"content-type": "text/html"},
            )
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    with caplog.at_level(logging.WARNING, logger="core.engine.http_bridge_dispatcher"):
        rows = disp.query_quote("600000.SH")
    assert rows == []
    assert any("non-JSON" in r.message for r in caplog.records), \
        "桥返回非 JSON 应触发 warning，不应静默"


# ---------------- check_price_limit（GET /price_limit，fail-open） ----------------
def _price_responder(data, status=200, ok=True):
    def respond(request):
        if request.url.path == "/price_limit":
            return httpx.Response(status, json={"ok": ok, "data": data})
        return httpx.Response(404)
    return respond


def test_check_price_limit_blocks_buy_at_up_limit():
    """买价 ≥ 交易所涨停价 → 返回拒绝文案（口径与 TDX/THS 一致）。"""
    disp, rec = _make_dispatcher(_Recorder(respond=_price_responder(
        {"code": "600000.SH", "pre_close": 10.0,
         "up_limit": 11.0, "down_limit": 9.0})))
    reason = disp.check_price_limit("600000.SH", "buy", Decimal("11.00"))
    assert reason and "涨停" in reason
    req = rec.requests[-1]
    assert req.method == "GET" and req.url.path == "/price_limit"
    assert req.url.params["code"] == "600000.SH"


def test_check_price_limit_blocks_sell_at_down_limit():
    disp, _ = _make_dispatcher(_Recorder(respond=_price_responder(
        {"code": "600000.SH", "pre_close": 10.0,
         "up_limit": 11.0, "down_limit": 9.0})))
    reason = disp.check_price_limit("600000.SH", "sell", Decimal("9.00"))
    assert reason and "跌停" in reason
    assert disp.check_price_limit("600000.SH", "buy", Decimal("10.50")) is None


def test_check_price_limit_derives_limits_from_pre_close():
    """桥只回前收（涨跌停价 null）→ 按板块比例推出：创业板 20%。"""
    disp, _ = _make_dispatcher(_Recorder(respond=_price_responder(
        {"code": "300001.SZ", "pre_close": 10.0,
         "up_limit": None, "down_limit": None})))
    assert disp.check_price_limit("300001.SZ", "buy", Decimal("11.50")) is None
    reason = disp.check_price_limit("300001.SZ", "buy", Decimal("12.00"))
    assert reason and "涨停" in reason


def test_check_price_limit_fail_open():
    """老桥无端点（404）/ 无有效价格 → 一律放行，绝不阻断下单。"""
    disp, _ = _make_dispatcher(_Recorder())          # 默认 respond：/price_limit → 404
    assert disp.check_price_limit("600000.SH", "buy", Decimal("999")) is None

    disp2, _ = _make_dispatcher(_Recorder(respond=_price_responder(
        {"code": "600000.SH", "pre_close": None,
         "up_limit": None, "down_limit": None})))
    assert disp2.check_price_limit("600000.SH", "buy", Decimal("999")) is None

    off, _ = _make_dispatcher(_Recorder(fail=True))   # 桥离线
    assert off.check_price_limit("600000.SH", "buy", Decimal("9.5")) is None


def test_price_limit_uses_channel_timeout_not_hardcoded_3s():
    """/price_limit 走通道解析后的读超时（旧实现硬编码 3.0s，掘金桥 8~13s 必然超时）。"""
    captured = []

    class _Resp:
        status_code = 200

        @staticmethod
        def json():
            return {"ok": True, "data": {"code": "600000.SH", "pre_close": 10.0,
                                         "up_limit": 11.0, "down_limit": 9.0}}

    client = httpx.Client(transport=httpx.MockTransport(lambda r: httpx.Response(404)))
    disp = HttpBridgeDispatcher(base_url="http://127.0.0.1:7801", client=client,
                                timeout=45.0, channel="goldminer_bridge")

    def fake_do_request(method, path, **kwargs):
        captured.append((method, path, kwargs))
        return _Resp()

    disp._do_request = fake_do_request
    assert disp.check_price_limit("600000.SH", "buy", Decimal("10.50")) is None
    assert disp.price_limit_bounds("600000.SH") is not None

    assert len(captured) == 2
    for method, path, kwargs in captured:
        assert method == "GET" and path.startswith("/price_limit")
        assert kwargs["timeout"] == 45.0        # 通道读超时
        assert kwargs["timeout"] != 3.0         # 不再是旧硬编码


def test_check_price_limit_skips_market_order_without_request():
    disp, rec = _make_dispatcher(_Recorder())
    assert disp.check_price_limit("600000.SH", "buy", None) is None
    assert disp.check_price_limit("600000.SH", "buy", 0) is None
    assert rec.requests == []


# ---------------- cancel_order（POST /cancel，老桥 404 降级） ----------------
def test_cancel_order_posts_ref_and_returns_true():
    def respond(request):
        if request.url.path == "/cancel":
            return httpx.Response(200, json={
                "ok": True, "data": {"result": "canceled", "status": 54}})
        return httpx.Response(404)

    disp, rec = _make_dispatcher(_Recorder(respond=respond))
    assert disp.cancel_order("cl-123", "600000.SH") is True
    req = rec.requests[-1]
    assert req.method == "POST"
    payload = json.loads(req.content)
    assert payload["order_ref"] == "cl-123"
    assert payload["cl_ord_id"] == "cl-123"
    assert payload["code"] == "600000.SH"
    assert disp.last_cancel_result == "canceled"


def test_cancel_order_already_canceled_is_idempotent_success():
    """已处于撤销态（ok:false + result:canceled）→ 按成功处理，不刷失败重试。"""
    disp, _ = _make_dispatcher(_Recorder(respond=lambda r: httpx.Response(
        200, json={"ok": False,
                   "error": "cannot cancel: 委托已终态（Canceled）",
                   "data": {"result": "canceled", "status": "Canceled"}})))
    assert disp.cancel_order("cl-9") is True


def test_cancel_order_not_found_and_rejected_return_false():
    for result, err in (("not_found", "order not found: cl-x"),
                        ("rejected", "cannot cancel: order already final")):
        disp, _ = _make_dispatcher(_Recorder(respond=lambda r, res=result, e=err:
            httpx.Response(200, json={"ok": False, "error": e,
                                      "data": {"result": res}})))
        assert disp.cancel_order("cl-x") is False
        assert disp.last_cancel_result == result
        assert err.split(":")[0] in (disp.last_error or "")


def test_cancel_order_old_bridge_404_disables_cancel():
    """老版本桥无 /cancel（HTTP 404）→ 一次失败后 supports_cancel=False。"""
    disp, _ = _make_dispatcher(_Recorder())          # 默认 respond：/cancel → 404
    assert disp.supports_cancel is True
    assert disp.cancel_order("cl-1") is False
    assert disp.supports_cancel is False
    assert "不支持撤单" in (disp.last_error or "")


def test_cancel_order_requires_ref():
    disp, rec = _make_dispatcher(_Recorder())
    assert disp.cancel_order("", "") is False
    assert rec.requests == []


def test_cancel_order_pending_then_canceled_is_confirmed():
    """缺陷2：桥先回 pending，复查拿到 canceled 才算成功（pending 不是终态）。"""
    bodies = [
        {"ok": True, "data": {"result": "pending", "status": 52}},
        {"ok": True, "data": {"result": "canceled", "status": 54}},
    ]
    state = {"i": 0}

    def respond(request):
        if request.url.path == "/cancel":
            body = bodies[min(state["i"], len(bodies) - 1)]
            state["i"] += 1
            return httpx.Response(200, json=body)
        return httpx.Response(404)

    disp, rec = _make_dispatcher(_Recorder(respond=respond))
    disp._cancel_recheck_interval = 0
    assert disp.cancel_order("cl-p1", "600000.SH") is True
    assert len([r for r in rec.requests if r.url.path == "/cancel"]) == 2
    assert disp.last_cancel_result == "canceled"


def test_cancel_order_pending_exhausted_is_not_canceled():
    """缺陷2：复查仍是 pending → 不判已撤：False + last_cancel_result=pending +
    可读的「撤单未确认」，由调用方保留在途/超时兜底。"""
    disp, rec = _make_dispatcher(_Recorder(respond=lambda r: httpx.Response(
        200, json={"ok": True, "data": {"result": "pending"}})))
    disp._cancel_recheck_interval = 0
    disp._cancel_recheck_attempts = 2
    assert disp.cancel_order("cl-p2") is False
    assert len([r for r in rec.requests if r.url.path == "/cancel"]) == 2  # 有限复查
    assert disp.last_cancel_result == "pending"
    assert "撤单未确认" in (disp.last_error or "")


def test_cancel_order_pending_then_rejected_keeps_semantics():
    """缺陷2：复查从 pending 转 rejected（柜台拒撤）→ 语义不变（False + rejected）。"""
    bodies = [
        {"ok": True, "data": {"result": "pending"}},
        {"ok": False, "error": "cannot cancel: order already final",
         "data": {"result": "rejected"}},
    ]
    state = {"i": 0}

    def respond(request):
        if request.url.path == "/cancel":
            body = bodies[min(state["i"], len(bodies) - 1)]
            state["i"] += 1
            return httpx.Response(200, json=body)
        return httpx.Response(404)

    disp, _ = _make_dispatcher(_Recorder(respond=respond))
    disp._cancel_recheck_interval = 0
    assert disp.cancel_order("cl-p3") is False
    assert disp.last_cancel_result == "rejected"


# ---------------- bridge_order_id / remark 认领键护栏 ----------------
def test_order_id_len_and_remark_key_len():
    """确定性 ID 32 位；认领键（柜台 m_strRemark）恒 20 位，不改既有格式。"""
    oid = HttpBridgeDispatcher.order_id(_make_order())
    assert len(oid) == 32
    assert len(oid) <= 32  # live_orders.bridge_order_id String(32)
    claim = HttpBridgeDispatcher.remark_of(oid)
    assert claim == oid[:20]
    assert len(claim) == HttpBridgeDispatcher.REMARK_MAX_LEN == 20


def test_order_id_consecutive_orders_differ_same_order_stable():
    """两笔不同单（数量不同）ID/认领键必不同；同订单重生成恒等（幂等）。"""
    o1 = _make_order(quantity=100)
    o2 = _make_order(quantity=200)
    oid1, oid2 = (HttpBridgeDispatcher.order_id(o) for o in (o1, o2))
    assert oid1 != oid2
    assert HttpBridgeDispatcher.remark_of(oid1) != HttpBridgeDispatcher.remark_of(oid2)
    assert HttpBridgeDispatcher.order_id(_make_order(quantity=100)) == oid1


def test_place_order_rejects_short_order_id_without_truncating(monkeypatch, caplog):
    """规则退化生成 <20 位 ID → 抛错拒单，绝不静默压成短 remark 键。"""
    import logging

    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    monkeypatch.setattr(disp, "order_id", lambda order: "short-id-15chr")
    with caplog.at_level(logging.ERROR, logger="core.engine.http_bridge_dispatcher"):
        with pytest.raises(BridgeOrderIdError):
            disp.place_order(_make_order())
    assert rec.requests == []  # 未发出 /order
    assert any("too short" in r.message for r in caplog.records)


def test_place_order_rejects_remark_collision(monkeypatch, caplog):
    """两笔不同单前 20 位相同（>20 位后才分叉）→ 第二笔拒绝下单（不重生成）。"""
    import logging

    rec = _Recorder(respond=lambda req: httpx.Response(200, json={"ok": True}))
    disp, _ = _make_dispatcher(rec)

    def fake_order_id(order):
        # 22 位，前 20 位恒同，仅尾部随数量分叉 → remark 撞单
        return "0123456789abcdef0123" + ("x" if order.quantity == 100 else "y")

    monkeypatch.setattr(disp, "order_id", fake_order_id)
    disp.place_order(_make_order(quantity=100))
    with caplog.at_level(logging.ERROR, logger="core.engine.http_bridge_dispatcher"):
        with pytest.raises(BridgeOrderIdError):
            disp.place_order(_make_order(quantity=200))
    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    assert any("collision" in r.message for r in caplog.records)


def test_place_order_same_order_retry_allowed():
    """同一订单重复提交（引擎重试/桥按 order_id 去重）不被护栏误拦。"""
    rec = _Recorder(respond=lambda req: httpx.Response(200, json={"ok": True}))
    disp, _ = _make_dispatcher(rec)
    disp.place_order(_make_order())
    disp.place_order(_make_order())  # 同 oid 同 claim → 放行
    assert len([r for r in rec.requests if r.url.path == "/order"]) == 2
