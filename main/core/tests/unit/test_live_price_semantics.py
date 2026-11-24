"""取价机制（2026-11-24 架构梳理）回归用例。

覆盖四类已确认缺陷：
- F1 同日陈旧快照被当作实时价用于报单（陈旧价不标注）；
- F2 成交回报缺金额/价 → 按 0 价落库（静默用 0）；
- F3 通道不返回 fill_price 时用委托价顶替成交价（无来源标注）；
- F4 无价/非法价的失败语义含糊（价与量混报，桥 limit 静默发 0）；
- F5 顺延单等首根 1m bar 无任何「等待中」标记；
- F6 熔断处置无价跳过无痕迹。

全部用注入时钟 / 假 dispatcher / object.__new__ 轻量桩，不连通道、不落库。
"""
from datetime import datetime, time as dtime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from core.engine.event import OrderEvent
from core.engine.http_bridge_dispatcher import (
    BridgeOrderRejected,
    HttpBridgeDispatcher,
)
from core.engine.live.order_machine import (
    OrderStateMachine,
    _aggregate_deals,
)
from core.engine.live.timing import now_shanghai
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.tdx_tq_dispatcher import TdxTqDispatcher
from core.engine.ths_ui_dispatcher import ThsUiDispatcher
from tq_iquant_shared.constants import SignalType, TradeType

_TODAY = now_shanghai().date()


def _at(hh: int, mm: int) -> datetime:
    return datetime.combine(_TODAY, dtime(hh, mm))


def _order(price, qty: int = 100) -> OrderEvent:
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=qty, price=price, bar_time=_at(9, 30), signal_name="open",
    )


def _price_refresh_engine(snap):
    eng = object.__new__(LiveEngine)
    eng.ctx = SimpleNamespace(clock=lambda: _at(10, 16))
    eng.session_id = "s-test"
    eng._dispatcher = SimpleNamespace(
        query_quote=lambda *a, **k: [])
    eng._snapshot_latest_price = lambda code: snap
    eng._warn_stale_quote = lambda *a, **k: None
    recorded = []
    eng._recorder = SimpleNamespace(record=lambda **kw: recorded.append(kw))
    return eng, recorded


# ---------------------------------------------------------------- F1
def test_stale_snapshot_is_not_used_as_current_price():
    """10:16 报单时，09:31 的快照样本（同日但 45 分钟前）不得当实时价。"""
    order = _order(Decimal("9.50"))
    eng, recorded = _price_refresh_engine(
        (Decimal("9.31"), _at(9, 31)))
    source = eng._refresh_submit_price(order)
    assert source == "snapshot_stale"
    assert order.price == Decimal("9.50")  # 保持信号价，不阻断
    assert any("snapshot_age_seconds=" in r.get("message", "")
               for r in recorded)


def test_fresh_snapshot_is_still_adopted():
    """60 秒内的快照样本仍按实时价采用（不误伤正常路径）。"""
    order = _order(Decimal("9.50"))
    eng, _ = _price_refresh_engine((Decimal("9.45"), _at(10, 15)))
    source = eng._refresh_submit_price(order)
    assert source == "snapshot"
    assert order.price == Decimal("9.45")


# ---------------------------------------------------------------- F2
def test_aggregate_deals_derives_amount_from_price():
    qty, amount, fallback = _aggregate_deals([{"volume": 100, "price": 10}])
    assert (qty, amount, fallback) == (100, Decimal("1000"), False)


def test_aggregate_deals_without_amount_or_price_stays_zero():
    qty, amount, fallback = _aggregate_deals([{"volume": 100}])
    assert qty == 100
    assert amount == 0
    assert fallback is False


def test_aggregate_deals_marks_order_price_fallback():
    qty, amount, fallback = _aggregate_deals([{
        "volume": 100, "price": 10, "price_source": "order_price_fallback"}])
    assert qty == 100
    assert amount == Decimal("1000")
    assert fallback is True


def test_backfill_refuses_zero_price_booking():
    """缺金额也缺价 → 保持 submitted 并告警，绝不写 0 成交价。"""
    om = object.__new__(OrderStateMachine)
    om._deal_amount_missing_alerted = set()
    om._deal_price_fallback_alerted = set()
    events = []
    om._emit = lambda topic, payload: events.append((topic, payload))
    order = SimpleNamespace(
        id=7, stock_code="600000", trade_type="buy", quantity=100,
        status="submitted", filled_quantity=0, filled_price=None,
        portfolio_strategy_id=1, bar_time=None,
    )
    OrderStateMachine.backfill_order(om, None, order, [{"volume": 100}])
    assert order.status == "submitted"
    assert order.filled_price is None
    assert events and events[0][1]["rule"] == "deal_amount_missing"


# ---------------------------------------------------------------- F3
def test_ths_deal_marks_order_price_fallback():
    d = object.__new__(ThsUiDispatcher)
    d._last_error = None
    d.get_orders = lambda: [{
        "order_id": "R1", "filled_volume": 100, "symbol": "600000",
        "direction": "buy", "price": 10.0,
    }]
    d._split_time = lambda raw: ("20261124", "100000")
    d._exchange_for = lambda code, market="": "SH"
    rows = d.query_deals()
    assert rows[0]["price"] == 10.0
    assert rows[0]["price_source"] == "order_price_fallback"


def test_tdx_deal_marks_fill_price_source():
    d = object.__new__(TdxTqDispatcher)
    d._last_error = None
    d._submit_times = {}
    d._position_delta_map = lambda: {}
    d.get_orders = lambda: [{
        "order_id": "R1", "filled_volume": 100, "symbol": "600000",
        "direction_known": True, "direction": "buy",
        "fill_price": 10.2, "price": 10.0,
    }]
    rows = d.query_deals()
    assert rows[0]["price"] == 10.2
    assert rows[0]["price_source"] == "fill"


# ---------------------------------------------------------------- F4
def test_tdx_zero_price_semantics_are_explicit():
    d = object.__new__(TdxTqDispatcher)
    d._ensure = lambda: None
    with pytest.raises(BridgeOrderRejected) as exc:
        TdxTqDispatcher.place_order(d, _order(Decimal("0")))
    assert "invalid order price" in str(exc.value)
    with pytest.raises(BridgeOrderRejected) as exc:
        TdxTqDispatcher.place_order(d, _order(Decimal("1"), qty=0))
    assert "invalid order volume" in str(exc.value)


def test_ths_file_mode_zero_price_semantics_are_explicit():
    d = object.__new__(ThsUiDispatcher)
    with pytest.raises(BridgeOrderRejected) as exc:
        ThsUiDispatcher._place_order_via_file(d, _order(Decimal("0")))
    assert "invalid order price" in str(exc.value)


def test_bridge_limit_rejects_zero_price_but_opponent_allows():
    d = object.__new__(HttpBridgeDispatcher)
    d.order_id = lambda o: "oid-1"
    d.resolved_price_type = lambda: "opponent"
    d._do_request = lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("limit 0 价不应发到桥"))
    with pytest.raises(BridgeOrderRejected) as exc:
        HttpBridgeDispatcher.place_order(
            d, _order(Decimal("0")), price_type="limit")
    assert "invalid order price" in str(exc.value)


# ---------------------------------------------------------------- F5
def _waiting_engine(queue):
    eng = object.__new__(LiveEngine)
    eng.ctx = SimpleNamespace(session_id="s-test")
    eng.session_id = "s-test"
    eng._engine_key = "tdx"
    eng._last_minute_bar_date = None
    eng._calendar = None
    eng._signal_bar_deferred = queue
    eng._deferred_waiting_warned = set()
    eng._db_session_factory = None
    events = []
    eng._emit_risk_safe = lambda payload: events.append(("risk", payload))
    return eng, events


def test_waiting_first_bar_is_emitted_once_per_day():
    queue = [
        SimpleNamespace(bar_time=_at(14, 55)),
        SimpleNamespace(bar_time=_at(14, 55)),
    ]
    eng, events = _waiting_engine(queue)
    eng._note_deferred_waiting_first_bar(_at(9, 40))
    eng._note_deferred_waiting_first_bar(_at(9, 41))
    assert len(events) == 1
    payload = events[0][1]
    assert payload["rule"] == "deferred_waiting_first_bar"
    assert payload["action"] == "waiting"
    assert payload["count"] == 2
    assert payload["waiting_since"].startswith(_TODAY.isoformat())
    assert "不是报单失败" in payload["message"]
    # 收到当日分钟 bar 后不再告警
    eng._last_minute_bar_date = _TODAY
    eng._note_deferred_waiting_first_bar(_at(9, 42))
    assert len(events) == 1


def test_waiting_first_bar_silent_before_0931_or_empty_queue():
    eng, events = _waiting_engine([SimpleNamespace(bar_time=_at(14, 55))])
    eng._note_deferred_waiting_first_bar(_at(9, 20))
    assert events == []
    eng2, events2 = _waiting_engine([])
    eng2._note_deferred_waiting_first_bar(_at(9, 40))
    assert events2 == []


def test_waiting_marker_written_to_deferred_rows():
    """deferred 行 error_message 追加等待起点，status 仍由原行保持 deferred。"""
    eng, _ = _waiting_engine([])
    row = SimpleNamespace(id=5, error_message="deferred:due=2026-11-24")

    class _Q:
        def filter(self, *args):
            return self

        def all(self):
            return [row]

    class _DB:
        committed = False

        def query(self, *args):
            return _Q()

        def commit(self):
            self.committed = True

        def rollback(self):
            pass

        def close(self):
            pass

    db = _DB()
    eng._db_session_factory = lambda: db
    eng._mark_deferred_waiting_rows(
        [SimpleNamespace(_deferred_row_id=5, bar_time=_at(14, 55))],
        _at(14, 55))
    assert "deferred:due=2026-11-24" in row.error_message
    assert ("waiting_first_bar:since=%sT14:55" % _TODAY.isoformat()
            in row.error_message)
    assert db.committed


# ---------------------------------------------------------------- F6
def test_breaker_skip_records_event_when_no_price():
    p = object.__new__(Portfolio)
    p.portfolio_id = 1
    p.risk_manager = SimpleNamespace(pending_disposal="liquidate")
    p.strategies = [SimpleNamespace(strategy_id=1)]
    p._iter_positions = lambda ctx: iter(
        [("600000", SimpleNamespace(quantity=100))])
    p._lot = lambda code: 100
    recorded = []
    p.recorder = SimpleNamespace(record=lambda **kw: recorded.append(kw))
    bar = SimpleNamespace(
        stocks={"600000": {"close": 0}}, exec_prices={},
        bar_time=_at(9, 31), period="1m")
    orders = Portfolio._breaker_disposal_orders(p, bar)
    assert orders == []
    assert any(r.get("action") == "skip" and "无有效价格" in r.get("message", "")
               for r in recorded)
