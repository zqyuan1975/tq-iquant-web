"""B5 影子模式：ShadowDispatcher 本地模拟成交 + 不占真实资源。"""
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.engine.event import OrderEvent
from core.engine.execution_engine import ShadowDispatcher
from core.models import Base, LiveSession, PortfolioStrategy
from core.services import live_service
from tq_iquant_shared.constants import TradeType, SignalType


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _order(trade_type, quantity=100, price="10.00"):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000.SH",
        trade_type=trade_type, signal_type=SignalType.OPEN,
        quantity=quantity, price=Decimal(price),
        bar_time=datetime(2026, 11, 18, 10, 0),
    )


class _Boom:
    """内层真实通道：任何下单/撤单被调用都视为失败。"""

    def __init__(self):
        self.calls = []

    def place_order(self, *a, **kw):
        self.calls.append("place_order")
        raise AssertionError("shadow must not call inner place_order")

    def cancel_order(self, *a, **kw):
        self.calls.append("cancel_order")
        raise AssertionError("shadow must not call inner cancel_order")

    def query_quote(self, code):
        self.calls.append("query_quote")
        return {"code": code, "last": Decimal("10.00")}

    def heartbeat(self):
        self.calls.append("heartbeat")
        return True


def test_shadow_fills_at_order_price_without_touching_inner():
    inner = _Boom()
    d = ShadowDispatcher(inner)
    order = _order(TradeType.BUY, quantity=200)
    trade = d.place_order(order)

    assert trade is not None
    assert trade.price == Decimal("10.00")
    assert trade.quantity == 200
    assert trade.amount == Decimal("2000.00")
    assert trade.commission == Decimal("5")       # min 佣金
    assert trade.stamp_duty == Decimal("0")
    assert trade.transfer_fee == Decimal("0.02")
    assert inner.calls == []


def test_shadow_sell_charges_stamp_duty():
    d = ShadowDispatcher(None)
    trade = d.place_order(_order(TradeType.SELL, quantity=1000))
    assert trade.stamp_duty == Decimal("5.00000")  # 10000 × 0.0005
    assert trade.commission == Decimal("5")        # min 佣金兜底


def test_shadow_delegates_quotes_and_heartbeat_to_inner():
    inner = _Boom()
    d = ShadowDispatcher(inner)
    assert d.heartbeat() is True
    assert d.query_quote("600000.SH")["last"] == Decimal("10.00")
    assert inner.calls == ["heartbeat", "query_quote"]


def test_shadow_cancel_succeeds_and_positions_empty():
    d = ShadowDispatcher(_Boom())
    assert d.cancel_order("ref-1", "600000.SH") is True
    assert d.last_cancel_result is True
    assert d.query_positions() == []


def test_preflight_accepts_shadow_mode():
    db = _db()
    p = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    sid = live_service.create_session(
        db,
        {"name": "shadow", "engine": "tdx", "execution_mode": "shadow",
         "portfolio_ids": [p.id]},
        "simulation",
    )["id"]
    assert live_service.preflight_signal_config(db, sid, "shadow") is None


def test_shadow_session_not_counted_as_occupied():
    db = _db()
    p = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    real_id = live_service.create_session(
        db, {"name": "real", "engine": "tdx", "execution_mode": "auto",
             "portfolio_ids": [p.id]}, "simulation")["id"]
    shadow_id = live_service.create_session(
        db, {"name": "shadow", "engine": "tdx", "execution_mode": "shadow",
             "portfolio_ids": [p.id]}, "simulation")["id"]
    for sid in (real_id, shadow_id):
        row = db.query(LiveSession).filter_by(id=sid).first()
        row.status = "running"
    db.commit()

    occupied = live_service._occupied_sessions(db, {}, exclude_id=-1)
    assert real_id in occupied
    assert shadow_id not in occupied


def test_update_session_accepts_shadow():
    db = _db()
    p = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    sid = live_service.create_session(
        db, {"name": "s", "engine": "tdx", "execution_mode": "auto",
             "portfolio_ids": [p.id]}, "simulation")["id"]
    ok, result = live_service.update_session(
        db, sid, {"execution_mode": "shadow"}, {})
    assert ok is True
    assert db.query(LiveSession).filter_by(id=sid).first().execution_mode == "shadow"
