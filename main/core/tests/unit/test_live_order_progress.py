"""H5 委托详情进度：拆单分组/序号/追价次数持久化与组级汇总。"""
from datetime import datetime
from types import SimpleNamespace
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.engine.event import OrderEvent
from core.engine.live.order_machine import OrderStateMachine
from core.models import Base, LiveOrder, LiveSession
from core.services import live_service
from tq_iquant_shared.constants import TradeType, SignalType


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _order(**extra):
    order = OrderEvent(
        strategy_id=3, portfolio_id=2, stock_code="600000.SH",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=300, price=Decimal("10.00"),
        bar_time=datetime(2026, 11, 18, 10, 0),
        signal_name="open_sig",
    )
    for k, v in extra.items():
        setattr(order, k, v)
    return order


def test_persist_order_submitted_carries_progress_fields():
    db = _db()
    seq = SimpleNamespace(session_id=1)
    machine = OrderStateMachine(seq, lambda *a, **k: None)
    order = _order(_split_child=True, _split_seq=2, _split_total=3,
                   _split_group="sp-3-600000.SH-BUY-1")
    row = machine.persist_order_submitted(db, order)
    db.commit()

    assert row.split_group == "sp-3-600000.SH-BUY-1"
    assert row.split_seq == 2
    assert row.split_total == 3
    assert row.requote_attempt is None


def test_persist_plain_order_has_no_split_progress():
    db = _db()
    machine = OrderStateMachine(SimpleNamespace(session_id=1), lambda *a, **k: None)
    row = machine.persist_order_submitted(db, _order())
    db.commit()
    assert row.split_group is None and row.split_seq is None


def test_persist_requote_attempt():
    db = _db()
    machine = OrderStateMachine(SimpleNamespace(session_id=1), lambda *a, **k: None)
    row = machine.persist_order_submitted(
        db, _order(_requote_attempt=2))
    db.commit()
    assert row.requote_attempt == 2


def test_session_orders_aggregates_split_group():
    db = _db()
    db.add(LiveSession(id=1, name="s", mode="simulation", status="stopped"))
    db.add_all([
        LiveOrder(live_session_id=1, portfolio_strategy_id=2, strategy_id=3,
                  stock_code="600000.SH", trade_type="buy", order_type="limit",
                  price=Decimal("10.00"), quantity=200, filled_quantity=200,
                  status="filled", split_group="g1", split_seq=1, split_total=2),
        LiveOrder(live_session_id=1, portfolio_strategy_id=2, strategy_id=3,
                  stock_code="600000.SH", trade_type="buy", order_type="limit",
                  price=Decimal("10.00"), quantity=100, filled_quantity=0,
                  status="submitted", split_group="g1", split_seq=2,
                  split_total=2, requote_attempt=1),
    ])
    db.commit()

    items = live_service.session_orders(db, 1)
    assert len(items) == 2
    s = items[0]["split_summary"]
    assert s["total"] == 2
    assert s["filled_children"] == 1
    assert s["remaining_quantity"] == 100
    assert items[0]["requote_attempt"] in (None, 1)
