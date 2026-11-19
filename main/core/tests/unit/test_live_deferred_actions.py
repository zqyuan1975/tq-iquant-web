"""G1：待放行顺延委托的人工放行 / 放弃（2026-11-18）单元测试。"""
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import OrderEvent
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import Base, LiveDecisionEvent, LiveOrder
from core.tq.utils import TDXLockTimeout
from tq_iquant_shared.constants import SignalType, TradeType

_STOCK = "600000.SH"
_SIGNAL_DAY = datetime(2026, 8, 4, 15, 0)


def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    return SessionLocal, engine


def _portfolio():
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1m",
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("0.1"),
    )
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port, ctx


class _StubDispatcher:
    channel = "test"
    supports_remark = False
    supports_cancel = True
    supports_backfill = False
    last_enqueued_order_id = None
    last_unconfirmed_order_id = None
    last_broker_order_id = None
    last_unconfirmed_message = None
    last_cancel_result = None

    def __init__(self, exc=None):
        self._exc = exc
        self.placed = []

    def query_calendar(self, *args, **kwargs):
        raise RuntimeError("calendar unavailable in stub")

    def query_orders(self):
        return []

    def heartbeat(self):
        return True

    def order_id(self, order):
        return ("stub" + str(order.stock_code)
                + str(order.bar_time)).ljust(32, "0")[:32]

    def place_order(self, order, price_type=None):
        self.placed.append(order)
        if self._exc is not None:
            raise self._exc
        from core.engine.event import TradeEvent
        return TradeEvent(
            strategy_id=order.strategy_id, portfolio_id=order.portfolio_id,
            stock_code=order.stock_code, trade_type=order.trade_type,
            price=order.price, quantity=order.quantity,
            amount=order.price * order.quantity,
            commission=Decimal("5"), stamp_duty=Decimal("0"),
            trade_time=order.bar_time, signal_type=order.signal_type,
            signal_name=order.signal_name,
        )

    def check_price_limit(self, code, side, price):
        return None

    def price_limit_bounds(self, code):
        return None

    def get_positions(self):
        return []

    def get_cash(self):
        return {}

    def execution_receipts(self):
        return []


def _engine(factory, port, disp):
    poller = BarPoller(disp, [_STOCK], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    # 固定到交易时段（人工放行按当下重走闸门；09:31 允许盘中提交）
    engine.ctx.clock = lambda: datetime(2026, 8, 5, 9, 31)
    return engine


def _order(bar_time=None):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=_STOCK,
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("9.3"),
        bar_time=bar_time or _SIGNAL_DAY, signal_name="open_sig",
    )


def _deferred_row(db, bar_time=None, status="deferred", retry=2):
    row = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code=_STOCK, trade_type="buy", order_type="limit",
        price=Decimal("9.3"), quantity=100, status=status,
        bar_time=bar_time or _SIGNAL_DAY,
        error_message="gate=x;deferred_retry:n=%d;reason=test" % retry,
        signal_type="open", signal_name="open_sig",
    )
    db.add(row)
    db.commit()
    return row


def _queue_order(engine, row):
    order = _order()
    order._deferred_row_id = row.id
    engine._signal_bar_deferred.append(order)
    return order


# ---- 列表 ----

def test_list_deferred_orders_marks_memory_and_retry():
    factory, _ = _db_factory()
    port, _ = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row1 = _deferred_row(db, retry=2)
    row2 = _deferred_row(db, retry=0)
    _queue_order(engine, row1)

    items = engine.list_deferred_orders()
    by_id = {i["id"]: i for i in items}
    assert by_id[row1.id]["in_memory"] is True
    assert by_id[row1.id]["retry_count"] == 2
    assert by_id[row1.id]["signal_bar_date"] == "2026-08-04"
    assert by_id[row2.id]["in_memory"] is False
    db.close()


# ---- 放行 ----

def test_release_deferred_order_submits_and_clears_old_row():
    factory, _ = _db_factory()
    port, _ = _portfolio()
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp)
    engine._refresh_release_price = lambda order: None
    db = factory()
    row = _deferred_row(db)
    row_id = row.id
    target = _queue_order(engine, row)

    result = engine.release_deferred_order(row_id)
    assert result["status"] == "released"
    assert engine._signal_bar_deferred == []
    assert target in disp.placed

    db.expire_all()
    assert db.query(LiveOrder).filter(LiveOrder.id == row_id).first() is None
    rows = db.query(LiveOrder).all()
    assert len(rows) == 1
    assert rows[0].status in ("submitted", "filled")

    events = db.query(LiveDecisionEvent).filter(
        LiveDecisionEvent.gate == "deferred_manual").all()
    assert [e.action for e in events] == ["release"]
    db.close()


def test_release_rejects_non_deferred_status():
    factory, _ = _db_factory()
    port, _ = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row = _deferred_row(db, status="canceled")
    with pytest.raises(ValueError):
        engine.release_deferred_order(row.id)
    db.close()


def test_release_rejects_unknown_and_non_auto():
    factory, _ = _db_factory()
    port, _ = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    with pytest.raises(LookupError):
        engine.release_deferred_order(999)

    db = factory()
    row = _deferred_row(db)
    engine.execution_mode = "alert"
    with pytest.raises(ValueError):
        engine.release_deferred_order(row.id)
    db.close()


def test_release_not_in_memory_rejected():
    factory, _ = _db_factory()
    port, _ = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row = _deferred_row(db)
    with pytest.raises(ValueError):
        engine.release_deferred_order(row.id)
    db.close()


def test_release_submit_error_requeues_and_keeps_deferred_row():
    """发单异常被引擎判定为可重排拒单 → 释放动作回 blocked，不产生双份委托。"""
    factory, _ = _db_factory()
    port, _ = _portfolio()
    disp = _StubDispatcher(exc=TDXLockTimeout("tq lock timeout"))
    engine = _engine(factory, port, disp)
    engine._refresh_release_price = lambda order: None
    db = factory()
    row = _deferred_row(db)
    target = _queue_order(engine, row)

    result = engine.release_deferred_order(row.id)

    assert result["status"] == "blocked"
    assert target in engine._signal_bar_deferred
    db.expire_all()
    deferred = db.query(LiveOrder).filter(
        LiveOrder.status == "deferred").all()
    assert deferred and deferred[0].id == target._deferred_row_id
    db.close()


# ---- 放弃 ----

def test_abandon_deferred_order_marks_canceled_and_audits():
    factory, _ = _db_factory()
    port, _ = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row = _deferred_row(db)
    _queue_order(engine, row)

    result = engine.abandon_deferred_order(row.id, "人工判断信号失效")
    assert result["status"] == "abandoned"
    assert engine._signal_bar_deferred == []

    db.expire_all()
    kept = db.query(LiveOrder).filter(LiveOrder.id == row.id).first()
    assert kept.status == "canceled"
    assert "人工放弃" in (kept.error_message or "")
    events = db.query(LiveDecisionEvent).filter(
        LiveDecisionEvent.gate == "deferred_manual").all()
    assert any(e.action == "abandon" for e in events)
    db.close()


def test_abandon_requires_deferred_status_and_confirm_less_is_api_level():
    factory, _ = _db_factory()
    port, _ = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row = _deferred_row(db, status="submitted")
    with pytest.raises(ValueError):
        engine.abandon_deferred_order(row.id)
    with pytest.raises(LookupError):
        engine.abandon_deferred_order(123456)
    db.close()
