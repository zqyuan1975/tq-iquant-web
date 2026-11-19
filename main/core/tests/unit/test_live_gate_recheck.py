# -*- coding: utf-8 -*-
"""E1：执行前重判门控（默认关）单元测试。

覆盖：开关关闭放行 / 空头拦 BUY / 卖出不拦 / 无 tracker 放行 /
会话创建与编辑透传开关。
"""
from datetime import date, datetime
from decimal import Decimal

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import OrderEvent
from core.engine.http_bridge_dispatcher import HttpBridgeDispatcher
from core.engine.live_engine import LiveEngine
from core.engine.market_gate import MarketGateTracker
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager
from core.models import Base, LiveSession, PortfolioStrategy
from core.services import live_service
from tq_iquant_shared.constants import SignalType, TradeType


def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    def factory():
        return SessionLocal()

    return factory


def _engine(recheck=False):
    factory = _db_factory()
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"ok": True}))
    disp = HttpBridgeDispatcher(
        base_url="http://127.0.0.1:8790",
        client=httpx.Client(transport=transport))
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    eng = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        gate_provider=None, gate_portfolio_ids={1},
        gate_recheck_on_execute=recheck,
    )
    return eng, port


def _tracker(states):
    t = MarketGateTracker()
    t.feed_history(states, {d: 60 for d in states})
    return t


def _order(trade_type=TradeType.BUY):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000.SH",
        trade_type=trade_type, signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("10"),
        bar_time=datetime(2025, 6, 16, 10, 0),
        signal_name="open_sig",
    )


def test_recheck_disabled_passes_even_in_bear():
    """默认关：空头 tracker 也不拦，行为与接线前一致。"""
    eng, port = _engine(recheck=False)
    port.current_market_gate = _tracker({date(2025, 6, 16): -1})
    assert eng._gate_recheck_blocked(
        port, _order(), datetime(2025, 6, 17, 9, 31)) is None


def test_recheck_blocks_buy_in_bear():
    """开启：执行日前一收盘空头 → 拦下 BUY 并给出原因。"""
    eng, port = _engine(recheck=True)
    port.current_market_gate = _tracker({date(2025, 6, 16): -1})
    reason = eng._gate_recheck_blocked(
        port, _order(), datetime(2025, 6, 17, 9, 31))
    assert reason is not None
    assert "空头" in reason and "2025-06-17" in reason


def test_recheck_passes_buy_in_bull():
    eng, port = _engine(recheck=True)
    port.current_market_gate = _tracker({date(2025, 6, 16): 1})
    assert eng._gate_recheck_blocked(
        port, _order(), datetime(2025, 6, 17, 9, 31)) is None


def test_recheck_never_blocks_sell():
    """卖出/风控不受门控约束：空头也放行。"""
    eng, port = _engine(recheck=True)
    port.current_market_gate = _tracker({date(2025, 6, 16): -1})
    assert eng._gate_recheck_blocked(
        port, _order(TradeType.SELL), datetime(2025, 6, 17, 9, 31)) is None


def test_recheck_without_tracker_passes():
    eng, port = _engine(recheck=True)
    port.current_market_gate = None
    assert eng._gate_recheck_blocked(
        port, _order(), datetime(2025, 6, 17, 9, 31)) is None


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def test_session_create_and_edit_persist_flag():
    db = _db()
    p = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    created = live_service.create_session(db, {
        "name": "s1", "engine": "tdx", "execution_mode": "alert",
        "portfolio_ids": [p.id], "gate_recheck_on_execute": True,
    }, "simulation")
    assert created["gate_recheck_on_execute"] is True
    sid = created["id"]
    assert db.query(LiveSession).filter_by(id=sid).first() \
        .gate_recheck_on_execute is True

    ok, result = live_service.update_session(
        db, sid, {"gate_recheck_on_execute": False}, {})
    assert ok is True
    assert result["effective_at"] == "now"
    assert result["session"]["gate_recheck_on_execute"] is False
