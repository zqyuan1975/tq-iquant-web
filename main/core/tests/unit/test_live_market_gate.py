# -*- coding: utf-8 -*-
"""实盘市场多空门控接线：每日更新 / 异常沿用旧状态 / 同日幂等 / 白名单隔离。

LiveEngine 最小构造参照 test_live_engine.py 既有模式（内存 SQLite + MockTransport
dispatcher + BarPoller），注入 gate_provider 与 gate_portfolio_ids。
"""
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.http_bridge_dispatcher import HttpBridgeDispatcher
from core.engine.live_engine import LiveEngine
from core.engine.market_gate import MarketGateTracker
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager
from core.models import Base


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

    return factory, engine


def _engine(gate_provider, gate_portfolio_ids=None, portfolios=None):
    factory, _ = _db_factory()
    if portfolios is None:
        pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                                  daily_loss_limit=Decimal("0.05"))
        portfolios = [Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                                risk_manager=pm)]
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, json={"ok": True}))
    disp = HttpBridgeDispatcher(
        base_url="http://127.0.0.1:8790",
        client=httpx.Client(transport=transport))
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=portfolios, dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        gate_provider=gate_provider,
        gate_portfolio_ids={1} if gate_portfolio_ids is None else gate_portfolio_ids,
        benchmark_codes={1: "000001.SH"},
    )


def _tracker(states):
    t = MarketGateTracker()
    t.feed_history(states, {d: 60 for d in states})
    return t


def test_first_day_failure_leaves_none(caplog):
    """取数失败且此前无状态 → 保持 None（放行，绝不变成永不开仓）。"""
    def provider_raises(pid, d):
        raise ValueError("取数失败")
    eng = _engine(provider_raises)
    port = MagicMock(portfolio_id=1)
    port.current_market_gate = None
    eng._maybe_update_gate(port, date(2025, 6, 17))
    assert port.current_market_gate is None


def test_failure_keeps_previous_tracker(caplog):
    """成功后某日失败 → 沿用上一次 tracker（fail-open）。"""
    t1 = _tracker({date(2025, 6, 16): 1})
    calls = {"n": 0}

    def provider(pid, d):
        calls["n"] += 1
        if calls["n"] == 1:
            return t1
        raise ValueError("boom")

    eng = _engine(provider)
    port = MagicMock(portfolio_id=1)
    port.current_market_gate = None
    eng._maybe_update_gate(port, date(2025, 6, 17))
    assert port.current_market_gate is t1
    eng._maybe_update_gate(port, date(2025, 6, 18))
    assert port.current_market_gate is t1


def test_daily_idempotent():
    """同一交易日只取数一次（每组合每日一次的自然节流）。"""
    calls = {"n": 0}

    def provider(pid, d):
        calls["n"] += 1
        return MarketGateTracker()

    eng = _engine(provider)
    port = MagicMock(portfolio_id=1)
    port.current_market_gate = None
    eng._maybe_update_gate(port, date(2025, 6, 17))
    eng._maybe_update_gate(port, date(2025, 6, 17))
    assert calls["n"] == 1


def test_provider_none_is_noop():
    """gate_provider=None（默认）→ 完全不触碰 portfolio，行为逐字不变。"""
    eng = _engine(None)
    port = MagicMock(portfolio_id=1)
    port.current_market_gate = None
    eng._maybe_update_gate(port, date(2025, 6, 17))
    assert port.current_market_gate is None


def test_disabled_portfolio_not_polluted():
    """白名单外的组合不调 provider，current_market_gate 恒 None。"""
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port_enabled = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                             risk_manager=pm)
    port_disabled = Portfolio(portfolio_id=2, initial_capital=Decimal("100000"),
                              risk_manager=pm)
    calls = {"n": 0}

    def provider(pid, d):
        calls["n"] += 1
        return MarketGateTracker()

    eng = _engine(provider, gate_portfolio_ids={1},
                  portfolios=[port_enabled, port_disabled])
    d = date(2025, 6, 17)
    eng._maybe_update_gate(port_disabled, d)
    assert calls["n"] == 0
    assert port_disabled.current_market_gate is None
    eng._maybe_update_gate(port_enabled, d)
    assert calls["n"] == 1
    assert port_enabled.current_market_gate is not None
    eng._maybe_update_gate(port_disabled, date(2025, 6, 18))
    assert calls["n"] == 1
    assert port_disabled.current_market_gate is None
