"""G3：顺延单积压告警（默认关 + 每交易日去重）单元测试。"""
from datetime import datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import core.engine.live_engine as live_engine_mod
from core.engine.bar_poller import BarPoller
from core.engine.event import OrderEvent
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager
from core.models import Base
from tq_iquant_shared.constants import SignalType, TradeType

_STOCK = "600000.SH"


class _StubDispatcher:
    channel = "test"
    supports_remark = False
    supports_cancel = True
    supports_backfill = False

    def query_calendar(self, *args, **kwargs):
        raise RuntimeError("calendar unavailable in stub")

    def query_orders(self):
        return []

    def heartbeat(self):
        return True


def _engine(count: int = 0, days: int = 0):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    port = Portfolio(
        portfolio_id=1, initial_capital=Decimal("100000"),
        risk_manager=PortfolioRiskManager(
            max_drawdown=Decimal("0.2"),
            daily_loss_limit=Decimal("0.05")))
    disp = _StubDispatcher()
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=BarPoller(disp, [_STOCK], period="1m", count=10),
        db_session_factory=factory,
        deferred_backlog_warn_count=count,
        deferred_backlog_warn_days=days,
    )


def _defer(engine, bar_time: datetime):
    order = OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=_STOCK,
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("9.3"), bar_time=bar_time,
        signal_name="open_sig",
    )
    engine._signal_bar_deferred.append(order)


def _capture(engine, monkeypatch):
    events = []
    notes = []
    engine._emit = lambda kind, payload: events.append((kind, payload))
    monkeypatch.setattr(live_engine_mod, "notify",
                        lambda *a, **k: notes.append((a, k)))
    return events, notes


def test_default_off_no_events(monkeypatch):
    engine = _engine()
    _defer(engine, datetime(2026, 8, 4, 15, 0))
    events, notes = _capture(engine, monkeypatch)
    engine._check_deferred_backlog(datetime(2026, 8, 11, 9, 31))
    assert events == [] and notes == []


def test_count_threshold_warns_once_per_day(monkeypatch):
    engine = _engine(count=2)
    _defer(engine, datetime(2026, 8, 4, 15, 0))
    _defer(engine, datetime(2026, 8, 4, 15, 0))
    events, notes = _capture(engine, monkeypatch)
    now = datetime(2026, 8, 5, 9, 31)

    engine._check_deferred_backlog(now)
    engine._check_deferred_backlog(now)

    risk = [p for k, p in events if k == "risk"]
    assert len(risk) == 1
    assert risk[0]["gate"] == "deferred_backlog"
    assert risk[0]["rule"] == "count"
    assert risk[0]["count"] == 2
    assert risk[0]["threshold"] == 2
    assert len(notes) == 1

    # 下一交易日重新提醒
    engine._check_deferred_backlog(datetime(2026, 8, 6, 9, 31))
    assert len([p for k, p in events if k == "risk" and p["rule"] == "count"]) == 2


def test_age_threshold_uses_trading_days(monkeypatch):
    engine = _engine(days=3)
    _defer(engine, datetime(2026, 8, 4, 15, 0))
    events, _ = _capture(engine, monkeypatch)
    engine._check_deferred_backlog(datetime(2026, 8, 11, 9, 31))

    risk = [p for k, p in events if k == "risk"]
    assert len(risk) == 1
    assert risk[0]["rule"] == "age"
    assert risk[0]["age_trading_days"] > 3
    assert risk[0]["oldest_signal_date"] == "2026-08-04"


def test_age_not_triggered_when_within_threshold(monkeypatch):
    engine = _engine(days=3)
    _defer(engine, datetime(2026, 8, 4, 15, 0))
    events, _ = _capture(engine, monkeypatch)
    engine._check_deferred_backlog(datetime(2026, 8, 5, 9, 31))
    assert events == []


def test_empty_queue_is_silent(monkeypatch):
    engine = _engine(count=1, days=1)
    events, _ = _capture(engine, monkeypatch)
    engine._check_deferred_backlog(datetime(2026, 8, 11, 9, 31))
    assert events == []
