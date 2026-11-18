"""G4：盘前效果统计（按日/周聚合）单元测试（2026-11-18）。"""
from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

import core.engine.live_engine as live_engine_mod
from core.engine.bar_poller import BarPoller
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager
from core.models import Base, LiveDecisionEvent, LiveOrder, LiveSession
from core.services.live_service import pre_open_stats

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

    def get_positions(self):
        return []

    def get_cash(self):
        return {}


def _db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return engine, sessionmaker(bind=engine, expire_on_commit=False)


def _session(db, sid=1):
    db.add(LiveSession(id=sid, name="s", mode="simulation", status="stopped"))
    db.commit()


def _decision(db, bar_time, action, sid=1, gate="pre_open_auction"):
    db.add(LiveDecisionEvent(
        live_session_id=sid, gate=gate, layer="live_gate",
        action=action, bar_time=bar_time,
    ))


def _order(db, bar_time, status, sid=1):
    db.add(LiveOrder(
        live_session_id=sid, portfolio_strategy_id=1, strategy_id=1,
        stock_code=_STOCK, trade_type="buy", order_type="limit",
        price=Decimal("9.3"), quantity=100, status=status, bar_time=bar_time,
    ))


def test_day_bucket_counts_all_metrics():
    _, factory = _db()
    db = factory()
    _session(db)
    # 决策：2 次报出、1 次拦截、1 次越价撤单并重排
    _decision(db, datetime(2026, 8, 4, 9, 16), "release")
    _decision(db, datetime(2026, 8, 4, 9, 17), "release")
    _decision(db, datetime(2026, 8, 4, 9, 18), "block")
    _decision(db, datetime(2026, 8, 4, 9, 20), "cancel_requeue")
    # 委托：2 笔盘前成交 + 1 笔仍积压的顺延单
    _order(db, datetime(2026, 8, 4, 9, 20), "filled")
    _order(db, datetime(2026, 8, 4, 9, 21), "filled")
    _order(db, datetime(2026, 8, 4, 15, 0), "deferred")
    db.commit()

    data = pre_open_stats(db, 1, date(2026, 8, 4), date(2026, 8, 4), "day")

    assert data["group_by"] == "day"
    assert [i["bucket"] for i in data["items"]] == ["2026-08-04"]
    row = data["items"][0]
    assert row["reported"] == 2
    assert row["filled"] == 2
    assert row["cancel_crossed"] == 1
    assert row["requeued"] == 1
    assert row["blocked"] == 1
    assert row["deferred"] == 1
    assert data["totals"] == {
        "reported": 2, "filled": 2, "cancel_crossed": 1,
        "requeued": 1, "blocked": 1, "deferred": 1,
    }
    db.close()


def test_week_bucket_merges_days():
    _, factory = _db()
    db = factory()
    _session(db)
    _decision(db, datetime(2026, 8, 4, 9, 16), "release")
    _decision(db, datetime(2026, 8, 5, 9, 16), "release")
    _order(db, datetime(2026, 8, 4, 9, 20), "filled")
    db.commit()

    data = pre_open_stats(db, 1, date(2026, 8, 4), date(2026, 8, 9), "week")

    iso = date(2026, 8, 4).isocalendar()
    expected_key = "%04d-W%02d" % (iso[0], iso[1])
    assert [i["bucket"] for i in data["items"]] == [expected_key]
    assert data["items"][0]["reported"] == 2
    assert data["items"][0]["filled"] == 1
    assert data["totals"]["reported"] == 2
    db.close()


def test_empty_range_returns_zero_totals():
    _, factory = _db()
    db = factory()
    _session(db)

    data = pre_open_stats(db, 1, date(2026, 8, 1), date(2026, 8, 7), "day")

    assert data["items"] == []
    assert data["totals"] == {
        "reported": 0, "filled": 0, "cancel_crossed": 0,
        "requeued": 0, "blocked": 0, "deferred": 0,
    }
    db.close()


def test_unknown_session_returns_none():
    _, factory = _db()
    db = factory()
    assert pre_open_stats(db, 999, date(2026, 8, 1), date(2026, 8, 7)) is None
    db.close()


def test_invalid_arguments_raise():
    _, factory = _db()
    db = factory()
    _session(db)
    with pytest.raises(ValueError):
        pre_open_stats(db, 1, date(2026, 8, 1), date(2026, 8, 7), "month")
    with pytest.raises(ValueError):
        pre_open_stats(db, 1, date(2026, 8, 7), date(2026, 8, 1), "day")
    db.close()


def test_reconciliation_report_includes_pre_open_detail(monkeypatch):
    engine_db, factory = _db()
    db = factory()
    _session(db)
    _decision(db, datetime(2026, 8, 4, 9, 16), "release")
    _order(db, datetime(2026, 8, 4, 9, 20), "filled")
    db.commit()

    port = Portfolio(
        portfolio_id=1, initial_capital=Decimal("100000"),
        risk_manager=PortfolioRiskManager(
            max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05")))
    disp = _StubDispatcher()
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=BarPoller(disp, [_STOCK], period="1m", count=10),
        db_session_factory=factory,
    )
    monkeypatch.setattr(live_engine_mod, "now_shanghai",
                        lambda: datetime(2026, 8, 4, 15, 6))

    report = engine._build_reconciliation_report(db)

    assert report["detail"]["pre_open"]["reported"] == 1
    assert report["detail"]["pre_open"]["filled"] == 1
    db.close()
