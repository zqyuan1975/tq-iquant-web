# -*- coding: utf-8 -*-
"""E2：每日市场门控快照落库与查询。"""
from datetime import date, timedelta

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.live_engine import LiveEngine
from core.engine.market_gate import MarketGateTracker
from core.models import Base, LiveGateSnapshot, PortfolioStrategy
from core.services import live_service


def _make_db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    return SessionLocal


def _tracker(states):
    t = MarketGateTracker()
    t.feed_history(states, {d: 60 for d in states})
    return t


class _FakeDispatcher:
    def query_calendar(self):
        return []


def _setup():
    SessionLocal = _make_db()
    db = SessionLocal()
    p = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    created = live_service.create_session(db, {
        "name": "s1", "engine": "tdx", "execution_mode": "alert",
        "portfolio_ids": [p.id],
    }, "simulation")
    sid = created["id"]
    eng = LiveEngine(
        session_id=sid, portfolios=[], dispatcher=_FakeDispatcher(),
        bar_poller=object(), db_session_factory=SessionLocal,
        gate_specs={p.id: {"benchmark_index": "000300.SH",
                           "market_gate_engine": "python"}},
    )
    return db, SessionLocal, eng, sid, p.id


def test_persist_gate_snapshot_upserts_one_row():
    db, _factory, eng, sid, pid = _setup()
    today = date.today()
    prev = today - timedelta(days=1)
    tracker = _tracker({prev - timedelta(days=3): 1, prev: -1})
    eng._persist_gate_snapshot(pid, today, tracker)
    eng._persist_gate_snapshot(pid, today, tracker)
    rows = db.query(LiveGateSnapshot).filter_by(live_session_id=sid).all()
    assert len(rows) == 1
    row = rows[0]
    assert row.snap_date == today
    assert row.direction == -1          # 右移一日：用前一交易日收盘状态
    assert row.strength == 60           # 强度原值（0–100 计分）
    assert row.benchmark_index == "000300.SH"
    assert row.gate_engine == "python"
    assert row.as_of == prev


def test_list_gate_snapshots_serializes():
    db, _factory, eng, sid, pid = _setup()
    today = date.today()
    prev = today - timedelta(days=1)
    eng._persist_gate_snapshot(pid, today, _tracker({prev: -1}))
    items = live_service.list_gate_snapshots(db, sid)
    assert len(items) == 1
    item = items[0]
    assert item["snap_date"] == today.isoformat()
    assert item["portfolio_id"] == pid
    assert item["direction"] == -1
    assert item["benchmark_index"] == "000300.SH"
    assert item["as_of"] == prev.isoformat()
    assert item["gate_engine"] == "python"


def test_list_gate_snapshots_missing_session_returns_none():
    db = _make_db()()
    assert live_service.list_gate_snapshots(db, 99999) is None
