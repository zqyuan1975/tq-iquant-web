"""H2 拆单剩余量持久化与一键补发。"""
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.engine.event import OrderEvent
from core.engine.live_engine import LiveEngine
from core.models import (
    Base, LiveSession, LiveSessionPendingChange, PortfolioStrategy,
)
from core.services import live_service
from tq_iquant_shared.constants import TradeType, SignalType


def _make():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    maker = sessionmaker(bind=engine)
    return maker, maker()


def _session(db):
    ps = PortfolioStrategy(name="p1", stock_pool_id=1,
                           initial_capital=Decimal("100000"))
    db.add(ps)
    db.commit()
    row = LiveSession(name="s1", mode="simulation", status="stopped",
                      engine="tdx", execution_mode="auto")
    db.add(row)
    db.commit()
    return row.id, ps.id


def _child(strategy_id, qty, price="10.00", tt=TradeType.BUY):
    return OrderEvent(
        strategy_id=strategy_id, portfolio_id=1, stock_code="600000.SH",
        trade_type=tt, signal_type=SignalType.OPEN, quantity=qty,
        price=Decimal(price), bar_time=datetime(2026, 11, 18, 10, 0),
        signal_name="open_sig", period="1d",
    )


def test_persist_writes_one_row_per_pending_child_and_replaces_old():
    maker, db = _make()
    sid, pid = _session(db)
    # 旧快照：本次写入应被作废
    db.add(LiveSessionPendingChange(
        live_session_id=sid, portfolio_strategy_id=pid, field="split_remainder",
        old_value="000001.SZ|old", new_value="BUY|100|9.00|OPEN|1d",
        status="pending",
    ))
    db.commit()

    eng = SimpleNamespace(session_id=sid, _db_session_factory=maker)
    entries = [{
        "portfolio": SimpleNamespace(portfolio_id=pid),
        "period": "1d",
        "inflight": _child(7, 100),
        "pending": [_child(7, 200), _child(7, 300, tt=TradeType.SELL)],
    }]
    LiveEngine._persist_split_remainders_now(eng, entries)

    rows = db.query(LiveSessionPendingChange).filter_by(
        live_session_id=sid, field="split_remainder").all()
    assert sum(1 for r in rows if r.status == "pending") == 2
    assert sum(1 for r in rows if r.status == "discarded") == 1
    pending = [r for r in rows if r.status == "pending"]
    assert {r.strategy_id for r in pending} == {7}
    assert any(r.new_value.startswith("SELL|300") for r in pending)


def test_list_split_remainders_parses_rows():
    maker, db = _make()
    sid, pid = _session(db)
    db.add(LiveSessionPendingChange(
        live_session_id=sid, portfolio_strategy_id=pid, strategy_id=3,
        field="split_remainder", old_value="600000.SH|trailing_stop",
        new_value="SELL|500|9.87|CLOSE|1d", status="pending",
    ))
    db.commit()

    items = live_service.list_split_remainders(db, sid)
    assert len(items) == 1
    item = items[0]
    assert item["stock_code"] == "600000.SH"
    assert item["trade_type"] == "SELL"
    assert item["quantity"] == 500
    assert item["price"] == 9.87
    assert item["signal_type"] == "CLOSE"
    assert item["period"] == "1d"
    assert item["signal_name"] == "trailing_stop"


def test_reorder_marks_accepted_rows_applied():
    maker, db = _make()
    sid, pid = _session(db)
    db.add_all([
        LiveSessionPendingChange(
            live_session_id=sid, portfolio_strategy_id=pid, strategy_id=3,
            field="split_remainder", old_value="600000.SH|open_sig",
            new_value="BUY|100|10.00|OPEN|1d", status="pending"),
        LiveSessionPendingChange(
            live_session_id=sid, portfolio_strategy_id=pid, strategy_id=3,
            field="split_remainder", old_value="000001.SZ|open_sig",
            new_value="BUY|200|10.00|OPEN|1d", status="pending"),
    ])
    db.commit()
    items = live_service.list_split_remainders(db, sid)

    class FakeEngine:
        def reorder_split_remainders(self, payload):
            return {"results": [
                {"id": payload[0]["id"], "status": "accepted"},
                {"id": payload[1]["id"], "status": "blocked"},
            ]}

    result, error = live_service.reorder_split_remainders(
        db, sid, FakeEngine())
    assert error is None
    assert result["accepted"] == 1
    assert result["remaining"] == 1
    rows = {r.id: r.status for r in db.query(LiveSessionPendingChange).filter_by(
        live_session_id=sid, field="split_remainder").all()}
    assert rows[items[0]["id"]] == "applied"
    assert rows[items[1]["id"]] == "pending"


def test_apply_pending_changes_leaves_split_rows_pending():
    maker, db = _make()
    sid, pid = _session(db)
    row = LiveSessionPendingChange(
        live_session_id=sid, portfolio_strategy_id=pid, strategy_id=3,
        field="split_remainder", old_value="600000.SH|open_sig",
        new_value="BUY|100|10.00|OPEN|1d", status="pending",
    )
    db.add(row)
    db.commit()

    live_service.apply_pending_changes(db, sid)
    db.refresh(row)
    assert row.status == "pending"
