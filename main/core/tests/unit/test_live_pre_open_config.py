"""G2：会话级盘前集合竞价参数覆盖（2026-11-18）单元测试。"""
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, LiveSession, LiveSessionPendingChange, PortfolioStrategy
from core.services import live_service


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _portfolio(db, name="p1"):
    p = PortfolioStrategy(name=name, stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    return p


def _session(db, pids, status="stopped", **extra):
    data = {
        "name": "s1", "engine": "tdx", "execution_mode": "alert",
        "portfolio_ids": pids, "capital_pool_mode": "off",
    }
    data.update(extra)
    return live_service.create_session(db, data, "simulation")


# ---- 纯函数：归一 / 解析 ----

def test_normalize_pre_open_overrides_three_state():
    patch, error = live_service.normalize_pre_open_overrides({
        "pre_open_auction_enabled": None,
        "pre_open_auction_end_time": "",
        "pre_open_auction_price_bias_pct": None,
    })
    assert error is None
    assert patch == {
        "pre_open_auction_enabled": None,
        "pre_open_auction_end_time": None,
        "pre_open_auction_price_bias_pct": None,
    }


def test_normalize_pre_open_overrides_accepts_valid_values():
    patch, error = live_service.normalize_pre_open_overrides({
        "pre_open_auction_enabled": False,
        "pre_open_auction_end_time": "9:25",
        "pre_open_auction_price_bias_pct": "1.5",
    })
    assert error is None
    assert patch["pre_open_auction_enabled"] is False
    assert patch["pre_open_auction_end_time"] == "09:25"
    assert patch["pre_open_auction_price_bias_pct"] == Decimal("1.500000")


def test_normalize_pre_open_overrides_rejects_invalid_values():
    _, error = live_service.normalize_pre_open_overrides(
        {"pre_open_auction_end_time": "09:40"})
    assert error is not None and error[0] == 400
    _, error = live_service.normalize_pre_open_overrides(
        {"pre_open_auction_price_bias_pct": 11})
    assert error is not None and error[0] == 400
    _, error = live_service.normalize_pre_open_overrides(
        {"pre_open_auction_enabled": "maybe"})
    assert error is not None and error[0] == 400


def test_resolve_pre_open_inherits_null_and_applies_override():
    global_cfg = {
        "pre_open_auction_enabled": True,
        "pre_open_auction_end_time": "09:20",
        "pre_open_auction_price_bias_pct": 2.0,
    }
    row = SimpleNamespace(
        pre_open_auction_enabled=None,
        pre_open_auction_end_time=None,
        pre_open_auction_price_bias_pct=None,
    )
    cfg, overrides = live_service.resolve_pre_open_auction_config(row, global_cfg)
    assert cfg == global_cfg
    assert overrides == []

    row = SimpleNamespace(
        pre_open_auction_enabled=False,
        pre_open_auction_end_time="09:25",
        pre_open_auction_price_bias_pct=Decimal("1.250000"),
    )
    cfg, overrides = live_service.resolve_pre_open_auction_config(row, global_cfg)
    assert cfg["pre_open_auction_enabled"] is False
    assert cfg["pre_open_auction_end_time"] == "09:25"
    assert cfg["pre_open_auction_price_bias_pct"] == 1.25
    assert set(overrides) == {"enabled", "end_time", "price_bias_pct"}


def test_pre_open_auction_effective_marks_source():
    row = SimpleNamespace(
        pre_open_auction_enabled=None,
        pre_open_auction_end_time=None,
        pre_open_auction_price_bias_pct=None,
    )
    effective = live_service.pre_open_auction_effective(row, {})
    assert effective["source"] == "global"
    assert effective["enabled"] is True
    assert effective["end_time"] == "09:20"
    assert effective["price_bias_pct"] == 2.0

    row.pre_open_auction_enabled = False
    effective = live_service.pre_open_auction_effective(row, {})
    assert effective["source"] == "session"
    assert effective["enabled"] is False
    assert effective["overrides"] == ["enabled"]


# ---- 持久化 / 编辑 ----

def test_model_has_pre_open_override_columns():
    columns = set(LiveSession.__table__.columns.keys())
    assert {"pre_open_auction_enabled", "pre_open_auction_end_time",
            "pre_open_auction_price_bias_pct"} <= columns


def test_create_session_persists_overrides_and_echoes_effective():
    db = _db()
    p = _portfolio(db)
    result = _session(
        db, [p.id], pre_open_auction_enabled=True,
        pre_open_auction_end_time="09:25",
        pre_open_auction_price_bias_pct=1.5,
    )
    assert result["pre_open_auction_enabled"] is True
    assert result["pre_open_auction_end_time"] == "09:25"
    assert result["pre_open_auction_effective"]["source"] == "session"
    assert result["pre_open_auction_effective"]["end_time"] == "09:25"

    session = db.query(LiveSession).filter_by(id=result["id"]).first()
    assert session.pre_open_auction_enabled is True
    assert session.pre_open_auction_end_time == "09:25"
    assert float(session.pre_open_auction_price_bias_pct) == 1.5


def test_create_session_without_overrides_follows_global():
    db = _db()
    p = _portfolio(db)
    result = _session(db, [p.id])
    session = db.query(LiveSession).filter_by(id=result["id"]).first()
    assert session.pre_open_auction_enabled is None
    assert session.pre_open_auction_end_time is None
    assert session.pre_open_auction_price_bias_pct is None
    assert result["pre_open_auction_effective"]["source"] == "global"


def test_stopped_session_edit_applies_pre_open_overrides_now():
    db = _db()
    p = _portfolio(db)
    sid = _session(db, [p.id])["id"]

    ok, result = live_service.update_session(
        db, sid,
        {"pre_open_auction_enabled": False,
         "pre_open_auction_end_time": "09:26",
         "pre_open_auction_price_bias_pct": 0.5},
        {},
    )
    assert ok is True
    assert result["effective_at"] == "now"
    assert set(result["applied"]) == {
        "pre_open_auction_enabled", "pre_open_auction_end_time",
        "pre_open_auction_price_bias_pct"}
    session = db.query(LiveSession).filter_by(id=sid).first()
    assert session.pre_open_auction_enabled is False
    assert session.pre_open_auction_end_time == "09:26"
    assert float(session.pre_open_auction_price_bias_pct) == 0.5


def test_running_session_edit_queues_pre_open_overrides():
    db = _db()
    p = _portfolio(db)
    sid = _session(db, [p.id])["id"]
    engines = {sid: SimpleNamespace(
        bridge_online=True, pending_orders_count=0, last_backfill_time=None,
        reconcile_mismatches=[], _calendar=None)}

    ok, result = live_service.update_session(
        db, sid, {"pre_open_auction_end_time": "09:28"}, engines)
    assert ok is True
    assert result["effective_at"] == "next_start"
    assert result["pending"] == ["pre_open_auction_end_time"]
    session = db.query(LiveSession).filter_by(id=sid).first()
    assert session.pre_open_auction_end_time is None
    pending = db.query(LiveSessionPendingChange).filter_by(
        live_session_id=sid, field="pre_open_auction_end_time").first()
    assert pending is not None and pending.status == "pending"


def test_edit_rejects_invalid_pre_open_end_time():
    db = _db()
    p = _portfolio(db)
    sid = _session(db, [p.id])["id"]
    ok, error = live_service.update_session(
        db, sid, {"pre_open_auction_end_time": "10:00"}, {})
    assert ok is False
    assert error[0] == 400
