"""H1 拆单通道一致性：非 tdx 通道启用拆单时启动软提示。"""
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, Formula, PortfolioStrategy, Strategy
from core.services import live_service


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _session(db, *, split_enabled: bool, execution_mode: str = "auto"):
    formula = Formula(name="f1", content="c", engine="tq")
    db.add(formula)
    db.flush()
    ps = PortfolioStrategy(name="p1", stock_pool_id=1,
                           initial_capital=Decimal("100000"))
    db.add(ps)
    db.flush()
    db.add(Strategy(
        portfolio_id=ps.id, name="大单策略", formula_id=formula.id,
        period="1d", role="master", split_order_enabled=split_enabled,
        split_order_threshold=500000.0,
    ))
    db.commit()
    return live_service.create_session(
        db,
        {"name": "s1", "engine": "iquant", "execution_mode": execution_mode,
         "portfolio_ids": [ps.id]},
        "simulation",
    )["id"]


def test_warns_when_split_enabled_on_non_tdx_channel():
    db = _db()
    sid = _session(db, split_enabled=True)
    warns = live_service.split_channel_warnings(db, sid, "auto", "iquant_bridge")
    assert len(warns) == 1
    assert "不支持拆单" in warns[0]
    assert "大单策略" in warns[0]


def test_no_warning_on_tdx_channel():
    db = _db()
    sid = _session(db, split_enabled=True)
    assert live_service.split_channel_warnings(db, sid, "auto", "tdx_tq") == []


def test_no_warning_in_alert_mode():
    db = _db()
    sid = _session(db, split_enabled=True, execution_mode="alert")
    assert live_service.split_channel_warnings(
        db, sid, "alert", "alert") == []


def test_no_warning_when_split_disabled():
    db = _db()
    sid = _session(db, split_enabled=False)
    assert live_service.split_channel_warnings(
        db, sid, "auto", "iquant_bridge") == []
