"""N2/K7 理论 vs 实际对照 单测（service 层，内存 SQLite）。"""

from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.models import (
    Base, BacktestRecord, BacktestDailySnapshot, BacktestTrade,
    LiveSession, LivePortfolioDaily, LiveTrade, LiveOrder,
)
from core.services import live_service as svc


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False},
        poolclass=StaticPool)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    yield db
    db.close()


def _seed(db, bound=True):
    rec = BacktestRecord(
        portfolio_strategy_id=1, name="理论回测",
        start_date=date(2026, 7, 1), end_date=date(2026, 7, 31),
        status="completed",
    )
    db.add(rec)
    db.flush()
    session = LiveSession(
        name="实盘A", mode="paper", status="running",
        backtest_record_id=rec.id if bound else None,
    )
    db.add(session)
    db.flush()

    # 理论曲线：100000 → 110000（+10%）
    for i, value in enumerate((100000, 105000, 110000)):
        db.add(BacktestDailySnapshot(
            backtest_record_id=rec.id, target_type="portfolio", target_id=1,
            snap_date=date(2026, 7, 29) + timedelta(days=i),
            total_value=Decimal(str(value)), cash=Decimal("0"),
            market_value=Decimal(str(value)),
        ))
    db.add(BacktestTrade(
        backtest_record_id=rec.id, strategy_id=1, signal_name="open_sig",
        signal_type="OPEN", stock_code="000001.SZ", trade_type="BUY",
        price=Decimal("10"), quantity=1000, amount=Decimal("10000"),
        commission=Decimal("5"), stamp_duty=Decimal("0"),
        transfer_fee=Decimal("0"), bar_time=datetime(2026, 7, 29, 10),
    ))

    # 实盘曲线：200000 → 214000（+7%）
    for i, value in enumerate((200000, 207000, 214000)):
        db.add(LivePortfolioDaily(
            live_session_id=session.id, portfolio_strategy_id=1,
            trade_date=date(2026, 7, 29) + timedelta(days=i),
            total_value=Decimal(str(value)), risk_value=Decimal(str(value)),
        ))
    db.add(LiveTrade(
        live_session_id=session.id, portfolio_strategy_id=1, strategy_id=1,
        stock_code="000001.SZ", trade_type="BUY", price=Decimal("10.2"),
        quantity=1000, amount=Decimal("10200"), commission=Decimal("5.1"),
        stamp_duty=Decimal("0"), trade_time=datetime(2026, 7, 29, 10, 3),
    ))
    for qty, filled in ((100, 100), (200, 200), (100, 40)):
        db.add(LiveOrder(
            live_session_id=session.id, portfolio_strategy_id=1, strategy_id=1,
            stock_code="000001.SZ", trade_type="BUY", order_type="limit",
            quantity=qty, filled_quantity=filled, status="filled",
            bar_time=datetime(2026, 7, 29, 10, 3),
        ))
    db.commit()
    return session, rec


def test_theory_vs_live_bound_curves_and_slippage(db_session):
    session, rec = _seed(db_session)

    out = svc.compute_theory_vs_live(db_session, session)

    assert out["bound"] is True
    assert out["backtest_record_id"] == rec.id
    # 窗口默认跟随实盘日序列
    assert out["window"] == {"start": "2026-07-29", "end": "2026-07-31"}
    # 两侧曲线均首日归一
    assert out["theory"]["curve"][0]["index_value"] == pytest.approx(1.0)
    assert out["theory"]["curve"][-1]["index_value"] == pytest.approx(1.1)
    assert out["live"]["curve"][0]["index_value"] == pytest.approx(1.0)
    assert out["live"]["curve"][-1]["index_value"] == pytest.approx(1.07)

    assert out["theory"]["metrics"]["total_return"] == pytest.approx(0.1)
    assert out["live"]["metrics"]["total_return"] == pytest.approx(0.07)
    assert out["live"]["metrics"]["trades_count"] == 1
    assert out["live"]["metrics"]["orders_count"] == 3
    assert out["live"]["metrics"]["filled_orders"] == 2
    assert out["live"]["metrics"]["partial_orders"] == 1
    assert out["live"]["metrics"]["fill_rate"] == pytest.approx(0.6667, abs=1e-4)

    dev = out["deviation"]
    assert dev["return_gap"] == pytest.approx(-0.03)
    assert dev["trades_gap"] == 0
    # 理论买入 10.0 vs 实盘 10.2 → 买贵 200bps
    assert dev["slippage"]["matched_pairs"] == 1
    assert dev["slippage"]["weighted_deviation_bps"] == pytest.approx(200.0)
    assert dev["slippage"]["details"][0]["stock_code"] == "000001.SZ"


def test_theory_vs_live_unbound_still_returns_live_side(db_session):
    session, _ = _seed(db_session, bound=False)

    out = svc.compute_theory_vs_live(db_session, session)

    assert out["bound"] is False
    assert out["deviation"] is None
    assert len(out["live"]["curve"]) == 3
    assert any("未绑定回测记录" in w for w in out["warnings"])
