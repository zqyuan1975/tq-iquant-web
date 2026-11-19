"""资金池 C2：历史利用率统计（纯函数 + 只读接口）。

覆盖：
- 分位与均值可复算（与线性插值口径一致）；
- 样本不足只降级 no_history，不报错；
- 缺市值行计入 invalid、门控关闭行默认剔除、权益跳变剔除；
- 接口：开关关闭 disabled、回测来源、实盘来源、组合/记录不匹配 404。
"""
from datetime import date, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.db import get_db
from core.engine.capital_reserve import (
    UtilizationPoint,
    build_utilization_points,
    summarize_utilization,
)
from core.main import app
from core.models import (
    BacktestDailySnapshot,
    BacktestRecord,
    Base,
    LivePortfolioDaily,
    PortfolioStrategy,
    StockPool,
)


# ---------------------------------------------------------------------------
# 纯函数
# ---------------------------------------------------------------------------

def test_percentiles_recomputable():
    us = [Decimal(i) / Decimal("100") for i in range(1, 101)]  # 0.01..1.00
    points = [UtilizationPoint(utilization=u) for u in us]
    stats = summarize_utilization(points, window=1000, min_samples=1,
                                  exclude_first_days=0)
    assert stats.status == "ok"
    assert stats.samples == 100
    assert stats.u_min == Decimal("0.01")
    assert stats.u_max == Decimal("1.00")
    assert stats.u_current == Decimal("1.00")
    # 线性插值 p95：位置 (100-1)*0.95 = 94.05 → 0.95 + 0.05*(0.96-0.95)
    assert stats.u_p95 == Decimal("0.9505")
    assert stats.u_p50 == Decimal("0.5050")
    assert stats.u_mean == Decimal("0.5050")


def test_small_sample_degrades_without_error():
    points = [UtilizationPoint(utilization=Decimal("0.5"))] * 30
    stats = summarize_utilization(points, exclude_first_days=0)
    assert stats.status == "degraded"
    assert stats.samples == 30
    assert "no_history" in stats.degraded


def test_gate_closed_and_missing_rows_excluded():
    rows = [
        {"total_value": 100, "market_value": 50, "gate_state": 1},
        {"total_value": 100, "market_value": 60, "gate_state": -1},  # 门控关闭
        {"total_value": 100, "market_value": None, "gate_state": 1},  # 缺市值
        {"total_value": 100, "market_value": 70, "gate_state": 0},
    ]
    points = build_utilization_points(rows)
    stats = summarize_utilization(points, window=10, min_samples=1,
                                  exclude_first_days=0)
    assert stats.samples == 2
    assert stats.excluded["gate_closed"] == 1
    assert stats.excluded["invalid"] == 1
    assert stats.u_max == Decimal("0.7")


def test_equity_jump_rows_excluded():
    rows = [
        {"total_value": 100, "market_value": 50},
        {"total_value": 100, "market_value": 50},
        {"total_value": 500, "market_value": 250},  # +400% 增资跳变
        {"total_value": 500, "market_value": 250},
    ]
    stats = summarize_utilization(
        build_utilization_points(rows), window=10, min_samples=1,
        exclude_first_days=0)
    assert stats.samples == 3
    assert stats.excluded["equity_jump"] == 1


def test_series_matches_kept_points():
    rows = [
        {"snap_date": date(2026, 1, 1) + timedelta(days=i),
         "total_value": 100, "market_value": 40 + i}
        for i in range(5)
    ]
    stats = summarize_utilization(
        build_utilization_points(rows), window=10, min_samples=1,
        exclude_first_days=0, include_series=True)
    assert len(stats.series) == 5
    assert stats.series[0]["utilization"] == "0.4"
    assert stats.series[-1]["utilization"] == "0.44"


# ---------------------------------------------------------------------------
# 接口
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    engine = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c, Session
    app.dependency_overrides.clear()


def _enable(monkeypatch):
    monkeypatch.setattr(
        "core.api.capital.load_config",
        lambda: {"capital_pool": {"enabled": True}},
    )


def _seed(db, *, days=80, live_days=0):
    pool = StockPool(code="TQCS", name="tq自选")
    db.add(pool)
    db.flush()
    ps = PortfolioStrategy(
        name="PS1", stock_pool_id=pool.id,
        initial_capital=Decimal("1000000"), max_drawdown=Decimal("0.2"),
        daily_loss_limit=Decimal("0.05"), max_holdings=10,
    )
    db.add(ps)
    db.flush()
    rec = BacktestRecord(
        portfolio_strategy_id=ps.id, name="回测",
        start_date=date(2025, 1, 1), end_date=date(2026, 1, 1),
        status="completed",
    )
    db.add(rec)
    db.flush()
    for i in range(days):
        db.add(BacktestDailySnapshot(
            backtest_record_id=rec.id, target_type="portfolio",
            target_id=ps.id, snap_date=date(2025, 1, 1) + timedelta(days=i),
            total_value=Decimal("1000000"),
            cash=Decimal("500000"),
            market_value=Decimal("500000") + Decimal(i * 1000),
        ))
    for i in range(live_days):
        db.add(LivePortfolioDaily(
            live_session_id=7, portfolio_strategy_id=ps.id,
            trade_date=date(2026, 1, 1) + timedelta(days=i),
            total_value=Decimal("1000000"),
            cash=Decimal("400000"),
            market_value=Decimal("600000"),
        ))
    db.commit()
    return ps.id, rec.id


def test_utilization_disabled_by_default(client, monkeypatch):
    c, Session = client
    db = Session()
    pid, _ = _seed(db)
    db.close()
    monkeypatch.setattr("core.api.capital.load_config", lambda: {})
    body = c.get("/api/capital/utilization",
                 params={"portfolio_id": pid}).json()
    assert body["code"] == 0
    assert body["data"]["enabled"] is False
    assert body["data"]["status"] == "disabled"


def test_utilization_backtest_source(client, monkeypatch):
    c, Session = client
    db = Session()
    pid, rid = _seed(db, days=80)
    db.close()
    _enable(monkeypatch)
    body = c.get("/api/capital/utilization",
                 params={"portfolio_id": pid}).json()
    assert body["code"] == 0
    data = body["data"]
    assert data["enabled"] is True
    assert data["source"] == "backtest"
    assert data["record_id"] == rid
    assert data["samples"] == 60  # 80 - 启动 20 日
    assert data["u_p50"] and data["u_max"] and data["u_current"]
    assert len(data["series"]) == 60


def test_utilization_live_source(client, monkeypatch):
    c, Session = client
    db = Session()
    pid, _ = _seed(db, days=0, live_days=40)
    db.close()
    _enable(monkeypatch)
    body = c.get("/api/capital/utilization",
                 params={"portfolio_id": pid, "source": "live"}).json()
    assert body["code"] == 0
    data = body["data"]
    assert data["session_id"] == 7
    assert data["samples"] == 20  # 40 - 启动 20 日
    assert Decimal(data["u_p50"]) == Decimal("0.6")


def test_utilization_missing_portfolio_404(client, monkeypatch):
    c, _ = client
    _enable(monkeypatch)
    body = c.get("/api/capital/utilization",
                 params={"portfolio_id": 9999}).json()
    assert body["code"] == 404


def test_utilization_record_mismatch_404(client, monkeypatch):
    c, Session = client
    db = Session()
    pid, _ = _seed(db)
    other_rec = BacktestRecord(
        portfolio_strategy_id=pid + 1, name="别的记录",
        start_date=date(2025, 1, 1), end_date=date(2025, 2, 1),
        status="completed",
    )
    db.add(other_rec)
    db.commit()
    rid = other_rec.id
    db.close()
    _enable(monkeypatch)
    body = c.get("/api/capital/utilization",
                 params={"portfolio_id": pid, "record_id": rid}).json()
    assert body["code"] == 404
