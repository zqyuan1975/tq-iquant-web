"""资金池额度推导只读接口（0021 A4）测试。

覆盖：
- 开关关闭（默认）→ 200 + enabled=false + status=disabled，不做任何计算；
- 开关开启 → 200，返回结构化额度（含 binding / limits / version）；
- 组合不存在 → 404；
- 无法推导（组合下无策略）→ 422 并附原因。
"""
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.db import get_db
from core.main import app
from core.models import (
    Base, Formula, PortfolioStrategy, StockPool, Strategy,
)


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


def _seed(db, *, with_strategy=True):
    pool = StockPool(code="TQCS", name="tq自选")
    db.add(pool)
    db.flush()
    formula = Formula(name="F1", content="REF(CLOSE,1)")
    db.add(formula)
    db.flush()
    ps = PortfolioStrategy(
        name="PS1", stock_pool_id=pool.id,
        initial_capital=Decimal("2000000"), max_drawdown=Decimal("0.2"),
        daily_loss_limit=Decimal("0.05"), max_holdings=10,
    )
    db.add(ps)
    db.flush()
    if with_strategy:
        db.add(Strategy(
            portfolio_id=ps.id, name="S1", formula_id=formula.id,
            period="1d", role="independent",
            capital_ratio=Decimal("1"), max_positions=20,
            single_open_ratio=Decimal("0.05"),
        ))
    db.commit()
    return ps.id


def _enable(monkeypatch):
    monkeypatch.setattr(
        "core.api.capital.load_config",
        lambda: {"capital_pool": {"enabled": True}},
    )


def test_derive_disabled_by_default(client, monkeypatch):
    c, Session = client
    db = Session()
    pid = _seed(db)
    db.close()

    # 不配置 capital_pool 块 = 默认关闭
    monkeypatch.setattr("core.api.capital.load_config", lambda: {})
    body = c.get("/api/capital/derive", params={"portfolio_id": pid}).json()
    assert body["code"] == 0
    assert body["data"]["enabled"] is False
    assert body["data"]["status"] == "disabled"
    assert body["data"]["version"]["quota_algo_version"]


def test_derive_enabled_returns_structured_quota(client, monkeypatch):
    c, Session = client
    db = Session()
    pid = _seed(db)
    db.close()
    _enable(monkeypatch)

    body = c.get("/api/capital/derive", params={"portfolio_id": pid}).json()
    assert body["code"] == 0
    data = body["data"]
    assert data["enabled"] is True
    # 单仓 = 5% × 200 万 = 10 万；20 槽毛额度 200 万，但 max_holdings=10 → 100 万
    assert Decimal(data["entitlement"]) == Decimal("1000000")
    assert data["binding"] == ["max_holdings"]
    assert data["strategies"][0]["binding"] == ["single_open_ratio"]
    assert any(x["key"] == "target_utilization" for x in data["limits"])
    # 每个分项都必须带来源与计算过程，供界面核对
    for lim in data["limits"]:
        assert lim["source"] and lim["formula"]


def test_derive_missing_portfolio_returns_404(client, monkeypatch):
    c, _ = client
    _enable(monkeypatch)
    body = c.get("/api/capital/derive", params={"portfolio_id": 9999}).json()
    assert body["code"] == 404


def test_derive_not_derivable_returns_422(client, monkeypatch):
    c, Session = client
    db = Session()
    pid = _seed(db, with_strategy=False)
    db.close()
    _enable(monkeypatch)
    body = c.get("/api/capital/derive", params={"portfolio_id": pid}).json()
    assert body["code"] == 422
    assert body["data"]["status"] == "not_derivable"


# ---------------------------------------------------------------------------
# G3 拼盘健康接口（默认关闭；开启后可写入观测、暂停、恢复、退役）
# ---------------------------------------------------------------------------
def _enable_pairing(monkeypatch):
    monkeypatch.setattr(
        "core.api.pairing.load_config",
        lambda: {"capital_pool": {"enabled": True, "pairing_enabled": True}},
    )


def _seed_plan(Session) -> int:
    db = Session()
    from core.models import CapitalPairingPlan
    plan = CapitalPairingPlan(
        name="健康测试拼盘", scope="L2", status="APPROVED",
        portfolio_strategy_id=1, lender_strategy_id=1, max_borrowers=1,
        principal=Decimal("100000"))
    db.add(plan)
    db.commit()
    pid = plan.id
    db.close()
    return pid


def _normal_metrics():
    return {
        "corr_60d": 0.2, "drawdown_excess_pp": 0.5,
        "adv_participation": 0.005, "grab_ratio_month": 0.01,
        "recall_per_year": 4, "shortfall_ratio": 0.005,
        "borrower_fill_rate": 0.98, "lender_recovery_fill_rate": 0.95,
        "params_hash_match": True, "reconcile_failed": False,
    }


def test_health_disabled_by_default(client, monkeypatch):
    c, Session = client
    pid = _seed_plan(Session)
    monkeypatch.setattr("core.api.pairing.load_config", lambda: {})
    body = c.get("/api/pairing/health").json()
    assert body["code"] == 0
    assert body["data"]["enabled"] is False
    body = c.post(f"/api/pairing/plans/{pid}/health",
                  json={"metrics": _normal_metrics()}).json()
    assert body["data"]["enabled"] is False


def test_health_record_suspend_and_resume_flow(client, monkeypatch):
    c, Session = client
    pid = _seed_plan(Session)
    _enable_pairing(monkeypatch)

    body = c.post(f"/api/pairing/plans/{pid}/health",
                  json={"metrics": _normal_metrics()}).json()
    assert body["code"] == 0
    assert body["data"]["status"] == "ACTIVE"

    bad = dict(_normal_metrics(), shortfall_ratio=0.06)
    body = c.post(f"/api/pairing/plans/{pid}/health",
                  json={"metrics": bad}).json()
    assert body["data"]["status"] == "SUSPENDED"
    assert body["data"]["plan_status"] == "SUSPENDED"

    body = c.post(f"/api/pairing/plans/{pid}/health/resume",
                  json={"reason": "提前恢复"}).json()
    assert body["code"] == 409

    listed = c.get("/api/pairing/health").json()["data"]
    assert listed[0]["plan_id"] == pid
    assert listed[0]["plan_status"] == "SUSPENDED"
    assert listed[0]["latest"]["status"] == "SUSPENDED"

    body = c.post(f"/api/pairing/plans/{pid}/health/retire",
                  json={"reason": "测试退役", "operator": "tester"}).json()
    assert body["code"] == 0
    assert body["data"]["plan_status"] == "RETIRED"
    assert body["data"]["retire_reason"]["reason"] == "测试退役"
