"""资金池 C4：现金出口注册表与 C1–C8 判据（只读影子，不下单）。"""
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.db import get_db
from core.main import app
from core.models import Base, CashOutlet
from core.services import cash_outlet_service as svc


NOW = datetime(2026, 11, 20, 15, 0)


def _outlet(**over):
    """一个「全部通过」的基准出口；用例只覆盖需要破坏的字段。"""
    values = dict(
        code="GC001", name="上交所1天逆回购", category="repo",
        channel="tq", channel_order_enabled=True,
        yield_source="tq.repo.quote", yield_freq="daily", yield_history_days=120,
        liquidity="t0", term_days=None, settlement_max_days=1,
        holiday_postpone=True,
        min_amount=Decimal("1000"), max_single_amount=Decimal("1000000"),
        max_daily_amount=Decimal("5000000"), min_increment=Decimal("1000"),
        redeemable_early=False, auto_mature_return=True,
        risk_level="R1", principal_guaranteed=False,
        arrival_buffer_days=0, inflow_certainty="certain",
        verified=True, verified_at=datetime(2026, 6, 1),
        verify_amount=Decimal("1000"), verify_result="到期回款一致",
        verify_expires_at=date(2027, 6, 1),
        status="active",
    )
    values.update(over)
    outlet = CashOutlet(**values)
    outlet.id = 1
    return outlet


def test_fully_valid_outlet_ready():
    result = svc.evaluate_outlet(_outlet(), now=NOW)
    assert result["status"] == svc.STATUS_READY
    assert result["codes"] == []
    assert result["yield_backtestable"] is True
    assert {c["code"] for c in result["checks"]} >= {
        "C1", "C2", "C3", "C4", "C5", "C6", "C7", "C8"}
    assert result["version"] == svc.OUTLET_VERSION


def test_unverified_and_expired_blocked():
    result = svc.evaluate_outlet(_outlet(verified=False), now=NOW)
    assert result["codes"] == [svc.OUTLET_NOT_VERIFIED]

    expired = svc.evaluate_outlet(
        _outlet(verify_expires_at=date(2026, 1, 1)), now=NOW)
    assert expired["codes"] == [svc.OUTLET_NOT_VERIFIED]


def test_channel_manual_unsupported():
    result = svc.evaluate_outlet(
        _outlet(channel="manual", channel_order_enabled=False), now=NOW)
    assert svc.CHANNEL_UNSUPPORTED in result["codes"]


def test_yield_history_short_is_warning_not_block():
    result = svc.evaluate_outlet(_outlet(yield_history_days=30), now=NOW)
    assert result["status"] == svc.STATUS_READY
    assert result["yield_backtestable"] is False
    assert "not_backtestable" in result["warnings"]

    missing = svc.evaluate_outlet(_outlet(yield_source=None), now=NOW)
    assert svc.YIELD_DATA_MISSING in missing["codes"]


def test_liquidity_term_limit():
    bad = svc.evaluate_outlet(
        _outlet(liquidity="term", term_days=10, settlement_max_days=10),
        now=NOW)
    assert svc.LIQUIDITY_MISMATCH in bad["codes"]

    ok = svc.evaluate_outlet(
        _outlet(liquidity="term", term_days=7, settlement_max_days=7), now=NOW)
    assert ok["status"] == svc.STATUS_READY


def test_limits_checked_against_requested_amount():
    small = svc.evaluate_outlet(
        _outlet(), required_amount="500", now=NOW)
    assert svc.LIMIT_TOO_SMALL in small["codes"]

    # 1100 不满足 1000 + n*1000 的递增单位
    increment = svc.evaluate_outlet(
        _outlet(), required_amount="1100", now=NOW)
    assert svc.LIMIT_TOO_SMALL in increment["codes"]

    ok = svc.evaluate_outlet(_outlet(), required_amount="5000", now=NOW)
    assert ok["status"] == svc.STATUS_READY


def test_redemption_and_risk_rules():
    restricted = svc.evaluate_outlet(
        _outlet(redeemable_early=False, auto_mature_return=False), now=NOW)
    assert svc.REDEMPTION_RESTRICTED in restricted["codes"]

    high_risk = svc.evaluate_outlet(_outlet(risk_level="R3"), now=NOW)
    assert svc.RISK_LEVEL_HIGH in high_risk["codes"]


def test_settlement_must_be_before_need_date():
    tomorrow = NOW.date() + timedelta(days=1)
    # t0 + 结算 1 天 → 明天可用；用钱日就是明天 → 通过
    assert svc.evaluate_outlet(_outlet(), need_date=tomorrow,
                               now=NOW)["status"] == svc.STATUS_READY
    # 缓冲 2 天 → 后天可用，晚于用钱日 → 拒绝
    late = svc.evaluate_outlet(
        _outlet(arrival_buffer_days=2), need_date=tomorrow, now=NOW)
    assert svc.SETTLEMENT_UNCERTAIN in late["codes"]


def test_evaluate_is_reproducible():
    a = svc.evaluate_outlet(_outlet(), now=NOW)
    b = svc.evaluate_outlet(_outlet(), now=NOW)
    assert a == b


# ---------------------------------------------------------------------------
# 接口
# ---------------------------------------------------------------------------

@pytest.fixture
def client():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False},
        poolclass=StaticPool)
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
        lambda: {"capital_pool": {"enabled": True,
                                  "cash_outlet_enabled": True}})


def _payload(**over):
    base = {
        "code": "GC001", "name": "上交所1天逆回购", "category": "repo",
        "channel": "tq", "channel_order_enabled": True,
        "yield_source": "tq.repo.quote", "yield_freq": "daily",
        "yield_history_days": 120, "liquidity": "t0",
        "settlement_max_days": 1, "min_amount": "1000",
        "max_single_amount": "1000000", "max_daily_amount": "5000000",
        "min_increment": "1000", "auto_mature_return": True,
        "risk_level": "R1", "inflow_certainty": "certain",
    }
    base.update(over)
    return base


def test_outlets_disabled_by_default(client, monkeypatch):
    c, _ = client
    monkeypatch.setattr("core.api.capital.load_config", lambda: {})
    body = c.get("/api/capital/outlets").json()
    assert body["code"] == 0
    assert body["data"]["enabled"] is False
    assert body["data"]["status"] == "disabled"


def test_outlet_lifecycle(client, monkeypatch):
    c, _ = client
    _enable(monkeypatch)
    created = c.post("/api/capital/outlets", json=_payload()).json()
    assert created["code"] == 0
    item = created["data"]["item"]
    assert item["code"] == "GC001"
    assert item["verified"] is False
    oid = item["id"]

    dup = c.post("/api/capital/outlets", json=_payload()).json()
    assert dup["code"] == 422

    listed = c.get("/api/capital/outlets").json()
    assert [x["code"] for x in listed["data"]["items"]] == ["GC001"]

    # 未验证 → 一律 OUTLET_NOT_VERIFIED
    blocked = c.post(f"/api/capital/outlets/{oid}/evaluate", json={}).json()
    assert blocked["data"]["status"] == "BLOCKED"
    assert "OUTLET_NOT_VERIFIED" in blocked["data"]["codes"]

    verified = c.post(
        f"/api/capital/outlets/{oid}/verify",
        json={"amount": "1000", "result": "申购赎回对账一致"}).json()
    assert verified["code"] == 0
    assert verified["data"]["item"]["verified"] is True

    ready = c.post(f"/api/capital/outlets/{oid}/evaluate",
                   json={"required_amount": "5000"}).json()
    assert ready["data"]["status"] == "READY"

    updated = c.put(f"/api/capital/outlets/{oid}",
                    json={"max_single_amount": "2000000"}).json()
    assert updated["data"]["item"]["max_single_amount"] == "2000000.00"


def test_outlet_404_and_bad_amount(client, monkeypatch):
    c, _ = client
    _enable(monkeypatch)
    assert c.get("/api/capital/outlets/999").json()["code"] == 404
    oid = c.post("/api/capital/outlets", json=_payload()).json()["data"]["item"]["id"]
    bad = c.post(f"/api/capital/outlets/{oid}/verify",
                 json={"amount": "abc", "result": "x"}).json()
    assert bad["code"] == 422
    zero = c.post(f"/api/capital/outlets/{oid}/verify",
                  json={"amount": "0", "result": "x"}).json()
    assert zero["code"] == 422
