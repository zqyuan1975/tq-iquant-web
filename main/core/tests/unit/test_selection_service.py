# -*- coding: utf-8 -*-
"""方案与版本服务（A1）：权重校验、版本不可变、激活互斥、API 冒烟。"""
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.db import get_db
from core.main import app
from core.models import Base
from core.selection.field_dict import UnregisteredField
from core.services import selection_service as svc


SPEC = {"pool_key": "value", "weights": {"ep": 0.6, "bp": 0.4}, "top_n": 50}


@pytest.fixture
def api_client():
    """独立内存库 + StaticPool：TestClient 在别的线程，不能共用 session 级 :memory:。"""
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False},
                           poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)

    def override():
        session = factory()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override
    yield TestClient(app)
    app.dependency_overrides.pop(get_db, None)
    engine.dispose()


def test_create_scheme_and_version(db_session):
    scheme = svc.create_scheme(db_session, "value_test", "价值测试", "value")
    version = svc.create_version(db_session, scheme.id, SPEC)
    assert version.version == 1
    assert len(version.spec_hash) == 16
    assert len(version.field_fingerprint) == 16
    assert float(version.weight_sum) == pytest.approx(1.0)
    assert version.uncalibrated is True


def test_weight_sum_must_be_one(db_session):
    scheme = svc.create_scheme(db_session, "v2", "价值2", "value")
    with pytest.raises(svc.SpecError, match="权重和"):
        svc.create_version(db_session, scheme.id,
                           {"pool_key": "value", "weights": {"ep": 0.9}})


def test_negative_weight_rejected(db_session):
    scheme = svc.create_scheme(db_session, "v3", "价值3", "value")
    with pytest.raises(svc.SpecError, match="不能为负"):
        svc.create_version(db_session, scheme.id,
                           {"pool_key": "value",
                            "weights": {"ep": 1.2, "bp": -0.2}})


def test_unregistered_field_rejected(db_session):
    scheme = svc.create_scheme(db_session, "v4", "价值4", "value")
    with pytest.raises(UnregisteredField):
        svc.create_version(db_session, scheme.id,
                           {"pool_key": "value",
                            "weights": {"ep": 0.5, "mystery": 0.5}})


def test_bad_pool_key_rejected(db_session):
    with pytest.raises(svc.SpecError, match="pool_key"):
        svc.create_scheme(db_session, "v5", "坏池", "not_a_pool")


def test_versions_are_immutable_and_increment(db_session):
    scheme = svc.create_scheme(db_session, "growth_x", "成长x", "growth")
    v1 = svc.create_version(db_session, scheme.id, SPEC)
    v1_hash = v1.spec_hash
    v2 = svc.create_version(db_session, scheme.id,
                            {"pool_key": "growth", "track": "historical",
                             "weights": {"g_hist_3y": 1.0}, "top_n": 100})
    assert v2.version == 2
    db_session.refresh(v1)
    assert v1.spec_hash == v1_hash, "旧版本参数不得被改写"
    assert v2.spec_hash != v1_hash


def test_activate_is_exclusive(db_session):
    scheme = svc.create_scheme(db_session, "q_x", "质量x", "quality")
    v1 = svc.create_version(db_session, scheme.id, SPEC)
    v2 = svc.create_version(db_session, scheme.id, SPEC)
    svc.activate_version(db_session, scheme.id, v1.id)
    svc.activate_version(db_session, scheme.id, v2.id)
    active = [v.id for v in svc.list_versions(db_session, scheme.id)
              if v.status == "active"]
    assert active == [v2.id]


def test_seed_is_idempotent(db_session):
    assert svc.seed_default_schemes(db_session) == 4
    assert svc.seed_default_schemes(db_session) == 0


def test_api_smoke(api_client):
    body = api_client.get("/api/selection/meta").json()
    assert body["data"]["field_set_version"].startswith("draft")
    assert any(f["key"] == "total_mv_wan" for f in body["data"]["fields"])

    created = api_client.post("/api/selection/schemes", json={
        "code": "dividend_api", "name": "红利API", "pool_key": "dividend",
    }).json()
    assert created.get("code") == 0, created
    scheme_id = created["data"]["id"]

    bad = api_client.post("/api/selection/schemes/%d/versions" % scheme_id,
                          json={"spec": {"pool_key": "dividend",
                                         "weights": {"ep": 0.5}}}).json()
    assert bad["code"] == 400, bad

    good = api_client.post("/api/selection/schemes/%d/versions" % scheme_id,
                           json={"spec": {"pool_key": "dividend",
                                          "weights": {"dp_annual": 1.0}}}).json()
    assert good["code"] == 0, good
    assert good["data"]["version"] == 1

    status = api_client.get("/api/selection/wide-db/status").json()
    assert status["code"] == 0, "宽库缺失也必须返回成功码 + ready=false"
