# -*- coding: utf-8 -*-
"""会话数据体检的预算 / 去重（服务层）。

锁定口径：
1. 默认必须带预算（max_codes + time_budget_seconds），不再允许扫全池；
2. 同一会话并发体检只跑一份，后来者直接拿「进行中」，不再起第二个不可取消线程；
3. 成功结果在 TTL 内复用（cached=True），避免前端轮询反复取数。
"""
import threading

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, LiveSession
from core.services import live_service as ls


@pytest.fixture
def session_factory(tmp_path):
    engine = create_engine(f"sqlite:///{(tmp_path / 'data-health.db').as_posix()}")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


@pytest.fixture(autouse=True)
def _clear_health_state():
    ls._HEALTH_CACHE.clear()
    ls._HEALTH_INFLIGHT.clear()
    yield
    ls._HEALTH_CACHE.clear()
    ls._HEALTH_INFLIGHT.clear()


def _session(factory):
    db = factory()
    row = LiveSession(name="会话A", mode="simulation", engine="tdx",
                      status="stopped")
    db.add(row)
    db.commit()
    sid = row.id
    db.close()
    return sid


def _patch_light(monkeypatch):
    monkeypatch.setattr(ls, "_session_strategies", lambda db, sid: [])
    monkeypatch.setattr(ls, "make_dispatcher", lambda *a, **kw: object())


def test_default_budget_is_passed_to_health_check(session_factory, monkeypatch):
    sid = _session(session_factory)
    _patch_light(monkeypatch)
    seen = {}

    def _fake(dispatcher, codes, periods, **kw):
        seen.update(kw)
        return {"health": {"healthy": False, "truncated": True,
                           "checked_codes": 30, "periods": {}},
                "auto_fill": None}

    monkeypatch.setattr(ls, "run_health_check", _fake)
    db = session_factory()
    try:
        out = ls.session_data_health(db, sid)
    finally:
        db.close()

    assert seen["max_codes"] == ls._DATA_HEALTH_MAX_CODES
    assert seen["time_budget_seconds"] == ls._DATA_HEALTH_TIME_BUDGET_SECONDS
    assert seen["session_id"] == sid
    assert out["truncated"] is True
    assert out["codes_checked"] == 30
    assert out["budget"]["max_codes"] == ls._DATA_HEALTH_MAX_CODES


def test_concurrent_same_session_runs_single_scan(session_factory, monkeypatch):
    sid = _session(session_factory)
    _patch_light(monkeypatch)
    started = threading.Event()
    release = threading.Event()
    calls = []

    def _slow(dispatcher, codes, periods, **kw):
        calls.append(1)
        started.set()
        release.wait(5)
        return {"health": {"healthy": True, "periods": {}}, "auto_fill": None}

    monkeypatch.setattr(ls, "run_health_check", _slow)

    result = {}

    def _first():
        db = session_factory()
        try:
            result["first"] = ls.session_data_health(db, sid)
        finally:
            db.close()

    t = threading.Thread(target=_first)
    t.start()
    assert started.wait(3)
    db2 = session_factory()
    try:
        second = ls.session_data_health(db2, sid)
    finally:
        db2.close()
    release.set()
    t.join(5)

    assert second["in_progress"] is True
    assert second["available"] is False
    assert "已有数据体检在执行" in second["reason"]
    assert len(calls) == 1  # 并发刷新没有起第二个取数
    assert result["first"]["available"] is True


def test_result_cached_within_ttl(session_factory, monkeypatch):
    sid = _session(session_factory)
    _patch_light(monkeypatch)
    calls = []

    def _fake(dispatcher, codes, periods, **kw):
        calls.append(1)
        return {"health": {"healthy": True, "periods": {}}, "auto_fill": None}

    monkeypatch.setattr(ls, "run_health_check", _fake)
    db = session_factory()
    try:
        first = ls.session_data_health(db, sid)
        second = ls.session_data_health(db, sid)
    finally:
        db.close()

    assert first["cached"] is False
    assert second["cached"] is True
    assert len(calls) == 1


def test_inflight_marker_cleared_after_failure(session_factory, monkeypatch):
    """取数抛错也要清在跑标记，否则该会话会一直「进行中」。"""
    sid = _session(session_factory)
    _patch_light(monkeypatch)

    def _boom(*a, **kw):
        raise RuntimeError("桥炸了")

    monkeypatch.setattr(ls, "run_health_check", _boom)
    db = session_factory()
    try:
        with pytest.raises(RuntimeError):
            ls.session_data_health(db, sid)
        assert sid not in ls._HEALTH_INFLIGHT

        monkeypatch.setattr(ls, "run_health_check",
                            lambda *a, **kw: {"health": {"healthy": True,
                                                         "periods": {}},
                                              "auto_fill": None})
        out = ls.session_data_health(db, sid)
    finally:
        db.close()
    assert out["available"] is True
