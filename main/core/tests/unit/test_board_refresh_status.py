# -*- coding: utf-8 -*-
"""看板刷新状态口径：可重试缺口 vs 历史不足（结构性缺口）。

背景（2026-11-24 实测）：每次全量 1d 刷新都会稳定跳过 212 项（1 个行业 + 211
只次新股），它们都是「已闭合日线 < 300 根、评级公式不输出方向」，补跑不可能
消除，却把 run 永远钉在 partial，前端「跳过 N 项，将在放行窗补跑」常挂。
这里锁定修正后的口径：只有可重试缺口计入 skipped/partial。
"""
from datetime import date, datetime, timedelta

import polars as pl
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.models import Base
from core.models.investment_board import BoardRun
from core.services import board_service as svc

AS_OF = date(2026, 11, 24)
NOW = datetime(2026, 11, 24, 20, 0)


def _frame(n: int) -> pl.DataFrame:
    base = datetime(2020, 1, 1)
    return pl.DataFrame({
        "datetime": [base + timedelta(days=i) for i in range(n)],
        "Open": [10.0] * n, "High": [10.0] * n, "Low": [10.0] * n,
        "Close": [10.0] * n, "Volume": [100.0] * n,
    })


@pytest.fixture
def Session(monkeypatch):
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False},
        poolclass=StaticPool)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine)
    monkeypatch.setattr("core.db.SessionLocal", factory)
    yield factory
    engine.dispose()


def test_skip_reason_splits_missing_from_insufficient():
    assert svc._skip_reason(None, "1d", AS_OF, NOW) == "no_data"
    assert svc._skip_reason(_frame(0), "1d", AS_OF, NOW) == "no_data"
    # 次新股：行数 < MIN_BARS → 结构性，补跑无用
    assert svc._skip_reason(_frame(svc.MIN_BARS - 1), "1d", AS_OF, NOW) == "insufficient"
    # 历史足够但公式无输出 → 归为可重试
    assert svc._skip_reason(_frame(svc.MIN_BARS), "1d", AS_OF, NOW) == "no_rating"


def test_split_skips_orders_codes_by_reason():
    frames = {"600000.SH": _frame(400), "301999.SZ": _frame(80)}
    pending, insufficient = svc._split_skips(
        ["600000.SH", "301999.SZ", "000001.SZ"], frames, "1d", AS_OF, NOW)
    # 批次有数据但该标的无序列（未上市/退市）→ 结构性，不是可重试缺口
    assert pending == ["600000.SH"]
    assert insufficient == ["301999.SZ", "000001.SZ"]


def test_split_skips_batch_failure_is_retryable():
    """整批取数失败（TQ 抖动）→ 全部可重试，等待放行窗补跑。"""
    pending, insufficient = svc._split_skips(
        ["600000.SH", "000001.SZ"], {}, "1d", AS_OF, NOW)
    assert pending == ["600000.SH", "000001.SZ"]
    assert insufficient == []


def _seed_run(Session, *, trigger="manual", status="running") -> int:
    db = Session()
    try:
        run = svc.create_run(db, "1d", "all", trigger=trigger)
        run.status = status
        return run.id
    finally:
        db.close()


def _patch_compute(monkeypatch, ind: dict, stk: dict):
    monkeypatch.setattr(svc, "refresh_universe",
                        lambda db: {"ok": True, "count": 128, "expected": 128})
    monkeypatch.setattr(
        svc, "compute_industries",
        lambda db, period, as_of, trigger=None, run_id=None, codes=None: ind)
    monkeypatch.setattr(
        svc, "compute_stocks",
        lambda db, period, as_of, trigger=None, run_id=None,
        industry_codes=None: stk)


def _load_run(Session, run_id: int) -> BoardRun:
    db = Session()
    try:
        return db.get(BoardRun, run_id)
    finally:
        db.close()


def test_execute_run_insufficient_only_is_ok(Session, monkeypatch):
    """历史不足（含 881479 行业与 211 只次新股）不拉低状态、不计 skipped。"""
    _patch_compute(
        monkeypatch,
        ind={"total": 128, "ok": 127, "skipped": 0, "insufficient": 1},
        stk={"total": 5545, "ok": 5334, "skipped": 0, "insufficient": 211},
    )
    run_id = _seed_run(Session)
    got = svc.execute_run(run_id, "1d", "all")
    assert got["status"] == "ok"
    run = _load_run(Session, run_id)
    assert run.status == "ok"
    assert run.skipped == 0
    assert run.total == 128 + 5545
    assert run.ok == 127 + 5334


def test_execute_run_retryable_skip_is_partial(Session, monkeypatch):
    _patch_compute(
        monkeypatch,
        ind={"total": 128, "ok": 127, "skipped": 0, "insufficient": 1},
        stk={"total": 5545, "ok": 5334, "skipped": 3, "insufficient": 208},
    )
    run_id = _seed_run(Session)
    got = svc.execute_run(run_id, "1d", "all")
    assert got["status"] == "partial"
    run = _load_run(Session, run_id)
    assert run.status == "partial"
    assert run.skipped == 3
