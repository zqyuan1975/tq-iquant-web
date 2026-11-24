# -*- coding: utf-8 -*-
"""公司行为自动同步调度（2026-11-24）。

覆盖：
- 未启用 / 未到点 / 非交易日 → 不动作；
- P2 未放行 → 退避等待且零 TQ 取数（不记失败、不突破闸门）；
- 范围只取 missing_coverage_codes（已覆盖票跳过）；
- 同日成功后再跑 → 跳过（跨重启由状态文件兜底）；
- 单票失败记 failed 且本轮不重试，冷却后只补失败票。
"""
from contextlib import nullcontext
from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy.orm import sessionmaker

from core.models import StockPool, StockPoolStock
from core.services import corporate_action_sync as cas


def _frame(rows):
    return pd.DataFrame(
        rows,
        columns=["Date", "Type", "Bonus", "AllotPrice", "ShareBonus", "Allotment"],
    ).set_index("Date")


class _FakeTQ:
    table = {}
    fail = set()
    calls = []

    def get_divid_factors(self, code, start="", end=""):
        _FakeTQ.calls.append(code)
        if code in _FakeTQ.fail:
            raise RuntimeError("tdx down")
        return _FakeTQ.table.get(code, pd.DataFrame())


@pytest.fixture(autouse=True)
def _patch_env(monkeypatch, tmp_path):
    monkeypatch.setattr(cas, "coverage_path", lambda: tmp_path / "coverage.json")
    monkeypatch.setattr(cas, "sync_state_path", lambda: tmp_path / "state.json")
    monkeypatch.setattr("core.tq.data.TQData", lambda: _FakeTQ())
    monkeypatch.setattr("core.tq.gate.is_trading_day", lambda d: True)
    monkeypatch.setattr("core.tq.gate.evaluate_policy",
                        lambda: {"p2_allowed": True})
    monkeypatch.setattr("core.tq.gate.tdx_priority", lambda *a, **k: nullcontext())
    _FakeTQ.table = {}
    _FakeTQ.fail = set()
    _FakeTQ.calls = []


@pytest.fixture()
def scheduler(db_session):
    factory = sessionmaker(bind=db_session.get_bind())
    db_session.add_all([
        StockPool(id=1, code="TQCS", name="成长精选"),
        StockPool(id=4, code="TQ1109", name="1109池"),
        StockPoolStock(pool_id=1, stock_code="000001.SZ"),
        StockPoolStock(pool_id=1, stock_code="600211.SH"),
        StockPoolStock(pool_id=4, stock_code="300286.SZ"),
    ])
    db_session.commit()
    clock = {"now": datetime(2026, 11, 24, 15, 25)}

    def _make(**cfg):
        base = {"enabled": True, "daily_at": "15:20",
                "retry_cooldown_minutes": 30}
        base.update(cfg)
        return cas.CorporateActionSyncScheduler(
            cfg_provider=lambda: base,
            session_factory=factory,
            now_fn=lambda: clock["now"],
            interval_seconds=5,
        )

    return _make, clock


def _mark(code, status):
    data = cas._read_coverage()
    data[code] = {"status": status, "window": ["", ""],
                  "checked_at": "2026-11-24T15:00:00"}
    cas._write_coverage(data)


def test_skips_when_disabled_or_before_time(scheduler):
    make, clock = scheduler
    assert make(enabled=False).maybe_run() is None
    clock["now"] = datetime(2026, 11, 24, 15, 19)
    assert make().maybe_run() is None
    assert _FakeTQ.calls == []


def test_skips_non_trading_day(scheduler, monkeypatch):
    make, _ = scheduler
    monkeypatch.setattr("core.tq.gate.is_trading_day", lambda d: False)
    assert make().maybe_run() is None
    assert _FakeTQ.calls == []


def test_p2_blocked_defers_without_touching_tq(scheduler, monkeypatch):
    make, clock = scheduler
    monkeypatch.setattr("core.tq.gate.evaluate_policy",
                        lambda: {"p2_allowed": False})
    sched = make()
    assert sched.maybe_run()["status"] == "deferred"
    assert _FakeTQ.calls == []
    # 5 分钟退避内不再尝试，也不写失败状态
    clock["now"] += timedelta(minutes=1)
    assert sched.maybe_run() is None
    assert cas.read_sync_state().get("status") is None


def test_only_missing_codes_are_synced_and_same_day_dedup(scheduler):
    make, clock = scheduler
    _mark("000001.SZ", "covered")
    _FakeTQ.table = {"600211.SH": _frame([
        {"Date": "2026-06-11", "Type": "1", "Bonus": 2.5,
         "AllotPrice": 0, "ShareBonus": 0, "Allotment": 0},
    ])}
    _mark("300286.SZ", "no_events")

    result = make().maybe_run()

    assert result["status"] == "success"
    assert _FakeTQ.calls == ["600211.SH"]  # 已覆盖/无事件票不取数
    assert result["requested"] == 1
    state = cas.read_sync_state()
    assert state["last_success_date"] == date(2026, 11, 24).isoformat()
    assert state["result"]["added"] == 1
    # 同日再跑（含重启后新实例）直接跳过
    clock["now"] += timedelta(minutes=30)
    assert make().maybe_run() is None
    assert _FakeTQ.calls == ["600211.SH"]


def test_failed_code_retried_after_cooldown(scheduler):
    make, clock = scheduler
    _mark("000001.SZ", "covered")
    _mark("300286.SZ", "covered")
    _FakeTQ.fail = {"600211.SH"}
    sched = make()

    result = sched.maybe_run()
    assert result["status"] == "failed"
    assert result["failed"] == ["600211.SH"]
    state = cas.read_sync_state()
    assert state["status"] == "failed"
    assert state["last_success_date"] is None

    # 冷却内不再打 TQ
    clock["now"] += timedelta(minutes=5)
    assert sched.maybe_run() is None
    assert _FakeTQ.calls == ["600211.SH"]

    # 冷却后只补失败票
    _FakeTQ.fail = set()
    _FakeTQ.table = {"600211.SH": _frame([
        {"Date": "2026-09-15", "Type": "1", "Bonus": 11.46,
         "AllotPrice": 0, "ShareBonus": 0, "Allotment": 0},
    ])}
    clock["now"] += timedelta(minutes=30)
    result = sched.maybe_run()
    assert result["status"] == "success"
    assert _FakeTQ.calls == ["600211.SH", "600211.SH"]
    assert cas.read_sync_state()["last_success_date"] == "2026-11-24"
