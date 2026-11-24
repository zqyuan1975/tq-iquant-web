# -*- coding: utf-8 -*-
"""投资看板自动刷新调度用例（不取真实行情、不写真实库）。"""
from datetime import datetime

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.models import Base
from core.services import board_service as svc
from core.services.board_auto_refresh import (
    BoardAutoRefreshScheduler, _hm_minutes, settings,
)

TRADING_DAY = datetime(2026, 11, 24, 9, 5)      # 周二（交易日）
SATURDAY = datetime(2026, 11, 28, 16, 0)        # 周六（非交易日）


@pytest.fixture
def session_factory():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False},
        poolclass=StaticPool)
    Base.metadata.create_all(engine)
    yield sessionmaker(bind=engine)
    engine.dispose()


def _make(Session, calls, now_holder, notifies=None, *,
          enabled=True, status="ok", policy=None,
          times=("09:05", "16:00"), times_30m=("11:35", "16:00"),
          retry_partial=True, cooldown=5):
    notifies = notifies if notifies is not None else []
    cfg = {"investment_board": {"auto_refresh": {
        "enabled": enabled,
        "times": list(times),
        "period": "1d",
        "scope": "all",
        "times_30m": list(times_30m),
        "scope_30m": "industries",
        "retry_partial": retry_partial,
        "retry_cooldown_minutes": cooldown,
    }}}

    def run_fn(trigger, period, scope, as_of):
        calls.append((trigger, period, scope, as_of))
        db = Session()
        try:
            run = svc.create_run(db, period, scope, trigger=trigger)
            # 用注入时钟写 started_at/finished_at，保证用例与真实日期无关
            run.started_at = now_holder[0]
            run.status = status
            run.error = "boom" if status == "failed" else None
            run.skipped = 2 if status == "partial" else 0
            run.finished_at = now_holder[0]
            db.commit()
            stats = {"industries": {"skipped": 2}} if status == "partial" else {}
            return {"status": status, "error": run.error,
                    "as_of": "2026-11-24", "stats": stats}
        finally:
            db.close()

    return BoardAutoRefreshScheduler(
        cfg_provider=lambda: cfg,
        session_factory=Session,
        run_fn=run_fn,
        notify_fn=lambda *args: notifies.append(args),
        policy_fn=lambda: policy or {"p2_allowed": True, "window": "after_close"},
        is_trading_day_fn=lambda d: d.weekday() < 5,
        now_fn=lambda: now_holder[0],
    )


def _triggers(calls):
    return [c[0] for c in calls]


def test_settings_two_tracks_and_yaml_sexagesimal():
    # YAML 会把未加引号的 09:05 / 16:00 解析成 545 / 960
    cfg = {"investment_board": {"auto_refresh": {
        "enabled": True, "times": [960, 545], "period": "bad", "scope": "bad",
        "times_30m": [695, 960], "scope_30m": "bad"}}}
    got = settings(cfg)
    assert [t["period"] for t in got["tracks"]] == ["1d", "30m"]
    assert [s["trigger"] for s in got["tracks"][0]["slots"]] == ["auto:0905", "auto:1600"]
    assert [s["trigger"] for s in got["tracks"][1]["slots"]] == ["auto:1135", "auto:1600"]
    assert got["tracks"][0]["period"] == "1d"
    assert got["tracks"][1]["scope"] == "industries"
    assert got["retry_partial"] is True
    assert _hm_minutes("16:00") == 960
    assert _hm_minutes("oops") is None


def test_non_trading_day_skipped(session_factory):
    calls, now = [], [SATURDAY]
    sched = _make(session_factory, calls, now)
    assert sched.maybe_run() == []
    assert calls == []


def test_before_slot_then_runs_once_per_slot(session_factory):
    calls, now = [], [datetime(2026, 11, 24, 8, 59)]
    sched = _make(session_factory, calls, now)
    assert sched.maybe_run() == []
    assert calls == []

    now[0] = TRADING_DAY
    sched.maybe_run()
    # 09:05 只到日线时点；30m 首点 11:35 未到
    assert calls == [("auto:0905", "1d", "all", None)]

    now[0] = datetime(2026, 11, 24, 9, 6)
    sched.maybe_run()
    assert len(calls) == 1

    # 11:35：30m 行业层补齐上午 4 根，as_of 必须取当日
    now[0] = datetime(2026, 11, 24, 11, 35)
    sched.maybe_run()
    assert calls[-1] == ("auto:1135", "30m", "industries",
                         datetime(2026, 11, 24).date())

    # 16:00：日线 + 30m 各一次
    now[0] = datetime(2026, 11, 24, 16, 0)
    sched.maybe_run()
    assert _triggers(calls)[-2:] == ["auto:1600", "auto:1600"]
    assert {c[1] for c in calls[-2:]} == {"1d", "30m"}


def test_catch_up_only_latest_missed_slot(session_factory):
    calls, now = [], [datetime(2026, 11, 24, 20, 0)]
    sched = _make(session_factory, calls, now)
    sched.maybe_run()
    # 两条轨道各补最近一个时点（16:00），不补 09:05 / 11:35
    assert sorted(_triggers(calls)) == ["auto:1600", "auto:1600"]
    assert len(calls) == 2


def test_restart_does_not_rerun_same_slot(session_factory):
    calls, now = [], [TRADING_DAY]
    _make(session_factory, calls, now).maybe_run()
    assert len(calls) == 1
    # 模拟进程重启：新实例读同一库，当天该时点已有 run → 不再刷新
    now[0] = datetime(2026, 11, 24, 9, 30)
    _make(session_factory, calls, now).maybe_run()
    assert len(calls) == 1


def test_disabled_config_no_run(session_factory):
    calls, now = [], [TRADING_DAY]
    sched = _make(session_factory, calls, now, enabled=False)
    assert sched.maybe_run() == []
    assert calls == []


def test_p2_blocked_waits_for_open_window(session_factory):
    calls, now = [], [TRADING_DAY]
    policy = {"p2_allowed": False, "window": "trading"}
    sched = _make(session_factory, calls, now, policy=policy)
    assert sched.maybe_run() == []
    assert calls == []

    # 09:05 被交易时段拒绝；11:35 午休放行后补跑 09:05（日线），
    # 同时 30m 的 11:35 时点到达，各跑一次
    now[0] = datetime(2026, 11, 24, 11, 35)
    sched._policy_fn = lambda: {"p2_allowed": True, "window": "lunch"}
    sched.maybe_run()
    assert sorted(_triggers(calls)) == ["auto:0905", "auto:1135"]


def test_truncated_run_retries_once_in_open_window(session_factory):
    calls, now = [], [TRADING_DAY]
    sched = _make(session_factory, calls, now, status="partial", cooldown=5)
    sched.maybe_run()
    assert calls == [("auto:0905", "1d", "all", None)]

    # 冷却未到 → 不补跑
    now[0] = datetime(2026, 11, 24, 9, 6)
    sched.maybe_run()
    assert len(calls) == 1

    # 09:10 后进入交易时段，闸拒；11:35 放行后补跑一次（trigger 带 r）
    sched._policy_fn = lambda: {"p2_allowed": True, "window": "lunch"}
    now[0] = datetime(2026, 11, 24, 11, 35)
    sched.maybe_run()
    assert "auto:0905r" in _triggers(calls)
    # 30m 的 11:35 时点同时到期
    assert "auto:1135" in _triggers(calls)

    # 每时点只补一次：不再产生 auto:0905r
    now[0] = datetime(2026, 11, 24, 11, 40)
    sched.maybe_run()
    assert _triggers(calls).count("auto:0905r") == 1


def test_failed_run_notifies_and_is_not_retried_when_disabled(session_factory):
    calls, now, notifies = [], [TRADING_DAY], []
    sched = _make(session_factory, calls, now, notifies,
                  status="failed", retry_partial=False)
    sched.maybe_run()
    assert len(calls) == 1
    assert len(notifies) == 1
    assert "失败" in notifies[0][1]

    now[0] = datetime(2026, 11, 24, 16, 0)
    sched.maybe_run()
    # 同一时点不再重试；但 16:00 新时点会执行（日线 + 30m）
    assert _triggers(calls).count("auto:0905") == 1


def test_partial_after_retry_notifies(session_factory):
    calls, now, notifies = [], [TRADING_DAY], []
    sched = _make(session_factory, calls, now, notifies,
                  status="partial", cooldown=0)
    sched.maybe_run()          # base partial，不告警（等补跑）
    assert notifies == []
    sched.maybe_run()          # 冷却 0 + 闸放行 → 补跑，仍 partial → 告警
    assert _triggers(calls) == ["auto:0905", "auto:0905r"]
    assert len(notifies) == 1
    assert "缺漏" in notifies[0][1]


def test_manual_refresh_lock_skips_then_resumes(session_factory):
    calls, now = [], [TRADING_DAY]
    sched = _make(session_factory, calls, now)
    assert svc.REFRESH_LOCK.acquire(blocking=False)
    try:
        assert sched.maybe_run() == []
        assert calls == []
    finally:
        svc.REFRESH_LOCK.release()
    sched.maybe_run()
    assert calls == [("auto:0905", "1d", "all", None)]
