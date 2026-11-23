# -*- coding: utf-8 -*-
"""实盘门控/日线轮次 P0 接线 + 看板快照解耦 + 门控失败显式化（2026-11-23）。

背景：实盘 15:05 日线轮次与每日门控刷新走 TQ 时默认 P1，被 14:55–15:10
收盘保护窗（只放行 P0）拒绝，当天轮次不执行、门控静默失败；看板同链路失败
被吞成「请下载指数日线」。本文件覆盖：
1) 保护窗内 P0 可通行、P1 仍被拒（闸本身的行为基线）；
2) DailyCloser 取 1d 日线走 P0；
3) 实盘门控 tracker 构建走 P0 + 透传 raise_gate_errors；
4) 门控更新失败落 risk 事件 + 决策事件（fail-open 不变、每日去重）；
5) 看板优先读 live_gate_snapshots，无快照回退实时取数。
"""
from datetime import date, datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.api import market_gate as market_gate_api
from core.engine.live.daily_closer import DailyCloser
from core.engine.live_engine import LiveEngine
from core.models import Base, LiveDecisionEvent, LiveGateSnapshot, PortfolioStrategy
from core.services import live_service
from core.services import market_gate_service as svc
from core.tq import gate as g
from core.tq.gate import P0, P1, TdxTaskBlocked, current_priority
from core.tq.utils import get_tdx_lock

_TZ = timezone(timedelta(hours=8))
_CFG = {
    "enabled": True,
    "pre_market_p2": ["08:00", "09:10"],
    "lunch_p2": ["11:30", "13:00"],
    "after_close_p2": ["15:15", "24:00"],
    "trading": [["09:10", "11:30"], ["13:00", "15:10"]],
    "p0_only": ["14:55", "15:10"],
    "p1_max_per_minute": 6000,
    "p2_max_per_minute": 6000,
    "p2_chunk_size": 60,
    "override_max_minutes": 120,
}


def _at(hh: int, mm: int) -> datetime:
    """2026-11-23 周一（交易日）15:05 收盘保护窗内。"""
    return datetime(2026, 11, 23, hh, mm, tzinfo=_TZ)


@pytest.fixture(autouse=True)
def _gate(monkeypatch):
    monkeypatch.delenv("TQ_GATE_FORCE_ALLOW", raising=False)
    g.reset_for_tests(config=dict(_CFG), clock=lambda: _at(15, 5),
                      trading_day_fn=lambda d: d.weekday() < 5,
                      live_running_fn=lambda: True)
    g.clear_override()
    g.GATE.reset_state()
    yield
    g.clear_override()
    g.reset_for_tests(config=None, clock=None, trading_day_fn=None,
                      live_running_fn=None)
    g.GATE.reset_state()


def _make_db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


# --------------------------------------------------------------- 闸行为基线

def test_protected_window_allows_p0_and_still_rejects_p1():
    """保护窗只挡 P1/P2；P0 可通行（实盘主链靠它放行）。"""
    with pytest.raises(TdxTaskBlocked):
        g.GATE.acquire(P1, timeout=1)
    assert g.GATE.acquire(P0, timeout=1)
    g.GATE.release()


# ------------------------------------------------------- 日线轮次取数走 P0

class _Disp:
    """模拟 dispatcher：用默认优先级抢通道（不显式传 P0 就会被闸拒绝）。"""

    def __init__(self):
        self.priorities = []
        self.bars = [{
            "stime": "20261123", "open": "9.0", "high": "9.3",
            "low": "8.9", "close": "9.1", "volume": "100000",
        }]

    def query_quote(self, code, period="1d", count=250):
        # 与 TQData.get_history_raw 一致：不显式传优先级 → 取线程默认优先级
        with get_tdx_lock():
            self.priorities.append(current_priority())
            return self.bars


class _MD:
    _strategy_periods = {"1d"}
    _period_count = {"1d": 10}
    _formula_count = 10
    _bar_poller = SimpleNamespace(stock_codes=["600000.SH"])

    def _sort_and_cap(self, bars, count):
        return bars


def _mk_closer(dispatcher):
    ctx = SimpleNamespace(
        session_id=1, portfolios=[], db_session_factory=None,
        dispatcher=dispatcher, code_period_count={},
        clock=lambda: _at(15, 5),
    )
    return DailyCloser(
        ctx=ctx, market_data=_MD(), breaker=None, orders=None,
        on_bar=lambda *a, **k: None,
        calendar_provider=lambda: SimpleNamespace(),
        emit=lambda *a, **k: None,
        set_bridge_offline=lambda: None,
    )


def test_daily_bars_fetch_runs_as_p0_inside_protected_window():
    disp = _Disp()
    closer = _mk_closer(disp)
    bars = closer._fetch_1d_bars()
    assert "600000.SH" in bars          # 保护窗内未被闸拒绝
    assert disp.priorities == [P0]


def test_daily_bars_fetch_control_p1_would_be_blocked():
    """对照：同一时刻直接以默认 P1 取数仍被拒（证明放行来自 P0 包装）。"""
    with pytest.raises(TdxTaskBlocked):
        with get_tdx_lock():
            pass


# ----------------------------------------------- 实盘门控 tracker 构建走 P0

def test_live_gate_tracker_built_under_p0(monkeypatch):
    from core.services import backtest_service

    seen = {}

    def fake_build(ps, start, end, enabled=None, **kwargs):
        seen["priority"] = current_priority()
        seen["kwargs"] = kwargs
        return "tracker"

    monkeypatch.setattr(backtest_service, "build_gate_tracker", fake_build)
    spec = {"market_gate_enabled": True,
            "benchmark_index": "000300.SH",
            "market_gate_engine": "python"}
    out = live_service.build_live_gate_tracker(
        spec, date(2026, 11, 22), date(2026, 11, 23))
    assert out == "tracker"
    assert seen["priority"] == P0
    assert seen["kwargs"].get("raise_gate_errors") is True


# ------------------------------------------------ 门控更新失败显式化（risk）

def test_gate_update_failure_emits_risk_and_persists_event():
    SessionLocal = _make_db()
    db = SessionLocal()
    p = PortfolioStrategy(name="p1", stock_pool_id=1, initial_capital=100000)
    db.add(p)
    db.commit()
    created = live_service.create_session(db, {
        "name": "s1", "engine": "tdx", "execution_mode": "alert",
        "portfolio_ids": [p.id],
    }, "simulation")

    blocked = TdxTaskBlocked(
        "实盘收盘保护窗口（14:55–15:10）内暂停实盘辅助 TQ 任务",
        priority=P1, reason="protected_window", window="14:55–15:10")

    def provider(pid, bar_date):
        raise blocked

    eng = LiveEngine(
        session_id=created["id"], portfolios=[],
        dispatcher=SimpleNamespace(query_calendar=lambda: []),
        bar_poller=object(), db_session_factory=SessionLocal,
        gate_provider=provider, gate_portfolio_ids={p.id},
        gate_specs={p.id: {"benchmark_index": "000300.SH"}},
    )
    risks = []
    eng._emit = lambda kind, payload: risks.append((kind, payload))

    portfolio = SimpleNamespace(portfolio_id=p.id, current_market_gate="旧门控")
    today = date(2026, 11, 23)
    eng._maybe_update_gate(portfolio, today)

    # fail-open：旧门控状态保留，且不写「当日已更新」水位（后续 tick 仍可重试）
    assert portfolio.current_market_gate == "旧门控"
    assert eng._gate_updated.get(p.id) is None
    # risk 事件可见，并区分「闸拒绝」
    assert len(risks) == 1
    kind, payload = risks[0]
    assert kind == "risk"
    assert payload["gate"] == "market_gate_update"
    assert payload["action"] == "fail"
    assert payload["reason"] == "gate_blocked"
    assert "保护窗" in payload["message"]
    # 决策事件落库
    rows = db.query(LiveDecisionEvent).filter_by(
        live_session_id=created["id"]).all()
    assert len(rows) == 1
    assert rows[0].gate == "market_gate_update"
    assert rows[0].param_name == "gate_blocked"
    assert rows[0].action == "fail"
    # 同一交易日重复失败不刷屏（每日每组合一条）
    eng._maybe_update_gate(portfolio, today)
    assert len(risks) == 1
    assert db.query(LiveDecisionEvent).count() == 1


# ------------------------------------------- 看板：快照优先 / 无快照回退

def _snapshot(db, *, as_of, direction=-1, strength=40,
              benchmark="000300.SH", session_id=7, pid=9,
              snap_date=None):
    row = LiveGateSnapshot(
        live_session_id=session_id, portfolio_strategy_id=pid,
        snap_date=snap_date or as_of, benchmark_index=benchmark,
        gate_engine="python", direction=direction, strength=strength,
        as_of=as_of)
    db.add(row)
    db.commit()
    return row


def test_snapshot_payload_keeps_existing_keys_and_marks_source():
    db = _make_db()()
    today = date.today()
    _snapshot(db, as_of=today)
    data = svc.fetch_gate_status_from_snapshot(db, benchmark_index="000001.SH")
    assert data is not None
    for key in ("code", "benchmark_name", "period", "as_of", "direction",
                "direction_label", "strength", "since", "duration_bars",
                "transitions", "bars", "latest_trading_day", "stale_days",
                "stale", "cached"):
        assert key in data
    assert data["source"] == "live_snapshot"
    assert data["data_time"] == today.isoformat()
    assert data["code"] == "000300.SH"      # 实盘快照的基准（不是请求默认值）
    assert data["code_matched"] is False


def test_snapshot_missing_returns_none():
    db = _make_db()()
    assert svc.fetch_gate_status_from_snapshot(db) is None


def test_status_route_prefers_snapshot_and_skips_realtime(monkeypatch):
    db = _make_db()()
    today = date.today()
    _snapshot(db, as_of=today)

    def boom(*a, **k):
        raise AssertionError("有快照时不得实时取数")

    monkeypatch.setattr(market_gate_api, "fetch_gate_status", boom)
    resp = market_gate_api.market_gate_status(
        code="000001.SH", period="1d", count=500, history=5,
        refresh=False, db=db)
    assert resp["code"] == 0
    assert resp["data"]["source"] == "live_snapshot"
    assert resp["data"]["direction"] == -1


def test_status_route_falls_back_to_realtime_without_snapshot(monkeypatch):
    db = _make_db()()
    sentinel = {"code": "000001.SH", "source": "live_fetch", "direction": 1}
    calls = {"n": 0}

    def fake(*a, **k):
        calls["n"] += 1
        return sentinel

    monkeypatch.setattr(market_gate_api, "fetch_gate_status", fake)
    resp = market_gate_api.market_gate_status(
        code="000001.SH", period="1d", count=500, history=5,
        refresh=False, db=db)
    assert calls["n"] == 1
    assert resp["code"] == 0
    assert resp["data"] is sentinel


def test_status_route_reports_distinct_failure_reasons(monkeypatch):
    db = _make_db()()

    def fake(*a, **k):
        raise svc._unavailable("门控基准被 TQ 通道闸拒绝", "gate_blocked")

    monkeypatch.setattr(market_gate_api, "fetch_gate_status", fake)
    resp = market_gate_api.market_gate_status(
        code="000001.SH", period="1d", count=500, history=5,
        refresh=False, db=db)
    assert resp["code"] == 400
    assert "闸拒绝" in resp["message"]
    assert resp["data"]["reason"] == "gate_blocked"
