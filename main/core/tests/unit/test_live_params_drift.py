"""H2 参数绑定：params_drift 默认只告警、绝不阻断启动；可选开关才 409（A7）。

不启动真实引擎：build_engine / resolve_stock_codes / preflight 全部 mock。
"""
import json
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from core.models import (
    BacktestRecord, LiveDecisionEvent, LiveSession, LiveSessionPortfolio,
    PortfolioStrategy,
)
from core.services import live_service as svc


def _fake_engine():
    dispatcher = SimpleNamespace(
        heartbeat=lambda: True, channel="alert", last_error="",
        execute_mode="direct",
    )
    return SimpleNamespace(
        dispatcher=dispatcher, start_warnings=[], portfolios=[],
        recover=lambda db: None,
    )


def _mk_ps(db, *, initial_capital="500000.00", max_holdings=10):
    ps = PortfolioStrategy(
        name="p", stock_pool_id=1,
        initial_capital=Decimal(initial_capital), max_holdings=max_holdings,
    )
    db.add(ps)
    db.commit()
    db.refresh(ps)
    return ps


def _mk_record(db, ps_id, snapshot):
    rec = BacktestRecord(
        portfolio_strategy_id=ps_id, name="r",
        start_date=date(2026, 1, 1), end_date=date(2026, 6, 30),
        status="completed", params_snapshot=json.dumps(snapshot),
    )
    db.add(rec)
    db.commit()
    db.refresh(rec)
    return rec


def _mk_session(db, record_id=None, portfolio_id=None):
    row = LiveSession(
        name="s", mode="simulation", engine="tdx", status="stopped",
        execution_mode="alert", backtest_record_id=record_id,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    if portfolio_id is not None:
        db.add(LiveSessionPortfolio(
            session_id=row.id, portfolio_strategy_id=portfolio_id))
        db.commit()
    return row


def test_unbound_session_has_no_drift(db_session):
    session = _mk_session(db_session)
    assert svc.compute_params_drift(db_session, session) is None


def test_drift_detected_and_run_level_fields_ignored(db_session):
    ps = _mk_ps(db_session)
    rec = _mk_record(db_session, ps.id, {
        "initial_capital": "800000.00",   # 与组合配置不一致 → 漂移
        "max_holdings": 10,               # 数值归一后相等 → 不漂移
        "volume_limit_ratio": "0.30",     # run 级覆盖字段 → 跳过
    })
    session = _mk_session(db_session, rec.id)
    drift = svc.compute_params_drift(db_session, session)
    assert drift["checked"] is True
    assert drift["drifted"] is True
    names = [f["field"] for f in drift["fields"]]
    assert names == ["initial_capital"]
    assert drift["block_enabled"] is False


def test_numeric_normalization_avoids_false_positive(db_session):
    ps = _mk_ps(db_session, initial_capital="500000.00")
    rec = _mk_record(db_session, ps.id, {
        "initial_capital": "500000.0",    # 只是格式差异
        "max_holdings": "10.0",
    })
    session = _mk_session(db_session, rec.id)
    drift = svc.compute_params_drift(db_session, session)
    assert drift["drifted"] is False and drift["fields"] == []


def test_params_drift_does_not_block_start(db_session, monkeypatch):
    ps = _mk_ps(db_session)
    rec = _mk_record(db_session, ps.id, {"initial_capital": "800000.00"})
    session = _mk_session(db_session, rec.id, portfolio_id=ps.id)

    monkeypatch.setattr(svc, "load_config", lambda: {"tq_trade": {}})
    monkeypatch.setattr(svc, "resolve_stock_codes",
                        lambda db, pid: {"600000.SH"})
    monkeypatch.setattr(svc, "build_engine", lambda *a, **kw: _fake_engine())
    monkeypatch.setattr(svc, "preflight_signal_config",
                        lambda db, sid, mode: None)
    monkeypatch.setattr(svc, "recommended_channel_warnings",
                        lambda db, session, engines: [])

    engine, sess, err = svc.start_session(
        db_session, session.id, {}, "simulation")
    assert err is None and engine is not None
    events = (
        db_session.query(LiveDecisionEvent)
        .filter(LiveDecisionEvent.live_session_id == session.id).all()
    )
    assert [e for e in events if e.gate == "params_drift" and e.action == "notify"]


def test_params_drift_block_switch_returns_409(db_session, monkeypatch):
    ps = _mk_ps(db_session)
    rec = _mk_record(db_session, ps.id, {"initial_capital": "800000.00"})
    session = _mk_session(db_session, rec.id)

    monkeypatch.setattr(svc, "load_config", lambda: {
        "tq_trade": {"params_drift_block": True},
    })

    engine, sess, err = svc.start_session(
        db_session, session.id, {}, "simulation")
    assert engine is None
    assert err is not None and err[0] == 409
    assert err[2]["rule"] == "params_drift"
    assert any(f["field"] == "initial_capital" for f in err[2]["fields"])
