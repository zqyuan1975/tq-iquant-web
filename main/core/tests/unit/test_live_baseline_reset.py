"""资金基准重置接口：交易日判据 / 盘中闸 / 只缩不放 / 四锚点 / 计数与手动恢复保留。

A5（2026-11-16）：非交易日直接 403「今天不是交易日」，不再只说「盘中禁止」。
"""
import json
from datetime import date, datetime
from decimal import Decimal

from core.models import (
    LiveAccountSnapshot, LiveBaselineReset, LiveSession, LiveSessionPortfolio,
)
from core.services import live_service as svc


class _Cal:
    def __init__(self, trading: bool):
        self.trading = trading

    def is_trading_day(self, d):  # noqa: ARG002
        return self.trading


def _mk_session(db, *, baseline_value=None, version=0):
    row = LiveSession(
        name="s", mode="simulation", engine="tdx", status="stopped",
        execution_mode="alert", baseline_value=baseline_value,
        live_capital=baseline_value, baseline_version=version,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _mk_link(db, session_id, portfolio_id=1, risk_state=None):
    link = LiveSessionPortfolio(
        session_id=session_id, portfolio_strategy_id=portfolio_id,
        risk_state=risk_state, circuit_breaker_count=2,
        seam_date=date(2026, 11, 10), seam_cash=Decimal("1000.00"),
        seam_trade_cursor=7,
    )
    db.add(link)
    db.commit()
    db.refresh(link)
    return link


def _mk_snapshot(db, session_id, total="263515.51"):
    row = LiveAccountSnapshot(
        live_session_id=session_id, engine="tdx", captured_at=datetime.now(),
        total_asset=Decimal(total), available_cash=Decimal("108513.51"),
    )
    db.add(row)
    db.commit()
    return row


def _freeze(monkeypatch, *, now, trading):
    monkeypatch.setattr(svc, "now_shanghai", lambda: now)
    monkeypatch.setattr(svc, "_baseline_reset_calendar", lambda: _Cal(trading))


def test_window_rejects_non_trading_day():
    ok, reason = svc._baseline_reset_window_allowed(
        datetime(2026, 11, 16, 15, 30), calendar=_Cal(False))
    assert ok is False
    assert "今天不是交易日" in reason


def test_window_rejects_intraday_on_trading_day():
    ok, reason = svc._baseline_reset_window_allowed(
        datetime(2026, 11, 16, 10, 0), calendar=_Cal(True))
    assert ok is False
    assert "盘中禁止重置" in reason


def test_window_allows_after_close():
    ok, reason = svc._baseline_reset_window_allowed(
        datetime(2026, 11, 16, 15, 30), calendar=_Cal(True))
    assert ok is True and reason == ""


def test_reset_403_on_non_trading_day(db_session, monkeypatch):
    session = _mk_session(db_session)
    _mk_link(db_session, session.id)
    _mk_snapshot(db_session, session.id)
    _freeze(monkeypatch, now=datetime(2026, 11, 16, 15, 30), trading=False)
    ok, err = svc.reset_baseline(db_session, session.id, 1, None)
    assert ok is False
    assert err[0] == 403
    assert "今天不是交易日" in err[1]


def test_reset_403_intraday(db_session, monkeypatch):
    session = _mk_session(db_session)
    _mk_link(db_session, session.id)
    _mk_snapshot(db_session, session.id)
    _freeze(monkeypatch, now=datetime(2026, 11, 16, 10, 0), trading=True)
    ok, err = svc.reset_baseline(db_session, session.id, 1, None)
    assert ok is False
    assert err[0] == 403
    assert "盘中禁止重置" in err[1]


def test_reset_only_shrink_rejects_above_total_asset(db_session, monkeypatch):
    session = _mk_session(db_session, baseline_value=Decimal("300000.00"))
    _mk_link(db_session, session.id)
    _mk_snapshot(db_session, session.id, total="263515.51")
    _freeze(monkeypatch, now=datetime(2026, 11, 16, 15, 30), trading=True)
    ok, err = svc.reset_baseline(db_session, session.id, 1, 270000)
    assert ok is False
    assert err[0] == 400
    assert "只缩不放" in err[1]


def test_reset_shrinks_to_total_and_reanchors_four_anchors(db_session, monkeypatch):
    session = _mk_session(
        db_session, baseline_value=Decimal("300000.00"), version=2)
    link = _mk_link(db_session, session.id, risk_state=json.dumps({
        "schema": 2,
        "peak_value": "290000.00",
        "prev_close_value": "285000.00",
        "circuit_breaker_count": 2,
        "manual_recovery": True,
        "paused_until": "2026-11-17",
        "last_bar_date": "2026-11-13",
    }))
    _mk_snapshot(db_session, session.id, total="263515.51")
    _freeze(monkeypatch, now=datetime(2026, 11, 16, 15, 30), trading=True)

    ok, err = svc.reset_baseline(db_session, session.id, 1, None)
    assert ok is True and err is None

    db_session.refresh(session)
    assert session.baseline_version == 3
    assert session.baseline_value == Decimal("263515.51")
    assert session.baseline_mode == "total_asset"

    db_session.refresh(link)
    state = json.loads(link.risk_state)
    assert state["peak_value"] == "263515.51"
    assert state["prev_close_value"] == "263515.51"
    assert state["last_bar_total_value"] == "263515.51"
    assert state["last_bar_date"] == "2026-11-16"
    # 累计触发次数与手动恢复标志原样保留（重置不等于解锁熔断）
    assert link.circuit_breaker_count == 2
    assert state["manual_recovery"] is True
    assert state["paused_until"] == "2026-11-17"
    # 未运行：接缝清空，待下次启动 recover 后重写
    assert link.seam_date is None
    assert link.seam_cash is None
    assert link.seam_trade_cursor == 0

    audit = db_session.query(LiveBaselineReset).filter_by(
        live_session_id=session.id).one()
    assert audit.old_baseline == Decimal("300000.00")
    assert audit.new_baseline == Decimal("263515.51")
    assert audit.baseline_version == 3
    # 起账基准按 min(填写, 真实总资产) 只缩不放
    assert svc.session_baseline_value(session) == float(Decimal("263515.51"))
