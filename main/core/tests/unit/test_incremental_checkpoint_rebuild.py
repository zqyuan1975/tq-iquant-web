# -*- coding: utf-8 -*-
"""K3 接缝判废可观测 + 一键重建（2026-11-18）。

覆盖：判废落 live_decision_events（summary 暴露 last_invalidation）；
force_rebuild 无视有效 checkpoint 强制从接缝日重建；未判废时 summary 为 None。
"""
from datetime import date
from decimal import Decimal

import sqlalchemy as sa

from core.db import MAINTENANCE_GATE
from core.models import LiveDecisionEvent
from core.services import incremental_backtest as ib

from core.tests.unit.test_incremental_backtest import (
    _minute_klines, _minute_sig_cache, _seed_minute_checkpoint,
    _seed_minute_link,
)


def test_invalidation_records_decision_event_and_summary(db_session, monkeypatch):
    sess, ps, link, strat = _seed_minute_link(db_session)
    _seed_minute_checkpoint(db_session, sess, ps, as_of=date(2026, 1, 5),
                            as_of_bar=None, day_complete=True)
    # 改参数 → 参数指纹变化 → checkpoint 判废
    strat.stop_loss_ratio = Decimal("0.09")
    db_session.commit()
    monkeypatch.setattr(
        ib, "build_klines",
        lambda *a, **k: _minute_klines([date(2026, 1, 5), date(2026, 1, 6)]))
    monkeypatch.setattr(ib, "build_signal_cache",
                        lambda *a, **k: _minute_sig_cache(strat.id, []))

    result = ib.run_incremental(
        db_session, sess.id, ps.id, today=date(2026, 1, 6))
    assert result["status"] == "ok"
    assert result["checkpoint_invalidated"]

    events = (
        db_session.query(LiveDecisionEvent)
        .filter_by(live_session_id=sess.id, gate="incremental_checkpoint",
                   action="invalidated")
        .all()
    )
    assert len(events) == 1
    assert events[0].message
    assert float(events[0].actual_value) == float(ps.id)

    s = ib.summary(db_session, sess.id, ps.id)
    assert s["last_invalidation"] is not None
    assert s["last_invalidation"]["message"] == events[0].message
    assert s["last_invalidation"]["at"]


def test_force_rebuild_discards_valid_checkpoint(db_session, monkeypatch):
    sess, ps, link, strat = _seed_minute_link(db_session)
    monkeypatch.setattr(
        ib, "build_klines", lambda *a, **k: _minute_klines([date(2026, 1, 5)]))
    monkeypatch.setattr(ib, "build_signal_cache",
                        lambda *a, **k: _minute_sig_cache(strat.id, []))

    first = ib.run_incremental(
        db_session, sess.id, ps.id, today=date(2026, 1, 5))
    assert first["status"] == "ok"
    assert not first.get("checkpoint_invalidated")

    # 同日再跑：正常路径会「当日已续跑」直接返回；强制重建必须真跑
    second = ib.run_incremental(
        db_session, sess.id, ps.id, today=date(2026, 1, 5), force_rebuild=True)
    assert second["status"] == "ok"
    assert second["checkpoint_invalidated"]["categories"] == ["用户手动重建接缝"]
    assert second["days"] == 1

    s = ib.summary(db_session, sess.id, ps.id)
    assert s["last_invalidation"]["message"] == "用户手动重建接缝"


def test_summary_last_invalidation_none_when_never_invalidated(db_session):
    sess, ps, link, strat = _seed_minute_link(db_session)
    assert ib.summary(db_session, sess.id, ps.id)["last_invalidation"] is None


# ---------------------------------------------------------------- 锁争用（2026-11-20）
def test_invalidation_defers_writes_until_after_engine_run(db_session, monkeypatch):
    """判废事件与旧 checkpoint 删除必须推迟到计算之后。

    原实现在计算前就 delete+flush，SQLite 库级写锁横跨整个回测计算，与 15:10
    自动备份同分钟触发时实盘快照写入 5s 超时报 database is locked。
    """
    sess, ps, link, strat = _seed_minute_link(db_session)
    _seed_minute_checkpoint(db_session, sess, ps, as_of=date(2026, 1, 5),
                            as_of_bar=None, day_complete=True)
    strat.stop_loss_ratio = Decimal("0.09")
    db_session.commit()
    monkeypatch.setattr(
        ib, "build_klines",
        lambda *a, **k: _minute_klines([date(2026, 1, 5), date(2026, 1, 6)]))
    monkeypatch.setattr(ib, "build_signal_cache",
                        lambda *a, **k: _minute_sig_cache(strat.id, []))

    dml = []
    bind = db_session.get_bind()

    def _before(conn, cursor, statement, params, context, executemany):
        if statement.lstrip()[:6].upper() in ("INSERT", "UPDATE", "DELETE"):
            dml.append(statement.strip().split(None, 1)[0].upper())

    sa.event.listen(bind, "before_cursor_execute", _before)
    seen = {}
    real_cls = ib.BacktestEngine

    class _Spy(real_cls):
        def run(self, *a, **k):
            seen["dml_during_run"] = list(dml)
            return super().run(*a, **k)

    monkeypatch.setattr(ib, "BacktestEngine", _Spy)
    try:
        result = ib.run_incremental(
            db_session, sess.id, ps.id, today=date(2026, 1, 6))
    finally:
        sa.event.remove(bind, "before_cursor_execute", _before)

    assert result["status"] == "ok" and result["checkpoint_invalidated"]
    assert seen["dml_during_run"] == [], "计算期间不得持有 SQLite 写事务"
    # 语义不变：判废事件仍在重建事务内落库
    assert db_session.query(LiveDecisionEvent).filter_by(
        live_session_id=sess.id, gate="incremental_checkpoint").count() == 1


def test_run_incremental_defers_when_backup_holds_gate(db_session, monkeypatch):
    """自动备份持闸时增量回测本轮让出（不写库、不报错），由调度器稍后重试。"""
    monkeypatch.setattr(ib, "GATE_WAIT_SECONDS", 0.0)
    assert MAINTENANCE_GATE.acquire(blocking=False)
    try:
        r = ib.run_incremental(db_session, 1, 1, today=date(2026, 1, 6))
    finally:
        MAINTENANCE_GATE.release()
    assert r["status"] == "deferred"
    assert "自动备份进行中" in r["reason"]
