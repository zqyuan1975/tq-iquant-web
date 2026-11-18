# -*- coding: utf-8 -*-
"""实盘公司行为接入的轻量验证（J1/J2，2026-11-18）。

不启动 LiveEngine（需要行情/通道），只验证三件结构性问题：
1. 未配置任何组合时 ``_maybe_apply_corporate_actions`` 第一行即返回（不读库、不改账）；
2. 上一交易日回退（事件日落在周末 → 顺延到周一，且 ``after`` 是周五）；
3. 事件集合 digest 稳定且对内容敏感（进 params_snapshot + 增量指纹）。
"""
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.corporate_actions import (
    CorporateActionEvent, CorporateActionTimeline,
)
from core.engine.live_engine import LiveEngine
from core.models import Base, LiveCorporateAction, LiveSession, StockPool
from core.services import live_service


class _Calendar:
    def is_trading_day(self, day):
        return day.weekday() < 5


def test_no_specs_returns_immediately():
    engine = LiveEngine.__new__(LiveEngine)
    engine._corporate_action_specs = {}
    # 只要第一行生效，就不会去读这些属性（未设置，访问即 AttributeError）
    engine._maybe_apply_corporate_actions(date(2026, 7, 20))


def test_prev_trading_day_skips_weekend():
    engine = LiveEngine.__new__(LiveEngine)
    engine._calendar = _Calendar()
    # 2026-07-20 是周一 → 上一交易日为周五 07-17
    assert engine._ca_prev_trading_day(date(2026, 7, 20)) == date(2026, 7, 17)
    # 周日 → 周五
    assert engine._ca_prev_trading_day(date(2026, 7, 19)) == date(2026, 7, 17)


def test_timeline_digest_stable_and_sensitive():
    def _tl(ratio):
        return CorporateActionTimeline([
            CorporateActionEvent(code="600000.SH", event_type="dividend",
                                 ex_date=date(2026, 7, 20),
                                 cash_per_share=ratio),
            CorporateActionEvent(code="600000.SH", event_type="split",
                                 ex_date=date(2026, 7, 21),
                                 share_ratio=ratio),
        ])

    a, b = _tl("0.35"), _tl("0.35")
    assert a.digest() == b.digest()
    assert a.digest() != _tl("0.36").digest()
    assert len(a) == 2
    assert CorporateActionTimeline([]).is_empty()


def test_events_up_to_handles_postpone_and_window():
    events = [CorporateActionEvent(code="600000.SH", event_type="dividend",
                                   ex_date=date(2026, 7, 18),
                                   cash_per_share=Decimal("1"))]
    tl = CorporateActionTimeline(events)
    # 周五（事件日之前）不生效；周一（事件日之后）生效；再调用窗口为空
    assert tl.events_up_to(date(2026, 7, 17)) == []
    assert len(tl.events_up_to(date(2026, 7, 20), after=date(2026, 7, 17))) == 1
    assert tl.events_up_to(date(2026, 7, 20), after=date(2026, 7, 20)) == []


# ---------------- C5：启发式送转落台账 ----------------
def _db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    return SessionLocal()


def test_heuristic_share_change_recorded_once_per_day():
    db = _db()
    try:
        db.add(StockPool(id=1, code="POOL1", name="池"))
        db.add(LiveSession(id=1, name="会话", mode="live", status="running"))
        db.commit()

        class _Shim:
            session_id = 1
            _record_heuristic_share_change = \
                LiveEngine._record_heuristic_share_change

        shim = _Shim()
        port = SimpleNamespace(portfolio_id=1)
        shim._record_heuristic_share_change(db, port, "600000.SH", 300)
        db.commit()
        rows = db.query(LiveCorporateAction).all()
        assert len(rows) == 1
        assert rows[0].event_type == "split" and rows[0].qty_delta == 300
        assert rows[0].amount == Decimal("0")

        # 同一会话/组合/代码/事件日重复识别 → 幂等，不追加
        shim._record_heuristic_share_change(db, port, "600000.SH", 300)
        db.commit()
        assert db.query(LiveCorporateAction).count() == 1

        listed = live_service.list_live_corporate_actions(db, 1)
        assert len(listed) == 1 and listed[0]["qty_delta"] == 300
        assert live_service.list_live_corporate_actions(db, 999) is None
    finally:
        db.close()
