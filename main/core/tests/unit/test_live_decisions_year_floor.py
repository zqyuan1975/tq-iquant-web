"""决策事件读取侧年份下限过滤单测（2026-11-13）。

背景：旧 ``parse_bar_time`` 把 ``20261106`` 当 epoch 秒，2026-11-06 一次性写入
66 条 ``bar_time = 1970-08-23 20:05:06`` 的脏行（全部会话 4，id 836–901）。
写入侧已修，不会再产生新的；历史脏行按约定**保留在库里**以备追溯，故在读取侧
加一道年份下限（``bar_time >= 2000-01-01``），让脏行不进决策事件列表与统计聚合。
"""
from datetime import datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.models import Base, LiveDecisionEvent, LiveSession
from core.services.live_service import session_decisions

_DIRTY = datetime(1970, 8, 23, 20, 5, 6)     # epoch 秒误解析的历史脏行
_NORMAL = datetime(2026, 11, 6, 10, 30)


def _db(extra_null=False):
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    db = SessionLocal()
    db.add(LiveSession(id=4, name="s4", mode="live", engine="tdx", status="stopped"))
    db.add(LiveDecisionEvent(
        live_session_id=4, gate="stop_loss", layer="strategy_risk",
        action="trigger", portfolio_id=4, stock_code="002922.SZ",
        bar_time=_DIRTY, param_name="stop_loss_ratio",
        param_value=0.05, actual_value=0.06, message="epoch 秒误解析脏行",
    ))
    db.add(LiveDecisionEvent(
        live_session_id=4, gate="stop_loss", layer="strategy_risk",
        action="trigger", portfolio_id=4, stock_code="002922.SZ",
        bar_time=_NORMAL, param_name="stop_loss_ratio",
        param_value=0.05, actual_value=0.061, message="正常行",
    ))
    if extra_null:
        # bar_time=NULL 是正常事件（peak_seed 审计），不得被年份下限误杀
        db.add(LiveDecisionEvent(
            live_session_id=4, gate="peak_seed", layer="live_gate",
            action="fallback", portfolio_id=4, strategy_id=1,
            stock_code="002922.SZ", bar_time=None, message="峰值补种无可用数据",
        ))
    db.commit()
    return db


def test_dirty_epoch_row_hidden_from_events_and_summary():
    """1970 脏行不出现在事件列表，也不污染聚合的计数与首末次时间。"""
    db = _db()
    data = session_decisions(db, 4)
    db.close()

    assert [e["message"] for e in data["events"]] == ["正常行"]
    assert len(data["summary"]) == 1
    row = data["summary"][0]
    assert row["gate"] == "stop_loss"
    assert row["count"] == 1
    assert row["first_bar_time"] == _NORMAL
    assert row["last_bar_time"] == _NORMAL


def test_normal_row_unaffected():
    """正常行字段完整返回（过滤不误伤）。"""
    db = _db()
    data = session_decisions(db, 4)
    db.close()

    assert len(data["events"]) == 1
    ev = data["events"][0]
    assert ev["message"] == "正常行"
    assert ev["bar_time"].startswith("2026-11-06T10:30:00")
    assert ev["param_value"] == 0.05
    assert ev["actual_value"] == 0.061


def test_null_bar_time_event_not_dropped():
    """bar_time 为 NULL 的正常事件（peak_seed 审计）不被年份下限误杀。"""
    db = _db(extra_null=True)
    data = session_decisions(db, 4)
    db.close()

    assert {e["message"] for e in data["events"]} == {"正常行", "峰值补种无可用数据"}
    assert {s["gate"] for s in data["summary"]} == {"stop_loss", "peak_seed"}
