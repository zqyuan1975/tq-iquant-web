"""通道故障留痕 / 盘前判据 / 心跳转场留痕（2026-11-13）。

事故：通达信引擎随客户端在盘前退出，会话仍 running，只有 logger/SSE，不落
``live_decision_events``，前端也看不到通道状态。本文件锁定四项口径：

1. watchdog 首次判定即落一条 ``channel_offline``（重复周期不重复落），恢复落
   ``channel_recovered``；``engine_stale`` 属引擎层，不冒充通道状态；
2. 盘前 08:55–09:30 只看客户端窗口/进程是否存在（不依赖心跳侧证）；盘后不判；
3. 连续竞价的 ``bridge_online=False`` 与 tdx 客户端消失都能判；
4. LiveEngine 心跳在线态翻转（True→False / False→True）各留一条事件，短窗口
   闪断也能在库里查到。
"""
from datetime import datetime, timedelta
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.live.engine_watchdog import EngineWatchdog, in_pre_open_window
from core.engine.live_engine import LiveEngine
from core.models import Base, LiveDecisionEvent

NOW = datetime(2026, 11, 5, 10, 30, 0)  # 周四，连续竞价
PRE_OPEN = datetime(2026, 11, 5, 9, 10, 0)  # 周四盘前集合竞价


class _Dispatcher:
    def __init__(self, channel):
        self.channel = channel


class _Engine:
    def __init__(self, channel="tdx_tq", started=None, tick=None, running=True):
        self.session_id = 1
        self._running = running
        self._started_at = started
        self._last_tick_at = tick
        self._dispatcher = _Dispatcher(channel)
        self._bridge_online = True
        self._available_bar = None
        self._snapshot_synth = None
        self._file_handoff = None
        self.events = []

    def _emit(self, event_type, payload):
        self.events.append({"type": event_type, **payload})


class _Recorder:
    def __init__(self):
        self.rows = []

    def __call__(self, **kw):
        self.rows.append(kw)


def _watchdog(engines, **kw):
    kw.setdefault("now_fn", lambda: NOW)
    kw.setdefault("mono_fn", lambda: 0.0)
    kw.setdefault("snapshot_fn", lambda sid: None)
    kw.setdefault("in_session_fn", lambda dt: True)
    kw.setdefault("client_online_fn", lambda ch: True)
    kw.setdefault("timeout_seconds", 10 ** 9)
    kw.setdefault("execution_stall_seconds", 180)
    kw.setdefault("bridge_timeout_seconds", 90)
    kw.setdefault("market_stall_seconds", 180)
    kw.setdefault("grace_seconds", 120)
    return EngineWatchdog(lambda: engines, **kw)


def _risk(eng, rule):
    return [e for e in eng.events if e["type"] == "risk" and e.get("rule") == rule]


def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    def factory():
        return SessionLocal()

    return factory, engine


# ---------------------------------------------------------------- 1. 落库留痕

def test_bridge_offline_persisted_once_then_recovered():
    mono = [0.0]
    eng = _Engine(channel="goldminer_bridge",
                  started=NOW - timedelta(seconds=900), tick=NOW)
    eng._bridge_online = False
    rec = _Recorder()
    wd = _watchdog({1: eng}, bridge_timeout_seconds=90,
                   mono_fn=lambda: mono[0], persist_fn=rec)
    wd.scan()                       # 起点，未到阈值
    assert rec.rows == []

    mono[0] = 100.0
    wd.scan()                       # 首次判定 → 立即落库
    assert len(rec.rows) == 1
    row = rec.rows[0]
    assert row["gate"] == "channel_offline"
    assert row["action"] == "trigger"
    assert row["channel"] == "goldminer_bridge"
    assert row["session_id"] == 1
    assert row["seconds"] >= 100
    assert row["basis"] == "bridge_unreachable"

    mono[0] = 150.0
    wd.scan()                       # 300s 重复周期内不再重复落库
    assert len(rec.rows) == 1

    eng._bridge_online = True
    mono[0] = 200.0
    wd.scan()                       # 恢复 → 一条 recovered
    assert [r["gate"] for r in rec.rows] == [
        "channel_offline", "channel_recovered"]
    assert rec.rows[1]["action"] == "recover"
    assert rec.rows[1]["channel"] == "goldminer_bridge"


def test_engine_stale_is_not_persisted_as_channel_offline():
    eng = _Engine(channel="unknown",
                  started=NOW - timedelta(seconds=900),
                  tick=NOW - timedelta(seconds=200))
    rec = _Recorder()
    wd = _watchdog({1: eng}, timeout_seconds=90, persist_fn=rec)
    wd.scan()
    assert len(_risk(eng, "engine_stale")) == 1
    assert rec.rows == []


def test_persist_failure_does_not_break_scan_or_sse():
    eng = _Engine(channel="goldminer_bridge",
                  started=NOW - timedelta(seconds=900), tick=NOW)
    eng._bridge_online = False
    mono = [0.0]

    def boom(**kw):
        raise RuntimeError("db down")

    wd = _watchdog({1: eng}, bridge_timeout_seconds=90,
                   mono_fn=lambda: mono[0], persist_fn=boom)
    wd.scan()                       # 起点
    mono[0] = 100.0
    wd.scan()                       # 首次判定；落库抛异常不得影响 scan/SSE
    assert len(_risk(eng, "bridge_unreachable")) == 1


# ---------------------------------------------------------------- 2. 盘前时段

def test_in_pre_open_window_covers_auction_and_excludes_post_close():
    assert in_pre_open_window(PRE_OPEN) is True
    assert in_pre_open_window(datetime(2026, 11, 5, 8, 54, 0)) is False
    assert in_pre_open_window(datetime(2026, 11, 5, 9, 30, 0)) is False
    assert in_pre_open_window(datetime(2026, 11, 5, 15, 5, 0)) is False
    assert in_pre_open_window(datetime(2026, 11, 7, 9, 10, 0)) is False  # 周六


def test_pre_open_client_missing_alerts_and_persists():
    eng = _Engine(channel="tdx_tq",
                  started=PRE_OPEN - timedelta(seconds=900), tick=PRE_OPEN)
    rec = _Recorder()
    wd = _watchdog({1: eng}, now_fn=lambda: PRE_OPEN,
                   in_session_fn=lambda dt: False,
                   client_online_fn=lambda ch: False, persist_fn=rec)
    wd.scan()
    alerts = _risk(eng, "client_offline")
    assert len(alerts) == 1
    assert alerts[0]["basis"] == "client_window"
    assert len(rec.rows) == 1
    assert rec.rows[0]["gate"] == "channel_offline"
    assert rec.rows[0]["basis"] == "client_offline"


def test_pre_open_client_present_no_alarm():
    eng = _Engine(channel="tdx_tq",
                  started=PRE_OPEN - timedelta(seconds=900), tick=PRE_OPEN)
    rec = _Recorder()
    wd = _watchdog({1: eng}, now_fn=lambda: PRE_OPEN,
                   in_session_fn=lambda dt: False,
                   client_online_fn=lambda ch: True, persist_fn=rec)
    wd.scan()
    assert eng.events == []
    assert rec.rows == []


def test_post_close_client_missing_not_judged():
    post = datetime(2026, 11, 5, 15, 30, 0)
    eng = _Engine(channel="tdx_tq",
                  started=post - timedelta(seconds=900), tick=post)
    rec = _Recorder()
    wd = _watchdog({1: eng}, now_fn=lambda: post,
                   in_session_fn=lambda dt: False,
                   client_online_fn=lambda ch: False, persist_fn=rec)
    wd.scan()
    assert eng.events == []
    assert rec.rows == []


def test_in_session_client_missing_alerts_without_heartbeat_evidence():
    """连续竞价：客户端窗口消失即判，无需等 bridge_online/行情停摆。"""
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng._snapshot_synth = SimpleNamespace(last_completed_at=NOW)  # 行情仍新鲜
    rec = _Recorder()
    wd = _watchdog({1: eng}, client_online_fn=lambda ch: False, persist_fn=rec)
    wd.scan()
    assert len(_risk(eng, "client_offline")) == 1
    assert len(rec.rows) == 1


def test_client_offline_recovers():
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng._snapshot_synth = SimpleNamespace(last_completed_at=NOW)
    online = [False]
    rec = _Recorder()
    wd = _watchdog({1: eng}, client_online_fn=lambda ch: online[0], persist_fn=rec)
    wd.scan()
    assert len(_risk(eng, "client_offline")) == 1
    online[0] = True
    wd.scan()
    assert len([e for e in eng.events if e["type"] == "client_recovered"]) == 1
    assert [r["gate"] for r in rec.rows] == [
        "channel_offline", "channel_recovered"]


# ---------------------------------------------------------------- 4. 引擎心跳转场

def _bare_engine(factory):
    eng = LiveEngine.__new__(LiveEngine)
    # session_id 是穿透 ctx 的 property，必须先有 ctx
    eng.ctx = SimpleNamespace(session_id=1, db_session_factory=factory)
    eng._engine_key = "tdx"
    eng._dispatcher = _Dispatcher("tdx_tq")
    eng._bridge_online = True
    eng._offline_since = None
    return eng


def test_bridge_state_transitions_persist_once_each():
    factory, db_engine = _db_factory()
    eng = _bare_engine(factory)
    now = NOW

    eng._mark_bridge_offline(now)
    eng._mark_bridge_offline(now + timedelta(seconds=60))  # 持续离线不重复落
    db = factory()
    try:
        rows = db.query(LiveDecisionEvent).order_by(LiveDecisionEvent.id).all()
        assert [r.gate for r in rows] == ["channel_offline"]
        assert rows[0].action == "trigger"
        assert rows[0].message and "heartbeat_failed" in rows[0].message
    finally:
        db.close()

    eng._mark_bridge_online(now + timedelta(seconds=120))
    eng._mark_bridge_online(now + timedelta(seconds=180))  # 已在线不重复落
    db = factory()
    try:
        rows = db.query(LiveDecisionEvent).order_by(LiveDecisionEvent.id).all()
        assert [r.gate for r in rows] == [
            "channel_offline", "channel_recovered"]
        assert rows[1].action == "recover"
        # 恢复行带上「断了多久」：离线 60s 起算，120s 恢复 → ≥60s
        assert rows[1].actual_value >= 60
        assert "已持续" in (rows[1].message or "")
    finally:
        db.close()
    assert eng._bridge_online is True


def test_new_engine_instance_closes_previous_offline_row():
    """会话重启后新实例 ``_bridge_online`` 初值即 True：仍要补一条 recovered。

    2026-11-13 实证：id=1038（heartbeat_failed）重启后至今没有对应 recovered，
    因为 ``_mark_bridge_online`` 命中「已在线」直接返回。改为首个心跳时反查该
    会话/通道最新一条通道事件，若仍是 offline 则补恢复（每实例只查一次）。
    """
    factory, _ = _db_factory()
    db = factory()
    try:
        db.add(LiveDecisionEvent(
            live_session_id=1, gate="channel_offline", layer="live_gate",
            action="trigger", param_name="channel:tdx_tq", actual_value=0.0,
            message="旧进程留下的离线",
            created_at=datetime.utcnow() - timedelta(seconds=330),
        ))
        db.commit()
    finally:
        db.close()

    eng = _bare_engine(factory)
    eng._bridge_online = True          # 新实例初值即在线
    eng._mark_bridge_online(NOW)
    eng._mark_bridge_online(NOW)       # 重复心跳不重复补记
    db = factory()
    try:
        rows = db.query(LiveDecisionEvent).order_by(LiveDecisionEvent.id).all()
        assert [r.gate for r in rows] == [
            "channel_offline", "channel_recovered"]
        assert rows[1].actual_value >= 300
        assert "已持续" in (rows[1].message or "")
    finally:
        db.close()


def test_no_stale_recovery_when_last_event_already_recovered():
    """最新一条已是 recovered → 重启不补第二条（防重复恢复行）。"""
    factory, _ = _db_factory()
    db = factory()
    try:
        db.add(LiveDecisionEvent(
            live_session_id=1, gate="channel_recovered", layer="live_gate",
            action="recover", param_name="channel:tdx_tq", actual_value=10.0,
            message="已闭环"))
        db.commit()
    finally:
        db.close()
    eng = _bare_engine(factory)
    eng._bridge_online = True
    eng._mark_bridge_online(NOW)
    db = factory()
    try:
        rows = db.query(LiveDecisionEvent).all()
        assert [r.gate for r in rows] == ["channel_recovered"]
    finally:
        db.close()


# ---------------------------------------------------------------- 5. 状态可见/清理

def test_recovered_event_carries_offline_seconds():
    """watchdog 恢复行带离线持续秒数（首次判定 → 恢复的差值），不再恒 0。"""
    mono = [0.0]
    eng = _Engine(channel="goldminer_bridge",
                  started=NOW - timedelta(seconds=900), tick=NOW)
    eng._bridge_online = False
    rec = _Recorder()
    wd = _watchdog({1: eng}, bridge_timeout_seconds=90,
                   mono_fn=lambda: mono[0], persist_fn=rec)
    wd.scan()
    mono[0] = 100.0
    wd.scan()                          # 首次判定（t=100）
    mono[0] = 400.0
    eng._bridge_online = True
    wd.scan()                          # 恢复（t=400）
    row = rec.rows[1]
    assert row["gate"] == "channel_recovered"
    assert row["seconds"] >= 300
    assert "已持续" in row["message"]


def test_status_alarm_exposes_channel_and_clear_session():
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    wd = _watchdog({1: eng}, client_online_fn=lambda ch: False,
                   persist_fn=_Recorder())
    wd.scan()
    alarms = wd.status()["active_alarms"]
    assert alarms and alarms[0]["channel"] == "tdx_tq"
    wd.clear_session(1)
    assert wd.status()["active_alarms"] == []


def test_stop_session_clears_watchdog_alarm(db_session, monkeypatch):
    """会话 stop（从 ENGINES 弹出）时必须清监控态，否则 active_alarms 永久挂红灯。"""
    from core.engine.live import engine_watchdog as wd_mod
    from core.models import LiveSession
    from core.services import live_service as svc

    session = LiveSession(name="s", mode="simulation", engine="tdx",
                          status="running")
    db_session.add(session)
    db_session.commit()
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng.session_id = session.id
    wd = _watchdog({session.id: eng}, client_online_fn=lambda ch: False,
                   persist_fn=_Recorder())
    wd.scan()
    assert wd.status()["active_alarms"]
    monkeypatch.setattr(wd_mod, "_ACTIVE_WATCHDOG", wd)

    svc.stop_session(db_session, session.id, {session.id: eng})
    assert wd.status()["active_alarms"] == []
