"""进程内引擎存活监控（EngineWatchdog）单测。

覆盖：启动/停止、空注册表、引擎卡死告警去重与恢复、启动宽限、盘外不误报、
ths 文件交接层积压与恢复、桥不可达、tdx bar 停滞、配置非法回退默认。
"""
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

from core.engine.live.engine_watchdog import (
    EngineWatchdog, config_seconds, current_watchdog,
)

NOW = datetime(2026, 11, 5, 10, 30, 0)


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


def _watchdog(engines, **kw):
    kw.setdefault("now_fn", lambda: NOW)
    kw.setdefault("mono_fn", lambda: 0.0)
    kw.setdefault("snapshot_fn", lambda sid: None)
    # 默认注入「DB running」：单测不读真实 dev.db，也避免错配自检干扰既有断言
    kw.setdefault("session_status_fn", lambda sid: "running")
    # 默认无 DB 启动时刻（错配自愈的「同代」判据恒不满足 → 只告警不杀）
    kw.setdefault("session_started_at_fn", lambda sid: None)
    kw.setdefault("in_session_fn", lambda dt: True)
    # 客户端窗口探针注入固定「在线」：单测不依赖本机是否真的开着通达信
    kw.setdefault("client_online_fn", lambda ch: True)
    # 默认关掉引擎层阈值，避免干扰通道探针断言
    kw.setdefault("timeout_seconds", 10 ** 9)
    kw.setdefault("execution_stall_seconds", 180)
    kw.setdefault("bridge_timeout_seconds", 90)
    kw.setdefault("market_stall_seconds", 180)
    kw.setdefault("grace_seconds", 120)
    return EngineWatchdog(lambda: engines, **kw)


def _risk(eng, rule):
    return [e for e in eng.events if e["type"] == "risk" and e.get("rule") == rule]


# ---------------- 生命周期 / 空注册表 ----------------

def test_start_stop_no_exception():
    wd = _watchdog({})
    wd.start()
    wd.stop()


def test_empty_registry_scan_is_safe():
    wd = _watchdog({})
    wd.scan()  # 不抛异常
    assert wd._states == {}


def test_engine_not_running_is_not_probed():
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900),
                  tick=NOW - timedelta(seconds=900), running=False)
    eng._bridge_online = False
    wd = _watchdog({1: eng}, bridge_timeout_seconds=1)
    wd.scan()
    assert eng.events == []


# ---------------- 引擎层心跳 ----------------

def test_stale_engine_alerts_once_then_recovers():
    eng = _Engine(channel="unknown",
                  started=NOW - timedelta(seconds=900),
                  tick=NOW - timedelta(seconds=200))
    wd = _watchdog({1: eng}, timeout_seconds=90)
    wd.scan()
    alerts = _risk(eng, "engine_stale")
    assert len(alerts) == 1
    assert alerts[0]["seconds"] >= 200
    assert "已静默" in alerts[0]["message"]

    wd.scan()  # 同一轮卡死不重复告警
    assert len(_risk(eng, "engine_stale")) == 1

    eng._last_tick_at = NOW  # 恢复
    wd.scan()
    recovered = [e for e in eng.events if e["type"] == "engine_recovered"]
    assert len(recovered) == 1
    assert _risk(eng, "engine_stale") == [alerts[0]]  # 无新增告警


def test_startup_grace_silences_alerts():
    eng = _Engine(channel="unknown", started=NOW - timedelta(seconds=30), tick=None)
    wd = _watchdog({1: eng}, timeout_seconds=5, grace_seconds=120)
    wd.scan()
    assert eng.events == []


def test_out_of_session_does_not_alert_engine_stale():
    eng = _Engine(channel="unknown",
                  started=NOW - timedelta(seconds=900),
                  tick=NOW - timedelta(seconds=300))
    wd = _watchdog({1: eng}, timeout_seconds=90,
                   in_session_fn=lambda dt: False)
    wd.scan()
    assert eng.events == []


# ---------------- 通道探针：ths ----------------

def _write_handoff(tmp_path, lines, offset=0):
    orders = tmp_path / "orders.jsonl"
    orders.write_text("".join(json.dumps(r) + "\n" for r in lines), encoding="utf-8")
    cursor = tmp_path / "cursor.json"
    cursor.write_text(json.dumps({"orders_offset": offset}), encoding="utf-8")
    return SimpleNamespace(orders_path=str(orders), cursor_path=str(cursor))


def test_ths_backlog_stall_alerts_and_recovers(tmp_path):
    eng = _Engine(channel="ths_ui", started=NOW - timedelta(seconds=900), tick=NOW)
    old = (NOW - timedelta(seconds=600)).isoformat(timespec="seconds")
    handoff = _write_handoff(tmp_path, [
        {"order_id": "a", "ts": old},
        {"order_id": "b", "ts": old},
    ], offset=0)
    eng._file_handoff = handoff
    wd = _watchdog({1: eng})
    wd.scan()
    alerts = _risk(eng, "execution_stalled")
    assert len(alerts) == 1
    assert "积压 2 行" in alerts[0]["message"]
    assert alerts[0]["backlog"] == 2

    # cursor 推进到 2 → 不再积压 → 恢复事件
    Path(handoff.cursor_path).write_text(
        json.dumps({"orders_offset": 2}), encoding="utf-8")
    wd.scan()
    recovered = [e for e in eng.events if e["type"] == "execution_recovered"]
    assert len(recovered) == 1


def test_ths_backlog_alerts_out_of_session(tmp_path):
    """ths 积压不受交易时段限制：盘外仍告警。"""
    eng = _Engine(channel="ths_ui", started=NOW - timedelta(seconds=900), tick=NOW)
    old = (NOW - timedelta(seconds=600)).isoformat(timespec="seconds")
    eng._file_handoff = _write_handoff(tmp_path, [{"order_id": "a", "ts": old}])
    wd = _watchdog({1: eng}, in_session_fn=lambda dt: False)
    wd.scan()
    assert len(_risk(eng, "execution_stalled")) == 1


# ---------------- 通道探针：桥 ----------------

def test_bridge_unreachable_alerts_after_timeout():
    mono = [0.0]
    eng = _Engine(channel="goldminer_bridge", started=NOW - timedelta(seconds=900))
    eng._last_tick_at = NOW
    eng._bridge_online = False
    wd = _watchdog({1: eng}, bridge_timeout_seconds=90,
                   mono_fn=lambda: mono[0])
    wd.scan()                     # 起点，wait=0
    assert eng.events == []
    mono[0] = 100.0
    wd.scan()                     # wait=100 > 90
    alerts = _risk(eng, "bridge_unreachable")
    assert len(alerts) == 1
    assert alerts[0]["engine"] == "goldminer_bridge"


def test_tdx_bar_stall_alerts():
    """1m 快照合成真停 200s → 告警（基准为最近已闭合 1m bar）。"""
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng._snapshot_synth = SimpleNamespace(
        last_completed_at=NOW - timedelta(seconds=200))
    wd = _watchdog({1: eng}, market_stall_seconds=180)
    wd.scan()
    alerts = _risk(eng, "market_data_stalled")
    assert len(alerts) == 1
    assert "未推进" in alerts[0]["message"]
    assert alerts[0]["basis"] == "snapshot_1m"


def test_tdx_daily_strategy_with_fresh_1m_snapshot_no_alert():
    """日线策略的 00:00:00 不参与判定：1m 采样新鲜 → 不报（2026-11-05 假警回归）。"""
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng._available_bar = SimpleNamespace(
        period="1d", bar_time=NOW.replace(hour=0, minute=0, second=0))
    eng._snapshot_synth = SimpleNamespace(
        last_completed_at=NOW - timedelta(seconds=10))
    wd = _watchdog({1: eng}, market_stall_seconds=180)
    wd.scan()
    assert eng.events == []


def test_tdx_daily_bar_time_never_used_as_basis():
    """无合成器时，日线 bar_time=00:00:00 仍不得成为停滞判定基准。"""
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng._available_bar = SimpleNamespace(
        period="1d", bar_time=NOW.replace(hour=0, minute=0, second=0))
    wd = _watchdog({1: eng}, market_stall_seconds=180)
    wd.scan()
    assert eng.events == []


def test_tdx_minute_bar_stall_still_alerts_without_synth():
    """无合成器（非 tdx 盘官方路径）：分钟级 bar 停滞照旧告警。"""
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng._available_bar = SimpleNamespace(
        period="1m", bar_time=NOW - timedelta(seconds=600))
    wd = _watchdog({1: eng}, market_stall_seconds=180)
    wd.scan()
    assert len(_risk(eng, "market_data_stalled")) == 1


def test_tdx_bar_stall_not_reported_out_of_session():
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng._snapshot_synth = SimpleNamespace(
        last_completed_at=NOW - timedelta(seconds=600))
    wd = _watchdog({1: eng}, in_session_fn=lambda dt: False)
    wd.scan()
    assert eng.events == []


def test_lunch_break_gap_not_counted_as_stall():
    """13:00 开盘首分钟：午休 11:30–13:00 不算停滞（2026-11-05 13:00 假警回归）。"""
    lunch_now = datetime(2026, 11, 5, 13, 0, 2)
    eng = _Engine(channel="tdx_tq",
                  started=lunch_now - timedelta(hours=8), tick=lunch_now)
    eng._snapshot_synth = SimpleNamespace(
        last_completed_at=datetime(2026, 11, 5, 11, 41, 0))
    wd = _watchdog({1: eng}, now_fn=lambda: lunch_now)
    wd.scan()
    assert eng.events == []


def test_overnight_gap_not_counted_as_stall():
    """09:30 开盘首分钟：隔夜静默从当日开盘起算，不报。"""
    morning = datetime(2026, 11, 5, 9, 31, 30)
    eng = _Engine(channel="tdx_tq",
                  started=morning - timedelta(hours=1), tick=morning)
    eng._snapshot_synth = SimpleNamespace(
        last_completed_at=datetime(2026, 11, 4, 14, 59, 0))
    wd = _watchdog({1: eng}, now_fn=lambda: morning)
    wd.scan()
    assert eng.events == []


def test_goldminer_snapshot_stall_alerts_even_if_bridge_online():
    """掘金行情走 TQ 快照合成：桥心跳正常但快照停摆也必须告警（P1 2026-11-09）。"""
    eng = _Engine(channel="goldminer_bridge",
                  started=NOW - timedelta(seconds=900), tick=NOW)
    eng._snapshot_synth = SimpleNamespace(
        last_completed_at=NOW - timedelta(seconds=200))
    wd = _watchdog({1: eng}, market_stall_seconds=180)
    wd.scan()
    alerts = _risk(eng, "market_data_stalled")
    assert len(alerts) == 1
    assert alerts[0]["basis"] == "snapshot_1m"


def test_same_day_start_basis_clamped_to_open_no_0930_false_alarm():
    """同日起步（09:20 启动）：09:30:30 尚无闭合 1m bar 不得报假警；09:35 真停滞才报。"""
    started = datetime(2026, 11, 5, 9, 20)
    early = datetime(2026, 11, 5, 9, 30, 30)
    eng1 = _Engine(channel="tdx_tq", started=started, tick=early)
    eng1._snapshot_synth = SimpleNamespace(last_completed_at=None)
    wd1 = _watchdog({1: eng1}, now_fn=lambda: early, market_stall_seconds=180)
    wd1.scan()
    assert _risk(eng1, "market_data_stalled") == []

    late = datetime(2026, 11, 5, 9, 35, 0)
    eng2 = _Engine(channel="tdx_tq", started=started, tick=late)
    eng2._snapshot_synth = SimpleNamespace(last_completed_at=None)
    wd2 = _watchdog({2: eng2}, now_fn=lambda: late, market_stall_seconds=180)
    wd2.scan()
    assert len(_risk(eng2, "market_data_stalled")) == 1


def test_market_data_recovery_clears_active_alarms():
    """误报解除后 active_alarms 必须清空（面板不挂红标）。"""
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=900), tick=NOW)
    eng._snapshot_synth = SimpleNamespace(
        last_completed_at=NOW - timedelta(seconds=600))
    wd = _watchdog({1: eng}, market_stall_seconds=180)
    wd.scan()
    assert wd.status()["active_alarms"] == [
        {"session_id": 1, "rule": "market_data_stalled", "channel": "tdx_tq"}]

    eng._snapshot_synth.last_completed_at = NOW
    wd.scan()
    assert wd.status()["active_alarms"] == []
    assert any(e["type"] == "market_data_recovered" for e in eng.events)


# ---------------- 自身可观测性 ----------------

def test_scan_advances_status_counters(caplog):
    now = [NOW]
    eng = _Engine(channel="unknown",
                  started=NOW - timedelta(seconds=900), tick=NOW)
    wd = _watchdog({1: eng}, now_fn=lambda: now[0])
    assert wd.status()["scan_count"] == 0

    wd.scan()
    st = wd.status()
    assert st["scan_count"] == 1
    assert st["last_scan_at"] == NOW.isoformat()
    assert st["running"] is False  # 未 start()，线程不在跑

    now[0] = NOW + timedelta(seconds=30)
    with caplog.at_level(logging.DEBUG):
        wd.scan()
    st2 = wd.status()
    assert st2["scan_count"] == 2
    assert st2["last_scan_at"] != st["last_scan_at"]
    assert any("engine watchdog scan: sessions=1" in r.getMessage()
               for r in caplog.records)


def test_current_watchdog_cleared_after_stop():
    wd = _watchdog({})
    wd.start()
    assert current_watchdog() is wd
    assert wd.status()["running"] is True
    wd.stop()
    assert current_watchdog() is None
    assert wd.status()["running"] is False


def test_watchdog_status_endpoint_without_watchdog():
    """watchdog 未启动时接口返回 running=false，不抛 500。"""
    from core.api.live import watchdog_status
    assert current_watchdog() is None
    resp = watchdog_status()
    assert resp["code"] == 0
    assert resp["data"]["running"] is False
    assert resp["data"]["scan_count"] == 0
    assert resp["data"]["active_alarms"] == []


def test_config_seconds_invalid_falls_back_to_default():
    assert config_seconds(
        "heartbeat_timeout_seconds", 90,
        {"live": {"heartbeat_timeout_seconds": "abc"}}) == 90
    assert config_seconds(
        "heartbeat_timeout_seconds", 90,
        {"live": {"heartbeat_timeout_seconds": -5}}) == 90
    assert config_seconds(
        "heartbeat_timeout_seconds", 90,
        {"live": {"heartbeat_timeout_seconds": 45}}) == 45


# ---------------- 进程/会话状态错配（DB stopped，本进程引擎仍在跑） ----------------

def _running_engine():
    """引擎处于运行态且行情新鲜，避免其它探针产生告警干扰断言。"""
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=3600), tick=NOW)
    eng._available_bar = SimpleNamespace(bar_time=NOW, period="1m")
    return eng


def _orphan_engine():
    """引擎仍在注册表但快照/心跳均已静默（自愈证据充分的那种孤儿）。"""
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=3600),
                  tick=NOW - timedelta(seconds=900))
    eng._available_bar = SimpleNamespace(
        bar_time=NOW - timedelta(seconds=900), period="1m")
    return eng


def test_owner_mismatch_alerts_once_and_persists():
    eng = _running_engine()
    persisted = []
    wd = _watchdog({1: eng}, session_status_fn=lambda sid: "stopped",
                   persist_fn=lambda **kw: persisted.append(kw))
    wd.scan()

    alerts = _risk(eng, "engine_owner_mismatch")
    assert len(alerts) == 1
    assert alerts[0]["db_status"] == "stopped"
    assert "进程/会话状态错配" in alerts[0]["message"]
    assert "pid=" in alerts[0]["message"]
    assert len(persisted) == 1
    assert persisted[0]["gate"] == "engine_owner_mismatch"
    assert persisted[0]["action"] == "trigger"
    assert wd.status()["owner_mismatch"] == [
        {"session_id": 1, "db_status": "stopped"}]

    wd.scan()  # 重复周期内不重复 ERROR/落库
    assert len(_risk(eng, "engine_owner_mismatch")) == 1
    assert len(persisted) == 1


def test_owner_mismatch_stopping_status_also_detected():
    eng = _running_engine()
    wd = _watchdog({1: eng}, session_status_fn=lambda sid: "stopping")
    wd.scan()
    assert len(_risk(eng, "engine_owner_mismatch")) == 1


def test_owner_mismatch_recovers_when_db_running_again():
    eng = _running_engine()
    status = {"value": "stopped"}
    persisted = []
    wd = _watchdog({1: eng}, session_status_fn=lambda sid: status["value"],
                   persist_fn=lambda **kw: persisted.append(kw))
    wd.scan()
    assert len(_risk(eng, "engine_owner_mismatch")) == 1

    status["value"] = "running"
    wd.scan()
    recovered = [e for e in eng.events
                 if e["type"] == "engine_owner_recovered"]
    assert len(recovered) == 1
    assert persisted[-1]["gate"] == "engine_owner_recovered"
    assert wd.status()["owner_mismatch"] == []


def test_owner_mismatch_silenced_by_startup_grace():
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=30), tick=NOW)
    eng._available_bar = SimpleNamespace(bar_time=NOW, period="1m")
    wd = _watchdog({1: eng}, session_status_fn=lambda sid: "stopped",
                   grace_seconds=120)
    wd.scan()
    assert eng.events == []


def test_owner_mismatch_not_checked_when_engine_stopped():
    eng = _Engine(channel="tdx_tq", started=NOW - timedelta(seconds=3600),
                  tick=NOW, running=False)
    wd = _watchdog({1: eng}, session_status_fn=lambda sid: "stopped")
    wd.scan()
    assert eng.events == []


def test_owner_mismatch_status_probe_failure_is_ignored():
    eng = _running_engine()

    def boom(sid):
        raise RuntimeError("db down")

    wd = _watchdog({1: eng}, session_status_fn=boom)
    wd.scan()
    assert _risk(eng, "engine_owner_mismatch") == []


def test_process_tag_contains_pid():
    from core.runtime import process_tag
    tag = process_tag()
    assert tag.startswith("pid=")
    assert "port=" in tag


# ---------------- 错配自愈（仅在注入 unregister_fn 的生产路径启用） ----------------

def test_owner_mismatch_self_heal_stops_and_unregisters():
    engines = {}
    eng = _orphan_engine()
    eng._stop_requested = False
    engines[1] = eng
    unregistered = []

    def unregister(sid):
        unregistered.append(sid)
        engines.pop(sid, None)

    wd = _watchdog(engines, session_status_fn=lambda sid: "stopped",
                   session_started_at_fn=lambda sid: eng._started_at,
                   timeout_seconds=90,
                   unregister_fn=unregister)
    wd.scan()

    assert eng._running is False          # 循环下一拍退出
    assert eng._stop_requested is True    # 拒单闸同步关闭
    assert unregistered == [1]
    assert 1 not in engines
    assert len(_risk(eng, "engine_owner_mismatch")) == 1

    wd.scan()  # 已移出注册表：不重复告警、不残留错配态
    assert len(_risk(eng, "engine_owner_mismatch")) == 1
    assert wd.status()["owner_mismatch"] == []


def test_owner_mismatch_no_self_heal_without_unregister():
    """默认（未注入 unregister_fn）只告警，不动引擎——保持既有语义/单测安全。"""
    eng = _running_engine()
    wd = _watchdog({1: eng}, session_status_fn=lambda sid: "stopped")
    wd.scan()
    assert eng._running is True
    assert len(_risk(eng, "engine_owner_mismatch")) == 1


def test_owner_mismatch_fresh_activity_skips_self_heal():
    """DB 已停 + 引擎健在 + 快照新鲜 → 证据不足，只告警不自愈（2026-11-19 错杀回归）。

    事故形态：DB 被写成 stopped，引擎仍在每 5s 落快照。此时看门狗若照旧按
    DB 一行 stopped 就杀引擎，等于盘中掐断健在的交易链路。
    """
    engines = {}
    eng = _running_engine()          # 心跳/行情新鲜
    engines[1] = eng
    unregistered = []
    persisted = []

    def unregister(sid):
        unregistered.append(sid)
        engines.pop(sid, None)

    wd = _watchdog(engines, session_status_fn=lambda sid: "stopped",
                   session_started_at_fn=lambda sid: eng._started_at,
                   snapshot_fn=lambda sid: NOW - timedelta(seconds=5),
                   timeout_seconds=90,
                   unregister_fn=unregister,
                   persist_fn=lambda **kw: persisted.append(kw))
    wd.scan()

    assert eng._running is True            # 不杀健在引擎
    assert unregistered == []
    assert 1 in engines
    alerts = _risk(eng, "engine_owner_mismatch")
    assert len(alerts) == 1
    assert "证据不足，跳过自愈" in alerts[0]["message"]
    assert alerts[0]["self_heal"] is False
    # 告警仍落库（让用户看到错配），只是不执行自愈
    assert len(persisted) == 1
    assert persisted[0]["gate"] == "engine_owner_mismatch"

    wd.scan()  # 继续保持新鲜 → 仍不杀
    assert eng._running is True
    assert unregistered == []


def test_owner_mismatch_different_generation_skips_self_heal():
    """引擎与 DB started_at 不同代（DB 是上一次启动/另一代）→ 证据不足不杀。"""
    engines = {}
    eng = _orphan_engine()
    engines[1] = eng
    unregistered = []
    wd = _watchdog(engines, session_status_fn=lambda sid: "stopped",
                   session_started_at_fn=lambda sid: NOW - timedelta(seconds=300),
                   timeout_seconds=90,
                   unregister_fn=lambda sid: unregistered.append(sid))
    wd.scan()
    assert eng._running is True
    assert unregistered == []
    assert "不同代" in _risk(eng, "engine_owner_mismatch")[0]["message"]


def test_owner_mismatch_self_heal_unregister_failure_is_tolerated():
    eng = _orphan_engine()

    def boom(sid):
        raise RuntimeError("registry busy")

    wd = _watchdog({1: eng}, session_status_fn=lambda sid: "stopped",
                   session_started_at_fn=lambda sid: eng._started_at,
                   timeout_seconds=90,
                   unregister_fn=boom)
    wd.scan()  # 不抛异常；标志已置，告警仍留痕
    assert eng._running is False
    assert len(_risk(eng, "engine_owner_mismatch")) == 1


def test_owner_mismatch_stale_snapshot_after_registry_pop_is_ignored():
    """stop_session 先 pop 注册表再 commit：扫描快照取到旧引擎不应判错配。"""
    eng = _running_engine()
    calls = {"n": 0}
    live = {1: eng}

    def provider():
        calls["n"] += 1
        return dict(live) if calls["n"] == 1 else {}

    wd = EngineWatchdog(
        provider, now_fn=lambda: NOW, mono_fn=lambda: 0.0,
        snapshot_fn=lambda sid: None, session_status_fn=lambda sid: "stopping",
        in_session_fn=lambda dt: True, client_online_fn=lambda ch: True,
        timeout_seconds=10 ** 9, grace_seconds=120)
    wd.scan()
    assert _risk(eng, "engine_owner_mismatch") == []
    assert eng._running is True
