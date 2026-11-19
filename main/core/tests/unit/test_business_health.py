"""L6 业务级心跳单元测试（只读判定，不触发重启/交易）。"""
from datetime import datetime, timedelta
from types import SimpleNamespace

from core.services import business_health
from core.supervisor import Supervisor

NOW = datetime(2026, 11, 18, 10, 0)  # 周三盘中


class _Synth:
    def __init__(self, last_completed_at):
        self.last_completed_at = last_completed_at


class _Engine:
    def __init__(self, *, tick=None, bar=None, started=None, running=True,
                 synth=True):
        self._running = running
        self._started_at = started or (NOW - timedelta(hours=1))
        self._last_tick_at = tick
        self._snapshot_synth = _Synth(bar) if synth else None
        self._available_bar = SimpleNamespace(period="1m", bar_time=bar)


def test_fresh_tick_and_bar_is_ok():
    engine = _Engine(tick=NOW - timedelta(seconds=10),
                     bar=NOW - timedelta(seconds=60))
    result = business_health.evaluate({1: engine}, NOW)
    assert result["ok"] is True
    assert result["in_session"] is True
    assert result["sessions"][0]["stale"] is False


def test_stalled_engine_is_not_ok():
    engine = _Engine(tick=NOW - timedelta(seconds=600),
                     bar=NOW - timedelta(seconds=600))
    result = business_health.evaluate({3: engine}, NOW, stall_seconds=180)
    assert result["ok"] is False
    assert result["sessions"][0]["stale"] is True
    assert "会话 3" in result["problems"][0]


def test_off_session_is_ignored():
    engine = _Engine(tick=NOW - timedelta(days=2),
                     bar=NOW - timedelta(days=2))
    weekend = datetime(2026, 11, 22, 10, 0)  # 周日
    result = business_health.evaluate({1: engine}, weekend)
    assert result["in_session"] is False
    assert result["ok"] is True


def test_warming_session_is_not_stale():
    started = NOW - timedelta(seconds=30)
    engine = _Engine(tick=None, bar=None, started=started)
    result = business_health.evaluate(
        {2: engine}, NOW, stall_seconds=180, grace_seconds=120)
    assert result["ok"] is True
    assert result["sessions"][0]["warming"] is True


def test_long_period_bar_ignored():
    engine = _Engine(tick=NOW - timedelta(seconds=5), bar=None, synth=False)
    engine._available_bar = SimpleNamespace(
        period="1d", bar_time=NOW - timedelta(hours=8))
    result = business_health.evaluate({1: engine}, NOW)
    assert result["ok"] is True
    assert result["sessions"][0]["last_bar_at"] is None


def test_snapshot_disabled_skips_engines(monkeypatch):
    monkeypatch.setattr(business_health, "probe_settings",
                        lambda: {"enabled": False, "stall_seconds": 180,
                                 "grace_seconds": 120})
    called = []
    result = business_health.snapshot(
        now=NOW, engine_provider=lambda: called.append(1) or {})
    assert result == {"enabled": False, "ok": True}
    assert called == []


def test_snapshot_enabled_aggregates(monkeypatch):
    monkeypatch.setattr(business_health, "probe_settings",
                        lambda: {"enabled": True, "stall_seconds": 180,
                                 "grace_seconds": 120})
    monkeypatch.setattr(business_health, "_watchdog_state",
                        lambda now: {"running": True, "scan_count": 5,
                                     "last_scan_at": now.isoformat(),
                                     "active_alarms": 0, "stale": False})
    engine = _Engine(tick=NOW - timedelta(seconds=5),
                     bar=NOW - timedelta(seconds=30))
    result = business_health.snapshot(
        now=NOW, engine_provider=lambda: {1: engine})
    assert result["enabled"] is True
    assert result["ok"] is True
    assert result["watchdog"]["scan_count"] == 5


def test_snapshot_watchdog_stale_marks_not_ok(monkeypatch):
    monkeypatch.setattr(business_health, "probe_settings",
                        lambda: {"enabled": True, "stall_seconds": 180,
                                 "grace_seconds": 120})
    monkeypatch.setattr(business_health, "_watchdog_state",
                        lambda now: {"running": True, "stale": True,
                                     "active_alarms": 0})
    result = business_health.snapshot(now=NOW, engine_provider=lambda: {})
    assert result["ok"] is False
    assert any("watchdog" in p for p in result["problems"])


def test_supervisor_business_gate_default_off(monkeypatch):
    monkeypatch.setattr("core.config.load_config", lambda: {})
    assert Supervisor._business_ok({"business": {"ok": False}}) is True


def test_supervisor_business_gate_enabled(monkeypatch):
    monkeypatch.setattr("core.config.load_config", lambda: {
        "health": {"business_probe_enabled": True}})
    assert Supervisor._business_ok({"business": {"ok": False}}) is False
    assert Supervisor._business_ok({"business": {"ok": True}}) is True
    # 旧子进程/降级：载荷无 business 字段时不误判
    assert Supervisor._business_ok({"status": "ok"}) is True
