"""L5 升级 / 回滚路径单元测试（默认关闭、只读检查、备份前置）。"""
import logging
from datetime import datetime, timedelta
from pathlib import Path

from core.services import backup_service, upgrade_service


def test_guard_settings_defaults_off(monkeypatch):
    monkeypatch.setattr(upgrade_service, "load_config", lambda: {})
    s = upgrade_service.guard_settings()
    assert s["enabled"] is False
    assert s["require_backup"] is True
    assert s["max_backup_age_hours"] == 24.0


def test_guard_settings_reads_override(monkeypatch):
    monkeypatch.setattr(upgrade_service, "load_config", lambda: {
        "upgrade": {"enabled": True, "require_backup": False,
                    "max_backup_age_hours": 6}})
    s = upgrade_service.guard_settings()
    assert s == {"enabled": True, "require_backup": False,
                 "max_backup_age_hours": 6.0}


def test_startup_check_disabled_is_noop(monkeypatch, caplog):
    monkeypatch.setattr(upgrade_service, "guard_settings",
                        lambda: {"enabled": False, "require_backup": True,
                                 "max_backup_age_hours": 24.0})

    def _boom():
        raise AssertionError("关闭时不应读取迁移状态")

    monkeypatch.setattr(upgrade_service, "migration_status", _boom)
    with caplog.at_level(logging.DEBUG, logger=upgrade_service.logger.name):
        assert upgrade_service.startup_check() == {"enabled": False}
    assert caplog.records == []


def test_migration_status_unavailable_on_config_error(monkeypatch):
    def _boom():
        raise RuntimeError("no ini")

    monkeypatch.setattr(upgrade_service, "_alembic_config", _boom)
    status = upgrade_service.migration_status()
    assert status["available"] is False
    assert "no ini" in status["error"]
    assert status["pending_count"] == 0


def _patch_preflight(monkeypatch, *, enabled, age_hours, require_backup=True):
    now = datetime(2026, 11, 18, 12, 0)
    monkeypatch.setattr(upgrade_service, "guard_settings", lambda: {
        "enabled": enabled, "require_backup": require_backup,
        "max_backup_age_hours": 24.0})
    monkeypatch.setattr(upgrade_service, "migration_status", lambda: {
        "available": True, "error": None, "current": ["rev_a"],
        "heads": ["rev_b"], "pending": [{"revision": "rev_b"}],
        "pending_count": 1, "single_head": True, "at_head": False})
    finished = None
    if age_hours is not None:
        finished = (now - timedelta(hours=age_hours)).isoformat()
    record = {"id": 1, "status": backup_service.STATUS_SUCCESS,
              "filename": "dev-1.db", "started_at": finished,
              "finished_at": finished}
    monkeypatch.setattr(upgrade_service.backup_service, "get_status",
                        lambda **kw: {"backup_dir": "data/backup",
                                      "min_free_mb": 500})
    monkeypatch.setattr(upgrade_service.backup_service, "list_records",
                        lambda *a, **kw: [record] if record else [])
    monkeypatch.setattr(upgrade_service.backup_service, "database_path",
                        lambda: Path(__file__))
    monkeypatch.setattr(upgrade_service.backup_service, "free_disk_mb",
                        lambda p: 1024.0)
    return now


def test_preflight_requires_fresh_backup_when_enabled(monkeypatch):
    now = _patch_preflight(monkeypatch, enabled=True, age_hours=30)
    result = upgrade_service.preflight(now=now)
    assert result["migrations"]["pending_count"] == 1
    assert result["backup"]["fresh"] is False
    assert result["backup"]["required"] is True
    assert result["disk"]["ok"] is True
    assert any("downgrade rev_a" in x for x in result["rollback"])


def test_preflight_fresh_backup_not_required(monkeypatch):
    now = _patch_preflight(monkeypatch, enabled=True, age_hours=2)
    result = upgrade_service.preflight(now=now)
    assert result["backup"]["fresh"] is True
    assert result["backup"]["required"] is False


def test_preflight_disabled_never_requires_backup(monkeypatch):
    now = _patch_preflight(monkeypatch, enabled=False, age_hours=None)
    result = upgrade_service.preflight(now=now)
    assert result["enabled"] is False
    assert result["backup"]["required"] is False


def test_prepare_backup_rejects_when_disabled(monkeypatch):
    calls = []
    monkeypatch.setattr(upgrade_service, "guard_settings",
                        lambda: {"enabled": False, "require_backup": True,
                                 "max_backup_age_hours": 24.0})
    result = upgrade_service.prepare_backup(
        confirm=True, run_backup=lambda *a, **kw: calls.append(1))
    assert result["ok"] is False and result["status"] == "rejected"
    assert calls == []


def test_prepare_backup_requires_confirm(monkeypatch):
    calls = []
    monkeypatch.setattr(upgrade_service, "guard_settings",
                        lambda: {"enabled": True, "require_backup": True,
                                 "max_backup_age_hours": 24.0})
    result = upgrade_service.prepare_backup(
        confirm=False, run_backup=lambda *a, **kw: calls.append(1))
    assert result["ok"] is False
    assert calls == []


def test_prepare_backup_runs_manual_backup(monkeypatch):
    seen = []

    def _fake(kind):
        seen.append(kind)
        return {"status": backup_service.STATUS_SUCCESS, "filename": "x.db"}

    monkeypatch.setattr(upgrade_service, "guard_settings",
                        lambda: {"enabled": True, "require_backup": True,
                                 "max_backup_age_hours": 24.0})
    result = upgrade_service.prepare_backup(confirm=True, run_backup=_fake)
    assert result["ok"] is True
    assert seen == [backup_service.KIND_MANUAL]
    assert result["record"]["filename"] == "x.db"
