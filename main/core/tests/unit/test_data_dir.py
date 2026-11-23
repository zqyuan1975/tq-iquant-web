"""运行期数据目录统一（core.runtime.data_dir / data_path / 迁移）。

锁定：
- 源码模式 runtime 目录与 DB 同锚（main/data），不再落仓库根 data/；
- 打包 / TQ_APP_ROOT 模式锚 app_root()，行为不变；
- 体检游标走统一入口；
- 一次性迁移：旧文件复制到新位置、原文件留 .bak、同目录/禁用开关 no-op。
"""
from __future__ import annotations

from pathlib import Path

import pytest

from core import config, runtime
from core.services import live_data_health


@pytest.fixture
def src_mode(monkeypatch):
    """纯源码模式：无 TQ_APP_ROOT、非打包、清空 app_root 缓存。"""
    monkeypatch.delenv("TQ_APP_ROOT", raising=False)
    monkeypatch.setattr(config, "_APP_ROOT", None)
    yield
    # monkeypatch 会在 teardown 恢复 _APP_ROOT / 环境变量


@pytest.fixture
def app_root_mode(tmp_path, monkeypatch):
    """TQ_APP_ROOT 模式（等价打包锚定）。"""
    monkeypatch.setenv("TQ_APP_ROOT", str(tmp_path))
    monkeypatch.setattr(config, "_APP_ROOT", None)
    yield tmp_path


def test_source_mode_data_dir_is_main_data(src_mode):
    from core import db as core_db

    assert runtime.data_dir() == Path(runtime.__file__).resolve().parent.parent / "data"
    # 与 DB 锚点一致：main/data/dev.db 的父目录
    assert runtime._MAIN_DIR == core_db._MAIN_DIR == runtime.data_dir().parent


def test_data_dir_follows_app_root(app_root_mode):
    base = app_root_mode
    assert runtime.data_dir() == base / "data"
    assert runtime.runtime_dir() == base / "data" / "runtime"
    assert runtime.data_path("health_sample_cursor.json") == (
        base / "data" / "health_sample_cursor.json")
    assert runtime.data_path("data/kline_cache") == base / "data" / "kline_cache"


def test_data_path_absolute_unchanged():
    p = Path("C:/tmp/abs.json")
    assert runtime.data_path(p) == p


def test_cursor_path_uses_unified_data_dir(app_root_mode, monkeypatch):
    monkeypatch.delenv("TQ_HEALTH_CURSOR_PATH", raising=False)
    assert live_data_health._cursor_path() == (
        app_root_mode / "data" / "health_sample_cursor.json")


def test_migrate_copies_and_backs_up(tmp_path, monkeypatch):
    monkeypatch.delenv("TQ_DATA_DIR_MIGRATION_DISABLED", raising=False)
    old, new = tmp_path / "old", tmp_path / "new"
    old.mkdir()
    (old / "health_sample_cursor.json").write_text('{"k": 1}', encoding="utf-8")

    moved = runtime.migrate_legacy_data_files(old, new)
    assert len(moved) == 1 and moved[0]["action"] == "copied"
    assert (new / "health_sample_cursor.json").read_text(encoding="utf-8") == '{"k": 1}'
    assert not (old / "health_sample_cursor.json").exists()
    baks = list(old.glob("health_sample_cursor.json.pre-migrate-*.bak"))
    assert len(baks) == 1
    assert baks[0].read_text(encoding="utf-8") == '{"k": 1}'


def test_migrate_keeps_new_when_both_exist(tmp_path, monkeypatch):
    monkeypatch.delenv("TQ_DATA_DIR_MIGRATION_DISABLED", raising=False)
    old, new = tmp_path / "old", tmp_path / "new"
    old.mkdir()
    new.mkdir()
    (old / "health_sample_cursor.json").write_text('{"old": 1}', encoding="utf-8")
    (new / "health_sample_cursor.json").write_text('{"new": 1}', encoding="utf-8")

    moved = runtime.migrate_legacy_data_files(old, new)
    assert moved[0]["action"] == "kept_new"
    assert (new / "health_sample_cursor.json").read_text(encoding="utf-8") == '{"new": 1}'
    assert not (old / "health_sample_cursor.json").exists()  # 旧文件仍被留档


def test_migrate_noop_when_same_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("TQ_DATA_DIR_MIGRATION_DISABLED", raising=False)
    d = tmp_path / "same"
    d.mkdir()
    (d / "health_sample_cursor.json").write_text("{}", encoding="utf-8")
    assert runtime.migrate_legacy_data_files(d, d) == []
    assert (d / "health_sample_cursor.json").exists()


def test_migrate_disabled_by_env(tmp_path, monkeypatch):
    monkeypatch.setenv("TQ_DATA_DIR_MIGRATION_DISABLED", "1")
    old, new = tmp_path / "old", tmp_path / "new"
    old.mkdir()
    (old / "health_sample_cursor.json").write_text("{}", encoding="utf-8")
    assert runtime.migrate_legacy_data_files(old, new) == []
    assert (old / "health_sample_cursor.json").exists()
    assert not new.exists()
