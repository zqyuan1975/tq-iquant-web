"""外部信号注入 — live_sessions.signal_file_path 列迁移 up-down 往返测试。

真跑 Alembic 迁移（tmp SQLite，monkeypatch core.config.load_config 指临时库——
env.py 每次迁移都重新执行 from-import 读取），upgrade head 后 live_sessions 含
signal_file_path 列；downgrade 前一 head（a8f9b0c1d2e3）→ 列移除。
"""
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect

_MAIN_DIR = Path(__file__).resolve().parents[3]

SIGNAL_FILE_COLUMN = {"signal_file_path"}
# 14:50 提前收盘价（功能二）：会话级开关 + 时点 + 幂等日期列
ADVANCE_CLOSE_COLUMNS = {
    "advance_close_enabled", "advance_close_time", "advance_close_done_date",
}
_PREV_HEAD = "a8f9b0c1d2e3"
_ADVANCE_PREV_HEAD = "e9f0a1b2c3d4"


@pytest.fixture
def alembic_cfg(tmp_path, monkeypatch):
    """指向 tmp SQLite 库的 Alembic Config（同 test_strategy_execution_migration 范式）。"""
    monkeypatch.setattr("logging.config.fileConfig", lambda *a, **kw: None)
    db_file = tmp_path / "migration.db"
    monkeypatch.setattr(
        "core.config.load_config",
        lambda: {"database": {"sqlite_path": str(db_file)}},
    )
    return Config(str(_MAIN_DIR / "alembic.ini"))


def _session_columns(cfg):
    from core.config import load_config
    sqlite_path = load_config()["database"]["sqlite_path"]
    engine = create_engine("sqlite:///%s" % sqlite_path)
    try:
        return {c["name"] for c in inspect(engine).get_columns("live_sessions")}
    finally:
        engine.dispose()


def test_upgrade_head_adds_signal_file_path_column(alembic_cfg):
    command.upgrade(alembic_cfg, "head")
    assert SIGNAL_FILE_COLUMN <= _session_columns(alembic_cfg)


def test_downgrade_prev_head_removes_signal_file_path_column(alembic_cfg):
    command.upgrade(alembic_cfg, "head")
    command.downgrade(alembic_cfg, _PREV_HEAD)
    assert not (SIGNAL_FILE_COLUMN & _session_columns(alembic_cfg))


def test_upgrade_head_adds_advance_close_columns(alembic_cfg):
    """f1a2b3c4d5e6：upgrade head 后 live_sessions 含提前收盘价三列。"""
    command.upgrade(alembic_cfg, "head")
    assert ADVANCE_CLOSE_COLUMNS <= _session_columns(alembic_cfg)


def test_downgrade_before_advance_close_removes_columns(alembic_cfg):
    """降级到 e9f0a1b2c3d4：提前收盘价三列移除。"""
    command.upgrade(alembic_cfg, "head")
    command.downgrade(alembic_cfg, _ADVANCE_PREV_HEAD)
    assert not (ADVANCE_CLOSE_COLUMNS & _session_columns(alembic_cfg))
