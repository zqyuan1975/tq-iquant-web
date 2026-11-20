# -*- coding: utf-8 -*-
"""迁移闸回归：测试进程绝不允许改动生产 dev.db 的 alembic_version。

背景：2026-11-20 12:35，`with TestClient(app)` 的 lifespan 调用 init_db，
把生产 dev.db 从 g4a5b6c7d8e9 静默迁到 h5b6c7d8e9f0。conftest 现已默认
`TQ_INIT_DB_DISABLED=1` 且把库重定向到 %TEMP%，本文件把这两条锁成用例。

注意：本用例刻意读**真实** main/data/dev.db（而不是进程内 TQ_DB_PATH），
否则「测试库重定向」会掩盖真实库是否被动过。
"""
import os
import sqlite3
from pathlib import Path

import pytest

# 仓库内生产库绝对路径：main/core/tests/integration/x.py → parents[3] = main/
REAL_DB = Path(__file__).resolve().parents[3] / "data" / "dev.db"


def _alembic_version(path: Path):
    """只读连接读 alembic_version（生产库正被 8001 以 WAL 打开，不能用写连接）。"""
    conn = sqlite3.connect("file:%s?mode=ro" % path.as_posix(), uri=True)
    try:
        row = conn.execute("select version_num from alembic_version").fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def test_pytest_process_uses_gate_and_isolated_db():
    """conftest 必须同时开闸并重定向库；缺一不可。"""
    assert os.environ.get("TQ_INIT_DB_DISABLED") == "1"
    assert os.environ.get("TQ_HANDOFF_ARCHIVE_DISABLED") == "1"
    db_path = Path(os.environ["TQ_DB_PATH"]).resolve()
    assert db_path != REAL_DB.resolve()


def test_init_db_returns_without_upgrade(monkeypatch):
    """TQ_INIT_DB_DISABLED=1 时 init_db 直接返回，不触碰 alembic。"""
    calls = []

    def _fake_upgrade(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr("alembic.command.upgrade", _fake_upgrade)

    from core.db import init_db

    init_db()
    assert calls == [], "TQ_INIT_DB_DISABLED=1 下不应执行 alembic upgrade"


def test_lifespan_does_not_change_real_dev_db_version(monkeypatch):
    """跑一次带 lifespan 的集成用例，真实 dev.db 的 alembic_version 必须不变。"""
    if not REAL_DB.exists():
        pytest.skip("本机无生产库，跳过真实库隔离校验")
    before = _alembic_version(REAL_DB)

    def _boom(*args, **kwargs):
        raise AssertionError("测试进程触发了 alembic upgrade——迁移闸失效")

    monkeypatch.setattr("alembic.command.upgrade", _boom)

    from fastapi.testclient import TestClient
    from core.main import app

    with TestClient(app, raise_server_exceptions=False):
        pass

    assert _alembic_version(REAL_DB) == before
