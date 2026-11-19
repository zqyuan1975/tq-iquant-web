"""内嵌数据备份单测（VACUUM INTO 一致性 / WAL 收敛 / 滚动保留 / 磁盘保护 / 配置 / 接口）。

全部在临时目录上跑，不触碰真实 dev.db；记录表用临时 SQLite 文件库承载
（服务每写完一条记录就关连接，内存库会丢数据，故必须用文件库）。
"""
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base, LiveSession
from core.services import backup_service as bs

NOW = datetime(2026, 11, 10, 15, 10, 0)
PLENTY = 10_000.0  # 充足磁盘（MB）


@pytest.fixture
def session_factory(tmp_path):
    engine = create_engine(f"sqlite:///{(tmp_path / 'records.db').as_posix()}")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)


@pytest.fixture
def live_db(tmp_path):
    """带 WAL 与未收敛事务的源库；连接保持打开模拟运行中的实盘进程。"""
    path = tmp_path / "dev.db"
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)")
    conn.execute("CREATE TABLE b (id INTEGER PRIMARY KEY)")
    conn.executemany("INSERT INTO a (v) VALUES (?)", [("x",), ("y",)])
    conn.execute("INSERT INTO b DEFAULT VALUES")
    conn.commit()
    yield path, conn
    conn.close()


def _cfg(**over):
    base = {
        "enabled": True,
        "daily_at": "15:10",
        "keep": 10,
        "min_free_mb": 500,
        "dir": "data/backup",
    }
    base.update(over)
    return {"backup": base}


def _run(kind, db, dest, session_factory, now=NOW, cfg=None, free=PLENTY):
    return bs.run_backup(
        kind,
        db_path=db,
        backup_dir=dest,
        cfg=cfg or _cfg(),
        session_factory=session_factory,
        now_fn=lambda: now,
        free_mb_fn=lambda p: free,
    )


# ---------------------------------------------------------------- 一致性副本
def test_vacuum_into_copy_is_openable_and_matches(tmp_path, session_factory, live_db):
    db, _conn = live_db
    dest_dir = tmp_path / "backup"

    res = _run(bs.KIND_AUTO, db, dest_dir, session_factory)

    assert res["status"] == bs.STATUS_SUCCESS
    dest = Path(res["path"])
    assert dest.exists()
    assert dest.name == "dev-20261110-151000.db"
    with sqlite3.connect(dest) as c:
        names = {
            r[0] for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%'")
        }
        assert names == {"a", "b"}
        assert c.execute("SELECT COUNT(*) FROM a").fetchone()[0] == 2
        assert c.execute("SELECT COUNT(*) FROM b").fetchone()[0] == 1
        assert c.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    # 记录落库（时间/大小/类型/结果）
    rec = bs.latest_record(bs.KIND_AUTO, session_factory)
    assert rec is not None
    assert rec.status == bs.STATUS_SUCCESS
    assert rec.size_bytes == dest.stat().st_size > 0
    assert rec.started_at == NOW


def test_backup_truncates_wal(tmp_path, session_factory, live_db):
    """备份完成后 PRAGMA wal_checkpoint(TRUNCATE) 收敛 -wal 体积。"""
    db, _conn = live_db
    wal = Path(str(db) + "-wal")
    assert wal.exists() and wal.stat().st_size > 0

    _run(bs.KIND_AUTO, db, tmp_path / "backup", session_factory)

    assert wal.stat().st_size == 0


# ---------------------------------------------------------------- 滚动保留
def test_retention_keeps_newest_and_leaves_manual_untouched(
    tmp_path, session_factory, live_db
):
    db, _conn = live_db
    dest_dir = tmp_path / "backup"
    dest_dir.mkdir()
    for i in range(12):  # 12 份旧自动备份（2026-01-01 ~ 2026-01-12）
        d = date(2026, 1, i + 1)
        (dest_dir / f"dev-{d:%Y%m%d}-030000.db").write_bytes(b"old")
    manual = dest_dir / "dev-manual-20260101-030000.db"
    manual.write_bytes(b"manual")

    _run(bs.KIND_AUTO, db, dest_dir, session_factory, cfg=_cfg(keep=10))

    autos = sorted(
        p for p in dest_dir.glob("*.db")
        if (m := bs.AUTO_NAME_RE.match(p.name)) and m.group("stem") == "dev")
    assert len(autos) == 10  # 12 旧 + 1 新 - 删 3 最旧
    assert not (dest_dir / "dev-20260101-030000.db").exists()
    assert not (dest_dir / "dev-20260102-030000.db").exists()
    assert not (dest_dir / "dev-20260103-030000.db").exists()
    assert (dest_dir / "dev-20260104-030000.db").exists()
    assert (dest_dir / "dev-20261110-151000.db").exists()
    # 手动备份不参与自动清理
    assert manual.exists()


def test_keep_from_config_is_honored(tmp_path, session_factory, live_db):
    db, _conn = live_db
    dest_dir = tmp_path / "backup"
    dest_dir.mkdir()
    for i in range(5):
        (dest_dir / f"dev-2026010{i + 1}-030000.db").write_bytes(b"old")

    _run(bs.KIND_AUTO, db, dest_dir, session_factory, cfg=_cfg(keep=2))

    autos = [
        p for p in dest_dir.glob("*.db")
        if (m := bs.AUTO_NAME_RE.match(p.name)) and m.group("stem") == "dev"]
    assert len(autos) == 2
    assert (dest_dir / "dev-20261110-151000.db").exists()


# ---------------------------------------------------------------- 磁盘保护
def test_low_disk_skips_backup_and_records_warning(
    tmp_path, session_factory, live_db
):
    db, _conn = live_db
    dest_dir = tmp_path / "backup"

    res = _run(bs.KIND_AUTO, db, dest_dir, session_factory, free=100.0)

    assert res["status"] == bs.STATUS_SKIPPED
    assert not list(dest_dir.glob("*.db"))
    assert "磁盘" in res["message"]
    rec = bs.latest_record(bs.KIND_AUTO, session_factory)
    assert rec.status == bs.STATUS_SKIPPED


def test_missing_source_db_records_failure(tmp_path, session_factory):
    res = bs.run_backup(
        bs.KIND_MANUAL,
        db_path=tmp_path / "nope.db",
        backup_dir=tmp_path / "backup",
        cfg=_cfg(),
        session_factory=session_factory,
        now_fn=lambda: NOW,
        free_mb_fn=lambda p: PLENTY,
    )
    assert res["status"] == bs.STATUS_FAILED
    assert "不存在" in res["message"]


# ---------------------------------------------------------------- 配置
def test_backup_settings_defaults_and_parsing():
    s = bs.backup_settings({})
    assert s["enabled"] is True
    assert s["daily_at_hm"] == (15, 10)
    assert s["keep"] == 10
    assert s["min_free_mb"] == 500

    s = bs.backup_settings(
        {"backup": {"enabled": False, "daily_at": "09:05", "keep": "3",
                    "min_free_mb": 0}})
    assert s["enabled"] is False
    assert s["daily_at_hm"] == (9, 5)
    assert s["keep"] == 3
    assert s["min_free_mb"] == 0

    # 非法值回退默认（不因配置笔误整日不备份）
    s = bs.backup_settings(
        {"backup": {"daily_at": "25:99", "keep": 0, "min_free_mb": -1}})
    assert s["daily_at_hm"] == (15, 10)
    assert s["keep"] == 10
    assert s["min_free_mb"] == 500


def test_auto_needs_run_once_per_day(session_factory):
    assert bs.auto_needs_run(NOW, session_factory) is True

    bs.save_record(
        session_factory, kind=bs.KIND_AUTO, status=bs.STATUS_SUCCESS,
        started_at=NOW, finished_at=NOW)

    assert bs.auto_needs_run(NOW, session_factory) is False
    # 次日重新到期
    assert bs.auto_needs_run(NOW + timedelta(days=1), session_factory) is True


def test_auto_retries_failed_only_after_cooldown(session_factory):
    bs.save_record(
        session_factory, kind=bs.KIND_AUTO, status=bs.STATUS_FAILED,
        started_at=NOW - timedelta(minutes=5))

    assert bs.auto_needs_run(NOW, session_factory) is False
    assert bs.auto_needs_run(NOW + timedelta(minutes=10), session_factory) is True


def test_scheduler_respects_enabled_time_and_daily_idempotency(session_factory):
    calls = []
    now = {"t": datetime(2026, 11, 10, 15, 9)}

    def _run_fn(kind, **kwargs):
        calls.append(kind)
        # 真实 run_backup 会落记录；这里模拟落库以验证跨调度实例（重启）的当日幂等
        bs.save_record(session_factory, kind=kind, status=bs.STATUS_SUCCESS,
                       started_at=now["t"], finished_at=now["t"])
        return {"status": bs.STATUS_SUCCESS}

    def _sched(cfg=None):
        return bs.BackupScheduler(
            run_fn=_run_fn,
            cfg_provider=lambda: cfg or _cfg(),
            session_factory=session_factory,
            now_fn=lambda: now["t"],
        )

    sched = _sched()
    assert sched.maybe_run() is None  # 15:09 未到点
    assert calls == []

    now["t"] = datetime(2026, 11, 10, 15, 10)
    sched.maybe_run()
    assert calls == ["auto"]

    now["t"] = datetime(2026, 11, 10, 17, 0)
    sched.maybe_run()  # 同实例：进程内已完成，不再重复
    assert calls == ["auto"]

    _sched().maybe_run()  # 新实例（重启）：DB 记录当日已备份，不再重复
    assert calls == ["auto"]

    now["t"] = datetime(2026, 11, 11, 15, 30)
    _sched(cfg=_cfg(enabled=False)).maybe_run()  # 关闭自动备份 → 不触发
    assert calls == ["auto"]


# ---------------------------------------------------------------- 同日自动 + 手动
def test_auto_and_manual_backups_coexist_same_day(
    tmp_path, session_factory, live_db
):
    db, _conn = live_db
    dest_dir = tmp_path / "backup"

    auto = _run(bs.KIND_AUTO, db, dest_dir, session_factory)
    manual = _run(
        bs.KIND_MANUAL, db, dest_dir, session_factory, now=NOW + timedelta(minutes=5))

    assert auto["status"] == manual["status"] == bs.STATUS_SUCCESS
    assert Path(auto["path"]).exists() and Path(manual["path"]).exists()
    assert Path(manual["path"]).name.startswith("dev-manual-")
    assert bs.latest_record(bs.KIND_AUTO, session_factory).id == auto["id"]
    assert bs.latest_record(bs.KIND_MANUAL, session_factory).id == manual["id"]

    status = bs.get_status(
        now_fn=lambda: NOW + timedelta(minutes=5), session_factory=session_factory)
    assert status["auto_done_today"] is True
    assert status["last_auto"]["kind"] == bs.KIND_AUTO
    assert status["last_manual"]["kind"] == bs.KIND_MANUAL


# ---------------------------------------------------------------- HTTP 接口
def test_backup_api_returns_record_and_error(monkeypatch):
    from httpx import ASGITransport, AsyncClient

    from core.api import system as system_api
    from core.main import app

    monkeypatch.setattr(
        system_api.backup_service, "get_status",
        lambda: {"enabled": True, "daily_at": "15:10", "keep": 10,
                 "min_free_mb": 500, "backup_dir": "x", "last_auto": None,
                 "last_manual": None, "auto_done_today": False})
    monkeypatch.setattr(
        system_api.backup_service, "run_backup",
        lambda kind, **kw: {"id": 7, "kind": kind, "status": "success",
                            "filename": "dev-manual-20261110-151000.db",
                            "size_bytes": 1024, "message": None})

    async def _call():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            return await c.get("/api/system/backup"), await c.post("/api/system/backup")

    import asyncio
    get_resp, post_resp = asyncio.run(_call())
    assert get_resp.json()["code"] == 0
    assert get_resp.json()["data"]["daily_at"] == "15:10"
    body = post_resp.json()
    assert body["code"] == 0
    assert body["data"]["filename"] == "dev-manual-20261110-151000.db"
    assert body["data"]["kind"] == "manual"


def test_backup_api_reports_failure(monkeypatch):
    from httpx import ASGITransport, AsyncClient

    from core.api import system as system_api
    from core.main import app

    monkeypatch.setattr(
        system_api.backup_service, "run_backup",
        lambda kind, **kw: {"status": "skipped",
                            "message": "磁盘剩余空间不足，已跳过本次手动备份"})

    async def _call():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            return await c.post("/api/system/backup")

    import asyncio
    body = asyncio.run(_call()).json()
    assert body["code"] == 500
    assert "磁盘" in body["message"]


# ================================================================ 批次21 L2
# 备份校验 / 恢复：默认关闭，任何前置校验失败都不得改动当前库。
def _restore_cfg(**over):
    cfg = _cfg(restore_enabled=True)
    cfg["backup"].update(over)
    return cfg


def _make_db(path, values=("v1",)):
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE a (id INTEGER PRIMARY KEY, v TEXT)")
    conn.executemany("INSERT INTO a (v) VALUES (?)", [(v,) for v in values])
    conn.commit()
    conn.close()
    return path


def _values(path):
    with sqlite3.connect(path) as c:
        return [r[0] for r in c.execute("SELECT v FROM a ORDER BY id")]


@pytest.fixture
def target_db(tmp_path):
    return _make_db(tmp_path / "live.db", ("current",))


def test_verify_backup_ok_and_missing_and_garbage(tmp_path):
    good = _make_db(tmp_path / "good.db", ("x", "y"))

    res = bs.verify_backup(good)
    assert res["ok"] is True
    assert res["integrity"] == "ok"
    assert res["table_count"] == 1 and res["tables"] == ["a"]
    assert res["row_counts"] == {"a": 2}
    assert res["size_bytes"] > 0

    missing = bs.verify_backup(tmp_path / "nope.db")
    assert missing["ok"] is False and "不存在" in missing["message"]

    garbage = tmp_path / "bad.db"
    garbage.write_bytes(b"this is not a sqlite file" * 100)
    bad = bs.verify_backup(garbage)
    assert bad["ok"] is False and bad["message"]


def test_restore_requires_confirm_and_switch(tmp_path, session_factory, target_db):
    src = _make_db(tmp_path / "snapshot.db", ("old1", "old2"))

    no_confirm = bs.restore_backup(
        path=src, confirm=False, cfg=_restore_cfg(), db_path=target_db,
        session_factory=session_factory, now_fn=lambda: NOW)
    assert no_confirm["ok"] is False and "confirm" in no_confirm["message"]

    switch_off = bs.restore_backup(
        path=src, confirm=True, cfg=_cfg(), db_path=target_db,
        session_factory=session_factory, now_fn=lambda: NOW)
    assert switch_off["ok"] is False
    assert "restore_enabled" in switch_off["message"]
    assert _values(target_db) == ["current"]  # 当前库未被改动


def test_restore_replaces_target_and_keeps_safety_copy(
        tmp_path, session_factory, target_db):
    src = _make_db(tmp_path / "snapshot.db", ("old1", "old2"))
    backup_dir = tmp_path / "backup"

    res = bs.restore_backup(
        path=src, confirm=True, cfg=_restore_cfg(), db_path=target_db,
        backup_dir=backup_dir, session_factory=session_factory,
        now_fn=lambda: NOW)

    assert res["ok"] is True and res["matched"] is True
    assert res["restored_from"] == str(src)
    assert res["safety_backup"]["status"] == bs.STATUS_SUCCESS
    assert _values(target_db) == ["old1", "old2"]  # 已换成备份内容
    safety = Path(res["safety_backup"]["path"])
    assert safety.exists()
    assert _values(safety) == ["current"]  # 恢复前副本保存旧数据
    assert not list(tmp_path.glob(".live.db.restore-*.tmp"))


def test_restore_refuses_running_live_session(
        tmp_path, session_factory, target_db):
    src = _make_db(tmp_path / "snapshot.db", ("old",))
    db = session_factory()
    db.add(LiveSession(name="实盘A", mode="paper", status="running"))
    db.commit()
    db.close()

    res = bs.restore_backup(
        path=src, confirm=True, cfg=_restore_cfg(), db_path=target_db,
        backup_dir=tmp_path / "backup", session_factory=session_factory,
        now_fn=lambda: NOW)
    assert res["ok"] is False and "实盘会话" in res["message"]
    assert _values(target_db) == ["current"]


def test_restore_aborts_when_safety_backup_fails(
        tmp_path, session_factory, target_db):
    src = _make_db(tmp_path / "snapshot.db", ("old",))

    def _fail_backup(kind, **kw):
        return {"status": bs.STATUS_FAILED, "message": "磁盘剩余空间不足"}

    res = bs.restore_backup(
        path=src, confirm=True, cfg=_restore_cfg(), db_path=target_db,
        session_factory=session_factory, backup_fn=_fail_backup,
        now_fn=lambda: NOW)
    assert res["ok"] is False and "已中止" in res["message"]
    assert _values(target_db) == ["current"]


def test_restore_rejects_missing_source_and_bad_record(
        tmp_path, session_factory, target_db):
    res = bs.restore_backup(
        path=tmp_path / "nope.db", confirm=True, cfg=_restore_cfg(),
        db_path=target_db, session_factory=session_factory, now_fn=lambda: NOW)
    assert res["ok"] is False and "不存在" in res["message"]

    res = bs.restore_backup(
        record_id=999, confirm=True, cfg=_restore_cfg(), db_path=target_db,
        session_factory=session_factory, now_fn=lambda: NOW)
    assert res["ok"] is False and "备份记录不存在" in res["message"]


def test_restore_by_record_id(tmp_path, session_factory, target_db):
    src = _make_db(tmp_path / "snapshot.db", ("from-record",))
    rec = bs.save_record(
        session_factory, kind=bs.KIND_MANUAL, status=bs.STATUS_SUCCESS,
        started_at=NOW, finished_at=NOW, filename=src.name, path=str(src))

    res = bs.restore_backup(
        record_id=rec["id"], confirm=True, cfg=_restore_cfg(),
        db_path=target_db, backup_dir=tmp_path / "backup",
        session_factory=session_factory, now_fn=lambda: NOW)
    assert res["ok"] is True
    assert _values(target_db) == ["from-record"]


def test_list_records_newest_first_and_limited(session_factory):
    for i in range(3):
        bs.save_record(
            session_factory, kind=bs.KIND_AUTO, status=bs.STATUS_SUCCESS,
            started_at=NOW + timedelta(minutes=i))
    rows = bs.list_records(2, session_factory)
    assert len(rows) == 2
    assert rows[0]["started_at"] > rows[1]["started_at"]


def test_backup_restore_switch_defaults_off():
    assert bs.backup_settings({})["restore_enabled"] is False


def test_verify_and_restore_api(monkeypatch):
    from httpx import ASGITransport, AsyncClient

    from core.api import system as system_api
    from core.main import app

    monkeypatch.setattr(system_api.backup_service, "list_records",
                        lambda limit=20: [{"id": 3, "filename": "x.db"}])
    monkeypatch.setattr(system_api.backup_service, "verify_backup",
                        lambda p: {"ok": True, "path": str(p),
                                   "message": "校验通过", "table_count": 1})
    monkeypatch.setattr(system_api.backup_service, "restore_backup",
                        lambda **kw: {"ok": kw.get("confirm") is True,
                                      "matched": True, "message": "恢复完成"})

    async def _call():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            return (await c.get("/api/system/backup/records"),
                    await c.post("/api/system/backup/verify",
                                 json={"path": "data/backup/x.db"}),
                    await c.post("/api/system/backup/restore",
                                 json={"path": "data/backup/x.db",
                                       "confirm": True}))

    import asyncio
    rec, ver, res = asyncio.run(_call())
    assert rec.json()["code"] == 0 and rec.json()["data"][0]["id"] == 3
    assert ver.json()["code"] == 0 and ver.json()["data"]["table_count"] == 1
    assert res.json()["code"] == 0 and res.json()["data"]["matched"] is True


def test_restore_api_reports_refusal(monkeypatch):
    from httpx import ASGITransport, AsyncClient

    from core.api import system as system_api
    from core.main import app

    monkeypatch.setattr(
        system_api.backup_service, "verify_backup",
        lambda p: {"ok": False, "message": "备份文件不存在"})
    monkeypatch.setattr(
        system_api.backup_service, "restore_backup",
        lambda **kw: {"ok": False, "message": "恢复功能未启用"})

    async def _call():
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            return (await c.post("/api/system/backup/verify",
                                 json={"path": "nope.db"}),
                    await c.post("/api/system/backup/restore",
                                 json={"path": "nope.db"}))

    import asyncio
    ver, res = asyncio.run(_call())
    assert ver.json()["code"] == 400 and "不存在" in ver.json()["message"]
    assert res.json()["code"] == 400 and "未启用" in res.json()["message"]
