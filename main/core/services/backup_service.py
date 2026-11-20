"""SQLite 内嵌数据备份（VACUUM INTO 一致性副本 + WAL 收敛 + 滚动保留 + 磁盘保护）。

背景：平台库开启 WAL 后，人工 `cp dev.db` 可能拿到不含最新事务的副本，
且 `dev.db-wal` 会持续增长。本服务在平台进程内完成：

- **一致性副本**：`VACUUM INTO` 在线生成（不停服、不阻塞实盘读写）；
- **WAL 收敛**：备份后执行一次 `PRAGMA wal_checkpoint(TRUNCATE)`（每日一次）；
- **滚动保留**：自动备份只保留最近 keep 份，手动备份不参与自动清理；
- **磁盘保护**：剩余空间低于 min_free_mb 直接跳过并记 WARNING，绝不写坏库。

调度由 ``BackupScheduler``（daemon 线程，默认每日 15:10）驱动，不依赖外部
计划任务，不阻塞 FastAPI 事件循环，也不触碰实盘 ``_book_lock`` / TQ 锁。
"""
from __future__ import annotations

import logging
import os
import re
import shutil
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

from core.config import app_root, load_config, use_app_root
from core.db import SessionLocal, engine, MAINTENANCE_GATE
from core.engine.live.timing import now_shanghai
from core.models import DataBackupRecord, LiveSession

logger = logging.getLogger(__name__)

KIND_AUTO = "auto"
KIND_MANUAL = "manual"

STATUS_SUCCESS = "success"
STATUS_SKIPPED = "skipped"
STATUS_FAILED = "failed"
#: 本轮让出（增量回测正在写库），不落记录、不消耗当日自动备份机会，60s 后重试。
STATUS_DEFERRED = "deferred"

DEFAULT_ENABLED = True
DEFAULT_DAILY_AT = "15:10"
DEFAULT_KEEP = 10
DEFAULT_MIN_FREE_MB = 500
DEFAULT_BACKUP_DIR = "data/backup"
DEFAULT_RETRY_COOLDOWN = timedelta(minutes=10)
#: 恢复功能默认关闭：需在 config.yaml backup.restore_enabled=true 才允许走 API 恢复。
DEFAULT_RESTORE_ENABLED = False
#: 校验时最多统计多少张表的行数（防御异常库把单次校验拖死）。
MAX_VERIFY_TABLES = 200

# main/（本文件位于 main/core/services/）
_MAIN_DIR = Path(__file__).resolve().parent.parent.parent

#: 自动备份文件名：{源库 stem}-YYYYMMDD-HHMMSS.db（同秒重复备份追加 -N）。
#: 手动备份为 {stem}-manual-YYYYMMDD-HHMMSS.db（stem 不含 manual，不会被自动清理）。
AUTO_NAME_RE = re.compile(
    r"^(?P<stem>.+)-(?P<date>\d{8})-(?P<time>\d{6})(?:-\d+)?\.db$")
_MANUAL_TAG = "-manual-"


# ---------------------------------------------------------------- 配置
def parse_hhmm(raw, default=(15, 10)):
    """解析 HH:MM；非法/缺省回退 default（时点自由度收敛到分钟）。"""
    try:
        hh, mm = str(raw or "").strip().split(":")
        hh, mm = int(hh), int(mm)
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return hh, mm
    except (TypeError, ValueError):
        pass
    return default


def _as_int(value, default: int) -> int:
    try:
        n = int(value)
    except (TypeError, ValueError):
        return default
    return n if n > 0 else default


def _as_float(value, default: float) -> float:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return default
    return n if n >= 0 else default


def backup_settings(cfg: Optional[dict] = None) -> dict:
    """读取 ``backup.*`` 配置；缺省/非法一律回退默认（不因笔误整日不备份）。"""
    cfg = load_config() if cfg is None else cfg
    raw = cfg.get("backup") if isinstance(cfg, dict) else None
    raw = raw if isinstance(raw, dict) else {}
    return {
        "enabled": bool(raw.get("enabled", DEFAULT_ENABLED)),
        "daily_at": str(raw.get("daily_at") or DEFAULT_DAILY_AT),
        "daily_at_hm": parse_hhmm(raw.get("daily_at")),
        "keep": _as_int(raw.get("keep"), DEFAULT_KEEP),
        "min_free_mb": _as_float(raw.get("min_free_mb"), DEFAULT_MIN_FREE_MB),
        "dir": str(raw.get("dir") or DEFAULT_BACKUP_DIR),
        "restore_enabled": bool(raw.get(
            "restore_enabled", DEFAULT_RESTORE_ENABLED)),
    }


def resolve_path(raw: str) -> Path:
    """相对路径锚定 main/（源码运行）或 app_root()（打包版），同 core.db 口径。"""
    p = Path(raw)
    if not p.is_absolute():
        p = (app_root() if use_app_root() else _MAIN_DIR) / p
    return p


def database_path() -> Path:
    """当前生效的 SQLite 文件路径（与 core.db 的 engine 同源）。"""
    return Path(str(engine.url.database))


def free_disk_mb(path: Path) -> float:
    return shutil.disk_usage(str(path)).free / (1024.0 * 1024.0)


# ---------------------------------------------------------------- 记录
def record_to_dict(row: DataBackupRecord) -> dict:
    return {
        "id": row.id,
        "kind": row.kind,
        "status": row.status,
        "filename": row.filename,
        "path": row.path,
        "size_bytes": int(row.size_bytes) if row.size_bytes is not None else None,
        "message": row.message,
        "started_at": row.started_at.isoformat() if row.started_at else None,
        "finished_at": row.finished_at.isoformat() if row.finished_at else None,
    }


def save_record(session_factory=None, *, kind: str, status: str,
                started_at: datetime, finished_at: Optional[datetime] = None,
                filename: Optional[str] = None, path: Optional[str] = None,
                size_bytes: Optional[int] = None,
                message: Optional[str] = None) -> dict:
    """落一条备份记录并返回可序列化 dict。

    记录写失败只告警不抛出：备份文件是否成功以文件系统为准，
    记录缺一行不应把成功备份报成失败。
    """
    factory = session_factory or SessionLocal
    db = factory()
    try:
        row = DataBackupRecord(
            kind=kind, status=status, filename=filename, path=path,
            size_bytes=size_bytes, message=message,
            started_at=started_at, finished_at=finished_at,
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        data = record_to_dict(row)
        db.expunge(row)
        return data
    except Exception:  # noqa: BLE001 — 记录失败不掩盖备份结果
        db.rollback()
        logger.exception("备份记录写入失败（备份结果仍以文件为准）")
        return {
            "id": None, "kind": kind, "status": status, "filename": filename,
            "path": path, "size_bytes": size_bytes, "message": message,
            "started_at": started_at.isoformat() if started_at else None,
            "finished_at": finished_at.isoformat() if finished_at else None,
        }
    finally:
        db.close()


def latest_record(kind: str, session_factory=None) -> Optional[DataBackupRecord]:
    factory = session_factory or SessionLocal
    db = factory()
    try:
        row = (
            db.query(DataBackupRecord)
            .filter(DataBackupRecord.kind == kind)
            .order_by(DataBackupRecord.started_at.desc(),
                      DataBackupRecord.id.desc())
            .first()
        )
        if row is not None:
            db.expunge(row)
        return row
    finally:
        db.close()


def list_records(limit: int = 20, session_factory=None) -> list:
    """最近备份记录（系统配置页恢复入口用；按时间倒序，limit 夹在 1~200）。"""
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = 20
    limit = min(max(limit, 1), 200)
    factory = session_factory or SessionLocal
    db = factory()
    try:
        rows = (db.query(DataBackupRecord)
                .order_by(DataBackupRecord.started_at.desc(),
                          DataBackupRecord.id.desc())
                .limit(limit)
                .all())
        data = [record_to_dict(r) for r in rows]
        for r in rows:
            db.expunge(r)
        return data
    finally:
        db.close()


def auto_needs_run(now: datetime, session_factory=None,
                   retry_cooldown: timedelta = DEFAULT_RETRY_COOLDOWN) -> bool:
    """当日是否还需要跑自动备份。

    - 今日无 auto 记录 / 记录来自更早日期 → 需要；
    - 今日 success/skipped → 不需要（已备份过 / 已有保护性跳过结论）；
    - 今日 failed → 冷却 retry_cooldown 后重试（坏盘不会每轮刷屏）。
    """
    rec = latest_record(KIND_AUTO, session_factory)
    if rec is None or rec.started_at is None:
        return True
    if rec.started_at.date() != now.date():
        return True
    if rec.status in (STATUS_SUCCESS, STATUS_SKIPPED):
        return False
    return (now - rec.started_at) >= retry_cooldown


# ---------------------------------------------------------------- 备份本体
def _backup_filename(stem: str, kind: str, now: datetime) -> str:
    ts = now.strftime("%Y%m%d-%H%M%S")
    if kind == KIND_MANUAL:
        return f"{stem}{_MANUAL_TAG}{ts}.db"
    return f"{stem}-{ts}.db"


def _unique_path(directory: Path, name: str) -> Path:
    """同一秒内重复备份（如连点两次「立即备份」）不覆盖已有副本。"""
    path = directory / name
    if not path.exists():
        return path
    base, suffix = name[:-3], name[-3:]  # 去掉 ".db"
    for i in range(1, 1000):
        cand = directory / f"{base}-{i}{suffix}"
        if not cand.exists():
            return cand
    raise RuntimeError(f"备份目录中同名副本过多：{name}")


def _vacuum_into(src: Path, dest: Path) -> None:
    """VACUUM INTO 生成一致性副本（源库可为 WAL 且正被其它连接读写）。"""
    conn = sqlite3.connect(str(src), timeout=30.0)
    try:
        conn.execute("VACUUM INTO ?", (str(dest),))
    finally:
        conn.close()


def checkpoint_wal(db_path: Path) -> str:
    """备份后收敛 WAL：PRAGMA wal_checkpoint(TRUNCATE)。

    返回 ``ok`` / ``busy``（有活跃读事务占用，本轮未收敛）/ ``skipped``（非 WAL 库）
    / ``error``。任何非 ok 都只告警，不影响备份结果。
    """
    conn = sqlite3.connect(str(db_path), timeout=30.0, isolation_level=None)
    try:
        mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
        if mode != "wal":
            return "skipped"
        row = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if row and int(row[0]) != 0:
            logger.warning(
                "WAL 收敛未完成（checkpoint busy，活跃读事务占用）：%s，下次再收敛",
                db_path)
            return "busy"
        return "ok"
    except sqlite3.Error as e:
        logger.warning("WAL 收敛失败：%s（%s）", db_path, e)
        return "error"
    finally:
        conn.close()


def cleanup_auto(dest_dir: Path, stem: str, keep: int) -> list:
    """删除超出 keep 的最旧自动备份；手动备份（stem-manual-*）不参与。"""
    files = []
    for p in dest_dir.glob("*.db"):
        m = AUTO_NAME_RE.match(p.name)
        if m and m.group("stem") == stem:
            files.append(p)
    files.sort(key=lambda p: p.name)  # 文件名内嵌时间戳，字典序即时间序
    removed = []
    for p in files[:max(len(files) - keep, 0)]:
        try:
            p.unlink()
            removed.append(p.name)
        except OSError:
            logger.warning("清理旧备份失败：%s", p, exc_info=True)
    return removed


def run_backup(kind: str = KIND_MANUAL, **kwargs) -> dict:
    """执行一次备份（手动/自动共用入口），返回记录 dict。

    与增量回测通过 ``MAINTENANCE_GATE`` 互斥（2026-11-20 15:10 争用）：自动备份
    抢不到就让出本轮（返回 ``status=deferred``、不落记录，调度器 60s 后重试，
    不会跳过当日备份）；手动备份最多等 120s，仍拿不到才让出。
    抢到后在 finally 释放，保证 VACUUM INTO 半途失败也不会把闸永久占死。
    """
    timeout = 0.0 if kind == KIND_AUTO else 120.0
    if not MAINTENANCE_GATE.acquire(timeout=timeout):
        logger.info("增量回测写入中，%s备份本轮延后（60s 后重试）",
                    "自动" if kind == KIND_AUTO else "手动")
        return {"id": None, "kind": kind, "status": STATUS_DEFERRED,
                "message": "增量回测写入中，本轮让出，稍后重试"}
    try:
        return _run_backup_impl(kind, **kwargs)
    finally:
        MAINTENANCE_GATE.release()


def _run_backup_impl(kind: str = KIND_MANUAL, *, db_path=None, backup_dir=None,
                     cfg: Optional[dict] = None, session_factory=None,
                     now_fn: Callable[[], datetime] = now_shanghai,
                     free_mb_fn: Callable[[Path], float] = free_disk_mb) -> dict:
    """备份的实际执行体（只在持有 ``MAINTENANCE_GATE`` 时调用）。

    失败/跳过都会落记录并写日志，绝不假装成功；调用方按 ``status`` 判定。
    """
    settings = backup_settings(cfg)
    now = now_fn()
    src = Path(db_path) if db_path is not None else database_path()
    dest_dir = (Path(backup_dir) if backup_dir is not None
                else resolve_path(settings["dir"]))
    label = "自动" if kind == KIND_AUTO else "手动"

    try:
        dest_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        message = f"备份目录不可用：{dest_dir}（{e}）"
        logger.error("%s", message)
        return save_record(session_factory, kind=kind, status=STATUS_FAILED,
                           started_at=now, finished_at=now, message=message)

    if not src.exists():
        message = f"数据库文件不存在：{src}"
        logger.error("%s", message)
        return save_record(session_factory, kind=kind, status=STATUS_FAILED,
                           started_at=now, finished_at=now, message=message)

    free = free_mb_fn(dest_dir)
    if free < settings["min_free_mb"]:
        message = (f"磁盘剩余空间不足（{free:.0f} MB < 阈值 "
                   f"{settings['min_free_mb']:.0f} MB），已跳过本次{label}备份")
        logger.warning("%s：%s", message, src)
        return save_record(session_factory, kind=kind, status=STATUS_SKIPPED,
                           started_at=now, finished_at=now_fn(), message=message)

    dest = _unique_path(dest_dir, _backup_filename(src.stem, kind, now))
    try:
        _vacuum_into(src, dest)
        size = dest.stat().st_size
    except Exception as e:  # noqa: BLE001 — 任何失败都要落记录并清理半成品
        try:
            if dest.exists():
                dest.unlink()
        except OSError:
            logger.warning("清理不完整备份失败：%s", dest, exc_info=True)
        message = f"{label}备份失败：{e}"
        logger.exception("%s -> %s", message, dest)
        return save_record(session_factory, kind=kind, status=STATUS_FAILED,
                           started_at=now, finished_at=now_fn(),
                           filename=dest.name, path=str(dest), message=message)

    checkpoint = checkpoint_wal(src)
    # 只清理自动备份：手动备份是用户显式动作，不静默删除（保留策略只约束自动）。
    removed = (cleanup_auto(dest_dir, src.stem, settings["keep"])
               if kind == KIND_AUTO else [])
    notes = []
    if removed:
        notes.append(f"已清理最旧自动备份 {len(removed)} 份")
    if checkpoint not in ("ok", "skipped"):
        notes.append(f"WAL 收敛未完成（{checkpoint}）")
    logger.info(
        "数据备份完成（%s）：%s -> %s（%.1f MB，WAL=%s，清理 %d 份）",
        label, src, dest, size / (1024.0 * 1024.0), checkpoint, len(removed))
    return save_record(session_factory, kind=kind, status=STATUS_SUCCESS,
                       started_at=now, finished_at=now_fn(),
                       filename=dest.name, path=str(dest), size_bytes=size,
                       message="；".join(notes) or None)


def get_status(now_fn: Callable[[], datetime] = now_shanghai,
               session_factory=None) -> dict:
    """备份状态（系统配置页展示：配置 + 上次自动/手动记录 + 今日是否已自动备份）。"""
    settings = backup_settings()
    now = now_fn()
    last_auto = latest_record(KIND_AUTO, session_factory)
    last_manual = latest_record(KIND_MANUAL, session_factory)
    auto_done_today = bool(
        last_auto is not None
        and last_auto.status == STATUS_SUCCESS
        and last_auto.started_at is not None
        and last_auto.started_at.date() == now.date())
    return {
        "enabled": settings["enabled"],
        "daily_at": settings["daily_at"],
        "keep": settings["keep"],
        "min_free_mb": settings["min_free_mb"],
        "backup_dir": str(resolve_path(settings["dir"])),
        "restore_enabled": settings["restore_enabled"],
        "last_auto": record_to_dict(last_auto) if last_auto else None,
        "last_manual": record_to_dict(last_manual) if last_manual else None,
        "auto_done_today": auto_done_today,
    }


# ---------------------------------------------------------------- 校验 / 恢复
def verify_backup(path) -> dict:
    """只读校验一个备份文件：``PRAGMA integrity_check`` + 表清单 + 各表行数。

    返回 ``{ok, path, exists, size_bytes, integrity, table_count, tables,
    row_counts, latest_record, message}``；任何失败都返回 ``ok=False`` 而不是抛异常
    （校验是恢复前的准入检查，调用方按 ok 判定）。
    """
    p = Path(path)
    result = {
        "ok": False, "path": str(p), "exists": p.exists(), "size_bytes": None,
        "integrity": "", "table_count": 0, "tables": [], "row_counts": {},
        "latest_record": None, "message": "",
    }
    if not p.exists():
        result["message"] = "备份文件不存在"
        return result
    result["size_bytes"] = p.stat().st_size
    tables = []
    try:
        conn = sqlite3.connect(
            "file:%s?mode=ro" % p.as_posix(), uri=True, timeout=30.0)
        try:
            row = conn.execute("PRAGMA integrity_check").fetchone()
            integrity = str(row[0]) if row else ""
            result["integrity"] = integrity
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name LIMIT ?",
                (MAX_VERIFY_TABLES,))]
            result["tables"] = tables
            result["table_count"] = len(tables)
            counts = {}
            for name in tables:
                safe = str(name).replace('"', '""')
                counts[str(name)] = int(conn.execute(
                    'SELECT COUNT(*) FROM "%s"' % safe).fetchone()[0])
            result["row_counts"] = counts
            if "data_backup_records" in counts:
                row = conn.execute(
                    "SELECT MAX(started_at) FROM data_backup_records").fetchone()
                result["latest_record"] = row[0] if row else None
        finally:
            conn.close()
    except sqlite3.Error as e:
        result["message"] = "备份文件校验失败：%s" % e
        return result
    if result["integrity"] != "ok":
        result["message"] = "备份文件完整性校验未通过（%s）" % result["integrity"]
        return result
    if not tables:
        result["message"] = "备份文件中没有业务表"
        return result
    result["ok"] = True
    result["message"] = "校验通过（%d 张表）" % len(tables)
    return result


def find_record(record_id: int, session_factory=None) -> Optional[DataBackupRecord]:
    factory = session_factory or SessionLocal
    db = factory()
    try:
        row = (db.query(DataBackupRecord)
               .filter(DataBackupRecord.id == int(record_id)).first())
        if row is not None:
            db.expunge(row)
        return row
    finally:
        db.close()


def running_live_sessions(session_factory=None) -> list:
    """存在 running/stopping 的实盘会话时禁止恢复（库文件正被交易链路使用）。"""
    factory = session_factory or SessionLocal
    db = factory()
    try:
        rows = (db.query(LiveSession)
                .filter(LiveSession.status.in_(("running", "stopping")))
                .order_by(LiveSession.id)
                .all())
        data = [{"id": r.id, "name": r.name, "status": r.status} for r in rows]
        for r in rows:
            db.expunge(r)
        return data
    finally:
        db.close()


def _remove_sqlite_sidecars(db_path: Path) -> None:
    """删除 ``-wal`` / ``-shm``：换库前必须清掉旧库的 WAL，否则会污染新库。"""
    for suffix in ("-wal", "-shm"):
        side = Path(str(db_path) + suffix)
        if side.exists():
            side.unlink()


def restore_backup(record_id: Optional[int] = None, path=None, *,
                   confirm: bool = False,
                   cfg: Optional[dict] = None,
                   db_path=None,
                   backup_dir=None,
                   session_factory=None,
                   backup_fn: Optional[Callable[..., dict]] = None,
                   now_fn: Callable[[], datetime] = now_shanghai,
                   dispose_fn: Optional[Callable[[], None]] = None) -> dict:
    """从备份文件恢复当前库（**破坏性操作，多重前置校验全部通过才执行**）。

    顺序：显式 ``confirm`` → 功能开关 ``backup.restore_enabled``（默认关闭）→
    定位备份文件 → 拒绝存在 running/stopping 实盘会话 → 只读校验备份文件 →
    先给当前库做一份手动备份（失败即中止）→ 收敛 WAL → 关闭连接 →
    原子替换 → 重开连接校验表数与行数。

    返回 ``{ok, message, restored_from, safety_backup, verify, matched, ...}``；
    任何失败都 ``ok=False`` 并给出原因，绝不抛异常、绝不删除旧库（替换前先备份）。
    """
    settings = backup_settings(cfg)
    target = Path(db_path) if db_path is not None else database_path()
    result = {"ok": False, "message": "", "restored_from": None,
              "safety_backup": None, "verify": None, "source_verify": None,
              "matched": False}

    if not confirm:
        result["message"] = "恢复数据库属于破坏性操作，需显式确认（confirm=true）"
        return result
    if not settings["restore_enabled"]:
        result["message"] = ("恢复功能未启用：请在 config.yaml 设置 "
                             "backup.restore_enabled=true 后重试")
        return result

    # ---- 定位备份文件 ----
    src: Optional[Path] = None
    if record_id is not None:
        rec = find_record(record_id, session_factory)
        if rec is None or not rec.path:
            result["message"] = "备份记录不存在或缺少文件路径：id=%s" % record_id
            return result
        src = Path(rec.path)
    elif path:
        src = Path(path)
    else:
        result["message"] = "请指定备份记录 id 或备份文件路径"
        return result
    result["restored_from"] = str(src)
    if not src.exists():
        result["message"] = "备份文件不存在：%s" % src
        return result
    if src.resolve() == target.resolve():
        result["message"] = "备份文件与当前库是同一个文件，无需恢复"
        return result

    # ---- 实盘会话保护 ----
    running = running_live_sessions(session_factory)
    if running:
        names = "、".join("%s(%s)" % (r["name"], r["status"]) for r in running[:5])
        result["message"] = "存在运行中/停止中的实盘会话（%s），请先手动停止后再恢复" % names
        return result

    # ---- 备份文件准入 ----
    source_verify = verify_backup(src)
    result["verify"] = source_verify
    result["source_verify"] = source_verify
    if not source_verify["ok"]:
        result["message"] = "恢复前校验未通过：%s" % source_verify["message"]
        return result

    # ---- 先备份当前库（失败即中止，绝不无副本直接覆盖）----
    backup = (backup_fn or run_backup)(
        KIND_MANUAL, db_path=target, backup_dir=backup_dir, cfg=cfg,
        session_factory=session_factory, now_fn=now_fn)
    result["safety_backup"] = {
        "id": backup.get("id"), "filename": backup.get("filename"),
        "path": backup.get("path"), "status": backup.get("status"),
    }
    if backup.get("status") != STATUS_SUCCESS:
        result["message"] = ("恢复前自动备份当前库失败（%s），已中止恢复"
                             % (backup.get("message") or backup.get("status")))
        return result

    # ---- 收敛 WAL → 关连接 → 原子替换 ----
    temp = target.with_name(".%s.restore-%s.tmp" % (
        target.name, now_fn().strftime("%Y%m%d%H%M%S")))
    dispose = dispose_fn
    if dispose is None and db_path is None:
        dispose = engine.dispose
    try:
        checkpoint_wal(target)
        if dispose is not None:
            dispose()
        shutil.copy2(src, temp)
        copy_verify = verify_backup(temp)
        if not copy_verify["ok"]:
            result["message"] = "临时副本校验失败：%s" % copy_verify["message"]
            return result
        _remove_sqlite_sidecars(target)
        os.replace(temp, target)
    except Exception as e:  # noqa: BLE001 — 失败必须保留现场并给出原因
        logger.exception("恢复数据库失败：%s -> %s", src, target)
        try:
            if temp.exists():
                temp.unlink()
        except OSError:
            logger.warning("清理恢复临时文件失败：%s", temp, exc_info=True)
        result["message"] = "恢复失败：%s" % e
        return result
    finally:
        if dispose is not None:
            # 替换后再次清池，确保后续连接打开的是新库文件
            try:
                dispose()
            except Exception:  # noqa: BLE001
                logger.warning("恢复后清理连接池失败", exc_info=True)

    # ---- 重开连接校验：表数与行数必须与备份文件一致 ----
    after = verify_backup(target)
    result["verify"] = after
    matched = bool(
        after["ok"]
        and after["tables"] == source_verify["tables"]
        and after["row_counts"] == source_verify["row_counts"])
    result["matched"] = matched
    if not after["ok"]:
        result["message"] = "恢复后校验失败：%s" % after["message"]
        return result
    if not matched:
        result["message"] = "恢复后校验不一致（表数/行数与备份文件不符），请人工核查"
        logger.error("恢复后校验不一致：%s", target)
        return result
    result["ok"] = True
    result["message"] = "恢复完成并通过校验（%d 张表，%d 行）" % (
        after["table_count"], sum(after["row_counts"].values()))
    logger.warning("数据库已从备份恢复：%s -> %s（恢复前副本 %s）",
                   src, target, backup.get("filename"))
    return result


# ---------------------------------------------------------------- 调度
class BackupScheduler:
    """进程内每日自动备份调度（daemon 线程，分钟级轮询）。

    启动即检查一次（服务在 15:10 之后才启动时当日补备），之后每
    ``interval_seconds`` 检查：enabled → 到点 → 当日未备份（失败按冷却重试）。
    备份在调度线程内同步执行，不占用 FastAPI 事件循环。
    """

    def __init__(self, *, run_fn: Optional[Callable[..., dict]] = None,
                 cfg_provider: Optional[Callable[[], dict]] = None,
                 session_factory=None,
                 now_fn: Callable[[], datetime] = now_shanghai,
                 interval_seconds: float = 60.0,
                 retry_cooldown: timedelta = DEFAULT_RETRY_COOLDOWN):
        self._run = run_fn or run_backup
        self._cfg = cfg_provider or load_config
        self._session_factory = session_factory
        self._now = now_fn
        self._interval = max(float(interval_seconds), 5.0)
        self._retry_cooldown = retry_cooldown
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_attempt_at: Optional[datetime] = None
        # 本轮进程内已完成（成功/保护性跳过）：即便记录落库失败也不重复复制，
        # 避免 DB 抖动时每 60s 生成一份副本。跨重启由 data_backup_records 兜底。
        self._last_attempt_ok = False

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="data-backup-scheduler", daemon=True)
            self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.maybe_run()
            except Exception:  # noqa: BLE001 — 备份调度失败不得拖垮进程
                logger.exception("自动备份调度异常（下轮重试）")
            if self._stop.wait(self._interval):
                break

    def maybe_run(self) -> Optional[dict]:
        """单次检查（测试直接调用）：未到点/已备份/已关闭 → None。"""
        settings = backup_settings(self._cfg())
        if not settings["enabled"]:
            return None
        now = self._now()
        if (now.hour, now.minute) < settings["daily_at_hm"]:
            return None
        if (self._last_attempt_ok and self._last_attempt_at is not None
                and self._last_attempt_at.date() == now.date()):
            return None
        if not auto_needs_run(now, self._session_factory, self._retry_cooldown):
            return None
        logger.info("开始每日自动数据备份（%s）", now.strftime("%Y-%m-%d %H:%M"))
        result = self._run(KIND_AUTO, session_factory=self._session_factory,
                           now_fn=self._now)
        self._last_attempt_at = now
        self._last_attempt_ok = isinstance(result, dict) and result.get(
            "status") in (STATUS_SUCCESS, STATUS_SKIPPED)
        return result
