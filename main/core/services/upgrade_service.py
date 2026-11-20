# -*- coding: utf-8 -*-
"""升级 / 回滚路径（批次22 L5）：启动后只读版本复核 + 升级前强制备份。

设计边界
--------
- **启动自动迁移（真实行为）**：``core.main`` 的 lifespan 会调用 ``core.db.init_db()``，
  后者执行 ``alembic upgrade head``——即**应用每次启动都会把库迁移到代码 head**
  （失败则 fail-fast 阻断启动；``TQ_INIT_DB_DISABLED=1`` 可关闭，测试进程默认开启）。
  本模块**不执行迁移**，只做迁移后的只读复核、升级前清单与备份。
- **默认关闭**：``upgrade.enabled``（默认 false）只控制启动复核日志与
  「升级前备份」动作；关闭时新增入口不产生任何日志 / 写库。
- **只读检查**：``preflight`` 仅读 ``alembic_version`` 表与备份记录，供系统配置页展示。
- 备份复用 ``backup_service.run_backup``（VACUUM INTO 一致性副本），不触碰业务数据。
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

from alembic.config import Config
from alembic.runtime.migration import MigrationContext
from alembic.script import ScriptDirectory

from core.config import app_root, load_config, use_app_root
from core.db import engine
from core.engine.live.timing import now_shanghai
from core.services import backup_service

logger = logging.getLogger(__name__)

DEFAULT_ENABLED = False
DEFAULT_MAX_BACKUP_AGE_HOURS = 24.0

# main/（本文件位于 main/core/services/）
_MAIN_DIR = Path(__file__).resolve().parent.parent.parent


def _as_float(value, default: float) -> float:
    try:
        n = float(value)
    except (TypeError, ValueError):
        return default
    return n if n >= 0 else default


def guard_settings(cfg: Optional[dict] = None) -> dict:
    """读取 ``upgrade.*``：``enabled`` 默认 false，``max_backup_age_hours`` 默认 24。

    缺省/非法一律回退默认；``require_backup`` 仅在开关开启后生效。
    """
    cfg = load_config() if cfg is None else cfg
    raw = cfg.get("upgrade") if isinstance(cfg, dict) else None
    raw = raw if isinstance(raw, dict) else {}
    return {
        "enabled": bool(raw.get("enabled", DEFAULT_ENABLED)),
        "require_backup": bool(raw.get("require_backup", True)),
        "max_backup_age_hours": _as_float(
            raw.get("max_backup_age_hours"), DEFAULT_MAX_BACKUP_AGE_HOURS),
    }


def _alembic_config() -> Config:
    base = app_root() if use_app_root() else _MAIN_DIR
    return Config(str(base / "alembic.ini"))


def _down_revisions(revision) -> tuple:
    down = revision.down_revision
    if down is None:
        return ()
    if isinstance(down, (tuple, list)):
        return tuple(down)
    return (down,)


def migration_status() -> dict:
    """只读对比数据库 ``alembic_version`` 与脚本 head（不执行任何迁移）。"""
    try:
        script = ScriptDirectory.from_config(_alembic_config())
        heads = list(script.get_heads())
        with engine.connect() as conn:
            current = list(MigrationContext.configure(conn).get_current_heads())
    except Exception as exc:  # noqa: BLE001 — 读取失败降级，不影响启动
        logger.info("升级检查：读取迁移版本失败（%s: %s）", type(exc).__name__, exc)
        return {
            "available": False,
            "error": "%s: %s" % (type(exc).__name__, exc),
            "current": [], "heads": [], "pending": [], "pending_count": 0,
            "single_head": None, "at_head": None,
        }

    current_set = set(current)
    seen = set()
    pending = []
    for head in heads:
        rev = script.get_revision(head)
        while rev is not None and rev.revision not in current_set:
            if rev.revision in seen:
                break
            seen.add(rev.revision)
            pending.append({
                "revision": rev.revision,
                "doc": getattr(rev, "doc", None),
                "down_revision": rev.down_revision,
            })
            downs = _down_revisions(rev)
            rev = script.get_revision(downs[0]) if downs else None

    return {
        "available": True,
        "error": None,
        "current": current,
        "heads": heads,
        "pending": pending,
        "pending_count": len(pending),
        "single_head": len(heads) == 1,
        "at_head": bool(current_set) and current_set == set(heads),
    }


def _latest_success_backup(records) -> Optional[dict]:
    for rec in records or []:
        if rec.get("status") == backup_service.STATUS_SUCCESS:
            return rec
    return None


def _backup_age_hours(record: Optional[dict], now: datetime) -> Optional[float]:
    if not record:
        return None
    raw = record.get("finished_at") or record.get("started_at")
    if not raw:
        return None
    try:
        ts = datetime.fromisoformat(str(raw))
    except ValueError:
        return None
    return max((now - ts).total_seconds() / 3600.0, 0.0)


def _rollback_hints(current, backup_dir: str) -> list:
    cur = current[0] if current else "<当前revision>"
    return [
        "升级失败时先停服，再执行 `alembic downgrade %s` 回到当前版本" % cur,
        "若库文件损坏：停服后用最近一次备份（%s）替换当前 SQLite 文件" % backup_dir,
        "替换前同时移除同名 -wal / -shm 残留文件，避免回放未完成事务",
        "任何情况下都不要在服务运行中直接覆盖数据库文件",
    ]


def preflight(now: Optional[datetime] = None,
              session_factory=None) -> dict:
    """升级前只读清单：版本差异 / 最近备份是否新鲜 / 磁盘 / 回滚提示。"""
    now = now or now_shanghai()
    settings = guard_settings()
    status = migration_status()
    backup_status = backup_service.get_status(now_fn=lambda: now,
                                              session_factory=session_factory)
    backup_dir = backup_status.get("backup_dir") or ""
    record = _latest_success_backup(backup_service.list_records(
        20, session_factory=session_factory))
    age = _backup_age_hours(record, now)
    fresh = age is not None and age <= settings["max_backup_age_hours"]
    backup_required = bool(settings["enabled"] and settings["require_backup"]
                           and not fresh)

    db_path = backup_service.database_path()
    base = Path(backup_dir) if backup_dir else db_path.parent
    if not base.exists():
        base = db_path.parent
    free_mb = backup_service.free_disk_mb(base)

    steps = [
        "1. 执行「升级前备份」并确认最近备份成功（本页按钮或系统备份面板）",
        "2. 停止服务（监督进程与实盘会话按台账流程停）",
        "3. 在 main/ 下执行 `alembic upgrade head`",
        "4. 启动服务并检查 /health 与健康总览",
        "5. 异常时按下方回滚步骤恢复",
    ]
    return {
        "enabled": settings["enabled"],
        "checked_at": now.isoformat(),
        "migrations": status,
        "backup": {
            "latest": record,
            "age_hours": round(age, 2) if age is not None else None,
            "max_age_hours": settings["max_backup_age_hours"],
            "fresh": fresh,
            "required": backup_required,
            "dir": backup_dir,
        },
        "disk": {
            "free_mb": round(free_mb, 1),
            "min_free_mb": backup_status.get("min_free_mb"),
            "ok": free_mb >= float(backup_status.get("min_free_mb") or 0),
        },
        "steps": steps,
        "rollback": _rollback_hints(status.get("current") or [], backup_dir),
    }


def startup_check() -> dict:
    """启动后版本复核：init_db 已自动迁移，这里只做只读比对（不迁移）。

    仅在 ``upgrade.enabled=true`` 时输出日志；关闭时无副作用。复核结果落后说明
    启动迁移未执行成功（或被 ``TQ_INIT_DB_DISABLED=1`` 跳过），此时高亮告警，
    但本函数绝不自行 upgrade。
    """
    settings = guard_settings()
    if not settings["enabled"]:
        return {"enabled": False}
    status = migration_status()
    if not status.get("available"):
        logger.warning("升级检查：无法读取迁移版本（%s），请人工确认", status.get("error"))
        return {"enabled": True, **status}
    if not status.get("at_head"):
        logger.warning(
            "升级检查：数据库版本 %s 落后于代码 head %s（待执行 %d 条迁移）；"
            "正常情况下启动时 init_db 已自动迁移，若仍落后请检查迁移是否失败、"
            "是否被 TQ_INIT_DB_DISABLED=1 跳过，或命令行迁移指向了别的库（TQ_DB_PATH）",
            status.get("current") or "空", status.get("heads"),
            status.get("pending_count"))
    else:
        logger.info("升级检查：数据库版本已在 head（%s，启动自动迁移后复核）",
                    status.get("heads"))
    return {"enabled": True, **status}


def prepare_backup(confirm: bool = False,
                   run_backup: Callable[..., dict] = backup_service.run_backup) -> dict:
    """升级前强制备份：需 ``upgrade.enabled=true`` 且显式 ``confirm=true``。"""
    if not guard_settings()["enabled"]:
        return {"ok": False, "status": "rejected",
                "message": "升级前备份未启用（需配置 upgrade.enabled=true）"}
    if not confirm:
        return {"ok": False, "status": "rejected",
                "message": "需显式 confirm=true 才会执行备份"}
    result = run_backup(backup_service.KIND_MANUAL)
    ok = result.get("status") == backup_service.STATUS_SUCCESS
    return {
        "ok": ok,
        "status": result.get("status"),
        "message": result.get("message"),
        "record": result,
    }
