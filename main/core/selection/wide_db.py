# -*- coding: utf-8 -*-
"""外部宽表库只读连接器（A0-2）。

D1 决策：宽库是**一次性导入源**，不是运行期依赖。
- 导入期：``open_wide_db()`` 只读打开、批量读、写进平台库；
- 运行期：选股与回测**不碰这个文件**，即使它被移动/改表结构也不影响已导入数据。

因此本模块只提供三样东西：路径解析、只读连接、结构探针。
不做增量同步、不做写入、不做「运行期兜底读宽库」——那会违背 D1。
"""

from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Dict, List, Optional

#: 未配置时的默认位置（与外部选股实现同机同路径）。
DEFAULT_WIDE_DB = r"C:\Users\ouy1\TQData\tq_wide.sqlite3"


def resolve_wide_db_path() -> str:
    """优先级：环境变量 > config.selection.wide_db_path > 默认路径。"""
    env = (os.environ.get("TQ_WIDE_DB") or "").strip()
    if env:
        return env
    try:
        from ..config import load_config
        cfg = load_config() or {}
        configured = ((cfg.get("selection") or {}).get("wide_db_path") or "").strip()
        if configured:
            return configured
    except Exception:  # 配置缺失不应让探针失败
        pass
    return DEFAULT_WIDE_DB


def _uri(path: str) -> str:
    """Windows 反斜杠必须转成 POSIX 才能拼 ``file:`` URI。"""
    return "file:%s?mode=ro" % Path(path).as_posix()


@contextmanager
def open_wide_db(path: str = ""):
    """只读连接（``mode=ro`` + ``query_only``）；文件不存在时 sqlite 直接报错。"""
    target = path or resolve_wide_db_path()
    conn = sqlite3.connect(_uri(target), uri=True, timeout=5)
    try:
        conn.execute("PRAGMA query_only = ON")
        yield conn
    finally:
        conn.close()


def probe(path: str = "") -> Dict:
    """结构探针：只回答「能不能读、有哪些表、各表有多少列、有没有数据」。

    刻意不做 ``COUNT(*)``：kline 类大表全表扫描会让状态接口变慢，
    行数交给导入器按需统计。``rows_estimate`` 取自 ``sqlite_stat1``（有则用）。
    """
    target = path or resolve_wide_db_path()
    info: Dict = {
        "path": target,
        "ready": False,
        "readonly": True,
        "size_mb": None,
        "tables": [],
        "error": None,
    }
    p = Path(target)
    if not p.exists():
        info["error"] = "wide_db_not_found"
        return info
    try:
        info["size_mb"] = round(p.stat().st_size / 1024 / 1024, 1)
        with open_wide_db(target) as conn:
            names = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name")]
            stats = {}
            try:
                for name, stat in conn.execute(
                        "SELECT tbl, stat FROM sqlite_stat1"):
                    try:
                        stats[name] = int(str(stat).split()[0])
                    except (ValueError, IndexError):
                        pass
            except sqlite3.DatabaseError:
                pass  # 未 ANALYZE 过，没有统计表
            for name in names:
                cols = conn.execute("PRAGMA table_info(%s)" % _quote(name)).fetchall()
                has_rows = conn.execute(
                    "SELECT 1 FROM %s LIMIT 1" % _quote(name)).fetchone() is not None
                info["tables"].append({
                    "name": name,
                    "columns": len(cols),
                    "has_rows": has_rows,
                    "rows_estimate": stats.get(name),
                })
        info["ready"] = True
    except sqlite3.Error as exc:
        info["error"] = "wide_db_unreadable: %s" % exc
    return info


def read_sql(sql: str, params: tuple = (), path: str = "") -> List[tuple]:
    """导入期专用：执行一条只读查询并取全部行。

    仅允许 SELECT/WITH/PRAGMA；写语句直接拒绝，避免误把导入器写成双向同步。
    """
    head = sql.lstrip().split(None, 1)[0].upper() if sql.strip() else ""
    if head not in ("SELECT", "WITH", "PRAGMA"):
        raise ValueError("wide_db 只读，拒绝执行：%s" % head)
    with open_wide_db(path) as conn:
        return conn.execute(sql, params).fetchall()


def _quote(name: str) -> str:
    return '"%s"' % name.replace('"', '""')


def table_columns(table: str, path: str = "") -> List[str]:
    with open_wide_db(path) as conn:
        rows = conn.execute("PRAGMA table_info(%s)" % _quote(table)).fetchall()
    return [r[1] for r in rows]
