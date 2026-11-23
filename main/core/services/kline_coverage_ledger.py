"""K 线数据覆盖台账（P1-4，2026-11-23）。

这是**新增的备用能力**：为行情数据本身补一份「哪些交易日已核对完整」的台账，
让补数/完整性检查可以先查台账、少开库；开启与否不改变既有补数与校验行为。

存储选型：独立 SQLite（``data/kline_ledger.sqlite3``），而不是并入 ``dev.db``。
理由：
1. 台账是旁路数据，独立库不触碰既有表结构与 Alembic 迁移链，回退只需删文件；
2. 行情库可能被外部工具（通达信/掘金）读写，独立库避免长事务争锁；
3. 未来做界面查询时可直接只读打开，不影响主库备份/恢复语义。

三张表（与整改清单 P1-4 的建议一致）：
    coverage(code, period, field, ranges, source, updated_at)
        已核对完整的连续日期段，ranges 为 ``[[start, end], ...]`` JSON。
    empty_days(code, period, days, source, updated_at)
        数据源确认无行情的交易日（例如停牌），days 为 ``["YYYY-MM-DD", ...]``。
    stamps(code, period, source, path, mtime_ns, size, wal_mtime_ns, updated_at)
        来源库文件指纹。SQLite 源额外登记 ``-wal`` 的 mtime；parquet 源只用到
        mtime/size。

信任规则（P1-4）：
1. 指纹（mtime_ns/size）与登记不一致 → 整份记录作废，退回逐日核对；
2. 只有「缺口全部在尾部」才直接信任台账；头部或中间缺口一律退回逐日核对。

缺口三分类：
    source_empty   数据源确认无行情（停牌等）。通达信源只允许「夹在两段已有
                   数据之间」的缺日，头尾缺口不得判为无行情；
    not_downloaded 本地未下载（通达信源的头尾缺口优先归此类）；
    unknown        证据不足。

上市日裁剪：项目内**没有**可靠的上市日期数据源（回测侧无上市日字段；实盘
``_prev_close_and_listing`` 只是用最近 10 根日线推算已上市交易日数，不是上市
日期）。因此本模块不自动裁剪，只保留 :func:`clip_request_range` 供调用方显式
传入上市日时使用；没有数据源就不引入外部依赖或在线接口。

所有写入口都在默认关闭的开关之后（``backtest.kline_ledger.enabled``，默认
False；环境变量 ``TQ_KLINE_LEDGER_ENABLED`` 可覆盖）。关闭时既有补数流程逐字
不变。
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

logger = logging.getLogger(__name__)

# 来源常量
SOURCE_TDX = "tdx"
# 字段常量：all 表示整根 bar（用 Close 是否存在判定），也允许按 OHLCV 单字段登记
FIELD_ALL = "all"

# 缺口分类
GAP_SOURCE_EMPTY = "source_empty"
GAP_NOT_DOWNLOADED = "not_downloaded"
GAP_UNKNOWN = "unknown"

LEDGER_FILE_NAME = "kline_ledger.sqlite3"

#: 项目内是否存在可复用的上市日数据源。当前为 False → 不做上市日裁剪。
LISTING_DATE_SOURCE_AVAILABLE = False

_SCHEMA = """
CREATE TABLE IF NOT EXISTS coverage (
    code       TEXT NOT NULL,
    period     TEXT NOT NULL,
    field      TEXT NOT NULL,
    ranges     TEXT NOT NULL,
    source     TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (code, period, field, source)
);
CREATE TABLE IF NOT EXISTS empty_days (
    code       TEXT NOT NULL,
    period     TEXT NOT NULL,
    days       TEXT NOT NULL,
    source     TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (code, period, source)
);
CREATE TABLE IF NOT EXISTS stamps (
    code          TEXT NOT NULL,
    period        TEXT NOT NULL,
    source        TEXT NOT NULL,
    path          TEXT,
    mtime_ns      INTEGER,
    size          INTEGER,
    wal_mtime_ns  INTEGER,
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (code, period, source)
);
"""

_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# 路径与开关
# ---------------------------------------------------------------------------

def _app_base_dir() -> Path:
    """相对路径的锚定目录，与 ``core.db._db_path`` 的 ``sqlite_path`` 规则一致。

    非打包态锚到 ``main/``（``core/db.py`` 所在工程根）；打包态锚到
    ``app_root()``，避免安装目录只读时落错位置。台账路径**不能**锚到
    ``_db_path().parent``，否则配置里的 ``data/...`` 会被再拼一层 ``data``。
    """
    try:
        from core.db import _MAIN_DIR, app_root, use_app_root

        return app_root() if use_app_root() else Path(_MAIN_DIR)
    except Exception:  # pragma: no cover - 仅在异常环境降级
        return Path(__file__).resolve().parent.parent.parent


def ledger_path() -> Path:
    """台账库路径（唯一解析入口）。

    优先级：``TQ_KLINE_LEDGER_PATH`` 环境变量 > 配置
    ``backtest.kline_ledger.path`` > ``<app_base>/data/kline_ledger.sqlite3``。
    相对路径统一相对应用根目录（``main/`` 或打包 ``app_root()``）解析，与
    ``core.db._db_path`` 对 ``sqlite_path`` 的规则完全一致，避免出现
    ``data/data/kline_ledger.sqlite3``。
    """
    env = os.environ.get("TQ_KLINE_LEDGER_PATH")
    base = _app_base_dir()
    if env:
        p = Path(env)
        return p if p.is_absolute() else base / p
    try:
        from core.config import load_config

        cfg = load_config()
        configured = (
            (cfg.get("backtest") or {}).get("kline_ledger") or {}
        ).get("path")
    except Exception:  # pragma: no cover - 配置加载失败时用默认路径
        configured = None
    if configured:
        p = Path(configured)
        return p if p.is_absolute() else base / p
    return base / "data" / LEDGER_FILE_NAME


def is_enabled(cfg: Optional[dict] = None) -> bool:
    """台账写入口总开关，默认关闭。"""
    env = os.environ.get("TQ_KLINE_LEDGER_ENABLED")
    if env is not None:
        return env.strip().lower() in ("1", "true", "yes", "on")
    try:
        if cfg is None:
            from core.config import load_config

            cfg = load_config()
        section = (cfg.get("backtest") or {}).get("kline_ledger") or {}
        return bool(section.get("enabled", False))
    except Exception:  # pragma: no cover
        return False


# ---------------------------------------------------------------------------
# 连接与序列化
# ---------------------------------------------------------------------------

@contextmanager
def _connect(db_path: Optional[Path] = None, *, readonly: bool = False):
    """打开台账库。readonly=True 时文件不存在直接返回 None，绝不建库。

    写连接用进程内锁串行化：同一进程可能有多个回测线程同时登记，SQLite 的
    busy_timeout 只能兜底，先本地串行更稳。
    """
    target = Path(db_path) if db_path else ledger_path()
    if readonly:
        if not target.exists():
            yield None
            return
        uri = "file:%s?mode=ro" % quote(target.as_posix(), safe="/:")
        conn = sqlite3.connect(uri, uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
        finally:
            conn.close()
        return

    _LOCK.acquire()
    conn = None
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(target), timeout=5.0)
        conn.executescript(_SCHEMA)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    finally:
        if conn is not None:
            conn.close()
        _LOCK.release()


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _as_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date") and not isinstance(value, str):
        try:
            return value.date()
        except Exception:
            return None
    text = str(value).strip()
    if not text:
        return None
    # 支持 2026-01-05 / 20260105 / 2026-01-05 00:00:00
    for candidate in (text[:10], text[:8]):
        for fmt in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(candidate, fmt).date()
            except ValueError:
                continue
    return None


def _dates(values: Iterable[Any]) -> List[date]:
    if values is None:
        return []
    out = []
    for value in values:
        d = _as_date(value)
        if d is not None:
            out.append(d)
    return sorted(set(out))


def _ranges_json(ranges: Sequence[Tuple[date, date]]) -> str:
    return json.dumps(
        [[a.isoformat(), b.isoformat()] for a, b in ranges], ensure_ascii=False
    )


def _days_json(days: Sequence[date]) -> str:
    return json.dumps([d.isoformat() for d in days], ensure_ascii=False)


def _loads_ranges(raw: Optional[str]) -> List[Tuple[date, date]]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return []
    return normalize_ranges(data)


def _loads_days(raw: Optional[str]) -> List[date]:
    if not raw:
        return []
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return []
    return _dates(data)


def normalize_ranges(ranges: Iterable[Any]) -> List[Tuple[date, date]]:
    """规范化并合并日期段。

    接受 ``[[start, end], ...]``、``[(date, date), ...]`` 或单个日期字符串。
    两段之间没有交易日时合并（跨周末/节假日连续），有交易日缺口（停牌/未下载）
    则不合并，避免把真实缺口藏进一个连续段里。
    """
    parsed: List[Tuple[date, date]] = []
    if ranges is None:
        return []
    for item in ranges:
        if isinstance(item, str):
            d = _as_date(item)
            if d is not None:
                parsed.append((d, d))
            continue
        try:
            a, b = item
        except (TypeError, ValueError):
            continue
        da, db = _as_date(a), _as_date(b)
        if da is None or db is None:
            continue
        if db < da:
            da, db = db, da
        parsed.append((da, db))
    parsed.sort()
    merged: List[Tuple[date, date]] = []
    for a, b in parsed:
        if merged and _no_trading_day_between(merged[-1][1], a):
            if b > merged[-1][1]:
                merged[-1] = (merged[-1][0], b)
        else:
            merged.append((a, b))
    return merged


def _no_trading_day_between(prev_end: date, next_start: date) -> bool:
    """两段之间没有交易日时可合并（跨周末/节假日，但停牌缺日不合并）。"""
    if next_start <= prev_end + timedelta(days=1):
        return True
    return not trading_days(prev_end + timedelta(days=1),
                            next_start - timedelta(days=1))


# ---------------------------------------------------------------------------
# 交易日历
# ---------------------------------------------------------------------------

def trading_days(start: date, end: date) -> List[date]:
    """[start, end] 内的交易日（周末 + 项目内置节假日闭市表）。

    复用 ``core.engine.live.calendar.BUILTIN_CLOSURES``，不引入新的外部数据源；
    该表只收录 2026/2027，未收录年份与项目既有口径一致按工作日 fail-open，
    导入失败时同样降级为「周一至周五」（仅影响离线测试/异常环境）。
    """
    if end < start:
        return []
    try:
        from core.engine.live.calendar import BUILTIN_CLOSURES

        closures = BUILTIN_CLOSURES
    except Exception:  # pragma: no cover
        closures = {}
    out = []
    cur = start
    while cur <= end:
        closed = closures.get(cur.year, frozenset())
        if cur.weekday() < 5 and cur not in closed:
            out.append(cur)
        cur += timedelta(days=1)
    return out


# ---------------------------------------------------------------------------
# 来源文件指纹
# ---------------------------------------------------------------------------

def fingerprint(source_path: str | Path) -> Dict[str, Any]:
    """读取来源库文件指纹：mtime_ns / size，SQLite 额外带 -wal mtime。"""
    p = Path(source_path)
    st = p.stat()
    wal = Path(str(p) + "-wal")
    wal_mtime = None
    if wal.exists():
        try:
            wal_mtime = int(wal.stat().st_mtime_ns)
        except OSError:
            wal_mtime = None
    return {
        "path": str(p),
        "mtime_ns": int(st.st_mtime_ns),
        "size": int(st.st_size),
        "wal_mtime_ns": wal_mtime,
    }


def _stamp_matches(stamp: Optional[Dict[str, Any]],
                   source_path: Optional[str | Path] = None) -> bool:
    """指纹是否一致。任一侧缺失或文件不可读都视为不一致。"""
    if not stamp:
        return False
    target = source_path or stamp.get("path")
    if not target:
        return False
    try:
        current = fingerprint(target)
    except OSError:
        return False
    if stamp.get("mtime_ns") is None or stamp.get("size") is None:
        return False
    if int(stamp["mtime_ns"]) != current["mtime_ns"]:
        return False
    if int(stamp["size"]) != current["size"]:
        return False
    # -wal 指纹只在登记侧和当前侧都拿得到时比较；文件被 checkpoint 清掉不算变化
    if stamp.get("wal_mtime_ns") is not None and current.get("wal_mtime_ns") is not None:
        if int(stamp["wal_mtime_ns"]) != int(current["wal_mtime_ns"]):
            return False
    return True


# ---------------------------------------------------------------------------
# 读写台账
# ---------------------------------------------------------------------------

def _read_ranges(conn, code: str, period: str, field: str, source: str):
    row = conn.execute(
        "SELECT ranges FROM coverage WHERE code=? AND period=? AND field=? AND source=?",
        (code, period, field, source),
    ).fetchone()
    return _loads_ranges(row["ranges"] if row else None)


def _read_empty(conn, code: str, period: str, source: str):
    row = conn.execute(
        "SELECT days FROM empty_days WHERE code=? AND period=? AND source=?",
        (code, period, source),
    ).fetchone()
    return _loads_days(row["days"] if row else None)


def _read_stamp(conn, code: str, period: str, source: str):
    row = conn.execute(
        "SELECT path, mtime_ns, size, wal_mtime_ns, updated_at FROM stamps "
        "WHERE code=? AND period=? AND source=?",
        (code, period, source),
    ).fetchone()
    return dict(row) if row else None


def _upsert_coverage(conn, code: str, period: str, field: str,
                     ranges: Sequence[Tuple[date, date]], source: str) -> None:
    conn.execute(
        "INSERT INTO coverage(code, period, field, ranges, source, updated_at) "
        "VALUES(?,?,?,?,?,?) ON CONFLICT(code, period, field, source) DO UPDATE SET "
        "ranges=excluded.ranges, updated_at=excluded.updated_at",
        (code, period, field, _ranges_json(ranges), source, _now_iso()),
    )


def _upsert_empty(conn, code: str, period: str, days: Sequence[date],
                  source: str) -> None:
    conn.execute(
        "INSERT INTO empty_days(code, period, days, source, updated_at) "
        "VALUES(?,?,?,?,?) ON CONFLICT(code, period, source) DO UPDATE SET "
        "days=excluded.days, updated_at=excluded.updated_at",
        (code, period, _days_json(days), source, _now_iso()),
    )


def _upsert_stamp(conn, code: str, period: str, source: str,
                  source_path: str | Path) -> Dict[str, Any]:
    fp = fingerprint(source_path)
    conn.execute(
        "INSERT INTO stamps(code, period, source, path, mtime_ns, size, "
        "wal_mtime_ns, updated_at) VALUES(?,?,?,?,?,?,?,?) "
        "ON CONFLICT(code, period, source) DO UPDATE SET "
        "path=excluded.path, mtime_ns=excluded.mtime_ns, size=excluded.size, "
        "wal_mtime_ns=excluded.wal_mtime_ns, updated_at=excluded.updated_at",
        (code, period, source, fp["path"], fp["mtime_ns"], fp["size"],
         fp["wal_mtime_ns"], _now_iso()),
    )
    return fp


def _internal_empty_days(observed: Sequence[date], source: str) -> List[date]:
    """两段已有数据之间的缺日（只对通达信源登记）。

    头尾缺口不登记：那可能是「没下载」，不能当停牌。
    """
    if source != SOURCE_TDX or len(observed) < 2:
        return []
    seen = set(observed)
    first, last = observed[0], observed[-1]
    return [d for d in trading_days(first, last) if d not in seen]


def record_observation(code: str, period: str, field: str,
                       dates: Iterable[Any],
                       requested_start: Optional[Any] = None,
                       requested_end: Optional[Any] = None,
                       source: str = SOURCE_TDX, *,
                       db_path: Optional[str | Path] = None,
                       source_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """登记一次取数观测（单条）。

    仅记录实际取到的日期，并按 ``requested_start/end`` 裁剪；未取到的头尾缺口
    不会写成 empty_days（那会被误判为停牌）。
    """
    return record_observations(
        [{
            "code": code,
            "period": period,
            "field": field,
            "dates": list(dates) if dates is not None else [],
            "source": source,
            "source_path": source_path,
        }],
        requested_start=requested_start,
        requested_end=requested_end,
        db_path=db_path,
    )


def record_observations(items: Iterable[Dict[str, Any]], *,
                        requested_start: Optional[Any] = None,
                        requested_end: Optional[Any] = None,
                        db_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """批量登记取数观测。

    items: ``{code, period, field, dates, source, source_path}``。
    同一 ``(code, period, source)`` 的多个字段共享一次指纹与 empty_days。

    指纹变化时**整份旧登记作废**（不合并旧 ranges/empty_days），与 P1-4 的
    「指纹不一致整份作废」保持一致；指纹未变则增量合并。
    """
    items = list(items or [])
    if not items:
        return {"recorded": 0, "codes": 0}
    start_d = _as_date(requested_start)
    end_d = _as_date(requested_end)

    # 按 (code, period, source) 聚合字段与日期
    groups: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for item in items:
        code = str(item.get("code") or "").strip()
        period = str(item.get("period") or "").strip()
        if not code or not period:
            continue
        source = str(item.get("source") or SOURCE_TDX)
        field = str(item.get("field") or FIELD_ALL)
        ds = _dates(item.get("dates"))
        if start_d is not None:
            ds = [d for d in ds if d >= start_d]
        if end_d is not None:
            ds = [d for d in ds if d <= end_d]
        key = (code, period, source)
        group = groups.setdefault(key, {
            "fields": {}, "dates": set(), "source_path": None,
        })
        group["fields"].setdefault(field, set()).update(ds)
        group["dates"].update(ds)
        if item.get("source_path"):
            group["source_path"] = item["source_path"]

    recorded = 0
    with _connect(Path(db_path) if db_path else None) as conn:
        for (code, period, source), group in groups.items():
            observed = sorted(group["dates"])
            if not observed:
                continue
            source_path = group["source_path"]
            new_stamp = None
            if source_path:
                try:
                    new_stamp = fingerprint(source_path)
                except OSError:
                    new_stamp = None

            old_stamp = _read_stamp(conn, code, period, source)
            stamp_changed = bool(
                old_stamp and new_stamp and (
                    int(old_stamp.get("mtime_ns") or -1) != new_stamp["mtime_ns"]
                    or int(old_stamp.get("size") or -1) != new_stamp["size"]
                )
            )
            if stamp_changed:
                # 来源库已变化：整份旧登记作废，按本次观测重建（含本次未出现的
                # 旧字段，避免残留过期覆盖段）
                conn.execute(
                    "DELETE FROM coverage WHERE code=? AND period=? AND source=?",
                    (code, period, source),
                )
                conn.execute(
                    "DELETE FROM empty_days WHERE code=? AND period=? AND source=?",
                    (code, period, source),
                )
                old_empty: List[date] = []
            else:
                old_empty = _read_empty(conn, code, period, source)

            for field, field_dates in group["fields"].items():
                ds = sorted(field_dates)
                if not ds:
                    continue
                if stamp_changed:
                    old_ranges: List[Tuple[date, date]] = []
                else:
                    old_ranges = _read_ranges(conn, code, period, field, source)
                merged = normalize_ranges(old_ranges + [(d, d) for d in ds])
                _upsert_coverage(conn, code, period, field, merged, source)
                recorded += 1

            internal = _internal_empty_days(observed, source)
            if stamp_changed:
                merged_empty = sorted(set(internal))
            else:
                merged_empty = sorted(set(old_empty) | set(internal))
            _upsert_empty(conn, code, period, merged_empty, source)

            if source_path:
                try:
                    _upsert_stamp(conn, code, period, source, source_path)
                except OSError:
                    pass

    return {"recorded": recorded, "codes": len(groups)}


def record_coverage(code: str, period: str, field: str,
                    ranges: Iterable[Any], source: str = SOURCE_TDX, *,
                    db_path: Optional[str | Path] = None,
                    source_path: Optional[str | Path] = None,
                    merge: bool = True) -> None:
    """直接登记日期段（供测试/批量导入用）。

    ``merge=False`` 时覆盖旧记录；``source_path`` 存在时同时登记指纹，指纹与
    旧登记不一致时整份旧记录作废后重建。
    """
    new_ranges = normalize_ranges(ranges)
    with _connect(Path(db_path) if db_path else None) as conn:
        stamp_changed = False
        if source_path:
            old_stamp = _read_stamp(conn, code, period, source)
            try:
                new_fp = fingerprint(source_path)
            except OSError:
                new_fp = None
            stamp_changed = bool(
                old_stamp and new_fp and (
                    int(old_stamp.get("mtime_ns") or -1) != new_fp["mtime_ns"]
                    or int(old_stamp.get("size") or -1) != new_fp["size"]
                )
            )
        if stamp_changed:
            # 指纹变化：整份旧登记作废，不只是当前 field
            conn.execute(
                "DELETE FROM coverage WHERE code=? AND period=? AND source=?",
                (code, period, source),
            )
            conn.execute(
                "DELETE FROM empty_days WHERE code=? AND period=? AND source=?",
                (code, period, source),
            )
        elif merge:
            old = _read_ranges(conn, code, period, field, source)
            new_ranges = normalize_ranges(old + new_ranges)
        _upsert_coverage(conn, code, period, field, new_ranges, source)
        if source_path:
            _upsert_stamp(conn, code, period, source, source_path)


def record_empty_days(code: str, period: str, days: Iterable[Any],
                      source: str = SOURCE_TDX, *,
                      db_path: Optional[str | Path] = None,
                      merge: bool = True) -> None:
    new_days = _dates(days)
    with _connect(Path(db_path) if db_path else None) as conn:
        if merge:
            new_days = sorted(set(_read_empty(conn, code, period, source)) | set(new_days))
        _upsert_empty(conn, code, period, new_days, source)


def invalidate(code: str, period: str, source: str = SOURCE_TDX, *,
               db_path: Optional[str | Path] = None) -> Dict[str, int]:
    """整份作废某 (code, period, source) 的台账（coverage + empty_days + stamp）。"""
    with _connect(Path(db_path) if db_path else None) as conn:
        cur1 = conn.execute(
            "DELETE FROM coverage WHERE code=? AND period=? AND source=?",
            (code, period, source),
        )
        cur2 = conn.execute(
            "DELETE FROM empty_days WHERE code=? AND period=? AND source=?",
            (code, period, source),
        )
        cur3 = conn.execute(
            "DELETE FROM stamps WHERE code=? AND period=? AND source=?",
            (code, period, source),
        )
        return {
            "coverage": cur1.rowcount,
            "empty_days": cur2.rowcount,
            "stamps": cur3.rowcount,
        }


def invalidate_if_stamp_changed(code: str, period: str,
                                source: str = SOURCE_TDX, *,
                                source_path: Optional[str | Path] = None,
                                db_path: Optional[str | Path] = None) -> bool:
    """指纹与登记不一致时整份作废；返回是否已作废。"""
    rec = load_record(code, period, source=source, db_path=db_path)
    if rec["stamp"] and not _stamp_matches(rec["stamp"], source_path):
        invalidate(code, period, source, db_path=db_path)
        return True
    return False


# ---------------------------------------------------------------------------
# 读/信任判定
# ---------------------------------------------------------------------------

def _empty_record(code: str, period: str, field: str,
                  source: str) -> Dict[str, Any]:
    return {
        "code": code, "period": period, "field": field,
        "source": source, "ranges": [], "ranges_text": [],
        "empty_days": [], "empty_days_text": [], "stamp": None,
        "stamp_valid": False, "updated_at": None,
    }


def load_record(code: str, period: str, source: str = SOURCE_TDX,
                field: str = FIELD_ALL, *,
                db_path: Optional[str | Path] = None,
                source_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """读取单条台账并校验指纹（只读，不建库、不写库）。"""
    with _connect(Path(db_path) if db_path else None, readonly=True) as conn:
        if conn is None:
            return _empty_record(code, period, field, source)
        try:
            ranges = _read_ranges(conn, code, period, field, source)
            empty = _read_empty(conn, code, period, source)
            stamp = _read_stamp(conn, code, period, source)
            row = conn.execute(
                "SELECT updated_at FROM coverage WHERE code=? AND period=? "
                "AND field=? AND source=?",
                (code, period, field, source),
            ).fetchone()
        except sqlite3.Error:
            logger.warning("读取 K 线覆盖台账失败（按无记录处理）", exc_info=True)
            return _empty_record(code, period, field, source)
    return {
        "code": code,
        "period": period,
        "field": field,
        "source": source,
        "ranges": ranges,
        "ranges_text": [[a.isoformat(), b.isoformat()] for a, b in ranges],
        "empty_days": empty,
        "empty_days_text": [d.isoformat() for d in empty],
        "stamp": stamp,
        "stamp_valid": _stamp_matches(stamp, source_path) if stamp else False,
        "updated_at": row["updated_at"] if row else None,
    }


def _valid_empty_days(empty: Sequence[date],
                      ranges: Sequence[Tuple[date, date]],
                      source: str) -> List[date]:
    """过滤掉不应被信任的 empty_days。

    通达信源只认「两段已有数据之间」的缺日；头/尾登记一律忽略，避免把
    未下载误判成停牌。
    """
    if not empty:
        return []
    if source != SOURCE_TDX:
        return sorted(empty)
    out = []
    for d in sorted(set(empty)):
        # 只要存在「早于 d 的覆盖段」和「晚于 d 的覆盖段」，d 就是内部缺日
        has_before = any(a < d for a, _ in ranges)
        has_after = any(b > d for _, b in ranges)
        if has_before and has_after:
            out.append(d)
    return out


def plan_from_ledger(code: str, period: str, start: date, end: date,
                     source: str = SOURCE_TDX, field: str = FIELD_ALL, *,
                     db_path: Optional[str | Path] = None,
                     source_path: Optional[str | Path] = None,
                     local_days: Optional[Iterable[Any]] = None) -> Dict[str, Any]:
    """查询台账并给出补数计划（只读）。

    trusted=True 表示「缺口全部在尾部」或「完全覆盖」，可直接信任台账；
    trusted=False 时必须退回逐日核对。

    ``local_days`` 传入时附带缺口三分类结果。
    """
    start_d, end_d = _as_date(start), _as_date(end)
    if start_d is None or end_d is None or end_d < start_d:
        return {
            "code": code, "period": period, "field": field, "source": source,
            "trusted": False, "reason": "invalid_range",
            "missing_days": [], "tail_gap": False, "stamp_valid": False,
            "ranges": [], "empty_days": [], "expected_days": 0,
            "covered_days": 0,
        }

    rec = load_record(code, period, source=source, field=field,
                      db_path=db_path, source_path=source_path)
    base = {
        "code": code,
        "period": period,
        "field": field,
        "source": source,
        "requested": [start_d.isoformat(), end_d.isoformat()],
        "ranges": rec["ranges_text"],
        "empty_days": rec["empty_days_text"],
        "stamp_valid": rec["stamp_valid"],
        "trusted": False,
        "reason": "no_record",
        "missing_days": [],
        "tail_gap": False,
        "expected_days": 0,
        "covered_days": 0,
    }

    if not rec["ranges"]:
        return base
    if not rec["stamp_valid"]:
        base["reason"] = "stamp_mismatch" if rec["stamp"] else "no_stamp"
        return base

    ranges = rec["ranges"]
    valid_empty = _valid_empty_days(rec["empty_days"], ranges, source)
    expected = trading_days(start_d, end_d)
    known = set()
    covered = 0
    for d in expected:
        if any(a <= d <= b for a, b in ranges):
            known.add(d)
            covered += 1
        elif d in valid_empty:
            known.add(d)

    missing = [d for d in expected if d not in known]
    base.update({
        "empty_days": [d.isoformat() for d in valid_empty],
        "expected_days": len(expected),
        "covered_days": covered,
    })
    if not missing:
        base.update({"trusted": True, "reason": "complete"})
        return base

    # 只信任尾部缺口：所有缺失日都必须晚于最后一个已知日期；否则是头/中缺口
    if not known:
        base["reason"] = "no_coverage"
    else:
        last_known = max(known)
        first_known = min(known)
        if all(d > last_known for d in missing):
            base.update({"trusted": True, "reason": "tail_gap", "tail_gap": True})
        elif all(d < first_known for d in missing):
            base["reason"] = "head_gap"
        else:
            base["reason"] = "middle_gap"

    base["missing_days"] = [d.isoformat() for d in missing]
    if local_days is not None:
        base["gap_classification"] = classify_gaps(
            missing, local_days, source=source, ledger_empty_days=valid_empty,
        )
    return base


# ---------------------------------------------------------------------------
# 缺口三分类
# ---------------------------------------------------------------------------

def classify_gaps(missing_days: Iterable[Any], local_days: Iterable[Any],
                  source: str = SOURCE_TDX, *,
                  ledger_empty_days: Iterable[Any] = ()) -> Dict[str, Any]:
    """把缺日分成 source_empty / not_downloaded / unknown。

    通达信源只有「夹在两段已有数据之间」的缺日可判为 source_empty；头尾缺口
    不得判为无行情（优先 not_downloaded，完全没有本地数据时 unknown）。
    非通达信源默认 unknown，只有台账显式登记为无行情才认 source_empty。
    """
    local = set(_dates(local_days))
    ledger_empty = set(_dates(ledger_empty_days))
    by_day: Dict[str, str] = {}
    counts = {GAP_SOURCE_EMPTY: 0, GAP_NOT_DOWNLOADED: 0, GAP_UNKNOWN: 0}
    for d in _dates(missing_days):
        before = any(x < d for x in local)
        after = any(x > d for x in local)
        internal = before and after
        if d in ledger_empty and (source != SOURCE_TDX or internal):
            cls = GAP_SOURCE_EMPTY
        elif source == SOURCE_TDX and internal:
            cls = GAP_SOURCE_EMPTY
        elif source == SOURCE_TDX and local:
            # 头/尾缺口：更像没下载，不能判停牌
            cls = GAP_NOT_DOWNLOADED
        else:
            cls = GAP_UNKNOWN
        by_day[d.isoformat()] = cls
        counts[cls] += 1
    return {"by_day": by_day, "counts": counts}


# ---------------------------------------------------------------------------
# 上市日裁剪（无数据源 → 默认不启用）
# ---------------------------------------------------------------------------

def clip_request_range(start: date, end: date,
                       listing_date: Optional[date] = None
                       ) -> Tuple[Optional[date], Optional[date]]:
    """按上市日裁剪请求区间。

    项目内没有上市日数据源，调用方不传 ``listing_date`` 时原样返回；即使传入，
    也只做纯日期裁剪，不访问外部接口。若整个区间都在上市前，返回 (None, None)。
    """
    if listing_date is None or listing_date <= start:
        return start, end
    if listing_date > end:
        return None, None
    return listing_date, end


# ---------------------------------------------------------------------------
# 查询（供界面/API 使用，只读）
# ---------------------------------------------------------------------------

def query_records(code: Optional[str] = None, period: Optional[str] = None,
                  source: Optional[str] = None, field: Optional[str] = None,
                  limit: int = 200, *,
                  db_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """只读查询台账记录，供后续界面使用。"""
    target = Path(db_path) if db_path else ledger_path()
    with _connect(target, readonly=True) as conn:
        if conn is None:
            return {
                "ledger_path": str(target), "enabled": is_enabled(),
                "records": [], "count": 0,
            }
        sql = (
            "SELECT c.code, c.period, c.field, c.ranges, c.source, c.updated_at, "
            "s.path, s.mtime_ns, s.size, s.wal_mtime_ns "
            "FROM coverage c LEFT JOIN stamps s "
            "ON c.code=s.code AND c.period=s.period AND c.source=s.source "
            "WHERE 1=1"
        )
        args: List[Any] = []
        if code:
            sql += " AND c.code=?"
            args.append(code)
        if period:
            sql += " AND c.period=?"
            args.append(period)
        if source:
            sql += " AND c.source=?"
            args.append(source)
        if field:
            sql += " AND c.field=?"
            args.append(field)
        sql += " ORDER BY c.updated_at DESC LIMIT ?"
        args.append(max(1, min(int(limit or 200), 2000)))
        rows = conn.execute(sql, args).fetchall()
        # empty_days 按 (code, period, source) 单独查，避免 JOIN 放大
        records = []
        for row in rows:
            d = dict(row)
            d["ranges"] = _loads_ranges(d.get("ranges"))
            d["ranges"] = [[a.isoformat(), b.isoformat()] for a, b in d["ranges"]]
            empty = conn.execute(
                "SELECT days FROM empty_days WHERE code=? AND period=? AND source=?",
                (d["code"], d["period"], d["source"]),
            ).fetchone()
            d["empty_days"] = [x.isoformat() for x in _loads_days(empty["days"])] if empty else []
            stamp = {
                "path": d.pop("path", None),
                "mtime_ns": d.pop("mtime_ns", None),
                "size": d.pop("size", None),
                "wal_mtime_ns": d.pop("wal_mtime_ns", None),
            }
            d["stamp"] = stamp if stamp["path"] else None
            d["stamp_valid"] = _stamp_matches(d["stamp"]) if d["stamp"] else False
            records.append(d)
    return {
        "ledger_path": str(target),
        "enabled": is_enabled(),
        "records": records,
        "count": len(records),
    }


def status(*, db_path: Optional[str | Path] = None) -> Dict[str, Any]:
    """台账状态：是否开启、库路径、记录数、最近更新时间。"""
    target = Path(db_path) if db_path else ledger_path()
    info: Dict[str, Any] = {
        "enabled": is_enabled(),
        "ledger_path": str(target),
        "exists": target.exists(),
        "coverage": 0,
        "empty_days": 0,
        "stamps": 0,
        "latest_updated_at": None,
    }
    if not target.exists():
        return info
    with _connect(target, readonly=True) as conn:
        if conn is None:
            return info
        for table in ("coverage", "empty_days", "stamps"):
            row = conn.execute("SELECT COUNT(*) AS n FROM %s" % table).fetchone()
            info[table] = int(row["n"]) if row else 0
        row = conn.execute(
            "SELECT MAX(updated_at) AS ts FROM coverage"
        ).fetchone()
        info["latest_updated_at"] = row["ts"] if row else None
    return info


# ---------------------------------------------------------------------------
# 通达信源文件路径（指纹用；找不到文件则不登记指纹）
# ---------------------------------------------------------------------------

_PERIOD_FILE_DIR = {
    "1d": ("lday", ".day"),
    "1w": ("lday", ".day"),
    "1mon": ("lday", ".day"),
    "1m": ("minline", ".lc1"),
    "5m": ("fzline", ".lc5"),
    "15m": ("fzline", ".lc5"),
    "30m": ("fzline", ".lc5"),
    "60m": ("fzline", ".lc5"),
    "1h": ("fzline", ".lc5"),
}


def tdx_source_path(code: str, period: str,
                    tdx_root: Optional[str | Path] = None) -> Optional[str]:
    """按通达信目录结构定位来源文件；不存在返回 None。

    路径结构：``<tdx_path>/vipdoc/<sh|sz|bj>/<lday|fzline|minline>/<market><code>.<ext>``。
    无法确定周期/市场时不猜，直接返回 None（此时台账无指纹、不会被信任）。
    """
    period = str(period or "").lower()
    mapping = _PERIOD_FILE_DIR.get(period)
    if mapping is None:
        return None
    if tdx_root is None:
        try:
            from core.config import load_config

            tdx_root = (load_config().get("tdx_path") or "").strip()
        except Exception:
            tdx_root = ""
    if not tdx_root:
        return None
    text = str(code).strip()
    if "." in text:
        num, suffix = text.split(".", 1)
        market = {"SH": "sh", "SZ": "sz", "BJ": "bj"}.get(suffix.upper())
        if market is None:
            return None
    else:
        num = text
        if num.startswith(("6", "5", "9")):
            market = "sh"
        elif num.startswith(("0", "1", "2", "3")):
            market = "sz"
        elif num.startswith(("4", "8")):
            market = "bj"
        else:
            return None
    subdir, ext = mapping
    path = Path(str(tdx_root)) / "vipdoc" / market / subdir / f"{market}{num}{ext}"
    return str(path) if path.exists() else None
