"""分钟数据自有存储（SQLite，WAL）。

口径（设计稿 §4）：
- 主键 ``(code, period, bar_time)``，``bar_time`` 为 ``YYYY-MM-DD HH:MM``；
- 价格为**不复权原始价**；``volume`` 单位为**股**；
- 只写本库，绝不触碰通达信 ``vipdoc``；
- 5m 一律由 1m 聚合（``aggregate_5m``），不双写，避免两套真值。

路径解析优先级：``TQ_MINUTE_STORE_PATH`` 环境变量 > 配置
``live.minute_download.store_path`` > ``<app_base>/data/minute_store.sqlite3``。
相对路径锚定应用根目录，规则与 ``kline_coverage_ledger.ledger_path`` 一致。
"""
from __future__ import annotations

import os
import sqlite3
import threading
from datetime import date, datetime, timedelta
from datetime import time as dt_time
from pathlib import Path
from typing import Iterable, List, Optional

STORE_FILE_NAME = "minute_store.sqlite3"
SCHEMA_VERSION = "1"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS minute_bars (
    code       TEXT NOT NULL,
    period     TEXT NOT NULL,
    bar_time   TEXT NOT NULL,
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL,
    volume     INTEGER,
    amount     REAL,
    source     TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (code, period, bar_time)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS minute_meta (k TEXT PRIMARY KEY, v TEXT);
"""

_LOCK = threading.Lock()


def _app_base_dir() -> Path:
    """相对路径锚定目录，规则与 ``kline_coverage_ledger._app_base_dir`` 一致。"""
    try:
        from core.db import _MAIN_DIR, app_root, use_app_root

        return app_root() if use_app_root() else Path(_MAIN_DIR)
    except Exception:  # pragma: no cover - 仅在异常环境降级
        return Path(__file__).resolve().parent.parent.parent


def default_store_path() -> Path:
    env = os.environ.get("TQ_MINUTE_STORE_PATH")
    base = _app_base_dir()
    if env:
        p = Path(env)
        return p if p.is_absolute() else base / p
    configured = None
    try:
        from core.config import load_config

        cfg = load_config()
        configured = (
            ((cfg.get("live") or {}).get("minute_download") or {})
            .get("store_path")
        )
    except Exception:  # pragma: no cover - 配置加载失败用默认路径
        configured = None
    if configured:
        p = Path(configured)
        return p if p.is_absolute() else base / p
    return base / "data" / STORE_FILE_NAME


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def normalize_bar_time(bar: dict) -> Optional[str]:
    """bar dict → ``YYYY-MM-DD HH:MM``（stime 或 bar_time 两种输入）。"""
    stime = str(bar.get("stime") or "")
    if len(stime) >= 12 and stime[:12].isdigit():
        return "%s-%s-%s %s:%s" % (
            stime[0:4], stime[4:6], stime[6:8], stime[8:10], stime[10:12])
    text = str(bar.get("bar_time") or "")
    if len(text) >= 16:
        return text[:16]
    return None


class MinuteStore:
    """分钟 K 线读写（线程安全：写串行、读各自连接）。"""

    def __init__(self, path: Optional[str | Path] = None,
                 source: str = "tdx7709") -> None:
        self.path = Path(path) if path else default_store_path()
        self.source = str(source or "tdx7709")
        self._schema_ready = False

    # ------------------------------------------------------------------
    # 连接 / schema
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=5.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        if self._schema_ready:
            return
        with _LOCK:
            conn = self._connect()
            try:
                conn.executescript(_SCHEMA)
                conn.execute(
                    "INSERT INTO minute_meta(k, v) VALUES('schema_version', ?) "
                    "ON CONFLICT(k) DO UPDATE SET v=excluded.v",
                    (SCHEMA_VERSION,),
                )
                conn.commit()
            finally:
                conn.close()
        self._schema_ready = True

    # ------------------------------------------------------------------
    # 写
    # ------------------------------------------------------------------

    def upsert_bars(self, code: str, period: str,
                    bars: Iterable[dict]) -> int:
        """幂等写入；返回实际提交的行数。单次调用一个事务。"""
        rows = []
        now = _now_iso()
        code6 = str(code or "").strip()[:6]
        period = str(period or "1m")
        for bar in bars or []:
            bt = normalize_bar_time(bar)
            if not bt:
                continue
            try:
                close = float(bar.get("close"))
            except (TypeError, ValueError):
                continue
            if close <= 0:
                continue
            rows.append((
                code6, period, bt,
                _num(bar.get("open")), _num(bar.get("high")),
                _num(bar.get("low")), close,
                int(_num(bar.get("volume")) or 0),
                _num(bar.get("amount")),
                self.source, now,
            ))
        if not rows:
            return 0
        self.ensure_schema()
        with _LOCK:
            conn = self._connect()
            try:
                conn.executemany(
                    "INSERT INTO minute_bars"
                    "(code, period, bar_time, open, high, low, close, volume,"
                    " amount, source, updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(code, period, bar_time) DO UPDATE SET "
                    "open=excluded.open, high=excluded.high, low=excluded.low, "
                    "close=excluded.close, volume=excluded.volume, "
                    "amount=excluded.amount, source=excluded.source, "
                    "updated_at=excluded.updated_at",
                    rows,
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()
        return len(rows)

    # ------------------------------------------------------------------
    # 读
    # ------------------------------------------------------------------

    def count(self, code: str, period: str = "1m") -> int:
        if not self.path.exists():
            return 0
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(1) AS n FROM minute_bars WHERE code=? AND period=?",
                (str(code)[:6], str(period)),
            ).fetchone()
            return int(row["n"]) if row else 0
        finally:
            conn.close()

    def latest_bar_time(self, code: str, period: str = "1m") -> Optional[str]:
        if not self.path.exists():
            return None
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT MAX(bar_time) AS t FROM minute_bars "
                "WHERE code=? AND period=?",
                (str(code)[:6], str(period)),
            ).fetchone()
            return row["t"] if row and row["t"] else None
        finally:
            conn.close()

    def recent_bars(self, code: str, period: str = "1m",
                    count: int = 10) -> List[dict]:
        if not self.path.exists():
            return []
        n = max(int(count or 0), 1)
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT bar_time, open, high, low, close, volume, amount "
                "FROM minute_bars WHERE code=? AND period=? "
                "ORDER BY bar_time DESC LIMIT ?",
                (str(code)[:6], str(period), n),
            ).fetchall()
        finally:
            conn.close()
        return [_row_to_bar(r) for r in reversed(rows)]

    def bars_between(self, code: str, period: str, start: str,
                     end: str) -> List[dict]:
        """区间读取（含端点，``YYYY-MM-DD HH:MM`` 或 ``YYYY-MM-DD``）。"""
        if not self.path.exists():
            return []
        lo = _range_bound(start, upper=False)
        hi = _range_bound(end, upper=True)
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT bar_time, open, high, low, close, volume, amount "
                "FROM minute_bars WHERE code=? AND period=? "
                "AND bar_time>=? AND bar_time<=? ORDER BY bar_time ASC",
                (str(code)[:6], str(period), lo, hi),
            ).fetchall()
        finally:
            conn.close()
        return [_row_to_bar(r) for r in rows]

    def available_days(self, code: str, period: str = "1m") -> List[date]:
        """已落库的交易日（去重升序）。"""
        if not self.path.exists():
            return []
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT DISTINCT substr(bar_time, 1, 10) AS d FROM minute_bars "
                "WHERE code=? AND period=? ORDER BY d ASC",
                (str(code)[:6], str(period)),
            ).fetchall()
        finally:
            conn.close()
        out: List[date] = []
        for row in rows:
            try:
                out.append(date.fromisoformat(row["d"]))
            except (TypeError, ValueError):
                continue
        return out

    def aggregate_5m(self, code: str, start: str, end: str,
                     min_bars: int = 5) -> List[dict]:
        """由 1m 聚合 5m；只输出完整桶（桶内 1m 根数 >= ``min_bars``）。

        桶锚点按整 5 分钟向下取整；标签为桶内最后一根 1m 的结束时刻。
        """
        bars = self.bars_between(code, "1m", start, end)
        buckets: dict = {}
        for bar in bars:
            try:
                t = datetime.strptime(bar["stime"][:12], "%Y%m%d%H%M")
            except (KeyError, TypeError, ValueError):
                continue
            buckets.setdefault(_bucket_start(t), []).append((t, bar))
        out = []
        for key in sorted(buckets):
            items = sorted(buckets[key], key=lambda x: x[0])
            if len(items) < max(int(min_bars), 1):
                continue
            rows = [b for _, b in items]
            label = key + timedelta(minutes=5)
            out.append({
                "stime": label.strftime("%Y%m%d%H%M%S"),
                "open": rows[0]["open"],
                "high": max(r["high"] for r in rows),
                "low": min(r["low"] for r in rows),
                "close": rows[-1]["close"],
                "volume": sum(int(r["volume"] or 0) for r in rows),
                "amount": sum(float(r["amount"] or 0.0) for r in rows),
            })
        return out


def _bucket_start(t: datetime) -> datetime:
    """1m 标签 → 所属 5m 桶的起点（上午锚 09:30、下午锚 13:00）。

    1m 标签是区间终点：09:35 的 bar 属于 [09:30, 09:35) 桶，故用
    ``(elapsed - 1) // 5`` 而非 ``elapsed // 5``。
    """
    if t.time() <= dt_time(11, 30):
        anchor = t.replace(hour=9, minute=30, second=0, microsecond=0)
    else:
        anchor = t.replace(hour=13, minute=0, second=0, microsecond=0)
    elapsed = int((t - anchor).total_seconds() // 60)
    idx = max((elapsed - 1) // 5, 0)
    return anchor + timedelta(minutes=idx * 5)


def _num(value) -> Optional[float]:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


def _row_to_bar(row: sqlite3.Row) -> dict:
    bt = str(row["bar_time"])
    stime = bt.replace("-", "").replace(" ", "").replace(":", "") + "00"
    return {
        "stime": stime,
        "bar_time": bt,
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
        "volume": row["volume"],
        "amount": row["amount"],
    }


def _range_bound(value: str, *, upper: bool) -> str:
    text = str(value or "").strip()
    if len(text) <= 10:
        return text[:10] + (" 23:59" if upper else " 00:00")
    return text[:16]


__all__ = [
    "MinuteStore", "default_store_path", "normalize_bar_time",
    "STORE_FILE_NAME",
]
