"""J6：回测 K 线本地 parquet 缓存（默认关闭，开启后对回测透明）。

背景：每次回测都对股票池全量 ``get_market_data`` 拉历史日线/分钟线（W-A 后含
公式预热段），同一区间重复跑（参数试验、双跑校验、失败重跑）反复打满通达信
取数链路。这里按 ``(code, period, 复权, 区间)`` 把**原始行情**落到 parquet，
命中直接重建 ``TQData.get_history_raw`` 的返回结构，取数/转换代码零改动。

失效策略（与区间是否含当日相关）：
- 闭区间（``end < 今天``）：行情已定型，长期有效，不设过期；
- 含当日（``end >= 今天``）：当日数据仍在变化，按 ``today_ttl_seconds``
  （默认 300 秒）按文件 mtime 失效，过期即重拉并覆盖。

安全边界：
- 任何读写失败只记 warning 并视为未命中（回测回退 TQ 实时取数，绝不让缓存
  故障中断回测）；
- 写文件走临时文件 + ``os.replace`` 原子替换，多回测线程并发写同一 key 不会
  读到半截文件；
- 值按字符串原样保存（TQ 真机 Volume/Amount 可能是 str、价格为 Decimal），
  重建后仍由既有 ``_to_decimal`` / ``_to_int`` 规整，口径与不缓存逐字一致。

配置（``config.yaml``，缺省关闭 = 行为与接入前完全一致）::

    backtest:
      kline_cache:
        enabled: false
        dir: data/kline_cache
        today_ttl_seconds: 300
"""
from __future__ import annotations

import hashlib
import logging
import os
import threading
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd
import polars as pl

from ..config import load_config

logger = logging.getLogger(__name__)

FIELDS = ("Open", "High", "Low", "Close", "Volume", "Amount")

# 临时文件后缀：同 key 并发写互不覆盖（进程内线程 + 进程名隔离由 pid 提供）。
_TMP_SUFFIX = ".tmp"


class KlineCache:
    """原始行情 parquet 缓存：``(code, period, dividend, start, end)`` → 单文件。"""

    def __init__(
        self,
        base_dir: str,
        *,
        today_ttl_seconds: float = 300.0,
        clock=None,
    ) -> None:
        self.base_dir = Path(base_dir)
        self.today_ttl_seconds = max(0.0, float(today_ttl_seconds))
        self._clock = clock or datetime.now
        self._lock = threading.Lock()

    @classmethod
    def from_config(cls, cfg: Optional[dict] = None) -> Optional["KlineCache"]:
        """读配置构造；未启用/配置异常 → None（调用方按无缓存处理）。"""
        if cfg is None:
            try:
                cfg = load_config()
            except Exception:  # noqa: BLE001 — 配置读取失败按无缓存
                logger.warning("kline cache: 配置读取失败，按未启用处理", exc_info=True)
                return None
        section = ((cfg or {}).get("backtest") or {}).get("kline_cache") or {}
        if not isinstance(section, dict) or not bool(section.get("enabled", False)):
            return None
        base_dir = str(section.get("dir") or "data/kline_cache")
        try:
            ttl = float(section.get("today_ttl_seconds", 300))
        except (TypeError, ValueError):
            ttl = 300.0
        return cls(base_dir, today_ttl_seconds=ttl)

    # ---------------- 取数入口 ----------------
    def load_period(
        self,
        stocks: list,
        period: str,
        start: str,
        end: str,
        dividend_type: str,
    ) -> Optional[dict]:
        """读一个周期的全部股票；任一股票缺失/过期/损坏 → None（该周期整体重拉）。

        返回结构与 ``TQData.get_history_raw`` 的单个周期一致：
        ``{field: pandas.DataFrame(index=时间戳, columns=股票代码)}``。
        """
        frames: Dict[str, pl.DataFrame] = {}
        for code in stocks:
            frame = self._read(code, period, start, end, dividend_type)
            if frame is None or frame.height == 0:
                return None
            frames[code] = frame
        if not frames:
            return None
        return _frames_to_raw(frames)

    def store_period(
        self,
        raw: dict,
        stocks: list,
        period: str,
        start: str,
        end: str,
        dividend_type: str,
    ) -> None:
        """把一个周期的原始行情按股票拆写 parquet（失败只告警）。"""
        close_df = (raw or {}).get("Close")
        if close_df is None or len(getattr(close_df, "index", [])) == 0:
            return
        timestamps = list(close_df.index)
        if not timestamps:
            return
        dt_values = [_to_datetime(ts) for ts in timestamps]
        for code in stocks:
            columns = {"dt": dt_values}
            usable = True
            for field in FIELDS:
                frame = raw.get(field)
                if frame is None or code not in getattr(frame, "columns", []):
                    usable = False
                    break
                columns[field] = [_cell(v) for v in frame.loc[timestamps, code].tolist()]
            if not usable:
                continue
            self._write(code, period, start, end, dividend_type,
                        pl.DataFrame(columns, schema_overrides={"dt": pl.Datetime("us")}))

    # ---------------- 内部：路径/有效期 ----------------
    def _key(self, code: str, period: str, start: str, end: str,
             dividend_type: str) -> str:
        raw = "|".join([str(code), str(period), str(dividend_type),
                        str(start), str(end)])
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    def _path(self, code: str, period: str, start: str, end: str,
              dividend_type: str) -> Path:
        return self.base_dir / (self._key(code, period, start, end,
                                          dividend_type) + ".parquet")

    def _fresh(self, path: Path, end: str) -> bool:
        try:
            mtime = path.stat().st_mtime
        except OSError:
            return False
        if not self._includes_today(end):
            return True
        return (self._clock().timestamp() - mtime) <= self.today_ttl_seconds

    def _includes_today(self, end: str) -> bool:
        """区间是否含当日；无法解析 end 时按含当日处理（保守走 TTL）。"""
        try:
            end_date = datetime.strptime(str(end), "%Y%m%d").date()
        except (TypeError, ValueError):
            return True
        return end_date >= self._clock().date()

    # ---------------- 内部：读写 ----------------
    def _read(self, code: str, period: str, start: str, end: str,
              dividend_type: str) -> Optional[pl.DataFrame]:
        path = self._path(code, period, start, end, dividend_type)
        if not path.is_file() or not self._fresh(path, end):
            return None
        try:
            frame = pl.read_parquet(path)
        except Exception:  # noqa: BLE001 — 损坏/版本不符按未命中
            logger.warning("kline cache: 读取失败 %s", path, exc_info=True)
            return None
        if "dt" not in frame.columns or any(f not in frame.columns for f in FIELDS):
            return None
        return frame

    def _write(self, code: str, period: str, start: str, end: str,
               dividend_type: str, frame: pl.DataFrame) -> None:
        path = self._path(code, period, start, end, dividend_type)
        tmp = path.with_name(
            "%s.%d%s" % (path.name, os.getpid(), _TMP_SUFFIX))
        try:
            with self._lock:
                path.parent.mkdir(parents=True, exist_ok=True)
                frame.write_parquet(tmp)
                os.replace(tmp, path)
        except Exception:  # noqa: BLE001 — 缓存写失败不影响回测
            logger.warning("kline cache: 写入失败 %s", path, exc_info=True)
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass


def _to_datetime(ts) -> datetime:
    """pandas 索引时间戳 → naive datetime（与 TQ 原始索引口径一致）。"""
    try:
        return ts.to_pydatetime().replace(tzinfo=None)
    except AttributeError:
        return ts


def _cell(value) -> Optional[str]:
    """单个行情值 → 字符串（None/NaN → None），保留 TQ 原始数值表示。"""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return None if not text or text.lower() in ("nan", "none", "null") else value
    try:
        if value != value:  # NaN
            return None
    except Exception:  # noqa: BLE001
        pass
    return str(value)


def _frames_to_raw(frames: Dict[str, pl.DataFrame]) -> dict:
    """{code: parquet frame} → TQ 原始结构 {field: DataFrame(index, columns)}。"""
    all_dt = sorted({d for frame in frames.values() for d in frame["dt"].to_list()})
    index = pd.to_datetime(all_dt)
    raw: dict = {}
    for field in FIELDS:
        data = {}
        for code, frame in frames.items():
            series = pd.Series(
                frame[field].to_list(), index=pd.to_datetime(frame["dt"].to_list()))
            data[code] = series.reindex(index)
        raw[field] = pd.DataFrame(data, index=index)
    return raw
