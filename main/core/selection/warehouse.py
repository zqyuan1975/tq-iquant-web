# -*- coding: utf-8 -*-
"""宽库 → 平台导入仓（D1：宽库只做一次性导入源）。

运行期（选股引擎）只读 ``selection_wh_*`` 四张平台表，不打开外部宽库。
导入可重跑：每次导入在同一事务里整表替换快照，重复执行同一 ``as_of``
得到同样结果；外部宽库结构变化只影响本模块。

口径与披露：
- 财报序列保留 ``tag_time`` / ``announce_time``，供运行期做 PIT 过滤；
- 分红事件 ``bonus`` 是元/股（税前），已用 600519 实测核对；
- K 线派生只用不复权收盘（与外部方案 v4 一致），计算导入日前 252 个
  交易日的波动率与最大回撤，不落 180 万行原始 K 线；
- ``is_financial`` 在外部宽库全为 0（未标定），导入时按板块标签
  （``金融单列`` / ``金融地产``）补一个保守判断，并在 counts 里披露。
"""
from __future__ import annotations

import json
import math
import os
import statistics
import zlib
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

from sqlalchemy import delete, insert

from core.models.selection import (
    SelectionFactorValidation,
    SelectionGpSnapshot,
    SelectionWarehouseDividend,
    SelectionWarehouseFin,
    SelectionWarehouseMeta,
    SelectionWarehouseStock,
)
from core.selection import validation, wide_db

# 引擎会用到、导入时固定搬运的 FN 列（外部方案 v4 实际调用的集合）
REQUIRED_FN = [
    "FN197",  # 年化 ROE %
    "FN336",  # 审计意见
    "FN96",   # 归母净利（年）
    "FN307",  # 经营现金流（年）
    "FN319",  # 营业收入（年）
    "FN107",  # 经营现金流（期）
    "FN114",  # 资本开支（期）
    "FN125",  # 归母净利（期）
    "FN232",  # 归母净利（季）
    "FN230",  # 营业收入（季）
    "FN184",  # 营收同比
    "FN285",  # 归母净利同比
    "FN329",  # ROIC
    "FN202",  # 毛利率
    "FN228",  # 应计/净利（盈利质量）
    "FN170",  # 流动比率
    "FN210",  # 资产负债率
    "FN327",  # 有息负债率
    "FN337",  # 分红率
    "FN238",  # 总股本（备用）
    "FN239",  # 流通股本（备用）
]

FIN_COLUMNS = ["code", "tag_time", "announce_time"] + REQUIRED_FN


class WarehouseError(RuntimeError):
    """导入失败：源库缺失/不可读/结构不符。"""


def _decode_blob(blob: Optional[bytes]) -> Dict[str, Any]:
    if not blob:
        return {}
    try:
        return json.loads(zlib.decompress(blob).decode("utf-8"))
    except Exception:
        try:
            return json.loads(blob.decode("utf-8"))
        except Exception:
            return {}


def _encode_json(obj: Any) -> bytes:
    return zlib.compress(
        json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8"),
        level=6,
    )


def _common_asof(conn) -> str:
    """选一个「成分完整 + 估值/一致预期都在」的快照日。

    宽库可能存在当天只导了 12 只的进行中快照（universe 有 20261114 的残片），
    因此按 universe 行数取最大，而不是简单取最大日期。
    """
    uni = {str(r[0]): int(r[1]) for r in conn.execute(
        "SELECT asof, COUNT(*) FROM universe GROUP BY asof") if r[0]}
    if not uni:
        raise WarehouseError("宽库没有 universe 数据")
    gp = {str(r[0]) for r in conn.execute("SELECT DISTINCT asof FROM gp_snapshot")}
    go = {str(r[0]) for r in conn.execute("SELECT DISTINCT asof FROM go_snapshot")}
    valid = [d for d in uni if (not gp or d in gp) and (not go or d in go)]
    if not valid:
        valid = list(uni)
    return max(valid, key=lambda d: (uni[d], d))


def _load_stocks(conn, asof: str) -> List[Dict[str, Any]]:
    rows: Dict[str, Dict[str, Any]] = {}
    for code, name, exchange, scode, sname, pool, tags, is_fin in conn.execute(
        "SELECT u.code, u.name, u.exchange, s.sector_code, s.sector_name, "
        "s.pool, s.tags, s.is_financial FROM universe u "
        "LEFT JOIN sector_map s ON s.asof=u.asof AND s.code=u.code "
        "WHERE u.asof=?",
        (asof,),
    ):
        tags = tags or ""
        financial = bool(is_fin) or ("金融单列" in tags) or (pool or "") == "金融地产"
        rows[code] = {
            "code": code,
            "name": name,
            "exchange": exchange,
            "sector_code": scode,
            "sector_name": sname,
            "pool": pool,
            "tags": tags,
            "is_financial": financial,
            "more_blob": None,
            "info_blob": None,
            "go_blob": None,
            "gp_blob": None,
            "close": None,
            "volatility_pct": None,
            "max_drawdown_pct": None,
            "fin_periods": 0,
            "fin_last_tag": None,
        }
    if not rows:
        raise WarehouseError(f"宽库 universe 在 {asof} 无数据")
    return list(rows.values())


def _load_payloads(conn, asof: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    by_code = {r["code"]: r for r in rows}
    counts = {"more_info": 0, "stock_info": 0}
    blob_key = {"more_info": "more_blob", "stock_info": "info_blob"}
    for code, family, payload in conn.execute(
        "SELECT code, family, payload FROM snapshot_json "
        "WHERE asof=? AND family IN ('more_info','stock_info')",
        (asof,),
    ):
        rec = by_code.get(code)
        if not rec or not payload:
            continue
        rec[blob_key[family]] = payload
        counts[family] += 1
    return counts


def _load_go_gp(conn, asof: str, rows: List[Dict[str, Any]]) -> Dict[str, int]:
    by_code = {r["code"]: r for r in rows}
    counts = {"go": 0, "gp": 0}
    for key, table, blob_key, prefix in (
        ("go", "go_snapshot", "go_blob", "GO"),
        ("gp", "gp_snapshot", "gp_blob", "GP"),
    ):
        cur = conn.execute(
            "SELECT * FROM %s WHERE asof=?" % table, (asof,)
        )
        names = [d[0] for d in cur.description]
        for row in cur:
            rec = by_code.get(row[0])
            if not rec:
                continue
            vals = {}
            for name, val in zip(names, row):
                if not name.startswith(prefix):
                    continue
                if val in (None, 0, 0.0, ""):
                    continue
                vals[name] = val
            # 只有真正有值的行才计入覆盖：全 0 行 = 该票无此快照数据，
            # 计入会把覆盖率披露虚高（GO 实测仅约 1/4 个股有值）。
            if not vals:
                continue
            rec[blob_key] = _encode_json(vals)
            counts[key] += 1
    return counts


def _load_fin(conn, asof: str) -> List[Dict[str, Any]]:
    cols = ", ".join(FIN_COLUMNS)
    sql = (
        "SELECT %s FROM fin_wide WHERE tag_time<>'' AND announce_time<>'' "
        "AND announce_time<=? ORDER BY code, tag_time" % cols
    )
    out: List[Dict[str, Any]] = []
    cur_code: Optional[str] = None
    series: Dict[str, list] = {}

    def flush(code: Optional[str], buf: Dict[str, list]) -> None:
        if not code or not buf.get("tag_time"):
            return
        out.append({
            "code": code,
            "n_periods": len(buf["tag_time"]),
            "last_tag_time": buf["tag_time"][-1],
            "last_announce_time": buf["announce_time"][-1],
            "series_blob": _encode_json(buf),
        })

    for row in conn.execute(sql, (asof,)):
        code = row[0]
        if code != cur_code:
            flush(cur_code, series)
            cur_code = code
            series = {"tag_time": [], "announce_time": []}
            for fn in REQUIRED_FN:
                series[fn] = []
        series["tag_time"].append(str(row[1]))
        series["announce_time"].append(str(row[2]))
        for idx, fn in enumerate(REQUIRED_FN, start=3):
            series[fn].append(row[idx])
    flush(cur_code, series)
    return out


def _load_dividends(conn, asof: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    cur_code: Optional[str] = None
    events: List[list] = []

    def flush(code: Optional[str], buf: List[list]) -> None:
        if not code or not buf:
            return
        out.append({
            "code": code,
            "n_events": len(buf),
            "last_ex_date": str(buf[-1][0]),
            "rows_blob": _encode_json(buf),
        })

    for code, ex_date, year, bonus in conn.execute(
        "SELECT code, ex_date, year, bonus FROM dividend_event "
        "WHERE ex_date<=? ORDER BY code, ex_date",
        (asof,),
    ):
        if code != cur_code:
            flush(cur_code, events)
            cur_code = code
            events = []
        if bonus is None:
            continue
        events.append([str(ex_date), year, float(bonus)])
    flush(cur_code, events)
    return out


def _load_kline_metrics(conn, asof: str) -> Dict[str, tuple]:
    """code → (close, volatility_pct, max_drawdown_pct)，只用不复权收盘。"""
    out: Dict[str, tuple] = {}
    cur_code: Optional[str] = None
    closes: List[float] = []

    def flush(code: Optional[str], buf: List[float]) -> None:
        if not code or len(buf) < 2:
            return
        window = buf[-252:]
        rets = [
            window[i] / window[i - 1] - 1.0
            for i in range(1, len(window))
            if window[i - 1]
        ]
        vol = None
        if len(rets) >= 40:
            vol = statistics.pstdev(rets) * math.sqrt(250) * 100.0
        peak = window[0]
        mdd = 0.0
        for val in window:
            peak = max(peak, val)
            if peak:
                mdd = min(mdd, val / peak - 1.0)
        out[code] = (window[-1], vol, mdd * 100.0)

    for code, close in conn.execute(
        "SELECT code, close FROM kline_daily WHERE trade_date<=? "
        "ORDER BY code, trade_date",
        (asof,),
    ):
        if code != cur_code:
            flush(cur_code, closes)
            cur_code = code
            closes = []
        if close is None or close <= 0:
            continue
        closes.append(float(close))
    flush(cur_code, closes)
    return out


def _load_gp16(conn, asof: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for code, val in conn.execute(
        "SELECT code, GP16 FROM gp_snapshot WHERE asof=?", (asof,)
    ):
        if val not in (None, 0, 0.0):
            out[code] = float(val)
    return out


def _insert_chunks(db, model, rows: List[Dict[str, Any]], size: int = 400) -> None:
    for i in range(0, len(rows), size):
        db.execute(insert(model), rows[i:i + size])


def import_warehouse(db, path: Optional[str] = None,
                     as_of: Optional[date] = None) -> Dict[str, Any]:
    """把宽库当前快照导入平台仓（整表替换，可重跑）。

    返回 ``{as_of, source_path, counts, imported_at}``。
    """
    src = path or wide_db.resolve_wide_db_path()
    if not os.path.exists(src):
        raise WarehouseError("宽库文件不存在：%s" % src)
    try:
        conn_ctx = wide_db.open_wide_db(src)
        conn = conn_ctx.__enter__()
    except Exception as exc:  # pragma: no cover - 依赖文件系统
        raise WarehouseError("宽库无法只读打开：%s" % exc)

    try:
        asof_s = as_of.strftime("%Y%m%d") if as_of else _common_asof(conn)
        asof_date = date(int(asof_s[:4]), int(asof_s[4:6]), int(asof_s[6:8]))
        stocks = _load_stocks(conn, asof_s)
        payload_counts = _load_payloads(conn, asof_s, stocks)
        og_counts = _load_go_gp(conn, asof_s, stocks)
        fin_rows = _load_fin(conn, asof_s)
        div_rows = _load_dividends(conn, asof_s)
        kline = _load_kline_metrics(conn, asof_s)
        gp16 = _load_gp16(conn, asof_s)
        try:
            validation_payload = validation.compute(conn, asof_date)
        except Exception as exc:  # pragma: no cover - 验证失败不能拖垮导入
            validation_payload = {
                "available": False, "reason": "验证计算失败：%s" % exc}
    finally:
        conn_ctx.__exit__(None, None, None)

    fin_by_code = {r["code"]: r for r in fin_rows}
    for rec in stocks:
        rec["fin_periods"] = fin_by_code.get(rec["code"], {}).get("n_periods", 0)
        rec["fin_last_tag"] = fin_by_code.get(rec["code"], {}).get("last_tag_time")
        metrics = kline.get(rec["code"])
        if metrics:
            close, vol, mdd = metrics
            rec["close"] = round(close, 4)
            rec["volatility_pct"] = round(vol, 4) if vol is not None else None
            rec["max_drawdown_pct"] = round(mdd, 4) if mdd is not None else None

    counts = {
        "universe": len(stocks),
        "fin": len(fin_rows),
        "dividend": len(div_rows),
        "kline": len(kline),
        "gp16": len(gp16),
        "more_info": payload_counts["more_info"],
        "stock_info": payload_counts["stock_info"],
        "go": og_counts["go"],
        "gp": og_counts["gp"],
        "sector_missing": sum(1 for r in stocks if not r["sector_name"]),
        "is_financial_marked": sum(1 for r in stocks if r["is_financial"]),
        "fin_history_lt8": sum(1 for r in stocks if r["fin_periods"] < 8),
        "validation_available": bool(validation_payload.get("available")),
        "validation_periods": int(validation_payload.get("n_periods") or 0),
        "notes": [
            "宽库 is_financial 全为 0（未标定），金融股按板块标签保守识别",
            "K 线派生使用不复权收盘，窗口为导入日前 252 个交易日",
            "GO/GP 为导入日单时点快照，非历史序列",
            "因子稳定性验证在导入时回算，估值类因子无历史快照不可验证",
        ],
    }

    imported_at = datetime.now()
    meta_row = {
        "id": 1,
        "as_of": date(int(asof_s[:4]), int(asof_s[4:6]), int(asof_s[6:8])),
        "source_path": src,
        "source_size_mb": round(os.path.getsize(src) / 1024 / 1024, 1),
        "imported_at": imported_at,
        "stock_count": len(stocks),
        "fin_count": len(fin_rows),
        "dividend_count": len(div_rows),
        "counts_json": json.dumps(counts, ensure_ascii=False),
    }

    # 用当前 session 的事务提交：调用方（API/脚本）可能已开事务，
    # ``with db.begin()`` 会因「事务已存在」直接报错。这里整块提交，
    # 失败整体回滚，保证「整表替换」的原子性不被破坏。
    try:
        db.execute(delete(SelectionWarehouseStock))
        db.execute(delete(SelectionWarehouseFin))
        db.execute(delete(SelectionWarehouseDividend))
        db.execute(delete(SelectionWarehouseMeta))
        db.execute(delete(SelectionFactorValidation))
        db.execute(delete(SelectionGpSnapshot).where(
            SelectionGpSnapshot.as_of == meta_row["as_of"]))
        _insert_chunks(db, SelectionWarehouseStock, stocks)
        _insert_chunks(db, SelectionWarehouseFin, fin_rows)
        _insert_chunks(db, SelectionWarehouseDividend, div_rows)
        _insert_chunks(db, SelectionWarehouseMeta, [meta_row])
        db.add(SelectionFactorValidation(
            as_of=meta_row["as_of"],
            horizon_days=int(validation_payload.get("horizon_days") or 20),
            step_days=int(validation_payload.get("step_days") or 21),
            n_periods=int(validation_payload.get("n_periods") or 0),
            payload_json=json.dumps(validation_payload, ensure_ascii=False),
        ))
        if gp16:
            db.execute(insert(SelectionGpSnapshot), [
                {"code": code, "as_of": meta_row["as_of"],
                 "total_mv_wan": val, "source": "wide_db:GP16"}
                for code, val in gp16.items()
            ])
        db.commit()
    except Exception:
        db.rollback()
        raise

    return {
        "as_of": meta_row["as_of"].isoformat(),
        "source_path": src,
        "counts": counts,
        "imported_at": imported_at.isoformat(timespec="seconds"),
    }
