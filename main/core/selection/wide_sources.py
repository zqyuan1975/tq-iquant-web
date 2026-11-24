# -*- coding: utf-8 -*-
"""内置宽库构建（阶段二）· 口径层。

只做三件事：

1. 把平台自有数据源（TQData / ``corporate_actions``）取成规范化行；
2. **只允许 field_dict 已登记的字段**进入宽库——未登记列一律过滤并留痕；
3. 不做落库、不做 PIT 截断、不在事务里做判断。

数据源分工：

* ``universe``：``TQData.get_all_stocks("5")``（全 A 股代码）；
* ``profile``：``get_more_info_batch``（估值/市值）+ ``get_stock_info_batch``（名称）；
* ``fin``：``get_financial_data``（REQUIRED_FN × 报告期，带 tag/announce）；
* ``dividend``：平台 ``corporate_actions``（已由公司行为同步回填，单位元/股）；
* ``gp``：``get_gpjy_value_by_date``（构建日 GP16 总市值，万元）；
* ``sector``：``get_stock_industry``（仅补缺失票的二级行业）；
* ``daily``：``get_history_raw``（不复权日线，回放窗口用）。

外部宽库（``tq_wide.sqlite3``）不再是默认来源；它的导入入口保留为对账/兜底。
"""
from __future__ import annotations

import math
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from core.selection import field_dict
from core.selection.warehouse import REQUIRED_FN

#: more_info 里允许进入宽库的列（均已在 field_dict 登记）。
MORE_INFO_FIELDS: tuple = ("StaticPE_TTM", "PB_MRQ", "Zsz", "Ltsz", "HqDate")
#: 逐日指标里允许进入宽库的列。
GP_FIELDS: tuple = ("GP16",)
#: 日线字段（回放/派生指标）。
DAILY_FIELDS: tuple = ("Open", "High", "Low", "Close", "Volume", "Amount")

_CODE_COLUMNS = ("Code", "code", "CODE")
_DATE_COLUMNS = ("Date", "date", "trade_date")


class WideSourceError(RuntimeError):
    """取数结果不可用（结构不符/字段未登记/源整体失败）。"""


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _clean(value: Any) -> Any:
    """保留字符串日期/枚举，数值统一转 float；空值返回 None。"""
    if value is None or value == "":
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, (datetime, date)):
        return value.strftime("%Y%m%d")
    return _num(value)


def to_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        try:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None


def _day(value: Any) -> str:
    day = to_date(value)
    return day.strftime("%Y%m%d") if day else ""


def registered_columns(source: str) -> set:
    """field_dict 中某来源已登记的原始列名；口径层只允许这些列。"""
    return {
        spec.column for spec in field_dict.FIELD_REGISTRY.values()
        if spec.source == source and not spec.column.startswith("get_")
        and "/" not in spec.column
    }


def _filter_payload(raw: Dict[str, Any], fields: Sequence[str]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key in fields:
        if key in raw and raw[key] not in (None, "", 0, 0.0):
            out[key] = _clean(raw[key])
    return out


# ---------------------------------------------------------------- universe
def fetch_universe(tq=None) -> List[str]:
    """全 A 股代码（去重排序）。取不到即报错，不返回空表冒充成功。"""
    from core.tq.data import TQData

    client = tq or TQData()
    raw = client.get_all_stocks("5") or []
    codes = sorted({str(c).strip().upper() for c in raw if str(c or "").strip()})
    if not codes:
        raise WideSourceError("TQ 未返回股票名单（get_all_stocks market=5）")
    return codes


# ---------------------------------------------------------------- profile
def fetch_profiles(tq, codes: Sequence[str]) -> List[Dict[str, Any]]:
    """估值/市值 + 名称。返回 [{code,name,more,info}]，空值不写入。"""
    if not codes:
        return []
    more = tq.get_more_info_batch(list(codes), list(MORE_INFO_FIELDS)) or {}
    info = tq.get_stock_info_batch(list(codes), ["Name"]) or {}
    out: List[Dict[str, Any]] = []
    for code in codes:
        m_raw = more.get(code) or {}
        i_raw = info.get(code) or {}
        more_vals = _filter_payload(m_raw, MORE_INFO_FIELDS)
        name = None
        for key in ("Name", "name"):
            if i_raw.get(key):
                name = str(i_raw[key]).strip()
                break
        if name is None:
            for key in ("Name", "name"):
                if m_raw.get(key):
                    name = str(m_raw[key]).strip()
                    break
        out.append({
            "code": code,
            "name": name,
            "more": more_vals,
            "info": {"Name": name} if name else {},
        })
    return out


# ---------------------------------------------------------------- financials
def fetch_financials(tq, codes: Sequence[str],
                     start_time: str = "20100101") -> List[Dict[str, Any]]:
    """财报序列 → ``selection_wh_fin`` 行（series 为 dict[str, list]）。"""
    if not codes:
        return []
    allowed = registered_columns(field_dict.SOURCE_FN)
    columns = [c for c in REQUIRED_FN if c in allowed]
    if not columns:
        raise WideSourceError("field_dict 没有可用的 fn 字段")
    frame = tq.get_financial_data(
        list(codes), columns, start_time=start_time, priority="P2")
    if frame is None or getattr(frame, "empty", True):
        return []
    code_col = next((c for c in _CODE_COLUMNS if c in frame.columns), None)
    if code_col is None:
        raise WideSourceError("财报结果缺少 Code 列")
    if "tag_time" not in frame.columns or "announce_time" not in frame.columns:
        raise WideSourceError("财报结果缺少 tag_time/announce_time 列")
    out: List[Dict[str, Any]] = []
    for code, part in frame.groupby(code_col, sort=False):
        part = part.sort_values("tag_time")
        series: Dict[str, list] = {"tag_time": [], "announce_time": []}
        for col in columns:
            series[col] = []
        for _, row in part.iterrows():
            tag = str(row.get("tag_time") or "").strip()
            announce = str(row.get("announce_time") or "").strip()
            if not tag or not announce:
                continue
            series["tag_time"].append(tag)
            series["announce_time"].append(announce)
            for col in columns:
                series[col].append(_num(row.get(col)))
        if not series["tag_time"]:
            continue
        out.append({
            "code": str(code),
            "n_periods": len(series["tag_time"]),
            "last_tag_time": series["tag_time"][-1],
            "last_announce_time": series["announce_time"][-1],
            "series": series,
        })
    return out


# ---------------------------------------------------------------- GP16
def fetch_gp16(tq, codes: Sequence[str], as_of: date) -> Dict[str, Dict[str, float]]:
    """构建日 GP16（总市值，万元）；只有非 0 值进入宽库。"""
    if not codes:
        return {}
    frame = tq.get_gpjy_value_by_date(
        list(codes), as_of.year, "%02d%02d" % (as_of.month, as_of.day),
        list(GP_FIELDS), priority="P2")
    if frame is None or getattr(frame, "empty", True):
        return {}
    code_col = next((c for c in _CODE_COLUMNS if c in frame.columns), None)
    if code_col is None:
        return {}
    out: Dict[str, Dict[str, float]] = {}
    for _, row in frame.iterrows():
        code = str(row.get(code_col) or "")
        vals = {}
        for field in GP_FIELDS:
            val = _num(row.get(field))
            if val not in (None, 0.0):
                vals[field] = val
        if code and vals:
            out[code] = vals
    return out


# ---------------------------------------------------------------- dividend
def fetch_dividends_from_actions(db, codes: Sequence[str],
                                 as_of: date) -> List[Dict[str, Any]]:
    """从 ``corporate_actions`` 聚合现金分红 → ``selection_wh_dividend`` 行。

    与外部宽库口径一致：``bonus`` 为每股税前现金（元/股），按年聚合时
    只保留年度合计 > 0 的年份；无事件代码不生成行（与导入仓一致）。
    """
    if not codes:
        return []
    from core.models import CorporateAction

    wanted = list(codes)
    events: Dict[str, Dict[int, float]] = {}
    for i in range(0, len(wanted), 400):
        part = wanted[i:i + 400]
        rows = (db.query(CorporateAction.code, CorporateAction.ex_date,
                         CorporateAction.cash_per_share)
                .filter(CorporateAction.code.in_(part),
                        CorporateAction.event_type == "dividend",
                        CorporateAction.cash_per_share.isnot(None),
                        CorporateAction.ex_date <= as_of)
                .order_by(CorporateAction.code, CorporateAction.ex_date).all())
        for code, ex_date, cash in rows:
            value = _num(cash)
            if value is None or value <= 0 or ex_date is None:
                continue
            events.setdefault(str(code), {})[ex_date.year] = (
                events.get(str(code), {}).get(ex_date.year, 0.0) + value)
    out: List[Dict[str, Any]] = []
    for code, by_year in events.items():
        # 行格式沿用导入仓：[ex_date, year, bonus]；同一年用该年末代表日，
        # 与外部宽库「年度聚合」口径一致。
        rows = []
        for year in sorted(by_year):
            total = by_year[year]
            if total <= 0:
                continue
            rows.append(["%d1231" % year, year, round(total, 6)])
        if not rows:
            continue
        out.append({
            "code": code,
            "n_events": len(rows),
            "last_ex_date": rows[-1][0],
            "rows": rows,
        })
    return out


# ---------------------------------------------------------------- sector
def fetch_sector(tq, codes: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    """逐票二级行业（仅用于新票补缺；全量刷新不重复打 TQ）。"""
    out: Dict[str, Dict[str, Any]] = {}
    for code in codes:
        try:
            item = tq.get_stock_industry(code)
        except Exception:  # noqa: BLE001 — 单票行业失败不阻断构建
            continue
        if item:
            out[code] = {
                "sector_code": item.get("code"),
                "sector_name": item.get("name"),
            }
    return out


# ---------------------------------------------------------------- daily
def fetch_daily(tq, codes: Sequence[str], start: date,
                end: date) -> List[Dict[str, Any]]:
    """不复权日线 → ``selection_wh_daily`` 行；无数据的票跳过。"""
    if not codes or start > end:
        return []
    raw = tq.get_history_raw(
        stocks=list(codes), periods=["1d"],
        start=start.strftime("%Y%m%d"), end=end.strftime("%Y%m%d"),
        dividend_type="none", count=-1, priority="P2")
    bucket = (raw or {}).get("1d") or {}
    close_df = bucket.get("Close")
    if close_df is None or getattr(close_df, "empty", True):
        return []
    dates = [to_date(idx) for idx in close_df.index]
    frames = {field: bucket.get(field) for field in DAILY_FIELDS
              if bucket.get(field) is not None}
    out: List[Dict[str, Any]] = []
    for code in codes:
        if code not in close_df.columns:
            continue
        series = {field: frame[code] for field, frame in frames.items()
                  if code in getattr(frame, "columns", [])}
        if "Close" not in series:
            continue
        for pos, trade_date in enumerate(dates):
            if trade_date is None:
                continue
            close = _num(series["Close"].iloc[pos])
            if close is None or close <= 0:
                continue
            row = {
                "code": code,
                "trade_date": trade_date,
                "open": _num(series["Open"].iloc[pos]) if "Open" in series else None,
                "high": _num(series["High"].iloc[pos]) if "High" in series else None,
                "low": _num(series["Low"].iloc[pos]) if "Low" in series else None,
                "close": close,
                "volume": _num(series["Volume"].iloc[pos]) if "Volume" in series else None,
                "amount": _num(series["Amount"].iloc[pos]) if "Amount" in series else None,
                "source": "tq:history",
            }
            out.append(row)
    return out
