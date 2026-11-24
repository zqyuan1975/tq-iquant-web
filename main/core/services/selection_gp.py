# -*- coding: utf-8 -*-
"""GP16 历史总市值取数与落地（D2 口径）。

三项配套（用户已拍板，不做「价格比近似」、不做「股本 × 收盘价」重建）：
1. **除权断点核对**：GP16 日间跳变超过阈值时，必须能在公司行为事件表找到
   当日除权事件；找不到的记为 ``unexplained``，只报告不改数；
2. **停牌缺值**：GP16 为空且当日落在 ``suspend`` 区间内 → 标记 ``suspended``
   并按 ``end_date`` 顺延，**不做插值**；区间外为空才是真缺口；
3. **事件集指纹**：快照元数据带上现有公司行为指纹（版本 + digest），
   事件集一变即判定旧快照不可比。
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Dict, Iterable, List, Optional

import pandas as pd
from sqlalchemy.orm import Session

from core.engine.corporate_actions import CORPORATE_ACTION_VERSION
from core.models.corporate_action import CorporateAction
from core.models.selection import SelectionGpSnapshot

GP_COLUMN = "GP16"
CODE_COLUMNS = ("Code", "code", "代码", "stock_code")
BREAKPOINT_PCT = 5.0   # 总市值日变化超过 5% 才值得核对除权事件


def _norm_code(raw) -> str:
    return str(raw).strip().upper()


def parse_gp16(df) -> Dict[str, float]:
    """把 TQ 返回表解析成 ``{code: 万元}``；空值跳过，不填 0。"""
    if not isinstance(df, pd.DataFrame) or df.empty:
        return {}
    col = GP_COLUMN if GP_COLUMN in df.columns else None
    if col is None:
        numeric = [c for c in df.columns if c not in CODE_COLUMNS]
        if not numeric:
            return {}
        col = numeric[0]
    code_col = next((c for c in CODE_COLUMNS if c in df.columns), None)
    out: Dict[str, float] = {}
    for idx, row in df.iterrows():
        code = _norm_code(row[code_col] if code_col else idx)
        value = row[col]
        if value is None or (isinstance(value, float) and pd.isna(value)):
            continue
        try:
            out[code] = float(value)
        except (TypeError, ValueError):
            continue
    return out


def ingest_gp16(db: Session, codes: Iterable[str], day: date,
                priority: str = "P2") -> Dict:
    """按日取 GP16 并 upsert；同一 (code, as_of) 只覆盖数值，不新增行。"""
    from core.tq.data import TQData

    codes = [_norm_code(c) for c in (codes or []) if str(c).strip()]
    if not codes:
        return {"requested": 0, "written": 0, "as_of": day.isoformat()}
    df = TQData().get_gpjy_value_by_date(
        codes, day.year, day.strftime("%m%d"), [GP_COLUMN], priority=priority)
    parsed = parse_gp16(df)

    existing = {
        row.code: row for row in db.query(SelectionGpSnapshot).filter(
            SelectionGpSnapshot.as_of == day,
            SelectionGpSnapshot.code.in_(list(parsed))).all()
    } if parsed else {}
    written = 0
    for code, value in parsed.items():
        row = existing.get(code)
        if row is None:
            db.add(SelectionGpSnapshot(code=code, as_of=day,
                                       total_mv_wan=value, source="tq:GP16"))
        else:
            row.total_mv_wan = value
            row.source = "tq:GP16"
        written += 1
    if written:
        db.commit()
    return {
        "requested": len(codes),
        "written": written,
        "as_of": day.isoformat(),
        "corporate": corporate_context(db, as_of=day),
    }


def ingest_row(db: Session, code: str, day: date,
               total_mv_wan: Optional[float],
               source: str = "manual") -> SelectionGpSnapshot:
    """直接写一条（导入器 / 手工补数用）；同一 (code, as_of) 覆盖不刷行。"""
    code = _norm_code(code)
    row = (db.query(SelectionGpSnapshot)
           .filter_by(code=code, as_of=day).first())
    if row is None:
        row = SelectionGpSnapshot(code=code, as_of=day)
        db.add(row)
    row.total_mv_wan = total_mv_wan
    row.source = source
    db.commit()
    db.refresh(row)
    return row


def list_gp16(db: Session, start: Optional[date] = None,
              end: Optional[date] = None,
              codes: Optional[List[str]] = None) -> List[dict]:
    query = db.query(SelectionGpSnapshot)
    if start is not None:
        query = query.filter(SelectionGpSnapshot.as_of >= start)
    if end is not None:
        query = query.filter(SelectionGpSnapshot.as_of <= end)
    if codes:
        query = query.filter(SelectionGpSnapshot.code.in_(
            [_norm_code(c) for c in codes]))
    rows = query.order_by(SelectionGpSnapshot.as_of,
                          SelectionGpSnapshot.code).all()
    return [{
        "code": r.code, "as_of": r.as_of.isoformat(),
        "total_mv_wan": float(r.total_mv_wan) if r.total_mv_wan is not None else None,
        "source": r.source,
    } for r in rows]


def audit_day(db: Session, day: date, threshold_pct: float = BREAKPOINT_PCT
              ) -> Dict:
    """当日数据质量：缺口分类 + 除权断点核对。

    - ``suspended``：当日有 suspend 事件且未复牌，缺值是正常的；
    - ``missing``：既无 GP16 值、又不在停牌区间，属于真缺口；
    - ``aligned``：GP16 跳变 > 阈值且当日确有除权事件；
    - ``unexplained``：跳变 > 阈值但无除权事件，需人工确认（只报告不改数）。
    """
    present = {r.code: float(r.total_mv_wan) for r in
               db.query(SelectionGpSnapshot)
               .filter(SelectionGpSnapshot.as_of == day,
                       SelectionGpSnapshot.total_mv_wan.isnot(None)).all()}

    events = db.query(CorporateAction).filter(
        CorporateAction.ex_date == day).all()
    ex_codes = {_norm_code(e.code) for e in events
                if e.event_type in ("dividend", "split", "rights")}
    suspended = {_norm_code(e.code) for e in events
                 if e.event_type == "suspend"
                 and (e.end_date is None or e.end_date >= day)}

    prev_day = _prev_value_day(db, day)
    prev = {}
    if prev_day is not None:
        prev = {r.code: float(r.total_mv_wan) for r in
                db.query(SelectionGpSnapshot)
                .filter(SelectionGpSnapshot.as_of == prev_day,
                        SelectionGpSnapshot.total_mv_wan.isnot(None)).all()}

    aligned, unexplained = [], []
    for code, value in present.items():
        base = prev.get(code)
        if not base:
            continue
        change = (value / base - 1.0) * 100.0
        if abs(change) < threshold_pct:
            continue
        item = {"code": code, "change_pct": round(change, 2)}
        (aligned if code in ex_codes else unexplained).append(item)

    universe_gap = sorted(set(prev) - set(present) - suspended)
    return {
        "as_of": day.isoformat(),
        "prev_day": prev_day.isoformat() if prev_day else None,
        "covered": len(present),
        "suspended": sorted(suspended),
        "missing": universe_gap,
        "breakpoints": {"aligned": aligned, "unexplained": unexplained},
        "corporate": corporate_context(db, as_of=day),
    }


def corporate_context(db: Session, as_of: Optional[date] = None) -> Dict:
    """复用既有公司行为事件集指纹：``ex_date <= as_of`` 的事件摘要。"""
    from core.services.backtest_service import build_corporate_timeline

    timeline = build_corporate_timeline(db, end=as_of)
    return {"version": CORPORATE_ACTION_VERSION, "digest": timeline.digest()}


def _prev_value_day(db: Session, day: date) -> Optional[date]:
    row = (db.query(SelectionGpSnapshot.as_of)
           .filter(SelectionGpSnapshot.as_of < day)
           .order_by(SelectionGpSnapshot.as_of.desc()).first())
    return row[0] if row else None


def trading_day_candidates(day: date) -> List[date]:
    """GP16 缺值排查用的候选日（含前 3 个自然日，供人工确认是否停牌/假期）。"""
    return [day - timedelta(days=i) for i in range(0, 4)]
