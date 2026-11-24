# -*- coding: utf-8 -*-
"""池子有效性（召回）验证。

口径（用户 2026-11-24 校正）：池只是**候选域**，最终买什么由导入公式择时决定。
因此用「公式在全市场入选集合 vs 公式在池内入选集合」的召回率衡量池子，而不是
用横截面 rank IC 衡量池内排序——IC 只对「出池即全买」的组合有意义。

本模块只做读取与计算：
- 公式求值走平台既有 ``core.tq.formula.TQFormula``（按名称调用 TQ/TDX 公式），
  不新增交易/下单路径；
- 漏票明细复用选股快照里已落库的分位与淘汰原因，不重新打分；
- 事后 N 日收益用 TQ 不复权日线只读回补，取不到时显式标注缺失。
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from core.models.formula import Formula
from core.models.formula_signal import FormulaSignal
from core.models.selection import (
    SelectionPoolRecall, SelectionSnapshot, SelectionSnapshotStock,
    SelectionWarehouseStock,
)

OPEN = "OPEN"


class RecallError(ValueError):
    """召回验证参数不合法（API 层转 400）。"""


def list_formulas(db) -> List[Dict[str, Any]]:
    """列出可做择时验证的公式及其实效信号线。"""
    rows = db.query(Formula).order_by(Formula.id).all()
    out = []
    for f in rows:
        sigs = (db.query(FormulaSignal)
                .filter_by(formula_id=f.id).order_by(FormulaSignal.id).all())
        signals = [{
            "signal_name": s.signal_name,
            "signal_type": s.signal_type,
            "trigger_value": s.trigger_value,
        } for s in sigs]
        out.append({
            "formula_id": f.id,
            "name": f.name,
            "engine": f.engine,
            "signals": signals,
            "open_signals": [s["signal_name"] for s in signals
                             if (s["signal_type"] or "").upper() == OPEN],
        })
    return out


def _fmt_day(day: date) -> str:
    return day.strftime("%Y%m%d")


def _line_value(series, day: date) -> Optional[float]:
    """取公式输出序列在 day（含）之前的最后一个值。"""
    last = None
    for item in series or []:
        if not isinstance(item, dict):
            continue
        d = item.get("Date") or item.get("date")
        if d is None:
            continue
        ds = str(d)[:10].replace("-", "")
        if ds > _fmt_day(day):
            continue
        v = item.get("Value")
        if v is None:
            continue
        try:
            last = float(v)
        except (TypeError, ValueError):
            continue
    return last


def _evaluate(db, formula: Formula, open_signals: Dict[str, float],
              codes: List[str], as_of: date, warmup_days: int = 300,
              chunk: int = 80) -> Dict[str, Dict[str, Any]]:
    """逐批调用公式，返回 {code: {selected, lines}}；只保留 as_of 当日值。"""
    from core.tq.formula import TQFormula

    if not open_signals:
        raise RecallError("公式 %s 没有 OPEN 信号线，无法做择时验证" % formula.name)
    start = _fmt_day(as_of - timedelta(days=warmup_days))
    end = _fmt_day(as_of)
    api = TQFormula()
    out: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(codes), chunk):
        part = codes[i:i + chunk]
        try:
            raw = api.compute(formula.name, "", part, period="1d", count=-1,
                              dividend_type=0, start_time=start, end_time=end)
        except Exception as exc:  # 公式求值失败整批跳过并留痕
            for code in part:
                out[code] = {"selected": None, "error": str(exc), "lines": {}}
            continue
        for code in part:
            series_by_line = raw.get(code) or {}
            lines = {}
            hit = False
            seen = False
            for name, trigger in open_signals.items():
                val = _line_value(series_by_line.get(name), as_of)
                if val is None:
                    continue
                seen = True
                lines[name] = val
                if abs(val - trigger) < 1e-6:
                    hit = True
            out[code] = {
                "selected": hit if seen else None,
                "lines": lines,
            }
    return out


def _forward_return(codes: List[str], as_of: date,
                    horizon_days: int) -> Dict[str, Optional[float]]:
    """TQ 不复权日线：as_of 起第 N 个交易日相对 as_of 收盘的收益。取不到给 None。"""
    if not codes:
        return {}
    try:
        from core.tq.data import TQData
        raw = TQData().get_history_raw(
            stocks=codes, periods=["1d"], start=_fmt_day(as_of),
            end=_fmt_day(as_of + timedelta(days=horizon_days * 3 + 10)),
            dividend_type="none", count=-1)
        close = (raw.get("1d") or {}).get("Close")
        if close is None:
            return {c: None for c in codes}
        out: Dict[str, Optional[float]] = {}
        for code in codes:
            if code not in getattr(close, "columns", []):
                out[code] = None
                continue
            ser = close[code].dropna()
            if len(ser) < 2:
                out[code] = None
                continue
            base = float(ser.iloc[0])
            idx = min(horizon_days, len(ser) - 1)
            if base == 0:
                out[code] = None
                continue
            out[code] = round(float(ser.iloc[idx]) / base - 1.0, 6)
        return out
    except Exception:
        return {c: None for c in codes}


def _cause(row: Optional[SelectionSnapshotStock]) -> str:
    if row is None:
        return "not_candidate"
    reason = (row.exclude_reason or "").strip()
    if reason:
        return reason if ":" in reason else "gate:%s" % reason
    return "rank_cut"


def _missed_detail(row: Optional[SelectionSnapshotStock], code: str,
                   fwd: Optional[float], horizon_days: int) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "code": code,
        "cause": _cause(row),
        "forward_return_pct": None if fwd is None else round(fwd * 100, 2),
        "horizon_days": horizon_days,
    }
    if row is not None:
        item["rank"] = row.rank
        item["score"] = None if row.score is None else round(float(row.score), 6)
        try:
            payload = json.loads(row.factor_json or "{}")
        except Exception:
            payload = {}
        item["raw_pcts"] = payload.get("raw_pcts") or {}
        explain = payload.get("explain") or []
        item["top_factors"] = explain[:3]
    else:
        item["note"] = "未进入当期候选打分集（闸门淘汰或不在 universe）"
    return item


def _upsert(db, snapshot: SelectionSnapshot, formula_id: int, horizon_days: int,
            payload: Dict[str, Any]) -> SelectionPoolRecall:
    row = (db.query(SelectionPoolRecall)
           .filter_by(snapshot_id=snapshot.id, formula_id=formula_id,
                      horizon_days=horizon_days).one_or_none())
    if row is None:
        row = SelectionPoolRecall(
            as_of=snapshot.as_of, formula_id=formula_id,
            snapshot_id=snapshot.id, horizon_days=horizon_days,
            payload_json=json.dumps(payload, ensure_ascii=False))
        db.add(row)
    else:
        row.payload_json = json.dumps(payload, ensure_ascii=False)
        row.computed_at = datetime.now()
    db.commit()
    if row.id:
        db.refresh(row)
    return row


def compute_recall(db, snapshot_id: int, formula_id: int,
                   horizon_days: int = 20, refresh: bool = False) -> Dict[str, Any]:
    """计算（或读取缓存）某快照 × 某公式的召回验证结果。"""
    snapshot = db.get(SelectionSnapshot, snapshot_id)
    if snapshot is None:
        raise RecallError("快照不存在：%s" % snapshot_id)
    if not snapshot.frozen:
        raise RecallError("快照尚未冻结，先冻结再验证（避免口径漂移）")
    formula = db.get(Formula, formula_id)
    if formula is None:
        raise RecallError("公式不存在：%s" % formula_id)
    if horizon_days < 1 or horizon_days > 120:
        raise RecallError("horizon_days 需在 1..120")

    if not refresh:
        cached = (db.query(SelectionPoolRecall)
                  .filter_by(snapshot_id=snapshot_id, formula_id=formula_id,
                             horizon_days=horizon_days).one_or_none())
        if cached is not None:
            data = json.loads(cached.payload_json)
            data["cached"] = True
            return data

    sig_rows = (db.query(FormulaSignal)
                .filter_by(formula_id=formula_id).all())
    triggers: Dict[str, float] = {}
    for s in sig_rows:
        if (s.signal_type or "").upper() != OPEN:
            continue
        try:
            triggers[s.signal_name] = float(s.trigger_value or 1)
        except (TypeError, ValueError):
            triggers[s.signal_name] = 1.0
    if not triggers:
        raise RecallError("公式 %s 没有 OPEN 信号线" % formula.name)

    stock_rows = (db.query(SelectionSnapshotStock)
                  .filter_by(snapshot_id=snapshot.id).all())
    by_code = {r.code: r for r in stock_rows}
    pool_codes = sorted({r.code for r in stock_rows if r.selected})
    if not pool_codes:
        raise RecallError("该快照没有命中成分，无法验证召回")
    universe = [r[0] for r in db.query(SelectionWarehouseStock.code).all()]

    market = _evaluate(db, formula, triggers, universe, snapshot.as_of)
    pool = _evaluate(db, formula, triggers, pool_codes, snapshot.as_of)

    market_sel = {c for c, v in market.items() if v.get("selected")}
    pool_sel = {c for c, v in pool.items() if v.get("selected")}
    missed = sorted(market_sel - set(pool_codes))
    in_pool_hit = sorted(market_sel & set(pool_codes))

    fwd = _forward_return(missed, snapshot.as_of, horizon_days) if missed else {}
    detail = [_missed_detail(by_code.get(c), c, fwd.get(c), horizon_days)
              for c in missed]
    causes: Dict[str, int] = {}
    for item in detail:
        causes[item["cause"]] = causes.get(item["cause"], 0) + 1

    eval_errors = [c for c, v in market.items() if v.get("selected") is None]
    payload: Dict[str, Any] = {
        "snapshot_id": snapshot.id,
        "as_of": snapshot.as_of.isoformat(),
        "scheme_id": None,
        "formula": {"id": formula.id, "name": formula.name,
                    "engine": formula.engine, "open_signals": triggers},
        "horizon_days": horizon_days,
        "universe_size": len(universe),
        "pool_size": len(pool_codes),
        "formula_selected_market": len(market_sel),
        "formula_selected_pool": len(pool_sel),
        "recall": (round(len(pool_sel) / len(market_sel), 6)
                   if market_sel else None),
        "in_pool_hit": in_pool_hit,
        "missed_count": len(missed),
        "cause_counts": causes,
        "missed": detail,
        "eval_error_count": len(eval_errors),
        "eval_error_sample": eval_errors[:10],
        "method": "single_pass_intersection",
        "notes": [
            "池只是候选域，最终买入由公式择时决定；召回率=池内入选/全市场入选。",
            "漏票收益为 as_of 起第 N 个交易日的不复权涨幅，取不到时置 null。",
            "IC/分组单调性仅作参考，不参与任何决策。",
        ],
        "computed_at": datetime.now().isoformat(timespec="seconds"),
        "cached": False,
    }
    _upsert(db, snapshot, formula_id, horizon_days, payload)
    return payload


def list_recalls(db, snapshot_id: Optional[int] = None,
                 limit: int = 20) -> List[Dict[str, Any]]:
    q = db.query(SelectionPoolRecall)
    if snapshot_id:
        q = q.filter(SelectionPoolRecall.snapshot_id == snapshot_id)
    rows = q.order_by(SelectionPoolRecall.id.desc()).limit(limit).all()
    out = []
    for r in rows:
        try:
            data = json.loads(r.payload_json)
        except Exception:
            data = {}
        out.append({
            "id": r.id, "as_of": r.as_of.isoformat(),
            "snapshot_id": r.snapshot_id, "formula_id": r.formula_id,
            "horizon_days": r.horizon_days,
            "recall": data.get("recall"),
            "formula_selected_market": data.get("formula_selected_market"),
            "formula_selected_pool": data.get("formula_selected_pool"),
            "missed_count": data.get("missed_count"),
            "cause_counts": data.get("cause_counts") or {},
            "computed_at": r.computed_at.isoformat(timespec="seconds")
            if r.computed_at else None,
        })
    return out
