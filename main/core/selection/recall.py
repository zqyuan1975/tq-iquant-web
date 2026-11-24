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
    SelectionPoolRecall, SelectionSchemeVersion, SelectionSnapshot,
    SelectionSnapshotStock, SelectionWarehouseStock,
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


def _line_rows(series, day: date) -> List[tuple]:
    """公式输出序列在 day（含）之前的 (日期串, 值) 列表，按日期升序。"""
    rows: List[tuple] = []
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
            fv = float(v)
        except (TypeError, ValueError):
            continue
        rows.append((ds, fv))
    rows.sort()
    return rows


def _evaluate(db, formula: Formula, open_signals: Dict[str, float],
              codes: List[str], as_of: date, warmup_days: int = 300,
              chunk: int = 80, signal_window_days: int = 20,
              ) -> Dict[str, Dict[str, Any]]:
    """逐批调用公式，返回 {code: {selected, lines, last_fire}}。

    入选判定：OPEN 信号线在 as_of 及之前最近 ``signal_window_days`` 个**交易bar**
    内出现过触发值。择时公式的 OPEN 是稀疏脉冲（实测单票每年仅 1-3 次，单日
    全市场经常 0 只），只用 as_of 单日会把召回口径退化成 0/0，故按「同期窗口」
    取集合；``signal_window_days=1`` 即用户最初要求的严格单日口径。
    """
    from core.tq.formula import TQFormula

    if not open_signals:
        raise RecallError("公式 %s 没有 OPEN 信号线，无法做择时验证" % formula.name)
    window = max(1, int(signal_window_days or 1))
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
            last_fire = None
            hit = False
            seen = False
            for name, trigger in open_signals.items():
                rows = _line_rows(series_by_line.get(name), as_of)
                if not rows:
                    continue
                seen = True
                lines[name] = rows[-1][1]
                for ds, val in rows[-window:]:
                    if abs(val - trigger) < 1e-6:
                        hit = True
                        if last_fire is None or ds > last_fire:
                            last_fire = ds
            out[code] = {
                "selected": hit if seen else None,
                "lines": lines,
                "last_fire": last_fire,
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


def _recheck_gates(db, codes: List[str], as_of: date,
                   gates: Dict[str, Any],
                   pe_mode: str = "strict") -> Dict[str, str]:
    """对快照未留痕的漏票，用仓库快照重放可复核的硬闸门。

    快照只留痕「入选 + 2×TopN 边缘」，其余漏票没有淘汰原因可读。这里按引擎
    ``_apply_gates`` 的顺序重放 ST/退市/次新/PE/PB/流通市值/财报期数六道基础闸门，
    能归因的给出 ``gate:xxx``，全部通过则归为 ``rank_cut``（排名在 TopN 之外或
    被风格专属闸门淘汰）。
    """
    if not codes:
        return {}
    from core.selection.engine import (  # 与引擎共用阈值，避免口径漂移
        MAX_PE, MIN_FIN_PERIODS, MIN_FLOAT_MV, _json_blob, _num,
    )
    max_pe = float(gates.get("max_pe") or MAX_PE)
    min_mv = float(gates.get("min_float_mv") or MIN_FLOAT_MV)
    stocks = (db.query(SelectionWarehouseStock)
              .filter(SelectionWarehouseStock.code.in_(codes)).all())
    known = set()
    out: Dict[str, str] = {}
    for s in stocks:
        known.add(s.code)
        info = _json_blob(s.info_blob)
        more = _json_blob(s.more_blob)
        if (_num(info.get("IsSTGP")) or 0) != 0:
            out[s.code] = "gate:st"
            continue
        if (_num(info.get("IsQuitGP")) or 0) != 0:
            out[s.code] = "gate:quit"
            continue
        j = str(info.get("J_start") or "")
        if len(j) == 8:
            try:
                listed = date(int(j[:4]), int(j[4:6]), int(j[6:8]))
                if (as_of - listed).days < 365:
                    out[s.code] = "gate:new_listing"
                    continue
            except ValueError:
                pass
        pe = _num(more.get("StaticPE_TTM"))
        if pe is None:
            out[s.code] = "gate:pe_missing"
            continue
        if pe > max_pe:
            out[s.code] = "gate:pe_out_of_range"
            continue
        if pe <= 0 and pe_mode == "strict":
            out[s.code] = "gate:pe_out_of_range"
            continue
        pb = _num(more.get("PB_MRQ"))
        if pb is None or pb <= 0:
            out[s.code] = "gate:pb_invalid"
            continue
        ltsz = _num(more.get("Ltsz"))
        if ltsz is None or ltsz < min_mv:
            out[s.code] = "gate:float_mv_low"
            continue
        if int(s.fin_periods or 0) < MIN_FIN_PERIODS:
            out[s.code] = "gate:fin_history_short"
            continue
        out[s.code] = "rank_cut"
    for c in codes:
        if c not in known:
            out[c] = "not_in_warehouse"
    return out


def _missed_detail(row: Optional[SelectionSnapshotStock], code: str,
                   fwd: Optional[float], horizon_days: int,
                   last_fire: Optional[str] = None) -> Dict[str, Any]:
    item: Dict[str, Any] = {
        "code": code,
        "cause": _cause(row),
        "forward_return_pct": None if fwd is None else round(fwd * 100, 2),
        "horizon_days": horizon_days,
        "last_fire": last_fire,
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
                   horizon_days: int = 20, refresh: bool = False,
                   signal_window_days: int = 20) -> Dict[str, Any]:
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
    window = max(1, int(signal_window_days or 1))

    if not refresh:
        cached = (db.query(SelectionPoolRecall)
                  .filter_by(snapshot_id=snapshot_id, formula_id=formula_id,
                             horizon_days=horizon_days).one_or_none())
        if cached is not None:
            data = json.loads(cached.payload_json)
            # 口径变了（窗口不同）不能复用旧结果，避免新旧口径混看
            if int(data.get("signal_window_days") or 1) == window:
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

    market = _evaluate(db, formula, triggers, universe, snapshot.as_of,
                       signal_window_days=window)
    pool = _evaluate(db, formula, triggers, pool_codes, snapshot.as_of,
                     signal_window_days=window)

    market_sel = {c for c, v in market.items() if v.get("selected")}
    pool_sel = {c for c, v in pool.items() if v.get("selected")}
    missed = sorted(market_sel - set(pool_codes))
    in_pool_hit = sorted(market_sel & set(pool_codes))

    fwd = _forward_return(missed, snapshot.as_of, horizon_days) if missed else {}
    detail = [_missed_detail(by_code.get(c), c, fwd.get(c), horizon_days,
                             (market.get(c) or {}).get("last_fire"))
              for c in missed]
    # 快照只留痕边缘票；其余漏票用仓库快照重放基础闸门，避免清一色「未进候选」
    version = db.get(SelectionSchemeVersion, snapshot.scheme_version_id)
    gates: Dict[str, Any] = {}
    pe_mode = "strict"
    if version is not None:
        try:
            spec = json.loads(version.spec_json) or {}
        except (TypeError, ValueError):
            spec = {}
        gates = spec.get("gates") or {}
        pe_mode = spec.get("pe_mode") or "strict"
    unknown = [it["code"] for it in detail if it["cause"] == "not_candidate"]
    recheck = _recheck_gates(db, unknown, snapshot.as_of, gates, pe_mode)
    for item in detail:
        if item["cause"] == "not_candidate" and item["code"] in recheck:
            item["cause"] = recheck[item["code"]]
    causes: Dict[str, int] = {}
    for item in detail:
        causes[item["cause"]] = causes.get(item["cause"], 0) + 1

    eval_errors = [c for c, v in market.items() if v.get("selected") is None]
    # 求值大面积失败（TQ 公式服务不可用/并发冲突）时不能把 0 入选写成真实结论
    if len(eval_errors) > max(20, int(len(universe) * 0.5)):
        raise RecallError(
            "公式求值失败 %d/%d（疑似 TQ 公式服务不可用或并发冲突），"
            "本次不落库，请稍后重试" % (len(eval_errors), len(universe)))
    payload: Dict[str, Any] = {
        "snapshot_id": snapshot.id,
        "as_of": snapshot.as_of.isoformat(),
        "scheme_id": None,
        "formula": {"id": formula.id, "name": formula.name,
                    "engine": formula.engine, "open_signals": triggers},
        "horizon_days": horizon_days,
        "signal_window_days": window,
        "pe_mode": pe_mode,
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
            "入选=OPEN 信号线在 as_of 前最近 %d 个交易 bar 内触发（择时脉冲稀疏，"
            "单日口径常为 0/0）；signal_window_days=1 即严格单日口径。" % window,
            "漏票收益为 as_of 起第 N 个交易日的不复权涨幅，取不到时置 null。",
            "漏票成因：快照留痕优先；未留痕票按仓库快照重放基础闸门"
            "（ST/退市/次新/PE/PB/流通市值/财报期数），全部通过计 rank_cut"
            "（被 TopN 或风格专属闸门淘汰）。",
            "PE 口径：%s（strict=PE≤0 全挡；loss_discount=亏损股进候选但按亏损幅度降权）。"
            % pe_mode,
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
