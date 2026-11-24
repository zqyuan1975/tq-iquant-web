# -*- coding: utf-8 -*-
"""条件选股执行引擎（A2）。

数据只来自平台导入仓 ``selection_wh_*``（D1：运行期不读外部宽库）。
四池共用一套因子，按池权重做行业分位打分：
1) 硬闸门（ST/退市/次新/PE/PB/流通市值/财报期数/审计意见）
2) 因子计算（财报序列 + 导入日估值/一致预期快照 + 分红序列 + K线派生）
3) 去极值（1/99 分位）+ 行业内百分位（缺失 → 0.5 中性）
4) 池条件 + 因子覆盖不足排除 + top_n 命中

不造数：整列无数据时该池直接「暂不计算」；覆盖不足的单票不参与排序；
所有缺口写进 quality 披露。
"""
from __future__ import annotations

import json
import math
import statistics
import zlib
from datetime import date
from typing import Any, Dict, Iterable, List, Optional, Tuple

from core.models.selection import (
    SelectionWarehouseDividend,
    SelectionWarehouseFin,
    SelectionWarehouseMeta,
    SelectionWarehouseStock,
)
from core.selection.field_dict import (
    FIELD_SET_VERSION, SCORABLE_FIELDS, field_set_fingerprint)

MAX_PE = 500.0
MIN_FLOAT_MV = 20.0
MIN_FIN_PERIODS = 8
MIN_COVERAGE = 0.5
NEUTRAL_PCT = 0.5

POOL_LABELS = {"growth": "成长", "quality": "质量",
               "value": "价值", "dividend": "红利"}


class EngineError(RuntimeError):
    """数据前置不满足（导入仓缺失 / 历史回放不支持）。"""


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


def _json_blob(blob: Optional[bytes]) -> Dict[str, Any]:
    if not blob:
        return {}
    try:
        return json.loads(zlib.decompress(blob).decode("utf-8"))
    except Exception:
        try:
            return json.loads(blob.decode("utf-8"))
        except Exception:
            return {}


def _series_map(fin_rows: Iterable[SelectionWarehouseFin]) -> Dict[str, dict]:
    out: Dict[str, dict] = {}
    for row in fin_rows:
        out[row.code] = _json_blob(row.series_blob)
    return out


def _div_map(div_rows: Iterable[SelectionWarehouseDividend]) -> Dict[str, list]:
    out: Dict[str, list] = {}
    for row in div_rows:
        out[row.code] = _json_blob(row.rows_blob)
    return out


def warehouse_meta(db) -> Optional[SelectionWarehouseMeta]:
    return db.query(SelectionWarehouseMeta).first()


# ---------------------------------------------------------------------------
# 序列工具
# ---------------------------------------------------------------------------

def _col(series: dict, key: str) -> List[Optional[float]]:
    return [_num(v) for v in (series.get(key) or [])]


def _last(series: dict, key: str) -> Optional[float]:
    for val in reversed(_col(series, key)):
        if val is not None:
            return val
    return None


def _tail(series: dict, key: str, n: int) -> List[float]:
    return [v for v in _col(series, key)[-n:] if v is not None]


def _tail_nonzero(series: dict, key: str, n: int) -> List[float]:
    return [v for v in _col(series, key)[-n:] if v not in (None, 0.0)]


def _yoy(series: dict, key: str) -> Optional[float]:
    vals = _col(series, key)
    if len(vals) < 5:
        return None
    a, b = vals[-1], vals[-5]
    if a is None or b in (None, 0.0):
        return None
    return (a / b - 1.0) * 100.0


def _annual(series: dict, key: str) -> List[Tuple[str, float]]:
    tags = [str(t) for t in (series.get("tag_time") or [])]
    vals = _col(series, key)
    out: List[Tuple[str, float]] = []
    for tag, val in zip(tags, vals):
        if tag[4:8] == "1231" and val is not None:
            out.append((tag, val))
    return out


def _annual_cagr(series: dict, key: str, years: int) -> Optional[float]:
    pairs = _annual(series, key)
    if len(pairs) < years + 1:
        return None
    latest, base = pairs[-1][1], pairs[-1 - years][1]
    if latest <= 0 or base <= 0:
        return None
    try:
        return ((latest / base) ** (1.0 / years) - 1.0) * 100.0
    except (ValueError, ZeroDivisionError):
        return None


def _trend(series: dict, key: str, n: int) -> Optional[float]:
    vals = _tail(series, key, n)
    if len(vals) < 3:
        return None
    xs = list(range(len(vals)))
    mx = sum(xs) / len(xs)
    my = sum(vals) / len(vals)
    denom = sum((x - mx) ** 2 for x in xs)
    if not denom:
        return None
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, vals)) / denom
    scale = abs(my) if abs(my) > 1e-9 else 1.0
    return slope / scale * 100.0


def _mean(vals: List[float]) -> Optional[float]:
    return sum(vals) / len(vals) if vals else None


def _percentile(sorted_vals: List[float], q: float) -> float:
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    pos = (len(sorted_vals) - 1) * q
    low = int(math.floor(pos))
    high = min(low + 1, len(sorted_vals) - 1)
    frac = pos - low
    return sorted_vals[low] * (1 - frac) + sorted_vals[high] * frac


# ---------------------------------------------------------------------------
# 因子
# ---------------------------------------------------------------------------

def _dividend_factors(rows: list, price: Optional[float]) -> Dict[str, Any]:
    by_year: Dict[int, float] = {}
    for item in rows or []:
        try:
            ex_date, year, bonus = item[0], int(item[1]), float(item[2])
        except (TypeError, ValueError, IndexError):
            continue
        if bonus <= 0:
            continue
        by_year[year] = by_year.get(year, 0.0) + bonus
    years = sorted(by_year)
    streak = 0
    for year in reversed(years):
        if by_year[year] > 0:
            streak += 1
        else:
            break
    recent = [by_year[y] for y in years[-3:]]
    dps3 = sum(recent) / min(3, len(recent)) if recent else None
    dps_growth = None
    if len(years) >= 2:
        window = [by_year[y] for y in years[-5:]]
        sl = _trend({"t": window}, "t", len(window))
        dps_growth = sl
    dp3 = dps3 / price * 100.0 if dps3 is not None and price else None
    dps_now = by_year[years[-1]] if years else None
    dp_annual = dps_now / price * 100.0 if dps_now is not None and price else None
    return {"div_streak": float(streak) if years else None,
            "dp3": dp3, "dp_annual": dp_annual, "dps_growth": dps_growth}


def _build_records(db, meta) -> List[Dict[str, Any]]:
    stocks = db.query(SelectionWarehouseStock).all()
    fins = _series_map(db.query(SelectionWarehouseFin).all())
    divs = _div_map(db.query(SelectionWarehouseDividend).all())

    recs: List[Dict[str, Any]] = []
    for stock in stocks:
        more = _json_blob(stock.more_blob)
        info = _json_blob(stock.info_blob)
        go = _json_blob(stock.go_blob)

        series = fins.get(stock.code, {})
        price = float(stock.close) if stock.close else None
        pe = _num(more.get("StaticPE_TTM"))
        pb = _num(more.get("PB_MRQ"))
        zsz = _num(more.get("Zsz"))
        ltsz = _num(more.get("Ltsz"))
        rev = _last(series, "FN319")
        ocf = _last(series, "FN107")
        capex = _last(series, "FN114")
        np_last = _last(series, "FN125")
        cap_yuan = zsz * 1e8 if zsz else None

        roic = _last(series, "FN329")
        roe = _last(series, "FN197")
        gm = _last(series, "FN202")
        earn_qual = _last(series, "FN228")
        solvency_vals = []
        for key, sign in (("FN170", 1.0), ("FN210", -1.0), ("FN327", -1.0)):
            val = _last(series, key)
            if val is not None:
                solvency_vals.append(sign * val)
        audit = _last(series, "FN336")
        payout = _last(series, "FN337")
        payouts = _tail_nonzero(series, "FN337", 8)

        fcf = None
        cfp = None
        if ocf is not None and capex is not None:
            fcf = ocf - capex
            if cap_yuan:
                cfp = fcf / cap_yuan * 100.0
        elif ocf is not None and cap_yuan:
            cfp = ocf / cap_yuan * 100.0
        fcf_cover = (fcf / np_last) if fcf is not None and np_last and np_last != 0 else None

        g_fwd = None
        g_rev = None
        go8, go10 = _num(go.get("GO8")), _num(go.get("GO10"))
        go11, go13 = _num(go.get("GO11")), _num(go.get("GO13"))
        if go8 and go10 and go8 > 0 and go10 > 0:
            g_fwd = ((go10 / go8) ** 0.5 - 1.0) * 100.0
        if go11 and go13 and go11 > 0 and go13 > 0:
            g_rev = ((go13 / go11) ** 0.5 - 1.0) * 100.0
        peg_fwd = None
        if pe and g_fwd and g_fwd > 0:
            peg_fwd = pe / g_fwd
        neg_peg_fwd = -peg_fwd if peg_fwd is not None and 0 < peg_fwd < 100 else None

        g_hist = _last(series, "FN184")
        g_hist_3y = _annual_cagr(series, "FN96", 3)
        peg_hist = None
        if pe and g_hist_3y and g_hist_3y > 0:
            peg_hist = pe / g_hist_3y
        neg_peg_hist = -peg_hist if peg_hist is not None and 0 < peg_hist < 100 else None

        div = _dividend_factors(divs.get(stock.code), price)

        factors = {
            "ep": 1.0 / pe if pe and pe > 0 else None,
            "bp": 1.0 / pb if pb and pb > 0 else None,
            "cfp": cfp,
            "sp": rev / (zsz * 1e4) * 100.0 if rev and zsz else None,
            "dp3": div["dp3"],
            "dp_annual": div["dp_annual"],
            "div_streak": div["div_streak"],
            "dps_growth": div["dps_growth"],
            "payout": payout,
            "payout_stab": -statistics.pstdev(payouts) if len(payouts) >= 2 else None,
            "fcf_cover": fcf_cover,
            "roic_adj": roic if roic is not None else roe,
            "roe_adj": roe,
            "roe_yoy": _yoy(series, "FN197"),
            "gm": gm if gm not in (None, 0.0) else None,
            "earn_qual": earn_qual if earn_qual is not None else None,
            "solvency": _mean(solvency_vals),
            "govern": -audit if audit is not None else None,
            "g_fwd_2y": g_fwd,
            "g_rev_2y": g_rev,
            "peg_fwd": peg_fwd,
            "neg_peg_fwd": neg_peg_fwd,
            "g_hist_3y": g_hist_3y,
            "np_yoy": _yoy(series, "FN232"),
            "rev_yoy": _yoy(series, "FN230"),
            "accel": _trend(series, "FN184", 8),
            "deliver": (g_hist - _last(series, "FN285"))
            if g_hist is not None and _last(series, "FN285") is not None else None,
            "peg_hist": peg_hist,
            "neg_peg_hist": neg_peg_hist,
            "roic_trend": (_trend(series, "FN329", 12)
                           if _trend(series, "FN329", 12) is not None
                           else _trend(series, "FN197", 12)),
            "volatility": float(stock.volatility_pct)
            if stock.volatility_pct is not None else None,
            "max_drawdown": float(stock.max_drawdown_pct)
            if stock.max_drawdown_pct is not None else None,
            "size": ltsz,
        }
        # 财报名义字段做极值护栏（与外部方案一致）
        if factors["g_fwd_2y"] is not None and not -50 <= factors["g_fwd_2y"] <= 200:
            factors["g_fwd_2y"] = None
        for key in ("np_yoy", "rev_yoy", "g_hist_3y", "dps_growth"):
            if factors[key] is not None and not -100 <= factors[key] <= 300:
                factors[key] = None

        j_start = str(info.get("J_start") or "")
        recs.append({
            "code": stock.code,
            "name": stock.name or info.get("Name") or "",
            "sector": stock.sector_name or "未分类",
            "tags": stock.tags or "",
            "is_financial": bool(stock.is_financial),
            "pe": pe, "pb": pb, "ltsz": ltsz,
            "is_st": (_num(info.get("IsSTGP")) or 0) != 0,
            "is_quit": (_num(info.get("IsQuitGP")) or 0) != 0,
            "j_start": j_start,
            "fin_periods": int(stock.fin_periods or 0),
            "audit": audit,
            "go_covered": bool(g_fwd is not None),
            "factors": factors,
            "gate": None,
            "pct": {},
            "score": None,
            "coverage": None,
            "track": None,
        })
    return recs


# ---------------------------------------------------------------------------
# 闸门 / 去极值 / 分位
# ---------------------------------------------------------------------------

def _apply_gates(recs: List[Dict[str, Any]], as_of: date,
                 gates: Dict[str, Any]) -> Dict[str, int]:
    max_pe = float(gates.get("max_pe", MAX_PE))
    min_mv = float(gates.get("min_float_mv", MIN_FLOAT_MV))
    drops: Dict[str, int] = {}

    def drop(rec, reason: str) -> None:
        rec["gate"] = reason
        drops[reason] = drops.get(reason, 0) + 1

    for rec in recs:
        if rec["is_st"]:
            drop(rec, "st")
            continue
        if rec["is_quit"]:
            drop(rec, "quit")
            continue
        start = rec["j_start"]
        if len(start) == 8:
            try:
                listed = date(int(start[:4]), int(start[4:6]), int(start[6:8]))
                if (as_of - listed).days < 365:
                    drop(rec, "new_listing")
                    continue
            except ValueError:
                pass
        if rec["pe"] is None:
            drop(rec, "pe_missing")
            continue
        if rec["pe"] <= 0 or rec["pe"] > max_pe:
            drop(rec, "pe_out_of_range")
            continue
        if rec["pb"] is None or rec["pb"] <= 0:
            drop(rec, "pb_invalid")
            continue
        if rec["ltsz"] is None or rec["ltsz"] < min_mv:
            drop(rec, "float_mv_low")
            continue
        if rec["fin_periods"] < MIN_FIN_PERIODS:
            drop(rec, "fin_history_short")
            continue
        if rec["audit"] is not None and rec["audit"] >= 3:
            drop(rec, "audit_nonstandard")
    return drops


def _winsorize(recs: List[Dict[str, Any]], keys: Iterable[str]) -> None:
    for key in keys:
        vals = sorted(r["factors"][key] for r in recs
                      if r["gate"] is None and r["factors"].get(key) is not None)
        if len(vals) < 10:
            continue
        low, high = _percentile(vals, 0.01), _percentile(vals, 0.99)
        if low == high:
            continue
        for rec in recs:
            val = rec["factors"].get(key)
            if val is not None:
                rec["factors"][key] = min(max(val, low), high)


def _industry_percentile(recs: List[Dict[str, Any]], keys: Iterable[str]) -> None:
    ok = [r for r in recs if r["gate"] is None]
    for key in keys:
        groups: Dict[str, List[Tuple[float, Dict[str, Any]]]] = {}
        for rec in ok:
            val = rec["factors"].get(key)
            if val is not None:
                groups.setdefault(rec["sector"], []).append((val, rec))
        for items in groups.values():
            items.sort(key=lambda item: item[0])
            n = len(items)
            if n == 0:
                continue
            for idx, (_, rec) in enumerate(items):
                rec["pct"][key] = (idx + 0.5) / n
    for rec in ok:
        for key in keys:
            rec["pct"].setdefault(key, NEUTRAL_PCT)


def _ols_slope(xs: List[float], ys: List[float]) -> Tuple[float, float]:
    n = len(xs)
    if n < 2:
        return 0.0, (ys[0] if ys else NEUTRAL_PCT)
    mx = sum(xs) / n
    my = sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    if not denom:
        return 0.0, my
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom
    return slope, my - slope * mx


def _neutralize(recs: List[Dict[str, Any]], keys: Iterable[str],
                enabled: bool) -> Dict[str, Any]:
    """行业 + 市值中性化（在行业分位基础上剔除市值秩的线性影响）。

    做法：行业内把因子分位对市值分位做一元回归，取残差后**重新在行业内排秩**，
    避免「成长=小市值」「质量=某行业」的风格漂移。样本不足（<30 只或行业内
    <5 只）时整体跳过并披露 ``skipped_small_sample``——小样本回归不可信。
    原始分位保留在 ``raw_pct``，供解释与相关性质检。
    """
    ok = [r for r in recs if r["gate"] is None]
    for rec in ok:
        rec["raw_pct"] = dict(rec["pct"])
    keys = [k for k in keys if k != "size"]
    if not enabled:
        return {"applied": False, "method": "disabled_by_spec",
                "reason": "版本参数关闭了中性化"}
    if len(ok) < 30:
        return {"applied": False, "method": "skipped_small_sample",
                "reason": "过闸门样本 %d < 30，小样本回归不可信" % len(ok)}
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for rec in ok:
        groups.setdefault(rec["sector"], []).append(rec)
    adjusted = 0
    for members in groups.values():
        if len(members) < 5:
            continue
        xs = [m["raw_pct"].get("size", NEUTRAL_PCT) for m in members]
        for key in keys:
            ys = [m["raw_pct"].get(key, NEUTRAL_PCT) for m in members]
            slope, intercept = _ols_slope(xs, ys)
            residuals = [(y - (slope * x + intercept), m)
                         for x, y, m in zip(xs, ys, members)]
            residuals.sort(key=lambda item: item[0])
            n = len(residuals)
            for idx, (_, m) in enumerate(residuals):
                m["pct"][key] = (idx + 0.5) / n
            adjusted += 1
    return {"applied": True, "method": "industry_size_residual_rank",
            "reason": "行业内对市值分位回归取残差后重排秩",
            "group_factor_adjusted": adjusted}


def _spearman(rows_a: List[float], rows_b: List[float]) -> Optional[float]:
    n = len(rows_a)
    if n < 30:
        return None
    ma, mb = sum(rows_a) / n, sum(rows_b) / n
    da = [a - ma for a in rows_a]
    db = [b - mb for b in rows_b]
    denom = math.sqrt(sum(x * x for x in da) * sum(y * y for y in db))
    if not denom:
        return None
    return sum(x * y for x, y in zip(da, db)) / denom


def _correlation_report(recs: List[Dict[str, Any]], keys: Iterable[str],
                        threshold: float = 0.8) -> List[Dict[str, Any]]:
    """风格内因子相关性检查：只报告高相关对，不改权重（避免静默改口径）。"""
    ok = [r for r in recs if r["gate"] is None]
    if len(ok) < 30:
        return []
    keys = sorted(keys)
    pairs = []
    for i, a in enumerate(keys):
        va = [r["raw_pct"].get(a, NEUTRAL_PCT) for r in ok]
        for b in keys[i + 1:]:
            vb = [r["raw_pct"].get(b, NEUTRAL_PCT) for r in ok]
            rho = _spearman(va, vb)
            if rho is not None and abs(rho) >= threshold:
                pairs.append({"a": a, "b": b, "spearman": round(rho, 3)})
    pairs.sort(key=lambda p: -abs(p["spearman"]))
    return pairs[:10]


def _score(rec: Dict[str, Any], weights: Dict[str, float]) -> Tuple[Optional[float], float]:
    total_w = 0.0
    total = 0.0
    covered = 0.0
    for key, weight in weights.items():
        raw = rec["factors"].get(key)
        if raw is None:
            continue
        covered += float(weight)
        total += float(weight) * rec["pct"].get(key, NEUTRAL_PCT)
        total_w += float(weight)
    if total_w <= 0 or covered < MIN_COVERAGE:
        return None, covered
    return total / total_w, covered


def _rank_select(recs: List[Dict[str, Any]], pool: str, spec: dict,
                 as_of: date) -> Dict[str, Any]:
    weights = spec["weights"]
    weights_hist = spec.get("weights_hist") or {}
    gates = spec.get("gates") or {}
    top_n = int(spec.get("top_n", 100))
    track = spec.get("track", "both")
    stats: Dict[str, Any] = {"pool_key": pool, "eligible": 0,
                             "selected": 0, "tracks": {}}

    eligible: List[Dict[str, Any]] = []
    for rec in recs:
        if rec["gate"] is not None:
            continue
        f = rec["factors"]
        reason = None
        if pool == "dividend":
            if (f.get("div_streak") or 0) < gates.get("min_div_streak", 3):
                reason = "div_streak_short"
            elif not f.get("dp3") or f["dp3"] <= 0:
                reason = "no_dividend"
            elif f.get("payout") is not None and not 0 < f["payout"] <= gates.get("max_payout", 100):
                reason = "payout_out_of_range"
            elif f.get("fcf_cover") is not None and f["fcf_cover"] < gates.get("min_fcf_cover", 1.0):
                reason = "fcf_cover_low"
            use = weights
            rec["track"] = "cash"
        elif pool == "value":
            if f.get("roic_trend") is not None and f["roic_trend"] < gates.get("max_roic_trend_drop", -5.0):
                reason = "roic_trend_break"
            use = weights
            rec["track"] = "value"
        elif pool == "quality":
            if f.get("roic_adj") is None:
                reason = "roic_missing"
            use = weights
            rec["track"] = "quality"
        elif pool == "growth":
            if track == "forward":
                if not rec["go_covered"]:
                    reason = "go_uncovered"
                rec["track"] = "forward"
                use = weights
            elif track == "historical":
                if rec["go_covered"]:
                    reason = "forward_covered"
                rec["track"] = "historical"
                # 单轨模式：weights 就是本轨权重（weights_hist 仅 both 模式使用）
                use = weights
            else:
                rec["track"] = "forward" if rec["go_covered"] else "historical"
                use = weights if rec["go_covered"] else weights_hist
        else:
            reason = "pool_unsupported"
            use = weights
        if reason:
            rec["exclude_reason"] = reason
            continue
        if rec["is_financial"] and pool == "quality":
            # 金融股用 ROE 代替 ROIC（已在 roic_adj 回退），偿付能力口径失真但保留并披露
            pass
        score, coverage = _score(rec, use)
        if score is None:
            rec["exclude_reason"] = "factor_coverage_low"
            continue
        rec["weights_used"] = use
        rec["score"] = round(score, 6)
        rec["coverage"] = round(coverage, 4)
        eligible.append(rec)

    stats["eligible"] = len(eligible)
    selected: List[Dict[str, Any]] = []
    if pool == "growth" and track == "both":
        for tr in ("forward", "historical"):
            bucket = [r for r in eligible if r["track"] == tr]
            bucket.sort(key=lambda r: (-(r["score"] or 0), r["code"]))
            stats["tracks"][tr] = {"eligible": len(bucket),
                                   "selected": min(top_n, len(bucket))}
            selected.extend(bucket[:top_n])
        selected.sort(key=lambda r: (-(r["score"] or 0), r["code"]))
        for idx, rec in enumerate(selected, start=1):
            rec["rank"] = idx
    else:
        eligible.sort(key=lambda r: (-(r["score"] or 0), r["code"]))
        for idx, rec in enumerate(eligible[:top_n], start=1):
            rec["rank"] = idx
            selected.append(rec)
        stats["tracks"][track if pool == "growth" else pool] = {
            "eligible": len(eligible), "selected": len(selected)}
    stats["selected"] = len(selected)
    stats["capped"] = len(eligible) > len(selected)
    return stats


def _row_payload(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "code": rec["code"],
        "name": rec["name"],
        "sector": rec["sector"],
        "is_financial": rec["is_financial"],
        "track": rec.get("track"),
        "score": rec.get("score"),
        "coverage": rec.get("coverage"),
        "rank": rec.get("rank"),
        "selected": rec.get("rank") is not None,
        "exclude_reason": rec.get("exclude_reason"),
        "gate": rec.get("gate"),
    }


def compute(db, spec: dict, as_of: Optional[date] = None) -> Dict[str, Any]:
    """计算一个方案的选股结果，返回 {as_of, pool_key, stats, quality, rows}。"""
    meta = warehouse_meta(db)
    if meta is None:
        raise EngineError("尚未导入宽库：请先在「数据体检」执行导入")
    if as_of is None:
        as_of = meta.as_of
    if as_of != meta.as_of:
        raise EngineError(
            "历史回放暂不支持：导入仓只有 %s 单时点估值/K线快照，"
            "重算 %s 会用到未来数据" % (meta.as_of.isoformat(), as_of.isoformat()))

    pool = spec["pool_key"]
    if pool not in POOL_LABELS:
        raise EngineError("暂不支持该池：%s（首发只做四风格池）" % pool)

    recs = _build_records(db, meta)
    gates = spec.get("gates") or {}
    drops = _apply_gates(recs, as_of, gates)

    weights = spec["weights"]
    keys = set(weights) | set(spec.get("weights_hist") or {})
    missing_columns = []
    for key in sorted(keys):
        if key not in SCORABLE_FIELDS:
            raise EngineError("字段未接入打分：%s" % key)
        if not any(r["gate"] is None and r["factors"].get(key) is not None for r in recs):
            missing_columns.append(key)
    if missing_columns:
        raise EngineError("整列无数据，暂不计算：%s" % ", ".join(missing_columns))

    _winsorize(recs, keys)
    _industry_percentile(recs, keys)
    neutral = _neutralize(recs, keys, enabled=bool(spec.get("neutralize", True)))
    corr_pairs = _correlation_report(recs, keys)

    stats = _rank_select(recs, pool, spec, as_of)
    stats.update({
        "universe": len(recs),
        "gate_drops": drops,
        "gate_passed": sum(1 for r in recs if r["gate"] is None),
        "top_n": int(spec.get("top_n", 100)),
        "track_mode": spec.get("track", "both") if pool == "growth" else None,
        "neutralization": neutral,
        "high_corr_pairs": corr_pairs,
        "corr_threshold": 0.8,
        "factor_coverage": {
            key: sum(1 for r in recs if r["gate"] is None
                     and r["factors"].get(key) is not None)
            for key in sorted(keys)},
    })

    selected = [r for r in recs if r.get("rank") is not None]
    near = [r for r in recs if r["gate"] is None
            and "exclude_reason" not in r and r.get("rank") is None]
    near.sort(key=lambda r: (-(r.get("score") or 0), r["code"]))
    rows: List[Dict[str, Any]] = []
    for rec in selected + near[:max(0, int(spec.get("top_n", 100)) * 2)]:
        payload = _row_payload(rec)
        payload["factors"] = {k: rec["factors"].get(k) for k in keys
                              if rec["factors"].get(k) is not None}
        payload["pcts"] = {k: round(rec["pct"].get(k, NEUTRAL_PCT), 4) for k in keys}
        payload["raw_pcts"] = {
            k: round(rec.get("raw_pct", rec["pct"]).get(k, NEUTRAL_PCT), 4)
            for k in keys}
        used = rec.get("weights_used") or weights
        explain = []
        for key in sorted(used):
            val = rec["factors"].get(key)
            if val is None:
                continue
            pct = rec["pct"].get(key, NEUTRAL_PCT)
            explain.append({
                "factor": key, "value": val, "pct": round(pct, 4),
                "weight": used[key],
                "contribution": round(float(used[key]) * pct, 4),
            })
        explain.sort(key=lambda item: -item["contribution"])
        payload["explain"] = explain[:8]
        rows.append(payload)
    dropped = [r for r in recs if r["gate"] is not None
               or r.get("exclude_reason")]
    stats["rows_kept"] = len(rows)
    stats["rows_dropped_detail_omitted"] = len(dropped)

    fields = sorted(keys)
    quality = {
        "field_set_version": FIELD_SET_VERSION,
        "field_fingerprint": field_set_fingerprint(fields),
        "fields": fields,
        "warehouse_as_of": meta.as_of.isoformat(),
        "warehouse_imported_at": meta.imported_at.isoformat(timespec="seconds")
        if meta.imported_at else None,
        "is_financial_source": "宽库标记全 0，按板块标签（金融单列/金融地产）识别",
        "valuation_source": "导入日单时点 PE/PB/市值/一致预期快照",
        "kline_source": "不复权收盘，导入日前 252 个交易日",
        "missing_policy": "因子缺失→行业分位中性 0.5；因子覆盖<50% 不参与排序；整列无数据则该池暂不计算",
        "neutralization": (
            "行业分位（行业内排秩）+ 市值中性：行业内对市值分位回归取残差后重排秩；"
            "样本<30 或行业内<5 时跳过并披露"
            if neutral["applied"] else
            "中性化未生效：%s" % neutral["reason"]),
        "correlation_check": (
            "风格内因子两两 Spearman，|ρ|≥0.8 的在高相关对中列出（只报告，不自动改权重）"),
        "validation": "因子稳定性（滚动 IC/分组单调性）见 /api/selection/factor-validation；估值类因子无历史快照，不可回验",
        "survivorship": "universe 只有导入日成分，历史回放暂不支持，存在幸存者偏差",
        "uncalibrated": "内置权重未校准，仅作起点",
        "notes": json.loads(meta.counts_json).get("notes", [])
        if meta.counts_json else [],
    }
    return {"as_of": as_of, "pool_key": pool, "stats": stats,
            "quality": quality, "rows": rows}
