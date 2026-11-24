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
from core.selection import sector_pools

MAX_PE = 500.0
MIN_FLOAT_MV = 20.0
MIN_FIN_PERIODS = 8
MIN_COVERAGE = 0.5
NEUTRAL_PCT = 0.5

#: PE 口径（版本参数，可随版本回退）
#: - strict：现值，``PE<=0`` 或 ``PE>max_pe`` 一律挡在候选外；
#: - loss_discount：只放宽 ``PE<=0``——亏损股进候选，但按亏损幅度对综合得分
#:   打折（``PE>max_pe`` 仍挡）；
#: - loss_allowed：周期/农业行业池专用——PE<=0 与 PE 缺失都不剔除（按 PB>0 +
#:   财务序列 ≥8 期把关），不做折扣，PE 缺失在因子层自然置空。
PE_MODES = ("strict", "loss_discount", "loss_allowed")
DEFAULT_PE_MODE = "strict"
#: 亏损档位依据「亏损收益率 = -1/PE」（亏损额/市值，越大亏得越狠）：
#: PE<=-100 亏损<1% → 0.90；( -100,-30 ] 1%~3.3% → 0.75；
#: ( -30,-10 ] 3.3%~10% → 0.55；> -10 亏损>10% → 0.35。
#: 系数只乘在最终综合得分上，不改因子值、权重与任何闸门。
LOSS_DISCOUNT_BANDS = (
    (-100.0, 0.90, "亏损<1%"),
    (-30.0, 0.75, "亏损1%~3.3%"),
    (-10.0, 0.55, "亏损3.3%~10%"),
    (0.0, 0.35, "亏损>10%"),
)

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


def _build_records(db, meta, pit: Optional[dict] = None) -> List[Dict[str, Any]]:
    """构造打分记录。

    ``pit`` 非空 = 历史回放时点重建（见 ``replay.py``）：财报只用截断序列，
    K 线派生与总市值取 as_of 当日，PE/PB/一致预期/ST 标记等不可重建项留空，
    绝不用导入日快照冒充历史值。
    """
    stocks = db.query(SelectionWarehouseStock).all()
    if pit is None:
        fins = _series_map(db.query(SelectionWarehouseFin).all())
        divs = _div_map(db.query(SelectionWarehouseDividend).all())
        metrics: Dict[str, dict] = {}
        gp_map: Dict[str, dict] = {}
    else:
        fins = pit.get("series") or {}
        divs = pit.get("dividends") or {}
        metrics = pit.get("metrics") or {}
        gp_map = pit.get("gp") or {}

    recs: List[Dict[str, Any]] = []
    for stock in stocks:
        info = _json_blob(stock.info_blob)
        if pit is None:
            more = _json_blob(stock.more_blob)
            go = _json_blob(stock.go_blob)
            series = fins.get(stock.code, {})
            price = float(stock.close) if stock.close else None
            pe = _num(more.get("StaticPE_TTM"))
            pb = _num(more.get("PB_MRQ"))
            zsz = _num(more.get("Zsz"))
            ltsz = _num(more.get("Ltsz"))
            volatility = float(stock.volatility_pct) \
                if stock.volatility_pct is not None else None
            drawdown = float(stock.max_drawdown_pct) \
                if stock.max_drawdown_pct is not None else None
            is_st = (_num(info.get("IsSTGP")) or 0) != 0
            is_quit = (_num(info.get("IsQuitGP")) or 0) != 0
            fin_periods = int(stock.fin_periods or 0)
            gp_yield = None
        else:
            more = {}
            go = {}
            series = fins.get(stock.code, {})
            metric = metrics.get(stock.code) or {}
            price = _num(metric.get("close"))
            pe = None
            pb = None
            gp = gp_map.get(stock.code) or {}
            gp16 = _num(gp.get("GP16"))
            # GP16 单位万元 → 亿元；流通市值无时点数据，规模因子用总市值代理
            zsz = gp16 / 1e4 if gp16 else None
            ltsz = zsz
            volatility = _num(metric.get("volatility_pct"))
            drawdown = _num(metric.get("max_drawdown_pct"))
            # ST/退市标记只有当前值，不可 PIT 重建 → 一律置否并在 quality 披露
            is_st = False
            is_quit = False
            fin_periods = len(series.get("tag_time") or [])
            gp_yield = _num(gp.get("GP21"))
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
        if pit is None:
            payout = _last(series, "FN337")
        else:
            # PIT：TQ 的 FN337 在多数季报为 0（分红率只在年报口径完整），直取
            # 「最近一期」会把正常分红的票误杀。改用 ≤T 最近年报的非零值，仍
            # 属纯 PIT 口径（只用到 as_of 前已公告数据）。
            payout = next((v for _tag, v in reversed(_annual(series, "FN337"))
                           if v not in (None, 0.0)), None)
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
        if gp_yield is not None and gp_yield > 0:
            # GP21 = as_of 当日股息率（%）：可直接重建，优先于「用滞后分红除以
            # 当日股价」的近似（后者在分红序列不完整时会低估）
            div["dp_annual"] = gp_yield
            div["dp3"] = gp_yield

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
            "volatility": volatility,
            "volatility_rev": -volatility if volatility is not None else None,
            "max_drawdown": drawdown,
            "size": ltsz,
        }
        # 财报名义字段做极值护栏（与外部方案一致）
        if factors["g_fwd_2y"] is not None and not -50 <= factors["g_fwd_2y"] <= 200:
            factors["g_fwd_2y"] = None
        for key in ("np_yoy", "rev_yoy", "g_hist_3y", "dps_growth"):
            if factors[key] is not None and not -100 <= factors[key] <= 300:
                factors[key] = None

        j_start = str(info.get("J_start") or "")
        l1, l2_tags, _sector_code = sector_pools.classify(stock.sector_name)
        recs.append({
            "code": stock.code,
            "name": stock.name or info.get("Name") or "",
            "sector": stock.sector_name or "未分类",
            "tags": stock.tags or "",
            "l1": l1,
            "l2_tags": l2_tags,
            "is_financial": bool(stock.is_financial),
            "pe": pe, "pb": pb, "ltsz": ltsz,
            "is_st": is_st,
            "is_quit": is_quit,
            "j_start": j_start,
            "fin_periods": fin_periods,
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

def _loss_discount(pe: float) -> Tuple[float, str]:
    """亏损股折扣系数：按 ``-1/PE`` 的亏损幅度分档（见 LOSS_DISCOUNT_BANDS）。"""
    for upper, factor, label in LOSS_DISCOUNT_BANDS:
        if pe <= upper:
            return factor, label
    return 1.0, ""


def _apply_gates(recs: List[Dict[str, Any]], as_of: date,
                 gates: Dict[str, Any],
                 pe_mode: str = DEFAULT_PE_MODE,
                 group_only: bool = False,
                 pit: bool = False) -> Dict[str, int]:
    """硬闸门。``pit=True`` 时跳过依赖导入日快照的门槛（PE/PB/流通市值），
    因为这些字段在历史 as_of 不可重建；ST/退市标记同理不参与（见 quality）。
    """
    max_pe = float(gates.get("max_pe", MAX_PE))
    min_mv = float(gates.get("min_float_mv", MIN_FLOAT_MV))
    if pe_mode not in PE_MODES:
        raise EngineError("未知 PE 口径：%r" % pe_mode)
    drops: Dict[str, int] = {}

    def drop(rec, reason: str) -> None:
        rec["gate"] = reason
        drops[reason] = drops.get(reason, 0) + 1

    for rec in recs:
        if not pit:
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
        if group_only:
            # 金融地产第一阶段只分组：不套估值/财务门槛，避免把成员挡掉
            continue
        if not pit:
            if rec["pe"] is None:
                if pe_mode != "loss_allowed":
                    drop(rec, "pe_missing")
                    continue
            elif rec["pe"] > max_pe:
                drop(rec, "pe_out_of_range")
                continue
            elif rec["pe"] <= 0:
                if pe_mode == "strict":
                    drop(rec, "pe_out_of_range")
                    continue
                if pe_mode == "loss_discount":
                    # 亏损股降权：放行但记折扣，打分后乘到综合得分上
                    rec["pe_discount"], rec["pe_discount_band"] = _loss_discount(rec["pe"])
                # loss_allowed：放行且不打折
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


def _renorm_weights(weights: Optional[Dict[str, float]],
                    available: set) -> Tuple[Dict[str, float], List[str]]:
    """剔除全列不可得的因子后按剩余权重归一化；返回 (新权重, 被剔除因子)。"""
    raw = {k: float(v) for k, v in (weights or {}).items()
           if float(v) > 0}
    if not raw:
        return {}, []
    kept = {k: v for k, v in raw.items() if k in available}
    dropped = sorted(set(raw) - set(kept))
    total = sum(kept.values())
    if not kept or total <= 0:
        return {}, dropped
    return {k: round(v / total, 6) for k, v in kept.items()}, dropped


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
        discount = float(rec.get("pe_discount") or 1.0)
        if discount < 1.0:
            rec["score_undiscounted"] = round(score, 6)
            score = score * discount
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


def _l1_weights(l1: str) -> Optional[Dict[str, float]]:
    for pool in sector_pools.L1_POOLS.values():
        if pool.get("l1") == l1:
            return pool.get("weights") or None
    return None


def _l1_weights_hist(l1: str) -> Optional[Dict[str, float]]:
    for pool in sector_pools.L1_POOLS.values():
        if pool.get("l1") == l1:
            return pool.get("weights_hist") or None
    return None


def _rank_select_industry(recs: List[Dict[str, Any]], pool: str, spec: dict,
                          as_of: date,
                          l1_map: Optional[Dict[str, Tuple[dict, dict]]] = None
                          ) -> Dict[str, Any]:
    """行业池排序：L1 主池按自身权重；L2 标签池按个股所属 L1 权重。

    金融地产（L1 与 L2 内的成员）只分组不打分：``score`` 保持 None，
    不写 0.5 之类占位值。
    """
    pool_def = sector_pools.get_pool(pool)
    group_only = bool(spec.get("group_only") or pool_def.get("group_only"))
    cross = bool(spec.get("cross_pool") or pool_def.get("cross_pool"))
    top_n = int(spec.get("top_n", pool_def.get("top_n", 200)))
    no_truncate = bool(spec.get("no_truncate") or pool_def.get("no_truncate"))
    track = pool_def.get("track")
    weights = spec.get("weights") or {}
    weights_hist = spec.get("weights_hist") or {}
    stats: Dict[str, Any] = {"pool_key": pool, "eligible": 0, "selected": 0,
                             "tracks": {}, "group_only": group_only,
                             "cross_pool": cross}

    eligible: List[Dict[str, Any]] = []
    for rec in recs:
        if rec["gate"] is not None:
            continue
        if group_only:
            rec["score"] = None
            rec["coverage"] = None
            rec["score_status"] = "group_only"
            rec["track"] = None
            eligible.append(rec)
            continue
        use = weights
        if cross:
            l1 = rec.get("l1") or "其他综合"
            if l1_map is not None:
                use = (l1_map.get(l1) or (None, None))[0]
            else:
                use = _l1_weights(l1)
            if not use or l1 == "金融地产":
                # 金融地产在第一阶段只分组：L2 标签池里的金融成员同样留空分值
                rec["score"] = None
                rec["coverage"] = None
                rec["score_status"] = "group_only"
                rec["track"] = "l1:" + l1
                eligible.append(rec)
                continue
            if track == "both":
                rec["track"] = "forward" if rec["go_covered"] else "historical"
                if not rec["go_covered"]:
                    if l1_map is not None:
                        use = (l1_map.get(l1) or (None, None))[1] or use
                    else:
                        use = (_l1_weights_hist(l1) or use)
            else:
                rec["track"] = "l1:" + l1
        elif track == "both":
            rec["track"] = "forward" if rec["go_covered"] else "historical"
            use = weights if rec["go_covered"] else weights_hist
        else:
            rec["track"] = pool_def.get("label")
        score, coverage = _score(rec, use or {})
        if score is None:
            rec["exclude_reason"] = "factor_coverage_low"
            continue
        rec["weights_used"] = use
        rec["score"] = round(score, 6)
        rec["coverage"] = round(coverage, 4)
        eligible.append(rec)

    stats["eligible"] = len(eligible)
    if group_only:
        eligible.sort(key=lambda r: r["code"])
        take = eligible
    elif cross:
        # 跨 L1 的分值不可直接比较：先按 L1 分组，再组内按分排序；
        # 无分值（金融地产）排在有分值成员之后
        eligible.sort(key=lambda r: (r.get("score") is None, r.get("l1") or "",
                                     -(r["score"] or 0), r["code"]))
        take = eligible if no_truncate else eligible[:top_n]
    else:
        eligible.sort(key=lambda r: (-(r["score"] or 0), r["code"]))
        take = eligible if no_truncate else eligible[:top_n]
    for idx, rec in enumerate(take, start=1):
        rec["rank"] = idx
    stats["selected"] = len(take)
    stats["capped"] = len(eligible) > len(take)
    breakdown: Dict[str, int] = {}
    for rec in take:
        key = rec.get("l1") or "未分类"
        breakdown[key] = breakdown.get(key, 0) + 1
    stats["l1_breakdown"] = breakdown
    stats["tracks"][pool_def.get("label") or pool] = {
        "eligible": len(eligible), "selected": len(take)}
    return stats


def _row_payload(rec: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "code": rec["code"],
        "name": rec["name"],
        "sector": rec["sector"],
        "l1": rec.get("l1"),
        "is_financial": rec["is_financial"],
        "track": rec.get("track"),
        "score": rec.get("score"),
        "score_status": rec.get("score_status"),
        "coverage": rec.get("coverage"),
        "rank": rec.get("rank"),
        "selected": rec.get("rank") is not None,
        "exclude_reason": rec.get("exclude_reason"),
        "gate": rec.get("gate"),
        "pe": rec.get("pe"),
        "pe_discount": rec.get("pe_discount"),
        "pe_discount_band": rec.get("pe_discount_band"),
        "score_undiscounted": rec.get("score_undiscounted"),
    }


def compute(db, spec: dict, as_of: Optional[date] = None,
            replay_ctx: Optional[dict] = None) -> Dict[str, Any]:
    """计算一个方案的选股结果，返回 {as_of, pool_key, stats, quality, rows}。

    ``as_of`` 早于导入仓快照日时走 PIT 回放（``replay.py`` 重建），不读导入日
    估值/一致预期快照；``replay_ctx`` 可复用同一 as_of 的重建结果，避免逐方案
    重复全表扫描。
    """
    meta = warehouse_meta(db)
    if meta is None:
        raise EngineError("尚未导入宽库：请先在「数据体检」执行导入")
    from core.selection import replay as replay_mod
    if as_of is None:
        as_of = meta.as_of
    if replay_ctx is None and as_of != meta.as_of:
        replay_ctx = replay_mod.context(db, as_of)
    pit_mode = replay_ctx is not None

    pool = spec["pool_key"]
    industry = sector_pools.is_industry_pool(pool)
    pool_def: Optional[dict] = None
    if industry:
        try:
            pool_def = sector_pools.get_pool(pool)
        except sector_pools.SectorPoolError as exc:
            raise EngineError(str(exc)) from None
        group_only = bool(spec.get("group_only") or pool_def.get("group_only"))
    elif pool in POOL_LABELS:
        group_only = False
    else:
        raise EngineError("暂不支持该池：%s" % pool)

    if pit_mode:
        recs = replay_mod.copy_records(replay_ctx)
    else:
        recs = _build_records(db, meta)
    universe_total = len(recs)
    membership_used = None
    if industry:
        if pit_mode:
            member_codes, membership_used, _fallback = \
                sector_pools.membership_codes_asof(db, as_of, pool)
        else:
            sector_pools.ensure_membership(db, meta.as_of)
            member_codes = sector_pools.membership_codes(db, meta.as_of, pool)
        recs = [r for r in recs if r["code"] in member_codes]
        if not recs:
            raise EngineError("行业池「%s」在 %s 没有成员：先刷新行业归属"
                              % (pool_def["label"], as_of.isoformat()))
    gates = spec.get("gates") or {}
    pe_mode = spec.get("pe_mode") or DEFAULT_PE_MODE
    drops = _apply_gates(recs, as_of, gates, pe_mode=pe_mode,
                         group_only=group_only, pit=pit_mode)

    weights = spec.get("weights") or {}
    keys = set(weights) | set(spec.get("weights_hist") or {})
    if industry:
        if pool_def.get("cross_pool"):
            for item in sector_pools.L1_POOLS.values():
                keys |= set(item.get("weights") or {})
                keys |= set(item.get("weights_hist") or {})
        keys |= set(pool_def.get("weights") or {})
        keys |= set(pool_def.get("weights_hist") or {})
        if group_only:
            # 只分组不打分：仅展示字段，不参与权重
            keys |= {"ep", "bp", "dp_annual", "roe_adj", "div_streak"}
    dropped_factors: List[str] = []
    renormalized: Dict[str, Any] = {}
    l1_map: Optional[Dict[str, Tuple[dict, dict]]] = None
    if not group_only:
        if pit_mode:
            # 回放：整列不可得的因子剔除后按剩余权重归一化，不因 PE/GO 缺失整体失败
            available = {
                key for key in keys
                if key in SCORABLE_FIELDS
                and any(r["gate"] is None and r["factors"].get(key) is not None
                        for r in recs)}
            dropped_factors = sorted(k for k in keys if k not in available)
            if not available:
                raise EngineError(
                    "历史回放：该池全部因子不可得（%s）" % ", ".join(sorted(keys)))
            keys = available
            spec = dict(spec)
            new_w, dropped_w = _renorm_weights(spec.get("weights"), available)
            new_wh, dropped_wh = _renorm_weights(
                spec.get("weights_hist"), available)
            if spec.get("weights") and not new_w:
                raise EngineError("历史回放：该池权重因子全部不可得")
            renormalized = {
                "weights": {"raw": dict(spec.get("weights") or {}),
                            "used": new_w, "dropped": dropped_w},
                "weights_hist": {"raw": dict(spec.get("weights_hist") or {}),
                                 "used": new_wh, "dropped": dropped_wh},
            }
            spec["weights"] = new_w
            spec["weights_hist"] = new_wh
            weights = new_w
            if industry and pool_def.get("cross_pool"):
                l1_map = {}
                for item in sector_pools.L1_POOLS.values():
                    w, dw = _renorm_weights(item.get("weights"), available)
                    wh, dwh = _renorm_weights(item.get("weights_hist"), available)
                    l1_map[item.get("l1") or "其他综合"] = (w, wh)
                    renormalized["l1:" + (item.get("l1") or "其他综合")] = {
                        "used": w, "dropped": dw,
                        "hist_used": wh, "hist_dropped": dwh}
        else:
            missing_columns = []
            for key in sorted(keys):
                if key not in SCORABLE_FIELDS:
                    raise EngineError("字段未接入打分：%s" % key)
                if not any(r["gate"] is None and r["factors"].get(key) is not None
                           for r in recs):
                    missing_columns.append(key)
            if missing_columns:
                raise EngineError("整列无数据，暂不计算：%s"
                                  % ", ".join(missing_columns))

    _winsorize(recs, keys)
    _industry_percentile(recs, keys)
    neutral = _neutralize(recs, keys, enabled=bool(spec.get("neutralize", True)))
    corr_pairs = _correlation_report(recs, keys)

    if industry:
        stats = _rank_select_industry(recs, pool, spec, as_of, l1_map=l1_map)
    else:
        stats = _rank_select(recs, pool, spec, as_of)
    stats.update({
        "universe": len(recs),
        "universe_total": universe_total,
        "gate_drops": drops,
        "gate_passed": sum(1 for r in recs if r["gate"] is None),
        "top_n": int(spec.get("top_n", 100)),
        "track_mode": spec.get("track", "both") if pool == "growth" else None,
        "pe_mode": pe_mode,
        "pit_mode": pit_mode,
        "factors_dropped": dropped_factors,
        "loss_candidates": sum(1 for r in recs if r.get("pe_discount")),
        "loss_selected": sum(1 for r in recs
                             if r.get("pe_discount") and r.get("rank") is not None),
        "neutralization": neutral,
        "high_corr_pairs": corr_pairs,
        "corr_threshold": 0.8,
        "factor_coverage": {
            key: sum(1 for r in recs if r["gate"] is None
                     and r["factors"].get(key) is not None)
            for key in sorted(keys)},
    })
    if industry:
        stats["sector_pool"] = {
            "key": pool,
            "label": pool_def["label"],
            "kind": "l2" if pool_def.get("cross_pool") else "l1",
            "group_only": group_only,
            "no_truncate": bool(spec.get("no_truncate")
                                or pool_def.get("no_truncate")),
            "member_count": len(recs),
            "universe_total": universe_total,
        }

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
        "pe_handling": (
            "严格：PE≤0 或 PE>max_pe 挡在候选外"
            if pe_mode == "strict" else
            "亏损股降权：PE≤0 允许进候选，按亏损收益率(-1/PE)分档对综合得分打折"
            "（<-100×0.90；-100~-30×0.75；-30~-10×0.55；>-10×0.35），PE>max_pe 仍挡"
            if pe_mode == "loss_discount" else
            "周期/农业放宽 PE>0：PE≤0 与 PE 缺失都不剔除，按 PB>0 + 财务序列≥8 期"
            "把关，不做折扣；PE 缺失在因子层自然置空"),
        "validation": "因子稳定性（滚动 IC/分组单调性）见 /api/selection/factor-validation；估值类因子无历史快照，不可回验",
        "survivorship": "universe 只有导入日成分，历史回放暂不支持，存在幸存者偏差",
        "uncalibrated": "内置权重未校准，仅作起点",
        "notes": json.loads(meta.counts_json).get("notes", [])
        if meta.counts_json else [],
    }
    if pit_mode:
        ctx_quality = replay_ctx.get("quality") or {}
        quality.update({
            "mode": "replay",
            "valuation_source": "PIT：PE/PB/一致预期不可重建 → 留空，不用今日值冒充",
            "kline_source": "selection_wh_daily 不复权收盘；as_of 前 252 个交易日（取区间后二次截断）",
            "missing_policy": "回放：整列不可得因子剔除后权重归一化；单票覆盖<50% 不排序；全池皆不可得才报错",
            "survivorship": ("universe 为当前导入名单：已退市票缺失（幸存者偏差）；"
                             "未上市票由 new_listing 闸门剔除"),
            "pit_unavailable": ctx_quality.get("pit_unavailable") or [],
            "pit": ctx_quality,
            "gates_skipped": ["st", "quit", "pe_missing", "pe_out_of_range",
                              "pb_invalid", "float_mv_low"],
            "factors_dropped": dropped_factors,
            "weights_renormalized": renormalized,
        })
        if industry:
            quality["sector_membership"] = (
                "回放按 ≤as_of 的最近一期行业归属取成员（%s）：归属仅有刷新日快照，"
                "存在分类偏差，已在 quality 披露"
                % (membership_used.isoformat() if membership_used else "无"))
    if industry:
        quality["sector_pool"] = {
            "key": pool,
            "label": pool_def["label"],
            "kind": "l2" if pool_def.get("cross_pool") else "l1",
            "group_only": group_only,
            "weights_source": (
                "L2 标签池：按个股所属 L1 主池权重打分，跨 L1 分值不可直接比较，"
                "排序先按 L1 分组" if pool_def.get("cross_pool")
                else "L1 主池专属权重（未经回测校准）"),
            "pe_mode": pe_mode,
        }
        if not pit_mode:
            quality["sector_membership"] = (
                "行业归属只有当前值，无法按公告日重建；历史 as_of 早于首次刷新日时"
                "属于按当前名单回看，存在分类偏差")
        if group_only:
            quality["group_only"] = (
                "金融地产第一阶段只分组不打分：综合分留空，不用 0.5 等占位值冒充；"
                "不套估值/财务门槛")
    return {"as_of": as_of, "pool_key": pool, "stats": stats,
            "quality": quality, "rows": rows}
