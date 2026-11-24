# -*- coding: utf-8 -*-
"""因子稳定性验证（导入期回算，结果随快照日冻结）。

用宽库内的日线前向收益，对**历史上可 PIT 复算**的财务/量价因子做滚动
rank IC 与分组单调性检验：

* 每 ``step_days`` 个交易日一次调仓，前向 ``horizon_days`` 个交易日收益；
* 因子值只用 ``announce_time <= 调仓日`` 的财报行计算（无未来函数）；
* 分组单调性 = 因子分五组后各组平均前向收益的秩相关。

覆盖不到的因子（EP/BP/现金流/股息率/前瞻增速等依赖历史估值或一致预期快照）
**不验证**，在输出里显式列入 ``unvalidated`` —— 不拿单时点数据假装历史。

已知限制：宽库 K 线为不复权收盘，未做除权复权；股票池为 K 线全样本，
未限定当时上市/退市状态（幸存者偏差小于「当前成分」口径，但非零）。
"""
from __future__ import annotations

import bisect
import json
import math
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

# 能在导入仓/宽库内 PIT 复算的因子
VALIDATED_FACTORS = ("roe_adj", "roic_adj", "gm", "earn_qual",
                     "np_yoy", "rev_yoy", "g_hist_3y", "volatility")

UNVALIDATED_REASONS = {
    "ep": "历史 PE 无快照（宽库仅导入日单时点估值）",
    "bp": "历史 PB 无快照",
    "cfp": "历史市值无快照",
    "sp": "历史市值无快照",
    "dp3": "历史收盘价不复权 + 分红事件时点不完整",
    "dp_annual": "同上",
    "payout_stab": "历史分红率序列可用性未验证",
    "size": "历史总市值仅 GP16 单日缓存",
    "g_fwd_2y": "一致预期仅有导入日单时点快照",
    "g_rev_2y": "一致预期仅有导入日单时点快照",
    "neg_peg_fwd": "依赖前瞻增速与 PE 快照",
    "peg_fwd": "依赖前瞻增速与 PE 快照",
    "neg_peg_hist": "PE 无历史快照",
    "peg_hist": "PE 无历史快照",
    "accel": "历史单季同比序列覆盖不全，未验证",
    "deliver": "历史单季同比序列覆盖不全，未验证",
    "roic_trend": "历史序列可用但未纳入本轮验证，待补",
    "fcf_cover": "历史市值无快照",
    "div_streak": "分红事件时点未做 PIT 校验",
    "dps_growth": "同上",
    "payout": "历史分红率可用性未验证",
    "solvency": "复合因子（流动比率/负债率），未单独验证",
    "volatility": "量价派生，按滚动 60 日年化波动验证",
}

_NULLISH = (None, "", 0, 0.0)


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


def _rank(vals: List[float]) -> List[float]:
    """平均秩（并列取均值），1..n。"""
    order = sorted(range(len(vals)), key=lambda i: vals[i])
    ranks = [0.0] * len(vals)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and vals[order[j + 1]] == vals[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _pearson(xs: List[float], ys: List[float]) -> Optional[float]:
    n = len(xs)
    if n < 3:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    denom = math.sqrt(sum(a * a for a in dx) * sum(b * b for b in dy))
    if not denom:
        return None
    return sum(a * b for a, b in zip(dx, dy)) / denom


def _spearman(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) < 3:
        return None
    return _pearson(_rank(xs), _rank(ys))


def _quintile_means(values: List[float], rets: List[float]) -> List[Optional[float]]:
    idx = sorted(range(len(values)), key=lambda i: values[i])
    n = len(idx)
    out: List[Optional[float]] = []
    for q in range(5):
        lo = int(n * q / 5)
        hi = int(n * (q + 1) / 5)
        if hi <= lo:
            out.append(None)
            continue
        vals = [rets[i] for i in idx[lo:hi]]
        out.append(sum(vals) / len(vals))
    return out


def _load_kline(conn, as_of: str):
    cur = conn.execute(
        "SELECT code, trade_date, close FROM kline_daily "
        "WHERE trade_date <= ? AND close IS NOT NULL "
        "ORDER BY code, trade_date", (as_of,))
    closes: Dict[str, Dict[str, float]] = {}
    dates = set()
    for code, trade_date, close in cur:
        val = _num(close)
        if val is None or val <= 0:
            continue
        closes.setdefault(code, {})[str(trade_date)] = val
        dates.add(str(trade_date))
    return closes, sorted(dates)


def _load_fin(conn, as_of: str, columns: Tuple[str, ...]):
    cols = ", ".join(columns)
    cur = conn.execute(
        "SELECT code, tag_time, announce_time, %s FROM fin_wide "
        "WHERE announce_time <= ? AND announce_time <> '' "
        "ORDER BY code, tag_time" % cols, (as_of,))
    out: Dict[str, List[Tuple[str, str, Dict[str, Optional[float]]]]] = {}
    for row in cur:
        code, tag, announce = str(row[0]), str(row[1]), str(row[2])
        values = {name: _num(row[3 + i]) for i, name in enumerate(columns)}
        out.setdefault(code, []).append((tag, announce, values))
    return out


def _factor_history(rows: List[Tuple[str, str, Dict[str, Optional[float]]]]
                    ) -> Tuple[List[str], List[Dict[str, Optional[float]]]]:
    """按财报行顺序增量计算因子；返回 (announce_dates, factor_dicts)。"""
    dates: List[str] = []
    factors: List[Dict[str, Optional[float]]] = []
    last_roe = last_roic = last_gm = last_eq = None
    annual: List[float] = []
    for idx, (tag, announce, values) in enumerate(rows):
        roe = values.get("FN197")
        if roe is not None:
            last_roe = roe
        roic = values.get("FN329")
        if roic is not None:
            last_roic = roic
        gm = values.get("FN202")
        if gm not in _NULLISH:
            last_gm = gm
        eq = values.get("FN228")
        if eq is not None:
            last_eq = eq
        if tag[4:8] == "1231" and values.get("FN96") not in _NULLISH:
            annual.append(values["FN96"])

        np_yoy = rev_yoy = g_hist = None
        if idx >= 4:
            cur_np = values.get("FN232")
            base_np = rows[idx - 4][2].get("FN232")
            if cur_np not in _NULLISH and base_np not in _NULLISH:
                np_yoy = (cur_np / base_np - 1.0) * 100.0
            cur_rev = values.get("FN230")
            base_rev = rows[idx - 4][2].get("FN230")
            if cur_rev not in _NULLISH and base_rev not in _NULLISH:
                rev_yoy = (cur_rev / base_rev - 1.0) * 100.0
        if len(annual) >= 4 and annual[-4] > 0 and annual[-1] > 0:
            g_hist = ((annual[-1] / annual[-4]) ** (1.0 / 3.0) - 1.0) * 100.0

        dates.append(announce)
        factors.append({
            "roe_adj": last_roe,
            "roic_adj": last_roic if last_roic is not None else last_roe,
            "gm": last_gm,
            "earn_qual": last_eq,
            "np_yoy": np_yoy,
            "rev_yoy": rev_yoy,
            "g_hist_3y": g_hist,
        })
    return dates, factors


def _volatility(closes: Dict[str, float], trade_dates: List[str],
                end_idx: int, window: int = 60) -> Optional[float]:
    lo = max(0, end_idx - window)
    vals = [closes[d] for d in trade_dates[lo:end_idx + 1] if d in closes]
    if len(vals) < 20:
        return None
    rets = [vals[i] / vals[i - 1] - 1.0 for i in range(1, len(vals))]
    mean = sum(rets) / len(rets)
    var = sum((r - mean) ** 2 for r in rets) / len(rets)
    return math.sqrt(var) * math.sqrt(250.0) * 100.0


def compute(conn, as_of: date, horizon_days: int = 20,
            step_days: int = 21, max_periods: int = 24) -> Dict[str, Any]:
    """回算验证摘要；数据不足时返回 ``available=False`` 而不是空指标。"""
    as_of_str = as_of.strftime("%Y%m%d")
    closes, trade_dates = _load_kline(conn, as_of_str)
    if len(trade_dates) < horizon_days + step_days * 3:
        return {"available": False,
                "reason": "K 线历史不足：%d 个交易日" % len(trade_dates)}
    fin = _load_fin(conn, as_of_str, ("FN197", "FN329", "FN202", "FN228",
                                      "FN232", "FN230", "FN96"))

    exit_end = len(trade_dates) - 1 - horizon_days
    indices = list(range(exit_end, max(0, exit_end - step_days * max_periods),
                         -step_days))
    indices.reverse()
    if not indices:
        return {"available": False, "reason": "可用调仓期数不足"}

    series: Dict[str, Tuple[List[str], List[Dict[str, Optional[float]]]]] = {}
    for code, rows in fin.items():
        dts, facs = _factor_history(rows)
        if dts:
            series[code] = (dts, facs)

    per_factor: Dict[str, List[Dict[str, Any]]] = {
        key: [] for key in VALIDATED_FACTORS}
    used_periods = 0
    for idx in indices:
        d = trade_dates[idx]
        exit_d = trade_dates[idx + horizon_days]
        sample: List[Tuple[str, float, Dict[str, Optional[float]]]] = []
        for code, by_date in closes.items():
            c0, c1 = by_date.get(d), by_date.get(exit_d)
            if not c0 or not c1:
                continue
            fmap: Dict[str, Optional[float]] = {}
            item = series.get(code)
            if item:
                dts, facs = item
                pos = bisect.bisect_right(dts, d) - 1
                if pos >= 0:
                    fmap = facs[pos]
            fmap = dict(fmap)
            fmap["volatility"] = _volatility(by_date, trade_dates, idx)
            sample.append((code, c1 / c0 - 1.0, fmap))
        if len(sample) < 100:
            continue
        used_periods += 1
        rets = [s[1] for s in sample]
        for key in VALIDATED_FACTORS:
            vals, rts = [], []
            for _, ret, fmap in sample:
                val = fmap.get(key)
                if val is None:
                    continue
                vals.append(val)
                rts.append(ret)
            if len(vals) < 100:
                continue
            ic = _spearman(vals, rts)
            if ic is None:
                continue
            per_factor[key].append({
                "ic": ic, "quintiles": _quintile_means(vals, rts)})

    factors_out: Dict[str, Any] = {}
    for key, records in per_factor.items():
        if len(records) < 3:
            factors_out[key] = {"n_periods": len(records),
                                "insufficient": True,
                                "reason": "有效调仓期数 < 3，不下结论"}
            continue
        ics = [r["ic"] for r in records]
        mean_ic = sum(ics) / len(ics)
        std = (sum((x - mean_ic) ** 2 for x in ics) / len(ics)) ** 0.5
        quint = [r["quintiles"] for r in records]
        means: List[Optional[float]] = []
        for q in range(5):
            vals = [row[q] for row in quint if row[q] is not None]
            means.append(round(sum(vals) / len(vals) * 100, 3) if vals else None)
        mono = None
        pairs = [(i, m) for i, m in enumerate(means) if m is not None]
        if len(pairs) == 5:
            mono = _spearman([float(p[0]) for p in pairs],
                             [p[1] for p in pairs])
        factors_out[key] = {
            "n_periods": len(ics),
            "mean_ic": round(mean_ic, 4),
            "ic_std": round(std, 4),
            "icir": round(mean_ic / std, 3) if std else None,
            "positive_ratio": round(sum(1 for x in ics if x > 0) / len(ics), 3),
            "quintile_means_pct": means,
            "monotonic_rho": round(mono, 3) if mono is not None else None,
            "monotonic": bool(mono is not None and abs(mono) >= 0.8),
        }

    if not used_periods:
        return {"available": False, "reason": "没有满足样本量的调仓期"}
    return {
        "available": True,
        "as_of": as_of.isoformat(),
        "horizon_days": horizon_days,
        "step_days": step_days,
        "n_periods": used_periods,
        "factors": factors_out,
        "unvalidated": UNVALIDATED_REASONS,
        "method": {
            "price": "宽库 kline_daily 不复权收盘",
            "pit": "财报按 announce_time <= 调仓日过滤",
            "survivorship": "K 线全样本，未限定当时上市/退市状态",
            "adjustment": "未做除权复权，长周期收益可能失真（故取 20 日前向）",
        },
    }
