# -*- coding: utf-8 -*-
"""评级计算内核：门控状态机与投资看板共用的唯一公式实现。

口径真值源：``docs/plans/2026-11-21-行业评级系统与门控公式比对.md``
§12（逐行公式真值，含 ``WO`` 的 ROUND、``180/3.1416``、``偏离分_空``）
与 §13（超买超卖重定义：250 日滚动 z、不含当日、多空对称、k=2.5）。

相对 ``tqgate_py_v1`` 的三处口径修正（版本号 → ``tqgate_py_v2``）：

1. 方向取 ``m_enh``（原文输出行 ``多头强度:IF(UP_ENH,…)`` 的口径），
   不再存在 M / ENH 双口径；
2. 空头强度补 ``偏离分_空``（原文 ``IF(偏差率<-3,10,IF(偏差率<-1,…))``）；
3. ``< 300`` 根不输出方向（看板侧；门控侧由调用方显式传 ``min_bars=0``
   保持原有 fail-open 行为不变）。

超买超卖不再使用 ``ABS(BIAS_AO)>5 AND EXT_JD_DN AND C<CL``，改为乖离率的
滚动 z 分数；``JD`` 与 ``CH/CL`` 轨道降级为二级过滤展示（``jd_filter`` /
``channel_filter``），不参与必要条件。
"""
from __future__ import annotations

import numpy as np
import pandas as pd

RATING_FORMULA_VERSION = "tqgate_py_v2"
# 最小根数：不足不输出方向（MA288 预热 288 根 + 余量，§12 M3）
MIN_BARS = 300
# 超买超卖：乖离率 250 日滚动 z 分数（不含当日），多空对称，k=2.5（§13 方案 A）
BIAS_Z_WINDOW = 250
BIAS_Z_K = 2.5
# 观察档下限：2.0 <= |z| < 2.5 只作轻度提示，主档（k=2.5）语义不变
BIAS_Z_WATCH = 2.0

RATING_COLUMNS = [
    "date", "bars", "close",
    "ah", "ao", "al", "bo", "zo", "co", "ch", "cl",
    "jd", "bias", "bias_z", "jj",
    "m", "m_enh", "direction", "up_enh", "down_enh",
    "up_str", "dn_str", "cur_str", "level",
    "overbought", "oversold", "jd_filter", "channel_filter",
    "bull_dur", "bear_dur",
    # 六项强度分解（看板详情按需展示；不落库）
    "up_angle", "up_ma", "up_price", "up_bias", "up_pattern", "up_volume",
    "up_ma_count",
    "dn_angle", "dn_ma", "dn_price", "dn_bias", "dn_pattern", "dn_volume",
    "dn_ma_count", "vol_up", "vol_down",
]

_LEVELS = ("strong_bull", "medium_bull", "weak_bull",
           "strong_bear", "medium_bear", "weak_bear", "none")


# ---------------- TDX 数值语义辅助（numpy/pandas，NaN=null） ----------------

def _round_half_away(x):
    """TDX ROUND(x) 单参数：整数舍入，half away from zero。NaN 透传。"""
    a = np.asarray(x, dtype=float)
    out = np.sign(a) * np.floor(np.abs(a) + 0.5)
    if isinstance(x, pd.Series):
        return pd.Series(out, index=x.index)
    return out


def _round2(x):
    """ROUND(x*100)/100 技巧：两位小数。输入 Series 时保 index。"""
    out = _round_half_away(np.asarray(x, dtype=float) * 100.0) / 100.0
    if isinstance(x, pd.Series):
        return pd.Series(out, index=x.index)
    return out


def _ma(s: pd.Series, n: int) -> pd.Series:
    """TDX MA：min_periods=n，不足 NaN。"""
    return s.rolling(n, min_periods=n).mean()


def _ref(s: pd.Series, k: int = 1) -> pd.Series:
    return s.shift(k)


def _count(s: pd.Series, n: int) -> pd.Series:
    """TDX COUNT(cond,n)：滚动窗内条件成立次数（NaN→0）。"""
    return s.fillna(0.0).astype(float).rolling(n, min_periods=n).sum()


def _if(cond: pd.Series, a, b) -> pd.Series:
    """TDX IF：cond 为 NaN/False → b。"""
    c = cond.fillna(False).astype(bool)
    return pd.Series(np.where(c, a, b), index=cond.index)


def _and(*series) -> pd.Series:
    out = series[0].fillna(False).astype(bool)
    for s in series[1:]:
        out &= s.fillna(False).astype(bool)
    return out


def _or(*series) -> pd.Series:
    out = series[0].fillna(False).astype(bool)
    for s in series[1:]:
        out |= s.fillna(False).astype(bool)
    return out


def _gt(s, x) -> pd.Series:
    """NaN 参与比较 → False（TDX null 语义）。"""
    return (s > x).fillna(False)


def _lt(s, x) -> pd.Series:
    return (s < x).fillna(False)


def _le(s, x) -> pd.Series:
    return (s <= x).fillna(False)


def _ge(s, x) -> pd.Series:
    return (s >= x).fillna(False)


def _consecutive(flag: pd.Series) -> pd.Series:
    """连续成立计数（等价 BARSLAST(flag=0) 的持续长度，不成立为 0）。"""
    v = flag.fillna(False).astype(bool).astype(int)
    grp = (v == 0).cumsum()
    return v.groupby(grp).cumsum()


def _level(direction: int, strength: float) -> str:
    if direction in (1, 2):
        if strength >= 75:
            return "strong_bull"
        return "medium_bull" if strength >= 50 else "weak_bull"
    if direction == -1:
        if strength >= 75:
            return "strong_bear"
        return "medium_bear" if strength >= 50 else "weak_bear"
    return "none"


def empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=RATING_COLUMNS)


def compute_rating_frame(ohlcv, min_bars: int = MIN_BARS) -> pd.DataFrame:
    """行情 → 逐 bar 评级明细（列见 ``RATING_COLUMNS``）。

    ``ohlcv``：polars DataFrame，含 ``datetime/Open/High/Low/Close/Volume``，
    内部先按 datetime 升序。``min_bars>0`` 且根数不足时返回空 frame（不输出方向）。
    """
    if ohlcv is None or getattr(ohlcv, "height", 0) < 1:
        return empty_frame()
    if min_bars and ohlcv.height < int(min_bars):
        return empty_frame()

    df = ohlcv.sort("datetime")
    dt_list = df.get_column("datetime").to_list()
    idx = pd.Index(dt_list)

    def _col(name: str) -> pd.Series:
        return pd.Series(df.get_column(name).to_numpy(), index=idx, dtype=float)

    C = _col("Close")
    H = _col("High")
    L = _col("Low")
    O = _col("Open")
    V = _col("Volume")

    # 均线组（ROUND 两位小数）
    ah = _round2(_ma(C, 26) * 1.006)
    ao = _round2(_ma(C, 26))
    al = _round2(_ma(C, 26) * 0.994)
    bo = _round2(_ma(C, 55))
    zo = _round2(_ma(C, 144))
    ch = _round2(_ma(C, 288) * 1.006)
    co = _round2(_ma(C, 288))
    cl = _round2(_ma(C, 288) * 0.994)

    # 信号与角度
    avgp = (C + O + H + L) / 4
    mavgp = _ma(avgp, 26)
    signal = _if(_ge(C, mavgp), 1, _if(_lt(C, mavgp), -1, 0))
    s = _if(_gt(_ma(signal, 3), 0), 1, _if(_lt(_ma(signal, 3), 0), -1, 0))
    s3 = _if(_lt(s, 0), 1, 0)
    wo = _round_half_away(_ma(C, 13))          # §12：WO 必须 ROUND
    wo_prev = _ref(wo, 1)
    jd = (np.arctan((wo / wo_prev - 1) * 100) * 180 / 3.1416)   # 原文 180/3.1416
    jd = pd.Series(jd, index=idx).replace([np.inf, -np.inf], np.nan)

    s1 = _and(_gt(jd, 10), _gt(s, 0), _lt(_ref(s, 1), 0),
              _gt(jd, _ref(jd, 1))).astype(float)
    s2 = _and(_lt(jd, -10), _gt(s, 0), _lt(_ref(s, 1), 0),
              _lt(jd, _ref(jd, 1))).astype(float)

    d = _and(_ge(ah, _ref(ah, 1)), _gt(C, ah), s3 == 0)
    e = _lt(C, al)
    q = _and(_count(_ge(_ref(C, 1), _ref(ah, 1)).astype(float), 5) == 5,
             _gt(L, al), _le(L, ah))
    i = _and(_count(_ge(_ref(C, 1), _ref(co, 1)).astype(float), 5) == 0,
             _gt(H, cl), _le(H, ch))

    m_in = _or(_gt(s1, 0), _gt(s2, 0), d, q)
    m_out = _or(s3.astype(bool), e, i)
    m_now = _if(m_in, 1, _if(m_out, -1, 0))    # M_IN 优先
    m_now_f = m_now.replace(0, np.nan).ffill().fillna(0)
    m1 = m_now.where(m_now != 0, m_now_f)
    jj = _and(_count(_lt(_ref(C, 1), _ref(ah, 1)).astype(float), 5) == 5,
              _lt(ao, bo), _lt(bo, zo), _lt(zo, co), _gt(C, bo))
    m = _if(jj, 2, m1)

    # 增强版（方向与强度的唯一口径）
    s1_enh = _and(_gt(jd, 10), _gt(s, 0), _lt(_ref(s, 1), 0))
    d_enh = _and(_ge(ah, _ref(ah, 1)),
                 _or(_gt(C, ah), _gt(_ref(C, 1), _ref(ah, 1))), s3 == 0)
    m_in_enh = _or(s1_enh, s2.astype(bool), d_enh, q)
    m_now_enh = _if(m_in_enh, 1, _if(m_out, -1, 0))
    m1_enh = m_now_enh.replace(0, np.nan).ffill().fillna(0)
    m_enh = _if(jj, 2, m1_enh)
    up_enh = _or(m_enh == 1, m_enh == 2)
    down_enh = (m_enh == -1)

    # 强度计分（多空各六项，空头含偏离分_空）
    vol_up = _and(_gt(V, _ma(V, 20)), _gt(C, _ref(C, 1)))
    vol_down = _and(_gt(V, _ma(V, 20)), _lt(C, _ref(C, 1)))
    bias = (C - ao) / ao * 100
    bear_pattern = _and(_count(_gt(_ref(C, 1), _ref(al, 1)).astype(float), 5) == 5,
                        _gt(ao, bo), _gt(bo, zo), _gt(zo, co), _lt(C, bo))

    ang_long = _if(_le(jd, 8), 0,
                   _if(_le(jd, 15), (jd - 8) / 7 * 10,
                       np.minimum(20, 10 + (jd - 15) / 5 * 10)))
    dev_long = _if(_gt(bias, 3), 10,
                   _if(_gt(bias, 1), 5 + (bias - 1) * 2.5, 0))
    ma_pts_long = (_lt(ao, bo).astype(float) + _lt(bo, zo).astype(float)
                   + _lt(zo, co).astype(float)) / 3 * 25
    price_pts_long = (_gt(C, ao).astype(float) + _gt(C, bo).astype(float)
                      + _gt(C, zo).astype(float) + _gt(C, co).astype(float)) / 4 * 20
    up_ma_count = (_lt(ao, bo).astype(float) + _lt(bo, zo).astype(float)
                   + _lt(zo, co).astype(float))
    up_pattern = _if(jj, 10, 0)
    up_volume = _if(vol_up, 10, _if(up_enh, 5, 0))
    up_str = pd.Series(np.minimum(
        ang_long.fillna(0) + ma_pts_long.fillna(0) + price_pts_long
        + dev_long.fillna(0)
        + up_pattern
        + up_volume,
        100), index=idx)

    ang_short = _if(_ge(jd, -8), 0,
                    _if(_ge(jd, -15), (-8 - jd) / 7 * 10,
                        np.minimum(20, 10 + (-15 - jd) / 5 * 10)))
    dev_short = _if(_lt(bias, -3), 10,
                    _if(_lt(bias, -1), 5 + (-1 - bias) * 2.5, 0))   # §12 偏离分_空
    ma_pts_short = (_gt(ao, bo).astype(float) + _gt(bo, zo).astype(float)
                    + _gt(zo, co).astype(float)) / 3 * 25
    price_pts_short = (_lt(C, ao).astype(float) + _lt(C, bo).astype(float)
                       + _lt(C, zo).astype(float) + _lt(C, co).astype(float)) / 4 * 20
    dn_ma_count = (_gt(ao, bo).astype(float) + _gt(bo, zo).astype(float)
                   + _gt(zo, co).astype(float))
    dn_pattern = _if(bear_pattern, 10, 0)
    dn_volume = _if(vol_down, 10, _if(down_enh, 5, 0))
    dn_str = pd.Series(np.minimum(
        ang_short.fillna(0) + ma_pts_short.fillna(0) + price_pts_short
        + dev_short.fillna(0)
        + dn_pattern
        + dn_volume,
        100), index=idx)

    cur_str = _if(up_enh, up_str, _if(down_enh, dn_str, 0))

    # 超买超卖（§13 方案 A）：乖离率 250 日滚动 z，参数不含当日
    prev_bias = bias.shift(1)
    z_mean = prev_bias.rolling(BIAS_Z_WINDOW, min_periods=BIAS_Z_WINDOW).mean()
    z_sd = prev_bias.rolling(BIAS_Z_WINDOW, min_periods=BIAS_Z_WINDOW).std(ddof=1)
    bias_z = (bias - z_mean) / z_sd.where(z_sd > 0)
    bias_z = bias_z.replace([np.inf, -np.inf], np.nan)
    overbought = (bias_z >= BIAS_Z_K).fillna(False)
    oversold = (bias_z <= -BIAS_Z_K).fillna(False)

    # 二级过滤展示（不作必要条件）
    jd_filter = pd.Series(
        np.where(jd > 25, 1, np.where(jd < -25, -1, 0)), index=idx)
    channel_filter = pd.Series(
        np.where(C > ch, 1, np.where(C < cl, -1, 0)), index=idx)

    bull_dur = _consecutive(up_enh)
    bear_dur = _consecutive(down_enh)

    dates = [ts.date() if hasattr(ts, "date") else ts for ts in dt_list]
    direction = [int(v) if pd.notna(v) else 0 for v in m_enh]
    out = pd.DataFrame({
        "date": dates,
        "bars": len(dates),
        "close": C.to_numpy(),
        "ah": ah.to_numpy(), "ao": ao.to_numpy(), "al": al.to_numpy(),
        "bo": bo.to_numpy(), "zo": zo.to_numpy(),
        "co": co.to_numpy(), "ch": ch.to_numpy(), "cl": cl.to_numpy(),
        "jd": jd.to_numpy(),
        "bias": bias.to_numpy(),
        "bias_z": bias_z.to_numpy(),
        "jj": jj.to_numpy(),
        "m": [int(v) if pd.notna(v) else 0 for v in m],
        "m_enh": direction,
        "direction": direction,
        "up_enh": up_enh.to_numpy(),
        "down_enh": down_enh.to_numpy(),
        "up_str": up_str.to_numpy(),
        "dn_str": dn_str.to_numpy(),
        "cur_str": cur_str.to_numpy(),
        "level": [_level(d, float(sv) if pd.notna(sv) else 0.0)
                  for d, sv in zip(direction, cur_str)],
        "overbought": overbought.to_numpy(),
        "oversold": oversold.to_numpy(),
        "jd_filter": jd_filter.to_numpy(),
        "channel_filter": channel_filter.to_numpy(),
        "bull_dur": bull_dur.to_numpy(),
        "bear_dur": bear_dur.to_numpy(),
        "up_angle": ang_long.to_numpy(),
        "up_ma": ma_pts_long.to_numpy(),
        "up_price": price_pts_long.to_numpy(),
        "up_bias": dev_long.to_numpy(),
        "up_pattern": up_pattern.to_numpy(),
        "up_volume": up_volume.to_numpy(),
        "up_ma_count": up_ma_count.to_numpy(),
        "dn_angle": ang_short.to_numpy(),
        "dn_ma": ma_pts_short.to_numpy(),
        "dn_price": price_pts_short.to_numpy(),
        "dn_bias": dev_short.to_numpy(),
        "dn_pattern": dn_pattern.to_numpy(),
        "dn_volume": dn_volume.to_numpy(),
        "dn_ma_count": dn_ma_count.to_numpy(),
        "vol_up": vol_up.to_numpy(),
        "vol_down": vol_down.to_numpy(),
    })
    return out[RATING_COLUMNS]
