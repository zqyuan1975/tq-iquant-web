# -*- coding: utf-8 -*-
"""市场多空门控（2026-11-17 设计文档）。

计算通道（组合级 market_gate_engine 选择）：
- 'python'（默认）：compute_gate_states —— numpy/pandas 精确重实现 TDX 公式
  M 状态机（规格=设计文档附录 A，公式真相源冻结于此语义 + 单测）；
- 'tdx'：客户端公式库按名 compute（TQFormula.compute），解析
  多头强度/空头强度/强度线 输出行（两方向均 0 的日子 carry-forward 前一方向）。

MarketGateTracker 消费 {date: 方向} 序列，查询一律右移一日
（昨日收盘状态管今日开仓，state_for 取严格早于 d 的最近状态日）。
方向语义：1/2=多头、-1=空头、0=未定（放行，无证据不拦）。
"""
from __future__ import annotations

import bisect
import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from itertools import pairwise
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    import polars as pl

logger = logging.getLogger(__name__)

GATE_FORMULA_NAME_DEFAULT = "TQGATE"
GATE_WARMUP_DAYS = 500          # 自然日取数余量（288 交易日 MA 预热）
GATE_TDX_COUNT_BARS = 400       # tdx 模式/实盘日更取的日线根数
# 公式口径版本（冻结入 params_snapshot）：公式/常量任何改动都要改版本号，
# 否则新旧回测结果会被误当作同口径直接比较。
GATE_FORMULA_VERSION = "tqgate_py_v1"

_TDX_LINES = ("多头强度", "空头强度", "强度线")


class MarketGateUnavailableError(ValueError):
    """门控基准取数/公式无输出（可向用户明示的配置或数据问题）。

    继承 ValueError 保持既有"参数/数据错误"语义，同时让 API 层能精确捕获，
    不会把回测内部的其它 ValueError 一并误判成 400。
    """


class MarketGateTracker:
    """逐日门控状态查询（右移一日）。线程约束：回测单线程/实盘主池串行调用。"""

    def __init__(self):
        self._states: dict = {}
        self._strengths: dict = {}
        self._sorted_days: list = []

    def feed_history(self, states: dict, strengths: dict) -> None:
        self._states = {d: int(v) for d, v in (states or {}).items()}
        self._strengths = {d: int(v) for d, v in (strengths or {}).items()}
        self._sorted_days = sorted(self._states)

    def _last_state_before(self, d: date):
        i = bisect.bisect_left(self._sorted_days, d)
        return self._sorted_days[i - 1] if i > 0 else None

    def state_for(self, d: date) -> int:
        sd = self._last_state_before(d)
        return self._states[sd] if sd is not None else 0

    def strength_for(self, d: date) -> int:
        sd = self._last_state_before(d)
        return self._strengths.get(sd, 0) if sd is not None else 0

    def blocks(self, d: date) -> bool:
        return self.state_for(d) == -1

    def switches(self) -> int:
        """相邻状态变化次数（0=未定期不计入变化）。"""
        seq = [self._states[d] for d in self._sorted_days]
        nz = [v for v in seq if v != 0]
        return sum(1 for a, b in pairwise(nz) if a != b)

    def span(self) -> Optional[tuple]:
        if not self._sorted_days:
            return None
        return (self._sorted_days[0], self._sorted_days[-1])


# ---------------- D1 门控时间轴（回测快照 → 只读轴） ----------------

GATE_CLOSED_STATE = -1
GATE_OPEN_STATES = frozenset({1, 2})


@dataclass(frozen=True)
class GateTimeline:
    """逐日门控状态轴（只读）。

    ``states`` 取回测快照里记录的**当日生效值**（引擎已按「昨日收盘状态管今日
    开仓」右移），这里不再平移、也不参考任何后续日期：某天的判定只依赖当天
    记录，因此天然无前视。``days`` 为升序交易日元组。
    """

    days: tuple
    states: dict

    def state_on(self, day: date) -> int:
        return int(self.states.get(day, 0) or 0)

    def closed_on(self, day: date) -> bool:
        return self.state_on(day) == GATE_CLOSED_STATE

    @property
    def closed_days(self) -> frozenset:
        return frozenset(d for d in self.days if self.closed_on(d))

    def runs(self) -> list:
        """连续关闭区间 ``[(start, end), ...]``（按相邻记录日，不插自然日）。"""
        out: list = []
        start = prev = None
        for day in self.days:
            if self.closed_on(day):
                if start is None:
                    start = day
                prev = day
            elif start is not None:
                out.append((start, prev))
                start = prev = None
        if start is not None:
            out.append((start, prev))
        return out

    def coverage(self) -> dict:
        closed = sum(1 for d in self.days if self.closed_on(d))
        opened = sum(1 for d in self.days
                     if self.state_on(d) in GATE_OPEN_STATES)
        return {"days": len(self.days), "closed": closed,
                "open": opened, "neutral": len(self.days) - closed - opened}

    def to_dict(self) -> dict:
        """紧凑摘要（不逐日展开，避免接口膨胀）。"""
        return {
            "days": len(self.days),
            "first_day": self.days[0].isoformat() if self.days else None,
            "last_day": self.days[-1].isoformat() if self.days else None,
            "coverage": self.coverage(),
            "closed_runs": [
                {"start": s.isoformat(), "end": e.isoformat(),
                 "days": sum(1 for d in self.days
                             if s <= d <= e and self.closed_on(d))}
                for s, e in self.runs()
            ],
        }


def build_gate_timeline(rows) -> Optional[GateTimeline]:
    """构造只读门控时间轴。

    ``rows`` 支持 ``{date: state}`` 映射或 ``(date, state)`` 序列（例如日快照的
    ``(snap_date, gate_state)``）；日期或状态为 ``None`` 的记录跳过，同一日期
    重复出现以最后一条为准。无有效记录 → ``None``。
    """
    items = rows.items() if isinstance(rows, Mapping) else rows
    states: dict = {}
    for day, state in items:
        if day is None or state is None:
            continue
        if not isinstance(day, date):
            parsed = _parse_gate_date(day)
            if parsed is None:
                continue
            day = parsed
        states[day] = int(state)
    if not states:
        return None
    return GateTimeline(days=tuple(sorted(states)), states=states)


def gate_off_overlap(lender: GateTimeline,
                     borrower: GateTimeline) -> Optional[dict]:
    """双方门控关闭日的 Jaccard（仅共同交易日，0020 §3.3）。

    = 双方同日关闭数 / 任一方关闭日数；双方在共同区间内都没有关闭日 → 0.0。
    只看两边已记录的当日生效状态，不回看、不补未来状态。
    """
    common = sorted(set(lender.days) & set(borrower.days))
    if not common:
        return None
    both = either = lender_closed = borrower_closed = 0
    for day in common:
        lc, bc = lender.closed_on(day), borrower.closed_on(day)
        lender_closed += 1 if lc else 0
        borrower_closed += 1 if bc else 0
        if lc and bc:
            both += 1
        if lc or bc:
            either += 1
    return {
        "overlap": (both / either) if either else 0.0,
        "common_days": len(common),
        "both_closed": both,
        "either_closed": either,
        "lender_closed": lender_closed,
        "borrower_closed": borrower_closed,
    }


# ---------------- E3 多指数投票（组合级维度扩展） ----------------

def parse_gate_indices(raw) -> list:
    """组合门控指数列表：JSON 数组 / 逗号分隔 / None → 去重保序的代码列表。

    无法解析的项忽略（配置层负责格式校验并返回 400）。
    """
    if not raw:
        return []
    if isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        text = str(raw).strip()
        if not text:
            return []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError):
                return []
            items = parsed if isinstance(parsed, list) else []
        else:
            items = text.split(",")
    out: list = []
    for item in items:
        code = str(item).strip()
        if code and code not in out:
            out.append(code)
    return out


def gate_enabled_for(ps, strategies=None, requested=None) -> bool:
    """组合门控启用判定（E3）：run 级三态覆盖 > 组合开关 > 任一策略 force-on。

    requested 为 None 表示跟随组合配置；显式 bool 为 run 级覆盖。
    策略级 market_gate_mode="on" 允许在组合总开关关闭时单独启用门控。
    """
    if requested is not None:
        return bool(requested)
    if bool(getattr(ps, "market_gate_enabled", False)):
        return True
    return any(
        (getattr(s, "market_gate_mode", None) or "inherit") == "on"
        for s in (strategies or [])
    )


class CompositeGateTracker:
    """E3：多指数投票门控（接口与 MarketGateTracker 对齐）。

    每个指数先按自身 M 状态机得到 (方向, 强度)，再按票型聚合：
    - 空头票：state == -1 且 strength >= strength_threshold；
    - 多头票：state in (1, 2)；
    - vote_mode: any=任一空头即拦 / majority=过半 / all=全部空头才拦。

    右移一日语义由各子 tracker 负责（查询日期 < 状态日），此处不再平移。
    单指数 + any + threshold=0 时与直接使用 MarketGateTracker 判定等价
    （仅在 state 细化值 2→1，方向语义不变）。
    """

    def __init__(self, trackers, vote_mode: str = "any",
                 strength_threshold: int = 0):
        self._trackers = [t for t in (trackers or []) if t is not None]
        self._vote_mode = (vote_mode or "any").lower()
        self._threshold = max(0, int(strength_threshold or 0))
        days = set()
        for t in self._trackers:
            days.update(getattr(t, "_sorted_days", []) or [])
        self._days = sorted(days)

    def _votes(self, d: date) -> tuple:
        bears, bulls = [], []
        for t in self._trackers:
            state = int(t.state_for(d))
            strength = int(t.strength_for(d))
            if state == -1:
                if strength >= self._threshold:
                    bears.append(strength)
            elif state in (1, 2):
                bulls.append(strength)
        return bears, bulls

    def blocks(self, d: date) -> bool:
        if not self._trackers:
            return False
        bears, _ = self._votes(d)
        n = len(self._trackers)
        if self._vote_mode == "all":
            return len(bears) == n
        if self._vote_mode == "majority":
            return len(bears) * 2 > n
        return bool(bears)

    def state_for(self, d: date) -> int:
        if self.blocks(d):
            return -1
        _, bulls = self._votes(d)
        return 1 if bulls else 0

    def strength_for(self, d: date) -> int:
        bears, bulls = self._votes(d)
        return max(bears) if bears else (max(bulls) if bulls else 0)

    def switches(self) -> int:
        seq = [self.state_for(d) for d in self._days]
        nz = [v for v in seq if v != 0]
        return sum(1 for a, b in pairwise(nz) if a != b)

    def span(self) -> Optional[tuple]:
        spans = [t.span() for t in self._trackers if t.span()]
        if not spans:
            return None
        return (min(s[0] for s in spans), max(s[1] for s in spans))


# ---------------- TDX 数值语义辅助（numpy/pandas，NaN=null） ----------------

def _round_half_away(x):
    """TDX ROUND(x) 单参数：整数舍入，half away from zero。NaN 透传。

    np.round 是 half-to-even，不能直接用；输入 Series 时返回同 index 的 Series。
    """
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
    """TDX COUNT(cond,n)：滚动窗内条件成立次数（cond 布尔 0/1，NaN→0）。"""
    v = s.fillna(0.0).astype(float)
    return v.rolling(n, min_periods=n).sum()


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


# ---------------- compute_gate_states（Python 状态机） ----------------

def compute_gate_states(ohlcv: pl.DataFrame) -> tuple[dict, dict]:
    """M 状态机（设计文档附录 A 逐字语义）→ ({date: 方向}, {date: 强度})。

    ohlcv: polars DataFrame，datetime 升序，列 Open/High/Low/Close/Volume。
    """
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
    # SIGNAL:=IF(C>=MAVGP,1,IF(C<MAVGP,-1,0))——MAVGP 为 NaN 时两条件均 False → 0
    signal = _if(_ge(C, mavgp), 1, _if(_lt(C, mavgp), -1, 0))
    s = _if(_gt(_ma(signal, 3), 0), 1, _if(_lt(_ma(signal, 3), 0), -1, 0))
    s3 = _if(_lt(s, 0), 1, 0)
    wo = _round_half_away(_ma(C, 13))
    wo_prev = _ref(wo, 1)
    jd = (np.arctan((wo / wo_prev - 1) * 100) * 180 / 3.1416)  # 附录 A 原文 3.1416
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
    m_now = _if(m_in, 1, _if(m_out, -1, 0))  # M_IN 优先（TDX IF 嵌套顺序）
    # carry-forward：T_M := REF(M_NOW, BARSLAST(M_NOW)) ≡ ffill 最后一个非零值
    m_now_f = m_now.replace(0, np.nan).ffill().fillna(0)
    m1 = m_now.where(m_now != 0, m_now_f)
    jj = _and(_count(_lt(_ref(C, 1), _ref(ah, 1)).astype(float), 5) == 5,
              _lt(ao, bo), _lt(bo, zo), _lt(zo, co), _gt(C, bo))
    m = _if(jj, 2, m1)

    # 增强版（强度分母）
    s1_enh = _and(_gt(jd, 10), _gt(s, 0), _lt(_ref(s, 1), 0))
    d_enh = _and(_ge(ah, _ref(ah, 1)),
                 _or(_gt(C, ah), _gt(_ref(C, 1), _ref(ah, 1))), s3 == 0)
    m_in_enh = _or(s1_enh, s2.astype(bool), d_enh, q)
    m_now_enh = _if(m_in_enh, 1, _if(m_out, -1, 0))
    m1_enh = m_now_enh.replace(0, np.nan).ffill().fillna(0)
    m_enh = _if(jj, 2, m1_enh)
    up_enh = _or(m_enh == 1, m_enh == 2)
    down_enh = (m_enh == -1)

    # 强度计分（空头侧照录原文：不含偏离分_空）
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
    up_str = pd.Series(np.minimum(
        ang_long.fillna(0) + ma_pts_long.fillna(0) + price_pts_long
        + dev_long.fillna(0)
        + _if(jj, 10, 0)
        + _if(vol_up, 10, _if(up_enh, 5, 0)),
        100), index=idx)

    ang_short = _if(_ge(jd, -8), 0,
                    _if(_ge(jd, -15), (-8 - jd) / 7 * 10,
                        np.minimum(20, 10 + (-15 - jd) / 5 * 10)))
    ma_pts_short = (_gt(ao, bo).astype(float) + _gt(bo, zo).astype(float)
                    + _lt(zo, co).astype(float)) / 3 * 25
    price_pts_short = (_lt(C, ao).astype(float) + _lt(C, bo).astype(float)
                       + _lt(C, zo).astype(float) + _lt(C, co).astype(float)) / 4 * 20
    dn_str = pd.Series(np.minimum(
        ang_short.fillna(0) + ma_pts_short.fillna(0) + price_pts_short
        + _if(bear_pattern, 10, 0)
        + _if(vol_down, 10, _if(down_enh, 5, 0)),
        100), index=idx)

    cur_str = _if(up_enh, up_str, _if(down_enh, dn_str, 0))

    states = {}
    strengths = {}
    for ts, mv, sv in zip(dt_list, m, cur_str):
        key = ts.date() if isinstance(ts, datetime) else ts
        states[key] = int(mv) if pd.notna(mv) else 0
        strengths[key] = round(float(sv)) if pd.notna(sv) else 0
    return states, strengths


# ---------------- parse_tdx_gate_outputs（tdx 通道） ----------------

def _parse_gate_date(v) -> Optional[date]:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = str(v).strip()
    try:
        return date.fromisoformat(s)
    except ValueError:
        pass
    try:
        return datetime.strptime(s, "%Y%m%d").date()
    except ValueError:
        return None


def _line_points(stock_data: dict, line_name: str) -> dict:
    """提取单输出行 → {date: value}；缺行/空行 → ValueError。"""
    raw = stock_data.get(line_name)
    pts: dict = {}
    if isinstance(raw, dict):
        dates = raw.get("date") or raw.get("Date") or []
        values = raw.get("value") or raw.get("Value") or []
        if not dates:
            raise ValueError(f"门控公式输出行 {line_name} 为空")
        pts = {
            dd: float(vv) for dd, vv in zip(dates, values) if vv is not None
        }
    elif isinstance(raw, list):
        # TQFormula.compute 原生形态：[{"Date": "YYYYMMDD", "Value": float}, ...]
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            dd = _parse_gate_date(entry.get("Date"))
            vv = entry.get("Value")
            if dd is not None and vv is not None:
                pts[dd] = float(vv)
        if not pts:
            raise ValueError(f"门控公式输出行 {line_name} 为空")
    else:
        raise ValueError(f"门控公式缺少输出行 {line_name}")
    return {_parse_gate_date(dd) or dd: vv for dd, vv in pts.items()}


def parse_tdx_gate_outputs(raw: dict, index_code: str) -> tuple[dict, dict]:
    """tdx 通道公式输出 → ({date: 方向 1|-1|0}, {date: 强度 0-100})。

    raw: TQFormula.compute 返回（兼容 {date:[...], value:[...]} 与
    [{"Date":..., "Value":...}] 两种行形态）；读 多头强度/空头强度/强度线
    三输出行，缺行 → ValueError。
    方向：多头强度>0 → 1；空头强度>0 → -1；两方向均 0 → carry-forward
    前一方向（设计 §2 契约，与 M1 语义一致）。强度取 强度线 原值。
    """
    if not isinstance(raw, dict) or not raw:
        raise ValueError("门控公式执行无返回")
    err = raw.get("ErrorId")
    if err is not None and str(err) != "0":
        raise ValueError(f"门控公式执行失败 ErrorId={err} Error={raw.get('Error')}")
    stock_data = raw.get(index_code)
    if not isinstance(stock_data, dict):
        raise ValueError(f"门控公式输出缺少基准指数 {index_code}")
    lines = {name: _line_points(stock_data, name) for name in _TDX_LINES}

    states: dict = {}
    strengths: dict = {}
    prev = 0
    all_days = sorted(set().union(*(set(l) for l in lines.values())))
    for d in all_days:
        long_v = lines["多头强度"].get(d, 0.0)
        short_v = lines["空头强度"].get(d, 0.0)
        if long_v > 0:
            state = 1
        elif short_v > 0:
            state = -1
        else:
            state = prev
        prev = state
        states[d] = state
        strengths[d] = round(lines["强度线"].get(d, 0.0))
    return states, strengths
