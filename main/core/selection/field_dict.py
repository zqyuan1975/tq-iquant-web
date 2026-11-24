# -*- coding: utf-8 -*-
"""条件选股字段字典（A0-3）。

定位：只登记「TQ 实测可得 + 语义已在方案中写清」的字段，机制留扩展位。
D3（首发字段集宽窄）尚未定稿，所以本文件只做三件事：
1. 声明字段的来源、单位、缺失语义与 PIT 可用性；
2. 拒绝未登记字段（宁可启动失败，也不让来源不明的列混进打分）；
3. 给出字段集指纹，供快照判定「两次结果是否可比」。

三条硬约束：
- TQ 对财务缺失统一**填 0**，因此每个数值字段必须显式声明 ``zero_is_missing``；
- ``pit_safe=False`` 的字段**只能用于当期**，不得进入历史快照（回看会泄漏未来）；
- 单位在字典里固定，打分链路不做单位猜测（万元/亿元混用是历史踩过的坑）。

来源（source）取值：
``fn`` 财报序列 / ``go`` 一致预期 / ``gp`` 历史逐日指标 /
``kline`` 日线 / ``dividend`` 分红送配 / ``more_info`` 行情附加 / ``list`` 名单与分类。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import pandas as pd

#: 字段集版本。D3 定稿并冻结首发字段集时改为正式版本号；
#: 在此之前所有快照都带 draft 标记，不承诺跨版本可比。
FIELD_SET_VERSION = "draft-2026-11-24"

GROWTH = "growth"
QUALITY = "quality"
VALUE = "value"
DIVIDEND = "dividend"
ALL_POOLS: Tuple[str, ...] = (GROWTH, QUALITY, VALUE, DIVIDEND)

SOURCE_FN = "fn"
SOURCE_GO = "go"
SOURCE_GP = "gp"
SOURCE_KLINE = "kline"
SOURCE_DIVIDEND = "dividend"
SOURCE_MORE_INFO = "more_info"
SOURCE_LIST = "list"


class UnregisteredField(KeyError):
    """字段未登记：调用方必须在 ``field_dict`` 里先声明，否则拒绝使用。"""


@dataclass(frozen=True)
class FieldSpec:
    key: str            # 平台内部字段名（英文）
    source: str         # 来源，见模块 docstring
    column: str         # TQ 原始列名，复合字段写主列，其余见 note
    unit: str           # 单位（中文，直接用于前端展示）
    zero_is_missing: bool   # TQ 填 0 是否应视为缺失
    pit_safe: bool           # 是否可用于历史快照（False = 只有当前值）
    pools: Tuple[str, ...] = ALL_POOLS
    note: str = ""


def _spec(key, source, column, unit, zero_is_missing, pit_safe,
          pools=ALL_POOLS, note=""):
    return FieldSpec(key, source, column, unit, zero_is_missing, pit_safe,
                     pools, note)


#: 首发字段集。逐条对应外部方案的字段字典（TQ 实测可得列），
#: 复合字段只登记主列，派生公式写在 ``note``。
_SPECS: Tuple[FieldSpec, ...] = (
    # --- L0：名单 / 分类 / 时点 ---
    _spec("code", SOURCE_LIST, "get_stock_list(market=5)", "-", False, True,
          note="母池代码；market=23/24/25/28 可收窄到指数成分（无历史）"),
    _spec("name", SOURCE_LIST, "list_type=1", "-", False, True, note="证券名称"),
    _spec("sector_l2", SOURCE_LIST, "get_stock_list(market=17)", "类", False, True,
          note="128 个二级行业；金融股 market=49 单列"),
    _spec("hq_date", SOURCE_MORE_INFO, "HqDate", "日期", False, False,
          note="行情时点；停牌时落后于当日，可作停牌间接判据"),
    _spec("announce_time", SOURCE_FN, "FN314", "YYMMDD", False, True,
          note="公告日，PIT 硬闸锚点；FN313/FN315 为平行数组"),

    # --- L0：估值与规模 ---
    _spec("pe_ttm", SOURCE_MORE_INFO, "StaticPE_TTM", "倍", False, False,
          note="硬门槛：<=0 或 >500 剔除；只有当前值"),
    _spec("pb_mrq", SOURCE_MORE_INFO, "PB_MRQ", "倍", False, False,
          note="硬门槛：<=0 剔除；只有当前值"),
    _spec("zsz", SOURCE_MORE_INFO, "Zsz", "亿元", False, False,
          note="CFP/SP 分母；只有当前值"),
    _spec("total_mv_wan", SOURCE_GP, "GP16", "万元", True, True,
          note="当日总市值，按 year+mmdd 分片取；D2 选定口径"),

    # --- L1：派生估值 ---
    _spec("ep", SOURCE_MORE_INFO, "StaticPE_TTM", "1/倍", True, False,
          note="1/PE_TTM；PE<=0 已在硬门槛剔除"),
    _spec("bp", SOURCE_MORE_INFO, "PB_MRQ", "1/倍", True, False, pools=(VALUE,),
          note="1/PB_MRQ"),
    _spec("cfp", SOURCE_FN, "FN307", "1/倍", True, True, pools=(VALUE, DIVIDEND),
          note="FN307(元) / (Zsz(亿元)*1e8)；量纲勿混用"),
    _spec("sp", SOURCE_FN, "FN319", "1/倍", True, True, pools=(VALUE,),
          note="FN319(万元) / (Zsz(亿元)*1e4)"),

    # --- L1：红利 ---
    _spec("dp_annual", SOURCE_DIVIDEND, "Bonus", "%", False, True, pools=(DIVIDEND,),
          note="sum(Bonus/10)/当年均价；一年多次分红须先年度聚合"),
    _spec("div_streak", SOURCE_DIVIDEND, "Bonus", "年", False, True, pools=(DIVIDEND,),
          note="按年度聚合 Bonus>0 后向前连续计数；<3 淘汰"),
    _spec("dp3", SOURCE_DIVIDEND, "Bonus", "%", False, True, pools=(DIVIDEND,),
          note="近 3 年平均股息率；不足 3 年淘汰"),
    _spec("payout", SOURCE_FN, "FN337", "%", True, True, pools=(DIVIDEND,),
          note="FN337(tag_time 以 1231 结尾)；缺失时用 DPS*FN238/FN96 自算"),
    _spec("dps_growth", SOURCE_DIVIDEND, "Bonus", "%/年", False, True, pools=(DIVIDEND,),
          note="逐年 DPS 回归斜率/首年 DPS；≥3 年"),
    _spec("fcf_cover", SOURCE_FN, "FN107", "倍", True, True, pools=(QUALITY, DIVIDEND),
          note="(FN107-FN114)/现金分红总额；FN125 含利息须标注"),
    _spec("div_gt_raise", SOURCE_GO, "GO42", "布尔", False, False, pools=(DIVIDEND,),
          note="GO42>GO43；不否决但标注；GO 无历史"),

    # --- L1：质量 ---
    _spec("roic_adj", SOURCE_FN, "FN329", "%", True, True,
          note="mean(近12季)-std(近12季)；金融股用 FN197 代理并标注"),
    _spec("roe_adj", SOURCE_FN, "FN197", "%", True, True, pools=(QUALITY,),
          note="mean(近12季)-std(近12季)"),
    _spec("roe_yoy", SOURCE_FN, "FN197", "pp", True, True, pools=(GROWTH, QUALITY),
          note="FN197[t]-FN197[t-4]；不足 5 期置空"),
    _spec("gm", SOURCE_FN, "FN202", "%", True, True, pools=(QUALITY,),
          note="FN202 水平 + 近 8 季趋势；金融股缺则分组"),
    _spec("earn_qual", SOURCE_FN, "FN228", "倍", True, True, pools=(QUALITY,),
          note="mean(FN228,(FN107-FN86)/FN40)；eq2 分支的 FN86/FN40 尚未取数，恒空"),
    _spec("solvency", SOURCE_FN, "FN170", "%", True, True, pools=(QUALITY,),
          note="FN170/FN210/FN327 合成；金融股不适用"),
    _spec("govern", SOURCE_FN, "FN336", "布尔", False, True, pools=(QUALITY,),
          note="FN336 in {1,2} 且 GP20<阈值；不否决但标注"),

    # --- L1：成长（前瞻轨） ---
    _spec("g_fwd_2y", SOURCE_GO, "GO8", "%", True, False, pools=(GROWTH,),
          note="(GO10/GO8)^(1/2)-1；GO 无历史，历史快照整轨停用"),
    _spec("g_rev_2y", SOURCE_GO, "GO11", "%", True, False, pools=(GROWTH,),
          note="(GO13/GO11)^(1/2)-1"),
    _spec("peg_fwd", SOURCE_GO, "GO23", "倍", True, False, pools=(GROWTH,),
          note="GO23/g_fwd_2y(%)；g_fwd<=0 置空"),
    _spec("target_upside", SOURCE_GO, "GO3", "%", True, False, pools=(),
          note="GO3/收盘价-1；覆盖低，仅展示不入模"),

    # --- L1：成长（历史轨） ---
    _spec("g_hist_3y", SOURCE_FN, "FN96", "%", True, True, pools=(GROWTH,),
          note="(FN96[2025]/FN96[2022])^(1/3)-1；点不足置空不外推"),
    _spec("g_hist_4y", SOURCE_FN, "FN96", "%", True, True, pools=(GROWTH,),
          note="同上，跨度 4 年"),
    _spec("g_hist_5y", SOURCE_FN, "FN96", "%", True, True, pools=(GROWTH,),
          note="同上，跨度 5 年"),
    _spec("np_yoy", SOURCE_FN, "FN232", "%", True, True, pools=(GROWTH,),
          note="FN232[t]/FN232[t-4]-1；基期<=0 置空"),
    _spec("rev_yoy", SOURCE_FN, "FN230", "%", True, True, pools=(GROWTH,),
          note="FN230[t]/FN230[t-4]-1；基期<=0 置空"),
    _spec("accel", SOURCE_FN, "FN184", "pp/季", True, True, pools=(GROWTH,),
          note="近 8 季 FN184 同比线性斜率；不足置空"),
    _spec("roic_trend", SOURCE_FN, "FN329", "pp/季", True, True, pools=(GROWTH, DIVIDEND),
          note="FN329 近 12 季斜率；金融股用 ROE 代理"),
    _spec("deliver", SOURCE_FN, "FN285", "pp", True, True, pools=(GROWTH,),
          note="mean(FN285,FN286)-FN184；无预告则中性 0.5"),
    _spec("peg_hist", SOURCE_MORE_INFO, "StaticPE_TTM", "倍", True, False, pools=(GROWTH,),
          note="PE_TTM/FN184(%)；增速<=0 置空"),

    # --- L1：风险（K 线 / GP 时序） ---
    _spec("volatility", SOURCE_KLINE, "Close", "%", False, True, pools=(QUALITY, DIVIDEND),
          note="std(日收益)*sqrt(250)；<60 日剔除"),
    _spec("max_drawdown", SOURCE_KLINE, "Close", "%", False, True, pools=(DIVIDEND,),
          note="min(Close/cummax(Close)-1)；<60 日剔除"),
    _spec("dp_5y_pct", SOURCE_GP, "GP21", "0-1", False, True, pools=(DIVIDEND,),
          note="GP21 近 5 年百分位；时序短则留空"),

    # --- 辅助：股本与原始财务（供 PIT 与面值计算） ---
    _spec("total_shares", SOURCE_FN, "FN238", "股", True, True,
          note="总股本；与 GP16/收盘价互相校验"),
    _spec("float_shares", SOURCE_FN, "FN239", "股", True, True,
          note="流通股本"),
    _spec("net_profit", SOURCE_FN, "FN96", "元", True, True,
          note="归母净利润（年报点用于 CAGR）"),
    _spec("ocf", SOURCE_FN, "FN107", "元", True, True,
          note="经营活动现金流净额"),
    _spec("capex", SOURCE_FN, "FN114", "元", True, True,
          note="资本开支；FCF = FN107 - FN114"),
    _spec("audit_opinion", SOURCE_FN, "FN336", "枚举", False, True,
          note="审计意见；>=3 视为非标"),
    _spec("pledge_ratio", SOURCE_GP, "GP20", "%", True, True,
          note="质押比例（治理项辅助）"),
)

FIELD_REGISTRY: Dict[str, FieldSpec] = {s.key: s for s in _SPECS}


def get_field(key: str) -> FieldSpec:
    """取字段定义；未登记直接抛 ``UnregisteredField``。"""
    try:
        return FIELD_REGISTRY[key]
    except KeyError:
        raise UnregisteredField(
            "字段 %r 未在 field_dict 登记；新增字段必须先声明来源/单位/缺失语义"
            % (key,)) from None


def require_fields(keys: Iterable[str], source: str = "",
                   pool: str = "") -> List[FieldSpec]:
    """批量校验：字段必须已登记，且来源/适用池匹配（留空表示不校验）。"""
    out: List[FieldSpec] = []
    for key in keys:
        spec = get_field(key)
        if source and spec.source != source:
            raise ValueError(
                "字段 %r 来源为 %s，与请求来源 %s 不符" % (key, spec.source, source))
        if pool and spec.pools and pool not in spec.pools:
            raise ValueError(
                "字段 %r 不适用于 %s 池（适用：%s）"
                % (key, pool, ",".join(spec.pools)))
        out.append(spec)
    return out


def validate_columns(columns: Iterable[str], source: str) -> None:
    """校验一批 TQ 原始列名都能映射到已登记字段；未登记即拒绝。"""
    known = {s.column for s in _SPECS if s.source == source}
    unknown = sorted({c for c in columns if c not in known})
    if unknown:
        raise UnregisteredField(
            "来源 %s 存在未登记列：%s" % (source, ", ".join(unknown)))


def missing_mask(df: pd.DataFrame, keys: Iterable[str]) -> pd.DataFrame:
    """返回与 ``df[keys]`` 同形的布尔表（True = 缺失）。

    缺失判定：NaN / None 一律缺失；``zero_is_missing`` 的字段额外把 0 视为缺失
    （TQ 对财务缺失统一填 0，若不屏蔽会拿到“真实为零”的假信号）。
    """
    cols = list(keys)
    mask = df[cols].isna()
    for key in cols:
        spec = get_field(key)
        if spec.zero_is_missing:
            mask[key] = mask[key] | (df[key] == 0)
    return mask


def field_set_fingerprint(keys: Iterable[str],
                          version: str = FIELD_SET_VERSION) -> str:
    """字段集指纹：同版本同字段集 → 同一指纹，用于判定两次快照是否可比。"""
    normalized = ",".join(sorted(set(keys)))
    raw = "%s|%s" % (version, normalized)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
