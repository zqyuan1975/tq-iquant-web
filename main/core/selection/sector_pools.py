# -*- coding: utf-8 -*-
"""行业风格池（L1 互斥主池 + L2 可重叠标签池）。

口径：外部规划的 129 个二级行业 → 9 个 L1 主池（互斥）+ 14 个 L2 标签
（可重叠）。映射已内嵌为平台数据（``SECTOR_TABLE``），**运行期不读取外部
目录**；外部映射表只作口径参照与对账样本。

边界与披露：
- 行业分类接口只有当前值，无法按公告日重建。历史归属只能从首次刷新起按
  ``as_of`` 累积到 ``selection_sector_membership``；早于首次刷新日的回看属于
  「按当前名单回看」，存在分类偏差，必须显式披露。
- 金融地产第一阶段**只分组不打分**：综合分留空，不用 0.5 之类占位值冒充；
- 周期 / 农业放宽 ``PE>0`` 硬门槛（底部亏损段 PE 为负），改为 PB>0 + 财务
  序列 ≥8 期，PE 缺失置空而非剔除；
- 行业池与四风格池正交：池 key 统一 ``industry:*``，互不覆盖。
"""

from __future__ import annotations

from typing import Dict, Iterable, List, Optional, Tuple

from core.models.selection import (
    SelectionSectorMembership,
    SelectionWarehouseStock,
)

L1_ORDER: Tuple[str, ...] = (
    "周期", "消费", "科技", "医药", "高端制造", "金融地产", "公用事业",
    "农业", "其他综合",
)

#: 二级行业 → (行业代码, 二级行业名, L1 主池, L2 标签)
SECTOR_TABLE: Tuple[Tuple[str, str, str, str], ...] = (
    ("881002.SH", "煤炭开采", "周期", "上游资源;高股息防御"),
    ("881005.SH", "焦炭加工", "周期", "上游资源"),
    ("881007.SH", "油气开采", "周期", "上游资源;高股息防御"),
    ("881008.SH", "油服工程", "周期", "上游资源"),
    ("881011.SH", "石油化工", "周期", "中游材料"),
    ("881016.SH", "日用化工", "消费", "必选消费;品牌"),
    ("881019.SH", "化纤", "周期", "中游材料"),
    ("881026.SH", "化学原料", "周期", "中游材料"),
    ("881034.SH", "化学制品", "周期", "中游材料"),
    ("881044.SH", "塑料", "周期", "中游材料"),
    ("881051.SH", "橡胶", "周期", "中游材料"),
    ("881055.SH", "农用化工", "周期", "中游材料"),
    ("881062.SH", "冶钢原料", "周期", "上游资源"),
    ("881065.SH", "普钢", "周期", "中游材料"),
    ("881069.SH", "特钢", "周期", "中游材料"),
    ("881071.SH", "工业金属", "周期", "上游资源"),
    ("881075.SH", "贵金属", "周期", "上游资源"),
    ("881078.SH", "能源金属", "周期", "上游资源;产能周期"),
    ("881082.SH", "稀有金属", "周期", "上游资源"),
    ("881087.SH", "金属新材料", "周期", "上游资源"),
    ("881091.SH", "水泥", "周期", "中游材料"),
    ("881094.SH", "玻璃玻纤", "周期", "中游材料"),
    ("881097.SH", "装饰建材", "周期", "中游材料;地产链"),
    ("881104.SH", "非金属材料", "周期", "中游材料"),
    ("881106.SH", "种植业", "农业", "农产品周期"),
    ("881111.SH", "养殖业", "农业", "农产品周期"),
    ("881115.SH", "林业", "农业", "农产品周期"),
    ("881116.SH", "渔业", "农业", "农产品周期"),
    ("881119.SH", "饲料", "农业", "农产品周期"),
    ("881123.SH", "农产品加工", "农业", "必选消费"),
    ("881127.SH", "动物保健", "农业", "农产品周期"),
    ("881130.SH", "酿酒", "消费", "必选消费;品牌"),
    ("881136.SH", "饮料乳品", "消费", "必选消费"),
    ("881139.SH", "调味品", "消费", "必选消费"),
    ("881140.SH", "休闲食品", "消费", "必选消费"),
    ("881144.SH", "食品加工", "消费", "必选消费"),
    ("881151.SH", "纺织制造", "消费", "可选消费"),
    ("881157.SH", "服装家纺", "消费", "可选消费"),
    ("881162.SH", "饰品", "消费", "可选消费"),
    ("881167.SH", "造纸", "周期", "中游材料"),
    ("881171.SH", "包装印刷", "周期", "中游材料"),
    ("881177.SH", "家居用品", "消费", "可选消费;地产链"),
    ("881180.SH", "文娱用品", "消费", "可选消费"),
    ("881184.SH", "白色家电", "消费", "可选消费;地产链;产能周期"),
    ("881187.SH", "黑色家电", "消费", "可选消费;地产链;产能周期"),
    ("881190.SH", "小家电", "消费", "可选消费;产能周期"),
    ("881194.SH", "厨卫电器", "消费", "可选消费;地产链;产能周期"),
    ("881198.SH", "家电零部件", "高端制造", "可选消费"),
    ("881200.SH", "一般零售", "消费", "必选消费"),
    ("881204.SH", "商业物业经营", "金融地产", "地产链"),
    ("881205.SH", "专业连锁", "消费", "可选消费"),
    ("881206.SH", "贸易", "消费", "可选消费"),
    ("881207.SH", "电子商务", "消费", "可选消费;平台"),
    ("881212.SH", "乘用车", "消费", "可选消费;产能周期"),
    ("881215.SH", "商用车", "高端制造", "可选消费;产能周期"),
    ("881218.SH", "汽车零部件", "高端制造", "可选消费;产能周期"),
    ("881224.SH", "汽车服务", "消费", "可选消费"),
    ("881227.SH", "摩托车及其他", "消费", "可选消费"),
    ("881231.SH", "化学制药", "医药", "高研发"),
    ("881234.SH", "生物制品", "医药", "高研发"),
    ("881241.SH", "医疗器械", "医药", "高研发"),
    ("881247.SH", "医疗服务", "医药", "高研发"),
    ("881252.SH", "医药商业", "医药", ""),
    ("881256.SH", "中药", "医药", "品牌"),
    ("881257.SH", "医疗美容", "消费", "可选消费;品牌"),
    ("881261.SH", "电机制造", "高端制造", "产能周期"),
    ("881262.SH", "电池", "高端制造", "产能周期"),
    ("881268.SH", "电网设备", "高端制造", "产能周期"),
    ("881275.SH", "光伏设备", "高端制造", "产能周期"),
    ("881282.SH", "风电设备", "高端制造", "产能周期"),
    ("881285.SH", "其他发电设备", "高端制造", "产能周期"),
    ("881287.SH", "地面兵装", "高端制造", "军工"),
    ("881288.SH", "航空装备", "高端制造", "军工"),
    ("881289.SH", "航天装备", "高端制造", "军工"),
    ("881290.SH", "航海装备", "高端制造", "军工"),
    ("881291.SH", "军工电子", "科技", "军工;高研发"),
    ("881293.SH", "轨交设备", "高端制造", ""),
    ("881294.SH", "通用设备", "高端制造", "产能周期"),
    ("881303.SH", "专用设备", "高端制造", "产能周期"),
    ("881310.SH", "工程机械", "高端制造", "产能周期"),
    ("881313.SH", "自动化设备", "高端制造", ""),
    ("881319.SH", "半导体", "科技", "高研发;产能周期"),
    ("881326.SH", "消费电子", "科技", "可选消费;高研发"),
    ("881329.SH", "光学光电", "科技", "高研发;产能周期"),
    ("881333.SH", "元器件", "科技", "高研发"),
    ("881336.SH", "其他电子", "科技", "高研发"),
    ("881338.SH", "通信设备", "科技", "高研发"),
    ("881344.SH", "电信服务", "科技", "高股息防御"),
    ("881347.SH", "通信工程", "科技", ""),
    ("881352.SH", "IT设备", "科技", "高研发"),
    ("881355.SH", "软件服务", "科技", "高研发"),
    ("881359.SH", "云服务", "科技", "高研发"),
    ("881364.SH", "产业互联网", "科技", "平台"),
    ("881369.SH", "游戏", "科技", "可选消费"),
    ("881370.SH", "广告营销", "科技", "可选消费"),
    ("881373.SH", "影视院线", "科技", "可选消费"),
    ("881376.SH", "数字媒体", "科技", "可选消费"),
    ("881380.SH", "出版业", "科技", "高股息防御"),
    ("881384.SH", "广播电视", "科技", ""),
    ("881386.SH", "全国性银行", "金融地产", "金融单列;高股息防御"),
    ("881389.SH", "地方性银行", "金融地产", "金融单列;高股息防御"),
    ("881394.SH", "证券", "金融地产", "金融单列"),
    ("881395.SH", "保险", "金融地产", "金融单列"),
    ("881396.SH", "多元金融", "金融地产", "金融单列"),
    ("881406.SH", "房屋建设", "金融地产", "地产链"),
    ("881407.SH", "基础建设", "金融地产", "地产链"),
    ("881410.SH", "专业工程", "金融地产", "地产链"),
    ("881415.SH", "工程咨询服务", "金融地产", "地产链"),
    ("881416.SH", "装修装饰", "金融地产", "地产链"),
    ("881418.SH", "房地产开发", "金融地产", "地产链"),
    ("881422.SH", "房产服务", "金融地产", "地产链"),
    ("881427.SH", "体育", "消费", "可选消费"),
    ("881428.SH", "教育培训", "消费", "可选消费"),
    ("881429.SH", "酒店餐饮", "消费", "可选消费"),
    ("881432.SH", "旅游", "消费", "可选消费"),
    ("881436.SH", "专业服务", "消费", "可选消费"),
    ("881442.SH", "公路铁路", "公用事业", "高股息防御"),
    ("881446.SH", "航空机场", "周期", "交运周期"),
    ("881449.SH", "航运港口", "周期", "交运周期"),
    ("881452.SH", "物流", "周期", "交运周期"),
    ("881459.SH", "电力", "公用事业", "高股息防御"),
    ("881467.SH", "燃气", "公用事业", "高股息防御"),
    ("881468.SH", "水务", "公用事业", "高股息防御"),
    ("881470.SH", "环保设备", "公用事业", ""),
    ("881471.SH", "环境治理", "公用事业", ""),
    ("881476.SH", "环境监测", "公用事业", ""),
    ("881478.SH", "综合类", "其他综合", ""),
    ("881479.SH", "电子化学品", "科技", "高研发"),
    ("金融单列", "金融单列(market=49)", "金融地产", "金融单列;高股息防御"),
)

#: 行业名 → (代码, L1, L2 标签元组)
SECTOR_BY_NAME: Dict[str, Tuple[str, str, Tuple[str, ...]]] = {
    name: (code, l1, tuple(t for t in tags.split(";") if t))
    for code, name, l1, tags in SECTOR_TABLE
}

#: L2 标签 → (稳定 code, 中文名)
L2_TAGS: Dict[str, Tuple[str, str]] = {
    "必选消费": ("staple_consumer", "必选消费"),
    "可选消费": ("discretionary_consumer", "可选消费"),
    "品牌": ("brand", "品牌"),
    "上游资源": ("upstream_resource", "上游资源"),
    "中游材料": ("midstream_material", "中游材料"),
    "产能周期": ("capacity_cycle", "产能周期"),
    "高研发": ("high_rd", "高研发"),
    "高股息防御": ("high_dividend_defense", "高股息防御"),
    "金融单列": ("financial_standalone", "金融单列"),
    "地产链": ("realestate_chain", "地产链"),
    "军工": ("defense", "军工"),
    "平台": ("platform", "平台"),
    "交运周期": ("transport_cycle", "交运周期"),
    "农产品周期": ("agri_cycle", "农产品周期"),
}

#: L1 主池：权重为经验起点（未校准），字段全部在 field_dict.SCORABLE_FIELDS 内
L1_POOLS: Dict[str, dict] = {
    "industry:consumer": {
        "key": "industry:consumer", "scheme_code": "ind_consumer",
        "label": "消费", "l1": "消费", "top_n": 200,
        "weights": {"roic_adj": .25, "gm": .20, "earn_qual": .15,
                    "roe_adj": .10, "dps_growth": .10, "dp3": .10,
                    "rev_yoy": .10},
        "description": "品牌与现金流稳定性；不用 GO 增速/PEG 作主因子",
    },
    "industry:tech": {
        "key": "industry:tech", "scheme_code": "ind_tech",
        "label": "科技", "l1": "科技", "top_n": 200, "track": "both",
        "weights": {"g_fwd_2y": .30, "neg_peg_fwd": .30, "rev_yoy": .15,
                    "roic_trend": .10, "g_hist_3y": .10, "earn_qual": .05},
        "weights_hist": {"np_yoy": .35, "neg_peg_hist": .30, "rev_yoy": .20,
                         "g_hist_3y": .10, "roic_trend": .05},
        "description": "前瞻轨优先，GO 未覆盖切历史轨；不调用 PB/股息/波动率",
    },
    "industry:cycle": {
        "key": "industry:cycle", "scheme_code": "ind_cycle",
        "label": "周期", "l1": "周期", "top_n": 200,
        "pe_mode": "loss_allowed",
        "weights": {"bp": .25, "cfp": .20, "roic_adj": .20,
                    "roic_trend": .15, "sp": .10, "size": .10},
        "description": "买周期位置而非绝对便宜；放宽 PE>0 门槛",
    },
    "industry:pharma": {
        "key": "industry:pharma", "scheme_code": "ind_pharma",
        "label": "医药", "l1": "医药", "top_n": 200,
        "weights": {"g_fwd_2y": .20, "roic_adj": .20, "gm": .20,
                    "earn_qual": .15, "neg_peg_fwd": .15, "roe_adj": .10},
        "description": "研发费用化使 PE/ROIC 失真，单列口径；集采风险另行标注",
    },
    "industry:manufacturing": {
        "key": "industry:manufacturing", "scheme_code": "ind_manufacturing",
        "label": "高端制造", "l1": "高端制造", "top_n": 200,
        "weights": {"roic_trend": .25, "rev_yoy": .20, "cfp": .20,
                    "g_fwd_2y": .15, "neg_peg_fwd": .10, "size": .10},
        "description": "ROIC 趋势为主；不调用股息率/分红连续年数/PB 底部",
    },
    "industry:financial": {
        "key": "industry:financial", "scheme_code": "ind_financial",
        "label": "金融地产", "l1": "金融地产", "top_n": 400,
        "group_only": True, "no_truncate": True, "weights": {},
        "description": "第一阶段只分组不打分：综合分留空，展示 PB/EP/股息率/ROE",
    },
    "industry:utility": {
        "key": "industry:utility", "scheme_code": "ind_utility",
        "label": "公用事业", "l1": "公用事业", "top_n": 200,
        "weights": {"dp3": .25, "fcf_cover": .20, "cfp": .20,
                    "roic_adj": .15, "roic_trend": .10, "volatility_rev": .10},
        "description": "高股息可持续性 + 低波；不用 PEG/GO 增速/PB 作主因子",
    },
    "industry:agriculture": {
        "key": "industry:agriculture", "scheme_code": "ind_agriculture",
        "label": "农业", "l1": "农业", "top_n": 200,
        "pe_mode": "loss_allowed", "no_truncate": True,
        "weights": {"bp": .25, "cfp": .20, "roic_adj": .20,
                    "roic_trend": .15, "sp": .10, "size": .10},
        "description": "与周期同权重；必须放宽 PE>0，全量输出不截断",
    },
    "industry:other": {
        "key": "industry:other", "scheme_code": "ind_other",
        "label": "其他综合", "l1": "其他综合", "top_n": 200,
        "weights": {"ep": .25, "bp": .20, "cfp": .20, "roic_adj": .20,
                    "size": .15},
        "description": "兜底池（1 个二级行业）；通用价值+盈利口径",
    },
}

#: L2 标签池：可重叠；打分沿用个股所属 L1 主池权重，池内再排序
L2_POOLS: Dict[str, dict] = {
    "industry:l2:%s" % code: {
        "key": "industry:l2:%s" % code, "scheme_code": "ind_l2_%s" % code,
        "label": label, "l2_tag": tag, "top_n": 200, "cross_pool": True,
        "weights": {},
        "description": "%s标签池（可重叠）；按个股所属 L1 主池权重打分" % label,
    }
    for tag, (code, label) in L2_TAGS.items()
}

POOL_DEFS: Dict[str, dict] = {**L1_POOLS, **L2_POOLS}


class SectorPoolError(ValueError):
    """池 key 非法或成员不可解析。"""


def is_industry_pool(pool_key: str) -> bool:
    return str(pool_key or "").startswith("industry:")


def get_pool(pool_key: str) -> dict:
    try:
        return POOL_DEFS[pool_key]
    except KeyError:
        raise SectorPoolError("未知行业池：%s" % pool_key) from None


def pools_payload() -> List[dict]:
    """前端/接口用池清单（顺序：9 个 L1 在前，14 个 L2 在后）。"""
    out: List[dict] = []
    for key in list(L1_POOLS) + list(L2_POOLS):
        spec = POOL_DEFS[key]
        out.append({
            "key": key, "code": spec["scheme_code"], "label": spec["label"],
            "kind": "l1" if key in L1_POOLS else "l2",
            "l1": spec.get("l1"), "l2_tag": spec.get("l2_tag"),
            "group_only": bool(spec.get("group_only")),
            "no_truncate": bool(spec.get("no_truncate")),
            "top_n": spec.get("top_n", 200),
        })
    return out


def classify(sector_name: Optional[str]) -> Tuple[str, Tuple[str, ...], Optional[str]]:
    """二级行业名 → (L1 主池, L2 标签, 行业代码)；未登记归「其他综合」。"""
    item = SECTOR_BY_NAME.get(sector_name or "")
    if item is None:
        return "其他综合", (), None
    code, l1, tags = item
    return l1, tags, code


def default_spec(pool_key: str) -> dict:
    """行业池默认参数（与四风格共用 gates/neutralize/pe_mode 结构）。"""
    from core.services.selection_service import DEFAULT_GATES

    pool = get_pool(pool_key)
    spec: dict = {
        "pool_key": pool_key,
        "top_n": int(pool.get("top_n", 200)),
        "weights": dict(pool.get("weights") or {}),
        "gates": dict(DEFAULT_GATES),
        "neutralize": True,
        "pe_mode": pool.get("pe_mode", "strict"),
    }
    if pool.get("track"):
        spec["track"] = pool["track"]
        spec["weights_hist"] = dict(pool.get("weights_hist") or {})
    if pool.get("group_only"):
        spec["group_only"] = True
    if pool.get("no_truncate"):
        spec["no_truncate"] = True
    if pool.get("cross_pool"):
        spec["cross_pool"] = True
        spec["l2_tag"] = pool["l2_tag"]
    return spec


def refresh_membership(db, as_of) -> int:
    """按当前导入仓的行业名刷新 ``as_of`` 的行业归属（幂等，只补缺失）。"""
    existing = {
        code for (code,) in db.query(SelectionSectorMembership.code)
        .filter(SelectionSectorMembership.as_of == as_of).all()
    }
    added = 0
    for stock in db.query(SelectionWarehouseStock).all():
        if stock.code in existing:
            continue
        l1, tags, sector_code = classify(stock.sector_name)
        db.add(SelectionSectorMembership(
            as_of=as_of, code=stock.code,
            sector_code=stock.sector_code or sector_code,
            sector_name=stock.sector_name,
            l1_pool=l1, l2_tags=";".join(tags)))
        added += 1
    db.commit()
    return added


def ensure_membership(db, as_of) -> int:
    """该 as_of 没有归属行时自动补一次；返回补的行数（已有则 0）。"""
    count = (db.query(SelectionSectorMembership)
             .filter(SelectionSectorMembership.as_of == as_of).count())
    if count:
        return 0
    return refresh_membership(db, as_of)


def membership_codes(db, as_of, pool_key: str) -> Optional[set]:
    """池成员代码集合；``cross_pool`` 传 L2 标签池 key。"""
    pool = get_pool(pool_key)
    q = db.query(SelectionSectorMembership.code).filter(
        SelectionSectorMembership.as_of == as_of)
    if pool.get("l1"):
        q = q.filter(SelectionSectorMembership.l1_pool == pool["l1"])
    else:
        tag = pool["l2_tag"]
        q = q.filter(SelectionSectorMembership.l2_tags.like("%" + tag + "%"))
    return {code for (code,) in q.all()}


def membership_codes_asof(db, as_of, pool_key: str):
    """按历史 as_of 取池成员：取 ≤ as_of 的最近一期归属，并披露回退。

    行业归属只有刷新日快照（当前两期），没有公告日级别的历史；回放时不得
    静默使用未来名单。返回值 ``(codes, used_as_of, fallback)``，``fallback``
    为真表示没有与 as_of 同日归属、用了别的日期（调用方必须写进 quality）。
    """
    from core.models.selection import SelectionSectorMembership as _M

    days = [d for (d,) in
            db.query(_M.as_of).distinct().order_by(_M.as_of.asc()).all()]
    if not days:
        return set(), None, True
    eligible = [d for d in days if d <= as_of]
    if eligible:
        used = eligible[-1]
        return membership_codes(db, used, pool_key), used, used != as_of
    # as_of 早于首期归属：只能按最早一期回看（未来名单前视），显式标记
    used = days[0]
    return membership_codes(db, used, pool_key), used, True


def membership_counts(db, as_of) -> Dict[str, int]:
    """各 L1 池成员数（供前端/体检展示）。"""
    out: Dict[str, int] = {}
    for l1, count in (db.query(SelectionSectorMembership.l1_pool,
                                __import__("sqlalchemy").func.count())
                      .filter(SelectionSectorMembership.as_of == as_of)
                      .group_by(SelectionSectorMembership.l1_pool).all()):
        out[l1] = count
    return out
