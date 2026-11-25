"""实盘继承矩阵与 live_haircut（0021 F2）。

回测/拼盘与实盘**零继承**：实盘启动时按当前策略行重建参数，再与拼盘冻结的
``binding_json`` 逐字段比对。本模块只回答两件事，不改任何交易语义：

1. **继承矩阵**：哪些项可以按当前策略行逐位继承（``inherit``）、哪些必须人工
   确认（``confirm``）、哪些实盘不可复现只能打折（``haircut``）。
2. **live_haircut**：五类不可复现因素的默认折扣与合成系数，逐项给出原因，
   供影子对照与后续强制阶段使用。

全部为纯函数：不读数据库、不读时钟；数据库读取由 ``live_service`` 完成并把
策略快照传进来，便于回放与测试。
"""
import json
from typing import Dict, List, Optional

from core.services.pairing_eligibility import _normalize

INHERITANCE_VERSION = "capital-inheritance-f2"
LIVE_HAIRCUT_VERSION = "live-haircut-v1"

#: 与拼盘绑定同源的逐项字段（顺序固定，便于前端稳定展示）。
INHERIT_BINDING_FIELDS = (
    "strategy_id", "name", "role", "formula_id", "period",
    "signal_frequency", "signal_at", "risk_timing",
    "requote_enabled", "requote_max_attempts", "requote_price_mode",
    "requote_max_slippage_ticks", "max_order_amount",
    "risk_chase_ticks", "risk_chase_max_attempts",
    "split_order_enabled", "split_order_threshold",
)

#: 只读继承矩阵。``mode`` 语义：
#:   inherit  = 按当前策略行重建，逐字段比对，漂移即失效；
#:   confirm  = 不可自动等价，须人工确认后才进入实盘；
#:   haircut  = 实盘不可复现，只能按 live_haircut 折扣折算。
INHERITANCE_MATRIX = tuple(
    {"key": field, "mode": "inherit", "note": "按当前策略行重建"}
    for field in INHERIT_BINDING_FIELDS
) + (
    {"key": "principal", "mode": "confirm", "note": "本金/资金底线由人工确认"},
    {"key": "loan_term_days", "mode": "confirm", "note": "借期由人工确认"},
    {"key": "recall_notice_days", "mode": "confirm", "note": "召回通知期由人工确认"},
    {"key": "release_delay_days", "mode": "confirm", "note": "释放延迟由人工确认"},
    {"key": "execution_mode", "mode": "confirm", "note": "会话执行档位由用户选择"},
    {"key": "t_plus_1", "mode": "haircut", "note": "T+1：当日买入不可卖出"},
    {"key": "real_available", "mode": "haircut", "note": "真实可用以柜台为准"},
    {"key": "in_flight", "mode": "haircut", "note": "未成交委托占用额度"},
    {"key": "trade_backfill", "mode": "haircut", "note": "成交回补晚于信号"},
    {"key": "bridge_latency", "mode": "haircut", "note": "桥延迟带来滑点"},
)

#: 五类不可复现因素的默认折扣（保留比例，越小越保守）。
LIVE_HAIRCUT_FACTORS = (
    {"key": "t_plus_1", "factor": 0.95,
     "reason": "T+1：当日买入不可卖出，卖出额度需打折"},
    {"key": "real_available", "factor": 0.98,
     "reason": "真实可用资金以柜台为准，含冻结与在途"},
    {"key": "in_flight", "factor": 0.99,
     "reason": "未成交委托持续占用额度"},
    {"key": "trade_backfill", "factor": 0.99,
     "reason": "成交回补晚于信号，存在时间差"},
    {"key": "bridge_latency", "factor": 0.995,
     "reason": "桥延迟导致委托价与信号价偏差"},
)

#: 人工确认项从拼盘 ``loan`` 读取。
CONFIRM_LOAN_FIELDS = (
    "principal", "loan_term_days", "recall_notice_days", "release_delay_days",
)


def _load(text) -> dict:
    try:
        value = json.loads(text) if text else None
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def normalize_binding(binding) -> dict:
    """把拼盘 ``binding_json``（dict 或 JSON 文本）归一成统一结构。

    返回 ``{lender, borrowers:[{strategy_id, binding}], loan}``：
    L2 单借款方、L3 多借款方、QUEUED 无嵌套绑定三种形态都覆盖。
    """
    data = _load(binding) if isinstance(binding, str) else (binding or {})
    if not isinstance(data, dict):
        data = {}
    lender = data.get("lender") if isinstance(data.get("lender"), dict) else {}
    borrowers: List[dict] = []
    raw_borrowers = data.get("borrowers")
    if isinstance(raw_borrowers, list):
        for item in raw_borrowers:
            if not isinstance(item, dict):
                continue
            inner = item.get("binding")
            borrowers.append({
                "strategy_id": item.get("strategy_id"),
                "binding": inner if isinstance(inner, dict) else item,
            })
    elif isinstance(data.get("borrower"), dict):
        borrower = data["borrower"]
        borrowers.append({
            "strategy_id": borrower.get("strategy_id"),
            "binding": borrower,
        })
    loan = data.get("loan") if isinstance(data.get("loan"), dict) else {}
    return {"lender": lender, "borrowers": borrowers, "loan": loan}


def binding_strategy_ids(binding) -> List[int]:
    """拼盘涉及的全部策略 id（出借方 + 各借款方），去重且保持出现顺序。"""
    bundle = normalize_binding(binding)
    ids: List[int] = []
    candidates = [bundle["lender"]] + [b["binding"] for b in bundle["borrowers"]]
    for item in candidates:
        raw = item.get("strategy_id") if isinstance(item, dict) else None
        try:
            sid = int(raw)
        except (TypeError, ValueError):
            continue
        if sid not in ids:
            ids.append(sid)
    return ids


def build_live_haircut(overrides: Optional[dict] = None) -> dict:
    """五类折扣的默认值、可解释原因与合成系数；非法覆盖回退默认并告警。"""
    overrides = overrides if isinstance(overrides, dict) else {}
    warnings: List[dict] = []
    factors: List[dict] = []
    known = {spec["key"] for spec in LIVE_HAIRCUT_FACTORS}
    for key in overrides:
        if key not in known:
            warnings.append({"code": "live_haircut_unknown", "key": key})
    composite = 1.0
    for spec in LIVE_HAIRCUT_FACTORS:
        default = float(spec["factor"])
        value = default
        overridden = False
        if spec["key"] in overrides:
            raw = overrides.get(spec["key"])
            try:
                parsed = float(raw)
            except (TypeError, ValueError):
                parsed = None
            if parsed is None or not (0 < parsed <= 1):
                warnings.append({
                    "code": "live_haircut_invalid",
                    "key": spec["key"], "value": raw,
                    "fallback": default,
                })
            else:
                value = parsed
                overridden = True
        composite *= value
        factors.append({
            "key": spec["key"], "factor": round(value, 6),
            "default": round(default, 6), "overridden": overridden,
            "reason": spec["reason"],
        })
    return {
        "version": LIVE_HAIRCUT_VERSION,
        "factors": factors,
        "composite": round(composite, 6),
        "warnings": warnings,
    }


def _compare_side(label: str, plan_binding: dict, live: dict,
                  items: List[dict], drift: List[str],
                  warnings: List[dict]) -> None:
    """逐字段比对单侧（出借方或某个借款方）的冻结绑定与实盘策略快照。"""
    for field in INHERIT_BINDING_FIELDS:
        plan_value = plan_binding.get(field)
        live_value = live.get(field)
        if plan_value is None:
            warnings.append({
                "code": "inheritance_field_missing",
                "side": label, "field": field,
            })
            status = "missing"
        elif _normalize(plan_value) == _normalize(live_value):
            status = "match"
        else:
            status = "drift"
            drift.append("%s.%s" % (label, field))
        items.append({
            "side": label, "field": field, "mode": "inherit",
            "plan": plan_value, "live": live_value, "status": status,
        })


def build_report(binding, current_snapshots: Dict[int, dict], *,
                 pairing_version: Optional[str] = None,
                 overrides: Optional[dict] = None) -> dict:
    """生成只读继承报告：矩阵 + 逐字段比对 + 人工确认项 + live_haircut。

    ``current_snapshots``：``{strategy_id: binding_snapshot(strategy)}``，
    由调用方按当前策略行构建。缺失策略不抛异常，落 ``strategy_missing`` 告警。
    """
    bundle = normalize_binding(binding)
    items: List[dict] = []
    drift: List[str] = []
    warnings: List[dict] = []

    sides = [("lender", bundle["lender"])]
    for index, borrower in enumerate(bundle["borrowers"]):
        sides.append(("borrowers[%d]" % index, borrower["binding"]))

    for label, plan_binding in sides:
        raw_id = plan_binding.get("strategy_id")
        try:
            sid = int(raw_id)
        except (TypeError, ValueError):
            sid = None
        if sid is None:
            warnings.append({"code": "strategy_id_missing", "side": label})
            continue
        live = current_snapshots.get(sid)
        if live is None:
            warnings.append({
                "code": "strategy_missing", "side": label, "strategy_id": sid,
            })
            items.append({
                "side": label, "field": "strategy_id", "mode": "inherit",
                "plan": sid, "live": None, "status": "missing",
            })
            continue
        _compare_side(label, plan_binding, live, items, drift, warnings)

    loan = bundle["loan"]
    for field in CONFIRM_LOAN_FIELDS:
        value = loan.get(field)
        if value is None or value == "":
            warnings.append({
                "code": "inheritance_field_missing",
                "side": "loan", "field": field,
            })
            status = "missing"
        else:
            status = "confirm"
        items.append({
            "side": "loan", "field": field, "mode": "confirm",
            "plan": value, "live": None, "status": status,
        })

    haircut = build_live_haircut(overrides)
    return {
        "enabled": True,
        "version": INHERITANCE_VERSION,
        "pairing_version": pairing_version,
        "matrix": [dict(row) for row in INHERITANCE_MATRIX],
        "items": items,
        "drift_fields": drift,
        "drift_count": len(drift),
        "haircut": haircut,
        "warnings": warnings + haircut["warnings"],
    }
