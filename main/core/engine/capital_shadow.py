"""实盘资金池影子（0021 F3）——只计算「计划 vs 实际」，不产生任何委托。

边界：
- 不读写 ``LiveOrder`` / ``LiveTrade`` / ``LiveDecisionEvent`` / 账户账本；
- 不调用 ``_submit_one_order``，不参与拆单/审批/风控；
- 仅把计划的额度/借据/召回口径与实盘现金/市值并排落一行
  ``live_capital_shadow``，供 F4 成员对照与影子健康使用。

因此「影子前后订单完全一致」由结构保证：本模块没有下单入口。
"""
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional

SHADOW_VERSION = "capital-shadow-f3"


def _dec(value: Any) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _money(value: Optional[Decimal]) -> Optional[str]:
    return None if value is None else str(value.quantize(Decimal("0.01")))


def build_shadow_payload(
    *,
    portfolio_id: int,
    bar_time: Optional[datetime],
    loan_plan: Optional[dict],
    members: Optional[List[dict]],
    cash: Any,
    total_value: Any,
    haircut: Optional[dict],
    drift_fields: Optional[List[str]] = None,
    warning_codes: Optional[List[str]] = None,
) -> dict:
    """构建一行影子快照（纯函数，无 IO）。

    计划侧取 B3 ``loan_model_plan``（本金/借期/借款方）并按 live_haircut 折算
    可用本金；实际侧取实盘现金与总市值。G1 之前实盘无借据，实际借出固定记 0，
    并在 ``loan_ledger`` 标明原因，避免把「尚未实现」误读成「已对账相等」。
    """
    loan_plan = loan_plan or {}
    principal = _dec(loan_plan.get("principal"))
    composite = _dec((haircut or {}).get("composite"))
    if composite is None:
        composite = Decimal(1)
    usable = (principal * composite) if principal is not None else None
    cash_d = _dec(cash)
    total_d = _dec(total_value)
    positions_d = (total_d - cash_d
                   if total_d is not None and cash_d is not None else None)
    gap = (usable - cash_d
           if usable is not None and cash_d is not None else None)

    drift = list(drift_fields or [])
    warnings: List[dict] = [
        {"code": code} for code in (warning_codes or [])
    ]
    if drift:
        warnings.append({"code": "param_drift", "fields": drift})

    return {
        "version": SHADOW_VERSION,
        "bar_time": bar_time.isoformat() if bar_time else None,
        "portfolio_id": portfolio_id,
        "plan": {
            "plan_id": loan_plan.get("plan_id"),
            "lender_strategy_id": loan_plan.get("lender_strategy_id"),
            "borrowers": loan_plan.get("borrowers") or [],
            "principal": _money(principal),
            "loan_term_days": loan_plan.get("due_days"),
            "haircut_composite": str(composite),
            "usable_principal": _money(usable),
            "members": list(members or []),
        },
        "actual": {
            "cash": _money(cash_d),
            "positions_value": _money(positions_d),
            "total_value": _money(total_d),
            # G1 之前实盘没有借据账本：0 是「未实现」，不是「对账相等」。
            "outstanding_loans": "0",
            "loan_ledger": "g1_not_enabled",
        },
        "gap": _money(gap),
        "recall": {
            "state": "not_started",
            "due_days": loan_plan.get("due_days"),
            "notice_days": loan_plan.get("recall_notice_days"),
        },
        "drift_fields": drift,
        "warnings": warnings,
        "orders_unchanged": True,
    }
