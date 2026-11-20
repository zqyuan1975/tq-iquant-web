"""G2 资金池对账：0019 恒等式 (1)–(10)（0021）。

纯计算模块：只读入参、无 IO、无时钟、不改状态。调用方（LiveEngine）在资金
强制开启时按节流周期喂入内存态，失败项决定是否停止新增买入。

恒等式：
 (1) 虚拟现金总额守恒：Σ 组合现金 == 会话账本现金（期望值由调用方给出）
 (2) 资金池自由额度非负：Σ现金 − Σ现金下限 − Σ待回收 ≥ 0（PROTECTED=0）
 (3) 预算不超发：Σ预算 ≤ 自由额度，且单策略 已用+在途 ≤ 预算
 (4) 借据守恒：LoanBook 自检为空，且 Σ未归还 == 账本未归还总额
 (5) 单一贷出方不超发：每贷出方 Σ未归还 ≤ 其历史累计可借出
 (6) 负现金防护：每个组合现金 ≥ 0
 (7) 组合总市值与真实总资产偏差 ≤ 0.5%
 (8) 真实侧额度合计 ≤ 真实可用 × 0.98
 (9) 当日现金滚动：开盘现金 + 成交净额 + 外部净流入 == 当前现金
(10) 会话单票集中度：单票市值 ≤ 阈值 × 会话总市值

缺输入（例如未持久化当日开盘现金）的条目记 ``skipped`` 而不是 ``fail``；
只有 ``fail`` 才触发阻断，避免把“没数据”当成“账不平”。
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional, Set

from core.engine.capital_loan import STATUS_RECALL_PENDING

RECONCILE_VERSION = "capital-reconcile-g2"

#: 失败即停止新买入（0019 §7.5）
BLOCKING_IDS = (2, 3, 4, 6)

_CENT = Decimal("0.01")

_IDENTITY_NAMES = {
    1: "虚拟现金总额守恒",
    2: "资金池自由额度非负",
    3: "预算不超发",
    4: "借据守恒",
    5: "单一贷出方不超发",
    6: "负现金防护",
    7: "组合总市值与真实总资产偏差",
    8: "真实侧额度合计不超真实可用",
    9: "当日现金滚动",
    10: "会话单票集中度",
}


def _dec(value: Any, default: Optional[Decimal] = None) -> Optional[Decimal]:
    if value is None or value == "":
        return default
    try:
        out = Decimal(str(value))
    except (TypeError, ValueError):
        return default
    return out if out.is_finite() else default


def _item(idx: int, status: str, detail: dict, message: str = "") -> dict:
    return {
        "id": idx,
        "name": _IDENTITY_NAMES[idx],
        "status": status,
        "detail": detail,
        "message": message,
    }


def _ok(idx: int, detail: dict) -> dict:
    return _item(idx, "ok", detail)


def _fail(idx: int, detail: dict, message: str) -> dict:
    return _item(idx, "fail", detail, message)


def _skip(idx: int, reason: str) -> dict:
    return _item(idx, "skipped", {"reason": reason}, reason)


def reconcile(
    *,
    portfolios: Iterable[dict],
    loan_book: Any = None,
    expected_cash_total: Any = None,
    real_total_asset: Any = None,
    real_available: Any = None,
    real_quotas: Optional[Dict[int, Any]] = None,
    concentration_limit: Any = None,
    opening_cash: Optional[Dict[int, Any]] = None,
    trade_cash_delta: Optional[Dict[int, Any]] = None,
    external_flow_delta: Optional[Dict[int, Any]] = None,
    tolerance: Any = Decimal("0.005"),
    checks: Optional[Set[int]] = None,
) -> dict:
    """按可用输入逐条计算恒等式；返回 ``{ok, items, block_new_buys, alerts}``。"""
    ports = list(portfolios or [])
    tol = _dec(tolerance, Decimal("0.005")) or Decimal("0.005")
    cash_total = sum((_dec(p.get("cash"), Decimal(0)) for p in ports), Decimal(0))
    equity_total = sum(
        (_dec(p.get("total_value"), Decimal(0)) for p in ports), Decimal(0))
    items: List[dict] = []

    def want(idx: int) -> bool:
        return checks is None or idx in checks

    # (1) 虚拟现金总额守恒
    if not want(1):
        items.append(_skip(1, "未启用该检查"))
    else:
        expected = _dec(expected_cash_total)
        if expected is None:
            items.append(_skip(1, "缺少会话账本现金（未持久化）"))
        else:
            diff = abs(cash_total - expected)
            items.append(
                _ok(1, {"actual": str(cash_total), "expected": str(expected),
                        "diff": str(diff)})
                if diff <= _CENT else
                _fail(1, {"actual": str(cash_total), "expected": str(expected),
                          "diff": str(diff)}, "虚拟现金总额与账本不一致"))

    # (2) 自由额度非负（PROTECTED 在实盘未建模，按 0）
    floors = sum((_dec(p.get("cash_floor"), Decimal(0)) for p in ports),
                 Decimal(0))
    pending_recall = sum(
        (lot.outstanding for lot in (loan_book.open_lots() if loan_book else [])
         if lot.status == STATUS_RECALL_PENDING), Decimal(0))
    free = cash_total - floors - pending_recall
    items.append(
        _ok(2, {"cash_total": str(cash_total), "cash_floor": str(floors),
                "pending_recall": str(pending_recall), "free": str(free)})
        if free >= 0 else
        _fail(2, {"cash_total": str(cash_total), "cash_floor": str(floors),
                  "pending_recall": str(pending_recall), "free": str(free)},
              "资金池自由额度为负"))

    # (3) 预算不超发（未下发预算 → skipped）
    budgets = [p for p in ports if _dec(p.get("budget")) is not None]
    if not budgets:
        items.append(_skip(3, "会话未下发策略预算"))
    else:
        budget_total = sum((_dec(p.get("budget"), Decimal(0)) for p in budgets),
                           Decimal(0))
        over = [
            p.get("portfolio_id") for p in budgets
            if (_dec(p.get("used"), Decimal(0))
                + _dec(p.get("reserved_buy"), Decimal(0)))
            > _dec(p.get("budget"), Decimal(0))
        ]
        failed = budget_total > free or bool(over)
        items.append(
            _fail(3, {"budget_total": str(budget_total), "free": str(free),
                      "over": over}, "预算超出自由额度或单组合超预算")
            if failed else
            _ok(3, {"budget_total": str(budget_total), "free": str(free)}))

    # (4) 借据守恒
    if loan_book is None:
        items.append(_skip(4, "未绑定拼盘/无借据账本"))
    else:
        violations = loan_book.check_invariants()
        outstanding = loan_book.total_outstanding()
        items.append(
            _fail(4, {"violations": violations, "outstanding": str(outstanding)},
                  "借据账本自检不通过")
            if violations else
            _ok(4, {"outstanding": str(outstanding)}))

    # (5) 单一贷出方不超发
    if loan_book is None:
        items.append(_skip(5, "未绑定拼盘/无借据账本"))
    else:
        by_lender: Dict[Any, Decimal] = {}
        lender_owner = {}
        for lot in loan_book.open_lots():
            by_lender[lot.lender_id] = (
                by_lender.get(lot.lender_id, Decimal(0)) + lot.outstanding)
            lender_owner[lot.lender_id] = lot.lender
        over = {
            str(k): str(v) for k, v in by_lender.items()
            if v > loan_book.deposited_for(lender_owner[k])
        }
        items.append(
            _fail(5, {"over": over}, "贷出方未归还超过历史可借出")
            if over else
            _ok(5, {"lenders": len(by_lender)}))

    # (6) 负现金防护
    negative = {
        str(p.get("portfolio_id")): str(_dec(p.get("cash"), Decimal(0)))
        for p in ports if _dec(p.get("cash"), Decimal(0)) < 0
    }
    items.append(
        _fail(6, {"negative": negative}, "存在负现金组合")
        if negative else _ok(6, {"portfolios": len(ports)}))

    # (7) 总市值 vs 真实总资产（0.5%）
    real_asset = _dec(real_total_asset)
    if real_asset is None or real_asset <= 0:
        items.append(_skip(7, "缺少真实账户总资产快照"))
    else:
        diff = abs(equity_total - real_asset)
        items.append(
            _ok(7, {"total_value": str(equity_total),
                    "real_total_asset": str(real_asset), "diff": str(diff)})
            if diff <= real_asset * tol else
            _fail(7, {"total_value": str(equity_total),
                      "real_total_asset": str(real_asset), "diff": str(diff)},
                  "组合总市值偏离真实总资产超过 %.2f%%" % (tol * 100)))

    # (8) 真实侧额度合计 ≤ 真实可用 × 0.98
    available = _dec(real_available)
    if available is None or real_quotas is None:
        items.append(_skip(8, "缺少真实可用/真实侧额度"))
    else:
        quota_total = sum((_dec(v, Decimal(0)) for v in real_quotas.values()),
                          Decimal(0))
        cap = (available * Decimal("0.98")).quantize(_CENT)
        items.append(
            _ok(8, {"quota_total": str(quota_total), "cap": str(cap)})
            if quota_total <= cap + _CENT else
            _fail(8, {"quota_total": str(quota_total), "cap": str(cap)},
                  "真实侧额度合计超过真实可用 98%"))

    # (9) 当日现金滚动
    if opening_cash is None or trade_cash_delta is None:
        items.append(_skip(9, "缺少当日开盘现金/成交净额"))
    else:
        flows = external_flow_delta or {}
        bad = {}
        for p in ports:
            pid = p.get("portfolio_id")
            expected = (_dec(opening_cash.get(pid), Decimal(0))
                        + _dec(trade_cash_delta.get(pid), Decimal(0))
                        + _dec(flows.get(pid), Decimal(0)))
            actual = _dec(p.get("cash"), Decimal(0))
            if abs(actual - expected) > _CENT:
                bad[str(pid)] = {"actual": str(actual),
                                 "expected": str(expected)}
        items.append(
            _fail(9, {"mismatch": bad}, "当日现金滚动不闭合") if bad
            else _ok(9, {"portfolios": len(ports)}))

    # (10) 会话单票集中度
    limit = _dec(concentration_limit)
    if limit is None or limit <= 0 or equity_total <= 0:
        items.append(_skip(10, "未配置集中度阈值/总市值为 0"))
    else:
        by_code: Dict[str, Decimal] = {}
        for p in ports:
            for code, value in (p.get("positions") or {}).items():
                by_code[code] = by_code.get(code, Decimal(0)) + _dec(
                    value, Decimal(0))
        worst_code, worst_value = None, Decimal(0)
        for code, value in sorted(by_code.items()):
            if value > worst_value:
                worst_code, worst_value = code, value
        items.append(
            _ok(10, {"code": worst_code, "value": str(worst_value),
                     "limit": str(limit * equity_total)})
            if worst_value <= limit * equity_total else
            _fail(10, {"code": worst_code, "value": str(worst_value),
                       "limit": str(limit * equity_total)},
                  "单票市值超过会话集中度阈值"))

    failed = [it["id"] for it in items if it["status"] == "fail"]
    blocking = sorted(set(failed) & set(BLOCKING_IDS))
    return {
        "version": RECONCILE_VERSION,
        "ok": not failed,
        "items": items,
        "failed": failed,
        "blocking": blocking,
        "block_new_buys": bool(blocking),
        "alerts": [{"code": "identity_%d_failed" % i} for i in failed],
    }
