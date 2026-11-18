"""同一 bar 内买单的资金审批内核（N1，2026-11-18）。

**判定只有一份**：实盘 ``LiveEngine._allocate_batch_buys`` 与回测多组合共享现金
（``BacktestEngine.run_multi``）都调用本模块，避免两端各写一套「目标份额 / 80%
门槛 / 不做等比缩量」的逻辑而慢慢漂移。

口径（与 2026-11-16 实盘资金基准对齐）：
  * 每笔目标量来自信号生成时的「比例 × 策略资金」（已整手）；
  * 按信号产生顺序逐笔检查剩余可用：可买金额 < 目标份额 × ``MIN_FILL_RATIO``
    （80%）→ **整笔跳过**（记 ``insufficient_cash``），不按可用额等比稀释；
  * 可买金额够门槛 → 按可买额取整手提交（不放大、不超过原目标量）；
  * 组合级护栏（单票集中度/现金留存）通过 ``caps`` 回调注入，本模块只保证
    「先资金门槛、再 caps、扣减可用额、同代码累计」的顺序一致。

默认关闭语义：调用方在功能关闭时不走本模块（回测单组合路径完全不调用），
所以「本模块是否存在」不影响任何历史结果。
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Dict, List, Optional

from .event import OrderEvent

MIN_FILL_RATIO = Decimal("0.8")

REASON_OK = "ok"
REASON_INSUFFICIENT_CASH = "insufficient_cash"
REASON_INSUFFICIENT_LOT = "insufficient_lot"
REASON_CAPPED_ZERO = "capped_to_zero"
REASON_INVALID = "invalid_order"


@dataclass
class AllocationDecision:
    """单笔审批结果（``order`` 已被就地写入最终数量，accepted=False 时不可提交）。"""

    order: OrderEvent
    qty: int
    accepted: bool
    reason: str
    original_qty: int = 0
    target_share: Decimal = Decimal("0")
    available_before: Decimal = Decimal("0")
    remaining_after: Decimal = Decimal("0")
    extra: dict = field(default_factory=dict)


def _price_of_default(order: OrderEvent) -> Optional[Decimal]:
    return order.price


def allocate_batch_buys(
    candidates: List[OrderEvent],
    available: Decimal,
    *,
    lot_of: Callable[[str], int],
    target_share_of: Optional[Callable[[OrderEvent], Optional[Decimal]]] = None,
    caps: Optional[Callable[[OrderEvent, int, Decimal, Decimal], int]] = None,
    sort_key: Optional[Callable[[OrderEvent], tuple]] = None,
    price_of: Optional[Callable[[OrderEvent], Optional[Decimal]]] = None,
    min_fill_ratio: Decimal = MIN_FILL_RATIO,
) -> List[AllocationDecision]:
    """逐笔审批并就地写 ``order.quantity``。

    参数：
      candidates    已过滤（BUY、有价、qty>0）的候选单；不会被本函数修改顺序，
                    除非传入 ``sort_key``（稳定排序后处理）。
      available     共享可用现金（实盘 = 账本现金 − 在途预留 + 真实侧上限；
                    回测多组合 = Σ account.cash − Σ 在途 BUY 金额）。
      lot_of        code -> 每手股数（股票 100 / 转债 10）。
      target_share_of  订单 -> 目标份额金额（None/<=0 时用本单目标金额兜底）。
      caps          组合级护栏回调 (order, qty, avail_now, pending_same_code) -> qty。
      sort_key      处理顺序键（实盘 = (bar_time, seq)；回测 = 原列表序，传 None）。
    """
    price_of = price_of or _price_of_default
    ordered = list(candidates)
    if sort_key is not None:
        ordered.sort(key=sort_key)
    avail = Decimal(available or 0)
    decisions: List[AllocationDecision] = []
    batch_code_value: Dict[str, Decimal] = {}

    for order in ordered:
        original = int(order.quantity or 0)
        price = price_of(order)
        if price is None or price <= 0 or original <= 0:
            decisions.append(AllocationDecision(
                order, 0, False, REASON_INVALID, original_qty=original,
                available_before=avail, remaining_after=avail))
            continue
        need = price * Decimal(original)
        target_share = (
            target_share_of(order) if target_share_of is not None else None)
        if target_share is None or target_share <= 0:
            target_share = need
        lot = int(lot_of(order.stock_code) or 1)

        if avail >= need:
            qty = original
        else:
            affordable = avail
            min_accept = (target_share * min_fill_ratio).quantize(Decimal("0.01"))
            if affordable < min_accept:
                decisions.append(AllocationDecision(
                    order, 0, False, REASON_INSUFFICIENT_CASH,
                    original_qty=original,
                    target_share=target_share, available_before=avail,
                    remaining_after=avail))
                continue
            qty = min(int(affordable / price / lot) * lot, original)
            if qty < lot:
                decisions.append(AllocationDecision(
                    order, 0, False, REASON_INSUFFICIENT_LOT,
                    original_qty=original,
                    target_share=target_share, available_before=avail,
                    remaining_after=avail))
                continue

        pending_value = batch_code_value.get(order.stock_code, Decimal("0"))
        if caps is not None:
            qty = int(caps(order, qty, avail, pending_value) or 0)
        if qty < lot:
            decisions.append(AllocationDecision(
                order, 0, False, REASON_CAPPED_ZERO,
                original_qty=original,
                target_share=target_share, available_before=avail,
                remaining_after=avail))
            continue

        order.quantity = qty
        avail -= price * Decimal(qty)
        batch_code_value[order.stock_code] = (
            pending_value + price * Decimal(qty))
        decisions.append(AllocationDecision(
            order, qty, True, REASON_OK, original_qty=original,
            target_share=target_share,
            available_before=avail + price * Decimal(qty),
            remaining_after=avail))
    return decisions

def compute_capital_pool_weights(portfolios, mode: str) -> List[float]:
    """A1 资金池权重（纯函数，实盘/回测共用口径）。

    - ``equal``：等权 1/n；
    - ``risk_budget``：按 1/组合最大回撤 归一（回撤上限越小 → 风险预算越大；
      缺失/非法回撤按 0.15 兜底，与账户级默认一致）；
    - ``priority``：按组合 ``priority`` 归一（全 0 → 回退等权）；
    - 其他（含 ``off``）：等权（调用方本就不该在 off 时调用，兜底不抛错）。
    """
    n = len(portfolios)
    if n == 0:
        return []
    if mode == "priority":
        vals = []
        for p in portfolios:
            try:
                vals.append(max(0, int(getattr(p, "priority", 0) or 0)))
            except (TypeError, ValueError):
                vals.append(0)
        total = sum(vals)
        if total > 0:
            return [v / total for v in vals]
        return [1.0 / n] * n
    if mode == "risk_budget":
        inv = []
        for p in portfolios:
            rm = getattr(p, "risk_manager", None)
            try:
                dd = Decimal(str(getattr(rm, "max_drawdown", None) or "0.15"))
            except (ArithmeticError, TypeError, ValueError):
                dd = Decimal("0.15")
            if dd <= 0:
                dd = Decimal("0.15")
            inv.append(Decimal("1") / dd)
        total = sum(inv)
        if total > 0:
            return [float(v / total) for v in inv]
        return [1.0 / n] * n
    return [1.0 / n] * n
