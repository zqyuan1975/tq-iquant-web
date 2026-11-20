"""会话池分配与仲裁（0021 E1）：带上下限的水位归一化 + 同票配额。

权威关系：公式以 0018 §5.2/§5.3 与 0019 Step 8 为准，实施范围以 0021 E1 为准。

- 纯计算：不读库、不读时钟、不做 IO；同一输入必得同一输出。
- 金额用 ``Decimal``，``to_dict()`` 一律输出字符串。
- 默认关闭：本模块不接入 ``run_multi`` / 实盘，调用方自行决定何时使用。
- 水位归一化只决定「本窗口授权额度」，不是现金划转；业务上限（需求、毛额度、
  可用现金、目标仓位）在最后取 min，未分完的额度留在池里。
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Dict, List, Sequence, Tuple

from core.engine.capital_contracts import dec_str, to_decimal

SESSION_POOL_VERSION = "session-pool-e1"

DEFAULT_MIN_SHARE = Decimal("0.05")  # 0018 §5.2 默认下限 5%
DEFAULT_MAX_SHARE = Decimal("0.50")  # 0018 §5.2 默认上限 50%
DEFAULT_LOT = 100  # A 股整手

_EPS = Decimal("0.000001")


def _clamp(value: Decimal, low: Decimal, high: Decimal) -> Decimal:
    return max(low, min(high, value))


def _non_negative(value: Any) -> Decimal:
    d = to_decimal(value)
    if d is None:
        return Decimal(0)
    return d if d > 0 else Decimal(0)


# ---------------------------------------------------------------------------
# 会话池：水位归一化
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class PoolMember:
    """单个组合的分配输入（0018 §5.2）。

    ``raw_i = λ × demand_share + (1 − λ) × risk_share`` 由调用方算好传入；
    ``need_i`` 是理论最大需求，``max_gross_i`` 已含风险因子时直接传最终值。
    ``active=False`` 的成员不参与分配（额度留在池里）。
    """

    portfolio_id: Any
    raw_weight: Any = 0
    need: Any = None
    max_gross: Any = None
    available: Any = None
    utilization_cap: Any = None
    active: bool = True

    def __post_init__(self) -> None:
        if self.portfolio_id is None:
            raise ValueError("portfolio_id 不能为空")


@dataclass(frozen=True)
class MemberBudget:
    portfolio_id: Any
    floor: Decimal
    cap: Decimal
    water_filled: Decimal
    budget: Decimal
    binding: Tuple[str, ...] = ()
    reasons: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "portfolio_id": self.portfolio_id,
            "floor": dec_str(self.floor),
            "cap": dec_str(self.cap),
            "water_filled": dec_str(self.water_filled),
            "budget": dec_str(self.budget),
            "binding": list(self.binding),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class SessionPoolPlan:
    total: Decimal
    allocated: Decimal
    unallocated: Decimal
    members: Tuple[MemberBudget, ...] = ()
    degraded: Tuple[str, ...] = ()
    reasons: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "version": SESSION_POOL_VERSION,
            "total": dec_str(self.total),
            "allocated": dec_str(self.allocated),
            "unallocated": dec_str(self.unallocated),
            "members": [m.to_dict() for m in self.members],
            "degraded": list(self.degraded),
            "reasons": list(self.reasons),
        }


def _business_limit(member: PoolMember, cap: Decimal) -> Tuple[Decimal, List[str]]:
    """业务上限取 min；None 表示该维度不约束。"""
    limit = cap
    binding: List[str] = []
    for key in ("need", "max_gross", "available", "utilization_cap"):
        raw = getattr(member, key)
        if raw is None:
            continue
        value = to_decimal(raw)
        if value is None:
            continue
        value = max(Decimal(0), value)
        if value < limit:
            limit = value
            binding.append(key)
    return limit, binding


def allocate_session_pool(
    total: Any,
    members: Sequence[PoolMember],
    *,
    min_share: Any = DEFAULT_MIN_SHARE,
    max_share: Any = DEFAULT_MAX_SHARE,
) -> SessionPoolPlan:
    """带上下限的水位归一化（0018 §5.2）。

    步骤：
    1. 活跃成员先拿 ``floor_i = min(need_i, min_share × total)``；
    2. 剩余额度按 ``raw_weight`` 迭代分配，超过 ``cap_i = max_share × total``
       的部分截断后继续分给未达上限的成员；
    3. 最后与业务上限取 min，未分完的额度留在池里。

    全 0 权重兜底：活跃成员权重合计为 0 时，剩余额度按成员等分（确定性），
    并记 ``zero_weight_equal_split``；没有活跃成员则不分。
    """
    total_d = _non_negative(total)
    low = _clamp(to_decimal(min_share) or DEFAULT_MIN_SHARE, Decimal(0), Decimal(1))
    high = _clamp(to_decimal(max_share) or DEFAULT_MAX_SHARE, low, Decimal(1))

    ids = [m.portfolio_id for m in members]
    if len(set(ids)) != len(ids):
        raise ValueError("portfolio_id 重复，无法确定唯一归属")

    degraded: List[str] = []
    reasons: List[str] = []
    if total_d <= 0 or not members:
        budgets = tuple(
            MemberBudget(
                portfolio_id=m.portfolio_id,
                floor=Decimal(0),
                cap=Decimal(0),
                water_filled=Decimal(0),
                budget=Decimal(0),
                reasons=("无可分配池",),
            )
            for m in members
        )
        return SessionPoolPlan(
            total=total_d,
            allocated=Decimal(0),
            unallocated=total_d,
            members=budgets,
            reasons=("可分配池为 0 或无成员，不分配",),
        )

    active: List[PoolMember] = [m for m in members if m.active]
    floor_limit = low * total_d
    cap_limit = high * total_d

    water: Dict[Any, Decimal] = {m.portfolio_id: Decimal(0) for m in members}
    floors: Dict[Any, Decimal] = {m.portfolio_id: Decimal(0) for m in members}
    caps: Dict[Any, Decimal] = {m.portfolio_id: cap_limit for m in active}

    remaining = total_d
    for m in active:
        floors[m.portfolio_id] = min(_non_negative(m.need), floor_limit)
        water[m.portfolio_id] = floors[m.portfolio_id]
        remaining -= floors[m.portfolio_id]

    if remaining > _EPS and active:
        weights: Dict[Any, Decimal] = {}
        for m in active:
            w = to_decimal(m.raw_weight)
            weights[m.portfolio_id] = w if w is not None and w > 0 else Decimal(0)
        weight_sum = sum(weights.values(), Decimal(0))
        if weight_sum <= 0:
            degraded.append("zero_weight_equal_split")
            reasons.append("活跃成员权重合计为 0，剩余额度按成员等分兜底")
            equal = Decimal(1)
            weights = {m.portfolio_id: equal for m in active}
            weight_sum = Decimal(len(active))

        guard = 0
        while remaining > _EPS and guard < 64:
            guard += 1
            candidates = [
                m for m in active
                if water[m.portfolio_id] < cap_limit - _EPS
                and water[m.portfolio_id] < _non_negative(m.need) - _EPS
            ]
            if not candidates:
                break
            cand_sum = sum((weights[m.portfolio_id] for m in candidates), Decimal(0))
            if cand_sum <= 0:
                cand_sum = Decimal(len(candidates))
                for m in candidates:
                    weights[m.portfolio_id] = Decimal(1)

            distributed = Decimal(0)
            for m in candidates:
                share = remaining * weights[m.portfolio_id] / cand_sum
                room = min(cap_limit, _non_negative(m.need)) - water[m.portfolio_id]
                delta = min(share, room)
                if delta <= 0:
                    continue
                water[m.portfolio_id] += delta
                distributed += delta
            if distributed <= _EPS:
                break
            remaining -= distributed

    budgets: List[MemberBudget] = []
    allocated = Decimal(0)
    for m in members:
        pid = m.portfolio_id
        floor = floors.get(pid, Decimal(0))
        cap = caps.get(pid, cap_limit if m.active else Decimal(0))
        if not m.active:
            budgets.append(
                MemberBudget(
                    portfolio_id=pid,
                    floor=Decimal(0),
                    cap=Decimal(0),
                    water_filled=Decimal(0),
                    budget=Decimal(0),
                    reasons=("成员未激活，不参与分配",),
                )
            )
            continue

        limit, binding = _business_limit(m, cap)
        budget = min(water.get(pid, Decimal(0)), limit)
        member_reasons = []
        if binding:
            member_reasons.append("业务上限生效：" + ", ".join(binding))
        if budget < water.get(pid, Decimal(0)) - _EPS:
            member_reasons.append("水位分配被业务上限截断，差额留在池里")
        allocated += budget
        budgets.append(
            MemberBudget(
                portfolio_id=pid,
                floor=floor,
                cap=cap,
                water_filled=water.get(pid, Decimal(0)),
                budget=budget,
                binding=tuple(binding),
                reasons=tuple(member_reasons),
            )
        )

    budgets.sort(key=lambda b: str(b.portfolio_id))
    unallocated = total_d - allocated
    if unallocated > _EPS:
        reasons.append("未分配额度留在会话池，不强行分完")
    return SessionPoolPlan(
        total=total_d,
        allocated=allocated,
        unallocated=unallocated,
        members=tuple(budgets),
        degraded=tuple(degraded),
        reasons=tuple(reasons),
    )


# ---------------------------------------------------------------------------
# 同票配额：同一 code 的容量在多个借款人之间仲裁
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class CodeDemand:
    """同票需求（0018 §5.3 / 0019 Step 8）。

    ``priority`` 沿用组合/策略优先级；<=0 视为 1，避免默认 0 直接饿死。
    ``master=True`` 的成员先拿，剩余容量再给 slave。
    """

    owner_id: Any
    demand_qty: int
    priority: int = 0
    master: bool = False

    def __post_init__(self) -> None:
        if self.owner_id is None:
            raise ValueError("owner_id 不能为空")


@dataclass(frozen=True)
class CodeQuota:
    owner_id: Any
    demand_qty: int
    weight: Decimal
    quota_qty: int
    master: bool
    binding: Tuple[str, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "owner_id": self.owner_id,
            "demand_qty": self.demand_qty,
            "weight": dec_str(self.weight),
            "quota_qty": self.quota_qty,
            "master": self.master,
            "binding": list(self.binding),
        }


@dataclass(frozen=True)
class CodeAllocation:
    code: str
    capacity_qty: int
    allocatable_qty: int
    allocated_qty: int
    remaining_qty: int
    quotas: Tuple[CodeQuota, ...] = ()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "capacity_qty": self.capacity_qty,
            "allocatable_qty": self.allocatable_qty,
            "allocated_qty": self.allocated_qty,
            "remaining_qty": self.remaining_qty,
            "quotas": [q.to_dict() for q in self.quotas],
        }


def _split_lots(
    capacity: int,
    demands: Sequence[CodeDemand],
    *,
    lot: int,
) -> Dict[Any, int]:
    """按 priority × demand 分配整手配额，余数用确定性排序补 1 手。"""
    if not demands:
        return {}
    total_w = sum((max(int(d.priority), 1) * int(d.demand_qty) for d in demands), 0)
    if total_w <= 0:
        return {d.owner_id: 0 for d in demands}

    lots_total = capacity // lot
    max_lots = {d.owner_id: max(0, int(d.demand_qty) // lot) for d in demands}
    raw_lots: Dict[Any, Decimal] = {}
    base_lots: Dict[Any, int] = {}
    used = 0
    for d in demands:
        w = Decimal(max(int(d.priority), 1) * int(d.demand_qty))
        raw = Decimal(lots_total) * w / Decimal(total_w)
        raw = min(raw, Decimal(max_lots[d.owner_id]))
        raw_lots[d.owner_id] = raw
        base = int(raw)
        base_lots[d.owner_id] = base
        used += base

    left = min(lots_total - used,
               sum(max_lots[d.owner_id] - base_lots[d.owner_id] for d in demands))
    if left > 0:
        order = sorted(
            demands,
            key=lambda d: (
                -(raw_lots[d.owner_id] - base_lots[d.owner_id]),
                -max(int(d.priority), 1),
                -int(d.demand_qty),
                str(d.owner_id),
            ),
        )
        i = 0
        while left > 0:
            d = order[i % len(order)]
            if base_lots[d.owner_id] < max_lots[d.owner_id]:
                base_lots[d.owner_id] += 1
                left -= 1
            i += 1
            if i > len(order) * (lots_total + 1):
                break

    return {d.owner_id: base_lots[d.owner_id] * lot for d in demands}


def allocate_code_quota(
    code: str,
    capacity_qty: Any,
    demands: Sequence[CodeDemand],
    *,
    lot: int = DEFAULT_LOT,
) -> CodeAllocation:
    """同日同票容量仲裁（0018 §5.3 / 0019 Step 8）。

    - 先按 ``w = priority × demand`` 分整手；
    - master 先拿，剩余容量再给 slave；
    - 余数按 (小数余数 desc, priority desc, demand desc, owner_id) 补 1 手；
    - 输出保证 ``Σ quota_qty ≤ 可分配容量`` 且均为 ``lot`` 的整数倍。
    """
    if lot <= 0:
        raise ValueError("lot 必须为正整数")
    try:
        capacity = int(capacity_qty)
    except (TypeError, ValueError) as exc:
        raise ValueError("capacity_qty 必须为整数股数") from exc
    capacity = max(0, capacity)
    allocatable = capacity // lot * lot

    ids = [d.owner_id for d in demands]
    if len(set(ids)) != len(ids):
        raise ValueError("owner_id 重复，无法确定唯一配额")

    valid = [d for d in demands if int(d.demand_qty) > 0]
    masters = [d for d in valid if d.master]
    slaves = [d for d in valid if not d.master]

    quotas: Dict[Any, int] = {d.owner_id: 0 for d in demands}
    remaining = allocatable
    for group in (masters, slaves):
        if not group or remaining <= 0:
            continue
        split = _split_lots(remaining, group, lot=lot)
        for d in group:
            quotas[d.owner_id] = split.get(d.owner_id, 0)
            remaining -= quotas[d.owner_id]

    ordered = sorted(
        demands,
        key=lambda d: (
            0 if d.master else 1,
            -max(int(d.priority), 1),
            -int(d.demand_qty),
            str(d.owner_id),
        ),
    )
    result: List[CodeQuota] = []
    for d in ordered:
        qty = quotas.get(d.owner_id, 0)
        weight = Decimal(max(int(d.priority), 1) * int(d.demand_qty))
        binding: Tuple[str, ...] = ()
        if qty < int(d.demand_qty):
            binding = ("capacity",)
        result.append(
            CodeQuota(
                owner_id=d.owner_id,
                demand_qty=int(d.demand_qty),
                weight=weight,
                quota_qty=qty,
                master=bool(d.master),
                binding=binding,
            )
        )

    allocated = sum(q.quota_qty for q in result)
    return CodeAllocation(
        code=code,
        capacity_qty=capacity,
        allocatable_qty=allocatable,
        allocated_qty=allocated,
        remaining_qty=allocatable - allocated,
        quotas=tuple(result),
    )
