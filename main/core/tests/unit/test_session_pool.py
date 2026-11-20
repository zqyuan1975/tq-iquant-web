"""会话池分配与同票配额（0021 E1）单测。"""
from decimal import Decimal

import pytest

from core.engine.session_pool import (
    CodeDemand,
    PoolMember,
    SESSION_POOL_VERSION,
    allocate_code_quota,
    allocate_session_pool,
)

D = Decimal


# ---------------- 水位归一化 ----------------
def test_three_borrowers_water_filling_with_floor_and_cap():
    """3 个借款方：先给 5% 下限，剩余按权重分，均不超过 50% 上限。"""
    plan = allocate_session_pool(
        D("300000"),
        [
            PoolMember(1, raw_weight=D("0.5"), need=D("200000")),
            PoolMember(2, raw_weight=D("0.3"), need=D("200000")),
            PoolMember(3, raw_weight=D("0.2"), need=D("200000")),
        ],
    )

    budgets = {m.portfolio_id: m for m in plan.members}
    assert budgets[1].floor == D("15000")
    assert budgets[1].cap == D("150000")
    assert budgets[1].budget == D("142500")
    assert budgets[2].budget == D("91500")
    assert budgets[3].budget == D("66000")
    assert plan.allocated == D("300000")
    assert plan.unallocated == D("0")


def test_business_caps_apply_after_water_filling():
    """业务上限（毛额度/可用现金/目标仓位）最后取 min，差额留在池里。"""
    plan = allocate_session_pool(
        D("300000"),
        [
            PoolMember(1, raw_weight=D("1"), need=D("300000"),
                       max_gross=D("60000")),
            PoolMember(2, raw_weight=D("0"), need=D("300000"),
                       available=D("40000")),
        ],
    )

    budgets = {m.portfolio_id: m for m in plan.members}
    assert budgets[1].water_filled == D("150000")
    assert budgets[1].budget == D("60000")
    assert "max_gross" in budgets[1].binding
    assert budgets[2].water_filled == D("150000")
    assert budgets[2].budget == D("40000")
    assert "available" in budgets[2].binding
    assert plan.allocated == D("100000")
    assert plan.unallocated == D("200000")


def test_zero_weight_falls_back_to_equal_split():
    """所有活跃成员权重为 0：剩余额度等分兜底，不抛 ZeroDivisionError。"""
    plan = allocate_session_pool(
        D("300000"),
        [
            PoolMember(1, raw_weight=D("0"), need=D("300000")),
            PoolMember(2, raw_weight=D("0"), need=D("300000")),
            PoolMember(3, raw_weight=D("0"), need=D("300000")),
        ],
    )

    assert "zero_weight_equal_split" in plan.degraded
    assert [m.budget for m in plan.members] == [D("100000")] * 3
    assert plan.allocated == D("300000")
    assert plan.unallocated == D("0")


def test_inactive_members_and_empty_pool_keep_money_in_pool():
    plan = allocate_session_pool(
        D("300000"),
        [PoolMember(1, raw_weight=D("1"), need=D("300000"), active=False)],
    )
    assert plan.members[0].budget == D("0")
    assert plan.unallocated == D("300000")

    empty = allocate_session_pool(D("0"), [PoolMember(1, need=D("100"))])
    assert empty.allocated == D("0")
    assert empty.unallocated == D("0")


def test_duplicate_portfolio_id_rejected():
    with pytest.raises(ValueError):
        allocate_session_pool(
            D("100"),
            [PoolMember(1), PoolMember(1)],
        )


def test_plan_to_dict_uses_decimal_strings_and_version():
    plan = allocate_session_pool(
        D("300000"),
        [PoolMember(1, raw_weight=D("1"), need=D("100000"))],
    )
    body = plan.to_dict()
    assert body["version"] == SESSION_POOL_VERSION
    assert body["total"] == "300000"
    assert body["members"][0]["budget"] == "100000.00"
    assert body["members"][0]["floor"] == "15000.00"


# ---------------- 同票配额 ----------------
def test_three_borrowers_same_code_share_capacity():
    alloc = allocate_code_quota(
        "600000",
        1000,
        [
            CodeDemand(1, demand_qty=1000, priority=3),
            CodeDemand(2, demand_qty=1000, priority=2),
            CodeDemand(3, demand_qty=1000, priority=1),
        ],
    )

    quotas = {q.owner_id: q.quota_qty for q in alloc.quotas}
    assert quotas == {1: 500, 2: 300, 3: 200}
    assert sum(quotas.values()) == 1000 <= alloc.capacity_qty
    assert all(q % 100 == 0 for q in quotas.values())


def test_master_takes_before_slaves_when_capacity_scarce():
    alloc = allocate_code_quota(
        "600000",
        600,
        [
            CodeDemand("M", demand_qty=1000, master=True),
            CodeDemand("s1", demand_qty=1000),
            CodeDemand("s2", demand_qty=1000),
        ],
    )

    quotas = {q.owner_id: q.quota_qty for q in alloc.quotas}
    assert quotas["M"] == 600
    assert quotas["s1"] == 0
    assert quotas["s2"] == 0
    assert alloc.allocated_qty == 600


def test_quota_never_exceeds_capacity_or_demand():
    alloc = allocate_code_quota(
        "600000",
        1050,
        [CodeDemand(1, demand_qty=150, priority=1)],
    )

    assert alloc.allocatable_qty == 1000
    assert alloc.quotas[0].quota_qty == 100
    assert alloc.allocated_qty == 100
    assert alloc.allocated_qty <= alloc.capacity_qty

    tiny = allocate_code_quota("600000", 50, [CodeDemand(1, demand_qty=100)])
    assert tiny.allocatable_qty == 0
    assert tiny.quotas[0].quota_qty == 0


def test_same_code_ordering_is_deterministic_with_unique_tiebreak():
    """同优先级同需求：余数按 owner_id 兜底，输入顺序不影响结果。"""
    a = allocate_code_quota(
        "600000", 300,
        [CodeDemand("b", demand_qty=1000), CodeDemand("a", demand_qty=1000)],
    )
    b = allocate_code_quota(
        "600000", 300,
        [CodeDemand("a", demand_qty=1000), CodeDemand("b", demand_qty=1000)],
    )

    qa = {q.owner_id: q.quota_qty for q in a.quotas}
    qb = {q.owner_id: q.quota_qty for q in b.quotas}
    assert qa == qb == {"a": 200, "b": 100}
    assert [q.owner_id for q in a.quotas] == ["a", "b"]


def test_duplicate_owner_id_in_code_quota_rejected():
    with pytest.raises(ValueError):
        allocate_code_quota(
            "600000", 1000,
            [CodeDemand(1, demand_qty=100), CodeDemand(1, demand_qty=100)],
        )
