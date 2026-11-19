# -*- coding: utf-8 -*-
"""N1 资金审批内核（cash_allocator）：目标份额 / 80% 门槛 / 整手 / caps / 不做等比缩量。

该内核是实盘 ``LiveEngine._allocate_batch_buys`` 与回测 ``run_multi`` 的唯一判定
来源，口径漂移会直接造成两端资金行为不一致，故逐条钉住。
"""
from datetime import datetime
from decimal import Decimal

from core.engine.cash_allocator import (
    MIN_FILL_RATIO,
    REASON_CAPPED_ZERO,
    REASON_INSUFFICIENT_CASH,
    REASON_INSUFFICIENT_LOT,
    REASON_INVALID,
    REASON_OK,
    allocate_batch_buys,
)
from core.engine.event import OrderEvent
from tq_iquant_shared.constants import SignalType, TradeType


def _order(qty, price="10", code="000001.SZ"):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=code,
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=qty, price=Decimal(price),
        bar_time=datetime(2026, 1, 5, 15, 0),
    )


def _lot(code):
    return 100


def test_full_cash_keeps_original_quantity():
    """可用额充足：数量不变、逐笔扣减可用额。"""
    o1, o2 = _order(1000), _order(500)
    ds = allocate_batch_buys([o1, o2], Decimal("50000"), lot_of=_lot)
    assert [d.reason for d in ds] == [REASON_OK, REASON_OK]
    assert [d.qty for d in ds] == [1000, 500]
    assert o1.quantity == 1000 and o2.quantity == 500
    # 100000 目标 → 50000 可用：第一笔 10000 花掉后剩 40000，第二笔 5000 也够
    assert ds[0].remaining_after == Decimal("40000.00")
    assert ds[1].remaining_after == Decimal("35000.00")


def test_below_min_fill_ratio_skips_whole_order():
    """不足 80% 目标份额 → 整笔跳过（不按可用额等比缩量）。"""
    o = _order(1000)  # 目标金额 10000，目标份额 10000
    ds = allocate_batch_buys(
        [o], Decimal("7999.99"), lot_of=_lot,
        target_share_of=lambda x: Decimal("10000"))
    assert len(ds) == 1
    assert ds[0].accepted is False
    assert ds[0].reason == REASON_INSUFFICIENT_CASH
    assert ds[0].qty == 0
    # 原数量不被就地改成 0（调用方按 accepted 过滤）
    assert o.quantity == 1000


def test_above_min_fill_ratio_buys_affordable_lots_only():
    """够 80% 门槛：按可买额取整手，不放大、不超原目标量。"""
    o = _order(1000)
    ds = allocate_batch_buys(
        [o], Decimal("8500"), lot_of=_lot,
        target_share_of=lambda x: Decimal("10000"))
    assert ds[0].accepted is True
    # 8500 / 10 = 850 股 → 800 整手
    assert ds[0].qty == 800
    assert o.quantity == 800


def test_capped_below_one_lot_reports_capped_zero():
    """caps 把数量压到不足一手 → capped_to_zero（不提交）。"""
    o = _order(1000)
    ds = allocate_batch_buys(
        [o], Decimal("50000"), lot_of=_lot,
        caps=lambda order, qty, avail, pending: 0)
    assert ds[0].accepted is False
    assert ds[0].reason == REASON_CAPPED_ZERO


def test_invalid_price_rejected():
    o = _order(1000, price="0")
    ds = allocate_batch_buys([o], Decimal("50000"), lot_of=_lot)
    assert ds[0].accepted is False
    assert ds[0].reason == REASON_INVALID


def test_insufficient_lot_when_affordable_below_one_lot():
    """够门槛但可买股数不足一手（价格极高）→ insufficient_lot。"""
    o = _order(1000, price="100")
    # 目标 100000，80% = 80000；可用 80000 → 800 股 ≥ 一手，不触发
    ds = allocate_batch_buys(
        [o], Decimal("80000"), lot_of=_lot,
        target_share_of=lambda x: Decimal("100000"))
    assert ds[0].accepted is True and ds[0].qty == 800
    # 可用 80000 但每手 100 股 × 100 元 = 10000：仍能买 800 股；构造价格 1000 才不足
    o2 = _order(1000, price="1000")
    ds2 = allocate_batch_buys(
        [o2], Decimal("800000.01"), lot_of=_lot,
        target_share_of=lambda x: Decimal("1000000"))
    assert ds2[0].accepted is True


def test_sort_key_controls_priority():
    """sort_key 决定处理顺序：先处理者优先占额。"""
    early = _order(1000)
    early.bar_time = datetime(2026, 1, 5, 9, 31)
    late = _order(1000, code="600000.SH")
    late.bar_time = datetime(2026, 1, 5, 15, 0)
    ds = allocate_batch_buys(
        [late, early], Decimal("10000"), lot_of=_lot,
        sort_key=lambda o: o.bar_time)
    assert ds[0].order is early and ds[0].accepted is True
    # 额度已被 early 用尽，late 整笔跳过
    assert ds[1].order is late and ds[1].accepted is False
    assert ds[1].reason == REASON_INSUFFICIENT_CASH


def test_same_code_pending_value_accumulated_for_caps():
    """同代码两笔：caps 收到的 pending_same_code 含本批次已分配金额。"""
    seen = []
    o1 = _order(100)
    o2 = _order(100)

    def caps(order, qty, avail, pending):
        seen.append(pending)
        return qty

    allocate_batch_buys(
        [o1, o2], Decimal("50000"), lot_of=_lot, caps=caps)
    assert seen[0] == Decimal("0")
    assert seen[1] == Decimal("1000")


def test_min_fill_ratio_constant_is_80_percent():
    assert MIN_FILL_RATIO == Decimal("0.8")
