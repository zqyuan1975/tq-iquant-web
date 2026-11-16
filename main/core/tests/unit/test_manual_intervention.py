"""人工干预标记（manual_intervention）单元测试。

场景：人在柜台直接操作了**平台账内**的仓（手动买入 / 手动减仓 / 手动清仓），
平台台账 + 在途单解释不了这部分数量差。要求：

- 打标 + 落审计事件（gate=manual_intervention，含代码/虚拟量/真实量/差额/方向/原因）；
- 不阻断交易、不误判风控：虚拟数量与成本仍按柜台口径收敛，人工减仓按账面成本
  退回虚拟现金（否则虚拟净值凭空少掉 → 假回撤可能误触熔断）；
- 柜台整行缺失（清仓）需连续两次确认才清账，收盘再由
  ``_apply_close_counter_correction`` 兜底修正并保留审计行。
"""
from decimal import Decimal

from core.engine.position import Position
from core.models import LiveDecisionEvent
from core.engine.live.timing import now_shanghai
from core.tests.unit.test_live_engine import _db_factory
from core.tests.unit.test_live_account_alignment import _AcctDispatcher, _mk_engine

CODE = "600000.SH"


def _seed_holding(port, qty=1000, cost=Decimal("10")):
    """预置平台账内持仓（等价于平台自己买入后的账本状态）。"""
    pos = Position(CODE)
    pos.mirror_bridge(qty, qty, cost, now_shanghai().date())
    port.strategies[0].positions[CODE] = pos
    return pos


def _intervention_events(factory):
    db = factory()
    try:
        return db.query(LiveDecisionEvent).filter(
            LiveDecisionEvent.gate == "manual_intervention").all()
    finally:
        db.close()


def test_counter_extra_qty_marks_intervention_and_deducts_cash():
    """柜台多出（人工买入）：打标 increase、审计落库；虚拟按柜台覆盖并扣现金。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single_port()
    _seed_holding(port, qty=1000, cost=Decimal("10"))
    disp = _AcctDispatcher(positions=[
        {"symbol": CODE, "volume": 1200, "available": 1200,
         "cost": 10.0, "market_value": 12000.0},
    ])
    engine = _mk_engine(disp, factory, port, engine_key="tdx",
                        account_snapshot_interval=5.0)
    with factory() as db:
        engine._capture_account_snapshot(db)

    pos = ctx.positions[CODE]
    assert pos.quantity == 1200                      # 数量按柜台口径
    assert port.account.cash == Decimal("98000")     # 200 股 × 10 元市值被扣
    events = _intervention_events(factory)
    assert len(events) == 1
    ev = events[0]
    assert ev.stock_code == CODE
    assert ev.requested_qty == 1000 and ev.final_qty == 1200
    assert float(ev.param_value) == 200.0
    assert "方向=增加" in ev.message and "虚拟=1000" in ev.message


def test_counter_lower_qty_marks_intervention_and_credits_cash():
    """柜台少于台账（人工部分卖出）：打标 decrease、按账面成本退回虚拟现金。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single_port()
    _seed_holding(port, qty=1000, cost=Decimal("10"))
    disp = _AcctDispatcher(positions=[
        {"symbol": CODE, "volume": 600, "available": 600,
         "cost": 10.0, "market_value": 6000.0},
    ])
    engine = _mk_engine(disp, factory, port, engine_key="tdx",
                        account_snapshot_interval=5.0)
    with factory() as db:
        engine._capture_account_snapshot(db)

    pos = ctx.positions[CODE]
    assert pos.quantity == 600
    # 400 股 × 账面成本 10 = 4000 退回虚拟现金（不是按现价，避免制造假盈亏）
    assert port.account.cash == Decimal("104000")
    events = _intervention_events(factory)
    assert len(events) == 1
    ev = events[0]
    assert ev.requested_qty == 1000 and ev.final_qty == 600
    assert float(ev.param_value) == -400.0
    assert "方向=减少" in ev.message


def test_counter_row_gone_clears_after_confirmation():
    """柜台整行缺失（人工清仓）：首轮只待确认、不清账；次轮确认后清账 + 审计 apply。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single_port()
    _seed_holding(port, qty=1000, cost=Decimal("10"))
    # 空表必须伴随「可信账户」才会进确认计数（live_engine._account_read_trustworthy：
    # 空表 + 资产不可读 = 查询失败，只 notice、不累计确认，绝不误清仓）
    disp = _AcctDispatcher(
        positions=[],
        asset={"total_asset": 110000.0, "market_value": 0.0,
               "available": 110000.0},
    )
    engine = _mk_engine(disp, factory, port, engine_key="tdx",
                        account_snapshot_interval=5.0)

    with factory() as db:
        engine._capture_account_snapshot(db)
        assert ctx.positions[CODE].quantity == 1000   # 单次缺失不清仓
        assert _intervention_events(factory) == []
        engine._capture_account_snapshot(db)

    assert ctx.positions[CODE].quantity == 0
    assert port.account.cash == Decimal("110000")     # 1000 × 10 退回虚拟现金
    events = _intervention_events(factory)
    assert len(events) == 1
    ev = events[0]
    assert ev.action == "apply"
    assert ev.requested_qty == 1000 and ev.final_qty == 0
    assert "柜台已无此持仓" in ev.message


def test_close_reconcile_correction_clears_counter_missing_holding():
    """收盘对账兜底：虚拟有、柜台为 0 的仓按柜台口径清零并留审计行。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single_port()
    _seed_holding(port, qty=1000, cost=Decimal("10"))
    disp = _AcctDispatcher(positions=[])
    engine = _mk_engine(disp, factory, port, engine_key="tdx",
                        account_snapshot_interval=5.0)
    engine._reconcile_mismatches = [
        {"code": CODE, "virtual": 1000, "real": 0, "diff": -1000},
    ]

    assert engine._apply_close_counter_correction() == 1
    assert ctx.positions[CODE].quantity == 0
    assert port.account.cash == Decimal("110000")
    events = _intervention_events(factory)
    assert len(events) == 1
    assert events[0].action == "apply"
    assert "收盘对账" in events[0].message
    assert engine._reconcile_mismatches == []


def _portfolio_single_port():
    from core.tests.unit.test_live_engine import _portfolio_single
    return _portfolio_single()
