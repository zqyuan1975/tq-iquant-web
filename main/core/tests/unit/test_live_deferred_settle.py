"""P0-1（2026-11-24）回归：放行后未提交的 deferred 单不再无条件 canceled。

真实故障（2026-11-24 10:16）：放行的 deferred BUY 在 `_allocate_batch_buys`
资金分配阶段因「目标份额不足 80%」整笔跳过（日志 73.99% < 80%），根本没进入
`_submit_one_order`（dispatcher 未被调用，不可能重复成交）；旧收口仅凭
`_deferred_row_id` 非空就写 canceled → 漏单，且事件只有「闸门拦截」笼统文案。

本用例覆盖：
- 可重试闸门（资金/仓位/在途/审批）→ 回 deferred 并计次（行保持 deferred）；
- 不可重试闸门（T+1 当日已成交/人工审批/薄量）→ 仍 canceled，但带真实原因；
- 重排次数用尽 → canceled；
- 已发出（`_submit_outcome=submitted`）→ 绝不改终态。
"""
from datetime import datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import OrderEvent
from core.engine.live_engine import (
    LiveEngine, _SUBMIT_SUBMITTED, _DEFERRED_RETRY_MAX, _RETRY_MARKER,
)
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import Base, LiveOrder
from tq_iquant_shared.constants import SignalType, TradeType

_BT = datetime(2026, 8, 5, 10, 0)
_STOCK = "600000.SH"


def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


class _StubDispatcher:
    channel = "test"
    supports_remark = False
    supports_cancel = True
    supports_backfill = False

    def query_calendar(self, *args, **kwargs):
        raise RuntimeError("calendar unavailable in stub")

    def heartbeat(self):
        return True

    def order_id(self, order):
        return "stuborder".ljust(32, "0")[:32]

    def get_positions(self):
        return []

    def get_cash(self):
        return {}

    def place_order(self, order, price_type=None):  # pragma: no cover - 不应被调用
        raise AssertionError("settle 路径不得触发真实下单")


def _portfolio():
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1m", capital_ratio=Decimal("0.6"),
        max_positions=5, single_open_ratio=Decimal("0.1"),
    )
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port


def _engine(factory, port, disp):
    poller = BarPoller(disp, [_STOCK], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )


def _deferred_row(db, bar_time=None):
    row = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code=_STOCK, trade_type="buy", order_type="limit",
        status="deferred",
        quantity=100, price=Decimal("9.3"), bar_time=bar_time or _BT,
        signal_name="open_sig", error_message=None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def _released_order(row_id):
    order = OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=_STOCK,
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("9.3"),
        bar_time=_BT, signal_name="open_sig",
    )
    order._deferred_row_id = row_id
    order._submit_outcome = "blocked"
    order._submit_reason = "资金不足跳过（低于目标份额 80% 门槛）"
    return order


def test_safe_to_retry_requeues_instead_of_cancel():
    factory = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp)
    db = factory()
    row = _deferred_row(db)
    order = _released_order(row.id)
    order._safe_to_retry = True

    engine._settle_unsubmitted_released(db, port, [order])

    db.expire_all()
    after = db.query(LiveOrder).filter(LiveOrder.id == row.id).first()
    assert after.status == "deferred", "未发出的可重试单必须回 deferred，不能判死"
    assert _RETRY_MARKER + "1" in (after.error_message or "")
    assert "资金不足跳过" in (after.error_message or "")
    assert order._requeued_for_retry is True
    assert order in engine._signal_bar_deferred
    events = [e for e in engine._recorder.drain()
              if e.gate == "deferred_release"]
    assert events and events[-1].action == "requeue"
    assert "资金不足跳过" in (events[-1].message or "")
    db.close()


def test_retry_budget_exhausted_cancels_with_real_reason():
    factory = _db_factory()
    port = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row = _deferred_row(db)
    order = _released_order(row.id)
    order._safe_to_retry = True
    order._deferred_retry_count = _DEFERRED_RETRY_MAX

    engine._settle_unsubmitted_released(db, port, [order])
    db.commit()

    db.expire_all()
    after = db.query(LiveOrder).filter(LiveOrder.id == row.id).first()
    assert after.status == "canceled"
    assert "资金不足跳过" in (after.error_message or "")
    assert getattr(order, "_requeued_for_retry", False) is not True
    db.close()


def test_not_safe_to_retry_cancels_with_reason():
    """T+1 当日已成交/人工审批/薄量等不可重试闸门：仍 canceled，但原因可读。"""
    factory = _db_factory()
    port = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row = _deferred_row(db)
    order = _released_order(row.id)
    order._submit_reason = "同股当日已有成交（T+1 排重）"

    engine._settle_unsubmitted_released(db, port, [order])
    db.commit()

    db.expire_all()
    after = db.query(LiveOrder).filter(LiveOrder.id == row.id).first()
    assert after.status == "canceled"
    assert "T+1 排重" in (after.error_message or "")
    db.close()


def test_submitted_outcome_never_cancelled():
    """防御：可能已发出的单绝不在这里改终态。"""
    factory = _db_factory()
    port = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row = _deferred_row(db)
    order = _released_order(row.id)
    order._submit_outcome = _SUBMIT_SUBMITTED
    order._safe_to_retry = True

    engine._settle_unsubmitted_released(db, port, [order])

    db.expire_all()
    after = db.query(LiveOrder).filter(LiveOrder.id == row.id).first()
    assert after.status == "deferred"
    assert order._deferred_row_id == row.id
    db.close()


def test_already_requeued_is_skipped():
    factory = _db_factory()
    port = _portfolio()
    engine = _engine(factory, port, _StubDispatcher())
    db = factory()
    row = _deferred_row(db)
    order = _released_order(row.id)
    order._requeued_for_retry = True

    engine._settle_unsubmitted_released(db, port, [order])

    db.expire_all()
    after = db.query(LiveOrder).filter(LiveOrder.id == row.id).first()
    assert after.status == "deferred"
    db.close()
