"""P0-1 / P1-2 / P1-6 回归用例（2026-11-12 实盘执行链审计修复）。

覆盖：
- P0-1：`_submit_one_order` execute 段抛非 `Bridge*` 异常（`TDXLockTimeout`）时，
  只有「请求未发出」才允许回滚幻影（DB 行不留在 submitted、`_pending_orders` 摘除、
  bar 幂等标记清掉）；「请求已发出」必须保留 submitted 待核对。
- P1-2：`_expected_minute_labels` 不把正在形成的分钟算进应有分钟。
- P1-6：分钟让价在重排（deferred_retry）路径不重复叠加。
"""
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import OrderEvent, TradeEvent
from core.engine.live_engine import LiveEngine, _expected_minute_labels
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import Base, LiveOrder
from core.tq.utils import TDXLockTimeout
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
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    return SessionLocal, engine


def _portfolio(requote_enabled=False):
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1m",
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("0.1"),
        requote_enabled=requote_enabled,
    )
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port, ctx


class _StubDispatcher:
    """最小 dispatcher：只实现 `_submit_one_order` 会碰到的接口。"""

    channel = "test"
    supports_remark = False
    supports_cancel = True
    supports_backfill = False
    last_enqueued_order_id = None
    last_unconfirmed_order_id = None
    last_broker_order_id = None
    last_unconfirmed_message = None
    last_cancel_result = None

    def __init__(self, exc=None, set_broker_oid=False):
        self._exc = exc
        self._set_broker_oid = set_broker_oid
        self.placed = []

    # --- 引擎构造依赖 ---
    def query_calendar(self, *args, **kwargs):
        raise RuntimeError("calendar unavailable in stub")

    def heartbeat(self):
        return True

    def order_id(self, order):
        return ("stub" + str(order.stock_code) + str(order.bar_time)).ljust(32, "0")[:32]

    # --- 下单 ---
    def place_order(self, order, price_type=None):
        self.placed.append(order)
        if self._set_broker_oid:
            # 模拟「柜台已受理并回显委托号」后再抛异常
            self.last_broker_order_id = "BROKER%04d" % len(self.placed)
        if self._exc is not None:
            raise self._exc
        return TradeEvent(
            strategy_id=order.strategy_id,
            portfolio_id=order.portfolio_id,
            stock_code=order.stock_code,
            trade_type=order.trade_type,
            price=order.price,
            quantity=order.quantity,
            amount=order.price * order.quantity,
            commission=Decimal("5"),
            stamp_duty=Decimal("0"),
            trade_time=order.bar_time,
            signal_type=order.signal_type,
            signal_name=order.signal_name,
        )

    # --- 其它闸门/查询 ---
    def check_price_limit(self, code, side, price):
        return None

    def price_limit_bounds(self, code):
        return None

    def get_positions(self):
        return []

    def get_cash(self):
        return {}

    def execution_receipts(self):
        return []


def _engine(factory, port, disp):
    poller = BarPoller(disp, [_STOCK], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )


def _order(trade_type=TradeType.BUY, stock=_STOCK, bar_time=None):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=trade_type,
        signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("9.3"),
        bar_time=bar_time or _BT, signal_name="open_sig",
    )


# ---------------- P0-1 ----------------
def test_submit_error_tdx_lock_timeout_no_phantom_submitted():
    """execute 抛 TDXLockTimeout（请求未发出）→ 不留 submitted 幻影行/幻影在途/bar 标记。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    disp = _StubDispatcher(exc=TDXLockTimeout("tq lock timeout"))
    engine = _engine(factory, port, disp)
    db = factory()
    order = _order()

    with pytest.raises(TDXLockTimeout):
        engine._submit_one_order(db, port, order, "1m")

    db.expire_all()
    rows = db.query(LiveOrder).all()
    assert len(rows) == 1
    assert rows[0].status == "rejected"           # 不留在 submitted 幻影
    assert "submit_error" in (rows[0].error_message or "")
    assert rows[0].id not in engine._pending_orders   # 不留在途
    assert engine._bar_already_processed(db, order) is None   # 重试不被幂等键判死
    assert disp.placed, "必须真的走到发单（异常来自发单段）"
    db.close()


def test_submit_error_retry_not_blocked_by_inflight():
    """回滚后在途集合为空 → 同码同向卖单重试不被 inflight_sell 判死。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio()
    disp = _StubDispatcher(exc=TDXLockTimeout("tq lock timeout"))
    engine = _engine(factory, port, disp)
    db = factory()
    sell = _order(trade_type=TradeType.SELL)
    # 预置可卖持仓（T+1 可用量取 position.quantity，无桥快照时不缩量）
    ctx.positions[_STOCK] = Position(_STOCK)
    ctx.positions[_STOCK].buy(300, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))

    with pytest.raises(TDXLockTimeout):
        engine._submit_one_order(db, port, sell, "1m")

    # 旧实现：幻影 submitted 行仍在 _pending_orders → 这里会返回 []（整单跳过）
    kept = engine._dedupe_inflight_sells(port, [sell])
    assert kept == [sell]
    assert engine._bar_already_processed(db, sell) is None
    db.close()


def test_submit_error_after_broker_id_keeps_submitted():
    """dispatcher 已回显柜台委托号后再抛异常 → 保留 submitted 并留痕，绝不回滚成 rejected。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    disp = _StubDispatcher(exc=TDXLockTimeout("after send"), set_broker_oid=True)
    engine = _engine(factory, port, disp)
    db = factory()
    order = _order()

    with pytest.raises(TDXLockTimeout):
        engine._submit_one_order(db, port, order, "1m")

    db.expire_all()
    rows = db.query(LiveOrder).all()
    assert len(rows) == 1
    assert rows[0].status == "submitted"          # 柜台可能已受理，保持待核对
    assert "submit_error_after_send" in (rows[0].error_message or "")
    assert rows[0].id in engine._pending_orders   # 仍在途，等成交回填/超时收口
    db.close()


# ---------------- P1-2 ----------------
def test_expected_minute_labels_contract_is_closed_interval():
    """契约：``(start, end]`` 的**已闭合**分钟标签（end 本身计入）。

    排除「正在形成的那一分钟」不是本函数的职责——调用方 ``_plan_gap_recovery``
    先把 ``gap_end`` 取成 ``(now − 1s)`` 截到分钟（live_engine.py L1813–1815），
    再传进来；因此 end 恒为「最近一根已闭合 bar 的标签」，本函数必须把它算进去
    （2026-11-13 修：旧实现双端各减 1 秒 → 每轮恒少 1 根、action 恒 partial）。
    """
    start = datetime(2026, 11, 12, 10, 0)
    labels = _expected_minute_labels(start, datetime(2026, 11, 12, 10, 6, 0))
    assert labels == [
        datetime(2026, 11, 12, 10, m) for m in (1, 2, 3, 4, 5, 6)
    ]
    # 带秒的 end 按分钟标签比较：10:05:30 → 已闭合到 10:05
    labels2 = _expected_minute_labels(start, datetime(2026, 11, 12, 10, 5, 30))
    assert labels2[-1] == datetime(2026, 11, 12, 10, 5)


# ---------------- P1-6 ----------------
def test_minute_price_bias_not_reapplied_on_requeue():
    """同一 OrderEvent 二次进入让价（deferred 重排复用对象）只偏 1 跳，不偏 2 跳。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio(requote_enabled=True)
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp)
    order = _order()

    engine._apply_minute_price_bias(port, order, ctx)
    first = order.price
    assert first == Decimal("9.31")   # BUY +1 跳（tick=0.01）

    engine._apply_minute_price_bias(port, order, ctx)
    assert order.price == first       # 重排路径不再叠加第二跳
    assert getattr(order, "_price_biased", False) is True


# ===========================================================================
# 端到端（真引擎链路 + 伪通道）：TDX 已点击未生效不再静默吃 rejected（2026-11-18）
# ===========================================================================
class _UnconfirmedTdxDispatcher(_StubDispatcher):
    """模拟 TDX：place_order 返回 None + last_unconfirmed_message，行仍待发送。"""

    def __init__(self, row_state="pending",
                 message="已受理未认领：已点击『发送』，但该行仍为待发送状态"):
        super().__init__()
        self._row_state = row_state
        self.last_unconfirmed_message = message
        self.row_checks = []

    def place_order(self, order, price_type=None):
        self.placed.append(order)
        self.last_unconfirmed_order_id = self.order_id(order)
        return None

    def query_deals(self):
        return []

    def pending_row_state(self, live_order):
        self.row_checks.append(live_order.id)
        return self._row_state


def _submit_unconfirmed(factory, port, disp):
    engine = _engine(factory, port, disp)
    db = factory()
    order = _order()
    engine._submit_one_order(db, port, order, "1m")
    return engine, db


def _age_to_timeout(db):
    lo = db.query(LiveOrder).one()
    lo.created_at = datetime.now(timezone.utc).replace(tzinfo=None) \
        - timedelta(seconds=200)
    db.commit()
    db.expire_all()
    return db.query(LiveOrder).one()


def test_unconfirmed_row_still_pending_requeues_instead_of_rejected():
    """点击未生效 → error_message 带真实原因；180s 复核仍待发送 → deferred 重排。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    disp = _UnconfirmedTdxDispatcher("pending")
    engine, db = _submit_unconfirmed(factory, port, disp)

    lo = db.query(LiveOrder).one()
    assert lo.status == "submitted"
    # 改动 4：TDX 的 last_unconfirmed_message 落进 live_orders.error_message
    assert "已点击『发送』" in (lo.error_message or "")
    assert lo.id in engine._pending_orders

    _age_to_timeout(db)
    engine._expire_stale_orders(db)      # 真引擎：复核 → 重排 → 再 expire
    db.expire_all()
    lo = db.query(LiveOrder).one()
    assert lo.status == "deferred"       # 不再是静默 rejected
    assert (lo.error_message or "").startswith("deferred_retry:n=1")
    assert lo.id not in engine._pending_orders
    assert engine._signal_bar_deferred, "应回内存 deferred 队列等下一交易日放行"
    assert disp.row_checks, "必须真的复核过该行"
    db.close()


def test_unconfirmed_row_absent_keeps_expire_rejected():
    """行已消失/已转『已发送』→ 视为已报出，保持既有 expire 行为（不重报）。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    disp = _UnconfirmedTdxDispatcher("absent")
    engine, db = _submit_unconfirmed(factory, port, disp)
    _age_to_timeout(db)
    engine._expire_stale_orders(db)
    db.commit()          # expire 的调用方负责 commit（与 _tick_main 同口径）
    db.expire_all()
    lo = db.query(LiveOrder).one()
    assert lo.status == "rejected"
    assert "order match timeout" in (lo.error_message or "")
    assert not engine._signal_bar_deferred
    db.close()


def test_unconfirmed_requeue_respects_retry_cap():
    """重排次数上限 _DEFERRED_RETRY_MAX=2：用尽后仍待发送 → 交 expire 终态。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    disp = _UnconfirmedTdxDispatcher("pending")
    engine, db = _submit_unconfirmed(factory, port, disp)
    lo = _age_to_timeout(db)
    lo.error_message = "deferred_retry:n=2; 已重排两次"
    db.commit()

    engine._expire_stale_orders(db)
    db.commit()
    db.expire_all()
    lo = db.query(LiveOrder).one()
    assert lo.status == "rejected"
    assert not engine._signal_bar_deferred
    db.close()

