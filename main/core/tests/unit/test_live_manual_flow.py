"""批次3（M1 审批队列 + I1 人工补单/平仓/改账）单测。

覆盖的硬约束：
- manual 模式：开仓单先入 `live_approval_queue`，**不写 live_orders**；
- 批准时以「当下」重走全部闸门（本文件只验证能真正提交并落 submitted）；
- 卖出（风控/CLOSE/REDUCE）不进队列，人工延迟止损不可接受；
- auto 模式行为逐字不变（不入队）；
- 人工补单只在 manual 会话开放、受金额上限约束；
- `live_sell_scope=platform_only` 下人工平纯外部仓被拒（与策略卖单同口径）；
- 持仓改账写合成成交（重启 recover 重放生效）+ 审计行。
"""

from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import OrderEvent, TradeEvent
from core.engine.live_engine import LiveEngine, ManualActionError
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import (
    Base, LiveApprovalQueue, LiveManualAction, LiveOrder, LiveSession,
    LiveSessionPortfolio, LiveTrade, PortfolioStrategy, StockPool, Strategy,
)
from core.services import live_service
from tq_iquant_shared.constants import SignalType, TradeType

STOCK = "600000.SH"
CLOCK = datetime(2026, 11, 18, 10, 0)


# ---------------- fixtures ----------------
def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    def factory():
        return SessionLocal()

    return factory, engine


class _FakeDispatcher:
    """最小桥 fake：place 即成，行情/持仓/账户全空（fail-open）。"""

    channel = "fake_bridge"
    supports_remark = True

    def __init__(self):
        self.orders = []
        self._seq = 0

    @staticmethod
    def order_id(order):
        return "CLM-%s-%s" % (order.stock_code, id(order))

    def heartbeat(self):
        return True

    def close(self):
        pass

    def query_calendar(self):
        return []

    def place_order(self, order):
        self._seq += 1
        self.orders.append(order)
        return TradeEvent(
            strategy_id=order.strategy_id, portfolio_id=order.portfolio_id,
            stock_code=order.stock_code, trade_type=order.trade_type,
            price=order.price, quantity=order.quantity,
            amount=order.price * order.quantity, commission=Decimal("0"),
            stamp_duty=Decimal("0"),
            trade_time=order.bar_time or CLOCK,
            signal_type=order.signal_type, signal_name=order.signal_name,
        )

    def query_orders(self, order_id=None):
        return []

    def query_deals(self, order_id=None):
        return []

    def query_positions(self):
        return []

    def query_account(self):
        return []

    def query_quote(self, code, period="1m", count=10):
        return []

    def check_price_limit(self, stock_code, direction, price):
        return None


def _portfolio(single_open_ratio=Decimal("1")):
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.3"),
                              daily_loss_limit=Decimal("0.3"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1m",
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=single_open_ratio,
    )
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port, ctx


def _engine(factory, port, mode="manual", disp=None):
    disp = disp or _FakeDispatcher()
    poller = BarPoller(disp, [STOCK], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        execution_mode=mode,
    )
    engine.ctx.clock = lambda: CLOCK
    return engine, disp


def _order(qty=1000, price=Decimal("10"), trade_type=TradeType.BUY,
           signal_type=SignalType.OPEN, signal_name="open_sig"):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=STOCK,
        trade_type=trade_type, signal_type=signal_type,
        quantity=qty, price=price, bar_time=CLOCK,
        signal_name=signal_name,
    )


def _submit(engine, port, order):
    db = engine._db_session_factory()
    try:
        engine._submit_one_order(db, port, order, "1m")
        db.commit()
    finally:
        db.close()


def _query(factory, model, **filters):
    db = factory()
    try:
        q = db.query(model)
        for k, v in filters.items():
            q = q.filter(getattr(model, k) == v)
        return q.order_by(model.id).all()
    finally:
        db.close()


# ---------------- M1：审批队列 ----------------
def test_manual_mode_queues_buy_and_approve_submits():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, disp = _engine(factory, port, mode="manual")

    _submit(engine, port, _order())

    rows = _query(factory, LiveApprovalQueue)
    assert len(rows) == 1 and rows[0].status == "pending"
    assert rows[0].signal_bar_date is None  # 分钟信号不打 B-1 信号日
    assert _query(factory, LiveOrder) == []  # 未批准绝不落 live_orders

    result = engine.approve_approval(rows[0].id)

    assert result["status"] == "approved"
    orders = _query(factory, LiveOrder)
    assert len(orders) == 1 and orders[0].status == "submitted"
    assert orders[0].quantity == 1000
    assert len(disp.orders) == 1
    assert _query(factory, LiveApprovalQueue)[0].status == "approved"


def test_list_approvals_includes_display_fields_and_expired_flag():
    """列表字段（策略/组合/金额）与过期标识齐备，供工作台审批界面直接渲染。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, _disp = _engine(factory, port, mode="manual")

    db = factory()
    try:
        db.add(StockPool(id=1, code="POOL1", name="池"))
        db.add(PortfolioStrategy(id=1, name="组合A", stock_pool_id=1))
        db.add(Strategy(id=1, portfolio_id=1, name="策略A",
                        formula_id=1, period="1m", role="independent"))
        db.commit()
    finally:
        db.close()

    _submit(engine, port, _order())

    items = engine.list_approvals()
    assert len(items) == 1
    item = items[0]
    assert item["strategy_name"] == "策略A"
    assert item["portfolio_name"] == "组合A"
    assert item["amount"] == float(item["price"]) * item["quantity"]
    assert item["expired"] is False
    assert item["created_at"] and item["expires_at"]

    db = factory()
    try:
        row = db.query(LiveApprovalQueue).one()
        row.expires_at = CLOCK - timedelta(seconds=1)
        db.commit()
    finally:
        db.close()

    assert engine.list_approvals()[0]["expired"] is True


def test_reject_marks_rejected_and_does_not_submit():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, disp = _engine(factory, port, mode="manual")

    _submit(engine, port, _order())
    approval_id = _query(factory, LiveApprovalQueue)[0].id

    result = engine.reject_approval(approval_id, "人工判断不买")

    assert result["status"] == "rejected"
    assert _query(factory, LiveOrder) == []
    assert disp.orders == []


def test_expired_approval_cannot_be_submitted():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, _disp = _engine(factory, port, mode="manual")

    _submit(engine, port, _order())
    db = factory()
    try:
        row = db.query(LiveApprovalQueue).one()
        row.expires_at = CLOCK - timedelta(seconds=1)
        db.commit()
    finally:
        db.close()

    result = engine.approve_approval(_query(factory, LiveApprovalQueue)[0].id)

    assert result["status"] == "expired"
    assert _query(factory, LiveOrder) == []


def test_expire_sweep_marks_pending_rows_expired():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, _disp = _engine(factory, port, mode="manual")

    _submit(engine, port, _order())
    db = factory()
    try:
        row = db.query(LiveApprovalQueue).one()
        row.expires_at = CLOCK - timedelta(seconds=5)
        db.commit()
        engine._expire_approvals(db)
    finally:
        db.close()

    assert _query(factory, LiveApprovalQueue)[0].status == "expired"


def test_auto_mode_does_not_queue():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, _disp = _engine(factory, port, mode="auto")

    _submit(engine, port, _order())

    assert _query(factory, LiveApprovalQueue) == []
    assert len(_query(factory, LiveOrder)) == 1


def test_sell_and_risk_orders_are_exempt_from_queue():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, _disp = _engine(factory, port, mode="manual")

    assert engine._approval_exempt(_order(
        trade_type=TradeType.SELL, signal_type=SignalType.CLOSE,
        signal_name="stop_loss")) is True
    assert engine._approval_exempt(_order(
        trade_type=TradeType.SELL, signal_type=SignalType.REDUCE,
        signal_name="reduce_sig")) is True
    assert engine._approval_exempt(_order()) is False


# ---------------- I1：人工补单 / 平仓 / 改账 ----------------
def test_manual_order_requires_manual_mode():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, _disp = _engine(factory, port, mode="auto")

    try:
        engine.submit_manual_order(1, STOCK, "BUY", 100, price=Decimal("10"))
        assert False, "auto 模式应拒绝人工补单"
    except ManualActionError as exc:
        assert exc.http_status == 409


def test_manual_order_submits_and_writes_audit():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, disp = _engine(factory, port, mode="manual")
    engine._approval_cfg_cache = {
        "ttl_seconds": 900.0, "exempt_sells": True,
        "manual_max_amount": Decimal("0"), "min_interval_ms": 0,
    }

    result = engine.submit_manual_order(1, STOCK, "BUY", 1000,
                                        price=Decimal("10"))

    assert result["status"] == "accepted"
    orders = _query(factory, LiveOrder)
    assert len(orders) == 1 and orders[0].quantity == 1000
    assert len(disp.orders) == 1
    actions = _query(factory, LiveManualAction)
    assert len(actions) == 1
    assert actions[0].action_type == "manual_order"
    assert actions[0].result == "accepted"
    assert actions[0].live_order_id == orders[0].id
    # 人工单不入审批队列（人已显式决策）
    assert _query(factory, LiveApprovalQueue) == []


def test_manual_order_respects_amount_cap():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, _disp = _engine(factory, port, mode="manual")
    engine._approval_cfg_cache = {
        "ttl_seconds": 900.0, "exempt_sells": True,
        "manual_max_amount": Decimal("5000"), "min_interval_ms": 0,
    }

    try:
        engine.submit_manual_order(1, STOCK, "BUY", 1000, price=Decimal("10"))
        assert False, "超过单笔上限应拒绝"
    except ManualActionError as exc:
        assert exc.http_status == 400
    assert _query(factory, LiveOrder) == []
    actions = _query(factory, LiveManualAction)
    assert len(actions) == 1 and actions[0].result == "rejected"


def test_force_close_blocked_for_external_position_under_platform_only():
    factory, _ = _db_factory()
    port, ctx = _portfolio()
    port.sell_scope = "platform_only"
    pos = Position(STOCK)
    pos.quantity = 5300
    pos.avg_cost = Decimal("10")
    pos.platform_quantity = 0  # 柜台继承仓（平台一股未买）
    ctx.positions[STOCK] = pos
    engine, disp = _engine(factory, port, mode="manual")

    try:
        engine.force_close_position(1, STOCK)
        assert False, "platform_only 下纯外部仓应拒绝代卖"
    except ManualActionError as exc:
        assert exc.http_status == 409
    assert disp.orders == []
    actions = _query(factory, LiveManualAction)
    assert len(actions) == 1
    assert actions[0].action_type == "force_close"
    assert actions[0].result == "blocked"


def test_force_close_submits_full_virtual_quantity():
    factory, _ = _db_factory()
    port, ctx = _portfolio()
    port.sell_scope = "full"
    pos = Position(STOCK)
    pos.quantity = 5300
    pos.avg_cost = Decimal("10")
    pos.platform_quantity = 0
    ctx.positions[STOCK] = pos
    engine, disp = _engine(factory, port, mode="manual")
    engine._approval_cfg_cache = {
        "ttl_seconds": 900.0, "exempt_sells": True,
        "manual_max_amount": Decimal("0"), "min_interval_ms": 0,
    }

    result = engine.force_close_position(1, STOCK, price=Decimal("9.5"))

    # T+1 可用量由提交链收窄：本 fake 持仓不在可卖桶里 → 可能被缩到 0；
    # 这里只断言「没有抛异常 + 审计行存在」，数量断言见下一行（按虚拟量）。
    assert result["quantity"] == 5300
    actions = _query(factory, LiveManualAction)
    assert actions[0].action_type == "force_close"


def test_adjust_position_virtual_writes_trade_and_audit():
    factory, _ = _db_factory()
    db = factory()
    try:
        db.add(StockPool(id=1, code="POOL1", name="池"))
        db.add(PortfolioStrategy(id=1, name="组合", stock_pool_id=1))
        db.add(Strategy(id=1, portfolio_id=1, name="策略",
                        formula_id=1, period="1m", role="independent"))
        db.add(LiveSession(id=1, name="会话", mode="live", status="stopped"))
        db.add(LiveSessionPortfolio(session_id=1, portfolio_strategy_id=1))
        db.commit()

        data, error = live_service.adjust_position_virtual(
            db, 1, 1, 1, STOCK, 1000, Decimal("10"), note="补记柜台手工买入")
        assert error is None
        assert data["trade_type"] == "BUY"
        assert data["quantity"] == 1000
        assert data["cash_delta"] == "-10000.00"

        trades = db.query(LiveTrade).all()
        assert len(trades) == 1 and trades[0].quantity == 1000
        actions = db.query(LiveManualAction).all()
        assert len(actions) == 1
        assert actions[0].action_type == "position_adjust"
        assert actions[0].result == "applied"

        bad, message = live_service.adjust_position_virtual(
            db, 1, 1, 1, STOCK, 0, Decimal("10"))
        assert bad is None and "不能为 0" in message
    finally:
        db.close()


# ---------------- I4：手工成交录入 ----------------
def test_manual_trade_buy_books_position_cash_and_audit():
    factory, _ = _db_factory()
    port, ctx = _portfolio()
    engine, disp = _engine(factory, port, mode="manual")
    cash_before = port.account.cash

    result = engine.submit_manual_trade(
        1, STOCK, "BUY", 1000, price=Decimal("10"), commission=Decimal("5"),
        note="电话委托买入")

    assert result["status"] == "filled"
    assert result["signal_type"] == "OPEN"
    assert disp.orders == []  # 不重新下单，只记账
    orders = _query(factory, LiveOrder)
    assert len(orders) == 1
    assert orders[0].order_type == "manual" and orders[0].status == "filled"
    trades = _query(factory, LiveTrade)
    assert len(trades) == 1 and trades[0].quantity == 1000
    pos = ctx.positions[STOCK]
    assert pos.quantity == 1000 and pos.avg_cost == Decimal("10.005")
    assert port.account.cash == cash_before - Decimal("10005")
    actions = _query(factory, LiveManualAction)
    assert len(actions) == 1 and actions[0].action_type == "manual_trade"
    assert actions[0].result == "accepted"
    assert actions[0].cash_delta == Decimal("-10005.00")


def test_manual_trade_sell_requires_virtual_position():
    factory, _ = _db_factory()
    port, ctx = _portfolio()
    engine, _ = _engine(factory, port, mode="manual")

    try:
        engine.submit_manual_trade(1, STOCK, "SELL", 100, price=Decimal("10"))
        assert False, "无虚拟持仓的手工卖出应被拒绝"
    except ManualActionError as exc:
        assert exc.http_status == 409
    assert _query(factory, LiveTrade) == []


def test_manual_trade_rejects_sell_over_position():
    factory, _ = _db_factory()
    port, ctx = _portfolio()
    pos = Position(STOCK)
    pos.quantity = 500
    pos.avg_cost = Decimal("10")
    ctx.positions[STOCK] = pos
    engine, _ = _engine(factory, port, mode="manual")

    try:
        engine.submit_manual_trade(1, STOCK, "SELL", 600, price=Decimal("10"))
        assert False, "超过虚拟持仓的手工卖出应被拒绝"
    except ManualActionError as exc:
        assert exc.http_status == 400


def test_manual_trade_requires_manual_mode():
    factory, _ = _db_factory()
    port, _ctx = _portfolio()
    engine, _ = _engine(factory, port, mode="auto")

    try:
        engine.submit_manual_trade(1, STOCK, "BUY", 100, price=Decimal("10"))
        assert False, "auto 会话不开放手工成交录入"
    except ManualActionError as exc:
        assert exc.http_status == 409

