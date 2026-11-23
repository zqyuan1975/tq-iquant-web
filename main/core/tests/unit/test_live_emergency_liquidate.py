"""M2 一键全平（引擎级）单测：三态计数、整仓数量、审计字段与失败分支。

覆盖用户要求的九个分支：
- 逐票遍历（多组合/多策略/多标的）
- 卖出数量取整仓（含外部/继承仓）
- 无价跳过（行情快照与成本价都缺失）
- 单票异常回滚但继续其它票
- 审计写入（operator / stock_code / requested_qty / final_qty / result，成功与失败都写）
- T+1 拒绝（available=0 不得计为 submitted）
- 桥离线拒单（不得计为 submitted）
- 收盘后转 deferred（22:35 触发）
- 部分成功计数（submitted / blocked 各自准确）
"""
from datetime import datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import TradeEvent
from core.engine.http_bridge_dispatcher import BridgeUnavailableError
from core.engine.live_engine import LiveEngine, _SUBMIT_OPERATOR_UNKNOWN
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import Base, LiveDecisionEvent, LiveOrder

STOCK_A = "600000.SH"
STOCK_B = "000001.SZ"
CLOCK = datetime(2026, 11, 18, 10, 0)
CLOCK_AFTER_CLOSE = datetime(2026, 11, 18, 22, 35)


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

    return factory


class _FakeDispatcher:
    channel = "fake_bridge"
    supports_remark = True

    def __init__(self):
        self.orders = []

    def heartbeat(self):
        return True

    def close(self):
        pass

    def query_calendar(self):
        return []

    @staticmethod
    def order_id(order):
        return "CLM-%s-%s" % (order.stock_code, id(order))

    def place_order(self, order):
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


def _portfolio():
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.3"),
                              daily_loss_limit=Decimal("0.3"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1d",
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("1"),
    )
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port, ctx


def _position(code, qty, cost=Decimal("10")):
    pos = Position(code)
    pos.quantity = qty
    pos.avg_cost = cost
    pos.platform_quantity = 0  # 纯外部/继承仓也要整仓卖出
    return pos


def _engine(factory, port, disp=None, clock=CLOCK):
    disp = disp or _FakeDispatcher()
    poller = BarPoller(disp, [STOCK_A, STOCK_B], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        execution_mode="auto",
    )
    engine.ctx.clock = lambda: clock
    return engine, disp


def _events(factory, gate="global_emergency"):
    db = factory()
    try:
        return (db.query(LiveDecisionEvent)
                .filter(LiveDecisionEvent.gate == gate)
                .order_by(LiveDecisionEvent.id).all())
    finally:
        db.close()


def test_liquidates_every_position_with_full_quantity_and_audits():
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 300)
    ctx.positions[STOCK_B] = _position(STOCK_B, 700, cost=Decimal("20"))
    engine, disp = _engine(factory, port)

    result = engine.emergency_liquidate_all(reason="风控降险", operator="张三")

    assert result["submitted"] == 2
    assert result["blocked"] == 0
    assert result["deferred"] == 0
    assert {o.stock_code: o.quantity for o in disp.orders} == {
        STOCK_A: 300, STOCK_B: 700}
    events = _events(factory)
    assert len(events) == 2
    by_code = {e.stock_code: e for e in events}
    assert set(by_code) == {STOCK_A, STOCK_B}
    for code, qty in ((STOCK_A, 300), (STOCK_B, 700)):
        ev = by_code[code]
        assert ev.operator == "张三"
        assert ev.result == "submitted"
        assert ev.requested_qty == qty
        assert ev.final_qty == qty
        assert ev.gate == "global_emergency" and ev.action == "liquidate"


def test_audit_operator_defaults_when_not_given():
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 100)
    engine, _ = _engine(factory, port)

    engine.emergency_liquidate_all()

    assert _events(factory)[0].operator == _SUBMIT_OPERATOR_UNKNOWN


def test_no_price_skips_and_still_audits():
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 500, cost=None)
    engine, disp = _engine(factory, port)

    result = engine.emergency_liquidate_all()

    assert result["submitted"] == 0
    assert result["blocked"] == 1
    assert disp.orders == []
    assert "无可用价格" in result["details"][0]["reason"]
    ev = _events(factory)[0]
    assert ev.result == "blocked" and ev.final_qty == 0


def test_single_position_exception_rolls_back_and_continues():
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 100)
    ctx.positions[STOCK_B] = _position(STOCK_B, 200)
    engine, disp = _engine(factory, port)
    original = engine._submit_one_order

    def flaky(db, portfolio, order, period, **kwargs):
        if order.stock_code == STOCK_A:
            raise RuntimeError("mock submit boom")
        return original(db, portfolio, order, period, **kwargs)

    engine._submit_one_order = flaky

    result = engine.emergency_liquidate_all()

    assert result["submitted"] == 1
    assert result["errors"] == 1
    assert [o.stock_code for o in disp.orders] == [STOCK_B]
    events = _events(factory)
    assert len(events) == 2
    by_code = {e.stock_code: e for e in events}
    assert by_code[STOCK_A].result == "blocked"
    assert "提交异常" in by_code[STOCK_A].message
    assert by_code[STOCK_A].final_qty == 0
    assert by_code[STOCK_B].result == "submitted"


def test_t1_zero_available_is_blocked_not_submitted():
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 1000)
    engine, disp = _engine(factory, port)
    engine._t1_checker.set_available_map({STOCK_A: 0})

    result = engine.emergency_liquidate_all()

    assert result["submitted"] == 0
    assert result["blocked"] == 1
    assert disp.orders == []
    assert "T+1" in result["details"][0]["reason"]
    assert _events(factory)[0].result == "blocked"


def test_bridge_unavailable_is_blocked_not_submitted():
    factory = _db_factory()

    class _OfflineDispatcher(_FakeDispatcher):
        def place_order(self, order):
            raise BridgeUnavailableError("mock bridge offline")

    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 100)
    engine, _ = _engine(factory, port, disp=_OfflineDispatcher())

    result = engine.emergency_liquidate_all()

    assert result["submitted"] == 0
    assert result["blocked"] == 1
    assert "桥离线" in result["details"][0]["reason"]
    assert _events(factory)[0].result == "blocked"


def test_after_close_is_deferred_not_submitted():
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 400)
    engine, disp = _engine(factory, port, clock=CLOCK_AFTER_CLOSE)

    result = engine.emergency_liquidate_all()

    assert result["submitted"] == 0
    assert result["deferred"] == 1
    assert disp.orders == []
    assert "顺延" in result["details"][0]["reason"]
    ev = _events(factory)[0]
    assert ev.result == "deferred" and ev.final_qty == 0


def test_partial_success_counts_are_exact():
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 100)
    ctx.positions[STOCK_B] = _position(STOCK_B, 200)
    engine, disp = _engine(factory, port)
    engine._t1_checker.set_available_map({STOCK_B: 0})

    result = engine.emergency_liquidate_all()

    assert result["submitted"] == 1
    assert result["blocked"] == 1
    assert result["deferred"] == 0
    assert result["errors"] == 0
    assert [o.stock_code for o in disp.orders] == [STOCK_A]
    status = {d["code"]: d["status"] for d in result["details"]}
    assert status == {STOCK_A: "submitted", STOCK_B: "blocked"}


def _live_order_count(factory):
    db = factory()
    try:
        return db.query(LiveOrder).count()
    finally:
        db.close()


def test_dry_run_tristate_without_side_effects():
    """dry-run：三态照真跑语义算，但不写单、不动持仓与资金，审计标 dry-run。"""
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 100)
    ctx.positions[STOCK_B] = _position(STOCK_B, 200)
    engine, disp = _engine(factory, port)
    engine._t1_checker.set_available_map({STOCK_B: 0})
    cash_before = port.account.cash
    qty_before = {c: p.quantity for c, p in ctx.positions.items()}

    result = engine.emergency_liquidate_all(
        reason="演练", operator="张三", dry_run=True)

    # 三态语义 = 真跑会发生什么
    assert result["submitted"] == 1
    assert result["blocked"] == 1
    assert result["deferred"] == 0
    assert result["errors"] == 0
    status = {d["code"]: d for d in result["details"]}
    assert set(status) == {STOCK_A, STOCK_B}
    assert status[STOCK_A]["status"] == "submitted"
    assert status[STOCK_A]["quantity"] == 100
    assert status[STOCK_B]["status"] == "blocked"
    assert "T+1" in status[STOCK_B]["reason"]
    assert set(status[STOCK_A]) == {"code", "quantity", "status", "reason"}

    # 零副作用：不发桥、不写 live_orders、持仓与资金不变
    assert disp.orders == []
    assert _live_order_count(factory) == 0
    assert {c: p.quantity for c, p in ctx.positions.items()} == qty_before
    assert port.account.cash == cash_before

    # 审计照写但标 dry-run，且 archived_at 已置（不进待处理告警）
    events = _events(factory, gate="global_emergency_dry_run")
    assert len(events) == 2
    for ev in events:
        assert ev.operator == "张三"
        assert ev.action == "liquidate_dry_run"
        assert ev.archived_at is not None
        assert ev.archive_reason == "dry-run"
        assert ev.message.startswith("[dry-run]")
    by_code = {e.stock_code: e for e in events}
    assert by_code[STOCK_A].result == "submitted"
    assert by_code[STOCK_B].result == "blocked"
    assert _events(factory) == []  # 不混入真实全平审计


def test_dry_run_after_close_reports_deferred():
    factory = _db_factory()
    port, ctx = _portfolio()
    ctx.positions[STOCK_A] = _position(STOCK_A, 400)
    engine, disp = _engine(factory, port, clock=CLOCK_AFTER_CLOSE)

    result = engine.emergency_liquidate_all(dry_run=True)

    assert result["deferred"] == 1
    assert result["submitted"] == 0
    assert result["blocked"] == 0
    assert result["details"][0]["status"] == "deferred"
    assert "顺延" in result["details"][0]["reason"]
    assert disp.orders == []
    assert _live_order_count(factory) == 0
    ev = _events(factory, gate="global_emergency_dry_run")[0]
    assert ev.result == "deferred" and ev.final_qty == 0
