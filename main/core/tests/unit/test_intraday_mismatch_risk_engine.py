"""盘中逐 1m 错周期风控：真实引擎链路用例（Portfolio.on_bar → LiveEngine._handle_bar → 提交链）。

规范（2026-11-20）：
- ``intraday_mismatched_risk_enabled=true`` 时，盘中 1m bar 跌破止损 →
  风控卖单**当日提交**，不受 ``risk_timing=next_day`` 顺延；与 ``immediate`` 结果一致。
- 开关关闭（默认；NULL 未设置同样为关）→ 1m 错周期 bar 只更新价格快照，
  不评估风控、不产生卖单。
- 1m bar 必须真正驱动 1d 策略的风控评估（不被周期对齐逻辑吞掉）。
"""
from datetime import datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import BarEvent, TradeEvent
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import Base, LiveOrder
from tq_iquant_shared.constants import SignalType, TradeType

_STOCK = "600000.SH"
# 周三 10:31（盘中）：bar 标签在交易时段内，日历 fail-open 可放行
_BAR_TIME = datetime(2026, 8, 5, 10, 31)
_BUY_TIME = datetime(2026, 7, 29, 10, 0)


def _db_factory():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    return SessionLocal, engine


class _StubDispatcher:
    """最小 dispatcher：只实现提交链会碰到的接口（成交即时回报）。"""

    channel = "test"
    supports_remark = False
    supports_cancel = True
    supports_backfill = False
    last_enqueued_order_id = None
    last_unconfirmed_order_id = None
    last_broker_order_id = None
    last_unconfirmed_message = None
    last_cancel_result = None

    def __init__(self):
        self.placed = []

    def query_calendar(self, *args, **kwargs):
        raise RuntimeError("calendar unavailable in stub")

    def heartbeat(self):
        return True

    def order_id(self, order):
        return ("stub" + str(order.stock_code) + str(order.bar_time)).ljust(32, "0")[:32]

    def place_order(self, order, price_type=None):
        self.placed.append(order)
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

    def check_price_limit(self, code, side, price):
        return None

    def price_limit_bounds(self, code):
        return None

    def get_positions(self):
        return []

    def query_positions(self, *args, **kwargs):
        return []

    def query_quote(self, *args, **kwargs):
        return []

    def query_account(self, *args, **kwargs):
        return {}

    def get_cash(self):
        return {}

    def execution_receipts(self):
        return []


def _daily_portfolio(risk_timing="next_day", stop_loss=Decimal("0.05")):
    """1d 策略 + 10 元成本 1000 股持仓（止损 5%）。"""
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1d",
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("0.1"),
        risk_timing=risk_timing,
    )
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=stop_loss,
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    pos = Position(_STOCK)
    pos.apply_trade(TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=_STOCK,
        trade_type=TradeType.BUY, price=Decimal("10"), quantity=1000,
        amount=Decimal("10000"), commission=Decimal("0"),
        stamp_duty=Decimal("0"), trade_time=_BUY_TIME,
    ))
    ctx.positions[_STOCK] = pos
    port.strategies.append(ctx)
    return port, ctx


def _bar_1m(close, bar_time=_BAR_TIME):
    return BarEvent(
        stocks={_STOCK: {
            "open": Decimal("9.6"), "high": Decimal("9.6"),
            "low": Decimal(str(close)), "close": Decimal(str(close)),
            "volume": 10000,
        }},
        bar_time=bar_time,
        period="1m",
    )


def _engine(factory, port, disp, mismatch_flag):
    poller = BarPoller(disp, [_STOCK], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        intraday_mismatched_risk_enabled=mismatch_flag,
    )


def _sell_orders(db, statuses=None):
    q = db.query(LiveOrder).filter(LiveOrder.trade_type == "sell")
    if statuses:
        q = q.filter(LiveOrder.status.in_(statuses))
    return q.all()


def test_1m_stop_loss_with_flag_off_produces_no_sell_order():
    """开关关闭（含 NULL 未设置）→ 1m 错周期 bar 不触发风控，不产生任何卖单。"""
    factory, _ = _db_factory()
    port, _ctx = _daily_portfolio()
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp, mismatch_flag=False)

    engine._handle_bar(port, _bar_1m("9.0"))  # 跌破 9.5 止损线

    assert disp.placed == []
    db = factory()
    try:
        assert db.query(LiveOrder).count() == 0
    finally:
        db.close()
    assert engine._signal_bar_deferred == []


def test_1m_stop_loss_with_flag_on_submits_same_day_despite_next_day():
    """开关开启 + next_day：1m 跌破止损 → 当日提交，不进入次日 deferred。"""
    factory, _ = _db_factory()
    port, ctx = _daily_portfolio(risk_timing="next_day")
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp, mismatch_flag=True)

    engine._handle_bar(port, _bar_1m("9.0"))

    assert len(disp.placed) == 1
    order = disp.placed[0]
    assert order.trade_type == TradeType.SELL
    assert order.signal_type == SignalType.STOP_LOSS
    assert order.quantity == 1000          # 整仓
    # 信号 bar 收盘价 9.0；风控卖单默认不让价（risk_chase_ticks=0）→ 原价 9.0
    assert order.price == Decimal("9.0")
    assert order.bar_time == _BAR_TIME     # 就是那根 1m bar，未被对齐成日线 00:00
    assert order.signal_bar_date is None   # 风控单不再携带未来函数日期
    assert ctx.period == "1d"              # 1m bar 真的驱动了 1d 策略
    assert engine._signal_bar_deferred == []

    db = factory()
    try:
        rows = _sell_orders(db)
        assert len(rows) == 1
        assert rows[0].status in ("submitted", "filled")
        assert rows[0].bar_time == _BAR_TIME
        # 对照：不存在次日 deferred 行
        assert db.query(LiveOrder).filter(LiveOrder.status == "deferred").count() == 0
    finally:
        db.close()


def test_1m_stop_loss_immediate_and_next_day_have_same_result():
    """risk_timing=immediate 与 next_day 的风控卖单完全一致（价格/数量/时点/状态）。"""
    def _run(risk_timing):
        factory, _ = _db_factory()
        port, _ctx = _daily_portfolio(risk_timing=risk_timing)
        disp = _StubDispatcher()
        engine = _engine(factory, port, disp, mismatch_flag=True)
        engine._handle_bar(port, _bar_1m("9.0"))
        db = factory()
        try:
            rows = _sell_orders(db)
            status = rows[0].status if rows else None
        finally:
            db.close()
        assert len(disp.placed) == 1
        o = disp.placed[0]
        return (o.price, o.quantity, o.bar_time, o.signal_bar_date, status)

    assert _run("immediate") == _run("next_day")


def test_mismatched_1m_bar_not_swallowed_for_daily_strategy():
    """周期不匹配不被吞：1m bar + period='1m' 仍产出 1d 策略的止损卖单。"""
    port, ctx = _daily_portfolio()
    orders = port.on_bar(_bar_1m("9.0"), period="1m", mismatched_risk=True)

    assert ctx.period == "1d"
    assert len(orders) == 1
    assert orders[0].signal_type == SignalType.STOP_LOSS
    assert orders[0].bar_time == _BAR_TIME
    assert orders[0].period == "1m"

    # 对照：同一根 bar 关闭错周期风控 → 不评估、不出单
    port2, _ctx2 = _daily_portfolio()
    assert port2.on_bar(_bar_1m("9.0"), period="1m", mismatched_risk=False) == []


def test_daily_bar_risk_sell_also_bypasses_signal_bar_date():
    """风控单在任何周期都不携带 signal_bar_date（修复点：日线 bar 的风控单曾会 deferred）。"""
    port, _ctx = _daily_portfolio(risk_timing="next_day")
    bar = BarEvent(
        stocks={_STOCK: {
            "open": Decimal("9.6"), "high": Decimal("9.6"),
            "low": Decimal("9.0"), "close": Decimal("9.0"), "volume": 10000,
        }},
        bar_time=datetime(2026, 8, 5, 0, 0),
        period="1d",
    )
    orders = port.on_bar(bar, period="1d")

    assert len(orders) == 1
    assert orders[0].signal_type == SignalType.STOP_LOSS
    assert orders[0].signal_bar_date is None
