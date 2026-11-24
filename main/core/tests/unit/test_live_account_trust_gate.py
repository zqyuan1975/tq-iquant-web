"""P0-4（2026-11-24）回归：账户读取不可信时停止新开仓 BUY。

真实故障（2026-11-24 08:54）：TDX 客户端连接失败 → 账户快照整轮跳过，但
`_latest_account_cash` 保留失败前旧值，提交链没有任何可信度闸门（fail-open），
BUY 额度继续按旧快照放大；直到 09:22 重启会话才恢复，且外发告警因 DNS 失败无人收到。

覆盖（注入时钟状态，不连真实柜台）：
- 曾成功、随后不可信 >120s → BUY 拦截、SELL 放行、决策事件/原因可读；
- 不可信未超过阈值 → 不拦（单次抖动/正常重连）；
- 从未成功读取过（通道不支持账户查询/刚启动）→ 不拦，避免误伤。
"""
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import OrderEvent, TradeEvent
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import Base
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
    last_unconfirmed_order_id = None
    last_broker_order_id = None

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

    def get_cash(self):
        return {}

    def query_orders(self, order_id=None):
        return []

    def execution_receipts(self):
        return []


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


def _order(trade_type=TradeType.BUY):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=_STOCK,
        trade_type=trade_type, signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("9.3"),
        bar_time=_BT, signal_name="open_sig",
    )


def _mark_untrusted(engine, seconds):
    import time
    engine._latest_account_cash = Decimal("100000")
    engine._latest_account_cash_at = datetime(2026, 8, 5, 9, 20)
    engine._acct_untrusted_since = time.monotonic() - seconds


def test_buy_blocked_when_account_untrusted():
    factory = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp)
    _mark_untrusted(engine, 200)

    db = factory()
    order = _order(TradeType.BUY)
    engine._submit_one_order(db, port, order, "1m")
    db.close()

    assert disp.placed == [], "账户读取不可信超过阈值必须停新开仓"
    assert "账户读取不可信" in (order._submit_reason or "")
    events = [e for e in engine._recorder.drain()
              if e.gate == "account_untrusted"]
    assert events and events[-1].action == "block"
    assert "最后成功快照" in (events[-1].message or "")


def test_sell_allowed_when_account_untrusted():
    factory = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp)
    _mark_untrusted(engine, 200)

    db = factory()
    order = _order(TradeType.SELL)
    port.account.cash = Decimal("100000")
    ctx = port.find_strategy(1)
    ctx.positions[_STOCK] = Position(_STOCK)
    ctx.positions[_STOCK].buy(100, Decimal("9.0"), datetime(2026, 8, 4, 9, 30))
    engine._submit_one_order(db, port, order, "1m")
    db.close()

    assert len(disp.placed) == 1, "降敞口的 SELL 不能被账户可信度闸门拦截"


def test_within_threshold_not_blocked():
    factory = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp)
    _mark_untrusted(engine, 30)

    db = factory()
    order = _order(TradeType.BUY)
    engine._submit_one_order(db, port, order, "1m")
    db.close()

    assert len(disp.placed) == 1, "短时抖动/正常重连不得触发拦截"


def test_never_succeeded_not_blocked():
    factory = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher()
    engine = _engine(factory, port, disp)
    import time
    engine._acct_untrusted_since = time.monotonic() - 200
    engine._latest_account_cash_at = None  # 从未成功读取过

    db = factory()
    order = _order(TradeType.BUY)
    engine._submit_one_order(db, port, order, "1m")
    db.close()

    assert len(disp.placed) == 1, "从未成功读取的通道不应被误拦（live_capital 兜底）"
