"""recover 峰值补种单测（2026-11-12）。

回测峰值 = max(建仓成交价, 建仓后每根该周期 bar 的 close)，逐根累积、只升不降。
实盘 recover 只有成交记录，中间 bar 的 close 从未落盘 → 峰值退化为买入价/成本，
移动止损基准偏低（触发更晚、保护更弱）。`LiveEngine._seed_position_peaks` 用
「日线 close 重算 → 账户快照 highest_price 历史 MAX → 现状（成本）」三级补种，
且只 update_high（只升不降）。

本文件覆盖：
1. 日线 close 重算得到真实峰值
2. 快照脏值（被写成成本）不污染结果
3. 日线取数失败 → 快照 MAX 兜底
4. 日线与快照都无 → 维持现状 + `gate=peak_seed/action=fallback` 事件
5. 锚点取 live_trades 最早一笔 BUY
6. 日线与快照 MAX 取较大者
7. 只升不降：重算值低于现有内存峰值时不覆盖
"""
from datetime import datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import (
    Base, LiveAccountPositionSnapshot, LiveAccountSnapshot,
    LiveDecisionEvent, LiveTrade,
)
from tq_iquant_shared.constants import SignalType, TradeType

_STOCK = "002922.SZ"
_SID = 1          # 组合 = 策略（单组合单策略）
_STRATEGY = 1


# ---------------- 共用辅助 ----------------
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


class _StubDispatcher:
    """最小 dispatcher：日线取数可配置（返回序列 / 抛异常），其余端点空实现。"""

    channel = "tdx_tq"

    def __init__(self, bars=None, exc=None):
        self._bars = bars if bars is not None else []
        self._exc = exc
        self.quote_calls = []

    def query_quote(self, code, period="1m", count=10):
        self.quote_calls.append((code, period, count))
        if self._exc is not None:
            raise self._exc
        return list(self._bars)

    def query_calendar(self, *args, **kwargs):
        raise RuntimeError("calendar unavailable")

    def place_order(self, *args, **kwargs):  # pragma: no cover — 本文件不下单
        raise AssertionError("recover 不应下单")

    def get_positions(self):
        return []

    def query_positions(self):
        return []

    def get_cash(self):
        return None

    def execution_receipts(self):
        return []

    def check_price_limit(self, *args, **kwargs):
        return None

    def price_limit_bounds(self, *args, **kwargs):
        return None


def _portfolio(initial=Decimal("100000")):
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=_SID, initial_capital=initial, risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=_STRATEGY, period="1d",
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("0.1"),
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN, "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0.03"),
    )
    port.strategies.append(ctx)
    return port


def _engine(factory, disp, port):
    poller = BarPoller(disp, [_STOCK], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )


def _buy_trade(db, when, price="22.00", qty=2500):
    db.add(LiveTrade(
        live_session_id=1,
        portfolio_strategy_id=_SID,
        strategy_id=_STRATEGY,
        stock_code=_STOCK,
        trade_type="buy",
        price=Decimal(price),
        quantity=qty,
        amount=Decimal(price) * qty,
        commission=Decimal("5"),
        stamp_duty=Decimal("0"),
        trade_time=when,
    ))
    db.commit()


def _bar(day, close):
    """日线 bar（TDX 口径：stime 为 14 位 'yyyymmddHHMMSS'）。"""
    return {
        "stime": day.replace("-", "") + "150000",
        "open": close, "high": close, "low": close, "close": close,
        "volume": 10000,
    }


def _snapshot(db, when, hp):
    snap = LiveAccountSnapshot(
        live_session_id=1, engine="tdx", captured_at=when, total_asset=Decimal("100000"),
    )
    db.add(snap)
    db.flush()
    db.add(LiveAccountPositionSnapshot(
        snapshot_id=snap.id,
        stock_code=_STOCK,
        quantity=2500,
        cost_price=Decimal("22.003"),
        current_price=Decimal("21.32"),
        market_value=Decimal("53300"),
        highest_price=Decimal(hp),
    ))
    db.commit()


def _events(db, gate="peak_seed"):
    return db.query(LiveDecisionEvent).filter(LiveDecisionEvent.gate == gate).all()


# ---------------- 1. 日线重算 ----------------
def test_recover_peak_from_daily_closes():
    """日线 close 序列 [22.00, 22.04, 22.07, 21.32] → recover 后峰值 22.07。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[
        _bar("2026-11-10", "22.00"), _bar("2026-11-11", "22.04"),
        _bar("2026-11-12", "22.07"), _bar("2026-11-13", "21.32"),
    ])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    engine.recover(db)
    db.close()

    pos = port.strategies[0].positions[_STOCK]
    assert pos.highest_price == Decimal("22.07")
    assert pos.quantity == 2500
    # 取数走日线、且每 code 只拉一次
    assert [c[1] for c in disp.quote_calls] == ["1d"]


# ---------------- 2. 脏快照不污染 ----------------
def test_recover_peak_ignores_dirty_snapshot():
    """快照峰值被写成成本 22.003（脏），日线给 22.07 → 取 22.07。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[
        _bar("2026-11-10", "22.00"), _bar("2026-11-11", "22.04"),
        _bar("2026-11-12", "22.07"), _bar("2026-11-13", "21.32"),
    ])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    _snapshot(db, datetime(2026, 11, 12, 13, 21, 17), "22.003")
    engine.recover(db)
    db.close()

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.07")


# ---------------- 3. 日线失败 → 快照 MAX ----------------
def test_recover_peak_tq_failure_falls_back_to_snapshot_max():
    """日线取数抛异常 → 用历史快照 highest_price MAX（22.07），不落 fallback 事件。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(exc=TimeoutError("tdx lock timeout"))
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    _snapshot(db, datetime(2026, 11, 11, 9, 32), "22.07")
    _snapshot(db, datetime(2026, 11, 12, 13, 21, 17), "22.003")  # 脏值
    engine.recover(db)
    events = _events(db)
    db.close()

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.07")
    assert events == []


# ---------------- 4. 两级都无 → 维持现状 + fallback 事件 ----------------
def test_recover_peak_no_history_keeps_cost_seed():
    """日线返回空、无快照 → 峰值维持买入价（现状），并落 peak_seed/fallback 事件。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    engine.recover(db)
    events = _events(db)
    db.close()

    # 成交重放的买入价，不因「无数据」被拉低或抬高
    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.00")
    assert len(events) == 1
    assert events[0].action == "fallback"
    assert events[0].stock_code == _STOCK


# ---------------- 5. 锚点取最早一笔 BUY ----------------
def test_peak_anchor_uses_earliest_buy_trade():
    """两笔买入（11-10 / 11-11）→ 锚点取 11-10，11-10 的高 close 计入峰值。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[
        _bar("2026-11-10", "22.50"), _bar("2026-11-11", "22.10"),
        _bar("2026-11-12", "21.90"),
    ])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 11, 9, 31, 10), price="22.10", qty=1000)
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54), price="22.00", qty=1000)
    engine.recover(db)
    db.close()

    pos = port.strategies[0].positions[_STOCK]
    assert pos.quantity == 2000
    # 22.50 只在 11-10 出现：锚点若不是最早那笔就不会取到
    assert pos.highest_price == Decimal("22.50")


# ---------------- 6. 日线与快照 MAX 取较大者 ----------------
def test_recover_peak_takes_max_of_daily_and_snapshot():
    """日线 22.07、快照 MAX 22.63 → 取 22.63（快照兜底不压低更高值）。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[
        _bar("2026-11-10", "22.00"), _bar("2026-11-11", "22.07"),
    ])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    _snapshot(db, datetime(2026, 11, 11, 9, 32), "22.63")
    engine.recover(db)
    db.close()

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.63")


# ---------------- 7. 只升不降 ----------------
def test_peak_seed_never_lowers_existing_peak():
    """重算值低于现有内存峰值 → 不覆盖（update_high 只升不降）。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[_bar("2026-11-10", "21.50")])
    engine = _engine(factory, disp, port)
    pos = Position(_STOCK)
    pos.buy(2500, Decimal("20.00"), datetime(2026, 11, 10, 9, 31, 54))
    pos.update_high(Decimal("23.00"))
    port.strategies[0].positions[_STOCK] = pos

    db = factory()
    engine._seed_position_peaks(db, {_SID: port}, {})
    db.close()

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("23.00")
