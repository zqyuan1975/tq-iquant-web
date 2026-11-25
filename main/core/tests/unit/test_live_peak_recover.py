"""实盘峰值补种口径（2026-11-25 收敛）。

实盘峰值 = max(基准价（入场价/成本）, 建仓后逐 bar 抬峰并落库的表值)，只升不降。
``LiveEngine._seed_position_peaks`` 只认两个来源：``live_position_peaks`` 表值
与基准价 ``peak_base_price``（缺失用 ``avg_cost``）。历史上「策略周期 bar /
日线 close / 账户快照 highest_price 历史 MAX」的补历史来源已删除——它们把继承
仓/存量仓的峰值抬到入场前的高点，与回测「峰值从建仓价起算」不同源。

本文件覆盖：
1. 只认基准价：日线 bar 与账户快照都不参与补种（零 TQ、无 peak_seed 事件）
2. 基准价/基准时刻回填 peak_base_price / peak_base_at
3. 表值优先且完全不调 TQ
4. 只升不降：补种不压低现有内存峰值
5. UPSERT 不写低 / 清仓删行 / 超期清理
"""
from datetime import datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.bar_poller import BarPoller
from core.engine.event import BarEvent
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import (
    Base, LiveAccountPositionSnapshot, LiveAccountSnapshot,
    LiveDecisionEvent, LivePositionPeak, LiveTrade,
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

    def __init__(self, bars=None, exc=None, positions=None, bars_by_code=None):
        self._bars = bars if bars is not None else []
        self._exc = exc
        self._positions = positions if positions is not None else []
        self._bars_by_code = bars_by_code or {}
        self.quote_calls = []

    def query_quote(self, code, period="1m", count=10):
        self.quote_calls.append((code, period, count))
        if self._exc is not None:
            raise self._exc
        if code in self._bars_by_code:
            return list(self._bars_by_code[code])
        return list(self._bars)

    def query_calendar(self, *args, **kwargs):
        raise RuntimeError("calendar unavailable")

    def place_order(self, *args, **kwargs):  # pragma: no cover — 本文件不下单
        raise AssertionError("recover 不应下单")

    def get_positions(self):
        return list(self._positions)

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


def _snapshot(db, when, hp, code=_STOCK, qty=2500):
    snap = LiveAccountSnapshot(
        live_session_id=1, engine="tdx", captured_at=when, total_asset=Decimal("100000"),
    )
    db.add(snap)
    db.flush()
    db.add(LiveAccountPositionSnapshot(
        snapshot_id=snap.id,
        stock_code=code,
        quantity=qty,
        cost_price=Decimal("22.003"),
        current_price=Decimal("21.32"),
        market_value=Decimal("53300"),
        highest_price=Decimal(hp),
    ))
    db.commit()


def _events(db, gate="peak_seed"):
    return db.query(LiveDecisionEvent).filter(LiveDecisionEvent.gate == gate).all()


# ---------------- 1. 只认基准价，不补历史 ----------------
def test_recover_peak_baseline_only_ignores_bars_and_snapshot():
    """日线 bar 与账户快照都不再参与补种：峰值 = 基准价（买入价），零 TQ 取数。

    历史口径会把峰值抬到入场前的高点（22.07 / 22.63），与回测「峰值从建仓价
    起算」不同源 → 这里显式断言不再回补。
    """
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[
        _bar("2026-11-10", "22.00"), _bar("2026-11-11", "22.04"),
        _bar("2026-11-12", "22.07"), _bar("2026-11-13", "21.32"),
    ])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    _snapshot(db, datetime(2026, 11, 12, 13, 21, 17), "22.63")
    engine.recover(db)
    events = _events(db)
    db.close()

    pos = port.strategies[0].positions[_STOCK]
    assert pos.quantity == 2500
    # 峰值 = 基准价（含佣金成本），不是日线 22.07 / 快照 22.63
    assert pos.highest_price == pos.avg_cost == Decimal("22.002")
    assert disp.quote_calls == []
    assert events == []


# ---------------- 2. 基准价/基准时刻回填 ----------------
def test_recover_writes_peak_base_price_and_at():
    """表里没有基准列 → 按持仓成本 + 最早 BUY 时刻回填，供下次重启复用。"""
    factory, _ = _db_factory()
    port = _portfolio()
    engine = _engine(factory, _StubDispatcher(bars=[]), port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    engine.recover(db)
    row = db.query(LivePositionPeak).one()
    pos = port.strategies[0].positions[_STOCK]
    db.close()

    assert row.highest_price == pos.avg_cost
    assert row.peak_base_price == pos.avg_cost
    assert row.peak_base_at == datetime(2026, 11, 10, 9, 31, 54)


# ---------------- 3. 表值优先，不调 TQ ----------------
def test_recover_prefers_persisted_peak_table_without_tq():
    """live_position_peaks 有值 → 直接采用，**完全不调 TQ**（重启零取数）。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[_bar("2026-11-10", "22.00")])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    db.add(LivePositionPeak(
        live_session_id=1,
        portfolio_strategy_id=_SID,
        stock_code=_STOCK,
        highest_price=Decimal("22.63"),
        peak_bar_time=datetime(2026, 11, 11, 15, 0),
        peak_base_price=Decimal("22.00"),
        peak_base_at=datetime(2026, 11, 10, 9, 31, 54),
    ))
    db.commit()

    engine.recover(db)
    db.close()

    pos = port.strategies[0].positions[_STOCK]
    assert pos.highest_price == Decimal("22.63")
    assert disp.quote_calls == []


# ---------------- 4. 只升不降 ----------------
def test_peak_seed_never_lowers_existing_peak():
    """表值/基准价都低于现有内存峰值 → 不覆盖（update_high 只升不降）。"""
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
    assert disp.quote_calls == []


# ---------------- 5. 策略周期 bar 不再参与补种 ----------------
def test_recover_does_not_seed_from_strategy_period_bars():
    """策略周期 5m 也**不**用 5m bar close 补历史：峰值仍为基准价。"""
    factory, _ = _db_factory()
    port = _portfolio()
    port.strategies[0].period = "5m"
    disp = _StubDispatcher(bars=[
        {"stime": "20261111103000", "open": "22.40", "high": "22.60",
         "low": "22.30", "close": "22.50", "volume": 1000},
    ])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    engine.recover(db)
    db.close()

    pos = port.strategies[0].positions[_STOCK]
    assert pos.highest_price == pos.avg_cost
    assert disp.quote_calls == []


# ---------------- 阶段二 A：只升不降 / 清仓删行 / 超期清理 ----------------
def _with_position(port, qty=1000):
    pos = Position(_STOCK)
    pos.quantity = qty
    pos.avg_cost = Decimal("20.00")
    pos.highest_price = Decimal("20.00")
    port.strategies[0].positions[_STOCK] = pos
    return pos


def test_peak_flush_upserts_and_never_lowers():
    factory, _ = _db_factory()
    port = _portfolio()
    pos = _with_position(port)
    engine = _engine(factory, _StubDispatcher(), port)
    db = factory()

    engine._note_peak_raised(_SID, _STOCK, Decimal("22.50"), None)
    engine._flush_position_peaks(db)
    db.commit()
    engine._note_peak_raised(_SID, _STOCK, Decimal("21.00"), None)
    engine._flush_position_peaks(db)
    db.commit()

    row = db.query(LivePositionPeak).one()
    assert Decimal(str(row.highest_price)) == Decimal("22.50")
    db.close()


def test_peak_flush_deletes_row_after_position_cleared():
    """收尾清扫（allow_delete=True）在真正清仓后仍删行。"""
    factory, _ = _db_factory()
    port = _portfolio()
    pos = _with_position(port)
    engine = _engine(factory, _StubDispatcher(), port)
    db = factory()

    engine._note_peak_raised(_SID, _STOCK, Decimal("22.50"), None)
    engine._flush_position_peaks(db, allow_delete=True)
    db.commit()
    assert db.query(LivePositionPeak).count() == 1

    pos.quantity = 0
    engine._flush_position_peaks(db, allow_delete=True)
    db.commit()
    assert db.query(LivePositionPeak).count() == 0
    db.close()


# ---------------- 缺陷一：补种期只 UPSERT，绝不清仓删行 ----------------
def test_seed_first_pass_keeps_unimported_position_peak_row():
    """继承仓尚未导入时（第一轮补种），不得把它的峰值行当「已清仓」删掉。

    002745.SZ 的行存在于表里，但此刻 ctx.positions 只有 live_trades 重放出的
    002922.SZ —— 存活集合不完整。补种期默认 allow_delete=False → 只 UPSERT。
    """
    factory, _ = _db_factory()
    port = _portfolio()
    _with_position(port)  # 只有 002922.SZ（普通持仓）
    disp = _StubDispatcher(bars=[_bar("2026-11-10", "22.00")])
    engine = _engine(factory, disp, port)
    db = factory()
    db.add(LivePositionPeak(
        live_session_id=1, portfolio_strategy_id=_SID,
        stock_code="002745.SZ", highest_price=Decimal("10.83"),
        peak_bar_time=datetime(2026, 11, 10, 15, 0),
    ))
    db.commit()

    before = db.query(LivePositionPeak).count()
    engine._seed_position_peaks(db, {_SID: port}, {})
    db.commit()
    after = db.query(LivePositionPeak).count()

    rows = {r.stock_code: Decimal(str(r.highest_price))
            for r in db.query(LivePositionPeak).all()}
    db.close()

    assert before == 1 and after == 2  # 旧行保留 + 新种出 002922 行
    assert rows["002745.SZ"] == Decimal("10.83")  # 未被误删、未被写低
    assert rows[_STOCK] == Decimal("20.00")       # 无表值 → 基准价（成本）


def test_flush_without_allow_delete_never_deletes_rows():
    """默认 allow_delete=False：持仓已不在也只 UPSERT，表行数不变。"""
    factory, _ = _db_factory()
    port = _portfolio()
    pos = _with_position(port)
    engine = _engine(factory, _StubDispatcher(), port)
    db = factory()

    engine._note_peak_raised(_SID, _STOCK, Decimal("22.50"), None)
    engine._flush_position_peaks(db)
    db.commit()
    assert db.query(LivePositionPeak).count() == 1

    pos.quantity = 0  # 该票已清仓，但此刻集合不完整 → 不许删
    engine._flush_position_peaks(db)
    db.commit()
    assert db.query(LivePositionPeak).count() == 1
    db.close()


def test_recover_coexisting_traded_and_imported_positions():
    """普通持仓 + 继承仓共存：两行都保留，各自峰值正确。

    2026-11-25 口径：普通持仓无表值 → 峰值 = 基准价（成本 22.002，不再用日线/
    快照回补）；继承仓有表值 10.46 → 直接采用表值，**不再**用快照 10.83 自愈。
    """
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(
        bars_by_code={
            _STOCK: [_bar("2026-11-10", "22.00"), _bar("2026-11-11", "22.07")],
            "002745.SZ": [_bar("2026-11-12", "10.46")],
        },
        positions=[{
            "symbol": "002745.SZ", "volume": 5300, "available": 5300,
            "cost": "10.221", "market_value": "54171.3",
        }],
    )
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    _snapshot(db, datetime(2026, 11, 11, 9, 32), "22.07")
    _snapshot(db, datetime(2026, 11, 12, 15, 0), "10.83",
              code="002745.SZ", qty=5300)
    db.add(LivePositionPeak(
        live_session_id=1, portfolio_strategy_id=_SID,
        stock_code="002745.SZ", highest_price=Decimal("10.46"),
        peak_bar_time=datetime(2026, 11, 12, 15, 0),
    ))
    db.commit()

    engine.recover(db)
    rows = {r.stock_code: Decimal(str(r.highest_price))
            for r in db.query(LivePositionPeak).all()}
    db.close()

    traded = port.strategies[0].positions[_STOCK]
    imported = port.strategies[0].positions["002745.SZ"]
    assert rows[_STOCK] == traded.avg_cost == Decimal("22.002")
    assert rows["002745.SZ"] == Decimal("10.46")  # 表值权威，不用快照 10.83 抬高
    assert traded.highest_price == Decimal("22.002")
    assert imported.highest_price == Decimal("10.46")
    assert disp.quote_calls == []


# ---------------- 缺陷二：等值 UPSERT 刷新 updated_at（心跳） ----------------
def test_peak_flush_equal_value_refreshes_updated_at_only():
    """等值写入：highest_price / peak_bar_time 不变，updated_at 刷新（心跳）。"""
    factory, _ = _db_factory()
    port = _portfolio()
    _with_position(port)
    engine = _engine(factory, _StubDispatcher(), port)
    db = factory()
    old_ts = datetime(2026, 1, 1, 15, 0)
    old_bt = datetime(2026, 11, 10, 15, 0)
    db.add(LivePositionPeak(
        live_session_id=1, portfolio_strategy_id=_SID, stock_code=_STOCK,
        highest_price=Decimal("22.50"), peak_bar_time=old_bt, updated_at=old_ts,
    ))
    db.commit()

    engine._note_peak_raised(_SID, _STOCK, Decimal("22.50"), None)  # 等值
    engine._flush_position_peaks(db)
    db.commit()

    hp, bt, updated = (
        db.query(LivePositionPeak.highest_price,
                 LivePositionPeak.peak_bar_time,
                 LivePositionPeak.updated_at).one())
    db.close()

    assert Decimal(str(hp)) == Decimal("22.50")
    assert bt == old_bt            # 未抬高 → 来源 bar 时间保持原值
    assert updated > old_ts        # 心跳刷新，避免被 30 交易日清理误删


def test_peak_flush_equal_value_never_lowers_then_refreshes():
    """更低值不写（值不变）；随后等值心跳刷新 updated_at。"""
    factory, _ = _db_factory()
    port = _portfolio()
    _with_position(port)
    engine = _engine(factory, _StubDispatcher(), port)
    db = factory()
    old_ts = datetime(2026, 1, 1, 15, 0)
    db.add(LivePositionPeak(
        live_session_id=1, portfolio_strategy_id=_SID, stock_code=_STOCK,
        highest_price=Decimal("22.50"), updated_at=old_ts,
    ))
    db.commit()

    # 更低值：WHERE 不命中 → 整行不写（连 updated_at 也不动）
    engine._note_peak_raised(_SID, _STOCK, Decimal("21.00"), None)
    engine._flush_position_peaks(db)
    db.commit()
    hp, updated = db.query(
        LivePositionPeak.highest_price, LivePositionPeak.updated_at).one()
    assert Decimal(str(hp)) == Decimal("22.50")
    assert updated == old_ts

    # 等值：刷新心跳
    engine._note_peak_raised(_SID, _STOCK, Decimal("22.50"), None)
    engine._flush_position_peaks(db)
    db.commit()
    hp2, updated2 = db.query(
        LivePositionPeak.highest_price, LivePositionPeak.updated_at).one()
    db.close()
    assert Decimal(str(hp2)) == Decimal("22.50")
    assert updated2 > old_ts


def test_cleanup_stale_peak_rows_deletes_unheld_rows():
    factory, _ = _db_factory()
    port = _portfolio()
    engine = _engine(factory, _StubDispatcher(), port)
    db = factory()
    db.add(LivePositionPeak(
        live_session_id=1,
        portfolio_strategy_id=_SID,
        stock_code="600000.SH",
        highest_price=Decimal("9.90"),
        updated_at=datetime(2026, 1, 1, 15, 0),
    ))
    db.commit()

    engine._cleanup_stale_position_peaks(db)
    db.commit()
    assert db.query(LivePositionPeak).count() == 0
    db.close()


# ---------------- 阶段二 A：Portfolio 峰值钩子只在抬峰时触发 ----------------
def test_portfolio_peak_hook_fires_only_when_peak_raised():
    port = _portfolio()
    ctx = port.strategies[0]
    ctx.period = "1d"
    calls = []
    port.peak_persist_hook = (
        lambda pid, code, price, bt: calls.append(
            (pid, code, Decimal(str(price)))))
    _with_position(port)
    ctx.positions[_STOCK].highest_price = Decimal("22.00")

    def _bar_at(close):
        return BarEvent(
            stocks={_STOCK: {
                "open": close, "high": close, "low": close,
                "close": close, "volume": 1000,
            }},
            bar_time=datetime(2026, 11, 12, 9, 31),
            period="1d",
        )

    port.on_bar(_bar_at(Decimal("21.00")), signal_cache={}, period="1d")
    assert calls == []
    port.on_bar(_bar_at(Decimal("23.00")), signal_cache={}, period="1d")
    assert calls == [(_SID, _STOCK, Decimal("23.00"))]


# ---------------- 缺口一：继承仓（无 live_trades）导入后补种 ----------------
def test_recover_seeds_imported_position_without_trades():
    """无 live_trades 的继承仓：导入后按基准价补种，不再用快照/bar 回补。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(
        bars_by_code={"002745.SZ": [_bar("2026-11-12", "10.46")]},
        positions=[{
            "symbol": "002745.SZ", "volume": 5300, "available": 5300,
            "cost": "10.221", "market_value": "54171.3",
        }],
    )
    engine = _engine(factory, disp, port)
    db = factory()
    _snapshot(db, datetime(2026, 11, 12, 15, 0), "10.83",
              code="002745.SZ", qty=5300)
    engine.recover(db)
    db.close()

    pos = port.strategies[0].positions["002745.SZ"]
    assert pos.quantity == 5300
    # 快照 MAX 10.83 / 日线 close 10.46 都不再参与 → 峰值从入场成本起算
    assert pos.highest_price == Decimal("10.221")
    assert disp.quote_calls == []


# ---------------- 表值优先：快照 MAX 不再参与比对 ----------------
def test_peak_table_above_snapshot_max_uses_table_without_tq():
    """表值 22.63 ≥ 快照 MAX 22.07 → 直接采用表值，**零 TQ**。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[_bar("2026-11-10", "22.00")])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    _snapshot(db, datetime(2026, 11, 11, 9, 32), "22.07")
    db.add(LivePositionPeak(
        live_session_id=1,
        portfolio_strategy_id=_SID,
        stock_code=_STOCK,
        highest_price=Decimal("22.63"),
        peak_bar_time=datetime(2026, 11, 11, 15, 0),
    ))
    db.commit()

    engine.recover(db)
    events = _events(db)
    db.close()

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.63")
    assert disp.quote_calls == []
    assert events == []  # 不落 fallback / recompute


def test_peak_table_below_snapshot_max_is_still_authoritative():
    """表值 10.46 低于快照 MAX 10.83 也**不再重算**：快照不参与峰值口径。

    历史口径会判可疑并用快照/周期重算抬高（10.83 → 10.95），与回测不同源；
    2026-11-25 起表值即权威，恢复只 update_high，同样零 TQ、零审计事件。
    """
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[_bar("2026-11-12", "10.95")])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54), price="10.46", qty=5300)
    _snapshot(db, datetime(2026, 11, 11, 9, 32), "10.83", qty=5300)
    db.add(LivePositionPeak(
        live_session_id=1,
        portfolio_strategy_id=_SID,
        stock_code=_STOCK,
        highest_price=Decimal("10.46"),
        peak_bar_time=datetime(2026, 11, 12, 15, 0),
    ))
    db.commit()

    engine.recover(db)
    events = _events(db)
    db.close()

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("10.46")
    assert disp.quote_calls == []
    assert events == []


# ---------------- 缺口三：导入仓表值退化不再自愈（002745 场景） ----------------
def test_imported_position_keeps_table_peak_without_healing():
    """导入仓 + 表值 10.46 + 快照 10.83：不再自愈到 10.83，峰值保持表值。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(
        bars_by_code={"002745.SZ": [_bar("2026-11-12", "10.46")]},
        positions=[{
            "symbol": "002745.SZ", "volume": 5300, "available": 5300,
            "cost": "10.221", "market_value": "54171.3",
        }],
    )
    engine = _engine(factory, disp, port)
    db = factory()
    _snapshot(db, datetime(2026, 11, 10, 9, 32), "10.83",
              code="002745.SZ", qty=5300)
    db.add(LivePositionPeak(
        live_session_id=1,
        portfolio_strategy_id=_SID,
        stock_code="002745.SZ",
        highest_price=Decimal("10.46"),
        peak_bar_time=datetime(2026, 11, 12, 15, 0),
    ))
    db.commit()

    engine.recover(db)
    db.close()

    pos = port.strategies[0].positions["002745.SZ"]
    assert pos.highest_price == Decimal("10.46")
    assert disp.quote_calls == []


# ---------------- 缺口一：已在 live_trades 的票不重复补种 ----------------
def test_recover_import_pass_does_not_reseed_traded_codes():
    """成交重放补过的票不进第二轮 → 各自补种一次、零 TQ 取数。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(
        bars_by_code={
            _STOCK: [_bar("2026-11-10", "22.07")],
            "002745.SZ": [_bar("2026-11-12", "10.46")],
        },
        positions=[{
            "symbol": "002745.SZ", "volume": 5300, "available": 5300,
            "cost": "10.221", "market_value": "54171.3",
        }],
    )
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    engine.recover(db)
    db.close()

    # 补种只读表/基准价 → 两个组合都不取数
    assert disp.quote_calls == []
    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.002")
    assert port.strategies[0].positions["002745.SZ"].highest_price == Decimal("10.221")
