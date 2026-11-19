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


# ---------------- 阶段二 A：峰值表优先于取数 ----------------
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
    ))
    db.commit()

    engine.recover(db)
    db.close()

    pos = port.strategies[0].positions[_STOCK]
    assert pos.highest_price == Decimal("22.63")
    assert disp.quote_calls == []


# ---------------- 阶段二 B：按策略自身周期补种 ----------------
def test_recover_seeds_from_strategy_period_bars():
    """策略周期 5m → 用 5m bar close 重算（不再默认走日线）。"""
    factory, _ = _db_factory()
    port = _portfolio()
    port.strategies[0].period = "5m"
    disp = _StubDispatcher(bars=[
        {"stime": "20261110103000", "open": "22.10", "high": "22.20",
         "low": "22.00", "close": "22.10", "volume": 1000},
        {"stime": "20261111103000", "open": "22.40", "high": "22.60",
         "low": "22.30", "close": "22.50", "volume": 1000},
        {"stime": "20261112103000", "open": "22.20", "high": "22.40",
         "low": "22.10", "close": "22.30", "volume": 1000},
    ])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    engine.recover(db)
    db.close()

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.50")
    assert [c[1] for c in disp.quote_calls] == ["5m"]


def test_recover_falls_back_to_daily_when_strategy_period_empty():
    """5m 取不到（返回空）→ 降级日线 close，日志标注实际使用周期。"""
    factory, _ = _db_factory()
    port = _portfolio()
    port.strategies[0].period = "5m"
    disp = _StubDispatcher(bars=[])
    engine = _engine(factory, disp, port)
    db = factory()
    _buy_trade(db, datetime(2026, 11, 10, 9, 31, 54))
    _snapshot(db, datetime(2026, 11, 11, 9, 32), "22.63")
    engine.recover(db)
    db.close()

    # 5m 空 → 日线也空 → 快照 MAX 兜底
    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.63")
    assert [c[1] for c in disp.quote_calls] == ["5m", "1d"]


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
    assert rows[_STOCK] == Decimal("22.00")


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
    """普通持仓 + 继承仓共存：两行都保留，各自峰值正确（002745 自愈到 10.83）。"""
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

    assert rows[_STOCK] == Decimal("22.07")
    assert rows["002745.SZ"] == Decimal("10.83")  # 第一轮没删、第二轮种出峰值
    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("22.07")
    assert port.strategies[0].positions["002745.SZ"].highest_price == Decimal("10.83")


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
    """无 live_trades 的继承仓：补种移到导入之后 → 快照 MAX 10.83 胜出，不退成本。"""
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
    # 成本 10.221 / 日线 close 10.46 / 快照 MAX 10.83 → 取最大者，而非成本兜底
    assert pos.highest_price == Decimal("10.83")
    assert [c[0] for c in disp.quote_calls] == ["002745.SZ"]


# ---------------- 缺口二：表值只作下限，先与快照 MAX 比对 ----------------
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


def test_peak_table_below_snapshot_max_triggers_recompute():
    """表值 10.46 < 快照 MAX 10.83 → 判可疑，重算并取最大者 + recompute 审计。"""
    factory, _ = _db_factory()
    port = _portfolio()
    disp = _StubDispatcher(bars=[_bar("2026-11-12", "10.46")])
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

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("10.83")
    assert [c[1] for c in disp.quote_calls] == ["1d"]  # 走了重算取数
    assert len(events) == 1
    assert events[0].action == "recompute"
    assert "10.46" in events[0].message and "10.83" in events[0].message


def test_peak_recompute_event_carries_recomputed_value():
    """recompute 审计行必须含「代码 / 表值 / 重算值 / 快照 MAX」四项。

    表值 10.46 < 快照 MAX 10.83 < 周期重算 10.95：final=10.95，审计行要在
    message 里给出重算值，param/actual 分别落表值与快照 MAX，库内可直接核对。
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

    assert port.strategies[0].positions[_STOCK].highest_price == Decimal("10.95")
    assert len(events) == 1
    ev = events[0]
    assert ev.action == "recompute"
    assert ev.stock_code == _STOCK
    assert ev.param_value == 10.46            # 表值
    assert ev.actual_value == 10.83           # 快照 MAX
    assert "10.95" in ev.message              # 重算值


# ---------------- 缺口三：继承仓表值退化自愈（002745 场景） ----------------
def test_imported_position_heals_stale_table_peak():
    """导入仓（buy_time=今日）+ 表值 10.46（已退化）+ 11-10 快照 10.83：

    锚点回退到 11-10（快照首次见到该仓）→ 表值 < 快照 MAX → 重算取 10.83，
    退化值不因落表而永久固化。
    """
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
    events = _events(db)
    db.close()

    pos = port.strategies[0].positions["002745.SZ"]
    assert pos.highest_price == Decimal("10.83")
    assert any(e.action == "recompute" and e.stock_code == "002745.SZ"
               for e in events)


# ---------------- 缺口一：已在 live_trades 的票不重复补种 ----------------
def test_recover_import_pass_does_not_reseed_traded_codes():
    """成交重放补过的票不进第二轮 → 每只票只取数一次。"""
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

    codes = [c[0] for c in disp.quote_calls]
    assert codes.count(_STOCK) == 1
    assert codes.count("002745.SZ") == 1
