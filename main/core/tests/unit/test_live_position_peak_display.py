"""D6 持仓峰值 / 峰值日期 / 来源展示单元测试。"""
from datetime import datetime
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.engine.event import TradeEvent
from core.engine.position import Position
from core.models import Base, LivePositionPeak, LiveSession, LiveTrade
from core.services import live_service
from tq_iquant_shared.constants import TradeType


def _db():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


class _FakeCtx:
    def __init__(self, strategy_id, positions):
        self.strategy_id = strategy_id
        self.positions = positions


class _FakePortfolio:
    def __init__(self, portfolio_id, strategies):
        self.portfolio_id = portfolio_id
        self.strategies = strategies


class _FakeEngine:
    dispatcher = None

    def __init__(self, portfolios, peak_buffer=None):
        self.portfolios = portfolios
        self._peak_buffer = peak_buffer or {}


def _position(code="000001.SZ", price="10", qty=1000):
    pos = Position(code)
    pos.apply_trade(TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=code,
        trade_type=TradeType.BUY, price=Decimal(price), quantity=qty,
        amount=Decimal(price) * qty, commission=Decimal("0"),
        stamp_duty=Decimal("0"), trade_time=datetime(2026, 11, 17, 9, 31),
    ))
    return pos


def test_stopped_positions_show_peak_from_table():
    """停止态：峰值/峰值日期/来源全部来自 live_position_peaks 表。"""
    db = _db()
    db.add(LiveSession(id=2, name="s2", mode="simulation", status="stopped",
                       engine="tdx"))
    db.add(LiveTrade(
        live_session_id=2, portfolio_strategy_id=1, strategy_id=1,
        stock_code="000001.SZ", trade_type="BUY", price=Decimal("10"),
        quantity=1000, amount=Decimal("10000"), commission=Decimal("0"),
        stamp_duty=Decimal("0"), trade_time=datetime(2026, 11, 17, 9, 31)))
    db.add(LivePositionPeak(
        live_session_id=2, portfolio_strategy_id=1, stock_code="000001.SZ",
        highest_price=Decimal("12.5"),
        peak_bar_time=datetime(2026, 11, 17, 10, 5),
        peak_source="period"))
    db.commit()

    rows = live_service.session_positions(db, 2, None)
    row = next(r for r in rows if r["stock_code"] == "000001.SZ")
    assert row["highest_price"] == 12.5
    assert row["peak_source"] == "period"
    assert row["peak_source_label"] == "策略周期补种"
    assert row["peak_bar_time"].startswith("2026-11-17T10:05")


def test_running_positions_prefer_newest_memory_peak():
    """运行态：内存缓冲比表值新 → 展示缓冲峰值与来源（逐 bar 抬高）。"""
    db = _db()
    db.add(LiveSession(id=2, name="s2", mode="simulation", status="running",
                       engine="tdx"))
    db.add(LivePositionPeak(
        live_session_id=2, portfolio_strategy_id=1, stock_code="000001.SZ",
        highest_price=Decimal("12.5"), peak_source="table"))
    db.commit()

    pos = _position()
    pos.update_high(Decimal("13.2"))
    engine = _FakeEngine(
        [_FakePortfolio(1, [_FakeCtx(1, {"000001.SZ": pos})])],
        peak_buffer={(1, "000001.SZ"): (
            Decimal("13.5"), datetime(2026, 11, 18, 10, 0), "bar")},
    )

    rows = live_service.session_positions(db, 2, engine)
    row = next(r for r in rows if r["stock_code"] == "000001.SZ")
    assert row["highest_price"] == 13.5
    assert row["peak_source"] == "bar"
    assert row["peak_source_label"] == "逐bar抬高"
    assert row["peak_bar_time"].startswith("2026-11-18T10:00")


def test_running_positions_without_table_infer_source():
    """无峰值表/缓冲：峰值=成本 → 成本兜底；高于成本 → 逐 bar 抬高。"""
    db = _db()
    db.add(LiveSession(id=2, name="s2", mode="simulation", status="running",
                       engine="tdx"))
    db.commit()

    at_cost = _position()
    engine = _FakeEngine([_FakePortfolio(1, [_FakeCtx(1, {"000001.SZ": at_cost})])])
    row = live_service.session_positions(db, 2, engine)[0]
    assert row["peak_source"] == "cost"
    assert row["peak_source_label"] == "成本兜底"

    above_cost = _position()
    above_cost.update_high(Decimal("11.0"))
    engine2 = _FakeEngine([_FakePortfolio(1, [_FakeCtx(1, {"000001.SZ": above_cost})])])
    row2 = live_service.session_positions(db, 2, engine2)[0]
    assert row2["peak_source"] == "bar"
    assert row2["highest_price"] == 11.0
