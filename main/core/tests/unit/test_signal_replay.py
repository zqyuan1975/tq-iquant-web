"""N3 信号回放校验 单测（monkeypatch 数据层，内存 SQLite）。"""

from datetime import date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.models import (
    Base, StockPool, StockPoolStock, Formula, FormulaSignal,
    PortfolioStrategy, Strategy, LiveSession, LiveOrder,
)
from core.services import backtest_service as bt
from core.services import signal_replay as sr


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False},
        poolclass=StaticPool)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    yield db
    db.close()


def _seed(db):
    pool = StockPool(code="TEST", name="test_pool")
    db.add(pool)
    db.flush()
    db.add(StockPoolStock(
        pool_id=pool.id, stock_code="000001.SZ", stock_name="平安银行"))
    formula = Formula(name="f", content="REF(CLOSE,1)")
    db.add(formula)
    db.flush()
    db.add(FormulaSignal(
        formula_id=formula.id, signal_name="open_sig",
        signal_type="OPEN", trigger_value=1))
    db.add(FormulaSignal(
        formula_id=formula.id, signal_name="close_sig",
        signal_type="CLOSE", trigger_value=1))
    db.flush()
    ps = PortfolioStrategy(
        name="replay_pf", stock_pool_id=pool.id,
        initial_capital=Decimal("100000"),
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    db.add(ps)
    db.flush()
    strat = Strategy(
        portfolio_id=ps.id, name="s1", formula_id=formula.id,
        period="1d", role="master", capital_ratio=Decimal("0.6"),
        max_positions=5, stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"), trailing_stop_ratio=Decimal("0"))
    db.add(strat)
    db.commit()
    return ps, strat


def _cache(strat_id):
    return {
        (strat_id, "000001.SZ", datetime(2026, 7, 30, 10, 0)): [
            {"name": "open_sig", "value": 1}],
        (strat_id, "000002.SZ", datetime(2026, 7, 31, 14, 0)): [
            {"name": "close_sig", "value": 1}],
        # 未触发（value=0）不应出现在回放结果
        (strat_id, "000003.SZ", datetime(2026, 7, 31, 14, 0)): [
            {"name": "open_sig", "value": 0}],
    }


def test_replay_signals_flattens_and_maps_direction(db_session, monkeypatch):
    ps, strat = _seed(db_session)
    monkeypatch.setattr(bt, "build_klines",
                        lambda *a, **kw: {"000001.SZ": {"1d": None}})
    monkeypatch.setattr(bt, "build_signal_cache",
                        lambda p, k, **kw: _cache(strat.id))

    payload, err = sr.replay_signals(
        db_session, ps.id, date(2026, 7, 30), date(2026, 7, 31))

    assert err is None
    assert len(payload["rows"]) == 2
    assert payload["rows"][0]["stock_code"] == "000001.SZ"
    assert payload["rows"][0]["directions"] == ["BUY"]
    assert payload["rows"][1]["directions"] == ["SELL"]
    assert payload["summary"]["signal_bars"] == 2
    assert payload["summary"]["signals"] == 2
    assert payload["summary"]["by_date"] == [
        {"date": "2026-07-30", "count": 1},
        {"date": "2026-07-31", "count": 1},
    ]
    assert payload["comparison"] is None


def test_replay_signals_compares_with_live_orders(db_session, monkeypatch):
    ps, strat = _seed(db_session)
    session = LiveSession(name="实盘", mode="paper", status="running")
    db_session.add(session)
    db_session.flush()
    for code, ttype in (("000001.SZ", "BUY"), ("000002.SZ", "SELL")):
        db_session.add(LiveOrder(
            live_session_id=session.id, portfolio_strategy_id=ps.id,
            strategy_id=strat.id, stock_code=code, trade_type=ttype,
            order_type="limit", quantity=100, filled_quantity=100,
            status="filled",
            bar_time=datetime(2026, 7, 30 if ttype == "BUY" else 31, 10)))
    # 实盘多出的委托（回放无对应信号）
    db_session.add(LiveOrder(
        live_session_id=session.id, portfolio_strategy_id=ps.id,
        strategy_id=strat.id, stock_code="000003.SZ", trade_type="BUY",
        order_type="limit", quantity=100, filled_quantity=0,
        status="canceled", bar_time=datetime(2026, 7, 31, 10)))
    db_session.commit()

    monkeypatch.setattr(bt, "build_klines", lambda *a, **kw: {"x": {"1d": None}})
    monkeypatch.setattr(bt, "build_signal_cache",
                        lambda p, k, **kw: _cache(strat.id))

    payload, err = sr.replay_signals(
        db_session, ps.id, date(2026, 7, 30), date(2026, 7, 31),
        session_id=session.id)

    assert err is None
    comp = payload["comparison"]
    assert comp["matched"] == 2
    assert comp["only_replay_count"] == 0
    assert comp["only_live_count"] == 1
    assert comp["only_live"][0]["stock_code"] == "000003.SZ"


def test_replay_signals_validates_input(db_session, monkeypatch):
    ps, _ = _seed(db_session)
    payload, err = sr.replay_signals(
        db_session, 999999, date(2026, 7, 30), date(2026, 7, 31))
    assert payload is None and "组合策略不存在" in err

    payload, err = sr.replay_signals(
        db_session, ps.id, date(2026, 8, 1), date(2026, 7, 1))
    assert payload is None and "开始日期" in err

    # 无行情：返回空结果 + warning，不报错
    monkeypatch.setattr(bt, "build_klines", lambda *a, **kw: {})
    payload, err = sr.replay_signals(
        db_session, ps.id, date(2026, 7, 30), date(2026, 7, 31))
    assert err is None
    assert payload["rows"] == []
    assert payload["warnings"]
