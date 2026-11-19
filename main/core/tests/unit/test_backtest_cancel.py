# -*- coding: utf-8 -*-
"""K1 回测协作式取消（2026-11-18）。

覆盖：取消令牌登记/置位/摘除；引擎单组合与多组合在 bar 粒度中止并返回进度；
service 层落 canceled 且不持久化半截结果。
"""
from datetime import datetime, date
from decimal import Decimal

import polars as pl
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.backtest_engine import BacktestEngine
from core.engine.portfolio import Portfolio
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import (
    Base, StockPool, StockPoolStock, Formula, FormulaSignal,
    PortfolioStrategy, Strategy, BacktestRecord, BacktestTrade,
)
from core.services import backtest_cancel, backtest_service as svc
from tq_iquant_shared.constants import SignalType

CODE = "000001.SZ"


def _klines():
    df = pl.DataFrame({
        "datetime": [datetime(2026, 7, 29), datetime(2026, 7, 30),
                     datetime(2026, 7, 31)],
        "Open": [Decimal("10"), Decimal("10"), Decimal("10")],
        "High": [Decimal("10.3"), Decimal("10.3"), Decimal("10.3")],
        "Low": [Decimal("9.9"), Decimal("9.9"), Decimal("9.9")],
        "Close": [Decimal("10.2"), Decimal("10.1"), Decimal("10.1")],
        "Volume": [100000, 100000, 100000],
    })
    return {CODE: {"1d": df}}


def _portfolio(pid=1, sid=1):
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.5"), daily_loss_limit=Decimal("0.5"))
    port = Portfolio(portfolio_id=pid, initial_capital=Decimal("10000"),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=sid, period="1d", capital_ratio=Decimal("1"),
        max_positions=5, single_open_ratio=Decimal("1"))
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.5"), take_profit_ratio=Decimal("0.9"),
        trailing_stop_ratio=Decimal("0"))
    port.strategies.append(ctx)
    return port


def _cache(sids):
    out = {}
    for sid in sids:
        out[(sid, CODE, datetime(2026, 7, 29))] = [{"name": "open_sig", "value": 1}]
        out[(sid, CODE, datetime(2026, 7, 30))] = [{"name": "open_sig", "value": -1}]
        out[(sid, CODE, datetime(2026, 7, 31))] = [{"name": "open_sig", "value": -1}]
    return out


# ---------------------------------------------------------------- 令牌
def test_registry_register_cancel_release():
    event = backtest_cancel.register([101, 102])
    assert backtest_cancel.is_registered(101)
    assert not event.is_set()

    assert backtest_cancel.cancel(102) is True
    assert event.is_set()

    backtest_cancel.release([101, 102])
    assert not backtest_cancel.is_registered(101)
    # 已摘除后再取消 → False（不误判为已登记）
    assert backtest_cancel.cancel(101) is False


# ---------------------------------------------------------------- 引擎
def test_run_cancel_before_first_bar():
    port = _portfolio()
    result = BacktestEngine().run(
        port, klines=_klines(), signal_cache=_cache([1]),
        cancel_check=lambda: True)

    assert result["canceled"] is True
    assert result["progress"] == {"done_bars": 0, "total_bars": 3}
    assert result["trades"] == []
    assert result["snapshots"] == []


def test_run_cancel_after_one_bar_reports_progress():
    port = _portfolio()
    checks = {"n": 0}

    def _check():
        checks["n"] += 1
        return checks["n"] > 1  # 第 1 根 bar 放行，第 2 根前取消

    result = BacktestEngine().run(
        port, klines=_klines(), signal_cache=_cache([1]), cancel_check=_check)

    assert result["canceled"] is True
    assert result["progress"] == {"done_bars": 1, "total_bars": 3}
    assert result["trades"] == []


def test_run_multi_cancel_returns_canceled_marker():
    result = BacktestEngine().run_multi(
        [_portfolio(1, 1), _portfolio(2, 2)],
        klines=_klines(), signal_cache=_cache([1, 2]),
        cancel_check=lambda: True)

    assert result["_canceled"] is True
    assert result["_progress"] == {"done_bars": 0, "total_bars": 3}
    assert result["_decisions"] == []


# ---------------------------------------------------------------- service 落 canceled
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
    db.add(StockPoolStock(pool_id=pool.id, stock_code=CODE, stock_name="平安银行"))
    formula = Formula(name="open_formula", content="REF(CLOSE,1)")
    db.add(formula)
    db.flush()
    db.add(FormulaSignal(
        formula_id=formula.id, signal_name="open_sig",
        signal_type="OPEN", trigger_value=1))
    db.flush()
    ps = PortfolioStrategy(
        name="test_portfolio", stock_pool_id=pool.id,
        initial_capital=Decimal("100000"),
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    db.add(ps)
    db.flush()
    db.add(Strategy(
        portfolio_id=ps.id, name="s1", formula_id=formula.id,
        period="1d", role="master", capital_ratio=Decimal("0.6"),
        max_positions=5, stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"), trailing_stop_ratio=Decimal("0")))
    db.commit()
    return ps


class _Req:
    name = "cancel_test"
    start_date = date(2026, 7, 29)
    end_date = date(2026, 7, 31)
    dividend_mode = "front"
    fee_reserve_enabled = False
    adaptive_slippage_enabled = None
    slippage_impact_coef = None
    bond_t0_enabled = False
    bond_lot_enabled = False
    daily_close_peak_enabled = False


def test_service_marks_canceled_and_skips_persistence(db_session, monkeypatch):
    ps = _seed(db_session)
    monkeypatch.setattr(
        svc, "build_klines",
        lambda ps, start, end, db=None, **kw: _klines())
    monkeypatch.setattr(
        svc, "build_signal_cache",
        lambda ps, klines, db=None, **kw: {})
    monkeypatch.setattr(
        svc, "build_benchmark_data",
        lambda ps, start, end, db=None, **kw: {})
    monkeypatch.setattr(svc, "safe_compute_crisis_alpha", lambda *a, **kw: None)
    monkeypatch.setattr(
        svc.BacktestEngine, "run",
        lambda self, portfolio, **kw: {
            "trades": [], "snapshots": [], "evaluations": {},
            "strategy_snapshots": {}, "strategy_evaluations": {},
            "canceled": True, "progress": {"done_bars": 1, "total_bars": 3},
        })

    result = svc.run_backtest(db_session, ps, _Req())

    assert result["canceled"] is True
    assert result["trades_count"] == 0
    rec = db_session.get(BacktestRecord, result["record_id"])
    assert rec.status == "canceled"
    assert "已跑 1/3 根 bar" in rec.error_message
    assert rec.completed_at is not None
    # 不落半截结果
    assert db_session.query(BacktestTrade).filter_by(
        backtest_record_id=rec.id).count() == 0
    # 令牌已摘除
    assert not backtest_cancel.is_registered(rec.id)
