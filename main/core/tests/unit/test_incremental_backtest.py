"""接缝续跑增量回测：状态序列化 / 接缝锚定 / 续跑等价 / 补跑断档 / 双跑校验 单元测试。"""
import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import polars as pl
import pytest

from core.engine.account import Account
from core.engine.backtest_engine import BacktestEngine
from core.engine.event import OrderEvent
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import (
    Base, FormulaSignal, LiveSession, LiveSessionPortfolio,
    PortfolioStrategy, StockPool, StockPoolStock, Strategy,
)
from tq_iquant_shared.constants import SignalType, TradeType

from core.services import incremental_backtest as ib


# ---------------------------------------------------------------- 公共构造
def _make_portfolio(cash="100000", strategy_id=1, period="1d") -> Portfolio:
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.50"), daily_loss_limit=Decimal("0.50"))
    port = Portfolio(1, Decimal(cash), pm)
    ctx = StrategyContext(
        strategy_id=strategy_id,
        period=period,
        capital_ratio=Decimal("1.0"),
        max_positions=5,
        single_open_ratio=Decimal("1.0"),
    )
    ctx.stock_pool = {"000001.SZ"}
    ctx.formula_signals = [{
        "signal_name": "open_sig",
        "signal_type": SignalType.OPEN,
        "trigger_value": 1,
    }]
    port.strategies.append(ctx)
    return port


def _klines(days, price=Decimal("10")):
    rows = []
    for i, d in enumerate(days):
        rows.append({
            "datetime": datetime(d.year, d.month, d.day),
            "Open": price,
            "High": price,
            "Low": price,
            "Close": price,
            "Volume": 100000,
        })
    return {"000001.SZ": {"1d": pl.DataFrame(rows)}}


# ---------------------------------------------------------------- 1. 状态往返
def test_state_roundtrip_covers_positions_pending_and_risk():
    port = _make_portfolio()
    ctx = port.strategies[0]
    pos = Position("000001.SZ")
    pos.buy(1000, Decimal("10"), trade_time=datetime(2026, 1, 5))
    pos.highest_price = Decimal("11.5")
    pos.add_count = 1
    ctx.positions["000001.SZ"] = pos
    port.account.cash = Decimal("90000")
    port.risk_manager.peak_value = Decimal("105000")
    port.risk_manager.prev_close_value = Decimal("102000")
    port.risk_manager._last_bar_date = date(2026, 1, 6)
    port.risk_manager._last_bar_total_value = Decimal("102000")
    port.risk_manager._last_daily_date = date(2026, 1, 6)
    port._price_snapshot["000001.SZ"] = Decimal("11.5")
    pending = [OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="000001.SZ",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=500, price=Decimal("11.5"),
        bar_time=datetime(2026, 1, 6), signal_name="open_sig",
        signal_bar_date=date(2026, 1, 6), period="1d",
    )]

    payload = ib.state_payload(port, date(2026, 1, 6), pending_orders=pending)
    # json 可序列化（落 Text 列）
    json.dumps(payload, ensure_ascii=False)

    fresh = _make_portfolio()
    restored_pending = ib.restore_portfolio(fresh, payload)
    rpos = fresh.strategies[0].positions["000001.SZ"]
    assert rpos.quantity == 1000
    assert rpos.avg_cost == Decimal("10")
    assert rpos.highest_price == Decimal("11.5")
    assert rpos.buy_time == datetime(2026, 1, 5)
    assert rpos.add_count == 1
    assert fresh.account.cash == Decimal("90000")
    assert fresh.risk_manager.peak_value == Decimal("105000")
    assert fresh.risk_manager._last_daily_date == date(2026, 1, 6)
    assert fresh._price_snapshot["000001.SZ"] == Decimal("11.5")
    assert len(restored_pending) == 1
    o = restored_pending[0]
    assert o.stock_code == "000001.SZ"
    assert o.quantity == 500
    assert o.signal_bar_date == date(2026, 1, 6)
    assert o.period == "1d"


# ---------------------------------------------------------------- 2. 接缝捕获
class _FakeEngine:
    def __init__(self, portfolios):
        self.portfolios = portfolios


def _seed_live_link(db, seam=False):
    pool = StockPool(name="p", code="P1")
    db.add(pool)
    db.flush()
    db.add(StockPoolStock(pool_id=pool.id, stock_code="000001.SZ"))
    ps = PortfolioStrategy(name="组合", stock_pool_id=pool.id, initial_capital=100000)
    db.add(ps)
    db.flush()
    db.add(Strategy(
        portfolio_id=ps.id, name="s", formula_id=1, period="1d",
        role="independent", capital_ratio=Decimal("1.0"),
        max_positions=5, single_open_ratio=Decimal("1.0"),
    ))
    sess = LiveSession(name="sess", mode="live", engine="tdx", status="running")
    db.add(sess)
    db.flush()
    link = LiveSessionPortfolio(
        session_id=sess.id, portfolio_strategy_id=ps.id)
    db.add(link)
    db.commit()
    return sess, ps, link


def test_capture_seam_writes_baseline_once(db_session):
    sess, ps, link = _seed_live_link(db_session)
    port = _make_portfolio(cash="80000", strategy_id=1)
    Pos = Position("000001.SZ")
    Pos.buy(1000, Decimal("10"), trade_time=datetime(2026, 1, 5))
    port.strategies[0].positions["000001.SZ"] = Pos

    ib.capture_seam(db_session, sess.id, _FakeEngine([port]))
    db_session.refresh(link)
    assert link.seam_date == date.today()
    assert Decimal(str(link.seam_cash)) == Decimal("80000")
    seeded = json.loads(link.seam_positions_json)
    assert seeded[0]["stock_code"] == "000001.SZ"
    assert seeded[0]["highest_price"] == "10"

    # 二次捕获不覆盖（接缝日一旦写定不再漂移）
    port.account.cash = Decimal("1")
    ib.capture_seam(db_session, sess.id, _FakeEngine([port]))
    db_session.refresh(link)
    assert Decimal(str(link.seam_cash)) == Decimal("80000")


# ---------------------------------------------------------------- 3. 续跑等价
def test_resume_matches_full_run_with_pending_order():
    days = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
    kl = _klines(days)
    code = "000001.SZ"
    # 第 2 日信号（signal_bar_date=第 2 日）→ 第 3 日开盘成交
    sig = {(1, code, datetime(2026, 1, 6)): [
        {"name": "open_sig", "value": 1}]}

    full = _make_portfolio()
    full_result = BacktestEngine().run(
        full, klines={code: {"1d": kl[code]["1d"]}},
        signal_cache=dict(sig))
    full_day3 = [s for s in full_result["snapshots"]
                 if s["snap_date"] == days[2]][0]["total_value"]

    # 前两日分段跑
    part = _make_portfolio()
    first = BacktestEngine().run(
        part,
        klines={code: {"1d": kl[code]["1d"].filter(
            pl.col("datetime") < datetime(2026, 1, 7))}},
        signal_cache=dict(sig))
    payload = ib.state_payload(
        part, days[1], pending_orders=first["pending_orders"])

    resumed = _make_portfolio()
    pending = ib.restore_portfolio(resumed, payload)
    tail = BacktestEngine().run(
        resumed,
        klines={code: {"1d": kl[code]["1d"].filter(
            pl.col("datetime") >= datetime(2026, 1, 7))}},
        signal_cache={}, initial_pending_orders=pending)
    day3 = [s for s in tail["snapshots"] if s["snap_date"] == days[2]][0]

    assert Decimal(str(day3["total_value"])) == Decimal(str(full_day3))
    assert Decimal(str(day3["market_value"])) > 0


# ---------------------------------------------------------------- 4. 补跑 / 断档
def test_plan_resume_marks_gap_beyond_cap():
    as_of = date(2026, 1, 1)
    trading = [date(2026, 1, 2), date(2026, 1, 5), date(2026, 1, 6),
               date(2026, 1, 7), date(2026, 1, 8), date(2026, 1, 9)]
    plan = ib.plan_resume(as_of, date(2026, 1, 9), max_catchup_days=5,
                          trading_days=trading)
    assert plan["gap_dates"] == trading
    assert plan["rebuild_from_seam"] is True

    plan2 = ib.plan_resume(as_of, date(2026, 1, 6), max_catchup_days=5,
                           trading_days=trading[:3])
    assert plan2["gap_dates"] == []
    assert plan2["dates"] == trading[:3]


# ---------------------------------------------------------------- 5. 口径版本闸
def test_checkpoint_invalidated_by_params_hash_and_schema():
    ok = ib.checkpoint_valid(
        params_hash="h1", schema_fp="f1", schema_version=1,
        expected_params_hash="h1", expected_schema_fp="f1")
    assert ok is True
    assert ib.checkpoint_valid(
        params_hash="h2", schema_fp="f1", schema_version=1,
        expected_params_hash="h1", expected_schema_fp="f1") is False
    assert ib.checkpoint_valid(
        params_hash="h1", schema_fp="f2", schema_version=1,
        expected_params_hash="h1", expected_schema_fp="f1") is False
    assert ib.checkpoint_valid(
        params_hash="h1", schema_fp="f1", schema_version=0,
        expected_params_hash="h1", expected_schema_fp="f1") is False


# ---------------------------------------------------------------- 6. 双跑校验判据
def _row(d, net, trades=2, direction=True):
    return {"trade_date": d, "inc_total_value": Decimal(str(net)),
            "inc_trades_count": trades, "direction_match": direction}


def test_evaluate_alignment_pass_and_fail():
    tol = {"max_net_dev_pct": 0.5, "mean_net_dev_pct": 0.2,
           "trade_count_match_ratio": 0.9, "direction_match_ratio": 0.9,
           "max_drawdown_dev_pct": 1.0}
    rows = [_row(date(2026, 1, 5), 100000), _row(date(2026, 1, 6), 101000)]
    inc = {r["trade_date"]: r for r in rows}
    full = {r["trade_date"]: dict(r) for r in rows}
    pass_res = ib.evaluate_alignment(inc, full, tol)
    assert pass_res["status"] == "pass"
    assert pass_res["first_divergence_date"] is None

    bad = {r["trade_date"]: dict(r) for r in rows}
    bad[date(2026, 1, 6)]["inc_total_value"] = Decimal("110000")
    fail_res = ib.evaluate_alignment(inc, bad, tol)
    assert fail_res["status"] == "fail"
    assert fail_res["first_divergence_date"] == date(2026, 1, 6)


# ---------------------------------------------------------------- 7. 不支持场景
def test_minute_strategy_is_unsupported(db_session):
    sess, ps, link = _seed_live_link(db_session)
    # 把策略周期改成 5m
    db_session.query(Strategy).filter_by(portfolio_id=ps.id).update(
        {"period": "5m"})
    db_session.commit()
    result = ib.run_incremental(db_session, sess.id, ps.id)
    assert result["status"] == "unsupported"
    assert "分钟级" in result["reason"]
