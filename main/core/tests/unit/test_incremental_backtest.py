"""接缝续跑增量回测：状态序列化 / 接缝锚定 / 续跑等价 / 补跑断档 / 双跑校验 单元测试。"""
import json
import logging
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
    Base, FormulaSignal, IncrementalBacktestCheckpoint, IncrementalBacktestDay,
    LiveAccountPositionSnapshot, LiveAccountSnapshot,
    LiveSession, LiveSessionPortfolio, LiveTrade, PortfolioStrategy, StockPool,
    StockPoolStock, Strategy,
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


def _add_snapshot(db, sess, code, highest, captured_at, cost="10"):
    snap = LiveAccountSnapshot(
        live_session_id=sess.id, engine="tdx", captured_at=captured_at)
    snap.positions.append(LiveAccountPositionSnapshot(
        stock_code=code, quantity=1000, cost_price=Decimal(cost),
        current_price=Decimal(cost), highest_price=highest))
    db.add(snap)
    db.commit()


def test_capture_seam_seeds_peak_from_prior_snapshot(db_session):
    """接缝前快照已记录的持仓期最高价必须带入起账状态（否则续跑侧移动止损
    基准低于实盘，实盘止损、回测不止损）。"""
    sess, ps, link = _seed_live_link(db_session)
    _add_snapshot(db_session, sess, "000001.SZ", Decimal("10.83"),
                  datetime(2026, 1, 5, 15, 5))
    port = _make_portfolio(cash="80000", strategy_id=1)
    pos = Position("000001.SZ")
    pos.buy(1000, Decimal("10"), trade_time=datetime(2026, 1, 5))
    port.strategies[0].positions["000001.SZ"] = pos

    ib.capture_seam(db_session, sess.id, _FakeEngine([port]))
    db_session.refresh(link)
    seeded = json.loads(link.seam_positions_json)
    assert Decimal(seeded[0]["highest_price"]) == Decimal("10.83")


def test_capture_seam_ignores_stale_snapshot_peak(db_session):
    """快照早于本笔建仓（同代码先清仓再重开）→ 上一轮高点不得复活。"""
    sess, ps, link = _seed_live_link(db_session)
    _add_snapshot(db_session, sess, "000001.SZ", Decimal("12"),
                  datetime(2026, 1, 2, 15, 5))
    port = _make_portfolio(cash="80000", strategy_id=1)
    pos = Position("000001.SZ")
    pos.buy(1000, Decimal("10"), trade_time=datetime(2026, 1, 5))
    port.strategies[0].positions["000001.SZ"] = pos

    ib.capture_seam(db_session, sess.id, _FakeEngine([port]))
    db_session.refresh(link)
    seeded = json.loads(link.seam_positions_json)
    assert Decimal(seeded[0]["highest_price"]) == Decimal("10")


def _add_empty_snapshot(db, sess, captured_at, market_value=0):
    """桥只回现金、持仓行为空的快照（会话停止后的典型形态）。"""
    snap = LiveAccountSnapshot(
        live_session_id=sess.id, engine="tdx", captured_at=captured_at,
        total_asset=Decimal(str(market_value)),
        available_cash=Decimal(str(market_value)),
        market_value=Decimal(str(market_value)))
    db.add(snap)
    db.commit()
    return snap


def test_capture_seam_skips_empty_snapshot_for_prior_peak(db_session):
    """最近一份快照持仓为空时，跳过它、取上一份含持仓快照的峰值种下。

    回归：2026-11-11 12:08 那份快照 market_value=0 且无持仓行，若按「最近一份」
    取 snapshot，snap_peaks 为空 → 峰值退化成成本价，峰值种植白做。
    """
    sess, ps, link = _seed_live_link(db_session)
    _add_snapshot(db_session, sess, "000001.SZ", Decimal("10.83"),
                  datetime(2026, 1, 5, 11, 29))
    _add_empty_snapshot(db_session, sess, datetime(2026, 1, 5, 12, 8))
    port = _make_portfolio(cash="80000", strategy_id=1)
    pos = Position("000001.SZ")
    pos.buy(1000, Decimal("10"), trade_time=datetime(2026, 1, 5))
    port.strategies[0].positions["000001.SZ"] = pos

    ib.capture_seam(db_session, sess.id, _FakeEngine([port]))
    db_session.refresh(link)
    seeded = json.loads(link.seam_positions_json)
    assert Decimal(seeded[0]["highest_price"]) == Decimal("10.83")


def test_capture_seam_falls_back_to_cost_with_warning(db_session, caplog):
    """全历史都没有含持仓的快照 → 峰值退回成本价，且必须留 WARNING 留痕。"""
    sess, ps, link = _seed_live_link(db_session)
    _add_empty_snapshot(db_session, sess, datetime(2026, 1, 5, 12, 8))
    port = _make_portfolio(cash="80000", strategy_id=1)
    pos = Position("000001.SZ")
    pos.buy(1000, Decimal("10"), trade_time=datetime(2026, 1, 5))
    port.strategies[0].positions["000001.SZ"] = pos

    with caplog.at_level(logging.WARNING):
        ib.capture_seam(db_session, sess.id, _FakeEngine([port]))
    db_session.refresh(link)
    seeded = json.loads(link.seam_positions_json)
    assert Decimal(seeded[0]["highest_price"]) == Decimal("10")
    assert any("无任何含有效持仓的账户快照" in r.getMessage()
               for r in caplog.records)


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


# ---------------------------------------------------------------- 7. 分钟级续跑
# 日内 bar 序列（bar 时间戳 = bar 终点，与 TDX/快照合成口径一致）：
# 含跨午休（11:30 → 13:05）与收盘前最后一根（15:00）。
_MINUTE_TIMES = ((9, 35), (10, 30), (11, 30), (13, 5), (14, 0), (15, 0))


def _minute_klines(days, price=Decimal("10"), period="5m"):
    rows = []
    for d in days:
        for h, m in _MINUTE_TIMES:
            rows.append({
                "datetime": datetime(d.year, d.month, d.day, h, m),
                "Open": price, "High": price, "Low": price, "Close": price,
                "Volume": 100000,
            })
    return {"000001.SZ": {period: pl.DataFrame(rows)}}


def _minute_sig_cache(strategy_id, times, code="000001.SZ", name="open_sig"):
    return {(strategy_id, code, t): [{"name": name, "value": 1}]
            for t in times}


def _seed_minute_link(db, period="5m"):
    """组合 + 策略周期 = 分钟级 + 一条 OPEN 公式信号（供 signal_cache 命中）。"""
    sess, ps, link = _seed_live_link(db)
    strat = db.query(Strategy).filter_by(portfolio_id=ps.id).first()
    strat.period = period
    db.add(FormulaSignal(formula_id=1, signal_name="open_sig",
                         signal_type="OPEN", trigger_value=1))
    link.seam_date = date(2026, 1, 5)
    link.seam_cash = Decimal("100000")
    link.seam_positions_json = "[]"
    link.seam_trade_cursor = 0
    db.commit()
    return sess, ps, link, strat


def _seed_minute_checkpoint(db, sess, ps, as_of, as_of_bar=None,
                            day_complete=False):
    """落一个「已跑到 as_of 某根 bar」的 checkpoint（省略则整日已跑完）。"""
    dm = ib._cfg().get("dividend_mode", "dual")
    fp = ib.params_fingerprint(ps, dm, db)
    port = _make_portfolio(cash="100000", period="5m")
    state = ib.state_payload(port, as_of, pending_orders=[],
                             as_of_bar_time=as_of_bar,
                             day_complete=day_complete)
    state["schema_fp"] = ib.schema_fingerprint(dm)
    state["params_fp_parts"] = fp["parts"]
    db.add(IncrementalBacktestCheckpoint(
        session_id=sess.id, portfolio_strategy_id=ps.id, seam_date=as_of,
        as_of_date=as_of, state_json=json.dumps(state),
        params_hash=fp["hash"], schema_version=ib.SCHEMA_VERSION))
    db.commit()
    return port


def _ckpt_state(db, sess, ps):
    ckpt = (
        db.query(IncrementalBacktestCheckpoint)
        .filter_by(session_id=sess.id, portfolio_strategy_id=ps.id)
        .first()
    )
    return json.loads(ckpt.state_json)


def test_minute_strategy_incremental_supported_with_intraday_cursor(
        db_session, monkeypatch):
    """分钟级策略不再判 unsupported；checkpoint 落「交易日 + 日内游标」。"""
    sess, ps, link, strat = _seed_minute_link(db_session)
    t = datetime(2026, 1, 5, 10, 30)
    monkeypatch.setattr(ib, "build_klines",
                        lambda *a, **k: _minute_klines([date(2026, 1, 5)]))
    monkeypatch.setattr(ib, "build_signal_cache",
                        lambda *a, **k: _minute_sig_cache(strat.id, [t]))

    result = ib.run_incremental(
        db_session, sess.id, ps.id, today=date(2026, 1, 5))
    assert result["status"] == "ok"
    assert result["days"] == 1
    state = _ckpt_state(db_session, sess, ps)
    assert state["as_of_date"] == "2026-01-05"
    assert state["as_of_bar_time"] == "2026-01-05T15:00:00"
    assert state["day_complete"] is True
    # summary 也把 supported 放开（前端不再显示「本期不支持」）
    assert ib.summary(db_session, sess.id, ps.id)["supported"] is True


def test_minute_resume_matches_full_run_and_no_double_count():
    """分段续跑 = 全量重跑：净值逐字相等，接缝日成交不重复计入。"""
    days = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]
    code = "000001.SZ"
    kl = _minute_klines(days)
    t1 = datetime(2026, 1, 5, 10, 30)   # 第 1 日开仓
    t3 = datetime(2026, 1, 7, 10, 30)   # 第 3 日清仓（成交落在下一根 bar）
    sig = {}
    sig.update(_minute_sig_cache(1, [t1], name="open_sig"))
    sig.update(_minute_sig_cache(1, [t3], name="close_sig"))

    def _port():
        p = _make_portfolio(period="5m")
        p.strategies[0].formula_signals.append({
            "signal_name": "close_sig", "signal_type": SignalType.CLOSE,
            "trigger_value": 1,
        })
        return p

    full = _port()
    full_result = BacktestEngine().run(
        full, klines={code: {"5m": kl[code]["5m"]}}, signal_cache=dict(sig))
    full_day3 = [s for s in full_result["snapshots"]
                 if s["snap_date"] == days[2]][0]["total_value"]
    full_day3_trades = [x for x in full_result["trades"]
                        if x.trade_time.date() == days[2]]
    assert len(full_day3_trades) == 1

    part = _port()
    first = BacktestEngine().run(
        part,
        klines={code: {"5m": kl[code]["5m"].filter(
            pl.col("datetime") < datetime(2026, 1, 7))}},
        signal_cache=dict(sig))
    payload = ib.state_payload(
        part, days[1], pending_orders=first["pending_orders"],
        as_of_bar_time=datetime(2026, 1, 6, 15, 0), day_complete=True)

    resumed = _port()
    pending = ib.restore_portfolio(resumed, payload)
    tail = BacktestEngine().run(
        resumed,
        klines={code: {"5m": kl[code]["5m"].filter(
            pl.col("datetime") >= datetime(2026, 1, 7))}},
        signal_cache={(1, code, t3): [{"name": "close_sig", "value": 1}]},
        initial_pending_orders=pending)
    day3 = [s for s in tail["snapshots"] if s["snap_date"] == days[2]][0]
    day3_trades = [x for x in tail["trades"]
                   if x.trade_time.date() == days[2]]

    assert Decimal(str(day3["total_value"])) == Decimal(str(full_day3)), (
        "full=%s tail=%s | full_trades=%s tail_trades=%s"
        % (full_day3, day3["total_value"],
           [(t.trade_type.value, t.quantity, t.price, t.trade_time)
            for t in full_result["trades"]],
           [(t.trade_type.value, t.quantity, t.price, t.trade_time)
            for t in tail["trades"]]))
    # 接缝日只成交 1 笔（不是 2 笔）：没有把上一段的成交/信号重放一次
    assert len(day3_trades) == 1
    assert len(day3_trades) == len(full_day3_trades)
    assert day3_trades[0].price == full_day3_trades[0].price


def test_minute_intraday_cursor_does_not_replay_processed_bars(
        db_session, monkeypatch):
    """同一交易日按游标续跑：游标之前的 bar 不重放，当日成交明细合并成整日口径。"""
    sess, ps, link, strat = _seed_minute_link(db_session)
    d = date(2026, 1, 5)
    # 上午那轮只跑到 11:30（day_complete=False）
    _seed_minute_checkpoint(db_session, sess, ps, as_of=d,
                            as_of_bar=datetime(2026, 1, 5, 11, 30),
                            day_complete=False)
    # 当日已有上午的成交记录（BUY），续跑后应与下午的成交合并
    db_session.add(IncrementalBacktestDay(
        session_id=sess.id, portfolio_strategy_id=ps.id, trade_date=d,
        inc_total_value=Decimal("100000"), inc_cash=Decimal("98995"),
        inc_market_value=Decimal("1000"), live_total_value=Decimal("100000"),
        inc_trades_count=1, live_trades_count=1,
        inc_dirs_json=json.dumps(["BUY"]), direction_match=True, status="ok"))
    db_session.commit()

    before = datetime(2026, 1, 5, 10, 30)   # 游标之前（重放才会产生成交）
    after = datetime(2026, 1, 5, 13, 5)     # 游标之后
    monkeypatch.setattr(ib, "build_klines",
                        lambda *a, **k: _minute_klines([d]))
    monkeypatch.setattr(
        ib, "build_signal_cache",
        lambda *a, **k: _minute_sig_cache(strat.id, [before, after]))

    result = ib.run_incremental(db_session, sess.id, ps.id, today=d)
    assert result["status"] == "ok"
    state = _ckpt_state(db_session, sess, ps)
    # 游标推进到当日收盘，并标记当日已收满（否则 15:10 调度会跳过）
    assert state["as_of_bar_time"] == "2026-01-05T15:00:00"
    assert state["day_complete"] is True

    row = (
        db_session.query(IncrementalBacktestDay)
        .filter_by(session_id=sess.id, portfolio_strategy_id=ps.id,
                   trade_date=d).first()
    )
    # 只有游标之后那一根触发成交（上午那根没被重放），且与既有行合并
    assert json.loads(row.inc_dirs_json) == ["BUY", "000001.SZ:BUY"]
    assert row.inc_trades_count == 2


def test_minute_incremental_detects_gap_and_rebuilds_from_seam(
        db_session, monkeypatch):
    """分钟级断档：中间交易日不得静默跳过（超补跑上限 → 从接缝日重建）。"""
    sess, ps, link, strat = _seed_minute_link(db_session)
    as_of = date(2026, 1, 5)
    _seed_minute_checkpoint(db_session, sess, ps, as_of=as_of,
                            as_of_bar=datetime(2026, 1, 5, 15, 0),
                            day_complete=True)
    days = [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7),
            date(2026, 1, 8), date(2026, 1, 9), date(2026, 1, 12),
            date(2026, 1, 13)]
    monkeypatch.setattr(ib, "build_klines", lambda *a, **k: _minute_klines(days))
    monkeypatch.setattr(ib, "build_signal_cache",
                        lambda *a, **k: _minute_sig_cache(strat.id, []))

    result = ib.run_incremental(
        db_session, sess.id, ps.id, today=date(2026, 1, 13))
    assert result["status"] == "ok"
    assert result["gap_days"] > 0
    rows = (
        db_session.query(IncrementalBacktestDay)
        .filter_by(session_id=sess.id, portfolio_strategy_id=ps.id)
        .all()
    )
    # 中间交易日一个都不能少（gap 只是标记，不是跳过）
    assert sorted(r.trade_date for r in rows) == days


def test_minute_checkpoint_invalidated_by_params_change(db_session, monkeypatch):
    """分钟级 checkpoint 同样受参数指纹闸约束（参数变更 → 作废重建）。"""
    sess, ps, link, strat = _seed_minute_link(db_session)
    _seed_minute_checkpoint(db_session, sess, ps, as_of=date(2026, 1, 5),
                            as_of_bar=datetime(2026, 1, 5, 15, 0),
                            day_complete=True)
    strat.stop_loss_ratio = Decimal("0.09")
    db_session.commit()
    monkeypatch.setattr(
        ib, "build_klines",
        lambda *a, **k: _minute_klines([date(2026, 1, 5), date(2026, 1, 6)]))
    monkeypatch.setattr(ib, "build_signal_cache",
                        lambda *a, **k: _minute_sig_cache(strat.id, []))

    result = ib.run_incremental(
        db_session, sess.id, ps.id, today=date(2026, 1, 6))
    assert result["status"] == "ok"
    assert result["checkpoint_invalidated"]
    assert result["checkpoint_invalidated"]["from_as_of"] == "2026-01-05"
    rows = (
        db_session.query(IncrementalBacktestDay)
        .filter_by(session_id=sess.id, portfolio_strategy_id=ps.id)
        .all()
    )
    # 作废后从接缝日（1/5）重建，两日都在
    assert sorted(r.trade_date for r in rows) == [date(2026, 1, 5),
                                                  date(2026, 1, 6)]


def test_checkpoint_done_requires_day_complete_for_intraday():
    """调度跳过判据：盘中续跑过的当日 checkpoint 不算完成（15:10 还要补跑）。"""
    from types import SimpleNamespace

    today = date(2026, 1, 5)

    def _ckpt(as_of, state):
        return SimpleNamespace(as_of_date=as_of, state_json=json.dumps(state))

    assert ib._checkpoint_done(
        _ckpt(today, {"day_complete": False}), today) is False
    assert ib._checkpoint_done(
        _ckpt(today, {"day_complete": True}), today) is True
    # 旧 checkpoint 无该字段 → 按已完成处理（与改动前行为一致）
    assert ib._checkpoint_done(_ckpt(today, {}), today) is True
    assert ib._checkpoint_done(
        _ckpt(date(2026, 1, 4), {}), today) is False
    assert ib._checkpoint_done(None, today) is False


def test_minute_validation_supported_and_small_sample_exempt(
        db_session, monkeypatch):
    """分钟级双跑校验不再判 unsupported；小样本笔数/方向一致率判 None。"""
    sess, ps, link, strat = _seed_minute_link(db_session)
    days = [date(2026, 1, 5), date(2026, 1, 6)]
    t = datetime(2026, 1, 5, 10, 30)
    monkeypatch.setattr(ib, "build_klines", lambda *a, **k: _minute_klines(days))
    monkeypatch.setattr(ib, "build_signal_cache",
                        lambda *a, **k: _minute_sig_cache(strat.id, [t]))

    inc = ib.run_incremental(
        db_session, sess.id, ps.id, today=date(2026, 1, 6))
    assert inc["status"] == "ok"
    res = ib.run_validation(db_session, sess.id, ps.id,
                            from_date=date(2026, 1, 5),
                            to_date=date(2026, 1, 6))
    assert res["status"] == "pass"
    assert res["trade_count_match_ratio"] is None
    assert res["direction_match_ratio"] is None
    assert res["small_sample"] is True


# ---------------------------------------------------------------- 8. 参数指纹
def test_params_hash_covers_strategy_formula_pool_and_add_delete(db_session):
    """指纹必须识别：策略数值参数 / 公式 / 股票池成分 / 策略增删。"""
    sess, ps, link = _seed_live_link(db_session)
    strat = db_session.query(Strategy).filter_by(portfolio_id=ps.id).first()
    base = ib.params_hash(ps, "dual", db_session)

    # 数值参数（止损）
    strat.stop_loss_ratio = Decimal("0.09")
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) != base
    strat.stop_loss_ratio = Decimal("0.05")
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) == base

    # 加仓阈值 / 资金占比
    strat.add_position_threshold = Decimal("0.03")
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) != base
    strat.add_position_threshold = Decimal("0.05")
    strat.capital_ratio = Decimal("0.60")
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) != base
    strat.capital_ratio = Decimal("1.0")
    db_session.commit()

    # 公式变更
    strat.formula_id = 2
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) != base
    strat.formula_id = 1
    db_session.commit()

    # 股票池成分变化
    extra = StockPoolStock(pool_id=ps.stock_pool_id, stock_code="600000.SH")
    db_session.add(extra)
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) != base
    db_session.delete(extra)
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) == base

    # 策略增删
    added = Strategy(
        portfolio_id=ps.id, name="s2", formula_id=1, period="1d",
        role="independent", capital_ratio=Decimal("0.5"),
        max_positions=3, single_open_ratio=Decimal("0.5"),
    )
    db_session.add(added)
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) != base
    db_session.delete(added)
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) == base

    # 重命名不影响口径（不参与指纹）
    strat.name = "renamed"
    db_session.commit()
    assert ib.params_hash(ps, "dual", db_session) == base


def test_invalidation_reasons_locate_changed_section(db_session):
    """作废 checkpoint 时必须能说明是哪一类变化（不是只丢一句「参数变了」）。"""
    sess, ps, link = _seed_live_link(db_session)
    old = ib.params_fingerprint(ps, "dual", db_session)
    strat = db_session.query(Strategy).filter_by(portfolio_id=ps.id).first()
    strat.take_profit_ratio = Decimal("0.20")
    db_session.commit()
    new = ib.params_fingerprint(ps, "dual", db_session)
    schema_fp = ib.schema_fingerprint("dual")
    reasons = ib._invalidation_reasons(
        {"params_fp_parts": old["parts"]}, schema_fp, ib.SCHEMA_VERSION,
        schema_fp, new["parts"])
    assert any("策略参数" in r for r in reasons)
    assert not any("股票池成分" in r for r in reasons)

    # 口径/复权模式变化单独成一条
    reasons2 = ib._invalidation_reasons(
        {"params_fp_parts": old["parts"]}, schema_fp, ib.SCHEMA_VERSION,
        ib.schema_fingerprint("front"), old["parts"])
    assert any("复权" in r for r in reasons2)

    # 旧 checkpoint 没有分项指纹 → 明确标注无法定位
    reasons3 = ib._invalidation_reasons(
        None, schema_fp, ib.SCHEMA_VERSION, schema_fp, new["parts"])
    assert any("无分项指纹" in r for r in reasons3)


def test_engine_schema_v2_invalidates_with_readable_reason():
    """修复 6：引擎口径升 v2；旧 checkpoint 作废时必须点名「引擎版本 v1 → v2」。"""
    assert ib._ENGINE_SCHEMA == "backtest_engine_v2"
    want_parts = ib.schema_parts("dual")
    old_parts = dict(want_parts, engine="backtest_engine_v1")
    reasons = ib._invalidation_reasons(
        {"schema_fp": "old-fp", "schema_parts": old_parts, "params_fp_parts": {}},
        "old-fp", ib.SCHEMA_VERSION, ib.schema_fingerprint("dual"), {},
        want_parts)
    assert any("引擎版本" in r and "backtest_engine_v1" in r
               and "backtest_engine_v2" in r for r in reasons)


def test_schema_parts_include_engine_version():
    """checkpoint 落盘的口径分项必须含引擎版本（供下次作废定位）。"""
    parts = ib.schema_parts("dual")
    assert parts["engine"] == ib._ENGINE_SCHEMA
    assert parts["dividend_mode"] == "dual"


# ---------------------------------------------------------------- 9. 快路径闸
def test_fast_path_only_when_gap_is_exactly_today():
    """连续多日未跑 + 当天有交接日线 → 禁止快路径（否则中间交易日被静默跳过）。"""
    handoff = {"000001.SZ": [{"stime": "20260108150000"}]}
    today = date(2026, 1, 8)          # 周四
    assert ib.fast_path_allowed(date(2026, 1, 7), today, ["1d"], handoff, 5)
    # 1/5 之后一直没跑：缺口 = 1/6、1/7、1/8，不能只用今天一天的交接日线
    assert not ib.fast_path_allowed(date(2026, 1, 5), today, ["1d"], handoff, 5)
    # 没有交接日线 / 含非日线周期 → 不走快路径
    assert not ib.fast_path_allowed(date(2026, 1, 7), today, ["1d"], None, 5)
    assert not ib.fast_path_allowed(
        date(2026, 1, 7), today, ["1d", "1w"], handoff, 5)


def test_multi_day_gap_with_handoff_does_not_skip_middle_days(
        db_session, monkeypatch):
    """构造用例：checkpoint 停在 1/5，1/8 有交接日线且中间两天没跑。

    旧实现会直接走快路径只跑 1/8（1/6、1/7 静默丢失）；修复后必须退回常规
    取数路径，把 1/6、1/7、1/8 三天都跑出来。
    """
    sess, ps, link = _seed_live_link(db_session)
    dm = ib._cfg().get("dividend_mode", "dual")
    fp = ib.params_fingerprint(ps, dm, db_session)
    as_of = date(2026, 1, 5)
    link.seam_date = as_of
    link.seam_cash = Decimal("100000")
    link.seam_positions_json = "[]"
    link.seam_trade_cursor = 0
    port = _make_portfolio(cash="100000")
    state = ib.state_payload(port, as_of, pending_orders=[])
    state["schema_fp"] = ib.schema_fingerprint(dm)
    state["params_fp_parts"] = fp["parts"]
    db_session.add(IncrementalBacktestCheckpoint(
        session_id=sess.id, portfolio_strategy_id=ps.id, seam_date=as_of,
        as_of_date=as_of, state_json=json.dumps(state),
        params_hash=fp["hash"], schema_version=ib.SCHEMA_VERSION))
    db_session.commit()

    days = [date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 8)]
    monkeypatch.setattr(ib, "take_daily_bars", lambda *a, **k: {
        "000001.SZ": [{"stime": "20260108150000", "open": 10, "high": 10,
                       "low": 10, "close": 10, "volume": 100}]})
    monkeypatch.setattr(ib, "build_klines", lambda *a, **k: _klines(days))
    monkeypatch.setattr(ib, "build_signal_cache", lambda *a, **k: {})

    result = ib.run_incremental(
        db_session, sess.id, ps.id, today=date(2026, 1, 8))
    assert result["status"] == "ok"
    assert result["fast_path"] is False
    # 三天都进快照（= 中间交易日没有被静默跳过）
    assert result["days"] == 3
    rows = (
        db_session.query(IncrementalBacktestDay)
        .filter_by(session_id=sess.id, portfolio_strategy_id=ps.id)
        .all()
    )
    assert sorted(r.trade_date for r in rows) == days


# ---------------------------------------------------------------- 10. 接缝成交游标
def _add_live_trade(db, sess, ps, strat, code, trade_type, price, qty,
                    when):
    db.add(LiveTrade(
        live_session_id=sess.id, portfolio_strategy_id=ps.id,
        strategy_id=strat.id, stock_code=code, trade_type=trade_type,
        price=Decimal(str(price)), quantity=qty,
        amount=Decimal(str(price)) * qty,
        commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=when))
    db.commit()


def test_capture_seam_records_trade_cursor_and_live_rows_no_double_count(
        db_session):
    """盘中重启 + 首次锚定 + 当日已有成交：起账状态已含该笔成交，重放不得再算一次。"""
    sess, ps, link = _seed_live_link(db_session)
    strat = db_session.query(Strategy).filter_by(portfolio_id=ps.id).first()
    today = date(2026, 1, 8)
    _add_live_trade(db_session, sess, ps, strat, "000001.SZ", "BUY",
                    "10", 1000, datetime(2026, 1, 8, 10, 0))

    # recover 之后的引擎状态：现金已被买入扣减、持仓 1000
    port = _make_portfolio(cash="90000", strategy_id=strat.id)
    pos = Position("000001.SZ")
    pos.buy(1000, Decimal("10"), trade_time=datetime(2026, 1, 8, 10, 0))
    port.strategies[0].positions["000001.SZ"] = pos

    ib.capture_seam(db_session, sess.id, _FakeEngine([port]), today=today)
    db_session.refresh(link)
    assert link.seam_date == today
    assert link.seam_trade_cursor is not None

    # 捕获之后再成交一笔（同一交易日）
    _add_live_trade(db_session, sess, ps, strat, "000001.SZ", "SELL",
                    "11", 1000, datetime(2026, 1, 8, 14, 0))

    rows = ib._live_rows(
        db_session, link, [today], {"000001.SZ": {today: Decimal("11")}})
    # 只重放捕获后的卖出；若把捕获前的买入再算一次，持仓会残留 1000 股
    assert rows[today]["trades_count"] == 1
    assert Decimal(str(rows[today]["total_value"])) == Decimal("101000")


# ---------------------------------------------------------------- 11. 方向判据语义
def test_evaluate_alignment_direction_uses_own_dirs_and_none_when_missing():
    tol = {"max_net_dev_pct": 0.5, "mean_net_dev_pct": 0.2,
           "trade_count_match_ratio": 0.9, "direction_match_ratio": 0.9,
           "max_drawdown_dev_pct": 1.0}
    # 5 个交易日 × 两侧各 1 笔 = 10 笔 ≥ 小样本阈值，笔数/方向两项参与判定
    days = [date(2026, 1, 5 + i) for i in range(5)]
    inc = {d: {"trade_date": d, "inc_total_value": Decimal("100000"),
               "inc_trades_count": 1, "dirs": ["000001.SZ:BUY"]} for d in days}
    full_bad = {d: {"trade_date": d, "total_value": Decimal("100000"),
                    "trades_count": 1, "dirs": ["000001.SZ:BUY"]} for d in days}
    # 最后一天方向相反 → 方向一致率 4/5=0.8 < 0.9 → fail，首个背离日 = 该日
    full_bad[days[-1]] = {**full_bad[days[-1]], "dirs": ["000001.SZ:SELL"]}
    res = ib.evaluate_alignment(inc, full_bad, tol)
    assert res["direction_match_ratio"] == 0.8
    assert res["status"] == "fail"
    assert res["first_divergence_date"] == days[-1]

    # 任一侧无方向明细 → 该日不参与判定（None），而不是恒真通过
    inc_no = {d: ({**inc[d], "dirs": None} if d == days[0] else inc[d]) for d in days}
    full_ok = {d: {**full_bad[d], "dirs": ["000001.SZ:BUY"]} for d in days}
    res2 = ib.evaluate_alignment(inc_no, full_ok, tol)
    assert res2["direction_match_ratio"] == 1.0
    assert res2["status"] == "pass"

    # 全部日期都无方向明细 → 返回 None（不参与），而不是 0.0
    inc_all_none = {d: {**inc[d], "dirs": None} for d in days}
    res3 = ib.evaluate_alignment(inc_all_none, full_ok, tol)
    assert res3["direction_match_ratio"] is None
    assert res3["status"] == "pass"


def test_evaluate_alignment_small_sample_exempts_count_and_direction():
    """修复 9：成交笔数（两侧之和）< SMALL_SAMPLE_MIN_TRADES 时，笔数/方向一致率
    判 None 不参与总体结论；净值/回撤偏差仍照常全量校验。"""
    assert ib.SMALL_SAMPLE_MIN_TRADES == 5
    tol = {"max_net_dev_pct": 0.5, "mean_net_dev_pct": 0.2,
           "trade_count_match_ratio": 0.9, "direction_match_ratio": 0.9,
           "max_drawdown_dev_pct": 1.0}
    d = date(2026, 1, 5)
    # 2 + 1 = 3 笔 < 5：笔数/方向都不同（旧口径必 fail）→ 小样本豁免后 pass
    inc = {d: {"trade_date": d, "inc_total_value": Decimal("100000"),
               "inc_trades_count": 2, "dirs": ["000001.SZ:BUY", "600000.SH:BUY"]}}
    full = {d: {"trade_date": d, "total_value": Decimal("100000"),
                "trades_count": 1, "dirs": ["000001.SZ:SELL"]}}
    res = ib.evaluate_alignment(inc, full, tol)
    assert res["small_sample"] is True
    assert res["trades_seen"] == 3
    assert res["trade_count_match_ratio"] is None
    assert res["direction_match_ratio"] is None
    assert res["first_divergence_date"] is None
    assert res["status"] == "pass"

    # 小样本不放过净值口径：净值偏差超容差仍判 fail
    inc_bad = {d: {**inc[d], "inc_total_value": Decimal("90000")}}
    res2 = ib.evaluate_alignment(inc_bad, full, tol)
    assert res2["status"] == "fail"
    assert res2["first_divergence_date"] == d

    # 达到阈值（5 笔）后恢复原判定：笔数不一致 → 一致率下降并 fail
    full5 = {d: {"trade_date": d, "total_value": Decimal("100000"),
                 "trades_count": 4, "dirs": ["000001.SZ:SELL"] * 4}}
    inc1 = {d: {"trade_date": d, "inc_total_value": Decimal("100000"),
                "inc_trades_count": 1, "dirs": ["000001.SZ:BUY"]}}
    res3 = ib.evaluate_alignment(inc1, full5, tol)
    assert res3["small_sample"] is False
    assert res3["trade_count_match_ratio"] == 0.0
    assert res3["status"] == "fail"


# ---------------------------------------------------------------- 12. 校验标记可清除
def test_apply_validation_marks_clears_stale_validation_fail(db_session):
    sess, ps, link = _seed_live_link(db_session)
    rows = []
    for i, (tags, status) in enumerate([
            (["validation_fail"], "warn"),
            (["validation_fail", "net_dev"], "warn"),
            (["gap"], "gap")]):
        r = IncrementalBacktestDay(
            session_id=sess.id, portfolio_strategy_id=ps.id,
            trade_date=date(2026, 1, 5 + i), status=status,
            tags_json=json.dumps(tags))
        db_session.add(r)
        rows.append(r)
    db_session.flush()

    # 本轮通过 → 历史 validation_fail 必须被清掉、状态回落
    ib._apply_validation_marks(rows, "pass", None)
    assert json.loads(rows[0].tags_json) == []
    assert rows[0].status == "ok"
    assert json.loads(rows[1].tags_json) == ["net_dev"]
    assert rows[1].status == "warn"
    assert json.loads(rows[2].tags_json) == ["gap"]
    assert rows[2].status == "gap"

    # 本轮失败 → 只在首个背离日之后重标
    ib._apply_validation_marks(rows, "fail", date(2026, 1, 6))
    assert json.loads(rows[0].tags_json) == []
    assert "validation_fail" in json.loads(rows[1].tags_json)
    assert "validation_fail" in json.loads(rows[2].tags_json)
    assert rows[2].status == "gap"
