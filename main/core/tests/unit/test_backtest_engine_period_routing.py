"""W4.3：BacktestEngine 按周期路由（消混合周期前视）。

背景：旧引擎把所有周期的行合并到同一时间轴，且 on_bar 不传 period，
导致 1d 00:00 日线 bar 驱动 5m 策略、其订单在下一时间轴 bar（当日 09:30）
成交 = 混合周期前视；同 00:00 的 1d/1w/1mon 行还会互相覆盖 OHLC。

新引擎：时间轴为 (datetime, period) 步；价格索引按 (datetime, period) 键；
每步只驱动 period 相同的策略、只成交 order.period 相同的订单。
"""
from datetime import date, datetime
from decimal import Decimal

import polars as pl

from core.engine.backtest_engine import BacktestEngine
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.strategy_context import StrategyContext
from core.engine.risk_manager import StrategyRiskManager, PortfolioRiskManager
from tq_iquant_shared.constants import SignalType, TradeType


def test_daily_bar_does_not_drive_minute_strategy_same_day():
    """D 00:00 日线 bar 不驱动 5m 策略当日 09:30 成交（消混合周期前视）。

    5m 策略的信号仅存在于日线时刻 D 00:00（模拟按日线时间键控的信号缓存）。
    旧引擎：D 00:00 步 bar 含该票（日线行）→ on_bar 驱动 5m 策略 → BUY 在下一
    时间轴 bar（D 09:30）成交。新引擎：D 00:00 是 1d 步，5m 策略被跳过；
    D 09:30 是 5m 步但该时刻无信号 → 不产生任何成交。
    """
    stock = "000001.SZ"
    D = datetime(2026, 7, 30)
    df_1d = pl.DataFrame({
        "datetime": [D],
        "Open": [Decimal("10")], "High": [Decimal("10.2")],
        "Low": [Decimal("9.9")], "Close": [Decimal("10")], "Volume": [1000],
    })
    df_5m = pl.DataFrame({
        "datetime": [datetime(2026, 7, 30, 9, 30), datetime(2026, 7, 30, 9, 35)],
        "Open": [Decimal("10"), Decimal("10")],
        "High": [Decimal("10.1"), Decimal("10.1")],
        "Low": [Decimal("9.9"), Decimal("9.9")],
        "Close": [Decimal("10"), Decimal("10")],
        "Volume": [500, 500],
    })
    klines = {stock: {"1d": df_1d, "5m": df_5m}}

    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.5"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx_5m = StrategyContext(
        strategy_id=2, period="5m",
        capital_ratio=Decimal("1.0"), max_positions=5,
    )
    ctx_5m.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN, "trigger_value": 1},
    ]
    ctx_5m.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx_5m)

    # 5m 策略的信号只在日线时刻 D 00:00 命中（旧引擎会据此在当日 09:30 成交）
    cache = {(2, stock, D): [{"name": "open_sig", "value": 1}]}
    result = BacktestEngine().run(port, klines=klines, signal_cache=cache)

    assert result["trades"] == []


def test_single_period_daily_golden_unchanged():
    """单周期组合行为不变：3 根日线 → 2 笔成交（BUY→STOP_LOSS）+ 3 行快照。"""
    stock = "000001.SZ"
    df_1d = pl.DataFrame({
        "datetime": [
            datetime(2026, 7, 29),
            datetime(2026, 7, 30),
            datetime(2026, 7, 31),
        ],
        "Open": [Decimal("10"), Decimal("10.2"), Decimal("9.0")],
        "High": [Decimal("10.3"), Decimal("10.5"), Decimal("9.2")],
        "Low": [Decimal("9.9"), Decimal("8.9"), Decimal("8.8")],
        "Close": [Decimal("10.2"), Decimal("9.0"), Decimal("9.1")],
        "Volume": [1000, 1000, 1000],
    })
    klines = {stock: {"1d": df_1d}}

    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.5"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1d",
        capital_ratio=Decimal("0.6"), max_positions=5,
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN, "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)

    cache = {
        (1, stock, datetime(2026, 7, 29)): [{"name": "open_sig", "value": 1}],
        (1, stock, datetime(2026, 7, 30)): [{"name": "open_sig", "value": -1}],
        (1, stock, datetime(2026, 7, 31)): [{"name": "open_sig", "value": -1}],
    }
    result = BacktestEngine().run(port, klines=klines, signal_cache=cache)

    trades = result["trades"]
    assert [t.trade_type for t in trades] == [TradeType.BUY, TradeType.SELL]
    assert trades[0].price == Decimal("10.2")
    assert trades[1].price == Decimal("9.0")
    assert trades[1].signal_type == SignalType.STOP_LOSS
    # W4.4：每交易日至一行（3 个交易日 → 3 行）
    assert len(result["snapshots"]) == 3


def _mixed_period_portfolio_with_coarse_close():
    """1d 策略 + 持仓；同一周一额外有一根 close=100 的 1w bar（日线 close=10）。

    1w bar 的 close 是该周期末价格，对周一 00:00 而言是未来价。组合估值会被它
    瞬间抬高；若每步都 update_peak，随后 1d 收盘会制造假 max_drawdown。
    """
    stock = "000001.SZ"
    dates = [
        datetime(2026, 7, 27), datetime(2026, 7, 28),
        datetime(2026, 7, 29), datetime(2026, 7, 30),
        datetime(2026, 7, 31),
    ]
    df_1d = pl.DataFrame({
        "datetime": dates,
        "Open": [Decimal("10")] * len(dates),
        "High": [Decimal("10")] * len(dates),
        "Low": [Decimal("10")] * len(dates),
        "Close": [Decimal("10")] * len(dates),
        "Volume": [1000] * len(dates),
    })
    df_1w = pl.DataFrame({
        "datetime": [dates[0]],
        "Open": [Decimal("100")], "High": [Decimal("100")],
        "Low": [Decimal("100")], "Close": [Decimal("100")], "Volume": [1000],
    })
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.5")
    )
    port = Portfolio(
        portfolio_id=1, initial_capital=Decimal("10000"),
        risk_manager=pm,
    )
    port.account.cash = Decimal("0")
    ctx = StrategyContext(
        strategy_id=1, period="1d",
        capital_ratio=Decimal("1.0"), max_positions=5,
    )
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.9"), take_profit_ratio=Decimal("9"),
        trailing_stop_ratio=Decimal("0"),
    )
    pos = Position(stock)
    pos.quantity = 1000
    pos.avg_cost = Decimal("10")
    pos.highest_price = Decimal("10")
    ctx.positions[stock] = pos
    port.strategies.append(ctx)
    return port, {stock: {"1d": df_1d, "1w": df_1w}}


def test_daily_close_peak_guard_ignores_coarse_period_future_close():
    """开启日末 peak 保护后，1w 粗 bar 的未来 close 不得污染当日 peak/max_drawdown。"""
    port, klines = _mixed_period_portfolio_with_coarse_close()

    result = BacktestEngine().run(
        port, klines=klines, daily_close_peak_enabled=True,
    )

    assert port.risk_manager.peak_value == Decimal("10000")
    assert port.risk_manager.circuit_breaker_active is False
    assert [e for e in result["decisions"] if e["gate"] == "max_drawdown"] == []


def test_daily_close_peak_guard_default_off_still_ignores_coarse_future_close():
    """默认关闭（D8，2026-11-12）：粗周期 1w 在未来时点的 close 也不得抬峰。

    旧行为是「默认关闭 = 每步都抬峰」，混合周期下会被粗 bar 的周期末 close 前视
    污染（本用例旧断言为假回撤事件存在）。阶段二 D8 判定为真缺陷并修正：粗周期
    bar（1d/1w/1mon）只在「日终估值步」抬峰，分钟 bar 才每根抬 —— 该修正是默认
    生效的（`daily_close_peak_enabled` 仍是可选的更严格模式，收紧为全部只在
    日终步抬峰，默认关）。
    """
    port, klines = _mixed_period_portfolio_with_coarse_close()

    result = BacktestEngine().run(port, klines=klines)

    assert [e for e in result["decisions"] if e["gate"] == "max_drawdown"] == []
    assert port.risk_manager.peak_value == Decimal("10000")


def test_pure_daily_backtest_still_updates_peak_every_day():
    """纯日线回测：唯一时间戳步即日终步，峰值仍逐日更新（D8 不影响纯日线口径）。"""
    stock = "000001.SZ"
    dates = [datetime(2026, 7, 27), datetime(2026, 7, 28), datetime(2026, 7, 29)]
    df_1d = pl.DataFrame({
        "datetime": dates,
        "Open": [Decimal("10")] * 3,
        "High": [Decimal("10")] * 3,
        "Low": [Decimal("10")] * 3,
        "Close": [Decimal("10"), Decimal("11"), Decimal("12")],
        "Volume": [1000] * 3,
    })
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.9"), daily_loss_limit=Decimal("0.9"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("10000"),
                     risk_manager=pm)
    port.account.cash = Decimal("0")
    ctx = StrategyContext(
        strategy_id=1, period="1d", capital_ratio=Decimal("1.0"), max_positions=5)
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.9"), take_profit_ratio=Decimal("9"),
        trailing_stop_ratio=Decimal("0"))
    pos = Position(stock)
    pos.quantity = 1000
    pos.avg_cost = Decimal("10")
    pos.highest_price = Decimal("10")
    ctx.positions[stock] = pos
    port.strategies.append(ctx)

    BacktestEngine().run(port, klines={stock: {"1d": df_1d}})

    # 1000 股 × 12 = 12000，日线收盘逐日抬峰到最高 12000
    assert port.risk_manager.peak_value == Decimal("12000")


def test_period_order_covers_1h_and_legacy_60m_alias():
    """周期键名：全系统周期串为 "1h"，"60m" 仅作历史别名，二者同权重且落在 30m 与 1d 之间。"""
    from core.engine.backtest_engine import _PERIOD_ORDER

    assert _PERIOD_ORDER["1h"] == _PERIOD_ORDER["60m"]
    assert _PERIOD_ORDER["30m"] < _PERIOD_ORDER["1h"] < _PERIOD_ORDER["1d"]
    assert _PERIOD_ORDER.get("1h", 99) != 99


def test_timeline_orders_same_timestamp_fine_to_coarse():
    """同时间戳多周期处理顺序为细→粗（与实盘 periods_on_boundary 同序）。

    10:30 同时有 1m/5m/15m/30m/1h 五档；00:00 同时有 1d/1w/1mon 三档。
    """
    stock = "000001.SZ"
    day = datetime(2026, 7, 30)
    def _df(t):
        return pl.DataFrame({
            "datetime": [t],
            "Open": [Decimal("10")], "High": [Decimal("10")],
            "Low": [Decimal("10")], "Close": [Decimal("10")], "Volume": [1000],
        })
    klines = {stock: {
        "1mon": _df(day), "1w": _df(day), "1d": _df(day),
        "1h": _df(datetime(2026, 7, 30, 10, 30)),
        "30m": _df(datetime(2026, 7, 30, 10, 30)),
        "15m": _df(datetime(2026, 7, 30, 10, 30)),
        "5m": _df(datetime(2026, 7, 30, 10, 30)),
        "1m": _df(datetime(2026, 7, 30, 10, 30)),
    }}

    timeline = BacktestEngine()._build_timeline(klines)

    assert [p for t, p in timeline if t == datetime(2026, 7, 30, 10, 30)] == [
        "1m", "5m", "15m", "30m", "1h",
    ]
    assert [p for t, p in timeline if t == day] == ["1d", "1w", "1mon"]


# ---------------------------------------------------------------- 分钟级成交口径
# 口径 = 「下一根同周期 bar 的 open ×(1±滑点)」，与日线口径同构：
# - 信号 bar 只驱动信号，成交发生在该周期下一根 bar 的成交步（backtest_engine
#   run 第 1 步 `_bar_open_prices(price_index, t, period)` → SimulatedDispatcher
#   `fill_price = open ×(1±slippage)`）；
# - 跨午休 = 11:30 bar 的信号在 13:05 bar 成交（取 13:00 开盘价）；
# - 日内最后一根 = 15:00 bar 的信号顺延到次一交易日首根 bar（取次日开盘价）；
# - 接缝跨日 = 段末未成交的在途单由 `initial_pending_orders` 在次段首根成交。
def _minute_port(stock, cash="100000"):
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.9"), daily_loss_limit=Decimal("0.9"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal(cash),
                     risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="5m",
        capital_ratio=Decimal("1.0"), max_positions=5,
        single_open_ratio=Decimal("0.5"),
    )
    ctx.stock_pool = {stock}
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN,
         "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.9"), take_profit_ratio=Decimal("9"),
        trailing_stop_ratio=Decimal("0"))
    port.strategies.append(ctx)
    return port


def _df_5m(rows):
    return pl.DataFrame({
        "datetime": [r[0] for r in rows],
        "Open": [r[1] for r in rows],
        "High": [r[2] for r in rows],
        "Low": [r[3] for r in rows],
        "Close": [r[4] for r in rows],
        "Volume": [1000 for _ in rows],
    })


def _d(v):
    return Decimal(str(v))


def test_minute_signal_across_lunch_break_fills_next_bar_open():
    """跨午休：11:30 bar 信号 → 13:05 bar 成交，取 13:05 的 open（=13:00 价）。"""
    stock = "000001.SZ"
    rows = [
        (datetime(2026, 7, 30, 11, 30), _d("10"), _d("10"), _d("10"),
         _d("10")),
        (datetime(2026, 7, 30, 13, 5), _d("10.5"), _d("10.6"), _d("10.4"),
         _d("10.5")),
        (datetime(2026, 7, 30, 14, 0), _d("11"), _d("11"), _d("11"), _d("11")),
    ]
    port = _minute_port(stock)
    cache = {(1, stock, datetime(2026, 7, 30, 11, 30)): [
        {"name": "open_sig", "value": 1}]}

    result = BacktestEngine().run(
        port, klines={stock: {"5m": _df_5m(rows)}}, signal_cache=cache)

    trades = result["trades"]
    assert len(trades) == 1
    assert trades[0].trade_type == TradeType.BUY
    assert trades[0].trade_time == datetime(2026, 7, 30, 13, 5)
    assert trades[0].price == _d("10.5")


def test_minute_last_bar_of_day_fills_next_day_first_bar_open():
    """日内最后一根（15:00）信号不当日成交，顺延到次一交易日首根 bar 的 open。"""
    stock = "000001.SZ"
    rows = [
        (datetime(2026, 7, 30, 15, 0), _d("10"), _d("10"), _d("10"), _d("10")),
        (datetime(2026, 7, 31, 9, 35), _d("10.2"), _d("10.2"), _d("10.2"),
         _d("10.2")),
        (datetime(2026, 7, 31, 9, 40), _d("10.4"), _d("10.4"), _d("10.4"),
         _d("10.4")),
    ]
    port = _minute_port(stock)
    cache = {(1, stock, datetime(2026, 7, 30, 15, 0)): [
        {"name": "open_sig", "value": 1}]}

    result = BacktestEngine().run(
        port, klines={stock: {"5m": _df_5m(rows)}}, signal_cache=cache)

    trades = result["trades"]
    assert len(trades) == 1
    # 取次日首根（09:35）open=10.2，而不是信号 bar 的 close=10 或再下一根 10.4
    assert trades[0].trade_time == datetime(2026, 7, 31, 9, 35)
    assert trades[0].price == _d("10.2")


def test_minute_pending_order_crosses_incremental_seam_fills_next_segment_open():
    """接缝跨日：段末未成交的 5m 单在次段首根 bar 成交（取该 bar open）。"""
    from core.services import incremental_backtest as ib

    stock = "000001.SZ"
    day1 = [(datetime(2026, 7, 30, 15, 0), _d("10"), _d("10"), _d("10"),
             _d("10"))]
    day2 = [
        (datetime(2026, 7, 31, 9, 35), _d("10.2"), _d("10.2"), _d("10.2"),
         _d("10.2")),
        (datetime(2026, 7, 31, 9, 40), _d("10.4"), _d("10.4"), _d("10.4"),
         _d("10.4")),
    ]
    cache = {(1, stock, datetime(2026, 7, 30, 15, 0)): [
        {"name": "open_sig", "value": 1}]}

    port = _minute_port(stock)
    first = BacktestEngine().run(
        port, klines={stock: {"5m": _df_5m(day1)}}, signal_cache=cache)
    assert first["trades"] == []
    assert len(first["pending_orders"]) == 1

    payload = ib.state_payload(
        port, date(2026, 7, 30), pending_orders=first["pending_orders"],
        as_of_bar_time=datetime(2026, 7, 30, 15, 0), day_complete=True)
    resumed = _minute_port(stock)
    pending = ib.restore_portfolio(resumed, payload)
    tail = BacktestEngine().run(
        resumed, klines={stock: {"5m": _df_5m(day2)}}, signal_cache={},
        initial_pending_orders=pending)

    assert len(tail["trades"]) == 1
    assert tail["trades"][0].trade_time == datetime(2026, 7, 31, 9, 35)
    assert tail["trades"][0].price == _d("10.2")

