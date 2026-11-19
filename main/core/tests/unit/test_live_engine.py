"""LiveEngine 单测（0009 切片4）— Mock dispatcher（httpx MockTransport）+ 内存 SQLite。

验证实盘端到端链路：bar → 信号 → 真实下单（桥受理）→ 落 live_orders/live_trades，
以及 Core 重启后从 live_trades 恢复虚拟持仓/虚拟现金。引擎核心（Portfolio/ExecutionEngine）
复用回测逻辑，仅注入 HttpBridgeDispatcher + LiveT1Checker。
"""
import asyncio
import json
import logging
import threading
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.engine.account import Account
from core.engine.bar_poller import parse_bar_time
from core.engine.event import BarEvent, OrderEvent, TradeEvent
from core.engine.execution_engine import LiveT1Checker
from core.engine.http_bridge_dispatcher import (
    BridgeUnavailableError,
    HttpBridgeDispatcher,
)
from core.engine.live_engine import LiveEngine, now_shanghai
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import (
    Base, LiveOrder, LiveTrade, LiveSession, LiveSessionPortfolio,
    LiveDecisionEvent, LiveBarOrderMark,
)
from tq_iquant_shared.constants import SignalType, TradeType


# ---------------- 共用辅助 ----------------
def _db_factory():
    """内存 SQLite Session 工厂：返回 () -> Session，引擎每根 bar 取一个独立 Session。

    StaticPool + check_same_thread=False：审计 #3 后 _loop/_deals_loop 的 tick 在
    单 worker 线程执行，_poll_deals 在 worker 线程访问 DB。默认 :memory: 每连接一个
    独立库 → 跨线程新连接看不到建表；StaticPool 复用同一连接、check_same_thread=False
    允许跨线程使用，让 worker 线程的 Session 见到同一份表数据。单线程测试行为不变。
    """
    from sqlalchemy.pool import StaticPool
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    # expire_on_commit=False：commit 后对象属性仍可访问（测试断言方便，不触发 refresh）
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)

    def factory():
        return SessionLocal()

    return factory, engine


class _Recorder:
    """MockTransport：记录请求，/order 返回受理成功，/ping 返回 ok。"""

    def __init__(self, respond=None, fail_paths=None):
        self.requests = []
        self._respond = respond
        self._fail_paths = fail_paths or set()

    def handler(self, request):
        self.requests.append(request)
        path = request.url.path
        if path in self._fail_paths:
            raise httpx.ConnectError("connection refused")
        if self._respond is not None:
            return self._respond(request)
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": []})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})


def _make_dispatcher(rec, fail_paths=None):
    client = httpx.Client(transport=httpx.MockTransport(rec.handler))
    return HttpBridgeDispatcher(base_url="http://127.0.0.1:8790", client=client), rec


def _portfolio_single(period="1m", strategy_id=1):
    """单组合单策略，formula_signal 配 OPEN（trigger_value=1）。"""
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=strategy_id, period=period,
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("0.1"),
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN, "trigger_value": 1},
    ]
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port, ctx


def _portfolio_two(periods=("1m", "5m")):
    """单组合两策略（periods 指定各自周期），各配 OPEN 信号。"""
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctxs = []
    for sid, period in enumerate(periods, start=1):
        ctx = StrategyContext(
            strategy_id=sid, period=period,
            capital_ratio=Decimal("0.6"), max_positions=5,
            single_open_ratio=Decimal("0.1"),
        )
        ctx.formula_signals = [
            {"signal_name": "open_sig", "signal_type": SignalType.OPEN, "trigger_value": 1},
        ]
        ctx.strategy_risk = StrategyRiskManager(
            stop_loss_ratio=Decimal("0.05"),
            take_profit_ratio=Decimal("0.2"),
            trailing_stop_ratio=Decimal("0"),
        )
        port.strategies.append(ctx)
        ctxs.append(ctx)
    return port, ctxs


def _bar(stock, close, bar_time):
    """单股票 OHLCV bar，close 触发信号用。"""
    return BarEvent(
        stocks={stock: {
            "open": Decimal("9.0"), "high": Decimal(close),
            "low": Decimal("9.0"), "close": Decimal(close), "volume": 10000,
        }},
        bar_time=bar_time,
    )


# ---- 多组合资金：分账、不分池（P1-①）----
def test_buy_available_scales_by_capital_share_when_real_short():
    """真实账户可用不足时按配置资金占比缩量（60k/40k，可用 5 万 → 29400/19600）。"""
    factory, _ = _db_factory()
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"),
                              daily_loss_limit=Decimal("0.05"))
    p1 = Portfolio(portfolio_id=1, initial_capital=Decimal("60000"),
                   risk_manager=pm)
    p2 = Portfolio(portfolio_id=2, initial_capital=Decimal("40000"),
                   risk_manager=pm)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[p1, p2], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    # 各组合账本独立（不被账户总资产覆盖）
    assert p1.account.cash == Decimal("60000")
    assert p2.account.cash == Decimal("40000")

    engine._latest_account_cash = Decimal("50000")
    engine._latest_account_cash_at = datetime(2026, 11, 11, 10, 0)
    # 50000×0.98=49000 → 60% / 40% 等比缩量
    assert engine._buy_available_cash(p1) == Decimal("29400.00")
    assert engine._buy_available_cash(p2) == Decimal("19600.00")


# ---------------- _handle_bar 下单落库（切片5：先 submitted 后回填）----------------
def test_handle_bar_signal_to_trade_persisted():
    """OPEN 信号 → BUY 发单 → LiveOrder(status=submitted) 落库，不写 LiveTrade、不 apply。

    切片5 时序：先写 submitted + commit，再发 passorder。submitted 阶段不写 LiveTrade
    （回填确认 filled 才写）、不 apply_trade（持仓/现金不变，避免受理即成交的近似）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    # 预置信号缓存：触发 open_sig=1
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    trades = db.query(LiveTrade).all()
    orders = db.query(LiveOrder).all()
    assert len(trades) == 0  # 回填确认成交前不写 LiveTrade
    assert len(orders) == 1
    assert orders[0].status == "submitted"
    assert orders[0].signal_name == "open_sig"
    # 下单量由 _signal_to_order 计算：int(0.1 * 60000 / 9.3 / 100) * 100 = 600
    assert orders[0].quantity == 600
    assert orders[0].filled_quantity == 0
    # submitted 阶段不 apply：持仓/现金不变
    assert ctx.positions[stock].quantity == 0
    assert port.account.cash == Decimal("100000")
    db.close()


# ---- 交易日历下单总闸（#144）----
def _calendar_engine(factory, port, disp, trading_dates, stock, engine_key=""):
    """构造带注入假日历的 LiveEngine（假日历不经桥，直接给日期列表）。"""
    from core.engine.trading_calendar import TradingCalendar
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    cal = TradingCalendar(lambda: list(trading_dates))
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        trading_calendar=cal, engine_key=engine_key,
    )


def test_handle_bar_blocks_order_on_non_trading_day():
    """交易日历总闸：节假日/非交易日 → 信号推但不落单、不发桥。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    # 2026-10-01 国庆：日历里没有它
    bar_time = datetime(2026, 10, 1, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [date(2026, 10, 9)], stock)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    assert db.query(LiveOrder).count() == 0  # 非交易日不落单
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)  # 不发桥


def test_handle_bar_blocks_order_before_open():
    """日内周期：盘前 09:29 不下单。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 24, 9, 29)  # 周一，交易日，但未开盘
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 24)], stock)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()


def test_handle_bar_daily_period_trading_day_at_midnight_ok():
    """1d 周期 bar_time=00:00（14:30 驱动约定）：只判交易日，不卡时段。

    B-1 未来函数闸门：当日仅进 deferred 队列（不被收盘/时段门误杀），
    次日首 bar 放行后正常提交。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 24, 0, 0)  # 日线约定 00:00
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 24), date(2026, 8, 25)], stock)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1d"

    engine._handle_bar(port, bar)

    assert len(engine._signal_bar_deferred) == 1  # 当日 deferred（非时段门拦截）

    # 次日首 bar 放行 → 正常提交
    engine.signal_cache = {}
    engine._handle_bar(port, _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31)))

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "submitted"
    db.close()


# ---- B-1 未来函数闸门实盘化（日线信号次日执行，对齐回测）----
def test_handle_bar_daily_signal_deferred_same_day():
    """B-1：日线信号当日不下单——进 deferred 队列 + 记 signal_bar_date defer 决策事件。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 24, 0, 0)  # 日线约定 00:00
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 24), date(2026, 8, 25)], stock
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1d"

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    # 当日不提交：仅落 status=deferred 行（P2 持久化，重启 recover 重建队列）
    assert len(orders) == 1 and orders[0].status == "deferred"
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)  # 不发桥
    assert len(engine._signal_bar_deferred) == 1  # 进 deferred 队列
    deferred = engine._signal_bar_deferred[0]
    assert deferred.signal_bar_date == date(2026, 8, 24)
    assert deferred.bar_time == bar_time  # 生成时上下文（决策时间戳）保持
    db = factory()
    evs = db.query(LiveDecisionEvent).filter_by(gate="signal_bar_date").all()
    assert len(evs) == 1 and evs[0].action == "defer"
    db.close()


def test_handle_bar_deferred_daily_signal_released_next_day():
    """B-1：次日首 bar 到来 → deferred 订单自动放行重走提交链（bar_time 更新为当前 bar）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    day1 = datetime(2026, 8, 24, 0, 0)
    day2 = datetime(2026, 8, 25, 9, 31)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 24), date(2026, 8, 25)], stock
    )
    engine.signal_cache = {(1, stock, day1): [{"name": "open_sig", "value": 1}]}
    bar1 = _bar(stock, "9.3", day1)
    bar1.period = "1d"
    engine._handle_bar(port, bar1)
    assert len(engine._signal_bar_deferred) == 1

    engine.signal_cache = {}  # 次日无新信号
    engine._handle_bar(port, _bar(stock, "9.3", day2))

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "submitted"
    assert orders[0].bar_time == day2  # 决策时间戳 = 放行 bar
    assert engine._signal_bar_deferred == []  # 队列清空
    db.close()
    assert any(r.url.path == "/order" for r in rec.requests)  # 已发桥


def test_goldminer_bridge_quote_releases_deferred_without_snapshot_bar(monkeypatch):
    """掘金通道：桥 1m 今日已完成 bar → 立即放行 deferred（不等快照首根完成 bar）。

    goldminer 的 1m 驱动行情仍走快照合成（无历史），但放行闸可由桥 /quote 补齐：
    取桥返回的「今日已完成」1m bar 构造 baseline BarEvent，只放行 deferred。
    """
    # 冻结墙钟：after_close_block 闸门按真实时间判「收盘后分钟级信号过期」，
    # 否则本用例在 15:00 之后跑会必然失败（时间相关假阴性）。
    monkeypatch.setattr("core.engine.live_engine.now_shanghai",
                        lambda: datetime(2026, 11, 10, 11, 29, 30))
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 11, 9), date(2026, 11, 10)
    quote_bars = [
        {"stime": "20261110112900", "open": 10.0, "high": 10.1,
         "low": 9.9, "close": 10.05, "volume": 1000},
        {"stime": "20261110113000", "open": 10.05, "high": 10.1,
         "low": 10.0, "close": 10.08, "volume": 1200},
    ]

    def _respond(req):
        if req.url.path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {stock: quote_bars}})
        if req.url.path == "/order":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(200, json={"ok": True, "data": []})

    rec = _Recorder(respond=_respond)
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2], stock,
                              engine_key="goldminer")
    db = factory()
    engine._submit_one_order(db, port, _deferred_buy_order(stock, d1), "1d")
    db.close()
    assert len(engine._signal_bar_deferred) == 1

    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 45))

    assert engine._signal_bar_deferred == []  # 首轮即放行，不等快照首根完成 bar
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "submitted"
    # 放行 bar_time = 桥返回的今日已完成 1m bar（11:29，最新一根 11:30 视为 forming）
    assert orders[0].bar_time == datetime(2026, 11, 10, 11, 29)
    db.close()
    assert any(r.url.path == "/order" for r in rec.requests)


def test_goldminer_bridge_probe_failsafe_and_engine_isolation():
    """桥 /quote 失败 → 不放行不清单不崩；非掘金通道不探测（不改变既有行为）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 11, 9), date(2026, 11, 10)

    # ① 掘金通道 + 桥 /quote 断连：fail-safe，队列不动、不发单
    rec = _Recorder(fail_paths={"/quote"})
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2], stock,
                              engine_key="goldminer")
    db = factory()
    engine._submit_one_order(db, port, _deferred_buy_order(stock, d1), "1d")
    db.close()
    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 45))
    assert len(engine._signal_bar_deferred) == 1
    assert not any(r.url.path == "/order" for r in rec.requests)

    # ② 非掘金通道：直接跳过，零行情请求（不改 tdx/ths/iquant 行为）
    rec2 = _Recorder()
    disp2, _ = _make_dispatcher(rec2)
    engine2 = _calendar_engine(factory, port, disp2, [d1, d2], stock, engine_key="tdx")
    db = factory()
    engine2._submit_one_order(db, port, _deferred_buy_order(stock, d1), "1d")
    db.close()
    engine2._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 45))
    assert len(engine2._signal_bar_deferred) == 1
    assert not any(r.url.path == "/quote" for r in rec2.requests)


def _quote_responder(bars_by_code):
    """按请求 code 返回 /quote 结果（goldminer 桥同构响应）。"""
    def _respond(req):
        if req.url.path == "/quote":
            code = req.url.params.get("code")
            return httpx.Response(
                200, json={"ok": True, "data": {code: bars_by_code.get(code, [])}})
        if req.url.path == "/order":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(200, json={"ok": True, "data": []})
    return _respond


def _today_bars_600000():
    return [
        {"stime": "20261110112900", "open": 10.0, "high": 10.1,
         "low": 9.9, "close": 10.05, "volume": 1000},
        {"stime": "20261110113000", "open": 10.05, "high": 10.1,
         "low": 10.0, "close": 10.08, "volume": 1200},
    ]


def test_goldminer_bridge_probe_zero_cost_without_cross_day_deferred():
    """辅助探测成本≈0：没有跨日 deferred（同一交易日的单不算）→ 一次都不探。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    today = date(2026, 11, 10)
    rec = _Recorder(respond=_quote_responder({stock: _today_bars_600000()}))
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [today], stock, engine_key="goldminer")
    db = factory()
    engine._submit_one_order(db, port, _deferred_buy_order(stock, today), "1d")
    db.close()
    assert len(engine._signal_bar_deferred) == 1

    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 45))

    assert not any(r.url.path == "/quote" for r in rec.requests)
    assert len(engine._signal_bar_deferred) == 1


def test_goldminer_bridge_probe_budget_exhausted_warns_and_backs_off():
    """单轮预算耗尽 → 记 WARNING、放弃本轮、置退避；不发单、不清单。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 11, 9), date(2026, 11, 10)
    rec = _Recorder(respond=_quote_responder({stock: _today_bars_600000()}))
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2], stock, engine_key="goldminer")
    engine._BRIDGE_PROBE_BUDGET_SECONDS = 0.0  # 预算立即耗尽
    db = factory()
    engine._submit_one_order(db, port, _deferred_buy_order(stock, d1), "1d")
    db.close()

    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 45))

    assert not any(r.url.path == "/quote" for r in rec.requests)
    assert len(engine._signal_bar_deferred) == 1
    assert not any(r.url.path == "/order" for r in rec.requests)
    assert engine._bridge_probe_next_at == datetime(2026, 11, 10, 12, 50)


def test_goldminer_bridge_probe_backoff_skips_next_ticks():
    """桥失败 → 退避 5 分钟内零探测；退避结束后才重试。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 11, 9), date(2026, 11, 10)
    rec = _Recorder(fail_paths={"/quote"})
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2], stock, engine_key="goldminer")
    db = factory()
    engine._submit_one_order(db, port, _deferred_buy_order(stock, d1), "1d")
    db.close()

    def _quotes():
        return len([r for r in rec.requests if r.url.path == "/quote"])

    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 45))
    assert _quotes() == 1
    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 46))
    assert _quotes() == 1  # 退避期内
    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 50))
    assert _quotes() == 2  # 退避结束再试
    assert len(engine._signal_bar_deferred) == 1


def test_goldminer_bridge_probe_only_needed_codes_and_no_repeat():
    """只探挂跨日 deferred 的 code（不碰池内其它 code）；放行过的不再重复探。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock, other = "600000.SH", "000001.SZ"
    d1, d2 = date(2026, 11, 9), date(2026, 11, 10)
    bars = _today_bars_600000()
    rec = _Recorder(respond=_quote_responder({stock: bars, other: bars}))
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2], stock, engine_key="goldminer")
    engine._bar_poller.add_codes([other])  # 池内还有别的 code
    db = factory()
    engine._submit_one_order(db, port, _deferred_buy_order(stock, d1), "1d")
    db.close()

    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 45))
    quote_codes = [r.url.params.get("code") for r in rec.requests if r.url.path == "/quote"]
    # 探测只打 deferred 的 code（提交链自己的定价取价请求也在其中，但绝不含池内其它 code）
    assert quote_codes and set(quote_codes) == {stock}
    assert engine._signal_bar_deferred == []

    # 该 code 已借桥放行过：队列里再出现同 code 跨日单也不再探（等快照兜底）
    engine._signal_bar_deferred.append(_deferred_buy_order(stock, d1))
    before = len([r for r in rec.requests if r.url.path == "/quote"])
    engine._maybe_bridge_deferred_release(datetime(2026, 11, 10, 12, 46))
    assert len([r for r in rec.requests if r.url.path == "/quote"]) == before


def _deferred_buy_order(stock, day, qty=300):
    """BUY 形状的 deferred 日线信号单（放行后走提交链）。"""
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=qty, price=Decimal("9.0"),
        bar_time=datetime.combine(day, datetime.min.time()),
        signal_name="open_sig", signal_bar_date=day,
    )


def test_release_expires_deferred_after_one_trading_day():
    """R2：只允许在 signal_bar_date 的下一交易日放行；再晚 → canceled + 决策事件。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2, d3 = date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2, d3], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()
    assert len(engine._signal_bar_deferred) == 1

    # 8/26 已跨过 8/25（唯一放行窗口）→ 过期取消，不下单
    # due 当日（8/25）引擎收到过分钟 bar（正常有行情）→ 不算「行情失效」，不顺延
    engine._note_minute_bar_date(
        _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31)))
    engine._note_minute_bar_date(
        _bar(stock, "9.3", datetime(2026, 8, 26, 9, 31)))
    bar = _bar(stock, "9.3",
               datetime.combine(d3, datetime.min.time()).replace(hour=9, minute=31))
    released = engine._release_signal_bar_deferred(port, bar)
    assert released == []
    assert engine._signal_bar_deferred == []

    db = factory()
    row = db.query(LiveOrder).first()
    db.close()
    assert row.status == "canceled"
    assert "stale_release" in (row.error_message or "")
    assert not any(r.url.path == "/order" for r in rec.requests)

    events = engine._recorder.drain()
    assert any(e.gate == "signal_bar_date" and e.action == "expire" for e in events)


def test_release_extends_once_when_due_day_had_no_minute_bar():
    """P1（2026-11-09）：due 当日一根分钟 bar 都没有（行情失效）→ 顺延一个交易日放行。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    # SELL 放行需有虚拟持仓（无持仓 cap_quantity=None → 放行后被结算取消）
    ctx.positions[stock] = Position(stock)
    ctx.positions[stock].buy(300, Decimal("9.0"), datetime(2026, 8, 24, 9, 30))
    d1, d2, d3 = date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2, d3], stock)
    engine.signal_cache = {}

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()
    assert len(engine._signal_bar_deferred) == 1

    events = []
    engine._emit = lambda *a: events.append(a)

    # d2（due）整日没有分钟 bar（行情链路失效）；d3 首根分钟 bar 才恢复
    engine._handle_bar(
        port, _bar(stock, "9.3", datetime(2026, 8, 26, 9, 31)))

    assert engine._signal_bar_deferred == []                       # 顺延后已放行
    assert any(r.url.path == "/order" for r in rec.requests)       # 确实走了提交链
    risk = [p for t, p in events if t == "risk"]
    assert any(p.get("rule") == "deferred_extended" for p in risk)
    assert not any(p.get("rule") == "deferred_expired" for p in risk)
    db = factory()
    evs = db.query(LiveDecisionEvent).filter_by(gate="signal_bar_date").all()
    rows = db.query(LiveOrder).all()
    db.close()
    assert any(e.action == "extend" for e in evs)
    assert any(r.status == "submitted" for r in rows)
    assert not any(r.status == "canceled" for r in rows)


def test_deferred_extended_marker_cancels_on_second_due_day():
    """P1：已顺延一次（DB 标记读回）→ 第二个到期日仍超期 → 正常取消，不再顺延。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2, d3, d4 = (date(2026, 8, 24), date(2026, 8, 25),
                      date(2026, 8, 26), date(2026, 8, 27))
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2, d3, d4], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()
    # = recover 从 DB 行 error_message 读回「已顺延一次」
    engine._signal_bar_deferred[0]._deferred_extended = True

    events = []
    engine._emit = lambda *a: events.append(a)
    # 顺延后到期日 = d3；d4 才来 bar → 已超期 → 取消
    released = engine._release_signal_bar_deferred(
        port, _bar(stock, "9.3", datetime(2026, 8, 27, 9, 31)))

    assert released == [] and engine._signal_bar_deferred == []
    risk = [p for t, p in events if t == "risk"]
    assert any(p.get("rule") == "deferred_expired" for p in risk)
    assert not any(p.get("rule") == "deferred_extended" for p in risk)
    assert not any(r.url.path == "/order" for r in rec.requests)

    db = factory()
    row = db.query(LiveOrder).first()
    db.close()
    assert row.status == "canceled"
    assert "stale_release" in (row.error_message or "")


def test_baseline_bar_releases_deferred_without_signal_eval():
    """P1（2026-11-09）：全新启动首根完成 bar 被 BarPoller 当基线时，deferred 在这一根
    就放行提交（不再等第二根）；但公式信号评估仍不执行（防重放历史 bar）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    # SELL 放行需有虚拟持仓（无持仓 cap_quantity=None → 放行后被结算取消）
    ctx.positions[stock] = Position(stock)
    ctx.positions[stock].buy(300, Decimal("9.0"), datetime(2026, 8, 24, 9, 30))
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2], stock)
    engine.signal_cache = {}

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()
    assert len(engine._signal_bar_deferred) == 1

    fill_calls = []
    engine._fill_signal_cache = lambda *a, **k: fill_calls.append(1)

    bar = _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31))
    bar.baseline = True
    engine._handle_bar(port, bar)

    assert engine._signal_bar_deferred == []                   # 基线即放行，不等第二根
    assert any(r.url.path == "/order" for r in rec.requests)
    assert fill_calls == []                                    # 公式信号评估未执行


def test_recover_alerts_stale_deferred_drop_and_reads_extended_marker(monkeypatch):
    """P0/P1：recover 丢弃陈旧 deferred 不再静默（risk 事件）；顺延标记可读回。"""
    import importlib

    from core.engine.trading_calendar import TradingCalendar
    live_mod = importlib.import_module("core.engine.live_engine")
    factory, _ = _db_factory()
    engine, _ = _breaker_engine(factory)
    monkeypatch.setattr(
        live_mod, "now_shanghai", lambda: datetime(2026, 8, 25, 9, 0))
    engine._calendar = TradingCalendar(
        lambda: [date(2026, 8, 21), date(2026, 8, 24), date(2026, 8, 25)])

    db = factory()
    _add_deferred_row(db, "600519.SH", datetime(2026, 8, 20, 0, 0))       # 早于上一交易日
    kept = _add_deferred_row(db, "600000.SH", datetime(2026, 8, 24, 0, 0))
    kept.error_message = "deferred_extended:due=2026-08-24"
    db.commit()

    events = []
    monkeypatch.setattr(engine, "_emit", lambda *a: events.append(a))
    engine.recover(db)
    db.close()

    assert [o.stock_code for o in engine._signal_bar_deferred] == ["600000.SH"]
    assert engine._signal_bar_deferred[0]._deferred_extended is True
    risk = [p for t, p in events
            if t == "risk" and p.get("rule") == "deferred_dropped_stale"]
    assert len(risk) == 1 and risk[0]["stock_code"] == "600519.SH"


def test_post_close_restart_keeps_prev_day_deferred_until_next_day():
    """P0（2026-11-09）：收盘后启动时 15:05 日线轮不得放行上一交易日的 deferred 单。

    复现路径：d1 信号 → d2 15:05（收盘后）引擎才起来，日线级别轮次到来。旧实现把
    该 bar 当放行 bar：真实报单（收盘后必成废单）且 bar_time 被改写成 d2 00:00，
    令 15:00 收盘守卫失效。修复后：日线/收盘落定 bar 一律不放行，单保留在队列，
    等 d3 首根分钟 bar 再放行（收盘后启动导致的顺延）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2, d3 = date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2, d3], stock)
    engine._started_at = datetime(2026, 8, 25, 15, 6)  # 收盘后才启动
    engine.signal_cache = {}

    db = factory()
    engine._submit_one_order(
        db, port,
        _buy_order(stock, "9.3", 300, datetime(2026, 8, 24, 14, 55), d1), "1d")
    db.close()
    assert len(engine._signal_bar_deferred) == 1

    # d2 15:05 日线级别轮次（post_close）→ 不放行、不提交
    bar2 = _bar(stock, "9.3", datetime(2026, 8, 25, 15, 5))
    bar2.period = "1d"
    bar2.post_close = True
    engine._handle_bar(port, bar2)
    assert len(engine._signal_bar_deferred) == 1
    assert not any(r.url.path == "/order" for r in rec.requests)

    # d3 首根分钟 bar → 放行提交（顺延一天，不是作废）
    engine._handle_bar(port, _bar(stock, "9.3", datetime(2026, 8, 26, 9, 31)))
    assert engine._signal_bar_deferred == []
    assert any(r.url.path == "/order" for r in rec.requests)


def test_released_deferred_orders_join_batch_allocation():
    """P3：放行后的 deferred 单（bar_time 已刷新为放行 bar）参与同批资金分配。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    port.account.cash = Decimal("20000")  # 两笔各目标 10000，现金足够满额
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    d1 = date(2026, 8, 24)
    bt = datetime(2026, 8, 25, 9, 31)  # 放行 bar（bar_time 已刷新）
    o1 = _buy_order("600000.SH", "10", 1000, bt)
    o2 = _buy_order("600001.SH", "10", 1000, bt)
    for o in (o1, o2):
        o.signal_bar_date = d1

    kept = engine._allocate_batch_buys(port, [o1, o2])

    assert len(kept) == 2  # 两笔都参与（未被当作 deferred 排除）
    assert all(o.quantity == 1000 for o in kept)  # 资金足够 → 按目标量满额


def test_settle_unsubmitted_released_marks_row_canceled():
    """放行但未提交（闸门拦截）→ DB 行 deferred 转 canceled，避免重启重复恢复。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1 = date(2026, 8, 24)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.commit()
    order = engine._signal_bar_deferred[0]
    assert getattr(order, "_deferred_row_id", None)

    engine._settle_unsubmitted_released(db, port, [order])
    db.commit()
    db.close()

    db = factory()
    row = db.query(LiveOrder).first()
    db.close()
    assert row.status == "canceled"
    assert "released_not_submitted" in (row.error_message or "")
    assert getattr(order, "_deferred_row_id", None) is None


def test_handle_bar_minute_signal_not_gated():
    """B-1：分钟信号（signal_bar_date=None）不受闸门影响，当 bar 照常提交。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"

    engine._handle_bar(port, bar)

    db = factory()
    assert db.query(LiveOrder).count() == 1
    assert engine._signal_bar_deferred == []  # 分钟信号不进队列
    db.close()


def test_stop_clears_signal_bar_deferred():
    """B-1：stop() 清空内存队列，但必须把无行单如实重落库（status=deferred）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    order = OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000.SH",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=600, price=Decimal("9.3"),
        bar_time=datetime(2026, 8, 24, 0, 0),
        signal_bar_date=date(2026, 8, 24),
    )
    engine._signal_bar_deferred.append(order)

    asyncio.run(engine.stop())

    assert engine._signal_bar_deferred == []
    db = factory()
    rows = db.query(LiveOrder).filter(LiveOrder.status == "deferred").all()
    db.close()
    assert len(rows) == 1                       # 如实落库，任意时刻可 recover
    assert rows[0].stock_code == "600000.SH"
    assert rows[0].quantity == 600
    assert getattr(order, "_deferred_row_id", None) == rows[0].id


# ---- B-2 策略级敞口上限实盘透传（对齐回测 _exposure_kwargs）----
def _preset_position(ctx, stock, quantity, avg_cost):
    """预置虚拟持仓（实盘每策略独立 ctx.positions）。

    直接写 quantity/avg_cost 视为「平台自己买入的持仓」，同步 platform_quantity
    （槽位闸口径：外部/手工导入仓 platform_quantity 恒 0，不占名额）。
    """
    pos = Position(stock)
    pos.quantity = quantity
    pos.avg_cost = avg_cost
    pos.platform_quantity = quantity
    ctx.positions[stock] = pos


def test_handle_bar_exposure_cap_rejects_buy_over_limit():
    """B-2：exposure_cap_enabled=True + 策略持仓市值超 capital_ratio×初始资金 → 拒单+事件。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1m")
    ctx.exposure_cap_enabled = True  # capital_ratio=0.6 × 100000 = 上限 60000
    stock = "600000.SH"
    _preset_position(ctx, "000001.SZ", 7000, Decimal("9.0"))  # 市值 63000 > 60000
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"

    engine._handle_bar(port, bar)

    db = factory()
    assert db.query(LiveOrder).count() == 0  # 敞口超限拒单，不落单
    evs = db.query(LiveDecisionEvent).filter_by(gate="exposure_cap").all()
    assert len(evs) == 1 and evs[0].action == "reject"
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)


def test_handle_bar_exposure_cap_disabled_keeps_old_behavior():
    """B-2：exposure_cap_enabled=False（默认关）→ 不约束，同 setup 照常提交。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1m")
    assert ctx.exposure_cap_enabled is False
    stock = "600000.SH"
    _preset_position(ctx, "000001.SZ", 7000, Decimal("9.0"))
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "submitted"
    assert not any(
        d.gate == "exposure_cap" for d in db.query(LiveDecisionEvent).all()
    )
    db.close()


# ---- 策略级单笔最大买入金额（max_order_amount，实盘透传 cap_quantity）----
def test_handle_bar_amount_cap_shrinks_buy_before_submit():
    """max_order_amount=1000 @9.3 → 缩至 100 股（1 整手）落 submitted + 事件。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1m")
    ctx.max_order_amount = Decimal("1000")  # 1000/9.3=107.5 → 1 整手 = 100 股
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "submitted"
    assert orders[0].quantity == 100  # 默认请求 600 → 金额封顶 100
    evs = db.query(LiveDecisionEvent).filter_by(gate="order_amount_cap").all()
    assert len(evs) == 1 and evs[0].action == "shrink"
    db.close()
    assert any(r.url.path == "/order" for r in rec.requests)  # 缩量后照常发桥


def test_handle_bar_amount_cap_none_keeps_old_behavior():
    """max_order_amount=None（默认）→ 600 股照常提交，无金额事件。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1m")
    assert ctx.max_order_amount is None
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].quantity == 600
    assert not any(
        d.gate == "order_amount_cap" for d in db.query(LiveDecisionEvent).all()
    )
    db.close()


# ---- 同批买单资金分配 + 在途预留（避免 FIFO 饿死/最后一笔资金不足）----
def _buy_order(stock, price, qty, bar_time, signal_bar_date=None):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=qty, price=Decimal(price), bar_time=bar_time,
        signal_name="open_sig", signal_bar_date=signal_bar_date,
    )


def test_reserved_buy_cash_and_available_uses_pending_orders():
    """在途 BUY 预留 = 委托量×挂单价；可用现金 = min(账本−预留, 真实可用×0.98)。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    engine._pending_orders = {
        7: LiveOrder(
            id=7, portfolio_strategy_id=1, trade_type="buy",
            status="submitted", quantity=1000, price=Decimal("10.00"),
        ),
        8: LiveOrder(  # 未成交买入的另一组合：不计入组合 1
            id=8, portfolio_strategy_id=2, trade_type="buy",
            status="submitted", quantity=500, price=Decimal("10.00"),
        ),
        9: LiveOrder(  # 已全成交（status=filled）：不预留，避免与账本扣款双计
            id=9, portfolio_strategy_id=1, trade_type="buy",
            status="filled", quantity=1000, price=Decimal("10.00"),
        ),
        10: LiveOrder(  # 卖单不占现金
            id=10, portfolio_strategy_id=1, trade_type="sell",
            status="submitted", quantity=1000, price=Decimal("10.00"),
        ),
    }
    assert engine._reserved_buy_cash(1) == Decimal("10000.00")

    # 无真实快照：纯虚拟账本口径
    engine._latest_account_cash = None
    assert engine._buy_available_cash(port) == Decimal("90000")

    # 真实可用更低 → 取小（真实仍是硬上限）。真实侧扣**会话级**预留（10000+5000）：
    # 多组合共享同一账户，各组合只扣自己的预留都会看到「整账户可用」并各自吃满。
    assert engine._reserved_buy_cash() == Decimal("15000.00")
    engine._latest_account_cash = Decimal("50000")
    assert engine._buy_available_cash(port) == Decimal("34000.00")


def test_buy_available_cash_skips_inflight_already_in_snapshot():
    """001311 复盘：提交早于快照的在途单冻结已含在真实可用里，不得再扣一次。

    快照 57,260.51 已扣 300415 冻结 50,966.90；若再减预留 50,960，
    真实侧会从 56,115.30 跌到 5,155.30（3500 股被压成 300 股）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    snap_at = datetime(2026, 11, 10, 13, 1, 42, 804000)
    early = LiveOrder(
        id=7, portfolio_strategy_id=1, trade_type="buy",
        status="submitted", quantity=2800, price=Decimal("18.20"),
    )
    early._submit_at = snap_at - timedelta(seconds=1)  # 快照采集前已提交
    engine._pending_orders = {7: early}
    engine._latest_account_cash = Decimal("57260.51")
    engine._latest_account_cash_at = snap_at
    port.account.cash = Decimal("107164.10")
    # 账本侧 107,164.10-50,960=56,204.10；真实侧 57,260.51×0.98=56,115.30（不扣预留）
    assert engine._buy_available_cash(port) == Decimal("56115.30")


def test_buy_available_cash_still_counts_inflight_after_snapshot():
    """提交晚于快照的在途单快照没反映 → 真实侧照扣（保守，防超买）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    snap_at = datetime(2026, 11, 10, 13, 1, 42, 804000)
    late = LiveOrder(
        id=8, portfolio_strategy_id=1, trade_type="buy",
        status="submitted", quantity=2800, price=Decimal("18.20"),
    )
    late._submit_at = snap_at + timedelta(milliseconds=50)  # 快照之后才提交
    engine._pending_orders = {8: late}
    engine._latest_account_cash = Decimal("57260.51")
    engine._latest_account_cash_at = snap_at
    port.account.cash = Decimal("200000")
    # 真实侧 57,260.51×0.98-50,960=5,155.30 → 取小
    assert engine._buy_available_cash(port) == Decimal("5155.30")


def test_buy_available_cash_conservative_without_snapshot_time(caplog):
    """快照采集时间不可知 → 保守全额扣减，并打 WARNING 标注走了保守分支。"""
    caplog.set_level("WARNING")
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    early = LiveOrder(
        id=9, portfolio_strategy_id=1, trade_type="buy",
        status="submitted", quantity=1000, price=Decimal("10.00"),
    )
    early._submit_at = datetime(2026, 11, 10, 9, 31, 0)
    engine._pending_orders = {9: early}
    engine._latest_account_cash = Decimal("50000")
    engine._latest_account_cash_at = None  # 尚未采集（recover 后）
    # 真实侧 50,000×0.98-10,000=39,000；账本 100,000-10,000=90,000 → 取小
    assert engine._buy_available_cash(port) == Decimal("39000.00")
    assert any("保守口径" in r.getMessage() for r in caplog.records)
    # 去重：再调一次不再重复告警
    engine._buy_available_cash(port)
    assert sum("保守口径" in r.getMessage() for r in caplog.records) == 1


def test_allocate_batch_buys_full_target_then_skip_when_cash_short():
    """4 笔各 1 万、目标份额 1.2 万：可用 3 万满额开 3 只，第 4 只整笔跳过（不缩量）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    ctx.single_open_ratio = Decimal("0.2")  # 目标份额 = 0.2 × 0.6 × 100000 = 12000
    port.account.cash = Decimal("30000")  # 4 笔各 10000 目标合计 40000 > 30000
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    bt = datetime(2026, 8, 5, 10, 0)
    orders = [
        _buy_order("600000.SH", "10", 1000, bt),
        _buy_order("600001.SH", "10", 1000, bt),
        _buy_order("600002.SH", "10", 1000, bt),
        _buy_order("600003.SH", "10", 1000, bt),
    ]

    kept = engine._allocate_batch_buys(port, list(orders))

    assert len(kept) == 3
    assert [o.quantity for o in orders[:3]] == [1000, 1000, 1000]  # 满额，不缩量
    assert orders[3].quantity == 1000  # 被跳过，原量不变（不下单）
    evs = engine._recorder.drain()
    assert len(evs) == 1
    assert evs[0].gate == "insufficient_cash" and evs[0].action == "skip"
    assert evs[0].stock_code == "600003.SH"
    assert evs[0].final_qty == 0


def test_allocate_batch_buys_skips_below_min_fill_ratio():
    """实际可买金额 < 目标份额 80% → 整笔跳过并记 insufficient_cash/skip。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()  # 目标份额 = 0.1 × 0.6 × 100000 = 6000（80% = 4800）
    port.account.cash = Decimal("4000")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    bt = datetime(2026, 8, 5, 10, 0)
    order = _buy_order("600000.SH", "10", 1000, bt)  # 目标 1 万

    kept = engine._allocate_batch_buys(port, [order])

    assert kept == []
    assert order.quantity == 1000  # 原量不变（不下单）
    evs = engine._recorder.drain()
    assert len(evs) == 1
    assert evs[0].gate == "insufficient_cash" and evs[0].action == "skip"
    assert evs[0].actual_value == 4000.0
    assert evs[0].param_value == 6000.0


def test_allocate_batch_buys_excludes_deferred_orders():
    """deferred 单不参与本 bar 资金分配（其额度以放行 bar 为准），数量保持目标值。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()  # 目标份额 6000（80% = 4800）
    port.account.cash = Decimal("15000")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    bt = datetime(2026, 8, 5, 10, 0)
    a = _buy_order("600000.SH", "10", 1000, bt)
    b = _buy_order("600001.SH", "10", 1000, bt)
    deferred = _buy_order("600002.SH", "10", 1000, bt,
                          signal_bar_date=bt.date())
    orders = [a, b, deferred]

    kept = engine._allocate_batch_buys(port, orders)

    assert len(kept) == 3
    assert deferred.quantity == 1000           # 不参与分配
    # a 满额 1000（1 万），剩余 5000 给 b（≥4800 门槛）→ 500 股
    assert (a.quantity, b.quantity) == (1000, 500)
    assert Decimal(a.quantity) * a.price + Decimal(b.quantity) * b.price \
        == Decimal("15000")


# ---- 组合级护栏：单票集中度上限 / 现金留存下限（2026-11-16 仅实盘接线）----
def _equity_caps_port(cash="100000", cap=None, reserve=None):
    """构造单组合单策略 port + 两个护栏阈值（None=不设限）。"""
    port, ctx = _portfolio_single()
    ctx.single_open_ratio = Decimal("0.2")   # 目标份额 12000（80% = 9600）
    port.account.cash = Decimal(cash)
    port.max_single_position_pct = (
        Decimal(str(cap)) if cap is not None else None)
    port.min_cash_reserve_pct = (
        Decimal(str(reserve)) if reserve is not None else None)
    return port, ctx


def test_allocation_caps_default_unconstrained_keeps_tomorrow_plan():
    """默认不约束（NULL / 0）：明日两只（700@67.40、1300@39.50）数量逐字不变。"""
    factory, _ = _db_factory()
    bt = datetime(2026, 11, 17, 9, 31)
    for cap, reserve in ((None, None), (Decimal("0"), Decimal("0"))):
        port, _ = _equity_caps_port(cash="108513.51", cap=cap, reserve=reserve)
        disp, _ = _make_dispatcher(_Recorder())
        engine = _make_engine(disp, factory, port)
        a = _buy_order("301600.SZ", "67.40", 700, bt)
        b = _buy_order("600211.SH", "39.50", 1300, bt)

        kept = engine._allocate_batch_buys(port, [a, b])

        assert len(kept) == 2
        assert (a.quantity, b.quantity) == (700, 1300)
        assert engine._recorder.drain() == []    # 未命中任何约束，无事件


def test_allocation_caps_max_single_position_truncates():
    """单票上限 5%（组合总资产 10 万 → 5,000 元）：1000 股 @10 截断到 500 股。"""
    factory, _ = _db_factory()
    port, _ = _equity_caps_port(cash="100000", cap="0.05")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    order = _buy_order("600000.SH", "10", 1000, datetime(2026, 8, 5, 10, 0))

    kept = engine._allocate_batch_buys(port, [order])

    assert kept == [order]
    assert order.quantity == 500            # 5,000 / 10 = 500 股（整手）
    evs = engine._recorder.drain()
    assert len(evs) == 1
    ev = evs[0]
    assert ev.gate == "max_single_position" and ev.action == "shrink"
    assert ev.param_value == 0.05
    assert ev.actual_value == 0.1           # 未截断时买入后占比 10,000/100,000
    assert (ev.requested_qty, ev.final_qty) == (1000, 500)


def test_allocation_caps_min_cash_reserve_truncates():
    """现金下限 95%（10 万 → 最多花 5,000）：1000 股 @10 截断到 500 股。"""
    factory, _ = _db_factory()
    port, _ = _equity_caps_port(cash="100000", reserve="0.95")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    order = _buy_order("600000.SH", "10", 1000, datetime(2026, 8, 5, 10, 0))

    kept = engine._allocate_batch_buys(port, [order])

    assert kept == [order]
    assert order.quantity == 500
    evs = engine._recorder.drain()
    assert len(evs) == 1
    ev = evs[0]
    assert ev.gate == "min_cash_reserve" and ev.action == "shrink"
    assert ev.param_value == 0.95
    assert ev.actual_value == 0.9           # 未截断时买完只剩 10% 现金（< 95%）
    assert (ev.requested_qty, ev.final_qty) == (1000, 500)


def test_allocation_caps_both_hit_take_smaller_and_record_both():
    """两者同时命中：单票 3%（3,000 元 → 300 股）先截，现金 98%（→ 200 股）再截。"""
    factory, _ = _db_factory()
    port, _ = _equity_caps_port(cash="100000", cap="0.03", reserve="0.98")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    order = _buy_order("600000.SH", "10", 1000, datetime(2026, 8, 5, 10, 0))

    kept = engine._allocate_batch_buys(port, [order])

    assert kept == [order]
    assert order.quantity == 200            # 取两者更小：2,000 元可花 → 200 股
    evs = engine._recorder.drain()
    assert [e.gate for e in evs] == ["max_single_position", "min_cash_reserve"]
    assert (evs[0].requested_qty, evs[0].final_qty) == (1000, 300)
    assert (evs[1].requested_qty, evs[1].final_qty) == (300, 200)


def test_allocation_caps_truncate_below_one_lot_skips():
    """截断后不足 1 手（单票上限 0.1% → 100 元买不到 1 手）→ 整笔跳过并记 block。"""
    factory, _ = _db_factory()
    port, _ = _equity_caps_port(cash="100000", cap="0.001")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _make_engine(disp, factory, port)
    order = _buy_order("600000.SH", "10", 1000, datetime(2026, 8, 5, 10, 0))

    kept = engine._allocate_batch_buys(port, [order])

    assert kept == []
    assert order.quantity == 1000           # 原量不变（不下单）
    evs = engine._recorder.drain()
    assert len(evs) == 1
    assert evs[0].gate == "max_single_position" and evs[0].action == "block"
    assert evs[0].final_qty == 0


def test_handle_bar_allocates_batch_buy_cash(monkeypatch):
    """_handle_bar 接线：同 bar 4 笔买单在可用 3 万下满额开 3 只、第 4 只跳过。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio_single()
    port.account.cash = Decimal("30000")
    engine, _rec = _gate_engine(factory, port)
    submitted = []
    monkeypatch.setattr(
        engine, "_submit_one_order",
        lambda db, p, o, period, emit_signal=True, **kw: submitted.append(
            (o.stock_code, o.quantity)))
    buys = [
        _buy_order(code, "10", 1000, _BT)
        for code in ("600000.SH", "600001.SH", "600002.SH", "600003.SH")
    ]
    monkeypatch.setattr(port, "on_bar", lambda *a, **k: list(buys))

    engine._handle_bar(port, _bar("600000.SH", "10", _BT))

    assert [q for _, q in submitted] == [1000, 1000, 1000]  # 第 4 只资金不足跳过


def test_no_signal_no_trade():
    """bar 不触发信号 → 不落 trade、不落 order。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 1)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    # 信号缓存空 → 无触发
    engine.signal_cache = {}
    bar = _bar(stock, "9.3", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    assert db.query(LiveTrade).count() == 0
    assert db.query(LiveOrder).count() == 0
    db.close()


# ---------------- 持仓恢复 ----------------
def test_recover_rebuilds_positions_from_trades():
    """预置 live_trades(1 笔 BUY 100 股) → recover → position.quantity==100、现金==初始-金额。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置一笔成交落库（模拟 Core 重启前的历史交易）
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code=stock,
        trade_type="buy", order_type="limit", price=Decimal("9.3"), quantity=100,
        filled_quantity=100, filled_price=Decimal("9.3"), status="accepted",
        signal_name="open_sig", signal_type="OPEN", bar_time=datetime(2026, 8, 5, 10, 0),
    )
    db.add(lo)
    db.flush()
    db.add(LiveTrade(
        live_session_id=1, live_order_id=lo.id, portfolio_strategy_id=1, strategy_id=1,
        stock_code=stock, trade_type="buy", price=Decimal("9.3"), quantity=100,
        amount=Decimal("930"), commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=datetime(2026, 8, 5, 10, 0),
    ))
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    db = factory()
    engine.recover(db)
    db.close()

    assert ctx.positions[stock].quantity == 100
    assert port.account.cash == Decimal("100000") - Decimal("930")


def test_recover_sell_without_buy_does_not_credit_cash():
    """P0-1：继承仓只有 SELL 没有 BUY → 重启重放不得凭空增加虚拟现金。

    继承仓（对账导入）没有平台 BUY 台账，重放时虚拟持仓为 0；旧实现按成交额全额
    入账 → 现金虚增（净值/回撤分母被污染，且与在线时的镜面口径不一致）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    disp, _ = _make_dispatcher(_Recorder())
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code=stock,
        trade_type="sell", order_type="limit", price=Decimal("9.5"), quantity=1000,
        filled_quantity=1000, filled_price=Decimal("9.5"), status="filled",
        bar_time=datetime(2026, 8, 5, 10, 0),
    )
    db.add(lo)
    db.flush()
    db.add(LiveTrade(
        live_session_id=1, live_order_id=lo.id, portfolio_strategy_id=1, strategy_id=1,
        stock_code=stock, trade_type="sell", price=Decimal("9.5"), quantity=1000,
        amount=Decimal("9500"), commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=datetime(2026, 8, 5, 10, 0),
    ))
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    db = factory()
    engine.recover(db)
    db.close()

    assert ctx.positions[stock].quantity == 0
    # 无虚拟持仓 → 一分钱都不入账（旧实现会变成 109500）
    assert port.account.cash == Decimal("100000")


def test_clamped_sell_credits_only_applied_quantity():
    """P0-1：人工先减仓（镜面已按成本回补现金）后在途卖单成交只补一次。

    虚拟持仓已被镜面降到 500，在途卖单成交 1000 股 → ``Position.apply_trade``
    返回实际扣减 500，现金只按 500 股成交额入账（旧实现按 1000 股全额入账）。
    """
    stock = "600000.SH"
    pos = Position(stock)
    pos.buy(1000, Decimal("10"), datetime(2026, 8, 4, 9, 30))
    account = Account(Decimal("100000"))
    # 模拟人工减仓 500 已被镜面覆盖：数量 500、按成本回补现金 5000
    pos.quantity = 500
    pos._lots = {date(2026, 8, 4): 500}
    account.add_cash(Decimal("5000"))

    trade = TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=TradeType.SELL, price=Decimal("11"), quantity=1000,
        amount=Decimal("11000"), commission=Decimal("5"),
        stamp_duty=Decimal("5.5"), trade_time=datetime(2026, 8, 5, 10, 0),
    )
    applied = pos.apply_trade(trade)
    assert applied == 500
    account.apply_trade(trade.scaled_to(applied))

    assert pos.quantity == 0
    # 100000 + 5000（人工减仓回补）+ 500×11 − 费用的一半（10.5/2≈5.25）
    assert account.cash == Decimal("110494.75")


def test_on_bar_no_runtime_error_when_deals_insert_positions_concurrently():
    """P0-2：deals 池并发插入新仓时，on_bar 不再抛 dict changed size。

    旧实现对 ``ctx.positions`` 直接迭代（风控/槽位/市值合计），deals 回填线程
    插入新仓即触发 ``RuntimeError: dictionary changed size during iteration``，
    被 _on_bar 的宽 except 吞成一行 handle_bar error → 整批信号与风控丢失。
    """
    port, ctx = _portfolio_single()
    port.book_lock = threading.RLock()
    held = Position("600000.SH")
    held.buy(1000, Decimal("10"), datetime(2026, 8, 4, 9, 30))
    ctx.positions["600000.SH"] = held

    stop = threading.Event()
    inserted = []

    def writer():
        i = 0
        while not stop.is_set():
            code = "300%03d.SZ" % (i % 900)
            with port.book_lock:
                if code not in ctx.positions:
                    p = Position(code)
                    p.buy(100, Decimal("5"), datetime(2026, 8, 4, 9, 30))
                    ctx.positions[code] = p
                    inserted.append(code)
            i += 1

    th = threading.Thread(target=writer, daemon=True)
    th.start()
    try:
        for _ in range(200):
            port.on_bar(
                _bar("600000.SH", "9.0", datetime(2026, 8, 5, 10, 0)),
                signal_cache={}, period="1m", inflight_opens={},
            )
    finally:
        stop.set()
        th.join(timeout=5)
    assert inserted, "并发写入线程未产生任何插入，用例没有覆盖到并发窗口"


def test_check_risks_waits_for_book_lock():
    """P0-2：风控读持仓持账本锁 —— 写线程持锁时 on_bar 必须等待。

    这保证止损数量取自一致快照：不会在 deals 镜面「数量已改、成本未改」的中间态
    读到半更新持仓（旧实现锁外读，卖出数量可能算错）。
    """
    port, ctx = _portfolio_single()
    lock = threading.RLock()
    port.book_lock = lock
    pos = Position("600000.SH")
    pos.buy(1000, Decimal("10"), datetime(2026, 8, 4, 9, 30))
    ctx.positions["600000.SH"] = pos

    done = threading.Event()

    def run():
        port.on_bar(
            _bar("600000.SH", "9.0", datetime(2026, 8, 5, 10, 0)),
            signal_cache={}, period="1m", inflight_opens={},
        )
        done.set()

    lock.acquire()  # 主线程持锁（模拟 deals 回填正在改账本）
    th = threading.Thread(target=run)
    th.start()
    try:
        assert not done.wait(0.3), "on_bar 未等待账本锁 → 可能读到半更新持仓"
    finally:
        lock.release()
    assert done.wait(3), "释放账本锁后 on_bar 仍未完成"
    th.join(5)


def test_allow_peak_update_only_on_daily_rounds_for_daily_strategies():
    """P1-2：全为日线策略的组合，盘中 1m bar 不抬净值峰值；日线轮次/分钟策略才抬。

    回测里日线策略每天只有一根 bar（峰值每日更新一次）；实盘额外收到的 1m bar 若
    也抬峰，回撤分母偏大 → 比回测更易熔断。
    """
    from types import SimpleNamespace
    from core.engine.live_engine import _allow_peak_update

    daily_only = SimpleNamespace(strategies=[SimpleNamespace(period="1d")])
    mixed = SimpleNamespace(strategies=[
        SimpleNamespace(period="1d"), SimpleNamespace(period="5m")])

    bar_1m = BarEvent(stocks={}, bar_time=datetime(2026, 8, 5, 10, 30), period="1m")
    assert _allow_peak_update(daily_only, bar_1m) is False
    # 日线收盘轮次（period=1d）→ 抬峰
    assert _allow_peak_update(
        daily_only, BarEvent(stocks={}, bar_time=datetime(2026, 8, 5, 15, 5), period="1d")
    ) is True
    # 组合含分钟策略 → 盘中照常抬峰（与分钟回测一致）
    assert _allow_peak_update(mixed, bar_1m) is True


def test_on_bar_update_skips_peak_for_intraday_bar_of_daily_portfolio():
    """P1-2：日线组合的盘中 1m bar 不抬峰，但当日末值/prev_close 仍推进。"""
    port, _ = _portfolio_single()
    rm = port.risk_manager
    from decimal import Decimal as D
    rm.update_peak(D("100000"), date(2026, 8, 4))
    # 模拟引擎对日线组合的盘中 1m bar：allow_peak_update=False
    rm.update_peak(D("115000"), date(2026, 8, 5), allow_peak_update=False)
    assert rm.peak_value == D("100000")
    assert rm.prev_close_value == D("100000")
    assert rm._last_bar_total_value == D("115000")


def test_recover_skips_external_trade_replay():
    """补记单（order_type=external）只留痕不记账：重启重放不得改虚拟现金。

    会话运行期 record_external_deals 刻意不 apply（持仓由桥快照镜面覆盖，
    资金基准已含该资产）。若 recover 全量重放，外部卖出会被 Position.sell
    截断为 0 持仓却照加卖出所得 → 虚拟现金虚增（净值/单仓金额被放大）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code=stock,
        trade_type="sell", order_type="external", price=Decimal("9.5"), quantity=100,
        filled_quantity=100, filled_price=Decimal("9.5"), status="filled",
        bar_time=datetime(2026, 8, 5, 10, 0),
    )
    db.add(lo)
    db.flush()
    db.add(LiveTrade(
        live_session_id=1, live_order_id=lo.id, portfolio_strategy_id=1, strategy_id=1,
        stock_code=stock, trade_type="sell", price=Decimal("9.5"), quantity=100,
        amount=Decimal("950"), commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=datetime(2026, 8, 5, 10, 0),
    ))
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    db = factory()
    engine.recover(db)
    db.close()

    assert stock not in ctx.positions
    assert port.account.cash == Decimal("100000")


# ---------------- LiveT1Checker ----------------
def test_live_t1_checker_full_quantity_without_bridge_map():
    """F5：桥可用表未取到（空表）→ 全量放行（券商端 T+1 兜底，不误伤正常卖出）。"""
    pos = Position("600000.SH")
    pos.buy(100, Decimal("9.0"), datetime(2026, 8, 5, 10, 0))
    checker = LiveT1Checker()
    assert checker.get_available_shares(pos, datetime(2026, 8, 5).date()) == 100


def test_live_t1_checker_caps_by_bridge_available():
    """F5：桥 available=200（昨仓 200、今买 400 不可卖）→ SELL 上限 min(600,200)=200。"""
    pos = Position("600000.SH")
    pos.buy(600, Decimal("9.0"), datetime(2026, 8, 5, 10, 0))
    checker = LiveT1Checker()
    checker.set_available_map({"600000.SH": 200})
    assert checker.get_available_shares(pos, datetime(2026, 8, 5).date()) == 200


def test_live_t1_checker_min_when_bridge_available_larger():
    """F5：桥 available > 持有量 → 以持有量为准（min）。"""
    pos = Position("600000.SH")
    pos.buy(100, Decimal("9.0"), datetime(2026, 8, 5, 10, 0))
    checker = LiveT1Checker()
    checker.set_available_map({"600000.SH": 500})
    assert checker.get_available_shares(pos, datetime(2026, 8, 5).date()) == 100


def test_handle_bar_sell_capped_by_bridge_available():
    """F5：CLOSE 信号 SELL 600，桥 /positions available=200 → LiveOrder.quantity==200。

    先 cap_quantity 定最终量再落 submitted——DB 下单量与实发一致，回填不误判 partial。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(strategy_id=1, period="1m")
    ctx.formula_signals = [
        {"signal_name": "close_sig", "signal_type": SignalType.CLOSE, "trigger_value": -1},
    ]
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    # 虚拟持仓 600 股（昨仓 200 + 今买 400，今买不可卖 → 桥 available=200）
    ctx.positions[stock] = Position(stock)
    ctx.positions[stock].buy(600, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))

    def respond(request):
        path = request.url.path
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [
                {"instrument": "600000", "exchange": "SH",
                 "available": 200, "volume": 600,
                 "yesterday_volume": 200, "on_road_volume": 400},
            ]})
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "close_sig", "value": -1}]}
    bar = _bar(stock, "9.0", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1
    assert orders[0].status == "submitted"
    assert orders[0].quantity == 200  # 先 cap 再落库：DB 量与实发一致
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert placed and json.loads(placed[0].content)["volume"] == 200
    db.close()


def test_submit_one_order_buy_rejected_by_cap_logs_info(caplog):
    """⑤ 实盘 cap_quantity 返回 None（资金不足不足1手）→ logger.info。

    资金不足的单在上游 ``_allocate_batch_buys`` 已整笔跳过（insufficient_cash），
    故这里直接调 ``_submit_one_order``，锁定执行层 cap 拒绝仍打 INFO 日志的兜底。
    """
    import logging
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(strategy_id=1, period="1m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    port.account.cash = Decimal("0")  # 现金清零 → 不足1手拒绝
    port.position_sizing = "fixed"

    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    order = _buy_order(stock, "9.0", 100, bar_time)
    db = factory()
    try:
        with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
            engine._submit_one_order(db, port, order, "1m")
        assert db.query(LiveOrder).all() == []  # 不下单
    finally:
        db.close()
    assert any(
        r.levelno == logging.INFO for r in caplog.records
    ), "cap_quantity None 拦截应打 info 日志"


def test_handle_bar_refreshes_available_once_per_bar():
    """F5：多组合共享同一 bar 对象 → 每 bar 只刷一次 /positions（强引用去重）。"""
    factory, _ = _db_factory()
    port1, _ = _portfolio_single(strategy_id=1)
    port2, _ = _portfolio_single(strategy_id=2)
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    engine = LiveEngine(
        session_id=1, portfolios=[port1, port2], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    bar = _bar(stock, "9.3", bar_time)

    engine._handle_bar(port1, bar)
    engine._handle_bar(port2, bar)

    pos_reqs = [r for r in rec.requests if r.url.path == "/positions"]
    assert len(pos_reqs) == 1


# ---------------- 桥离线暂停 ----------------
def test_bridge_offline_pauses_no_trade():
    """dispatcher heartbeat=False → _loop 不下单、bridge_online=False、不抛。"""
    import asyncio

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    # /ping 也 fail → heartbeat False
    rec = _Recorder(fail_paths={"/ping", "/quote"})
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, poll_interval=0.01,
    )
    # 预置信号，若下单会落 trade（此处验证不下单）
    engine.signal_cache = {
        (1, stock, datetime(2026, 8, 5, 10, 0)): [{"name": "open_sig", "value": 1}]
    }

    async def run_a_bit():
        await engine.start()
        await asyncio.sleep(0.05)  # 跑几轮心跳
        await engine.stop()

    asyncio.run(run_a_bit())

    assert engine.bridge_online is False
    db = factory()
    assert db.query(LiveTrade).count() == 0
    db.close()


def test_handle_bar_bridge_unavailable_no_persist():
    """_handle_bar 下单时桥抛 BridgeUnavailableError → 先写 submitted 再标 rejected。

    切片5 时序：submitted 已先落库（I4 命门窗口闭合），发单桥异常 → 标 rejected、
    _bridge_online=False（上层心跳暂停下单），不写 LiveTrade、不 apply。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    # /order fail → place_order 抛 BridgeUnavailableError
    rec = _Recorder(fail_paths={"/order"})
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)

    engine._on_bar(bar)  # _on_bar 捕获 BridgeUnavailableError，不外抛

    assert engine.bridge_online is False
    db = factory()
    assert db.query(LiveTrade).count() == 0
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1
    assert orders[0].status == "rejected"
    assert orders[0].error_message == "bridge unavailable"
    assert ctx.positions[stock].quantity == 0  # 未 apply
    db.close()


# ---------------- 生命周期 ----------------
def test_start_stop_lifecycle():
    """start 起 loop 任务、stop 取消任务（短 poll_interval + mock poll）。"""
    import asyncio

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, poll_interval=0.01,
    )

    async def run():
        await engine.start()
        assert engine._task is not None
        await asyncio.sleep(0.03)
        await engine.stop()
        assert engine._task is None
        assert engine._running is False

    asyncio.run(run())


# ---------------- 公式信号注入（0010）----------------
from unittest.mock import MagicMock  # noqa: E402

from core.tq.formula import TQFormula  # noqa: E402


def _quote_bars(code, bar_times):
    """构造桥 /quote 返回的 bar dict 列表（字段同 xtdata：index/time/open/..）。"""
    bars = []
    for i, bt in enumerate(bar_times):
        bars.append({
            "index": bt.strftime("%Y%m%d%H%M%S"),
            "time": int(bt.timestamp() * 1000),
            "open": 9.0 + i * 0.01, "high": 9.3 + i * 0.01,
            "low": 9.0, "close": 9.2 + i * 0.01,
            "volume": 10000 + i, "amount": 92000.0 + i,
        })
    return bars


def _mock_dispatcher_with_quote(rec, code, bars):
    """让 _Recorder 的 /quote 返回固定 bars。"""
    def respond(request):
        if request.url.path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {code: bars}})
        if request.url.path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, json={"ok": False})
    rec._respond = respond


def _make_engine_with_formula(disp, factory, formula_by_strategy, formula_count=200):
    """构造带 tq_formula + formula_by_strategy 的 LiveEngine。"""
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        tq_formula=TQFormula(), formula_by_strategy=formula_by_strategy,
        formula_count=formula_count,
    )


def test_bars_to_formula_df_field_mapping():
    """桥 bar dict（index 时间 + 小写 OHLCV）→ {大写字段: DataFrame}，列=code，DatetimeIndex。"""
    bars = _quote_bars("600000.SH", [datetime(2026, 8, 5, 10, i) for i in range(3)])
    df = LiveEngine._bars_to_formula_df(bars, "600000.SH")
    # 六个字段齐全
    for field in ("Open", "High", "Low", "Close", "Volume", "Amount"):
        assert field in df
    # 每个都是单列 DataFrame（列=code）
    assert list(df["Close"].columns) == ["600000.SH"]
    # 行数 = bar 数
    assert len(df["Close"]) == 3
    # DatetimeIndex 升序
    idx = list(df["Close"].index)
    assert idx == sorted(idx)
    # 值正确映射（首 bar close=9.2）
    assert float(df["Close"].iloc[0, 0]) == 9.2


def test_bars_to_formula_df_empty_returns_none():
    """空 bar 列表 → None（无法注入）。"""
    assert LiveEngine._bars_to_formula_df([], "600000.SH") is None


def test_extract_latest_signal_takes_last_bar():
    """raw 多条输出 → 只取最后一条 bar 的 {var: value} → [{name, value}]。"""
    raw = {
        "ErrorId": "0",
        "600000.SH": {
            "open_sig": [
                {"Date": "202608051000", "Value": 0.0},
                {"Date": "202608051001", "Value": 1.0},
                {"Date": "202608051002", "Value": 0.0},
            ],
            "ma5": [
                {"Date": "202608051000", "Value": 9.1},
                {"Date": "202608051001", "Value": 9.2},
                {"Date": "202608051002", "Value": 9.3},
            ],
        },
    }
    outputs = LiveEngine._extract_latest_signal(raw, "600000.SH")
    # 取最后一条：open_sig=0, ma5=9.3
    out_map = {o["name"]: o["value"] for o in outputs}
    assert out_map["open_sig"] == 0
    assert out_map["ma5"] == 9


def test_extract_latest_signal_errorid_nonzero_returns_empty():
    """ErrorId 非 0/19 → 返回空列表（公式出错）。"""
    raw = {"ErrorId": "1", "Error": "compute fail"}
    assert LiveEngine._extract_latest_signal(raw, "600000.SH") == []


def test_extract_latest_signal_no_stock_returns_empty():
    """raw 中无该股票 → 空列表。"""
    raw = {"ErrorId": "0", "000001.SZ": {"x": [{"Value": 1.0}]}}
    assert LiveEngine._extract_latest_signal(raw, "600000.SH") == []


def test_fill_signal_cache_populates_cache():
    """mock query_quote 返回 bars + mock compute_injected 返回 raw → signal_cache 填入。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 2)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars(stock, [datetime(2026, 8, 5, 10, i) for i in range(3)])
    _mock_dispatcher_with_quote(rec, stock, bars)

    engine = _make_engine_with_formula(disp, factory, {1: "MACROSSPRO"})
    engine.portfolios = [port]
    # mock compute_injected：返回最后一条 open_sig=1
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        "ErrorId": "0",
        stock: {"open_sig": [
            {"Date": "202608051000", "Value": 0.0},
            {"Date": "202608051001", "Value": 0.0},
            {"Date": "202608051002", "Value": 1.0},
        ]},
    }
    bar = _bar(stock, "9.3", bar_time)

    engine._fill_signal_cache(port, bar)

    key = (1, stock, bar_time)
    assert key in engine.signal_cache
    out_map = {o["name"]: o["value"] for o in engine.signal_cache[key]}
    assert out_map["open_sig"] == 1


def test_fill_signal_cache_logs_inject_timing_summary(caplog):
    """fill_signal_cache 末尾打一条 INFO 汇总：注入 codes 数 + tq/fetch/total 耗时。

    每根 bar 每个有注入的组合一条（1m 每分钟一条），用于实测"每分钟公式花多少秒、
    是否随会话累积变慢"；tq= 为 tqcenter compute_injected 累计、fetch= 为拉桥累计，
    据此区分瓶颈在通达信公式还是桥。
    """
    import logging
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 2)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars(stock, [datetime(2026, 8, 5, 10, i) for i in range(3)])
    _mock_dispatcher_with_quote(rec, stock, bars)

    engine = _make_engine_with_formula(disp, factory, {1: "MACROSSPRO"})
    engine.portfolios = [port]
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        "ErrorId": "0",
        stock: {"open_sig": [{"Date": "202608051002", "Value": 1.0}]},
    }
    bar = _bar(stock, "9.3", bar_time)

    with caplog.at_level(logging.INFO, logger="core.engine.live.market_data"):
        engine._fill_signal_cache(port, bar)

    msgs = [r.message for r in caplog.records if "signal inject" in r.message]
    assert msgs, "expected INFO inject timing summary"
    m = msgs[-1]
    assert "codes=1" in m
    for token in ("tq=", "fetch=", "total="):
        assert token in m


def test_fill_signal_cache_skips_strategy_without_formula():
    """策略不在 formula_by_strategy → 跳过，不查 quote 不算公式。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_with_formula(disp, factory, {})  # 空：无策略映射
    engine.portfolios = [port]
    engine._tq_formula.compute_injected_batch =MagicMock(return_value=None)
    bar = _bar(stock, "9.3", bar_time)

    engine._fill_signal_cache(port, bar)

    assert engine.signal_cache == {}
    engine._tq_formula.compute_injected_batch.assert_not_called()


def test_fill_signal_cache_empty_bars_skipped():
    """query_quote 返回空 bars → _bars_to_formula_df None → 跳过该股票。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    _mock_dispatcher_with_quote(rec, stock, [])  # 空 bars
    engine = _make_engine_with_formula(disp, factory, {1: "MACROSSPRO"})
    engine.portfolios = [port]
    engine._tq_formula.compute_injected_batch =MagicMock(return_value=None)
    bar = _bar(stock, "9.3", bar_time)

    engine._fill_signal_cache(port, bar)

    engine._tq_formula.compute_injected_batch.assert_not_called()
    assert engine.signal_cache == {}


def test_handle_bar_with_formula_signal_triggers_trade():
    """_handle_bar 接入 _fill_signal_cache：mock 公式返回 open_sig=1 → BUY 落 submitted（非预置 cache）。

    切片5：公式信号触发发单 → LiveOrder(status=submitted) + signal_name=open_sig；
    不写 LiveTrade（回填确认 filled 才写）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 2)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars(stock, [datetime(2026, 8, 5, 10, i) for i in range(3)])
    _mock_dispatcher_with_quote(rec, stock, bars)

    engine = _make_engine_with_formula(disp, factory, {1: "MACROSSPRO"})
    engine.portfolios = [port]
    # mock compute_injected：最后一条 open_sig=1 → 触发 OPEN
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        "ErrorId": "0",
        stock: {"open_sig": [
            {"Date": "202608051000", "Value": 0.0},
            {"Date": "202608051001", "Value": 0.0},
            {"Date": "202608051002", "Value": 1.0},
        ]},
    }
    bar = _bar(stock, "9.3", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    trades = db.query(LiveTrade).all()
    assert len(trades) == 0
    # signal_name 来自公式信号 open_sig（非风控），status=submitted 待回填
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1
    assert orders[0].signal_name == "open_sig"
    assert orders[0].status == "submitted"
    assert ctx.positions[stock].quantity == 0  # 未 apply
    db.close()


# ===========================================================================
# 切片5：订单状态机 + /deals 回填（0011）
# ===========================================================================
def _mock_orders_deals(rec, orders=None, deals=None):
    """让 _Recorder 的 /orders 与 /deals 返回固定数据。"""
    orders = orders if orders is not None else []
    deals = deals if deals is not None else []

    def respond(request):
        path = request.url.path
        if path == "/orders":
            return httpx.Response(200, json={"ok": True, "data": orders})
        if path == "/deals":
            return httpx.Response(200, json={"ok": True, "data": deals})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})

    rec._respond = respond


def _make_engine(disp, factory, port):
    """构造 LiveEngine（含 poller）。dispatcher 由 _make_dispatcher 提供。"""
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )


# 测试用确定性 oid（模拟 Core 下发的 bridge_order_id，前 20 位 = m_strRemark）。
_TEST_OID = "a" * 32
_TEST_REMARK = _TEST_OID[:20]


def _submitted_order(db, quantity=600, status="submitted", bridge_order_id=_TEST_OID):
    """预置一笔 submitted 状态的 LiveOrder（模拟 _handle_bar 已发单）。

    bridge_order_id 默认 _TEST_OID：真实链路下单后必回写 oid，匹配器据此走 remark
    精确匹配。测遗留/模糊路径时显式传 None。
    """
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code="600000.SH", trade_type="buy", order_type="limit",
        price=Decimal("9.3"), quantity=quantity, filled_quantity=0,
        filled_price=None, status=status, signal_name="open_sig",
        signal_type="OPEN", bar_time=datetime(2026, 8, 5, 10, 0),
        bridge_order_id=bridge_order_id,
    )
    db.add(lo)
    db.flush()
    return lo


def test_persist_order_submitted_writes_status_submitted():
    """_persist_order_submitted → LiveOrder(status=submitted)，无 LiveTrade。"""
    factory, _ = _db_factory()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    port, _ = _portfolio_single()
    engine = _make_engine(disp, factory, port)
    order = OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000.SH",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=600, price=Decimal("9.3"), bar_time=datetime(2026, 8, 5, 10, 0),
        signal_name="open_sig",
    )
    db = factory()
    lo = engine._persist_order_submitted(db, order)
    lo_id = lo.id  # commit 前取 id（commit 后对象 expire，闭 session 前访问会 Detached）
    db.commit()
    db.close()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "submitted"
    assert lo2.filled_quantity == 0
    assert lo2.filled_price is None
    assert db.query(LiveTrade).count() == 0
    db.close()


def test_order_status_transitions_submitted_to_filled():
    """submitted → _poll_deals 回填 → filled，真实价/量/佣金写入 LiveTrade + apply。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    # /orders 返回 BRIDGE 委托（匹配 order_ref），/deals 返回真实成交
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-abc-123",
        "remark": _TEST_REMARK,
    }], deals=[{
        "order_ref": "ref-abc-123", "price": 9.25, "volume": 600,
        "amount": 5550.0, "commission": 1.39, "trade_time": "150001",
        "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "filled"
    assert lo2.order_ref == "ref-abc-123"
    assert lo2.filled_quantity == 600
    assert lo2.filled_price == Decimal("9.25")
    trades = db.query(LiveTrade).all()
    assert len(trades) == 1
    assert trades[0].price == Decimal("9.25")
    assert trades[0].quantity == 600
    assert trades[0].amount == Decimal("5550.0")
    assert trades[0].commission == Decimal("1.39")
    # filled 后 apply：持仓 600，现金扣 amount+commission
    assert ctx.positions[stock].quantity == 600
    assert port.account.cash == Decimal("100000") - (Decimal("5550.0") + Decimal("1.39"))
    db.close()


def test_order_status_transitions_partial_fill():
    """部分成交 → status=partial，写部分 LiveTrade，不 apply（等最终确认）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-partial",
        "remark": _TEST_REMARK,
    }], deals=[{
        "order_ref": "ref-partial", "price": 9.25, "volume": 300,
        "amount": 2775.0, "commission": 0.69, "trade_time": "150001",
        "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "partial"
    assert lo2.filled_quantity == 300
    trades = db.query(LiveTrade).all()
    assert len(trades) == 1
    assert trades[0].quantity == 300
    # partial 不 apply：持仓不变（该股无 Position 或 quantity==0）
    assert ctx.positions.get(stock) is None or ctx.positions[stock].quantity == 0
    assert port.account.cash == Decimal("100000")
    db.close()


def test_try_match_order_ref_ignores_gui_and_mismatch():
    """_try_match_order_ref：GUI 单/不匹配方向量的单被忽略，找不到 order_ref 留 None。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    # 只有 GUI 单 + 不匹配的 BRIDGE 单（量不符）
    _mock_orders_deals(rec, orders=[
        {"source": "GUI", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "gui-ref"},
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 999, "order_ref": "wrong-vol"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref is None  # 都不匹配
    db.close()


def test_try_match_order_ref_finds_bridge_order():
    """_try_match_order_ref 定位 BRIDGE 委托 → order_ref 回写。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-found",
        "remark": _TEST_REMARK,
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref == "ref-found"
    db.close()


def test_try_match_order_ref_clears_untraceable_marker():
    """迟到的 order_ref 追平后撤掉 bridge_order_untraceable 标记。

    在途闸口径统一的配套：180s 不可追溯只是「一次性放行」，不是永久豁免。
    该单若后来被 /orders 追平（确实在柜台在途），必须重新占用同股同向门，
    否则同一只票可能被重复报单。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-late",
        "remark": _TEST_REMARK,
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    lo.error_message = ("bridge_order_untraceable: 桥通道 180s 未匹配到柜台委托号"
                        "（仍在途，不判 rejected；已释放同股同向门）")
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref == "ref-late"
    assert lo.error_message is None
    db.close()


def test_try_match_order_ref_does_not_collide_same_code_dir_volume():
    """回归：同股票同向同量的多笔在途单，order_ref 匹配不得撞同一个 ref。

    实盘根因：1m 公式持续发 OPEN，连续多笔买单代码/方向/委托量完全相同，
    _try_match_order_ref 取第一个匹配项 → 多笔 LiveOrder 共用同一 order_ref →
    同一笔成交回填到所有单 → apply_trade 重复执行 → 虚拟持仓虚高。
    修复：候选按 insert_date+insert_time 取最新，且排除已被本 session 其他单占用的 ref。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    # 两笔同代码同向同量的 BRIDGE 委托，但 remark（= 各单 oid 前缀）不同：
    # remark 精确匹配天然把两单各认各的，不可能撞同一个 order_ref。
    oid1, oid2 = "b" * 32, "c" * 32
    _mock_orders_deals(rec, orders=[
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "ref-old",
         "remark": oid1[:20],
         "insert_date": "20260805", "insert_time": "095200"},
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "ref-new",
         "remark": oid2[:20],
         "insert_date": "20260805", "insert_time": "095300"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo1 = _submitted_order(db, quantity=600, bridge_order_id=oid1)
    lo2 = _submitted_order(db, quantity=600, bridge_order_id=oid2)
    db.commit()

    # 经 _poll_deals 一轮匹配（无成交，仅回写 order_ref）
    engine._poll_deals()

    db.refresh(lo1)
    db.refresh(lo2)
    assert lo1.order_ref is not None
    assert lo2.order_ref is not None
    assert lo1.order_ref != lo2.order_ref, "两笔在途单不得共用同一 order_ref"
    assert {lo1.order_ref, lo2.order_ref} == {"ref-old", "ref-new"}
    db.close()


def test_match_remark_exact_ignores_legacy_same_code_dir_volume():
    """真机回归（2026-08-13）：新单不得匹配到跨会话遗留的同代码同向同量旧单。

    场景：本单 11:21（bridge_order_id 已回写 → 走 remark 精确匹配），桥 /orders 里
    有一笔上个会话 09:55 的遗留单（同代码同向同量、无 remark）。旧逻辑按 code+dir+vol
    模糊匹配 + “取最新”，在真单尚未可查时把新单绑到旧单 → 回填错误成交、真单丢失。
    修复：remark 精确匹配只认 m_strRemark == oid 前缀的委托；无 remark 的遗留单被忽略。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        # 上个会话遗留单：同代码同向同量、无 remark，时间早于本单
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "ref-legacy",
         "insert_date": "20260813", "insert_time": "095500"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)  # bridge_order_id=_TEST_OID
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)  # UTC = 北京 11:21
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref is None  # 遗留单无 remark，绝不绑定
    db.close()


def test_match_remark_exact_binds_correct_order_among_similar():
    """remark 精确匹配在多笔相似委托中认领到本单（即便委托量不同也能认到）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        # 别的单的委托（带它自己的 remark），代码方向相同但量不同
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 999, "order_ref": "ref-other",
         "remark": "z" * 20},
        # 本单的委托：remark 命中
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "ref-mine",
         "remark": _TEST_REMARK},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref == "ref-mine"
    db.close()


def test_match_legacy_fuzzy_excludes_candidate_before_creation():
    """遗留单（无 bridge_order_id）走模糊匹配，但 insert 早于本单创建的候选必须排除。

    真机 bug 的等价场景：新单 11:21 创建时，/orders 里只有上个会话 09:55 的同代码同向
    同量遗留单（真单尚未可查）。旧逻辑“取最新匹配项”会把新单绑到这笔旧单。时间窗要求
    候选 insert >= 本单 created_at（容差），故此时 order_ref 留 None，下轮真单出现再绑。
    insert_date/time 是 Asia/Shanghai 本地时间，created_at 是 UTC naive，匹配器内部换算。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        # 太早：本地 09:55（= UTC 01:55）< 本单 UTC 03:21 → 排除，不绑定
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "ref-too-old",
         "insert_date": "20260813", "insert_time": "095500"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=None)  # 遗留单，无 remark
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)  # UTC = 北京 11:21
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref is None  # 早于本单的遗留单绝不绑定
    db.close()


def test_match_legacy_fuzzy_binds_recent_candidate():
    """遗留单模糊匹配：insert 晚于本单创建（容差内）的候选正常绑定。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        # 本地 11:22（= UTC 03:22）>= 本单 UTC 03:21 - 容差 → 命中
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "ref-recent",
         "insert_date": "20260813", "insert_time": "112200"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=None)
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)  # UTC = 北京 11:21
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref == "ref-recent"
    db.close()


def test_match_legacy_fuzzy_claims_unique_smaller_volume_and_records_actual():
    """P0-1 放宽：柜台委托量(300) < 审批量(600) 且是唯一候选 → 认领，
    并按实际委托量改写订单量（后续 backfill 可按 300 判 filled 记账）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 300, "order_ref": "ref-clamped",
         "insert_date": "20260813", "insert_time": "112200"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=None)
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)  # UTC = 北京 11:21
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref == "ref-clamped"
    assert lo.quantity == 300  # 认领成功按柜台真实委托量记录
    db.close()


def test_match_legacy_fuzzy_rejects_ambiguous_smaller_volumes():
    """多个「更小量」候选 → 歧义，不认领（保持原失败语义），委托量不变。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 300, "order_ref": "ref-a",
         "insert_date": "20260813", "insert_time": "112200"},
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 200, "order_ref": "ref-b",
         "insert_date": "20260813", "insert_time": "112300"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=None)
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref is None
    assert lo.quantity == 600
    db.close()


def test_match_legacy_fuzzy_rejects_volume_greater_than_target():
    """柜台委托量 > 审批量 → 不认领（放宽只接受 ≤ 目标量）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 700, "order_ref": "ref-big",
         "insert_date": "20260813", "insert_time": "112200"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=None)
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref is None
    assert lo.quantity == 600
    db.close()


def test_match_legacy_fuzzy_strict_volume_wins_over_smaller_candidate():
    """存在数量完全相等的候选时优先严格匹配（放宽只在无严格候选时生效）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "ref-strict",
         "insert_date": "20260813", "insert_time": "112200"},
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 300, "order_ref": "ref-small",
         "insert_date": "20260813", "insert_time": "112300"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=None)
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref == "ref-strict"
    assert lo.quantity == 600
    db.close()


def test_poll_deals_expires_stale_order_without_order_ref():
    """order_ref 始终匹配不到（桥 /orders 无此单）的陈旧 submitted 单 → 保持 pending + risk。

    HTTP 桥通道（supports_remark=True）：柜台当日委托查询不含废单、桥重启丢内存台账，
    无法追溯时不自动驳回，仅记 bridge_order_untraceable 一次性 risk 事件供人工核对。fresh 单不动。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])  # 桥侧无任何匹配委托
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    stale = _submitted_order(db, quantity=600)
    # 5 分钟：已超 180s 匹配窗但未到 600s 超长兜底窗 → 保持 pending（只告警不收口）
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=5)
    fresh = _submitted_order(db, quantity=700)
    fresh.created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    stale2 = db.get(LiveOrder, stale.id)
    fresh2 = db.get(LiveOrder, fresh.id)
    # HTTP 桥（supports_remark=True）：无法追溯时保持 pending，绝不自动驳回
    assert stale2.status == "submitted"
    assert stale2.order_ref is None
    assert (stale2.error_message or "").startswith("bridge_order_untraceable:")
    assert fresh2.status == "submitted"  # 未超时不动
    pending = db.query(LiveOrder).filter(
        LiveOrder.live_session_id == 1,
        LiveOrder.status.in_(["submitted", "partial"]),
    ).all()
    assert {o.id for o in pending} == {stale.id, fresh.id}
    db.close()


def test_poll_deals_backfills_via_remark_when_order_ref_missing():
    """order_ref 匹配不上（/orders 实时表已无此单）但 /deals 有 remark 匹配成交 → 回填。

    真机 2026-08-19 id45/46 根因：QMT get_trade_detail_data(ORDER) 对已成交单不可靠
    （成交后从 ORDER 实时表移除），Core 轮询 /orders 拿不到 order_ref → 旧逻辑走到超时
    rejected 且成交不回填。但 /deals（DEAL 表）保留全部已成交记录，且 DEAL 对象也带
    m_strRemark。本测试验证修复 A：order_ref 匹配失败的单，按 bridge_order_id[:20] 在
    /deals 直连 remark 匹配回填，绕过 order_ref。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    # /orders 不返回本单（模拟成交后已从 ORDER 实时表移除 → order_ref 永远匹配不上）
    # /deals 返回本单成交，remark = bridge_order_id[:20]（DEAL 对象带 m_strRemark）
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ref-never-matched",  # 真实 order_ref，但 Core 拿不到（/orders 无此单）
        "remark": _TEST_REMARK,            # 按 remark 直连匹配
        "price": 9.25, "volume": 600,
        "amount": 5550.0, "commission": 1.39,
        "trade_time": "150001", "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)  # bridge_order_id=_TEST_OID, remark=_TEST_REMARK
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    # 修复 A：order_ref 匹配不上，但按 remark 从 /deals 直连回填 → filled
    assert lo2.status == "filled"
    assert lo2.filled_quantity == 600
    assert lo2.filled_price == Decimal("9.25")
    trades = db.query(LiveTrade).all()
    assert len(trades) == 1
    assert trades[0].quantity == 600
    assert trades[0].amount == Decimal("5550.0")
    # filled 后 apply：持仓 600，现金扣 amount+commission（同正常回填路径）
    assert ctx.positions[stock].quantity == 600
    assert port.account.cash == Decimal("100000") - (Decimal("5550.0") + Decimal("1.39"))
    db.close()


def test_poll_deals_remark_backfill_partial_then_filled_across_rounds():
    """慢成交单：第1轮 /deals 只有部分成交(partial) → 第2轮补齐 → filled。

    真机 id45 场景：限价单 17 分钟才补完，前 4800 股先成交。修复 A 让每轮按 remark
    从 /deals 回填，partial 不 apply，补齐后 filled + apply。不依赖 order_ref、不超时。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    # /orders 始终无此单；/deals 第1轮 300 股，第2轮补到 600
    deals_round1 = [{
        "order_ref": "ref-slow", "remark": _TEST_REMARK,
        "price": 9.25, "volume": 300, "amount": 2775.0, "commission": 0.69,
        "trade_time": "150001", "trade_date": "20260805",
    }]
    deals_round2 = [{
        "order_ref": "ref-slow", "remark": _TEST_REMARK,
        "price": 9.25, "volume": 600, "amount": 5550.0, "commission": 1.39,
        "trade_time": "150002", "trade_date": "20260805",
    }]
    _mock_orders_deals(rec, orders=[], deals=deals_round1)
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()
    db.close()

    # 第1轮：partial（300/600），不 apply
    engine._poll_deals()
    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "partial"
    assert lo2.filled_quantity == 300
    assert ctx.positions.get(stock) is None or ctx.positions[stock].quantity == 0
    db.close()

    # 第2轮：/deals 补齐到 600 → filled + apply
    _mock_orders_deals(rec, orders=[], deals=deals_round2)
    engine._poll_deals()
    db = factory()
    lo3 = db.get(LiveOrder, lo.id)
    assert lo3.status == "filled"
    assert lo3.filled_quantity == 600
    assert ctx.positions[stock].quantity == 600
    db.close()


def test_poll_deals_bridge_offline_skips():
    """query_deals 抛 BridgeUnavailableError → 本轮跳过，状态不变，下轮重试。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    # /orders 匹配成功（order_ref 回写），但 /deals 桥不可用（ConnectError）
    rec = _Recorder(fail_paths={"/deals"})
    orders_data = [{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-offline",
        "remark": _TEST_REMARK,
    }]

    def respond(request):
        path = request.url.path
        if path == "/orders":
            return httpx.Response(200, json={"ok": True, "data": orders_data})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})

    rec._respond = respond
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "submitted"  # 未回填，下轮重试
    assert db.query(LiveTrade).count() == 0
    db.close()


def test_recover_finds_pending_orders():
    """recover 挂回 submitted/partial，忽略 filled/rejected。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    _submitted_order(db, quantity=600, status="submitted")
    _submitted_order(db, quantity=600, status="partial")
    _submitted_order(db, quantity=600, status="filled")
    _submitted_order(db, quantity=600, status="rejected")
    db.commit()
    db.close()

    db = factory()
    engine.recover(db)
    db.close()

    assert len(engine._pending_orders) == 2  # 只有 submitted + partial


# ===========================================================================
# G7：桥状态并入 session API（0011 §5.11）
# ===========================================================================
def test_handle_bar_tracks_pending_orders():
    """_handle_bar 发单后该单计入 _pending_orders（G7 pending 计数）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)

    engine._handle_bar(port, bar)

    assert engine.pending_orders_count == 1  # 发单后在途
    db = factory()
    assert db.query(LiveOrder).count() == 1
    db.close()


# ---------------- 跨重启去重门（D6：BUY 同 bar_time 已有未完结单则跳过）----------------
def _seed_buy_submitted(db, bar_time, status="submitted", portfolio_strategy_id=1, strategy_id=1, trade_type="buy", stock_code="600000.SH"):
    """预置一笔 LiveOrder（模拟重启前同一 bar 已下过的单）。trade_type 可 buy/sell。"""
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=portfolio_strategy_id, strategy_id=strategy_id,
        stock_code=stock_code, trade_type=trade_type, order_type="limit",
        price=Decimal("9.3"), quantity=600, filled_quantity=0,
        filled_price=None, status=status, signal_name="open_sig",
        signal_type="OPEN", bar_time=bar_time,
    )
    db.add(lo)
    db.flush()
    return lo


def test_handle_bar_dup_buy_same_bar_time_skipped():
    """D6：BUY 在同一 bar_time 已有 submitted LiveOrder → 重启后重驱到同一 bar 不重复下单。

    根因：页面停止/再启动 = Core 重启 = 新 LiveEngine 实例内存丢失，1d/1w/1mon
    被重新驱动到同一 daily_time，且 order_id=live_order.id（自增 PK）每次新号，
    桥侧 _placed 幂等无效。按 (组合/股票/bar_time/buy) 去重 → 重启不重复。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：重启前该 bar 已下过 BUY（submitted）
    db = factory()
    _seed_buy_submitted(db, bar_time, status="submitted")
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    # 仍只有预置的那一笔，未新增
    assert db.query(LiveOrder).count() == 1
    # 桥未收到新的 /order 请求
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 0
    db.close()


def test_handle_bar_dup_buy_different_bar_time_allowed():
    """在途单门安全侧：上一单已确认（filled）→ 不同 bar_time 的 BUY 不被误杀。

    bar_time 本身不是拦截依据——分钟策略日内多 bar 多次开仓正常。
    在途单门只拦「同股同向未确认」单（submitted/partial）；上一单已成交（filled）
    或已拒（rejected）→ 门释放，不同 bar_time 的信号照常下单。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time_first = datetime(2026, 8, 5, 10, 0)
    bar_time_next = datetime(2026, 8, 5, 10, 5)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：10:00 的 BUY 已成交回填（filled）——在途集合外，不拦 10:05
    db = factory()
    _seed_buy_submitted(db, bar_time_first, status="filled")
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    # 10:05 的信号（不同 bar_time）
    engine.signal_cache = {(1, stock, bar_time_next): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time_next)

    engine._handle_bar(port, bar)

    db = factory()
    # 预置 1 笔 + 新增 1 笔 = 2 笔
    assert db.query(LiveOrder).count() == 2
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 1  # 桥收到新单
    db.close()


def test_handle_bar_sell_not_blocked_by_existing_buy():
    """D6 跨方向：SELL 不被同 bar 的 BUY 单拦截——方向是去重键的一部分。

    同 bar 已有 BUY 单不影响同 bar 的 SELL（不同 trade_type 不匹配）。
    平仓与开仓各自独立去重，互不干扰。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(strategy_id=1, period="1m")
    ctx.formula_signals = [
        {"signal_name": "close_sig", "signal_type": SignalType.CLOSE, "trigger_value": -1},
    ]
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    # 虚拟持仓 600（可卖）
    ctx.positions[stock] = Position(stock)
    ctx.positions[stock].buy(600, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))

    def respond(request):
        path = request.url.path
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [
                {"instrument": "600000", "exchange": "SH",
                 "available": 600, "volume": 600,
                 "yesterday_volume": 600, "on_road_volume": 0},
            ]})
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：同 bar 已有一笔 BUY submitted（不该拦住 SELL——方向不同）
    db = factory()
    _seed_buy_submitted(db, bar_time, status="submitted")
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "close_sig", "value": -1}]}
    bar = _bar(stock, "9.0", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    # 预置 1 笔 BUY + 新增 1 笔 SELL
    assert len(orders) == 2
    sell_orders = [o for o in orders if o.trade_type == "sell"]
    assert len(sell_orders) == 1
    assert sell_orders[0].status == "submitted"
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 1  # SELL 单发到桥
    db.close()


def test_handle_bar_dup_sell_same_bar_time_skipped():
    """D6 对称：SELL 在同一 bar_time 已有未完结单 → 重启后重驱到同一 bar 不重复卖。

    重复 SELL 危害不亚于重复 BUY：已卖完再卖 → 桥超卖拒单噪音 / 持仓误判。
    BUY/SELL 对称去重，按 (策略+股票+bar_time+方向) 拦同方向重驱。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(strategy_id=1, period="1m")
    ctx.formula_signals = [
        {"signal_name": "close_sig", "signal_type": SignalType.CLOSE, "trigger_value": -1},
    ]
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    # 虚拟持仓 600（可卖）
    ctx.positions[stock] = Position(stock)
    ctx.positions[stock].buy(600, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))

    def respond(request):
        path = request.url.path
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [
                {"instrument": "600000", "exchange": "SH",
                 "available": 600, "volume": 600,
                 "yesterday_volume": 600, "on_road_volume": 0},
            ]})
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：重启前该 bar 已下过 SELL（submitted）
    db = factory()
    _seed_buy_submitted(db, bar_time, status="submitted", trade_type="sell")
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "close_sig", "value": -1}]}
    bar = _bar(stock, "9.0", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    # 仍只有预置的那一笔 SELL，未新增
    assert db.query(LiveOrder).count() == 1
    # 桥未收到新的 /order 请求
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 0
    db.close()


def test_handle_bar_buy_blocked_by_inflight_buy_next_bar():
    """在途单门：同 (策略,股票) 已有 submitted 买单 → 下一根 bar 的 OPEN 被压掉。

    根因（真机 2026-08-13，159888/159929/159936）：下单→/deals 成交回填之间存在
    在途窗口，期间虚拟持仓仍是 0，下一根连续 bar 的 OPEN 信号看持仓=0 以为可开仓
    → 同股同向重复买单（信号却记成 OPEN）。在途单门按「同股同向未确认单
    （submitted/partial）」拦截；回填确认后由 portfolio.py 持仓守卫接管。
    bar_time 不同不构成放行理由——不同 bar 只是两次信号时点，上一单未确认就再买
    仍是重复。方向同向才拦：在途 BUY 不拦 SELL。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time_first = datetime(2026, 8, 5, 10, 0)
    bar_time_next = datetime(2026, 8, 5, 10, 5)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：10:00 已下过 BUY，仍在途（submitted，成交未回填）
    db = factory()
    _seed_buy_submitted(db, bar_time_first, status="submitted")
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    # 10:05 的信号（不同 bar_time，但上一单还在途）
    engine.signal_cache = {(1, stock, bar_time_next): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time_next)

    engine._handle_bar(port, bar)

    db = factory()
    # 仍只有预置的那一笔，未新增
    assert db.query(LiveOrder).count() == 1
    # 桥未收到新的 /order 请求
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 0
    db.close()


def test_handle_bar_sell_blocked_by_inflight_sell_next_bar():
    """在途单门对称：同 (策略,股票) 已有 submitted 卖单 → 下一根 bar 的 SELL 被压掉。

    与 BUY 同理（用户要求卖出同样处理）：上一笔 SELL 未回填前虚拟持仓未减，
    下一根 bar 的 CLOSE 信号看到持仓仍满 → 同量再卖 → 超卖/桥拒单噪音。
    方向同向才拦：在途 SELL 不拦 BUY、在途 BUY 不拦 SELL。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(strategy_id=1, period="1m")
    ctx.formula_signals = [
        {"signal_name": "close_sig", "signal_type": SignalType.CLOSE, "trigger_value": -1},
    ]
    stock = "600000.SH"
    bar_time_first = datetime(2026, 8, 5, 10, 0)
    bar_time_next = datetime(2026, 8, 5, 10, 5)
    # 虚拟持仓 600（可卖）
    ctx.positions[stock] = Position(stock)
    ctx.positions[stock].buy(600, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))

    def respond(request):
        path = request.url.path
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [
                {"instrument": "600000", "exchange": "SH",
                 "available": 600, "volume": 600,
                 "yesterday_volume": 600, "on_road_volume": 0},
            ]})
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：10:00 已下过 SELL，仍在途
    db = factory()
    _seed_buy_submitted(db, bar_time_first, status="submitted", trade_type="sell")
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time_next): [{"name": "close_sig", "value": -1}]}
    bar = _bar(stock, "9.0", bar_time_next)

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    # 仍只有预置的那一笔 SELL，未新增
    assert len(orders) == 1
    assert orders[0].trade_type == "sell"
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 0
    db.close()


def test_handle_bar_sell_allowed_when_inflight_untraceable():
    """在途闸口径统一（2026-11-10）：已打 bridge_order_untraceable 标记的单不再占
    同股同向门 → 止损/平仓卖单不被桥通道的 600s 账务窗口压住（最多压 180s）。

    该单仍是 submitted（不判 rejected、迟到成交仍回填纠正），只是不挡新单。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(strategy_id=1, period="1m")
    ctx.formula_signals = [
        {"signal_name": "close_sig", "signal_type": SignalType.CLOSE, "trigger_value": -1},
    ]
    stock = "600000.SH"
    bar_time_first = datetime(2026, 8, 5, 10, 0)
    bar_time_next = datetime(2026, 8, 5, 10, 5)
    ctx.positions[stock] = Position(stock)
    ctx.positions[stock].buy(600, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))

    def respond(request):
        path = request.url.path
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [
                {"instrument": "600000", "exchange": "SH",
                 "available": 600, "volume": 600,
                 "yesterday_volume": 600, "on_road_volume": 0},
            ]})
        if path in ("/order", "/ping"):
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：10:00 已下过 SELL，180s 后仍未拿到 order_ref → 已打不可追溯标记
    db = factory()
    lo = _seed_buy_submitted(db, bar_time_first, status="submitted", trade_type="sell")
    lo.error_message = (
        "bridge_order_untraceable: 桥通道 180s 未匹配到柜台委托号（仍在途，"
        "不判 rejected；已释放同股同向门）"
    )
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time_next): [{"name": "close_sig", "value": -1}]}
    bar = _bar(stock, "9.0", bar_time_next)

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 2  # 不可追溯单（仍 submitted）+ 新止损卖单
    assert sorted(o.trade_type for o in orders) == ["sell", "sell"]
    assert {o.status for o in orders if o.id != lo.id} == {"submitted"}
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 1
    db.close()


def test_handle_bar_buy_allowed_after_inflight_rejected():
    """在途单门释放：上一单 rejected（桥拒单/离线）→ 门不拦，新 OPEN 正常下单。

    status=rejected 不在在途集合（submitted/partial）：被拒的单不会成交建仓，
    必须允许重试，否则同一股票会被一纸拒单卡死无法再买。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    bar_time_first = datetime(2026, 8, 5, 10, 0)
    bar_time_next = datetime(2026, 8, 5, 10, 5)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：10:00 的 BUY 已被桥拒（rejected）
    db = factory()
    _seed_buy_submitted(db, bar_time_first, status="rejected")
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time_next): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time_next)

    engine._handle_bar(port, bar)

    db = factory()
    # 预置 1 笔（rejected）+ 新增 1 笔 = 2 笔
    assert db.query(LiveOrder).count() == 2
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 1  # 桥收到新单
    db.close()


def test_handle_bar_dup_buy_different_strategy_same_bar_allowed():
    """D6 安全侧：同组合/同股票/同 bar，不同策略各自下单不被误杀。

    去重键含 strategy_id：主从策略 / 多周期策略可能同 bar 同股各自开仓，
    仅 (组合+股票+bar) 去重会误拦第二个策略。必须加 strategy_id 才精确到
    「同一策略重驱到同一 bar」这一真正的重复场景。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "1m"))  # 同组合两策略，均 1m
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)

    # 预置：策略 1 该 bar 已下过 BUY
    db = factory()
    _seed_buy_submitted(db, bar_time, status="submitted", strategy_id=1)
    db.commit()
    db.close()

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    # 两个策略同 bar 都触发信号
    engine.signal_cache = {
        (1, stock, bar_time): [{"name": "open_sig", "value": 1}],
        (2, stock, bar_time): [{"name": "open_sig", "value": 1}],
    }
    bar = _bar(stock, "9.3", bar_time)  # period=None → 两策略都处理

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    # 预置 1 笔（策略1）+ 新增 1 笔（策略2）；策略1 重复被跳过，策略2 放行
    assert sorted(o.strategy_id for o in orders) == [1, 2]
    placed = [r for r in rec.requests if r.url.path == "/order"]
    assert len(placed) == 1  # 仅策略2 发单
    db.close()


def test_poll_deals_syncs_pending_orders_from_db():
    """未成交的单：_poll_deals 后仍在 pending（计数 1）。

    last_backfill_time 语义自 2026-11-06 起改为「本轮 /deals 成功查询时点」
    （0 条成交也写，用于判读轮询是否在跑），故此处不再是 None。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])  # 无匹配 → 保持 pending
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    _submitted_order(db, quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    assert engine.pending_orders_count == 1
    assert engine.last_backfill_time is not None


def test_backfill_updates_last_backfill_time_and_clears_pending():
    """_poll_deals 回填 filled 后：last_backfill_time 更新，pending 计数清零（G7）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-abc-123",
        "remark": _TEST_REMARK,
    }], deals=[{
        "order_ref": "ref-abc-123", "price": 9.25, "volume": 600,
        "amount": 5550.0, "commission": 1.39, "trade_time": "150001",
        "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    assert engine.last_backfill_time is not None
    assert engine.pending_orders_count == 0  # filled 后不在途
    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "filled"
    db.close()


def test_external_deals_are_backfilled_without_pending_orders():
    """柜台侧人工成交（无平台在途单）→ 补记 LiveOrder/LiveTrade 留痕，不 apply 账本。

    2026-11-06 实证：8790 手工买卖两笔 bridge flow 已成交、平台成交/委托列表 0 条。
    旧 _poll_deals 在无在途单时直接 return，连 /deals 都不查，外部成交永久丢失。
    记账口径（二次修正）：只留痕；持仓数量/成本由 60s 桥快照镜面覆盖，避免
    「桥持仓已含该成交 + 平台再 apply」的双计（实测 000001 虚拟=200/实际=100）。
    方向：股票 m_nDirection 恒为 48，必须用 offset_flag/side 判定买卖。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    ctx.positions["601827.SH"] = Position("601827.SH")
    ctx.positions["601827.SH"].buy(5400, Decimal("7.90"))
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        # 买单：新代码（桥行只有 instrument/exchange，无 symbol）
        "order_ref": "ext-buy-1", "trade_id": "T-BUY-1",
        "instrument": "000001", "exchange": "SZ",
        "direction": 48, "direction_raw": 48, "offset_flag": 48, "side": "BUY",
        "price": 11.83, "volume": 100, "amount": 1183.0,
        "commission": 5.0, "trade_time": "145614", "trade_date": "20261106",
        "remark": "t-buy-0001",
    }, {
        # 卖单：已导入底仓（direction 原值同为 48，靠 offset_flag=49 判卖）
        "order_ref": "ext-sell-1", "trade_id": "T-SELL-1",
        "instrument": "601827", "exchange": "SH",
        "direction": 49, "direction_raw": 48, "offset_flag": 49, "side": "SELL",
        "price": 7.98, "volume": 100, "amount": 798.0,
        "commission": 5.0, "trade_time": "145620", "trade_date": "20261106",
        "remark": "t-sell-0001",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)

    engine._poll_deals()

    assert engine.last_backfill_time is not None  # 轮询存活标记（无在途单也写）
    db = factory()
    orders = db.query(LiveOrder).filter(LiveOrder.live_session_id == 1).all()
    assert {o.order_type for o in orders} == {"external"}
    assert {o.signal_name for o in orders} == {"external_deal"}
    assert all((o.error_message or "").startswith("bridge_unmatched:")
               for o in orders)
    assert {(o.stock_code, o.trade_type, o.status) for o in orders} == {
        ("000001.SZ", "buy", "filled"), ("601827.SH", "sell", "filled")}
    trades = db.query(LiveTrade).all()
    assert {(t.stock_code, t.trade_type, t.quantity) for t in trades} == {
        ("000001.SZ", "buy", 100), ("601827.SH", "sell", 100)}
    db.close()
    # 只留痕：虚拟账本数量不变（数量/成本交给桥快照镜面覆盖，不重复计数）
    assert ctx.positions["601827.SH"].quantity == 5400
    assert "000001.SZ" not in ctx.positions


def test_external_deal_updates_strategy_attribution_only():
    """外部成交只更新 strategy_cost 归因：数量不变、风控成本不被反推。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    ctx.positions["000001.SZ"] = Position("000001.SZ")  # 0 股占位（等桥快照导入）
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ext-buy-1", "trade_id": "T-BUY-1",
        "instrument": "000001", "exchange": "SZ",
        "direction": 48, "offset_flag": 48, "side": "BUY",
        "price": 11.83, "volume": 100, "amount": 1183.0,
        "commission": 5.0, "trade_time": "145614", "trade_date": "20261106",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)

    engine._poll_deals()

    pos = ctx.positions["000001.SZ"]
    assert pos.quantity == 0                                     # 数量不动
    assert pos.avg_cost == Decimal("0")                          # 风控成本不反推
    assert pos.strategy_quantity == 100
    assert pos.strategy_cost == Decimal("11.88")                 # (1183 + 5) / 100


def test_external_deal_unknown_direction_is_skipped_with_warning(caplog):
    """方向不可判定（旧桥只有 direction=48，side/offset_flag 缺失）→ 不默认买入。"""
    import logging

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ext-unknown-1", "trade_id": "T-U-1",
        "instrument": "601827", "exchange": "SH", "direction": 48,
        "price": 7.98, "volume": 100, "amount": 798.0,
        "trade_time": "145620", "trade_date": "20261106",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)

    with caplog.at_level(logging.WARNING):
        engine._poll_deals()

    db = factory()
    assert db.query(LiveOrder).count() == 0
    assert db.query(LiveTrade).count() == 0
    db.close()
    assert any("direction unresolved" in r.getMessage() for r in caplog.records)


def test_external_deal_direction_uses_offset_flag_without_side():
    """方向判定以 m_nOffsetFlag 为准（QMT 官方文档：股票 m_nDirection 恒为 48）。

    桥行缺 side 时，direction=48 + offset_flag=49 必须判成卖（49~54=卖），
    不能因为 direction 是 48 就当买入，也不能当「未知方向」丢掉。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    ctx.positions["601827.SH"] = Position("601827.SH")
    ctx.positions["601827.SH"].buy(5400, Decimal("7.90"))
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ext-offset-sell-1", "trade_id": "T-OF-1",
        "instrument": "601827", "exchange": "SH",
        "direction": 48, "direction_raw": 48, "offset_flag": 49,  # 无 side
        "price": 7.98, "volume": 100, "amount": 798.0,
        "trade_time": "145620", "trade_date": "20261110",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)

    engine._poll_deals()

    db = factory()
    orders = db.query(LiveOrder).filter(LiveOrder.order_type == "external").all()
    assert [(o.stock_code, o.trade_type, o.status) for o in orders] == [
        ("601827.SH", "sell", "filled")]
    trades = db.query(LiveTrade).all()
    assert [(t.stock_code, t.trade_type, t.quantity) for t in trades] == [
        ("601827.SH", "sell", 100)]
    db.close()
    assert ctx.positions["601827.SH"].quantity == 5400  # 只留痕，不 apply


def test_external_deal_unknown_direction_logs_once_then_aggregates(caplog):
    """缺陷4 刷屏抑制：同一 (code, direction, 委托号) 只 ERROR 一次，之后按分钟聚合。

    48 不映射（QMT 原始 m_nDirection 对股票/ETF 买卖都回 48，无法判方向），
    首次 ERROR + 一次性 risk 事件给人工路径，后续轮询不再逐条刷屏。
    """
    import logging

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ext-48-1", "trade_id": "T-48-1",
        "instrument": "510050", "exchange": "SH", "direction": 48,
        "price": 3.05, "volume": 1000, "amount": 3050.0,
        "trade_time": "093000", "trade_date": "20261110",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    machine = engine._order_machine
    clock = [1000.0]
    machine._unknown_deal_dir_clock = lambda: clock[0]

    with caplog.at_level(logging.WARNING):
        engine._poll_deals()
        engine._poll_deals()
        engine._poll_deals()

    errors = [r for r in caplog.records if "external deal skipped" in r.getMessage()]
    assert len(errors) == 1  # 只记一次（后续两轮静默计数）
    assert machine._unknown_deal_dirs[("510050.SH", "48", "ext-48-1")]["count"] == 3
    risks = []
    while not q.empty():
        ev = q.get_nowait()
        if ev.get("type") == "risk":
            risks.append(ev)
    assert len(risks) == 1
    assert risks[0]["rule"] == "external_deal_unknown_direction"
    assert "510050.SH" in risks[0]["message"]
    # 一分钟后再刷到 → 只出一条聚合计数，不再逐条
    clock[0] += 61
    with caplog.at_level(logging.WARNING):
        engine._poll_deals()
    agg = [r for r in caplog.records if "external deal still unresolved" in r.getMessage()]
    assert len(agg) == 1
    assert "4 occurrences" in agg[0].getMessage()
    db = factory()
    assert db.query(LiveOrder).count() == 0  # 始终不入账（不猜方向）
    db.close()


def test_external_deals_are_idempotent_across_polls():
    """重复轮询同一批 /deals 不得重复入库（幂等键 ext:<order_ref>）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ext-buy-1", "trade_id": "T-BUY-1",
        "instrument": "000001", "exchange": "SZ",
        "direction": 48, "offset_flag": 48, "side": "BUY",
        "price": 11.83, "volume": 100, "amount": 1183.0,
        "trade_time": "145614", "trade_date": "20261106",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)

    engine._poll_deals()
    engine._poll_deals()
    engine._poll_deals()

    db = factory()
    assert db.query(LiveOrder).filter(LiveOrder.live_session_id == 1).count() == 1
    assert db.query(LiveTrade).count() == 1
    db.close()
    # 只留痕：不创建/累加虚拟持仓（数量以桥快照为准）
    assert "000001.SZ" not in engine.ctx.portfolios[0].strategies[0].positions


def test_external_deal_without_attribution_is_skipped_with_warning(caplog):
    """无任何组合可归属 → 不伪造外键，只告警跳过。"""
    import logging

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.strategies.clear()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ext-buy-1", "trade_id": "T-BUY-1",
        "instrument": "000001", "exchange": "SZ",
        "direction": 48, "offset_flag": 48, "side": "BUY",
        "price": 11.83, "volume": 100, "amount": 1183.0,
        "trade_time": "145614", "trade_date": "20261106",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)

    with caplog.at_level(logging.WARNING):
        engine._poll_deals()

    db = factory()
    assert db.query(LiveOrder).filter(LiveOrder.live_session_id == 1).count() == 0
    db.close()
    assert any("external deal not attributable" in r.getMessage()
               for r in caplog.records)


# ===========================================================================
# C6 三段式实盘周期链路 + E8 离线恢复（0011 切片5 定案）
# ===========================================================================
from core.engine.live_engine import periods_on_boundary  # noqa: E402


def _make_engine_formula_portfolio(disp, factory, port, formula_by_strategy,
                                   formula_count=200, formula_count_by_name=None):
    """构造带 tq_formula + formula_by_strategy 且含组合的 LiveEngine（C6 注入/分发用）。"""
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        tq_formula=TQFormula(), formula_by_strategy=formula_by_strategy,
        formula_count=formula_count, formula_count_by_name=formula_count_by_name,
    )


def _respond_quote_bars(stock, bars, fail_orders=False):
    """respond：/quote 固定返回 bars，/order 受理成功。"""
    def respond(request):
        path = request.url.path
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {stock: bars}})
        if path == "/order":
            if fail_orders:
                raise httpx.ConnectError("connection refused")
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    return respond


# ---- 周期边界判定 ----
def test_periods_on_boundary():
    """C6：1m bar 结束时刻 → 命中边界周期（可累积，只读 stime 不引本机时钟）。

    1h 边界 = 原生 60m bar / 聚合桶的终点 10:30、11:30、14:00、15:00。
    """
    assert periods_on_boundary(datetime(2026, 8, 5, 10, 5)) == ["5m"]
    assert periods_on_boundary(datetime(2026, 8, 5, 10, 15)) == ["5m", "15m"]
    assert periods_on_boundary(datetime(2026, 8, 5, 10, 30)) == ["5m", "15m", "30m", "1h"]
    assert periods_on_boundary(datetime(2026, 8, 5, 11, 0)) == ["5m", "15m", "30m"]
    assert periods_on_boundary(datetime(2026, 8, 5, 11, 30)) == ["5m", "15m", "30m", "1h"]
    assert periods_on_boundary(datetime(2026, 8, 5, 14, 0)) == ["5m", "15m", "30m", "1h"]
    assert periods_on_boundary(datetime(2026, 8, 5, 15, 0)) == ["5m", "15m", "30m", "1h"]
    assert periods_on_boundary(datetime(2026, 8, 5, 10, 3)) == []
    assert periods_on_boundary(None) == []


def test_1h_boundary_matches_aggregation_bucket_ends():
    """1h 派发边界集合 == snapshot_bars 1h 聚合桶终点集合，且同一分钟不重复派发。"""
    from core.engine.live.snapshot_bars import _bucket_end

    labels = [datetime(2026, 8, 5, 9, 31) + timedelta(minutes=i) for i in range(120)]
    labels += [datetime(2026, 8, 5, 13, 1) + timedelta(minutes=i) for i in range(120)]

    bucket_ends = {e for e in (_bucket_end(t, 60) for t in labels) if e is not None}
    assert bucket_ends == {
        datetime(2026, 8, 5, 10, 30), datetime(2026, 8, 5, 11, 30),
        datetime(2026, 8, 5, 14, 0), datetime(2026, 8, 5, 15, 0),
    }

    one_h_labels = {t for t in labels if "1h" in periods_on_boundary(t)}
    assert one_h_labels == bucket_ends
    assert all(periods_on_boundary(t).count("1h") <= 1 for t in labels)


# ---- 周期过滤（核心节拍正确性）----
def test_on_bar_1m_bar_only_drives_1m_strategy():
    """C6：1m bar(period=1m) 只驱动 1m 策略；5m 策略不被 1m 节拍触发。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "5m"))
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 5)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    # 两策略同 bar_time 同价都有 open_sig=1
    engine.signal_cache = {
        (1, stock, bar_time): [{"name": "open_sig", "value": 1}],
        (2, stock, bar_time): [{"name": "open_sig", "value": 1}],
    }
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert [o.strategy_id for o in orders] == [1]  # 只有 1m 策略下单
    db.close()


def test_on_bar_5m_bar_only_drives_5m_strategy():
    """C6：5m 边界 bar(period=5m) 只驱动 5m 策略；1m 策略不被 5m 边界触发（防风控单串周期）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "5m"))
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 5)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    engine.signal_cache = {
        (1, stock, bar_time): [{"name": "open_sig", "value": 1}],
        (2, stock, bar_time): [{"name": "open_sig", "value": 1}],
    }
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "5m"

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert [o.strategy_id for o in orders] == [2]  # 只有 5m 策略下单
    db.close()


def test_on_bar_no_period_processes_all_strategies():
    """C6：bar 无 period（回测/旧调用）→ 处理全部策略（向后兼容）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "5m"))
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 5)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    engine.signal_cache = {
        (1, stock, bar_time): [{"name": "open_sig", "value": 1}],
        (2, stock, bar_time): [{"name": "open_sig", "value": 1}],
    }
    bar = _bar(stock, "9.3", bar_time)  # period=None

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert sorted(o.strategy_id for o in orders) == [1, 2]
    db.close()


# ---- 边界分发 C6(A) ----
def test_dispatch_period_bar_drives_5m_strategy_only():
    """C6(A)：5m 边界 → 拉 5m bars → 注入信号填 cache(2, code, boundary) → 只 5m 策略下单。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "5m"))
    stock = "600000.SH"
    boundary = datetime(2026, 8, 5, 10, 5)
    bars_5m = [
        {"stime": "20260805100000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260805100500", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260805101000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, bars_5m))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS", 2: "MA_CROSS"})
    # mock compute_injected：返回 open_sig=1（无论周期）
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        stock: {"open_sig": [{"Date": "20260805", "Value": 1}], "ErrorId": 0}
    }

    engine._dispatch_period_bar("5m", boundary)

    # 信号缓存填了 5m 策略(2)的 key（bar.period=5m → 只注入 5m 策略）
    assert (2, stock, boundary) in engine.signal_cache
    assert (1, stock, boundary) not in engine.signal_cache
    db = factory()
    orders = db.query(LiveOrder).all()
    assert [o.strategy_id for o in orders] == [2]
    db.close()


def test_dispatch_period_bar_uses_latest_completed_bar():
    """C6(A)：BarEvent.stocks 用「最新已完成 bar」（非 forming 最新一根）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "5m"))
    stock = "600000.SH"
    boundary = datetime(2026, 8, 5, 10, 5)
    # forming 最新一根 10:10 的 close=9.8，完成 bar 10:05 close=9.4
    bars_5m = [
        {"stime": "20260805100000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260805100500", "open": 9.2, "high": 9.5, "low": 9.2, "close": 9.4, "volume": 10000, "amount": 94000.0},
        {"stime": "20260805101000", "open": 9.4, "high": 9.8, "low": 9.4, "close": 9.8, "volume": 10000, "amount": 98000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, bars_5m))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS", 2: "MA_CROSS"})
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        stock: {"open_sig": [{"Date": "20260805", "Value": 1}], "ErrorId": 0}
    }

    engine._dispatch_period_bar("5m", boundary)

    db = factory()
    orders = db.query(LiveOrder).all()
    # 下单价用完成 bar 10:05 的 close=9.4（而非 forming 10:10 的 9.8）
    assert orders[0].price == Decimal("9.4")
    db.close()


def test_dispatch_period_bar_bridge_offline_sets_offline():
    """C6(A)：分发拉 quote 桥离线 → 置离线返回，不崩。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "5m"))
    stock = "600000.SH"
    boundary = datetime(2026, 8, 5, 10, 5)
    rec = _Recorder(fail_paths={"/quote"})
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS", 2: "MA_CROSS"})

    with pytest.raises(BridgeUnavailableError):
        engine._dispatch_period_bar("5m", boundary)


# ---- 日终 1d C6(B) ----
def test_maybe_daily_bars_1505_drives_1d_strategy():
    """15:05 收盘落定 → 拉完整 1d bar → 注入 1d 策略 → 下单；同日幂等只驱动一次。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [
        {"stime": "20260807000000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260810000000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        stock: {"open_sig": [{"Date": "20260810", "Value": 1}], "ErrorId": 0}
    }
    daily_time = datetime(2026, 8, 10, 0, 0)

    engine._maybe_daily_bars(now=datetime(2026, 8, 10, 15, 5))

    assert (1, stock, daily_time) in engine.signal_cache
    assert engine._last_daily_bar_date == datetime(2026, 8, 10).date()
    # B-1：日线信号当日 deferred，次日首 bar 放行后落单
    assert len(engine._signal_bar_deferred) == 1
    engine._handle_bar(port, _bar(stock, "9.2", datetime(2026, 8, 11, 9, 31)))
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].strategy_id == 1
    db.close()

    # 同日幂等：15:06 再调不重复驱动
    engine._maybe_daily_bars(now=datetime(2026, 8, 10, 15, 6))
    db = factory()
    assert db.query(LiveOrder).count() == 1
    db.close()


def test_maybe_daily_bars_1d_uses_code_period_count_and_dedup():
    """C6(B)：日终 1d 拉取规则同启动预热——长度按 (code,'1d') 的 _code_period_count、去重按 stime。

    桥端可能返回同 stime 重复 bar（历史/拼接重叠）；_sort_and_cap 去重 + 截断后
    注入公式的窗口 = count 根唯一 bar，而非原样塞进公式（同 _preheat 语义）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    # 4 根含 1 根重复 stime；_code_period_count[(code,'1d')]=3 → 去重后截断为 3 根唯一 bar
    daily_bars = [
        {"stime": "20260806000000", "open": 9.0, "high": 9.2, "low": 8.9, "close": 9.1, "volume": 10000, "amount": 91000.0},
        {"stime": "20260807000000", "open": 9.1, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260807000000", "open": 9.0, "high": 9.2, "low": 8.9, "close": 9.0, "volume": 10000, "amount": 90000.0},  # dup stime
        {"stime": "20260810000000", "open": 9.2, "high": 9.5, "low": 9.1, "close": 9.4, "volume": 10000, "amount": 94000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    from core.tq.formula import TQFormula
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        tq_formula=TQFormula(), formula_by_strategy={1: "MA_CROSS"},
        formula_count=200, formula_count_by_name={"MA_CROSS": 3},
        code_period_count={(stock, "1d"): 3},
    )
    captured = {}

    def spy(**kw):
        # 批量路径：df 在 ohlcv_by_code[code]（单列 ohlcv_df），逐只喂入与旧路径一致
        captured["df"] = kw.get("ohlcv_by_code", {}).get(stock)
        return {stock: {"open_sig": [{"Date": "20260810", "Value": 1}], "ErrorId": 0}}

    engine._tq_formula.compute_injected_batch = spy

    engine._maybe_daily_bars(now=datetime(2026, 8, 10, 15, 5))

    # 长度规则：/quote 按 (code,'1d') 的 _code_period_count=3 拉（非周期级 _period_count/全局 200）
    assert any("period=1d" in str(r.url) and "count=3" in str(r.url) for r in rec.requests), \
        "日终 1d 拉取 count 应取该股该周期 _code_period_count"
    # 去重规则：注入公式的 df 行数 = 3 根唯一 bar（4 根去 1 重复 stime 再截断到 3）
    df = captured.get("df")
    assert df is not None and len(df["Close"]) == 3


def test_maybe_daily_bars_before_1505_no_trigger():
    """收盘未落定（< 15:05）不触发：不拉 bar、不驱动、不记幂等标记。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260810000000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})

    engine._maybe_daily_bars(now=datetime(2026, 8, 10, 15, 4))

    assert engine._last_daily_bar_date is None
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()


# ---- 14:50 提前收盘价（功能二：仅 1d，周线/月线不提前）----
class _SynthStub:
    """快照合成器桩：completed_bars 返回固定 1m bars，记录调用次数。"""

    def __init__(self, rows_by_code):
        self.rows_by_code = rows_by_code
        self.calls = 0

    def completed_bars(self, codes, count=10):
        self.calls += 1
        return {c: list(self.rows_by_code[c]) for c in codes if c in self.rows_by_code}

    def open_at(self, codes, start):
        """B 执行价：bar 标签 = 分钟终点（start + 1min），返回该 bar 的 open。"""
        label = start.replace(second=0, microsecond=0) + timedelta(minutes=1)
        out = {}
        for c in codes:
            for row in self.rows_by_code.get(c, []):
                try:
                    t = datetime.strptime(str(row.get("stime")), "%Y%m%d%H%M%S")
                except (TypeError, ValueError):
                    continue
                if t == label:
                    out[c] = row["open"]
                    break
        return out


def _make_advance_engine(disp, factory, port, stock, synth, trading_dates,
                         **extra):
    """构造开启 14:50 提前收盘价的 LiveEngine（注入假日历 + 快照合成器桩）。"""
    from core.engine.bar_poller import BarPoller
    from core.engine.trading_calendar import TradingCalendar
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        advance_close_enabled=True, advance_close_time="14:51",
        snapshot_synth=synth, **extra,
    )
    engine._calendar = TradingCalendar(lambda: list(trading_dates))
    return engine


def test_advance_close_skipped_on_half_day_market():
    """半日市（P2-10）：14:52 已收盘，提前收盘价轮次不得再下单。

    半日市当天 1d 信号由 15:05 收盘落定轮次按完整日线补跑（次日开盘执行），
    这里断言提前轮次完全不动（不取价、不驱动 on_bar、不落幂等标记、不下单）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20261231000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    synth = _SynthStub({stock: [
        {"stime": "20261231145100", "open": 9.3, "high": 9.4,
         "low": 9.2, "close": 9.6, "volume": 500, "amount": 4800.0},
    ]})
    engine = _make_advance_engine(disp, factory, port, stock, synth,
                                  [date(2026, 12, 31)])
    from core.engine.trading_calendar import TradingCalendar
    engine._calendar = TradingCalendar(
        lambda: [date(2026, 12, 31)],
        half_day_provider=lambda year: ["2026-12-31"],
    )
    engine.signal_cache = {
        (1, stock, datetime(2026, 12, 31, 0, 0)): [{"name": "open_sig", "value": 1}]
    }

    engine._maybe_advance_daily_close(now=datetime(2026, 12, 31, 14, 52))

    assert synth.calls == 0
    assert engine._last_advance_close_date is None
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()


def test_advance_close_drives_1d_with_1450_close_price():
    """功能二：14:51 用快照合成 14:50 1m bar 收盘价（9.6）驱动 1d 信号+风控。

    验证：① 收盘价覆盖为快照价（非桥 1d forming 9.2）；② period=1d；
    ③ mismatched_risk=False（周线/月线不参与）；④ 幂等标记落内存与 DB；
    ⑤ 同日再次 tick 不重复执行；⑥ 重启后从 DB 恢复幂等。
    """
    factory, _ = _db_factory()
    db = factory()
    db.add(LiveSession(id=1, name="adv", mode="live", engine="tdx",
                       status="running", execution_mode="auto"))
    db.commit()
    db.close()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260810000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    synth = _SynthStub({stock: [
        {"stime": "20260810145100", "open": 9.3, "high": 9.4,
         "low": 9.2, "close": 9.6, "volume": 500, "amount": 4800.0},
        # 14:51 起始 1m bar（兜底执行价）
        {"stime": "20260810145200", "open": 9.55, "high": 9.6,
         "low": 9.5, "close": 9.58, "volume": 400, "amount": 3800.0},
        # 14:52 起始 1m bar（主执行价）
        {"stime": "20260810145300", "open": 9.7, "high": 9.8,
         "low": 9.65, "close": 9.75, "volume": 300, "amount": 2900.0},
    ]})
    engine = _make_advance_engine(disp, factory, port, stock, synth,
                                  [date(2026, 8, 10)])
    daily_time = datetime(2026, 8, 10, 0, 0)
    engine.signal_cache = {(1, stock, daily_time): [{"name": "open_sig", "value": 1}]}
    seen = []
    orig_on_bar = port.on_bar

    def spy(bar, **kwargs):
        seen.append((bar.period, bar.stocks[stock]["close"],
                     kwargs.get("mismatched_risk")))
        return orig_on_bar(bar, **kwargs)

    port.on_bar = spy

    engine._maybe_advance_daily_close(now=datetime(2026, 8, 10, 14, 52))

    assert seen == [("1d", Decimal("9.6"), False)]
    # 提前收盘价轮次 = 当日尾盘直接报单（用户口径），不再进 B-1 次日延迟队列；
    # 即便该策略 risk_timing 仍是默认 next_day（收盘落定轮次依旧 defer）。
    assert port.strategies[0].risk_timing == "next_day"
    assert len(engine._signal_bar_deferred) == 0
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1
    assert orders[0].status == "submitted"
    assert orders[0].quantity == 600  # int(0.1*60000/9.6/100)*100，按 14:50 价 9.6 计算
    # B 执行价：委托价 = 14:52 起始 1m bar 的 open（9.7），不是合成收盘价 9.6
    assert Decimal(orders[0].price) == Decimal("9.7")
    assert db.query(LiveSession).first().advance_close_done_date == "2026-08-10"
    db.close()

    # 同日再次 tick（14:53）：不重复执行
    engine._maybe_advance_daily_close(now=datetime(2026, 8, 10, 14, 53))
    assert synth.calls == 1 and len(seen) == 1

    # 重启（新引擎同一 DB）：从 advance_close_done_date 恢复幂等，不再执行
    synth2 = _SynthStub({stock: [
        {"stime": "20260810145100", "open": 9.3, "high": 9.4,
         "low": 9.2, "close": 9.6, "volume": 500, "amount": 4800.0},
        {"stime": "20260810145300", "open": 9.7, "high": 9.8,
         "low": 9.65, "close": 9.75, "volume": 300, "amount": 2900.0},
    ]})
    engine2 = _make_advance_engine(disp, factory, port, stock, synth2,
                                   [date(2026, 8, 10)])
    engine2._maybe_advance_daily_close(now=datetime(2026, 8, 10, 14, 53))
    assert synth2.calls == 0


def test_advance_close_sell_still_blocked_by_t1_available():
    """提前收盘价轮次不绕过 T+1：当日可用为 0 的持仓，14:51 CLOSE 不下单。

    保留「尾盘买入当日不可卖」：advance_close 只改「当日提交」，不改 T+1 校验。
    """
    factory, _ = _db_factory()
    db = factory()
    db.add(LiveSession(id=1, name="adv_t1", mode="live", engine="tdx",
                       status="running", execution_mode="auto"))
    db.commit()
    db.close()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    ctx.formula_signals = [
        {"signal_name": "close_sig", "signal_type": SignalType.CLOSE, "trigger_value": 1},
    ]
    _hold(ctx)  # 持仓 1000（昨仓，但桥返回可用 0 = 今日已用掉额度）
    daily_bars = [{"stime": "20260810000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    quote_respond = rec._respond

    def respond(request):
        # 桥 /positions 返回该仓「可用=0」（今日额度已用完）→ T+1 拒卖
        if request.url.path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [{
                "instrument": "600000", "exchange": "SH",
                "available": 0, "volume": 1000,
                "yesterday_volume": 0, "on_road_volume": 0,
            }]})
        return quote_respond(request)

    rec._respond = respond
    disp, _ = _make_dispatcher(rec)
    synth = _SynthStub({stock: [
        {"stime": "20260810145100", "open": 9.3, "high": 9.4,
         "low": 9.2, "close": 9.6, "volume": 500, "amount": 4800.0},
        {"stime": "20260810145300", "open": 9.7, "high": 9.8,
         "low": 9.65, "close": 9.75, "volume": 300, "amount": 2900.0},
    ]})
    engine = _make_advance_engine(disp, factory, port, stock, synth,
                                  [date(2026, 8, 10)])
    daily_time = datetime(2026, 8, 10, 0, 0)
    engine.signal_cache = {(1, stock, daily_time): [{"name": "close_sig", "value": 1}]}

    engine._maybe_advance_daily_close(now=datetime(2026, 8, 10, 14, 52))

    db = factory()
    assert db.query(LiveOrder).count() == 0  # T+1 可用 0 → 拒卖
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)


def test_advance_close_missing_1450_bar_fails_closed():
    """取不到 14:50 快照价 → 不执行、不伪造价格；14:59 后 ERROR + risk 事件。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260810000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    # 只有 14:49 分钟 bar（14:50 未闭合/未采样）
    synth = _SynthStub({stock: [{
        "stime": "20260810145000", "open": 9.3, "high": 9.4,
        "low": 9.2, "close": 9.6, "volume": 500, "amount": 4800.0,
    }]})
    engine = _make_advance_engine(disp, factory, port, stock, synth,
                                  [date(2026, 8, 10)])
    engine.signal_cache = {}
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._maybe_advance_daily_close(now=datetime(2026, 8, 10, 14, 52))
    assert engine._last_advance_close_date is None
    assert q.empty()  # 仅 INFO 重试提示，不告警

    engine._maybe_advance_daily_close(now=datetime(2026, 8, 10, 14, 59))
    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "advance_close_unavailable"
    db = factory()
    assert db.query(LiveOrder).count() == 0  # 不伪造价格、不下单
    db.close()


def test_advance_close_exec_price_falls_back_to_1451_bar():
    """14:52 起始 bar 取不到 → 回退 14:51 起始 bar 的 open，并推 fallback 风控事件。"""
    factory, _ = _db_factory()
    db = factory()
    db.add(LiveSession(id=1, name="adv_fb", mode="live", engine="tdx",
                       status="running", execution_mode="auto"))
    db.commit()
    db.close()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260810000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    synth = _SynthStub({stock: [
        {"stime": "20260810145100", "open": 9.3, "high": 9.4,
         "low": 9.2, "close": 9.6, "volume": 500, "amount": 4800.0},
        # 只有 14:51 起始 bar（14:52 起始缺失）
        {"stime": "20260810145200", "open": 9.55, "high": 9.6,
         "low": 9.5, "close": 9.58, "volume": 400, "amount": 3800.0},
    ]})
    engine = _make_advance_engine(disp, factory, port, stock, synth,
                                  [date(2026, 8, 10)])
    daily_time = datetime(2026, 8, 10, 0, 0)
    engine.signal_cache = {(1, stock, daily_time): [{"name": "open_sig", "value": 1}]}
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._maybe_advance_daily_close(now=datetime(2026, 8, 10, 14, 53))

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1
    assert Decimal(orders[0].price) == Decimal("9.55")  # 14:51 起始 bar 的 open
    db.close()
    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "advance_close_exec_price_fallback"


def test_advance_close_exec_price_missing_fails_closed():
    """14:52/14:51 起始 bar 都取不到 → 该 code 不下单 + missing 风控事件（不伪造价格）。"""
    factory, _ = _db_factory()
    db = factory()
    db.add(LiveSession(id=1, name="adv_miss", mode="live", engine="tdx",
                       status="running", execution_mode="auto"))
    db.commit()
    db.close()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260810000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    synth = _SynthStub({stock: [
        {"stime": "20260810145100", "open": 9.3, "high": 9.4,
         "low": 9.2, "close": 9.6, "volume": 500, "amount": 4800.0},
    ]})
    engine = _make_advance_engine(disp, factory, port, stock, synth,
                                  [date(2026, 8, 10)])
    daily_time = datetime(2026, 8, 10, 0, 0)
    engine.signal_cache = {(1, stock, daily_time): [{"name": "open_sig", "value": 1}]}
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._maybe_advance_daily_close(now=datetime(2026, 8, 10, 14, 53))

    db = factory()
    assert db.query(LiveOrder).count() == 0  # fail-closed，不下单
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)
    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "advance_close_exec_price_missing"


def test_advance_enabled_skips_1d_on_post_close_round():
    """提前轮次**已成功执行**后，15:05 收盘落定轮次不再重复驱动 1d。

    2026-08-10 为周一（非周期末）→ periods 为空，不驱动任何周期，只记幂等标记。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260810000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    synth = _SynthStub({})
    from core.tq.formula import TQFormula
    engine = _make_advance_engine(
        disp, factory, port, stock, synth, [date(2026, 8, 10)],
        tq_formula=TQFormula(), formula_by_strategy={1: "MA_CROSS"},
    )
    engine._tq_formula.compute = MagicMock(return_value={})
    engine._tq_formula.compute_injected_batch = MagicMock(return_value=None)
    # 模拟 14:52 提前轮次当日已成功执行（advance_close_done_date 落库）
    engine._closer._advance_done_loaded = True
    engine._closer.last_advance_close_date = date(2026, 8, 10)
    seen_periods = []
    orig_on_bar = port.on_bar

    def spy(bar, **kwargs):
        seen_periods.append(bar.period)
        return orig_on_bar(bar, **kwargs)

    port.on_bar = spy

    engine._maybe_daily_bars(now=datetime(2026, 8, 10, 15, 5))

    assert seen_periods == []  # 1d 已由提前轮次负责；非周期末无 1w/1mon
    assert engine._last_daily_bar_date == date(2026, 8, 10)
    assert not any(r.url.path == "/quote" for r in rec.requests)


def test_advance_enabled_falls_back_to_post_close_1d_when_advance_failed():
    """P1：提前轮次失败（未落 advance_close_done_date）→ 15:05 仍按 post_close 驱动 1d。

    勾了 B 但 14:50 价/执行价取不到时，不能整天没有 1d 信号与风控：收盘落定轮次
    用完整日线补跑，订单带 post_close → 次日开盘执行。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260810000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    synth = _SynthStub({})
    from core.tq.formula import TQFormula
    engine = _make_advance_engine(
        disp, factory, port, stock, synth, [date(2026, 8, 10)],
        tq_formula=TQFormula(), formula_by_strategy={1: "MA_CROSS"},
    )
    engine._tq_formula.compute = MagicMock(return_value={})
    engine._tq_formula.compute_injected_batch = MagicMock(return_value=None)
    engine._closer._advance_done_loaded = True
    engine._closer.last_advance_close_date = None  # 提前轮次未成功
    seen = []
    orig_on_bar = port.on_bar

    def spy(bar, **kwargs):
        seen.append((bar.period, getattr(bar, "post_close", False)))
        return orig_on_bar(bar, **kwargs)

    port.on_bar = spy

    engine._maybe_daily_bars(now=datetime(2026, 8, 10, 15, 5))

    assert seen == [("1d", True)]  # 补跑 1d，且标记为收盘落定轮次


def test_maybe_daily_bars_period_end_drives_1w_1mon():
    """默认口径：非周期末只驱动 1d；月/周末才追加 1mon / 1w。

    2026-08-31 为周一（本月最后一个交易日，但不是本周最后一个交易日）→ 1d + 1mon。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260831000000", "open": 9.0, "high": 9.2,
                   "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    engine._tq_formula.compute = MagicMock(return_value={})
    engine._tq_formula.compute_injected_batch = MagicMock(return_value=None)
    seen_periods = []
    orig_on_bar = port.on_bar

    def spy(bar, **kwargs):
        seen_periods.append(bar.period)
        return orig_on_bar(bar, **kwargs)

    port.on_bar = spy

    engine._maybe_daily_bars(now=datetime(2026, 8, 31, 15, 5))

    assert seen_periods == ["1d", "1mon"]


# ---- 1w/1mon 通达信注入 C6(C) ----
def test_inject_startup_periods_fills_signal_cache():
    """C6(C)：1w 启动注入 → TQFormula.compute 通达信 → signal_cache 填 (sid, code, daily_time)。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1w", strategy_id=1)
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    daily_time = datetime(2026, 8, 10, 0, 0)
    engine._tq_formula.compute = lambda *a, **kw: {
        stock: {"open_sig": [{"Date": "20260810", "Value": 1}], "ErrorId": 0}
    }

    engine._inject_startup_periods(daily_time)

    assert engine.signal_cache[(1, stock, daily_time)] == [{"name": "open_sig", "value": 1}]


def test_maybe_daily_bars_drives_1w_from_prefilled_cache():
    """C6(C)：1w 策略在周期末（周五）收盘落定轮次命中启动预填信号，不拉桥注入。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1w")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260814000000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    daily_time = datetime(2026, 8, 14, 0, 0)
    engine.signal_cache[(1, stock, daily_time)] = [{"name": "open_sig", "value": 1}]
    engine._tq_formula.compute_injected_batch =MagicMock(return_value=None)

    engine._maybe_daily_bars(now=datetime(2026, 8, 14, 15, 5))

    # B-1：1w 信号当日 deferred，次日首 bar 放行后落单
    assert len(engine._signal_bar_deferred) == 1
    engine._handle_bar(port, _bar(stock, "9.2", datetime(2026, 8, 17, 9, 31)))
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].strategy_id == 1
    db.close()
    engine._tq_formula.compute_injected_batch.assert_not_called()  # 1w 不走桥注入


def test_maybe_daily_bars_day_rollover_reinjects_1w():
    """C6(C)：日切后 1w cache miss → 通达信补注入再驱动。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1w")
    stock = "600000.SH"
    daily_bars = [{"stime": "20260814000000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, daily_bars))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    # 启动注入在 08-13，日终落在 08-14 → 新 daily_time cache miss
    old_daily_time = datetime(2026, 8, 13, 0, 0)
    new_daily_time = datetime(2026, 8, 14, 0, 0)
    engine.signal_cache[(1, stock, old_daily_time)] = [{"name": "open_sig", "value": 1}]
    engine._tq_formula.compute = lambda *a, **kw: {
        stock: {"open_sig": [{"Date": "20260814", "Value": 1}], "ErrorId": 0}
    }

    engine._maybe_daily_bars(now=datetime(2026, 8, 14, 15, 5))

    # 日切补注入：新 daily_time 的 cache 已填
    assert (1, stock, new_daily_time) in engine.signal_cache
    # B-1：1w 信号当日 deferred，次日首 bar 放行后落单
    assert len(engine._signal_bar_deferred) == 1
    engine._handle_bar(port, _bar(stock, "9.2", datetime(2026, 8, 17, 9, 31)))
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].strategy_id == 1
    db.close()


# ---- _bars_to_formula_df 时间解析统一（修潜在 bug）----
def test_bars_to_formula_df_accepts_stime_only():
    """C6：stime-only bars（无 index/time）→ 仍能解析时间注入（统一 parse_bar_time）。"""
    bars = [
        {"stime": "20260805100000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260805100100", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    df = LiveEngine._bars_to_formula_df(bars, "600000.SH")
    assert df is not None
    assert len(df["Close"]) == 2
    idx = list(df["Close"].index)
    assert idx[0] == datetime(2026, 8, 5, 10, 0, 0)
    assert idx[1] == datetime(2026, 8, 5, 10, 1, 0)


# ---- E8 离线→在线转场 ----
def test_loop_offline_to_online_resets_baseline():
    """E8：快照通道离线→在线转场 → _loop 调 reset_baseline（重建基线，不补 bar）。

    P2-6 起，声明 supports_historical_quote 的桥（QMT/掘金）走缺口补派而非重建基线；
    本用例覆盖「不能回补」的快照通道，故显式关闭该能力。
    """
    import asyncio

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"

    class FlakyPing:
        """首次 /ping 失败（模拟启动时离线），之后恢复。"""
        def __init__(self):
            self.requests = []
            self.fail_remaining = 1

        def handler(self, request):
            self.requests.append(request)
            path = request.url.path
            if path == "/ping":
                if self.fail_remaining > 0:
                    self.fail_remaining -= 1
                    raise httpx.ConnectError("connection refused")
                return httpx.Response(200, json={"ok": True})
            if path == "/quote":
                return httpx.Response(200, json={"ok": True, "data": {}})
            if path == "/order":
                return httpx.Response(200, json={"ok": True})
            return httpx.Response(404, json={"ok": False})

    rec = FlakyPing()
    disp, _ = _make_dispatcher(rec)
    disp.supports_historical_quote = False  # 快照通道：不补派，只能重建基线
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    reset_calls = []
    orig_reset = poller.reset_baseline
    poller.reset_baseline = lambda: (reset_calls.append(1), orig_reset())[1]

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, poll_interval=0.01,
    )

    async def run_a_bit():
        await engine.start()
        # 审计 #37：原固定 sleep(0.08) 在调度抖动下偶发不足 1 次 reset_baseline。
        # 改为有界轮询：轮询 reset_calls 直到离线→在线转场完成或超时（同 :1831-1839 先例）。
        deadline = asyncio.get_event_loop().time() + 0.5
        while len(reset_calls) < 1 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.005)
        await engine.stop()

    asyncio.run(run_a_bit())

    assert engine.bridge_online is True
    assert len(reset_calls) == 1  # 离线→在线只转场一次


# ===========================================================================
# C4 三维去重（#28：拉取 (code,period) + 计算 (code,period,formula)）
# ===========================================================================
def _count_quote(rec):
    """统计 _Recorder 中 /quote 请求数。"""
    return [r for r in rec.requests if r.url.path == "/quote"]


def _counting_compute(engine, stock, value=1):
    """把 engine._tq_formula.compute_injected 换成计数版，返回 dict 计数。"""
    calls = {"n": 0}

    def _compute(**kw):
        calls["n"] += 1
        return {
            "ErrorId": "0",
            stock: {"open_sig": [
                {"Date": "202608051000", "Value": 0.0},
                {"Date": "202608051001", "Value": 0.0},
                {"Date": "202608051002", "Value": float(value)},
            ]},
        }

    engine._tq_formula.compute_injected_batch =_compute
    return calls


def test_on_bar_dedups_quote_and_compute_across_portfolios():
    """C4：#28 跨组合去重——同股票+周期+公式的多组合只拉一次/算一次。

    _on_bar 级共享 df_cache/raw_cache → /quote 只 1 次、compute_injected 只 1 次；
    signal_cache 两策略各有条目（隔离不变）。
    """
    factory, _ = _db_factory()
    port1, _ = _portfolio_single(strategy_id=1)
    port2, _ = _portfolio_single(strategy_id=2)
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 2)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars(stock, [datetime(2026, 8, 5, 10, i) for i in range(3)])
    _mock_dispatcher_with_quote(rec, stock, bars)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port1, port2], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        tq_formula=TQFormula(), formula_by_strategy={1: "MACROSSPRO", 2: "MACROSSPRO"},
    )
    calls = _counting_compute(engine, stock)
    bar = _bar(stock, "9.3", bar_time)

    engine._on_bar(bar)

    assert calls["n"] == 1  # 同 (code,period,formula) 只算一次
    assert len(_count_quote(rec)) == 1  # 同 (code,period) 只拉一次
    for sid in (1, 2):
        out_map = {o["name"]: o["value"] for o in engine.signal_cache[(sid, stock, bar_time)]}
        assert out_map["open_sig"] == 1


def test_fill_signal_cache_dedups_compute_by_formula():
    """C4：#28 计算去重——同 code+period 不同公式 → 拉一次、各算一次（formula 进 raw_cache key）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "1m"))  # 策略 1/2 同周期不同公式
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 2)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars(stock, [datetime(2026, 8, 5, 10, i) for i in range(3)])
    _mock_dispatcher_with_quote(rec, stock, bars)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS", 2: "MACD"})
    calls = _counting_compute(engine, stock)
    bar = _bar(stock, "9.3", bar_time)

    engine._fill_signal_cache(port, bar)

    assert calls["n"] == 2  # 两公式各算一次
    assert len(_count_quote(rec)) == 1  # 同 code+period 拉一次
    for sid in (1, 2):
        assert (sid, stock, bar_time) in engine.signal_cache


def test_fill_signal_cache_batches_process_across_stocks():
    """process 批量化：同公式同周期的多只 code 合并成 1 次 compute_injected_batch。

    旧路径逐 code 调 compute_injected（N 次 set + N 次 process = 2N 次 DLL 往返）；
    批量后 Phase B 每组 (formula,period) 只调 1 次，ohlcv_by_code 含全部 code，
    process_mul_zb 在 tqcenter 内批量求值（N set + 1 process）。各 code 信号仍独立填。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1m", strategy_id=1)
    s1, s2 = "600000.SH", "000001.SZ"
    bar_time = datetime(2026, 8, 5, 10, 2)
    bars1 = _quote_bars(s1, [datetime(2026, 8, 5, 10, i) for i in range(3)])
    bars2 = _quote_bars(s2, [datetime(2026, 8, 5, 10, i) for i in range(3)])

    def respond(request):
        if request.url.path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {s1: bars1, s2: bars2}})
        if request.url.path == "/ping":
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(404, json={"ok": False})

    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MACROSSPRO"})

    calls = {"n": 0, "codes": None}

    def _batch(**kw):
        calls["n"] += 1
        calls["codes"] = sorted(kw.get("ohlcv_by_code", {}).keys())
        return {"ErrorId": "0", **{
            c: {"open_sig": [{"Date": "202608051002", "Value": 1.0}]} for c in (s1, s2)
        }}

    engine._tq_formula.compute_injected_batch = _batch

    bar = _bar(s1, "9.3", bar_time)
    bar.stocks[s2] = {
        "open": Decimal("9.0"), "high": Decimal("9.3"),
        "low": Decimal("9.0"), "close": Decimal("9.3"), "volume": 10000,
    }
    engine._fill_signal_cache(port, bar)

    assert calls["n"] == 1                  # 同组 (MACROSSPRO,1m) 只批量 process 1 次
    assert set(calls["codes"]) == {s1, s2}  # 两只 df 都在同一批
    # 两只信号各自填入（隔离不丢）
    for code in (s1, s2):
        out = {o["name"]: o["value"] for o in engine.signal_cache[(1, code, bar_time)]}
        assert out["open_sig"] == 1


def test_fill_signal_cache_uses_formula_count():
    """#27→#28：注入 count 来自 Formula.formula_count（按公式配），非全局 200。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(strategy_id=1, period="1m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 2)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars(stock, [datetime(2026, 8, 5, 10, i) for i in range(3)])
    _mock_dispatcher_with_quote(rec, stock, bars)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "MA_CROSS"}, formula_count=200,
        formula_count_by_name={"MA_CROSS": 300},
    )
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        "ErrorId": "0", stock: {"open_sig": [{"Date": "202608051000", "Value": 1}]},
    }
    bar = _bar(stock, "9.3", bar_time)

    engine._fill_signal_cache(port, bar)

    quotes = _count_quote(rec)
    assert len(quotes) == 1
    assert "count=300" in str(quotes[0].url)  # 用公式级 count，非全局 200


def test_fill_signal_cache_upgrades_quote_count_for_larger_formula():
    """C4：#28 df_cache 升级——同 code+period 两公式 count 200/500 → 大 count 的公式升级重拉。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "1m"))  # 策略1 FORM_A(200) 策略2 FORM_B(500)
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 2)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars(stock, [datetime(2026, 8, 5, 10, i) for i in range(3)])
    _mock_dispatcher_with_quote(rec, stock, bars)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A", 2: "FORM_B"}, formula_count=200,
        formula_count_by_name={"FORM_A": 200, "FORM_B": 500},
    )
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        "ErrorId": "0", stock: {"open_sig": [{"Date": "202608051000", "Value": 1}]},
    }
    bar = _bar(stock, "9.3", bar_time)

    engine._fill_signal_cache(port, bar)

    queries = [str(q.url) for q in _count_quote(rec)]
    assert len(queries) == 2  # 200 后升级重拉 500
    assert any("count=200" in c for c in queries)
    assert any("count=500" in c for c in queries)


def test_on_bar_1m_inject_reuses_poller_bars_merge_cache_no_fetch():
    """优化：BarPoller 透传的本轮 1m bars 经 _on_bar → 注入并入预热缓存复用，零额外拉取。

    优化前：BarPoller 拉 count=10 判完成即弃，注入走 _get_bars_with_increment 增量
    再拉 count=10（双份冗余）；且 _fetch_cached_bars 遇 bars_by_code 无脑复用——拿
    3 根喂 200 根窗口公式（长均线 NaN 静默失效，真实缺陷）。优化后：本轮已拉 bars
    并入预热缓存（启动预热 code_period_count 根历史）→ 复用完整窗口，本轮零 /quote。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 2)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    provided = _quote_bars(stock, [datetime(2026, 8, 5, 10, i) for i in range(3)])  # BarPoller 本轮拉 3 根
    # 预热历史：200 根到 09:59 止（升序，provided 10:00-10:02 更新）
    hist = _quote_bars(stock, [datetime(2026, 8, 5, 9, 59) - timedelta(minutes=i) for i in range(200)])
    _mock_dispatcher_with_quote(rec, stock, hist)   # 兜底：任何 fallback 拉取也有数据
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "MA_CROSS"}, formula_count=200,
    )
    engine._preheat_cache[("600000.SH", "1m")] = engine._make_cache_entry(hist, 200)

    received = {}
    def _compute(**kw):
        df = kw.get("ohlcv_by_code", {}).get(stock) or {}
        received["rows"] = len(df["Close"]) if "Close" in df else 0
        return {"ErrorId": "0", stock: {"open_sig": [{"Date": "202608051002", "Value": 1}]}}
    engine._tq_formula.compute_injected_batch = _compute

    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"
    bar.bars_by_code = {stock: provided}   # BarPoller 本轮透传
    engine._on_bar(bar)

    # 公式拿到完整窗口（200 根）——并入预热缓存后复用，不是拿 3 根裸复用
    assert received["rows"] == 200
    # 本轮注入零额外 /quote 拉取（BarPoller 自身的拉取由 poll 外部计数）
    assert _count_quote(rec) == []
    # 信号仍正确填充（窗口完整 → 公式算出信号）
    assert (1, stock, bar_time) in engine.signal_cache


def test_dispatch_period_bar_uses_period_max_formula_count():
    """C4：#28 边界分发预拉 count = 该周期策略最大 formula_count（供注入，够最长公式）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "5m"))  # 5m 策略 strategy_id=2
    stock = "600000.SH"
    boundary = datetime(2026, 8, 5, 10, 5)
    bars_5m = [
        {"stime": "20260805100000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260805100500", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260805101000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, bars_5m))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "MA_CROSS", 2: "MA_CROSS"}, formula_count=200,
        formula_count_by_name={"MA_CROSS": 400},
    )
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        "ErrorId": "0", stock: {"open_sig": [{"Date": "20260805", "Value": 1}]},
    }

    engine._dispatch_period_bar("5m", boundary)

    quotes = _count_quote(rec)
    assert len(quotes) == 1
    assert "count=400" in str(quotes[0].url)  # 该周期公式最大 count=400


def test_dispatch_period_bar_skips_period_no_strategy_uses():
    """边界 guard：实例无 15m 策略时 _dispatch_period_bar('15m') 直接 return，不拉桥、不下单。

    periods_on_boundary 是纯算术（minute%15==0 → '15m'），不查实例有无 15m 策略。
    真机日志：实例只有 1m/5m/30m，14:30 却白拉 17 只 15m count=200（拉完 period 过滤全跳过）。
    guard 按 _strategy_periods 过滤——实例无该周期策略 → 0 次 /quote、0 订单。
    """
    factory, _ = _db_factory()
    # 实例周期 = 1m/5m/30m（对齐真机 sec 实例，无 15m）
    port, _ = _portfolio_two(periods=("1m", "5m"))
    # 再加一个 30m 策略 ctx，确保 _strategy_periods = {1m,5m,30m}，15m 仍不在内
    pm = port.risk_manager
    ctx30 = StrategyContext(
        strategy_id=3, period="30m",
        capital_ratio=Decimal("0.6"), max_positions=5,
        single_open_ratio=Decimal("0.1"),
    )
    ctx30.formula_signals = []
    ctx30.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx30)
    stock = "600000.SH"
    boundary = datetime(2026, 8, 12, 14, 30)  # 14:30 → periods_on_boundary 含 15m
    bars_15m = [
        {"stime": "20260812140000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260812141500", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260812143000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, bars_15m))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS", 2: "MA_CROSS", 3: "MA_CROSS"})

    engine._dispatch_period_bar("15m", boundary)

    # guard 拦截：未拉任何 15m quote、未产生订单
    assert _count_quote(rec) == []
    db = factory()
    assert db.query(LiveOrder).all() == []
    db.close()


def test_on_bar_dispatches_boundary_once_for_same_bar_time():
    """边界去重：同根 1m bar（14:30）被两轮 poll 触发时，周期边界只分发一次。

    BarPoller 按 code 独立判定完成——慢股票在下一轮 poll 才完成同一 stime
    （真机日志：14:30 的 15m 白拉两次，第二次即此重复分发）。_dispatched_boundaries
    挡掉第二次 _on_bar（同 bar_time）→ 不再拉周期 quote、不再重复求值周期策略。
    1m 策略按当轮 bar.stocks 逐股求值，不受此 guard 影响（测试单股票简化，不断言 1m 路径）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("1m", "5m"))  # 14:30 边界 → 5m 分发（15m/30m 被周期 guard 挡）
    stock = "600000.SH"
    boundary = datetime(2026, 8, 12, 14, 30)
    bars_5m = [
        {"stime": "20260812142500", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260812143000", "open": 9.0, "high": 9.3, "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, bars_5m))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS", 2: "MA_CROSS"})
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        "ErrorId": "0", stock: {"open_sig": [{"Date": "20260812", "Value": 1}]},
    }

    def _period_quotes(p):
        return [r for r in rec.requests if r.url.path == "/quote" and "period=%s" % p in str(r.url)]

    # bar.period="1m" → _fill_signal_cache 只注入 1m 策略，5m quote 全部来自边界分发
    # （否则 period=None 放行所有策略，1m bar 也注入 5m 公式数据，污染计数）。
    bar1 = _bar(stock, "9.3", boundary)
    bar1.period = "1m"
    engine._on_bar(bar1)  # 第一轮 poll：快股票完成 14:30 → 5m 边界分发一次
    five_after_first = len(_period_quotes("5m"))
    assert five_after_first == 1

    # 第二轮 poll：慢股票再次完成 14:30（同 bar_time）→ 边界不再重复分发
    bar2 = _bar(stock, "9.3", boundary)
    bar2.period = "1m"
    engine._on_bar(bar2)
    assert len(_period_quotes("5m")) == five_after_first


# ---------------- 周期边界新鲜度闸（gate=period_bar_freshness）----------------
def test_dispatch_period_bar_skips_stale_bar_and_emits_risk():
    """5m 最新完成 bar 停在上一交易日 → 不派发（绝不用旧 bar 触发）+ WARNING/risk 事件。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    boundary = datetime(2026, 8, 12, 10, 5)
    stale_bars = [
        {"stime": "20260811145500", "open": 9.0, "high": 9.3, "low": 9.0,
         "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260811150000", "open": 9.0, "high": 9.3, "low": 9.0,
         "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, stale_bars))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    events = []
    engine._emit = lambda et, payload: events.append((et, payload))

    status = engine._dispatch_period_bar("5m", boundary)

    assert status == "stale"            # 引擎据此挂待补偿，不登记 boundary
    assert _count_quote(rec)            # 拉过（判断数据陈旧必须看数据）
    db = factory()
    assert db.query(LiveOrder).all() == []
    db.close()
    risk = [p for t, p in events if t == "risk"]
    assert len(risk) == 1
    assert risk[0]["gate"] == "period_bar_freshness"
    assert risk[0]["period"] == "5m"
    assert risk[0]["bar_date"] == "2026-08-11"
    assert stock in risk[0]["codes"]
    assert risk[0]["stale_count"] == 1


def test_on_bar_stale_boundary_pending_then_compensated():
    """闸拦下的 boundary 不登记、挂 pending；后续 1m bar 拿到当日 bar 后补偿派发一次。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    boundary = datetime(2026, 8, 12, 10, 5)
    stale_bars = [
        {"stime": "20260811145500", "open": 9.0, "high": 9.3, "low": 9.0,
         "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260811150000", "open": 9.0, "high": 9.3, "low": 9.0,
         "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    rec = _Recorder(respond=_respond_quote_bars(stock, stale_bars))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    engine._tq_formula.compute_injected_batch = lambda **kw: {"ErrorId": "0"}

    bar = _bar(stock, "9.3", boundary)
    bar.period = "1m"
    engine._on_bar(bar)

    # 跳过而非吞掉：boundary 未登记为已分发，改挂待补偿
    assert boundary not in engine._dispatched_boundaries
    assert engine._pending_boundaries.get(boundary) == {"5m"}

    # TQ 刷新出当日 bar：下一根 1m（10:06 非边界）先补偿 pending boundary
    today_bars = [
        {"stime": "20260812100000", "open": 9.0, "high": 9.3, "low": 9.0,
         "close": 9.2, "volume": 10000, "amount": 92000.0},
        {"stime": "20260812100500", "open": 9.0, "high": 9.3, "low": 9.0,
         "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]
    rec._respond = _respond_quote_bars(stock, today_bars)
    bar2 = _bar(stock, "9.3", datetime(2026, 8, 12, 10, 6))
    bar2.period = "1m"
    engine._on_bar(bar2)

    five = [r for r in rec.requests
            if r.url.path == "/quote" and "period=5m" in str(r.url)]
    assert len(five) >= 2               # 首轮拉取（判定陈旧）+ 补偿拉取
    assert boundary not in engine._pending_boundaries


def test_on_bar_empty_boundary_pending_compensation():
    """P1#6（2026-11-16）：TQ 返回空（全部 code 无 bar）与陈旧同等挂待补偿。

    旧实现只对 "stale" 挂 `_pending_boundaries`，"empty" 直接丢弃（既不登记也不
    补偿），TQ 偶发空返回会让该 5m/15m/30m/1h 边界当日永久丢失。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    boundary = datetime(2026, 8, 12, 10, 5)
    rec = _Recorder()  # /quote 默认返回空 data → dispatch_period_bar 返回 empty
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})

    bar = _bar(stock, "9.3", boundary)
    bar.period = "1m"
    engine._on_bar(bar)

    assert boundary not in engine._dispatched_boundaries
    assert engine._pending_boundaries.get(boundary) == {"5m"}


def test_retry_pending_boundary_expires_after_ttl():
    """待补偿边界超 TTL（15 分钟）→ 放弃且不再拉取（旧 bar 不触发）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    boundary = datetime(2026, 8, 12, 10, 5)
    engine._pending_boundaries[boundary] = {"5m"}

    engine._retry_pending_boundaries(datetime(2026, 8, 12, 10, 21))

    assert boundary not in engine._pending_boundaries
    assert _count_quote(rec) == []



def test_preheat_pulls_each_code_period_once_with_code_period_count():
    """预热：遍历 _code_period_count，每 (code,period) 拉一次 code_period_count 根，
    跳过 1d/1w/1mon。按需不浪费——只拉实际有策略的 (code,period)。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_two(periods=("5m", "5m"))  # 两 5m 策略
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars("600000.SH", [datetime(2026, 8, 5, 10, i) for i in range(5)])
    _mock_dispatcher_with_quote(rec, "600000.SH", bars)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A", 2: "FORM_B"}, formula_count=200,
        formula_count_by_name={"FORM_A": 200, "FORM_B": 250},
    )
    # 模拟 _build_engine 算好的 (code,period)->max：600000.SH·5m = max(200,250)=250
    engine._code_period_count = {("600000.SH", "5m"): 250}

    engine._preheat()

    quotes = _count_quote(rec)
    assert len(quotes) == 1                      # 该 (code,period) 只拉一次
    assert "count=250" in str(quotes[0].url)     # 取两公式最大值 250
    assert ("600000.SH", "5m") in engine._preheat_cache
    entry = engine._preheat_cache[("600000.SH", "5m")]
    assert entry["count"] == 250
    assert entry["last_stime"] == datetime(2026, 8, 5, 10, 4)  # 最新 bar stime


def test_preheat_skips_1d_1w_1mon():
    """预热跳过 1d/1w/1mon（1d 走日终、1w/1mon 走通达信），不拉桥。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars("600000.SH", [datetime(2026, 8, 5) - timedelta(days=i) for i in range(3)])
    _mock_dispatcher_with_quote(rec, "600000.SH", bars)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A"}, formula_count=200,
    )
    engine._code_period_count = {("600000.SH", "1d"): 200, ("600000.SH", "1w"): 200}

    engine._preheat()

    assert len(_count_quote(rec)) == 0           # 1d/1w 都跳过，零拉取
    assert engine._preheat_cache == {}


def test_preheat_single_code_failure_does_not_block_others():
    """预热单 (code,period) 失败不阻断——其余正常入缓存，失败 key 运行期自愈。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    rec = _Recorder(fail_paths={"/quote"})  # /quote 连接失败
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A"}, formula_count=200,
    )
    engine._code_period_count = {("600000.SH", "5m"): 200}

    engine._preheat()  # 不抛异常

    assert engine._preheat_cache == {}           # 失败 → 未入缓存，运行期走全量补


def test_get_bars_with_increment_miss_full_pull_and_backfill():
    """缓存未命中：全量拉 count 根 + 回填缓存（异常/首次路径）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars("600000.SH", [datetime(2026, 8, 5, 10, i) for i in range(5)])
    _mock_dispatcher_with_quote(rec, "600000.SH", bars)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A"}, formula_count=200,
    )
    assert engine._preheat_cache == {}           # 未预热

    out = engine._get_bars_with_increment("600000.SH", "5m", 200)

    quotes = _count_quote(rec)
    assert len(quotes) == 1
    assert "count=200" in str(quotes[0].url)     # 全量拉 200
    assert ("600000.SH", "5m") in engine._preheat_cache  # 回填
    assert len(out) == 5


def test_get_bars_with_increment_hit_no_new_bar_returns_cache():
    """缓存命中 + 增量拉无新 bar：直接返缓存，不拼接（最省）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars("600000.SH", [datetime(2026, 8, 5, 10, i) for i in range(5)])
    _mock_dispatcher_with_quote(rec, "600000.SH", bars)  # 增量拉也返同样 5 根（无新 bar）
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A"}, formula_count=200,
    )
    engine._preheat_cache[("600000.SH", "5m")] = engine._make_cache_entry(bars, 200)

    out = engine._get_bars_with_increment("600000.SH", "5m", 200)

    quotes = _count_quote(rec)
    assert len(quotes) == 1                      # 仅增量拉一次（count=10）
    assert "count=10" in str(quotes[0].url)
    assert len(out) == 5                         # 无新 bar，缓存原样返回


def test_get_bars_with_increment_hit_new_bar_merge_and_cap():
    """缓存命中 + 增量有新 bar：拼到末尾，截断保持 count 长。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    # 缓存已有 10:00~10:04，增量拉返回 10:03~10:06（含 10:05/10:06 两根新）
    cached_bars = _quote_bars("600000.SH", [datetime(2026, 8, 5, 10, i) for i in range(5)])
    new_bars = _quote_bars("600000.SH", [datetime(2026, 8, 5, 10, i) for i in range(3, 7)])
    _mock_dispatcher_with_quote(rec, "600000.SH", new_bars)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A"}, formula_count=200,
    )
    engine._preheat_cache[("600000.SH", "5m")] = engine._make_cache_entry(cached_bars, 6)

    out = engine._get_bars_with_increment("600000.SH", "5m", 6)

    stimes = [parse_bar_time(b) for b in out]
    assert stimes == sorted(stimes)              # 升序
    assert len(out) == 6                         # 截断到 count=6
    assert parse_bar_time(out[-1]) == datetime(2026, 8, 5, 10, 6)  # 末尾是最新 10:06
    # 合并后 7 根（10:00~10:06），截断到 count=6 丢弃最旧的 10:00，保留 10:01~10:06
    assert parse_bar_time(out[0]) == datetime(2026, 8, 5, 10, 1)
    assert engine._preheat_cache[("600000.SH", "5m")]["last_stime"] == datetime(2026, 8, 5, 10, 6)


def test_get_bars_with_increment_upgrade_when_count_exceeds_cache():
    """缓存存在但请求 count > 缓存 count：升级全量拉（够长公式窗口，不增量）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars("600000.SH", [datetime(2026, 8, 5, 10, i) for i in range(5)])
    _mock_dispatcher_with_quote(rec, "600000.SH", bars)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A"}, formula_count=200,
    )
    # 缓存只有 200 根，请求 500 → 升级全量拉 500
    engine._preheat_cache[("600000.SH", "5m")] = engine._make_cache_entry(bars, 200)

    engine._get_bars_with_increment("600000.SH", "5m", 500)

    quotes = _count_quote(rec)
    assert len(quotes) == 1
    assert "count=500" in str(quotes[0].url)     # 升级全量 500，非增量 10
    assert engine._preheat_cache[("600000.SH", "5m")]["count"] == 500  # 缓存升级


def test_offline_recovery_clears_preheat_cache():
    """E8 离线恢复：_tick_main 清预热缓存，下次走全量重建。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    bars = _quote_bars("600000.SH", [datetime(2026, 8, 5, 10, i) for i in range(5)])
    _mock_dispatcher_with_quote(rec, "600000.SH", bars)
    engine = _make_engine_formula_portfolio(
        disp, factory, port, {1: "FORM_A"}, formula_count=200,
    )
    engine._preheat_cache[("600000.SH", "5m")] = engine._make_cache_entry(bars, 200)
    assert engine._preheat_cache != {}

    # 模拟离线→在线转场：was_online=False + heartbeat 成功
    engine._bridge_online = False
    rec2 = _Recorder(respond=_respond_quote_bars("600000.SH", bars))
    disp2, _ = _make_dispatcher(rec2)
    engine._dispatcher = disp2  # 在线桥
    engine._bar_poller._dispatcher = disp2

    # _tick_main 需在 worker 线程跑（run_in_executor），这里直接调同步体
    engine._tick_main()

    # 离线恢复清了预热缓存
    assert engine._preheat_cache == {}



def _breaker_engine(factory):
    """带 LiveSessionPortfolio link 的引擎（session_id=1, portfolio_strategy_id=1）。"""
    port, _ = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    db = factory()
    db.add(LiveSessionPortfolio(session_id=1, portfolio_strategy_id=1, circuit_breaker_count=0))
    db.commit()
    db.close()
    return engine, port


def _breaker_link(db, count):
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=1, portfolio_strategy_id=1
    ).first()
    assert link is not None
    assert link.circuit_breaker_count == count
    return link


def test_persist_breaker_count_writes_on_change():
    """H4：max_drawdown 触发计数变化 → 写 LiveSessionPortfolio.circuit_breaker_count。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)

    port.risk_manager.consecutive_drawdown_triggers = 1
    engine._persist_breaker_count(port)

    db = factory()
    link = _breaker_link(db, 1)
    assert link.status == "active"  # <3 不转 circuit_broken
    db.close()
    assert engine._breaker_count_written[1] == 1  # 记录已写计数，避免重复落库


def test_persist_breaker_count_unchanged_skips_write():
    """H4：计数未变 → 直接跳过（不查询/不落库）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    engine._breaker_count_written[1] = 2
    port.risk_manager.consecutive_drawdown_triggers = 2

    engine._persist_breaker_count(port)  # 计数 == written → return，不写库

    db = factory()
    _breaker_link(db, 0)  # DB 仍是初始 0
    db.close()


def test_persist_breaker_count_three_sets_circuit_broken():
    """H4：达 3 次 → status 转 circuit_broken（design §8.3）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)

    port.risk_manager.consecutive_drawdown_triggers = 3
    engine._persist_breaker_count(port)

    db = factory()
    link = _breaker_link(db, 3)
    assert link.status == "circuit_broken"
    db.close()


def test_handle_bar_persists_breaker_count_on_drawdown_trigger():
    """H4：_handle_bar 接 update_peak——max_drawdown 熔断触发（计数+1）→ circuit_breaker_count 落库。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    # 清风控比例：drawdown bar 不触发止损单，聚焦熔断计数断言
    port.strategies[0].strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0"), take_profit_ratio=Decimal("0"),
        trailing_stop_ratio=Decimal("0"),
    )
    stock = "600000.SH"
    # 持仓 1000 股 + 现金 0：总市值随 bar close 波动
    pos = Position(stock)
    pos.buy(1000, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))
    port.strategies[0].positions[stock] = pos
    port.account.cash = Decimal("0")

    # bar1: close=100 → 总市值 100000 建峰（无熔断）
    engine._handle_bar(port, _bar(stock, "100", datetime(2026, 8, 5, 10, 0)))
    db = factory()
    _breaker_link(db, 0)
    db.close()

    # bar2: close=70 → 回撤 30% > 20% → 熔断，计数+1 落库
    engine._handle_bar(port, _bar(stock, "70", datetime(2026, 8, 5, 10, 1)))
    db = factory()
    link = _breaker_link(db, 1)
    assert link.status == "active"
    db.close()


def test_recover_restores_risk_state_peak_and_daily_pause():
    """风控内存态持久化：重启后峰值/昨收/暂停态不丢。

    只恢复计数不恢复峰值时，重启首根 bar 会重设 peak → 历史回撤被抹平、max_drawdown
    少触发；日内暂停也会被意外解除。这里覆盖 peak/prev_close/daily_pause 的读回。
    """
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.peak_value = Decimal("1200000")
    rm.prev_close_value = Decimal("1180000")
    rm.daily_pause_active = True
    rm.daily_loss_trigger_date = date(2026, 10, 24)
    rm.breaker_trigger_date = date(2026, 10, 23)
    engine._breaker.persist_risk_state(port)

    # 模拟重启：内存态清零后走 recover 读回
    rm.peak_value = Decimal("0")
    rm.prev_close_value = None
    rm.daily_pause_active = False
    rm.daily_loss_trigger_date = None
    rm.breaker_trigger_date = None

    db = factory()
    engine.recover(db)
    db.close()

    assert rm.peak_value == Decimal("1200000")
    assert rm.prev_close_value == Decimal("1180000")
    assert rm.daily_pause_active is True
    assert rm.daily_loss_trigger_date == date(2026, 10, 24)
    assert rm.breaker_trigger_date == date(2026, 10, 23)


def test_recover_restores_risk_state_even_with_zero_trigger_count():
    """计数为 0 也要还原风险态：峰值与熔断计数是两套独立状态。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.peak_value = Decimal("900000")
    engine._breaker.persist_risk_state(port)

    rm.peak_value = Decimal("0")
    db = factory()
    engine.recover(db)
    db.close()
    assert rm.peak_value == Decimal("900000")


def test_recover_keeps_breaker_when_triggered_today():
    """当日触发 max_drawdown 后重启 → 熔断态保持（次日恢复语义，不得静默解除）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.circuit_breaker_active = True
    rm.breaker_trigger_date = now_shanghai().date()
    engine._breaker.persist_risk_state(port)

    rm.circuit_breaker_active = False
    rm.breaker_trigger_date = None

    db = factory()
    engine.recover(db)
    db.close()
    assert rm.circuit_breaker_active is True
    assert rm.is_trading_halted() is True


def test_recover_clears_breaker_when_trigger_date_passed():
    """触发日早于今天 → 重启时按次日恢复解除熔断（与盘中跨日恢复一致）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.circuit_breaker_active = True
    rm.breaker_trigger_date = now_shanghai().date() - timedelta(days=1)
    engine._breaker.persist_risk_state(port)

    rm.circuit_breaker_active = False
    rm.breaker_trigger_date = None

    db = factory()
    engine.recover(db)
    db.close()
    assert rm.circuit_breaker_active is False


def test_restore_risk_state_ignores_money_fields_from_v1_payload():
    """旧口径（无 schema 键）的 peak/prev_close 不还原：继承仓市值未扣现金前
    写下的峰值偏高，沿用会把升级后的正常净值误判成回撤 → 启动即误触熔断。"""
    from types import SimpleNamespace
    from core.engine.live.breaker import BreakerService

    rm = SimpleNamespace(
        peak_value=Decimal("0"),
        prev_close_value=None,
        daily_pause_active=False,
        circuit_breaker_active=False,
        breaker_trigger_date=None,
        daily_loss_trigger_date=None,
    )
    raw = json.dumps({
        "peak_value": "1200000",
        "prev_close_value": "1180000",
        "daily_pause_active": True,
        "breaker_trigger_date": "2026-10-23",
    })
    assert BreakerService._restore_risk_state(rm, raw) is True
    assert rm.peak_value == Decimal("0")          # 金额类不还原
    assert rm.prev_close_value is None
    assert rm.daily_pause_active is True          # 日期/开关类仍还原
    assert rm.breaker_trigger_date == date(2026, 10, 23)


def test_restore_risk_state_bad_date_does_not_partially_apply():
    """risk_state 日期字段损坏时整体放弃恢复，不能留下「半恢复」的峰值/暂停态。"""
    from types import SimpleNamespace
    from core.engine.live.breaker import BreakerService

    rm = SimpleNamespace(
        peak_value=Decimal("1000"),
        prev_close_value=None,
        daily_pause_active=False,
        breaker_trigger_date=None,
        daily_loss_trigger_date=None,
    )
    raw = json.dumps({
        "peak_value": "9999",
        "prev_close_value": "9000",
        "daily_pause_active": True,
        "breaker_trigger_date": "not-a-date",
    })
    assert BreakerService._restore_risk_state(rm, raw) is False
    assert rm.peak_value == Decimal("1000")
    assert rm.prev_close_value is None
    assert rm.daily_pause_active is False


def test_recover_restores_breaker_count():
    """D4：recover 读回 circuit_breaker_count → consecutive_drawdown_triggers（重启后不丢累计次数）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    db = factory()
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=1, portfolio_strategy_id=1
    ).first()
    link.circuit_breaker_count = 2
    db.commit()
    db.close()

    db = factory()
    engine.recover(db)
    db.close()

    assert port.risk_manager.consecutive_drawdown_triggers == 2
    assert port.risk_manager.manual_recovery is False   # 2 < 3 未转手动
    assert port.risk_manager.circuit_breaker_active is False
    assert engine._breaker_count_written[1] == 2  # 预置，避免首 bar 重复写


def test_recover_breaker_count_three_sets_manual_halt():
    """D4：达 3 次 → recover 后转手动恢复：manual_recovery + circuit_breaker_active（停新开仓）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    db = factory()
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=1, portfolio_strategy_id=1
    ).first()
    link.circuit_breaker_count = 3
    link.status = "circuit_broken"
    db.commit()
    db.close()

    db = factory()
    engine.recover(db)
    db.close()

    assert port.risk_manager.consecutive_drawdown_triggers == 3
    assert port.risk_manager.manual_recovery is True
    assert port.risk_manager.circuit_breaker_active is True
    assert port.risk_manager.is_trading_halted() is True


def test_recover_breaker_resets_manual_recovery():
    """§8.3 转手动恢复后的人工恢复入口：recover_breaker 清零计数 + 解除手动恢复 + 落库 active。

    构造 count=3 + status=circuit_broken（同 test_recover_breaker_count_three_sets_manual_halt
    前置），调 engine.recover_breaker(1) → 内存态全清 + DB status=active/count=0 +
    _breaker_count_written 同步（否则下 bar _persist_breaker_count 比对跳过回写）。
    peak_value 不重置（用户决策：保留历史峰值）。
    """
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    # 先置 3 次熔断转手动（内存 + DB）
    db = factory()
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=1, portfolio_strategy_id=1
    ).first()
    link.circuit_breaker_count = 3
    link.status = "circuit_broken"
    db.commit()
    db.close()
    db = factory()
    engine.recover(db)  # 还原内存态：count=3 → manual_recovery=True
    db.close()
    assert port.risk_manager.manual_recovery is True  # 前置：确实卡在手动恢复
    engine._breaker_count_written[1] = 3  # 模拟已落库 3（recover 预置）

    ok_flag = engine.recover_breaker(1)

    assert ok_flag is True
    # 内存态全清
    assert port.risk_manager.consecutive_drawdown_triggers == 0
    assert port.risk_manager.manual_recovery is False
    assert port.risk_manager.circuit_breaker_active is False
    assert port.risk_manager.breaker_trigger_date is None
    assert port.risk_manager.is_trading_halted() is False
    # _breaker_count_written 同步（防下 bar 跳过回写）
    assert engine._breaker_count_written[1] == 0
    # DB 落库
    db = factory()
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=1, portfolio_strategy_id=1
    ).first()
    assert link.status == "active"
    assert link.circuit_breaker_count == 0
    db.close()


def test_recover_breaker_unknown_portfolio_returns_false():
    """recover_breaker 对不属于本 session 的组合返回 False（不抛异常、不改任何状态）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)

    ok_flag = engine.recover_breaker(999)  # 不存在的 portfolio_id

    assert ok_flag is False
    # 现有组合状态未被动过
    assert port.risk_manager.consecutive_drawdown_triggers == 0
    assert port.risk_manager.manual_recovery is False


def test_recover_breaker_emits_risk_event():
    """recover_breaker 后 emit risk(triggered=False) 事件——前端 SSE 实时看到恢复。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    port.risk_manager.manual_recovery = True  # 前置：卡在手动恢复
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine.recover_breaker(1)

    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "max_drawdown"
    assert ev["triggered"] is False
    assert ev["count"] == 0
    assert ev["portfolio_id"] == 1
    assert q.empty()


def test_recover_breaker_keeps_peak_by_default(monkeypatch):
    """⑤ 默认不重锚：手动恢复保留峰值（回撤基准不变），事件说明再次触发属预期。"""
    from core.engine.live import breaker as breaker_mod
    monkeypatch.setattr(breaker_mod, "manual_reset_reanchor_enabled", lambda: False)
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.peak_value = Decimal("100000")
    rm._last_bar_total_value = Decimal("85000")   # 当前净值低于峰值
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    assert engine.recover_breaker(1) is True

    assert rm.peak_value == Decimal("100000")     # 峰值不动
    ev = q.get_nowait()
    assert ev["type"] == "risk" and ev["rule"] == "max_drawdown"
    assert ev["peak_value"] == "100000"
    assert ev["reanchored_peak"] is None
    assert "再次回撤到阈值仍会触发，属预期" in ev["message"]


def test_recover_breaker_reanchors_peak_when_enabled(monkeypatch):
    """⑤ 配置开启才重锚：峰值改为最近一次估值，回撤基准从当前净值重算。"""
    from core.engine.live import breaker as breaker_mod
    monkeypatch.setattr(breaker_mod, "manual_reset_reanchor_enabled", lambda: True)
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.peak_value = Decimal("100000")
    rm._last_bar_total_value = Decimal("85000")
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    assert engine.recover_breaker(1) is True

    assert rm.peak_value == Decimal("85000")
    ev = q.get_nowait()
    assert ev["reanchored_peak"] == "85000"
    assert "重锚" in ev["message"]
    # 落库的 risk_state 同步为新区间峰值（重启不再恢复旧峰值）
    db = factory()
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=1, portfolio_strategy_id=1).first()
    assert json.loads(link.risk_state)["peak_value"] == "85000"
    db.close()


def test_recover_breaker_config_flag_defaults_false(monkeypatch):
    """⑤ 配置缺省 → 不重锚（对齐回测的峰值只增不减）。"""
    from core.engine.live import breaker as breaker_mod
    monkeypatch.setattr(breaker_mod, "load_config", lambda: {"tq_trade": {}})
    assert breaker_mod.manual_reset_reanchor_enabled() is False
    monkeypatch.setattr(
        breaker_mod, "load_config",
        lambda: {"tq_trade": {"breaker_manual_reset_reanchor_peak": "true"}})
    assert breaker_mod.manual_reset_reanchor_enabled() is True


# ---------------- F6 同 bar 多策略超卖（bar 内可用量递减记账）----------------
def test_live_t1_checker_consume_available_decrements():
    """F6：consume_available 递减 bar 可用量——同 bar 后续 SELL 见递减后的值；重设快照恢复全量。"""
    checker = LiveT1Checker()
    checker.set_available_map({"600000.SH": 800})
    pos = Position("600000.SH")
    pos.buy(1000, Decimal("9.0"), datetime(2026, 8, 5, 10, 0))
    assert checker.get_available_shares(pos, datetime(2026, 8, 5).date()) == 800

    checker.consume_available("600000.SH", 600)  # A 卖 600 → 余 200
    assert checker.get_available_shares(pos, datetime(2026, 8, 5).date()) == 200

    checker.consume_available("600000.SH", 500)  # 超出 → 钳到 0，不出现负值
    assert checker.get_available_shares(pos, datetime(2026, 8, 5).date()) == 0

    checker.set_available_map({"600000.SH": 800})  # 下一 bar 重设快照 → 恢复全量
    assert checker.get_available_shares(pos, datetime(2026, 8, 5).date()) == 800


def test_handle_bar_two_sells_share_bar_available():
    """F6：同 bar A 卖 600 + B 卖 400，桥 available 800 → 递减记账后 B 只下 200（不超卖）。"""
    factory, _ = _db_factory()
    port, ctxs = _portfolio_two(periods=("1m", "1m"))  # 策略 1/2 同周期
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    for ctx, qty in zip(ctxs, (600, 400)):
        ctx.formula_signals = [
            {"signal_name": "close_sig", "signal_type": SignalType.CLOSE, "trigger_value": -1},
        ]
        ctx.positions[stock] = Position(stock)
        ctx.positions[stock].buy(qty, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))

    def respond(request):
        path = request.url.path
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [
                {"instrument": "600000", "exchange": "SH",
                 "available": 800, "volume": 1000,
                 "yesterday_volume": 1000, "on_road_volume": 0},
            ]})
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {
        (1, stock, bar_time): [{"name": "close_sig", "value": -1}],
        (2, stock, bar_time): [{"name": "close_sig", "value": -1}],
    }
    bar = _bar(stock, "9.0", bar_time)

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).order_by(LiveOrder.strategy_id).all()
    assert len(orders) == 2
    by_sid = {o.strategy_id: o for o in orders}
    assert by_sid[1].quantity == 600
    assert by_sid[2].quantity == 200  # B 见递减后 available=200，不超卖
    placed = [json.loads(r.content)["volume"] for r in rec.requests if r.url.path == "/order"]
    assert placed == [600, 200]
    db.close()


# ---------------- G5 回填轮询独立更短节拍（5s）----------------
def test_start_creates_independent_deals_task():
    """G5：start() 起独立 _deals_task（回填轮询，独立于主循环），stop() 取消。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    import asyncio

    async def run_a_bit():
        await engine.start()
        assert engine._deals_task is not None
        assert not engine._deals_task.done()
        await asyncio.sleep(0.01)
        await engine.stop()

    asyncio.run(run_a_bit())
    assert engine._deals_task is None
    assert engine._task is None


def test_deals_loop_polls_deals_at_own_interval():
    """G5：_deals_loop 按 deals_poll_interval(5s 默认) 独立调 _poll_deals，不依赖主循环节拍。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        deals_poll_interval=0.01,
    )
    calls = []
    engine._poll_deals = lambda: calls.append(1)
    import asyncio

    async def run_a_bit():
        await engine.start()
        # 审计 #37：原固定 sleep(0.05) 断言 >=3 在调度抖动下偶发失败（_tick_deals 经
        # 线程池派发，比直调多一次跨线程调度，时序更易抖）。改为有界等待：轮询 calls
        # 直到达到阈值或超时——稳态后必达 3（0.01s 间隔 × 0.5s 窗口足够余量）。
        deadline = asyncio.get_event_loop().time() + 0.5
        while len(calls) < 3 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.005)
        await engine.stop()

    asyncio.run(run_a_bit())
    assert len(calls) >= 3


def test_deals_poll_interval_backs_off_on_slow_round_and_recovers():
    """自适应节拍：慢一轮按实测耗时退避（上限 30s），快返回回落 5s。"""
    factory, _ = _db_factory()
    disp, _ = _make_dispatcher(_Recorder())
    port, _ = _portfolio_single()
    engine = _make_engine(disp, factory, port)

    assert engine._deals_poll_interval == 5.0
    assert engine._deals_next_interval(0.4) == 5.0                       # 快返回 → 5s
    assert engine._deals_next_interval(13.2) == pytest.approx(13.2)      # 掘金慢轮 → 退避
    assert engine._deals_next_interval(45.0) == 30.0                     # 上限 30s
    assert engine._deals_next_interval(0) == 5.0                         # 非法值 → 下限


# ---------------- D3 对账（虚拟持仓 vs 桥实际，仅告警不修正）----------------
def _seed_trade(db, stock, qty, price="9.0"):
    """预置一笔 BUY 成交（recover 据此重建虚拟持仓）。"""
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code=stock,
        trade_type="buy", order_type="limit", price=Decimal(price), quantity=qty,
        filled_quantity=qty, filled_price=Decimal(price), status="accepted",
        signal_name="open_sig", signal_type="OPEN", bar_time=datetime(2026, 8, 5, 10, 0),
    )
    db.add(lo)
    db.flush()
    db.add(LiveTrade(
        live_session_id=1, live_order_id=lo.id, portfolio_strategy_id=1, strategy_id=1,
        stock_code=stock, trade_type="buy", price=Decimal(price), quantity=qty,
        amount=Decimal(price) * qty, commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=datetime(2026, 8, 5, 10, 0),
    ))


def _positions_respond(volume):
    def respond(request):
        path = request.url.path
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [
                {"instrument": "600000", "exchange": "SH",
                 "available": volume, "volume": volume,
                 "yesterday_volume": volume, "on_road_volume": 0},
            ]})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    return respond


def _reconcile_engine(factory, seed_qty, respond=None, fail_paths=None):
    """seeded BUY + 指定 /positions respond 的引擎；recover 后返回 (engine, port, ctx)。"""
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    db = factory()
    _seed_trade(db, stock, seed_qty)
    db.commit()
    db.close()
    rec = _Recorder(respond=respond, fail_paths=fail_paths)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    db = factory()
    engine.recover(db)
    db.close()
    return engine, port, ctx


def test_reconcile_positions_reports_virtual_vs_real_mismatch():
    """D3：虚拟 1000 vs 桥实际 800 → 记录 mismatch，账面不修正（仍 1000）。"""
    factory, _ = _db_factory()
    engine, _, ctx = _reconcile_engine(factory, 1000, respond=_positions_respond(800))

    assert ctx.positions["600000.SH"].quantity == 1000  # 只告警，不改虚拟持仓
    got = engine._reconcile_mismatches
    assert len(got) == 1
    assert {k: got[0][k] for k in ("code", "virtual", "real", "diff")} == {
        "code": "600000.SH", "virtual": 1000, "real": 800, "diff": 200}


def test_reconcile_positions_clean_when_matching():
    """D3：虚拟 == 桥实际 → 无 mismatch 记录。"""
    factory, _ = _db_factory()
    engine, _, _ = _reconcile_engine(factory, 800, respond=_positions_respond(800))

    assert engine._reconcile_mismatches == []


def test_reconcile_positions_offline_skips():
    """D3：桥离线 → 跳过对账不崩，无 mismatch 记录。"""
    factory, _ = _db_factory()
    engine, _, _ = _reconcile_engine(factory, 1000, fail_paths={"/positions"})

    assert engine._reconcile_mismatches == []


# ---------------- B5 SSE 事件流（signal/order/trade/position/risk 推送）----------------
def _ss_engine(factory, port=None):
    """B5 辅助：单组合引擎 + 独立订阅队列，返回 (engine, q)。"""
    if port is None:
        port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    return engine, q


def test_emit_pushes_event_to_all_subscribers():
    """B5：_emit 向所有订阅队列广播 {type, **payload}；队列满丢弃不崩。"""
    factory, _ = _db_factory()
    engine, _ = _ss_engine(factory)
    q1 = asyncio.Queue()
    q2 = asyncio.Queue(maxsize=1)
    q2.put_nowait("full")
    engine._stream_subscribers.extend([q1, q2])

    engine._emit("signal", {"portfolio_id": 1, "strategy_id": 1})

    assert q1.get_nowait() == {"type": "signal", "portfolio_id": 1, "strategy_id": 1}
    assert q2.get_nowait() == "full"  # 积压队列丢新事件，原事件保留，不抛异常


def test_handle_bar_emits_signal_and_order_submitted():
    """B5：OPEN 信号 → 先 signal 后 order(submitted) 事件；未回填不 emit trade/position。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, q = _ss_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))

    events = _drain_queue(q)
    sig = events[0]
    assert sig["type"] == "signal"
    assert sig["portfolio_id"] == 1
    assert sig["strategy_id"] == 1
    assert sig["stock_code"] == stock
    assert sig["signal_name"] == "open_sig"
    assert sig["signal_type"] == "OPEN"
    order_ev = next(e for e in events if e["type"] == "order")
    assert order_ev["status"] == "submitted"
    assert order_ev["portfolio_id"] == 1
    assert order_ev["order_id"] is not None
    assert order_ev["stock_code"] == stock
    assert order_ev["trade_type"] == "BUY"
    assert order_ev["quantity"] > 0
    # 未成交回报前不 emit trade/position（decision 是闸门审计事件，不算成交）
    assert not any(e["type"] in ("trade", "position") for e in events)


def test_handle_bar_bridge_reject_emits_order_rejected():
    """B5：桥拒单（/order 连接失败）→ order(submitted) 后跟 order(rejected)，带 error_message。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder(fail_paths={"/order"})
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))

    events = [q.get_nowait(), q.get_nowait(), q.get_nowait()]
    assert events[0]["type"] == "signal"
    assert events[1]["type"] == "order" and events[1]["status"] == "submitted"
    assert events[2]["type"] == "order" and events[2]["status"] == "rejected"
    assert events[2]["order_id"] == events[1]["order_id"]
    assert "error_message" in events[2]
    # 决策闸门事件（调参可观测性）：桥离线拒单记 bridge_unavailable
    extra = _drain_queue(q)
    assert len(extra) == 1 and extra[0]["type"] == "decision"
    assert extra[0]["gate"] == "bridge_unavailable"


def test_handle_bar_bridge_business_reject_surfaces_error():
    """#4：桥业务拒单（白名单/限额，{ok:false,error:...}）→ order(rejected) 回显桥侧真实原因。

    此前 dispatcher 把桥 error 吞成 None → error_message 笼统 "approval failed or bridge rejected"，
    真机查不了拒单原因。现抛 BridgeOrderRejected → 回显 volume 超限文案。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)

    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={
                "ok": False, "error": "volume 33900 exceeds max 100000",
            })
        return httpx.Response(200, json={"ok": True})

    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))

    events = [q.get_nowait(), q.get_nowait(), q.get_nowait()]
    assert events[0]["type"] == "signal"
    assert events[1]["type"] == "order" and events[1]["status"] == "submitted"
    # P1-3：数量超上限属参数错误（exceeds）→ 终态 rejected，不回队
    assert events[2]["type"] == "order" and events[2]["status"] == "rejected"
    assert events[2]["error_message"] == "volume 33900 exceeds max 100000"
    # 决策闸门事件：桥业务拒单记 bridge_rejected
    extra = _drain_queue(q)
    assert len(extra) == 1 and extra[0]["type"] == "decision"
    assert extra[0]["gate"] == "bridge_rejected"
    assert not engine._signal_bar_deferred


# ---------------- Fix1：涨跌停前置校验（price_limit_check_enabled 门控，引擎接线）----------------
class _LimitCheckDispatcher(HttpBridgeDispatcher):
    """测试替身：check_price_limit 可编程返回（模拟 tdx/ths 通道校验）。"""

    def __init__(self, client, reason):
        super().__init__(client=client)
        self.check_calls = []
        self._reason = reason

    def check_price_limit(self, stock_code, direction, price):
        self.check_calls.append((stock_code, direction, str(price)))
        return self._reason


def _limit_check_engine(factory, port, reason):
    stock = "600000.SH"
    rec = _Recorder()
    client = httpx.Client(transport=httpx.MockTransport(rec.handler))
    disp = _LimitCheckDispatcher(client, reason)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    return engine, disp, rec


def test_handle_bar_price_limit_enabled_rejects_buy_at_limit_up():
    """Fix1 + P1-3：开启校验 + check_price_limit 返回原因 → 可重排拒单。

    2026-11-11（P1-3）：越涨跌停不再是终态——撤/拦后回 deferred，等下一交易日
    due 窗口重试（回测同口径），错误原因仍完整回显。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.price_limit_check_enabled = True
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    reason = "委托买价 9.30 ≥ 涨停价 9.30（前收 8.45 ×0.10），已拦截"
    engine, disp, rec = _limit_check_engine(factory, port, reason)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._handle_bar(port, _bar(stock, "9.3", bar_time))

    # 校验在发单前：桥未收到 /order
    assert not any(r.url.path == "/order" for r in rec.requests)
    assert disp.check_calls and disp.check_calls[0][0] == stock
    assert disp.check_calls[0][1] == "buy"
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1
    assert orders[0].status == "deferred"
    assert "deferred_retry:n=1" in (orders[0].error_message or "")
    assert reason in orders[0].error_message
    db.close()
    # 事件流：signal → order(submitted) → order(deferred 回显原因) → bridge_rejected
    events = [q.get_nowait(), q.get_nowait(), q.get_nowait()]
    assert events[0]["type"] == "signal"
    assert events[1]["type"] == "order" and events[1]["status"] == "submitted"
    assert events[2]["type"] == "order" and events[2]["status"] == "deferred"
    assert reason in events[2]["error_message"]
    extra = _drain_queue(q)
    risk_evs = [e for e in extra if e["type"] == "risk"]
    assert risk_evs and risk_evs[0]["gate"] == "bridge_rejected"
    assert risk_evs[0]["action"] == "requeue"
    dec = [e for e in extra if e["type"] == "decision"]
    assert len(dec) == 1 and dec[0]["gate"] == "bridge_rejected"


def test_handle_bar_price_limit_pass_through_submits():
    """Fix1：类默认开（直接构造 Portfolio 即开启）+ check_price_limit 返回
    None（通过）→ 照常发单。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    assert port.price_limit_check_enabled is True  # 默认开，无需显式置位
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, disp, rec = _limit_check_engine(factory, port, None)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.3", bar_time))

    assert disp.check_calls  # 校验被调用
    assert any(r.url.path == "/order" for r in rec.requests)
    db = factory()
    assert db.query(LiveOrder).first().status == "submitted"
    db.close()


def test_handle_bar_price_limit_disabled_skips_check():
    """Fix1：用户显式关闭（组合表 False）→ 完全不调校验（与回测门控一致）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.price_limit_check_enabled = False
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, disp, rec = _limit_check_engine(
        factory, port, "不应被调用")
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.3", bar_time))

    assert disp.check_calls == []  # 配置关 → 校验零调用
    assert any(r.url.path == "/order" for r in rec.requests)
    db = factory()
    assert db.query(LiveOrder).first().status == "submitted"
    db.close()


# ---------------- Fix3：trading_session（full/am/pm）组合时段闸 ----------------
def _session_bar(stock, bar_time):
    """日内 bar（period=1m）——时段闸仅约束日内周期。"""
    b = _bar(stock, "9.3", bar_time)
    b.period = "1m"
    return b


def test_handle_bar_trading_session_pm_skips_morning_then_trades(monkeypatch):
    """Fix3：pm-only 组合——上午（墙钟 10:00）整轮跳过（不落单/不发桥），记 1 条
    trading_session 决策事件；下午 13:30 正常下单。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.trading_session = "pm"
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai", lambda: datetime(2026, 8, 5, 10, 0))
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _session_bar(stock, bar_time))

    assert not any(r.url.path == "/order" for r in rec.requests)  # 不发桥
    db = factory()
    assert db.query(LiveOrder).count() == 0  # 不落单
    evs = db.query(LiveDecisionEvent).filter_by(gate="trading_session").all()
    assert len(evs) == 1 and evs[0].layer == "portfolio" and evs[0].action == "skip"
    assert "交易时段闸" in evs[0].message
    db.close()
    assert q.get_nowait()["type"] == "decision"  # 跳过事件即时推送

    # 下午 13:30（允许窗口）→ 正常下单，无新增 trading_session 事件
    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai", lambda: datetime(2026, 8, 5, 13, 30))
    bar_time = datetime(2026, 8, 5, 13, 30)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _session_bar(stock, bar_time))
    db = factory()
    assert db.query(LiveOrder).count() == 1
    assert db.query(LiveOrder).first().status == "submitted"
    assert db.query(LiveDecisionEvent).filter_by(gate="trading_session").count() == 1
    db.close()


def test_handle_bar_trading_session_skip_event_once_per_day(monkeypatch):
    """Fix3：跳过事件每日首次记 1 条（内存 set 按 (portfolio_id, date)）——同日
    多根 bar 不刷屏，跨日重记。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.trading_session = "am"  # am-only：下午跳过
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )

    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai", lambda: datetime(2026, 8, 5, 13, 30))
    for minute in (30, 31, 32):
        bt = datetime(2026, 8, 5, 13, minute)
        engine.signal_cache = {(1, stock, bt): [{"name": "open_sig", "value": 1}]}
        engine._handle_bar(port, _session_bar(stock, bt))
    db = factory()
    assert db.query(LiveOrder).count() == 0
    assert db.query(LiveDecisionEvent).filter_by(gate="trading_session").count() == 1
    db.close()

    # 跨日首根 bar → 再记 1 条
    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai", lambda: datetime(2026, 8, 6, 13, 30))
    bt = datetime(2026, 8, 6, 13, 30)
    engine.signal_cache = {(1, stock, bt): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _session_bar(stock, bt))
    db = factory()
    assert db.query(LiveDecisionEvent).filter_by(gate="trading_session").count() == 2
    db.close()


def test_handle_bar_trading_session_full_no_filter(monkeypatch):
    """Fix3：full（默认）不筛时段——任意**可交易**墙钟时间照常下单（全局闸已管）。

    P2-5 后 11:30–13:00 午休由全局闸拦截，故这里取 13:30 验证 full 不做额外时段筛选。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai", lambda: datetime(2026, 8, 5, 13, 30))
    bt = datetime(2026, 8, 5, 13, 30)
    engine.signal_cache = {(1, stock, bt): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _session_bar(stock, bt))
    db = factory()
    assert db.query(LiveOrder).count() == 1
    assert db.query(LiveDecisionEvent).filter_by(gate="trading_session").count() == 0
    db.close()


def test_handle_bar_blocks_order_during_lunch_break(monkeypatch):
    """P2-5：11:30–13:00 午休不下单（旧实现只判 09:30–15:00，11:30 边界 bar 会排进 TDX）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai", lambda: datetime(2026, 8, 5, 11, 30))
    bt = datetime(2026, 8, 5, 11, 30)
    engine.signal_cache = {(1, stock, bt): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _session_bar(stock, bt))
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)


# ---------------- Fix5：SELL 回填印花税补记 ----------------
def test_backfill_sell_records_stamp_duty():
    """Fix5：SELL 回填 stamp_duty = 组合 stamp_duty_rate × amount，且 apply_filled
    同口径扣费（虚拟现金回款 = amount - commission - stamp_duty）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.cost_params = {
        "min_commission": Decimal("5"),
        "buy_commission_rate": Decimal("0.00025"),
        "sell_commission_rate": Decimal("0.00025"),
        "stamp_duty_rate": Decimal("0.0005"),
        "slippage": Decimal("0"),
    }
    engine, q = _ss_engine(factory, port)
    # 预置虚拟持仓：SELL 回填 apply 需扣减（缺仓会走「SELL without virtual position」告警路径）
    pos = Position("600000.SH")
    pos.buy(1000, Decimal("9.0"), datetime(2026, 8, 4))
    port.strategies[0].positions["600000.SH"] = pos
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="sell", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=0, filled_price=None, status="submitted",
        signal_name="stop_loss", signal_type="STOP_LOSS",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-s1",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    db.close()

    cash_before = port.account.cash
    db = factory()
    lo = db.query(LiveOrder).filter_by(id=lo_id).first()
    deals = [{
        "order_ref": "ref-s1", "volume": 1000, "amount": 9000,
        "commission": 0, "trade_date": "20260805", "trade_time": "100100",
    }]
    engine._backfill_order(db, lo, deals)
    db.commit()
    trade = db.query(LiveTrade).filter_by(live_order_id=lo_id).first()
    assert trade.stamp_duty == Decimal("9000") * Decimal("0.0005")  # 4.50
    # 附加核查：桥 deals 的 commission=0 → 按组合费率补算，命中最低佣金 5 元
    assert trade.commission == Decimal("5.00")
    db.close()
    # apply_filled_trade 同口径：现金回款扣佣金 + 印花税
    assert port.account.cash == (
        cash_before + Decimal("9000") - Decimal("5.00") - Decimal("4.50"))
    # 持仓清零（SELL apply 正常）
    assert port.strategies[0].positions["600000.SH"].quantity == 0


def test_backfill_buy_keeps_stamp_duty_zero():
    """Fix5：买入回填印花税保持 0（仅 SELL 补记）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.cost_params = {
        "min_commission": Decimal("5"),
        "buy_commission_rate": Decimal("0.00025"),
        "sell_commission_rate": Decimal("0.00025"),
        "stamp_duty_rate": Decimal("0.0005"),
        "slippage": Decimal("0"),
    }
    engine, q = _ss_engine(factory, port)
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=0, filled_price=None, status="submitted",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-b1",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    db.close()

    db = factory()
    lo = db.query(LiveOrder).filter_by(id=lo_id).first()
    deals = [{
        "order_ref": "ref-b1", "volume": 1000, "amount": 9000,
        "commission": 0, "trade_date": "20260805", "trade_time": "100100",
    }]
    engine._backfill_order(db, lo, deals)
    db.commit()
    trade = db.query(LiveTrade).filter_by(live_order_id=lo_id).first()
    assert trade.stamp_duty == Decimal("0")
    # 附加核查：买入佣金同样在桥未给（0）时按费率补算（命中最低佣金 5 元）
    assert trade.commission == Decimal("5.00")
    db.close()


def test_backfill_keeps_bridge_commission_when_present():
    """附加核查：桥给了真实佣金（>0）→ 原样入账，不按费率覆盖。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.cost_params = {
        "min_commission": Decimal("5"),
        "buy_commission_rate": Decimal("0.00025"),
        "sell_commission_rate": Decimal("0.00025"),
        "stamp_duty_rate": Decimal("0.0005"),
    }
    engine, q = _ss_engine(factory, port)
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=0, filled_price=None, status="submitted",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-b2",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    deals = [{
        "order_ref": "ref-b2", "volume": 1000, "amount": 9000,
        "commission": 12.34, "trade_date": "20260805", "trade_time": "100100",
    }]
    engine._backfill_order(db, lo, deals)
    db.commit()
    trade = db.query(LiveTrade).filter_by(live_order_id=lo_id).first()
    assert trade.commission == Decimal("12.34")
    db.close()


def test_handle_bar_emits_risk_on_max_drawdown_trigger():
    """B5：max_drawdown 熔断触发（计数递增）→ risk(max_drawdown) 事件；计数未变不重发。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    # 下跌 bar（close 5）亏损 44% < 50% 止损线、无止盈 → 不产生风控 SELL，聚焦熔断事件
    port.strategies[0].strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.5"), take_profit_ratio=Decimal("1.0"),
        trailing_stop_ratio=Decimal("0"),
    )
    stock = "600000.SH"
    pos = Position(stock)
    pos.buy(1000, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))
    port.strategies[0].positions[stock] = pos
    port.account.cash = Decimal("0")
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    # bar1: close=9 → 总市值 9000 建峰，无熔断无信号
    engine._handle_bar(port, _bar(stock, "9", datetime(2026, 8, 5, 10, 0)))
    assert q.empty()

    # bar2: close=5 → 回撤 44% > 20% → 熔断，计数+1 → risk 事件
    engine._handle_bar(port, _bar(stock, "5", datetime(2026, 8, 5, 10, 1)))
    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "max_drawdown"
    assert ev["triggered"] is True
    assert ev["count"] == 1
    assert ev["portfolio_id"] == 1
    # 决策闸门事件：max_drawdown 熔断 halt 随本 bar drain 推一条 decision
    extra = _drain_queue(q)
    assert len(extra) == 1 and extra[0]["type"] == "decision"
    assert extra[0]["gate"] == "max_drawdown" and extra[0]["action"] == "halt"


def test_backfill_emits_order_filled_trade_position():
    """B5：成交回报回填 filled → order(filled) + trade + position 事件（真实价/量）。"""
    factory, _ = _db_factory()
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=0, filled_price=None, status="submitted",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref1",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    db.close()

    port, _ = _portfolio_single()
    engine, q = _ss_engine(factory, port)

    db = factory()
    lo = db.query(LiveOrder).filter_by(id=lo_id).first()
    deals = [{
        "order_ref": "ref1", "volume": 1000, "amount": 9000,
        "commission": 0, "trade_date": "20260805", "trade_time": "100100",
    }]
    engine._backfill_order(db, lo, deals)
    db.commit()
    db.close()

    events = [q.get_nowait(), q.get_nowait(), q.get_nowait()]
    assert events[0]["type"] == "order" and events[0]["status"] == "filled"
    assert events[0]["order_id"] == lo_id
    assert events[0]["filled_quantity"] == 1000
    assert events[0]["filled_price"] == 9.0
    assert events[1]["type"] == "trade"
    assert events[1]["portfolio_id"] == 1
    assert events[1]["trade_id"] is not None
    assert events[1]["stock_code"] == "600000.SH"
    assert events[1]["trade_type"] == "BUY"
    assert events[1]["quantity"] == 1000
    assert events[1]["price"] == 9.0
    assert events[1]["amount"] == 9000
    assert events[2]["type"] == "position"
    assert events[2]["portfolio_id"] == 1
    assert events[2]["strategy_id"] == 1
    assert events[2]["stock_code"] == "600000.SH"
    assert events[2]["quantity"] == 1000
    assert events[2]["avg_cost"] == 9.0
    assert q.empty()


def test_maybe_daily_close_emits_risk_on_daily_loss(monkeypatch):
    """B5：日内亏损触发 → risk(daily_loss) 事件，当日暂停新开仓。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    port.account.cash = Decimal("90000")
    port.risk_manager.prev_close_value = Decimal("100000")  # 昨日收盘基准
    engine, q = _ss_engine(factory, port)

    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 5, 14, 30)
    monkeypatch.setattr("core.engine.live.timing.datetime", _FakeDateTime)

    engine._maybe_daily_close()

    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "daily_loss"
    assert ev["triggered"] is True
    assert ev["portfolio_id"] == 1
    assert port.risk_manager.daily_pause_active is True


def test_daily_close_uses_market_value_does_not_false_trigger(monkeypatch):
    """R1：日终 total 用市值口径（现金 + 持仓市值），有持仓不误触 daily_loss。

    持仓 1000 股 avg_cost=10，昨收（prev_close_value）市值口径 95000（现金 8 万 +
    昨价 15×1000）。今市值口径 total=80000+11×1000=91000 → pnl=-4000（-4% < 5%
    不触发）。旧成本口径 total=80000+10×1000=90000 → pnl=-5000（-5% = 阈值，
    会误触）。断言市值口径下不暂停。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    port.account.cash = Decimal("80000")
    code = "600000.SH"
    pos = Position(code)
    pos.apply_trade(TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=code,
        trade_type=TradeType.BUY, price=Decimal("10"), quantity=1000,
        amount=Decimal("10000"), commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=datetime(2026, 8, 3, 9, 30),
    ))
    ctx.positions[code] = pos
    port._price_snapshot[code] = Decimal("11")  # 今市值 11×1000
    port.risk_manager.prev_close_value = Decimal("95000")  # 80000 + 15×1000
    engine, _ = _ss_engine(factory, port)

    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 8, 5, 14, 30)
    monkeypatch.setattr("core.engine.live.timing.datetime", _FakeDateTime)

    engine._maybe_daily_close()

    assert port.risk_manager.daily_pause_active is False


def test_now_shanghai_returns_shanghai_wall_clock(monkeypatch):
    """#23：now_shanghai() 返回上海时间（naive），不依赖本机时区。

    本机 datetime.now() 返回 UTC 06:30 时，now_shanghai() 应返回 14:30（+8）。
    证明日终 (14:30) 判定按上海时间，而非本机时间——Core 部署 UTC 服务器时不哑火。
    """
    from core.engine.live_engine import now_shanghai, _CST
    _fixed_utc = datetime(2026, 8, 10, 6, 30)  # 视为 UTC 06:30

    class _FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            # 模拟真实 datetime.now(tz=...) 语义：
            # 传 tz → 返回该 UTC 瞬时在 tz 的墙上时间（aware）。
            # _fixed_utc 视为 UTC，目标 tz 偏移 +8h → 上海 14:30。
            if tz is not None:
                return (_fixed_utc + tz.utcoffset(None)).replace(tzinfo=tz)
            return _fixed_utc
    monkeypatch.setattr("core.engine.live.timing.datetime", _FakeDateTime)

    # UTC 06:30 → 上海 14:30（+8）；now_shanghai 剥 tz 后仍 14:30
    assert now_shanghai() == datetime(2026, 8, 10, 14, 30)
    # 顺带验证 _CST 是 +8
    assert _CST.utcoffset(None) == timedelta(hours=8)


def test_maybe_daily_close_uses_shanghai_time(monkeypatch):
    """#23：_maybe_daily_close 日终判定按上海时间（14:30 阈值），可注入 now。

    now=14:30 → 触发 update_daily（_last_daily_date 置当日）；
    now=14:29 → 未触发（_last_daily_date 仍 None/初始）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    engine, _ = _ss_engine(factory, port)

    # 14:29 → 不触发
    engine._last_daily_date = None
    engine._maybe_daily_close(now=datetime(2026, 8, 10, 14, 29))
    assert engine._last_daily_date is None

    # 14:30 → 触发
    engine._maybe_daily_close(now=datetime(2026, 8, 10, 14, 30))
    assert engine._last_daily_date == datetime(2026, 8, 10).date()


# ---- 14:30 钩子交易日历守卫（#145）----
def test_maybe_daily_close_skips_non_trading_day():
    """非交易日（周末/节假日）14:30 不调 update_daily（不用陈旧 bar 误估值/误触熔断）。"""
    from core.engine.trading_calendar import TradingCalendar
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    engine, _ = _ss_engine(factory, port)
    # 注入只认 08-24 为交易日的日历：08-22 周六、10-01 国庆都不在
    engine._calendar = TradingCalendar(lambda: [date(2026, 8, 24)])
    engine._last_daily_date = None

    # 国庆 14:30 → 非交易日，不触发
    engine._maybe_daily_close(now=datetime(2026, 10, 1, 14, 30))
    assert engine._last_daily_date is None

    # 周六 14:30 → 非交易日，不触发
    engine._maybe_daily_close(now=datetime(2026, 8, 22, 14, 30))
    assert engine._last_daily_date is None

    # 交易日 14:30 → 正常触发
    engine._maybe_daily_close(now=datetime(2026, 8, 24, 14, 30))
    assert engine._last_daily_date == date(2026, 8, 24)


def test_maybe_daily_bars_skips_non_trading_day():
    """非交易日 14:30 不驱动日线（不拉桥 /quote?period=1d、不触发 1d 策略信号）。"""
    from core.engine.trading_calendar import TradingCalendar
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    rec = _Recorder(respond=_respond_quote_bars(stock, [
        {"stime": "20261001000000", "open": 9.0, "high": 9.3, "low": 9.0,
         "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    engine._calendar = TradingCalendar(lambda: [date(2026, 8, 24)])

    engine._maybe_daily_bars(now=datetime(2026, 10, 1, 15, 5))  # 国庆

    db = factory()
    assert db.query(LiveOrder).count() == 0  # 非交易日不落单
    db.close()
    # 没拉日线快照
    assert not any(r.url.path == "/quote" and "period=1d" in str(r.url) for r in rec.requests)


# ---- 收盘清扫 _maybe_close_sweep（15:05 确定性兜底，#146）----
def _sweep_engine(factory, port, rec, trading_day=date(2026, 8, 24)):
    """构造带交易日历的引擎（只认 trading_day 为交易日）。"""
    from core.engine.trading_calendar import TradingCalendar
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    engine._calendar = TradingCalendar(lambda: [trading_day])
    return engine, disp


def test_close_sweep_skips_before_1505():
    """15:05 前不执行清扫。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    _submitted_order(db)
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 4))

    db = factory()
    lo = db.query(LiveOrder).first()
    assert lo.status == "submitted"  # 未清扫
    assert engine._last_close_sweep_date is None
    db.close()


def test_close_sweep_skips_non_trading_day():
    """非交易日 15:05 不清扫（周末/假期）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    _submitted_order(db)
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 10, 1, 15, 5))  # 国庆

    db = factory()
    assert db.query(LiveOrder).first().status == "submitted"
    db.close()


def test_close_sweep_unfilled_order_marked_canceled():
    """收盘仍 submitted、无成交、/orders 缺席 → A 股收盘未成交=撤单，标 canceled。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])  # 实时表已无此单
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "canceled"
    assert "close sweep" in (lo2.error_message or "")
    # 无成交 → 不写 LiveTrade、不 apply
    assert db.query(LiveTrade).count() == 0
    db.close()
    assert ctx.positions.get(stock) is None
    assert port.account.cash == Decimal("100000")
    assert engine._last_close_sweep_date == date(2026, 8, 24)


def test_close_sweep_partial_fill_applied_then_canceled():
    """部分成交：先回填成交 apply 落持仓，剩余 canceled（不丢成交）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-part-1",
        "remark": _TEST_REMARK, "status": 55, "traded_volume": 300,  # 部成在途
    }], deals=[{
        "order_ref": "ref-part-1", "price": 9.2, "volume": 300,
        "amount": 2760.0, "commission": 0.69, "trade_time": "145500",
        "trade_date": "20260824",
    }])
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "canceled"          # 剩余收盘撤
    assert int(lo2.filled_quantity) == 300   # 成交部分保留
    assert db.query(LiveTrade).count() == 1
    db.close()
    # 部分成交已 apply：持仓 300，现金扣 2760+0.69
    assert ctx.positions[stock].quantity == 300
    assert port.account.cash == Decimal("100000") - (Decimal("2760.0") + Decimal("0.69"))


def test_close_sweep_terminal_status_54_canceled():
    """/orders 已报 54 全撤 → 终态同步标 canceled。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-can-1",
        "remark": _TEST_REMARK, "status": 54, "traded_volume": 0,
    }], deals=[])
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    lo = _submitted_order(db)
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))

    db = factory()
    assert db.get(LiveOrder, lo.id).status == "canceled"
    db.close()


def test_close_sweep_junk_57_marked_rejected():
    """/orders 报 57 废单 → rejected（语义：柜台拒单非我方撤）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-junk-1",
        "remark": _TEST_REMARK, "status": 57, "traded_volume": 0,
    }], deals=[])
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    lo = _submitted_order(db)
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))

    db = factory()
    assert db.get(LiveOrder, lo.id).status == "rejected"
    db.close()


def test_close_sweep_defers_when_traded_volume_ahead_of_filled():
    """/orders 报 traded_volume > filled_quantity（/deals 回报滞后）→ 延后不 cancel，
    不标记完成，等下轮 /deals 追上再清扫——绝不丢真实成交。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    # /orders 显示已成交 300，但 /deals 还没回报（空）——成交价/量/金额未回填齐
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-lag-1",
        "remark": _TEST_REMARK, "status": 55, "traded_volume": 300,
    }], deals=[])
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    lo.order_ref = "ref-lag-1"  # 已匹配到 ref，才能在 /orders 里按 ref 查到
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "submitted"          # 未 cancel（延后）
    assert int(lo2.filled_quantity) == 0       # /deals 没回报，不凭空 apply
    db.close()
    assert ctx.positions.get(stock) is None    # 未落持仓
    assert engine._last_close_sweep_date is None  # 不标记完成，下轮 60s 重试


def test_close_sweep_deferred_then_completes_after_deals_catch_up():
    """延后场景：下轮 /deals 追上回填成交 → apply 成交、剩余 canceled、标记完成。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-lag-2",
        "remark": _TEST_REMARK, "status": 55, "traded_volume": 300,
    }], deals=[{
        "order_ref": "ref-lag-2", "price": 9.2, "volume": 300,
        "amount": 2760.0, "commission": 0.69, "trade_time": "150200",
        "trade_date": "20260824",
    }])
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    lo.order_ref = "ref-lag-2"
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 6))

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "canceled"
    assert int(lo2.filled_quantity) == 300
    db.close()
    assert ctx.positions[stock].quantity == 300  # 成交已 apply
    assert engine._last_close_sweep_date == date(2026, 8, 24)


def test_close_sweep_idempotent_same_day():
    """同日重复清扫：第二次 no-op（不重复处理，_last_close_sweep_date 幂等）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])
    engine, _ = _sweep_engine(factory, port, rec)
    db = factory()
    _submitted_order(db)
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))
    # 再插一笔新 submitted（模拟清扫后又有单残留），同日第二次不应处理
    db = factory()
    lo2 = _submitted_order(db, bridge_order_id="b" * 32)
    db.commit()
    db.close()
    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 10))

    db = factory()
    assert db.get(LiveOrder, lo2.id).status == "submitted"  # 同日二次不清扫
    db.close()


def _ss_plain_engine(factory, ping_interval=0.05):
    """B5 辅助：无预订阅队列的引擎（stream_events 测试用，断言退订精确）。"""
    port, _ = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine._stream_ping_interval = ping_interval
    return engine


def test_stream_events_yields_events_then_ping_when_idle():
    """B5：stream_events 先 yield 事件，空闲超 ping 间隔 → yield ping 心跳；结束退订。"""
    factory, _ = _db_factory()
    engine = _ss_plain_engine(factory)
    engine._running = True

    async def drive():
        agen = engine.stream_events()
        it = agen.__aiter__()
        # 首轮 __anext__ 让生成器订阅（队列空开始等事件）；让出后 emit 才广播进队列
        first_task = asyncio.create_task(it.__anext__())
        await asyncio.sleep(0)
        engine._emit("signal", {"portfolio_id": 1, "strategy_id": 1})
        first = await first_task
        assert first["type"] == "signal"
        assert first["portfolio_id"] == 1
        second = await it.__anext__()  # 空闲 0.05s → ping
        assert second["type"] == "ping"
        assert "time" in second
        await agen.aclose()
        assert engine._stream_subscribers == []  # 退订

    asyncio.run(drive())


def test_stream_events_ends_when_engine_not_running():
    """B5：引擎未运行 → stream_events 无事件即结束并退订（端点侧转 ping-only 保活）。"""
    factory, _ = _db_factory()
    engine = _ss_plain_engine(factory)

    async def drive():
        events = [ev async for ev in engine.stream_events()]
        assert events == []
        assert engine._stream_subscribers == []

    asyncio.run(drive())


# ===========================================================================
# 审计 #3：异步循环内同步 I/O — _loop/_deals_loop 阻塞调用走线程池，事件循环不冻结
# ===========================================================================
# 现状（修复前）：_loop/_deals_loop 声明 async def，但内部直接调同步阻塞 I/O
# （dispatcher.heartbeat / bar_poller.poll / _poll_deals，全用同步 httpx.Client）。
# FastAPI 事件循环被这些阻塞调用占住期间，所有 HTTP 请求与 SSE 流冻结。
#
# 修复（方案 B）：整轮 tick 体通过 loop.run_in_executor 丢到线程池执行；
# 单 worker executor 让 _loop 与 _deals_loop 的 tick 串行（共享 _pending_orders /
# positions / signal_cache 无并发竞争）；_emit 跨线程用 call_soon_threadsafe
# 回到事件线程 put_nowait（asyncio.Queue 非线程安全）。

def test_loop_tick_runs_in_worker_thread_not_event_loop():
    """#3：_loop 整轮 tick 在线程池 worker 执行，事件循环线程不被阻塞占用。

    tick 内同步 I/O（heartbeat/poll）发生在 worker 线程；事件循环线程（drive 协程
    所在线程）能并发推进 —— 两者线程 id 必不同。
    """
    import asyncio
    import threading

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, poll_interval=0.01,
    )
    tick_threads = set()

    orig_heartbeat = disp.heartbeat

    def spy_heartbeat():
        tid = threading.get_ident()
        tick_threads.add(tid)
        return orig_heartbeat()

    disp.heartbeat = spy_heartbeat

    async def drive():
        await engine.start()
        await asyncio.sleep(0.06)  # 至少跑几轮 tick
        await engine.stop()

    loop_thread = threading.get_ident()
    asyncio.run(drive())

    assert tick_threads, "heartbeat 未被调用"
    assert loop_thread not in tick_threads, (
        "tick 在事件循环线程执行——同步 I/O 仍会冻结事件循环"
    )


def test_deals_loop_tick_runs_in_worker_thread():
    """#3：_deals_loop 的 _poll_deals 也在 worker 线程执行（同 _loop，不阻塞事件循环）。"""
    import asyncio
    import threading

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, deals_poll_interval=0.01,
    )
    poll_threads = set()

    def spy_poll_deals():
        poll_threads.add(threading.get_ident())
        return None

    engine._poll_deals = spy_poll_deals

    async def drive():
        await engine.start()
        await asyncio.sleep(0.05)
        await engine.stop()

    loop_thread = threading.get_ident()
    asyncio.run(drive())

    assert poll_threads, "_poll_deals 未被调用"
    assert loop_thread not in poll_threads, (
        "_poll_deals 在事件循环线程执行——同步 I/O 仍会冻结事件循环"
    )


def test_each_loop_ticks_serialize_within_own_worker():
    """I-8：主循环与 deals 循环各自单 worker 池——各池内 tick 严格串行（不重叠）。

    共享资源（TQ 锁 get_tdx_lock）由锁串行；两池独立后 deals 5s 节拍不再被
    60s 主 tick 饿死（原共享单池的跨池串行语义由锁接管）。
    """
    import asyncio
    import threading

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        poll_interval=0.005, deals_poll_interval=0.005,
    )

    lock = threading.Lock()
    main_active = {"n": 0, "max": 0}
    deals_active = {"n": 0, "max": 0}
    import time

    def _track(fn, state):
        def _w():
            with lock:
                state["n"] += 1
                state["max"] = max(state["max"], state["n"])
            try:
                time.sleep(0.01)  # 制造重叠窗口
                return fn() if fn is not None else None
            finally:
                with lock:
                    state["n"] -= 1
        return _w

    engine._poll_deals = _track(None, deals_active)
    orig_heartbeat = disp.heartbeat
    disp.heartbeat = _track(orig_heartbeat, main_active)

    async def drive():
        await engine.start()
        await asyncio.sleep(0.1)  # 5ms × 100ms → 两循环各约 20 轮
        await engine.stop()

    asyncio.run(drive())

    assert main_active["max"] <= 1, (
        "主循环 tick 并发执行（max=%d）——主池单 worker 串行被破坏" % main_active["max"]
    )
    assert deals_active["max"] <= 1, (
        "deals 循环 tick 并发执行（max=%d）——deals 池单 worker 串行被破坏"
        % deals_active["max"]
    )


def test_emit_from_worker_thread_lands_on_sse_queue():
    """#3：worker 线程内 _emit 经 call_soon_threadsafe 回到事件线程 put_nowait。

    _loop tick 在 worker 线程触发下单 → _emit 在 worker 线程被调；
    SSE 队列由事件线程持有（asyncio.Queue 非线程安全），必须回事件线程投递。
    直接在事件线程 await 一下让 scheduled 回调落地，再断言队列收到事件。
    """
    import asyncio

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)  # 完成的 bar stime = 10:00
    # 状态化 /quote：每次拉取多返回一根 bar —— 轮1 [bar0] 建基线，轮2 [bar0,bar1]
    # → bar0 退居第二 → 完成 → 触发 _on_bar(bar_time=10:00) → _handle_bar → _emit
    all_bars = _quote_bars(stock, [datetime(2026, 8, 5, 10, 0), datetime(2026, 8, 5, 10, 1)])
    poll_count = {"n": 0}

    def growing_quote(request):
        path = request.url.path
        if path == "/quote":
            poll_count["n"] += 1
            n = min(poll_count["n"], len(all_bars))
            return httpx.Response(200, json={"ok": True, "data": {stock: all_bars[:n]}})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": []})
        if path == "/deals":
            return httpx.Response(200, json={"ok": True, "data": []})
        return httpx.Response(404, json={"ok": False})

    rec = _Recorder(respond=growing_quote)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, poll_interval=0.01,
        # 本用例验证 _emit 跨线程送达，依赖 poll 在任意墙钟时刻都跑；
        # 显式关闭行情轮询窗口，保持用例与「当前是否盘中」无关（收盘后亦可跑）。
        quote_polling={"enabled": False},
    )
    # 预置信号让 _handle_bar 在 worker 线程 emit signal+order
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    async def drive():
        engine._stream_subscribers.append(asyncio.Queue(maxsize=200))
        await engine.start()
        # 多等几轮：建基线(轮1) → 完成bar触发(轮2+) → call_soon_threadsafe 调度落地
        await asyncio.sleep(0.15)
        await engine.stop()
        q = engine._stream_subscribers[0]
        events = []
        while not q.empty():
            events.append(q.get_nowait())
        return events

    events = asyncio.run(drive())
    types = [e["type"] for e in events]
    assert "signal" in types, "worker 线程 _emit 未送达 SSE 队列（call_soon_threadsafe 缺失）"
    assert "order" in types


def test_emit_direct_call_still_synchronous_without_running_loop():
    """#3 回归：无事件循环时直接调 _emit 仍同步入队（保留既有同步测试行为）。

    test_emit_pushes_event_to_all_subscribers 等直接调 _emit 后立即 get_nowait；
    call_soon_threadsafe 改造不得破坏此同步路径（无 loop captured → 直接 put_nowait）。
    """
    import asyncio

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    # 无 running loop、未 start（_loop_ref 为 None）→ 直接 put_nowait
    engine._emit("signal", {"portfolio_id": 1, "strategy_id": 1})

    assert q.get_nowait() == {"type": "signal", "portfolio_id": 1, "strategy_id": 1}


def test_executor_single_worker():
    """#3：LiveEngine 用单 worker 线程池（max_workers=1）串行两循环的 tick。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    assert engine._executor is not None
    assert engine._executor._max_workers == 1


def test_loop_offline_to_online_still_resets_baseline_after_thread_offload():
    """#3 回归：tick 走线程池后，快照通道离线→在线转场仍调 reset_baseline。

    同上：可回补的桥走 P2-6 缺口补派，本用例只覆盖快照通道的基线重建路径。
    """
    import asyncio

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"

    class FlakyPing:
        def __init__(self):
            self.fail_remaining = 1

        def handler(self, request):
            path = request.url.path
            if path == "/ping":
                if self.fail_remaining > 0:
                    self.fail_remaining -= 1
                    raise httpx.ConnectError("connection refused")
                return httpx.Response(200, json={"ok": True})
            if path == "/quote":
                return httpx.Response(200, json={"ok": True, "data": {}})
            if path == "/order":
                return httpx.Response(200, json={"ok": True})
            return httpx.Response(404, json={"ok": False})

    rec = FlakyPing()
    disp, _ = _make_dispatcher(rec)
    disp.supports_historical_quote = False  # 快照通道：不补派，只能重建基线
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    reset_calls = []
    orig = poller.reset_baseline
    poller.reset_baseline = lambda: (reset_calls.append(1), orig())[1]

    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, poll_interval=0.01,
    )

    async def run_a_bit():
        await engine.start()
        # 审计 #37：原固定 sleep(0.1) 偶发抖动，同 :1347 测试——改有界轮询等转场完成。
        deadline = asyncio.get_event_loop().time() + 0.5
        while len(reset_calls) < 1 and asyncio.get_event_loop().time() < deadline:
            await asyncio.sleep(0.005)
        await engine.stop()

    asyncio.run(run_a_bit())

    assert engine.bridge_online is True
    assert len(reset_calls) == 1


# ---------------- 收盘后信号转 deferred（方案 A，2026-11-13）----------------
def test_handle_bar_after_close_defers_order_instead_of_dropping():
    """方案 A：bar_time >= 15:00（收盘）→ 信号仍 emit，订单不丢弃而是落 deferred。

    旧行为直接丢弃，会让收盘 bar 的止损/止盈/CLOSE 静默不执行、留仓过夜；回测里
    同一信号本就在次日首根 bar 开盘成交。现改为进既有 deferred 链路（次日 09:31
    首根 1m bar 按当日开盘价限价放行），与回测同口径。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 15, 1)  # 收盘后
    engine, q = _ss_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.0", bar_time)
    bar.period = "5m"

    engine._handle_bar(port, bar)

    # 信号已推送（用户能看到收盘后的信号意图）
    events = []
    while not q.empty():
        events.append(q.get_nowait())
    assert any(e["type"] == "signal" and e["stock_code"] == stock for e in events)
    # 不即时落 submitted、不发 order 事件；改为 deferred 行 + 内存队列
    assert not any(e["type"] == "order" for e in events)
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "deferred"
    db.close()
    assert len(engine._signal_bar_deferred) == 1
    deferred = engine._signal_bar_deferred[0]
    assert deferred.signal_bar_date == date(2026, 8, 5)
    assert deferred.bar_time == bar_time  # 信号 bar 时间保持（放行时才改写）


def test_handle_bar_before_close_places_order_normally():
    """修复2回归：bar_time < 15:00（盘中）→ 守卫不拦，正常落 LiveOrder(submitted)。

    守卫不能误伤盘中合法 bar（含 14:59 的 bar：主循环 60s 延迟处理时其 bar_time 仍 <15:00）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 14, 59)  # 盘中最后一分钟
    engine, q = _ss_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1
    assert orders[0].status == "submitted"
    db.close()


def test_after_close_defer_released_next_day_at_first_1m_open(monkeypatch):
    """方案 A：收盘 deferred 单在次日首根 1m bar 放行，限价=当日首根 1m 开盘价。"""
    from core.engine.live.snapshot_bars import SnapshotBarSynthesizer

    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    day1 = datetime(2026, 8, 5, 15, 1)
    day2 = datetime(2026, 8, 6, 9, 31)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 5), date(2026, 8, 6)], stock)
    engine.signal_cache = {(1, stock, day1): [{"name": "open_sig", "value": 1}]}
    bar1 = _bar(stock, "9.20", day1)
    bar1.period = "5m"
    engine._handle_bar(port, bar1)
    assert len(engine._signal_bar_deferred) == 1

    # 次日：首根 1m bar（09:30）open=9.50，次根 9.60
    monkeypatch.setattr("core.engine.live_engine.now_shanghai",
                        lambda: datetime(2026, 8, 6, 9, 31, 2))
    snaps = {stock: {"Now": "9.50", "Volume": "1000"}}
    synth = SnapshotBarSynthesizer(
        [stock],
        fetch_snapshot=lambda codes: {c: dict(snaps[c]) for c in codes if c in snaps},
        in_session=lambda dt: True,
    )
    synth.sample(datetime(2026, 8, 6, 9, 30, 5))
    engine._snapshot_synth = synth

    engine.signal_cache = {}
    bar2 = _bar(stock, "9.60", day2)
    bar2.period = "1m"
    engine._handle_bar(port, bar2)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "submitted"
    assert orders[0].bar_time == day2          # 决策时间戳 = 放行 bar
    assert Decimal(str(orders[0].price)) == Decimal("9.50")  # 首根 1m open
    db.close()
    assert engine._signal_bar_deferred == []
    assert any(r.url.path == "/order" for r in rec.requests)


def test_after_close_defer_survives_restart_recover_and_releases(monkeypatch):
    """方案 A：收盘 deferred 行可跨重启由 recover 重建队列，次日仍能放行。"""
    from core.engine.bar_poller import BarPoller

    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    day1 = datetime(2026, 8, 5, 15, 1)
    day2 = datetime(2026, 8, 6, 9, 31)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 5), date(2026, 8, 6)], stock)
    engine.signal_cache = {(1, stock, day1): [{"name": "open_sig", "value": 1}]}
    bar1 = _bar(stock, "9.20", day1)
    bar1.period = "5m"
    engine._handle_bar(port, bar1)

    # 模拟重启：新引擎从 DB 足迹重建内存队列（时钟固定到次日，避免新鲜度清扫）
    monkeypatch.setattr("core.engine.live_engine.now_shanghai",
                        lambda: datetime(2026, 8, 6, 9, 31, 2))
    port2, _ = _portfolio_single(period="5m")
    engine2 = _calendar_engine(
        factory, port2, disp, [date(2026, 8, 5), date(2026, 8, 6)], stock)
    db = factory()
    engine2.recover(db)
    db.close()
    assert len(engine2._signal_bar_deferred) == 1

    engine2.signal_cache = {}
    bar2 = _bar(stock, "9.40", day2)
    bar2.period = "1m"
    engine2._handle_bar(port2, bar2)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "submitted"
    db.close()


def test_after_close_defer_dedupes_same_bar_replay():
    """方案 A：同一 15:00 bar 重放不产生第二行 deferred（六元组去重）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 15, 1)
    engine, _ = _ss_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.0", bar_time)
    bar.period = "5m"

    engine._handle_bar(port, bar)
    engine._handle_bar(port, bar)

    db = factory()
    rows = db.query(LiveOrder).filter(LiveOrder.status == "deferred").all()
    assert len(rows) == 1
    db.close()
    assert len(engine._signal_bar_deferred) == 1


def test_after_close_defers_even_when_risk_timing_immediate():
    """方案 A：risk_timing=immediate 也不能绕过收盘闸在 15:00 直接报单。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="5m")
    ctx.risk_timing = "immediate"
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 15, 1)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 5)], stock)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.0", bar_time)
    bar.period = "5m"

    engine._handle_bar(port, bar)

    db = factory()
    rows = db.query(LiveOrder).filter(LiveOrder.status == "deferred").all()
    assert len(rows) == 1
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)  # 不发桥


def test_after_close_deferred_minute_order_skips_pre_open_auction():
    """方案 A：15:00 分钟单不进 09:15–09:20 集合竞价，留给 09:31 首根 1m 路径。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    day1 = datetime(2026, 8, 5, 15, 1)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 5), date(2026, 8, 6)], stock)
    engine.signal_cache = {(1, stock, day1): [{"name": "open_sig", "value": 1}]}
    bar1 = _bar(stock, "9.20", day1)
    bar1.period = "5m"
    engine._handle_bar(port, bar1)

    # 次日 09:16 盘前窗口：分钟单必须被过滤掉
    engine._pre_open_release(port, datetime(2026, 8, 6, 9, 16))

    assert len(engine._signal_bar_deferred) == 1  # 仍在队列，未被摘走
    assert not any(r.url.path == "/order" for r in rec.requests)  # 未报出
    db = factory()
    assert db.query(LiveDecisionEvent).filter_by(
        gate="pre_open_auction").count() == 0
    db.close()


def test_after_close_defer_records_gate_action():
    """方案 A：收盘 defer 的决策事件 gate=after_close_defer / action=defer。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 15, 1)
    engine, _ = _ss_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.0", bar_time)
    bar.period = "5m"

    engine._handle_bar(port, bar)

    db = factory()
    evs = db.query(LiveDecisionEvent).filter_by(gate="after_close_defer").all()
    assert len(evs) == 1
    assert evs[0].action == "defer"
    assert evs[0].layer == "live_gate"
    assert evs[0].stock_code == stock
    db.close()


# ---------------- 桥到达确认 → 受理即丢弃即时 rejected（修复1）----------------
def test_handle_bar_bridge_dropped_order_marks_rejected_immediately():
    """修复1：桥 passorder 受理但回查无委托记录（受理即丢弃）→ ok:false + error
    "no order record (dropped)" → Core 走既有 BridgeOrderRejected 路径即时标 rejected。

    根因：真机 2026-08-14 id36-39，桥 passorder result=0（受理）但 QMT 未生成委托记录，
    Core 轮询 /orders 找不到 order_ref，180s 后才标 rejected。修复后桥 _confirm_order_arrival
    回查 ORDER 表无记录 → 返回 ok:false，Core 即时 rejected（error 含 "no order record"），
    不再等 180s 超时。Core 侧零改动——接 dispatcher 既有 BridgeOrderRejected 路径。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)

    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={
                "ok": False,
                "error": "passorder accepted but no order record (dropped)",
            })
        return httpx.Response(200, json={"ok": True})

    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))

    events = [q.get_nowait(), q.get_nowait(), q.get_nowait()]
    assert events[0]["type"] == "signal"
    assert events[1]["type"] == "order" and events[1]["status"] == "submitted"
    # 即时回 deferred（不等 180s 超时），error 回显桥侧到达确认失败原因；
    # P1-3：桥受理后未落柜台 = 没成交，重排安全 → 可重排而非终态
    assert events[2]["type"] == "order" and events[2]["status"] == "deferred"
    assert "no order record" in events[2]["error_message"]
    # 决策闸门事件：桥受理即丢弃走 BridgeOrderRejected → bridge_rejected
    extra = _drain_queue(q)
    risk_evs = [e for e in extra if e["type"] == "risk"]
    assert risk_evs and risk_evs[0]["gate"] == "bridge_rejected"
    assert risk_evs[0]["action"] == "requeue"
    dec = [e for e in extra if e["type"] == "decision"]
    assert len(dec) == 1 and dec[0]["gate"] == "bridge_rejected"
    # DB 落库：submitted → deferred（带重排次数），无残留 pending
    db = factory()
    lo = db.query(LiveOrder).filter(LiveOrder.status == "deferred").first()
    assert lo is not None
    assert "no order record" in lo.error_message
    assert "deferred_retry:n=1" in lo.error_message
    db.close()


# ---------------- 超时检查移主循环（修复3）----------------
def test_tick_main_expires_stale_order_without_order_ref():
    """修复3：order_ref 始终匹配不到且超过 180s 的陈旧 submitted 单 → _tick_main 保持 pending。

    根因：超时检查原在 _poll_deals（deals 循环 5s 节拍），但 deals 循环被 60s 主循环饿死
    （单 worker 串行，主循环拉 ~73 只行情耗 50-57s/轮），180s 超时实际 440s 才生效。
    修复后 _tick_main 每轮调 _expire_stale_orders，超时检查随主循环 60s 节拍跑。
    直接调 _tick_main 验证（先例 test_offline_recovery_clears_preheat_cache:2539）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])  # 桥侧无任何匹配委托
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    # 注入"今日非交易日"日历：隔离 expire 路径——_maybe_daily_close/bars 与
    # _maybe_close_sweep 都因非交易日提前返回，避免收盘后（墙钟≥15:05）清扫把
    # fresh 单标 canceled 污染本测试（时间敏感，收盘后跑会误失败）。
    # 覆盖范围含今日但不含今日（日历对"覆盖外日期"退回内置表/放行，故须显式覆盖）。
    from core.engine.trading_calendar import TradingCalendar
    _today = now_shanghai().date()
    engine._calendar = TradingCalendar(
        lambda: [_today - timedelta(days=1), _today + timedelta(days=1)])
    db = factory()
    stale = _submitted_order(db, quantity=600)
    # 5 分钟：超 180s 但未到 600s 兜底窗 → pending
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=5)
    fresh = _submitted_order(db, quantity=700)
    fresh.created_at = datetime.now(timezone.utc).replace(tzinfo=None)
    db.commit()
    db.close()

    engine._tick_main()

    db = factory()
    stale2 = db.get(LiveOrder, stale.id)
    fresh2 = db.get(LiveOrder, fresh.id)
    assert stale2.status == "submitted"   # 桥通道：无法追溯保持 pending
    assert (stale2.error_message or "").startswith("bridge_order_untraceable:")
    assert fresh2.status == "submitted"  # 未超时不动
    db.close()


# ---------------- 修复 A+：expire 前按 remark 兜底回填 ----------------
# 真机 2026-08-21 id54-58 同源复现：修复 A（remark 直连 /deals 回填）被 _expire_stale_orders
# 拦截——expire 在 deals 回填之前执行并 commit，超时单先变 rejected，deals 回填阶段见
# status=rejected 直接 continue（live_engine.py 旧逻辑）。修复 A+：_expire_stale_orders
# 标 rejected 前，先按 remark 查 /deals 兜底一次（成交则 filled 不 reject）。
# 同时覆盖两条调用路径：_poll_deals（deals 循环）与 _tick_main（主循环饿死场景）。

def test_expire_stale_backfills_via_remark_before_rejecting():
    """修复 A+：单已超 180s + order_ref 缺失 + /deals 有 remark 成交 → 回填 filled 不 rejected。

    复现 2026-08-21 id54 场景：单 created 14:32:25，QMT 秒成，ORDER 实时表移除 →
    order_ref 永远 None，旧逻辑 223s 超时 rejected。但 /deals（DEAL 表）保留成交记录且带
    m_strRemark，故 expire 标 rejected 前应先按 bridge_order_id[:20] 在 /deals 兜底回填。
    直接调 _expire_stale_orders 验证（内部兜底）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    # /orders 无本单（成交后从 ORDER 实时表移除），/deals 有 remark 命中成交
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ref-never-matched",  # 真实 ref，但 /orders 无此单 Core 拿不到
        "remark": _TEST_REMARK,            # 按 remark 直连匹配
        "price": 9.25, "volume": 600,
        "amount": 5550.0, "commission": 1.39,
        "trade_time": "150001", "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    stale = _submitted_order(db, quantity=600)  # bridge_order_id=_TEST_OID → remark=_TEST_REMARK
    # 回拨 created_at 10 分钟，超过 _ORDER_REF_MATCH_TIMEOUT(180s)
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
    db.commit()
    db.close()

    # expire 自身不 commit（调用方负责，同 _poll_deals line 1535-1536 模式）
    db = factory()
    engine._expire_stale_orders(db)
    db.commit()
    db.close()

    db = factory()
    lo2 = db.get(LiveOrder, stale.id)
    # 修复 A+：超时但 /deals 有 remark 成交 → 回填 filled，不 rejected
    assert lo2.status == "filled"
    assert lo2.filled_quantity == 600
    assert lo2.filled_price == Decimal("9.25")
    assert not lo2.error_message  # 超时错误未写入
    trades = db.query(LiveTrade).all()
    assert len(trades) == 1
    assert trades[0].quantity == 600
    assert trades[0].amount == Decimal("5550.0")
    # filled 后 apply：持仓 600，现金扣 amount+commission
    assert ctx.positions[stock].quantity == 600
    assert port.account.cash == Decimal("100000") - (Decimal("5550.0") + Decimal("1.39"))
    db.close()


def test_expire_stale_rejects_when_no_deals_match():
    """修复 A+：单已超时 + /deals 无成交 → 桥通道保持 pending（不伪造、不驳回）。

    确保兜底不误救真空单：桥真未落单（/deals 也无成交），也不得臆造成交；
    超窗只记 bridge_order_untraceable，由人工核对。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])  # /deals 无任何成交
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    stale = _submitted_order(db, quantity=600)
    # 5 分钟：未到 600s 超长兜底窗 → pending（收口前仍保持）
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=5)
    db.commit()
    db.close()

    db = factory()
    engine._expire_stale_orders(db)
    db.commit()
    db.close()

    db = factory()
    lo2 = db.get(LiveOrder, stale.id)
    assert lo2.status == "submitted"  # 桥通道：查不到不驳回
    assert (lo2.error_message or "").startswith("bridge_order_untraceable:")
    db.close()


def test_expire_stale_backfill_skips_order_without_bridge_order_id():
    """修复 A+：超时单无 bridge_order_id（遗留单无 remark 匹配键）→ 保持 pending。

    无 bridge_order_id 的单没有 remark 匹配键，/deals 兜底无意义；仍不臆造成交、
    不自动驳回，超窗只记 bridge_order_untraceable。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "remark": "other-order-remark-xx",  # 别的单的 remark，本单无匹配键
        "price": 9.25, "volume": 600, "amount": 5550.0, "commission": 1.39,
        "trade_time": "150001", "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    stale = _submitted_order(db, quantity=600, bridge_order_id=None)  # 无匹配键
    # 5 分钟：未到 600s 超长兜底窗 → pending（无 remark 键也不臆造/不收口）
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=5)
    db.commit()
    db.close()

    db = factory()
    engine._expire_stale_orders(db)
    db.commit()
    db.close()

    db = factory()
    lo2 = db.get(LiveOrder, stale.id)
    assert lo2.status == "submitted"  # 无 remark 键，兜底跳过，仍保持 pending
    assert (lo2.error_message or "").startswith("bridge_order_untraceable:")
    db.close()


def test_expire_stale_backfill_tolerates_bridge_offline():
    """修复 A+：兜底查 /deals 时桥离线（BridgeUnavailableError）→ 不崩，保持 pending。

    兜底不应因桥离线而抛异常中断 expire；无法验证成交时绝不臆造，留待人工核对。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()

    def respond(request):
        # /deals 抛离线（模拟桥不可用）
        if request.url.path == "/deals":
            raise BridgeUnavailableError("bridge offline")
        return httpx.Response(200, json={"ok": True, "data": []})

    rec._respond = respond
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    stale = _submitted_order(db, quantity=600)
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
    db.commit()
    db.close()

    db = factory()
    engine._expire_stale_orders(db)  # 不应抛异常
    db.commit()
    db.close()

    db = factory()
    lo2 = db.get(LiveOrder, stale.id)
    assert lo2.status == "submitted"  # 桥离线无法兜底，保持 pending
    db.close()


def test_poll_deals_does_not_reject_when_deals_has_remark_fill():
    """修复 A+：_poll_deals 全链路——超时单 + /deals 有 remark 成交 → filled 不 rejected。

    端到端验证：_poll_deals 内 expire 调用前的兜底生效，超时单不会被先 reject 再跳过。
    复现今天 id54 的 _poll_deals 调用路径（非直接调 expire）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ref-never-matched",
        "remark": _TEST_REMARK,
        "price": 9.25, "volume": 600,
        "amount": 5550.0, "commission": 1.39,
        "trade_time": "150001", "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    stale = _submitted_order(db, quantity=600)
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, stale.id)
    assert lo2.status == "filled"  # 走全链路也不被 reject
    assert lo2.filled_quantity == 600
    assert ctx.positions[stock].quantity == 600
    db.close()


# ---------------------------------------------------------------------------
# 第一层：Core 读 /orders 的 m_nOrderStatus 同步终态。
# 官方码（D:\iquant xtconstant.py，文档 QMT_Python_API_Doc.html:10643）：
#   52=部成待撤 53=部撤(终态) 54=已撤(终态) 55=部成(在途,剩余仍撮合)
#   56=已成(终态) 57=废单(终态)
# 关键：55 是「部成」非终态，绝不能当终态撤单——否则剩余后续成交会重复 apply/丢单。
# ---------------------------------------------------------------------------

def _order_with_ref(ref="ref-cancel-1", status=54, traded_volume=0, volume=600,
                    remark=_TEST_REMARK, direction=48, traded_price=None):
    """构造一笔 /orders 返回的 BRIDGE 委托字典（含 m_nOrderStatus）。"""
    return {
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": direction, "volume": volume, "order_ref": ref,
        "traded_volume": traded_volume, "traded_price": traded_price,
        "status": status, "remark": remark,
        "insert_date": "20260805", "insert_time": "100000",
    }


def _place_order_with_ref(db, ref, quantity=600, status="submitted"):
    """预置一笔已有 order_ref 的 LiveOrder（模拟 ref 已回填、等待成交/撤单）。"""
    lo = _submitted_order(db, quantity=quantity, status=status)
    lo.order_ref = ref
    db.flush()
    return lo


def test_poll_deals_marks_fully_canceled_order():
    """54 全撤 + 无成交 → canceled，无 LiveTrade、不 apply、出 order 事件。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[_order_with_ref(status=54, traded_volume=0)],
                       deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-cancel-1")
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "canceled"
    assert lo2.filled_quantity == 0
    assert db.query(LiveTrade).count() == 0
    assert ctx.positions.get("600000.SH") is None
    assert port.account.cash == Decimal("100000")
    db.close()


def test_poll_deals_partial_cancel_applies_fills_then_cancels():
    """53 部撤 + 部分成交 → 先 apply 真实成交，再 canceled（终态必须落持仓）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        _order_with_ref(ref="ref-pc", status=53, traded_volume=300, volume=600)],
        deals=[{
            "order_ref": "ref-pc", "price": 9.25, "volume": 300,
            "amount": 2775.0, "commission": 0.69,
            "trade_time": "150001", "trade_date": "20260805",
        }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-pc", quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "canceled"          # 部撤终态
    assert lo2.filled_quantity == 300        # 部分成交真实落账
    trades = db.query(LiveTrade).all()
    assert len(trades) == 1
    assert trades[0].quantity == 300
    # 部撤的部分成交必须 apply（现有 partial 不 apply，终态撤单必须补上）
    assert ctx.positions[stock].quantity == 300
    assert port.account.cash == Decimal("100000") - (Decimal("2775.0") + Decimal("0.69"))
    db.close()


def test_poll_deals_cancel_defers_when_deals_lag():
    """53 部撤但 traded_volume>0 且 /deals 尚未回报 → 不 cancel，留待下轮 /deals 追上。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    # /orders 说部撤 300，但 /deals 还没这笔（传播滞后）
    _mock_orders_deals(rec, orders=[
        _order_with_ref(ref="ref-lag", status=53, traded_volume=300, volume=600)],
        deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-lag", quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "submitted"         # 不提前 cancel，等 /deals
    assert db.query(LiveTrade).count() == 0
    db.close()


def test_poll_deals_filled_not_canceled_by_status_sync():
    """56 全成 + /deals 全成 → filled（成交回填优先），不被状态同步误 cancel。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        _order_with_ref(ref="ref-filled", status=56, traded_volume=600, volume=600)],
        deals=[{
            "order_ref": "ref-filled", "price": 9.25, "volume": 600,
            "amount": 5550.0, "commission": 1.39,
            "trade_time": "150001", "trade_date": "20260805",
        }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-filled", quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "filled"
    assert ctx.positions["600000.SH"].quantity == 600
    db.close()


def test_poll_deals_inflight_status_stays_submitted():
    """非终态 status（如 52 待报）→ 保持 submitted，不误判。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        _order_with_ref(ref="ref-inflight", status=52, traded_volume=0)],
        deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-inflight")
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "submitted"
    db.close()


def test_deals_down_filled_order_advances_from_orders_row():
    """缺陷3：/deals 连续失败时，56 全成改按 /orders 的成交量+成交价复核回填。

    /deals 一挂，旧实现只等成交回报 → 订单整天停在 submitted（柜台已成、账本没有）。
    兜底判据：/deals 连续失败 ≥2 轮 + /orders status=56 + traded_volume/traded_price。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    rec = _Recorder(fail_paths={"/deals"})
    _mock_orders_deals(rec, orders=[
        _order_with_ref(ref="ref-56", status=56, traded_volume=600,
                        traded_price=9.24)], deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-56", quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()  # 第 1 轮：/deals 失败 1 次，未达阈值 → 不动
    db = factory()
    assert db.get(LiveOrder, lo.id).status == "submitted"
    db.close()

    engine._poll_deals()  # 第 2 轮：连续失败 2 次 → 按 /orders 复核全成

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "filled"
    assert lo2.filled_quantity == 600
    assert lo2.filled_price == Decimal("9.24")
    trades = db.query(LiveTrade).all()
    assert len(trades) == 1 and trades[0].quantity == 600
    db.close()
    assert ctx.positions["600000.SH"].quantity == 600  # 正常 apply


def test_deals_down_unconfirmable_fill_alerts_and_stays_submitted():
    """缺陷3：/orders 报 56 但缺成交价 → 不假装成交：一次性 risk 告警 + 保持 submitted。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder(fail_paths={"/deals"})
    _mock_orders_deals(rec, orders=[
        _order_with_ref(ref="ref-56", status=56, traded_volume=600,
                        traded_price=None)], deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    db = factory()
    lo = _place_order_with_ref(db, "ref-56", quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()
    engine._poll_deals()   # 第 2 轮触发兜底，但确认不了成交
    engine._poll_deals()   # 第 3 轮：告警不刷屏

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "submitted"          # 绝不假装成交
    assert lo2.filled_quantity == 0
    assert (lo2.error_message or "").startswith("bridge_deals_stale:")
    assert db.query(LiveTrade).count() == 0
    db.close()
    rules = []
    while not q.empty():
        ev = q.get_nowait()
        if ev.get("type") == "risk":
            rules.append(ev.get("rule"))
    assert rules.count("bridge_deals_stale") == 1


def test_poll_deals_status_55_part_succ_stays_alive():
    """55=部成（部分成交、剩余仍在撮合）→ 绝不能当终态撤单。

    回归 bug（2026-08-24 官方码核对）：旧代码把 55 误当「部撤」终态，会提前 cancel
    真实在途单，剩余成交后重复 apply/丢单。55 必须保持 partial/submitted。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    # /orders 55 部成 300，/deals 同步回报 300（剩余 300 仍在途）
    _mock_orders_deals(rec, orders=[
        _order_with_ref(ref="ref-alive", status=55, traded_volume=300, volume=600)],
        deals=[{
            "order_ref": "ref-alive", "price": 9.25, "volume": 300,
            "amount": 2775.0, "commission": 0.69,
            "trade_time": "100001", "trade_date": "20260805",
        }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-alive", quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    # 仍是在途状态，绝不 canceled
    assert lo2.status in ("submitted", "partial")
    assert lo2.status != "canceled"
    assert lo2.filled_quantity == 300   # 部分成交已记录
    # partial 不 apply（等终态/全成），所以剩余后续成交不会重复 apply
    assert ctx.positions.get(stock) is None
    db.close()


def test_poll_deals_status_57_junk_marks_rejected():
    """57=废单（终态，柜台拒单）→ rejected，无成交、不 apply。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        _order_with_ref(ref="ref-junk", status=57, traded_volume=0, volume=600)],
        deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-junk", quantity=600)
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "rejected"
    assert lo2.filled_quantity == 0
    assert db.query(LiveTrade).count() == 0
    assert ctx.positions.get("600000.SH") is None
    assert port.account.cash == Decimal("100000")
    db.close()


def test_poll_deals_cancel_missing_from_orders_stays_submitted():
    """/orders 列表里查不到本单（实时表已移除）→ 不据缺席判撤，保持 submitted。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])  # 实时表无此单
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-gone")
    db.commit()
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "submitted"
    db.close()


def test_poll_deals_cancel_bridge_offline_stays_submitted():
    """/orders 桥离线 → 不改状态、不崩，下轮重试。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder(fail_paths={"/orders", "/deals"})
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _place_order_with_ref(db, "ref-off")
    db.commit()
    db.close()

    engine._poll_deals()  # 不应抛异常

    db = factory()
    lo2 = db.get(LiveOrder, lo.id)
    assert lo2.status == "submitted"
    db.close()



# ---- 关键状态流转日志可观测性（INFO/WARNING，低频事件，不刷屏）----
def _log_msgs(caplog):
    return [r.getMessage() for r in caplog.records]


def test_order_accepted_logs_info(caplog):
    """桥受理成功后落 logger.info（此前只发 SSE，日志无痕）。"""
    import logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single(strategy_id=1, period="1m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)

    def respond(request):
        path = request.url.path
        if path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": []})
        if path in ("/order", "/ping", "/quote"):
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False})

    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.0", bar_time)

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine._handle_bar(port, bar)

    assert any("order accepted" in m for m in _log_msgs(caplog)), _log_msgs(caplog)


def test_backfill_filled_logs_info(caplog):
    """/deals 成交回填 -> logger.info（此前只发 SSE order/trade）。"""
    import logging
    factory, _ = _db_factory()
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=0, filled_price=None, status="submitted",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-bf",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    db.close()

    port, _ = _portfolio_single()
    engine, _ = _ss_engine(factory, port)
    db = factory()
    lo = db.query(LiveOrder).filter_by(id=lo_id).first()
    deals = [{"order_ref": "ref-bf", "volume": 1000, "amount": 9000,
              "commission": 0, "trade_date": "20260805", "trade_time": "100100"}]
    with caplog.at_level(logging.INFO, logger="core.engine.live.order_machine"):
        engine._backfill_order(db, lo, deals)
        db.commit()
    db.close()
    assert any("backfill" in m and "600000.SH" in m and "filled" in m
               for m in _log_msgs(caplog)), _log_msgs(caplog)


def test_fill_price_deviation_emits_risk_event():
    """P0-3：桥通道成交价偏离审批价超阈值 → risk/fill_price_deviation（含三价）。"""
    factory, _ = _db_factory()
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=1000, filled_price=Decimal("9.5"), status="filled",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-dev",
    )
    db.add(lo)
    db.commit()
    db.close()

    port, _ = _portfolio_single()
    engine, _ = _ss_engine(factory, port)
    engine._fill_price_deviation_pct = Decimal("1.0")
    events = []
    engine._emit = lambda et, payload: events.append((et, payload))

    engine._check_fill_price_deviation()
    risks = [p for et, p in events if et == "risk"]
    assert len(risks) == 1, events
    assert risks[0]["gate"] == "fill_price_deviation"
    assert risks[0]["rule"] == "fill_price_deviation"
    assert risks[0]["approval_price"] == 9.0
    assert risks[0]["fill_price"] == 9.5
    assert risks[0]["deviation_pct"] == 5.5556
    assert risks[0]["threshold_pct"] == 1.0
    assert risks[0]["stock_code"] == "600000.SH"

    # 同一单（同 filled_quantity）重复扫描不重复报
    engine._check_fill_price_deviation()
    assert len([p for et, p in events if et == "risk"]) == 1


def test_fill_price_deviation_skips_ui_channels_and_small_deviation():
    """P0-3：TDX/THS（supports_remark=False）不纳入；偏离未超阈值不发。"""
    factory, _ = _db_factory()
    db = factory()
    db.add(LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=1000, filled_price=Decimal("9.05"), status="filled",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-small",
    ))
    db.commit()
    db.close()

    port, _ = _portfolio_single()
    engine, _ = _ss_engine(factory, port)
    engine._fill_price_deviation_pct = Decimal("1.0")
    events = []
    engine._emit = lambda et, payload: events.append((et, payload))

    engine._check_fill_price_deviation()          # 0.56% < 1%
    assert events == []

    engine._dispatcher.supports_remark = False    # UI 自动化通道
    db = factory()
    row = db.query(LiveOrder).filter_by(order_ref="ref-small").first()
    row.price = Decimal("9.0")
    row.filled_price = Decimal("9.5")
    db.commit()
    db.close()
    engine._fill_dev_checked.clear()
    engine._check_fill_price_deviation()
    assert events == []


def test_backfill_same_partial_does_not_spam_log(caplog):
    """5s 轮询对同一 partial 单反复回填相同成交量 -> 只首次记日志，不刷屏。"""
    import logging
    factory, _ = _db_factory()
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=0, filled_price=None, status="submitted",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-bf2",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    db.close()

    port, _ = _portfolio_single()
    engine, _ = _ss_engine(factory, port)
    db = factory()
    lo = db.query(LiveOrder).filter_by(id=lo_id).first()
    partial = [{"order_ref": "ref-bf2", "volume": 300, "amount": 2700,
                "commission": 0, "trade_date": "20260805", "trade_time": "100100"}]
    with caplog.at_level(logging.INFO, logger="core.engine.live.order_machine"):
        engine._backfill_order(db, lo, partial)   # 首次 partial：记日志
        engine._backfill_order(db, lo, partial)   # 同量同状态：不重复
        db.commit()
    backfill_logs = [m for m in _log_msgs(caplog) if "backfill" in m]
    assert len(backfill_logs) == 1, backfill_logs
    assert "partial" in backfill_logs[0]
    db.close()


def test_backfill_no_change_repeats_emit_nothing():
    """I2：同量同状态重复回填（5s 轮询同一 partial）→ 不重复发 order/trade 事件。"""
    factory, _ = _db_factory()
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=0, filled_price=None, status="submitted",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-bf-nc",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    db.close()

    port, _ = _portfolio_single()
    engine, q = _ss_engine(factory, port)
    db = factory()
    lo = db.query(LiveOrder).filter_by(id=lo_id).first()
    partial = [{"order_ref": "ref-bf-nc", "volume": 300, "amount": 2700,
                "commission": 0, "trade_date": "20260805", "trade_time": "100100"}]
    engine._backfill_order(db, lo, partial)   # 首次：order(partial) + trade 照发
    first = [q.get_nowait(), q.get_nowait()]
    assert first[0]["type"] == "order" and first[0]["status"] == "partial"
    assert first[1]["type"] == "trade"
    assert q.empty()
    engine._backfill_order(db, lo, partial)   # 同量同状态：零 emit
    engine._backfill_order(db, lo, partial)
    db.commit()
    db.close()
    assert q.empty()


def test_backfill_qty_change_emits_again():
    """I2：回填量变化（300→400）→ order/trade 事件照发。"""
    factory, _ = _db_factory()
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=0, filled_price=None, status="submitted",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-bf-qty",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    db.close()

    port, _ = _portfolio_single()
    engine, q = _ss_engine(factory, port)
    db = factory()
    lo = db.query(LiveOrder).filter_by(id=lo_id).first()
    partial300 = [{"order_ref": "ref-bf-qty", "volume": 300, "amount": 2700,
                   "commission": 0, "trade_date": "20260805", "trade_time": "100100"}]
    engine._backfill_order(db, lo, partial300)
    assert q.get_nowait()["type"] == "order"
    assert q.get_nowait()["type"] == "trade"
    assert q.empty()
    partial400 = [{"order_ref": "ref-bf-qty", "volume": 400, "amount": 3600,
                   "commission": 0, "trade_date": "20260805", "trade_time": "100200"}]
    engine._backfill_order(db, lo, partial400)  # 量变：照发
    db.commit()
    db.close()
    ev = [q.get_nowait(), q.get_nowait()]
    assert ev[0]["type"] == "order" and ev[0]["filled_quantity"] == 400
    assert ev[1]["type"] == "trade" and ev[1]["quantity"] == 400
    assert q.empty()


def test_backfill_status_change_emits_again():
    """I2：状态变（prev canceled → new partial）→ 事件照发（任一变化必发）。"""
    factory, _ = _db_factory()
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code="600000.SH",
        trade_type="buy", order_type="limit", price=Decimal("9.0"), quantity=1000,
        filled_quantity=300, filled_price=Decimal("9.0"), status="canceled",
        signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 5, 10, 0), order_ref="ref-bf-st",
    )
    db.add(lo)
    db.commit()
    lo_id = lo.id
    db.close()

    port, _ = _portfolio_single()
    engine, q = _ss_engine(factory, port)
    db = factory()
    lo = db.query(LiveOrder).filter_by(id=lo_id).first()
    partial = [{"order_ref": "ref-bf-st", "volume": 300, "amount": 2700,
                "commission": 0, "trade_date": "20260805", "trade_time": "100100"}]
    engine._backfill_order(db, lo, partial)  # 状态变：canceled→partial 照发
    db.commit()
    db.close()
    ev = [q.get_nowait(), q.get_nowait()]
    assert ev[0]["type"] == "order" and ev[0]["status"] == "partial"
    assert ev[1]["type"] == "trade"
    assert q.empty()


def test_expire_manual_review_logs_error(caplog):
    """expire 超窗无法追溯 -> 保持在途并 logger.error（此前是 warning + 收口 rejected）。"""
    import logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    stale = _submitted_order(db, quantity=600)
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
    db.commit()
    db.close()

    db = factory()
    with caplog.at_level(logging.INFO, logger="core.engine.live.order_machine"):
        engine._expire_stale_orders(db)
        db.commit()
    db.close()
    assert any(r.levelno == logging.ERROR and "expire" in m
               and "manual review" in m
               for r, m in zip(caplog.records, _log_msgs(caplog))), _log_msgs(caplog)


def test_expire_remark_backfill_logs_info(caplog):
    """A+ remark 兜底命中 -> logger.info（真机 id54 路径，事后可追溯）。"""
    import logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ref-never", "remark": _TEST_REMARK,
        "price": 9.25, "volume": 600, "amount": 5550.0, "commission": 1.39,
        "trade_time": "150001", "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    stale = _submitted_order(db, quantity=600)
    stale.created_at = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)
    db.commit()
    db.close()

    db = factory()
    with caplog.at_level(logging.INFO, logger="core.engine.live.order_machine"):
        engine._expire_stale_orders(db)
        db.commit()
    db.close()
    assert any("remark" in m and "backfill" in m for m in _log_msgs(caplog)), _log_msgs(caplog)


def test_terminal_sync_canceled_logs_info(caplog):
    """54 全撤终态同步 -> logger.info（此前只发 SSE canceled）。"""
    import logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[_order_with_ref(status=54, traded_volume=0)], deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    _place_order_with_ref(db, "ref-cancel-1")
    db.commit()
    db.close()

    with caplog.at_level(logging.INFO, logger="core.engine.live.order_machine"):
        engine._poll_deals()
    assert any("terminal" in m and "canceled" in m for m in _log_msgs(caplog)), _log_msgs(caplog)


def test_terminal_sync_junk_rejected_logs_warning(caplog):
    """57 废单终态同步 -> logger.warning（柜台拒单，需关注）。"""
    import logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[_order_with_ref(ref="ref-junk", status=57, traded_volume=0)],
                       deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    _place_order_with_ref(db, "ref-junk")
    db.commit()
    db.close()

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine._poll_deals()
    assert any(r.levelno == logging.WARNING and "junk" in m and "rejected" in m
               for r, m in zip(caplog.records, _log_msgs(caplog))), _log_msgs(caplog)


def test_maybe_daily_close_logs_info(caplog):
    """14:30 日终估值成功 -> logger.info（此前成功路径无痕）。"""
    import logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    engine, _ = _ss_engine(factory, port)
    with caplog.at_level(logging.INFO, logger="core.engine.live.daily_closer"):
        engine._maybe_daily_close(now=datetime(2026, 8, 24, 14, 30))
    assert any("daily close" in m for m in _log_msgs(caplog)), _log_msgs(caplog)


def test_maybe_daily_bars_logs_info(caplog):
    """15:05 日线驱动成功 -> logger.info（此前成功路径无痕）。"""
    import logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    rec = _Recorder(respond=_respond_quote_bars(stock, [
        {"stime": "20260824000000", "open": 9.0, "high": 9.3, "low": 9.0,
         "close": 9.2, "volume": 10000, "amount": 92000.0},
    ]))
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine_formula_portfolio(disp, factory, port, {1: "MA_CROSS"})
    engine._tq_formula.compute_injected_batch =lambda **kw: {
        stock: {"ErrorId": 0},
    }
    with caplog.at_level(logging.INFO, logger="core.engine.live.daily_closer"):
        engine._maybe_daily_bars(now=datetime(2026, 8, 24, 15, 5))
    assert any("daily bar" in m for m in _log_msgs(caplog)), _log_msgs(caplog)


def test_close_sweep_no_pending_logs_info(caplog):
    """15:05 无残留单 -> logger.info（干净日也要有清扫跑过的痕迹）。"""
    import logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    engine, _ = _sweep_engine(factory, port, rec)
    with caplog.at_level(logging.INFO, logger="core.engine.live.daily_closer"):
        engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))
    assert engine._last_close_sweep_date == date(2026, 8, 24)
    assert any("no pending" in m for m in _log_msgs(caplog)), _log_msgs(caplog)


# ===================== 风控状态转换日志补全（max_drawdown / daily_loss / 恢复 / 重启读回）=====================
def test_max_drawdown_trigger_logs_warning(caplog):
    """max_drawdown 熔断触发（计数+1）-> logger.warning（此前只 SSE，日志无痕）。"""
    import logging
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    # 清风控比例：drawdown bar 不触发止损单，聚焦熔断
    port.strategies[0].strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0"), take_profit_ratio=Decimal("0"),
        trailing_stop_ratio=Decimal("0"),
    )
    stock = "600000.SH"
    pos = Position(stock)
    pos.buy(1000, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))
    port.strategies[0].positions[stock] = pos
    port.account.cash = Decimal("0")

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine._handle_bar(port, _bar(stock, "100", datetime(2026, 8, 5, 10, 0)))  # 建峰
        engine._handle_bar(port, _bar(stock, "70", datetime(2026, 8, 5, 10, 1)))   # 回撤 30%>20% 熔断

    msgs = _log_msgs(caplog)
    assert any("max_drawdown" in m and "触发" in m and "1 次" in m for m in msgs), msgs


def test_max_drawdown_manual_halt_logs_warning(caplog):
    """累计 3 次转手动恢复 -> logger.warning 标明 manual（最该留痕的风控事件）。"""
    import logging
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    port.strategies[0].strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0"), take_profit_ratio=Decimal("0"),
        trailing_stop_ratio=Decimal("0"),
    )
    stock = "600000.SH"
    pos = Position(stock)
    pos.buy(1000, Decimal("9.0"), datetime(2026, 8, 5, 9, 30))
    port.strategies[0].positions[stock] = pos
    port.account.cash = Decimal("0")

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        # 第 3 次触发：前两次已累计 2，本 bar 回撤触发第 3 次 -> 转手动
        port.risk_manager.consecutive_drawdown_triggers = 2
        engine._breaker_count_written[1] = 2  # 模拟已落库 2，使本次递增被识别
        engine._handle_bar(port, _bar(stock, "100", datetime(2026, 8, 5, 10, 0)))  # 建峰
        engine._handle_bar(port, _bar(stock, "70", datetime(2026, 8, 5, 10, 1)))   # 第 3 次 -> 手动

    msgs = _log_msgs(caplog)
    assert any("max_drawdown" in m and "3 次" in m and "手动" in m for m in msgs), msgs


def test_max_drawdown_auto_recovery_logs_info(caplog):
    """max_drawdown 熔断次日自动恢复（非手动）-> logger.info（此前 risk_manager 内静默置 False）。

    走实盘真实路径 _handle_bar（每 bar 调 update_peak）：周五触发后周一一根回升 bar 自动恢复。
    """
    import logging
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.peak_value = Decimal("100000")
    rm.circuit_breaker_active = True
    rm.breaker_trigger_date = date(2026, 8, 21)       # 周五触发
    rm.consecutive_drawdown_triggers = 1
    engine._breaker_count_written[1] = 1
    # 清风控比例 + 无信号缓存：该 bar 不出单，聚焦恢复日志（恢复检测在 update_peak 后、on_bar 前）
    port.strategies[0].strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0"), take_profit_ratio=Decimal("0"),
        trailing_stop_ratio=Decimal("0"),
    )
    stock = "600000.SH"
    pos = Position(stock)
    pos.buy(1000, Decimal("9.0"), datetime(2026, 8, 21, 9, 30))
    port.strategies[0].positions[stock] = pos
    port.account.cash = Decimal("0")

    with caplog.at_level(logging.INFO, logger="core.engine.live.breaker"):
        # 次日（8/24 周一）close=99 → total=99000，回撤 1%<20% 不重触发，date>trigger 自动恢复
        engine._handle_bar(port, _bar(stock, "99", datetime(2026, 8, 24, 10, 0)))

    msgs = _log_msgs(caplog)
    assert rm.circuit_breaker_active is False
    assert any("max_drawdown" in m and "次日自动恢复" in m for m in msgs), msgs


def test_max_drawdown_halt_then_next_day_auto_recover_engine():
    """④ 实盘引擎路径：D 日回撤触发 → 当日停新开仓；D+1 首 bar 自动恢复（对齐回测）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.peak_value = Decimal("100000")
    stock = "600000.SH"
    port.account.cash = Decimal("79000")     # 回撤 21% ≥ 20%
    engine._handle_bar(port, _bar(stock, "79", datetime(2026, 8, 21, 10, 0)))
    assert rm.circuit_breaker_active is True
    assert rm.consecutive_drawdown_triggers == 1
    assert rm.is_trading_halted() is True
    db = factory()
    _breaker_link(db, 1)                     # 计数已落库（重启不丢）
    db.close()

    # 次日（8/24）首 bar：回撤收敛到 1% → 自动恢复（恢复判定在 on_bar 之前）
    port.account.cash = Decimal("99000")
    engine._handle_bar(port, _bar(stock, "99", datetime(2026, 8, 24, 9, 31)))
    assert rm.circuit_breaker_active is False
    assert rm.is_trading_halted() is False
    assert rm.consecutive_drawdown_triggers == 1   # 恢复不清零（累计口径）


def test_max_drawdown_three_triggers_manual_halt_survives_next_day():
    """④ 累计 3 次 → 转手动恢复；次日回撤收敛也不自动恢复，计数不再增长（对齐回测）。"""
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    rm = port.risk_manager
    rm.peak_value = Decimal("100000")
    rm.consecutive_drawdown_triggers = 2
    engine._breaker_count_written[1] = 2     # 模拟前两次已落库
    stock = "600000.SH"
    port.account.cash = Decimal("79000")
    engine._handle_bar(port, _bar(stock, "79", datetime(2026, 8, 21, 10, 0)))
    assert rm.circuit_breaker_active is True
    assert rm.manual_recovery is True
    assert rm.consecutive_drawdown_triggers == 3
    db = factory()
    link = _breaker_link(db, 3)
    assert link.status == "circuit_broken"
    db.close()

    # 次日回撤收敛：手动恢复态下不自动解除，也不再累加
    port.account.cash = Decimal("100000")
    engine._handle_bar(port, _bar(stock, "100", datetime(2026, 8, 24, 9, 31)))
    assert rm.circuit_breaker_active is True
    assert rm.is_trading_halted() is True
    assert rm.consecutive_drawdown_triggers == 3


def test_daily_loss_trigger_logs_warning(caplog):
    """日内亏损熔断触发 -> logger.warning（此前只 SSE，日志无痕）。"""
    import logging
    factory, _ = _db_factory()
    engine, _q = _ss_engine(factory)
    port = engine.portfolios[0]
    rm = port.risk_manager  # daily_loss_limit=0.05, initial_capital=100000
    # 建立昨收基准：Day1 收盘 100000
    rm.update_peak(Decimal("100000"), date(2026, 8, 21))

    # Day2 盘中跌到 94000（日内亏 6000=6% ≥ 5%），update_peak 记录当日最后 total
    rm.update_peak(Decimal("100000"), date(2026, 8, 24))  # 跨日刷新 prev_close=100000
    rm.update_peak(Decimal("94000"), date(2026, 8, 24))
    # _maybe_daily_close 用现金+持仓成本算 total，无持仓时即现金 → 置 94000 体现日内亏损
    port.account.cash = Decimal("94000")

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine._maybe_daily_close(now=datetime(2026, 8, 24, 14, 30))

    msgs = _log_msgs(caplog)
    assert any("daily_loss" in m and "触发" in m for m in msgs), msgs


def test_daily_loss_auto_recovery_logs_info(caplog):
    """daily_loss 暂停次日自动恢复 -> logger.info（此前 risk_manager 内静默置 False）。"""
    import logging
    factory, _ = _db_factory()
    engine, _q = _ss_engine(factory)
    rm = engine.portfolios[0].risk_manager
    rm.daily_pause_active = True
    rm.daily_loss_trigger_date = date(2026, 8, 21)  # 周五触发
    # 昨收基准：恢复日 total 不再重触（94000 接近 prev_close）
    rm.update_peak(Decimal("94000"), date(2026, 8, 21))
    rm.update_peak(Decimal("94000"), date(2026, 8, 24))  # 跨日 prev_close=94000

    with caplog.at_level(logging.INFO, logger="core.engine.live.breaker"):
        engine._maybe_daily_close(now=datetime(2026, 8, 24, 14, 30))  # 8/24 > 8/21 -> 恢复

    msgs = _log_msgs(caplog)
    assert rm.daily_pause_active is False
    assert any("daily_loss" in m and "恢复" in m for m in msgs), msgs


def test_daily_loss_pause_cleared_at_first_bar_next_day():
    """R3：daily_loss 暂停在次一交易日首根 bar 即解除（enter_day 镜像回测时序）。

    8/21（周五）触发暂停；8/24（周一）首根 1m bar 在 on_bar_update（置 _last_bar_date
    为当日）之前调 enter_day → 跨日补算 8/21、recovery_date=8/24 解除暂停。
    断言首根 bar 后 daily_pause_active 已为 False，且同日后续 bar 不重复补算。
    """
    factory, _ = _db_factory()
    engine, _q = _ss_engine(factory)
    rm = engine.portfolios[0].risk_manager
    rm.daily_pause_active = True
    rm.daily_loss_trigger_date = date(2026, 8, 21)  # 周五触发
    # 昨收：8/21 收盘 94000（建立 _last_bar_date/_last_bar_total_value）
    rm.update_peak(Decimal("94000"), date(2026, 8, 21))
    stock = "600000.SH"
    # 首根 bar（8/24 09:30）：enter_day 补算 8/21 → 恢复
    engine._handle_bar(engine.portfolios[0], _bar(stock, "94", datetime(2026, 8, 24, 9, 30)))
    assert rm.daily_pause_active is False
    assert engine._enter_day_updated[1] == date(2026, 8, 24)
    # 同日后续 bar（8/24 09:35）：不再重复 enter_day（幂等）
    engine._handle_bar(engine.portfolios[0], _bar(stock, "94", datetime(2026, 8, 24, 9, 35)))
    assert engine._enter_day_updated[1] == date(2026, 8, 24)


def test_daily_loss_pause_blocks_new_buys_until_next_day_bar():
    """R3：8/21 触发暂停后，8/24 首根 bar 恢复前整日剥 BUY；恢复后首个新日可开仓。

    对照：首根 bar 后暂停已清，同日再来的 OPEN 信号可正常下单（不再剥）。
    """
    factory, _ = _db_factory()
    engine, _q = _ss_engine(factory)
    port = engine.portfolios[0]
    rm = port.risk_manager
    rm.daily_pause_active = True
    rm.daily_loss_trigger_date = date(2026, 8, 21)
    rm.update_peak(Decimal("100000"), date(2026, 8, 21))
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 24, 9, 30)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.3", bar_time))

    # 首根 bar 恢复暂停 → OPEN 信号正常落单（不再剥 BUY）
    assert rm.daily_pause_active is False
    db = factory()
    orders = db.query(LiveOrder).all()
    db.close()
    assert len(orders) == 1
    assert orders[0].status == "submitted"


def test_recover_breaker_manual_logs_info(caplog):
    """API 手动恢复熔断 -> logger.info（此前只 SSE，日志无痕）。"""
    import logging
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    db = factory()
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=1, portfolio_strategy_id=1
    ).first()
    link.circuit_breaker_count = 3
    link.status = "circuit_broken"
    db.commit()
    db.close()
    db = factory()
    engine.recover(db)  # 还原内存态：count=3 -> manual_recovery=True
    db.close()
    assert port.risk_manager.manual_recovery is True
    engine._breaker_count_written[1] = 3

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine.recover_breaker(1)

    msgs = _log_msgs(caplog)
    assert any("手动恢复" in m and "portfolio 1" in m for m in msgs), msgs


def test_recover_restores_manual_breaker_logs_warning(caplog):
    """重启 recover 读回 count>=3 转手动 -> logger.warning（此前静默，重启后卡手动无痕迹）。"""
    import logging
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    db = factory()
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=1, portfolio_strategy_id=1
    ).first()
    link.circuit_breaker_count = 3
    link.status = "circuit_broken"
    db.commit()
    db.close()

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        db = factory()
        engine.recover(db)
        db.close()

    msgs = _log_msgs(caplog)
    assert port.risk_manager.manual_recovery is True
    assert any("重启读回" in m and "3 次" in m and "手动" in m for m in msgs), msgs


# ---------------- 决策闸门事件落库 + SSE（调参可观测性，#163）----------------
def _decision_rows(factory):
    db = factory()
    rows = db.query(LiveDecisionEvent).order_by(LiveDecisionEvent.id).all()
    db.close()
    return rows


def _drain_queue(q):
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def test_handle_bar_after_close_records_decision_event():
    """收盘后信号转 deferred → after_close_defer(live_gate/defer) 落决策事件 + SSE。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="5m")
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 15, 1)  # 收盘后
    engine, q = _ss_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.0", bar_time)
    bar.period = "5m"

    engine._handle_bar(port, bar)

    rows = _decision_rows(factory)
    assert len(rows) == 1
    ev = rows[0]
    assert ev.gate == "after_close_defer"
    assert ev.layer == "live_gate"
    assert ev.action == "defer"
    assert ev.portfolio_id == 1
    assert ev.strategy_id == 1
    assert ev.stock_code == stock
    assert ev.bar_time == bar_time
    # SSE：signal + decision，无 order
    queued = _drain_queue(q)
    dec = [e for e in queued if e.get("type") == "decision"]
    assert len(dec) == 1
    assert dec[0]["gate"] == "after_close_defer"
    assert dec[0]["layer"] == "live_gate"
    assert not any(e.get("type") == "order" for e in queued)
    # 落 deferred 行（不即时下单）
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "deferred"
    db.close()


def test_handle_bar_dup_skip_records_decision_event():
    """同 bar 同向重驱 → 第二根记 dup_skip(block) 落库 + SSE。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, q = _ss_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.0", bar_time)

    engine._handle_bar(port, bar)  # 首根正常落 submitted
    engine._handle_bar(port, bar)  # 同 bar 重驱 → dup

    # 首根下单后桥回读未确认会记一条 order_unconfirmed（在途语义），本用例只看 dup_skip
    gates = [r.gate for r in _decision_rows(factory) if r.gate != "order_unconfirmed"]
    assert gates == ["dup_skip"]
    dup = next(r for r in _decision_rows(factory) if r.gate == "dup_skip")
    assert dup.layer == "live_gate" and dup.action == "block"
    assert dup.stock_code == stock and dup.bar_time == bar_time
    queued = _drain_queue(q)
    assert any(e.get("type") == "decision" and e.get("gate") == "dup_skip" for e in queued)


def test_handle_bar_inflight_skip_records_decision_event():
    """同向在途单未确认 → 下一根记 inflight_skip(block) 落库 + SSE。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    t1 = datetime(2026, 8, 5, 10, 0)
    t2 = datetime(2026, 8, 5, 10, 5)
    engine, q = _ss_engine(factory, port)
    engine.signal_cache = {
        (1, stock, t1): [{"name": "open_sig", "value": 1}],
        (1, stock, t2): [{"name": "open_sig", "value": 1}],
    }

    engine._handle_bar(port, _bar(stock, "9.0", t1))  # 首根 submitted 在途
    engine._handle_bar(port, _bar(stock, "9.0", t2))  # 不同 bar → 过 dup，撞 inflight

    # 首根下单后桥回读未确认会记一条 order_unconfirmed（在途语义），本用例只看 inflight_skip
    gates = [r.gate for r in _decision_rows(factory) if r.gate != "order_unconfirmed"]
    assert gates == ["inflight_skip"]
    ev = next(r for r in _decision_rows(factory) if r.gate == "inflight_skip")
    assert ev.layer == "live_gate" and ev.action == "block"
    assert ev.stock_code == stock and ev.bar_time == t2
    queued = _drain_queue(q)
    assert any(e.get("type") == "decision" and e.get("gate") == "inflight_skip" for e in queued)


def test_handle_bar_bridge_unavailable_records_decision_event():
    """桥离线（/order 连接被拒）→ bridge_unavailable(reject) 落库 + SSE。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder(fail_paths={"/order"})
    disp, _ = _make_dispatcher(rec, fail_paths={"/order"})
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))

    # 心跳/下单发觉断线先落一条 channel_offline（通道状态留痕），再落 bridge_unavailable
    gates = [r.gate for r in _decision_rows(factory)]
    assert gates == ["channel_offline", "bridge_unavailable"]
    ev = _decision_rows(factory)[1]
    assert ev.layer == "live_gate" and ev.action == "reject"
    assert ev.stock_code == stock
    queued = _drain_queue(q)
    assert any(e.get("type") == "decision" and e.get("gate") == "bridge_unavailable"
               for e in queued)
    # 订单标 rejected
    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "rejected"
    db.close()


def test_handle_bar_no_orders_still_drains_halt_strip_event():
    """早退路径（orders 为空）也要 drain：熔断剥 BUY 记 halted_buy_strip 落库。

    熔断期 BUY 信号被 on_bar 剥掉（返回空 orders），但 halted_buy_strip 事件已记录；
    重构后 db 会话在 orders 判定前打开、drain 放循环后，空 orders 也落库。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    # 强置日内亏损暂停（on_bar_update 只调 update_peak，不碰 daily_pause，态保持）
    port.risk_manager.daily_pause_active = True
    port.risk_manager.daily_loss_trigger_date = bar_time.date()
    engine, q = _ss_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))

    rows = _decision_rows(factory)
    assert len(rows) == 1
    ev = rows[0]
    assert ev.gate == "halted_buy_strip"
    assert ev.layer == "portfolio_risk" and ev.action == "strip"
    assert ev.param_name == "daily_loss_limit"
    assert ev.stock_code == stock and ev.bar_time == bar_time
    # 熔断期不下单
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()
    queued = _drain_queue(q)
    assert any(e.get("type") == "decision" and e.get("gate") == "halted_buy_strip"
               for e in queued)


# ---------------- 展示层事件去重（daily_closer 1d/1w/1mon 三轮驱动）----------------
def test_same_bar_three_period_drive_emits_single_signal_and_decision():
    """同一根日线快照依次按 1d/1w/1mon 驱动：同票同规则只发一轮 signal + 一轮 decision。

    根因（2026-11-06 实盘 15:51:16 三重复）：daily_closer 用同一份日线快照
    `for period in ("1d","1w","1mon")` 各调一次 _handle_bar，而 Portfolio.on_bar
    周期不匹配时仍跑风控（include_signals=False）→ 同一 (bar_time, 票, 规则) 的
    decision 与 signal 被落库/推送三轮。落单侧已有 deferred 幂等兜底，这里锁事件层。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    _preset_position(ctx, stock, 100, Decimal("10.0"))  # 现价 9.0 → 亏 10% 触发止损
    bar_time = datetime(2026, 8, 24, 0, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 24)], stock)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    engine.signal_cache = {}

    for period in ("1d", "1w", "1mon"):
        engine._handle_bar(port, BarEvent(
            stocks={stock: {
                "open": Decimal("9.0"), "high": Decimal("9.0"),
                "low": Decimal("9.0"), "close": Decimal("9.0"),
                "volume": 10000,
            }},
            bar_time=bar_time, period=period,
        ))

    rows = _decision_rows(factory)
    # 风控决策（止损）只落一条；内部幂等提示 defer_duplicate 不再入库
    assert [(r.gate, r.action) for r in rows if r.gate == "stop_loss"] == [
        ("stop_loss", "trigger")]
    assert not any(r.action == "defer_duplicate" for r in rows)
    queued = _drain_queue(q)
    assert len([e for e in queued
                if e.get("type") == "signal" and e.get("stock_code") == stock]) == 1
    assert len([e for e in queued
                if e.get("type") == "decision" and e.get("gate") == "stop_loss"]) == 1


def test_drain_decisions_dedups_same_key_but_keeps_distinct_bars():
    """_drain_decisions 同键（含 bar_time）合并；不同 bar_time 各自保留。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    engine, q = _ss_engine(factory, port)
    bar_time = datetime(2026, 8, 5, 10, 0)

    db = factory()
    for _ in range(2):
        engine._recorder.record(
            gate="stop_loss", layer="strategy_risk", action="trigger",
            portfolio_id=1, strategy_id=1, stock_code="600000.SH",
            bar_time=bar_time, param_name="stop_loss_ratio",
        )
    engine._drain_decisions(db, port)
    engine._recorder.record(
        gate="stop_loss", layer="strategy_risk", action="trigger",
        portfolio_id=1, strategy_id=1, stock_code="600000.SH",
        bar_time=datetime(2026, 8, 5, 10, 1), param_name="stop_loss_ratio",
    )
    engine._drain_decisions(db, port)
    db.close()

    rows = _decision_rows(factory)
    assert len(rows) == 2
    assert len([e for e in _drain_queue(q)
                if e.get("type") == "decision" and e.get("gate") == "stop_loss"]) == 2


# ---------------- OPEN 闸门 × 在途单（max_positions 计入 submitted/partial BUY）----------------
def test_handle_bar_inflight_buy_counts_toward_max_positions():
    """在途 BUY（submitted 未成交）计入 max_positions：上限 1、已有在途 → 新 OPEN 拦截。

    根因：闸门 held 只数已成交持仓（quantity>0），submitted 单只建 Position(quantity=0)
    不计入 → 连续 bar 齐发 OPEN 超开（与回测 id7 同根因，共用 Portfolio.on_bar）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    ctx.max_positions = 1
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    # 预置一只在途 BUY（09:55 下单未成交，买的是另一只票）
    db = factory()
    _seed_buy_submitted(db, bar_time=datetime(2026, 8, 5, 9, 55), stock_code="000001.SZ")
    db.commit()
    db.close()
    db = factory()
    engine.recover(db)
    db.close()
    assert engine.pending_orders_count == 1

    engine.signal_cache = {(1, "600000.SH", bar_time): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _bar("600000.SH", "9.3", bar_time))

    # max_positions=1 已被在途占满 → 不下新单
    db = factory()
    assert db.query(LiveOrder).count() == 1
    db.close()


def test_handle_bar_inflight_dup_open_skipped_across_bars():
    """同票在途 BUY 未成交、下一 bar 电平信号再发 OPEN → 不重复下单。

    D6 去重门只拦同 bar_time，跨 bar 的重复 OPEN 靠在途注入拦（否则双重建仓）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    bar_time = datetime(2026, 8, 5, 10, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    _seed_buy_submitted(db, bar_time=datetime(2026, 8, 5, 9, 55))  # 600000.SH 在途
    db.commit()
    db.close()
    db = factory()
    engine.recover(db)
    db.close()

    engine.signal_cache = {(1, "600000.SH", bar_time): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _bar("600000.SH", "9.3", bar_time))

    db = factory()
    assert db.query(LiveOrder).count() == 1  # 未重复下单
    db.close()


# ===========================================================================
# A-2/A-3/A-4：在途单超时真实撤单 / 收盘清扫撤单 / 重启恢复补记提交时间
# （tq_trade.cancel_unfilled_minutes，I-4 + C-1/C-2/C-3 审计修复）
# ===========================================================================
class FakeTdxDispatcher:
    """TDX 形状的最小 dispatcher：委托/成交/撤单 + _submit_times/_known_refs。"""

    channel = "tdx_tq"
    supports_remark = False

    def __init__(self, orders=None, deals=None, cancel_ok=True):
        self.orders = orders or []
        self.deals = deals or []
        self.cancel_ok = cancel_ok
        self.cancel_calls = []
        self.place_calls = []
        self._submit_times = {}
        self._known_refs = set()
        self._claimed_refs = set()

    def heartbeat(self):
        return True

    @staticmethod
    def order_id(order):
        return "fake-%s-%s-%s-%s" % (
            order.strategy_id, order.stock_code, order.quantity, order.price,
        )

    def place_order(self, order):
        """TDX 形状下单：受理即回 TradeEvent（同桥 ok:true 语义，供 requote 提交链）。"""
        from core.engine.event import TradeEvent
        self.place_calls.append(order)
        price = order.price if order.price is not None else Decimal("0")
        return TradeEvent(
            strategy_id=order.strategy_id, portfolio_id=order.portfolio_id,
            stock_code=order.stock_code, trade_type=order.trade_type,
            price=price, quantity=order.quantity,
            amount=price * order.quantity, commission=Decimal("0"),
            stamp_duty=Decimal("0"), trade_time=order.bar_time,
            signal_type=order.signal_type, signal_name=order.signal_name,
        )

    def query_orders(self):
        return list(self.orders)

    def query_deals(self):
        return list(self.deals)

    def query_quote(self, *a, **kw):
        return []

    def query_calendar(self):
        return []

    def get_positions(self):
        return []

    def query_positions(self):
        return []

    def cancel_order(self, ref, code):
        self.cancel_calls.append((ref, code))
        return self.cancel_ok


def _bridge_row(ref, status=50, traded_volume=0, volume=600):
    """dispatcher.query_orders 的 BRIDGE 行（W001 在途单）。"""
    return {
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": volume, "order_ref": ref,
        "remark": "", "status": status, "traded_volume": traded_volume,
    }


def _pending_order_with_ref(db, ref="W001", created_at=None, status="submitted",
                            quantity=600):
    """预置一笔有 order_ref 的在途单（created_at 缺省 = 10 分钟前，UTC naive）。"""
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code="600000.SH", trade_type="buy", order_type="limit",
        price=Decimal("9.3"), quantity=quantity, filled_quantity=0,
        filled_price=None, status=status, signal_name="open_sig",
        signal_type="OPEN", bar_time=datetime(2026, 8, 5, 10, 0),
        order_ref=ref,
        created_at=created_at or (
            datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=10)),
    )
    db.add(lo)
    db.flush()
    return lo


def _cfg_minutes(minutes):
    """monkeypatch 用：core.engine.live.order_machine.load_config → 固定配置。"""
    return lambda: {"tq_trade": {"cancel_unfilled_minutes": minutes}}


_CANCEL_TAG = "超时撤单请求中："  # 与 order_machine._CANCEL_REQUESTED_TAG 一致


def test_cancel_unfilled_timeout_requests_cancel_and_tags(monkeypatch):
    """A-2：超时命中 → cancel_order(ref, code)；成功后打 cancel_requested 标记，
    不立即标 canceled（等 5s 终态同步收敛，防丢已成交量）。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo_id = lo.id
    db.commit()
    db.close()

    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()

    assert disp.cancel_calls == [("W001", "600000.SH")]
    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "submitted"  # 不立即 canceled
    assert (lo2.error_message or "").startswith(_CANCEL_TAG)
    db.close()


def test_cancel_unfilled_converges_when_ref_vanished(monkeypatch):
    """A-2：已请求撤单、委托表已查不到该单（撤单且无成交）→ 直接标 canceled。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[])  # 委托表已无此单
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo.error_message = _CANCEL_TAG + "超时未成交已撤单（阈值 3 分钟）"
    lo_id = lo.id
    db.commit()
    db.close()

    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()

    assert disp.cancel_calls == []  # 已请求过，不重复撤
    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "canceled"
    assert "超时未成交已撤单" in (lo2.error_message or "")
    db.close()


def test_cancel_unfilled_harvests_partial_fill_on_cancel(monkeypatch):
    """A-2：撤单成功后立即按既有 backfill 收割已成交量（不丢部分成交）。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    disp = FakeTdxDispatcher(
        orders=[_bridge_row("W001", status=55, traded_volume=300)],
        deals=[{
            "order_ref": "W001", "price": 9.25, "volume": 300,
            "amount": 2775.0, "commission": 0.7,
            "trade_time": "100001", "trade_date": "20260805",
        }],
    )
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo_id = lo.id
    db.commit()
    db.close()

    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert int(lo2.filled_quantity) == 300   # 已成交量收割
    assert lo2.status == "partial"           # 剩余待终态同步收敛（53/54）
    assert db.query(LiveTrade).count() == 1
    db.close()
    assert len(disp.cancel_calls) == 1


def test_cancel_unfilled_disabled_when_zero(monkeypatch):
    """A-2：cancel_unfilled_minutes<=0 → 完全跳过（禁用）。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(0))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine = _make_engine(disp, factory, port)
    db = factory()
    _pending_order_with_ref(db)
    db.commit()
    db.close()

    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()

    assert disp.cancel_calls == []


def test_cancel_unfilled_below_threshold_skips(monkeypatch):
    """A-2：未达阈值（< cancel_unfilled_minutes）不撤。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(30))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine = _make_engine(disp, factory, port)
    db = factory()
    _pending_order_with_ref(db)  # created_at = 10 分钟前 < 30 分钟
    db.commit()
    db.close()

    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()

    assert disp.cancel_calls == []


def test_cancel_unfilled_invalid_config_falls_back_to_default(monkeypatch):
    """A-2：配置非法（非整数）→ 回退默认 3 分钟（10 分钟前的单命中撤单）。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes("abc"))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine = _make_engine(disp, factory, port)
    db = factory()
    _pending_order_with_ref(db)
    db.commit()
    db.close()

    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()

    assert len(disp.cancel_calls) == 1


def test_cancel_unfilled_failure_alerts_risk_once(monkeypatch):
    """A-2：撤单连续失败 ≥3 次经 risk 事件告警一次（不刷屏），继续重试、单保持 submitted。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")], cancel_ok=False)
    engine = _make_engine(disp, factory, port)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo_id = lo.id
    db.commit()
    db.close()

    for _ in range(4):  # 4 轮失败
        db = factory()
        engine._cancel_unfilled_orders(db)
        db.commit()
        db.close()

    assert len(disp.cancel_calls) == 4  # 每轮都重试
    risks = []
    while not q.empty():
        ev = q.get_nowait()
        if ev.get("type") == "risk":
            risks.append(ev)
    assert len(risks) == 1  # 只告警一次
    db = factory()
    assert db.get(LiveOrder, lo_id).status == "submitted"  # 保留 submitted 继续重试
    db.close()


def test_cancel_unfilled_pending_is_unconfirmed_not_canceled(monkeypatch):
    """缺陷2：桥撤单返回 pending（复查后仍未确认）→ 记「撤单未确认」，不判已撤。

    判据：dispatcher.last_cancel_result == "pending"。订单保持 submitted 且下一轮
    仍会重试撤单（error_message 打 cancel_unconfirmed 标记而非 cancel_requested），
    并发一次性 risk 告警（人工可见），绝不静默当收工。
    """
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()

    class _PendingCancelDispatcher(FakeTdxDispatcher):
        def cancel_order(self, ref, code):
            self.cancel_calls.append((ref, code))
            self.last_cancel_result = "pending"
            return False

    disp = _PendingCancelDispatcher(orders=[_bridge_row("W001")])
    engine = _make_engine(disp, factory, port)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo_id = lo.id
    db.commit()
    db.close()

    for _ in range(2):  # 两轮都 pending
        db = factory()
        engine._cancel_unfilled_orders(db)
        db.commit()
        db.close()

    assert len(disp.cancel_calls) == 2  # pending 不算收工，下一轮继续重试
    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "submitted"  # 绝不判已撤
    assert (lo2.error_message or "").startswith("cancel_unconfirmed:")
    assert not (lo2.error_message or "").startswith(_CANCEL_TAG)
    db.close()
    risks = []
    while not q.empty():
        ev = q.get_nowait()
        if ev.get("type") == "risk":
            risks.append(ev)
    assert [r.get("rule") for r in risks] == ["cancel_unconfirmed"]  # 只告警一次


def test_close_sweep_cancels_real_order_before_marking(monkeypatch):
    """A-3：收盘清扫标 canceled 前 best-effort 撤真实单。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[], deals=[])  # 实时表已无此单
    engine = _make_engine(disp, factory, port)
    from core.engine.trading_calendar import TradingCalendar
    engine._calendar = TradingCalendar(lambda: [date(2026, 8, 24)])
    db = factory()
    _pending_order_with_ref(db, ref="ref-tdx-1")
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))

    assert disp.cancel_calls == [("ref-tdx-1", "600000.SH")]
    db = factory()
    lo = db.query(LiveOrder).first()
    assert lo.status == "canceled"
    db.close()


def test_close_sweep_cancel_failure_does_not_block_sweep():
    """A-3：撤单失败（best-effort）仅告警，不阻断清扫终态（收盘后当日单自然失效）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[], deals=[], cancel_ok=False)
    engine = _make_engine(disp, factory, port)
    from core.engine.trading_calendar import TradingCalendar
    engine._calendar = TradingCalendar(lambda: [date(2026, 8, 24)])
    db = factory()
    _pending_order_with_ref(db, ref="ref-tdx-1")
    db.commit()
    db.close()

    engine._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))

    db = factory()
    assert db.query(LiveOrder).first().status == "canceled"  # 清扫不被阻断
    db.close()


def test_recover_backfills_submit_times_and_known_refs():
    """A-4（C-2）：重启恢复挂回在途单时，ref 并入 _known_refs、按 created_at(UTC)
    换算上海时间补记 _submit_times（通达信委托表无时间列，不补记则窗口匹配哑火）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher()
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo1 = _pending_order_with_ref(db, ref="W001",
                                  created_at=datetime(2026, 10, 26, 2, 0))  # UTC 02:00
    _pending_order_with_ref(db, ref=None)  # 无 ref 的单：不补记，也不崩
    db.commit()
    db.close()

    db = factory()
    engine.recover(db)
    db.close()

    assert disp._known_refs == {"W001"}
    assert disp._submit_times["W001"] == ("20261026", "100000")  # UTC+8 → 上海 10:00


def test_recover_marks_pending_refs_as_claimed():
    """批次 A 评审：挂回单 ref 已绑定 DB 单（语义即已认领），必须同时进
    _claimed_refs——否则重启后 fallback「形状匹配且未 claimed」可能把新单
    绑到已恢复单的 ref，deals 按 ref 双份回填导致持仓双计。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher()
    engine = _make_engine(disp, factory, port)
    db = factory()
    _pending_order_with_ref(db, ref="W001")
    db.commit()
    db.close()

    db = factory()
    engine.recover(db)
    db.close()

    assert "W001" in disp._claimed_refs


# ---------------- 批次 D：实盘健壮性（I-5 stop 竞态 / I-7 对账可见 / I-8 deals 独立 worker）----------------
def test_handle_bar_rejects_orders_after_stop():
    """I-5：stop 请求后提交链入口拒单（deferred 放行与正常单共用入口），不再发起新单。

    竞态窗口：stop() 置停止标记后，worker 线程在途的最后一轮 tick 仍可能带着
    bar 走到 _handle_bar——入口闸保证这轮不再落单/发桥。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"

    engine._handle_bar(port, bar)  # 未 stop：正常提交（基线）

    db = factory()
    assert db.query(LiveOrder).count() == 1
    db.close()

    asyncio.run(engine.stop())
    # 清掉在途单，排除 F7 在途门干扰——只验证提交链入口闸本身；
    # 换一只股票 + 新 bar，排除同股重复开仓/同 bar 去重等既有守卫。
    engine._pending_orders.clear()
    stock2 = "000001.SZ"
    bar_time2 = datetime(2026, 8, 5, 10, 1)
    engine.signal_cache = {(1, stock2, bar_time2): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _bar(stock2, "9.3", bar_time2))

    db = factory()
    assert db.query(LiveOrder).count() == 1  # stop 后拒单，不落新单
    db.close()


# ===========================================================================
# （重建批次）deferred 源头幂等 / 恢复期异常告警（2026-11-05 上午批次）
#
# 说明：这 5 个用例在 2026-11-05 晚间因一次误操作（git checkout）从工作树丢失，
# 已按丢失前字节码与既有实现重建；语义不变（覆盖 A 源头幂等 + C 陈旧清理 +
# D 恢复期两条 risk 告警），实现仍以 live_engine.recover/_find_duplicate_deferred_row
# 为准。
# ===========================================================================


def _deferred_order(stock, day, qty=300):
    """B-1 deferred 形状的 OrderEvent（日线信号、当日 defer）。"""
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=TradeType.SELL, signal_type=SignalType.CLOSE,
        quantity=qty, price=Decimal("9.0"),
        bar_time=datetime.combine(day, datetime.min.time()),
        signal_name="stop_loss", signal_bar_date=day,
    )


def _add_deferred_row(db, stock, bar_time, qty=300):
    """直接造一条 deferred 落库行（模拟重启前残留）。"""
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code=stock, trade_type="sell", order_type="limit",
        price=Decimal("9.000"), quantity=qty, filled_quantity=0,
        status="deferred", signal_name="stop_loss", signal_type="CLOSE",
        bar_time=bar_time,
    )
    db.add(lo)
    db.flush()
    return lo


def test_defer_source_idempotent_skips_same_key(caplog):
    """A 源头幂等：同键第二次 defer 不落库、不进内存队列，并打 skip 日志。"""
    caplog.set_level("WARNING")
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    day = date(2026, 8, 24)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [day], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, day), "1d")
    engine._submit_one_order(db, port, _deferred_order(stock, day), "1d")
    db.close()

    db = factory()
    rows = db.query(LiveOrder).filter(LiveOrder.status == "deferred").all()
    db.close()
    assert len(rows) == 1
    assert len(engine._signal_bar_deferred) == 1
    hit = [r for r in caplog.records if "defer duplicate skipped" in r.getMessage()]
    assert hit
    assert ("existing deferred row id=%s" % rows[0].id) in hit[0].getMessage()


def test_defer_source_keeps_different_keys():
    """A 幂等键含 strategy/quantity：同股同日同向不同数量/不同股票均合法保留。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock, other = "600000.SH", "000001.SZ"
    day = date(2026, 8, 24)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [day], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, day, qty=100), "1d")
    engine._submit_one_order(db, port, _deferred_order(stock, day, qty=200), "1d")
    engine._submit_one_order(db, port, _deferred_order(other, day, qty=100), "1d")
    db.close()

    db = factory()
    assert db.query(LiveOrder).filter(LiveOrder.status == "deferred").count() == 3
    db.close()
    assert len(engine._signal_bar_deferred) == 3


def test_recover_drops_stale_deferred_keeps_prev_trading_day_and_today(monkeypatch):
    """C 陈旧清理：早于「上一交易日」置 canceled 留审计；上一交易日/当日转内存。

    2026-11-09 修正：阈值从「日历天 -1」改为「上一交易日」——周五 defer、
    周一启动不再被误判陈旧（旧实现 Friday < Sunday 直接丢）。
    """
    from core.engine.trading_calendar import TradingCalendar
    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    # 固定「今天」= 2026-08-24（周一），让上一交易日确定为 8-21（周五）
    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai",
        lambda: datetime(2026, 8, 24, 9, 0),
    )
    engine._calendar = TradingCalendar(
        lambda: [date(2026, 8, 20), date(2026, 8, 21), date(2026, 8, 24)]
    )
    stale_day = date(2026, 8, 20)   # 周四：早于上一交易日 → 陈旧
    prev_day = date(2026, 8, 21)    # 周五：上一交易日 → 保留（跨周末修复点）
    today = date(2026, 8, 24)
    db = factory()
    _add_deferred_row(db, "600000.SH", datetime.combine(stale_day, datetime.min.time()))
    _add_deferred_row(db, "600519.SH", datetime.combine(prev_day, datetime.min.time()))
    _add_deferred_row(db, "000001.SZ", datetime.combine(today, datetime.min.time()))
    db.commit()
    engine.recover(db)
    db.close()

    db = factory()
    rows = db.query(LiveOrder).order_by(LiveOrder.id).all()
    db.close()
    assert len(rows) == 3                          # 三行都保留（DB 为权威足迹）
    assert rows[0].status == "canceled"            # 仅陈旧行 canceled 留审计
    assert "stale_restart" in (rows[0].error_message or "")
    assert rows[0].stock_code == "600000.SH"
    assert [r.status for r in rows[1:]] == ["deferred", "deferred"]
    assert len(engine._signal_bar_deferred) == 2   # 上一交易日 + 今日转内存队列
    assert all(getattr(o, "_deferred_row_id", None)
               for o in engine._signal_bar_deferred)  # 行 id 已挂上（放行时收尾）

    bar = _bar("600519.SH", "9.3",
               datetime.combine(today, datetime.min.time()).replace(hour=9, minute=31))
    released = engine._release_signal_bar_deferred(port, bar)
    assert len(released) == 1
    assert released[0].stock_code == "600519.SH"


def test_recover_alerts_when_deferred_count_exceeds_threshold(monkeypatch):
    """D①：重建条数超阈值 → 一条 deferred_recover_volume risk 事件（含代码清单）。"""
    import importlib
    live_mod = importlib.import_module("core.engine.live_engine")
    monkeypatch.setattr(live_mod, "DEFERRED_RECOVER_ALERT_THRESHOLD", 2)
    factory, _ = _db_factory()
    engine, _ = _breaker_engine(factory)
    yday = now_shanghai().date() - timedelta(days=1)
    bt = datetime.combine(yday, datetime.min.time())
    db = factory()
    for code in ("600000.SH", "600519.SH", "000001.SZ"):
        _add_deferred_row(db, code, bt)
    db.commit()
    events = []
    monkeypatch.setattr(engine, "_emit", lambda *a: events.append(a))
    engine.recover(db)
    db.close()

    risk = [p for t, p in events
            if t == "risk" and p.get("rule") == "deferred_recover_volume"]
    assert len(risk) == 1
    assert risk[0]["count"] == 3
    assert risk[0]["threshold"] == 2
    assert set(risk[0]["stock_codes"]) == {"600000.SH", "600519.SH", "000001.SZ"}
    assert "3" in risk[0]["message"]
    assert "人工确认" in risk[0]["message"]
    assert len(engine._signal_bar_deferred) == 3


def test_recover_alerts_on_duplicate_deferred_rows(monkeypatch):
    """D②：同键多行（旧版本遗留）→ deferred_recover_duplicate risk 事件（含 row ids）。"""
    factory, _ = _db_factory()
    engine, _ = _breaker_engine(factory)
    yday = now_shanghai().date() - timedelta(days=1)
    bt = datetime.combine(yday, datetime.min.time())
    db = factory()
    _add_deferred_row(db, "600888.SH", bt, qty=900)
    _add_deferred_row(db, "600888.SH", bt, qty=900)
    db.commit()
    events = []
    monkeypatch.setattr(engine, "_emit", lambda *a: events.append(a))
    engine.recover(db)
    db.close()

    risk = [p for t, p in events
            if t == "risk" and p.get("rule") == "deferred_recover_duplicate"]
    assert len(risk) == 1
    assert risk[0]["count"] == 2
    assert len(risk[0]["row_ids"]) == 2
    assert risk[0]["stock_code"] == "600888.SH"
    assert risk[0]["quantity"] == 900
    assert "人工确认" in risk[0]["message"]
    # 启动恢复同键去重：保留首行进队列，重复行置 canceled（不再两笔都放行）
    assert len(engine._signal_bar_deferred) == 1
    db = factory()
    rows = db.query(LiveOrder).order_by(LiveOrder.id).all()
    db.close()
    assert len(rows) == 2
    assert [r.status for r in rows] == ["deferred", "canceled"]
    assert "duplicate_deferred" in (rows[1].error_message or "")


# ===========================================================================
# （重建批次）桥通道 / 快照合成器 / 循环自愈
# ===========================================================================


def _confirming_respond(request):
    """模拟可确认成交的桥：/order 带 cl_ord_id，/orders 回读为 filled。

    新语义（ok:true 不再直接等于成交）：只有拿到委托标识且回读到成交才算成交，
    故成功路径用例需用该 responder，否则订单只落 submitted（在途待确认）。
    """
    path = request.url.path
    if path == "/order":
        return httpx.Response(200, json={"ok": True, "cl_ord_id": "cl-mock"})
    if path == "/orders":
        return httpx.Response(200, json={
            "ok": True, "data": [{"cl_ord_id": "cl-mock", "status": "filled"}]})
    if path == "/ping":
        return httpx.Response(200, json={"ok": True})
    if path in ("/positions", "/quote"):
        return httpx.Response(404)
    return httpx.Response(200, json={"ok": False, "error": "unknown"})


def test_handle_bar_bridge_read_timeout_keeps_order_submitted():
    """桥 /order 读超时（请求已发出）→ 保持 submitted 在途，不判 rejected。

    原行为：超时抛 BridgeUnavailableError → rejected + 移出在途集合，真实委托
    脱离跟踪（无 ref、无成交回填）。修正：dispatcher 返回 None（在途）+
    last_unconfirmed_order_id，引擎按「已发出未认领」保持 submitted 等回查。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)

    def respond(request):
        if request.url.path == "/order":
            raise httpx.ReadTimeout("read timeout")
        return httpx.Response(200, json={"ok": True, "data": []})

    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1
    lo = orders[0]
    assert lo.status == "submitted", "超时不得判 rejected（真实在途单会脱跟踪）"
    assert lo.bridge_order_id == disp.last_unconfirmed_order_id
    assert "在途" in (lo.error_message or "")
    assert engine._bridge_online is True, "下单超时不应置桥离线（心跳仍正常）"
    assert db.query(LiveTrade).count() == 0
    assert len(engine._pending_orders) == 1
    db.close()

    events = _drain_queue(q)
    # 事件流含 signal/order（在途单先推 submitted，再推一条带原因说明的 submitted
    # 更新）以及每组合末尾 drain 的 decision 事件；只校验与本用例相关的两类。
    assert any(e["type"] == "signal" for e in events)
    assert any(e["type"] == "order" and e["status"] == "submitted" for e in events)
    assert all(e["type"] in ("signal", "order", "decision") for e in events)


def test_bridge_slow_emits_risk_event():
    """桥单次调用超慢阈值 → 引擎事件流一条 risk/bridge_slow（与离线/协议错误区分）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._on_bridge_slow("/orders", 20.0)

    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "bridge_slow"
    assert ev["endpoint"] == "/orders"
    assert ev["elapsed_seconds"] == 20.0
    assert "20.0" in ev["message"]
    assert ev["window_slow_count"] == 0


def test_bridge_slow_event_carries_window_summary():
    """窗口过期后的首条慢事件带上一窗口汇总（N 次 + 最慢端点/耗时）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._on_bridge_slow("/ping", 19.0, {
        "count": 3, "slowest_endpoint": "/account",
        "slowest_elapsed_seconds": 19.3, "window_seconds": 300.0,
    })

    ev = q.get_nowait()
    assert ev["rule"] == "bridge_slow"
    assert ev["window_slow_count"] == 3
    assert ev["slowest_endpoint"] == "/account"
    assert ev["slowest_elapsed_seconds"] == 19.3
    assert "另有 3 次慢请求" in ev["message"]
    assert "最慢 /account 19.3" in ev["message"]


def test_tick_deals_samples_snapshot_synth():
    """快照合成器挂 deals 节拍：_tick_deals 每轮 sample() 一次（无在途单也采样）。"""
    factory, _ = _db_factory()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    port, _ = _portfolio_single()
    engine = _make_engine(disp, factory, port)

    class _StubSynth:
        def __init__(self):
            self.sample_calls = 0
            self.reset_calls = 0

        def sample(self, now):
            self.sample_calls += 1

        def reset(self):
            self.reset_calls += 1

    stub = _StubSynth()
    engine._snapshot_synth = stub
    engine._tick_deals()
    engine._tick_deals()
    assert stub.sample_calls == 2
    assert stub.reset_calls == 0


def test_bridge_back_online_resets_snapshot_synth():
    """E8 离线恢复：reset_baseline 同时重置快照合成器（丢弃断线期 forming 态）。"""
    factory, _ = _db_factory()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    port, _ = _portfolio_single()
    engine = _make_engine(disp, factory, port)

    class _StubSynth:
        def __init__(self):
            self.reset_calls = 0

        def reset(self):
            self.reset_calls += 1

    stub = _StubSynth()
    engine._snapshot_synth = stub
    engine._bridge_online = False
    engine._tick_main()
    assert stub.reset_calls == 1


def test_loop_restarts_after_running_cancel(caplog):
    """生产 08:54 实证死法：_loop 任务在 _running=True 时被外部 cancel
    （非 stop 路径）→ 监控告警 + 自愈重启，引擎继续 tick（心跳继续发生）。"""
    import logging as _logging
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, poll_interval=0.01,
    )

    async def run_a_bit():
        engine._running = True
        task = asyncio.create_task(engine._loop())
        engine._task = task
        await asyncio.sleep(0.01)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        engine._on_loop_done("main", task)   # 模拟 done-callback（异常死亡）
        await asyncio.sleep(0.01)
        engine._running = False
        if engine._task is not None and not engine._task.done():
            engine._task.cancel()
        await asyncio.sleep(0)

    asyncio.run(run_a_bit())

    warns = [r for r in caplog.records
             if r.levelno >= _logging.WARNING and "auto-restart" in r.getMessage()]
    assert warns, "运行中被取消必须产生 auto-restart 告警"


def test_on_loop_done_restarts_while_running():
    """done-callback 直接单测：异常退出 + _running=True → 新 _loop 任务接管。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory, poll_interval=0.01,
    )

    async def drive():
        engine._running = True

        async def boom():
            raise RuntimeError("loop died")

        task = asyncio.create_task(boom())
        await asyncio.sleep(0)
        task.exception()  # 取走异常，避免 "never retrieved" 告警
        engine._on_loop_done("main", task)
        await asyncio.sleep(0.01)
        assert engine._task is not None and not engine._task.done()
        engine._running = False
        engine._task.cancel()

    asyncio.run(drive())


def test_on_loop_done_no_restart_after_stop():
    """stop 后（_running=False）任务退出不重启（正常停止路径静默）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )

    async def drive():
        engine._running = False

        async def boom():
            raise RuntimeError("loop died")

        task = asyncio.create_task(boom())
        await asyncio.sleep(0)
        task.exception()
        before = engine._task
        engine._on_loop_done("main", task)
        await asyncio.sleep(0.01)
        assert engine._task is before

    asyncio.run(drive())


# ===========================================================================
# 记帐真相加固：cl_ord_id 受理即落库 / 拒单立即落库 / 认领优先精确 / 无法追溯不伪造
# ===========================================================================
_CL_ORD_ID = "ebcae8fc-d8f5-11f1-99dc-e073e7b517ad"


def _place_via_handle_bar(respond, stock="600000.SH", bar_time=None):
    """走完整 _handle_bar 发单链（真实下单点），返回 (engine, disp, factory, port, rec)。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    bar_time = bar_time or datetime(2026, 8, 5, 10, 0)
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1m"
    engine._handle_bar(port, bar)
    return engine, disp, factory, port, rec


def test_order_accept_persists_broker_cl_ord_id_to_db():
    """桥 /order 受理应答带 cl_ord_id → dispatcher 暴露 + 落库 order_ref（重启不丢）。

    bridge_order_id 仍是确定性 oid（其前 20 位是写进柜台的 remark），不得被覆盖。
    """
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={
                "ok": True, "data": {"cl_ord_id": _CL_ORD_ID, "status": "54"}})
        return httpx.Response(200, json={"ok": True, "data": []})

    _, disp, factory, _, _ = _place_via_handle_bar(respond)

    assert disp.last_broker_order_id == _CL_ORD_ID
    db = factory()
    lo = db.query(LiveOrder).one()
    assert lo.status == "submitted"           # 未回读到成交 → 在途，不伪造 filled
    assert lo.order_ref == _CL_ORD_ID         # 柜台委托号受理即落库（精确认领键）
    assert lo.bridge_order_id != _CL_ORD_ID   # remark 认领键不被覆盖
    assert len(lo.bridge_order_id) == 32
    assert db.query(LiveTrade).count() == 0
    db.close()


def test_order_reject_persists_reason_immediately_without_readback():
    """桥 ok:false + 拒单原文 → live_orders 立即落库（P1-3 起为 deferred 回显原因），
    且不依赖 /orders 回读。"""
    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={
                "ok": False, "rejected": True, "reason": "价格超出涨跌停限制"})
        return httpx.Response(200, json={"ok": True, "data": []})

    _, disp, factory, _, rec = _place_via_handle_bar(respond)

    assert disp.last_order_rejection == "价格超出涨跌停限制"
    db = factory()
    lo = db.query(LiveOrder).one()
    assert lo.status == "deferred"
    assert "deferred_retry:n=1" in (lo.error_message or "")
    assert "价格超出涨跌停限制" in (lo.error_message or "")
    assert db.query(LiveTrade).count() == 0
    db.close()
    assert not any(r.url.path == "/orders" for r in rec.requests)


def test_poll_deals_claims_by_persisted_cl_ord_id_precisely():
    """已落库 cl_ord_id（order_ref）→ /orders、/deals 按该 ref 精确回填，零误绑。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": _CL_ORD_ID, "remark": "",
    }], deals=[{
        "order_ref": _CL_ORD_ID, "price": 9.25, "volume": 600,
        "amount": 5550.0, "commission": 1.39, "trade_time": "150001",
        "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=_TEST_OID)
    lo.order_ref = _CL_ORD_ID   # 受理时已落库的柜台委托号
    db.commit()
    lo_id = lo.id
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "filled"
    assert lo2.order_ref == _CL_ORD_ID
    assert lo2.filled_quantity == 600
    assert db.query(LiveTrade).count() == 1
    db.close()


def test_expire_untraceable_keeps_pending_and_emits_risk():
    """委托表/成交表都查不到且超窗 → 保持 pending + bridge_order_untraceable，不伪造。

    HTTP 桥（supports_remark=True）：柜台当日委托查询不含废单、桥重启丢内存台账，
    故不自动驳回（DB 是记帐真相，留待人工核对），也不得生成任何成交。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])   # 委托表/成交表都没有该单
    disp, _ = _make_dispatcher(rec)
    assert disp.supports_remark is True
    engine = _make_engine(disp, factory, port)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=_TEST_OID)
    # 5 分钟：超 180s 但未到 600s 兜底窗 → 保持 pending + 一次性告警
    lo.created_at = (
        datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=5))
    db.commit()
    lo_id = lo.id
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "submitted"                 # 保持 pending，不自动驳回
    assert (lo2.error_message or "").startswith("bridge_order_untraceable:")
    assert db.query(LiveTrade).count() == 0          # 绝不伪造成交
    db.close()

    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "bridge_order_untraceable"
    assert ev["order_id"] == lo_id
    assert ev["bridge_order_id"] == _TEST_OID
    assert ev["stock_code"] == "600000.SH"
    assert ev["trade_type"] == "BUY"
    assert ev["quantity"] == 600
    assert ev["submitted_at"]

    # 下一轮不重复告警（一次性 risk 事件），仍保持 pending
    engine._poll_deals()
    assert q.empty()
    db = factory()
    assert db.get(LiveOrder, lo_id).status == "submitted"
    db.close()


def test_stop_closes_dispatcher_in_worker_before_pool_shutdown():
    """I-5/I-8：stop 时 dispatcher.close 经主 executor（worker 线程）执行，
    且发生在两池 shutdown 之前（close 排队在途 tick 之后，需主 worker 存活）。"""
    import threading

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    observed = []

    async def drive():
        await engine.start()
        orig = engine._dispatcher_close_safe

        def spy():
            observed.append(
                (threading.current_thread().name, engine._executor._shutdown)
            )
            orig()

        engine._dispatcher_close_safe = spy
        await engine.stop()

    asyncio.run(drive())

    assert observed, "stop 未执行 dispatcher close"
    assert observed[0][0] != threading.main_thread().name, (
        "dispatcher close 在事件循环线程同步执行——TDX 退订对账会冻结事件循环"
    )
    assert observed[0][1] is False, "close 执行时主池已提前 shutdown（排队语义丢失）"
    assert engine._executor._shutdown
    assert engine._deals_executor._shutdown


def test_deals_loop_ticks_run_in_dedicated_deals_pool():
    """I-8：_deals_loop 的 tick 投独立单 worker 池（thread 名前缀 live-deals），
    与主循环各自的单 worker 池互不饿死。"""
    import threading

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        deals_poll_interval=0.01,
    )
    threads = set()
    engine._poll_deals = lambda: threads.add(threading.current_thread().name)

    async def drive():
        await engine.start()
        await asyncio.sleep(0.05)
        await engine.stop()

    asyncio.run(drive())

    assert threads, "_poll_deals 未被调用"
    assert all(t.startswith("live-deals") for t in threads), (
        "deals tick 未投独立池（线程名=%s）" % threads
    )


def test_reconcile_mismatch_emits_risk_event():
    """I-7：对账差异除日志外推 risk 事件（形状照既有 risk：rule/message），工作台可见。"""
    factory, _ = _db_factory()
    engine, _, ctx = _reconcile_engine(factory, 1000, respond=_positions_respond(800))
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._reconcile_positions()  # recover 已跑过一次，差异仍在，复查再推

    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "reconcile_mismatch"
    assert ev["stock_code"] == "600000.SH"
    assert ev["virtual"] == 1000
    assert ev["real"] == 800
    assert ev["diff"] == 200
    assert "600000.SH" in ev["message"]


def test_reconcile_mismatches_property_exposed():
    """I-7：引擎暴露 reconcile_mismatches 只读摘要（session API 序列化用）。"""
    factory, _ = _db_factory()
    engine, _, _ = _reconcile_engine(factory, 1000, respond=_positions_respond(800))

    got = engine.reconcile_mismatches
    assert len(got) == 1
    assert {k: got[0][k] for k in ("code", "virtual", "real", "diff")} == {
        "code": "600000.SH", "virtual": 1000, "real": 800, "diff": 200}


def test_close_reconcile_runs_once_after_sweep_completes():
    """I-7：收盘清扫完成后每交易日一次对账复查；清扫未完成/重复调用不触发。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    calls = []
    engine._reconcile_positions = lambda: calls.append(1)

    # 清扫未完成（closer 未置当日标记）→ 不触发
    engine._maybe_reconcile_after_close(now=datetime(2026, 8, 5, 15, 6))
    assert calls == []

    # 清扫完成 → 触发一次
    engine._last_close_sweep_date = date(2026, 8, 5)
    engine._maybe_reconcile_after_close(now=datetime(2026, 8, 5, 15, 6))
    assert len(calls) == 1

    # 同日重复 tick → 幂等不再触发
    engine._maybe_reconcile_after_close(now=datetime(2026, 8, 5, 15, 10))
    assert len(calls) == 1


# ===========================================================================
# 终审修复批次：C2 记账锁并发回归 / I-1 撤单收口收割 / I-2 fabricated 时间窗 /
# I-3 stop 在途单撤单
# ===========================================================================
def _db_factory_concurrent(tmp_path):
    """并发回归用文件 SQLite：两线程各取独立连接（非 StaticPool 单连接），
    WAL + busy_timeout 降低读写锁冲突。返回 (factory, engine)。"""
    from sqlalchemy import event as sa_event

    eng = create_engine(
        "sqlite:///%s" % (tmp_path / "conc.db"),
        connect_args={"check_same_thread": False, "timeout": 30},
    )

    @sa_event.listens_for(eng, "connect")
    def _pragma(dbapi_conn, _rec):
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.close()

    Base.metadata.create_all(eng)
    SessionLocal = sessionmaker(bind=eng, expire_on_commit=False)

    def factory():
        return SessionLocal()

    return factory, eng


def test_concurrent_sweep_and_deals_backfill_apply_once(monkeypatch, tmp_path):
    """C2 终审回归：主池超时收割（_sweep_timeout_orders → cancel 收割 apply）与
    deals 池回填（_tick_deals → poll_deals → backfill → apply）两线程 + Barrier
    并发驱动同一张单，apply_filled_trade 恰好一次（不双重记账、不重复写 LiveTrade）。
    两轮分别覆盖两种锁序：收割先进锁（cancel 恰一次）、回填先进锁（单已 filled，
    收割路径无单可撤）——两种交错下记账结果一致。"""
    import threading
    import time as _time

    monkeypatch.setattr(
        "core.engine.live.order_machine.load_config", _cfg_minutes(3))

    for round_no, sweep_first in enumerate((True, False), start=1):
        subdir = tmp_path / ("round%d" % round_no)
        subdir.mkdir()
        factory, _ = _db_factory_concurrent(subdir)
        port, ctx = _portfolio_single()
        stock = "600000.SH"
        disp = FakeTdxDispatcher(
            orders=[_bridge_row("W001", status=55, traded_volume=300)],
            deals=[{
                "order_ref": "W001", "price": 9.25, "volume": 600,
                "amount": 5550.0, "commission": 1.39,
                "trade_time": "100001", "trade_date": "20260805",
            }],
        )
        engine = _make_engine(disp, factory, port)
        db = factory()
        lo = _pending_order_with_ref(db)
        db.commit()
        db.close()

        applies = []
        orig_apply = engine._order_machine.apply_filled_trade

        def counting_apply(*a, **kw):
            applies.append(1)
            return orig_apply(*a, **kw)

        engine._order_machine.apply_filled_trade = counting_apply

        poll_results = []
        orig_poll = engine._order_machine.poll_deals

        def counting_poll():
            try:
                orig_poll()
                poll_results.append("ok")
            except Exception as e:  # noqa: BLE001 — 回传给断言（防测试空转通过）
                poll_results.append("err:%s" % e)
                raise

        engine._order_machine.poll_deals = counting_poll

        barrier = threading.Barrier(2, timeout=15)
        errors = []

        def run_sweep():
            db = factory()
            try:
                barrier.wait()
                if not sweep_first:
                    _time.sleep(0.1)  # 让 deals 路径先进锁
                engine._sweep_timeout_orders(db)  # 真实调用点：锁内 expire+cancel+commit
            except Exception as e:  # noqa: BLE001
                errors.append(("sweep", e))
            finally:
                db.close()

        def run_deals():
            try:
                barrier.wait()
                if sweep_first:
                    _time.sleep(0.1)  # 让收割路径先进锁
                engine._tick_deals()  # 真实调用点：锁内 poll_deals
            except Exception as e:  # noqa: BLE001
                errors.append(("deals", e))

        t1 = threading.Thread(target=run_sweep)
        t2 = threading.Thread(target=run_deals)
        t1.start()
        t2.start()
        t1.join(30)
        t2.join(30)

        tag = "round%d(sweep_first=%s)" % (round_no, sweep_first)
        assert errors == [], "%s 并发路径出现异常：%s" % (tag, errors)
        assert poll_results == ["ok"], "%s deals 回填未成功完成：%s" % (tag, poll_results)
        assert len(applies) == 1, (
            "%s apply_filled_trade 被执行 %d 次（应恰好 1 次）" % (tag, len(applies))
        )
        if sweep_first:
            assert len(disp.cancel_calls) == 1  # 收割路径先进锁：cancel 恰好一次
        else:
            assert disp.cancel_calls == []  # 回填先进锁：单已 filled，无单可撤
        db = factory()
        lo2 = db.get(LiveOrder, lo.id)
        assert lo2.status == "filled"
        assert int(lo2.filled_quantity) == 600
        assert db.query(LiveTrade).count() == 1
        db.close()
        assert ctx.positions[stock].quantity == 600  # 持仓只记一次
        assert port.account.cash == Decimal("100000") - (
            Decimal("5550.0") + Decimal("1.39"))


def test_finalize_timeout_harvests_residual_deal_before_canceled(monkeypatch):
    """I-1：撤单请求后委托表已查不到该单，但 /deals 有受理与生效间的残余成交 →
    finalize 前先收割成 LiveTrade + apply，再标 canceled（末笔成交不丢）。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    disp = FakeTdxDispatcher(
        orders=[],  # 委托表已无此单（撤单已生效）
        deals=[{
            "order_ref": "W001", "price": 9.25, "volume": 200,
            "amount": 1850.0, "commission": 0.5,
            "trade_time": "100001", "trade_date": "20260805",
        }],
    )
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo.error_message = _CANCEL_TAG + "超时未成交已撤单（阈值 3 分钟）"
    lo_id = lo.id
    db.commit()
    db.close()

    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()

    assert disp.cancel_calls == []  # 已 tag：不重复撤单
    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "canceled"
    assert int(lo2.filled_quantity) == 200  # 残余成交已收割
    assert db.query(LiveTrade).count() == 1
    db.close()
    assert ctx.positions["600000.SH"].quantity == 200  # 撤单终态前 apply 落持仓


def test_finalize_timeout_not_canceled_when_residual_deal_fills_order(monkeypatch):
    """I-1：残余成交把单补齐（filled）→ 终态由回填路径落定，不再标 canceled。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    disp = FakeTdxDispatcher(
        orders=[],
        deals=[{
            "order_ref": "W001", "price": 9.25, "volume": 600,
            "amount": 5550.0, "commission": 1.39,
            "trade_time": "100001", "trade_date": "20260805",
        }],
    )
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo.error_message = _CANCEL_TAG + "超时未成交已撤单（阈值 3 分钟）"
    lo_id = lo.id
    db.commit()
    db.close()

    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "filled"  # 残量补齐：filled 而非 canceled
    # error_message 保持撤单 tag 原样（finalize 未改写），未追加 canceled 收口文案
    assert (lo2.error_message or "").startswith(_CANCEL_TAG)
    db.close()
    assert ctx.positions["600000.SH"].quantity == 600


def test_match_legacy_fuzzy_ignores_fabricated_candidate():
    """I-2：fabricated 候选（陌生 ref 补记的当前时间）不参与 legacy 绑定——补记时间
    恒为 now，时间窗校验对它无意义，同形态遗留单会在新单创建后的首次回查即被误绑。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "W100",
         "insert_date": "20260813", "insert_time": "112200",
         "insert_fabricated": True},  # 陌生 ref 补记的 now，非柜台真实插入时间
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=None)
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)  # UTC = 北京 11:21
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref is None  # fabricated 候选绝不绑定
    db.close()


def test_match_legacy_fuzzy_prefers_real_time_over_fabricated():
    """I-2：真实时间候选与 fabricated 候选同形状并存 → 认领真实时间的新 ref，
    而非被补记 now 的遗留单（真机误绑形态：pre 含 W100 遗留单 + 新单 11:21）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "W100",
         "insert_date": "20260813", "insert_time": "112200",
         "insert_fabricated": True},
        {"source": "BRIDGE", "instrument": "600000", "exchange": "SH",
         "direction": 48, "volume": 600, "order_ref": "ref-real",
         "insert_date": "20260813", "insert_time": "112200"},
    ])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600, bridge_order_id=None)
    lo.created_at = datetime(2026, 8, 13, 3, 21, 0)  # UTC = 北京 11:21
    db.commit()

    engine._try_match_order_ref(lo)

    assert lo.order_ref == "ref-real"  # 认领新 ref 而非 W100
    db.close()


def test_stop_cancels_inflight_order_with_ref(caplog):
    """I-3：stop 时对在途真实单 best-effort 撤单；成功且无失败/无 ref → 不发 risk。
    deferred 队列清空条数记 info 日志。"""
    import logging as _logging

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[], deals=[])
    engine = _make_engine(disp, factory, port)
    db = factory()
    _pending_order_with_ref(db)
    db.commit()
    db.close()
    engine._signal_bar_deferred.append(OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000.SH",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=600, price=Decimal("9.3"), bar_time=datetime(2026, 8, 5, 10, 0),
        signal_name="open_sig", signal_bar_date=date(2026, 8, 5),
    ))
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    with caplog.at_level(_logging.INFO, logger="core.engine.live_engine"):
        asyncio.run(engine.stop())

    assert disp.cancel_calls == [("W001", "600000.SH")]
    assert q.empty()  # 全部撤单成功且无无-ref 单 → 不发风险汇总
    assert engine._signal_bar_deferred == []
    assert any(
        "deferred daily-signal order" in r.getMessage() for r in caplog.records
    )


def test_stop_emits_risk_summary_when_cancel_fails_or_no_ref():
    """I-3：撤单失败与无 order_ref 的在途单 → 一条中文 risk 汇总（工作台可见）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[], deals=[], cancel_ok=False)
    engine = _make_engine(disp, factory, port)
    db = factory()
    _pending_order_with_ref(db, ref="W001")
    _pending_order_with_ref(db, ref=None)  # 无 ref：无法撤，计入汇总
    db.commit()
    db.close()
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    asyncio.run(engine.stop())

    assert disp.cancel_calls == [("W001", "600000.SH")]  # 失败也尝试了
    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "stop_inflight_orders"
    assert "撤单失败" in ev["message"] and "W001" in ev["message"]
    assert "无 order_ref" in ev["message"] and "600000.SH" in ev["message"]
    assert q.empty()  # 只汇总一条，不刷屏


# ===========================================================================
# 批次1 C：风控/信号执行时点 risk_timing（immediate=当日确认立即执行 /
# next_day=当日信号次日执行，维持 B-1 deferred 现行为）
# ===========================================================================
def test_handle_bar_risk_timing_immediate_skips_deferred():
    """risk_timing=immediate：日线信号当日立即提交，跳过 deferred 闸门。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    ctx.risk_timing = "immediate"
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 24, 0, 0)  # 日线约定 00:00
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 24), date(2026, 8, 25)], stock
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1d"

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "submitted"  # 当日已提交
    db.close()
    assert engine._signal_bar_deferred == []  # 不进 deferred 队列
    assert any(r.url.path == "/order" for r in rec.requests)  # 已发桥


def test_handle_bar_post_close_forces_deferred_even_immediate():
    """15:05 收盘落定轮次（post_close=True）：immediate 策略也必须 defer 到次日。

    收盘后报单报不进交易所，只会成为待报/废单，故 post_close 无条件 defer。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    ctx.risk_timing = "immediate"
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 24, 0, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 24), date(2026, 8, 25)], stock
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1d"
    bar.post_close = True

    engine._handle_bar(port, bar)

    assert len(engine._signal_bar_deferred) == 1
    db = factory()
    row = db.query(LiveOrder).first()
    assert row is not None and row.status == "deferred"
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)


def test_handle_bar_risk_timing_next_day_keeps_deferred():
    """risk_timing=next_day（默认）：维持 B-1 现行为——当日 deferred，次日放行。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    ctx.risk_timing = "next_day"
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 24, 0, 0)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(
        factory, port, disp, [date(2026, 8, 24), date(2026, 8, 25)], stock
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    bar = _bar(stock, "9.3", bar_time)
    bar.period = "1d"

    engine._handle_bar(port, bar)

    db = factory()
    orders = db.query(LiveOrder).all()
    # 当日不提交：仅落 status=deferred 行（P2 持久化，重启 recover 重建队列）
    assert len(orders) == 1 and orders[0].status == "deferred"
    db.close()
    assert len(engine._signal_bar_deferred) == 1  # 进 deferred 队列


# ===========================================================================
# 批次1 3a：撤单后有限追价重下（requote_enabled；挂接批次 A 超时撤单收敛点）
# ===========================================================================
_REQUOTE_KEY = (1, "600000.SH", "open_sig", date(2026, 8, 5))


def _requote_engine(disp, factory, enabled=True, max_attempts=2):
    port, ctx = _portfolio_single()
    ctx.requote_enabled = enabled
    ctx.requote_max_attempts = max_attempts
    return _make_engine(disp, factory, port), port, ctx


def _sweep_two_rounds_to_cancel(engine, disp, factory):
    """超时撤单两轮：第 1 轮请求撤单（tagged），第 2 轮委托表无此单 → 收敛 canceled。"""
    db = factory()
    engine._sweep_timeout_orders(db)
    db.close()
    disp.orders = []  # 撤单生效：委托表已查不到该单
    db = factory()
    engine._sweep_timeout_orders(db)
    db.close()


def test_requote_after_timeout_cancel_places_better_buy(monkeypatch):
    """撤单收敛 canceled 且策略开启重下 → BUY 按 +1 tick 追价重下剩余量，attempts 记 1。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine, _, _ = _requote_engine(disp, factory, enabled=True)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo_id = lo.id
    db.commit()
    db.close()

    _sweep_two_rounds_to_cancel(engine, disp, factory)

    db = factory()
    orders = db.query(LiveOrder).order_by(LiveOrder.id).all()
    assert len(orders) == 2
    orig, requote = orders
    assert orig.id == lo_id and orig.status == "canceled"
    assert requote.status == "submitted"
    assert requote.strategy_id == 1 and requote.stock_code == "600000.SH"
    assert requote.trade_type == "buy"
    assert requote.price == Decimal("9.31")  # BUY：原价 9.3 + 1 tick(0.01)
    assert requote.quantity == 600           # 原量 − 已成交(0)
    assert requote.bar_time == datetime(2026, 8, 5, 10, 0)
    db.close()
    assert engine._requote_attempts[_REQUOTE_KEY] == 1
    # decision 事件：gate=requote，中文含追价后价格与第 N 次
    db = factory()
    evs = db.query(LiveDecisionEvent).filter_by(gate="requote").all()
    assert len(evs) == 1
    assert "9.31" in evs[0].message and "第 1" in evs[0].message
    db.close()


def test_requote_disabled_keeps_current_behavior(monkeypatch):
    """策略未开启重下（默认）→ 撤单收敛 canceled 后维持现行为，不重下。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine, _, _ = _requote_engine(disp, factory, enabled=False)
    db = factory()
    _pending_order_with_ref(db)
    db.commit()
    db.close()

    _sweep_two_rounds_to_cancel(engine, disp, factory)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "canceled"  # 终态不变
    db.close()
    assert engine._requote_attempts == {}


def test_requote_skips_when_attempts_exhausted(monkeypatch):
    """已达 requote_max_attempts → 不再重下，终态 canceled。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine, _, ctx = _requote_engine(disp, factory, enabled=True, max_attempts=2)
    engine._requote_attempts[_REQUOTE_KEY] = 2  # 前两次重下已用完额度
    db = factory()
    _pending_order_with_ref(db)
    db.commit()
    db.close()

    _sweep_two_rounds_to_cancel(engine, disp, factory)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "canceled"
    db.close()
    assert engine._requote_attempts[_REQUOTE_KEY] == 2  # 不再累加


def test_requote_quantity_minus_filled_and_sell_price_down(monkeypatch):
    """部分成交 SELL：重下量=原量−已成交量，价格 −1 tick（SELL 追价向下）。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine, port, ctx = _requote_engine(disp, factory, enabled=True)
    _preset_position(ctx, "600000.SH", 600, Decimal("9.3"))  # SELL cap 需虚拟持仓
    db = factory()
    lo = _pending_order_with_ref(db)
    lo.trade_type = "sell"
    lo.status = "partial"
    lo.filled_quantity = 300
    db.commit()
    db.close()

    _sweep_two_rounds_to_cancel(engine, disp, factory)

    db = factory()
    requote = db.query(LiveOrder).order_by(LiveOrder.id).all()[-1]
    assert requote.trade_type == "sell"
    assert requote.quantity == 300           # 原量 600 − 已成交 300
    assert requote.price == Decimal("9.29")  # SELL：原价 9.3 − 1 tick(0.01)
    db.close()
    assert engine._requote_attempts[_REQUOTE_KEY] == 1


def test_requote_skips_when_fully_filled_before_cancel(monkeypatch):
    """撤单前已全部成交（残余收割补齐）→ 剩余量 0，不重下。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    disp = FakeTdxDispatcher(
        orders=[_bridge_row("W001", status=55, traded_volume=600)],
        deals=[{
            "order_ref": "W001", "price": 9.25, "volume": 600,
            "amount": 5550.0, "commission": 1.4,
            "trade_time": "100001", "trade_date": "20260805",
        }],
    )
    engine, _, _ = _requote_engine(disp, factory, enabled=True)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo.status = "partial"
    lo.filled_quantity = 600
    db.commit()
    db.close()

    _sweep_two_rounds_to_cancel(engine, disp, factory)

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1  # 无重下单
    # 第 1 轮撤单成功即收割残余成交 → 终态由回填路径落定为 filled（非 canceled）
    assert orders[0].status == "filled"
    db.close()
    assert engine._requote_attempts == {}


# ===========================================================================
# 撤单收敛统一 requote 挂点：53/54 终态同步路径可达（终审修复）
# 真机事实：TDX/THS 当日委托表保留已撤单（不消失），超时撤单实际全走 5s 轮询
# sync_terminal_order_status 的 53/54 终态路径收敛——requote 必须覆盖该路径。
# ===========================================================================
def test_requote_fires_via_53_54_terminal_sync_path(monkeypatch):
    """带撤单标记的单走 53/54 终态同步收敛（此前 requote 不可达的路径）→ 追价重下。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    # 委托表保留该单且已呈 54 全撤终态（撤单已生效但行不消失）
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001", status=54)])
    engine, _, _ = _requote_engine(disp, factory, enabled=True)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo_id = lo.id
    db.commit()
    db.close()

    # 第 1 轮主循环：请求撤单（打标记）；委托表仍保留该单 → 不走消失路径
    db = factory()
    engine._sweep_timeout_orders(db)
    db.close()
    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "submitted"
    assert (lo2.error_message or "").startswith(_CANCEL_TAG)
    db.close()

    # 第 2 轮 deals 池：5s 终态同步收敛 canceled → 统一 requote 挂点触发
    engine._tick_deals()

    db = factory()
    orders = db.query(LiveOrder).order_by(LiveOrder.id).all()
    assert len(orders) == 2
    orig, requote = orders
    assert orig.id == lo_id and orig.status == "canceled"
    assert requote.status == "submitted"
    assert requote.price == Decimal("9.31")   # BUY：原价 9.3 + 1 tick
    assert requote.quantity == 600
    db.close()
    assert engine._requote_attempts[_REQUOTE_KEY] == 1


def test_requote_not_fired_for_untagged_53_54_cancel(monkeypatch):
    """非标记单（GUI 手撤/收盘自动撤）走 53/54 终态收敛 → 维持现行为，不重下。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001", status=54)])
    engine, _, _ = _requote_engine(disp, factory, enabled=True)
    db = factory()
    _pending_order_with_ref(db)  # 无撤单请求标记
    db.commit()
    db.close()

    engine._tick_deals()

    db = factory()
    orders = db.query(LiveOrder).all()
    assert len(orders) == 1 and orders[0].status == "canceled"
    db.close()
    assert engine._requote_attempts == {}
    assert engine._requote_fired == set()


def test_requote_fires_once_across_both_convergence_paths(monkeypatch):
    """防双触发：同一单两条收敛路径都进统一判定 → 只触发一次（引擎级 set）。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    disp = FakeTdxDispatcher(orders=[])
    engine, _, _ = _requote_engine(disp, factory, enabled=True)
    db = factory()
    lo = _pending_order_with_ref(db, status="canceled")
    lo.error_message = _CANCEL_TAG + "超时未成交已撤单（阈值 3 分钟）"
    lo_id = lo.id
    db.commit()
    db.close()

    # 模拟同一单同时被两条路径（消失路径返回 + 53/54 终态路径收集）递进判定
    engine._requote_after_cancel_converged([lo_id])
    engine._requote_after_cancel_converged([lo_id])

    db = factory()
    orders = db.query(LiveOrder).order_by(LiveOrder.id).all()
    assert len(orders) == 2  # 仅一次重下
    db.close()
    assert engine._requote_attempts[_REQUOTE_KEY] == 1
    assert engine._requote_fired == {_REQUOTE_KEY + (0,), ("order", lo_id)}
    assert len(disp.place_calls) == 1




# ---------------- 外部信号注入（0011 外部信号注入链路） ----------------

class _StubSignalSource:
    """外部信号源替身：read_new_signals 返回预置信号，reset_daily 计数。"""

    def __init__(self, signals=None):
        self._signals = list(signals or [])
        self.reset_calls = 0

    def read_new_signals(self):
        return list(self._signals)

    def reset_daily(self):
        self.reset_calls += 1


def _portfolio_external(formula_name="分时突击", max_positions=5, period="1m"):
    """单组合单策略：ctx.formula_name 指向外部预警公式，公式信号 OPEN(trigger=1)。"""
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period=period,
        capital_ratio=Decimal("0.6"), max_positions=max_positions,
        single_open_ratio=Decimal("0.1"),
    )
    ctx.formula_signals = [
        {"signal_name": "open_sig", "signal_type": SignalType.OPEN, "trigger_value": 1},
    ]
    ctx.formula_name = formula_name
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port, ctx


def _external_engine(factory, port):
    from core.engine.bar_poller import BarPoller
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )


_EXT_SIG = {
    "code": "600000", "name": "浦发银行", "time": datetime(2026, 10, 30, 9, 50),
    "price": Decimal("9.30"), "formula_name": "分时突击",
}


def test_process_external_signal_formula_match_runs_full_pipeline():
    """公式名匹配 → 合成 bar + signal_cache 注入 → _handle_bar 全套（submitted 落库）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    engine = _external_engine(factory, port)
    engine._signal_source = _StubSignalSource()

    engine._process_external_signal(dict(_EXT_SIG))

    db = factory()
    orders = db.query(LiveOrder).all()
    db.close()
    assert len(orders) == 1
    o = orders[0]
    assert o.status == "submitted"
    assert o.stock_code == "600000.SH"  # 6 位码 → 通达信带后缀口径
    assert o.trade_type == "buy"
    assert o.signal_name == "open_sig"
    assert o.bar_time == datetime(2026, 10, 30, 9, 50)  # bar_time 用文件时间（非墙钟）
    # 下单量与 bar 驱动同口径：int(0.1 * 60000 / 9.3 / 100) * 100 = 600
    assert o.quantity == 600
    # signal_cache 注入条目（策略信号名 + 值 1）
    assert engine.signal_cache[(1, "600000.SH", _EXT_SIG["time"])] == [
        {"name": "open_sig", "value": 1},
    ]
    # 处理成功后占本日「买入方向」去重名额（同代码同方向只认第一条）
    assert engine._ext_seen == {("600000.SH", "buy")}


def test_process_external_signal_dedup_by_direction():
    """同代码同方向已消费 → 跳过；卖出方向独立，不受买入已消费影响。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    engine = _external_engine(factory, port)
    engine._signal_source = _StubSignalSource()
    engine._ext_seen.add(("600000.SH", "buy"))

    engine._process_external_signal(dict(_EXT_SIG))

    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()
    # 买入方向被去重后，卖出方向未消费但该策略无卖出信号配置 → 无注入
    assert ("600000.SH", "sell") not in engine._ext_seen


def test_process_external_signal_max_positions_full_blocks(monkeypatch):
    """max_positions 满 → 注入前过滤（不构造合成 bar、不进 _handle_bar）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external(max_positions=1)
    pos = Position("000001.SZ")
    pos.quantity = 100
    pos.platform_quantity = 100  # 平台买入的持仓才占槽位
    pos.avg_cost = Decimal("10")
    ctx.positions["000001.SZ"] = pos
    engine = _external_engine(factory, port)
    engine._signal_source = _StubSignalSource()
    calls = []
    monkeypatch.setattr(engine, "_handle_bar", lambda *a, **k: calls.append(a))

    engine._process_external_signal(dict(_EXT_SIG))

    assert calls == []
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()
    # 满槽被闸门拦截 → 未实际注入，不占本日方向名额
    assert engine._ext_seen == set()


def test_process_external_signal_already_holding_blocks(monkeypatch):
    """该股票已持仓 → 跳过（不重复开仓，加仓走 ADD 信号职责）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    pos = Position("600000.SH")
    pos.quantity = 600
    pos.avg_cost = Decimal("9.0")
    ctx.positions["600000.SH"] = pos
    engine = _external_engine(factory, port)
    engine._signal_source = _StubSignalSource()
    calls = []
    monkeypatch.setattr(engine, "_handle_bar", lambda *a, **k: calls.append(a))

    engine._process_external_signal(dict(_EXT_SIG))

    assert calls == []
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()


# ---- 外部信号买卖双向放行（卖出类不做仓位/资金检查，但不得卖空、不越过 T+1）----

def _portfolio_external_multi(signal_types, formula_name="分时突击",
                              period="1m", max_positions=5,
                              reduce_ratio=Decimal("0.3")):
    """单组合单策略：按 signal_types 配公式信号，ctx.formula_name 指向预警公式。"""
    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period=period,
        capital_ratio=Decimal("0.6"), max_positions=max_positions,
        single_open_ratio=Decimal("0.1"),
        reduce_position_ratio=reduce_ratio,
    )
    ctx.formula_signals = [
        {"signal_name": "sig_" + st.value.lower(), "signal_type": st,
         "trigger_value": 1}
        for st in signal_types
    ]
    ctx.formula_name = formula_name
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"),
        take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    port.strategies.append(ctx)
    return port, ctx


def _external_engine_with(factory, port, available=None):
    """带 /positions 可用量应答的外部信号引擎（available=None → 空仓应答）。"""
    rec = _Recorder()

    def respond(request):
        path = request.url.path
        if path == "/positions":
            data = [] if available is None else [{
                "instrument": "600000", "exchange": "SH",
                "available": available, "volume": 1000,
                "yesterday_volume": available, "on_road_volume": 0,
            }]
            return httpx.Response(200, json={"ok": True, "data": data})
        if path == "/order":
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})

    rec._respond = respond
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine._signal_source = _StubSignalSource()
    return engine, rec


def _hold(ctx, stock="600000.SH", qty=1000, price="9.0"):
    pos = Position(stock)
    pos.buy(qty, Decimal(price), datetime(2026, 10, 29, 9, 30))  # 昨仓，T+1 可卖
    ctx.positions[stock] = pos


def test_external_sell_signal_allowed_with_position_and_t1_available():
    """卖出类放行：有持仓 + 桥可用=1000 → CLOSE 提交 SELL 1000。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external_multi([SignalType.CLOSE])
    _hold(ctx)
    engine, rec = _external_engine_with(factory, port, available=1000)

    engine._process_external_signal(dict(_EXT_SIG))

    db = factory()
    orders = db.query(LiveOrder).all()
    db.close()
    assert len(orders) == 1
    assert orders[0].trade_type == "sell"
    assert orders[0].signal_name == "sig_close"
    assert orders[0].quantity == 1000
    assert ("600000.SH", "sell") in engine._ext_seen


def test_external_sell_signal_blocked_when_t1_available_zero():
    """卖出类不越过 T+1：有持仓但桥可用=0 → 拒单（t1_insufficient），不发 /order。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external_multi([SignalType.CLOSE])
    _hold(ctx)
    engine, rec = _external_engine_with(factory, port, available=0)

    engine._process_external_signal(dict(_EXT_SIG))

    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)


def test_external_sell_signal_rejected_without_position(monkeypatch):
    """无持仓不卖（不得卖空）：不构造合成 bar、不下单，也不占卖出名额。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external_multi([SignalType.CLOSE])
    engine, rec = _external_engine_with(factory, port, available=1000)
    calls = []
    monkeypatch.setattr(engine, "_handle_bar", lambda *a, **k: calls.append(a))

    engine._process_external_signal(dict(_EXT_SIG))

    assert calls == []
    assert ("600000.SH", "sell") not in engine._ext_seen
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)


def test_external_buy_then_sell_same_day_not_swallowed():
    """同代码同日先买后卖：买入占 buy 名额，卖出仍独立放行（不被吞）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external_multi([SignalType.OPEN, SignalType.CLOSE])
    engine, rec = _external_engine_with(factory, port, available=600)

    engine._process_external_signal(dict(_EXT_SIG))  # 09:50 买入
    _hold(ctx, qty=600)  # 模拟买入回填成交
    sell_sig = dict(_EXT_SIG)
    sell_sig["time"] = datetime(2026, 10, 30, 10, 30)
    engine._process_external_signal(sell_sig)  # 10:30 卖出

    db = factory()
    orders = db.query(LiveOrder).order_by(LiveOrder.id).all()
    db.close()
    assert [o.trade_type for o in orders] == ["buy", "sell"]
    assert engine._ext_seen == {("600000.SH", "buy"), ("600000.SH", "sell")}


def test_external_same_direction_duplicates_only_first(monkeypatch):
    """同方向重复推送只认第一条（买入/卖出各自只执行一次）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external_multi([SignalType.OPEN, SignalType.CLOSE])
    engine, rec = _external_engine_with(factory, port, available=600)

    engine._process_external_signal(dict(_EXT_SIG))  # 09:50 买
    engine._process_external_signal(dict(_EXT_SIG))  # 重复买 → 忽略
    _hold(ctx, qty=600)
    sell_sig = dict(_EXT_SIG)
    sell_sig["time"] = datetime(2026, 10, 30, 10, 30)
    engine._process_external_signal(sell_sig)  # 卖
    sell_sig2 = dict(sell_sig)
    sell_sig2["time"] = datetime(2026, 10, 30, 10, 35)
    engine._process_external_signal(sell_sig2)  # 重复卖 → 忽略

    db = factory()
    orders = db.query(LiveOrder).all()
    db.close()
    assert [o.trade_type for o in orders] == ["buy", "sell"]


def test_external_reduce_without_ratio_clears_position():
    """REDUCE 无比例（预警文件无比例列）→ 按清仓执行 + 显式 risk 事件。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external_multi(
        [SignalType.REDUCE], reduce_ratio=Decimal("0.3"))
    _hold(ctx, qty=1000)
    engine, rec = _external_engine_with(factory, port, available=1000)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._process_external_signal(dict(_EXT_SIG))

    db = factory()
    orders = db.query(LiveOrder).all()
    db.close()
    # 不是 30% (300 股)，而是清仓 1000 股
    assert len(orders) == 1 and orders[0].quantity == 1000
    # 注入缓存以 CLOSE 覆盖动作（同一套 _signal_to_order 清仓路径）
    assert engine.signal_cache[(1, "600000.SH", _EXT_SIG["time"])] == [
        {"name": "sig_reduce", "value": 1, "signal_type": "CLOSE"},
    ]
    ev = q.get_nowait()
    assert ev["type"] == "risk"
    assert ev["rule"] == "external_reduce_no_ratio"
    assert "按清仓执行" in ev["message"]


def test_external_unknown_action_ignored_with_throttled_warning(caplog, monkeypatch):
    """不可解析动作（未知 signal_type）→ 丢弃 + 每 50 条 warning 一次，不猜方向。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external_multi([])
    # 直接注入未知动作配置（模拟脏配置/未来新增动作）
    ctx.formula_signals = [
        {"signal_name": "sig_weird", "signal_type": "SIDEWAYS", "trigger_value": 1},
    ]
    engine, rec = _external_engine_with(factory, port, available=1000)
    calls = []
    monkeypatch.setattr(engine, "_handle_bar", lambda *a, **k: calls.append(a))

    with caplog.at_level(logging.WARNING, logger="core.engine.live_engine"):
        for _ in range(60):
            engine._process_external_signal(dict(_EXT_SIG))

    assert calls == []  # 绝不猜方向、不默认买入
    assert engine._ext_seen == set()
    assert engine._ext_unparseable_count == 60
    warns = [r for r in caplog.records if "unparseable action" in r.getMessage()]
    assert len(warns) == 2  # 第 1 条与第 51 条
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()


def test_signal_loop_interval_and_gating():
    """独立轮询闸：alert 模式 / stop 后 / 窗口外不读文件；正常盘中读。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    engine = _external_engine(factory, port)
    engine._signal_source = _StubSignalSource()
    engine._signal_poll_interval = 5.0
    now = datetime(2026, 10, 30, 10, 0)
    assert engine._signal_tick_active(now) is True
    engine.execution_mode = "alert"
    assert engine._signal_tick_active(now) is False
    engine.execution_mode = "live"
    engine._stop_requested = True
    assert engine._signal_tick_active(now) is False
    engine._stop_requested = False
    # 行情轮询窗口外（午休 12:00）不读，与主循环 poll 同口径
    assert engine._signal_tick_active(datetime(2026, 10, 30, 12, 0)) is False


def test_signal_poll_interval_clamped_to_floor():
    """轮询间隔下限 1s：防误配负数导致空转占 CPU（0 视为未配置 → 默认 5s）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=BarPoller(disp, ["600000.SH"], period="1m", count=10),
        db_session_factory=factory, signal_poll_interval=-2,
    )
    assert engine._signal_poll_interval == 1.0


def test_process_external_signal_formula_mismatch_drops(monkeypatch):
    """公式名无匹配策略 → 信号丢弃（不占去重名额，换匹配公式后可再触发）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external(formula_name="别的公式")
    engine = _external_engine(factory, port)
    engine._signal_source = _StubSignalSource()
    calls = []
    monkeypatch.setattr(engine, "_handle_bar", lambda *a, **k: calls.append(a))

    engine._process_external_signal(dict(_EXT_SIG))

    assert calls == []
    assert engine._ext_seen == set()
    db = factory()
    assert db.query(LiveOrder).count() == 0
    db.close()


def test_tick_external_signals_dispatches_and_noops_without_source(monkeypatch):
    """_tick_external_signals：源非空 → 逐条注入；源 None → 直接返回（未启用）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    engine = _external_engine(factory, port)
    engine._signal_source = _StubSignalSource([dict(_EXT_SIG)])
    processed = []
    monkeypatch.setattr(engine, "_process_external_signal", lambda s: processed.append(s))

    engine._tick_external_signals()
    assert len(processed) == 1

    engine._signal_source = None
    engine._tick_external_signals()
    assert len(processed) == 1


def test_tick_external_signals_resets_source_on_day_change(monkeypatch):
    """跨日检测（audit Fix1）：now_shanghai 日期 != _ext_sig_last_date → reset_daily()。

    方向去重集合是「本日已消费 (代码, 方向)」——长跑会话跨日不重置则同代码次日
    买卖两方向都被误判已消费。日期变更轮：先重置源（游标归位当前时间）+ 清空
    方向去重集合并更新记账日期，再读信号注入。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    engine = _external_engine(factory, port)
    stub = _StubSignalSource([dict(_EXT_SIG)])
    engine._signal_source = stub
    engine._ext_sig_last_date = date(2026, 10, 30)
    engine._ext_seen.add(("600000.SH", "buy"))
    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai",
        lambda: datetime(2026, 10, 31, 9, 31))
    processed = []
    monkeypatch.setattr(engine, "_process_external_signal", lambda s: processed.append(s))

    engine._tick_external_signals()

    assert stub.reset_calls == 1
    assert engine._ext_seen == set()  # 跨日清空方向去重
    assert engine._ext_sig_last_date == date(2026, 10, 31)
    assert len(processed) == 1


def test_tick_external_signals_same_day_keeps_dedup_state(monkeypatch):
    """同日 tick（audit Fix1）：不调 reset_daily，当日方向去重状态保持。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    engine = _external_engine(factory, port)
    stub = _StubSignalSource([dict(_EXT_SIG)])
    engine._signal_source = stub
    engine._ext_sig_last_date = date(2026, 10, 31)
    engine._ext_seen.add(("600000.SH", "buy"))
    monkeypatch.setattr(
        "core.engine.live_engine.now_shanghai",
        lambda: datetime(2026, 10, 31, 10, 0))
    processed = []
    monkeypatch.setattr(engine, "_process_external_signal", lambda s: processed.append(s))

    engine._tick_external_signals()

    assert stub.reset_calls == 0
    assert engine._ext_seen == {("600000.SH", "buy")}
    assert engine._ext_sig_last_date == date(2026, 10, 31)
    assert len(processed) == 1


def test_engine_init_wires_signal_source_from_path():
    """signal_file_path 非空 → 建源（公式名集合取自 formula_by_strategy 值）；缺省 None。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    from core.engine.bar_poller import BarPoller
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    e1 = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        formula_by_strategy={1: "分时突击"},
        signal_file_path="D:/ths/alert.txt",
    )
    assert e1._signal_source is not None
    assert e1._signal_source._formula_names == {"分时突击"}
    e2 = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    assert e2._signal_source is None


class _StubTqFormulaRecalc:
    """TQ 重算替身（audit Fix2）：5m 重算有值 open_sig=0（非注入值 1）。"""

    @staticmethod
    def compute_injected_batch(**kw):
        return {
            "ErrorId": "0",
            "600000.SH": {"open_sig": [{"Date": "202610300950", "Value": 0.0}]},
        }


def test_external_signal_injection_survives_tq_recalc(monkeypatch):
    """注入条目不被 fill_signal_cache 的 TQ 重算覆盖（audit Fix2，非 1m 周期场景）。

    _process_external_signal 先写注入条目（open_sig=1）再经 _handle_bar →
    fill Phase C 的 TQ 重算返回 open_sig=0——修复前无条件覆盖同 key
    (strategy_id, code, bar_time)，OPEN(trigger=1) 不命中，注入信号被吞；
    修复后已有条目优先，注入信号走完整下单链。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_external(period="5m")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        tq_formula=_StubTqFormulaRecalc(),
        formula_by_strategy={1: "分时突击"},
    )
    engine._signal_source = _StubSignalSource()
    monkeypatch.setattr(
        engine.market_data, "get_bars_with_increment",
        lambda code, period, count: _quote_bars(
            code, [datetime(2026, 10, 30, 9, 30 + i) for i in range(10)]),
    )

    engine._process_external_signal(dict(_EXT_SIG))

    # 注入条目原样保留（未被 TQ 重算的 open_sig=0 覆盖）
    assert engine.signal_cache[(1, "600000.SH", _EXT_SIG["time"])] == [
        {"name": "open_sig", "value": 1},
    ]
    db = factory()
    orders = db.query(LiveOrder).all()
    db.close()
    assert len(orders) == 1
    assert orders[0].status == "submitted"
    assert orders[0].signal_name == "open_sig"


def test_external_mode_skips_tq_formula_fill(monkeypatch):
    """信号来源互斥：配置外部信号文件后 _handle_bar 不再跑 TQ 公式计算。

    否则 TQ 路径会另按自己的 bar_time 出信号，与预警文件路径形成双触发。
    未配置（留空）时仍照常填充（正反对照）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    bar = BarEvent(
        stocks={"600000.SH": {
            "open": Decimal("9.30"), "high": Decimal("9.30"),
            "low": Decimal("9.30"), "close": Decimal("9.30"), "volume": 0,
        }},
        bar_time=datetime(2026, 10, 30, 9, 51), period="1m",
    )

    on_engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        formula_by_strategy={1: "分时突击"},
        signal_file_path="D:/ths/alert.txt",
    )
    calls = []
    monkeypatch.setattr(on_engine, "_fill_signal_cache", lambda *a, **k: calls.append(a))
    on_engine._handle_bar(port, bar)
    assert calls == []

    off_engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        formula_by_strategy={1: "分时突击"},
    )
    calls_off = []
    monkeypatch.setattr(off_engine, "_fill_signal_cache", lambda *a, **k: calls_off.append(a))
    off_engine._handle_bar(port, bar)
    assert len(calls_off) == 1


# ---------------- 批次1：先卖后买 + 槽位/排重闸（_handle_bar 提交链）----------------

_BT = datetime(2026, 8, 5, 10, 0)


def _gate_engine(factory, port):
    """单组合单策略引擎（直调 _handle_bar/_submit_one_order 用）。"""
    from core.engine.bar_poller import BarPoller
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    return engine, rec


def _order(stock, trade_type, signal_type=None, signal_name="open_sig",
           bar_time=None):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=trade_type,
        signal_type=signal_type or SignalType.OPEN,
        quantity=100, price=Decimal("9.3"),
        bar_time=bar_time or _BT, signal_name=signal_name,
    )


def _seed_buy_trade(db, stock, trade_time, qty=600, price="9.0", status="accepted"):
    """预置一笔 BUY 成交（LiveOrder 终态 + LiveTrade），供排重/豁免用例。"""
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1, stock_code=stock,
        trade_type="buy", order_type="limit", price=Decimal(price), quantity=qty,
        filled_quantity=qty, filled_price=Decimal(price), status=status,
        signal_name="open_sig", signal_type="OPEN", bar_time=trade_time,
    )
    db.add(lo)
    db.flush()
    db.add(LiveTrade(
        live_session_id=1, live_order_id=lo.id, portfolio_strategy_id=1,
        strategy_id=1, stock_code=stock, trade_type="buy",
        price=Decimal(price), quantity=qty, amount=Decimal(price) * qty,
        commission=Decimal("0"), stamp_duty=Decimal("0"), trade_time=trade_time,
    ))
    db.flush()
    # 提交：外部信号链路引擎自建 session（StaticPool 同连接），未提交的跨
    # session 行不可见——预置成交必须 commit，引擎闸门查询才能命中。
    db.commit()
    return lo


def test_handle_bar_submits_sell_before_buy(monkeypatch):
    """混合 SELL+BUY：SELL 全部先于 BUY 提交（风控卖出优先释放现金/槽位）。

    SELL 提交后不等待成交（回填由 _tick_deals 5s 轮询处理），BUY 随即提交——
    BUY 审批用虚拟现金，SELL 未回填前现金未释放，BUY 可能因资金不足被拒/缩量，
    属保守正确（宁可 BUY 失败，不可超卖）。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    engine, _rec = _gate_engine(factory, port)
    submitted = []
    monkeypatch.setattr(
        engine, "_submit_one_order",
        lambda db, p, o, period, emit_signal=True, **kw: submitted.append(
            o.trade_type.value))
    sell = _order("000001.SZ", TradeType.SELL, SignalType.STOP_LOSS, "stop_loss")
    buy = _order("600000.SH", TradeType.BUY, SignalType.OPEN, "open_sig")
    monkeypatch.setattr(port, "on_bar", lambda *a, **k: [buy, sell])

    engine._handle_bar(port, _bar("600000.SH", "9.3", _BT))

    assert submitted == ["SELL", "BUY"]


def test_submit_one_order_blocks_buy_when_positions_full():
    """槽位闸：已持仓数 + 在途 BUY 单数 >= max_positions → 拦截 + position_full 事件。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()  # max_positions=5
    for code in ("600001.SH", "600002.SH", "600003.SH", "600004.SH"):
        _preset_position(ctx, code, 100, Decimal("9.0"))
    db = factory()
    _seed_buy_submitted(db, _BT, status="submitted", stock_code="600005.SH")
    engine, rec = _gate_engine(factory, port)

    engine._submit_one_order(db, port, _order("600006.SH", TradeType.BUY), "1m")
    engine._drain_decisions(db, port)

    # 槽满拦截：新 BUY 不落库（仅预置在途单）、不发桥
    assert db.query(LiveOrder).count() == 1
    assert not any(r.url.path == "/order" for r in rec.requests)
    rows = _decision_rows(factory)
    assert [(r.gate, r.action) for r in rows] == [("position_full", "block")]
    db.close()


def test_submit_one_order_allows_buy_when_slot_free():
    """槽位闸有位：已持仓 + 在途 BUY < max_positions → BUY 正常落库发桥。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    for code in ("600001.SH", "600002.SH", "600003.SH"):
        _preset_position(ctx, code, 100, Decimal("9.0"))
    db = factory()
    _seed_buy_submitted(db, _BT, status="submitted", stock_code="600004.SH")
    engine, rec = _gate_engine(factory, port)

    engine._submit_one_order(db, port, _order("600005.SH", TradeType.BUY), "1m")

    # 3 持仓 + 1 在途 = 4 < 5 → 新 BUY 提交（预置 + 新单共 2 笔）
    assert db.query(LiveOrder).count() == 2
    assert any(r.url.path == "/order" for r in rec.requests)
    db.close()


# ---- 槽位闸（方案 A）：只数平台自己买入成交过的持仓 + max_positions 取策略自身参数 ----
def _platform_pos(stock, qty=100):
    """平台买入成交过的持仓（apply_trade = 成交回填 / recover 重放的同一入口）。"""
    pos = Position(stock)
    pos.apply_trade(TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=TradeType.BUY, price=Decimal("9.0"), quantity=qty,
        amount=Decimal("9.0") * qty, commission=Decimal("0"),
        stamp_duty=Decimal("0"), trade_time=_BT, signal_type=SignalType.OPEN,
    ))
    return pos


def _external_pos(stock, qty=100):
    """外部/手工导入仓（桥快照镜面导入）：platform_quantity 恒 0 → 不占槽位。"""
    pos = Position(stock)
    pos.mirror_bridge(qty, qty, Decimal("9.0"), _BT.date())
    return pos


def test_slot_gate_external_positions_do_not_occupy_slots():
    """外部/手工导入仓不占名额：5 只外部仓 + max_positions=5 仍可开新仓。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()  # max_positions=5
    for i in range(5):
        ctx.positions["5100%02d.SH" % i] = _external_pos("5100%02d.SH" % i)
    db = factory()
    engine, rec = _gate_engine(factory, port)

    engine._submit_one_order(db, port, _order("600006.SH", TradeType.BUY), "1m")

    assert engine._platform_held_count(ctx) == 0
    assert db.query(LiveOrder).count() == 1  # 新 BUY 落库
    assert any(r.url.path == "/order" for r in rec.requests)
    db.close()


def test_slot_gate_platform_positions_occupy_slots():
    """平台买入的持仓占名额：5 只平台仓 + max_positions=5 → 拦截 + position_full。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    for i in range(5):
        ctx.positions["60001%d.SH" % i] = _platform_pos("60001%d.SH" % i)
    db = factory()
    engine, rec = _gate_engine(factory, port)

    engine._submit_one_order(db, port, _order("600020.SH", TradeType.BUY), "1m")
    engine._drain_decisions(db, port)

    assert engine._platform_held_count(ctx) == 5
    assert db.query(LiveOrder).count() == 0
    assert not any(r.url.path == "/order" for r in rec.requests)
    rows = _decision_rows(factory)
    assert [(r.gate, r.action) for r in rows] == [("position_full", "block")]
    db.close()


def test_slot_gate_reoccupies_after_external_sell_then_platform_buy():
    """外部仓卖出后平台再买入 → 重新占位（mirror 全平清零、成交回填重新累加）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    pos = _external_pos("510050.SH")
    ctx.positions["510050.SH"] = pos
    engine, _rec = _gate_engine(factory, port)

    assert engine._platform_held_count(ctx) == 0  # 手工仓不占位
    pos.mirror_bridge(0, 0, None, _BT.date())      # 外部仓被卖出（柜台无持仓）
    assert engine._platform_held_count(ctx) == 0
    pos.apply_trade(TradeEvent(                     # 平台随后买入
        strategy_id=1, portfolio_id=1, stock_code="510050.SH",
        trade_type=TradeType.BUY, price=Decimal("3.0"), quantity=100,
        amount=Decimal("300"), commission=Decimal("0"), stamp_duty=Decimal("0"),
        trade_time=_BT, signal_type=SignalType.OPEN,
    ))
    assert engine._platform_held_count(ctx) == 1  # 重新占位


def test_slot_gate_uses_each_strategy_own_max_positions():
    """两个组合策略各自的 max_positions 分别生效（策略 A=1 满、策略 B=3 有位）。"""
    from core.engine.bar_poller import BarPoller

    factory, _ = _db_factory()
    port1, ctx1 = _portfolio_single()
    ctx1.max_positions = 1
    ctx1.positions["600001.SH"] = _platform_pos("600001.SH")

    pm = PortfolioRiskManager(max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port2 = Portfolio(portfolio_id=2, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx2 = StrategyContext(
        strategy_id=2, period="1m",
        capital_ratio=Decimal("0.6"), max_positions=3,
        single_open_ratio=Decimal("0.1"),
    )
    ctx2.formula_signals = []
    ctx2.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.05"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"),
    )
    ctx2.positions["600002.SH"] = _platform_pos("600002.SH")
    port2.strategies.append(ctx2)

    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    poller = BarPoller(disp, ["600000.SH"], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port1, port2], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    assert engine._slot_limit(port1, ctx1) == 1
    assert engine._slot_limit(port2, ctx2) == 3

    def _buy(pid, sid, code):
        return OrderEvent(
            strategy_id=sid, portfolio_id=pid, stock_code=code,
            trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
            quantity=100, price=Decimal("9.3"),
            bar_time=_BT, signal_name="open_sig",
        )

    db = factory()
    engine._submit_one_order(db, port1, _buy(1, 1, "600010.SH"), "1m")  # 1 >= 1 → 拦
    engine._submit_one_order(db, port2, _buy(2, 2, "600011.SH"), "1m")  # 1 < 3 → 放行

    rows = db.query(LiveOrder).all()
    assert [(r.portfolio_strategy_id, r.stock_code) for r in rows] == [(2, "600011.SH")]
    db.close()


def test_slot_gate_skips_when_max_positions_missing(caplog):
    """max_positions 缺失（None）→ 跳过槽位闸（不限）+ WARNING，不回落默认 5。"""
    import logging

    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    ctx.max_positions = None
    for i in range(5):
        ctx.positions["60003%d.SH" % i] = _platform_pos("60003%d.SH" % i)
    db = factory()
    engine, rec = _gate_engine(factory, port)

    with caplog.at_level(logging.WARNING, logger="core.engine.live_engine"):
        engine._submit_one_order(db, port, _order("600040.SH", TradeType.BUY), "1m")

    assert engine._slot_limit(port, ctx) is None
    assert db.query(LiveOrder).count() == 1  # 不设上限 → 放行
    assert any(r.url.path == "/order" for r in rec.requests)
    assert "max_positions 缺失" in caplog.text
    db.close()


def test_submit_one_order_blocks_buy_already_traded_today():
    """排重闸：该策略该股票今日已有 BUY 成交 → 重复信号拦截 + duplicate_signal 事件。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    db = factory()
    _seed_buy_trade(db, "600000.SH", _BT)  # 当日（2026-8-5）已有 BUY 成交
    engine, rec = _gate_engine(factory, port)

    engine._submit_one_order(db, port, _order("600000.SH", TradeType.BUY), "1m")
    engine._drain_decisions(db, port)

    # 拦截：不落新单（库内仅预置成交的 LiveOrder）、不发桥
    assert db.query(LiveOrder).count() == 1
    assert not any(r.url.path == "/order" for r in rec.requests)
    rows = _decision_rows(factory)
    assert [(r.gate, r.action) for r in rows] == [("duplicate_signal", "block")]
    db.close()


def test_submit_one_order_allows_buy_traded_only_yesterday():
    """排重口径：昨日成交不算今日重复 → BUY 正常提交。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    db = factory()
    _seed_buy_trade(db, "600000.SH", _BT)  # 2026-8-5 成交
    engine, rec = _gate_engine(factory, port)

    engine._submit_one_order(
        db, port, _order("600000.SH", TradeType.BUY,
                         bar_time=datetime(2026, 8, 6, 10, 0)), "1m")

    # 昨日成交不算今日重复：新 BUY 落库（预置成交单 + 新单共 2 笔）
    assert db.query(LiveOrder).count() == 2
    assert any(r.url.path == "/order" for r in rec.requests)
    db.close()


def test_submit_one_order_sell_not_gated_by_slots_or_dedup():
    """SELL 不受槽位/排重闸（风控卖出必须放行）：槽满 + 当日已有成交仍提交。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    for i in range(5):
        _preset_position(ctx, "60000%d.SH" % i, 100, Decimal("9.0"))
    db = factory()
    _seed_buy_trade(db, "600000.SH", _BT)  # 当日已有 BUY 成交
    engine, rec = _gate_engine(factory, port)

    order = _order("600000.SH", TradeType.SELL, SignalType.STOP_LOSS, "stop_loss")
    engine._submit_one_order(db, port, order, "1m")

    # 预置成交单 + SELL 新单共 2 笔
    assert db.query(LiveOrder).count() == 2
    assert any(r.url.path == "/order" for r in rec.requests)
    db.close()


def test_submit_one_order_split_children_exempt_from_gates():
    """拆单子单豁免（_split_child）：同一逻辑单的等量切片，槽位/排重按父单口径。

    max_positions=1 已满 + 子单1当日已成交 → 非子单 BUY 必被拦；子单豁免正常提交。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_external(max_positions=1)
    _preset_position(ctx, "600000.SH", 600, Decimal("9.0"))  # 子单1成交已 apply
    db = factory()
    _seed_buy_trade(db, "600000.SH", datetime(2026, 8, 5, 10, 1))
    engine, rec = _gate_engine(factory, port)

    child = _order("600000.SH", TradeType.BUY,
                   bar_time=datetime(2026, 8, 5, 10, 2))
    child._split_child = True
    engine._submit_one_order(db, port, child, "1m")

    # 预置成交单 + 子单共 2 笔
    assert db.query(LiveOrder).count() == 2
    assert any(r.url.path == "/order" for r in rec.requests)
    db.close()


def test_external_signal_inherits_gates_duplicate_trade():
    """外部信号走 _handle_bar 同链：同股今日已有 BUY 成交 → duplicate_signal 拦截。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_external()
    db = factory()
    _seed_buy_trade(db, "600000.SH", _EXT_SIG["time"])  # 信号当日已有 BUY 成交
    engine = _external_engine(factory, port)
    engine._signal_source = _StubSignalSource()

    engine._process_external_signal(dict(_EXT_SIG))
    engine._drain_decisions(db, port)

    # 拦截：仅预置成交单，无新单
    assert db.query(LiveOrder).count() == 1
    rows = _decision_rows(factory)
    assert any(r.gate == "duplicate_signal" for r in rows)
    db.close()


def test_buy_same_order_continuation_bypasses_bought_today():
    """P1#12（2026-11-16）：同一张逻辑单的续做（追价/ deferred 重试）绕开 bought_today。

    部分成交（300/800）后追价重下剩余量、或拒单/越价撤单后回 deferred 重试，带的
    都是原单剩余量，不是新信号——若仍走信号级排重，剩余量会静默丢失。新信号仍
    保持严格（同日同策略同代码只买一次）。

    每个续做标记独立夹具（真实链路里续做发生在原单已收敛之后，同库残留的
    在途单会被 inflight 闸拦下，那是另一条正确的闸，与本用例无关）。
    """
    def _run(mark):
        factory, _ = _db_factory()
        port, _ctx = _portfolio_single()
        db = factory()
        _seed_buy_trade(db, "600000.SH", _BT, qty=300)  # 当日已部分成交 300 股
        engine, rec = _gate_engine(factory, port)

        # ① 新信号：仍被 duplicate_signal 拦下（不落新单）
        engine._submit_one_order(db, port, _order("600000.SH", TradeType.BUY), "1m")
        engine._drain_decisions(db, port)
        assert db.query(LiveOrder).count() == 1
        rows = _decision_rows(factory)
        assert any(r.gate == "duplicate_signal" for r in rows)

        # ② 续做（剩余量）：绕过 bought_today → 落库 + 发桥
        cont = _order("600000.SH", TradeType.BUY,
                      bar_time=datetime(2026, 8, 5, 9, 31))
        mark(cont)
        engine._submit_one_order(db, port, cont, "1m")
        assert db.query(LiveOrder).count() == 2
        assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
        db.close()

    _run(lambda o: setattr(o, "_requote", True))
    _run(lambda o: setattr(o, "_requeued_for_retry", True))
    _run(lambda o: setattr(o, "_deferred_retry_count", 1))


def test_bought_today_ignores_future_dated_trade_rows():
    """P1#10（2026-11-16）：排重查询补上界——异常的未来时间成交行不再永久阻断 BUY。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    db = factory()
    # 脏数据：成交时间在未来（次日 10:00）
    _seed_buy_trade(db, "600000.SH", datetime(2026, 8, 7, 10, 0))
    engine, rec = _gate_engine(factory, port)

    engine._submit_one_order(db, port, _order("600000.SH", TradeType.BUY), "1m")

    # 未来行不在 [当日 0 点, 次日 0 点) 内 → 正常提交
    assert db.query(LiveOrder).count() == 2
    assert any(r.url.path == "/order" for r in rec.requests)
    db.close()


def test_risk_event_persist_flag_writes_decision_row():
    """P1#3（2026-11-16）：risk 事件 persist=True 时同部落 live_decision_events。

    cash_diff/equity_deviation/valuation_missing_price 等原只推 SSE，刷新页面即
    消失；落库后工作台可查。persist=False 保持原行为（只推 SSE）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    engine, rec = _gate_engine(factory, port)

    engine._emit_risk_safe(
        {"gate": "cash_diff", "rule": "cash_diff", "action": "notice",
         "message": "柜台与虚拟现金不一致"})
    assert _decision_rows(factory) == []

    engine._emit_risk_safe(
        {"gate": "cash_diff", "rule": "cash_diff", "action": "notice",
         "message": "柜台与虚拟现金不一致"}, persist=True)
    rows = _decision_rows(factory)
    assert [(r.gate, r.layer, r.action) for r in rows] == [("cash_diff", "risk", "notice")]
    assert rows[0].live_session_id == 1


def test_available_map_refresh_dedupes_by_bar_time_not_object_identity():
    """P1#4（2026-11-16）：可用持仓刷新按 bar_time 判重，不按 bar 对象身份。

    1m 节拍 bar 与周期边界 bar 可能同 bar_time 但对象不同；按身份判重会各刷一次，
    把前一批 consume_available 的递减冲掉（同码多策略跨周期时可重复卖超可用量）。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    engine, _ = _gate_engine(factory, port)
    calls = []
    engine._refresh_available_map = lambda: calls.append(1)
    engine.signal_cache = {}

    engine._handle_bar(port, _bar("600000.SH", "9.0", _BT))
    engine._handle_bar(port, _bar("600000.SH", "9.0", _BT))  # 同 bar_time 新对象
    assert len(calls) == 1

    engine._handle_bar(port, _bar("600000.SH", "9.0",
                                  _BT + timedelta(minutes=1)))
    assert len(calls) == 2


def test_buy_cash_cap_computed_inside_book_lock():
    """P1#5（2026-11-16）：BUY 资金上限计算与 pending 注册同处 _book_lock 临界区。

    旧实现 cap 在锁外算、注册在锁内做，deals 池 requote 与主池新单可并发读到同一
    笔未预留现金 → 双份占用。这里断言 _buy_available_cash 被调用时已持有记账锁。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    engine, rec = _gate_engine(factory, port)
    owned = []
    orig = engine._buy_available_cash

    def _spy(portfolio):
        owned.append(engine._book_lock._is_owned())
        return orig(portfolio)

    engine._buy_available_cash = _spy
    db = factory()
    engine._submit_one_order(db, port, _order("600000.SH", TradeType.BUY), "1m")

    assert owned == [True]
    assert len(engine._pending_orders) == 1
    db.close()


def test_valuation_gap_dedupe_keyed_by_portfolio(monkeypatch):
    """P1#7（2026-11-16）：缺价事件去重键带 portfolio_id，多组合同码都可见。"""
    from types import SimpleNamespace as _NS

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    engine, _ = _gate_engine(factory, port)
    seen = []
    monkeypatch.setattr(
        engine, "_emit_risk_safe",
        lambda payload, persist=False: seen.append(payload))

    def _port(pid):
        return _NS(portfolio_id=pid, unpriced_codes=lambda: {"600000.SH"})

    bar = _NS(bar_time=_BT)
    engine._note_valuation_gaps(_port(1), bar)
    engine._note_valuation_gaps(_port(2), bar)
    engine._note_valuation_gaps(_port(1), bar)  # 组合 1 同 code 当日只报一次

    assert [(p["portfolio_id"], p["stock_code"]) for p in seen] == [
        (1, "600000.SH"), (2, "600000.SH")]


def test_is_buy_trade_type_normalizes_case_and_enum():
    """P1#8（2026-11-16）：trade_type 判定统一归一化（大小写/枚举都认）。"""
    from core.engine.live_engine import _is_buy_trade_type

    assert _is_buy_trade_type("buy")
    assert _is_buy_trade_type("BUY")
    assert _is_buy_trade_type(" Buy ")
    assert _is_buy_trade_type(TradeType.BUY)
    assert not _is_buy_trade_type("sell")
    assert not _is_buy_trade_type(None)


# ===========================================================================
# 方案 A：掘金/HTTP 桥「超长兜底窗口」收口 + 迟到回填
# （goldminer.untraceable_grace_seconds，默认 600s；TDX/THS supports_remark=False 不变）
# ===========================================================================
def _drain_events(q):
    out = []
    while not q.empty():
        out.append(q.get_nowait())
    return out


def test_expire_untraceable_expired_keeps_pending_for_manual_review():
    """超过 600s + /orders 与 /deals 均空 → 保持 submitted + 人工核对标记 + 一次性 risk。

    2026-11-11 起不再判 rejected：柜台当日委托查询不含废单、桥重启丢内存台账，
    「查不到」不等于「不存在」；判 rejection 会释放同向门 → 旧单若仍挂着就会再报
    一张（双份成交）。改为保留在途，由 _submit_one_order 的同向门按标记禁止重报。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    lo.created_at = (
        datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=15))
    db.commit()
    lo_id = lo.id
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "submitted"  # 保留在途，不判拒
    assert (lo2.error_message or "").startswith("bridge_order_manual_review:")
    assert "10 分钟" in lo2.error_message
    assert db.query(LiveTrade).count() == 0  # 绝不伪造成交
    db.close()
    pos = ctx.positions.get("600000.SH")
    assert (pos.quantity if pos else 0) == 0  # 不动虚拟持仓
    assert port.account.cash == Decimal("100000")  # 不动虚拟现金
    assert engine.pending_orders_count == 1  # 保留在途（供迟到回填/人工核对）

    events = _drain_events(q)
    assert not any(e.get("type") == "order" for e in events)
    ev = next(e for e in events if e.get("type") == "risk")
    assert ev["rule"] == "bridge_order_manual_review"
    assert ev["session_id"] == 1
    assert ev["order_id"] == lo_id
    assert ev["bridge_order_id"] == _TEST_OID
    assert ev["broker_order_id"] is None
    assert ev["stock_code"] == "600000.SH"
    assert ev["trade_type"] == "BUY"
    assert ev["quantity"] == 600
    assert ev["submitted_at"]
    assert ev["grace_seconds"] == 600
    assert ev["message"]

    # 幂等：下一轮仍无法追溯，不得重复发 risk 事件（每 5s 一轮不能刷屏）
    engine._poll_deals()
    assert not [e for e in _drain_events(q) if e.get("type") == "risk"]


def test_expire_untraceable_expired_hits_order_table_backfills_not_closed():
    """超过 600s 但 /orders 精确查询命中成交 → 正常回填 filled，不进入收口分支。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[{
        "source": "BRIDGE", "instrument": "600000", "exchange": "SH",
        "direction": 48, "volume": 600, "order_ref": "ref-hit",
        "remark": _TEST_REMARK,
    }], deals=[{
        "order_ref": "ref-hit", "remark": _TEST_REMARK,
        "price": 9.25, "volume": 600, "amount": 5550.0, "commission": 1.39,
        "trade_time": "150001", "trade_date": "20260805",
    }])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    lo.created_at = (
        datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=15))
    db.commit()
    lo_id = lo.id
    db.close()

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "filled"
    assert lo2.order_ref == "ref-hit"
    assert lo2.filled_quantity == 600
    assert not (lo2.error_message or "").startswith("bridge_order_untraceable")
    assert db.query(LiveTrade).count() == 1
    db.close()
    assert ctx.positions[stock].quantity == 600
    assert port.account.cash == Decimal("100000") - (Decimal("5550.0") + Decimal("1.39"))

    events = _drain_events(q)
    assert not any(e.get("rule") == "bridge_order_untraceable_expired" for e in events)


def test_untraceable_expired_blocks_new_same_direction_order():
    """grace 后保留在途：同股同向的新单必须被「人工核对闸」拦住（不再放行）。

    旧行为是收口 rejected 释放门后立刻重报；若旧单其实仍在柜台，就会双份成交。
    现在改为拦截 + 显式 gate 事件，直到人工核对或柜台回读终态。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])
    disp, _ = _make_dispatcher(rec)
    db = factory()
    old = _submitted_order(db, quantity=600)  # BUY，bar 10:00
    old.created_at = (
        datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=15))
    db.commit()
    old_id = old.id
    db.close()

    engine = _make_engine(disp, factory, port)

    # 超长兜底不再收口为 rejected，而是打人工核对标记留在途
    db = factory()
    engine._expire_stale_orders(db)
    db.commit()
    db.close()
    db = factory()
    assert db.get(LiveOrder, old_id).status == "submitted"
    assert (db.get(LiveOrder, old_id).error_message or "").startswith(
        "bridge_order_manual_review:")
    db.close()

    # 下一根 bar 的同向 OPEN 信号必须被拦，不得产生新单、不得打到桥 /order
    next_bar_time = datetime(2026, 8, 5, 10, 5)
    engine.signal_cache = {
        (1, stock, next_bar_time): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _bar(stock, "9.3", next_bar_time))

    db = factory()
    assert db.query(LiveOrder).count() == 1  # 只有旧的、需人工核对的那张
    db.close()
    assert not any(r.url.path == "/order" for r in rec.requests)


def test_late_fill_after_untraceable_corrects_and_notifies():
    """已收口 rejected 的单后续 /deals 出现成交 → 迟到回填纠正 + 记账 + risk 事件。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    _mock_orders_deals(rec, orders=[], deals=[])
    disp, _ = _make_dispatcher(rec)
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    lo.created_at = (
        datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=15))
    db.commit()
    lo_id = lo.id
    db.close()

    # 模拟旧版收口结果（2026-11-11 起 expire 不再自动判拒，但历史数据/其它通道仍
    # 可能留下 rejected 行）——本条验证「迟到成交纠正」路径对这类行仍有效。
    db = factory()
    closed = db.get(LiveOrder, lo_id)
    closed.status = "rejected"
    closed.error_message = (
        "bridge_order_untraceable_expired: 超过 10 分钟无法在柜台查询到该委托")
    db.commit()
    db.close()

    # 柜台迟到回显该 remark 的成交
    _mock_orders_deals(rec, orders=[], deals=[{
        "order_ref": "ref-late-fill", "remark": _TEST_REMARK,
        "price": 9.25, "volume": 600, "amount": 5550.0, "commission": 1.39,
        "trade_time": "150001", "trade_date": "20260805",
    }])
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._poll_deals()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "filled"                       # 已拒单被纠正为成交
    assert lo2.filled_quantity == 600
    assert lo2.filled_price == Decimal("9.25")
    assert (lo2.error_message or "").startswith("late_fill_after_untraceable:")
    assert db.query(LiveTrade).count() == 1             # 迟到成交正常落库
    db.close()
    assert ctx.positions[stock].quantity == 600         # 虚拟持仓同步
    assert port.account.cash == Decimal("100000") - (Decimal("5550.0") + Decimal("1.39"))
    assert engine.pending_orders_count == 0             # 补齐后离开在途集合

    events = _drain_events(q)
    ev = next(e for e in events if e.get("type") == "risk")
    assert ev["rule"] == "late_fill_after_untraceable"
    assert ev["order_id"] == lo_id
    assert ev["bridge_order_id"] == _TEST_OID
    assert ev["stock_code"] == "600000.SH"
    assert ev["quantity"] == 600
    assert "迟到成交" in ev["message"]


def test_tdx_untraceable_still_rejects_without_bridge_untraceable_events():
    """回归：TDX/THS（supports_remark=False）超窗仍直接 rejected，不产生 bridge_* 事件。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[], deals=[])
    engine = _make_engine(disp, factory, port)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    db = factory()
    lo = _submitted_order(db, quantity=600)
    lo.created_at = (
        datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=15))
    db.commit()
    lo_id = lo.id
    db.close()

    db = factory()
    engine._expire_stale_orders(db)
    db.commit()
    db.close()

    db = factory()
    lo2 = db.get(LiveOrder, lo_id)
    assert lo2.status == "rejected"  # 原行为不变：超窗直接拒
    assert "order match timeout" in (lo2.error_message or "")
    assert not (lo2.error_message or "").startswith("bridge_order_untraceable")
    db.close()

    events = _drain_events(q)
    assert not any(
        str(e.get("rule") or "").startswith("bridge_order_untraceable")
        for e in events
    )


# ===========================================================================
# 行情轮询窗口：收盘后停拉行情（config.live.quote_polling）
# ===========================================================================
def _window_engine(monkeypatch, fixed, trading_dates, codes=("600000.SH",),
                   quote_polling=None, code_period_count=None, respond=None):
    """带假日历 + 指定轮询窗口的 LiveEngine（行情轮询窗口用例专用）。"""
    from core.engine.bar_poller import BarPoller
    from core.engine.trading_calendar import TradingCalendar

    monkeypatch.setattr("core.engine.live_engine.now_shanghai", lambda: fixed)
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    poller = BarPoller(disp, list(codes), period="1m", count=10)
    cal = TradingCalendar(lambda: list(trading_dates))
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        trading_calendar=cal,
        code_period_count=code_period_count,
        quote_polling=quote_polling,
    )
    return engine, poller, rec


def test_quote_polling_paused_after_close(monkeypatch, caplog):
    """交易日 15:06 → 不发起 /quote（停行情轮询）；进入暂停只打一条 INFO。"""
    import logging

    fixed = datetime(2026, 8, 5, 15, 6)  # 周三交易日，收盘后
    engine, poller, rec = _window_engine(monkeypatch, fixed, [date(2026, 8, 5)])
    # 隔离变量：日终/清扫/对账标记置为该日已完成，只剩"行情轮询"一个差异
    engine._last_daily_date = fixed.date()
    engine._last_daily_bar_date = fixed.date()
    engine._last_close_sweep_date = fixed.date()
    engine._last_reconcile_date = fixed.date()

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine._tick_main()
        engine._tick_main()  # 第二轮：状态未跳变，不重复打日志

    assert _count_quote(rec) == []
    assert poller.last_completed_stime is None  # poll 从未执行
    paused_logs = [r for r in caplog.records if "行情轮询已暂停" in r.getMessage()]
    assert len(paused_logs) == 1  # 只在进入暂停时打一条，不按 tick 刷屏


def test_quote_polling_active_in_session(monkeypatch):
    """交易日 09:31 → 正常发起 1m /quote（盘中行为不变）。"""
    fixed = datetime(2026, 8, 5, 9, 31)
    engine, poller, rec = _window_engine(monkeypatch, fixed, [date(2026, 8, 5)])

    engine._tick_main()

    quotes = _count_quote(rec)
    assert len(quotes) == 1
    assert "period=1m" in str(quotes[0].url)
    assert "count=10" in str(quotes[0].url)


def test_quote_polling_skipped_during_lunch_break(monkeypatch, caplog):
    """午休 12:00：交易所无新 bar → 整段不轮询（每分钟 40 只 ETF 空转被堵掉）。"""
    import logging

    fixed = datetime(2026, 8, 5, 12, 0)  # 周三交易日，午休中
    engine, poller, rec = _window_engine(monkeypatch, fixed, [date(2026, 8, 5)])
    # 隔离变量：日终/清扫/对账标记置为该日已完成，只剩"行情轮询"一个差异
    engine._last_daily_date = fixed.date()
    engine._last_daily_bar_date = fixed.date()
    engine._last_close_sweep_date = fixed.date()
    engine._last_reconcile_date = fixed.date()

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine._tick_main()
        engine._tick_main()  # 第二轮：状态未跳变，不重复打日志

    assert _count_quote(rec) == []
    assert poller.last_completed_stime is None  # poll 从未执行
    paused_logs = [r for r in caplog.records if "行情轮询已暂停" in r.getMessage()]
    assert len(paused_logs) == 1  # 只在进入暂停时打一条，不按 tick 刷屏
    assert "午休" in paused_logs[0].getMessage()


def test_quote_polling_keeps_1130_bar_and_resumes_at_1300(monkeypatch, caplog):
    """11:30 收线那根不丢：午休不轮询，13:00 恢复首轮一次取回并派发。"""
    import asyncio
    import logging

    stock = "600000.SH"
    cur = {"now": datetime(2026, 8, 5, 11, 29, 30)}
    rows = {"bars": [datetime(2026, 8, 5, 11, 28), datetime(2026, 8, 5, 11, 29)]}

    def respond(request):
        path = request.url.path
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            code = request.url.params.get("code")
            return httpx.Response(200, json={"ok": True,
                                             "data": {code: _quote_bars(code, rows["bars"])}})
        return httpx.Response(404, json={"ok": False})

    engine, poller, rec = _window_engine(
        monkeypatch, cur["now"], [date(2026, 8, 5)],
        quote_polling={"enabled": True, "resume_time": "09:15", "pause_time": "15:05"},
        respond=respond,
    )
    # 时钟可推进（同一引擎跑三段时间）
    monkeypatch.setattr("core.engine.live_engine.now_shanghai", lambda: cur["now"])
    # 恢复预热单独打桩：本用例只验证 BarPoller 的 1m 轮询节拍
    monkeypatch.setattr(engine.market_data, "preheat", lambda only_missing=False: None)
    engine._last_daily_date = datetime(2026, 8, 5).date()
    engine._last_daily_bar_date = datetime(2026, 8, 5).date()
    engine._last_close_sweep_date = datetime(2026, 8, 5).date()
    engine._last_reconcile_date = datetime(2026, 8, 5).date()

    triggered = []
    orig_on_bar = poller.on_bar
    poller.on_bar = lambda bar: (triggered.append(bar.bar_time), orig_on_bar(bar))
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        # 11:29:30 —— 上午最后一轮：首轮只建基线（水位 11:28），11:29 仍在形成
        engine._tick_main()
        assert triggered == []
        quotes_after_am = len(_count_quote(rec))
        assert quotes_after_am == 1
        assert engine._quote_poll_paused is False

        # 11:31 —— 进入午休：不拉行情（11:30 那根不会丢，13:00 首轮取回）
        cur["now"] = datetime(2026, 8, 5, 11, 31)
        engine._tick_main()
        assert len(_count_quote(rec)) == quotes_after_am
        assert engine._quote_poll_paused is True
        assert "午休" in (engine._quote_poll_pause_reason or "")
        assert poller.last_completed_stime == datetime(2026, 8, 5, 11, 28)

        # 12:30 —— 仍在午休：继续停拉
        cur["now"] = datetime(2026, 8, 5, 12, 30)
        engine._tick_main()
        assert len(_count_quote(rec)) == quotes_after_am
        assert poller.last_completed_stime == datetime(2026, 8, 5, 11, 28)

        # 13:00:30 —— 恢复轮询：11:30 收线那根随首轮窗口取回并派发
        rows["bars"] = [datetime(2026, 8, 5, 11, 29), datetime(2026, 8, 5, 11, 30),
                        datetime(2026, 8, 5, 13, 0)]
        cur["now"] = datetime(2026, 8, 5, 13, 0, 30)
        engine._tick_main()

    assert len(_count_quote(rec)) > quotes_after_am
    assert engine._quote_poll_paused is False
    assert datetime(2026, 8, 5, 11, 30) in triggered
    assert poller.last_completed_stime == datetime(2026, 8, 5, 11, 30)
    assert any("行情轮询已恢复" in r.getMessage() for r in caplog.records)
    resumed = [e for e in _drain_events(q) if e.get("rule") == "quote_polling_resumed"]
    assert len(resumed) == 1
    assert "午休结束" in resumed[0]["message"]


def test_quote_polling_skipped_on_non_trading_day(monkeypatch, caplog):
    """非交易日（周六）：即使时刻落在 09:15–15:05 窗口内也不轮询。"""
    import logging

    fixed = datetime(2026, 8, 8, 10, 30)  # 周六
    engine, poller, rec = _window_engine(monkeypatch, fixed, [date(2026, 8, 5)])
    engine._last_daily_date = fixed.date()
    engine._last_daily_bar_date = fixed.date()
    engine._last_close_sweep_date = fixed.date()
    engine._last_reconcile_date = fixed.date()

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine._tick_main()

    assert _count_quote(rec) == []
    assert poller.last_completed_stime is None
    paused_logs = [r for r in caplog.records if "行情轮询已暂停" in r.getMessage()]
    assert paused_logs and "非交易日" in paused_logs[0].getMessage()


def test_quote_polling_resume_preheats_without_history_backfill(monkeypatch, caplog):
    """次日 09:16 恢复：触发预热（只补缺失缓存），且不把隔夜 bar 当盘中触发。"""
    import logging

    stock, new_code = "600000.SH", "000001.SZ"
    yday = [datetime(2026, 8, 4, 14, 58), datetime(2026, 8, 4, 14, 59),
            datetime(2026, 8, 4, 15, 0)]

    def respond(request):
        path = request.url.path
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            code = request.url.params.get("code")
            return httpx.Response(200, json={"ok": True,
                                             "data": {code: _quote_bars(code, yday)}})
        return httpx.Response(404, json={"ok": False})

    fixed = datetime(2026, 8, 5, 9, 16)
    engine, poller, rec = _window_engine(
        monkeypatch, fixed, [date(2026, 8, 4), date(2026, 8, 5)],
        quote_polling={"enabled": True, "resume_time": "09:15", "pause_time": "15:05"},
        code_period_count={(stock, "1m"): 200, (new_code, "1m"): 200},
        respond=respond,
    )
    # 昨日收盘前已建立水位；暂停期对账又扩池了一只新代码（无水位）
    poller._initialized = True
    poller._last_completed = {stock: yday[-1]}
    poller.add_codes([new_code])
    # 预热缓存完好：暂停期无 bar，历史可续接（全量重拉会是数百次 /quote 突发）
    for code in (stock, new_code):
        engine._preheat_cache[(code, "1m")] = engine.market_data._make_cache_entry(
            _quote_bars(code, yday), 200)

    preheat_calls = []
    orig_preheat = engine.market_data.preheat

    def fake_preheat(only_missing=False):
        preheat_calls.append(only_missing)
        return orig_preheat(only_missing=only_missing)

    monkeypatch.setattr(engine.market_data, "preheat", fake_preheat)

    triggered = []
    poller.on_bar = triggered.append
    engine._quote_poll_paused = True  # 上一交易日 15:05 已进入暂停

    with caplog.at_level(logging.INFO, logger="core.engine.live_engine"):
        engine._tick_main()

    assert preheat_calls == [True]        # 恢复触发一次预热，且只补缺失
    quotes = _count_quote(rec)
    assert len(quotes) == 2               # 每 code 一次 poll 拉取
    assert all("count=10" in str(q.url) for q in quotes)  # 无 count=200 历史重拉
    assert triggered == []                # 隔夜 bar 未被当盘中触发
    assert poller._last_completed[stock] == yday[-1]      # 已有水位不变
    assert poller._last_completed[new_code] == yday[-2]   # 新代码只建基线
    assert poller._resume_guard is False
    assert any("行情轮询已恢复" in r.getMessage() for r in caplog.records)


def test_quote_polling_paused_emits_risk_once(monkeypatch):
    """进入暂停 → 发一条 risk/quote_polling_paused；后续 tick 不重复发。"""
    import asyncio

    fixed = datetime(2026, 8, 5, 15, 6)  # 交易日收盘后
    engine, poller, rec = _window_engine(
        monkeypatch, fixed, [date(2026, 8, 5), date(2026, 8, 6)])
    engine._last_daily_date = fixed.date()
    engine._last_daily_bar_date = fixed.date()
    engine._last_close_sweep_date = fixed.date()
    engine._last_reconcile_date = fixed.date()
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._tick_main()
    engine._tick_main()  # 第二轮：状态未跳变 → 不再发事件

    risks = [e for e in _drain_events(q) if e.get("type") == "risk"]
    paused = [e for e in risks if e.get("rule") == "quote_polling_paused"]
    assert len(paused) == 1
    ev = paused[0]
    assert ev["session_id"] == 1
    assert ev["engine"] == engine._engine_key
    assert ev["next_resume_at"] == "2026-08-06 09:15"
    assert "行情轮询已暂停" in ev["message"]
    assert "下次恢复 2026-08-06 09:15" in ev["message"]


def test_quote_polling_resumed_emits_risk_once(monkeypatch):
    """恢复行情轮询 → 发一条 risk/quote_polling_resumed；后续 tick 不重复发。"""
    import asyncio

    fixed = datetime(2026, 8, 5, 9, 16)  # 交易日，进入 09:15 窗口
    engine, poller, rec = _window_engine(
        monkeypatch, fixed, [date(2026, 8, 4), date(2026, 8, 5)],
        quote_polling={"enabled": True, "resume_time": "09:15", "pause_time": "15:05"})
    # 预热走网络，本用例只验证事件：替换为空实现
    monkeypatch.setattr(engine, "_preheat", lambda **kw: None)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)
    engine._quote_poll_paused = True  # 上一交易日 15:05 已进入暂停

    engine._tick_main()
    engine._tick_main()  # 第二轮：已恢复，不再发事件

    risks = [e for e in _drain_events(q) if e.get("type") == "risk"]
    resumed = [e for e in risks if e.get("rule") == "quote_polling_resumed"]
    assert len(resumed) == 1
    ev = resumed[0]
    assert ev["session_id"] == 1
    assert ev["engine"] == engine._engine_key
    assert ev["resumed_at"] == "2026-08-05 09:16:00"
    assert "行情轮询已恢复" in ev["message"]
    assert "预热" in ev["message"]


def test_close_sweep_and_daily_closer_still_run_when_polling_paused(monkeypatch):
    """回归：15:06 停行情轮询后，日终/日线/收盘清扫入口仍执行（未被一并掐掉）。"""
    fixed = datetime(2026, 8, 5, 15, 6)

    def respond(request):
        path = request.url.path
        if path == "/ping":
            return httpx.Response(200, json={"ok": True})
        if path == "/quote":
            code = request.url.params.get("code")
            return httpx.Response(200, json={
                "ok": True,
                "data": {code: _quote_bars(code, [datetime(2026, 8, 5, 15, 0)])},
            })
        return httpx.Response(404, json={"ok": False})

    engine, poller, rec = _window_engine(
        monkeypatch, fixed, [date(2026, 8, 5)], respond=respond)
    engine._last_reconcile_date = fixed.date()  # 隔离收盘对账复查
    called = {}

    def spy_daily_close():
        called["daily_close"] = True
        engine._closer.maybe_daily_close(fixed)

    def spy_daily_bars():
        called["daily_bars"] = True
        engine._closer.maybe_daily_bars(fixed)

    def spy_close_sweep():
        called["close_sweep"] = True
        engine._closer.maybe_close_sweep(fixed)

    monkeypatch.setattr(engine, "_maybe_daily_close", spy_daily_close)
    monkeypatch.setattr(engine, "_maybe_daily_bars", spy_daily_bars)
    monkeypatch.setattr(engine, "_maybe_close_sweep", spy_close_sweep)

    engine._tick_main()

    assert called == {"daily_close": True, "daily_bars": True, "close_sweep": True}
    assert engine._last_close_sweep_date == fixed.date()
    # 收盘结算的日线快照照拉（1d /quote 不受窗口限制），盘中 1m 轮询已停
    assert any("period=1d" in str(r.url) for r in rec.requests)
    assert [r for r in rec.requests if "period=1m" in str(r.url)] == []


# ===========================================================================
# 2026-11-10 盘中实证收口：在途集合 detached+expired 打断整批提交
# ===========================================================================
def test_pending_orders_readable_after_session_close():
    """在途集合必须是「已加载的 detached 对象」：commit 后不摘出 session 会
    expired+detached，下一根 bar 读属性抛 DetachedInstanceError（2026-11-10 实证：
    `_reserved_buy_cash` 读 trade_type 抛错 → 整批 deferred 只放出 1 笔）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    disp, _ = _make_dispatcher(_Recorder())
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.3", bar_time))
    assert len(engine._pending_orders) == 1

    # _handle_bar 内的 session 已 close：读在途单属性不得抛 DetachedInstanceError
    lo = next(iter(engine._pending_orders.values()))
    assert lo.trade_type == "buy"
    assert lo.status == "submitted"
    assert lo.stock_code == stock
    assert engine._reserved_buy_cash(1) > Decimal("0")


def test_handle_bar_batch_isolates_single_submit_failure(monkeypatch, caplog):
    """单笔提交异常只影响该笔：同批其它单照常提交（2026-11-10 实证整批中断，
    300286/300415 两笔 deferred 完全未提交）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    s1, s2 = "600000.SH", "600001.SH"
    bt = datetime(2026, 8, 5, 10, 0)
    disp, _ = _make_dispatcher(_Recorder())
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [s1, s2], period="1m", count=10)
    engine = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    engine.signal_cache = {
        (1, s1, bt): [{"name": "open_sig", "value": 1}],
        (1, s2, bt): [{"name": "open_sig", "value": 1}],
    }
    calls = []
    real = engine._submit_one_order

    def flaky_submit(db, portfolio, order, period, **kwargs):
        calls.append(order.stock_code)
        if len(calls) == 1:
            raise RuntimeError("boom")
        return real(db, portfolio, order, period, **kwargs)

    monkeypatch.setattr(engine, "_submit_one_order", flaky_submit)

    bar = BarEvent(stocks={
        code: {"open": Decimal("9.0"), "high": Decimal("9.3"),
               "low": Decimal("9.0"), "close": Decimal("9.3"), "volume": 10000}
        for code in (s1, s2)
    }, bar_time=bt)
    with caplog.at_level(logging.ERROR):
        engine._handle_bar(port, bar)

    assert len(calls) == 2                      # 第二笔没有被第一笔异常拦住
    db = factory()
    rows = db.query(LiveOrder).all()
    db.close()
    assert [r.stock_code for r in rows] == [calls[1]]
    assert any("batch continues" in r.message for r in caplog.records)


# ===========================================================================
# 次日盘前（集合竞价）执行日线信号单（config.tq_trade.pre_open_auction*）
# 默认关闭 → 行为不变；开启 → 交易日 09:15–end 把当日 due 的 deferred 日线单
# 直接报进集合竞价，at-most-once（盘前已报出的单 09:31 不再重报）。
# ===========================================================================


def _pre_open_engine(factory, port, disp, trading_dates, stock, **cfg):
    """带盘前竞价开关的引擎（默认开启，可按需覆盖 end_time/bias）。"""
    from core.engine.trading_calendar import TradingCalendar
    from core.engine.bar_poller import BarPoller
    poller = BarPoller(disp, [stock], period="1m", count=10)
    cal = TradingCalendar(lambda: list(trading_dates))
    conf = {"pre_open_auction_enabled": True}
    conf.update(cfg)
    return LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
        trading_calendar=cal, pre_open_auction=conf,
    )


def _seed_sellable(ctx, stock, qty=300, price="9.0"):
    ctx.positions[stock] = Position(stock)
    ctx.positions[stock].buy(qty, Decimal(price), datetime(2026, 8, 24, 9, 30))


def test_pre_open_window_boundaries():
    """窗口 09:15 ≤ now < 09:20（左闭右开），非法结束时间回退默认。"""
    from core.engine.live_engine import PreOpenAuctionConfig
    conf = PreOpenAuctionConfig(enabled=True)
    assert conf.is_open(datetime(2026, 8, 25, 9, 15))
    assert conf.is_open(datetime(2026, 8, 25, 9, 19, 59))
    assert not conf.is_open(datetime(2026, 8, 25, 9, 14, 59))
    assert not conf.is_open(datetime(2026, 8, 25, 9, 20))
    # 默认关闭
    assert not PreOpenAuctionConfig().is_open(datetime(2026, 8, 25, 9, 16))
    # 非法值回退默认 09:20；负数让价归零
    bad = PreOpenAuctionConfig(enabled=True, end_time="25:99", price_bias_pct=-3)
    assert bad.end_time == (9, 20)
    assert bad.price_bias_pct == Decimal("0")


def test_pre_open_config_defaults_enabled():
    """代码默认开启：from_config 缺键/None → enabled=True；显式 False 仍可关。"""
    from core.config import _defaults
    from core.engine.live_engine import PreOpenAuctionConfig
    assert PreOpenAuctionConfig.from_config({}).enabled is True
    assert PreOpenAuctionConfig.from_config(None).enabled is True
    assert PreOpenAuctionConfig.from_config(
        {"pre_open_auction_enabled": False}).enabled is False
    assert _defaults()["tq_trade"]["pre_open_auction_enabled"] is True


def test_engine_mismatched_risk_flag_passed_to_portfolio(monkeypatch):
    """会话三态解析结果传给 Portfolio.on_bar：默认 False，显式开启才 True。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio_single(period="1d")
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [date(2026, 8, 25)], "600000.SH")
    captured = {}

    def fake_on_bar(bar, **kwargs):
        captured["mismatched"] = kwargs.get("mismatched_risk")
        return []

    monkeypatch.setattr(port, "on_bar", fake_on_bar)
    engine._handle_bar(port, _bar("600000.SH", "9.3",
                                  datetime(2026, 8, 25, 10, 0)))
    assert captured["mismatched"] is False
    engine._intraday_mismatched_risk = True
    engine._handle_bar(port, _bar("600000.SH", "9.3",
                                  datetime(2026, 8, 25, 10, 1)))
    assert captured["mismatched"] is True


def test_release_price_uses_first_1m_bar_open(monkeypatch):
    """09:31 兜底：限价 = 当日首根 1m bar 的 open，夹涨跌停并 quantize。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio_single(period="1d")
    rec = _Recorder(respond=_bridge_ping(("/ping", "/order", "/price_limit")))
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp,
                              [date(2026, 8, 25)], "600000.SH")
    monkeypatch.setattr("core.engine.live_engine.now_shanghai",
                        lambda: datetime(2026, 8, 25, 9, 31))
    monkeypatch.setattr(disp, "query_quote", lambda code, **kw: [
        {"stime": "20260825093000", "open": 10.05, "close": 10.10},
        {"stime": "20260825093100", "open": 10.10, "close": 10.20},
    ])
    order = _deferred_order("600000.SH", date(2026, 8, 24))
    source = engine._refresh_release_price(order)
    assert source == "first_bar_open"
    assert order.price == Decimal("10.05")
    assert order._price_type_override == "limit"


def test_release_price_falls_back_to_close_with_event(monkeypatch):
    """取不到当日首根 1m open → 退回实时 close 并记 fallback 事件。"""
    factory, _ = _db_factory()
    port, _ctx = _portfolio_single(period="1d")
    rec = _Recorder(respond=_bridge_ping(("/ping", "/order", "/price_limit")))
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp,
                              [date(2026, 8, 25)], "600000.SH")
    monkeypatch.setattr("core.engine.live_engine.now_shanghai",
                        lambda: datetime(2026, 8, 25, 9, 31))
    monkeypatch.setattr(disp, "query_quote", lambda code, **kw: [
        {"stime": "20260824093000", "open": 9.0, "close": 9.1},
    ])
    order = _deferred_order("600000.SH", date(2026, 8, 24))
    source = engine._refresh_release_price(order)
    assert source.startswith("quote_close_fallback")
    # 退回实时价（非信号价 9.5），来源可审计
    assert order.price != Decimal("9.5")


def test_pre_open_cancels_when_open_crosses_limit():
    """开盘价越过盘前限价（卖限价 10.5 > 开盘价 10.0）→ 主动撤单。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_bridge_ping(
        ("/ping", "/order", "/price_limit", "/cancel")))
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)
    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    # 模拟 /orders 轮询已回填柜台委托号（撤单必须按 order_ref 撤）
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    assert row is not None
    row.order_ref = "TEST-REF-1"
    # 模拟盘前已放行：deferred 行转 submitted（真实链路由 _pre_open_placed 推进）
    row.status = "submitted"
    db.commit()
    assert row.status == "submitted", row.status
    db.close()

    engine._pre_open_open_checks.append({
        "day": d2, "code": stock, "side": TradeType.SELL,
        "limit": Decimal("10.5"), "strategy_id": 1,
    })
    bar = _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31))
    bar.period = "1m"
    bar.stocks[stock]["open"] = Decimal("10.0")
    engine._check_pre_open_orders_at_open(bar)

    assert engine._pre_open_open_checks == []
    assert any(r.url.path == "/cancel" for r in rec.requests)


def test_pre_open_no_cancel_when_open_not_crossed():
    """开盘价未越过限价（卖限价 10.5 ≤ 开盘价 10.6）→ 不撤单，只清登记。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_bridge_ping(
        ("/ping", "/order", "/price_limit", "/cancel")))
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)

    engine._pre_open_open_checks.append({
        "day": d2, "code": stock, "side": TradeType.SELL,
        "limit": Decimal("10.5"), "strategy_id": 1,
    })
    bar = _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31))
    bar.period = "1m"
    bar.stocks[stock]["open"] = Decimal("10.6")
    engine._check_pre_open_orders_at_open(bar)

    assert engine._pre_open_open_checks == []
    assert not any(r.url.path == "/cancel" for r in rec.requests)


def test_pre_open_open_check_retries_when_bar_missing_open():
    """P2-4：首根 bar 缺 open（停牌/合成器丢采样）不得写死每日标记，下一根 bar 重试。

    旧实现先 `_pre_open_open_check_day = today` 再比对 → 首根 bar 缺价即当天永久漏撤。
    """
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_bridge_ping(
        ("/ping", "/order", "/price_limit", "/cancel")))
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)
    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    row.order_ref = "TEST-REF-RETRY"
    row.status = "submitted"
    db.commit()
    db.close()

    engine._pre_open_open_checks.append({
        "day": d2, "code": stock, "side": TradeType.SELL,
        "limit": Decimal("10.5"), "strategy_id": 1,
    })
    # 首根 bar：open=0（不可信，停牌/缺价）
    bar = _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31))
    bar.period = "1m"
    bar.stocks[stock]["open"] = Decimal("0")
    engine._check_pre_open_orders_at_open(bar)

    assert len(engine._pre_open_open_checks) == 1          # 保留登记
    assert engine._pre_open_open_check_day is None         # 未置「当日完成」
    assert not any(r.url.path == "/cancel" for r in rec.requests)

    # 下一根 bar：可信开盘价 10.0 < 卖限价 10.5 → 越价，撤单并解决
    bar2 = _bar(stock, "9.3", datetime(2026, 8, 25, 9, 32))
    bar2.period = "1m"
    bar2.stocks[stock]["open"] = Decimal("10.0")
    engine._check_pre_open_orders_at_open(bar2)

    assert engine._pre_open_open_checks == []
    assert engine._pre_open_open_check_day == d2
    assert any(r.url.path == "/cancel" for r in rec.requests)


def test_pre_open_open_check_stops_after_deadline_and_emits_event(monkeypatch):
    """P2-4：到 10:00 仍缺可信开盘价 → 停止重试、清登记并记 risk 事件（不静默）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_bridge_ping(("/ping", "/order", "/price_limit")))
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)
    events = []
    monkeypatch.setattr(engine, "_emit", lambda kind, payload: events.append((kind, payload)))
    engine._pre_open_open_checks.append({
        "day": d2, "code": stock, "side": TradeType.SELL,
        "limit": Decimal("10.5"), "strategy_id": 1,
    })
    bar = _bar(stock, "9.3", datetime(2026, 8, 25, 10, 5))
    bar.period = "1m"
    bar.stocks[stock]["open"] = Decimal("0")
    engine._check_pre_open_orders_at_open(bar)

    assert engine._pre_open_open_checks == []
    assert engine._pre_open_open_check_day == d2
    assert any(
        kind == "risk" and payload.get("rule") == "open_check_timeout"
        and payload.get("action") == "skip"
        and stock in (payload.get("codes") or [])
        for kind, payload in events
    ), events


def test_pre_open_disabled_keeps_old_behavior():
    """开关关闭：deferred 队列/DB 行/发桥全部不动，只由 09:31 首根 bar 放行。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [d1, d2], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()
    assert len(engine._signal_bar_deferred) == 1

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    assert len(engine._signal_bar_deferred) == 1          # 队列不动
    assert not any(r.url.path == "/order" for r in rec.requests)
    db = factory()
    assert db.query(LiveOrder).filter_by(status="deferred").count() == 1
    db.close()


def test_pre_open_releases_only_due_daily_orders_at_window():
    """只命中「当日 due」的 deferred 日线单；未到 due 的留在队列。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    due_stock, later_stock = "600000.SH", "600001.SH"
    d1, d2, d3 = date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26)
    _seed_sellable(ctx, due_stock)
    _seed_sellable(ctx, later_stock)
    rec = _Recorder(respond=_bridge_ping(("/ping", "/order", "/price_limit")))
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(
        factory, port, disp, [d1, d2, d3], due_stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(due_stock, d1), "1d")
    engine._submit_one_order(db, port, _deferred_order(later_stock, d2), "1d")
    db.close()
    assert len(engine._signal_bar_deferred) == 2

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    assert len(engine._signal_bar_deferred) == 1          # 只有 due 单被摘走
    assert engine._signal_bar_deferred[0].stock_code == later_stock
    orders = [r for r in rec.requests if r.url.path == "/order"]
    assert len(orders) == 1                              # 只报出一笔
    db = factory()
    submitted = db.query(LiveOrder).filter_by(status="submitted").all()
    deferred = db.query(LiveOrder).filter_by(status="deferred").all()
    db.close()
    assert [r.stock_code for r in submitted] == [due_stock]
    assert [r.stock_code for r in deferred] == [later_stock]


def test_pre_open_idempotent_single_submission_across_ticks():
    """窗口内多轮 tick 只报一次（同一逻辑单不重复报出）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_bridge_ping(("/ping", "/order", "/price_limit")))
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()

    for minute in (16, 17, 18, 19):
        engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, minute))

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    assert engine._signal_bar_deferred == []
    # 09:31 首根 bar 也不会重报
    engine._handle_bar(port, _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31)))
    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1


def test_pre_open_blocked_order_falls_back_to_first_bar_path():
    """盘前被闸门拦截（无持仓审批不过）→ 保持 deferred，09:31 首根 bar 兜底报出。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()

    # 无虚拟持仓 → SELL 审批不过（cap_quantity=None），盘前不报出
    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))
    assert not any(r.url.path == "/order" for r in rec.requests)
    assert len(engine._signal_bar_deferred) == 1
    db = factory()
    assert db.query(LiveOrder).filter_by(status="deferred").count() == 1
    db.close()

    # 09:31 首根完成 1m bar：同一笔单走常规放行路径报出（只报一次）
    _seed_sellable(ctx, stock)
    engine._handle_bar(port, _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31)))
    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    assert engine._signal_bar_deferred == []
    db = factory()
    assert db.query(LiveOrder).filter_by(status="submitted").count() == 1
    db.close()


def test_pre_open_price_bias_and_limit_clamp():
    """委托价 = 信号价 ± bias%，并夹进当日涨跌停（各收 1 跳）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _pre_open_engine(
        factory, port, disp, [date(2026, 8, 25)], "600000.SH",
        pre_open_auction_price_bias_pct=2,
    )
    # 昨收 9.00 / 涨停 9.05 / 跌停 8.90
    engine._price_limit_bounds = lambda code: (9.0, 9.05, 8.90)
    buy = OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000.SH",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("9.0"), bar_time=datetime(2026, 8, 24),
        signal_name="open_sig", signal_bar_date=date(2026, 8, 24),
    )
    # 9.00 × 1.02 = 9.18 → 被涨停−1 跳夹到 9.04
    assert engine._pre_open_price(buy) == Decimal("9.04")

    sell = OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="000001.SZ",
        trade_type=TradeType.SELL, signal_type=SignalType.CLOSE,
        quantity=100, price=Decimal("9.0"), bar_time=datetime(2026, 8, 24),
        signal_name="stop_loss", signal_bar_date=date(2026, 8, 24),
    )
    # 9.00 × 0.98 = 8.82 → 被跌停+1 跳夹到 8.91
    assert engine._pre_open_price(sell) == Decimal("8.91")
    # 无涨跌停数据（fail-open）→ 只按让价
    engine._price_limit_bounds = lambda code: None
    assert engine._pre_open_price(sell) == Decimal("8.82")


def test_pre_open_skips_non_trading_day_and_weekly_orders():
    """非交易日不执行；1w/1mon 周期单与日线同口径在盘前放行（2026-11-11 放开）。"""
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    saturday = date(2026, 8, 29)   # 周六：周末硬规则非交易日（不依赖日历覆盖）

    # ① 非交易日（周六）不执行
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    _seed_sellable(ctx, stock)
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1], stock)
    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()
    engine._maybe_pre_open_auction(datetime.combine(saturday, datetime.min.time())
                                   .replace(hour=9, minute=16))
    assert not any(r.url.path == "/order" for r in rec.requests)
    assert len(engine._signal_bar_deferred) == 1

    # ② 1w / 1mon 周期单同样在盘前放行（bias 让价 + 涨跌停夹板 + at-most-once）
    for period in ("1w", "1mon"):
        factory2, _ = _db_factory()
        port2, ctx2 = _portfolio_single(period=period)
        _seed_sellable(ctx2, stock)
        rec2 = _Recorder(respond=_bridge_ping(("/ping", "/order", "/price_limit")))
        disp2, _ = _make_dispatcher(rec2)
        engine2 = _pre_open_engine(factory2, port2, disp2, [d1, d2], stock)
        db = factory2()
        engine2._submit_one_order(db, port2, _deferred_order(stock, d1), period)
        db.close()
        engine2._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))
        assert len([r for r in rec2.requests if r.url.path == "/order"]) == 1
        assert engine2._signal_bar_deferred == []


def test_pre_open_bridge_rejection_requeues_deferred_then_first_bar_retries():
    """桥明确拒单：重落 deferred 行（原 signal bar_time）+ 回队，09:31 只报一次。"""
    state = {"reject": True}

    def respond(request):
        path = request.url.path
        if path == "/order":
            if state["reject"]:
                return httpx.Response(
                    200, json={"ok": False, "error": "price out of limit"})
            return httpx.Response(200, json={"ok": True})
        if path == "/ping":
            return httpx.Response(200, json={
                "ok": True, "endpoints": ["/ping", "/order", "/price_limit"]})
        return httpx.Response(404, json={"ok": False})

    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)
    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    # 拒绝行留痕 + 重落 deferred 行；回队且恢复原 signal bar_time（recover 仍 due 今日）
    db = factory()
    deferred_rows = db.query(LiveOrder).filter_by(status="deferred").all()
    db.close()
    # P1-3：拒单不再终态，改回 deferred 并带重排次数标记（审计在 decision 事件）
    assert len(deferred_rows) == 1
    assert "deferred_retry:n=1" in (deferred_rows[0].error_message or "")
    assert deferred_rows[0].bar_time.date() == d1
    assert len(engine._signal_bar_deferred) == 1
    assert engine._signal_bar_deferred[0].bar_time.date() == d1

    # 09:31 首根完成 1m bar：常规路径重报（唯一一笔成功单）
    state["reject"] = False
    engine._handle_bar(port, _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31)))
    assert len([r for r in rec.requests if r.url.path == "/order"]) == 2  # 1 拒 + 1 成
    db = factory()
    assert db.query(LiveOrder).filter_by(status="submitted").count() == 1
    db.close()


def test_pre_open_emits_audit_decision_events():
    """放行有可观测事件（工作台可见）：gate=pre_open_auction / action=release。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    disp, _ = _make_dispatcher(
        _Recorder(respond=_bridge_ping(("/ping", "/order", "/price_limit"))))
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)
    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    db = factory()
    evs = db.query(LiveDecisionEvent).filter_by(gate="pre_open_auction").all()
    db.close()
    assert any(e.action == "release" and stock in (e.stock_code or "") for e in evs)


# ===========================================================================
# 价格归属处置：QMT 盘前「桥能力闸」（指定价可确认才放行）/ 通达信认领价差转 risk
# / 盘前价格模式声明
# ===========================================================================

def _pre_open_bridge(rec, price_type=None):
    """真实 HttpBridgeDispatcher（MockTransport）+ 显式 price_type。"""
    client = httpx.Client(transport=httpx.MockTransport(rec.handler))
    return HttpBridgeDispatcher(
        base_url="http://127.0.0.1:8790", client=client, price_type=price_type)


def _bridge_ping(endpoints, order_reply=None):
    """responder：/ping 带 endpoints 自述，/order 返回受理应答（默认回显 limit）。"""
    def _respond(request):
        path = request.url.path
        if path == "/ping":
            return httpx.Response(
                200, json={"ok": True, "endpoints": list(endpoints)})
        if path == "/order":
            return httpx.Response(
                200, json=order_reply or {"ok": True, "price_type": "limit"})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})
    return _respond


def _iquant_pre_open_engine(factory, port, disp, trading_dates, stock):
    """盘前开关打开的 QMT 会话（engine=iquant）。"""
    engine = _pre_open_engine(factory, port, disp, trading_dates, stock)
    engine._engine_key = "iquant"
    return engine


def _pre_open_fixture(price_type=None, endpoints=("/ping", "/order", "/price_limit"),
                      order_reply=None):
    """(factory, port, ctx, rec, disp, engine) 的公共装配（单组合 1d + 一只可卖票）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_bridge_ping(endpoints, order_reply=order_reply))
    disp = _pre_open_bridge(rec, price_type=price_type)
    engine = _iquant_pre_open_engine(factory, port, disp, [d1, d2], stock)
    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()
    return factory, port, ctx, rec, disp, engine, stock


def test_pre_open_iquant_releases_when_bridge_supports_limit_price():
    """price_type=limit + 桥自述支持 → 放行；委托价是盘前限价（prType=11 原价）。"""
    factory, port, _, rec, _, engine, stock = _pre_open_fixture(
        price_type="limit")

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    orders = [r for r in rec.requests if r.url.path == "/order"]
    assert len(orders) == 1
    payload = json.loads(orders[0].content.decode("utf-8"))
    assert payload["price_type"] == "limit"      # 桥按指定价下发，不被回退
    assert payload["pr_type"] == 14              # 兼容老桥的历史字段，新桥以 price_type 为准
    assert payload["price"] > 0                  # 盘前算出的限价，非对手价 0
    assert engine._signal_bar_deferred == []
    db = factory()
    assert db.query(LiveOrder).filter_by(status="submitted").count() == 1
    assert db.query(LiveOrder).filter_by(status="deferred").count() == 0
    evs = db.query(LiveDecisionEvent).filter_by(
        gate="pre_open_auction", action="release").all()
    db.close()
    assert [e.stock_code for e in evs] == [stock]


def test_pre_open_iquant_per_order_limit_even_if_config_opponent():
    """会话配 opponent 也不拦：盘前单按单带 price_type=limit，盘中单仍用配置默认。"""
    factory, port, _, rec, _, engine, stock = _pre_open_fixture(price_type="opponent")

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    orders = [r for r in rec.requests if r.url.path == "/order"]
    assert len(orders) == 1
    payload = json.loads(orders[0].content.decode("utf-8"))
    assert payload["price_type"] == "limit"   # 按单覆盖，不受会话级 opponent 影响
    assert payload["pr_type"] == 14
    assert payload["price"] > 0
    assert engine._signal_bar_deferred == []
    assert engine._pre_open_echo_bad is None


def test_http_bridge_place_order_price_type_per_order_override():
    """盘中不带覆盖 → 配置默认 opponent；盘前显式 limit 按单覆盖，其它调用点不变。"""
    rec = _Recorder(respond=_bridge_ping(("/ping", "/order", "/price_limit")))
    disp = _pre_open_bridge(rec, price_type="opponent")
    order = OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code="600000.SH",
        trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
        quantity=100, price=Decimal("9.0"),
        bar_time=datetime(2026, 8, 25, 9, 40))
    disp.place_order(order)
    disp.place_order(order, price_type="limit")
    payloads = [json.loads(r.content.decode("utf-8"))
                for r in rec.requests if r.url.path == "/order"]
    assert [p["price_type"] for p in payloads] == ["opponent", "limit"]
    assert [p["pr_type"] for p in payloads] == [14, 14]


def test_pre_open_iquant_blocked_when_bridge_lacks_price_type_capability():
    """桥自述无指定价能力（未升级：prType 固定 14）→ 拦截，绝不静默降级对手价。"""
    factory, _, _, rec, _, engine, _ = _pre_open_fixture(
        price_type="limit", endpoints=("/ping", "/order"))
    events = []
    engine._emit = lambda et, payload: events.append((et, payload))

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    assert not any(r.url.path == "/order" for r in rec.requests)
    assert len(engine._signal_bar_deferred) == 1
    ev = [p for et, p in events if et == "risk"][0]
    assert ev["gate"] == "pre_open_auction" and ev["action"] == "block"
    assert "桥不支持指定价" in ev["message"]
    assert "盘前限价不可控" in ev["message"]


def test_pre_open_iquant_blocked_when_bridge_echoed_opponent():
    """盘前单 /order 回显 opponent（桥未采纳 limit）→ 当日后续盘前单拦截。"""
    factory, _, _, rec, _, engine, _ = _pre_open_fixture(
        price_type="opponent", order_reply={"ok": True, "price_type": "opponent"})

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    # 首单已报出（闸无法预知回显），但回显异常被记录 → 后续盘前单整日拦截
    assert any(r.url.path == "/order" for r in rec.requests)
    assert engine._pre_open_echo_bad == "opponent"
    reason = engine._pre_open_price_block_reason()
    assert reason and "回显" in reason and "limit" in reason


@pytest.mark.parametrize("engine_key", ["tdx", "ths", "goldminer"])
def test_pre_open_non_iquant_channels_unaffected(engine_key):
    """非 QMT 通道（通达信 PRICE_MY / 同花顺强制 approval / 掘金 OrderType_Limit）
    委托价本就由平台决定，不看 price_type，盘前行为不变。"""
    factory, port, _, rec, _, engine, _ = _pre_open_fixture(price_type="opponent")
    engine._engine_key = engine_key
    events = []
    engine._emit = lambda et, payload: events.append((et, payload))

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    assert engine._signal_bar_deferred == []
    assert not [p for et, p in events if p.get("action") == "block"]


def test_on_claim_anomaly_emits_risk_event():
    """通道认领异常（TDX 价差不符）→ risk 事件，补 session_id/engine。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 24)], "600000.SH")
    events = []
    engine._emit = lambda et, payload: events.append((et, payload))

    engine._on_claim_anomaly({
        "gate": "claim_price_mismatch", "stock_code": "600000.SH",
        "expected_price": 9.0, "expire_in_seconds": 180})

    assert len(events) == 1
    et, payload = events[0]
    assert et == "risk"
    assert payload["gate"] == "claim_price_mismatch"
    assert payload["session_id"] == engine.session_id
    assert payload["engine"] == engine._engine_key


def test_announce_pre_open_price_mode_emits_decision():
    """盘前强制 approval：启动时发 decision（gate=pre_open_price_mode/forced）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    disp, _ = _make_dispatcher(_Recorder())
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 24)], "600000.SH")
    engine._dispatcher.price_mode_forced_pre_open = True
    engine._dispatcher.ths_price_mode = "approval"
    events = []
    engine._emit = lambda et, payload: events.append((et, payload))

    engine._announce_pre_open_price_mode()

    assert len(events) == 1
    et, payload = events[0]
    assert et == "decision"
    assert payload["gate"] == "pre_open_price_mode"
    assert payload["action"] == "forced"
    assert payload["price_mode"] == "approval"
    assert "价格由平台键入" in payload["message"]




def test_dedupe_inflight_sells_skips_duplicate_and_narrows_partial():
    """B2：同票同向在途卖单去重（按未结单状态，不按当日标记）。

    - 在途 5300 未成交 + 新单 5300 → 整单跳过（skip 事件）；
    - 在途 5300 已成交 3000（剩 2300）+ 新目标 5300 → 收窄为 3000；
    - 其它票 / 买单不受影响；今天在途清完后明天可照常再卖（状态判据）。
    """
    import threading
    from types import SimpleNamespace
    from core.engine.live_engine import LiveEngine
    from tq_iquant_shared.constants import TradeType

    eng = object.__new__(LiveEngine)
    eng._book_lock = threading.RLock()
    calls = []
    eng._rec_live = lambda *a, **kw: calls.append((a, kw))

    portfolio = SimpleNamespace(portfolio_id=1)
    pending = SimpleNamespace(
        trade_type="SELL", portfolio_strategy_id=1, strategy_id=2,
        stock_code="601827.SH", quantity=5300, filled_quantity=0)
    eng._order_machine = SimpleNamespace(pending_orders={"a": pending})
    sell = lambda qty=5300, code="601827.SH": SimpleNamespace(
        trade_type=TradeType.SELL, strategy_id=2, stock_code=code, quantity=qty)

    assert eng._dedupe_inflight_sells(portfolio, [sell()]) == []
    assert calls and calls[-1][0][0] == "inflight_sell" and calls[-1][0][1] == "skip"

    # 部分成交：剩 2300 在途 → 新单目标 5300 收窄为 3000
    pending.filled_quantity = 3000
    kept = eng._dedupe_inflight_sells(portfolio, [sell()])
    assert len(kept) == 1 and kept[0].quantity == 3000
    assert calls[-1][0][1] == "reduce"

    # 其它票 / 买单不受影响
    fake_buy = SimpleNamespace(trade_type=TradeType.BUY, strategy_id=2,
                               stock_code="601827.SH", quantity=1000)
    kept2 = eng._dedupe_inflight_sells(portfolio, [sell(code="300174.SZ"), fake_buy])
    assert [o.stock_code for o in kept2] == ["300174.SZ", "601827.SH"]
    assert kept2[1].quantity == 1000


def test_dedupe_inflight_sells_ignores_untraceable_pending():
    """untraceable（180s 起）/ 人工核对（grace 后）的在途卖单不再占用批内去重额度。

    回归：DB 同向门已排除这两类单，批内去重若仍计入，止损卖单在 180s 释放后会被
    继续压住（最长到 600s），与统一口径矛盾。
    """
    import threading
    from types import SimpleNamespace
    from core.engine.live_engine import LiveEngine
    from tq_iquant_shared.constants import TradeType

    eng = object.__new__(LiveEngine)
    eng._book_lock = threading.RLock()
    calls = []
    eng._rec_live = lambda *a, **kw: calls.append((a, kw))
    portfolio = SimpleNamespace(portfolio_id=1)
    sell = lambda: SimpleNamespace(
        trade_type=TradeType.SELL, strategy_id=2, stock_code="601827.SH", quantity=5300)

    for marker in ("bridge_order_untraceable:", "bridge_order_manual_review:"):
        pending = SimpleNamespace(
            trade_type="SELL", portfolio_strategy_id=1, strategy_id=2,
            stock_code="601827.SH", quantity=5300, filled_quantity=0,
            error_message=marker + "无法追溯")
        eng._order_machine = SimpleNamespace(pending_orders={"a": pending})
        kept = eng._dedupe_inflight_sells(portfolio, [sell()])
        assert len(kept) == 1 and kept[0].quantity == 5300  # 不再被压
        assert not calls  # 不产生 inflight_sell 事件

    # 对照：同一张单没有标记时仍按未成交量拦截
    pending.error_message = None
    assert eng._dedupe_inflight_sells(portfolio, [sell()]) == []
    assert calls[-1][0][0] == "inflight_sell"

# ===========================================================================
# 盘前集合竞价：撤单时点 / 不可撤时段 / 放行截止 / 通道能力闸（2026-11-11）
# ===========================================================================

def test_pre_open_cancel_waits_until_0930_plus_threshold(monkeypatch, caplog):
    """盘前单 09:18 报出、阈值 3 分钟：09:21 不撤（旧口径按提交+3min 会在 09:21 撤），
    09:33（=09:30+3min）才撤。09:21 属 09:20–09:25 不可撤段 → 不发请求、不打标记、
    留一条可观测日志。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo.bar_time = datetime(2026, 8, 5, 9, 18)     # 盘前路径：bar_time=本次报单时点
    lo.created_at = datetime(2026, 8, 5, 1, 18)   # UTC naive = 北京 09:18
    lo_id = lo.id
    db.commit()
    db.close()

    # 09:21 —— 不可撤时段：跳过撤单
    monkeypatch.setattr("core.engine.live.order_machine.now_shanghai",
                        lambda: datetime(2026, 8, 5, 9, 21))
    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()
    assert disp.cancel_calls == []
    db = factory()
    assert db.get(LiveOrder, lo_id).error_message is None   # 不记失败/不记请求中
    db.close()
    assert "不可撤时段" in caplog.text

    # 09:33 —— 09:30 + 3 分钟到点：撤单并打「盘前单」标记
    monkeypatch.setattr("core.engine.live.order_machine.now_shanghai",
                        lambda: datetime(2026, 8, 5, 9, 33))
    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()
    assert disp.cancel_calls == [("W001", "600000.SH")]
    db = factory()
    msg = db.get(LiveOrder, lo_id).error_message or ""
    db.close()
    assert msg.startswith(_CANCEL_TAG)
    assert "盘前单超时未成交已撤单（自 09:30 起计时 3 分钟）" in msg


def test_non_pre_open_cancel_keeps_created_at_based_timeout(monkeypatch):
    """非盘前单维持旧口径：created_at + 阈值（09:31 的 bar 单、10 分钟后到点即撤）。"""
    monkeypatch.setattr("core.engine.live.order_machine.load_config", _cfg_minutes(3))
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    disp = FakeTdxDispatcher(orders=[_bridge_row("W001")])
    engine = _make_engine(disp, factory, port)
    db = factory()
    lo = _pending_order_with_ref(db)
    lo.bar_time = datetime(2026, 8, 5, 9, 31)      # 盘中 bar（非盘前时段）
    lo.created_at = datetime(2026, 8, 5, 1, 38)    # UTC = 北京 09:38
    lo_id = lo.id
    db.commit()
    db.close()

    monkeypatch.setattr("core.engine.live.order_machine.now_shanghai",
                        lambda: datetime(2026, 8, 5, 9, 41))
    db = factory()
    engine._cancel_unfilled_orders(db)
    db.commit()
    db.close()
    assert disp.cancel_calls == [("W001", "600000.SH")]
    db = factory()
    assert "阈值 3 分钟" in (db.get(LiveOrder, lo_id).error_message or "")
    db.close()


def test_pre_open_release_stops_after_cutoff_for_bridge():
    """桥通道余量 30s：09:19:30 之后不再放行，deferred 保持到 09:31 兜底。"""
    factory, port, _, rec, _, engine, stock = _pre_open_fixture(price_type="limit")

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 19, 31))

    assert [r for r in rec.requests if r.url.path == "/order"] == []
    assert engine._pre_open_deadline_day == date(2026, 8, 25)
    assert len(engine._signal_bar_deferred) == 1
    db = factory()
    assert db.query(LiveOrder).filter_by(status="deferred").count() == 1
    db.close()


def test_pre_open_release_allowed_just_before_cutoff_for_bridge():
    """桥通道余量 30s：09:19:29 仍在窗口内 → 正常放行。"""
    factory, port, _, rec, _, engine, stock = _pre_open_fixture(price_type="limit")

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 19, 29))

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    assert engine._pre_open_deadline_day is None
    assert engine._pre_open_release_cutoff == datetime(2026, 8, 25, 9, 19, 30)


def test_pre_open_release_margin_by_channel_type():
    """余量按通道类型：桥 30s；UI 自动化 25s/单、下限 120s、上限 240s。"""
    factory, port, _, rec, _, engine, stock = _pre_open_fixture(price_type="limit")
    assert engine._pre_open_release_margin(1) == 30
    engine._dispatcher.execute_mode = "file_builtin"   # UI 自动化通道特征
    assert engine._pre_open_release_margin(1) == 120   # 下限
    assert engine._pre_open_release_margin(10) == 240  # 上限（不把 300s 窗口压没）


def test_pre_open_price_gate_uses_channel_capability_not_engine_key():
    """掘金（engine=goldminer）同样受能力闸：桥自述含 /price_limit → 放行。"""
    factory, port, _, rec, _, engine, stock = _pre_open_fixture(price_type="limit")
    engine._engine_key = "goldminer"

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    assert engine._signal_bar_deferred == []


def test_pre_open_price_gate_blocks_goldminer_without_limit_capability():
    """掘金桥自述无 /price_limit（supports_price_type=False）→ 拦下，deferred 保留。"""
    factory, port, _, rec, _, engine, stock = _pre_open_fixture(
        price_type="limit", endpoints=("/ping", "/order"))
    engine._engine_key = "goldminer"

    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    assert [r for r in rec.requests if r.url.path == "/order"] == []
    assert len(engine._signal_bar_deferred) == 1


# ===========================================================================
# 同一根 bar 只下一次单的**持久**幂等标记（2026-11-11，live_bar_order_marks）
# 旧 D6 门只覆盖 submitted/partial/filled；原单 canceled/rejected 后重启重驱同一
# 根 bar（1d/1w/1mon 典型）会再次下单。标记按 (session/组合/策略/股票/bar_time/
# 方向/seq) 唯一，不看订单终态，唯一索引兜底并发。
# ===========================================================================

def _bar_mark_engine(factory, port, rec=None):
    """盘前无关的常规引擎（返回 engine, rec）。"""
    rec = rec or _Recorder()
    disp, _ = _make_dispatcher(rec)
    return _make_engine(disp, factory, port), rec


def _bar_mark_case(factory, port, engine, stock, bar_time, status):
    """首次下单 → 原单收口到 status（模拟历史终态/在途）→ 重驱同一根 bar。"""
    engine.signal_cache = {
        (1, stock, bar_time): [{"name": "open_sig", "value": 1}],
    }
    bar = _bar(stock, "9.0", bar_time)
    engine._handle_bar(port, bar)          # 首次：落 submitted + 写标记
    if status is not None:
        db = factory()
        row = db.query(LiveOrder).one()
        row.status = status
        db.commit()
        db.close()
    engine._handle_bar(port, bar)          # 重驱同一根 bar
    return bar


def test_bar_mark_blocks_redrive_after_original_canceled():
    """原单已 canceled：重驱同一根 bar 不再下单（旧 D6 门覆盖不到）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, rec = _bar_mark_engine(factory, port)

    _bar_mark_case(factory, port, engine, stock, bar_time, "canceled")

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    db = factory()
    assert db.query(LiveOrder).count() == 1          # 没有第二笔
    assert db.query(LiveBarOrderMark).count() == 1
    db.close()
    ev = next(r for r in _decision_rows(factory) if r.gate == "bar_processed")
    assert ev.action == "block" and "该 bar 已处理" in (ev.message or "")


def test_bar_mark_blocks_redrive_after_original_rejected():
    """原单已 rejected：重驱同一根 bar 仍被持久标记拦住。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, rec = _bar_mark_engine(factory, port)

    _bar_mark_case(factory, port, engine, stock, bar_time, "rejected")

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    db = factory()
    assert db.query(LiveOrder).count() == 1
    db.close()


def test_bar_mark_keeps_inflight_block_behavior():
    """原单在途（submitted）：行为与现在一致——仍拦（走既有 dup_skip 门）。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, rec = _bar_mark_engine(factory, port)

    _bar_mark_case(factory, port, engine, stock, bar_time, None)

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1
    gates = {r.gate for r in _decision_rows(factory)}
    assert "dup_skip" in gates


def test_bar_mark_allows_different_bar_time():
    """不同 bar_time 是两次决策：正常下单，不被标记误杀。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    t1, t2 = datetime(2026, 8, 5, 10, 0), datetime(2026, 8, 5, 10, 5)
    engine, rec = _bar_mark_engine(factory, port)
    engine.signal_cache = {
        (1, stock, t1): [{"name": "open_sig", "value": 1}],
        (1, stock, t2): [{"name": "open_sig", "value": 1}],
    }

    engine._handle_bar(port, _bar(stock, "9.0", t1))
    db = factory()
    db.query(LiveOrder).one().status = "filled"   # 收口首单，避免在途去重干扰
    db.commit()
    db.close()
    engine._handle_bar(port, _bar(stock, "9.0", t2))

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 2
    db = factory()
    assert db.query(LiveBarOrderMark).count() == 2
    db.close()


def test_bar_mark_exempts_requote_resubmission():
    """追价重下（同一张单加价重报）不被该门挡：原单标记已在，重报仍发桥。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single()
    ctx.requote_enabled = True
    ctx.requote_max_attempts = 2
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, rec = _bar_mark_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}

    engine._handle_bar(port, _bar(stock, "9.0", bar_time))
    db = factory()
    lo = db.query(LiveOrder).one()
    lo_id = lo.id
    lo.status = "canceled"
    db.commit()
    db.close()

    # 追价重下入口（主循环撤单收敛后调用）
    db = factory()
    engine._requote_after_timeout_cancel(db, [db.get(LiveOrder, lo_id)])
    db.commit()
    db.close()

    assert len([r for r in rec.requests if r.url.path == "/order"]) == 2
    db = factory()
    statuses = [r.status for r in db.query(LiveOrder).order_by(LiveOrder.id).all()]
    assert statuses == ["canceled", "submitted"]
    db.close()


def test_bar_mark_survives_engine_restart():
    """进程内新建引擎实例模拟重启：标记在库里，重启后第一次 tick 仍能拦住。"""
    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    bar_time = datetime(2026, 8, 5, 10, 0)
    engine, rec = _bar_mark_engine(factory, port)
    engine.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    engine._handle_bar(port, _bar(stock, "9.0", bar_time))
    db = factory()
    db.query(LiveOrder).one().status = "canceled"
    db.commit()
    db.close()

    # 重启：新引擎实例（内存集合全空），同一个 DB
    engine2, rec2 = _bar_mark_engine(factory, port)
    engine2.signal_cache = {(1, stock, bar_time): [{"name": "open_sig", "value": 1}]}
    engine2._handle_bar(port, _bar(stock, "9.0", bar_time))

    assert [r for r in rec2.requests if r.url.path == "/order"] == []
    db = factory()
    assert db.query(LiveOrder).count() == 1
    db.close()


# ---------------- P2-2 当日首根基线 bar 正常派发 ----------------
def test_today_baseline_bar_runs_formula_and_risk(monkeypatch):
    """P2-2：种子 bar 是**当日**（快照通道 09:31 首根）时不再按基线跳过。

    旧实现把「首次建基线的最新一根 bar」无条件当基线 → 只放行 deferred、不跑
    公式与风控，快照通道下等于每天稳定丢一次首分钟信号与止损。改为只有
    种子日早于当日（离线残留 / iquant 首窗口的昨日 bar）才跳过。
    """
    import importlib

    live_mod = importlib.import_module("core.engine.live_engine")
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1m")
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 25)], stock)
    monkeypatch.setattr(
        live_mod, "now_shanghai", lambda: datetime(2026, 8, 25, 9, 31))
    engine.signal_cache = {}
    fill_calls = []
    engine._fill_signal_cache = lambda *a, **k: fill_calls.append(1)

    bar = _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31))
    bar.baseline = True
    engine._on_bar(bar)

    assert fill_calls, "当日基线 bar 应正常跑公式信号评估（不再被当陈旧种子跳过）"


def test_stale_baseline_bar_still_skipped(monkeypatch):
    """P2-2：种子日早于当日（昨日 bar）仍按基线跳过，不重放历史信号。"""
    import importlib

    live_mod = importlib.import_module("core.engine.live_engine")
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1m")
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 25)], stock)
    monkeypatch.setattr(
        live_mod, "now_shanghai", lambda: datetime(2026, 8, 25, 9, 31))
    engine.signal_cache = {}
    fill_calls = []
    engine._fill_signal_cache = lambda *a, **k: fill_calls.append(1)

    bar = _bar(stock, "9.3", datetime(2026, 8, 24, 14, 59))
    bar.baseline = True
    engine._on_bar(bar)

    assert fill_calls == []


# ---------------- P2-3 首根 open 取自快照合成器 ----------------
def test_day_first_bar_open_uses_snapshot_synth_without_tq_query():
    """P2-3：有合成器时当日首根 open 来自内存 open_at()，不再打 250 根 TQ 查询。

    tdx/ths 通道盘中本地 1m 只到上一交易日 → 旧实现恒 None（放行价退化成实时价），
    且每单多打一次 250 根查询（会卡通达信）。
    """
    from core.engine.live.snapshot_bars import SnapshotBarSynthesizer

    factory, _ = _db_factory()
    port, _ = _portfolio_single()
    stock = "600000.SH"
    rec = _Recorder()
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 25)], stock)

    snaps = {stock: {"Now": "9.50", "Volume": "1000"}}
    synth = SnapshotBarSynthesizer(
        [stock],
        fetch_snapshot=lambda codes: {c: dict(snaps[c]) for c in codes if c in snaps},
        in_session=lambda dt: True,
    )
    today = now_shanghai().date()
    synth.sample(datetime(today.year, today.month, today.day, 9, 30, 5))
    snaps[stock] = {"Now": "9.80", "Volume": "1200"}
    synth.sample(datetime(today.year, today.month, today.day, 9, 31, 5))
    engine._snapshot_synth = synth

    assert engine._day_first_bar_open(stock) == Decimal("9.50")
    assert not any(r.url.path == "/quote" for r in rec.requests)  # 不新增 TQ 查询


# ---------------- P2-11 日线新鲜度跳过落 risk 事件 ----------------
def test_stale_daily_bars_emit_freshness_risk_event_once():
    """P2-11：日线最新非当日 → 跳过并记 gate=daily_bar_freshness（每交易日一条）。

    旧实现只打一行 WARNING，工作台看不出「今天日线策略没跑」。
    """
    factory, _ = _db_factory()
    port, _ = _portfolio_single(period="1d")
    stock = "600000.SH"
    # 只到 08-07（周五），08-10 当日线未落盘
    stale = [{"stime": "20260807000000", "open": 9.0, "high": 9.2,
              "low": 9.0, "close": 9.2, "volume": 10000, "amount": 92000.0}]
    rec = _Recorder(respond=_respond_quote_bars(stock, stale))
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [date(2026, 8, 10)], stock)
    q = asyncio.Queue()
    engine._stream_subscribers.append(q)

    engine._maybe_daily_bars(now=datetime(2026, 8, 10, 15, 5))
    engine._maybe_daily_bars(now=datetime(2026, 8, 10, 15, 6))  # 同日不重复

    events = []
    while not q.empty():
        events.append(q.get_nowait())
    freshness = [e for e in events if e.get("gate") == "daily_bar_freshness"]
    assert len(freshness) == 1
    assert freshness[0]["action"] == "skip"
    assert freshness[0]["stale_count"] == 1


# ---------------- P1-3 桥拒单/价格闸拦截 → 回 deferred 重排 ----------------
def _reject_responder(error, cancel_result="canceled"):
    """responder：/order 明确拒单；/cancel 按给定结果返回。"""
    def _respond(request):
        path = request.url.path
        if path == "/ping":
            return httpx.Response(200, json={
                "ok": True,
                "endpoints": ["/ping", "/order", "/price_limit", "/cancel"]})
        if path == "/order":
            return httpx.Response(200, json={"ok": False, "error": error})
        if path == "/cancel":
            return httpx.Response(
                200, json={"ok": True, "data": {"result": cancel_result}})
        return httpx.Response(404, json={"ok": False})
    return _respond


def _minute_sell_order(stock, bar_time, price="9.0", qty=300):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=TradeType.SELL, signal_type=SignalType.CLOSE,
        quantity=qty, price=Decimal(price),
        bar_time=bar_time, signal_name="stop_loss",
    )


def test_bridge_reject_price_limit_requeues_deferred():
    """P1-3：涨跌停拦截属可重排 → deferred 行 + 回队 + 次数标记（不是终态）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1m")
    stock = "600000.SH"
    day = date(2026, 8, 24)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_reject_responder(
        "委托买价 10.50 ≥ 涨停价 10.50（前收 9.55），已拦截"))
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [day], stock)
    order = _minute_sell_order(stock, datetime(2026, 8, 24, 10, 0))

    db = factory()
    engine._submit_one_order(db, port, order, "1m")
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    db.close()

    assert row.status == "deferred", row.status
    assert "deferred_retry:n=1" in (row.error_message or "")
    assert order._deferred_row_id == row.id
    assert engine._signal_bar_deferred and engine._signal_bar_deferred[-1] is order
    # 重排是「次日/下一根 bar 再试」，不是当日重报：本 bar 只发过一次
    assert len([r for r in rec.requests if r.url.path == "/order"]) == 1


def test_bridge_reject_terminal_stays_rejected():
    """P1-3：资金不足类真错误仍终态 rejected，不进队列。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1m")
    stock = "600000.SH"
    day = date(2026, 8, 24)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_reject_responder("可用资金不足"))
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, [day], stock)
    order = _minute_sell_order(stock, datetime(2026, 8, 24, 10, 0))

    db = factory()
    engine._submit_one_order(db, port, order, "1m")
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    db.close()

    assert row.status == "rejected", row.status
    assert not engine._signal_bar_deferred


def test_bridge_reject_requeue_retry_cap_then_terminal():
    """P1-3：重排次数上限 2（首报 + 2 次重排），超限转终态 rejected。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1m")
    stock = "600000.SH"
    days = [date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26)]
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_reject_responder(
        "委托卖价 8.20 ≤ 跌停价 8.20（前收 9.10），已拦截"))
    disp, _ = _make_dispatcher(rec)
    engine = _calendar_engine(factory, port, disp, days, stock)
    order = _minute_sell_order(stock, datetime(2026, 8, 24, 10, 0))

    db = factory()
    engine._submit_one_order(db, port, order, "1m")
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    assert row.status == "deferred"
    assert "deferred_retry:n=1" in (row.error_message or "")
    # 次日重试（signal_bar_date 已对齐原 bar 日，不会被未来函数闸再次 defer）
    order.bar_time = order.bar_time + timedelta(days=1)
    engine._submit_one_order(db, port, order, "1m")
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    assert row.status == "deferred"
    assert "deferred_retry:n=2" in (row.error_message or "")
    # 第三次：次数用尽 → 终态 rejected，不再回队
    order.bar_time = order.bar_time + timedelta(days=1)
    engine._submit_one_order(db, port, order, "1m")
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    db.close()

    assert row.status == "rejected", row.status
    assert order._deferred_retry_count == 2
    assert not engine._signal_bar_deferred
    assert len([r for r in rec.requests if r.url.path == "/order"]) == 3


# ---------------- P1-4 盘前越价撤单 → 回 deferred，09:31 首根 bar 重试 ----------------
def test_open_crossed_pre_open_order_requeues_to_first_bar(monkeypatch):
    """P1-4：开盘越价撤单后回 deferred（受次数上限），同一交易日 09:31 路径重定限价重试。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)

    def respond(request):
        path = request.url.path
        if path == "/ping":
            return httpx.Response(200, json={
                "ok": True,
                "endpoints": ["/ping", "/order", "/price_limit", "/cancel"]})
        if path == "/order":
            return httpx.Response(
                200, json={"ok": True, "price_type": "limit"})
        if path == "/cancel":
            return httpx.Response(
                200, json={"ok": True, "data": {"result": "canceled"}})
        return httpx.Response(404, json={"ok": False})

    rec = _Recorder(respond=respond)
    disp, _ = _make_dispatcher(rec)
    monkeypatch.setattr("core.engine.live_engine.now_shanghai",
                        lambda: datetime(2026, 8, 25, 9, 31))
    monkeypatch.setattr(disp, "query_quote", lambda code, **kw: [
        {"stime": "20260825093000", "open": 8.5, "close": 8.6},
    ])
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)

    db = factory()
    engine._submit_one_order(db, port, _deferred_order(stock, d1), "1d")
    db.close()
    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))
    assert any(r.url.path == "/order" for r in rec.requests)

    # 模拟 /orders 轮询回填柜台委托号（撤单按 order_ref）
    db = factory()
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    row.order_ref = "TEST-REF-P4"
    db.commit()
    db.close()

    bar = _bar(stock, "8.7", datetime(2026, 8, 25, 9, 31))
    bar.period = "1m"
    bar.stocks[stock]["open"] = Decimal("8.5")
    # 卖单盘前限价 = 9.0×(1−2%) = 8.82 > 开盘价 8.5 → 越价撤单
    engine._check_pre_open_orders_at_open(bar)

    db = factory()
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    db.close()
    assert row.status == "deferred", row.status
    assert "deferred_retry:n=1" in (row.error_message or "")
    assert "open_crossed_limit" in (row.error_message or "")
    assert engine._signal_bar_deferred, "撤单后未回队"

    # 同一交易日的 09:31 首根 1m 路径重报：限价 = 当日首根 open（不追价）
    engine._handle_bar(port, bar)
    orders = [json.loads(r.content) for r in rec.requests if r.url.path == "/order"]
    assert len(orders) == 2, orders
    assert Decimal(str(orders[-1]["price"])) == Decimal("8.5")
    db = factory()
    assert db.query(LiveOrder).filter_by(status="submitted").count() == 1
    # 事件留痕：新取舍写进 message（decision 事件由 _handle_bar 落库）
    evs = db.query(LiveDecisionEvent).filter_by(
        gate="pre_open_auction", action="cancel_requeue").all()
    db.close()
    assert evs, "缺 cancel_requeue 审计事件"
    assert "回 deferred" in (evs[0].message or "")


def test_cancel_not_converged_does_not_requeue():
    """P1-4 安全阀：撤单返回 pending（未确认）→ 只留痕不重排（防双份成交）。"""
    factory, _ = _db_factory()
    port, ctx = _portfolio_single(period="1d")
    stock = "600000.SH"
    d1, d2 = date(2026, 8, 24), date(2026, 8, 25)
    _seed_sellable(ctx, stock)
    rec = _Recorder(respond=_reject_responder("x", cancel_result="pending"))
    # /order 必须成功（单已报出），仅撤单返回 pending 未确认
    _orig_respond = rec._respond

    def respond(request):
        if request.url.path == "/order":
            return httpx.Response(200, json={"ok": True, "price_type": "limit"})
        return _orig_respond(request)

    rec._respond = respond
    disp, _ = _make_dispatcher(rec)
    engine = _pre_open_engine(factory, port, disp, [d1, d2], stock)

    db = factory()
    order = _deferred_order(stock, d1)
    engine._submit_one_order(db, port, order, "1d")
    db.close()
    engine._maybe_pre_open_auction(datetime(2026, 8, 25, 9, 16))

    db = factory()
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    row.order_ref = "TEST-REF-PENDING"
    db.commit()
    db.close()
    engine._signal_bar_deferred.clear()

    bar = _bar(stock, "9.3", datetime(2026, 8, 25, 9, 31))
    bar.period = "1m"
    bar.stocks[stock]["open"] = Decimal("8.5")
    engine._check_pre_open_orders_at_open(bar)

    db = factory()
    row = db.query(LiveOrder).order_by(LiveOrder.id.desc()).first()
    db.close()
    assert row.status == "submitted", row.status
    assert not engine._signal_bar_deferred


# ---------------- P1-5 分钟单提交价让 1 跳 ----------------
def test_minute_order_price_bias_uses_tick():
    """P1-5：minute + requote_enabled → 提交价在信号价上让 1 跳；关闭/日线不变。"""
    stock = "600000.SH"
    day = date(2026, 8, 24)

    def _submit(period, requote, price="10.00"):
        factory, _ = _db_factory()
        port, ctx = _portfolio_single(period=period)
        ctx.requote_enabled = requote
        rec = _Recorder()
        disp, _ = _make_dispatcher(rec)
        engine = _calendar_engine(factory, port, disp, [day], stock)
        order = OrderEvent(
            strategy_id=1, portfolio_id=1, stock_code=stock,
            trade_type=TradeType.BUY, signal_type=SignalType.OPEN,
            quantity=100, price=Decimal(price),
            bar_time=datetime(2026, 8, 24, 10, 0), signal_name="open_sig",
        )
        db = factory()
        engine._submit_one_order(db, port, order, period)
        db.close()
        bodies = [json.loads(r.content) for r in rec.requests
                  if r.url.path == "/order"]
        return Decimal(str(bodies[-1]["price"])), order

    # 买方向让 1 跳（10.00 → 10.01）
    assert _submit("1m", True)[0] == Decimal("10.01")
    # 开关关闭：保持信号价
    assert _submit("1m", False)[0] == Decimal("10.00")
    # 日线周期：路径不变
    assert _submit("1d", True)[0] == Decimal("10.00")


# ===========================================================================
# 已受理未认领的待发送行复核（2026-11-18）
# ===========================================================================


def _unconfirmed_live_order(**kw):
    from types import SimpleNamespace
    base = dict(
        id=11, order_ref=None, status="submitted", bridge_order_id="bm-1",
        created_at=(datetime.now(timezone.utc).replace(tzinfo=None)
                    - timedelta(seconds=200)),
        strategy_id=5, portfolio_strategy_id=3, stock_code="301600.SZ",
        trade_type="SELL", signal_type="CLOSE", quantity=220,
        price=Decimal("64.01"), bar_time=datetime(2026, 11, 18, 11, 8, 42),
        signal_name="stop_loss", error_message=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _recheck_engine(state="pending", orders=None):
    """轻量引擎桩：只装配 _recheck_unconfirmed_pending_rows 所需属性。"""
    from types import SimpleNamespace
    eng = object.__new__(LiveEngine)
    eng.ctx = SimpleNamespace(session_id=0, portfolios=[], dispatcher=None)
    eng._book_lock = threading.RLock()
    eng.session_id = 7
    eng._engine_key = "tdx"
    portfolio = SimpleNamespace(portfolio_id=3)
    eng.portfolios = [portfolio]
    if state is None:
        eng._dispatcher = SimpleNamespace()
    else:
        eng._dispatcher = SimpleNamespace(pending_row_state=lambda _lo: state)
    calls = []
    eng._requeue_rejected_order = (
        lambda db, p, order, lo, reason, **kw:
        calls.append((p, order, lo, reason, kw)) or True)
    return eng, portfolio, calls


def test_recheck_unconfirmed_requeues_when_row_still_pending():
    """180s 后行仍待发送 → 判未报出，重建订单并回 deferred（不是终态）。"""
    eng, portfolio, calls = _recheck_engine("pending")
    from types import SimpleNamespace
    lo = _unconfirmed_live_order()
    eng._recheck_unconfirmed_pending_rows(SimpleNamespace(commit=lambda: None), [lo])
    assert len(calls) == 1
    p, order, seen_lo, reason, kw = calls[0]
    assert p is portfolio and seen_lo is lo
    assert kw["gate"] == "order_unconfirmed"
    assert order.stock_code == "301600.SZ"
    assert order.trade_type == TradeType.SELL
    assert order.quantity == 220
    assert order.signal_bar_date == date(2026, 11, 18)  # 当日不重报，次日放行
    assert order._deferred_row_id == 11
    assert "点击未生效" in reason


def test_recheck_unconfirmed_keeps_expire_when_row_absent_or_unknown():
    """行已消失/不可判/通道无该能力 → 保持既有 expire 行为，绝不重排。"""
    for state in ("absent", "unknown", None):
        eng, _portfolio, calls = _recheck_engine(state)
        eng._recheck_unconfirmed_pending_rows(
            None, [_unconfirmed_live_order()])
        assert calls == [], state


def test_recheck_unconfirmed_skips_unknown_portfolio_and_young_orders():
    """找不到组合 / 未到 180s → 不重排（交现行为）。"""
    from types import SimpleNamespace

    eng, _portfolio, calls = _recheck_engine("pending")
    eng.portfolios = []
    eng._recheck_unconfirmed_pending_rows(
        SimpleNamespace(commit=lambda: None), [_unconfirmed_live_order()])
    assert calls == []

    eng2, _p2, calls2 = _recheck_engine("pending")
    eng2._recheck_unconfirmed_pending_rows(
        SimpleNamespace(commit=lambda: None),
        [_unconfirmed_live_order(
            created_at=datetime.now(timezone.utc).replace(tzinfo=None)
            - timedelta(seconds=30))])
    assert calls2 == []


def test_on_daily_update_uses_market_value_not_cost():
    """#8: daily_loss uses latest known market value, not cost basis."""
    from decimal import Decimal as _D
    from datetime import date as _date, datetime as _dt
    from core.engine.position import Position

    factory, _ = _db_factory()
    engine, port = _breaker_engine(factory)
    ctx = port.strategies[0]
    code = "000002.SZ"
    pos = Position(code)
    pos.buy(100, _D("10"), _dt(2026, 8, 20))
    ctx.positions[code] = pos
    port._price_snapshot[code] = _D("12")

    captured = {}
    orig = port.risk_manager.update_daily

    def spy(total, today, init, **kwargs):
        captured["total"] = total
        return orig(total, today, init, **kwargs)

    port.risk_manager.update_daily = spy
    engine._breaker.on_daily_update(port, _date(2026, 8, 21))

    assert captured["total"] == port.account.cash + _D("1200")

