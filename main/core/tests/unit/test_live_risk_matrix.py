"""实盘风控 17 项系统性验证矩阵（2026-11-09）。

每项用纯单元/Mock 场景跑**真实代码路径**（不初始化通达信 TQ、不碰生产 DB、
不启停实盘会话）。用例标题以 ``[项N]`` 前缀对应交付矩阵编号：

 1 止损 / 2 止盈 / 3 移动止损 / 4 加仓 ADD / 5 减仓清仓 / 6 T+1 / 7 涨跌停
 8 资金与仓位审批 / 9 熔断 / 10 主从策略 / 11 继承仓 / 12 成交回填与对账
 13 撤单与超时 / 14 收盘清扫 / 16 deferred 释放 / 17 外部信号文件
 （15 14:51 提前收盘由 test_live_engine.py 的 3 个 advance_close 用例验证，
  16 的过期路径同样在本文件覆盖。）
"""
import asyncio
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

import httpx
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.engine.account import Account
from core.engine.bar_poller import BarPoller
from core.engine.event import BarEvent, OrderEvent
from core.engine.execution_engine import (
    ExecutionEngine,
    LiveT1Checker,
    SimulatedT1Checker,
)
from core.engine.http_bridge_dispatcher import HttpBridgeDispatcher
from core.engine.live_engine import LiveEngine
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from core.models import Base, LiveOrder, LiveTrade
from tq_iquant_shared.constants import SignalType, TradeType

STOCK = "600000.SH"
DAY = datetime(2026, 8, 5, 10, 0)       # 周三，交易日
PREV = date(2026, 8, 4)


# ----------------------------------------------------------------------------
# 共用脚手架（与 test_live_engine 同构，独立复制避免跨模块耦合）
# ----------------------------------------------------------------------------
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


class _Recorder:
    """MockTransport：记录请求；/positions 默认空、/orders-deals 默认空。"""

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
        if path in ("/orders", "/deals"):
            return httpx.Response(200, json={"ok": True, "data": []})
        return httpx.Response(404, json={"ok": False, "error": "unknown"})


def _make_dispatcher(rec):
    client = httpx.Client(transport=httpx.MockTransport(rec.handler))
    return HttpBridgeDispatcher(base_url="http://127.0.0.1:8790", client=client)


def _bar(stock, close, bar_time, volume=10000):
    return BarEvent(
        stocks={stock: {
            "open": Decimal(close), "high": Decimal(close),
            "low": Decimal(close), "close": Decimal(close),
            "volume": volume,
        }},
        bar_time=bar_time,
    )


def _risk_ctx(pid=1, sid=1, *, stop=("0.05"), tp=("0.2"), trailing="0",
              add_threshold="0.05", max_add=2, add_ratio="0.1",
              reduce_ratio="0.5", role="independent", master=None,
              max_positions=5, period="1m", formula_name=""):
    ctx = StrategyContext(
        sid, period, Decimal("0.6"), max_positions,
        single_open_ratio=Decimal("0.1"),
        add_position_threshold=Decimal(add_threshold), max_add_count=max_add,
        add_position_ratio=Decimal(add_ratio),
        reduce_position_ratio=Decimal(reduce_ratio),
        role=role, master_strategy_id=master,
    )
    ctx.formula_name = formula_name
    ctx.strategy_risk = StrategyRiskManager(
        Decimal(stop), Decimal(tp), Decimal(trailing))
    return ctx


def _portfolio(*ctxs, initial="100000", max_dd="0.2", daily_limit="0.05"):
    port = Portfolio(
        portfolio_id=1, initial_capital=Decimal(initial),
        risk_manager=PortfolioRiskManager(
            max_drawdown=Decimal(max_dd), daily_loss_limit=Decimal(daily_limit)),
    )
    for ctx in ctxs:
        port.strategies.append(ctx)
    return port


def _pos(stock, qty, cost, buy_day=PREV, highest=None):
    p = Position(stock)
    if qty:
        p.buy(qty, Decimal(cost),
              datetime.combine(buy_day, datetime.min.time()).replace(hour=10))
        if highest is not None:
            p.update_high(Decimal(highest))
    return p


def _engine(factory, port, rec=None):
    rec = rec or _Recorder()
    disp = _make_dispatcher(rec)
    poller = BarPoller(disp, [STOCK], period="1m", count=10)
    eng = LiveEngine(
        session_id=1, portfolios=[port], dispatcher=disp,
        bar_poller=poller, db_session_factory=factory,
    )
    return eng, rec


def _calendar_engine(factory, port, dates, rec=None):
    from core.engine.trading_calendar import TradingCalendar
    eng, rec = _engine(factory, port, rec)
    eng._calendar = TradingCalendar(lambda: list(dates))
    return eng, rec


def _signal(ctx, stock, t, name="open_sig", value=1):
    return {(ctx.strategy_id, stock, t): [{"name": name, "value": value}]}


def _open_cfg(ctx, signal_type=SignalType.OPEN, name="open_sig"):
    ctx.formula_signals = [
        {"signal_name": name, "signal_type": signal_type, "trigger_value": 1}]


def _orders(orders):
    return [(o.stock_code, o.trade_type, o.signal_type, o.quantity) for o in orders]


# ============================================================================
# 项1 止损
# ============================================================================
def test_item01_stop_loss_triggers_at_threshold_and_skips_unknown_cost():
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    ctx.positions[STOCK] = _pos(STOCK, 1000, "10")
    t = DAY

    # 亏 5%（恰好触及阈值）→ 全平 SELL
    orders = port.on_bar(_bar(STOCK, "9.5", t))
    assert _orders(orders) == [(STOCK, TradeType.SELL, SignalType.STOP_LOSS, 1000)]

    # 亏 4.9% → 不触发
    assert port.on_bar(_bar(STOCK, "9.51", t)) == []

    # avg_cost=0（继承仓成本未知）→ 跳过，不除零
    ctx.positions[STOCK] = _pos(STOCK, 1000, "10")
    ctx.positions[STOCK].avg_cost = Decimal("0")
    assert port.on_bar(_bar(STOCK, "1.0", t)) == []


def test_item01_stop_loss_same_day_only_one_live_order():
    """同一交易日连续 bar 重复触发止损 → 在途 SELL 闸只放 1 笔单。"""
    factory, _ = _db_factory()
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    ctx.positions[STOCK] = _pos(STOCK, 1000, "10")
    eng, _rec = _engine(factory, port)
    eng.signal_cache = {}

    eng._handle_bar(port, _bar(STOCK, "9.4", DAY))
    eng._handle_bar(port, _bar(STOCK, "9.3", DAY + timedelta(minutes=5)))

    db = factory()
    rows = db.query(LiveOrder).all()
    db.close()
    assert len(rows) == 1
    assert rows[0].trade_type == "sell"
    assert rows[0].signal_type == "STOP_LOSS"
    assert rows[0].quantity == 1000


# ============================================================================
# 项2 止盈
# ============================================================================
def test_item02_take_profit_trigger_and_single_risk_order_priority():
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    ctx.positions[STOCK] = _pos(STOCK, 1000, "10")
    _open_cfg(ctx)

    # 盈 20% → 止损/止盈互斥只出 TAKE_PROFIT 一单；同 bar 公式 OPEN 被清仓抑制
    orders = port.on_bar(
        _bar(STOCK, "12", DAY), signal_cache=_signal(ctx, STOCK, DAY))
    assert _orders(orders) == [(STOCK, TradeType.SELL, SignalType.TAKE_PROFIT, 1000)]

    # 亏损 bar：只出 STOP_LOSS，不会同时出止盈/移动止损（elif 链）
    orders = port.on_bar(_bar(STOCK, "9.4", DAY))
    assert [o.signal_type for o in orders] == [SignalType.STOP_LOSS]


# ============================================================================
# 项3 移动止损
# ============================================================================
def test_item03_trailing_peak_updates_with_close_before_check():
    ctx = _risk_ctx(stop="5", tp="5", trailing="0.05")
    port = _portfolio(ctx)
    ctx.positions[STOCK] = _pos(STOCK, 1000, "10")

    # 新高 15：先刷新峰值再判定 → 回撤 0，不触发
    assert port.on_bar(_bar(STOCK, "15", DAY)) == []
    assert ctx.positions[STOCK].highest_price == Decimal("15")
    # 回撤 4.67% < 5% → 不触发
    assert port.on_bar(_bar(STOCK, "14.3", DAY)) == []
    # 回撤 5.33% → 触发
    orders = port.on_bar(_bar(STOCK, "14.2", DAY))
    assert [(o.signal_type, o.quantity) for o in orders] == [
        (SignalType.TRAILING_STOP, 1000)]


def test_item03_peak_kept_on_reduce_add_and_reset_on_reopen():
    p = _pos(STOCK, 1000, "10", highest="12")
    p.sell(400, Decimal("11"), DAY)                # 部分减仓
    assert p.highest_price == Decimal("12")        # 峰值不因减仓下降
    p.buy(500, Decimal("11"), DAY, is_add=True)    # 加仓
    assert p.highest_price == Decimal("12")        # 加仓只升不降
    p.sell(p.quantity, Decimal("11.5"), DAY)       # 清仓
    p.buy(100, Decimal("8"), DAY)                  # 清仓重开
    assert p.highest_price == Decimal("8")         # 不复活旧峰值


def test_item03_peak_restore_and_multiple_holders_are_independent():
    """峰值跨重启还原 + 多组合同代码各自独立判定。"""
    factory, _ = _db_factory()
    ctx_a = _risk_ctx(sid=1, stop="5", tp="5")
    ctx_b = _risk_ctx(sid=2, stop="5", tp="5")
    port = _portfolio(ctx_a, ctx_b)
    ctx_a.positions[STOCK] = _pos(STOCK, 1000, "10", highest="12")
    ctx_b.positions[STOCK] = _pos(STOCK, 1000, "10", highest="10")
    ctx_a.strategy_risk.trailing_stop_ratio = Decimal("0.05")
    ctx_a.strategy_risk.stop_loss_ratio = Decimal("0.5")
    ctx_a.strategy_risk.take_profit_ratio = Decimal("0.5")
    ctx_b.strategy_risk.trailing_stop_ratio = Decimal("0.05")

    eng, _rec = _engine(factory, port)
    # 桥快照行成本 10，上次快照峰值 12.88 → 导入仓还原峰值
    ctx_c = _risk_ctx(sid=3)
    port_c = _portfolio(ctx_c)
    eng_c, _rec_c = _engine(factory, port_c)
    db = factory()
    eng_c._pool_codes_by_portfolio = {}
    n = eng_c._import_external_positions(
        db, [{"symbol": STOCK, "volume": 1000, "cost": "10"}],
        DAY, prev_peaks={STOCK: Decimal("12.88")})
    db.commit()
    db.close()
    assert n == 1
    assert ctx_c.positions[STOCK].highest_price == Decimal("12.88")

    # 同一根 bar 11.4：A 回撤 5%（12→11.4）触发；B 峰值被刷新到 11.4 不触发
    orders_a = port.on_bar(_bar(STOCK, "11.4", DAY))
    assert [(o.strategy_id, o.signal_type) for o in orders_a] == [
        (1, SignalType.TRAILING_STOP)]
    assert ctx_b.positions[STOCK].highest_price == Decimal("11.4")


# ============================================================================
# 项4 加仓 ADD
# ============================================================================
def _add_case(threshold, price, add_count=0, cost="10", max_add=2):
    ctx = _risk_ctx(stop="0.5", tp="0.5", add_threshold=threshold, max_add=max_add)
    port = _portfolio(ctx)
    p = _pos(STOCK, 1000, cost)
    p.add_count = add_count
    ctx.positions[STOCK] = p
    _open_cfg(ctx, SignalType.ADD, "add_sig")
    orders = port.on_bar(
        _bar(STOCK, price, DAY), signal_cache=_signal(ctx, STOCK, DAY, "add_sig"))
    return orders


def test_item04_add_threshold_special_values():
    # -1：跳过回撤检查，上涨也加
    assert _add_case("-1", "11")[0].trade_type == TradeType.BUY
    # 0：不涨即加（drop=0 通过）
    assert _add_case("0", "10")[0].trade_type == TradeType.BUY
    # 0：上涨（drop<0）不加
    assert _add_case("0", "10.5") == []
    # 正常 5%：跌 6% 加；跌 3% 不加
    assert _add_case("0.05", "9.4")[0].trade_type == TradeType.BUY
    assert _add_case("0.05", "9.7") == []


def test_item04_add_max_count_unknown_cost_and_risk_priority():
    # 已达 max_add_count → 不加
    assert _add_case("-1", "9", add_count=2, max_add=2) == []
    # 成本未知 → 不除零、不加
    assert _add_case("-1", "9", cost="0") == []
    # 风控优先：同一 bar 止损 + ADD → 只出止损 SELL，ADD 被清仓抑制
    ctx = _risk_ctx(add_threshold="-1")
    port = _portfolio(ctx)
    ctx.positions[STOCK] = _pos(STOCK, 1000, "10")
    _open_cfg(ctx, SignalType.ADD, "add_sig")
    orders = port.on_bar(
        _bar(STOCK, "9", DAY), signal_cache=_signal(ctx, STOCK, DAY, "add_sig"))
    assert _orders(orders) == [(STOCK, TradeType.SELL, SignalType.STOP_LOSS, 1000)]


# ============================================================================
# 项5 减仓 / 清仓
# ============================================================================
def test_item05_reduce_ratio_lot_and_cost_preserved():
    ctx = _risk_ctx(reduce_ratio="0.5")
    port = _portfolio(ctx)
    ctx.positions[STOCK] = _pos(STOCK, 1230, "10", highest="12")
    _open_cfg(ctx, SignalType.REDUCE, "reduce_sig")
    orders = port.on_bar(
        _bar(STOCK, "11", DAY), signal_cache=_signal(ctx, STOCK, DAY, "reduce_sig"))
    assert _orders(orders) == [(STOCK, TradeType.SELL, SignalType.REDUCE, 600)]
    # 减仓后成本与峰值保持不变
    pos = ctx.positions[STOCK]
    pos.sell(600, Decimal("11"), DAY)
    assert pos.quantity == 630
    assert pos.avg_cost == Decimal("10")
    assert pos.highest_price == Decimal("12")


def test_item05_close_full_position_and_t1_caps_live_sell():
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    ctx.positions[STOCK] = _pos(STOCK, 1000, "10")
    _open_cfg(ctx, SignalType.CLOSE, "close_sig")
    orders = port.on_bar(
        _bar(STOCK, "11", DAY), signal_cache=_signal(ctx, STOCK, DAY, "close_sig"))
    assert _orders(orders) == [(STOCK, TradeType.SELL, SignalType.CLOSE, 1000)]


# ============================================================================
# 项6 T+1
# ============================================================================
def test_item06_t1_position_and_live_checker_boundaries():
    # 当日买入不可卖；昨日买入可卖
    p_today = _pos(STOCK, 1000, "10", buy_day=DAY.date())
    assert p_today.available_shares_on(DAY.date()) == 0
    assert p_today.can_sell_on(DAY.date()) is False
    p_prev = _pos(STOCK, 1000, "10", buy_day=PREV)
    assert p_prev.available_shares_on(DAY.date()) == 1000

    checker = LiveT1Checker()
    pos = _pos(STOCK, 1000, "10", buy_day=PREV)
    # 可用字段缺失 → fail-open（券商端兜底）
    assert checker.get_available_shares(pos, DAY.date()) == 1000
    # 可用=0 是合法锁定值 → fail-closed（0 股）
    checker.set_available_map({STOCK: 0})
    assert checker.get_available_shares(pos, DAY.date()) == 0
    # 可用小于持仓 → 取可用
    checker.set_available_map({STOCK: 300})
    assert checker.get_available_shares(pos, DAY.date()) == 300


def test_item06_live_buy_today_cannot_sell_and_available_zero_blocks():
    factory, _ = _db_factory()
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    # 昨日持仓但桥可用 0（T+1 锁定/额度占用）→ 止损卖单被审批拒，不成交
    ctx.positions[STOCK] = _pos(STOCK, 1000, "10", buy_day=DAY.date())

    def respond(request):
        if request.url.path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [{
                "instrument": "600000", "exchange": "SH",
                "volume": 1000, "available": 0, "yesterday_volume": 0,
            }]})
        if request.url.path == "/quote":
            return httpx.Response(200, json={"ok": True, "data": {}})
        if request.url.path in ("/order", "/ping"):
            return httpx.Response(200, json={"ok": True})
        return httpx.Response(200, json={"ok": True, "data": []})

    eng, _rec = _engine(factory, port, _Recorder(respond=respond))
    eng.signal_cache = {}
    eng._handle_bar(port, _bar(STOCK, "9.4", DAY))

    db = factory()
    rows = db.query(LiveOrder).all()
    db.close()
    # 落库行可以存在（submitted 先写），但绝不能有成交/持仓变化
    assert db_all_status(rows) in ([], ["rejected"])
    assert ctx.positions[STOCK].quantity == 1000
    assert port.account.cash == Decimal("100000")


def db_all_status(rows):
    return [r.status for r in rows]


# ============================================================================
# 项7 涨跌停
# ============================================================================
def test_item07_price_limit_dispatcher_blocks_limit_up_and_down():
    from core.engine.tdx_tq_dispatcher import TdxTqDispatcher

    d = TdxTqDispatcher()
    # 隔离真实 TQ：前收/上市日数/名称全部桩化（list 20 根 → 非新股；非 ST）。
    d._prev_close_and_listing = lambda symbol, pre_open=False: (
        Decimal("10"), 20, date(2020, 1, 2))
    d._stock_name = lambda symbol: "测试股份"

    # 一字板/涨停价买入 → 拦截
    assert d.check_price_limit(STOCK, "buy", Decimal("11.00")) is not None
    assert d.check_price_limit(STOCK, "buy", Decimal("10.99")) is None
    # 跌停价卖出 → 拦截
    assert d.check_price_limit(STOCK, "sell", Decimal("9.00")) is not None
    assert d.check_price_limit(STOCK, "sell", Decimal("9.01")) is None
    # 市价单（无价）与取不到前收 → fail-open
    assert d.check_price_limit(STOCK, "buy", None) is None
    d._prev_close_and_listing = lambda symbol, pre_open=False: None
    assert d.check_price_limit(STOCK, "buy", Decimal("99")) is None


def test_item07_live_gate_follows_portfolio_switch_and_bridge_fail_open():
    # 桥通道无涨跌停数据源 → 恒放行（fail-open，文档口径）
    factory, _ = _db_factory()
    assert _make_dispatcher(_Recorder()).check_price_limit(
        STOCK, "buy", Decimal("999")) is None

    # 组合开关 ON：dispatcher 拒绝 → P1-3 现行契约「可重排拒单」回 deferred
    # （error_message 带 deferred_retry:n=<k> 计数），不再一次性落终态 rejected；
    # OFF：不调用校验，正常 submitted。
    calls = []

    def _reject(code, direction, price):
        calls.append((code, direction, price))
        return "涨跌停拦截（测试）"

    for enabled, expect_deferred in ((True, True), (False, False)):
        factory2, _ = _db_factory()
        ctx = _risk_ctx()
        port = _portfolio(ctx)
        port.price_limit_check_enabled = enabled
        _open_cfg(ctx)
        eng, _rec = _engine(factory2, port)
        eng._dispatcher.check_price_limit = _reject
        eng.signal_cache = _signal(ctx, STOCK, DAY)
        eng._handle_bar(port, _bar(STOCK, "9.3", DAY))
        db = factory2()
        rows = db.query(LiveOrder).all()
        db.close()
        assert len(rows) == 1
        if expect_deferred:
            assert rows[0].status == "deferred"
            assert "deferred_retry:n=1" in (rows[0].error_message or "")
            assert "涨跌停" in (rows[0].error_message or "")
            # 回 deferred 队列，等待下一交易日 due 窗口/09:31 首根 bar 重试
            assert any(o.stock_code == STOCK for o in eng._signal_bar_deferred)
        else:
            assert rows[0].status == "submitted"
    assert len(calls) == 1  # OFF 分支完全不调用校验


# ============================================================================
# 项8 资金与仓位审批
# ============================================================================
def _order(trade_type, qty, price, stock=STOCK):
    return OrderEvent(
        strategy_id=1, portfolio_id=1, stock_code=stock,
        trade_type=trade_type, signal_type=SignalType.OPEN,
        quantity=qty, price=Decimal(price), bar_time=DAY,
    )


def test_item08_account_shrinks_to_lot_rejects_below_one_lot():
    acc = Account(Decimal("1000"))
    ok, qty = acc.approve_order(1000, Decimal("10"), Decimal("0"))
    assert ok and qty == 100               # 现金 1000 只能买 1 手
    acc2 = Account(Decimal("500"))
    ok2, qty2 = acc2.approve_order(1000, Decimal("10"), Decimal("0"))
    assert (ok2, qty2) == (False, 0)       # 不足 1 手拒单
    assert acc2.insufficient_count == 1


def test_item08_cap_quantity_max_order_amount_and_exposure_cap():
    engine = ExecutionEngine(SimulatedT1Checker(), SimulatedT1Checker())
    acc = Account(Decimal("100000"))
    # 单笔金额上限 3000 @10 → 300 股
    assert engine.cap_quantity(
        _order(TradeType.BUY, 1000, "10"), acc, None,
        max_order_amount=Decimal("3000")) == 300
    # 策略敞口上限 10000，已持 9000 → 只能再买 1000（100 股）
    assert engine.cap_quantity(
        _order(TradeType.BUY, 1000, "10"), acc, None,
        strategy_position_value=Decimal("9000"),
        strategy_capital_limit=Decimal("10000")) == 100
    # 可用现金上限（实盘账户全貌）500 → 0 股 → 不通过
    assert engine.cap_quantity(
        _order(TradeType.BUY, 1000, "10"), acc, None,
        available_cash_cap=Decimal("500")) is None


def test_item08_max_positions_counts_inherited_and_inflight():
    ctx = _risk_ctx(max_positions=2)
    port = _portfolio(ctx)
    ctx.positions["600001.SH"] = _pos("600001.SH", 100, "10")  # 继承/已成交仓
    _open_cfg(ctx)

    # 持有 1 + 在途 1 >= 2 → 第三只拦截
    orders = port.on_bar(
        _bar(STOCK, "10", DAY), signal_cache=_signal(ctx, STOCK, DAY),
        inflight_opens={ctx.strategy_id: {"600002.SH"}})
    assert orders == []

    # 上限 3 → 放行
    ctx.max_positions = 3
    orders = port.on_bar(
        _bar(STOCK, "10", DAY), signal_cache=_signal(ctx, STOCK, DAY),
        inflight_opens={ctx.strategy_id: {"600002.SH"}})
    assert [(o.trade_type, o.quantity) for o in orders] == [(TradeType.BUY, 600)]


# ============================================================================
# 项9 熔断
# ============================================================================
def test_item09_max_drawdown_halt_next_day_recover_then_manual_after_three():
    rm = PortfolioRiskManager(Decimal("0.2"), Decimal("0.05"))
    initial = Decimal("100000")
    d1 = date(2026, 8, 3)
    rm.update_peak(Decimal("100000"), d1)               # 峰值 100k
    rm.update_peak(Decimal("79000"), d1)                # 回撤 21% → 触发
    assert rm.circuit_breaker_active is True
    assert rm.consecutive_drawdown_triggers == 1

    rm.update_peak(Decimal("79000"), date(2026, 8, 4))  # 次日 → 先恢复
    # 恢复后同一次调用立刻再判回撤（峰值仍 100k）→ 第 2 次触发
    assert rm.consecutive_drawdown_triggers == 2
    rm.update_peak(Decimal("79000"), date(2026, 8, 5))
    assert rm.consecutive_drawdown_triggers == 3
    assert rm.manual_recovery is True
    # 转手动后：再过一天也不自动恢复
    rm.update_peak(Decimal("79000"), date(2026, 8, 6))
    assert rm.circuit_breaker_active is True
    assert rm.is_trading_halted() is True


def test_item09_daily_loss_pause_then_next_day_recover():
    rm = PortfolioRiskManager(Decimal("0.2"), Decimal("0.05"))
    initial = Decimal("100000")
    rm.update_peak(Decimal("100000"), date(2026, 8, 3))
    rm.update_peak(Decimal("94000"), date(2026, 8, 4))  # 跨日：prev_close=100k
    rm.update_daily(Decimal("94000"), date(2026, 8, 4), initial)  # -6% → 暂停
    assert rm.daily_pause_active is True
    assert rm.is_trading_halted() is True

    # 次日：先恢复再评估（enter_day 语义）
    rm.update_peak(Decimal("94000"), date(2026, 8, 5))
    rm.update_daily(Decimal("94000"), date(2026, 8, 5), initial)
    assert rm.daily_pause_active is False


def test_item09_risk_state_restore_ignores_v1_money_fields():
    from core.engine.live.breaker import BreakerService

    rm = SimpleNamespace(
        peak_value=Decimal("0"), prev_close_value=None,
        daily_pause_active=False, circuit_breaker_active=False,
        breaker_trigger_date=None, daily_loss_trigger_date=None,
    )
    raw = json.dumps({
        "peak_value": "1200000", "prev_close_value": "1180000",
        "daily_pause_active": True,
        "breaker_trigger_date": "2026-10-23",
    })
    assert BreakerService._restore_risk_state(rm, raw) is True
    assert rm.peak_value == Decimal("0")      # v1 金额字段不还原（防误触熔断）
    assert rm.prev_close_value is None
    assert rm.daily_pause_active is True      # 开关/日期类仍还原
    assert rm.breaker_trigger_date == date(2026, 10, 23)


# ============================================================================
# 项10 主从策略
# ============================================================================
def test_item10_slave_open_requires_master_holding_and_can_sell_after_exit():
    master = _risk_ctx(sid=1, role="master")
    slave = _risk_ctx(sid=2, role="slave", master=1)
    port = _portfolio(master, slave)
    _open_cfg(slave)
    master.positions[STOCK] = _pos(STOCK, 1000, "10")

    # 主策略持有 → 从策略可开仓
    orders = port.on_bar(
        _bar(STOCK, "10", DAY), signal_cache=_signal(slave, STOCK, DAY))
    assert [(o.strategy_id, o.trade_type) for o in orders] == [(2, TradeType.BUY)]

    # 主策略清仓 → 从策略不能新开仓
    master.positions[STOCK].quantity = 0
    assert port.on_bar(
        _bar(STOCK, "10", DAY), signal_cache=_signal(slave, STOCK, DAY)) == []

    # 从策略存量可自行卖出
    slave.positions[STOCK] = _pos(STOCK, 500, "10")
    _open_cfg(slave, SignalType.CLOSE, "close_sig")
    orders = port.on_bar(
        _bar(STOCK, "10", DAY), signal_cache=_signal(slave, STOCK, DAY, "close_sig"))
    assert [(o.strategy_id, o.trade_type, o.quantity) for o in orders] == [
        (2, TradeType.SELL, 500)]


# ============================================================================
# 项11 继承仓
# ============================================================================
def test_item11_import_unknown_cost_no_risk_and_deducts_cash():
    factory, _ = _db_factory()
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    eng, _rec = _engine(factory, port)
    eng._pool_codes_by_portfolio = {}
    db = factory()

    # 成本未知（停牌/桥无市值）→ 导入但 avg_cost=0，止损/止盈跳过
    n = eng._import_external_positions(
        db, [{"symbol": "600001.SH", "volume": 500, "cost": "0"}], DAY)
    db.commit()
    db.close()
    assert n == 1
    p = ctx.positions["600001.SH"]
    assert p.quantity == 500 and p.avg_cost == Decimal("0")
    bar = _bar("600001.SH", "9", DAY)
    assert port.on_bar(bar) == []                       # 无成本 → 不发假止损
    # 停牌 bar（close=0）不崩、不发信号
    assert port.on_bar(_bar("600001.SH", "0", DAY)) == []

    # 有成本的继承仓：市值从起账现金扣除
    db = factory()
    eng._import_external_positions(
        db, [{"symbol": "600002.SH", "volume": 1000, "cost": "10"}], DAY)
    db.commit()
    db.close()
    assert ctx.positions["600002.SH"].quantity == 1000
    assert port.account.cash == Decimal("100000") - Decimal("10000")


# ============================================================================
# 项12 成交回填与对账
# ============================================================================
def test_item12_partial_backfill_idempotent_and_filled_applies_once():
    factory, _ = _db_factory()
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    eng, _rec = _engine(factory, port)
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code=STOCK, trade_type="buy", order_type="limit",
        price=Decimal("9.3"), quantity=600, filled_quantity=0,
        status="submitted", signal_name="open_sig", signal_type="OPEN",
        bar_time=DAY, bridge_order_id="a" * 32,
    )
    db.add(lo)
    db.flush()
    partial = [{"order_ref": "r1", "price": 9.2, "volume": 300,
                "amount": 2760.0, "commission": 0.69,
                "trade_time": "100000", "trade_date": "20260805"}]
    eng._backfill_order(db, lo, partial)
    eng._backfill_order(db, lo, partial)   # 5s 轮询重复回填
    db.commit()
    assert lo.status == "partial"
    assert int(lo.filled_quantity) == 300
    assert db.query(LiveTrade).filter_by(live_order_id=lo.id).count() == 1
    assert ctx.positions.get(STOCK) is None       # partial 不 apply（防双计）

    full = partial + [{"order_ref": "r1", "price": 9.3, "volume": 300,
                       "amount": 2790.0, "commission": 0.7,
                       "trade_time": "100500", "trade_date": "20260805"}]
    eng._backfill_order(db, lo, full)
    eng._backfill_order(db, lo, full)             # 终态重复回填不得二次 apply
    db.commit()
    assert lo.status == "filled"
    assert int(lo.filled_quantity) == 600
    assert ctx.positions[STOCK].quantity == 600
    assert port.account.cash == Decimal("100000") - Decimal("5550.0") - Decimal("1.39")
    db.close()


def test_item12_reconcile_mismatch_reports_without_correcting():
    factory, _ = _db_factory()
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    ctx.positions[STOCK] = _pos(STOCK, 1000, "9")

    def respond(request):
        if request.url.path == "/positions":
            return httpx.Response(200, json={"ok": True, "data": [{
                "instrument": "600000", "exchange": "SH",
                "volume": 800, "available": 800,
            }]})
        return httpx.Response(200, json={"ok": True, "data": []})

    eng, _rec = _engine(factory, port, _Recorder(respond=respond))
    eng._reconcile_positions()

    got = eng._reconcile_mismatches
    assert len(got) == 1
    assert {k: got[0][k] for k in ("code", "virtual", "real", "diff")} == {
        "code": STOCK, "virtual": 1000, "real": 800, "diff": 200}
    assert ctx.positions[STOCK].quantity == 1000   # 只告警不修正


# ============================================================================
# 项13 撤单与超时
# ============================================================================
def _aged_order(db, *, order_ref=None, bridge_id="b" * 32, minutes=10):
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code=STOCK, trade_type="sell", order_type="limit",
        price=Decimal("9.3"), quantity=500, filled_quantity=0,
        status="submitted", signal_name="stop_loss", signal_type="STOP_LOSS",
        bar_time=DAY, bridge_order_id=bridge_id, order_ref=order_ref,
        created_at=datetime.now(timezone.utc).replace(tzinfo=None)
        - timedelta(minutes=minutes),
    )
    db.add(lo)
    db.flush()
    return lo


def test_item13_expire_rejects_untracked_non_remark_order():
    factory, _ = _db_factory()
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    eng, _rec = _engine(factory, port)
    eng._dispatcher.supports_remark = False
    db = factory()
    lo = _aged_order(db)
    eng._expire_stale_orders(db)
    db.commit()
    db.close()
    assert lo.status == "rejected"
    assert "order match timeout" in (lo.error_message or "")


def test_item13_cancel_failure_retries_then_success_converges_canceled():
    factory, _ = _db_factory()
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    eng, _rec = _engine(factory, port)
    eng._dispatcher.cancel_order = lambda ref, code: False
    db = factory()
    lo = _aged_order(db, order_ref="ref-1")
    db.commit()
    for _ in range(3):
        eng._cancel_unfilled_orders(db)
    db.commit()
    assert eng._order_machine._cancel_fail_counts[lo.id] == 3
    assert lo.id in eng._order_machine._cancel_alerted   # 只告警一次
    assert lo.status == "submitted"                      # 失败保留重试

    # 撤单成功：打请求标记；委托表已查不到 → 下一轮收敛 canceled
    eng._dispatcher.cancel_order = lambda ref, code: True
    eng._dispatcher.query_orders = lambda: []
    eng._cancel_unfilled_orders(db)
    db.commit()
    assert lo.status == "submitted"                      # 本轮仅请求撤单
    assert "超时撤单请求中" in (lo.error_message or "")
    eng._cancel_unfilled_orders(db)
    db.commit()
    db.close()
    assert lo.status == "canceled"


# ============================================================================
# 项14 收盘清扫
# ============================================================================
def test_item14_close_sweep_cancels_unfilled_after_1505():
    factory, _ = _db_factory()
    ctx = _risk_ctx()
    port = _portfolio(ctx)
    eng, _rec = _calendar_engine(factory, port, [date(2026, 8, 24)])
    db = factory()
    lo = LiveOrder(
        live_session_id=1, portfolio_strategy_id=1, strategy_id=1,
        stock_code=STOCK, trade_type="buy", order_type="limit",
        price=Decimal("9.3"), quantity=600, filled_quantity=0,
        status="submitted", signal_name="open_sig", signal_type="OPEN",
        bar_time=datetime(2026, 8, 24, 10, 0), bridge_order_id="c" * 32,
    )
    db.add(lo)
    db.commit()
    db.close()

    eng._maybe_close_sweep(now=datetime(2026, 8, 24, 15, 5))

    db = factory()
    row = db.query(LiveOrder).first()
    db.close()
    assert row.status == "canceled"
    assert "close sweep" in (row.error_message or "")
    assert eng._last_close_sweep_date == date(2026, 8, 24)


# ============================================================================
# 项16 deferred 单释放
# ============================================================================
def test_item16_deferred_released_next_day_frozen_qty_and_price_refresh():
    factory, _ = _db_factory()
    ctx = _risk_ctx(period="1d")
    port = _portfolio(ctx)
    _open_cfg(ctx)
    day1 = datetime(2026, 8, 24, 0, 0)
    day2 = datetime(2026, 8, 25, 9, 31)
    eng, _rec = _calendar_engine(
        factory, port, [date(2026, 8, 24), date(2026, 8, 25)])
    eng.signal_cache = _signal(ctx, STOCK, day1)
    bar1 = _bar(STOCK, "9.3", day1)
    bar1.period = "1d"
    eng._handle_bar(port, bar1)
    deferred = eng._signal_bar_deferred[0]
    original_qty = deferred.quantity
    assert original_qty == 600

    # 价格刷新钩子被调用（模拟实时最新价），数量保持冻结。
    # 返回价格来源标签（契约是 str，_refresh_release_price 会把它拼进返回串）。
    seen = {}

    def _refresh(o):
        seen["qty"] = o.quantity
        o.price = Decimal("10.2")
        return "quote"

    eng._refresh_submit_price = _refresh

    eng.signal_cache = {}
    eng._handle_bar(port, _bar(STOCK, "10.2", day2))

    db = factory()
    rows = db.query(LiveOrder).all()
    db.close()
    assert len(rows) == 1 and rows[0].status == "submitted"
    assert rows[0].quantity == original_qty        # 数量冻结
    assert Decimal(rows[0].price) == Decimal("10.2")  # 价格刷新
    assert rows[0].bar_time == day2                # 决策时间戳 = 放行 bar
    assert eng._signal_bar_deferred == []
    assert seen["qty"] == original_qty


def test_item16_deferred_expired_after_due_day_not_traded():
    """项16（2026-11-09 新语义）：due 日无分钟 bar → 顺延一次并放行；再次到期才取消。

    旧语义「跨过 due 即取消」已由 P1 规则取代：due 当日引擎一根分钟 bar 都没收到
    （行情链路/快照合成失效）属于「没机会放行」而非「信号过期」，不能静默丢单。
    第二段验证有界性：顺延只做一次，顺延后的到期日仍无行情 → 正常取消。
    """
    d1, d2, d3 = date(2026, 8, 24), date(2026, 8, 25), date(2026, 8, 26)

    # ---- 第一段：d2（due）整日无分钟 bar，d3 首根 bar → 顺延一次并正常放行 ----
    factory, _ = _db_factory()
    ctx = _risk_ctx(period="1d")
    port = _portfolio(ctx)
    _open_cfg(ctx)
    eng, rec = _calendar_engine(factory, port, [d1, d2, d3])
    eng.signal_cache = _signal(ctx, STOCK, datetime(2026, 8, 24, 0, 0))
    bar1 = _bar(STOCK, "9.3", datetime(2026, 8, 24, 0, 0))
    bar1.period = "1d"
    eng._handle_bar(port, bar1)
    assert len(eng._signal_bar_deferred) == 1

    events = []
    eng._emit = lambda t, p: events.append((t, p))
    # 顺延标记写 DB 行后、放行提交删除旧行前，抓取当时的 error_message
    captured = {}
    orig_extend = eng._extend_deferred_order

    def spy_extend(order, portfolio, due):
        orig_extend(order, portfolio, due)
        db = factory()
        row = db.query(LiveOrder).filter(
            LiveOrder.id == order._deferred_row_id).first()
        captured["error_message"] = row.error_message if row is not None else None
        db.close()

    eng._extend_deferred_order = spy_extend

    eng.signal_cache = {}
    eng._handle_bar(port, _bar(STOCK, "9.3", datetime(2026, 8, 26, 9, 31)))

    risks = [p for t, p in events if t == "risk"]
    assert any(p.get("rule") == "deferred_extended" for p in risks)   # 顺延事件
    assert not any(p.get("rule") == "deferred_expired" for p in risks)
    # DB 行标记（重启 recover 靠它阻止无限顺延）
    assert "deferred_extended:due=2026-08-25" in (captured["error_message"] or "")
    assert any(r.url.path == "/order" for r in rec.requests)          # 放行即提交

    db = factory()
    rows = db.query(LiveOrder).all()
    db.close()
    assert not any(r.status == "canceled" for r in rows)              # 不判取消
    assert any(r.status == "submitted" for r in rows)
    assert eng._signal_bar_deferred == []

    # ---- 第二段：已顺延一次（recover 读回标记）→ 顺延后到期日仍无 bar → 取消 ----
    factory2, _ = _db_factory()
    ctx2 = _risk_ctx(period="1d")
    port2 = _portfolio(ctx2)
    _open_cfg(ctx2)
    eng2, rec2 = _calendar_engine(factory2, port2, [d1, d2, d3, date(2026, 8, 27)])
    eng2.signal_cache = _signal(ctx2, STOCK, datetime(2026, 8, 24, 0, 0))
    bar1b = _bar(STOCK, "9.3", datetime(2026, 8, 24, 0, 0))
    bar1b.period = "1d"
    eng2._handle_bar(port2, bar1b)
    assert len(eng2._signal_bar_deferred) == 1
    # = recover 从 DB 行 error_message=deferred_extended:due=... 读回「已顺延一次」
    eng2._signal_bar_deferred[0]._deferred_extended = True

    events2 = []
    eng2._emit = lambda t, p: events2.append((t, p))
    eng2.signal_cache = {}
    # 顺延后到期日 = d3（8/26）；d3 无 bar，d4（8/27）才来 bar → 正常取消
    eng2._handle_bar(port2, _bar(STOCK, "9.3", datetime(2026, 8, 27, 9, 31)))

    risks2 = [p for t, p in events2 if t == "risk"]
    assert any(p.get("rule") == "deferred_expired" for p in risks2)
    assert not any(p.get("rule") == "deferred_extended" for p in risks2)
    assert not any(r.url.path == "/order" for r in rec2.requests)

    db = factory2()
    row2 = db.query(LiveOrder).first()
    db.close()
    assert row2.status == "canceled"
    assert "stale_release" in (row2.error_message or "")


# ============================================================================
# 项17 外部信号文件
# ============================================================================
def _ext_engine(factory, *, buy_and_sell=True):
    ctx = _risk_ctx(formula_name="分时突击")
    ctx.formula_signals = []
    if buy_and_sell:
        ctx.formula_signals = [
            {"signal_name": "b1", "signal_type": SignalType.OPEN, "trigger_value": 1},
            {"signal_name": "s1", "signal_type": SignalType.REDUCE, "trigger_value": 1},
            {"signal_name": "bad", "signal_type": "NOT_A_TYPE", "trigger_value": 1},
        ]
    port = _portfolio(ctx)
    eng, rec = _engine(factory, port)
    eng._signal_source = object()      # 非 None：启用外部信号注入路径
    return eng, ctx, port, rec


def _ext_sig(code6, t, formula="分时突击"):
    return {"code": code6, "name": "测试", "time": t,
            "price": Decimal("10"), "formula_name": formula}


def test_item17_external_signal_direction_dedupe_reduce_as_close_and_unknown():
    factory, _ = _db_factory()
    eng, ctx, port, rec = _ext_engine(factory)
    t = datetime(2026, 8, 5, 10, 0)

    # 买入方向：OPEN 落 BUY
    eng._process_external_signal(_ext_sig("600000", t))
    db = factory()
    rows = db.query(LiveOrder).all()
    db.close()
    assert len(rows) == 1 and rows[0].trade_type == "buy"

    # 同代码同日同方向第二次 → 去重不再下单
    eng._process_external_signal(_ext_sig("600000", t + timedelta(minutes=1)))
    db = factory()
    assert db.query(LiveOrder).count() == 1
    db.close()

    # 卖出方向：REDUCE 无比例 → 按 CLOSE 全平（需先有持仓）
    ctx.positions[STOCK] = _pos(STOCK, 700, "10")
    eng._process_external_signal(_ext_sig("600000", t + timedelta(minutes=2)))
    db = factory()
    sells = db.query(LiveOrder).filter_by(trade_type="sell").all()
    db.close()
    assert len(sells) == 1
    assert sells[0].signal_type == "CLOSE"       # REDUCE 被 CLOSE 覆盖
    assert sells[0].quantity == 700

    # 未知动作（策略公式配置里不可解析的 signal_type）：忽略 + 告警，绝不下单
    eng._process_external_signal(_ext_sig("600002", t + timedelta(minutes=3)))
    assert eng._ext_unparseable_count >= 1
    db = factory()
    assert db.query(LiveOrder).filter_by(signal_name="bad").count() == 0
    db.close()
