# -*- coding: utf-8 -*-
"""J7 时点股票池判定内核 + Portfolio 闸门。

覆盖三层：区间语义（含端点/开区间/重叠）、未覆盖日 fail-open、闸门只拦 OPEN/ADD。
"""
from datetime import date, datetime
from decimal import Decimal

from core.engine.decision import DecisionRecorder
from core.engine.event import BarEvent, SignalEvent, TradeEvent
from core.engine.pool_timeline import (
    POOL_TIMELINE_VERSION,
    PoolTimeline,
    build_pool_timeline,
)
from core.engine.portfolio import Portfolio
from core.engine.position import Position
from core.engine.risk_manager import PortfolioRiskManager, StrategyRiskManager
from core.engine.strategy_context import StrategyContext
from tq_iquant_shared.constants import SignalType, TradeType

CODE = "600000.SH"


# --------------------------------------------------------------------------- 内核
def test_interval_endpoints_inclusive_and_open_ended():
    tl = PoolTimeline({CODE: [(date(2026, 1, 1), date(2026, 6, 30))]})
    assert tl.contains(CODE, date(2026, 1, 1)) is True     # 起始日含
    assert tl.contains(CODE, date(2026, 6, 30)) is True    # 结束日含
    assert tl.contains(CODE, date(2025, 12, 31)) is False
    assert tl.contains(CODE, date(2026, 7, 1)) is False
    open_ended = PoolTimeline({CODE: [(date(2026, 1, 1), None)]})
    assert open_ended.contains(CODE, date(2030, 1, 1)) is True


def test_covered_reflects_any_interval_and_caches():
    tl = PoolTimeline({CODE: [(date(2026, 1, 1), None)]})
    assert tl.covered(date(2020, 1, 1)) is False   # 历史没录 → fail-open 判定依据
    assert tl.covered(date(2026, 1, 1)) is True
    # 缓存命中不改变结果
    assert tl.covered(date(2026, 1, 1)) is True
    assert PoolTimeline({}).covered(date(2026, 1, 1)) is False


def test_codes_on_and_digest_changes_with_intervals():
    rows = [
        {"stock_code": "600000.SH", "effective_from": date(2026, 1, 1),
         "effective_to": date(2026, 3, 1)},
        {"stock_code": "000001.SZ", "effective_from": date(2026, 2, 1),
         "effective_to": None},
    ]
    tl = build_pool_timeline(rows)
    assert tl.codes == ("000001.SZ", "600000.SH")
    assert tl.codes_on(date(2026, 1, 15)) == ("600000.SH",)
    assert tl.codes_on(date(2026, 2, 15)) == ("000001.SZ", "600000.SH")
    d1 = tl.digest()
    # 任何区间变动都改摘要（增量指纹依赖它判废接缝）
    rows2 = [dict(rows[0], effective_to=date(2026, 3, 2)), rows[1]]
    assert build_pool_timeline(rows2).digest() != d1


def test_build_skips_bad_rows():
    rows = [
        {"stock_code": "", "effective_from": date(2026, 1, 1)},
        {"stock_code": "600000.SH", "effective_from": None},
        {"stock_code": "600000.SH", "effective_from": date(2026, 3, 1),
         "effective_to": date(2026, 2, 1)},  # end<start 丢弃
    ]
    assert build_pool_timeline(rows).is_empty is True
    assert POOL_TIMELINE_VERSION == "manual_memberships_v1"


# --------------------------------------------------------------------------- 闸门
def _portfolio(pit_timeline=None):
    pm = PortfolioRiskManager(
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"))
    port = Portfolio(
        portfolio_id=1, initial_capital=Decimal("100000"), risk_manager=pm)
    ctx = StrategyContext(
        strategy_id=1, period="1d",
        capital_ratio=Decimal("0.6"), max_positions=5)
    ctx.strategy_risk = StrategyRiskManager(
        stop_loss_ratio=Decimal("0.1"), take_profit_ratio=Decimal("0.2"),
        trailing_stop_ratio=Decimal("0"))
    port.strategies.append(ctx)
    port.pool_timeline = pit_timeline
    port.pool_pit_enabled = pit_timeline is not None
    rec = DecisionRecorder()
    port.recorder = rec
    return port, ctx, rec


def _bar(day):
    return BarEvent(
        stocks={CODE: {
            "open": Decimal("10"), "high": Decimal("10.6"),
            "low": Decimal("9.4"), "close": Decimal("10.5"), "volume": 1000,
        }},
        bar_time=datetime(day.year, day.month, day.day, 15, 0))


def _sig(stype, day):
    return SignalEvent(
        strategy_id=1, stock_code=CODE, signal_name="sig",
        signal_type=stype,
        bar_time=datetime(day.year, day.month, day.day, 15, 0))


def _hold(ctx):
    pos = Position(CODE)
    pos.apply_trade(TradeEvent(
        strategy_id=1, portfolio_id=1, stock_code=CODE,
        trade_type=TradeType.BUY, price=Decimal("10"), quantity=1000,
        amount=Decimal("10000"), commission=Decimal("0"),
        stamp_duty=Decimal("0"), trade_time=datetime(2026, 1, 2, 9, 30),
    ))
    ctx.positions[CODE] = pos


def test_gate_blocks_open_out_of_pool():
    # 另一只票的区间覆盖了整个区间 → covered=True，故 CODE 出池会真正被拦
    tl = PoolTimeline({
        CODE: [(date(2026, 1, 1), date(2026, 1, 31))],
        "000001.SZ": [(date(2026, 1, 1), None)],
    })
    port, ctx, rec = _portfolio(tl)
    day = date(2026, 2, 3)
    assert port._signal_to_order(ctx, _sig(SignalType.OPEN, day), _bar(day)) == []
    gates = [e for e in rec.drain() if e.gate == "pool_pit_out_of_pool"]
    assert gates and gates[0].action == "block"


def test_gate_allows_in_pool_open_and_add():
    tl = PoolTimeline({CODE: [(date(2026, 1, 1), None)]})
    port, ctx, _ = _portfolio(tl)
    ctx.add_position_threshold = Decimal("-1")  # 跳过回撤检查，有 ADD 即加
    day = date(2026, 2, 3)
    assert port._signal_to_order(ctx, _sig(SignalType.OPEN, day), _bar(day))
    _hold(ctx)
    assert port._signal_to_order(ctx, _sig(SignalType.ADD, day), _bar(day))


def test_gate_never_blocks_sell_side():
    """CLOSE/REDUCE 与风控卖出不受时点池限制（只拦开仓/加仓）。"""
    tl = PoolTimeline({
        CODE: [(date(2026, 1, 1), date(2026, 1, 31))],
        "000001.SZ": [(date(2026, 1, 1), None)],
    })
    port, ctx, _ = _portfolio(tl)
    _hold(ctx)
    day = date(2026, 2, 3)  # 已出池
    assert port._signal_to_order(ctx, _sig(SignalType.CLOSE, day), _bar(day))
    assert port._signal_to_order(ctx, _sig(SignalType.REDUCE, day), _bar(day))


def test_gate_fail_open_when_day_uncovered():
    """历史没录成分的日子：放行，并每日一条 notice（绝不静默变零交易）。"""
    tl = PoolTimeline({CODE: [(date(2026, 5, 1), None)]})
    port, ctx, rec = _portfolio(tl)
    day = date(2026, 2, 3)  # 无任何区间覆盖
    orders = port._signal_to_order(ctx, _sig(SignalType.OPEN, day), _bar(day))
    assert orders, "未覆盖日必须 fail-open 放行"
    first = [e for e in rec.drain() if e.gate == "pool_pit_uncovered"]
    assert len(first) == 1
    # 同日第二条信号不再重复告警
    port._signal_to_order(ctx, _sig(SignalType.OPEN, day), _bar(day))
    assert [e for e in rec.drain() if e.gate == "pool_pit_uncovered"] == []


def test_gate_disabled_when_no_timeline():
    """默认关闭：pool_timeline=None → 闸门第一行返回，行为与改动前一致。"""
    port, ctx, rec = _portfolio(None)
    day = date(2026, 2, 3)
    assert port._signal_to_order(ctx, _sig(SignalType.OPEN, day), _bar(day))
    assert not [e for e in rec.drain() if str(e.gate).startswith("pool_pit")]
