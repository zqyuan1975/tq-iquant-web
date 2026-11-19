"""实盘成交量双层闸门 + 当日残量补单队列（2026-11-19 口径统一）。

覆盖用户验收要求：
- 缩量顺延：日线/分钟取小缩量，残量入当日队列，不丢弃；
- 盘前兜底：无已完成 1m bar 时按日线口径放行并落 daily_fallback 事件；
- 放弃条件：TTL（14:55）、价格偏离阈值、无进展；
- 达成率：残量 chunk 的 target_quantity 与累计成交可对照。
"""
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.engine import live_engine as live_mod
from core.engine.event import OrderEvent
from core.engine.live_engine import LiveEngine
from core.models import Base, LiveOrder
from tq_iquant_shared.constants import SignalType, TradeType


class _Synth:
    """快照合成器桩：按 code 给「最近完成 1m bar」。volume 单位=股（与合成器出口一致）。"""

    def __init__(self):
        self.bars = {}

    def completed_bars(self, codes, n):
        return {c: list(self.bars.get(c, []))[-n:] for c in codes}


def _db():
    db = sessionmaker(bind=create_engine("sqlite:///:memory:"))()
    Base.metadata.create_all(db.get_bind())
    return db


def _engine(monkeypatch, now=datetime(2026, 11, 19, 10, 0)):
    """只装配被测方法需要的属性（避免真实引擎的线程/轮询初始化）。"""
    engine = object.__new__(LiveEngine)
    engine.ctx = SimpleNamespace(session_id=1)
    engine.session_id = 1
    engine._recorder = live_mod.DecisionRecorder()
    engine._volume_residuals = {}
    engine._daily_volume_ref_cache = {}
    engine._snapshot_synth = _Synth()
    engine._engine = SimpleNamespace(_lot=lambda code: 100)
    engine.volume_residual_max_dev_pct = 0.02
    engine.volume_residual_max_attempts = 20
    monkeypatch.setattr(live_mod, "now_shanghai", lambda: now)
    return engine


def _portfolio(daily_ratio="0.05", minute_ratio="0.10"):
    return SimpleNamespace(
        portfolio_id=1,
        volume_limit_enabled=True,
        volume_limit_ratio=Decimal(daily_ratio),
        minute_volume_limit_ratio=(
            None if minute_ratio is None else Decimal(minute_ratio)),
    )


def _order(qty=30000, price="10.00", trade_type=TradeType.BUY,
           bar_time=datetime(2026, 11, 19, 9, 31)):
    return OrderEvent(
        strategy_id=3, portfolio_id=1, stock_code="600000.SH",
        trade_type=trade_type, signal_type=SignalType.OPEN,
        quantity=qty, price=Decimal(price), bar_time=bar_time,
        signal_name="open_sig", period="1m",
    )


def _events(engine):
    return [e for e in engine._recorder.drain()]


# ---------------- 缩量顺延 ----------------
def test_shrink_takes_min_of_daily_and_minute_and_enqueues_residual(monkeypatch):
    engine = _engine(monkeypatch)
    engine._daily_reference_volume = lambda code, d: 1_000_000  # 日线 5% → 50,000
    engine._snapshot_synth.bars["600000.SH"] = [
        {"stime": "20261119093100", "volume": 100000, "close": 10.0},  # 100,000 股（=1000 手）
    ]  # 分钟 10% → 10,000
    order = _order(qty=30000)
    engine._apply_volume_cap(order, _portfolio())

    assert order.quantity == 10000  # 取两层较小值（分钟层）
    assert len(engine._volume_residuals) == 1
    entry = next(iter(engine._volume_residuals.values()))
    assert entry["target_qty"] == 20000
    assert entry["ref_price"] == Decimal("10.00")

    evs = _events(engine)
    shrink = [e for e in evs if e.gate == "volume_cap_shrink"]
    assert len(shrink) == 1
    assert shrink[0].requested_qty == 30000 and shrink[0].final_qty == 10000


def test_daily_cap_binds_when_tighter_than_minute(monkeypatch):
    engine = _engine(monkeypatch)
    engine._daily_reference_volume = lambda code, d: 100_000  # 5% → 5,000
    engine._snapshot_synth.bars["600000.SH"] = [
        {"stime": "20261119093100", "volume": 100000, "close": 10.0},
    ]
    order = _order(qty=30000)
    engine._apply_volume_cap(order, _portfolio())

    assert order.quantity == 5000  # BUY 向下取整到 100 股整数倍
    entry = next(iter(engine._volume_residuals.values()))
    assert entry["target_qty"] == 25000


def test_minute_ratio_zero_disables_minute_gate(monkeypatch):
    """0 = 关闭分钟闸门（只按日线口径）；NULL = 回落日线占比。"""
    engine = _engine(monkeypatch)
    engine._daily_reference_volume = lambda code, d: 1_000_000  # 日线 5% → 50,000
    engine._snapshot_synth.bars["600000.SH"] = [
        {"stime": "20261119093100", "volume": 10000, "close": 10.0},  # 分钟层仅 10,000 股
    ]
    order = _order(qty=30000)
    engine._apply_volume_cap(order, _portfolio(minute_ratio="0"))
    assert order.quantity == 30000  # 分钟闸门关闭，不缩量
    assert not engine._volume_residuals


def test_minute_ratio_null_falls_back_to_daily_ratio_not_ten_pct(monkeypatch):
    """NULL 未配置 → 分钟比例取日线占比（4%），不得硬编码 10%。

    bar 100,000 股（=1000 手）：4% → 4,000；若误用 10% 会是 10,000。
    """
    engine = _engine(monkeypatch)
    engine._daily_reference_volume = lambda code, d: 1_000_000  # 日线 4% → 40,000
    engine._snapshot_synth.bars["600000.SH"] = [
        {"stime": "20261119093100", "volume": 100000, "close": 10.0},
    ]
    order = _order(qty=30000)
    engine._apply_volume_cap(order, _portfolio(daily_ratio="0.04", minute_ratio=None))

    assert order.quantity == 4000  # 分钟 4%，非 10%
    entry = next(iter(engine._volume_residuals.values()))
    assert entry["target_qty"] == 26000


# ---------------- 盘前兜底 ----------------
def test_pre_open_without_bar_falls_back_to_daily_and_records(monkeypatch):
    engine = _engine(monkeypatch, now=datetime(2026, 11, 19, 9, 15))
    engine._daily_reference_volume = lambda code, d: 1_000_000  # 5% → 50,000
    order = _order(qty=30000)  # 无快照 bar → 盘前
    engine._apply_volume_cap(order, _portfolio())

    assert order.quantity == 30000  # 日线口径上限 50,000，目标 30,000 放行
    fallback = [e for e in _events(engine) if e.action == "daily_fallback"]
    assert len(fallback) == 1
    assert fallback[0].final_qty == 30000


def test_pre_open_target_over_daily_cap_shrinks_by_daily(monkeypatch):
    engine = _engine(monkeypatch, now=datetime(2026, 11, 19, 9, 15))
    engine._daily_reference_volume = lambda code, d: 200_000  # 5% → 10,000
    order = _order(qty=30000)
    engine._apply_volume_cap(order, _portfolio())

    assert order.quantity == 10000
    assert next(iter(engine._volume_residuals.values()))["target_qty"] == 20000
    assert [e for e in _events(engine) if e.action == "daily_fallback"]


def test_no_reference_at_all_fails_open(monkeypatch):
    engine = _engine(monkeypatch)
    engine._daily_reference_volume = lambda code, d: None
    # 无快照 bar、无日线基准 → fail-open 放行，绝不回退当日量
    order = _order(qty=30000)
    engine._apply_volume_cap(order, _portfolio())

    assert order.quantity == 30000
    assert not engine._volume_residuals
    assert [e for e in _events(engine) if e.action == "fail_open"]


# ---------------- 放弃条件 ----------------
def _entry(engine, target=20000, ref_price="10.00"):
    key = "vr-test"
    engine._register_volume_residual(_portfolio(), _order(qty=20000), target)
    # 用可控 key 替换随机 key，便于直接推进
    entry = next(iter(engine._volume_residuals.values()))
    engine._volume_residuals.clear()
    engine._volume_residuals[key] = entry
    entry["ref_price"] = Decimal(ref_price)
    return key, entry


def test_abandon_on_ttl(monkeypatch):
    engine = _engine(monkeypatch, now=datetime(2026, 11, 19, 10, 0))
    key, entry = _entry(engine)  # 上午入队成功
    # 推进到 14:56：已过当日顺延截止（14:55）→ 放弃剩余并留痕
    monkeypatch.setattr(
        live_mod, "now_shanghai", lambda: datetime(2026, 11, 19, 14, 56))
    db = _db()
    engine._advance_volume_residual(db, key, entry)

    assert key not in engine._volume_residuals
    evs = [e for e in _events(engine) if e.gate == "volume_residual"]
    assert evs and evs[-1].action == "abandon" and evs[-1].requested_qty == 20000
    db.close()


def test_abandon_on_price_deviation(monkeypatch):
    engine = _engine(monkeypatch, now=datetime(2026, 11, 19, 10, 0))
    key, entry = _entry(engine, ref_price="10.00")
    entry["last_bar_key"] = "20261119093000"
    engine._snapshot_synth.bars["600000.SH"] = [
        {"stime": "20261119100000", "volume": 100000, "close": 10.30},  # +3% > 2%
    ]
    db = _db()
    engine._advance_volume_residual(db, key, entry)

    assert key not in engine._volume_residuals
    evs = [e for e in _events(engine) if e.gate == "volume_residual"]
    assert evs[-1].action == "abandon"
    assert "偏离" in (evs[-1].message or "")
    db.close()


def test_no_progress_abandons_after_max_attempts(monkeypatch):
    engine = _engine(monkeypatch, now=datetime(2026, 11, 19, 10, 0))
    engine.volume_residual_max_attempts = 2
    key, entry = _entry(engine)
    entry["attempts"] = 1
    # bar 量×10% 不足一手 → 可下量 0，且每根新 bar 计一次无进展
    engine._snapshot_synth.bars["600000.SH"] = [
        {"stime": "20261119100000", "volume": 100, "close": 10.0},
    ]
    db = _db()
    engine._advance_volume_residual(db, key, entry)

    assert key not in engine._volume_residuals
    evs = [e for e in _events(engine) if e.gate == "volume_residual"]
    assert evs[-1].action == "abandon"
    assert "可下量为 0" in (evs[-1].message or "")
    db.close()


# ---------------- 顺延推进（每根新 bar 一次 + 达成率） ----------------
def test_dispatch_once_per_bar_and_track_fills(monkeypatch):
    engine = _engine(monkeypatch, now=datetime(2026, 11, 19, 10, 0))
    engine._daily_reference_volume = lambda code, d: 10_000_000  # 日线不约束
    key, entry = _entry(engine, target=20000)
    entry["last_bar_key"] = "20261119095500"
    engine._snapshot_synth.bars["600000.SH"] = [
        {"stime": "20261119100000", "volume": 100000, "close": 10.0},  # 分钟 10% → 10,000
    ]
    calls = []

    def fake_submit(db, portfolio, order, period=None, emit_signal=True):
        calls.append(order)
        db.add(LiveOrder(
            live_session_id=1, portfolio_strategy_id=0, strategy_id=3,
            stock_code=order.stock_code, trade_type=order.trade_type.value,
            order_type="LIMIT", price=order.price, quantity=order.quantity,
            status="submitted", bar_time=order.bar_time,
            split_group=getattr(order, "_split_group", None),
            filled_quantity=0,
        ))
        db.commit()

    engine._submit_one_order = fake_submit
    db = _db()

    engine._advance_volume_residual(db, key, entry)
    assert len(calls) == 1
    assert calls[0].quantity == 10000
    # 达成率：chunk 目标 = 自身提交量
    assert calls[0].target_quantity == 10000

    # 同一根 bar 的 5s tick 不重复发单
    engine._advance_volume_residual(db, key, entry)
    assert len(calls) == 1

    # 该 chunk 全部成交 → 下一根 bar 续发剩余 10,000
    row = engine._find_volume_chunk_row(db, key)
    row.status = "filled"
    row.filled_quantity = 10000
    db.commit()
    engine._snapshot_synth.bars["600000.SH"].append(
        {"stime": "20261119100100", "volume": 100000, "close": 10.0})
    engine._advance_volume_residual(db, key, entry)

    assert len(calls) == 2
    assert entry["filled_qty"] == 10000
    assert calls[1].quantity == 10000

    # 第二笔也成交 → 队列完成并留痕（累计成交 = 目标）
    row2 = engine._find_volume_chunk_row(db, key)
    row2.status = "filled"
    row2.filled_quantity = 10000
    db.commit()
    engine._snapshot_synth.bars["600000.SH"].append(
        {"stime": "20261119100200", "volume": 100000, "close": 10.0})
    engine._advance_volume_residual(db, key, entry)

    assert key not in engine._volume_residuals
    done = [e for e in _events(engine)
            if e.gate == "volume_residual" and e.action == "complete"]
    assert done and done[-1].requested_qty == 20000 and done[-1].final_qty == 20000
    db.close()


def test_residual_chunk_is_not_resplit(monkeypatch):
    """残量 chunk 跳过拆单与让价，避免两套队列互相等待。"""
    engine = _engine(monkeypatch)
    order = _order(qty=5000)
    setattr(order, "_volume_residual_chunk", True)
    # _apply_volume_cap 对残量直接放行
    engine._apply_volume_cap(order, _portfolio())
    assert order.quantity == 5000
    assert not engine._volume_residuals


def test_requote_is_not_recapped(monkeypatch):
    """追价重下不再缩量（首报已过闸，重缩会重复登记残量）。"""
    engine = _engine(monkeypatch)
    order = _order(qty=5000)
    setattr(order, "_requote", True)
    engine._apply_volume_cap(order, _portfolio())
    assert order.quantity == 5000
    assert not engine._volume_residuals


def test_cap_regression_133_hand_bar_yields_532_shares(monkeypatch):
    """量纲回归（对齐 2026-11-18 09:31 301600 实盘事件）：13,300 股 ×4% → 532 股。

    修复前：合成 bar 出口=手（133），闸门再 ×100 得 13,300 股；
    修复后：出口已统一为股（13,300），闸门不再 ×100。
    最终上限必须仍逐位等于 532——既不能是 5.32（少乘一次），也不能是 53,200（多乘一次）。
    """
    engine = _engine(monkeypatch)
    engine._daily_reference_volume = lambda code, d: None
    engine._snapshot_synth.bars["600000.SH"] = [
        {"stime": "20261119093100", "volume": 13300, "close": 10.0},
    ]
    order = _order(qty=700, trade_type=TradeType.SELL)
    engine._apply_volume_cap(
        order, _portfolio(daily_ratio="0.04", minute_ratio="0.04"))

    assert order.quantity == 532
    assert next(iter(engine._volume_residuals.values()))["target_qty"] == 168
