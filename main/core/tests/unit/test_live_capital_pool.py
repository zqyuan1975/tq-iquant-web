"""A1 资金池模式与日再平衡单元测试。

覆盖：
- ``compute_capital_pool_weights``：equal / risk_budget / priority / 回退
- ``LiveEngine._apply_capital_pool``：按权重重分虚拟现金 + 写 capital_pool 决策事件
- ``LiveEngine._maybe_apply_capital_pool``：off 不触发、同日去重、再平衡开关
"""
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from core.engine.account import Account
from core.engine.cash_allocator import compute_capital_pool_weights
from core.engine.live_engine import LiveEngine


def _port(pid, cash, priority=0, max_drawdown=None):
    return SimpleNamespace(
        portfolio_id=pid,
        priority=priority,
        risk_manager=SimpleNamespace(max_drawdown=max_drawdown),
        account=Account(Decimal(str(cash))),
    )


# ---------------------------------------------------------------------------
# 权重函数
# ---------------------------------------------------------------------------

def test_weights_equal_and_empty():
    ports = [_port(i, 10000) for i in (1, 2, 3)]
    assert compute_capital_pool_weights(ports, "equal") == [1 / 3] * 3
    assert compute_capital_pool_weights([], "equal") == []


def test_weights_risk_budget_inverse_drawdown():
    ports = [_port(1, 10000, max_drawdown="0.1"),
             _port(2, 10000, max_drawdown="0.2")]
    w = compute_capital_pool_weights(ports, "risk_budget")
    assert w[0] == pytest.approx(2 / 3)
    assert w[1] == pytest.approx(1 / 3)


def test_weights_risk_budget_missing_drawdown_defaults():
    # 缺失 / 非正回撤按 0.15 兜底：0.15 vs 0.3 的权重比应为 2:1
    ports = [_port(1, 10000, max_drawdown=None),
             _port(2, 10000, max_drawdown="0.3")]
    w = compute_capital_pool_weights(ports, "risk_budget")
    assert w[0] == pytest.approx(2 / 3)
    assert w[1] == pytest.approx(1 / 3)
    # 0 / 非法值同样兜底 0.15
    ports = [_port(1, 10000, max_drawdown="0"),
             _port(2, 10000, max_drawdown="bad")]
    assert compute_capital_pool_weights(ports, "risk_budget") == [
        pytest.approx(0.5), pytest.approx(0.5)]


def test_weights_priority_and_fallback():
    assert compute_capital_pool_weights(
        [_port(1, 0, priority=1), _port(2, 0, priority=3)], "priority") == [0.25, 0.75]
    # 全 0 / 负数 → 回退等权（负数按 0，全 0 时等权）
    assert compute_capital_pool_weights(
        [_port(1, 0, priority=0), _port(2, 0, priority=0)], "priority") == [0.5, 0.5]
    # 非法值 / 未开启（off）→ 等权兜底，不抛错
    assert compute_capital_pool_weights(
        [_port(1, 0, priority="bad"), _port(2, 0, priority=None)], "priority") == [0.5, 0.5]
    assert compute_capital_pool_weights(
        [_port(1, 0), _port(2, 0)], "off") == [0.5, 0.5]


# ---------------------------------------------------------------------------
# 现金再分配
# ---------------------------------------------------------------------------

class _FakeRecorder:
    def __init__(self):
        self.events = []

    def record(self, **kwargs):
        self.events.append(kwargs)


class _FakeDB:
    def __init__(self):
        self.commits = 0
        self.closed = False

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


def _engine(ports, mode="equal"):
    db = _FakeDB()
    recorder = _FakeRecorder()
    engine = SimpleNamespace(
        portfolios=ports,
        capital_pool_mode=mode,
        _capital_pool_rebalance=False,
        _capital_pool_day=None,
        _recorder=recorder,
        _db_session_factory=lambda: db,
        _drain_decisions=lambda _db, _session: None,
    )
    return engine, db, recorder


def test_apply_capital_pool_redistributes_cash_and_records():
    ports = [_port(1, 100000), _port(2, 0)]
    engine, db, recorder = _engine(ports)
    LiveEngine._apply_capital_pool(engine, datetime(2026, 11, 18, 9, 31))
    assert [p.account.cash for p in ports] == [Decimal("50000.00")] * 2
    assert engine._capital_pool_day == date(2026, 11, 18)
    assert db.commits == 1 and db.closed is True
    assert len(recorder.events) == 2
    ev = recorder.events[0]
    assert ev["gate"] == "capital_pool"
    assert ev["layer"] == "portfolio_capital"
    assert ev["action"] == "rebalance"
    assert ev["param_name"] == "weight"
    assert ev["param_value"] == 0.5
    assert ev["actual_value"] == 50000.0


def test_apply_capital_pool_skips_accountless_portfolio():
    ports = [_port(1, 80000), _port(2, 20000),
             SimpleNamespace(portfolio_id=3, priority=0, account=None)]
    engine, _db, recorder = _engine(ports)
    LiveEngine._apply_capital_pool(engine, datetime(2026, 11, 18, 9, 31))
    assert [p.account.cash for p in ports[:2]] == [Decimal("50000.00")] * 2
    assert len(recorder.events) == 2


# ---------------------------------------------------------------------------
# 触发条件：off / 同日去重 / 再平衡开关
# ---------------------------------------------------------------------------

def test_maybe_apply_capital_pool_off_does_nothing():
    calls = []
    engine = SimpleNamespace(
        portfolios=[_port(1, 1000)],
        capital_pool_mode="off",
        _capital_pool_rebalance=True,
        _capital_pool_day=None,
        _apply_capital_pool=lambda bar_time: calls.append(bar_time),
    )
    LiveEngine._maybe_apply_capital_pool(engine, datetime(2026, 11, 18, 9, 31))
    assert calls == []


def _recording_engine(mode="equal", rebalance=False):
    calls = []
    engine = SimpleNamespace(
        portfolios=[_port(1, 1000)],
        capital_pool_mode=mode,
        _capital_pool_rebalance=rebalance,
        _capital_pool_day=None,
    )

    def _apply(bar_time):
        calls.append(bar_time)
        engine._capital_pool_day = bar_time.date()

    engine._apply_capital_pool = _apply
    return engine, calls


def test_maybe_apply_capital_pool_once_per_day_without_rebalance():
    engine, calls = _recording_engine(rebalance=False)
    LiveEngine._maybe_apply_capital_pool(engine, datetime(2026, 11, 18, 9, 31))
    LiveEngine._maybe_apply_capital_pool(engine, datetime(2026, 11, 18, 9, 32))
    LiveEngine._maybe_apply_capital_pool(engine, datetime(2026, 11, 19, 9, 31))
    assert [c.day for c in calls] == [18]


def test_maybe_apply_capital_pool_rebalances_each_day_when_enabled():
    engine, calls = _recording_engine(rebalance=True)
    LiveEngine._maybe_apply_capital_pool(engine, datetime(2026, 11, 18, 9, 31))
    LiveEngine._maybe_apply_capital_pool(engine, datetime(2026, 11, 18, 9, 32))
    LiveEngine._maybe_apply_capital_pool(engine, datetime(2026, 11, 19, 9, 31))
    assert [c.day for c in calls] == [18, 19]
