"""账户级聚合风控（D1/D2）单元测试。

用最小 fake portfolio（只暴露 portfolio_id + risk_equity）驱动
AccountRiskService，不构造完整 LiveEngine；notify 由 conftest 强制关闭。
"""
from datetime import datetime
from decimal import Decimal

import pytest

from core.engine.live.account_risk import (
    AccountRiskService,
    DEFAULT_DAILY_LOSS_LIMIT,
    DEFAULT_MAX_DRAWDOWN,
)
from core.engine.live.context import EngineContext


class _FakePortfolio:
    def __init__(self, portfolio_id: int, value):
        self.portfolio_id = portfolio_id
        self.value = Decimal(str(value))

    def risk_equity(self):
        return self.value


def _make(value, *, enabled=True, max_drawdown="0.10",
          daily_loss_limit="0.05", state=None, extra_values=()):
    ports = [_FakePortfolio(1, value)]
    for idx, extra in enumerate(extra_values, start=2):
        ports.append(_FakePortfolio(idx, extra))
    ctx = EngineContext(
        session_id=9, portfolios=ports, dispatcher=None,
        db_session_factory=lambda: (_ for _ in ()).throw(RuntimeError("no db")),
    )
    events = []
    svc = AccountRiskService(ctx, lambda kind, payload: events.append(
        (kind, payload)), {
        "enabled": enabled,
        "max_drawdown": max_drawdown,
        "daily_loss_limit": daily_loss_limit,
        "state": state,
    })
    return svc, ports, events


def _bar(hour, day=18):
    return datetime(2026, 11, day, hour, 0)


def test_disabled_service_is_noop():
    svc, ports, events = _make(100, enabled=False)
    svc.on_bar(_bar(10))
    assert svc.halted is False
    assert events == []
    assert svc.peak_value == Decimal("0")


def test_defaults_when_threshold_missing():
    svc, _, _ = _make(100, max_drawdown=None, daily_loss_limit=None)
    assert svc.max_drawdown == DEFAULT_MAX_DRAWDOWN
    assert svc.daily_loss_limit == DEFAULT_DAILY_LOSS_LIMIT


def test_warn_levels_fire_once_per_day():
    svc, ports, events = _make(100, max_drawdown="0.10")
    svc.on_bar(_bar(10))  # peak=100
    ports[0].value = Decimal("94")  # dd=6% → 60% 分档
    svc.on_bar(_bar(11))
    ports[0].value = Decimal("95")  # 仍 5% → 不新增
    svc.on_bar(_bar(12))
    ports[0].value = Decimal("92")  # dd=8% → 80% 分档
    svc.on_bar(_bar(13))
    levels = [p["level"] for k, p in events if p.get("rule") == "account_max_drawdown"]
    assert levels == ["60", "80"]
    assert svc.breaker_active is False


def test_breaker_triggers_and_halts():
    svc, ports, events = _make(100, max_drawdown="0.10")
    svc.on_bar(_bar(10))
    ports[0].value = Decimal("89")  # dd=11% ≥ 10%
    svc.on_bar(_bar(11))
    assert svc.breaker_active is True
    assert svc.halted is True
    assert svc.triggers == 1
    levels = [p["level"] for k, p in events if p.get("rule") == "account_max_drawdown"]
    assert "100" in levels


def test_breaker_recovers_next_trading_day():
    svc, ports, _ = _make(100, max_drawdown="0.10")
    svc.on_bar(_bar(10))
    ports[0].value = Decimal("89")
    svc.on_bar(_bar(11))
    assert svc.halted is True
    ports[0].value = Decimal("100")
    svc.on_bar(_bar(10, day=19))
    assert svc.breaker_active is False
    assert svc.halted is False


def test_manual_recovery_after_three_triggers():
    svc, ports, _ = _make(100, max_drawdown="0.10")
    for day in (18, 19, 20):
        ports[0].value = Decimal("100")
        svc.on_bar(_bar(10, day=day))
        ports[0].value = Decimal("89")
        svc.on_bar(_bar(11, day=day))
    assert svc.triggers >= 3
    assert svc.manual_recovery is True
    ports[0].value = Decimal("100")
    svc.on_bar(_bar(10, day=21))
    # 手动恢复后不再次日自动解除
    assert svc.breaker_active is True
    svc.recover(reason="人工确认")
    assert svc.breaker_active is False
    assert svc.manual_recovery is False
    assert svc.triggers == 0


def test_daily_loss_pauses_then_resets_next_day():
    svc, ports, events = _make(100, max_drawdown="0.50",
                               daily_loss_limit="0.05")
    svc.on_bar(_bar(10, day=18))
    svc.on_bar(_bar(14, day=18))  # 昨收 = 100
    ports[0].value = Decimal("94")  # -6% ≥ 5%
    svc.on_bar(_bar(10, day=19))
    assert svc.daily_pause_active is True
    assert svc.halted is True
    kinds = [p["rule"] for k, p in events]
    assert "account_daily_loss" in kinds
    ports[0].value = Decimal("100")
    svc.on_bar(_bar(10, day=20))
    assert svc.daily_pause_active is False
    assert svc.halted is False


def test_state_payload_roundtrip():
    svc, ports, _ = _make(100, max_drawdown="0.10")
    svc.on_bar(_bar(10))
    ports[0].value = Decimal("89")
    svc.on_bar(_bar(11))
    raw = svc.state_payload()

    restored, _, _ = _make(100, max_drawdown="0.10", state=raw)
    assert restored.peak_value == svc.peak_value
    assert restored.breaker_active is True
    assert restored.triggers == svc.triggers
    assert restored.peak_value == Decimal("100")


def test_multi_portfolio_aggregation():
    svc, ports, _ = _make(60, max_drawdown="0.10",
                          extra_values=[Decimal("40")])
    svc.on_bar(_bar(10))
    assert svc.peak_value == Decimal("100")
    ports[0].value = Decimal("50")
    ports[1].value = Decimal("39")  # 合计 89 → dd 11% 触发
    svc.on_bar(_bar(11))
    assert svc.breaker_active is True
