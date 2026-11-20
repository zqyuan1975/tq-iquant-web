"""G1 资金强制单元测试（0021）。

覆盖：
- ``build_enforce_config``：折扣后本金、按 max_share 分配、跳过缺席借款方、
  贷出方缺失/本金非法直接作废、恒等式（LoanBook.check_invariants 为空）
- ``portfolio_delta``：借入 + / 贷出 −
- ``release_all``：kill switch 释放后净额归零且不再新增
- ``LiveEngine._buy_available_cash`` / ``_capital_share``：开关关闭零变化
"""
from datetime import datetime
from decimal import Decimal
from types import MethodType, SimpleNamespace

from core.engine.account import Account
from core.engine.capital_enforce import (
    build_enforce_config,
    portfolio_delta,
    release_all,
    snapshot,
    strategy_delta,
)
from core.engine.live_engine import LiveEngine

AT = datetime(2026, 11, 20, 9, 31)


def _plan(**over):
    plan = {
        "enabled": True,
        "plan_id": 7,
        "lender_strategy_id": 11,
        "borrower_strategy_id": 21,
        "principal": "100000",
        "due_days": 3,
        "borrowers": [
            {"portfolio_strategy_id": 2, "strategy_id": 21,
             "max_share": "0.6"},
            {"portfolio_strategy_id": 3, "strategy_id": 31,
             "max_share": "0.4"},
        ],
    }
    plan.update(over)
    return plan


def _haircut(composite="0.9"):
    return {"composite": composite}


MAPPING = {11: 1, 21: 2, 31: 3}


def _build(**over):
    return build_enforce_config(
        loan_plan=_plan(**over), haircut=_haircut(),
        strategy_portfolio=MAPPING, at=AT)


def _port(pid, cash):
    return SimpleNamespace(
        portfolio_id=pid,
        account=Account(Decimal(str(cash))),
    )


def _engine(cfg, enabled, ports):
    engine = SimpleNamespace(
        session_id=1,
        portfolios=ports,
        _capital_enforce=cfg,
        _capital_enforce_enabled=enabled,
        _capital_enforce_checked_at=0.0,
        _capital_reconcile_at=0.0,
        _capital_reconcile_block="",
        _capital_health_blocked=False,
        _reserved_buy_cash=lambda pid: Decimal("0"),
        _latest_account_cash=None,
        _account_total_capital=None,
        _conservative_reserve_logged=False,
        _cash_share_scale_days={},
        ctx=SimpleNamespace(clock=lambda: datetime(2026, 11, 20, 10, 0)),
        _emit=lambda *args, **kwargs: None,
    )
    for name in ("_capital_enforce_net", "_effective_capital",
                 "_capital_share", "_buy_available_cash",
                 "_set_capital_enforce_enabled",
                 "set_capital_enforce_enabled"):
        setattr(engine, name, MethodType(getattr(LiveEngine, name), engine))
    return engine


# ---------------------------------------------------------------------------
# 额度构建
# ---------------------------------------------------------------------------

def test_build_applies_haircut_and_shares():
    cfg = _build()
    assert cfg["usable_principal"] == Decimal("90000.00")
    assert cfg["lent_total"] == Decimal("90000.00")
    assert portfolio_delta(cfg, 2) == Decimal("54000.00")
    assert portfolio_delta(cfg, 3) == Decimal("36000.00")
    assert portfolio_delta(cfg, 1) == Decimal("-90000.00")
    assert portfolio_delta(cfg, 99) == Decimal("0")
    assert strategy_delta(cfg, 21) == Decimal("54000.00")
    assert strategy_delta(cfg, 11) == Decimal("-90000.00")
    # 借据恒等式：账本自检为空 + 借出=未归还
    assert cfg["book"].check_invariants() == []
    assert cfg["book"].total_outstanding() == cfg["lent_total"]


def test_build_skips_borrower_outside_session():
    cfg = build_enforce_config(
        loan_plan=_plan(), haircut=_haircut(),
        strategy_portfolio={11: 1, 21: 2}, at=AT)
    assert "enforce_borrower_not_in_session:31" in cfg["warnings"]
    assert portfolio_delta(cfg, 2) == Decimal("90000.00")
    assert portfolio_delta(cfg, 1) == Decimal("-90000.00")
    assert cfg["book"].check_invariants() == []


def test_build_rejects_missing_lender_or_invalid_principal():
    assert build_enforce_config(
        loan_plan=_plan(), haircut=_haircut(),
        strategy_portfolio={21: 2, 31: 3}, at=AT) is None
    assert _build(principal="0") is None
    assert _build(principal=None) is None
    assert build_enforce_config(
        loan_plan=_plan(), haircut=_haircut("0"),
        strategy_portfolio=MAPPING, at=AT) is None


def test_release_all_zeroes_delta_and_never_reopens():
    cfg = _build()
    assert release_all(cfg, datetime(2026, 11, 20, 10, 0)) == 2
    assert cfg["book"].total_outstanding() == Decimal(0)
    assert cfg["book"].check_invariants() == []
    assert portfolio_delta(cfg, 2) == Decimal("0")
    assert portfolio_delta(cfg, 1) == Decimal("0")
    assert snapshot(cfg)["outstanding"] == "0.00"
    # 释放后没有新借据：再次调用不产生额度
    assert release_all(cfg, datetime(2026, 11, 20, 10, 1)) == 0
    assert portfolio_delta(cfg, 2) == Decimal("0")


# ---------------------------------------------------------------------------
# 引擎接入
# ---------------------------------------------------------------------------

def test_buy_available_cash_adds_credit_only_when_enabled():
    cfg = _build()
    ports = [_port(1, 50000), _port(2, 10000), _port(3, 10000)]
    off = _engine(cfg, False, ports)
    on = _engine(cfg, True, ports)
    assert off._buy_available_cash(ports[1]) == Decimal("10000")
    assert on._buy_available_cash(ports[1]) == Decimal("64000.00")
    # 贷出方被扣减到 0（不出现负可用）
    assert on._buy_available_cash(ports[0]) == Decimal("0")


def test_capital_share_uses_effective_capital_and_restores_on_kill_switch():
    cfg = _build()
    ports = [_port(1, 100000), _port(2, 50000), _port(3, 50000)]
    engine = _engine(cfg, True, ports)
    # 借入方 104000 / 200000（贷出方 10000、另一借入方 86000，总额不变）
    assert engine._capital_share(ports[1]) == (
        Decimal("104000") / Decimal("200000"))
    engine.set_capital_enforce_enabled(False)
    assert engine._capital_enforce_enabled is False
    # 关闭后回到原口径：50000 / 200000
    assert engine._capital_share(ports[1]) == Decimal("0.25")
    assert engine._buy_available_cash(ports[1]) == Decimal("50000")
    # 再次开启不会复借（只回收不新增）
    engine.set_capital_enforce_enabled(True)
    assert engine._capital_share(ports[1]) == Decimal("0.25")


def test_health_suspend_blocks_new_lending_without_releasing_lots():
    """G3：健康暂停只让借据净额为 0，不产生释放/新借记。"""
    cfg = _build()
    ports = [_port(1, 50000), _port(2, 10000), _port(3, 10000)]
    engine = _engine(cfg, True, ports)
    assert engine._buy_available_cash(ports[1]) == Decimal("64000.00")
    outstanding = cfg["book"].total_outstanding()

    engine._capital_health_blocked = True
    assert engine._buy_available_cash(ports[1]) == Decimal("10000")
    assert engine._buy_available_cash(ports[0]) == Decimal("50000")
    # 借据仍在，暂停不强制平仓
    assert cfg["book"].total_outstanding() == outstanding
    assert cfg["book"].check_invariants() == []

    engine._capital_health_blocked = False
    assert engine._buy_available_cash(ports[1]) == Decimal("64000.00")
    assert cfg["book"].total_outstanding() == outstanding
