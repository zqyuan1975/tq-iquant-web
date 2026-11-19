"""额度推导（0021 A2/A3）单测：示例值 / 正常推导 / 缺失降级 / 卡位 / 默认关闭。"""
from decimal import Decimal

from core.engine.capital_contracts import (
    PortfolioQuotaInput,
    STATUS_DEGRADED,
    STATUS_DISABLED,
    STATUS_NOT_DERIVABLE,
    STATUS_OK,
    StrategyQuotaInput,
    build_capital_pool_version,
)
from core.engine.capital_quota import (
    derive_portfolio_quota,
    derive_risk_factor,
    derive_strategy_quota,
    portfolio_quota_input_from_orm,
)

D = Decimal


def _strategy(**kw):
    base = dict(strategy_id=1, capital_ratio=D("1"), max_positions=10,
                single_open_ratio=D("0.10"), max_order_amount=None,
                max_order_pct_equity=None)
    base.update(kw)
    return StrategyQuotaInput(**base)


def _portfolio(**kw):
    base = dict(portfolio_id=1, equity=D("38880000"), equity_cap=None,
                target_utilization=D("0.95"), max_holdings=10,
                max_single_position_pct=None, gross_exposure=None,
                strategies=(_strategy(),), risk_factor=None)
    base.update(kw)
    return PortfolioQuotaInput(**base)


# ---------------- A2 验收示例（0019 §1.5）----------------
def test_example_10_slots_500k_is_5m_and_12_9pct():
    """10 槽 × 单笔 50 万 = 500 万，占权益 12.9%。"""
    cfg = _portfolio(strategies=(_strategy(max_order_amount=D("500000")),))
    r = derive_portfolio_quota(cfg, enabled=True)

    assert r.status == STATUS_OK
    assert r.strategies[0].fund == D("38880000")
    assert r.strategies[0].single_position_limit == D("500000")
    assert r.strategies[0].gross_quota == D("5000000")
    assert r.strategies[0].binding == ["max_order_amount"]
    assert r.gross_limit == D("5000000")
    assert r.entitlement == D("5000000")
    assert r.utilization_cap == D("5000000") / D("38880000")  # 12.86%
    assert abs(float(r.utilization_cap) - 0.1286) < 0.0001


def test_example_20_slots_3pct_is_23_33m_and_60pct():
    """20 槽 × 3% = 2333 万，占权益 60%。"""
    cfg = _portfolio(
        strategies=(_strategy(max_positions=20, single_open_ratio=D("0.03")),),
        max_holdings=20,
    )
    r = derive_portfolio_quota(cfg, enabled=True)

    assert r.strategies[0].single_position_limit == D("1166400")
    assert r.strategies[0].gross_quota == D("23328000")
    assert r.gross_limit == D("23328000")
    assert r.entitlement == D("23328000")
    assert r.utilization_cap == D("0.6")
    assert "target_utilization" not in r.binding


def test_example_20_slots_5pct_gross_3888m_but_target_caps_95pct():
    """20 槽 × 5% 毛额度 3888 万（100%），被 95% 目标仓位压到 3693.6 万。"""
    cfg = _portfolio(
        strategies=(_strategy(max_positions=20, single_open_ratio=D("0.05")),),
        max_holdings=20,
    )
    r = derive_portfolio_quota(cfg, enabled=True)

    assert r.strategies[0].gross_quota == D("38880000")
    assert r.gross_limit == D("36936000")
    assert r.entitlement == D("36936000")
    assert r.binding == ["target_utilization"]
    assert r.utilization_cap == D("0.95")


def test_to_dict_is_structured_and_versioned():
    cfg = _portfolio(strategies=(_strategy(max_order_amount=D("500000")),))
    data = derive_portfolio_quota(cfg, enabled=True).to_dict()

    assert data["status"] == STATUS_OK
    assert data["entitlement"] == "5000000"
    assert data["warnings"] == []
    assert data["strategies"][0]["limits"][0]["source"]
    assert data["version"]["quota_algo_version"]


def test_limit_items_carry_source_and_formula():
    cfg = _portfolio(strategies=(_strategy(max_order_amount=D("500000")),))
    r = derive_portfolio_quota(cfg, enabled=True)
    item = next(x for x in r.strategies[0].limits if x.key == "max_order_amount")
    assert item.amount == D("500000")
    assert item.source == "strategies.max_order_amount"
    assert "500000" in item.formula.replace(",", "")


def test_build_version_has_defaults():
    v = build_capital_pool_version()
    assert v["quota_algo_version"]
    assert v["defaults"]["target_utilization"] == "0.95"


# ---------------- 约束相互卡位 ----------------
def test_concentration_cap_binds_tighter_than_order_amount():
    cfg = _portfolio(
        strategies=(_strategy(max_order_amount=D("1000000")),),
        max_single_position_pct=D("0.02"),
    )
    r = derive_portfolio_quota(cfg, enabled=True)
    assert r.strategies[0].single_position_limit == D("777600")
    assert r.strategies[0].binding == ["max_single_position_pct"]


def test_max_order_pct_equity_vs_amount_takes_min():
    cfg = _portfolio(strategies=(_strategy(
        max_order_amount=D("500000"), max_order_pct_equity=D("0.01")),))
    r = derive_portfolio_quota(cfg, enabled=True)
    assert r.strategies[0].single_position_limit == D("388800")
    assert r.strategies[0].binding == ["max_order_pct_equity"]


def test_equity_cap_limits_fund_and_portfolio_gross():
    cfg = _portfolio(
        equity=D("10000000"), equity_cap=D("2000000"),
        target_utilization=D("1"),
        strategies=(_strategy(max_positions=1, single_open_ratio=D("1")),),
        max_holdings=1,
    )
    r = derive_portfolio_quota(cfg, enabled=True)
    assert r.effective_equity == D("2000000")
    assert r.strategies[0].fund == D("2000000")
    assert r.gross_limit == D("2000000")
    assert "equity_cap" in r.binding


def test_headroom_subtracts_exposure_and_floors_at_zero():
    cfg = _portfolio(
        strategies=(_strategy(max_order_amount=D("500000")),),
        gross_exposure=D("6000000"),
    )
    assert derive_portfolio_quota(cfg, enabled=True).headroom == D("0")

    cfg2 = _portfolio(
        strategies=(_strategy(max_order_amount=D("500000")),),
        gross_exposure=D("1000000"),
    )
    assert derive_portfolio_quota(cfg2, enabled=True).headroom == D("4000000")


def test_capital_ratio_normalized_when_sum_not_one():
    cfg = _portfolio(strategies=(
        _strategy(strategy_id=1, capital_ratio=D("3"), max_positions=10),
        _strategy(strategy_id=2, capital_ratio=D("1"), max_positions=10),
    ), max_holdings=20)
    r = derive_portfolio_quota(cfg, enabled=True)

    assert r.strategies[0].fund == D("29160000")
    assert r.strategies[1].fund == D("9720000")
    assert "capital_ratio_normalized" in r.degraded
    assert r.status == STATUS_DEGRADED


# ---------------- A3 缺失降级 ----------------
def test_missing_equity_is_not_derivable():
    r = derive_portfolio_quota(_portfolio(equity=None), enabled=True)
    assert r.status == STATUS_NOT_DERIVABLE
    assert r.entitlement is None
    assert any("权益" in x for x in r.reasons)


def test_missing_fields_degrade_but_never_infinite():
    cfg = _portfolio(
        equity=D("1000000"), max_holdings=None, target_utilization=None,
        strategies=(_strategy(max_positions=None, single_open_ratio=None,
                              max_order_amount=None),),
    )
    r = derive_portfolio_quota(cfg, enabled=True)

    assert r.entitlement == D("950000")  # 1 槽 × 100% 资金，再被 95% 目标压顶
    for key in ("max_positions_defaulted", "single_open_ratio_defaulted",
                "max_holdings_defaulted", "target_utilization_defaulted"):
        assert key in r.degraded


def test_missing_target_utilization_uses_0019_conservative_default():
    """0019 §1.7 取 0.95（比 0021 A3 的 1.0 更保守，按权威声明取 0019）。"""
    cfg = _portfolio(
        target_utilization=None,
        strategies=(_strategy(max_positions=20, single_open_ratio=D("0.05")),),
        max_holdings=20,
    )
    r = derive_portfolio_quota(cfg, enabled=True)
    assert r.entitlement == D("36936000")
    assert "target_utilization_defaulted" in r.degraded
    assert any("0.95" in x for x in r.reasons)


def test_missing_risk_factor_does_not_relax_constraints():
    cfg = _portfolio(strategies=(_strategy(max_order_amount=D("500000")),))
    r = derive_portfolio_quota(cfg, enabled=True)
    assert r.risk_factor == D("1")
    assert any("风控因子" in x for x in r.reasons)
    assert r.entitlement == D("5000000")


def test_no_strategies_is_not_derivable():
    r = derive_portfolio_quota(_portfolio(strategies=()), enabled=True)
    assert r.status == STATUS_NOT_DERIVABLE
    assert r.entitlement is None


def test_zero_slots_yields_zero_not_infinity():
    cfg = _portfolio(strategies=(_strategy(max_positions=0),), max_holdings=0)
    r = derive_portfolio_quota(cfg, enabled=True)
    assert r.entitlement == D("0")
    assert any("槽位" in x or "0" in x for x in r.reasons)


def test_no_single_position_cap_points_to_reserve_mode():
    """无 C_p 时不报错、不无上限；说明中指向 C1 的无上限预留口径。"""
    cfg = _portfolio(strategies=(_strategy(max_order_amount=D("500000")),))
    r = derive_portfolio_quota(cfg, enabled=True)
    assert any("无上限预留" in x for x in r.reasons)


# ---------------- 默认关闭 ----------------
def test_disabled_returns_no_quota_and_no_computation():
    r = derive_portfolio_quota(_portfolio(), enabled=False)
    assert r.status == STATUS_DISABLED
    assert r.entitlement is None and r.gross_limit is None
    assert r.strategies == []
    assert "enabled=False" in r.reasons[0]


def test_default_argument_is_disabled():
    assert derive_portfolio_quota(_portfolio()).status == STATUS_DISABLED


def test_disabled_without_config_does_not_raise():
    r = derive_portfolio_quota(None, enabled=False)
    assert r.status == STATUS_DISABLED
    assert r.portfolio_id is None


def test_strategy_level_disabled():
    q = derive_strategy_quota(
        _strategy(), portfolio_equity=D("1000000"),
        effective_equity=D("1000000"), capital_ratio=D("1"),
        max_positions_fallback=1, enabled=False,
    )
    assert q.status == STATUS_DISABLED
    assert q.entitlement is None


def test_orm_adapter_reads_optional_fields_only():
    class _PS:
        id = 3
        initial_capital = D("2000000")
        max_holdings = 20

    class _S:
        id = 7
        capital_ratio = D("1")
        max_positions = 20
        single_open_ratio = D("0.05")
        max_order_amount = D("500000")

    cfg = portfolio_quota_input_from_orm(_PS(), [_S()])
    r = derive_portfolio_quota(cfg, enabled=True)

    assert r.status == STATUS_DEGRADED
    assert r.strategies[0].single_position_limit == D("100000")
    assert r.entitlement == D("1900000")  # 95% × 200 万
    assert r.binding == ["target_utilization"]


# ---------------- 风险因子（0019 §1.6）----------------
def test_risk_factor_full_marks_high():
    r = derive_risk_factor(
        equity=D("1000000"), peak_equity=D("1000000"),
        max_drawdown=D("0.15"), daily_loss_limit=D("0.05"), day_loss=D("0"),
    )
    assert r.status == STATUS_OK
    assert r.risk_factor == D("1")


def test_risk_factor_half_on_half_drawdown():
    r = derive_risk_factor(
        equity=D("887500"), peak_equity=D("1000000"),
        max_drawdown=D("0.15"), daily_loss_limit=D("0.05"), day_loss=D("0"),
    )
    assert r.dd_factor == D("0.5")
    assert r.risk_factor == D("0.5")


def test_risk_factor_zero_at_drawdown_limit():
    r = derive_risk_factor(
        equity=D("850000"), peak_equity=D("1000000"),
        max_drawdown=D("0.15"), daily_loss_limit=D("0.05"), day_loss=D("0"),
    )
    assert r.risk_factor == D("0")


def test_risk_factor_breaker_zeroes_quota():
    r = derive_risk_factor(breaker_triggered=True)
    assert r.risk_factor == D("0")


def test_risk_factor_missing_max_drawdown_uses_015():
    r = derive_risk_factor(
        equity=D("1000000"), peak_equity=D("1000000"),
        max_drawdown=None, daily_loss_limit=D("0.05"), day_loss=D("0"),
    )
    assert r.risk_factor == D("1")
    assert "max_drawdown_defaulted" in r.degraded


def test_risk_factor_missing_daily_inputs_warns():
    r = derive_risk_factor(
        equity=D("1000000"), peak_equity=D("1000000"),
        max_drawdown=D("0.15"), daily_loss_limit=None, day_loss=None,
    )
    assert r.status == STATUS_DEGRADED
    assert "daily_loss_limit_defaulted" in r.degraded
    assert "day_loss_unknown" in r.degraded
