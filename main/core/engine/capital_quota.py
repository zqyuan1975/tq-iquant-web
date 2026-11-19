"""额度推导纯函数（0021 A2 + A3）。

实现 0019 §1 的 ``F_j / U_j / Gross_j / Entitlement_j / Gross_p``：
纯输入 → 纯输出，不读数据库、不读文件、不读时钟，不产生任何副作用。

默认关闭：``derive_portfolio_quota(cfg)`` 不传 ``enabled`` 时直接返回
``disabled``，保证接入方必须显式开启。

缺失降级（0019 §1.7 / 0021 A3）：
- ``max_order_amount`` 缺失 → 仍受 ``r_j × F_j`` 约束（天然有限）；
- ``max_positions`` 缺失 → 组合 ``max_holdings``，再缺 1；
- ``max_holdings`` 缺失 → 策略槽位之和，再缺 10；
- ``single_open_ratio`` 缺失 → ``1 / S_j``；
- ``target_utilization`` 缺失 → **0.95**（0019 §1.7；0021 A3 写 1.0，
  按权威声明「额度与风控以 0019 为准」取更保守的 0.95）；
- ``capital_ratio`` 缺失/合计≠1 → 等分或归一，并标记降级。
任何缺失都不会推出「无上限」；无法确定（如权益缺失）直接返回
``not_derivable``。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Iterable, List, Optional

from core.engine.capital_contracts import (
    DEFAULT_MAX_DRAWDOWN_FOR_RISK,
    DEFAULT_MAX_HOLDINGS,
    DEFAULT_TARGET_UTILIZATION,
    LimitItem,
    PortfolioQuota,
    PortfolioQuotaInput,
    STATUS_DEGRADED,
    STATUS_DISABLED,
    STATUS_NOT_DERIVABLE,
    STATUS_OK,
    StrategyQuota,
    StrategyQuotaInput,
    dec_str,
    to_decimal,
)


def _disabled_strategy(strategy_id: Any) -> StrategyQuota:
    return StrategyQuota(
        strategy_id=strategy_id, status=STATUS_DISABLED, fund=None,
        single_position_limit=None, gross_quota=None, entitlement=None,
        risk_factor=None,
        reasons=["推导器未启用（enabled=False），不做任何计算"],
    )


def _disabled_portfolio(portfolio_id: Any) -> PortfolioQuota:
    return PortfolioQuota(
        portfolio_id=portfolio_id, status=STATUS_DISABLED, equity=None,
        effective_equity=None, gross_limit=None, entitlement=None,
        headroom=None, risk_factor=None,
        reasons=["推导器未启用（enabled=False），不做任何计算"],
    )


def _resolve_capital_ratios(
    strategies:
    List[StrategyQuotaInput],
) -> tuple[List[Decimal], bool, bool]:
    """返回 (资本占比, 是否等分降级, 是否归一降级)。"""
    n = len(strategies)
    raw = [to_decimal(s.capital_ratio) for s in strategies]
    if n == 0:
        return [], False, False
    if any(x is None for x in raw):
        equal = Decimal(1) / Decimal(n)
        return [equal] * n, True, False
    total = sum(raw, Decimal(0))  # type: ignore[arg-type]
    if total <= 0:
        equal = Decimal(1) / Decimal(n)
        return [equal] * n, True, True
    if total == 1:
        return list(raw), False, False  # type: ignore[arg-type]
    return [x / total for x in raw], False, True  # type: ignore[operator]


def _resolve_slots(raw: Any, fallback: int) -> int:
    """策略槽位：优先 max_positions，缺失/非法回退组合 max_holdings（再缺 1）。"""
    if raw is None:
        return max(0, int(fallback))
    try:
        slots = int(raw)
    except (TypeError, ValueError):
        return max(0, int(fallback))
    return max(0, slots)


def _resolve_single_position_limits(
    cfg: StrategyQuotaInput,
    *,
    fund: Decimal,
    portfolio_equity: Decimal,
    max_single_position_pct: Optional[Decimal],
    slots: int,
    degraded: List[str],
) -> List[LimitItem]:
    """U_j 候选约束；至少含 ``r_j × F_j``，因此结果一定是有限值。"""
    limits: List[LimitItem] = []

    ratio = to_decimal(cfg.single_open_ratio)
    if ratio is None:
        ratio = (Decimal(1) / Decimal(slots)) if slots > 0 else Decimal(0)
        degraded.append("single_open_ratio_defaulted")
        source = "缺失 → 1 / max_positions（等权分槽，降级）"
        formula = "1 / %d = %s；× 策略资金 %s = %s" % (
            slots, dec_str(ratio), dec_str(fund), dec_str(ratio * fund))
    else:
        source = "strategies.single_open_ratio"
        formula = "%s × 策略资金 %s = %s" % (
            dec_str(ratio), dec_str(fund), dec_str(ratio * fund))
    if ratio < 0:
        degraded.append("single_open_ratio_negative")
        ratio = Decimal(0)
    limits.append(LimitItem(
        key="single_open_ratio",
        label="单仓目标（单仓比例 × 策略资金）",
        amount=ratio * fund,
        source=source,
        formula=formula,
    ))

    amount = to_decimal(cfg.max_order_amount)
    if amount is not None:
        if amount < 0:
            degraded.append("max_order_amount_negative")
            amount = Decimal(0)
        limits.append(LimitItem(
            key="max_order_amount",
            label="单笔绝对上限",
            amount=amount,
            source="strategies.max_order_amount",
            formula="配置值 %s" % dec_str(amount),
        ))

    pct = to_decimal(cfg.max_order_pct_equity)
    if pct is not None:
        if pct < 0:
            degraded.append("max_order_pct_equity_negative")
            pct = Decimal(0)
        limits.append(LimitItem(
            key="max_order_pct_equity",
            label="单笔权益比例上限",
            amount=pct * portfolio_equity,
            source="strategies.max_order_pct_equity",
            formula="%s × 组合权益 %s = %s" % (
                dec_str(pct), dec_str(portfolio_equity),
                dec_str(pct * portfolio_equity)),
        ))

    conc = to_decimal(max_single_position_pct)
    if conc is not None:
        if conc < 0:
            degraded.append("max_single_position_pct_negative")
            conc = Decimal(0)
        limits.append(LimitItem(
            key="max_single_position_pct",
            label="单票集中度上限（组合级）",
            amount=conc * portfolio_equity,
            source="portfolio_strategies.max_single_position_pct",
            formula="%s × 组合权益 %s = %s" % (
                dec_str(conc), dec_str(portfolio_equity),
                dec_str(conc * portfolio_equity)),
        ))

    return limits


def derive_strategy_quota(
    cfg: StrategyQuotaInput,
    *,
    portfolio_equity: Decimal,
    effective_equity: Decimal,
    capital_ratio: Decimal,
    max_positions_fallback: int,
    max_single_position_pct: Optional[Decimal] = None,
    risk_factor: Optional[Decimal] = None,
    enabled: bool = True,
) -> StrategyQuota:
    """推导单策略额度（0019 §1.2）。"""
    if not enabled:
        return _disabled_strategy(cfg.strategy_id)

    degraded: List[str] = []
    reasons: List[str] = []

    slots_raw = cfg.max_positions
    if slots_raw is None:
        slots = max(0, int(max_positions_fallback))
        degraded.append("max_positions_defaulted")
        reasons.append("max_positions 缺失，回退组合 max_holdings=%d" % slots)
    else:
        try:
            slots = int(slots_raw)
        except (TypeError, ValueError):
            slots = max(0, int(max_positions_fallback))
            degraded.append("max_positions_invalid")
            reasons.append("max_positions 非法，回退组合 max_holdings=%d" % slots)
        else:
            if slots < 0:
                degraded.append("max_positions_negative")
                slots = 0

    fund = capital_ratio * effective_equity

    if not portfolio_equity or portfolio_equity <= 0:
        return StrategyQuota(
            strategy_id=cfg.strategy_id, status=STATUS_NOT_DERIVABLE,
            fund=None, single_position_limit=None, gross_quota=None,
            entitlement=None, risk_factor=None, degraded=degraded,
            reasons=reasons + ["组合权益未知或非正，无法推导策略额度"],
        )

    limits = _resolve_single_position_limits(
        cfg, fund=fund, portfolio_equity=portfolio_equity,
        max_single_position_pct=max_single_position_pct, slots=slots,
        degraded=degraded,
    )
    unit = min(x.amount for x in limits)
    binding = [x.key for x in limits if x.amount == unit]

    gross = unit * slots

    rd = to_decimal(risk_factor)
    if rd is None:
        rd = Decimal(1)
        # 未提供风控因子按 1.0（中性，不放大配置约束），不算降级。
        reasons.append("未提供风控因子，按 1.0 计算（仅压降，不放大配置约束）")
    if rd < 0:
        degraded.append("risk_factor_negative")
        rd = Decimal(0)
    if rd > 1:
        degraded.append("risk_factor_above_one")
        rd = Decimal(1)

    entitlement = gross * rd

    if unit == 0:
        reasons.append("单仓上限被某条约束压到 0，策略额度为 0")
    if slots == 0:
        reasons.append("max_positions=0，策略不分配槽位")

    return StrategyQuota(
        strategy_id=cfg.strategy_id,
        status=STATUS_DEGRADED if degraded else STATUS_OK,
        fund=fund,
        single_position_limit=unit,
        gross_quota=gross,
        entitlement=entitlement,
        risk_factor=rd,
        limits=limits,
        binding=binding,
        degraded=degraded,
        reasons=reasons,
    )


def _resolve_max_holdings(
    cfg: PortfolioQuotaInput,
    strategy_slots: List[int],
    degraded: List[str],
    reasons: List[str],
) -> int:
    raw = cfg.max_holdings
    if raw is not None:
        try:
            h = int(raw)
        except (TypeError, ValueError):
            h = -1
        if h < 0:
            degraded.append("max_holdings_invalid")
            reasons.append("max_holdings 非法，回退策略槽位之和")
        else:
            return h
    total_slots = sum(x for x in strategy_slots if x > 0)
    if total_slots > 0:
        degraded.append("max_holdings_defaulted")
        reasons.append("max_holdings 缺失，回退策略 max_positions 之和=%d"
                       % total_slots)
        return total_slots
    degraded.append("max_holdings_defaulted")
    reasons.append("max_holdings 与策略槽位均缺失，回退保守值 %d"
                   % DEFAULT_MAX_HOLDINGS)
    return DEFAULT_MAX_HOLDINGS


def derive_portfolio_quota(
    cfg: Optional[PortfolioQuotaInput],
    *,
    enabled: bool = False,
) -> PortfolioQuota:
    """推导组合额度（0019 §1.3）。默认关闭：不启用时不做任何计算。"""
    if not enabled:
        return _disabled_portfolio(getattr(cfg, "portfolio_id", None))
    if cfg is None:
        return PortfolioQuota(
            portfolio_id=None, status=STATUS_NOT_DERIVABLE, equity=None,
            effective_equity=None, gross_limit=None, entitlement=None,
            headroom=None, risk_factor=None,
            reasons=["缺少组合配置，无法推导"],
        )

    degraded: List[str] = []
    reasons: List[str] = []

    equity = to_decimal(cfg.equity)
    if equity is None or equity <= 0:
        return PortfolioQuota(
            portfolio_id=cfg.portfolio_id, status=STATUS_NOT_DERIVABLE,
            equity=equity, effective_equity=None, gross_limit=None,
            entitlement=None, headroom=None, risk_factor=None,
            reasons=["组合权益缺失或非正，无法推导额度"],
        )

    cap = to_decimal(cfg.equity_cap)
    if cap is not None and cap < 0:
        degraded.append("equity_cap_negative")
        cap = Decimal(0)
    if cap is not None and cap == 0:
        degraded.append("equity_cap_zero")
    effective_equity = equity if cap is None else min(equity, cap)

    if not cfg.strategies:
        return PortfolioQuota(
            portfolio_id=cfg.portfolio_id, status=STATUS_NOT_DERIVABLE,
            equity=equity, effective_equity=effective_equity,
            gross_limit=None, entitlement=None, headroom=None,
            risk_factor=None, reasons=["组合下无策略配置，无额度可推导"],
        )

    ratios, equal_degraded, normalize_degraded = _resolve_capital_ratios(
        list(cfg.strategies))
    if equal_degraded:
        degraded.append("capital_ratio_defaulted")
        reasons.append("存在缺失的 capital_ratio，按等分归一处理")
    if normalize_degraded:
        degraded.append("capital_ratio_normalized")
        reasons.append("capital_ratio 合计不为 1，按合计归一")

    rd = to_decimal(cfg.risk_factor)
    if rd is None:
        rd = Decimal(1)
        reasons.append("未提供风控因子，按 1.0 计算（仅压降，不放大配置约束）")
    if rd < 0:
        degraded.append("risk_factor_negative")
        rd = Decimal(0)
    if rd > 1:
        degraded.append("risk_factor_above_one")
        rd = Decimal(1)

    if cfg.max_single_position_pct is None:
        reasons.append("未配置单票集中度上限，单票执行需叠加容量与无上限预留口径"
                       "（见 C1 capital_reserve）")

    strategy_results: List[StrategyQuota] = []
    strategy_slots: List[int] = []
    for idx, s_cfg in enumerate(cfg.strategies):
        slot_fallback = cfg.max_holdings if cfg.max_holdings is not None else 1
        try:
            slot_fallback = int(slot_fallback)
        except (TypeError, ValueError):
            slot_fallback = 1
        if slot_fallback < 1:
            slot_fallback = 1
        strategy_slots.append(_resolve_slots(s_cfg.max_positions, slot_fallback))
        strategy_results.append(derive_strategy_quota(
            s_cfg,
            portfolio_equity=equity,
            effective_equity=effective_equity,
            capital_ratio=ratios[idx],
            max_positions_fallback=slot_fallback,
            max_single_position_pct=cfg.max_single_position_pct,
            risk_factor=rd,
            enabled=True,
        ))

    if any(s.status == STATUS_NOT_DERIVABLE for s in strategy_results):
        return PortfolioQuota(
            portfolio_id=cfg.portfolio_id, status=STATUS_NOT_DERIVABLE,
            equity=equity, effective_equity=effective_equity,
            gross_limit=None, entitlement=None, headroom=None,
            risk_factor=rd, strategies=strategy_results,
            degraded=degraded,
            reasons=reasons + ["存在不可推导的策略额度，组合额度不给出"],
        )

    for s in strategy_results:
        for d in s.degraded:
            if d not in degraded:
                degraded.append(d)
        for r in s.reasons:
            if r not in reasons:
                reasons.append(r)

    sum_gross = sum((s.gross_quota or Decimal(0)) for s in strategy_results)
    max_unit = max((s.single_position_limit or Decimal(0))
                   for s in strategy_results)

    max_holdings = _resolve_max_holdings(cfg, strategy_slots, degraded, reasons)

    target = to_decimal(cfg.target_utilization)
    if target is None:
        # 0019 §1.7 定为 0.95；0021 A3 写 1.0，按权威声明以 0019 为准。
        target = DEFAULT_TARGET_UTILIZATION
        degraded.append("target_utilization_defaulted")
        reasons.append("target_utilization 缺失，按 0019 §1.7 的保守上限 %s 计算"
                       % dec_str(DEFAULT_TARGET_UTILIZATION))
    if target < 0:
        degraded.append("target_utilization_negative")
        target = Decimal(0)
    if target > 1:
        degraded.append("target_utilization_above_one")
        target = Decimal(1)

    limits: List[LimitItem] = [
        LimitItem(
            key="sum_strategy_gross",
            label="策略毛额度之和",
            amount=sum_gross,
            source="Σ_j (S_j × U_j)",
            formula="Σ 各策略毛额度 = %s" % dec_str(sum_gross),
        ),
        LimitItem(
            key="max_holdings",
            label="组合持仓数量上限 × 最大单仓",
            amount=Decimal(max_holdings) * max_unit,
            source="portfolio_strategies.max_holdings × max_j(U_j)",
            formula="%d × %s = %s" % (
                max_holdings, dec_str(max_unit),
                dec_str(Decimal(max_holdings) * max_unit)),
            degraded="max_holdings" in degraded,
        ),
        LimitItem(
            key="target_utilization",
            label="目标仓位上限 × 组合权益",
            amount=target * equity,
            source="portfolio_strategies.target_utilization",
            formula="%s × %s = %s" % (
                dec_str(target), dec_str(equity),
                dec_str(target * equity)),
            degraded="target_utilization" in degraded,
        ),
    ]
    if cap is not None:
        limits.append(LimitItem(
            key="equity_cap",
            label="组合权益上限",
            amount=cap,
            source="portfolio_strategies.equity_cap",
            formula="配置值 %s" % dec_str(cap),
            degraded=cap == 0,
        ))

    gross_limit = min(x.amount for x in limits)
    binding = [x.key for x in limits if x.amount == gross_limit]
    entitlement = gross_limit * rd

    headroom: Optional[Decimal] = None
    exposure = to_decimal(cfg.gross_exposure)
    if exposure is None:
        reasons.append("未提供当前毛敞口，未计算可用缺口 headroom")
    else:
        if exposure < 0:
            degraded.append("gross_exposure_negative")
            exposure = Decimal(0)
        headroom = max(Decimal(0), entitlement - exposure)

    return PortfolioQuota(
        portfolio_id=cfg.portfolio_id,
        status=STATUS_DEGRADED if degraded else STATUS_OK,
        equity=equity,
        effective_equity=effective_equity,
        gross_limit=gross_limit,
        entitlement=entitlement,
        headroom=headroom,
        risk_factor=rd,
        strategies=strategy_results,
        limits=limits,
        binding=binding,
        degraded=list(dict.fromkeys(degraded)),
        reasons=reasons,
    )


def portfolio_quota_input_from_orm(
    ps: Any,
    strategies: Iterable[Any],
    *,
    equity: Any = None,
    gross_exposure: Any = None,
) -> PortfolioQuotaInput:
    """从 ORM 行（或同形对象）构造输入；只读，不触发任何写入。

    ``equity`` 未显式传入时退化为 ``initial_capital``（仅用于「按配置推导」；
    实盘/回测决策时点必须传入真实权益）。
    """
    eq = to_decimal(equity)
    if eq is None:
        eq = to_decimal(getattr(ps, "initial_capital", None))
    items = []
    for s in strategies:
        items.append(StrategyQuotaInput(
            strategy_id=getattr(s, "id", None),
            capital_ratio=to_decimal(getattr(s, "capital_ratio", None)),
            max_positions=getattr(s, "max_positions", None),
            single_open_ratio=to_decimal(getattr(s, "single_open_ratio", None)),
            max_order_amount=to_decimal(getattr(s, "max_order_amount", None)),
            max_order_pct_equity=to_decimal(
                getattr(s, "max_order_pct_equity", None)),
        ))
    return PortfolioQuotaInput(
        portfolio_id=getattr(ps, "id", None),
        equity=eq,
        equity_cap=to_decimal(getattr(ps, "equity_cap", None)),
        target_utilization=to_decimal(
            getattr(ps, "target_utilization", None)),
        max_holdings=getattr(ps, "max_holdings", None),
        max_single_position_pct=to_decimal(
            getattr(ps, "max_single_position_pct", None)),
        gross_exposure=to_decimal(gross_exposure),
        strategies=items,
        risk_factor=None,
    )


# ---- 风险因子（0019 §1.6；A3 降级）--------------------------------------
@dataclass(frozen=True)
class RiskFactorResult:
    status: str
    risk_factor: Optional[Decimal]
    dd_factor: Optional[Decimal]
    daily_factor: Optional[Decimal]
    breaker_factor: Optional[Decimal]
    degraded: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "status": self.status,
            "risk_factor": dec_str(self.risk_factor),
            "dd_factor": dec_str(self.dd_factor),
            "daily_factor": dec_str(self.daily_factor),
            "breaker_factor": dec_str(self.breaker_factor),
            "degraded": list(self.degraded),
            "warnings": list(self.degraded),
            "reasons": list(self.reasons),
        }


def _clamp01(value: Decimal) -> Decimal:
    if value < 0:
        return Decimal(0)
    if value > 1:
        return Decimal(1)
    return value


def derive_risk_factor(
    *,
    equity: Any = None,
    peak_equity: Any = None,
    max_drawdown: Any = None,
    daily_loss_limit: Any = None,
    day_loss: Any = None,
    breaker_triggered: bool = False,
) -> RiskFactorResult:
    """风控因子 Rd（只压额度，不强制卖出）。

    降级：缺 ``max_drawdown`` 用 0.15（0021 A3）；缺权益/峰值按回撤 0（并告警）；
    缺日亏按 0、缺日亏上限用 0.05（项目 ORM 默认）并告警。
    """
    if breaker_triggered:
        return RiskFactorResult(
            status=STATUS_OK, risk_factor=Decimal(0), dd_factor=None,
            daily_factor=None, breaker_factor=Decimal(0),
            reasons=["组合/会话熔断或日亏暂停：本次新增开仓额度为 0"],
        )

    degraded: List[str] = []
    reasons: List[str] = []

    md = to_decimal(max_drawdown)
    if md is None:
        md = DEFAULT_MAX_DRAWDOWN_FOR_RISK
        degraded.append("max_drawdown_defaulted")
        reasons.append("max_drawdown 缺失，按 0.15 保守计算")
    if md <= 0:
        degraded.append("max_drawdown_invalid")
        return RiskFactorResult(
            status=STATUS_DEGRADED, risk_factor=Decimal(0), dd_factor=Decimal(0),
            daily_factor=None, breaker_factor=Decimal(1),
            degraded=degraded, reasons=reasons + ["max_drawdown 非正，额度置 0"],
        )

    eq = to_decimal(equity)
    peak = to_decimal(peak_equity)
    if eq is None or peak is None or peak <= 0:
        drawdown = Decimal(0)
        degraded.append("drawdown_unknown")
        reasons.append("权益/峰值缺失，按回撤 0 计算并标记降级")
    else:
        drawdown = max(Decimal(0), Decimal(1) - eq / peak)

    dd_factor = _clamp01((md - drawdown) / (md / 2))

    limit = to_decimal(daily_loss_limit)
    if limit is None:
        limit = Decimal("0.05")
        degraded.append("daily_loss_limit_defaulted")
        reasons.append("daily_loss_limit 缺失，按 0.05 保守计算")
    loss = to_decimal(day_loss)
    if loss is None:
        loss = Decimal(0)
        degraded.append("day_loss_unknown")
        reasons.append("日亏未知，按 0 计算并标记降级")
    loss = max(Decimal(0), loss)
    if limit <= 0:
        daily_factor = Decimal(0)
        degraded.append("daily_loss_limit_invalid")
    else:
        daily_factor = _clamp01((limit - loss) / (limit / 2))

    rd = min(dd_factor, daily_factor, Decimal(1))
    return RiskFactorResult(
        status=STATUS_DEGRADED if degraded else STATUS_OK,
        risk_factor=_clamp01(rd),
        dd_factor=dd_factor,
        daily_factor=daily_factor,
        breaker_factor=Decimal(1),
        degraded=degraded,
        reasons=reasons,
    )
