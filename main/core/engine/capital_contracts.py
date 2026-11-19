"""资金池合同层（0021 A1）：开关、默认值、版本字段与输入/输出 dataclass。

权威关系：额度公式以 0019 为准，实施范围/验收以 0021 为准。

- 所有开关默认关闭；关闭时不计算、不落库、不改变任何既有行为。
- 本模块只定义数据契约与常量，不做 IO、不读数据库、不读时钟。
- 金额/比例统一 ``Decimal``；``to_dict()`` 输出字符串，避免浮点误差。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Sequence

# ---- 开关（0021 §0 / A1）------------------------------------------------
SWITCH_CAPITAL_POOL = "capital_pool_enabled"
SWITCH_PAIRING = "pairing_enabled"
SWITCH_LOAN_MODEL = "loan_model_enabled"
SWITCH_CASH_OUTLET = "cash_outlet_enabled"

# ---- 版本（0021 A5 将写入快照/事件；A1 先定义常量）-----------------------
CAPITAL_POOL_VERSION = "capital-pool-a1"
CAPITAL_QUOTA_ALGO_VERSION = "quota-1.0.0"

# ---- 缺失降级默认值（0019 §1.7 / 0021 A3）-------------------------------
DEFAULT_TARGET_UTILIZATION = Decimal("0.95")  # 0019 §1.7；比 1.0 保守
DEFAULT_MAX_HOLDINGS = 10
DEFAULT_MIN_CASH_RESERVE_PCT = Decimal("0.03")
DEFAULT_MAX_DRAWDOWN_FOR_RISK = Decimal("0.15")  # 0021 A3

STATUS_OK = "ok"
STATUS_DEGRADED = "degraded"
STATUS_NOT_DERIVABLE = "not_derivable"
STATUS_DISABLED = "disabled"


def to_decimal(value: Any) -> Optional[Decimal]:
    """配置值 → Decimal；None / 非法 / NaN 返回 None（由调用方降级）。"""
    if value is None:
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not d.is_finite():
        return None
    return d


def dec_str(value: Optional[Decimal]) -> Optional[str]:
    if value is None:
        return None
    return format(value, "f")


@dataclass(frozen=True)
class CapitalPoolSwitches:
    """资金池四个开关；任何缺失/非法值一律视为关闭（保守）。"""

    capital_pool_enabled: bool = False
    pairing_enabled: bool = False
    loan_model_enabled: bool = False
    cash_outlet_enabled: bool = False

    @classmethod
    def from_config(cls, cfg: Optional[Dict[str, Any]]) -> "CapitalPoolSwitches":
        block = (cfg or {}).get("capital_pool")
        if not isinstance(block, dict):
            block = {}

        def _on(key: str) -> bool:
            value = block.get(key)
            if value is True:
                return True
            if isinstance(value, str):
                return value.strip().lower() in ("1", "true", "yes", "on")
            return False

        return cls(
            capital_pool_enabled=_on("enabled"),
            pairing_enabled=_on("pairing_enabled"),
            loan_model_enabled=_on("loan_model_enabled"),
            cash_outlet_enabled=_on("cash_outlet_enabled"),
        )

    def to_dict(self) -> Dict[str, bool]:
        return {
            "capital_pool_enabled": self.capital_pool_enabled,
            "pairing_enabled": self.pairing_enabled,
            "loan_model_enabled": self.loan_model_enabled,
            "cash_outlet_enabled": self.cash_outlet_enabled,
        }


def build_capital_pool_version() -> Dict[str, Any]:
    """A5 快照/事件用的版本摘要：算法版本 + 关键默认值。"""
    return {
        "capital_pool_version": CAPITAL_POOL_VERSION,
        "quota_algo_version": CAPITAL_QUOTA_ALGO_VERSION,
        "defaults": {
            "target_utilization": dec_str(DEFAULT_TARGET_UTILIZATION),
            "max_holdings": DEFAULT_MAX_HOLDINGS,
            "min_cash_reserve_pct": dec_str(DEFAULT_MIN_CASH_RESERVE_PCT),
        },
    }


# ---- 额度输入/输出契约 ---------------------------------------------------
@dataclass(frozen=True)
class LimitItem:
    """一条约束：金额上限 + 字段来源 + 计算过程。"""

    key: str
    label: str
    amount: Decimal
    source: str
    formula: str
    degraded: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "amount": dec_str(self.amount),
            "source": self.source,
            "formula": self.formula,
            "degraded": self.degraded,
        }


@dataclass(frozen=True)
class StrategyQuotaInput:
    """单策略推导输入（0019 §1.1）；允许 None，缺失由降级表处理。"""

    strategy_id: Any
    capital_ratio: Optional[Decimal] = None
    max_positions: Optional[int] = None
    single_open_ratio: Optional[Decimal] = None
    max_order_amount: Optional[Decimal] = None
    max_order_pct_equity: Optional[Decimal] = None


@dataclass(frozen=True)
class PortfolioQuotaInput:
    """组合推导输入；``equity`` 缺失时整体不可推导。"""

    portfolio_id: Any
    equity: Optional[Decimal] = None
    equity_cap: Optional[Decimal] = None
    target_utilization: Optional[Decimal] = None
    max_holdings: Optional[int] = None
    max_single_position_pct: Optional[Decimal] = None
    gross_exposure: Optional[Decimal] = None
    strategies: Sequence[StrategyQuotaInput] = field(default_factory=tuple)
    risk_factor: Optional[Decimal] = None


@dataclass(frozen=True)
class StrategyQuota:
    strategy_id: Any
    status: str
    fund: Optional[Decimal]
    single_position_limit: Optional[Decimal]
    gross_quota: Optional[Decimal]
    entitlement: Optional[Decimal]
    risk_factor: Optional[Decimal]
    limits: List[LimitItem] = field(default_factory=list)
    binding: List[str] = field(default_factory=list)
    degraded: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "status": self.status,
            "fund": dec_str(self.fund),
            "single_position_limit": dec_str(self.single_position_limit),
            "gross_quota": dec_str(self.gross_quota),
            "entitlement": dec_str(self.entitlement),
            "risk_factor": dec_str(self.risk_factor),
            "limits": [x.to_dict() for x in self.limits],
            "binding": list(self.binding),
            "degraded": list(self.degraded),
            "warnings": list(self.degraded),
            "reasons": list(self.reasons),
        }


@dataclass(frozen=True)
class PortfolioQuota:
    portfolio_id: Any
    status: str
    equity: Optional[Decimal]
    effective_equity: Optional[Decimal]
    gross_limit: Optional[Decimal]
    entitlement: Optional[Decimal]
    headroom: Optional[Decimal]
    risk_factor: Optional[Decimal]
    strategies: List[StrategyQuota] = field(default_factory=list)
    limits: List[LimitItem] = field(default_factory=list)
    binding: List[str] = field(default_factory=list)
    degraded: List[str] = field(default_factory=list)
    reasons: List[str] = field(default_factory=list)

    @property
    def utilization_cap(self) -> Optional[Decimal]:
        if self.entitlement is None or not self.equity or self.equity <= 0:
            return None
        return self.entitlement / self.equity

    def to_dict(self) -> Dict[str, Any]:
        return {
            "portfolio_id": self.portfolio_id,
            "status": self.status,
            "equity": dec_str(self.equity),
            "effective_equity": dec_str(self.effective_equity),
            "gross_limit": dec_str(self.gross_limit),
            "entitlement": dec_str(self.entitlement),
            "headroom": dec_str(self.headroom),
            "risk_factor": dec_str(self.risk_factor),
            "utilization_cap": dec_str(self.utilization_cap),
            "strategies": [s.to_dict() for s in self.strategies],
            "limits": [x.to_dict() for x in self.limits],
            "binding": list(self.binding),
            "degraded": list(self.degraded),
            "warnings": list(self.degraded),
            "reasons": list(self.reasons),
            "version": build_capital_pool_version(),
        }
