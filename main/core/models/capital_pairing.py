from sqlalchemy import (
    Column, DateTime, ForeignKey, Integer, Numeric, String, Text,
)
from sqlalchemy.sql import func

from .base import Base


class CapitalPairingPlan(Base):
    """拼盘头（0020 §12.3）——配置对象，不是回测结果。

    L2（同组合内多策略）在阶段 B 开放；``scope=L3`` 留给 E 阶段。
    ``binding_json`` 显式冻结「策略/周期/风控时点/追价/单笔上限」等绑定参数，
    实盘与回测是零继承：启动时按当前策略行重建，漂移即失效（F 阶段校验）。
    """

    __tablename__ = "capital_pairing_plans"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    scope = Column(String(10), nullable=False, default="L2",
                   server_default="L2")
    status = Column(String(15), nullable=False, default="DRAFT",
                    server_default="DRAFT")
    pairing_version = Column(String(64), nullable=True)
    params_hash = Column(String(64), nullable=True)

    portfolio_strategy_id = Column(
        Integer, ForeignKey("portfolio_strategies.id", ondelete="CASCADE"),
        nullable=False, index=True)
    lender_strategy_id = Column(Integer, nullable=True, index=True)
    lender_record_id = Column(Integer, nullable=True)
    max_borrowers = Column(Integer, nullable=False, default=1,
                           server_default="1")

    loan_term_days = Column(Integer, nullable=False, default=365,
                            server_default="365")
    principal = Column(Numeric(18, 2), nullable=True)
    recall_notice_days = Column(Integer, nullable=False, default=2,
                               server_default="2")
    release_delay_days = Column(Integer, nullable=False, default=0,
                                server_default="0")
    pool_scope = Column(String(30), nullable=True)
    ownership_policy = Column(String(30), nullable=True)
    no_cap_reserve_policy = Column(String(30), nullable=True)
    gate_hysteresis_days = Column(Integer, nullable=True)
    cash_outlet_enabled = Column(Integer, nullable=False, default=0,
                                 server_default="0")

    binding_json = Column(Text, nullable=True)
    evidence_json = Column(Text, nullable=True)
    backtest_record_id = Column(Integer, nullable=True)

    created_by = Column(String(50), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    approved_at = Column(DateTime, nullable=True)
    effective_from = Column(DateTime, nullable=True)
    effective_to = Column(DateTime, nullable=True)


class CapitalPairing(Base):
    """拼盘边（0020 §5.1）：出借方 → 借款方。一个拼盘内活跃借款方 ≤ 3。"""

    __tablename__ = "capital_pairings"

    id = Column(Integer, primary_key=True)
    pairing_plan_id = Column(
        Integer, ForeignKey("capital_pairing_plans.id", ondelete="CASCADE"),
        nullable=False, index=True)
    scope = Column(String(10), nullable=False, default="L2",
                   server_default="L2")
    lender_type = Column(String(15), nullable=False, default="strategy",
                          server_default="strategy")
    lender_id = Column(Integer, nullable=False)
    lender_record_id = Column(Integer, nullable=True)
    borrower_type = Column(String(15), nullable=False, default="strategy",
                            server_default="strategy")
    borrower_id = Column(Integer, nullable=False, index=True)
    borrower_record_id = Column(Integer, nullable=True)
    status = Column(String(15), nullable=False, default="ACTIVE",
                    server_default="ACTIVE")
    max_share = Column(Numeric(6, 4), nullable=True)
    loan_term_days = Column(Integer, nullable=True)
    recall_notice_days = Column(Integer, nullable=True)
    release_delay_days = Column(Integer, nullable=True)
    eligibility_backtest_id = Column(Integer, nullable=True)
    evaluation_id = Column(Integer, nullable=True)
    params_hash = Column(String(64), nullable=True)
    pairing_version = Column(String(64), nullable=True)
    outlet_verification_id = Column(Integer, nullable=True)
    binding_json = Column(Text, nullable=True)
    effective_from = Column(DateTime, nullable=True)
    effective_to = Column(DateTime, nullable=True)
    created_by = Column(String(50), nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class CapitalPairingEval(Base):
    """搭配评估冻结结果（0020 §12.3）：只读展示，不回写成员记录。"""

    __tablename__ = "capital_pairing_evals"

    id = Column(Integer, primary_key=True)
    pairing_plan_id = Column(
        Integer, ForeignKey("capital_pairing_plans.id", ondelete="CASCADE"),
        nullable=False, index=True)
    scope = Column(String(10), nullable=False, default="L2")
    eval_kind = Column(String(20), nullable=False, default="L2_PRECHECK")
    eval_version = Column(String(40), nullable=True)
    threshold_version = Column(String(40), nullable=True)
    status = Column(String(10), nullable=False)
    reasons_json = Column(Text, nullable=True)
    hard_json = Column(Text, nullable=True)
    soft_json = Column(Text, nullable=True)
    metrics_json = Column(Text, nullable=True)
    counts_json = Column(Text, nullable=True)
    lender_strategy_id = Column(Integer, nullable=True)
    borrower_strategy_id = Column(Integer, nullable=True)
    record_id = Column(Integer, nullable=True)
    common_days = Column(Integer, nullable=True)
    window_start = Column(String(10), nullable=True)
    window_end = Column(String(10), nullable=True)
    decided_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
