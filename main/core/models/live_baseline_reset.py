from sqlalchemy import Column, Integer, String, ForeignKey, Numeric, DateTime, Index
from sqlalchemy.sql import func

from .base import Base


class LiveBaselineReset(Base):
    """实盘资金基准重置审计（2026-11-16 资金基准对齐）。

    每次「收盘后重置」落一行，记录前后基准与风控锚点，确保改动可追溯。
    重置**绝不**走 live_cash_flows（避免污染 external_net_inflow / risk_equity），
    也不清零 circuit_breaker_count、不解锁手动恢复；这些字段不在此表复刻，
    审计只记录基准/峰值/前收/接缝四组前后值。
    """
    __tablename__ = "live_baseline_resets"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True)
    portfolio_strategy_id = Column(
        Integer, ForeignKey("portfolio_strategies.id"), nullable=False)
    old_baseline = Column(Numeric(18, 2), nullable=True)
    new_baseline = Column(Numeric(18, 2), nullable=True)
    old_peak = Column(Numeric(18, 2), nullable=True)
    new_peak = Column(Numeric(18, 2), nullable=True)
    old_prev_close = Column(Numeric(18, 2), nullable=True)
    new_prev_close = Column(Numeric(18, 2), nullable=True)
    old_seam_date = Column(String(10), nullable=True)
    new_seam_date = Column(String(10), nullable=True)
    baseline_version = Column(Integer, nullable=True)
    note = Column(String(500), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_live_baseline_resets_session", "live_session_id", "created_at"),
    )
