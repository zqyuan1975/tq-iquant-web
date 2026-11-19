"""组合资金日序列（A3，2026-11-18）。

每个交易日每个组合落一行：日终现金、持仓市值、总市值、风险口径净值
（总市值 − 累计外部净流入）、占账户权重与逐仓市值明细。用于资金复盘与
账户级熔断（D1）对照，不参与任何账务或风控判定。

唯一键 `(live_session_id, portfolio_strategy_id, trade_date)`：同一日重复
生成时覆盖更新，不会因重启/重复调度产生多行。
"""
from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Index, Integer, Numeric, String, Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from .base import Base


class LivePortfolioDaily(Base):
    __tablename__ = "live_portfolio_daily"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    portfolio_strategy_id = Column(Integer, nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    cash = Column(Numeric(18, 2), nullable=True)
    market_value = Column(Numeric(18, 2), nullable=True)
    total_value = Column(Numeric(18, 2), nullable=True)
    # 风险口径净值（总市值 − 累计外部净流入）：与熔断/回撤同源
    risk_value = Column(Numeric(18, 2), nullable=True)
    # 占账户总市值比例（0–1）；账户合计为 0 时为 NULL
    weight = Column(Numeric(10, 6), nullable=True)
    position_count = Column(Integer, nullable=True)
    # 逐仓市值 JSON：[{stock_code, quantity, market_value}]
    detail = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("live_session_id", "portfolio_strategy_id", "trade_date",
                         name="uq_lpd_session_portfolio_date"),
        Index("ix_lpd_session_date", "live_session_id", "trade_date"),
    )
