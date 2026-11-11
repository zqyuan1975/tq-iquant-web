from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Index, Integer, Numeric, String,
)
from sqlalchemy.sql import func

from .base import Base


class LiveCashFlow(Base):
    """实盘虚拟账本的外部现金流台账（③）。

    入金/出金/分红/其他流水：虚拟现金随台账增减（入金加、出金减），但净值/
    收益率/回撤必须**剔除**外部现金流——风险口径统一用
    ``Portfolio.total_value(bar) - Portfolio.external_net_inflow``
    （回测无外部现金流，该值为 0，行为不变）。

    amount 为**有符号**金额：>0 流入（入金/分红/其他流入），<0 流出（出金/
    其他流出）。口径由 sign 唯一决定，flow_type 只作分类展示，避免「类型与
    方向」两套判据在调用方各写一遍。
    """
    __tablename__ = "live_cash_flows"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False)
    portfolio_strategy_id = Column(
        Integer, ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
        nullable=False)
    # deposit（入金）/ withdraw（出金）/ dividend（分红）/ other（其他）
    flow_type = Column(String(16), nullable=False)
    # 有符号金额：>0 流入，<0 流出（元）
    amount = Column(Numeric(15, 2), nullable=False)
    flow_date = Column(Date, nullable=False)
    note = Column(String(200), nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_live_cash_flows_session_date", "live_session_id", "flow_date"),
        Index("ix_live_cash_flows_portfolio_date",
              "portfolio_strategy_id", "flow_date"),
    )
