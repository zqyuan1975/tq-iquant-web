# -*- coding: utf-8 -*-
"""M1 半自动执行：实盘审批队列（`execution_mode=manual`）。

只装「待人工批准后才真正下发」的开仓单。关键约定：

- **不写 `live_orders`**（审批通过后才走正常提交链落库）：避免污染
  `_pending_orders` 与各类「在途提交单」遍历。
- 批准时**重走全部闸门**（资金/T+1/涨跌停/在途去重/交易时段等），不照抄信号数量。
- 卖出默认不进队列（人为延迟止损等于让风控失效）。
- 会话停止即清队列（`dropped`），重启不重放。
"""
from datetime import datetime

from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Index, Integer, Numeric, String,
)

from .base import Base


class LiveApprovalQueue(Base):
    __tablename__ = "live_approval_queue"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False)
    portfolio_strategy_id = Column(
        Integer,
        ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
        nullable=False)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    stock_code = Column(String(20), nullable=False)
    trade_type = Column(String(4), nullable=False)          # BUY / SELL
    price = Column(Numeric(10, 3), nullable=True)           # 信号价（批准时重算）
    quantity = Column(Integer, nullable=False)              # 信号目标量
    signal_type = Column(String(15), nullable=True)
    signal_name = Column(String(50), nullable=True)
    period = Column(String(10), nullable=True)
    bar_time = Column(DateTime, nullable=True)              # 信号 bar 时间（审计用）
    # 原订单的未来函数闸门日（B-1）：日线信号=信号 bar 当日、分钟信号=NULL。
    # 批准时原样还原——若统一按 bar_time.date() 重建，分钟信号会被误顺延次日。
    signal_bar_date = Column(Date, nullable=True)
    status = Column(String(12), nullable=False, default="pending")
    # pending / approved / rejected / expired / dropped
    reason = Column(String(200), nullable=True)
    decided_at = Column(DateTime, nullable=True)
    expires_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        Index("ix_laq_session_status", "live_session_id", "status"),
        Index("ix_laq_expires", "expires_at"),
    )
