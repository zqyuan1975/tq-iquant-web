# -*- coding: utf-8 -*-
"""I1 人工操作审计表（补单 / 平仓 / 现金改账 / 持仓改账）。

一行 = 一次人工动作尝试（含被闸门拦下的），`result` 取值：

- `accepted`：已提交（补单/平仓产生了 `live_orders` 行）
- `blocked`：被闸门拦截（资金/T+1/涨跌停/在途/交易时段/sell_scope），`reason` 说明原因
- `rejected`：请求本身非法（参数/额度/间隔）
- `applied`：改账已生效（现金台账 / 合成成交）
"""
from datetime import datetime

from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, Numeric, String

from .base import Base


class LiveManualAction(Base):
    __tablename__ = "live_manual_actions"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False)
    portfolio_strategy_id = Column(
        Integer,
        ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
        nullable=False)
    # manual_order / force_close / cash_adjust / position_adjust
    action_type = Column(String(16), nullable=False)
    stock_code = Column(String(20), nullable=True)
    trade_type = Column(String(4), nullable=True)
    quantity = Column(Integer, nullable=True)
    price = Column(Numeric(10, 3), nullable=True)
    cash_delta = Column(Numeric(15, 2), nullable=True)   # 有符号
    result = Column(String(16), nullable=False)
    live_order_id = Column(Integer, nullable=True)
    reason = Column(String(200), nullable=True)
    created_at = Column(DateTime, nullable=False, default=datetime.now)

    __table_args__ = (
        Index("ix_lma_session_time", "live_session_id", "created_at"),
    )
