"""池外外部仓处置记录（I3，2026-11-18）。

柜台有仓、但代码不在任何组合股票池 → 平台不归属、不代卖，只作为「外部
持仓」展示。本表记录人工对这类仓的显式处置：

- ``include``：一键纳入某个组合的股票池（同时补 PIT 有效区间），下一轮采集
  自动按池内继承仓导入虚拟账本并扣减该组合配置资金；
- ``ignore``：永久忽略告警（仓位仍在展示层可见，但不再每轮刷
  ``out_of_pool_position`` 风险事件）。

同一会话同一代码只保留一行（重复处置覆盖）。处置动作是显式人工决定，
不改变任何既有的「池外不归属」默认语义。
"""
from sqlalchemy import (
    Column, DateTime, ForeignKey, Index, Integer, String, UniqueConstraint,
)
from sqlalchemy.sql import func

from .base import Base


class LiveOutOfPoolDecision(Base):
    __tablename__ = "live_out_of_pool_decisions"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    stock_code = Column(String(20), nullable=False)
    # include（纳入组合池）/ ignore（永久忽略告警）
    action = Column(String(10), nullable=False)
    portfolio_strategy_id = Column(Integer, nullable=True)
    note = Column(String(200), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("live_session_id", "stock_code",
                         name="uq_loop_session_code"),
        Index("ix_loop_session_action", "live_session_id", "action"),
    )
