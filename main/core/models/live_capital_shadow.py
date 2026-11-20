"""实盘资金池影子记录（0021 F3）。

只记录「计划 vs 实际」，不参与任何交易决策：本表由影子引擎在每根 bar 末尾
追加一行，订单数量/价格/拆单/审批/既有决策事件/账本均为零变化。
影子失败只记日志、回滚本行，不影响交易主链路。
"""
from sqlalchemy import Column, DateTime, Integer, Text
from sqlalchemy.sql import func

from .base import Base


class LiveCapitalShadow(Base):
    __tablename__ = "live_capital_shadow"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(Integer, nullable=False, index=True)
    portfolio_id = Column(Integer, nullable=False, index=True)
    # 触发影子计算的 bar 时间（上海 naive，与行情/决策事件同口径）。
    bar_time = Column(DateTime, nullable=True)
    # 影子快照：计划的额度/借据/召回口径 + 实盘现金/市值 + 漂移与告警。
    payload_json = Column(Text, nullable=True)
    version = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
