"""全局应急开关（M2，2026-11-18）。

单行表（id=1）：`halt_new_positions=1` 表示所有运行中会话立即剥 BUY（停新
开仓），SELL/风控/回填/对账照常。表或行缺失一律按关闭处理。
"""
from sqlalchemy import Boolean, Column, DateTime, Integer, String
from sqlalchemy.sql import func

from .base import Base


class LiveGlobalSwitch(Base):
    __tablename__ = "live_global_switches"

    id = Column(Integer, primary_key=True)
    halt_new_positions = Column(
        Boolean, nullable=False, server_default="0", default=False)
    reason = Column(String(200), nullable=True)
    operator = Column(String(64), nullable=True)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
