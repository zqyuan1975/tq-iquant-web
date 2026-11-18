"""会话待生效变更队列（D3，2026-11-18）。

运行中会话的风控参数变更不立即生效：先入队（pending），下次启动建引擎前
统一写回 `portfolio_strategies` 并标记 applied。未运行会话入队后可立即应用。

同一 (会话, 组合, 字段) 只保留一条 pending：新值覆盖旧 pending 的 new_value
并刷新 old_value（以数据库当前值为基准）。
"""
from sqlalchemy import (
    Column, DateTime, ForeignKey, Index, Integer, String,
)
from sqlalchemy.sql import func

from .base import Base


class LiveSessionPendingChange(Base):
    __tablename__ = "live_session_pending_changes"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    portfolio_strategy_id = Column(Integer, nullable=True, index=True)
    field = Column(String(64), nullable=False)
    old_value = Column(String(64), nullable=True)
    new_value = Column(String(64), nullable=True)
    # pending / applied / discarded
    status = Column(String(16), nullable=False, server_default="pending",
                    default="pending")
    note = Column(String(200), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    applied_at = Column(DateTime, nullable=True)

    __table_args__ = (
        Index("ix_lspc_session_status", "live_session_id", "status"),
    )
