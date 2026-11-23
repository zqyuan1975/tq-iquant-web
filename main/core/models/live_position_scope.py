"""按标的授权代卖白名单（sell_scope=platform_only 下的逐票放行）。

默认无记录 = 不代卖（与改动前的 platform_only 行为逐字一致）。一行 = 该会话对
该代码显式授权：``_scope_sell_quantity`` 不再把卖出数量夹到平台仓，按整仓可卖量
放行；T+1、涨跌停、审批、时点等既有闸门照旧生效。

收回时把 ``enabled`` 置 0 保留历史行（配合 ``updated_at`` / ``updated_by``
回答「谁在何时放开/收回哪只」）。
"""
from sqlalchemy import (
    Boolean, Column, DateTime, ForeignKey, Index, Integer, String,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from .base import Base


class LivePositionScope(Base):
    __tablename__ = "live_position_scope"

    id = Column(Integer, primary_key=True)
    session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False,
    )
    # 股票代码（含交易所后缀，如 002745.SZ）
    code = Column(String(20), nullable=False)
    # true=已授权代卖；false=已收回（保留行便于追溯）
    enabled = Column(Boolean, nullable=False, default=True,
                     server_default="1")
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now())
    # 操作者（系统无鉴权 → 固定文案，不伪造身份）
    updated_by = Column(String(64), nullable=True)

    __table_args__ = (
        UniqueConstraint("session_id", "code",
                         name="uq_live_position_scope_session_code"),
        Index("ix_live_position_scope_session_enabled", "session_id", "enabled"),
    )
