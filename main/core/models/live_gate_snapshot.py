"""每日市场门控快照（E2，2026-11-18）。

每个交易日、每个启用市场门控的组合落一行：当日生效方向（多/平/空）、
强度、基准指数与「状态数据截至日」。只用于工作台回看门控判定依据，
不参与任何交易/风控判定。

唯一键 `(live_session_id, portfolio_strategy_id, snap_date)`：同日重复
更新覆盖，不会因重启/多轮调度产生多行。
"""
from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Index, Integer, String,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from .base import Base


class LiveGateSnapshot(Base):
    __tablename__ = "live_gate_snapshots"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    portfolio_strategy_id = Column(Integer, nullable=False, index=True)
    snap_date = Column(Date, nullable=False, index=True)
    # 门控基准指数代码（如 000300.SH）；历史/未配置为 NULL
    benchmark_index = Column(String(16), nullable=True)
    # python | tdx
    gate_engine = Column(String(16), nullable=True)
    # 当日生效方向：1=多头 / 0=中性 / -1=空头（右移一日的收盘状态）
    direction = Column(Integer, nullable=True)
    # 连续同向天数（强度）
    strength = Column(Integer, nullable=True)
    # 门控状态数据截至日（基准日线最后一根 bar 的日期）
    as_of = Column(Date, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("live_session_id", "portfolio_strategy_id", "snap_date",
                         name="uq_lgs_session_portfolio_date"),
        Index("ix_lgs_session_date", "live_session_id", "snap_date"),
    )
