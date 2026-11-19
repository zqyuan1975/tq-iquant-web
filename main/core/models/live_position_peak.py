from sqlalchemy import Column, DateTime, ForeignKey, Index, Integer, Numeric, String
from sqlalchemy.sql import func

from .base import Base


class LivePositionPeak(Base):
    """实盘持仓「持仓期最高价」持久化（方案 E，2026-11-12）。

    背景：移动止损/止损的峰值 ``Position.highest_price`` 只存内存，Core 重启后
    只能靠「日线 close 重算」补种——分钟级策略的峰值不由日线 close 决定，且每次
    重启都要重新调 TQ 取数。本表把峰值逐 bar 落库（**只在峰值被抬高时** UPSERT），
    ``recover`` 优先读表，表里没有才走周期感知补种。

    键 = ``(live_session_id, portfolio_strategy_id, stock_code)``（唯一索引）：
    同一组合策略下同代码的多条策略共享一行（取最大峰值，只升不降），与
    ``live_account_position_snapshots`` 的会话级聚合口径同源；逐策略峰值差异
    在补种时仍按「取 max(当前持仓, 表值)」应用，不会降低任何持仓的峰值。

    写入点只在实盘引擎（回测不注入 hook，零影响）；清仓（该组合该代码数量归零）
    时删行；``updated_at`` 超过 30 个交易日未更新的行在 recover 时清理并记 INFO。
    """

    __tablename__ = "live_position_peaks"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False)
    portfolio_strategy_id = Column(Integer, nullable=False)
    stock_code = Column(String(20), nullable=False)
    highest_price = Column(Numeric(18, 4), nullable=False)
    # 最后一次抬峰所用 bar 的时间（补种/快照路径可能为 None）
    peak_bar_time = Column(DateTime, nullable=True)
    # D6 峰值来源：bar（逐 bar 抬高）/ table（峰值表）/ period（策略周期补种）
    # / daily（日线补种）/ snapshot（账户快照回填）/ cost（成本兜底）/ import（导入仓起点）
    peak_source = Column(String(16), nullable=True)
    updated_at = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index(
            "ux_live_position_peaks_key",
            "live_session_id", "portfolio_strategy_id", "stock_code",
            unique=True,
        ),
    )
