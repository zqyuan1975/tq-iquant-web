from sqlalchemy import (
    Boolean, Column, Integer, String, ForeignKey, UniqueConstraint, DateTime,
    Date, Numeric, Text,
)
from sqlalchemy.sql import func

from .base import Base


class LiveSessionPortfolio(Base):
    __tablename__ = "live_session_portfolios"

    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False)
    portfolio_strategy_id = Column(Integer, ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"), nullable=False)
    status = Column(String(15), default="active", server_default="active", nullable=False)
    circuit_breaker_count = Column(Integer, default=0, server_default="0", nullable=False)
    # 风控内存态快照（JSON 文本）：peak_value/prev_close_value/daily_pause_active/
    # breaker_trigger_date/daily_loss_trigger_date。实盘重启时读回，避免峰值/暂停态丢失
    # 导致 max_drawdown 少触发、日内暂停被意外解除。日终与触发时落库。
    risk_state = Column(Text, nullable=True)
    # ---- 接缝日（增量回测锚点）----
    # 该组合进入实盘的交易日。显式落库、不靠「会话第一条成交」推导（后端重启/
    # 会话删除重建会让推导值漂移）。NULL = 尚未建立接缝，增量回测不启动。
    seam_date = Column(Date, nullable=True)
    # 接缝日该组合的虚拟起账现金（Portfolio.account.cash）
    seam_cash = Column(Numeric(15, 2), nullable=True)
    # 接缝日存量仓 JSON：[{stock_code, quantity, avg_cost, buy_date, highest_price}]
    seam_positions_json = Column(Text, nullable=True)
    # 接缝捕获时刻已计入起账状态的 live_trades.id 水位（含当日已成交）。
    # 实盘侧重放（incremental_backtest._live_rows）只重放 id 大于该水位的成交，
    # 防「盘中重启 → 首次锚定 → 当日已有成交」把同一笔成交算两次。NULL = 旧接缝
    # （改动前捕获），只能按接缝日 00:00 重放并告警。
    seam_trade_cursor = Column(Integer, nullable=True)
    # 接缝锚定开关三态：NULL=跟随全局 incremental_backtest.anchor_on_live_start
    # （默认 True）/ 0 / 1。开启时续跑回测用实盘起账状态重锚（同起点同分母）。
    seam_anchor_enabled = Column(Boolean, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (UniqueConstraint("session_id", "portfolio_strategy_id"),)
