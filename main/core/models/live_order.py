from sqlalchemy import Column, Integer, String, ForeignKey, Numeric, DateTime, Index
from sqlalchemy.sql import func

from .base import Base


class LiveOrder(Base):
    __tablename__ = "live_orders"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False)
    portfolio_strategy_id = Column(Integer, ForeignKey("portfolio_strategies.id"), nullable=False)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False, index=True)
    stock_code = Column(String(20), nullable=False, index=True)
    trade_type = Column(String(4), nullable=False)
    order_type = Column(String(10), nullable=False)
    price = Column(Numeric(10, 3), nullable=True)
    quantity = Column(Integer, nullable=False)
    # 资金基准对齐（2026-11-16）：**仅基准重置时**给顺延单（status='deferred'）
    # 打快照——orig_quantity 首次重置时保存当时的 quantity（信号原始目标量），
    # 供审计/复盘对照。引擎**不读取**该列（放行时按真实可用动态缩量）。
    orig_quantity = Column(Integer, nullable=True)
    # 基准重置时写入的 live_sessions.baseline_version，仅作审计留痕。引擎当前
    # **不读取**该列——旧注释称「据此识别旧基准下的顺延单」与代码事实不符；
    # 实际识别靠 deferred 行本身 + bar_time/signal_bar_date 口径（见 live_engine
    # recover 的隐式契约注释）。
    baseline_version = Column(Integer, nullable=True)
    filled_quantity = Column(Integer, default=0, server_default="0", nullable=False)
    filled_price = Column(Numeric(10, 3), nullable=True)
    status = Column(String(15), nullable=False)
    error_message = Column(String(500), nullable=True)
    signal_name = Column(String(50), nullable=True)
    signal_type = Column(String(15), nullable=True)
    bar_time = Column(DateTime, nullable=True)
    # 切片5：桥 m_strOrderRef（匹配成交回报，G3 定案键）+ Core 生成的 MD5 order_id（透传桥幂等键）
    order_ref = Column(String(30), nullable=True)
    bridge_order_id = Column(String(32), nullable=True)
    # H5 委托详情进度：拆单分组（同一大额决策的 N 笔子单同组）、子单序号/总数、
    # 追价重下第几次。普通委托为 NULL，前端仅在非空时展示。
    split_group = Column(String(64), nullable=True, index=True)
    split_seq = Column(Integer, nullable=True)
    split_total = Column(Integer, nullable=True)
    requote_attempt = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        Index("ix_live_orders_session_status", "live_session_id", "status"),
        Index("ix_live_orders_portfolio", "portfolio_strategy_id"),
        # SQLite AUTOINCREMENT：id 由 sqlite_sequence 单调分配，删除最大行后不复用 rowid。
        # 防止 _applied_state（按 order_id 记已记账量）把新订单误认成历史订单。
        {"sqlite_autoincrement": True},
    )
