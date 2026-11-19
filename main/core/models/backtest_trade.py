from sqlalchemy import Column, Integer, String, ForeignKey, Numeric, DateTime, Index
from sqlalchemy.sql import func

from .base import Base


class BacktestTrade(Base):
    __tablename__ = "backtest_trades"

    id = Column(Integer, primary_key=True)
    backtest_record_id = Column(Integer, ForeignKey("backtest_records.id", ondelete="CASCADE"), nullable=False)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False, index=True)
    formula_signal_id = Column(Integer, ForeignKey("formula_signals.id"), nullable=True, index=True)
    signal_name = Column(String(50), nullable=False)
    signal_type = Column(String(15), nullable=False)
    stock_code = Column(String(20), nullable=False)
    trade_type = Column(String(4), nullable=False)
    price = Column(Numeric(10, 3), nullable=False)
    # 滑点前参考价（应用 1±滑点 之前的开盘价，买卖同源）；历史行为 NULL。
    # 仅用于成本容量「实现滑点」口径，不参与净值/成交计算。
    ref_price = Column(Numeric(10, 3), nullable=True)
    quantity = Column(Integer, nullable=False)
    amount = Column(Numeric(15, 2), nullable=False)
    commission = Column(Numeric(10, 2), nullable=False)
    stamp_duty = Column(Numeric(10, 2), nullable=False)
    # 过户费（双向，2026-11-11）：历史行为 0；新回测按组合 transfer_fee_rate 计。
    transfer_fee = Column(Numeric(10, 2), nullable=False, server_default="0")
    bar_time = Column(DateTime, nullable=False)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_backtest_trades_rec_bar", "backtest_record_id", "bar_time"),
    )
