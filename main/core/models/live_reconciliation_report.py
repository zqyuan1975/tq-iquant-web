"""实盘每日对账日报（C4，2026-11-18）。

每个交易日收盘清扫后落一行：把「柜台口径」与「虚拟账本」的持仓/现金/净值
差异、当日委托与成交笔数固化成可追溯的历史快照。报表本身**只读聚合**，不
触发任何改账（改账走 C1 差异台账 / I2 接受柜台口径）。

唯一键 `(live_session_id, report_date)`：同一天重复生成时覆盖更新，不会因
重启/重复调度产生多行。
"""
from sqlalchemy import (
    Column, Date, DateTime, ForeignKey, Index, Integer, Numeric, String, Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from .base import Base


class LiveReconciliationReport(Base):
    __tablename__ = "live_reconciliation_reports"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True,
    )
    report_date = Column(Date, nullable=False, index=True)
    engine = Column(String(20), nullable=True)
    # 持仓侧：柜台/虚拟代码并集数量、完全一致的代码数、存在差异的代码数
    positions_total = Column(Integer, nullable=True)
    positions_matched = Column(Integer, nullable=True)
    positions_diff = Column(Integer, nullable=True)
    # 现金侧：虚拟现金 vs 柜台可用，差额 = 柜台 − 虚拟（正=柜台更多）
    cash_expected = Column(Numeric(16, 2), nullable=True)
    cash_actual = Column(Numeric(16, 2), nullable=True)
    cash_diff = Column(Numeric(16, 2), nullable=True)
    # 净值侧：虚拟净值 vs 柜台总资产（剔除池外后的期望值一并记 detail）
    equity_expected = Column(Numeric(16, 2), nullable=True)
    equity_actual = Column(Numeric(16, 2), nullable=True)
    equity_deviation_pct = Column(Numeric(10, 4), nullable=True)
    order_count = Column(Integer, nullable=True)
    trade_count = Column(Integer, nullable=True)
    # 明细 JSON：逐代码差异、池外市值、生成时刻等（只读展示用）
    detail = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("live_session_id", "report_date",
                         name="uq_lrr_session_date"),
        Index("ix_lrr_session_date", "live_session_id", "report_date"),
    )
