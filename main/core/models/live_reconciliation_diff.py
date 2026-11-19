"""C1/I2：虚拟账本 vs 柜台真实账户的差异台账。

差异来源（全部由引擎现有告警点旁路写入，默认关闭）：
- `position`：人工干预（`manual_intervention`）——柜台量 ≠ 虚拟量；
  池外外部仓（`out_of_pool_position`）不写（平台从未管过，无需闭环）；
- `cash`：`cash_diff`（柜台现金与虚拟现金不一致，扣在途冻结后）；
- `equity`：`equity_deviation`（真实总资产 vs 虚拟净值偏差 > 20%）。

状态机：`pending` → `accepted`（以柜台为准，写合成成交改虚拟账）/ `resolved`
（以平台为准，仅留痕）/ `ignored`。同一 `(session, diff_type, stock_code)`
未关闭时复用同一行（更新 actual/delta/last_seen_at），不为每次刷新刷一行。
"""
from sqlalchemy import (
    Column, DateTime, ForeignKey, Index, Integer, Numeric, String, Text,
)
from sqlalchemy.sql import func

from .base import Base

DIFF_POSITION = "position"
DIFF_CASH = "cash"
DIFF_EQUITY = "equity"

DIFF_STATUS_PENDING = "pending"
DIFF_STATUS_ACCEPTED = "accepted"
DIFF_STATUS_RESOLVED = "resolved"
DIFF_STATUS_IGNORED = "ignored"


class LiveReconciliationDiff(Base):
    __tablename__ = "live_reconciliation_diffs"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False)
    portfolio_strategy_id = Column(Integer, nullable=True)
    # position / cash / equity
    diff_type = Column(String(16), nullable=False)
    stock_code = Column(String(16), nullable=True)
    # 期望（虚拟账本口径）与实际（柜台口径），金额或股数按 diff_type 解释
    expected = Column(Numeric(18, 4), nullable=True)
    actual = Column(Numeric(18, 4), nullable=True)
    delta = Column(Numeric(18, 4), nullable=True)
    # pending / accepted / resolved / ignored
    status = Column(String(12), nullable=False, server_default="pending")
    resolution = Column(String(200), nullable=True)
    detail = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(),
                        onupdate=func.now())

    __table_args__ = (
        Index("ix_lrd_session_status", "live_session_id", "status"),
        Index("ix_lrd_session_type_code",
              "live_session_id", "diff_type", "stock_code"),
    )
