"""决策闸门事件表（调参可观测性）。

信号→成交链路上每个闸门触发（风控止损/熔断/丢单/拒单/缩量/压制）落一行，
回测、实盘各一张表，随父记录级联删除。是调参时「这个参数实际触发了几次、
什么时点、偏离阈值多少、拦掉多少量」的权威数据源。

strategy_id 用普通 Integer（不加 FK）：组合层熔断事件 strategy_id 为空，且事件
属分析日志，不应被策略删除牵连。
"""
from sqlalchemy import Column, Integer, String, ForeignKey, Float, DateTime, Index
from sqlalchemy.sql import func

from .base import Base


class _DecisionEventColumns:
    """两张表共享列（Mixin）。"""

    # 闸门码：stop_loss/take_profit/trailing_stop/max_drawdown/daily_loss/halted_buy_strip/
    # max_positions_full/insufficient_funds/order_shrunk/t1_clamp/inflight_skip ...
    gate = Column(String(40), nullable=False)
    # 层：strategy_risk/portfolio_risk/signal_gate/capital_gate/t1/live_gate
    layer = Column(String(20), nullable=False)
    # 动作：trigger/halt/recover/strip/block/shrink/clamp/reject
    action = Column(String(20), nullable=False)
    portfolio_id = Column(Integer, nullable=True)
    strategy_id = Column(Integer, nullable=True, index=True)
    stock_code = Column(String(20), nullable=True)
    bar_time = Column(DateTime, nullable=True)
    # 关联调参名（max_positions/stop_loss_ratio...）、阈值、实际值
    param_name = Column(String(40), nullable=True)
    param_value = Column(Float, nullable=True)
    actual_value = Column(Float, nullable=True)
    requested_qty = Column(Integer, nullable=True)
    final_qty = Column(Integer, nullable=True)
    message = Column(String(200), nullable=True)
    created_at = Column(DateTime, server_default=func.now())


class BacktestDecisionEvent(_DecisionEventColumns, Base):
    __tablename__ = "backtest_decision_events"

    id = Column(Integer, primary_key=True)
    backtest_record_id = Column(
        Integer, ForeignKey("backtest_records.id", ondelete="CASCADE"), nullable=False
    )

    __table_args__ = (
        Index("ix_bt_decisions_rec_gate", "backtest_record_id", "gate"),
        Index("ix_bt_decisions_rec_bar", "backtest_record_id", "bar_time"),
    )


class LiveDecisionEvent(_DecisionEventColumns, Base):
    __tablename__ = "live_decision_events"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False
    )
    # 事件处理状态：open（未处理）/ acked（人工确认）/ archived（过期自动归档）
    ack_status = Column(String(16), nullable=False, default="open",
                        server_default="open")
    ack_note = Column(String(200), nullable=True)
    ack_at = Column(DateTime, nullable=True)
    # 确认操作者（谁确认的）；归档另记 archived_at / archive_reason。
    ack_by = Column(String(64), nullable=True)
    archived_at = Column(DateTime, nullable=True)
    archive_reason = Column(String(200), nullable=True)

    __table_args__ = (
        Index("ix_live_decisions_sess_gate", "live_session_id", "gate"),
        Index("ix_live_decisions_sess_bar", "live_session_id", "bar_time"),
        Index("ix_live_decisions_sess_ack", "live_session_id", "ack_status"),
        # 未处理计数/批量确认共用：open 且未归档。
        Index("ix_live_decisions_ack_archived", "ack_status", "archived_at"),
        # 过期归档扫描：created_at 截止 + 状态。
        Index("ix_live_decisions_archive_scan", "archived_at", "created_at"),
    )


class LiveDecisionAckLog(Base):
    """决策事件处理审计（单条确认 / 批量确认 / 过期归档）。

    批量操作除逐事件一行外，另写一行 ``event_id IS NULL`` 的汇总行
    （``filters_json`` + ``affected_count``），保证「本次批量操作本身」可追溯。
    不设外键：事件随会话级联删除后，审计行仍需保留。
    """
    __tablename__ = "live_decision_ack_logs"

    id = Column(Integer, primary_key=True)
    # 同一次批量/归档共用一个 batch_id；单条确认为 NULL。
    batch_id = Column(String(36), nullable=True)
    live_session_id = Column(Integer, nullable=False)
    # NULL = 批量汇总行（非具体事件）。
    event_id = Column(Integer, nullable=True)
    # ack / archive
    action = Column(String(16), nullable=False)
    from_status = Column(String(16), nullable=True)
    to_status = Column(String(16), nullable=True)
    operator = Column(String(64), nullable=True)
    reason = Column(String(200), nullable=True)
    # 仅批量汇总行：筛选条件 JSON + 影响条数。
    filters_json = Column(String(1000), nullable=True)
    affected_count = Column(Integer, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("ix_live_ack_logs_session_created", "live_session_id", "created_at"),
        Index("ix_live_ack_logs_event", "event_id"),
        Index("ix_live_ack_logs_batch", "batch_id"),
    )
