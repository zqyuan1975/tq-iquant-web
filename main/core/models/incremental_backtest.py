"""接缝续跑式增量回测的持久化模型。

三个表：
- incremental_backtest_checkpoints：每（会话, 组合策略）一份续跑内存态。
- incremental_backtest_days：续跑 vs 实盘的逐日对照行。
- incremental_backtest_validations：双跑校验（从接缝日全量重算对账）结论。
"""
from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Integer, Numeric, String,
    Text, UniqueConstraint,
)
from sqlalchemy.sql import func

from .base import Base


class IncrementalBacktestCheckpoint(Base):
    __tablename__ = "incremental_backtest_checkpoints"

    id = Column(Integer, primary_key=True)
    session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True)
    portfolio_strategy_id = Column(
        Integer, ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
        nullable=False, index=True)
    # 接缝日（该组合进入实盘、回测起跑锚定的交易日）
    seam_date = Column(Date, nullable=False)
    # 最后已处理的交易日（= 下一轮续跑的起点前一天）
    as_of_date = Column(Date, nullable=False)
    # 完整内存态 JSON（现金/持仓/风控/价格快照/在途单/已处理 bar）
    state_json = Column(Text, nullable=False)
    # 参数快照哈希（build_params_snapshot 的 sha256）
    params_hash = Column(String(64), nullable=False)
    # 口径版本（成本/成交价/复权/T+1/引擎任一变即作废）
    schema_version = Column(Integer, nullable=False, default=1)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("session_id", "portfolio_strategy_id"),
    )


class IncrementalBacktestDay(Base):
    __tablename__ = "incremental_backtest_days"

    id = Column(Integer, primary_key=True)
    session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True)
    portfolio_strategy_id = Column(
        Integer, ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
        nullable=False, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    # 续跑侧
    inc_total_value = Column(Numeric(18, 2), nullable=True)
    inc_cash = Column(Numeric(18, 2), nullable=True)
    inc_market_value = Column(Numeric(18, 2), nullable=True)
    # 实盘侧（接缝基线 + live_trades 逐日重放估值）
    live_total_value = Column(Numeric(18, 2), nullable=True)
    live_cash = Column(Numeric(18, 2), nullable=True)
    live_market_value = Column(Numeric(18, 2), nullable=True)
    # 相对偏差（%，|inc-live| / max(live,1) × 100）
    net_dev_pct = Column(Numeric(10, 4), nullable=True)
    inc_trades_count = Column(Integer, nullable=True)
    live_trades_count = Column(Integer, nullable=True)
    # 续跑侧当日成交方向列表 JSON（["000001.SZ:BUY", ...]），供双跑校验与
    # 「全量重算自身成交方向」逐日比较（NULL = 旧行，该判据不参与）。
    inc_dirs_json = Column(Text, nullable=True)
    direction_match = Column(Boolean, nullable=True)
    # ok / warn / gap
    status = Column(String(10), nullable=False, default="ok", server_default="ok")
    # JSON 数组：["gap","net_dev","trade_mismatch","direction_mismatch","validation_fail"]
    tags_json = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        UniqueConstraint("session_id", "portfolio_strategy_id", "trade_date"),
    )


class IncrementalBacktestValidation(Base):
    __tablename__ = "incremental_backtest_validations"

    id = Column(Integer, primary_key=True)
    session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="CASCADE"),
        nullable=False, index=True)
    portfolio_strategy_id = Column(
        Integer, ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
        nullable=False, index=True)
    from_date = Column(Date, nullable=False)
    to_date = Column(Date, nullable=False)
    # pass / fail / error
    status = Column(String(10), nullable=False)
    max_net_dev_pct = Column(Numeric(10, 4), nullable=True)
    mean_net_dev_pct = Column(Numeric(10, 4), nullable=True)
    trade_count_match_ratio = Column(Numeric(6, 4), nullable=True)
    direction_match_ratio = Column(Numeric(6, 4), nullable=True)
    max_drawdown_dev_pct = Column(Numeric(10, 4), nullable=True)
    first_divergence_date = Column(Date, nullable=True)
    # 本次判据（含阈值与依据）
    criteria_json = Column(Text, nullable=True)
    # 逐日明细摘要
    detail_json = Column(Text, nullable=True)
    created_at = Column(DateTime, server_default=func.now())
