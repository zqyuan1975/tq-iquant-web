from sqlalchemy import (
    Boolean, Column, Date, DateTime, ForeignKey, Index, Integer, Numeric,
    String, Text,
)
from sqlalchemy.sql import func

from .base import Base


class CashOutlet(Base):
    """现金出口白名单（0020 §9.2 / §13.2 C1–C8）。

    只登记「通道、期限、限额、风险、验证状态」等配置与证据，**不产生任何
    委托、划转或收益**；未验证/过期的出口在判据里一律 ``OUTLET_NOT_VERIFIED``。
    首期只接受 T+0 或到期 ≤7 天、可确定回流的品种（券商现金理财默认排除）。
    """

    __tablename__ = "cash_outlets"

    id = Column(Integer, primary_key=True)
    code = Column(String(20), nullable=False, unique=True)
    name = Column(String(100), nullable=False)
    # repo=交易所逆回购 / etf_money=货基 ETF / other
    category = Column(String(20), nullable=False, default="repo",
                      server_default="repo")
    # C1 通道：tq / qmt / gj / manual
    channel = Column(String(20), nullable=False, default="manual",
                     server_default="manual")
    channel_order_enabled = Column(Boolean, nullable=False, default=False,
                                   server_default="0")

    # C2 收益率数据
    yield_source = Column(String(100), nullable=True)
    yield_freq = Column(String(10), nullable=True)  # daily|weekly|quote
    yield_history_days = Column(Integer, nullable=True)

    # C3 流动性：t0 / t1 / term
    liquidity = Column(String(10), nullable=False, default="t1",
                       server_default="t1")
    term_days = Column(Integer, nullable=True)          # term 品种的到期天数
    settlement_max_days = Column(Integer, nullable=False, default=1,
                                 server_default="1")
    holiday_postpone = Column(Boolean, nullable=False, default=True,
                              server_default="1")

    # C4 限额（金额，元）
    min_amount = Column(Numeric(18, 2), nullable=True)
    max_single_amount = Column(Numeric(18, 2), nullable=True)
    max_daily_amount = Column(Numeric(18, 2), nullable=True)
    min_increment = Column(Numeric(18, 2), nullable=True)

    # C5 赎回/提前支取
    redeemable_early = Column(Boolean, nullable=False, default=False,
                              server_default="0")
    early_redeem_penalty = Column(Numeric(6, 4), nullable=True)
    auto_mature_return = Column(Boolean, nullable=False, default=True,
                                server_default="1")
    fast_redeem_limit = Column(Numeric(18, 2), nullable=True)

    # C6 风险
    risk_level = Column(String(10), nullable=True)   # R1/R2...
    principal_guaranteed = Column(Boolean, nullable=False, default=False,
                                  server_default="0")

    # C7 回流确定性
    arrival_buffer_days = Column(Integer, nullable=False, default=0,
                                 server_default="0")
    inflow_certainty = Column(String(20), nullable=True)  # certain|probable

    # C8 通道验证（小额实盘证据）
    verified = Column(Boolean, nullable=False, default=False,
                      server_default="0")
    verified_at = Column(DateTime, nullable=True)
    verify_amount = Column(Numeric(18, 2), nullable=True)
    verify_result = Column(String(200), nullable=True)
    verify_expires_at = Column(Date, nullable=True)

    status = Column(String(10), nullable=False, default="active",
                    server_default="active")
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(),
                        onupdate=func.now())


class CashOutletOp(Base):
    """现金出口执行台账（0021 G4）。

    只记录「计划 → 人工确认 → 已投放 → 已回流」的运维事实，**不写组合/策略
    现金与净值**：出口收益单独记在本表 ``yield_amount``，避免污染策略归因。
    ``shortfall_amount`` 与 ``shortfall_reason`` 保证少还/晚还可追溯。

    模式：``P1`` 纯人工（逐笔确认）、``P2`` 半自动（服务计划 + 人工确认）、
    ``P3`` 自动（仅限已验证、T+0/≤7 天、限额内的出口，服务直接放行）。
    """

    __tablename__ = "cash_outlet_ops"

    id = Column(Integer, primary_key=True)
    live_session_id = Column(
        Integer, ForeignKey("live_sessions.id", ondelete="SET NULL"),
        nullable=True, index=True)
    outlet_id = Column(
        Integer, ForeignKey("cash_outlets.id", ondelete="RESTRICT"),
        nullable=False, index=True)
    mode = Column(String(5), nullable=False, default="P1",
                  server_default="P1")
    amount = Column(Numeric(18, 2), nullable=False)
    status = Column(String(20), nullable=False, default="PENDING_CONFIRM",
                    server_default="PENDING_CONFIRM")
    idempotency_key = Column(String(80), nullable=True, unique=True)

    executed_at = Column(DateTime, nullable=True)
    expected_return_date = Column(Date, nullable=True)
    returned_at = Column(DateTime, nullable=True)
    principal_returned = Column(Numeric(18, 2), nullable=True)
    # 出口收益单列；不进入组合现金/净值（不污染策略归因）
    yield_amount = Column(Numeric(18, 2), nullable=False, default=0,
                          server_default="0")
    shortfall_amount = Column(Numeric(18, 2), nullable=False, default=0,
                              server_default="0")
    shortfall_reason = Column(String(200), nullable=True)

    approved_by = Column(String(50), nullable=True)
    approved_at = Column(DateTime, nullable=True)
    notes = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now(),
                        onupdate=func.now())

    __table_args__ = (
        Index("ix_cash_outlet_ops_session_status",
              "live_session_id", "status"),
        Index("ix_cash_outlet_ops_outlet_executed",
              "outlet_id", "executed_at"),
    )
