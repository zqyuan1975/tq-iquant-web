from sqlalchemy import (
    Boolean, Column, Date, DateTime, Integer, Numeric, String, Text,
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
