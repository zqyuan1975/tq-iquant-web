"""投资看板（只读）快照表。

四张表只承载「展示用快照」，不参与交易/门控/策略判定：

- ``board_industries``：通达信二级行业（881xxx，严格 128 个）名单快照；
- ``board_runs``：一次刷新任务的状态与统计（running/ok/partial/failed）；
- ``board_industry_snapshots``：行业相对大盘的评级快照（日线 / 30m）；
- ``board_stock_snapshots``：行业成分股相对行业指数的评级快照。

公式口径见 ``core.engine.rating_core``（tqgate_py_v2）。
"""
from sqlalchemy import (
    Column, Date, DateTime, Float, Index, Integer, Text, UniqueConstraint,
)
from sqlalchemy.sql import func

from .base import Base


class BoardIndustry(Base):
    __tablename__ = "board_industries"

    id = Column(Integer, primary_key=True)
    code = Column(Text, nullable=False, unique=True)
    name = Column(Text, nullable=True)
    active = Column(Integer, nullable=False, server_default="1")
    first_seen = Column(Date, nullable=True)
    last_seen = Column(Date, nullable=True)
    source = Column(Text, nullable=True)


class BoardRun(Base):
    __tablename__ = "board_runs"

    id = Column(Integer, primary_key=True)
    period = Column(Text, nullable=False)
    scope = Column(Text, nullable=False)
    as_of = Column(Date, nullable=True)
    trigger = Column(Text, nullable=True)
    status = Column(Text, nullable=False, server_default="running")
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    total = Column(Integer, nullable=False, server_default="0")
    ok = Column(Integer, nullable=False, server_default="0")
    skipped = Column(Integer, nullable=False, server_default="0")
    error = Column(Text, nullable=True)
    stats_json = Column(Text, nullable=True)


class BoardIndustrySnapshot(Base):
    __tablename__ = "board_industry_snapshots"
    __table_args__ = (
        UniqueConstraint("as_of", "period", "industry_code",
                         name="uq_bis_asof_period_code"),
        Index("ix_bis_period_asof", "period", "as_of"),
        Index("ix_bis_code_asof", "industry_code", "as_of"),
    )

    id = Column(Integer, primary_key=True)
    as_of = Column(Date, nullable=False)
    period = Column(Text, nullable=False)
    industry_code = Column(Text, nullable=False)
    industry_name = Column(Text, nullable=True)
    direction = Column(Integer, nullable=True)
    strength = Column(Float, nullable=True)
    level = Column(Text, nullable=True)
    bull_dur = Column(Integer, nullable=True)
    bear_dur = Column(Integer, nullable=True)
    bias = Column(Float, nullable=True)
    bias_z = Column(Float, nullable=True)
    overbought = Column(Integer, nullable=False, server_default="0")
    oversold = Column(Integer, nullable=False, server_default="0")
    jd = Column(Float, nullable=True)
    jd_filter = Column(Integer, nullable=False, server_default="0")
    channel_filter = Column(Integer, nullable=False, server_default="0")
    up_str = Column(Float, nullable=True)
    dn_str = Column(Float, nullable=True)
    bars = Column(Integer, nullable=True)
    stale = Column(Integer, nullable=False, server_default="0")
    bar_closed_at = Column(Text, nullable=True)
    effective_from = Column(Text, nullable=True)
    computed_at = Column(DateTime, server_default=func.now())
    run_id = Column(Integer, nullable=True)


class BoardStockSnapshot(Base):
    __tablename__ = "board_stock_snapshots"
    __table_args__ = (
        UniqueConstraint("as_of", "period", "industry_code", "stock_code",
                         name="uq_bss_asof_period_ind_code"),
        Index("ix_bss_asof_period", "as_of", "period"),
        Index("ix_bss_ind_asof", "industry_code", "as_of"),
    )

    id = Column(Integer, primary_key=True)
    as_of = Column(Date, nullable=False)
    period = Column(Text, nullable=False)
    industry_code = Column(Text, nullable=False)
    stock_code = Column(Text, nullable=False)
    stock_name = Column(Text, nullable=True)
    direction = Column(Integer, nullable=True)
    strength = Column(Float, nullable=True)
    level = Column(Text, nullable=True)
    bull_dur = Column(Integer, nullable=True)
    bear_dur = Column(Integer, nullable=True)
    lead_days = Column(Integer, nullable=True)
    bias = Column(Float, nullable=True)
    bias_z = Column(Float, nullable=True)
    overbought = Column(Integer, nullable=False, server_default="0")
    oversold = Column(Integer, nullable=False, server_default="0")
    bars = Column(Integer, nullable=True)
    stale = Column(Integer, nullable=False, server_default="0")
    bar_closed_at = Column(Text, nullable=True)
    effective_from = Column(Text, nullable=True)
    computed_at = Column(DateTime, server_default=func.now())
    run_id = Column(Integer, nullable=True)
