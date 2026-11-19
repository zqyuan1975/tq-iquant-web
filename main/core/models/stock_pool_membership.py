from sqlalchemy import (
    Column, Date, ForeignKey, Index, Integer, String, UniqueConstraint,
)

from .base import Base


class StockPoolMembership(Base):
    """股票池「时点成分」（J7，2026-11-18）。

    与 ``stock_pool_stocks``（当前成分，仅一行）不同，本表保留**生效区间**：
    ``effective_from`` 起、``effective_to``（含）止，``NULL`` = 仍在池内。
    回测按交易日取当日有效成分，避免用今天的池子回溯历史（幸存者偏差/前视偏差）。

    默认不启用：组合 ``pool_pit_enabled=0`` 时不读本表，选股范围仍是
    ``stock_pool_stocks``，行为与改动前逐字一致。手工维护优先，通达信同步只
    「新增→开区间、消失→关闭区间」，不重开历史已关闭记录。
    """

    __tablename__ = "stock_pool_memberships"

    id = Column(Integer, primary_key=True)
    pool_id = Column(
        Integer, ForeignKey("stock_pools.id", ondelete="CASCADE"), nullable=False)
    stock_code = Column(String(20), nullable=False)
    effective_from = Column(Date, nullable=False)
    effective_to = Column(Date, nullable=True)
    # manual = 手工维护；sync = 通达信同步（回填为 backfill）
    source = Column(String(20), nullable=False, default="manual")
    note = Column(String(200), nullable=True)

    __table_args__ = (
        UniqueConstraint(
            "pool_id", "stock_code", "effective_from",
            name="uq_stock_pool_memberships_key"),
        Index("ix_stock_pool_memberships_pool_code", "pool_id", "stock_code"),
        Index("ix_stock_pool_memberships_pool_dates", "pool_id", "effective_from"),
    )
