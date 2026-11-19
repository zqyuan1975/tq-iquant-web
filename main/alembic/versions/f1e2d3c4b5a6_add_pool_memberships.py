"""add stock pool memberships (J7) + portfolio pool_pit_enabled

Revision ID: f1e2d3c4b5a6
Revises: f0e1d2c3b4a6
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "f1e2d3c4b5a6"
down_revision = "f0e1d2c3b4a6"
branch_labels = None
depends_on = None

_BACKFILL_FROM = "1970-01-01"


def upgrade() -> None:
    op.create_table(
        "stock_pool_memberships",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "pool_id", sa.Integer(),
            sa.ForeignKey("stock_pools.id", ondelete="CASCADE"), nullable=False),
        sa.Column("stock_code", sa.String(length=20), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("source", sa.String(length=20), nullable=False,
                  server_default="manual"),
        sa.Column("note", sa.String(length=200), nullable=True),
        sa.UniqueConstraint(
            "pool_id", "stock_code", "effective_from",
            name="uq_stock_pool_memberships_key"),
    )
    op.create_index(
        "ix_stock_pool_memberships_pool_code", "stock_pool_memberships",
        ["pool_id", "stock_code"])
    op.create_index(
        "ix_stock_pool_memberships_pool_dates", "stock_pool_memberships",
        ["pool_id", "effective_from"])

    with op.batch_alter_table("portfolio_strategies") as batch:
        batch.add_column(sa.Column(
            "pool_pit_enabled", sa.Boolean(), nullable=False,
            server_default="0"))

    # 回填：现有成分作为「一直有效」的开区间记录，保证开关打开后 covered() 立即为真
    # （否则历史区间全未被覆盖 → 只能 fail-open，开关等于没开）。
    op.execute(
        "INSERT INTO stock_pool_memberships "
        "(pool_id, stock_code, effective_from, effective_to, source) "
        "SELECT pool_id, stock_code, DATE('%s'), NULL, 'backfill' "
        "FROM stock_pool_stocks" % _BACKFILL_FROM
    )


def downgrade() -> None:
    with op.batch_alter_table("portfolio_strategies") as batch:
        batch.drop_column("pool_pit_enabled")
    op.drop_index("ix_stock_pool_memberships_pool_dates",
                  table_name="stock_pool_memberships")
    op.drop_index("ix_stock_pool_memberships_pool_code",
                  table_name="stock_pool_memberships")
    op.drop_table("stock_pool_memberships")
