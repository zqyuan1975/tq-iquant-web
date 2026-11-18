"""H5 委托详情进度：live_orders 新增拆单分组/序号/总数与追价次数

Revision ID: h5a1b2c3d4e5
Revises: h3a1b2c3d4e5
Create Date: 2026-11-18

全部可空：只有真实发生的拆单子单/追价单会带值，普通委托为 NULL，
前端按空值不展示进度，行为与接线前一致。
"""
from alembic import op
import sqlalchemy as sa


revision = "h5a1b2c3d4e5"
down_revision = "h3a1b2c3d4e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("live_orders") as batch:
        batch.add_column(sa.Column("split_group", sa.String(64), nullable=True))
        batch.add_column(sa.Column("split_seq", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("split_total", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("requote_attempt", sa.Integer(), nullable=True))
        batch.create_index("ix_live_orders_split_group", ["split_group"])


def downgrade() -> None:
    with op.batch_alter_table("live_orders") as batch:
        batch.drop_index("ix_live_orders_split_group")
        batch.drop_column("requote_attempt")
        batch.drop_column("split_total")
        batch.drop_column("split_seq")
        batch.drop_column("split_group")
