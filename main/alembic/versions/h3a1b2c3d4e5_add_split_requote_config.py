"""H3 切片增强 + H4 自适应追价：strategies 新增执行配置列

Revision ID: h3a1b2c3d4e5
Revises: g1h2i3j4k5l6
Create Date: 2026-11-18

全部可空/带默认值：split_order_count/min_qty/interval/volume_pct 为 NULL，
requote_price_mode='tick'（原 ±1 tick 行为），requote_max_slippage_ticks=5。
不配置时行为与改动前逐字一致。
"""
from alembic import op
import sqlalchemy as sa


revision = "h3a1b2c3d4e5"
down_revision = "g1h2i3j4k5l6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("strategies") as batch:
        batch.add_column(sa.Column("requote_price_mode", sa.String(16),
                                   nullable=False, server_default="tick"))
        batch.add_column(sa.Column("requote_max_slippage_ticks", sa.Integer(),
                                   nullable=False, server_default="5"))
        batch.add_column(sa.Column("split_order_count", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("split_order_min_qty", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("split_order_interval_seconds", sa.Float(),
                                   nullable=True))
        batch.add_column(sa.Column("split_order_interval_jitter", sa.Boolean(),
                                   nullable=False, server_default="0"))
        batch.add_column(sa.Column("split_order_volume_pct", sa.Float(),
                                   nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("strategies") as batch:
        batch.drop_column("split_order_volume_pct")
        batch.drop_column("split_order_interval_jitter")
        batch.drop_column("split_order_interval_seconds")
        batch.drop_column("split_order_min_qty")
        batch.drop_column("split_order_count")
        batch.drop_column("requote_max_slippage_ticks")
        batch.drop_column("requote_price_mode")
