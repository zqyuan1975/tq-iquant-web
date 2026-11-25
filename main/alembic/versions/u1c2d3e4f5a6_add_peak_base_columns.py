"""峰值基准列：live_position_peaks.peak_base_price / peak_base_at。

Revision ID: u1c2d3e4f5a6
Revises: tq8j265f8g9
Create Date: 2026-11-25

两列均可空：NULL = 尚未回填，补种时按持仓成本/买入时刻回填。
峰值补种口径同时收敛为「表值 → 基准价」，不再用策略 bar / 日线 / 快照 MAX 回补。
"""
from alembic import op
import sqlalchemy as sa


revision = "u1c2d3e4f5a6"
down_revision = "tq8j265f8g9"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("live_position_peaks") as batch:
        # 峰值基准价（入场价/成本）：补种只从这里起算
        batch.add_column(sa.Column(
            "peak_base_price", sa.Numeric(18, 4), nullable=True))
        # 峰值基准时刻（入场时刻）
        batch.add_column(sa.Column("peak_base_at", sa.DateTime(), nullable=True))


def downgrade():
    with op.batch_alter_table("live_position_peaks") as batch:
        batch.drop_column("peak_base_at")
        batch.drop_column("peak_base_price")
