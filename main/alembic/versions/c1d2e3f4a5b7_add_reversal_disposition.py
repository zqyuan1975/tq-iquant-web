"""策略级 14:50 反转处置档（规范 3.6）

Revision ID: c1d2e3f4a5b7
Revises: a0b1c2d3e4f5
Create Date: 2026-11-23

``strategies.reversal_disposition``：``hold``（默认，只标记+告警）/
``close_next_open``（反转标的次日首根 bar 按开盘价减出）。
仅影响 14:50 提前收盘价（pre_close_1450）轮次的收盘复核处置，不改变
1d/1w/1mon 默认信号判定与执行时点。
"""
from alembic import op
import sqlalchemy as sa


revision = "c1d2e3f4a5b7"
down_revision = "a0b1c2d3e4f5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "strategies",
        sa.Column("reversal_disposition", sa.String(20),
                  server_default="hold", nullable=False),
    )


def downgrade() -> None:
    with op.batch_alter_table("strategies") as batch:
        batch.drop_column("reversal_disposition")
