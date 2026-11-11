"""add backtest_trades.transfer_fee（回测过户费留存）

Revision ID: a2c3d4e5f6b7
Revises: f1b2c3d4e5a6
Create Date: 2026-11-11

server_default 0：历史成交行没有过户费数据，保持 0（旧结果口径不变）；
新回测写入 SimulatedDispatcher 计算的 transfer_fee。
"""
from alembic import op
import sqlalchemy as sa


revision = "a2c3d4e5f6b7"
down_revision = "f1b2c3d4e5a6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_trades",
        sa.Column(
            "transfer_fee", sa.Numeric(10, 2),
            nullable=False, server_default="0",
        ),
    )


def downgrade() -> None:
    op.drop_column("backtest_trades", "transfer_fee")
