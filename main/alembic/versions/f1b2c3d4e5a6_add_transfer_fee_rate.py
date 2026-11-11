"""add portfolio_strategies.transfer_fee_rate（回测过户费，双向）

Revision ID: f1b2c3d4e5a6
Revises: e0a1b2c3d4f5
Create Date: 2026-11-11

A 股过户费现行按成交额 0.001% 双边收取。server_default 0.000010 让历史组合
立即采用新口径（回测成本含费）；如需保持旧结果，把该组合显式置 0 后重跑。
"""
from alembic import op
import sqlalchemy as sa


revision = "f1b2c3d4e5a6"
down_revision = "e0a1b2c3d4f5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "portfolio_strategies",
        sa.Column(
            "transfer_fee_rate", sa.Numeric(8, 6),
            nullable=False, server_default="0.000010",
        ),
    )


def downgrade() -> None:
    op.drop_column("portfolio_strategies", "transfer_fee_rate")
