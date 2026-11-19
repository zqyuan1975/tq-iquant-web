"""add portfolio_strategies.risk_cooldown_days（风控卖出后冷静期）

Revision ID: a7c8d9e0f1b2
Revises: d4f1a2b3c5e6
Create Date: 2026-11-12

止损/止盈/移动止损卖出成交后，同一 (策略, 代码) 在 N 个交易日内禁止新开仓
（当日算第 1 天）。server_default 1 = 「当日不再买回、次日恢复」；0 = 关闭
（等价改动前行为，需要旧结果可用时把该组合显式置 0）。
"""
from alembic import op
import sqlalchemy as sa


revision = "a7c8d9e0f1b2"
down_revision = "d4f1a2b3c5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "portfolio_strategies",
        sa.Column("risk_cooldown_days", sa.Integer(), nullable=False, server_default="1"),
    )


def downgrade() -> None:
    op.drop_column("portfolio_strategies", "risk_cooldown_days")
