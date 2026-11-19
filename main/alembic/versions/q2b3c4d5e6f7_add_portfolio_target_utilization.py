"""组合级目标仓位上限（资金池 0021 A 补充）

Revision ID: q2b3c4d5e6f7
Revises: p1a2b3c4d5e6
Create Date: 2026-11-20

`portfolio_strategies.target_utilization`：组合级可配置，取值 (0,1]；
NULL 表示用默认 0.95（0019 §1.7），推导结果与加列前完全一致。

只加可空列、不回填、不改既有列语义；回退直接 drop。
"""
from alembic import op
import sqlalchemy as sa


revision = "q2b3c4d5e6f7"
down_revision = "p1a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "portfolio_strategies",
        sa.Column("target_utilization", sa.Numeric(5, 4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("portfolio_strategies", "target_utilization")
