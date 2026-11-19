"""快照写入 L2 借据建模版本（资金池 0021 B3）

Revision ID: r3c4d5e6f7a8
Revises: q2b3c4d5e6f7
Create Date: 2026-11-20

`backtest_daily_snapshots.loan_model_version`：仅 loan_model 建模成立的 run 有值，
默认关闭/旧记录为 NULL。

只加可空列、不回填、不改既有列语义；回退直接 drop。
"""
from alembic import op
import sqlalchemy as sa


revision = "r3c4d5e6f7a8"
down_revision = "q2b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_daily_snapshots",
        sa.Column("loan_model_version", sa.String(40), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("backtest_daily_snapshots", "loan_model_version")
