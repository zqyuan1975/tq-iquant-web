"""回测批绑定 L2 拼盘头（资金池 0021 E3）

Revision ID: v8a9b0c1d2e3
Revises: u7f8a9b0c1d2
Create Date: 2026-11-20

`backtest_run_groups.pairing_plan_id`：仅从 L2 拼盘发起的多组合回测有值，
普通多组合回测为 NULL。只加可空列、不回填、不改既有列语义。
"""
from alembic import op
import sqlalchemy as sa


revision = "v8a9b0c1d2e3"
down_revision = "u7f8a9b0c1d2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_run_groups",
        sa.Column("pairing_plan_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("backtest_run_groups", "pairing_plan_id")
