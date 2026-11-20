"""实盘会话绑定拼盘/运行组（资金池 0021 F1）

Revision ID: w9b0c1d2e3f4
Revises: v8a9b0c1d2e3
Create Date: 2026-11-20

`live_sessions` 新增两个可空软引用列，与既有 `backtest_record_id` 三选一：
单记录 / 多组合运行组 / L2·L3 拼盘。只加可空列、不回填、不改既有列语义；
历史会话两列为 NULL，行为与升级前一致。
"""
from alembic import op
import sqlalchemy as sa


revision = "w9b0c1d2e3f4"
down_revision = "v8a9b0c1d2e3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_sessions",
        sa.Column("backtest_run_group_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "live_sessions",
        sa.Column("pairing_plan_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("live_sessions", "pairing_plan_id")
    op.drop_column("live_sessions", "backtest_run_group_id")
