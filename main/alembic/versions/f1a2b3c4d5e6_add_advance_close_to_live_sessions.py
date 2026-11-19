"""add advance close fields to live_sessions

14:50 提前收盘价（仅 1d）：会话级开关 + 执行时点 + 当日幂等标记。

Revision ID: f1a2b3c4d5e6
Revises: e9f0a1b2c3d4
Create Date: 2026-11-08
"""
from alembic import op
import sqlalchemy as sa


revision = "f1a2b3c4d5e6"
down_revision = "e9f0a1b2c3d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_sessions",
        sa.Column("advance_close_enabled", sa.Boolean(), nullable=False,
                  server_default="0"),
    )
    op.add_column(
        "live_sessions",
        sa.Column("advance_close_time", sa.String(length=5), nullable=False,
                  server_default="14:51"),
    )
    op.add_column(
        "live_sessions",
        sa.Column("advance_close_done_date", sa.String(length=10), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("live_sessions", "advance_close_done_date")
    op.drop_column("live_sessions", "advance_close_time")
    op.drop_column("live_sessions", "advance_close_enabled")
