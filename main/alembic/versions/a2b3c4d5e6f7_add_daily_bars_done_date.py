"""add daily_bars_done_date to live_sessions

15:05「收盘落定」日线级别轮次（1d/1w/1mon）的跨重启幂等标记。

Revision ID: a2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-11-09
"""
from alembic import op
import sqlalchemy as sa


revision = "a2b3c4d5e6f7"
down_revision = "f1a2b3c4d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_sessions",
        sa.Column("daily_bars_done_date", sa.String(length=10), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("live_sessions", "daily_bars_done_date")
