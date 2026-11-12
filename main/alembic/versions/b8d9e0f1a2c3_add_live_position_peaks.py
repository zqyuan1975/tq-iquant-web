"""add live_position_peaks（实盘持仓峰值逐 bar 持久化）

Revision ID: b8d9e0f1a2c3
Revises: a7c8d9e0f1b2
Create Date: 2026-11-12

方案 E：把 `Position.highest_price`（移动止损/止损的峰值基准）在**被抬高时**
UPSERT 落库，Core 重启后 `recover` 优先读表，不再依赖「日线 close 重算」；
清仓时删行，`updated_at` 超过 30 个交易日未更新的行在 recover 时清理。
键 = (live_session_id, portfolio_strategy_id, stock_code)，唯一索引保证并发
UPSERT 不产生重复行。回测不写本表。
"""
from alembic import op
import sqlalchemy as sa


revision = "b8d9e0f1a2c3"
down_revision = "a7c8d9e0f1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_position_peaks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "live_session_id",
            sa.Integer(),
            sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=False),
        sa.Column("stock_code", sa.String(length=20), nullable=False),
        sa.Column("highest_price", sa.Numeric(18, 4), nullable=False),
        sa.Column("peak_bar_time", sa.DateTime(), nullable=True),
        sa.Column(
            "updated_at", sa.DateTime(), nullable=False, server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ux_live_position_peaks_key",
        "live_position_peaks",
        ["live_session_id", "portfolio_strategy_id", "stock_code"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ux_live_position_peaks_key", table_name="live_position_peaks")
    op.drop_table("live_position_peaks")
