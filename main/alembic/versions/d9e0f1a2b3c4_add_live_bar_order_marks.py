"""add live_bar_order_marks（同一根 bar 只下一次单的持久幂等标记）

Revision ID: d9e0f1a2b3c4
Revises: b6c7d8e9f0a1
Create Date: 2026-11-11

`live_orders` 上的同 bar 同向门只在原单 submitted/partial/filled 时生效；原单
canceled/rejected 后重启重驱同一根 bar 会再次下单。本表只记「这根 bar 这个方向
已下过单」，不看订单终态，唯一索引保证并发下不漏。
"""
from alembic import op
import sqlalchemy as sa


revision = "d9e0f1a2b3c4"
down_revision = "b6c7d8e9f0a1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_bar_order_marks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "live_session_id", sa.Integer(),
            sa.ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=False),
        sa.Column("strategy_id", sa.Integer(), nullable=False),
        sa.Column("stock_code", sa.String(20), nullable=False),
        sa.Column("bar_time", sa.DateTime(), nullable=False),
        sa.Column("direction", sa.String(4), nullable=False),
        sa.Column("seq", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index(
        "ux_live_bar_order_marks_key",
        "live_bar_order_marks",
        ["live_session_id", "portfolio_strategy_id", "strategy_id",
         "stock_code", "bar_time", "direction", "seq"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ux_live_bar_order_marks_key", table_name="live_bar_order_marks")
    op.drop_table("live_bar_order_marks")
