"""实盘资金池影子表（0021 F3）

Revision ID: x0c1d2e3f4a5
Revises: w9b0c1d2e3f4
Create Date: 2026-11-20

只加新表，不改既有表：影子记录独立于 LiveDecisionEvent / LiveOrder / LiveTrade，
保证订单与既有事件零变化。
"""
from alembic import op
import sqlalchemy as sa


revision = "x0c1d2e3f4a5"
down_revision = "w9b0c1d2e3f4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_capital_shadow",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(), nullable=False),
        sa.Column("portfolio_id", sa.Integer(), nullable=False),
        sa.Column("bar_time", sa.DateTime(), nullable=True),
        sa.Column("payload_json", sa.Text(), nullable=True),
        sa.Column("version", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index(
        "ix_live_capital_shadow_live_session_id",
        "live_capital_shadow", ["live_session_id"],
    )
    op.create_index(
        "ix_live_capital_shadow_portfolio_id",
        "live_capital_shadow", ["portfolio_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_live_capital_shadow_portfolio_id",
                  table_name="live_capital_shadow")
    op.drop_index("ix_live_capital_shadow_live_session_id",
                  table_name="live_capital_shadow")
    op.drop_table("live_capital_shadow")
