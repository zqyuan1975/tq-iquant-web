"""M1 approval queue + I1 manual action audit

Revision ID: f3a4b5c6d7e8
Revises: f2e3d4c5b6a7
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "f3a4b5c6d7e8"
down_revision = "f2e3d4c5b6a7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_approval_queue",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(), nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=False),
        sa.Column("strategy_id", sa.Integer(), nullable=False),
        sa.Column("stock_code", sa.String(length=20), nullable=False),
        sa.Column("trade_type", sa.String(length=4), nullable=False),
        sa.Column("price", sa.Numeric(10, 3), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("signal_type", sa.String(length=15), nullable=True),
        sa.Column("signal_name", sa.String(length=50), nullable=True),
        sa.Column("period", sa.String(length=10), nullable=True),
        sa.Column("bar_time", sa.DateTime(), nullable=True),
        sa.Column("signal_bar_date", sa.Date(), nullable=True),
        sa.Column("status", sa.String(length=12), nullable=False,
                  server_default="pending"),
        sa.Column("reason", sa.String(length=200), nullable=True),
        sa.Column("decided_at", sa.DateTime(), nullable=True),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(),
                  nullable=False),
        sa.ForeignKeyConstraint(
            ["live_session_id"], ["live_sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["portfolio_strategy_id"], ["portfolio_strategies.id"],
            ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["strategy_id"], ["strategies.id"]),
    )
    op.create_index("ix_laq_session_status", "live_approval_queue",
                    ["live_session_id", "status"])
    op.create_index("ix_laq_expires", "live_approval_queue", ["expires_at"])

    op.create_table(
        "live_manual_actions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(), nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=False),
        sa.Column("action_type", sa.String(length=16), nullable=False),
        sa.Column("stock_code", sa.String(length=20), nullable=True),
        sa.Column("trade_type", sa.String(length=4), nullable=True),
        sa.Column("quantity", sa.Integer(), nullable=True),
        sa.Column("price", sa.Numeric(10, 3), nullable=True),
        sa.Column("cash_delta", sa.Numeric(15, 2), nullable=True),
        sa.Column("result", sa.String(length=16), nullable=False),
        sa.Column("live_order_id", sa.Integer(), nullable=True),
        sa.Column("reason", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now(),
                  nullable=False),
        sa.ForeignKeyConstraint(
            ["live_session_id"], ["live_sessions.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["portfolio_strategy_id"], ["portfolio_strategies.id"],
            ondelete="RESTRICT"),
    )
    op.create_index("ix_lma_session_time", "live_manual_actions",
                    ["live_session_id", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_lma_session_time", table_name="live_manual_actions")
    op.drop_table("live_manual_actions")
    op.drop_index("ix_laq_expires", table_name="live_approval_queue")
    op.drop_index("ix_laq_session_status", table_name="live_approval_queue")
    op.drop_table("live_approval_queue")
