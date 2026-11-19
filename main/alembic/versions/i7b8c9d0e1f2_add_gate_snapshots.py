"""E2：新增 live_gate_snapshots（每日每组合一条门控快照）。

Revision ID: i7b8c9d0e1f2
Revises: i6a7b8c9d0e1
Create Date: 2026-11-18
"""
from alembic import op
import sqlalchemy as sa


revision = "i7b8c9d0e1f2"
down_revision = "i6a7b8c9d0e1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "live_gate_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=False),
        sa.Column("snap_date", sa.Date(), nullable=False),
        sa.Column("benchmark_index", sa.String(16), nullable=True),
        sa.Column("gate_engine", sa.String(16), nullable=True),
        sa.Column("direction", sa.Integer(), nullable=True),
        sa.Column("strength", sa.Integer(), nullable=True),
        sa.Column("as_of", sa.Date(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("live_session_id", "portfolio_strategy_id",
                            "snap_date", name="uq_lgs_session_portfolio_date"),
    )
    op.create_index("ix_live_gate_snapshots_live_session_id",
                    "live_gate_snapshots", ["live_session_id"])
    op.create_index("ix_live_gate_snapshots_portfolio_strategy_id",
                    "live_gate_snapshots", ["portfolio_strategy_id"])
    op.create_index("ix_live_gate_snapshots_snap_date",
                    "live_gate_snapshots", ["snap_date"])
    op.create_index("ix_lgs_session_date", "live_gate_snapshots",
                    ["live_session_id", "snap_date"])


def downgrade():
    op.drop_index("ix_lgs_session_date", table_name="live_gate_snapshots")
    op.drop_index("ix_live_gate_snapshots_snap_date",
                  table_name="live_gate_snapshots")
    op.drop_index("ix_live_gate_snapshots_portfolio_strategy_id",
                  table_name="live_gate_snapshots")
    op.drop_index("ix_live_gate_snapshots_live_session_id",
                  table_name="live_gate_snapshots")
    op.drop_table("live_gate_snapshots")
