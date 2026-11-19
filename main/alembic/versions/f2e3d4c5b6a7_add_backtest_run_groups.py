"""add backtest run groups (N1) + backtest_records.run_group_id

Revision ID: f2e3d4c5b6a7
Revises: f1e2d3c4b5a6
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "f2e3d4c5b6a7"
down_revision = "f1e2d3c4b5a6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "backtest_run_groups",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=10), nullable=False,
                  server_default="running"),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("params_snapshot", sa.Text(), nullable=True),
        sa.Column("aggregate_json", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
    )
    with op.batch_alter_table("backtest_records") as batch:
        batch.add_column(sa.Column(
            "run_group_id", sa.Integer(), nullable=True))
        batch.create_foreign_key(
            "fk_backtest_records_run_group_id", "backtest_run_groups",
            ["run_group_id"], ["id"], ondelete="SET NULL")
        batch.create_index(
            "ix_backtest_records_run_group_id", ["run_group_id"])


def downgrade() -> None:
    with op.batch_alter_table("backtest_records") as batch:
        batch.drop_index("ix_backtest_records_run_group_id")
        batch.drop_constraint(
            "fk_backtest_records_run_group_id", type_="foreignkey")
        batch.drop_column("run_group_id")
    op.drop_table("backtest_run_groups")
