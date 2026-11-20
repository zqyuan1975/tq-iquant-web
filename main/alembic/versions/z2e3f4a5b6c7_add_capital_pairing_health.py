"""G3 拼盘健康指标日快照：capital_pairing_health_daily（0021，2026-11-20）

只新增表与索引，不改既有列；downgrade 直接删表（历史账本由
``capital_pairings`` / ``capital_pairing_evals`` 保留，不受影响）。

Revision ID: z2e3f4a5b6c7
Revises: y1d2e3f4a5b6
Create Date: 2026-11-20
"""
from alembic import op
import sqlalchemy as sa

revision = "z2e3f4a5b6c7"
down_revision = "y1d2e3f4a5b6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "capital_pairing_health_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "pairing_plan_id",
            sa.Integer(),
            sa.ForeignKey("capital_pairing_plans.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "pairing_id",
            sa.Integer(),
            sa.ForeignKey("capital_pairings.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("status", sa.String(length=15), nullable=False),
        sa.Column("health_version", sa.String(length=40), nullable=True),
        sa.Column("metrics_json", sa.Text(), nullable=True),
        sa.Column("items_json", sa.Text(), nullable=True),
        sa.Column("streaks_json", sa.Text(), nullable=True),
        sa.Column("reasons_json", sa.Text(), nullable=True),
        sa.Column("normal_streak", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("evaluated_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(),
                  server_default=sa.func.now()),
    )
    op.create_index(
        "ix_capital_pairing_health_daily_pairing_plan_id",
        "capital_pairing_health_daily", ["pairing_plan_id"])
    op.create_index(
        "ix_capital_pairing_health_daily_as_of",
        "capital_pairing_health_daily", ["as_of"])
    op.create_index(
        "ix_capital_pairing_health_plan_time",
        "capital_pairing_health_daily", ["pairing_plan_id", "as_of"])


def downgrade() -> None:
    op.drop_index("ix_capital_pairing_health_plan_time",
                  table_name="capital_pairing_health_daily")
    op.drop_index("ix_capital_pairing_health_daily_as_of",
                  table_name="capital_pairing_health_daily")
    op.drop_index("ix_capital_pairing_health_daily_pairing_plan_id",
                  table_name="capital_pairing_health_daily")
    op.drop_table("capital_pairing_health_daily")
