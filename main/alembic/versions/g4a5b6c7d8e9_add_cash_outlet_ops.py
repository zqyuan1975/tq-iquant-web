"""G4 现金出口执行台账（0021，2026-11-20）

- 新增 ``cash_outlet_ops``：计划/确认/投放/回流/收益/短款追溯，只记账不动组合；
- ``live_sessions.cash_outlet_mode``：会话级分级开关 OFF/P1/P2/P3，默认 OFF。

Revision ID: g4a5b6c7d8e9
Revises: z2e3f4a5b6c7
Create Date: 2026-11-20
"""
from alembic import op
import sqlalchemy as sa

revision = "g4a5b6c7d8e9"
down_revision = "z2e3f4a5b6c7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cash_outlet_ops",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "live_session_id",
            sa.Integer(),
            sa.ForeignKey("live_sessions.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "outlet_id",
            sa.Integer(),
            sa.ForeignKey("cash_outlets.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("mode", sa.String(length=5), nullable=False,
                  server_default="P1"),
        sa.Column("amount", sa.Numeric(18, 2), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False,
                  server_default="PENDING_CONFIRM"),
        sa.Column("idempotency_key", sa.String(length=80), nullable=True,
                  unique=True),
        sa.Column("executed_at", sa.DateTime(), nullable=True),
        sa.Column("expected_return_date", sa.Date(), nullable=True),
        sa.Column("returned_at", sa.DateTime(), nullable=True),
        sa.Column("principal_returned", sa.Numeric(18, 2), nullable=True),
        sa.Column("yield_amount", sa.Numeric(18, 2), nullable=False,
                  server_default="0"),
        sa.Column("shortfall_amount", sa.Numeric(18, 2), nullable=False,
                  server_default="0"),
        sa.Column("shortfall_reason", sa.String(length=200), nullable=True),
        sa.Column("approved_by", sa.String(length=50), nullable=True),
        sa.Column("approved_at", sa.DateTime(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(),
                  server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(),
                  server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_cash_outlet_ops_live_session_id",
                    "cash_outlet_ops", ["live_session_id"])
    op.create_index("ix_cash_outlet_ops_outlet_id",
                    "cash_outlet_ops", ["outlet_id"])
    op.create_index("ix_cash_outlet_ops_session_status",
                    "cash_outlet_ops", ["live_session_id", "status"])
    op.create_index("ix_cash_outlet_ops_outlet_executed",
                    "cash_outlet_ops", ["outlet_id", "executed_at"])

    with op.batch_alter_table("live_sessions") as batch:
        batch.add_column(sa.Column(
            "cash_outlet_mode", sa.String(length=5), nullable=False,
            server_default="OFF"))


def downgrade() -> None:
    with op.batch_alter_table("live_sessions") as batch:
        batch.drop_column("cash_outlet_mode")
    op.drop_index("ix_cash_outlet_ops_outlet_executed",
                  table_name="cash_outlet_ops")
    op.drop_index("ix_cash_outlet_ops_session_status",
                  table_name="cash_outlet_ops")
    op.drop_index("ix_cash_outlet_ops_outlet_id",
                  table_name="cash_outlet_ops")
    op.drop_index("ix_cash_outlet_ops_live_session_id",
                  table_name="cash_outlet_ops")
    op.drop_table("cash_outlet_ops")
