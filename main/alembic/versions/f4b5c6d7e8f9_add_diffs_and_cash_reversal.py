"""C1 差异台账 + C2 现金流红冲（revision f4b5c6d7e8f9）

- 新建 `live_reconciliation_diffs`（C1/I2 差异闭环）；
- `live_cash_flows` 增加 `reversal_of_id` / `reversed_by_id` / `voided`（C2 红冲，
  禁止物理删除）。

Revision ID: f4b5c6d7e8f9
Revises: f3a4b5c6d7e8
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "f4b5c6d7e8f9"
down_revision = "f3a4b5c6d7e8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_reconciliation_diffs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=True),
        sa.Column("diff_type", sa.String(length=16), nullable=False),
        sa.Column("stock_code", sa.String(length=16), nullable=True),
        sa.Column("expected", sa.Numeric(18, 4), nullable=True),
        sa.Column("actual", sa.Numeric(18, 4), nullable=True),
        sa.Column("delta", sa.Numeric(18, 4), nullable=True),
        sa.Column("status", sa.String(length=12), nullable=False,
                  server_default="pending"),
        sa.Column("resolution", sa.String(length=200), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_lrd_session_status", "live_reconciliation_diffs",
                    ["live_session_id", "status"])
    op.create_index("ix_lrd_session_type_code", "live_reconciliation_diffs",
                    ["live_session_id", "diff_type", "stock_code"])

    with op.batch_alter_table("live_cash_flows") as batch:
        batch.add_column(sa.Column("reversal_of_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("reversed_by_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column(
            "voided", sa.Boolean(), nullable=False, server_default="0"))
        batch.create_foreign_key(
            "fk_live_cash_flows_reversal_of", "live_cash_flows",
            ["reversal_of_id"], ["id"], ondelete="SET NULL")
        batch.create_foreign_key(
            "fk_live_cash_flows_reversed_by", "live_cash_flows",
            ["reversed_by_id"], ["id"], ondelete="SET NULL")


def downgrade() -> None:
    with op.batch_alter_table("live_cash_flows") as batch:
        batch.drop_constraint("fk_live_cash_flows_reversed_by",
                              type_="foreignkey")
        batch.drop_constraint("fk_live_cash_flows_reversal_of",
                              type_="foreignkey")
        batch.drop_column("voided")
        batch.drop_column("reversed_by_id")
        batch.drop_column("reversal_of_id")
    op.drop_index("ix_lrd_session_type_code", table_name="live_reconciliation_diffs")
    op.drop_index("ix_lrd_session_status", table_name="live_reconciliation_diffs")
    op.drop_table("live_reconciliation_diffs")
