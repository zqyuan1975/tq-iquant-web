"""公司行为漏记诊断表（只观测，不参与账务）

Revision ID: a0b1c2d3e4f5
Revises: i9b8c7d6e5f4
Create Date: 2026-11-23

新增 ``corporate_action_diagnostics``：记录「价格序列有显著除权断点、但公司行为
事件表查不到登记事件」的线索。只写入本表，绝不补录事件/改账。
"""
from alembic import op
import sqlalchemy as sa


revision = "a0b1c2d3e4f5"
down_revision = "i9b8c7d6e5f4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "corporate_action_diagnostics",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("diagnostic_type", sa.String(32), nullable=False),
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("break_date", sa.Date(), nullable=False),
        sa.Column("prev_trade_date", sa.Date(), nullable=True),
        sa.Column("implied_ratio", sa.Numeric(12, 6), nullable=True),
        sa.Column("implied_drop_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("prev_close", sa.Numeric(12, 6), nullable=True),
        sa.Column("implied_ex_price", sa.Numeric(12, 6), nullable=True),
        sa.Column("suspected_event_type", sa.String(16), nullable=True),
        sa.Column("window_start", sa.Date(), nullable=True),
        sa.Column("window_end", sa.Date(), nullable=True),
        sa.Column("source", sa.String(40), nullable=True),
        sa.Column("detail", sa.String(500), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("diagnostic_type", "code", "break_date",
                            name="uq_corporate_action_diag_key"),
    )
    op.create_index("ix_corporate_action_diag_code_date",
                    "corporate_action_diagnostics", ["code", "break_date"])


def downgrade() -> None:
    op.drop_index("ix_corporate_action_diag_code_date",
                  table_name="corporate_action_diagnostics")
    op.drop_table("corporate_action_diagnostics")
