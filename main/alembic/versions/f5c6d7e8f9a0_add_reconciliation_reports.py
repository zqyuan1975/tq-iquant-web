"""C4 每日对账日报（revision f5c6d7e8f9a0）

新建 `live_reconciliation_reports`：每交易日一行，固化柜台 vs 虚拟的
持仓/现金/净值差异与当日委托/成交笔数。唯一键
`(live_session_id, report_date)` 保证重复生成只覆盖不追加。

Revision ID: f5c6d7e8f9a0
Revises: f4b5c6d7e8f9
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "f5c6d7e8f9a0"
down_revision = "f4b5c6d7e8f9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_reconciliation_reports",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("report_date", sa.Date(), nullable=False),
        sa.Column("engine", sa.String(length=20), nullable=True),
        sa.Column("positions_total", sa.Integer(), nullable=True),
        sa.Column("positions_matched", sa.Integer(), nullable=True),
        sa.Column("positions_diff", sa.Integer(), nullable=True),
        sa.Column("cash_expected", sa.Numeric(16, 2), nullable=True),
        sa.Column("cash_actual", sa.Numeric(16, 2), nullable=True),
        sa.Column("cash_diff", sa.Numeric(16, 2), nullable=True),
        sa.Column("equity_expected", sa.Numeric(16, 2), nullable=True),
        sa.Column("equity_actual", sa.Numeric(16, 2), nullable=True),
        sa.Column("equity_deviation_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("order_count", sa.Integer(), nullable=True),
        sa.Column("trade_count", sa.Integer(), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("live_session_id", "report_date",
                            name="uq_lrr_session_date"),
    )
    op.create_index("ix_lrr_session_date", "live_reconciliation_reports",
                    ["live_session_id", "report_date"])
    op.create_index("ix_live_reconciliation_reports_live_session_id",
                    "live_reconciliation_reports", ["live_session_id"])
    op.create_index("ix_live_reconciliation_reports_report_date",
                    "live_reconciliation_reports", ["report_date"])


def downgrade() -> None:
    op.drop_index("ix_live_reconciliation_reports_report_date",
                  table_name="live_reconciliation_reports")
    op.drop_index("ix_live_reconciliation_reports_live_session_id",
                  table_name="live_reconciliation_reports")
    op.drop_index("ix_lrr_session_date", table_name="live_reconciliation_reports")
    op.drop_table("live_reconciliation_reports")
