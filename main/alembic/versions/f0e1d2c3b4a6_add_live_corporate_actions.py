"""live corporate actions ledger

Revision ID: f0e1d2c3b4a6
Revises: f0e1d2c3b4a5
Create Date: 2026-11-18

J1 实盘侧：现金分红必须落台账，否则 ``LiveEngine.recover`` 按 live_trades 重放
虚拟现金时会把分红金额丢掉（分红不是成交）。唯一键即幂等键
(session, portfolio, code, event_type, ex_date)，重复应用不会重复加钱。
不走 live_cash_flows：那张表金额计入 external_net_inflow，会把分红当外部入金抹掉。
"""
from alembic import op
import sqlalchemy as sa


revision = "f0e1d2c3b4a6"
down_revision = "f0e1d2c3b4a5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_corporate_actions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"), nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=False),
        sa.Column("code", sa.String(length=20), nullable=False),
        sa.Column("event_type", sa.String(length=16), nullable=False),
        sa.Column("ex_date", sa.Date(), nullable=False),
        sa.Column("applied_date", sa.Date(), nullable=False),
        sa.Column("amount", sa.Numeric(15, 2), nullable=False, server_default="0"),
        sa.Column("qty_delta", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("note", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("live_session_id", "portfolio_strategy_id", "code",
                            "event_type", "ex_date",
                            name="uq_live_corporate_actions_applied"),
    )
    op.create_index("ix_live_corporate_actions_session", "live_corporate_actions",
                    ["live_session_id", "applied_date"])


def downgrade() -> None:
    op.drop_index("ix_live_corporate_actions_session", table_name="live_corporate_actions")
    op.drop_table("live_corporate_actions")
