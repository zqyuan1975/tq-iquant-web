"""现金出口注册表（资金池 0021 C4）

Revision ID: t6e7f8a9b0c1
Revises: s5d6e7f8a9b0
Create Date: 2026-11-20

新增 ``cash_outlets``：白名单、通道、期限、限额、风险与 C8 验证状态。
纯加表，不改既有表；不产生任何委托/划转。回退直接 drop。
"""
from alembic import op
import sqlalchemy as sa


revision = "t6e7f8a9b0c1"
down_revision = "s5d6e7f8a9b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "cash_outlets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(20), nullable=False, unique=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("category", sa.String(20), nullable=False,
                  server_default="repo"),
        sa.Column("channel", sa.String(20), nullable=False,
                  server_default="manual"),
        sa.Column("channel_order_enabled", sa.Boolean(), nullable=False,
                  server_default="0"),
        sa.Column("yield_source", sa.String(100), nullable=True),
        sa.Column("yield_freq", sa.String(10), nullable=True),
        sa.Column("yield_history_days", sa.Integer(), nullable=True),
        sa.Column("liquidity", sa.String(10), nullable=False,
                  server_default="t1"),
        sa.Column("term_days", sa.Integer(), nullable=True),
        sa.Column("settlement_max_days", sa.Integer(), nullable=False,
                  server_default="1"),
        sa.Column("holiday_postpone", sa.Boolean(), nullable=False,
                  server_default="1"),
        sa.Column("min_amount", sa.Numeric(18, 2), nullable=True),
        sa.Column("max_single_amount", sa.Numeric(18, 2), nullable=True),
        sa.Column("max_daily_amount", sa.Numeric(18, 2), nullable=True),
        sa.Column("min_increment", sa.Numeric(18, 2), nullable=True),
        sa.Column("redeemable_early", sa.Boolean(), nullable=False,
                  server_default="0"),
        sa.Column("early_redeem_penalty", sa.Numeric(6, 4), nullable=True),
        sa.Column("auto_mature_return", sa.Boolean(), nullable=False,
                  server_default="1"),
        sa.Column("fast_redeem_limit", sa.Numeric(18, 2), nullable=True),
        sa.Column("risk_level", sa.String(10), nullable=True),
        sa.Column("principal_guaranteed", sa.Boolean(), nullable=False,
                  server_default="0"),
        sa.Column("arrival_buffer_days", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("inflow_certainty", sa.String(20), nullable=True),
        sa.Column("verified", sa.Boolean(), nullable=False,
                  server_default="0"),
        sa.Column("verified_at", sa.DateTime(), nullable=True),
        sa.Column("verify_amount", sa.Numeric(18, 2), nullable=True),
        sa.Column("verify_result", sa.String(200), nullable=True),
        sa.Column("verify_expires_at", sa.Date(), nullable=True),
        sa.Column("status", sa.String(10), nullable=False,
                  server_default="active"),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_cash_outlets_status", "cash_outlets", ["status"])


def downgrade() -> None:
    op.drop_table("cash_outlets")
