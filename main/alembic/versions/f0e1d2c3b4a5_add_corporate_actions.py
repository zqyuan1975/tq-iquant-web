"""corporate actions: manual events table + portfolio switches

Revision ID: f0e1d2c3b4a5
Revises: a4b5c6d7e8f9
Create Date: 2026-11-18

功能缺口 J1/J2（2026-11-18）：
- 新表 corporate_actions：分红 / 送转 / 配股 / 退市 / 停牌 手工事件台账，
  唯一键 (code, event_type, ex_date) 防重复录入；
- portfolio_strategies 四列开关：corporate_actions_enabled（默认 0）、
  dividend_reinvest_enabled（默认 0）、suspension_dispose_days（默认 0=不处置）、
  delist_force_close_enabled（默认 0）。
全部默认关闭：关闭时回测/实盘执行路径与改动前逐字一致。
"""
from alembic import op
import sqlalchemy as sa


revision = "f0e1d2c3b4a5"
down_revision = "a4b5c6d7e8f9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "corporate_actions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(length=20), nullable=False),
        sa.Column("event_type", sa.String(length=16), nullable=False),
        sa.Column("ex_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=True),
        sa.Column("cash_per_share", sa.Numeric(12, 6), nullable=True),
        sa.Column("cash_pct", sa.Numeric(10, 8), nullable=True),
        sa.Column("share_ratio", sa.Numeric(12, 6), nullable=True),
        sa.Column("rights_price", sa.Numeric(12, 6), nullable=True),
        sa.Column("note", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("code", "event_type", "ex_date",
                            name="uq_corporate_actions_code_type_date"),
    )
    op.create_index("ix_corporate_actions_code_date", "corporate_actions",
                    ["code", "ex_date"])

    with op.batch_alter_table("portfolio_strategies", schema=None) as batch_op:
        batch_op.add_column(sa.Column(
            "corporate_actions_enabled", sa.Boolean(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column(
            "dividend_reinvest_enabled", sa.Boolean(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column(
            "suspension_dispose_days", sa.Integer(), nullable=False, server_default="0"))
        batch_op.add_column(sa.Column(
            "delist_force_close_enabled", sa.Boolean(), nullable=False, server_default="0"))


def downgrade() -> None:
    with op.batch_alter_table("portfolio_strategies", schema=None) as batch_op:
        batch_op.drop_column("delist_force_close_enabled")
        batch_op.drop_column("suspension_dispose_days")
        batch_op.drop_column("dividend_reinvest_enabled")
        batch_op.drop_column("corporate_actions_enabled")

    op.drop_index("ix_corporate_actions_code_date", table_name="corporate_actions")
    op.drop_table("corporate_actions")
