"""add live_cash_flows (外部现金流台账)

Revision ID: f3b4c5d6e7f8
Revises: c4d5e6f7a8ba
Create Date: 2026-11-11

实盘虚拟账本的外部现金流台账（③）：入金/出金/分红/其他。
- amount 有符号：>0 流入，<0 流出。
- 虚拟现金随台账增减（入金加现金、出金减现金），但净值/收益率/回撤统一按
  ``净值 − 累计外部净流入`` 计算，使业绩口径与回测（无外部现金流）可对照。
- 柜台现金与虚拟现金不一致只记 cash_diff 告警，不改账。
"""
from alembic import op
import sqlalchemy as sa


revision = "f3b4c5d6e7f8"
down_revision = "c4d5e6f7a8ba"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_cash_flows",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(),
                  sa.ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
                  nullable=False),
        sa.Column("flow_type", sa.String(length=16), nullable=False),
        sa.Column("amount", sa.Numeric(15, 2), nullable=False),
        sa.Column("flow_date", sa.Date(), nullable=False),
        sa.Column("note", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_live_cash_flows_session_date", "live_cash_flows",
                    ["live_session_id", "flow_date"])
    op.create_index("ix_live_cash_flows_portfolio_date", "live_cash_flows",
                    ["portfolio_strategy_id", "flow_date"])


def downgrade() -> None:
    op.drop_index("ix_live_cash_flows_portfolio_date", table_name="live_cash_flows")
    op.drop_index("ix_live_cash_flows_session_date", table_name="live_cash_flows")
    op.drop_table("live_cash_flows")
