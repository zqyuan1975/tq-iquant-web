"""add backtest_trades.ref_price（滑点前参考价，成本容量 B 方案）

Revision ID: n3e4f5a6b7c8
Revises: m1c2d3e4f5a6
Create Date: 2026-11-19

可空列：历史成交没有参考价，读时回退到「持仓快照隐含收盘价」并标记为历史口径；
新回测在成交写入时记录应用滑点之前的开盘价，买卖两侧同源。
"""
from alembic import op
import sqlalchemy as sa


revision = "n3e4f5a6b7c8"
down_revision = "m1c2d3e4f5a6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_trades",
        sa.Column("ref_price", sa.Numeric(10, 3), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("backtest_trades", "ref_price")
