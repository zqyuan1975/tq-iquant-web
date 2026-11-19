"""资金池快照/事件字段（0021 A5）

Revision ID: p1a2b3c4d5e6
Revises: o4f5a6b7c8d9
Create Date: 2026-11-20

给回测日快照与回测决策事件各加两个可空列：
- capital_pool_version：资金池/额度算法版本（关闭时为 NULL）；
- entitlement_json：当日额度推导快照（关闭时为 NULL）。

只加可空列、不回填、不改既有列语义：旧记录读出即 NULL，读侧必须按
「NULL = 未启用/旧记录」兜底，保证与接线前的行为逐位一致。
"""
from alembic import op
import sqlalchemy as sa


revision = "p1a2b3c4d5e6"
down_revision = "o4f5a6b7c8d9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_daily_snapshots",
        sa.Column("capital_pool_version", sa.String(40), nullable=True),
    )
    op.add_column(
        "backtest_daily_snapshots",
        sa.Column("entitlement_json", sa.Text(), nullable=True),
    )
    op.add_column(
        "backtest_decision_events",
        sa.Column("capital_pool_version", sa.String(40), nullable=True),
    )
    op.add_column(
        "backtest_decision_events",
        sa.Column("entitlement_json", sa.String(4000), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("backtest_decision_events", "entitlement_json")
    op.drop_column("backtest_decision_events", "capital_pool_version")
    op.drop_column("backtest_daily_snapshots", "entitlement_json")
    op.drop_column("backtest_daily_snapshots", "capital_pool_version")
