"""add live_sessions.intraday_mismatched_risk_enabled（错周期风控三态开关）

Revision ID: e0a1b2c3d4f5
Revises: d9e0f1a2b3c4
Create Date: 2026-11-11

nullable 三态：NULL=跟随全局默认（config.tq_trade.intraday_mismatched_risk_enabled，
默认 False）；0/1=会话显式覆盖。不做数据迁移——历史会话保持 NULL，立即跟随新默认
（对齐回测：风控只在策略自身周期的 bar 上评估）。
"""
from alembic import op
import sqlalchemy as sa


revision = "e0a1b2c3d4f5"
down_revision = "d9e0f1a2b3c4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # server_default 不放默认值：ADD COLUMN 后历史行保持 NULL（跟随全局）
    op.add_column(
        "live_sessions",
        sa.Column("intraday_mismatched_risk_enabled", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("live_sessions", "intraday_mismatched_risk_enabled")
