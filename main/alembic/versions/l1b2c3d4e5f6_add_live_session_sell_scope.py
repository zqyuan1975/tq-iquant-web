"""H3：live_sessions 会话级卖出范围（存量/外部持仓是否纳入风控代卖）。

Revision ID: l1b2c3d4e5f6
Revises: k0a1b2c3d4e5
Create Date: 2026-11-19

可空列：NULL = 历史会话未显式设置（解析时回退全局
config.tq_trade.live_sell_scope）；platform_only = 关闭（默认档位）；
full = 开启（按整仓卖，含存量/外部仓）。运行中可切换。
"""
from alembic import op
import sqlalchemy as sa


revision = "l1b2c3d4e5f6"
down_revision = "k0a1b2c3d4e5"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("live_sessions") as batch:
        batch.add_column(sa.Column("sell_scope", sa.String(length=16), nullable=True))


def downgrade():
    with op.batch_alter_table("live_sessions") as batch:
        batch.drop_column("sell_scope")
