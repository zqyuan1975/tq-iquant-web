"""G2：live_sessions 盘前集合竞价参数会话级覆盖。

Revision ID: k0a1b2c3d4e5
Revises: j9d0e1f2a3b4
Create Date: 2026-11-18

三列均可空：NULL = 跟随 config.tq_trade.pre_open_auction_* 全局值，
非空 = 本会话显式覆盖。历史会话保持 NULL，行为与升级前逐字一致。
"""
from alembic import op
import sqlalchemy as sa


revision = "k0a1b2c3d4e5"
down_revision = "j9d0e1f2a3b4"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("live_sessions") as batch:
        # 是否启用盘前集合竞价放行（NULL=跟随全局）
        batch.add_column(sa.Column(
            "pre_open_auction_enabled", sa.Boolean(), nullable=True))
        # 盘前执行窗口结束时间 HH:MM（NULL=跟随全局，默认 09:20）
        batch.add_column(sa.Column(
            "pre_open_auction_end_time", sa.String(length=5), nullable=True))
        # 盘前委托价让价幅度 %（NULL=跟随全局，默认 2.0）
        batch.add_column(sa.Column(
            "pre_open_auction_price_bias_pct", sa.Numeric(9, 6), nullable=True))


def downgrade():
    with op.batch_alter_table("live_sessions") as batch:
        batch.drop_column("pre_open_auction_price_bias_pct")
        batch.drop_column("pre_open_auction_end_time")
        batch.drop_column("pre_open_auction_enabled")
