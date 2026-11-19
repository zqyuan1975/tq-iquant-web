"""E3：市场门控维度扩展配置列（多指数投票/强度分档/策略级覆盖/空头减仓）。

Revision ID: i8c9d0e1f2a3
Revises: i7b8c9d0e1f2
Create Date: 2026-11-18
"""
from alembic import op
import sqlalchemy as sa


revision = "i8c9d0e1f2a3"
down_revision = "i7b8c9d0e1f2"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("portfolio_strategies") as batch:
        # 多指数投票：JSON 数组（NULL/空 = 仅 benchmark_index，旧语义）
        batch.add_column(sa.Column("market_gate_indices", sa.Text(), nullable=True))
        # 投票模式：any（默认，任一空头即拦）/majority（过半）/all（全部）
        batch.add_column(sa.Column(
            "market_gate_vote_mode", sa.String(10), nullable=False,
            server_default="any"))
        # 空头强度分档门槛（0-100）：0=不设门槛（旧语义）
        batch.add_column(sa.Column(
            "market_gate_strength_threshold", sa.Integer(), nullable=False,
            server_default="0"))
        # 空头减仓档（0-1）：0=关闭（旧语义）；>0 时空头连续段内每只持仓减一次
        batch.add_column(sa.Column(
            "market_gate_bear_reduce_ratio", sa.Numeric(5, 4), nullable=False,
            server_default="0"))
    with op.batch_alter_table("strategies") as batch:
        # 策略级覆盖：inherit（默认，跟随组合）/off（本策略不受门控）/on（强制启用）
        batch.add_column(sa.Column(
            "market_gate_mode", sa.String(10), nullable=False,
            server_default="inherit"))
    # 多指数快照需要容纳逗号拼接的指数列表（单指数时与原值一致）
    with op.batch_alter_table("live_gate_snapshots") as batch:
        batch.alter_column(
            "benchmark_index", existing_type=sa.String(16),
            type_=sa.String(64), existing_nullable=True)


def downgrade():
    with op.batch_alter_table("live_gate_snapshots") as batch:
        batch.alter_column(
            "benchmark_index", existing_type=sa.String(64),
            type_=sa.String(16), existing_nullable=True)
    with op.batch_alter_table("strategies") as batch:
        batch.drop_column("market_gate_mode")
    with op.batch_alter_table("portfolio_strategies") as batch:
        batch.drop_column("market_gate_bear_reduce_ratio")
        batch.drop_column("market_gate_strength_threshold")
        batch.drop_column("market_gate_vote_mode")
        batch.drop_column("market_gate_indices")
