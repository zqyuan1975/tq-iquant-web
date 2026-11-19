"""market gate: portfolio columns + daily snapshot gate state

Revision ID: a4b5c6d7e8f9
Revises: e2f3a4b5c6d8
Create Date: 2026-11-18

市场多空门控（2026-11-18）：
- portfolio_strategies 三条配置列：
  market_gate_enabled（默认 0=关）/ market_gate_engine（python|tdx，默认 python）/
  market_gate_formula_name（tdx 通道公式名，NULL=内核默认 TQGATE）；
- backtest_daily_snapshots 两列：gate_state（-1/0/1/2，右移一日生效值）、
  gate_strength（强度 0-100）。NULL = 该 run 未启用门控（不参与评估口径）。
SQLite 加列走 batch（env.py 已 render_as_batch=True，这里显式 batch 与既有迁移一致）。
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "a4b5c6d7e8f9"
down_revision = "e2f3a4b5c6d8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("portfolio_strategies", schema=None) as batch_op:
        batch_op.add_column(sa.Column(
            "market_gate_enabled", sa.Boolean(), nullable=False,
            server_default="0"))
        batch_op.add_column(sa.Column(
            "market_gate_engine", sa.String(length=10), nullable=False,
            server_default="python"))
        batch_op.add_column(sa.Column(
            "market_gate_formula_name", sa.String(length=50), nullable=True))

    with op.batch_alter_table("backtest_daily_snapshots", schema=None) as batch_op:
        batch_op.add_column(sa.Column("gate_state", sa.Integer(), nullable=True))
        batch_op.add_column(sa.Column("gate_strength", sa.Integer(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("backtest_daily_snapshots", schema=None) as batch_op:
        batch_op.drop_column("gate_strength")
        batch_op.drop_column("gate_state")

    with op.batch_alter_table("portfolio_strategies", schema=None) as batch_op:
        batch_op.drop_column("market_gate_formula_name")
        batch_op.drop_column("market_gate_engine")
        batch_op.drop_column("market_gate_enabled")
