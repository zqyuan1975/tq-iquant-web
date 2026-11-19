"""实盘分钟级单次下单比例（volume_limit_ratio 拆分）

Revision ID: o4f5a6b7c8d9
Revises: n3e4f5a6b7c8
Create Date: 2026-11-19

背景：volume_limit_ratio 此前在回测按「当日 bar 量」、在实盘按「最近 1 分钟
bar 量」执行，同一比例两套分母，实盘单次可下单量被压到回测的约 1/240。
本次拆分：
- volume_limit_ratio 只承担日线流动性目标（回测/实盘统一为前 5 个交易日
  日均量 × ratio，严格不含当日）；
- 新增 minute_volume_limit_ratio，仅实盘使用，控制「单次提交 ≤ 最近完成
  1m bar 量 × 该比例」。NULL=未配置（引擎回落默认 0.10），0=关闭分钟闸门。
"""
from alembic import op
import sqlalchemy as sa


revision = "o4f5a6b7c8d9"
down_revision = "n3e4f5a6b7c8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "portfolio_strategies",
        sa.Column("minute_volume_limit_ratio", sa.Numeric(5, 4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("portfolio_strategies", "minute_volume_limit_ratio")
