"""add backtest_evaluations.downside_volatility / excess_calmar_ratio

Revision ID: c4d5e6f7a8ba
Revises: b3c4d5e6f7a8
Create Date: 2026-11-11

补齐两个风险收益指标（回测评估页指标卡空位）：
- downside_volatility：年化下行波动率，与索提诺比率分母同口径
  （仅负收益日样本求均方根 × √252）。
- excess_calmar_ratio：超额卡玛 = 年化超额收益 / |超额最大回撤|；
  超额最大回撤为 0 时无意义，落 NULL。

历史回测记录这两列为 NULL，页面显示「—」；重跑评估后补齐。
"""
from alembic import op
import sqlalchemy as sa


revision = "c4d5e6f7a8ba"
down_revision = "b3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_evaluations",
        sa.Column("downside_volatility", sa.Numeric(10, 4), nullable=True),
    )
    op.add_column(
        "backtest_evaluations",
        sa.Column("excess_calmar_ratio", sa.Numeric(10, 4), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("backtest_evaluations", "excess_calmar_ratio")
    op.drop_column("backtest_evaluations", "downside_volatility")
