"""F1：回测数据质量统计（覆盖率/缺根/被剔除假 bar）。

Revision ID: j9d0e1f2a3b4
Revises: i8c9d0e1f2a3
Create Date: 2026-11-18
"""
from alembic import op
import sqlalchemy as sa


revision = "j9d0e1f2a3b4"
down_revision = "i8c9d0e1f2a3"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("backtest_records") as batch:
        # 数据质量汇总（JSON 文本）：覆盖率/缺根/伪影价剔除/无成交假 bar 剔除/缺失股票。
        # 只做观测展示，不参与回测判定；历史记录为 NULL（前端整块隐藏）。
        batch.add_column(sa.Column("data_quality_json", sa.Text(), nullable=True))


def downgrade():
    with op.batch_alter_table("backtest_records") as batch:
        batch.drop_column("data_quality_json")
