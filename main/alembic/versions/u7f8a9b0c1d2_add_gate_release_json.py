"""回测记录写入门控释放/召回时间轴（资金池 0021 D4）

Revision ID: u7f8a9b0c1d2
Revises: t6e7f8a9b0c1
Create Date: 2026-11-20

`backtest_records.gate_release_json`：仅启用 ``gate_release_plan`` 的 run 有值，
默认关闭/旧记录为 NULL。只读展示（时间轴/原因码/最终额度），不参与判定。

只加可空列、不回填、不改既有列语义；回退直接 drop。
"""
from alembic import op
import sqlalchemy as sa


revision = "u7f8a9b0c1d2"
down_revision = "t6e7f8a9b0c1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "backtest_records",
        sa.Column("gate_release_json", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("backtest_records", "gate_release_json")
