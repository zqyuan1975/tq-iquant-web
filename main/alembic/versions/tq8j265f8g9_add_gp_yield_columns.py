"""add GP20/GP21 columns to selection_gp_snapshots

Revision ID: tq8j265f8g9
Revises: si5ig154e7f8
Create Date: 2026-11-24 23:55:00.000000

历史回放的 PIT 估值只保留「当日总市值 + 当日股息率」两个可重建口径：
- ``pledge_ratio``：GP20（质押比例，%）；
- ``div_yield_pct``：GP21（当日股息率，%），回放用它替代不可重建的
  「近 3 年平均股息率」。

只加两个可空列，不改既有数据；SQLite 加列不重写表。
"""
from alembic import op
import sqlalchemy as sa


revision = "tq8j265f8g9"
down_revision = "si5ig154e7f8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("selection_gp_snapshots",
                  sa.Column("pledge_ratio", sa.Numeric(10, 4), nullable=True))
    op.add_column("selection_gp_snapshots",
                  sa.Column("div_yield_pct", sa.Numeric(10, 4), nullable=True))


def downgrade() -> None:
    op.drop_column("selection_gp_snapshots", "div_yield_pct")
    op.drop_column("selection_gp_snapshots", "pledge_ratio")
