"""add selection pool recall table

Revision ID: sh4hf043d5e6
Revises: sg3ge932c4d5
Create Date: 2026-11-24 18:20:00.000000

池子有效性验证口径调整：池是候选域，最终由导入公式择时决定买入。新增召回
验证结果表 —— 同一 as_of 比较「公式全市场入选集合」与「公式池内入选集合」，
记录召回率、池外漏票明细与成因分类。IC 仅作参考，不落此表。仅新增表。
"""
from alembic import op
import sqlalchemy as sa


revision = "sh4hf043d5e6"
down_revision = "sg3ge932c4d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "selection_pool_recalls",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("formula_id", sa.Integer(), nullable=False),
        sa.Column("snapshot_id", sa.Integer(), nullable=False),
        sa.Column("horizon_days", sa.Integer(), nullable=False,
                  server_default="20"),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("computed_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("snapshot_id", "formula_id", "horizon_days",
                            name="uq_selection_pool_recall"),
    )


def downgrade() -> None:
    op.drop_table("selection_pool_recalls")
