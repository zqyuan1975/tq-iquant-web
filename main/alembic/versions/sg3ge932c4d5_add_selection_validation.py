"""add selection factor validation summary table

Revision ID: sg3ge932c4d5
Revises: sf2fd821b3c4
Create Date: 2026-11-24 17:30:00.000000

条件选股规则重构：因子稳定性验证摘要（单行）。导入宽库时用宽库内的日线
前向收益回算滚动 IC / 分组单调性，结果随导入快照日绑定；运行期只读这张表，
不再回头访问外部宽库。仅新增表。
"""
from alembic import op
import sqlalchemy as sa


revision = "sg3ge932c4d5"
down_revision = "sf2fd821b3c4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "selection_factor_validations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("horizon_days", sa.Integer(), nullable=False,
                  server_default="20"),
        sa.Column("step_days", sa.Integer(), nullable=False,
                  server_default="21"),
        sa.Column("n_periods", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("computed_at", sa.DateTime(), server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("selection_factor_validations")
