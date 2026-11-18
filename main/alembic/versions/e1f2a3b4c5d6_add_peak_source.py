"""D6 峰值来源标记（live_position_peaks.peak_source）

Revision ID: e1f2a3b4c5d6
Revises: d0e1f2a3b4c5
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "e1f2a3b4c5d6"
down_revision = "d0e1f2a3b4c5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("live_position_peaks") as batch_op:
        batch_op.add_column(sa.Column("peak_source", sa.String(length=16),
                                      nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("live_position_peaks") as batch_op:
        batch_op.drop_column("peak_source")
