"""A1 资金池模式与日再平衡（live_sessions.capital_pool_*）

Revision ID: f5d6e7f8a9b0
Revises: f4a5b6c7d8e9
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "f5d6e7f8a9b0"
down_revision = "f4a5b6c7d8e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("live_sessions") as batch_op:
        batch_op.add_column(sa.Column(
            "capital_pool_mode", sa.String(length=16), nullable=False,
            server_default="off"))
        batch_op.add_column(sa.Column(
            "capital_pool_rebalance_enabled", sa.Boolean(), nullable=False,
            server_default="0"))


def downgrade() -> None:
    with op.batch_alter_table("live_sessions") as batch_op:
        batch_op.drop_column("capital_pool_rebalance_enabled")
        batch_op.drop_column("capital_pool_mode")
