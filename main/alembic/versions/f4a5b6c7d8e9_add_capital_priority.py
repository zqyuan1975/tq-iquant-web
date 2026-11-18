"""A2 跨组合下单优先级（portfolio_strategies.priority）

Revision ID: f4a5b6c7d8e9
Revises: e2f3a4b5c6d7
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "f4a5b6c7d8e9"
down_revision = "e2f3a4b5c6d7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("portfolio_strategies") as batch_op:
        batch_op.add_column(sa.Column(
            "priority", sa.Integer(), nullable=False, server_default="0"))


def downgrade() -> None:
    with op.batch_alter_table("portfolio_strategies") as batch_op:
        batch_op.drop_column("priority")
