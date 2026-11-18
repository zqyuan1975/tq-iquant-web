"""D4 熔断处置动作（revision d0e1f2a3b4c5）

`portfolio_strategies.breaker_action`（默认 halt，不改既有行为）：
notify / halt / half / liquidate。

Revision ID: d0e1f2a3b4c5
Revises: b8c9d0e1f2a3
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "d0e1f2a3b4c5"
down_revision = "b8c9d0e1f2a3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("portfolio_strategies") as batch_op:
        batch_op.add_column(sa.Column(
            "breaker_action", sa.String(length=16), nullable=False,
            server_default="halt"))


def downgrade() -> None:
    with op.batch_alter_table("portfolio_strategies") as batch_op:
        batch_op.drop_column("breaker_action")
