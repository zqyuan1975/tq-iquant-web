"""风控卖单追价：strategies.risk_chase_ticks / risk_chase_max_attempts。

Revision ID: v2d3e4f5a6b7
Revises: u1c2d3e4f5a6
Create Date: 2026-11-25

默认 risk_chase_ticks=0（关闭，不改委托价）、risk_chase_max_attempts=0（不重下），
仅作用于风控卖单（STOP_LOSS / TAKE_PROFIT / TRAILING_STOP 的 SELL）。
"""
from alembic import op
import sqlalchemy as sa


revision = "v2d3e4f5a6b7"
down_revision = "u1c2d3e4f5a6"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("strategies") as batch:
        batch.add_column(sa.Column(
            "risk_chase_ticks", sa.Integer(), nullable=False, server_default="0"))
        batch.add_column(sa.Column(
            "risk_chase_max_attempts", sa.Integer(), nullable=False,
            server_default="0"))


def downgrade():
    with op.batch_alter_table("strategies") as batch:
        batch.drop_column("risk_chase_max_attempts")
        batch.drop_column("risk_chase_ticks")
