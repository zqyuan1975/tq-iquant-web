"""B2 主备通道：live_sessions 新增 backup_engine / failover_enabled

Revision ID: g1h2i3j4k5l6
Revises: f5d6e7f8a9b0
Create Date: 2026-11-18

默认关闭：backup_engine=NULL、failover_enabled=0，行为与接线前一致。
"""
from alembic import op
import sqlalchemy as sa


revision = "g1h2i3j4k5l6"
down_revision = "f5d6e7f8a9b0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("live_sessions") as batch:
        batch.add_column(sa.Column("backup_engine", sa.String(16), nullable=True))
        batch.add_column(sa.Column(
            "failover_enabled", sa.Boolean(), nullable=False,
            server_default="0"))


def downgrade() -> None:
    with op.batch_alter_table("live_sessions") as batch:
        batch.drop_column("failover_enabled")
        batch.drop_column("backup_engine")
