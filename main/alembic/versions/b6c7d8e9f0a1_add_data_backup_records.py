"""add data_backup_records（内嵌数据备份台账）

Revision ID: b6c7d8e9f0a1
Revises: c4d5e6f7a8b9
Create Date: 2026-11-10

每日自动备份 / 手动备份的结果记录，供系统配置页展示「上次自动/手动备份」。
"""
from alembic import op
import sqlalchemy as sa


revision = "b6c7d8e9f0a1"
down_revision = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "data_backup_records",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("kind", sa.String(10), nullable=False),
        sa.Column("status", sa.String(10), nullable=False),
        sa.Column("filename", sa.String(255), nullable=True),
        sa.Column("path", sa.String(500), nullable=True),
        sa.Column("size_bytes", sa.Integer(), nullable=True),
        sa.Column("message", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_data_backup_records_kind", "data_backup_records", ["kind"])
    op.create_index("ix_data_backup_records_status", "data_backup_records", ["status"])
    op.create_index("ix_data_backup_records_started_at", "data_backup_records",
                    ["started_at"])


def downgrade() -> None:
    op.drop_index("ix_data_backup_records_started_at",
                  table_name="data_backup_records")
    op.drop_index("ix_data_backup_records_status",
                  table_name="data_backup_records")
    op.drop_index("ix_data_backup_records_kind", table_name="data_backup_records")
    op.drop_table("data_backup_records")
