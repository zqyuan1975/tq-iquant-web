"""按标的授权代卖：live_position_scope（存量/外部仓逐票放行）

Revision ID: e3f4a5b6c7d9
Revises: d2e3f4a5b6c8
Create Date: 2026-11-23

只新增表与索引，不改既有列。默认无记录 = 不代卖，等价 platform_only 现状；
``enabled=0`` 表示已收回授权但保留历史行（updated_at/updated_by 可追溯）。
"""
from alembic import op
import sqlalchemy as sa


revision = "e3f4a5b6c7d9"
down_revision = "d2e3f4a5b6c8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_position_scope",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "session_id",
            sa.Integer(),
            sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("code", sa.String(length=20), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False,
                  server_default="1"),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_by", sa.String(length=64), nullable=True),
        sa.UniqueConstraint("session_id", "code",
                            name="uq_live_position_scope_session_code"),
    )
    op.create_index(
        "ix_live_position_scope_session_enabled",
        "live_position_scope", ["session_id", "enabled"])


def downgrade() -> None:
    op.drop_index("ix_live_position_scope_session_enabled",
                  table_name="live_position_scope")
    op.drop_table("live_position_scope")
