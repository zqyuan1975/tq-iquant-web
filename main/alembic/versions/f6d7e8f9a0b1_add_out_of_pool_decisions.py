"""I3 池外外部仓处置记录（revision f6d7e8f9a0b1）

新建 `live_out_of_pool_decisions`：同一会话同一代码唯一，记录
include（纳入组合池）/ ignore（永久忽略告警）的人工处置，供引擎过滤
`out_of_pool_position` 告警与审计追溯。

Revision ID: f6d7e8f9a0b1
Revises: f5c6d7e8f9a0
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "f6d7e8f9a0b1"
down_revision = "f5c6d7e8f9a0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_out_of_pool_decisions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("stock_code", sa.String(length=20), nullable=False),
        sa.Column("action", sa.String(length=10), nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=True),
        sa.Column("note", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("live_session_id", "stock_code",
                            name="uq_loop_session_code"),
    )
    op.create_index("ix_live_out_of_pool_decisions_live_session_id",
                    "live_out_of_pool_decisions", ["live_session_id"])
    op.create_index("ix_loop_session_action", "live_out_of_pool_decisions",
                    ["live_session_id", "action"])


def downgrade() -> None:
    op.drop_index("ix_loop_session_action",
                  table_name="live_out_of_pool_decisions")
    op.drop_index("ix_live_out_of_pool_decisions_live_session_id",
                  table_name="live_out_of_pool_decisions")
    op.drop_table("live_out_of_pool_decisions")
