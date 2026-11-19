"""决策事件过期归档 + 处理审计（live_decision_events.ack_by/archived_* + 审计表）

Revision ID: m1c2d3e4f5a6
Revises: l1b2c3d4e5f6
Create Date: 2026-11-19

背景：ack_status 只有 open/acked，未处理事件只增不减。本次增加：
- ``archived`` 第三状态 + ``archived_at`` / ``archive_reason``：过期归档只是移出
  「未处理」计数，事件行保留可查；
- ``ack_by``：确认操作者，配合审计表回答「谁在何时把什么状态改成了什么」；
- ``live_decision_ack_logs``：单条确认 / 批量确认 / 自动归档逐条留痕，
  批量另记一行汇总（event_id 为 NULL，含 filters_json 与 affected_count）。
"""
from alembic import op
import sqlalchemy as sa


revision = "m1c2d3e4f5a6"
down_revision = "l1b2c3d4e5f6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("live_decision_events") as batch:
        batch.add_column(sa.Column("ack_by", sa.String(length=64), nullable=True))
        batch.add_column(sa.Column("archived_at", sa.DateTime(), nullable=True))
        batch.add_column(sa.Column("archive_reason", sa.String(length=200),
                                   nullable=True))
        batch.create_index(
            "ix_live_decisions_ack_archived", ["ack_status", "archived_at"])
        batch.create_index(
            "ix_live_decisions_archive_scan", ["archived_at", "created_at"])

    op.create_table(
        "live_decision_ack_logs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("batch_id", sa.String(length=36), nullable=True),
        sa.Column("live_session_id", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.Integer(), nullable=True),
        sa.Column("action", sa.String(length=16), nullable=False),
        sa.Column("from_status", sa.String(length=16), nullable=True),
        sa.Column("to_status", sa.String(length=16), nullable=True),
        sa.Column("operator", sa.String(length=64), nullable=True),
        sa.Column("reason", sa.String(length=200), nullable=True),
        sa.Column("filters_json", sa.String(length=1000), nullable=True),
        sa.Column("affected_count", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_live_ack_logs_session_created", "live_decision_ack_logs",
                    ["live_session_id", "created_at"])
    op.create_index("ix_live_ack_logs_event", "live_decision_ack_logs",
                    ["event_id"])
    op.create_index("ix_live_ack_logs_batch", "live_decision_ack_logs",
                    ["batch_id"])


def downgrade() -> None:
    op.drop_index("ix_live_ack_logs_batch", table_name="live_decision_ack_logs")
    op.drop_index("ix_live_ack_logs_event", table_name="live_decision_ack_logs")
    op.drop_index("ix_live_ack_logs_session_created",
                  table_name="live_decision_ack_logs")
    op.drop_table("live_decision_ack_logs")

    with op.batch_alter_table("live_decision_events") as batch:
        batch.drop_index("ix_live_decisions_archive_scan")
        batch.drop_index("ix_live_decisions_ack_archived")
        batch.drop_column("archive_reason")
        batch.drop_column("archived_at")
        batch.drop_column("ack_by")
