"""M4 决策事件确认/关闭（live_decision_events.ack_*）

Revision ID: e2f3a4b5c6d7
Revises: e1f2a3b4c5d6
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "e2f3a4b5c6d7"
down_revision = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("live_decision_events") as batch_op:
        batch_op.add_column(sa.Column(
            "ack_status", sa.String(length=16), nullable=False,
            server_default="open"))
        batch_op.add_column(sa.Column("ack_note", sa.String(length=200),
                                      nullable=True))
        batch_op.add_column(sa.Column("ack_at", sa.DateTime(), nullable=True))
        batch_op.create_index(
            "ix_live_decisions_sess_ack", ["live_session_id", "ack_status"])


def downgrade() -> None:
    with op.batch_alter_table("live_decision_events") as batch_op:
        batch_op.drop_index("ix_live_decisions_sess_ack")
        batch_op.drop_column("ack_at")
        batch_op.drop_column("ack_note")
        batch_op.drop_column("ack_status")
