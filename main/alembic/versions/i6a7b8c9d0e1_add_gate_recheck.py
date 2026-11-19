"""E1：live_sessions 增加执行前重判门控开关。

默认 0（关闭）= 沿用信号日决策即执行的旧行为；开启后顺延次日单与盘前
集合竞价单在提交前用执行日前一收盘的门控状态再判一次。

Revision ID: i6a7b8c9d0e1
Revises: h5a1b2c3d4e5
Create Date: 2026-11-18
"""
from alembic import op
import sqlalchemy as sa


revision = "i6a7b8c9d0e1"
down_revision = "h5a1b2c3d4e5"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("live_sessions") as batch_op:
        batch_op.add_column(sa.Column(
            "gate_recheck_on_execute", sa.Boolean(), nullable=False,
            server_default="0"))


def downgrade():
    with op.batch_alter_table("live_sessions") as batch_op:
        batch_op.drop_column("gate_recheck_on_execute")
