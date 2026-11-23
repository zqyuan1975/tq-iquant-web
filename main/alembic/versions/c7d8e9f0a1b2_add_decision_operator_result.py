"""决策事件补操作者与提交结果（M2 一键全平审计）

Revision ID: c7d8e9f0a1b2
Revises: e3f4a5b6c7d9
Create Date: 2026-11-23

只新增两列可空字段，不改既有语义：
- ``operator``：高危全局操作发起者（无鉴权时记录「无鉴权，无法识别操作者」）；
- ``result``：提交三态 submitted / blocked / deferred，失败同样落行。
"""
from alembic import op
import sqlalchemy as sa


revision = "c7d8e9f0a1b2"
down_revision = "e3f4a5b6c7d9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table("live_decision_events") as batch:
        batch.add_column(sa.Column("operator", sa.String(length=64), nullable=True))
        batch.add_column(sa.Column("result", sa.String(length=16), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("live_decision_events") as batch:
        batch.drop_column("result")
        batch.drop_column("operator")
