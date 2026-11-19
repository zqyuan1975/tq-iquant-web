"""live_orders.id AUTOINCREMENT

消除 SQLite rowid 复用风险：普通 INTEGER PRIMARY KEY 在删除最大 id 行后会把
该 rowid 重新分配给新插入行；而 live_engine 的 _applied_state 以 order_id 为键
记录已记账量，id 复用会让新订单继承旧的已记账进度（理论撞键风险）。
改为 AUTOINCREMENT 后 id 由 sqlite_sequence 单调分配，永不复用。

SQLite 无法直接 ALTER 自增属性，故用 batch_alter_table 重建表：
列定义、默认值（filled_quantity / created_at / updated_at）、外键（含
ON DELETE CASCADE）、索引均从原表反射保留，现有数据按列原样搬运。

Revision ID: c4d5e6f7a8b9
Revises: a2b3c4d5e6f7
Create Date: 2026-11-09
"""
from alembic import op
import sqlalchemy as sa


revision = "c4d5e6f7a8b9"
down_revision = "a2b3c4d5e6f7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    with op.batch_alter_table(
        "live_orders",
        recreate="always",
        table_kwargs={"sqlite_autoincrement": True},
    ) as batch_op:
        batch_op.alter_column(
            "id",
            existing_type=sa.Integer(),
            autoincrement=True,
            existing_nullable=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("live_orders", recreate="always") as batch_op:
        batch_op.alter_column(
            "id",
            existing_type=sa.Integer(),
            autoincrement=False,
            existing_nullable=False,
        )
