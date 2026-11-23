"""反转处置默认档 hold → close_next_open（用户拍板 2026-11-23）

Revision ID: d2e3f4a5b6c8
Revises: c1d2e3f4a5b7
Create Date: 2026-11-23

背景：``strategies.reversal_disposition`` 由 c1d2e3f4a5b7 以
``server_default='hold'`` 引入，前端表单从未提交该字段，存量行全部是
迁移时写入的默认值 ``hold``，无法区分「用户显式选择 hold」与「默认 hold」。
按用户拍板「模型默认、存量策略一并切；若某策略被显式设过 hold 则保留」，
本迁移一次性把存量 ``hold`` 切为 ``close_next_open``；此后通过保存接口
显式写入的 ``hold`` 不再被本迁移影响（迁移只执行一次）。

留档：执行前可用以下 SQL 复核被切换的策略（upgrade 内也会输出到日志）：

    SELECT id, name FROM strategies WHERE reversal_disposition = 'hold';

不改变 1w/1mon 与默认 period_close 路径；不改变回测数据。
"""
from alembic import op
import sqlalchemy as sa
import logging

logger = logging.getLogger("alembic.runtime.migration")


revision = "d2e3f4a5b6c8"
down_revision = "c1d2e3f4a5b7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    rows = bind.execute(sa.text(
        "SELECT id, name FROM strategies WHERE reversal_disposition = 'hold'"
    )).fetchall()
    if rows:
        detail = ", ".join("%s:%s" % (r[0], r[1]) for r in rows)
        logger.info(
            "reversal_disposition 切换到 close_next_open：%d 条（%s）",
            len(rows), detail,
        )
    bind.execute(sa.text(
        "UPDATE strategies SET reversal_disposition = 'close_next_open' "
        "WHERE reversal_disposition = 'hold'"
    ))


def downgrade() -> None:
    # 不可逆：已切换的默认 hold 与显式 hold 无法区分，downgrade 只还原列默认值。
    with op.batch_alter_table("strategies") as batch:
        batch.alter_column(
            "reversal_disposition",
            server_default="hold",
            existing_type=sa.String(20),
            existing_nullable=False,
        )
