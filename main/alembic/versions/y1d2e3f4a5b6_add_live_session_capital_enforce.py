"""G1 资金强制：live_sessions.capital_enforce_enabled（0021，2026-11-20）

会话级 kill switch，默认 0（零行为变化）。开启后绑定拼盘的借据净额计入组合
可用现金与真实侧占比；关闭当根 bar 即释放全部借据（只回收不新增）。

Revision ID: y1d2e3f4a5b6
Revises: x0c1d2e3f4a5
Create Date: 2026-11-20
"""
from alembic import op
import sqlalchemy as sa

revision = "y1d2e3f4a5b6"
down_revision = "x0c1d2e3f4a5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_sessions",
        sa.Column(
            "capital_enforce_enabled",
            sa.Boolean(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    with op.batch_alter_table("live_sessions") as batch:
        batch.drop_column("capital_enforce_enabled")
