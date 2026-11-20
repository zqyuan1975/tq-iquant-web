"""F3 资金影子会话级开关：live_sessions.capital_shadow_enabled（0021，2026-11-20）

默认 0（零行为变化）。会话启动时据此构建影子账本：只逐 bar 追加
``live_capital_shadow``，不触订单/成交/决策事件、无下单入口。
与 pairing_plan_id / backtest_run_group_id 绑定入口互不覆盖——已绑定的会话
始终走计划侧原路径，本开关仅在未绑定时生效（只采实际侧）。

Revision ID: h5b6c7d8e9f0
Revises: g4a5b6c7d8e9
Create Date: 2026-11-20
"""
from alembic import op
import sqlalchemy as sa

revision = "h5b6c7d8e9f0"
down_revision = "g4a5b6c7d8e9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_sessions",
        sa.Column(
            "capital_shadow_enabled",
            sa.Boolean(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    with op.batch_alter_table("live_sessions") as batch:
        batch.drop_column("capital_shadow_enabled")
