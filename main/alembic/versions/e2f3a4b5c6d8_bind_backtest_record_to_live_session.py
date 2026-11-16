"""live session: bind backtest record (H2 params snapshot drift)

Revision ID: e2f3a4b5c6d8
Revises: c9e0f1a2b3d4
Create Date: 2026-11-16

H2 参数绑定（A7）：
- live_sessions.backtest_record_id（nullable，软引用不设 FK）：绑定一条回测记录，
  启动时把 backtest_records.params_snapshot 与当前组合参数比对；差异默认只告警
  （决策事件 gate=params_drift），tq_trade.params_drift_block=true 才升级阻断。
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "e2f3a4b5c6d8"
down_revision = "c9e0f1a2b3d4"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_sessions",
        sa.Column("backtest_record_id", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("live_sessions", "backtest_record_id")
