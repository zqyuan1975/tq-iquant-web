"""增量回测：接缝成交游标 + 续跑侧逐日成交方向

Revision ID: d4f1a2b3c5e6
Revises: f3b4c5d6e7f8
Create Date: 2026-11-12

两处修复配套的持久化字段（不改既有列语义）：

1. ``live_session_portfolios.seam_trade_cursor``：接缝捕获时刻已计入起账状态的
   ``live_trades.id`` 水位。实盘侧重放只重放该水位之后的成交，避免「盘中重启 →
   首次锚定 → 当日已有成交」把同一笔成交算两次。NULL = 旧接缝（改动前捕获），
   仍按接缝日 00:00 重放。
2. ``incremental_backtest_days.inc_dirs_json``：续跑侧当日成交方向列表，供双跑
   校验与「全量重算自身成交的方向」逐日比较；NULL = 旧行，该判据不参与判定。
"""
from alembic import op
import sqlalchemy as sa


revision = "d4f1a2b3c5e6"
down_revision = "f3b4c5d6e7f8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_session_portfolios",
        sa.Column("seam_trade_cursor", sa.Integer(), nullable=True),
    )
    op.add_column(
        "incremental_backtest_days",
        sa.Column("inc_dirs_json", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("incremental_backtest_days", "inc_dirs_json")
    op.drop_column("live_session_portfolios", "seam_trade_cursor")
