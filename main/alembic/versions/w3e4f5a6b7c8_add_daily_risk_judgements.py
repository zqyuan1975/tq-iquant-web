"""日线风控两段式裁定：judgement 表 + live_sessions 幂等标记。

Revision ID: w3e4f5a6b7c8
Revises: v2d3e4f5a6b7
Create Date: 2026-11-25

背景：日线策略的风控判定改为 15:05 预判（只留档）+ 16:00 终审（唯一可执行依据）。
- 新建 ``live_daily_risk_judgements``：唯一键 (live_session_id, strategy_id,
  stock_code, trade_date, phase)，保证「同一交易日同一持仓只有一条终审裁定」。
- ``live_sessions.daily_risk_final_done_date``：16:00 终审轮次的跨重启幂等标记。
"""
from alembic import op
import sqlalchemy as sa


revision = "w3e4f5a6b7c8"
down_revision = "v2d3e4f5a6b7"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "live_daily_risk_judgements",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(), nullable=False),
        sa.Column("portfolio_id", sa.Integer(), nullable=False),
        sa.Column("strategy_id", sa.Integer(), nullable=False),
        sa.Column("stock_code", sa.String(length=16), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("phase", sa.String(length=12), nullable=False),
        sa.Column("triggered", sa.Boolean(), nullable=False, server_default="0"),
        sa.Column("condition", sa.String(length=20), nullable=False,
                  server_default=""),
        sa.Column("close_price", sa.Numeric(18, 3), nullable=True),
        sa.Column("avg_cost", sa.Numeric(18, 4), nullable=True),
        sa.Column("highest_price", sa.Numeric(18, 4), nullable=True),
        sa.Column("threshold", sa.Numeric(10, 6), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), nullable=True),
        sa.UniqueConstraint(
            "live_session_id", "strategy_id", "stock_code", "trade_date", "phase",
            name="uq_live_daily_risk_judgement"),
    )
    op.create_index(
        "ix_live_daily_risk_judgements_session",
        "live_daily_risk_judgements", ["live_session_id"])
    op.create_index(
        "ix_live_daily_risk_judgements_trade_date",
        "live_daily_risk_judgements", ["trade_date"])
    with op.batch_alter_table("live_sessions") as batch:
        batch.add_column(sa.Column(
            "daily_risk_final_done_date", sa.String(length=10), nullable=True))


def downgrade():
    with op.batch_alter_table("live_sessions") as batch:
        batch.drop_column("daily_risk_final_done_date")
    op.drop_index("ix_live_daily_risk_judgements_trade_date",
                  table_name="live_daily_risk_judgements")
    op.drop_index("ix_live_daily_risk_judgements_session",
                  table_name="live_daily_risk_judgements")
    op.drop_table("live_daily_risk_judgements")
