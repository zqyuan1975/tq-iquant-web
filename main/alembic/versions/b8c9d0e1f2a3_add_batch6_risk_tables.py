"""批次 6 风险兜底表（revision b8c9d0e1f2a3）

- `live_portfolio_daily`：组合资金日序列（A3），唯一键
  (session, portfolio, trade_date)，同日覆盖更新。
- `live_session_pending_changes`：会话待生效变更队列（D3）。
- `live_global_switches`：全局应急开关单行表（M2）。
- `live_sessions` 加账户级风控列（D1/D2）：account_risk_enabled 默认 0、
  account_max_drawdown / account_daily_loss_limit 可空、account_risk_state JSON。

全部新增且默认关闭，不改变既有行为。

Revision ID: b8c9d0e1f2a3
Revises: f6d7e8f9a0b1
Create Date: 2026-11-18
"""

import sqlalchemy as sa
from alembic import op

revision = "b8c9d0e1f2a3"
down_revision = "f6d7e8f9a0b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "live_portfolio_daily",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("cash", sa.Numeric(18, 2), nullable=True),
        sa.Column("market_value", sa.Numeric(18, 2), nullable=True),
        sa.Column("total_value", sa.Numeric(18, 2), nullable=True),
        sa.Column("risk_value", sa.Numeric(18, 2), nullable=True),
        sa.Column("weight", sa.Numeric(10, 6), nullable=True),
        sa.Column("position_count", sa.Integer(), nullable=True),
        sa.Column("detail", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("live_session_id", "portfolio_strategy_id",
                            "trade_date", name="uq_lpd_session_portfolio_date"),
    )
    op.create_index("ix_live_portfolio_daily_live_session_id",
                    "live_portfolio_daily", ["live_session_id"])
    op.create_index("ix_live_portfolio_daily_portfolio_strategy_id",
                    "live_portfolio_daily", ["portfolio_strategy_id"])
    op.create_index("ix_live_portfolio_daily_trade_date",
                    "live_portfolio_daily", ["trade_date"])
    op.create_index("ix_lpd_session_date", "live_portfolio_daily",
                    ["live_session_id", "trade_date"])

    op.create_table(
        "live_session_pending_changes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(), nullable=True),
        sa.Column("strategy_id", sa.Integer(), nullable=True),
        sa.Column("field", sa.String(length=64), nullable=False),
        sa.Column("old_value", sa.String(length=64), nullable=True),
        sa.Column("new_value", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False,
                  server_default="pending"),
        sa.Column("note", sa.String(length=200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("applied_at", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_live_session_pending_changes_live_session_id",
                    "live_session_pending_changes", ["live_session_id"])
    op.create_index("ix_live_session_pending_changes_portfolio_strategy_id",
                    "live_session_pending_changes", ["portfolio_strategy_id"])
    op.create_index("ix_lspc_session_status", "live_session_pending_changes",
                    ["live_session_id", "status"])

    op.create_table(
        "live_global_switches",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("halt_new_positions", sa.Boolean(), nullable=False,
                  server_default="0"),
        sa.Column("reason", sa.String(length=200), nullable=True),
        sa.Column("operator", sa.String(length=64), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
    )

    with op.batch_alter_table("live_sessions") as batch_op:
        batch_op.add_column(sa.Column(
            "account_risk_enabled", sa.Boolean(), nullable=False,
            server_default="0"))
        batch_op.add_column(sa.Column(
            "account_max_drawdown", sa.Numeric(9, 6), nullable=True))
        batch_op.add_column(sa.Column(
            "account_daily_loss_limit", sa.Numeric(9, 6), nullable=True))
        batch_op.add_column(sa.Column("account_risk_state", sa.Text(),
                                      nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("live_sessions") as batch_op:
        batch_op.drop_column("account_risk_state")
        batch_op.drop_column("account_daily_loss_limit")
        batch_op.drop_column("account_max_drawdown")
        batch_op.drop_column("account_risk_enabled")

    op.drop_table("live_global_switches")

    op.drop_index("ix_lspc_session_status",
                  table_name="live_session_pending_changes")
    op.drop_index("ix_live_session_pending_changes_portfolio_strategy_id",
                  table_name="live_session_pending_changes")
    op.drop_index("ix_live_session_pending_changes_live_session_id",
                  table_name="live_session_pending_changes")
    op.drop_table("live_session_pending_changes")

    op.drop_index("ix_lpd_session_date", table_name="live_portfolio_daily")
    op.drop_index("ix_live_portfolio_daily_trade_date",
                  table_name="live_portfolio_daily")
    op.drop_index("ix_live_portfolio_daily_portfolio_strategy_id",
                  table_name="live_portfolio_daily")
    op.drop_index("ix_live_portfolio_daily_live_session_id",
                  table_name="live_portfolio_daily")
    op.drop_table("live_portfolio_daily")
