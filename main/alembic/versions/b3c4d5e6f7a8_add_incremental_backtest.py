"""接缝续跑式增量回测：接缝字段 + checkpoint/对照/校验三表

Revision ID: b3c4d5e6f7a8
Revises: a2c3d4e5f6b7
Create Date: 2026-11-11

- live_session_portfolios 新增接缝日锚点字段（seam_date / seam_cash /
  seam_positions_json / seam_anchor_enabled，均 nullable，历史行不重建）。
- 新增 incremental_backtest_checkpoints / incremental_backtest_days /
  incremental_backtest_validations 三表。
"""
from alembic import op
import sqlalchemy as sa


revision = "b3c4d5e6f7a8"
down_revision = "a2c3d4e5f6b7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "live_session_portfolios",
        sa.Column("seam_date", sa.Date(), nullable=True),
    )
    op.add_column(
        "live_session_portfolios",
        sa.Column("seam_cash", sa.Numeric(15, 2), nullable=True),
    )
    op.add_column(
        "live_session_portfolios",
        sa.Column("seam_positions_json", sa.Text(), nullable=True),
    )
    op.add_column(
        "live_session_portfolios",
        sa.Column("seam_anchor_enabled", sa.Boolean(), nullable=True),
    )

    op.create_table(
        "incremental_backtest_checkpoints",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "session_id", sa.Integer(),
            sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "portfolio_strategy_id", sa.Integer(),
            sa.ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("seam_date", sa.Date(), nullable=False),
        sa.Column("as_of_date", sa.Date(), nullable=False),
        sa.Column("state_json", sa.Text(), nullable=False),
        sa.Column("params_hash", sa.String(64), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False,
                  server_default="1"),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("session_id", "portfolio_strategy_id"),
    )
    op.create_index(
        "ix_inc_checkpoints_session", "incremental_backtest_checkpoints",
        ["session_id"])

    op.create_table(
        "incremental_backtest_days",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "session_id", sa.Integer(),
            sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "portfolio_strategy_id", sa.Integer(),
            sa.ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("inc_total_value", sa.Numeric(18, 2), nullable=True),
        sa.Column("inc_cash", sa.Numeric(18, 2), nullable=True),
        sa.Column("inc_market_value", sa.Numeric(18, 2), nullable=True),
        sa.Column("live_total_value", sa.Numeric(18, 2), nullable=True),
        sa.Column("live_cash", sa.Numeric(18, 2), nullable=True),
        sa.Column("live_market_value", sa.Numeric(18, 2), nullable=True),
        sa.Column("net_dev_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("inc_trades_count", sa.Integer(), nullable=True),
        sa.Column("live_trades_count", sa.Integer(), nullable=True),
        sa.Column("direction_match", sa.Boolean(), nullable=True),
        sa.Column("status", sa.String(10), nullable=False,
                  server_default="ok"),
        sa.Column("tags_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint(
            "session_id", "portfolio_strategy_id", "trade_date",
            name="uq_inc_days_session_ps_date"),
    )
    op.create_index(
        "ix_inc_days_session", "incremental_backtest_days", ["session_id"])
    op.create_index(
        "ix_inc_days_trade_date", "incremental_backtest_days", ["trade_date"])

    op.create_table(
        "incremental_backtest_validations",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "session_id", sa.Integer(),
            sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "portfolio_strategy_id", sa.Integer(),
            sa.ForeignKey("portfolio_strategies.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("from_date", sa.Date(), nullable=False),
        sa.Column("to_date", sa.Date(), nullable=False),
        sa.Column("status", sa.String(10), nullable=False),
        sa.Column("max_net_dev_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("mean_net_dev_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("trade_count_match_ratio", sa.Numeric(6, 4), nullable=True),
        sa.Column("direction_match_ratio", sa.Numeric(6, 4), nullable=True),
        sa.Column("max_drawdown_dev_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("first_divergence_date", sa.Date(), nullable=True),
        sa.Column("criteria_json", sa.Text(), nullable=True),
        sa.Column("detail_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index(
        "ix_inc_validations_session", "incremental_backtest_validations",
        ["session_id"])


def downgrade() -> None:
    op.drop_index(
        "ix_inc_validations_session",
        table_name="incremental_backtest_validations")
    op.drop_table("incremental_backtest_validations")

    op.drop_index("ix_inc_days_trade_date", table_name="incremental_backtest_days")
    op.drop_index("ix_inc_days_session", table_name="incremental_backtest_days")
    op.drop_table("incremental_backtest_days")

    op.drop_index(
        "ix_inc_checkpoints_session",
        table_name="incremental_backtest_checkpoints")
    op.drop_table("incremental_backtest_checkpoints")

    op.drop_column("live_session_portfolios", "seam_anchor_enabled")
    op.drop_column("live_session_portfolios", "seam_positions_json")
    op.drop_column("live_session_portfolios", "seam_cash")
    op.drop_column("live_session_portfolios", "seam_date")
