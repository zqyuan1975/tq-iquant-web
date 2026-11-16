"""live baseline alignment: baseline / account_key / orig_quantity / resets audit

Revision ID: c9e0f1a2b3d4
Revises: b8d9e0f1a2c3
Create Date: 2026-11-16

2026-11-16 实盘资金基准对齐（A1/A3/C2）：
- live_sessions：baseline_mode / baseline_value / baseline_set_at / baseline_version /
  adopt_inherited_positions（资金基准 = 真实总资产口径，只缩不放）+ account_key /
  account_key_source（C2 账户级独占）。
- live_orders：orig_quantity（信号原始目标量）+ baseline_version（基准版本标记）。
- portfolio_strategies：max_single_position_pct / min_cash_reserve_pct（可选护栏，
  NULL=不设限，保持旧行为）。
- 新表 live_baseline_resets：收盘后重置的审计（前后基准/峰值/前收/接缝）。
"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "c9e0f1a2b3d4"
down_revision = "b8d9e0f1a2c3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ---- live_sessions：资金基准 + 账户键 ----
    op.add_column(
        "live_sessions",
        sa.Column("baseline_mode", sa.String(length=16), nullable=True,
                  server_default="total_asset"),
    )
    op.add_column(
        "live_sessions",
        sa.Column("baseline_value", sa.Numeric(18, 2), nullable=True),
    )
    op.add_column(
        "live_sessions",
        sa.Column("baseline_set_at", sa.DateTime(), nullable=True),
    )
    op.add_column(
        "live_sessions",
        sa.Column("baseline_version", sa.Integer(), nullable=False,
                  server_default="0"),
    )
    op.add_column(
        "live_sessions",
        sa.Column("adopt_inherited_positions", sa.Boolean(), nullable=False,
                  server_default="1"),
    )
    op.add_column(
        "live_sessions",
        sa.Column("account_key", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "live_sessions",
        sa.Column("account_key_source", sa.String(length=16), nullable=True),
    )

    # ---- live_orders：原始目标量 + 基准版本 ----
    op.add_column(
        "live_orders",
        sa.Column("orig_quantity", sa.Integer(), nullable=True),
    )
    op.add_column(
        "live_orders",
        sa.Column("baseline_version", sa.Integer(), nullable=True),
    )

    # ---- portfolio_strategies：可选护栏 ----
    op.add_column(
        "portfolio_strategies",
        sa.Column("max_single_position_pct", sa.Numeric(5, 4), nullable=True),
    )
    op.add_column(
        "portfolio_strategies",
        sa.Column("min_cash_reserve_pct", sa.Numeric(5, 4), nullable=True),
    )

    # ---- 新表：基准重置审计 ----
    op.create_table(
        "live_baseline_resets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("live_session_id", sa.Integer(),
                  sa.ForeignKey("live_sessions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("portfolio_strategy_id", sa.Integer(),
                  sa.ForeignKey("portfolio_strategies.id"), nullable=False),
        sa.Column("old_baseline", sa.Numeric(18, 2), nullable=True),
        sa.Column("new_baseline", sa.Numeric(18, 2), nullable=True),
        sa.Column("old_peak", sa.Numeric(18, 2), nullable=True),
        sa.Column("new_peak", sa.Numeric(18, 2), nullable=True),
        sa.Column("old_prev_close", sa.Numeric(18, 2), nullable=True),
        sa.Column("new_prev_close", sa.Numeric(18, 2), nullable=True),
        sa.Column("old_seam_date", sa.String(length=10), nullable=True),
        sa.Column("new_seam_date", sa.String(length=10), nullable=True),
        sa.Column("baseline_version", sa.Integer(), nullable=True),
        sa.Column("note", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index(
        "ix_live_baseline_resets_session",
        "live_baseline_resets",
        ["live_session_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_live_baseline_resets_session",
                  table_name="live_baseline_resets")
    op.drop_table("live_baseline_resets")

    op.drop_column("portfolio_strategies", "min_cash_reserve_pct")
    op.drop_column("portfolio_strategies", "max_single_position_pct")

    op.drop_column("live_orders", "baseline_version")
    op.drop_column("live_orders", "orig_quantity")

    op.drop_column("live_sessions", "account_key_source")
    op.drop_column("live_sessions", "account_key")
    op.drop_column("live_sessions", "adopt_inherited_positions")
    op.drop_column("live_sessions", "baseline_version")
    op.drop_column("live_sessions", "baseline_set_at")
    op.drop_column("live_sessions", "baseline_value")
    op.drop_column("live_sessions", "baseline_mode")
