"""投资看板（只读）：行业名单 / 刷新任务 / 行业快照 / 成分股快照

只新增 4 张看板表，不改动任何既有表；看板不参与交易、门控与策略判定。

Revision ID: i9b8c7d6e5f4
Revises: h5b6c7d8e9f0
Create Date: 2026-11-21
"""
from alembic import op
import sqlalchemy as sa

revision = "i9b8c7d6e5f4"
down_revision = "h5b6c7d8e9f0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "board_industries",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.Text(), nullable=False, unique=True),
        sa.Column("name", sa.Text(), nullable=True),
        sa.Column("active", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("first_seen", sa.Date(), nullable=True),
        sa.Column("last_seen", sa.Date(), nullable=True),
        sa.Column("source", sa.Text(), nullable=True),
    )

    op.create_table(
        "board_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("scope", sa.Text(), nullable=False),
        sa.Column("as_of", sa.Date(), nullable=True),
        sa.Column("trigger", sa.Text(), nullable=True),
        sa.Column("status", sa.Text(), nullable=False, server_default="running"),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("total", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("ok", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("skipped", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("stats_json", sa.Text(), nullable=True),
    )

    op.create_table(
        "board_industry_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("industry_code", sa.Text(), nullable=False),
        sa.Column("industry_name", sa.Text(), nullable=True),
        sa.Column("direction", sa.Integer(), nullable=True),
        sa.Column("strength", sa.Float(), nullable=True),
        sa.Column("level", sa.Text(), nullable=True),
        sa.Column("bull_dur", sa.Integer(), nullable=True),
        sa.Column("bear_dur", sa.Integer(), nullable=True),
        sa.Column("bias", sa.Float(), nullable=True),
        sa.Column("bias_z", sa.Float(), nullable=True),
        sa.Column("overbought", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("oversold", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("jd", sa.Float(), nullable=True),
        sa.Column("jd_filter", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("channel_filter", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("up_str", sa.Float(), nullable=True),
        sa.Column("dn_str", sa.Float(), nullable=True),
        sa.Column("bars", sa.Integer(), nullable=True),
        sa.Column("stale", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("bar_closed_at", sa.Text(), nullable=True),
        sa.Column("effective_from", sa.Text(), nullable=True),
        sa.Column("computed_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.UniqueConstraint("as_of", "period", "industry_code",
                            name="uq_bis_asof_period_code"),
    )
    op.create_index("ix_bis_period_asof", "board_industry_snapshots",
                    ["period", "as_of"])
    op.create_index("ix_bis_code_asof", "board_industry_snapshots",
                    ["industry_code", "as_of"])

    op.create_table(
        "board_stock_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("period", sa.Text(), nullable=False),
        sa.Column("industry_code", sa.Text(), nullable=False),
        sa.Column("stock_code", sa.Text(), nullable=False),
        sa.Column("stock_name", sa.Text(), nullable=True),
        sa.Column("direction", sa.Integer(), nullable=True),
        sa.Column("strength", sa.Float(), nullable=True),
        sa.Column("level", sa.Text(), nullable=True),
        sa.Column("bull_dur", sa.Integer(), nullable=True),
        sa.Column("bear_dur", sa.Integer(), nullable=True),
        sa.Column("lead_days", sa.Integer(), nullable=True),
        sa.Column("bias", sa.Float(), nullable=True),
        sa.Column("bias_z", sa.Float(), nullable=True),
        sa.Column("overbought", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("oversold", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("bars", sa.Integer(), nullable=True),
        sa.Column("stale", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("bar_closed_at", sa.Text(), nullable=True),
        sa.Column("effective_from", sa.Text(), nullable=True),
        sa.Column("computed_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.UniqueConstraint("as_of", "period", "industry_code", "stock_code",
                            name="uq_bss_asof_period_ind_code"),
    )
    op.create_index("ix_bss_asof_period", "board_stock_snapshots",
                    ["as_of", "period"])
    op.create_index("ix_bss_ind_asof", "board_stock_snapshots",
                    ["industry_code", "as_of"])


def downgrade() -> None:
    op.drop_index("ix_bss_ind_asof", table_name="board_stock_snapshots")
    op.drop_index("ix_bss_asof_period", table_name="board_stock_snapshots")
    op.drop_table("board_stock_snapshots")
    op.drop_index("ix_bis_code_asof", table_name="board_industry_snapshots")
    op.drop_index("ix_bis_period_asof", table_name="board_industry_snapshots")
    op.drop_table("board_industry_snapshots")
    op.drop_table("board_runs")
    op.drop_table("board_industries")
