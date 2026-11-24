"""add sector membership / daily bars / build progress / pool intervals

Revision ID: si5ig154e7f8
Revises: sh4hf043d5e6
Create Date: 2026-11-24 22:00:00.000000

行业风格池与内置宽库构建的落库底座（一次迁移、单头）：
- selection_sector_membership：按 as_of 累积的行业归属（L1 互斥主池 + L2 标签）；
- selection_wh_daily：内置构建/回补的日线收盘序列（回放窗口从 2023-12 起）；
- selection_wh_build_progress：构建断点（run_id + phase + batch_key 幂等去重）；
- selection_pool_intervals：选股池成员区间（与 stock_pool_memberships 并行的
  选股侧时间线，行业池不建 StockPool 行也能按日期解析）。

只新增表，不改既有列；迁移失败整事务回滚。
"""
from alembic import op
import sqlalchemy as sa


revision = "si5ig154e7f8"
down_revision = "sh4hf043d5e6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "selection_sector_membership",
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("sector_code", sa.String(20), nullable=True),
        sa.Column("sector_name", sa.String(60), nullable=True),
        sa.Column("l1_pool", sa.String(20), nullable=False),
        sa.Column("l2_tags", sa.String(200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("as_of", "code",
                                name="pk_selection_sector_membership"),
    )
    op.create_index("ix_sel_sector_membership_l1", "selection_sector_membership",
                    ["as_of", "l1_pool"])

    op.create_table(
        "selection_wh_daily",
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("trade_date", sa.Date(), nullable=False),
        sa.Column("open", sa.Numeric(12, 4), nullable=True),
        sa.Column("high", sa.Numeric(12, 4), nullable=True),
        sa.Column("low", sa.Numeric(12, 4), nullable=True),
        sa.Column("close", sa.Numeric(12, 4), nullable=True),
        sa.Column("volume", sa.Numeric(20, 2), nullable=True),
        sa.Column("amount", sa.Numeric(20, 2), nullable=True),
        sa.Column("source", sa.String(24), nullable=False,
                  server_default="tq:history"),
        sa.Column("ingested_at", sa.DateTime(), server_default=sa.func.now()),
        sa.PrimaryKeyConstraint("code", "trade_date",
                                name="pk_selection_wh_daily"),
    )
    op.create_index("ix_selection_wh_daily_date", "selection_wh_daily",
                    ["trade_date"])

    op.create_table(
        "selection_wh_build_progress",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.String(40), nullable=False),
        sa.Column("mode", sa.String(16), nullable=False,
                  server_default="incremental"),
        sa.Column("phase", sa.String(40), nullable=False),
        sa.Column("batch_key", sa.String(80), nullable=False),
        sa.Column("status", sa.String(16), nullable=False,
                  server_default="pending"),
        sa.Column("total", sa.Integer(), nullable=True),
        sa.Column("done", sa.Integer(), nullable=True),
        sa.Column("error", sa.String(500), nullable=True),
        sa.Column("payload_json", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("run_id", "phase", "batch_key",
                            name="uq_selection_wh_build_progress_batch"),
    )
    op.create_index("ix_selection_wh_build_progress_run",
                    "selection_wh_build_progress", ["run_id", "phase"])

    op.create_table(
        "selection_pool_intervals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("pool_key", sa.String(60), nullable=False),
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("effective_from", sa.Date(), nullable=False),
        sa.Column("effective_to", sa.Date(), nullable=True),
        sa.Column("source", sa.String(20), nullable=False,
                  server_default="snapshot"),
        sa.Column("snapshot_id", sa.Integer(), nullable=True),
        sa.Column("note", sa.String(200), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("pool_key", "code", "effective_from",
                            name="uq_selection_pool_intervals_member"),
    )
    op.create_index("ix_selection_pool_intervals_lookup", "selection_pool_intervals",
                    ["pool_key", "effective_from"])


def downgrade() -> None:
    op.drop_index("ix_selection_pool_intervals_lookup",
                  table_name="selection_pool_intervals")
    op.drop_table("selection_pool_intervals")
    op.drop_index("ix_selection_wh_build_progress_run",
                  table_name="selection_wh_build_progress")
    op.drop_table("selection_wh_build_progress")
    op.drop_index("ix_selection_wh_daily_date", table_name="selection_wh_daily")
    op.drop_table("selection_wh_daily")
    op.drop_index("ix_sel_sector_membership_l1",
                  table_name="selection_sector_membership")
    op.drop_table("selection_sector_membership")
