"""add selection scheme/snapshot tables

Revision ID: se1ec710a1b2
Revises: c7d8e9f0a1b2
Create Date: 2026-11-24 10:00:00.000000

条件选股 A1：方案 / 版本 / 运行 / 快照 / 成分 / GP16 缓存，共 6 张新表。
全部为新增，不改任何既有表与列；downgrade 仅删除本迁移创建的对象。
"""
from alembic import op
import sqlalchemy as sa


revision = "se1ec710a1b2"
down_revision = "c7d8e9f0a1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "selection_schemes",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(40), nullable=False),
        sa.Column("name", sa.String(80), nullable=False),
        sa.Column("pool_key", sa.String(40), nullable=False),
        sa.Column("description", sa.String(300), nullable=True),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("code", name="uq_selection_schemes_code"),
    )
    op.create_index("ix_selection_schemes_pool", "selection_schemes",
                    ["pool_key", "status"])

    op.create_table(
        "selection_scheme_versions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scheme_id", sa.Integer(),
                  sa.ForeignKey("selection_schemes.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("spec_json", sa.Text(), nullable=False),
        sa.Column("spec_hash", sa.String(16), nullable=False),
        sa.Column("field_set_version", sa.String(40), nullable=False),
        sa.Column("field_fingerprint", sa.String(16), nullable=False),
        sa.Column("weight_sum", sa.Numeric(10, 6), nullable=True),
        sa.Column("uncalibrated", sa.Boolean(), nullable=False,
                  server_default=sa.true()),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("note", sa.String(300), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("scheme_id", "version",
                            name="uq_selection_scheme_versions_scheme_version"),
    )
    op.create_index("ix_selection_scheme_versions_hash",
                    "selection_scheme_versions", ["spec_hash"])

    op.create_table(
        "selection_runs",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scheme_version_id", sa.Integer(),
                  sa.ForeignKey("selection_scheme_versions.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("stock_count", sa.Integer(), nullable=True),
        sa.Column("params_hash", sa.String(16), nullable=True),
        sa.Column("error", sa.String(500), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_selection_runs_version_asof", "selection_runs",
                    ["scheme_version_id", "as_of"])
    op.create_index("ix_selection_runs_status", "selection_runs", ["status"])

    op.create_table(
        "selection_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("run_id", sa.Integer(),
                  sa.ForeignKey("selection_runs.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("scheme_version_id", sa.Integer(), nullable=False),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("fingerprint", sa.String(16), nullable=False),
        sa.Column("field_set_version", sa.String(40), nullable=False),
        sa.Column("spec_hash", sa.String(16), nullable=False),
        sa.Column("config_fingerprint", sa.String(16), nullable=False),
        sa.Column("stats_json", sa.Text(), nullable=True),
        sa.Column("quality_json", sa.Text(), nullable=True),
        sa.Column("frozen", sa.Boolean(), nullable=False,
                  server_default=sa.false()),
        sa.Column("published_pool_id", sa.Integer(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("run_id", name="uq_selection_snapshots_run"),
    )
    op.create_index("ix_selection_snapshots_asof", "selection_snapshots", ["as_of"])
    op.create_index("ix_selection_snapshots_pool", "selection_snapshots",
                    ["published_pool_id"])

    op.create_table(
        "selection_snapshot_stocks",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("snapshot_id", sa.Integer(),
                  sa.ForeignKey("selection_snapshots.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=True),
        sa.Column("score", sa.Numeric(12, 6), nullable=True),
        sa.Column("selected", sa.Boolean(), nullable=False,
                  server_default=sa.false()),
        sa.Column("pool_key", sa.String(40), nullable=False),
        sa.Column("exclude_reason", sa.String(200), nullable=True),
        sa.Column("factor_json", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("snapshot_id", "code",
                            name="uq_selection_snapshot_stocks_code"),
    )
    op.create_index("ix_selection_snapshot_stocks_rank",
                    "selection_snapshot_stocks", ["snapshot_id", "rank"])
    op.create_index("ix_selection_snapshot_stocks_selected",
                    "selection_snapshot_stocks", ["snapshot_id", "selected"])

    op.create_table(
        "selection_gp_snapshots",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("code", sa.String(20), nullable=False),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("total_mv_wan", sa.Numeric(20, 4), nullable=True),
        sa.Column("source", sa.String(40), nullable=False,
                  server_default="tq:GP16"),
        sa.Column("ingested_at", sa.DateTime(), server_default=sa.func.now()),
        sa.UniqueConstraint("code", "as_of",
                            name="uq_selection_gp_snapshots_code_asof"),
    )
    op.create_index("ix_selection_gp_snapshots_asof", "selection_gp_snapshots",
                    ["as_of"])


def downgrade() -> None:
    op.drop_index("ix_selection_gp_snapshots_asof",
                  table_name="selection_gp_snapshots")
    op.drop_table("selection_gp_snapshots")

    op.drop_index("ix_selection_snapshot_stocks_selected",
                  table_name="selection_snapshot_stocks")
    op.drop_index("ix_selection_snapshot_stocks_rank",
                  table_name="selection_snapshot_stocks")
    op.drop_table("selection_snapshot_stocks")

    op.drop_index("ix_selection_snapshots_pool", table_name="selection_snapshots")
    op.drop_index("ix_selection_snapshots_asof", table_name="selection_snapshots")
    op.drop_table("selection_snapshots")

    op.drop_index("ix_selection_runs_status", table_name="selection_runs")
    op.drop_index("ix_selection_runs_version_asof", table_name="selection_runs")
    op.drop_table("selection_runs")

    op.drop_index("ix_selection_scheme_versions_hash",
                  table_name="selection_scheme_versions")
    op.drop_table("selection_scheme_versions")

    op.drop_index("ix_selection_schemes_pool", table_name="selection_schemes")
    op.drop_table("selection_schemes")
