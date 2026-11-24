"""add selection warehouse tables (D1 import-only wide db)

Revision ID: sf2fd821b3c4
Revises: se1ec710a1b2
Create Date: 2026-11-24 15:00:00.000000

条件选股 A0-2 / D1：宽库作为「一次性导入源」。宽库只在导入期被只读读取，
数据落进这四张平台表后，运行期（选股引擎）不再依赖外部宽库在线。
- selection_wh_meta：单行元数据（快照日 / 源路径 / 导入时间 / 覆盖计数）
- selection_wh_stocks：每票一行（估值、上市信息、GO/GP、K 线派生指标）
- selection_wh_fin：每票一行财报序列（zlib(JSON)）
- selection_wh_dividend：每票一行分红事件序列（zlib(JSON)）
全部为新增表，不改既有表与列。
"""
from alembic import op
import sqlalchemy as sa


revision = "sf2fd821b3c4"
down_revision = "se1ec710a1b2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "selection_wh_meta",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("as_of", sa.Date(), nullable=False),
        sa.Column("source_path", sa.String(400), nullable=False),
        sa.Column("source_size_mb", sa.Numeric(10, 1), nullable=True),
        sa.Column("imported_at", sa.DateTime(), nullable=False,
                  server_default=sa.func.now()),
        sa.Column("stock_count", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("fin_count", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("dividend_count", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("counts_json", sa.Text(), nullable=True),
    )

    op.create_table(
        "selection_wh_stocks",
        sa.Column("code", sa.String(20), primary_key=True),
        sa.Column("name", sa.String(60), nullable=True),
        sa.Column("exchange", sa.String(10), nullable=True),
        sa.Column("sector_code", sa.String(40), nullable=True),
        sa.Column("sector_name", sa.String(60), nullable=True),
        sa.Column("pool", sa.String(40), nullable=True),
        sa.Column("tags", sa.String(200), nullable=True),
        sa.Column("is_financial", sa.Boolean(), nullable=False,
                  server_default=sa.false()),
        sa.Column("more_blob", sa.LargeBinary(), nullable=True),
        sa.Column("info_blob", sa.LargeBinary(), nullable=True),
        sa.Column("go_blob", sa.LargeBinary(), nullable=True),
        sa.Column("gp_blob", sa.LargeBinary(), nullable=True),
        sa.Column("close", sa.Numeric(12, 4), nullable=True),
        sa.Column("volatility_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("max_drawdown_pct", sa.Numeric(10, 4), nullable=True),
        sa.Column("fin_periods", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("fin_last_tag", sa.String(8), nullable=True),
    )
    op.create_index("ix_selection_wh_stocks_sector", "selection_wh_stocks",
                    ["sector_name"])

    op.create_table(
        "selection_wh_fin",
        sa.Column("code", sa.String(20), primary_key=True),
        sa.Column("n_periods", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("last_tag_time", sa.String(8), nullable=True),
        sa.Column("last_announce_time", sa.String(8), nullable=True),
        sa.Column("series_blob", sa.LargeBinary(), nullable=False),
    )

    op.create_table(
        "selection_wh_dividend",
        sa.Column("code", sa.String(20), primary_key=True),
        sa.Column("n_events", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("last_ex_date", sa.String(8), nullable=True),
        sa.Column("rows_blob", sa.LargeBinary(), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("selection_wh_dividend")
    op.drop_table("selection_wh_fin")
    op.drop_index("ix_selection_wh_stocks_sector", table_name="selection_wh_stocks")
    op.drop_table("selection_wh_stocks")
    op.drop_table("selection_wh_meta")
