"""拼盘三表（资金池 0021 B6）

Revision ID: s5d6e7f8a9b0
Revises: r3c4d5e6f7a8
Create Date: 2026-11-20

新增 ``capital_pairing_plans``（头）/ ``capital_pairings``（边）/
``capital_pairing_evals``（评估冻结）。纯加表，不改既有表；回退直接 drop。
"""
from alembic import op
import sqlalchemy as sa


revision = "s5d6e7f8a9b0"
down_revision = "r3c4d5e6f7a8"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "capital_pairing_plans",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(100), nullable=False),
        sa.Column("scope", sa.String(10), nullable=False,
                  server_default="L2"),
        sa.Column("status", sa.String(15), nullable=False,
                  server_default="DRAFT"),
        sa.Column("pairing_version", sa.String(64), nullable=True),
        sa.Column("params_hash", sa.String(64), nullable=True),
        sa.Column("portfolio_strategy_id", sa.Integer(),
                  sa.ForeignKey("portfolio_strategies.id", ondelete="CASCADE"),
                  nullable=False),
        sa.Column("lender_strategy_id", sa.Integer(), nullable=True),
        sa.Column("lender_record_id", sa.Integer(), nullable=True),
        sa.Column("max_borrowers", sa.Integer(), nullable=False,
                  server_default="1"),
        sa.Column("loan_term_days", sa.Integer(), nullable=False,
                  server_default="365"),
        sa.Column("principal", sa.Numeric(18, 2), nullable=True),
        sa.Column("recall_notice_days", sa.Integer(), nullable=False,
                  server_default="2"),
        sa.Column("release_delay_days", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("pool_scope", sa.String(30), nullable=True),
        sa.Column("ownership_policy", sa.String(30), nullable=True),
        sa.Column("no_cap_reserve_policy", sa.String(30), nullable=True),
        sa.Column("gate_hysteresis_days", sa.Integer(), nullable=True),
        sa.Column("cash_outlet_enabled", sa.Integer(), nullable=False,
                  server_default="0"),
        sa.Column("binding_json", sa.Text(), nullable=True),
        sa.Column("evidence_json", sa.Text(), nullable=True),
        sa.Column("backtest_record_id", sa.Integer(), nullable=True),
        sa.Column("created_by", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
        sa.Column("approved_at", sa.DateTime(), nullable=True),
        sa.Column("effective_from", sa.DateTime(), nullable=True),
        sa.Column("effective_to", sa.DateTime(), nullable=True),
    )
    op.create_index("ix_capital_pairing_plans_portfolio_strategy_id",
                    "capital_pairing_plans", ["portfolio_strategy_id"])
    op.create_index("ix_capital_pairing_plans_lender_strategy_id",
                    "capital_pairing_plans", ["lender_strategy_id"])

    op.create_table(
        "capital_pairings",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("pairing_plan_id", sa.Integer(),
                  sa.ForeignKey("capital_pairing_plans.id",
                                ondelete="CASCADE"),
                  nullable=False),
        sa.Column("scope", sa.String(10), nullable=False,
                  server_default="L2"),
        sa.Column("lender_type", sa.String(15), nullable=False,
                  server_default="strategy"),
        sa.Column("lender_id", sa.Integer(), nullable=False),
        sa.Column("lender_record_id", sa.Integer(), nullable=True),
        sa.Column("borrower_type", sa.String(15), nullable=False,
                  server_default="strategy"),
        sa.Column("borrower_id", sa.Integer(), nullable=False),
        sa.Column("borrower_record_id", sa.Integer(), nullable=True),
        sa.Column("status", sa.String(15), nullable=False,
                  server_default="ACTIVE"),
        sa.Column("max_share", sa.Numeric(6, 4), nullable=True),
        sa.Column("loan_term_days", sa.Integer(), nullable=True),
        sa.Column("recall_notice_days", sa.Integer(), nullable=True),
        sa.Column("release_delay_days", sa.Integer(), nullable=True),
        sa.Column("eligibility_backtest_id", sa.Integer(), nullable=True),
        sa.Column("evaluation_id", sa.Integer(), nullable=True),
        sa.Column("params_hash", sa.String(64), nullable=True),
        sa.Column("pairing_version", sa.String(64), nullable=True),
        sa.Column("outlet_verification_id", sa.Integer(), nullable=True),
        sa.Column("binding_json", sa.Text(), nullable=True),
        sa.Column("effective_from", sa.DateTime(), nullable=True),
        sa.Column("effective_to", sa.DateTime(), nullable=True),
        sa.Column("created_by", sa.String(50), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_capital_pairings_pairing_plan_id",
                    "capital_pairings", ["pairing_plan_id"])
    op.create_index("ix_capital_pairings_borrower_id",
                    "capital_pairings", ["borrower_id"])

    op.create_table(
        "capital_pairing_evals",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("pairing_plan_id", sa.Integer(),
                  sa.ForeignKey("capital_pairing_plans.id",
                                ondelete="CASCADE"),
                  nullable=False),
        sa.Column("scope", sa.String(10), nullable=False,
                  server_default="L2"),
        sa.Column("eval_kind", sa.String(20), nullable=False,
                  server_default="L2_PRECHECK"),
        sa.Column("eval_version", sa.String(40), nullable=True),
        sa.Column("threshold_version", sa.String(40), nullable=True),
        sa.Column("status", sa.String(10), nullable=False),
        sa.Column("reasons_json", sa.Text(), nullable=True),
        sa.Column("hard_json", sa.Text(), nullable=True),
        sa.Column("soft_json", sa.Text(), nullable=True),
        sa.Column("metrics_json", sa.Text(), nullable=True),
        sa.Column("counts_json", sa.Text(), nullable=True),
        sa.Column("lender_strategy_id", sa.Integer(), nullable=True),
        sa.Column("borrower_strategy_id", sa.Integer(), nullable=True),
        sa.Column("record_id", sa.Integer(), nullable=True),
        sa.Column("common_days", sa.Integer(), nullable=True),
        sa.Column("window_start", sa.String(10), nullable=True),
        sa.Column("window_end", sa.String(10), nullable=True),
        sa.Column("decided_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), server_default=sa.func.now()),
    )
    op.create_index("ix_capital_pairing_evals_pairing_plan_id",
                    "capital_pairing_evals", ["pairing_plan_id"])


def downgrade() -> None:
    op.drop_table("capital_pairing_evals")
    op.drop_table("capital_pairings")
    op.drop_table("capital_pairing_plans")
