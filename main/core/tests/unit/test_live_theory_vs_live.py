"""N2/K7 理论 vs 实际对照 单测（service 层，内存 SQLite）。"""

import json
from datetime import date, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.models import (
    Base, BacktestRecord, BacktestDailySnapshot, BacktestTrade,
    BacktestRunGroup, CapitalPairingPlan, LiveSession, LiveSessionPortfolio,
    LivePortfolioDaily, LiveTrade, LiveOrder, PortfolioStrategy, Strategy,
    LiveAccountSnapshot, LiveAccountPositionSnapshot,
)
from core.services import live_service as svc
from core.services.pairing_service import binding_snapshot


@pytest.fixture
def db_session():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False},
        poolclass=StaticPool)
    Base.metadata.create_all(engine)
    db = sessionmaker(bind=engine)()
    yield db
    db.close()


def _seed(db, bound=True):
    rec = BacktestRecord(
        portfolio_strategy_id=1, name="理论回测",
        start_date=date(2026, 7, 1), end_date=date(2026, 7, 31),
        status="completed",
        params_snapshot=json.dumps({"dividend_mode": "front"}),
    )
    db.add(rec)
    db.flush()
    session = LiveSession(
        name="实盘A", mode="paper", status="running",
        backtest_record_id=rec.id if bound else None,
    )
    db.add(session)
    db.flush()

    # 理论曲线：100000 → 110000（+10%）
    for i, value in enumerate((100000, 105000, 110000)):
        db.add(BacktestDailySnapshot(
            backtest_record_id=rec.id, target_type="portfolio", target_id=1,
            snap_date=date(2026, 7, 29) + timedelta(days=i),
            total_value=Decimal(str(value)), cash=Decimal("0"),
            market_value=Decimal(str(value)),
        ))
    db.add(BacktestTrade(
        backtest_record_id=rec.id, strategy_id=1, signal_name="open_sig",
        signal_type="OPEN", stock_code="000001.SZ", trade_type="BUY",
        price=Decimal("10"), quantity=1000, amount=Decimal("10000"),
        commission=Decimal("5"), stamp_duty=Decimal("0"),
        transfer_fee=Decimal("0"), bar_time=datetime(2026, 7, 29, 10),
    ))

    # 实盘曲线：200000 → 214000（+7%）
    for i, value in enumerate((200000, 207000, 214000)):
        db.add(LivePortfolioDaily(
            live_session_id=session.id, portfolio_strategy_id=1,
            trade_date=date(2026, 7, 29) + timedelta(days=i),
            total_value=Decimal(str(value)), risk_value=Decimal(str(value)),
        ))
    db.add(LiveTrade(
        live_session_id=session.id, portfolio_strategy_id=1, strategy_id=1,
        stock_code="000001.SZ", trade_type="BUY", price=Decimal("10.2"),
        quantity=1000, amount=Decimal("10200"), commission=Decimal("5.1"),
        stamp_duty=Decimal("0"), trade_time=datetime(2026, 7, 29, 10, 3),
    ))
    for qty, filled in ((100, 100), (200, 200), (100, 40)):
        db.add(LiveOrder(
            live_session_id=session.id, portfolio_strategy_id=1, strategy_id=1,
            stock_code="000001.SZ", trade_type="BUY", order_type="limit",
            quantity=qty, filled_quantity=filled, status="filled",
            bar_time=datetime(2026, 7, 29, 10, 3),
        ))
    db.commit()
    return session, rec


def test_theory_vs_live_bound_curves_and_slippage(db_session):
    session, rec = _seed(db_session)

    out = svc.compute_theory_vs_live(db_session, session)

    assert out["bound"] is True
    assert out["binding_source"] == "manual"
    assert out["backtest_record_id"] == rec.id
    # 曲线窗口取回测区间 ∪ 实盘区间，避免实盘从接缝日开始后裁掉整段理论曲线
    assert out["window"] == {"start": "2026-07-01", "end": "2026-07-31"}
    # 两侧曲线均首日归一
    assert out["theory"]["curve"][0]["index_value"] == pytest.approx(1.0)
    assert out["theory"]["curve"][-1]["index_value"] == pytest.approx(1.1)
    assert out["live"]["curve"][0]["index_value"] == pytest.approx(1.0)
    assert out["live"]["curve"][-1]["index_value"] == pytest.approx(1.07)

    assert out["theory"]["metrics"]["total_return"] == pytest.approx(0.1)
    assert out["live"]["metrics"]["total_return"] == pytest.approx(0.07)
    assert out["live"]["metrics"]["trades_count"] == 1
    assert out["live"]["metrics"]["orders_count"] == 3
    assert out["live"]["metrics"]["filled_orders"] == 2
    assert out["live"]["metrics"]["partial_orders"] == 1
    assert out["live"]["metrics"]["fill_rate"] == pytest.approx(0.6667, abs=1e-4)

    dev = out["deviation"]
    assert dev["return_gap"] == pytest.approx(-0.03)
    assert dev["trades_gap"] == 0
    # 理论买入 10.0 vs 实盘 10.2 → 买贵 200bps
    assert dev["slippage"]["matched_pairs"] == 1
    assert dev["slippage"]["weighted_deviation_bps"] == pytest.approx(200.0)
    assert dev["slippage"]["details"][0]["stock_code"] == "000001.SZ"
    # J3：回测前复权 vs 实盘桥不复权 → 显式口径差异提示
    assert any("口径差异（J3）" in w for w in out["warnings"])


def test_theory_vs_live_unbound_still_returns_live_side(db_session):
    session, _ = _seed(db_session, bound=False)

    out = svc.compute_theory_vs_live(db_session, session)

    assert out["bound"] is False
    assert out["binding_source"] is None
    assert out["theory_series_status"] == "no_binding"
    assert out["deviation"] is None
    assert len(out["live"]["curve"]) == 3
    assert any("未绑定回测记录" in w for w in out["warnings"])


def test_backfill_live_daily_from_account_snapshots_is_idempotent(db_session):
    session, _ = _seed(db_session, bound=False)
    db_session.add(LiveSessionPortfolio(
        session_id=session.id, portfolio_strategy_id=1, status="active",
        seam_date=date(2026, 7, 29),
    ))
    for day, total, mv, qty in (
        (date(2026, 8, 3), 200000, 150000, 1000),
        (date(2026, 8, 4), 210000, 160000, 1000),
    ):
        snap = LiveAccountSnapshot(
            live_session_id=session.id, engine="tdx",
            captured_at=datetime(day.year, day.month, day.day, 15),
            total_asset=Decimal(str(total)), available_cash=Decimal(str(total - mv)),
            market_value=Decimal(str(mv)),
        )
        db_session.add(snap)
        db_session.flush()
        db_session.add(LiveAccountPositionSnapshot(
            snapshot_id=snap.id, stock_code="000001.SZ", quantity=qty,
            market_value=Decimal(str(mv)),
        ))
    db_session.commit()

    assert svc._backfill_live_daily_from_account_snapshots(
        db_session, session.id) == 2
    rows = db_session.query(LivePortfolioDaily).filter_by(
        live_session_id=session.id).order_by(LivePortfolioDaily.trade_date).all()
    # _seed 已写 07-29~31 三行 → 只回补缺失的 08-03/08-04
    assert [r.trade_date for r in rows] == [
        date(2026, 7, 29), date(2026, 7, 30), date(2026, 7, 31),
        date(2026, 8, 3), date(2026, 8, 4)]
    backfilled = next(r for r in rows if r.trade_date == date(2026, 8, 4))
    assert backfilled.total_value == Decimal("210000")
    assert backfilled.risk_value == Decimal("210000")
    assert backfilled.position_count == 1
    # 再跑一次不会重复写
    assert svc._backfill_live_daily_from_account_snapshots(
        db_session, session.id) == 0


def test_theory_vs_live_auto_binds_latest_completed_portfolio_record(db_session):
    """会话未显式绑定时，按会话组合自动关联最近一次已完成回测。"""
    session, old_rec = _seed(db_session, bound=False)
    db_session.add(LiveSessionPortfolio(
        session_id=session.id, portfolio_strategy_id=1, status="active"))
    # 更新的同组合回测（id 更大），应被自动选中
    new_rec = BacktestRecord(
        portfolio_strategy_id=1, name="更新的理论回测",
        start_date=date(2026, 7, 29), end_date=date(2026, 7, 31),
        status="completed",
        params_snapshot=json.dumps({"dividend_mode": "front"}),
    )
    db_session.add(new_rec)
    db_session.flush()
    for i, value in enumerate((100000, 106000, 112000)):
        db_session.add(BacktestDailySnapshot(
            backtest_record_id=new_rec.id, target_type="portfolio", target_id=1,
            snap_date=date(2026, 7, 29) + timedelta(days=i),
            total_value=Decimal(str(value)), cash=Decimal("0"),
            market_value=Decimal(str(value)),
        ))
    db_session.commit()

    out = svc.compute_theory_vs_live(db_session, session)

    assert out["bound"] is True
    assert out["binding_source"] == "auto"
    assert "自动关联" in out["binding_note"]
    assert out["backtest_record_id"] == new_rec.id
    assert out["theory_series_status"] == "ok"
    assert out["theory"]["curve"][-1]["index_value"] == pytest.approx(1.12)


# ---------------------------------------------------------------------------
# F4：拼盘会话按成员对比
# ---------------------------------------------------------------------------

def _seed_pairing(db, *, with_group=True, with_records=True):
    ps = PortfolioStrategy(name="拼盘组合", stock_pool_id=1,
                           initial_capital=200000)
    db.add(ps)
    db.flush()
    lender = Strategy(portfolio_id=ps.id, name="主", formula_id=1,
                      period="1d", role="主")
    borrower = Strategy(portfolio_id=ps.id, name="从", formula_id=1,
                        period="1d", role="从")
    db.add_all([lender, borrower])
    db.flush()

    group = None
    if with_group:
        group = BacktestRunGroup(name="拼盘运行组", status="completed")
        db.add(group)
        db.flush()
    rec = None
    if with_records:
        rec = BacktestRecord(
            portfolio_strategy_id=ps.id, name="拼盘回测", status="completed",
            start_date=date(2026, 7, 29), end_date=date(2026, 7, 31),
            run_group_id=group.id if group else None,
            params_snapshot=json.dumps({"dividend_mode": "front"}),
        )
        db.add(rec)
        db.flush()
        for i, value in enumerate((200000, 205000, 210000)):
            db.add(BacktestDailySnapshot(
                backtest_record_id=rec.id, target_type="portfolio",
                target_id=ps.id,
                snap_date=date(2026, 7, 29) + timedelta(days=i),
                total_value=Decimal(str(value)), cash=Decimal("0"),
                market_value=Decimal(str(value)),
            ))
    plan = CapitalPairingPlan(
        name="拼盘", portfolio_strategy_id=ps.id, status="APPROVED",
        lender_strategy_id=lender.id,
        binding_json=json.dumps({
            "lender": binding_snapshot(lender),
            "borrower": binding_snapshot(borrower),
            "loan": {"principal": "50000"},
        }),
    )
    db.add(plan)
    db.flush()
    session = LiveSession(
        name="拼盘实盘", mode="paper", status="running",
        pairing_plan_id=plan.id,
        backtest_run_group_id=group.id if group else None,
    )
    db.add(session)
    db.flush()
    db.add(LiveSessionPortfolio(
        session_id=session.id, portfolio_strategy_id=ps.id, status="active"))
    # 实盘：180000 → 189000（+5%）
    for i, value in enumerate((180000, 184500, 189000)):
        db.add(LivePortfolioDaily(
            live_session_id=session.id, portfolio_strategy_id=ps.id,
            trade_date=date(2026, 7, 29) + timedelta(days=i),
            total_value=Decimal(str(value)),
            risk_value=Decimal(str(value)),
        ))
    db.commit()
    return session, plan, lender, borrower, rec


def test_pairing_session_compares_members(db_session):
    session, plan, lender, borrower, rec = _seed_pairing(db_session)

    out = svc.compute_theory_vs_live(db_session, session)

    assert out["bound"] is True
    assert out["binding_source"] == "pairing_plan"
    pair = out["pairing"]
    assert pair["found"] is True and pair["plan_id"] == plan.id
    assert pair["run_group_id"] == session.backtest_run_group_id
    assert [m["strategy_id"] for m in pair["members"]] == [
        lender.id, borrower.id]
    assert [m["role"] for m in pair["members"]] == ["lender", "borrower"]
    for member in pair["members"]:
        assert member["theory"]["record_id"] == rec.id
        assert member["theory"]["status"] == "ok"
        assert member["live"]["status"] == "ok"
        # 理论 +5%，实盘 +5% → 同日偏差为 0
        assert member["deviation"]["common_days"] == 3
        assert member["deviation"]["return_gap"] == pytest.approx(0.0)


def test_pairing_session_missing_member_record_is_explicit(db_session):
    session, *_ = _seed_pairing(
        db_session, with_group=False, with_records=False)

    out = svc.compute_theory_vs_live(db_session, session)

    pair = out["pairing"]
    assert pair["theory_series_status"] == "record_missing"
    for member in pair["members"]:
        assert member["theory"]["status"] == "record_missing"
        assert member["theory"]["record_id"] is None
        codes = [w["code"] for w in member["warnings"]]
        assert "member_record_missing" in codes
    assert any("缺少成员回测记录" in w for w in out["warnings"])


def test_pairing_session_missing_plan_is_explicit(db_session):
    session, plan, *_ = _seed_pairing(db_session)
    session.pairing_plan_id = 99999
    db_session.commit()

    out = svc.compute_theory_vs_live(db_session, session)

    pair = out["pairing"]
    assert pair["found"] is False
    assert pair["warnings"][0]["code"] == "pairing_plan_missing"
    assert any("绑定的拼盘不存在" in w for w in out["warnings"])


def test_non_pairing_session_has_no_pairing_block(db_session):
    session, _ = _seed(db_session)
    out = svc.compute_theory_vs_live(db_session, session)
    assert "pairing" not in out
