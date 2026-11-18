"""批次4（C1 差异台账 + C2 现金流红冲 + C3 日期边界 + I2 接受柜台为准）单测。

覆盖的硬约束：
- 差异台账同一 `(session, type, code)` 的 pending 行复用（不逐次追加噪声）；
- 状态机 pending → accepted/resolved/ignored，已处理行不能再次处理；
- `position` + accept 写合成成交改虚拟持仓，价格优先请求值、其次 detail 的 `price=`；
- `cash`/`equity` + accept 只确认口径，不改虚拟现金；
- 现金流红冲是「反向补一笔 + 原行作废」，可审计、幂等、禁止套娃红冲；
- 现金流日期边界：未来日期拒绝、早于会话起账日仅显式 allow_backfill 放行；
- 差异台账开关默认关闭（关闭时不写任何行）。
"""

from datetime import date, datetime, timedelta
from decimal import Decimal

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.models import (
    Base, LiveCashFlow, LiveReconciliationDiff, LiveSession,
    LiveSessionPortfolio, LiveTrade, PortfolioStrategy, StockPool, Strategy,
)
from core.services import live_service
from core.engine.live_engine import LiveEngine

STOCK = "600000.SH"
STARTED = datetime(2026, 11, 1, 9, 0)


# ---------------- fixtures ----------------
def _db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(engine)
    SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
    return SessionLocal()


def _seed(db, created_at=STARTED):
    db.add(StockPool(id=1, code="POOL1", name="池"))
    db.add(PortfolioStrategy(id=1, name="组合", stock_pool_id=1))
    db.add(Strategy(id=1, portfolio_id=1, name="策略",
                    formula_id=1, period="1m", role="independent"))
    db.add(LiveSession(id=1, name="会话", mode="live", status="stopped",
                       created_at=created_at))
    db.add(LiveSessionPortfolio(session_id=1, portfolio_strategy_id=1))
    db.commit()


# ---------------- C1：差异台账 ----------------
def test_diff_ledger_upserts_pending_and_computes_delta():
    db = _db()
    try:
        _seed(db)
        first = live_service.record_reconciliation_diff(
            db, 1, "position", portfolio_id=1, stock_code=STOCK,
            expected=1000, actual=1200, detail="direction=increase")
        assert Decimal(first["delta"]) == Decimal("200")
        second = live_service.record_reconciliation_diff(
            db, 1, "position", portfolio_id=1, stock_code=STOCK.lower(),
            expected=1000, actual=900, detail="direction=decrease")
        assert second["id"] == first["id"]  # 复用同一 pending 行
        assert Decimal(second["delta"]) == Decimal("-100")
        assert db.query(LiveReconciliationDiff).count() == 1

        data = live_service.list_reconciliation_diffs(db, 1)
        assert data["status_counts"] == {"pending": 1}
    finally:
        db.close()


def test_diff_resolve_state_machine_rejects_second_action():
    db = _db()
    try:
        _seed(db)
        row = live_service.record_reconciliation_diff(
            db, 1, "cash", expected=10000, actual=9000, detail="diff=-1000")
        out, err = live_service.resolve_reconciliation_diff(
            db, 1, row["id"], "ignore", resolution="柜台延迟，非真实差异")
        assert err is None and out["status"] == "ignored"
        again, err2 = live_service.resolve_reconciliation_diff(
            db, 1, row["id"], "accept")
        assert again is None and "已处理" in err2
    finally:
        db.close()


def test_diff_accept_position_writes_synthetic_trade_from_price_hint():
    db = _db()
    try:
        _seed(db)
        row = live_service.record_reconciliation_diff(
            db, 1, "position", portfolio_id=1, stock_code=STOCK,
            expected=1000, actual=1200, detail="price=10.5")
        out, err = live_service.resolve_reconciliation_diff(
            db, 1, row["id"], "accept")
        assert err is None and out["status"] == "accepted"
        trade = db.query(LiveTrade).one()
        assert trade.quantity == 200 and trade.trade_type == "BUY"
        assert Decimal(str(trade.price)) == Decimal("10.5")
    finally:
        db.close()


def test_diff_accept_position_without_price_is_rejected():
    db = _db()
    try:
        _seed(db)
        row = live_service.record_reconciliation_diff(
            db, 1, "position", portfolio_id=1, stock_code=STOCK,
            expected=1000, actual=1200)
        out, err = live_service.resolve_reconciliation_diff(
            db, 1, row["id"], "accept")
        assert out is None and "无法确定改账价格" in err
        assert db.query(LiveReconciliationDiff).one().status == "pending"
    finally:
        db.close()


def test_diff_accept_cash_and_equity_do_not_touch_ledger():
    db = _db()
    try:
        _seed(db)
        for diff_type in ("cash", "equity"):
            row = live_service.record_reconciliation_diff(
                db, 1, diff_type, expected=10000, actual=9000)
            out, err = live_service.resolve_reconciliation_diff(
                db, 1, row["id"], "accept")
            assert err is None and out["status"] == "accepted"
        assert db.query(LiveTrade).count() == 0
    finally:
        db.close()


def test_diff_ledger_disabled_by_default(monkeypatch):
    from core.engine import live_engine as le
    monkeypatch.setattr(le, "load_config", lambda: {"tq_trade": {}})

    class _Shim:
        _db_session_factory = None
        _diff_ledger_enabled = LiveEngine._diff_ledger_enabled
        _note_reconciliation_diff = LiveEngine._note_reconciliation_diff

    shim = _Shim()
    assert shim._diff_ledger_enabled() is False
    # 关闭时写入函数第一行返回，不触碰数据库
    shim._note_reconciliation_diff("cash", expected=1, actual=2)


# ---------------- C2：现金流红冲 ----------------
def test_cash_flow_reverse_is_append_only_and_idempotent():
    db = _db()
    try:
        _seed(db)
        row, err = live_service.record_cash_flow(db, 1, 1, "deposit", 10000)
        assert err is None
        rev, err = live_service.reverse_cash_flow(db, 1, row["id"], note="录错金额")
        assert err is None and rev["amount"] == "-10000.00"

        original = db.query(LiveCashFlow).filter_by(id=row["id"]).one()
        assert original.voided is True
        assert original.reversed_by_id == rev["id"]
        assert live_service.cash_flow_net_inflow(db, 1)[1] == Decimal("0.00")

        again, err2 = live_service.reverse_cash_flow(db, 1, row["id"])
        assert again is None and "已被红冲" in err2
        nested, err3 = live_service.reverse_cash_flow(db, 1, rev["id"])
        assert nested is None and "红冲行" in err3
    finally:
        db.close()


# ---------------- C3：现金流日期边界 ----------------
def test_cash_flow_rejects_future_date():
    db = _db()
    try:
        _seed(db)
        future = (datetime.now() + timedelta(days=1)).date()
        row, err = live_service.record_cash_flow(
            db, 1, 1, "deposit", 1000, flow_date=future)
        assert row is None and "不能晚于今天" in err
    finally:
        db.close()


def test_cash_flow_pre_start_requires_allow_backfill():
    db = _db()
    try:
        _seed(db)
        row, err = live_service.record_cash_flow(
            db, 1, 1, "deposit", 1000, flow_date="2026-10-01")
        assert row is None and "早于会话起账日" in err

        row, err = live_service.record_cash_flow(
            db, 1, 1, "deposit", 1000, flow_date="2026-10-01",
            allow_backfill=True)
        assert err is None and row["flow_date"] == "2026-10-01"
    finally:
        db.close()


# ---------------- C4：每日对账日报 ----------------
def test_report_upsert_overwrites_same_day_and_lists():
    db = _db()
    try:
        _seed(db)
        payload = {
            "report_date": date(2026, 11, 18), "engine": "tdx_tq",
            "positions_total": 3, "positions_matched": 2, "positions_diff": 1,
            "cash_expected": "1000", "cash_actual": "1100", "cash_diff": "100",
            "equity_expected": "5000", "equity_actual": "5100",
            "equity_deviation_pct": "0.02", "order_count": 4, "trade_count": 3,
            "detail": {"position_diffs": [{"stock_code": STOCK}]},
        }
        row, err = live_service.upsert_reconciliation_report(db, 1, payload)
        assert err is None
        assert row["report_date"] == "2026-11-18"
        assert row["cash_diff"] == "100.00"
        assert row["detail"]["position_diffs"][0]["stock_code"] == STOCK

        row2, err2 = live_service.upsert_reconciliation_report(
            db, 1, dict(payload, order_count=9))
        assert err2 is None
        assert row2["id"] == row["id"] and row2["order_count"] == 9
        assert len(live_service.list_reconciliation_reports(db, 1)) == 1
        assert live_service.list_reconciliation_reports(db, 999) is None
    finally:
        db.close()


def test_report_requires_existing_session():
    db = _db()
    try:
        _seed(db)
        row, err = live_service.upsert_reconciliation_report(db, 999, {})
        assert row is None and err == "资源不存在"
    finally:
        db.close()

