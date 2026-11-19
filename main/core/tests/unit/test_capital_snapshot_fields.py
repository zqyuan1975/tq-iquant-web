"""资金池快照/事件字段（0021 A5）测试。

覆盖：
- 新增列可空：不传时写入 NULL，旧记录读出为 None；
- 开关关闭（默认）：params_snapshot 不新增任何键，旧快照逐位一致；
- 开关开启：params_snapshot 冻结 capital_pool 版本口径；
- 决策事件/快照的写入通道能接收版本与额度快照（引擎未接入时为 None）。
"""
import json
from datetime import date
from decimal import Decimal
from types import SimpleNamespace

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import (
    Base, BacktestDailySnapshot, BacktestDecisionEvent, BacktestRecord,
)
from core.services import backtest_service as svc


def _db():
    engine = create_engine("sqlite://")
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine)()


def _ps():
    return SimpleNamespace(
        benchmark_index="000300.SH", initial_capital=Decimal("1000000"),
        max_drawdown=Decimal("0.2"), daily_loss_limit=Decimal("0.05"),
        max_holdings=10, min_commission=Decimal("5"),
        buy_commission_rate=Decimal("0.00025"), sell_commission_rate=Decimal("0.00025"),
        stamp_duty_rate=Decimal("0.0005"), slippage=Decimal("0"),
        trading_session="full", stock_pool_id=2,
    )


def _seed_record(db):
    rec = BacktestRecord(
        portfolio_strategy_id=1, name="t",
        start_date=date(2026, 1, 1), end_date=date(2026, 1, 2),
        status="done",
    )
    db.add(rec)
    db.commit()
    return rec.id


def test_snapshot_fields_nullable_and_default_none():
    db = _db()
    rid = _seed_record(db)
    db.add(BacktestDailySnapshot(
        backtest_record_id=rid, target_type="portfolio", target_id=1,
        snap_date=date(2026, 1, 5), total_value=Decimal("1000000"),
        cash=Decimal("1000000"), market_value=Decimal("0"),
    ))
    db.add(BacktestDecisionEvent(
        backtest_record_id=rid, gate="max_positions_full",
        layer="signal_gate", action="block",
    ))
    db.commit()

    snap = db.query(BacktestDailySnapshot).one()
    assert snap.capital_pool_version is None
    assert snap.entitlement_json is None
    ev = db.query(BacktestDecisionEvent).one()
    assert ev.capital_pool_version is None
    assert ev.entitlement_json is None
    db.close()


def test_params_snapshot_unchanged_when_capital_pool_disabled(monkeypatch):
    monkeypatch.setattr(svc, "load_config", lambda: {})
    snap = json.loads(svc.build_params_snapshot(_ps(), dividend_mode="front"))
    assert "capital_pool" not in snap


def test_params_snapshot_freezes_version_when_enabled(monkeypatch):
    monkeypatch.setattr(
        svc, "load_config", lambda: {"capital_pool": {"enabled": True}})
    snap = json.loads(svc.build_params_snapshot(_ps(), dividend_mode="front"))
    block = snap["capital_pool"]
    assert block["capital_pool_version"]
    assert block["quota_algo_version"]
    assert block["defaults"]["target_utilization"] == "0.95"


def test_snapshot_fields_written_when_provided():
    """写入通道就绪：引擎后续（B3）给出值即可落库。"""
    db = _db()
    rid = _seed_record(db)
    db.add(BacktestDailySnapshot(
        backtest_record_id=rid, target_type="portfolio", target_id=1,
        snap_date=date(2026, 1, 5), total_value=Decimal("1000000"),
        cash=Decimal("1000000"), market_value=Decimal("0"),
        capital_pool_version="capital-pool-a1",
        entitlement_json='{"entitlement": "950000"}',
    ))
    db.commit()
    snap = db.query(BacktestDailySnapshot).one()
    assert snap.capital_pool_version == "capital-pool-a1"
    assert json.loads(snap.entitlement_json)["entitlement"] == "950000"
    db.close()
