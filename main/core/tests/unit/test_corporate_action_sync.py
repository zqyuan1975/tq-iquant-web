# -*- coding: utf-8 -*-
"""公司行为自动同步（F 组，2026-11-22）。

覆盖：
- 每 10 股口径 → 每股换算（603187 06-11 派 0.25/股、600211 09-15 派 1.146/股）；
- 送转 / 配股分别落 split / rights，配股 note 带 rights_unhandled（不静默）；
- 单票取数失败只进 failed，不中断整批；覆盖清单区分「无事件」与「没回填」；
- 除权闸读库：表为空 → ok=False（fail-open + 记账），有事件 → ok=True；
- 回填范围 = 池 1 ∪ 回测记录涉及池 ∪ 各会话最新快照持仓。
"""
import json
from datetime import date, datetime

import pandas as pd
import pytest

from core.models import (
    BacktestRecord, CorporateAction, LiveAccountPositionSnapshot,
    LiveAccountSnapshot, StockPool, StockPoolStock,
)
from core.services import corporate_action_sync as cas


def _frame(rows):
    return pd.DataFrame(
        rows,
        columns=["Date", "Type", "Bonus", "AllotPrice", "ShareBonus", "Allotment"],
    ).set_index("Date")


class _FakeTQ:
    """按代码返回固定除权表；``fail`` 里的代码抛错模拟取数失败。"""

    table = {}
    fail = set()

    def get_divid_factors(self, code, start="", end=""):
        if code in self.fail:
            raise RuntimeError("tdx down")
        return self.table.get(code, pd.DataFrame())


@pytest.fixture(autouse=True)
def _patch_env(monkeypatch, tmp_path):
    monkeypatch.setattr(cas, "coverage_path", lambda: tmp_path / "coverage.json")
    monkeypatch.setattr("core.tq.data.TQData", lambda: _FakeTQ())
    _FakeTQ.table = {}
    _FakeTQ.fail = set()


def test_sync_converts_per_10_share_and_marks_rights(db_session):
    _FakeTQ.table = {
        "603187.SH": _frame([
            {"Date": "2026-06-11", "Type": "1", "Bonus": 2.5,
             "AllotPrice": 0, "ShareBonus": 0, "Allotment": 0},
        ]),
        "600211.SH": _frame([
            {"Date": "2026-09-15", "Type": "1", "Bonus": 11.46,
             "AllotPrice": 0, "ShareBonus": 0, "Allotment": 0},
            {"Date": "2026-09-15", "Type": "2", "Bonus": 0,
             "AllotPrice": 5.2, "ShareBonus": 3, "Allotment": 3},
        ]),
    }

    result = cas.sync_corporate_actions(db_session, ["603187.SH", "600211.SH"])

    assert result["requested"] == 2
    assert sorted(result["covered"]) == ["600211.SH", "603187.SH"]
    assert result["failed"] == []

    from decimal import Decimal

    rows = {(r.code, r.event_type): r for r in db_session.query(CorporateAction).all()}
    assert rows[("603187.SH", "dividend")].cash_per_share == Decimal("0.25")
    assert rows[("600211.SH", "dividend")].cash_per_share == Decimal("1.146")
    split = rows[("600211.SH", "split")]
    assert split.share_ratio == Decimal("0.3")
    rights = [r for (code, kind), r in rows.items()
              if code == "600211.SH" and kind == "rights"]
    assert rights and cas.RIGHTS_UNHANDLED in (rights[0].note or "")
    assert rights[0].rights_price == Decimal("5.2")

    coverage = cas._read_coverage()
    assert coverage["603187.SH"]["status"] == "covered"


def test_sync_isolates_failures_and_records_no_events(db_session):
    _FakeTQ.table = {"000001.SZ": _frame([])}
    _FakeTQ.fail = {"600000.SH"}

    result = cas.sync_corporate_actions(db_session, ["000001.SZ", "600000.SH"])

    assert result["failed"] == ["600000.SH"]
    assert result["no_events"] == ["000001.SZ"]
    assert result["requested"] == 2
    coverage = cas._read_coverage()
    assert coverage["000001.SZ"]["status"] == "no_events"
    assert coverage["600000.SH"]["status"] == "failed"


def test_missing_coverage_separates_no_events_from_absent(db_session):
    _FakeTQ.table = {"000001.SZ": _frame([])}
    cas.sync_corporate_actions(db_session, ["000001.SZ"])
    db_session.add(CorporateAction(code="600211.SH", event_type="dividend",
                                   ex_date=date(2026, 9, 15),
                                   cash_per_share=1.146))
    db_session.commit()

    missing = cas.missing_coverage_codes(
        db_session, ["000001.SZ", "600211.SH", "300286.SZ"])

    assert missing == ["300286.SZ"]


def test_load_ex_date_codes_fail_open_when_table_empty(db_session):
    assert cas.load_ex_date_codes(db_session, date(2026, 6, 11))["ok"] is False

    db_session.add(CorporateAction(code="603187.SH", event_type="dividend",
                                   ex_date=date(2026, 6, 11),
                                   cash_per_share=0.25))
    db_session.commit()

    info = cas.load_ex_date_codes(db_session, date(2026, 6, 11))
    assert info["ok"] is True
    assert info["codes"] == {"603187.SH": ["dividend"]}
    # 其它日期只返回空映射，但 ok=True（表可读，只是当天无事件）
    other = cas.load_ex_date_codes(db_session, date(2026, 6, 12))
    assert other == {"ok": True, "codes": {}}


def test_resolve_backfill_codes_unions_pool_records_and_holdings(db_session):
    db_session.add(StockPool(id=1, code="TQCS", name="成长精选"))
    db_session.add(StockPool(id=2, code="TQZT", name="题材精选"))
    db_session.add_all([
        StockPoolStock(pool_id=1, stock_code="000001.SZ"),
        StockPoolStock(pool_id=2, stock_code="000002.SZ"),
    ])
    db_session.add(BacktestRecord(
        portfolio_strategy_id=1, name="r1", start_date=date(2025, 1, 1),
        end_date=date(2025, 6, 30), status="completed",
        params_snapshot=json.dumps({"stock_pool_id": 2})))
    snapshot = LiveAccountSnapshot(live_session_id=1, engine="tdx",
                                   captured_at=datetime(2026, 11, 22, 15, 0, 0))
    db_session.add(snapshot)
    db_session.flush()
    db_session.add_all([
        LiveAccountPositionSnapshot(snapshot_id=snapshot.id,
                                    stock_code="300286.SZ", quantity=2900),
        LiveAccountPositionSnapshot(snapshot_id=snapshot.id,
                                    stock_code="600211.SH", quantity=0),
    ])
    db_session.commit()

    scope = cas.resolve_backfill_codes(db_session, pool_ids=[1])

    assert scope["codes"] == ["000001.SZ", "000002.SZ", "300286.SZ"]
    assert scope["pool_ids"] == [1, 2]
    assert scope["holdings"] == ["300286.SZ"]


def test_coverage_summary_partitions_pools_and_holdings(db_session):
    db_session.add_all([
        StockPool(id=1, code="TQCS", name="成长精选"),
        StockPool(id=2, code="TQZT", name="题材精选"),
        StockPoolStock(pool_id=1, stock_code="000001.SZ"),
        StockPoolStock(pool_id=1, stock_code="000002.SZ"),
        StockPoolStock(pool_id=1, stock_code="000003.SZ"),
        StockPoolStock(pool_id=2, stock_code="600211.SH"),
    ])
    snapshot = LiveAccountSnapshot(live_session_id=1, engine="tdx",
                                   captured_at=datetime(2026, 11, 24, 15, 0, 0))
    db_session.add(snapshot)
    db_session.flush()
    db_session.add(LiveAccountPositionSnapshot(
        snapshot_id=snapshot.id, stock_code="600000.SH", quantity=100))
    db_session.add(CorporateAction(code="600211.SH", event_type="dividend",
                                   ex_date=date(2026, 9, 15),
                                   cash_per_share=1.146))
    db_session.commit()

    cas._mark_coverage("000001.SZ", "covered")
    cas._mark_coverage("000002.SZ", "no_events")
    cas._mark_coverage("000003.SZ", "failed")

    summary = cas.coverage_summary(db_session, pool_ids=[1, 2])

    by_id = {p["pool_id"]: p for p in summary["pools"]}
    assert by_id[1] == {
        "pool_id": 1, "pool_name": "成长精选", "total": 3,
        "covered": 1, "no_events": 1, "failed": 1, "missing": 0,
    }
    assert by_id[2]["covered"] == 1 and by_id[2]["missing"] == 0

    # 池外持仓单列：未回填 → missing（不混进池合计）
    assert summary["holdings"]["codes"] == ["600000.SH"]
    assert summary["holdings"]["missing"] == 1
    assert summary["totals"] == {
        "total": 5, "covered": 2, "no_events": 1, "failed": 1, "missing": 1,
    }
    assert "last_sync" in summary and "scheduler" in summary
