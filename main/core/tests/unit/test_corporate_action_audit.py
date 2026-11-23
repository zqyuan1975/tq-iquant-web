# -*- coding: utf-8 -*-
"""公司行为漏记检测（2026-11-23）。

覆盖用户要求的三类样本：
- 有断点且**无**事件记录 → 必须报未登记（写诊断）；
- 有断点且**有**事件记录 → 不得误报；
- 无断点 → 不报。
外加：默认开关（关闭时不取数）、dry_run 不落库、事件补录后旧诊断自动清理。
"""
from datetime import date

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from core.db import get_db
from core.main import app
from core.models import Base, CorporateAction, CorporateActionDiagnostic
from core.services import corporate_action_audit as caa

D1, D2, D3 = date(2026, 6, 10), date(2026, 6, 11), date(2026, 6, 12)

CFG = {
    "enabled": True,
    "lookback_days": 60,
    "min_drop_pct": 1.0,
    "match_tolerance_days": 5,
    "max_codes": 0,
}

# 06-11 除息：不复权 10.00 → 9.50；前复权连续（比值跳变 1.0526，隐含跌幅 5%）
RAW = {D1: 10.00, D2: 9.50, D3: 9.55}
FRONT = {D1: 10.00, D2: 10.00, D3: 10.05}
FLAT_RAW = {D1: 10.00, D2: 10.10, D3: 10.20}
FLAT_FRONT = {D1: 10.00, D2: 10.10, D3: 10.20}


def _frame(series_by_code):
    dates = sorted({d for series in series_by_code.values() for d in series})
    data = {}
    for code, series in series_by_code.items():
        data[code] = [series.get(d) for d in dates]
    frame = pd.DataFrame(data, index=pd.to_datetime(dates))
    return frame


class _FakeTQ:
    """按复权口径返回固定 Close 序列；``boom=True`` 模拟取数不可用。"""

    def __init__(self, raw, front, boom=False):
        self.raw = raw
        self.front = front
        self.boom = boom

    def get_history_raw(self, stocks, periods, start="", end="",
                        dividend_type="none", count=-1):
        if self.boom:
            raise RuntimeError("tdx down")
        series = self.front if dividend_type == "front" else self.raw
        return {"1d": {"Close": _frame({c: series[c] for c in stocks})}}


def test_detect_breakpoint_reports_implied_values():
    items = caa.detect_price_breakpoints(RAW, FRONT, min_drop_pct=1.0)

    assert len(items) == 1
    item = items[0]
    assert item["break_date"] == D2
    assert item["prev_trade_date"] == D1
    assert item["implied_ratio"] == pytest.approx(1.052632, abs=1e-6)
    assert item["implied_drop_pct"] == pytest.approx(5.0, abs=1e-3)
    assert item["prev_close"] == pytest.approx(10.0)
    assert item["implied_ex_price"] == pytest.approx(9.5, abs=1e-6)
    assert caa.suspected_event_type(item["implied_ratio"]) == "dividend"


def test_detect_no_breakpoint_returns_empty():
    assert caa.detect_price_breakpoints(FLAT_RAW, FLAT_FRONT) == []


def test_suspected_type_heuristics():
    # 10 送 3（k=1.3）→ split；纯现金（k=1.03）→ dividend；大比例且不吻合送转 → rights
    assert caa.suspected_event_type(1.3) == "split"
    assert caa.suspected_event_type(1.03) == "dividend"
    assert caa.suspected_event_type(1.13) == "rights"


def test_breakpoint_without_event_writes_unverified_diagnostic(db_session):
    tq = _FakeTQ({"600211.SH": RAW}, {"600211.SH": FRONT})

    result = caa.audit_corporate_actions(
        db_session, ["600211.SH"], start="2026-06-01", end="2026-06-30",
        tq=tq, config=CFG)

    assert result["enabled"] is True and result["skipped"] is False
    assert result["scanned"] == 1
    assert result["breakpoints"] == 1
    assert result["matched"] == 0
    assert result["unverified"] == 1
    assert result["written"] == 1
    assert result["source"] == caa.SOURCE_RAW_VS_FRONT

    row = db_session.query(CorporateActionDiagnostic).one()
    assert row.diagnostic_type == "corporate_action_unverified"
    assert row.code == "600211.SH"
    assert row.break_date == D2
    assert row.prev_trade_date == D1
    assert float(row.implied_ratio) == pytest.approx(1.052632, abs=1e-6)
    assert float(row.implied_drop_pct) == pytest.approx(5.0, abs=1e-3)
    assert float(row.prev_close) == pytest.approx(10.0)
    assert float(row.implied_ex_price) == pytest.approx(9.5, abs=1e-6)
    assert row.suspected_event_type == "dividend"
    assert row.window_start == D1 and row.window_end == D3
    # 关键约束：不得自动补录事件、不得改账
    assert db_session.query(CorporateAction).count() == 0

    # 幂等：同一断点重复检测不刷行
    again = caa.audit_corporate_actions(
        db_session, ["600211.SH"], start="2026-06-01", end="2026-06-30",
        tq=tq, config=CFG)
    assert again["written"] == 0
    assert db_session.query(CorporateActionDiagnostic).count() == 1


def test_breakpoint_with_registered_event_not_flagged(db_session):
    db_session.add(CorporateAction(code="600211.SH", event_type="dividend",
                                   ex_date=D2, cash_per_share=0.5))
    db_session.commit()
    tq = _FakeTQ({"600211.SH": RAW}, {"600211.SH": FRONT})

    result = caa.audit_corporate_actions(
        db_session, ["600211.SH"], start="2026-06-01", end="2026-06-30",
        tq=tq, config=CFG)

    assert result["breakpoints"] == 1
    assert result["matched"] == 1
    assert result["unverified"] == 0
    assert db_session.query(CorporateActionDiagnostic).count() == 0
    # 已有事件保持原样
    row = db_session.query(CorporateAction).one()
    assert float(row.cash_per_share) == pytest.approx(0.5)


def test_no_breakpoint_no_diagnostic(db_session):
    tq = _FakeTQ({"000001.SZ": FLAT_RAW}, {"000001.SZ": FLAT_FRONT})

    result = caa.audit_corporate_actions(
        db_session, ["000001.SZ"], start="2026-06-01", end="2026-06-30",
        tq=tq, config=CFG)

    assert result["scanned"] == 1
    assert result["breakpoints"] == 0
    assert result["unverified"] == 0
    assert db_session.query(CorporateActionDiagnostic).count() == 0


def test_disabled_switch_skips_without_fetching(db_session):
    tq = _FakeTQ({}, {}, boom=True)

    result = caa.audit_corporate_actions(
        db_session, ["600211.SH"], tq=tq,
        config={"enabled": False, "lookback_days": 60})

    assert result["enabled"] is False
    assert result["skipped"] is True
    assert result["reason"] == "corporate_action_audit.enabled=false"
    assert db_session.query(CorporateActionDiagnostic).count() == 0


def test_dry_run_does_not_write(db_session):
    tq = _FakeTQ({"600211.SH": RAW}, {"600211.SH": FRONT})

    result = caa.audit_corporate_actions(
        db_session, ["600211.SH"], start="2026-06-01", end="2026-06-30",
        tq=tq, config=CFG, write=False)

    assert result["unverified"] == 1
    assert result["written"] == 0
    assert db_session.query(CorporateActionDiagnostic).count() == 0


def test_event_backfilled_later_resolves_stale_diagnostic(db_session):
    tq = _FakeTQ({"600211.SH": RAW}, {"600211.SH": FRONT})
    caa.audit_corporate_actions(
        db_session, ["600211.SH"], start="2026-06-01", end="2026-06-30",
        tq=tq, config=CFG)
    assert db_session.query(CorporateActionDiagnostic).count() == 1

    db_session.add(CorporateAction(code="600211.SH", event_type="dividend",
                                   ex_date=D2, cash_per_share=0.5))
    db_session.commit()

    result = caa.audit_corporate_actions(
        db_session, ["600211.SH"], start="2026-06-01", end="2026-06-30",
        tq=tq, config=CFG)

    assert result["matched"] == 1
    assert result["resolved"] == 1
    assert db_session.query(CorporateActionDiagnostic).count() == 0


def test_fetch_failure_is_non_blocking(db_session):
    tq = _FakeTQ({}, {}, boom=True)

    result = caa.audit_corporate_actions(
        db_session, ["600211.SH"], tq=tq, config=CFG)

    assert result["skipped"] is True
    assert result["reason"] == "行情取数失败"
    assert result["failed"] == ["600211.SH"]


def test_list_diagnostics_filters_and_serializes(db_session):
    tq = _FakeTQ({"600211.SH": RAW}, {"600211.SH": FRONT})
    caa.audit_corporate_actions(
        db_session, ["600211.SH"], start="2026-06-01", end="2026-06-30",
        tq=tq, config=CFG)

    page = caa.list_diagnostics(db_session, code="600211.SH")

    assert page["total"] == 1
    item = page["items"][0]
    assert set(item) >= {
        "code", "break_date", "prev_trade_date", "implied_ratio",
        "implied_drop_pct", "prev_close", "implied_ex_price",
        "suspected_event_type", "window_start", "window_end", "source",
    }
    assert item["break_date"] == "2026-06-11"
    assert item["source"] == caa.SOURCE_RAW_VS_FRONT
    assert item["detail"]["detector"] == "adj_raw_ratio_jump"
    assert item["detail"]["heuristic_only"] is True
    # 其它代码/其它类型不串
    assert caa.list_diagnostics(db_session, code="000001.SZ")["total"] == 0


@pytest.fixture
def client():
    """内存 SQLite（StaticPool，跨线程可用）+ TestClient，覆盖 get_db。"""
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False},
        poolclass=StaticPool)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)

    def override_get_db():
        db = Session()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as c:
        yield c
    app.dependency_overrides.clear()


def test_api_audit_then_list_diagnostics(client, monkeypatch):
    fake = _FakeTQ({"600211.SH": RAW}, {"600211.SH": FRONT})
    monkeypatch.setattr("core.tq.data.TQData", lambda: fake)

    resp = client.post("/api/corporate-actions/audit", json={
        "codes": ["600211.SH"], "start": "2026-06-01", "end": "2026-06-30"})
    body = resp.json()
    assert body["code"] == 0, body
    assert body["data"]["unverified"] == 1
    assert body["data"]["written"] == 1

    listed = client.get("/api/corporate-actions/diagnostics",
                        params={"code": "600211.SH"}).json()
    assert listed["code"] == 0
    assert listed["data"]["total"] == 1
    assert listed["data"]["items"][0]["suspected_event_type"] == "dividend"

