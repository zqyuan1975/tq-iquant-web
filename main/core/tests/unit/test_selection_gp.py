# -*- coding: utf-8 -*-
"""GP16 取数落地（D2）：解析、幂等 upsert、缺口分类、除权断点核对。"""
from datetime import date

import pandas as pd
import pytest

from core.models.corporate_action import CorporateAction
from core.services import selection_gp as gp
from core.tq.data import TQData

D1 = date(2024, 4, 29)
D2 = date(2024, 4, 30)


def test_parse_gp16_handles_code_column_and_nan():
    df = pd.DataFrame({
        "Code": ["000001.SZ", "600000.SH", "000002.SZ"],
        "GP16": [1000.0, None, 3000.0],
    })
    parsed = gp.parse_gp16(df)
    assert parsed == {"000001.SZ": 1000.0, "000002.SZ": 3000.0}


def test_parse_gp16_uses_index_when_no_code_column():
    df = pd.DataFrame({"GP16": [12.0]}, index=["600519.SH"])
    assert gp.parse_gp16(df) == {"600519.SH": 12.0}


def test_ingest_is_idempotent(db_session, monkeypatch):
    calls = []

    def fake_by_date(self, stocks, year, mmdd, table_list, priority=None):
        calls.append((list(stocks), year, mmdd, list(table_list), priority))
        return pd.DataFrame({"Code": ["000001.SZ"], "GP16": [1000.0]})

    monkeypatch.setattr(TQData, "get_gpjy_value_by_date", fake_by_date)
    gp.ingest_gp16(db_session, ["000001.SZ"], D1, priority="P2")
    gp.ingest_gp16(db_session, ["000001.SZ"], D1, priority="P2")

    rows = gp.list_gp16(db_session, start=D1, end=D1)
    assert len(rows) == 1, "同一 (code, as_of) 重复导入不得刷行"
    assert rows[0]["total_mv_wan"] == 1000.0
    assert calls[0][3] == ["GP16"] and calls[0][4] == "P2"
    assert calls[0][1] == 2024 and calls[0][2] == "0429"


def test_audit_classifies_gap_and_breakpoint(db_session):
    # 前一交易日：a/b/c 都有值；当日：a 正常跳变且是除权日，b 跳变无事件，
    # c 停牌缺值，d 无任何原因缺值。
    for code, mv in (("a", 100.0), ("b", 100.0), ("c", 100.0), ("d", 100.0)):
        gp.ingest_row(db_session, code, D1, mv)
    gp.ingest_row(db_session, "a", D2, 90.0)      # -10%，有除权事件
    gp.ingest_row(db_session, "b", D2, 90.0)      # -10%，无事件
    gp.ingest_row(db_session, "c", D2, None)      # 停牌
    db_session.add_all([
        CorporateAction(code="a", event_type="dividend", ex_date=D2,
                        cash_per_share=1.0),
        CorporateAction(code="c", event_type="suspend", ex_date=D2,
                        end_date=date(2024, 5, 10)),
    ])
    db_session.commit()

    out = gp.audit_day(db_session, D2, threshold_pct=5.0)
    assert out["covered"] == 2
    assert out["prev_day"] == D1.isoformat()
    assert out["suspended"] == ["C"]
    assert out["breakpoints"]["aligned"] == [{"code": "A", "change_pct": -10.0}]
    assert out["breakpoints"]["unexplained"] == [{"code": "B", "change_pct": -10.0}]
    assert "D" in out["missing"]
    assert out["corporate"]["version"]


def test_audit_corporate_digest_changes_with_events(db_session):
    before = gp.corporate_context(db_session, as_of=D2)["digest"]
    db_session.add(CorporateAction(code="z", event_type="split", ex_date=D1,
                                   share_ratio=1.0))
    db_session.commit()
    after = gp.corporate_context(db_session, as_of=D2)["digest"]
    assert before != after, "事件集变化必须改变指纹"
