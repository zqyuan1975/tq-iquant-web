# -*- coding: utf-8 -*-
"""成分股快照目标行业：日线全量（含空头），30m 只算由空转多且限流。"""
from datetime import date

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models.investment_board import BoardIndustrySnapshot
from core.services.board_service import (
    _STOCK_30M_MAX_INDUSTRIES,
    _stock_target_industries,
)

DAY = date(2026, 11, 20)


def _db(rows):
    engine = create_engine("sqlite://")
    BoardIndustrySnapshot.__table__.create(engine)
    db = sessionmaker(bind=engine)()
    for code, period, direction, bull in rows:
        db.add(BoardIndustrySnapshot(
            as_of=DAY, period=period, industry_code=code,
            direction=direction, bull_dur=bull))
    db.commit()
    return db


def test_daily_targets_include_bear_industries():
    db = _db([("881055.SH", "1d", -1, 0), ("881002.SH", "1d", -1, 0),
              ("881459.SH", "1d", 1, 3)])
    try:
        assert sorted(_stock_target_industries(db, "1d", DAY)) == [
            "881002.SH", "881055.SH", "881459.SH"]
    finally:
        db.close()


def test_30m_targets_only_reversal_and_capped():
    rows = [(f"8810{i:02d}.SH", "30m", 1, 1) for i in range(25)]
    rows += [("881999.SH", "30m", 1, 5), ("881998.SH", "30m", -1, 0)]
    db = _db(rows)
    try:
        got = _stock_target_industries(db, "30m", DAY)
        assert len(got) == _STOCK_30M_MAX_INDUSTRIES
        assert "881999.SH" not in got  # 非 bull_dur=1
        assert "881998.SH" not in got  # 非多头
    finally:
        db.close()
