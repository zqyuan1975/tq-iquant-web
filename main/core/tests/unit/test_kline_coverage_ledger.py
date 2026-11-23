# -*- coding: utf-8 -*-
"""K 线数据覆盖台账（P1-4，2026-11-23）。

覆盖：
- 指纹（mtime/size）不一致 → 整份记录作废、退回逐日核对；
- 缺口全部在尾部 → 直接信任台账；
- 头部/中间缺口 → 不信任；
- 通达信源的头尾缺口不得判为 source_empty（只有内部缺口才算停牌）；
- 台账查询/状态入口可用。
"""
import os
from datetime import date
from pathlib import Path

import pandas as pd

from core.services import kline_coverage_ledger as ledger


def _db(tmp_path):
    return tmp_path / "kline_ledger.sqlite3"


def _src(tmp_path):
    p = tmp_path / "source.bin"
    p.write_bytes(b"v1")
    return p


def _record(db, src, ranges):
    ledger.record_coverage(
        "600000.SH", "1d", ledger.FIELD_ALL, ranges,
        db_path=db, source_path=src,
    )


def test_ledger_path_resolves_single_data_dir(monkeypatch):
    """锁死路径解析：配置 ``data/kline_ledger.sqlite3`` 只能出现一层 data。

    回归背景：此前相对路径锚到主库所在目录（main/data），再拼配置里的
    ``data/...``，得到 ``main/data/data/kline_ledger.sqlite3``。
    """
    from core.db import _MAIN_DIR

    monkeypatch.delenv("TQ_KLINE_LEDGER_PATH", raising=False)
    monkeypatch.setattr("core.db.use_app_root", lambda: False)
    monkeypatch.setattr(
        "core.config.load_config",
        lambda *args, **kwargs: {
            "backtest": {"kline_ledger": {"path": "data/kline_ledger.sqlite3"}}
        },
    )

    resolved = ledger.ledger_path()
    assert resolved == Path(_MAIN_DIR) / "data" / "kline_ledger.sqlite3"
    assert f"data{os.sep}data" not in str(resolved)


def test_stamp_mismatch_invalidates_whole_record(tmp_path):
    db, src = _db(tmp_path), _src(tmp_path)
    _record(db, src, [("2026-01-05", "2026-01-09")])

    assert ledger.load_record(
        "600000.SH", "1d", db_path=db)["stamp_valid"] is True

    # 指纹未变：尾部缺口可信任
    plan = ledger.plan_from_ledger(
        "600000.SH", "1d", date(2026, 1, 5), date(2026, 1, 13), db_path=db)
    assert plan["trusted"] is True
    assert plan["reason"] == "tail_gap"

    # 源库文件变化（size 变）→ 整份记录作废，退回逐日核对
    src.write_bytes(b"v2-changed")
    plan2 = ledger.plan_from_ledger(
        "600000.SH", "1d", date(2026, 1, 5), date(2026, 1, 13), db_path=db)
    assert plan2["trusted"] is False
    assert plan2["reason"] == "stamp_mismatch"
    assert plan2["stamp_valid"] is False

    # 显式作废后，台账里不再有该记录
    assert ledger.invalidate_if_stamp_changed(
        "600000.SH", "1d", db_path=db) is True
    rec = ledger.load_record("600000.SH", "1d", db_path=db)
    assert rec["ranges"] == []
    assert rec["stamp"] is None


def test_tail_gap_is_trusted(tmp_path):
    db, src = _db(tmp_path), _src(tmp_path)
    _record(db, src, [("2026-01-05", "2026-01-09")])

    # 01-12、01-13 是最后一个已知日 01-09 之后的尾部缺口
    plan = ledger.plan_from_ledger(
        "600000.SH", "1d", date(2026, 1, 5), date(2026, 1, 13), db_path=db)
    assert plan["trusted"] is True
    assert plan["reason"] == "tail_gap"
    assert plan["tail_gap"] is True
    assert plan["missing_days"] == ["2026-01-12", "2026-01-13"]


def test_internal_gap_is_not_trusted(tmp_path):
    db, src = _db(tmp_path), _src(tmp_path)
    # 中间缺 01-08，后面仍有 01-09~01-13
    _record(db, src, [("2026-01-05", "2026-01-07"),
                      ("2026-01-09", "2026-01-13")])

    plan = ledger.plan_from_ledger(
        "600000.SH", "1d", date(2026, 1, 5), date(2026, 1, 13), db_path=db)
    assert plan["trusted"] is False
    assert plan["reason"] == "middle_gap"
    assert plan["missing_days"] == ["2026-01-08"]


def test_head_and_tail_gaps_are_not_source_empty():
    local = [date(2026, 1, 6), date(2026, 1, 7), date(2026, 1, 9)]
    missing = [date(2026, 1, 5), date(2026, 1, 8), date(2026, 1, 12)]

    result = ledger.classify_gaps(missing, local, source=ledger.SOURCE_TDX)
    # 01-08 夹在 01-07 与 01-09 之间 → 可判停牌
    assert result["by_day"]["2026-01-08"] == ledger.GAP_SOURCE_EMPTY
    # 头部 01-05 / 尾部 01-12 没有本地数据夹在中间 → 不得判无行情
    assert result["by_day"]["2026-01-05"] != ledger.GAP_SOURCE_EMPTY
    assert result["by_day"]["2026-01-12"] != ledger.GAP_SOURCE_EMPTY
    assert result["by_day"]["2026-01-05"] == ledger.GAP_NOT_DOWNLOADED
    assert result["by_day"]["2026-01-12"] == ledger.GAP_NOT_DOWNLOADED

    # 即使台账误把头部/尾部登记为无行情，也不能判 source_empty
    result2 = ledger.classify_gaps(
        missing, local, source=ledger.SOURCE_TDX,
        ledger_empty_days=[date(2026, 1, 5), date(2026, 1, 12)],
    )
    assert result2["by_day"]["2026-01-05"] != ledger.GAP_SOURCE_EMPTY
    assert result2["by_day"]["2026-01-12"] != ledger.GAP_SOURCE_EMPTY


def test_stamp_change_rebuilds_instead_of_merging(tmp_path):
    db, src = _db(tmp_path), _src(tmp_path)
    ledger.record_observation(
        "600000.SH", "1d", ledger.FIELD_ALL,
        ["2026-01-05", "2026-01-06", "2026-01-07"],
        db_path=db, source_path=src,
    )
    # 源库变化后重新观测：旧登记整份作废，只保留本次区间
    src.write_bytes(b"v2")
    ledger.record_observation(
        "600000.SH", "1d", ledger.FIELD_ALL,
        ["2026-01-09", "2026-01-12", "2026-01-13"],
        db_path=db, source_path=src,
    )
    rec = ledger.load_record("600000.SH", "1d", db_path=db)
    assert rec["ranges_text"] == [["2026-01-09", "2026-01-13"]]


def test_query_and_status_entrypoints(tmp_path):
    db, src = _db(tmp_path), _src(tmp_path)
    _record(db, src, [("2026-01-05", "2026-01-09")])

    q = ledger.query_records(code="600000.SH", db_path=db)
    assert q["count"] == 1
    assert q["records"][0]["ranges"] == [["2026-01-05", "2026-01-09"]]
    assert q["records"][0]["stamp_valid"] is True

    st = ledger.status(db_path=db)
    assert st["coverage"] == 1
    assert st["stamps"] == 1


def _close_frame(dates, values):
    df = pd.DataFrame({"600000.SH": values}, index=pd.to_datetime(dates))
    return df


def test_build_klines_ledger_is_default_off(monkeypatch, tmp_path):
    """默认关闭：build_klines 旁路登记不建库、不改变取数结果。"""
    from core.services import backtest_service as bs

    db = tmp_path / "ledger.sqlite3"
    monkeypatch.delenv("TQ_KLINE_LEDGER_ENABLED", raising=False)
    monkeypatch.setenv("TQ_KLINE_LEDGER_PATH", str(db))

    raw = {"1d": {"Close": _close_frame(["2026-01-05"], [1.0])}}
    bs._record_kline_ledger(raw, ["600000.SH"], ["1d"], date(2026, 1, 5), date(2026, 1, 5))
    assert not db.exists()


def test_build_klines_ledger_records_when_enabled(monkeypatch, tmp_path):
    from core.services import backtest_service as bs

    db = tmp_path / "ledger.sqlite3"
    monkeypatch.setenv("TQ_KLINE_LEDGER_ENABLED", "1")
    monkeypatch.setenv("TQ_KLINE_LEDGER_PATH", str(db))

    # 01-07/01-08 是内部缺日 → 登记为 source_empty 候选；头尾不登记
    raw = {"1d": {"Close": _close_frame(
        ["2026-01-05", "2026-01-06", "2026-01-09"], [1.0, 2.0, 3.0])}}
    bs._record_kline_ledger(
        raw, ["600000.SH"], ["1d"], date(2026, 1, 5), date(2026, 1, 9))

    rec = ledger.load_record("600000.SH", "1d", db_path=db)
    assert rec["ranges_text"] == [["2026-01-05", "2026-01-06"],
                                  ["2026-01-09", "2026-01-09"]]
    assert rec["empty_days_text"] == ["2026-01-07", "2026-01-08"]
