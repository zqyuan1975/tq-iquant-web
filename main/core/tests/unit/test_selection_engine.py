# -*- coding: utf-8 -*-
"""条件选股闭环：合成宽库 → 导入仓 → 引擎四池 → 运行/冻结/发布。

全部用合成数据（不依赖本机 TQData 宽库），验证的是口径与链路：
- 导入仓整表替换可重跑；
- 四池都能算出真实命中（不是占位）；
- 整列无数据 → EngineError（该池暂不计算，不拿缺数凑结果）；
- 非导入日重算 → 明确拒绝（历史回放不支持）；
- 发布同时写当前成分与 PIT 成员区间。
"""
from __future__ import annotations

import json
import math
import sqlite3
import zlib
from datetime import date, datetime, timedelta

import pytest

from core.models.live_session import LiveSession
from core.models.live_session_portfolio import LiveSessionPortfolio
from core.models.portfolio_strategy import PortfolioStrategy
from core.models.selection import (
    SelectionPoolInterval,
    SelectionScheme,
    SelectionWarehouseDividend,
    SelectionWarehouseFin,
    SelectionWarehouseMeta,
    SelectionWarehouseStock,
)
from core.models.stock_pool import StockPool
from core.models.stock_pool_membership import StockPoolMembership
from core.models.stock_pool_stock import StockPoolStock
from core.selection import engine, warehouse
from core.services import selection_pool_service as pool_svc
from core.services import selection_run_service as run_svc
from core.services import selection_service as svc

ASOF = "20261113"
FN_COLS = warehouse.REQUIRED_FN

# code, name, sector, pe, pb, 总市值(亿), 流通市值(亿), 收盘价, 有一致预期, 有分红
STOCKS = [
    ("000001.SZ", "A银行", "银行", 10.0, 1.0, 120.0, 100.0, 10.0, True, True),
    ("000002.SZ", "B银行", "银行", 40.0, 4.0, 60.0, 50.0, 8.0, True, True),
    ("000003.SZ", "C电子", "电子", 20.0, 3.0, 360.0, 300.0, 30.0, False, False),
]


def _quarter_tags():
    return ["%d%s" % (year, md) for year in range(2021, 2025)
            for md in ("0331", "0630", "0930", "1231")]


def _fin_rows(idx: int):
    """16 个季度（含 4 个年报），按 idx 分强弱，保证四池各有排名差异。"""
    strong = 1.0 + 0.3 * idx
    out = []
    for i, tag in enumerate(_quarter_tags()):
        year_idx = int(tag[:4]) - 2021
        annual_np = 1e8 * strong * (1.15 ** year_idx)
        annual_rev = 8e8 * strong * (1.12 ** year_idx)
        out.append({
            "tag_time": tag,
            "announce_time": tag,
            "FN197": 8.0 + idx * 3 + i * 0.1,
            "FN336": 0.0,
            "FN96": annual_np,
            "FN307": annual_np * 1.4,
            "FN319": annual_rev,
            "FN107": 1.5e8 * strong,
            "FN114": 3e7 * strong,
            "FN125": 3e7 * strong,
            "FN232": 3e7 * strong * (1 + 0.02 * i),
            "FN230": 1.2e8 * strong * (1 + 0.02 * i),
            "FN184": 12.0 + idx,
            "FN285": 10.0 + idx,
            "FN329": 6.0 + idx * 2 + i * 0.1,
            "FN202": 25.0 + idx * 4,
            "FN228": 0.8,
            "FN170": 2.0,
            "FN210": 40.0,
            "FN327": 20.0,
            "FN337": 30.0,
            "FN238": 1e9,
            "FN239": 8e8,
        })
    return out


def _kline_rows(price: float):
    start = datetime(2026, 6, 1)
    out = []
    for i in range(60):
        day = (start + timedelta(days=i)).strftime("%Y%m%d")
        close = price * (1 + 0.05 * math.sin(i / 7.0))
        out.append((day, round(close, 4)))
    return out


def _payload(obj) -> bytes:
    return zlib.compress(json.dumps(obj).encode("utf-8"))


def _build_wide_db(path):
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()
    cur.execute("CREATE TABLE universe (asof TEXT, code TEXT, name TEXT, exchange TEXT)")
    cur.execute("CREATE TABLE sector_map (asof TEXT, code TEXT, sector_code TEXT, "
                "sector_name TEXT, pool TEXT, tags TEXT, is_financial INTEGER)")
    cur.execute("CREATE TABLE snapshot_json (asof TEXT, code TEXT, family TEXT, payload BLOB)")
    cur.execute("CREATE TABLE go_snapshot (code TEXT, asof TEXT, GO8 REAL, GO10 REAL, "
                "GO11 REAL, GO13 REAL, GO23 REAL, GO33 REAL)")
    cur.execute("CREATE TABLE gp_snapshot (code TEXT, asof TEXT, GP16 REAL)")
    cur.execute("CREATE TABLE fin_wide (code TEXT, tag_time TEXT, announce_time TEXT, %s)"
                % ", ".join("%s REAL" % fn for fn in FN_COLS))
    cur.execute("CREATE TABLE dividend_event (code TEXT, ex_date TEXT, year INTEGER, bonus REAL)")
    cur.execute("CREATE TABLE kline_daily (code TEXT, trade_date TEXT, close REAL)")

    for idx, (code, name, sector, pe, pb, zsz, ltsz, price, has_go, has_div) in enumerate(STOCKS):
        cur.execute("INSERT INTO universe VALUES (?,?,?,?)", (ASOF, code, name, "SZ"))
        cur.execute("INSERT INTO sector_map VALUES (?,?,?,?,?,?,?)",
                    (ASOF, code, "S%d" % idx, sector, "综合", "", 0))
        cur.execute("INSERT INTO snapshot_json VALUES (?,?,?,?)", (ASOF, code, "more_info",
                    _payload({"StaticPE_TTM": pe, "PB_MRQ": pb, "Zsz": zsz,
                              "Ltsz": ltsz, "DYRatio": 5.0, "HqDate": ASOF})))
        cur.execute("INSERT INTO snapshot_json VALUES (?,?,?,?)", (ASOF, code, "stock_info",
                    _payload({"IsSTGP": 0, "IsQuitGP": 0,
                              "J_start": "20100101", "J_zgb": 1e9})))
        if has_go:
            strong = 1.0 + 0.3 * idx
            cur.execute("INSERT INTO go_snapshot VALUES (?,?,?,?,?,?,?,?)",
                        (code, ASOF, 1e8 * strong, 1.2e8 * strong,
                         5e8 * strong, 5.75e8 * strong, 0.9, 1e9))
        cur.execute("INSERT INTO gp_snapshot VALUES (?,?,?)", (code, ASOF, zsz * 1e4))

        fin = _fin_rows(idx)
        placeholders = ", ".join("?" * (3 + len(FN_COLS)))
        for row in fin:
            vals = [code, row["tag_time"], row["announce_time"]] + [row[fn] for fn in FN_COLS]
            cur.execute("INSERT INTO fin_wide VALUES (%s)" % placeholders, vals)

        if has_div:
            for year, bonus in ((2022, 0.5), (2023, 0.5), (2024, 0.6)):
                cur.execute("INSERT INTO dividend_event VALUES (?,?,?,?)",
                            (code, "%d0615" % year, year, bonus))

        for day, close in _kline_rows(price):
            cur.execute("INSERT INTO kline_daily VALUES (?,?,?)", (code, day, close))

    conn.commit()
    conn.close()


@pytest.fixture
def wide_db_file(tmp_path):
    path = tmp_path / "wide.sqlite3"
    _build_wide_db(path)
    return str(path)


@pytest.fixture
def imported(db_session, wide_db_file):
    result = warehouse.import_warehouse(db_session, path=wide_db_file)
    return db_session, result


def test_import_counts_and_idempotent(imported):
    db, result = imported
    assert result["as_of"] == "2026-11-13"
    counts = result["counts"]
    assert counts["universe"] == 3
    assert counts["fin"] == 3
    assert counts["dividend"] == 2
    assert counts["go"] == 2          # C 电子没有一致预期
    assert counts["gp16"] == 3
    assert counts["more_info"] == 3
    assert counts["kline"] == 3

    assert db.query(SelectionWarehouseMeta).count() == 1
    assert db.query(SelectionWarehouseStock).count() == 3
    assert db.query(SelectionWarehouseFin).count() == 3
    assert db.query(SelectionWarehouseDividend).count() == 2

    # 重跑同一快照：整表替换而不是追加
    again = warehouse.import_warehouse(db, path=result["source_path"])
    assert again["counts"]["universe"] == 3
    assert db.query(SelectionWarehouseStock).count() == 3


def test_engine_four_pools_real_hits(imported):
    db, _ = imported

    dividend = engine.compute(db, svc.default_spec("dividend"))
    assert dividend["pool_key"] == "dividend"
    assert dividend["stats"]["universe"] == 3
    assert dividend["stats"]["selected"] == 2       # 只有 A/B 有连续分红
    codes = {row["code"] for row in dividend["rows"] if row["selected"]}
    assert codes == {"000001.SZ", "000002.SZ"}
    for row in dividend["rows"]:
        if row["selected"]:
            assert row["score"] is not None and 0 <= row["score"] <= 1

    quality = engine.compute(db, svc.default_spec("quality"))
    assert quality["stats"]["selected"] == 3

    value = engine.compute(db, svc.default_spec("value"))
    assert value["stats"]["selected"] == 3
    top = [row for row in value["rows"] if row["selected"]][0]
    assert top["rank"] == 1 and top["score"] is not None

    growth = engine.compute(db, svc.default_spec("growth"))
    assert growth["stats"]["track_mode"] == "both"
    assert growth["stats"]["tracks"]["forward"]["eligible"] == 2
    assert growth["stats"]["tracks"]["historical"]["eligible"] == 1
    tracks = {(row["code"], row["track"]) for row in growth["rows"] if row["selected"]}
    assert ("000003.SZ", "historical") in tracks


def test_engine_replay_asof_uses_pit_only(imported):
    """历史 as_of 走 PIT 重建：重建不出因子时明确报错，绝不回落导入日快照。

    回放内核落地后，历史 as_of 不再直接拒绝；这里用「没有 GP/历史 K 线」的
    最小导入仓验证它不会拿导入日数据冒充历史值，而是抛 PIT 相关错误。
    """
    db, _ = imported
    with pytest.raises(engine.EngineError, match="历史回放"):
        engine.compute(db, svc.default_spec("value"), date(2026, 11, 12))


def test_engine_whole_column_missing_does_not_compute(imported):
    """分红序列整列缺失 → 红利池「暂不计算」，不拿缺数硬算。"""
    db, _ = imported
    db.query(SelectionWarehouseDividend).delete()
    db.commit()
    with pytest.raises(engine.EngineError, match="整列无数据"):
        engine.compute(db, svc.default_spec("dividend"))


def test_run_freeze_publish_chain(imported):
    db, _ = imported
    svc.seed_default_schemes(db)

    out = run_svc.run_schemes(db)
    assert out["as_of"] == "2026-11-13"
    assert len(out["runs"]) == 4
    assert all(run["status"] == "succeeded" for run in out["runs"])

    dividend_run = next(r for r in out["runs"] if r["pool_key"] == "dividend")
    assert dividend_run["stock_count"] == 2
    detail = run_svc.run_detail(db, dividend_run["id"], include_stocks=True,
                                selected_only=True)
    assert {s["code"] for s in detail["stocks"]} == {"000001.SZ", "000002.SZ"}
    assert detail["quality"]["warehouse_as_of"] == "2026-11-13"

    snap_id = detail["snapshot_id"]
    with pytest.raises(run_svc.RunError, match="未冻结"):
        run_svc.publish(db, snap_id)

    frozen = run_svc.freeze(db, snap_id)
    assert frozen["frozen"] is True

    pub = run_svc.publish(db, snap_id)
    assert pub["pool_code"] == "SEL_DIVIDEND_20261113"
    assert pub["stock_count"] == 2

    pool = db.query(StockPool).filter_by(code=pub["pool_code"]).one()
    members = db.query(StockPoolStock).filter_by(pool_id=pool.id).all()
    assert {m.stock_code for m in members} == {"000001.SZ", "000002.SZ"}

    intervals = db.query(StockPoolMembership).filter_by(pool_id=pool.id).all()
    assert {m.stock_code for m in intervals} == {"000001.SZ", "000002.SZ"}
    assert all(m.effective_from == date(2026, 11, 13) for m in intervals)
    assert all(m.effective_to is None for m in intervals)

    # 再发布一次：复用池、区间不重复刷行
    run_svc.publish(db, snap_id)
    assert db.query(StockPoolMembership).filter_by(pool_id=pool.id).count() == 2


def test_run_skips_scheme_without_active_version(imported):
    db, _ = imported
    svc.create_scheme(db, "custom", "自定义", "value")
    out = run_svc.run_schemes(db)
    assert out["runs"] == []
    assert len(out["skipped"]) == 1
    assert out["skipped"][0]["status"] == "skipped"
    assert "没有启用版本" in out["skipped"][0]["error"]


def test_deliver_dual_pools_and_backtest_resolve(imported):
    """T 盘后交付：版本池（实盘钉版本）+ 序列池（回测按日取成分）。"""
    db, _ = imported
    svc.seed_default_schemes(db)
    out = run_svc.deliver(db)

    assert len(out["delivered"]) == 4
    pools = pool_svc.list_pools(db)
    series = [p for p in pools if p["kind"] == "series"]
    versions = [p for p in pools if p["kind"] == "version"]
    assert len(series) == 4
    assert len(versions) == 4

    value_series = next(p for p in series if p["code"] == "SEL_VALUE")
    resolved = pool_svc.resolve_pool(
        db, value_series["pool_id"], day=date(2026, 11, 13))
    assert resolved["available"] is True
    assert resolved["source"] == "memberships"
    assert len(resolved["codes"]) == 3

    # 未发布过的交易日：显式 available=false，不冒充历史
    uncovered = pool_svc.resolve_pool(
        db, value_series["pool_id"], day=date(2026, 1, 5))
    assert uncovered["available"] is False
    assert uncovered["source"] == "current_fallback"

    # 版本池只有那一天：前一日不可用
    value_version = next(p for p in versions if p["code"] == "SEL_VALUE_20261113")
    assert pool_svc.resolve_pool(
        db, value_version["pool_id"], day=date(2026, 11, 13))["available"] is True
    assert pool_svc.resolve_pool(
        db, value_version["pool_id"], day=date(2026, 11, 12))["available"] is False


def test_pool_list_counts_selection_intervals_without_membership(db_session):
    """只有 selection_pool_intervals 的序列池也要显示非 0 PIT 区间。

    回放补建历史成分时只写区间表、不写 ``stock_pool_memberships``；池清单若只
    统计后者，新建序列池的「PIT 区间」恒为 0，与按日解析结果矛盾。
    """
    db_session.add(SelectionScheme(code="only_interval", name="仅区间池",
                                   pool_key="test:only", status="active"))
    db_session.add(StockPool(code="SEL_TEST_ONLY", name="仅区间池"))
    db_session.flush()
    for code in ("000001.SZ", "000002.SZ"):
        db_session.add(SelectionPoolInterval(
            pool_key="test:only", code=code,
            effective_from=date(2024, 4, 30), effective_to=None,
            source="backfill", note="replay:test"))
    db_session.commit()

    item = next(p for p in pool_svc.list_pools(db_session)
                if p["code"] == "SEL_TEST_ONLY")
    assert item["membership_count"] == 0
    assert item["interval_count"] == 2


def test_bind_session_to_version_pool_and_rollback(imported):
    db, _ = imported
    svc.seed_default_schemes(db)
    out = run_svc.deliver(db)
    value = next(d for d in out["delivered"] if d["pool_key"] == "value")

    legacy = StockPool(code="成长", name="既有成长池")
    db.add(legacy)
    db.commit()
    ps = PortfolioStrategy(name="实盘组合A", stock_pool_id=legacy.id,
                           pool_pit_enabled=False)
    db.add(ps)
    db.commit()
    session = LiveSession(name="实盘会话A", mode="simulation", status="stopped")
    db.add(session)
    db.commit()
    db.add(LiveSessionPortfolio(session_id=session.id,
                               portfolio_strategy_id=ps.id))
    db.commit()

    bound = pool_svc.bind_session(db, session.id,
                                  snapshot_id=value["snapshot_id"])
    assert bound["pool_code"] == "SEL_VALUE_20261113"
    assert bound["stock_count"] == 3
    db.refresh(ps)
    assert ps.stock_pool_id == value["version_pool"]["pool_id"]
    assert ps.pool_pit_enabled is True

    binding = pool_svc.session_binding(db, session.id)
    assert binding["items"][0]["pool_kind"] == "version"
    assert binding["items"][0]["frozen"] is True
    assert binding["items"][0]["as_of"] == "2026-11-13"

    # 序列池禁止挂实盘（会随新快照漂移）
    with pytest.raises(pool_svc.PoolBindError, match="版本池"):
        pool_svc.bind_session(db, session.id,
                              pool_id=value["series_pool"]["pool_id"])

    # 一键回退到绑定前的池
    pool_svc.unbind_session(db, session.id, bound["portfolios"])
    db.refresh(ps)
    assert ps.stock_pool_id == legacy.id
    assert ps.pool_pit_enabled is False


def test_bind_requires_frozen_snapshot(imported):
    db, _ = imported
    svc.seed_default_schemes(db)
    result = run_svc.run_schemes(db)
    value_run = next(r for r in result["runs"] if r["pool_key"] == "value")
    legacy = StockPool(code="备选", name="既有备选池")
    db.add(legacy)
    db.commit()
    ps = PortfolioStrategy(name="实盘组合B", stock_pool_id=legacy.id)
    db.add(ps)
    db.commit()
    session = LiveSession(name="实盘会话B", mode="simulation", status="stopped")
    db.add(session)
    db.commit()
    db.add(LiveSessionPortfolio(session_id=session.id,
                               portfolio_strategy_id=ps.id))
    db.commit()

    with pytest.raises(pool_svc.PoolBindError, match="冻结"):
        pool_svc.bind_session(db, session.id,
                              snapshot_id=value_run["snapshot_id"])
