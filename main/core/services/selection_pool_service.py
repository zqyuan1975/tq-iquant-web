# -*- coding: utf-8 -*-
"""条件选股池 ↔ 回测 / 实盘接入。

两条消费链路（同一套数据，不同取法）：

* **回测**：选序列池 ``SEL_{POOL}``，按 ``stock_pool_memberships`` 的生效区间
  逐日取成分（PIT）。未发布过的交易日没有任何区间 → 回测引擎按既有约定
  fail-open 回落当前成分并记 ``pool_pit_uncovered``；本模块的解析接口会显式
  标 ``available=False``，不假装有历史。
* **实盘**：会话关联组合（``live_session_portfolios`` → ``portfolio_strategies``）
  的 ``stock_pool_id`` 指向**版本池** ``SEL_{POOL}_{YYYYMMDD}``，并置
  ``pool_pit_enabled=1``；版本池成分固定，盘中不会因新快照漂移。

不改任何表结构：两种池都是既有 ``stock_pools`` 行，绑定信息落在既有组合列
（``stock_pool_id`` / ``pool_pit_enabled``），返回 before 快照支持一键回退。
"""
from __future__ import annotations

import re
from datetime import date
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from core.engine.pool_timeline import build_pool_timeline
from core.models.live_session import LiveSession
from core.models.live_session_portfolio import LiveSessionPortfolio
from core.models.portfolio_strategy import PortfolioStrategy
from core.models.selection import SelectionSnapshot
from core.models.stock_pool import StockPool
from core.models.stock_pool_membership import StockPoolMembership
from core.models.stock_pool_stock import StockPoolStock

SERIES_PREFIX = "SEL_"
_VERSION_SUFFIX = re.compile(r"_\d{8}$")


class PoolBindError(ValueError):
    """绑定/解析参数不合法（API 层转 400）。"""


def _kind(code: str) -> str:
    """version=SEL_风格_日期 / series=SEL_风格 / other=非条件选股池（既有四池）。"""
    if not (code or "").startswith(SERIES_PREFIX):
        return "other"
    return "version" if _VERSION_SUFFIX.search(code) else "series"


def _snapshot_index(db: Session) -> Dict[str, SelectionSnapshot]:
    """版本池 code → 快照：优先 published_pool_id，其次按默认命名反查。"""
    snaps = db.query(SelectionSnapshot).all()
    out: Dict[str, SelectionSnapshot] = {}
    by_pool_id = {s.published_pool_id: s for s in snaps if s.published_pool_id}
    pools = db.query(StockPool).filter(StockPool.code.like(SERIES_PREFIX + "%")).all()
    for pool in pools:
        snap = by_pool_id.get(pool.id)
        if snap is not None:
            out[pool.code] = snap
    return out


def list_pools(db: Session) -> List[Dict[str, Any]]:
    """条件选股池清单（供回测/实盘的选择器消费）。

    既有四池（成长 251 / 题材 150 / 备选 496 / 1109）不带 SEL_ 前缀，天然不被
    列出；条件选股新建自己的池，不覆盖、不合并。
    """
    snap_by_code = _snapshot_index(db)
    pools = (db.query(StockPool)
             .filter(StockPool.code.like(SERIES_PREFIX + "%"))
             .order_by(StockPool.code).all())
    pool_ids = [p.id for p in pools]
    current_counts: Dict[int, int] = {}
    interval_counts: Dict[int, int] = {}
    if pool_ids:
        for pool_id, count in (db.query(StockPoolStock.pool_id,
                                        __import__("sqlalchemy").func.count())
                               .filter(StockPoolStock.pool_id.in_(pool_ids))
                               .group_by(StockPoolStock.pool_id).all()):
            current_counts[pool_id] = count
        for pool_id, count in (db.query(StockPoolMembership.pool_id,
                                        __import__("sqlalchemy").func.count())
                               .filter(StockPoolMembership.pool_id.in_(pool_ids))
                               .group_by(StockPoolMembership.pool_id).all()):
            interval_counts[pool_id] = count

    out = []
    for pool in pools:
        snap = snap_by_code.get(pool.code)
        kind = _kind(pool.code)
        out.append({
            "pool_id": pool.id,
            "code": pool.code,
            "name": pool.name,
            "kind": kind,
            "snapshot_id": snap.id if snap else None,
            "as_of": snap.as_of.isoformat() if snap and snap.as_of else None,
            "pool_key": None,
            "frozen": bool(snap.frozen) if snap else False,
            "stock_count": current_counts.get(pool.id, 0),
            "interval_count": interval_counts.get(pool.id, 0),
            "synced_at": pool.synced_at.isoformat(timespec="seconds")
            if pool.synced_at else None,
        })
    return out


def resolve_pool(db: Session, pool_id: int,
                 day: Optional[date] = None) -> Dict[str, Any]:
    """按日期解析池成分（PIT）。

    ``available=False`` 表示该日没有任何成员区间（历史未发布）——此时返回当前
    成分仅供调用方 fail-open 参照，绝不冒充「该日历史成分」。
    """
    pool = db.get(StockPool, pool_id)
    if pool is None:
        raise LookupError("股票池不存在：%s" % pool_id)
    rows = (db.query(StockPoolMembership)
            .filter_by(pool_id=pool_id).all())
    timeline = build_pool_timeline(rows)
    target = day or date.today()

    def _interval_codes() -> Optional[List[str]]:
        """回放补建的历史区间（selection_pool_intervals）优先于当前成分。"""
        from core.selection import replay as replay_svc
        from core.models.selection import SelectionPoolInterval
        from sqlalchemy import or_
        key = replay_svc.pool_key_for_code(db, pool.code)
        if not key:
            return None
        hits = (db.query(SelectionPoolInterval.code)
                .filter(SelectionPoolInterval.pool_key == key,
                        SelectionPoolInterval.effective_from <= target,
                        or_(SelectionPoolInterval.effective_to.is_(None),
                            SelectionPoolInterval.effective_to >= target))
                .all())
        return sorted({code for (code,) in hits}) if hits else None

    if timeline.is_empty:
        interval_codes = _interval_codes()
        if interval_codes is not None:
            return {
                "pool_id": pool.id, "code": pool.code, "kind": _kind(pool.code),
                "day": target.isoformat(), "available": True,
                "covered": True, "codes": interval_codes,
                "source": "selection_intervals",
                "note": "按回放补建的成员区间解析（回放封闭、当期开口）",
            }
        current = [r.stock_code for r in
                   db.query(StockPoolStock).filter_by(pool_id=pool_id).all()]
        return {
            "pool_id": pool.id, "code": pool.code, "kind": _kind(pool.code),
            "day": target.isoformat(), "available": False,
            "covered": False, "codes": sorted(current),
            "source": "current_fallback",
            "note": "该池没有任何成员区间（从未发布过快照），只能回落当前成分",
        }
    covered = timeline.covered(target)
    if not covered:
        interval_codes = _interval_codes()
        if interval_codes is not None:
            return {
                "pool_id": pool.id, "code": pool.code, "kind": _kind(pool.code),
                "day": target.isoformat(), "available": True,
                "covered": True, "codes": interval_codes,
                "source": "selection_intervals",
                "note": "该日不在 membership 覆盖内，按回放成员区间解析",
            }
    codes = list(timeline.codes_on(target)) if covered else [
        r.stock_code for r in db.query(StockPoolStock).filter_by(pool_id=pool_id).all()
    ]
    return {
        "pool_id": pool.id, "code": pool.code, "kind": _kind(pool.code),
        "day": target.isoformat(), "available": bool(covered),
        "covered": bool(covered), "codes": sorted(codes),
        "source": "memberships" if covered else "current_fallback",
        "note": None if covered else "该交易日无成员区间，回测引擎将 fail-open 回落当前成分",
    }


def _linked_portfolios(db: Session, session_id: int) -> List[PortfolioStrategy]:
    links = (db.query(LiveSessionPortfolio)
             .filter_by(session_id=session_id).all())
    out = []
    for link in links:
        ps = db.get(PortfolioStrategy, link.portfolio_strategy_id)
        if ps is not None:
            out.append(ps)
    return out


def session_binding(db: Session, session_id: int) -> Dict[str, Any]:
    session = db.get(LiveSession, session_id)
    if session is None:
        raise LookupError("实盘会话不存在：%s" % session_id)
    items = []
    for ps in _linked_portfolios(db, session_id):
        pool = db.get(StockPool, ps.stock_pool_id) if ps.stock_pool_id else None
        snap = None
        if pool is not None and _kind(pool.code) == "version":
            snap = (db.query(SelectionSnapshot)
                    .filter_by(published_pool_id=pool.id)
                    .order_by(SelectionSnapshot.id.desc()).first())
        items.append({
            "portfolio_strategy_id": ps.id,
            "portfolio_name": ps.name,
            "stock_pool_id": ps.stock_pool_id,
            "pool_code": pool.code if pool else None,
            "pool_kind": _kind(pool.code) if pool else None,
            "snapshot_id": snap.id if snap else None,
            "as_of": snap.as_of.isoformat() if snap and snap.as_of else None,
            "frozen": bool(snap.frozen) if snap else None,
            "pool_pit_enabled": bool(ps.pool_pit_enabled),
        })
    return {"session_id": session.id, "session_name": session.name,
            "items": items}


def bind_session(db: Session, session_id: int,
                 pool_id: Optional[int] = None,
                 snapshot_id: Optional[int] = None) -> Dict[str, Any]:
    """把实盘会话挂到条件选股池版本。

    只接受「版本池」或指向版本池的快照；必须 frozen（冻结交付后 T+1 用）。
    ``pool_id`` 传序列池时显式拒绝——实盘要钉版本，用序列池会随次日发布漂移。
    """
    session = db.get(LiveSession, session_id)
    if session is None:
        raise LookupError("实盘会话不存在：%s" % session_id)

    snap = None
    if pool_id is None:
        if snapshot_id is None:
            raise PoolBindError("必须给 pool_id 或 snapshot_id")
        snap = db.get(SelectionSnapshot, snapshot_id)
        if snap is None:
            raise LookupError("快照不存在：%s" % snapshot_id)
        if not snap.frozen:
            raise PoolBindError("快照未冻结：先「冻结」再绑定实盘（冻结交付语义）")
        if not snap.published_pool_id:
            raise PoolBindError("快照尚未发布版本池：先在快照页发布")
        pool_id = snap.published_pool_id

    pool = db.get(StockPool, pool_id)
    if pool is None:
        raise LookupError("股票池不存在：%s" % pool_id)
    if not pool.code.startswith(SERIES_PREFIX):
        raise PoolBindError("只能绑定条件选股池（SEL_ 前缀）：%s" % pool.code)
    if _kind(pool.code) != "version":
        raise PoolBindError(
            "实盘只能挂版本池（SEL_风格_YYYYMMDD）；序列池 %s 会随新快照漂移" % pool.code)

    if snap is None:
        snap = (db.query(SelectionSnapshot)
                .filter_by(published_pool_id=pool.id)
                .order_by(SelectionSnapshot.id.desc()).first())
    if snap is not None and not snap.frozen:
        raise PoolBindError("目标版本池对应快照未冻结，拒绝绑定")

    portfolios = _linked_portfolios(db, session_id)
    if not portfolios:
        raise PoolBindError("会话没有关联组合，无法挂接股票池")

    before = []
    for ps in portfolios:
        before.append({
            "portfolio_strategy_id": ps.id,
            "stock_pool_id": ps.stock_pool_id,
            "pool_pit_enabled": bool(ps.pool_pit_enabled),
        })
        ps.stock_pool_id = pool.id
        ps.pool_pit_enabled = True
    db.commit()

    return {
        "session_id": session.id,
        "session_name": session.name,
        "pool_id": pool.id,
        "pool_code": pool.code,
        "pool_name": pool.name,
        "snapshot_id": snap.id if snap else None,
        "as_of": snap.as_of.isoformat() if snap and snap.as_of else None,
        "frozen": bool(snap.frozen) if snap else None,
        "stock_count": db.query(StockPoolStock)
        .filter_by(pool_id=pool.id).count(),
        "portfolios": before,
    }


def unbind_session(db: Session, session_id: int,
                   bindings: List[Dict[str, Any]]) -> Dict[str, Any]:
    """按 bind_session 返回的 before 快照回退（只认本会话的组合）。"""
    session = db.get(LiveSession, session_id)
    if session is None:
        raise LookupError("实盘会话不存在：%s" % session_id)
    owned = {ps.id for ps in _linked_portfolios(db, session_id)}
    restored = []
    for item in bindings or []:
        ps_id = item.get("portfolio_strategy_id")
        if ps_id not in owned:
            continue
        ps = db.get(PortfolioStrategy, ps_id)
        pool_id = item.get("stock_pool_id")
        if pool_id is not None and db.get(StockPool, pool_id) is None:
            raise PoolBindError("回退目标股票池不存在：%s" % pool_id)
        ps.stock_pool_id = pool_id if pool_id is not None else ps.stock_pool_id
        ps.pool_pit_enabled = bool(item.get("pool_pit_enabled", False))
        restored.append({
            "portfolio_strategy_id": ps.id,
            "stock_pool_id": ps.stock_pool_id,
            "pool_pit_enabled": bool(ps.pool_pit_enabled),
        })
    db.commit()
    return {"session_id": session.id, "restored": restored}
