# -*- coding: utf-8 -*-
"""投资看板 API（只读）。

- 榜单/快照读 ``board_*`` 表；
- 大盘多空卡直接复用既有门控状态服务（``fetch_gate_status``），只读不写，
  门控计算/配置/落库/策略拦截逻辑均不受影响；
- ``POST /refresh`` 只触发看板自己的刷新线程，不触碰实盘会话与维护闸。
"""
import logging
import threading
import time
from datetime import date
from typing import Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.engine.market_gate import MarketGateUnavailableError
from core.engine.rating_core import RATING_FORMULA_VERSION
from core.services import board_service as svc
from core.services.market_gate_service import fetch_gate_status

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/investment-board", tags=["investment-board"])

_CACHE_TTL_SECONDS = 60
_cache: dict = {}
_refresh_lock = threading.Lock()


def _cached(key: tuple, producer):
    now = time.monotonic()
    hit = _cache.get(key)
    if hit is not None and now - hit[0] <= _CACHE_TTL_SECONDS:
        return hit[1]
    data = producer()
    _cache[key] = (now, data)
    return data


def _parse_date(raw: Optional[str]) -> Optional[date]:
    if not raw:
        return None
    try:
        return date.fromisoformat(str(raw).strip())
    except ValueError:
        return None


def _check_period(period: Optional[str]) -> str:
    p = (period or "1d").strip()
    if p not in svc.BOARD_PERIODS:
        raise ValueError(f"period 仅支持 {svc.BOARD_PERIODS}")
    return p


class RefreshReq(BaseModel):
    period: str = "1d"
    scope: str = "all"


# ---------------- 汇总 / 元信息 ----------------

@router.get("/summary")
def get_summary(period: str = "1d", benchmark_index: Optional[str] = None,
                history: int = 5, force: bool = False,
                db: Session = Depends(get_db)):
    """大盘多空卡（等价迁移自备选股池）+ 行业等级计数 + 最新刷新任务。"""
    try:
        p = _check_period(period)
    except ValueError as exc:
        return err(422, str(exc))
    try:
        gate = fetch_gate_status(benchmark_index, period="1d",
                                 history=history, force=force)
    except MarketGateUnavailableError as exc:
        return err(400, str(exc))
    run = svc.latest_run(db, p)
    data = _cached(("summary", p, gate.get("code"), gate.get("as_of")), lambda: {
        "gate": gate,
        "period": p,
        "as_of": svc.latest_as_of(db, p).isoformat()
        if svc.latest_as_of(db, p) else None,
        "level_counts": svc.level_counts(db, p),
        "benchmark_bull_dur": svc.benchmark_bull_dur(db, p),
        "formula_version": RATING_FORMULA_VERSION,
        "latest_run": svc.serialize_run(run) if run else None,
    })
    data = dict(data)
    data["gate"] = gate
    return ok(data)


@router.get("/meta")
def get_meta(db: Session = Depends(get_db)):
    return ok(_cached(("meta",), lambda: svc.meta(db)))


# ---------------- 行业榜 ----------------

@router.get("/industries")
def get_industries(period: str = "1d", date: str = "", sort: str = "bull_dur",
                   order: str = "desc", level: str = "", limit: int = 200,
                   db: Session = Depends(get_db)):
    try:
        p = _check_period(period)
    except ValueError as exc:
        return err(422, str(exc))
    day = _parse_date(date)
    key = ("industries", p, day, sort, order, level, limit)
    return ok(_cached(key, lambda: svc.query_industries(
        db, p, day, sort=sort, order=order,
        level=level or None, limit=limit)))


@router.get("/industries/{code}")
def get_industry(code: str, period: str = "1d", date: str = "",
                 db: Session = Depends(get_db)):
    try:
        p = _check_period(period)
    except ValueError as exc:
        return err(422, str(exc))
    row = svc.query_industry(db, code.upper(), p, _parse_date(date))
    if row is None:
        return err(404, f"行业 {code} 在该周期/日期下无快照（请先刷新）")
    return ok(row)


@router.get("/industries/{code}/stocks")
def get_industry_stocks(code: str, period: str = "1d", date: str = "",
                        sort: str = "lead_days", order: str = "desc",
                        limit: int = 50, db: Session = Depends(get_db)):
    try:
        p = _check_period(period)
    except ValueError as exc:
        return err(422, str(exc))
    day = _parse_date(date)
    key = ("stocks", code.upper(), p, day, sort, order, limit)
    return ok(_cached(key, lambda: svc.query_stocks(
        db, code.upper(), p, day, sort=sort, order=order, limit=limit)))


@router.get("/leaders")
def get_leaders(period: str = "1d", date: str = "", limit: int = 10,
                db: Session = Depends(get_db)):
    try:
        p = _check_period(period)
    except ValueError as exc:
        return err(422, str(exc))
    day = _parse_date(date)
    key = ("leaders", p, day, limit)
    return ok(_cached(key, lambda: svc.query_leaders(db, p, day, limit=limit)))


# ---------------- 刷新任务 ----------------

@router.get("/runs/{run_id}")
def get_run(run_id: int, db: Session = Depends(get_db)):
    from core.models.investment_board import BoardRun

    run = db.get(BoardRun, run_id)
    if run is None:
        return err(404, "任务不存在")
    return ok(svc.serialize_run(run))


@router.post("/refresh")
def post_refresh(req: RefreshReq, db: Session = Depends(get_db)):
    try:
        p = _check_period(req.period)
    except ValueError as exc:
        return err(422, str(exc))
    scope = (req.scope or "all").strip()
    if scope not in ("industries", "stocks", "all"):
        return err(422, "scope 仅支持 industries / stocks / all")
    if not _refresh_lock.acquire(blocking=False):
        return err(409, "已有刷新任务在运行，请稍后再试")
    try:
        run = svc.create_run(db, p, scope, trigger="manual")
    except Exception:
        _refresh_lock.release()
        raise
    _cache.clear()

    def _worker(run_id: int):
        try:
            svc.execute_run(run_id, p, scope)
        finally:
            _refresh_lock.release()

    threading.Thread(target=_worker, args=(run.id,), daemon=True,
                     name=f"board-refresh-{run.id}").start()
    return ok({"run_id": run.id, "period": p, "scope": scope,
               "status": "running"})
