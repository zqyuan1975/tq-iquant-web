from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.services.stock_pool_service import (
    list_local_pools as _svc_list_local_pools,
    list_tdx_pools as _svc_list_tdx_pools,
    list_tdx_stocks as _svc_list_tdx_stocks,
    sync_pool as _svc_sync_pool,
    delete_pool as _svc_delete_pool,
    list_memberships as _svc_list_memberships,
    create_membership as _svc_create_membership,
    update_membership as _svc_update_membership,
    delete_membership as _svc_delete_membership,
)
from core.tq.utils import TDXConnectionError, TdxTaskBlocked

router = APIRouter(prefix="/api/stock-pools", tags=["stock_pools"])


class SyncReq(BaseModel):
    code: str


class MembershipReq(BaseModel):
    stock_code: str
    effective_from: str
    effective_to: str | None = None
    source: str | None = None
    note: str | None = None


class MembershipUpdateReq(BaseModel):
    stock_code: str | None = None
    effective_from: str | None = None
    effective_to: str | None = None
    note: str | None = None


# ---------------------------------------------------------------------------
# GET /api/stock-pools — 本地已同步池（供组合策略引用）
# ---------------------------------------------------------------------------
@router.get("")
def list_local_pools(db: Session = Depends(get_db)):
    return ok(_svc_list_local_pools(db))


# ---------------------------------------------------------------------------
# GET /api/stock-pools/tdx — 通达信用户板块 + 本地残留合并 + synced 标记
# ---------------------------------------------------------------------------
@router.get("/tdx")
def list_tdx_pools(db: Session = Depends(get_db)):
    """直读通达信用户板块（get_user_sector），合并本地残留（通达信已删但本地还在的）。

    返回 [{code, name, synced, exists_in_tdx, stock_count}]：
    - synced: 本地是否已同步
    - exists_in_tdx: 通达信是否还有此板块（False=本地残留）
    - stock_count: 本地成分股数（未同步为 0）
    """
    try:
        data = _svc_list_tdx_pools(db)
    except TdxTaskBlocked:
        raise  # 保护窗口内 P1 被闸拒绝 → 全局处理器返回 409
    except TDXConnectionError:
        return err(500, "通达信未启动或连接失败")
    return ok(data)


# ---------------------------------------------------------------------------
# GET /api/stock-pools/tdx/{code}/stocks — 通达信成分股实时
# ---------------------------------------------------------------------------
@router.get("/tdx/{code}/stocks")
def list_tdx_stocks(code: str):
    """实时成分股。板块在通达信不存在 → 404；通达信不可达 → 500。"""
    try:
        stocks = _svc_list_tdx_stocks(code)
    except TdxTaskBlocked:
        raise  # 保护窗口内 P1 被闸拒绝 → 全局处理器返回 409
    except TDXConnectionError:
        return err(500, "通达信未启动或连接失败")
    except LookupError:
        return err(404, "板块不存在")
    return ok(stocks)


# ---------------------------------------------------------------------------
# POST /api/stock-pools/sync — upsert 本地池 + 全量替换成分股
# ---------------------------------------------------------------------------
@router.post("/sync")
def sync_pool(req: SyncReq, db: Session = Depends(get_db)):
    """按 code upsert：查通达信拿 name + 成分股，本地有则更新无则新建，全量替换成分股。"""
    try:
        data = _svc_sync_pool(db, req.code)
    except TdxTaskBlocked:
        raise  # 保护窗口 14:55–15:10 内 P1 被闸拒绝 → 全局处理器返回 409
    except TDXConnectionError:
        return err(500, "通达信未启动或连接失败")
    except LookupError:
        return err(404, "板块不存在")
    return ok(data)


# ---------------------------------------------------------------------------
# DELETE /api/stock-pools/{id} — 删本地池（CASCADE 删成分股，RESTRICT 阻止被引用）
# ---------------------------------------------------------------------------
@router.delete("/{pool_id}")
def delete_pool(pool_id: int, db: Session = Depends(get_db)):
    try:
        if not _svc_delete_pool(db, pool_id):
            return err(404, "股票池不存在")
    except IntegrityError:
        db.rollback()
        return err(409, "该股票池被组合策略引用，无法删除")
    return ok()


# ---------------------------------------------------------------------------
# 时点成分区间（J7）：手工作业入口，组合 pool_pit_enabled 开启后回测按此判定
# ---------------------------------------------------------------------------
@router.get("/{pool_id}/memberships")
def list_memberships(pool_id: int, as_of: str | None = None,
                     db: Session = Depends(get_db)):
    try:
        return ok(_svc_list_memberships(db, pool_id, as_of=as_of))
    except ValueError as exc:
        return err(400, str(exc))


@router.post("/{pool_id}/memberships")
def create_membership(pool_id: int, req: MembershipReq,
                      db: Session = Depends(get_db)):
    try:
        return ok(_svc_create_membership(db, pool_id, req.model_dump()))
    except LookupError:
        return err(404, "股票池不存在")
    except ValueError as exc:
        return err(400, str(exc))


@router.put("/memberships/{membership_id}")
def update_membership(membership_id: int, req: MembershipUpdateReq,
                      db: Session = Depends(get_db)):
    payload = {k: v for k, v in req.model_dump().items() if v is not None}
    try:
        data = _svc_update_membership(db, membership_id, payload)
    except ValueError as exc:
        return err(400, str(exc))
    if data is None:
        return err(404, "成分记录不存在")
    return ok(data)


@router.delete("/memberships/{membership_id}")
def delete_membership(membership_id: int, db: Session = Depends(get_db)):
    if not _svc_delete_membership(db, membership_id):
        return err(404, "成分记录不存在")
    return ok()

