"""公司行为手工台账 API（2026-11-18，J1/J2）。

基础路径 ``/api/corporate-actions``；统一 envelope（``ok``/``err``，code 与
HTTP 语义对齐但 HTTP 仍 200，与全仓其它路由一致）。

错误码：400 字段非法 / 404 不存在 / 409 重复 / 422 批量导入有非法行
（返回 ``data.errors[]`` 带行号与原因，整批回滚）。
"""
from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.services.corporate_action_service import (
    CorporateActionConflict,
    CorporateActionNotFound,
    CorporateActionValidationError,
    batch_create,
    create_action,
    delete_action,
    list_actions,
    update_action,
)

router = APIRouter(prefix="/api/corporate-actions", tags=["corporate_actions"])


class CorporateActionReq(BaseModel):
    code: Optional[str] = None
    event_type: Optional[str] = None
    ex_date: Optional[str] = None
    end_date: Optional[str] = None
    cash_per_share: Optional[float] = None
    cash_pct: Optional[float] = None
    share_ratio: Optional[float] = None
    rights_price: Optional[float] = None
    note: Optional[str] = None


class BatchReq(BaseModel):
    items: List[CorporateActionReq]


class SyncReq(BaseModel):
    # 留空 = 自动范围：池 1 ∪ 回测记录涉及的池 ∪ 各会话最新快照持仓
    codes: Optional[List[str]] = None
    pool_ids: Optional[List[int]] = None
    start: str = ""
    end: str = ""
    # 只补「未回填」的代码（默认 False = 保持既有行为：按传入范围全量刷新）
    only_missing: bool = False


class AuditReq(BaseModel):
    # 留空 = 自动范围（同 sync）；支持按池 / 最近 N 日限定，避免全量扫描
    codes: Optional[List[str]] = None
    pool_ids: Optional[List[int]] = None
    start: str = ""
    end: str = ""
    lookback_days: Optional[int] = None
    # 只算不落库（试跑），默认 False = 写诊断
    dry_run: bool = False


@router.get("")
def list_corporate_actions(code: Optional[str] = None,
                           event_type: Optional[str] = None,
                           start: Optional[str] = None,
                           end: Optional[str] = None,
                           page: int = 1,
                           page_size: int = 50,
                           db: Session = Depends(get_db)):
    try:
        start_d = date.fromisoformat(start) if start else None
        end_d = date.fromisoformat(end) if end else None
    except ValueError:
        return err(400, "start/end 日期格式应为 YYYY-MM-DD")
    return ok(list_actions(db, code=code, event_type=event_type, start=start_d,
                           end=end_d, page=page, page_size=page_size))


def _payload(req: CorporateActionReq) -> dict:
    # exclude_unset：PUT 语义下未传字段保持原值；POST 由 normalize_payload 兜底
    return req.model_dump(exclude_unset=True)


@router.post("")
def create_corporate_action(req: CorporateActionReq, db: Session = Depends(get_db)):
    try:
        return ok(create_action(db, _payload(req)))
    except CorporateActionValidationError as exc:
        return err(400, str(exc))
    except CorporateActionConflict as exc:
        return err(409, str(exc))


def _scope_view(scope: Optional[dict], missing: Optional[int] = None) -> dict:
    view = {
        "pool_ids": scope["pool_ids"],
        "pool_counts": {str(k): len(v) for k, v in scope["by_pool"].items()},
        "holdings": scope["holdings"],
    }
    if missing is not None:
        view["missing_filtered"] = int(missing)
    return view


@router.post("/sync")
def sync_from_tdx(req: SyncReq, db: Session = Depends(get_db)):
    """从通达信批量回填公司行为（逐票独立成败，不阻塞其它票）。"""
    from core.services.corporate_action_sync import (
        missing_coverage_codes, record_sync_state, release_sync,
        resolve_backfill_codes, sync_corporate_actions, try_acquire_sync,
    )
    from core.tq import gate as tq_gate
    from core.tq.gate import P2, tdx_priority

    policy = tq_gate.evaluate_policy()
    if not policy["p2_allowed"]:
        nxt = policy.get("next_p2_open_at")
        return err(409, "公司行为回填在交易时段暂停（实盘优先），%s 后可用"
                   % (nxt.strftime("%m-%d %H:%M") if nxt else "收盘后"))

    codes = [str(c).strip() for c in (req.codes or []) if str(c).strip()]
    scope = None
    missing_filtered = None
    if not codes:
        scope = resolve_backfill_codes(db, pool_ids=req.pool_ids or [1])
        codes = scope["codes"]
    if req.only_missing:
        missing = missing_coverage_codes(db, codes)
        missing_filtered = len(missing)
        codes = missing
        if not codes:
            return ok({"requested": 0, "covered": [], "no_events": [],
                       "failed": [], "added": 0, "updated": 0, "rights": 0,
                       "missing_filtered": 0, "skipped": True,
                       "message": "当前无缺失代码，无需同步"})
    if not codes:
        return err(400, "回填范围为空：请显式传 codes 或先导入股票池")

    if not try_acquire_sync():
        return err(409, "公司行为同步正在进行中，请稍后刷新结果")

    try:
        with tdx_priority(P2):
            result = sync_corporate_actions(db, codes, start=req.start, end=req.end)
    finally:
        release_sync()

    scope_view = _scope_view(scope, missing_filtered) if scope is not None else None
    record_sync_state(
        "manual", "failed" if result["failed"] else "success",
        result=result, scope=scope_view,
        message="only_missing" if req.only_missing else None)
    if scope_view is not None:
        result["scope"] = scope_view
    elif missing_filtered is not None:
        result["missing_filtered"] = missing_filtered
    return ok(result)


@router.get("/coverage")
def get_coverage(db: Session = Depends(get_db), pool_ids: Optional[str] = None):
    """公司行为覆盖统计（只读、零 TQ 调用）：分池 covered/no_events/failed/missing
    + 最近一次同步结果 + 调度状态。``pool_ids`` 支持逗号分隔，留空 = 全部池。"""
    from core.services.corporate_action_sync import coverage_summary

    wanted: Optional[List[int]] = None
    if pool_ids:
        try:
            wanted = [int(x) for x in pool_ids.split(",") if x.strip()]
        except ValueError:
            return err(400, "pool_ids 应为逗号分隔的整数")
    return ok(coverage_summary(db, pool_ids=wanted))


@router.get("/diagnostics")
def list_corporate_action_diagnostics(code: Optional[str] = None,
                                      start: Optional[str] = None,
                                      end: Optional[str] = None,
                                      page: int = 1, page_size: int = 50,
                                      db: Session = Depends(get_db)):
    """查询公司行为漏记诊断（只读观测，不影响任何账务）。"""
    from core.services.corporate_action_audit import list_diagnostics

    try:
        start_d = date.fromisoformat(start) if start else None
        end_d = date.fromisoformat(end) if end else None
    except ValueError:
        return err(400, "start/end 日期格式应为 YYYY-MM-DD")
    return ok(list_diagnostics(db, code=code, start=start_d, end=end_d,
                               page=page, page_size=page_size))


@router.post("/audit")
def audit_corporate_actions(req: AuditReq, db: Session = Depends(get_db)):
    """检测「有显著除权断点但未登记事件」，只写诊断、绝不入账或改事件。"""
    from core.services.corporate_action_audit import (
        audit_config, audit_corporate_actions as run_audit,
    )
    from core.services.corporate_action_sync import resolve_backfill_codes
    from core.tq import gate as tq_gate
    from core.tq.gate import P2, tdx_priority

    if not audit_config()["enabled"]:
        return ok({"enabled": False, "skipped": True,
                   "reason": "corporate_action_audit.enabled=false"})

    codes = req.codes or []
    scope = None
    if not codes:
        scope = resolve_backfill_codes(db, pool_ids=req.pool_ids or [1])
        codes = scope["codes"]
    if not codes:
        return err(400, "检测范围为空：请显式传 codes 或先导入股票池")

    policy = tq_gate.evaluate_policy()
    if not policy["p2_allowed"]:
        nxt = policy.get("next_p2_open_at")
        return err(409, "公司行为检测在交易时段暂停（实盘优先），%s 后可用"
                   % (nxt.strftime("%m-%d %H:%M") if nxt else "收盘后"))

    with tdx_priority(P2):
        result = run_audit(db, codes, start=req.start, end=req.end,
                           lookback_days=req.lookback_days,
                           write=not req.dry_run)
    if scope is not None:
        result["scope"] = {
            "pool_ids": scope["pool_ids"],
            "pool_counts": {str(k): len(v) for k, v in scope["by_pool"].items()},
            "holdings": scope["holdings"],
        }
    return ok(result)


@router.post("/batch")
def batch_create_corporate_actions(req: BatchReq, db: Session = Depends(get_db)):
    try:
        return ok(batch_create(db, [_payload(item) for item in req.items]))
    except CorporateActionValidationError as exc:
        detail = exc.args[0] if exc.args else str(exc)
        if isinstance(detail, list):
            return err(422, "批量导入存在非法行，整批已回滚", {"errors": detail})
        return err(400, str(exc))
    except CorporateActionConflict as exc:
        return err(409, str(exc))


@router.put("/{action_id}")
def update_corporate_action(action_id: int, req: CorporateActionReq,
                            db: Session = Depends(get_db)):
    try:
        return ok(update_action(db, action_id, _payload(req)))
    except CorporateActionValidationError as exc:
        return err(400, str(exc))
    except CorporateActionNotFound as exc:
        return err(404, str(exc))
    except CorporateActionConflict as exc:
        return err(409, str(exc))


@router.delete("/{action_id}")
def delete_corporate_action(action_id: int, db: Session = Depends(get_db)):
    try:
        delete_action(db, action_id)
        return ok({"deleted": action_id})
    except CorporateActionNotFound as exc:
        return err(404, str(exc))
