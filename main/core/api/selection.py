# -*- coding: utf-8 -*-
"""条件选股 API（A1 + A2）。

方案 / 版本 / 宽库导入 / 运行 / 快照冻结 / 发布股票池。
选股引擎只读平台导入仓（D1），历史回放明确拒绝而不是用未来数据凑结果。
"""

from datetime import date
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from core.api.response import err, ok
from core.db import get_db
from core.models.selection import SelectionScheme
from core.selection import (engine, field_dict, recall as recall_svc,
                            replay as replay_svc, wide_build, wide_db,
                            warehouse)
from core.selection.field_dict import UnregisteredField
from core.services import selection_pool_service as pool_svc
from core.services import selection_run_service as run_svc
from core.services import selection_service as svc

router = APIRouter(prefix="/api/selection", tags=["selection"])


class SchemeReq(BaseModel):
    code: str
    name: str
    pool_key: str
    description: str = ""


class SchemeUpdateReq(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None


class VersionReq(BaseModel):
    spec: dict
    note: str = ""
    uncalibrated: bool = True


class RunReq(BaseModel):
    as_of: Optional[date] = None
    scheme_ids: Optional[List[int]] = None


class PublishReq(BaseModel):
    code: Optional[str] = None
    name: Optional[str] = None
    series: bool = False


class DeliverReq(BaseModel):
    as_of: Optional[date] = None


class BindReq(BaseModel):
    pool_id: Optional[int] = None
    snapshot_id: Optional[int] = None


class RecallReq(BaseModel):
    snapshot_id: int
    formula_id: int
    horizon_days: int = 20
    # 入选判定窗口（交易 bar）：择时 OPEN 脉冲稀疏，单日常为 0/0；
    # 1=严格单日口径，默认 20 个交易 bar。
    signal_window_days: int = 20
    refresh: bool = False


class UnbindReq(BaseModel):
    bindings: List[dict] = []


class WarehouseBuildReq(BaseModel):
    """内置宽库构建：incremental 日常补缺，full 仅灾备且必须显式传参。"""
    mode: str = "incremental"
    as_of: Optional[date] = None
    daily_start: Optional[date] = None


def _bad(exc: Exception):
    return err(400, str(exc))


@router.get("/meta")
def selection_meta(db: Session = Depends(get_db)):
    """字段字典与池枚举：前端筛选面板直接消费，避免硬编码。"""
    from core.selection import sector_pools

    fields = [{
        "key": s.key, "source": s.source, "column": s.column, "unit": s.unit,
        "zero_is_missing": s.zero_is_missing, "pit_safe": s.pit_safe,
        "pools": list(s.pools), "note": s.note,
    } for s in field_dict.FIELD_REGISTRY.values()]
    industry_pools = sector_pools.pools_payload()
    membership: Dict[str, int] = {}
    meta = engine.warehouse_meta(db)
    if meta is not None:
        try:
            membership = sector_pools.membership_counts(db, meta.as_of)
        except Exception:  # pragma: no cover - 空表/旧库兼容
            membership = {}
    return ok({
        "field_set_version": field_dict.FIELD_SET_VERSION,
        "pools": list(svc.POOL_KEYS),
        "industry_prefix": svc.INDUSTRY_PREFIX,
        "industry_pools": industry_pools,
        "industry_membership": membership,
        "scorable_fields": sorted(field_dict.SCORABLE_FIELDS),
        "fields": fields,
    })


@router.get("/defaults")
def selection_defaults():
    """四池默认参数（D3 窄字段集 + 外部 v4 经验权重，未校准）。"""
    return ok({pool: svc.default_spec(pool) for pool in svc.POOL_KEYS})


@router.get("/data-status")
def data_status(db: Session = Depends(get_db)):
    """导入仓状态（运行期视角）+ 外部宽库只读探针（仅体检展示）。"""
    return ok(run_svc.warehouse_status(db))


@router.post("/warehouse/import")
def warehouse_import(db: Session = Depends(get_db)):
    """宽库 → 平台导入仓：唯一读取外部宽库的入口，可重跑。"""
    try:
        result = warehouse.import_warehouse(db)
    except warehouse.WarehouseError as exc:
        db.rollback()
        return err(400, str(exc))
    except Exception as exc:  # pragma: no cover - 防御性
        db.rollback()
        return err(500, "导入失败：%s" % exc)
    return ok(result)


@router.post("/warehouse/build")
def warehouse_build(req: WarehouseBuildReq):
    """触发内置宽库构建（后台线程，不阻塞请求）。

    ``full`` 是灾备模式，必须显式传 ``mode=full``；默认 ``incremental`` 只补缺口。
    """
    if req.mode not in ("incremental", "full"):
        return err(400, "mode 只能是 incremental 或 full")
    if not wide_build.try_acquire_build():
        return ok({"status": "busy", "running": True,
                   "run_id": wide_build.today_run_id()})
    run_id = wide_build.today_run_id()

    def _worker() -> None:
        try:
            wide_build.run_build(
                mode=req.mode, run_id=run_id, as_of=req.as_of,
                daily_start=req.daily_start, trigger="api")
        finally:
            wide_build.release_build()

    threading.Thread(target=_worker, name="selection-wide-build",
                     daemon=True).start()
    return ok({"status": "started", "running": True, "run_id": run_id,
               "mode": req.mode})


@router.get("/warehouse/build/status")
def warehouse_build_status(db: Session = Depends(get_db)):
    """构建进度：阶段、批次完成度、as_of 与最近一次结果。"""
    return ok(wide_build.build_status(db))


@router.get("/wide-db/status")
def wide_db_status():
    """外部宽库探针（A0-2）：不可用只报 ready=false，不抛 500。"""
    return ok(wide_db.probe())


@router.get("/schemes")
def list_schemes(include_archived: bool = False, db: Session = Depends(get_db)):
    return ok(svc.list_schemes(db, include_archived=include_archived))


@router.post("/schemes")
def create_scheme(req: SchemeReq, db: Session = Depends(get_db)):
    try:
        scheme = svc.create_scheme(db, req.code, req.name, req.pool_key,
                                   req.description)
    except (svc.SpecError, UnregisteredField) as exc:
        return _bad(exc)
    except IntegrityError:
        db.rollback()
        return err(409, "方案 code 已存在：%s" % req.code)
    return ok({"id": scheme.id, "code": scheme.code, "name": scheme.name,
               "pool_key": scheme.pool_key, "status": scheme.status})


@router.post("/schemes/seed")
def seed_schemes(db: Session = Depends(get_db)):
    """幂等种子：四风格池 + 9 个行业 L1 主池 + 14 个 L2 标签池。"""
    created = svc.seed_default_schemes(db)
    industry_created = svc.seed_industry_schemes(db)
    return ok({"created": created, "industry_created": industry_created})


@router.post("/industry/refresh")
def refresh_industry(db: Session = Depends(get_db)):
    """按当前导入仓刷新行业归属（幂等，只补缺失 as_of 行）。"""
    from core.selection import sector_pools

    meta = engine.warehouse_meta(db)
    if meta is None:
        return err(400, "尚未导入宽库：请先在「数据体检」执行导入")
    try:
        added = sector_pools.refresh_membership(db, meta.as_of)
    except Exception as exc:  # pragma: no cover - 防御性
        db.rollback()
        return err(500, "刷新行业归属失败：%s" % exc)
    return ok({"as_of": meta.as_of.isoformat(), "added": added,
               "counts": sector_pools.membership_counts(db, meta.as_of)})


@router.get("/schemes/{scheme_id}")
def get_scheme(scheme_id: int, db: Session = Depends(get_db)):
    scheme = db.get(SelectionScheme, scheme_id)
    if scheme is None:
        return err(404, "方案不存在：%s" % scheme_id)
    versions = svc.list_versions(db, scheme_id)
    return ok({
        "id": scheme.id, "code": scheme.code, "name": scheme.name,
        "pool_key": scheme.pool_key, "description": scheme.description,
        "status": scheme.status,
        "versions": [svc.version_payload(v) for v in versions],
    })


@router.put("/schemes/{scheme_id}")
def update_scheme(scheme_id: int, req: SchemeUpdateReq,
                  db: Session = Depends(get_db)):
    try:
        scheme = svc.update_scheme(
            db, scheme_id,
            name=req.name, description=req.description, status=req.status)
    except LookupError as exc:
        return err(404, str(exc))
    except svc.SpecError as exc:
        return _bad(exc)
    return ok({"id": scheme.id, "name": scheme.name,
               "pool_key": scheme.pool_key, "status": scheme.status})


@router.get("/schemes/{scheme_id}/versions")
def list_versions(scheme_id: int, db: Session = Depends(get_db)):
    return ok([svc.version_payload(v) for v in svc.list_versions(db, scheme_id)])


@router.post("/schemes/{scheme_id}/versions")
def create_version(scheme_id: int, req: VersionReq,
                   db: Session = Depends(get_db)):
    try:
        version = svc.create_version(db, scheme_id, req.spec,
                                     note=req.note,
                                     uncalibrated=req.uncalibrated)
    except LookupError as exc:
        return err(404, str(exc))
    except (svc.SpecError, UnregisteredField) as exc:
        return _bad(exc)
    return ok(svc.version_payload(version))


@router.post("/schemes/{scheme_id}/versions/{version_id}/activate")
def activate_version(scheme_id: int, version_id: int,
                     db: Session = Depends(get_db)):
    try:
        version = svc.activate_version(db, scheme_id, version_id)
    except LookupError as exc:
        return err(404, str(exc))
    return ok(svc.version_payload(version))


# --- GP16 历史总市值（D2 口径）-------------------------------------------

class GpIngestReq(BaseModel):
    day: date
    codes: Optional[List[str]] = None   # 省略 = 全市场（TQ market=5）


@router.post("/gp/ingest")
def gp_ingest(req: GpIngestReq, db: Session = Depends(get_db)):
    """按日取 GP16 并落库；P2 优先级，分片在 TQData 内完成。"""
    from core.services import selection_gp as gp
    from core.tq.data import TQData

    codes = req.codes
    if not codes:
        codes = TQData().get_all_stocks(market="5")
    try:
        result = gp.ingest_gp16(db, codes, req.day, priority="P2")
    except Exception as exc:  # TQ 不可用/分片失败：如实报错，不落半批
        db.rollback()
        return err(502, "GP16 取数失败：%s" % exc)
    return ok(result)


@router.get("/gp/snapshots")
def gp_snapshots(start: Optional[date] = None, end: Optional[date] = None,
                 codes: Optional[str] = None, db: Session = Depends(get_db)):
    from core.services import selection_gp as gp

    code_list = [c for c in (codes or "").split(",") if c.strip()] or None
    return ok(gp.list_gp16(db, start=start, end=end, codes=code_list))


@router.get("/gp/audit")
def gp_audit(day: date = Query(...), threshold_pct: float = 5.0,
            db: Session = Depends(get_db)):
    """缺口分类（停牌/真缺失）+ 除权断点核对；只报告，不改数。"""
    from core.services import selection_gp as gp

    return ok(gp.audit_day(db, day, threshold_pct=threshold_pct))


# --- 运行 / 快照 / 发布 ---------------------------------------------------

@router.post("/runs")
def create_runs(req: RunReq, db: Session = Depends(get_db)):
    """跑一次选股并落库；返回逐方案结果（含跳过原因）。"""
    try:
        result = run_svc.run_schemes(db, scheme_ids=req.scheme_ids,
                                     as_of=req.as_of)
    except run_svc.RunError as exc:
        return _bad(exc)
    except Exception as exc:  # pragma: no cover - 防御性
        db.rollback()
        return err(500, "选股失败：%s" % exc)
    return ok(result)


@router.get("/runs")
def list_runs(limit: int = 50, scheme_id: Optional[int] = None,
              db: Session = Depends(get_db)):
    return ok(run_svc.list_runs(db, limit=max(1, min(limit, 200)),
                                scheme_id=scheme_id))


@router.get("/runs/{run_id}")
def get_run(run_id: int, include_stocks: bool = True,
            selected_only: bool = False, limit: int = 500,
            db: Session = Depends(get_db)):
    try:
        return ok(run_svc.run_detail(db, run_id, include_stocks=include_stocks,
                                     selected_only=selected_only, limit=limit))
    except LookupError as exc:
        return err(404, str(exc))


@router.get("/snapshots")
def list_snapshots(limit: int = 50, db: Session = Depends(get_db)):
    return ok(run_svc.list_snapshots(db, limit=max(1, min(limit, 200))))


@router.get("/snapshots/{snapshot_id}")
def get_snapshot(snapshot_id: int, include_stocks: bool = False,
                 selected_only: bool = True, limit: int = 500,
                 db: Session = Depends(get_db)):
    try:
        return ok(run_svc.snapshot_detail(
            db, snapshot_id, include_stocks=include_stocks,
            selected_only=selected_only, limit=limit))
    except LookupError as exc:
        return err(404, str(exc))


@router.post("/snapshots/{snapshot_id}/freeze")
def freeze_snapshot(snapshot_id: int, db: Session = Depends(get_db)):
    try:
        return ok(run_svc.freeze(db, snapshot_id))
    except LookupError as exc:
        return err(404, str(exc))


@router.post("/snapshots/{snapshot_id}/publish")
def publish_snapshot(snapshot_id: int, req: PublishReq,
                     db: Session = Depends(get_db)):
    """冻结后的快照发布为平台股票池（当前成分 + PIT 成员区间）。

    ``series=false``（默认）发版本池，供实盘钉版本；``series=true`` 发同风格
    序列池，供回测按日期取成分。
    """
    try:
        return ok(run_svc.publish(db, snapshot_id, code=req.code,
                                  name=req.name, series=req.series))
    except LookupError as exc:
        return err(404, str(exc))
    except run_svc.RunError as exc:
        return _bad(exc)
    except IntegrityError:
        db.rollback()
        return err(409, "股票池 code 冲突，请换一个 code")
    except Exception as exc:  # pragma: no cover - 防御性
        db.rollback()
        return err(500, "发布失败：%s" % exc)


@router.post("/deliver")
def deliver_selection(req: DeliverReq, db: Session = Depends(get_db)):
    """T 盘后一次交付：运行 → 冻结 → 发版本池 → 发序列池。"""
    try:
        return ok(run_svc.deliver(db, as_of=req.as_of))
    except run_svc.RunError as exc:
        return _bad(exc)
    except Exception as exc:  # pragma: no cover - 防御性
        db.rollback()
        return err(500, "交付失败：%s" % exc)


@router.get("/factor-validation")
def factor_validation(db: Session = Depends(get_db)):
    """因子稳定性摘要（导入期回算，随快照日冻结）。仅作参考数据，不参与决策。"""
    return ok(run_svc.factor_validation(db))


# --- 池子有效性（召回）：池是候选域，最终由公式择时决定买什么 ---------------

@router.get("/formulas")
def list_formulas(db: Session = Depends(get_db)):
    """可做择时验证的导入公式及其实效 OPEN 信号线。"""
    return ok(recall_svc.list_formulas(db))


@router.post("/pool-recall")
def compute_pool_recall(req: RecallReq, db: Session = Depends(get_db)):
    """按 as_of 比较公式全市场入选 vs 池内入选，算召回率 + 漏票明细。"""
    try:
        return ok(recall_svc.compute_recall(
            db, req.snapshot_id, req.formula_id,
            horizon_days=req.horizon_days, refresh=req.refresh,
            signal_window_days=req.signal_window_days))
    except recall_svc.RecallError as exc:
        return _bad(exc)
    except LookupError as exc:
        return err(404, str(exc))
    except Exception as exc:  # pragma: no cover - 公式求值等外部失败
        db.rollback()
        return err(500, "召回验证失败：%s" % exc)


@router.get("/pool-recall")
def list_pool_recalls(snapshot_id: Optional[int] = None, limit: int = 20,
                      db: Session = Depends(get_db)):
    return ok(recall_svc.list_recalls(db, snapshot_id=snapshot_id, limit=limit))


# --- 池接入：回测取成分 / 实盘挂版本 -------------------------------------

@router.get("/pools")
def list_pools(db: Session = Depends(get_db)):
    """条件选股池清单：series（回测用）/ version（实盘钉版本用）。"""
    return ok(pool_svc.list_pools(db))


@router.get("/pools/{pool_id}/resolve")
def resolve_pool(pool_id: int, day: Optional[date] = None,
                 db: Session = Depends(get_db)):
    """按日期解析池成分（PIT）；未覆盖日显式 available=false。"""
    try:
        return ok(pool_svc.resolve_pool(db, pool_id, day=day))
    except LookupError as exc:
        return err(404, str(exc))


@router.get("/sessions/{session_id}/binding")
def session_binding(session_id: int, db: Session = Depends(get_db)):
    try:
        return ok(pool_svc.session_binding(db, session_id))
    except LookupError as exc:
        return err(404, str(exc))


@router.post("/sessions/{session_id}/bind")
def bind_session(session_id: int, req: BindReq,
                 db: Session = Depends(get_db)):
    """把实盘会话关联组合挂到条件选股**版本池**（frozen 才允许）。"""
    try:
        return ok(pool_svc.bind_session(db, session_id,
                                        pool_id=req.pool_id,
                                        snapshot_id=req.snapshot_id))
    except LookupError as exc:
        return err(404, str(exc))
    except pool_svc.PoolBindError as exc:
        return _bad(exc)
    except Exception as exc:  # pragma: no cover - 防御性
        db.rollback()
        return err(500, "绑定失败：%s" % exc)


@router.post("/sessions/{session_id}/unbind")
def unbind_session(session_id: int, req: UnbindReq,
                   db: Session = Depends(get_db)):
    """按 bind 返回的 before 快照回退。"""
    try:
        return ok(pool_svc.unbind_session(db, session_id, req.bindings))
    except LookupError as exc:
        return err(404, str(exc))
    except pool_svc.PoolBindError as exc:
        return _bad(exc)
