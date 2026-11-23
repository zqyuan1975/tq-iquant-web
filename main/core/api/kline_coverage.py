"""K 线覆盖台账只读查询 API（P1-4，2026-11-23）。

基础路径 ``/api/kline-coverage``，统一 envelope（``ok``/``err``）。这里只读，
不写台账、不改既有补数/校验路径；台账写入口在 ``build_klines`` 旁路且默认
关闭（``backtest.kline_ledger.enabled``）。
"""
from datetime import date
from typing import Optional

from fastapi import APIRouter, Query

from core.api.response import err, ok
from core.services import kline_coverage_ledger as ledger

router = APIRouter(prefix="/api/kline-coverage", tags=["kline_coverage"])


@router.get("/status")
def coverage_status():
    """台账状态：开关、库路径、记录数与最近更新时间。"""
    return ok(ledger.status())


@router.get("")
def list_coverage(
    code: Optional[str] = Query(default=None),
    period: Optional[str] = Query(default=None),
    source: Optional[str] = Query(default=None),
    field: Optional[str] = Query(default=None),
    limit: int = Query(default=200, ge=1, le=2000),
):
    """查询台账记录（供界面展示覆盖段/空日/指纹状态）。"""
    return ok(ledger.query_records(
        code=code, period=period, source=source, field=field, limit=limit))


@router.get("/plan")
def coverage_plan(
    code: str = Query(...),
    period: str = Query(...),
    start: str = Query(...),
    end: str = Query(...),
    source: str = Query(default=ledger.SOURCE_TDX),
    field: str = Query(default=ledger.FIELD_ALL),
    local_days: Optional[str] = Query(
        default=None,
        description="逗号分隔的本地实际有数据日期 YYYY-MM-DD，用于缺口三分类",
    ),
):
    """按台账给出补数计划：是否可信任、缺失日与缺口三分类。"""
    try:
        start_d = date.fromisoformat(start[:10])
        end_d = date.fromisoformat(end[:10])
    except ValueError:
        return err(422, "start/end 须为 YYYY-MM-DD")
    if end_d < start_d:
        return err(422, "end 不能早于 start")
    days = None
    if local_days is not None:
        days = [x.strip() for x in local_days.split(",") if x.strip()]
    return ok(ledger.plan_from_ledger(
        code, period, start_d, end_d, source=source, field=field,
        local_days=days,
    ))
