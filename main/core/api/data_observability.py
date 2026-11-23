"""数据与口径观测汇总 API（P0-1，2026-11-23）。

基础路径 ``/api/data-observability``，只读。合并展示：

- 公司行为未验证诊断数（``corporate_action_diagnostics``）；
- 回测记录「口径已过期」派生标记数（不写库）；
- K 线覆盖台账摘要（``/api/kline-coverage/status`` 同源）。

不改任何账务/回测数据；不触发审计（审计自动跑在回测启动与 15:05 轮次）。
"""
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from core.api.response import ok
from core.db import get_db
from core.services.data_observability import observability_summary

router = APIRouter(prefix="/api/data-observability", tags=["data_observability"])


@router.get("/summary")
def data_observability_summary(db: Session = Depends(get_db)):
    """公司行为未验证数 + 口径过期标记数 + K 线覆盖摘要（只读）。"""
    return ok(observability_summary(db))
