"""数据与口径观测（P0-1 收尾，2026-11-23）。

三块只读观测合并成一个汇总入口，供体检面板展示：

1. **公司行为未验证数**：``corporate_action_diagnostics`` 里当前未解决的
   ``corporate_action_unverified`` 诊断行数（检测器见
   ``core.services.corporate_action_audit``）。
2. **口径过期标记数**：按 ``params_snapshot`` 现算的回测记录数（复权口径非
   ``exact`` 或缺 ``price_caliber.version``），与 ``backtest_service._caliber_stale``
   同一判据，但这里只做统计、不写库。
3. **K 线覆盖摘要**：``kline_coverage_ledger.status()``。

自动触发（用户拍板 2026-11-23）：回测启动时一次、每日 15:05 日线轮次之后一次。
只写诊断表，任何异常都只告警、不阻断调用方。测试可通过环境变量
``TQ_AUTO_CA_AUDIT_DISABLED=1`` 关闭自动触发。
"""
from __future__ import annotations

import logging
import os
import threading
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: 后台审计串行闸：同一时刻只跑一轮，避免反复触发叠加取数压力。
_AUDIT_LOCK = threading.Lock()

#: 最近一轮自动审计的结果（仅内存，供汇总接口展示；不进库）。
_LAST_AUDIT: Dict[str, object] = {}


def auto_audit_enabled() -> bool:
    """自动触发开关：显式环境变量关闭优先（测试/运维），否则跟随配置。"""
    if str(os.environ.get("TQ_AUTO_CA_AUDIT_DISABLED") or "").strip() == "1":
        return False
    try:
        from core.services.corporate_action_audit import audit_config
        return bool(audit_config()["enabled"])
    except Exception:  # noqa: BLE001 — 配置不可读时按关闭处理，避免意外取数
        return False


def _audit_window(start: Optional[date], end: Optional[date],
                  lookback_days: int) -> tuple:
    end_d = end or date.today()
    start_d = start or (end_d - timedelta(days=max(1, int(lookback_days))))
    return start_d, end_d


def run_auto_audit(codes: Iterable[str], *, start: Optional[date] = None,
                   end: Optional[date] = None, reason: str = "") -> dict:
    """同步执行一轮自动审计（供线程包装与单测直接调用）。

    与 API 手动触发共用 ``audit_corporate_actions``；只写诊断表，失败只告警。
    """
    from core.db import SessionLocal
    from core.services.corporate_action_audit import (
        audit_config, audit_corporate_actions,
    )

    wanted = [str(c or "").strip() for c in codes or []]
    wanted = list(dict.fromkeys(c for c in wanted if c))
    cfg = audit_config()
    if not cfg["enabled"] or not wanted:
        return {"skipped": True,
                "reason": "corporate_action_audit.enabled=false" if not cfg["enabled"]
                else "股票范围为空"}
    start_d, end_d = _audit_window(start, end, int(cfg["lookback_days"]))

    if not _AUDIT_LOCK.acquire(blocking=False):
        logger.info("公司行为审计已有一轮在跑，跳过本次触发（%s）", reason)
        return {"skipped": True, "reason": "audit_in_progress"}
    try:
        with SessionLocal() as db:
            from core.tq.gate import P2, tdx_priority
            try:
                with tdx_priority(P2):
                    result = audit_corporate_actions(
                        db, wanted, start=start_d.isoformat(),
                        end=end_d.isoformat())
            except Exception:  # noqa: BLE001 — 通道闸拒绝也不影响主流程
                logger.warning("公司行为审计自动执行被跳过（%s）", reason,
                               exc_info=True)
                return {"skipped": True, "reason": "channel_rejected"}
        _LAST_AUDIT.clear()
        _LAST_AUDIT.update({
            "at": datetime.now().isoformat(timespec="seconds"),
            "reason": reason,
            "window": {"start": start_d.isoformat(), "end": end_d.isoformat()},
            "requested": result.get("requested"),
            "unverified": result.get("unverified"),
            "written": result.get("written"),
            "resolved": result.get("resolved"),
        })
        logger.info(
            "公司行为审计自动执行完成（%s）：扫描 %s 只，未验证 %s，"
            "新增诊断 %s，清理 %s",
            reason, result.get("scanned"), result.get("unverified"),
            result.get("written"), result.get("resolved"))
        return result
    except Exception:  # noqa: BLE001 — 自动审计绝不影响回测/实盘主链路
        logger.warning("公司行为审计自动执行失败（忽略，不影响主流程）", exc_info=True)
        return {"skipped": True, "reason": "audit_failed"}
    finally:
        _AUDIT_LOCK.release()


def trigger_auto_audit(codes: Iterable[str], *, start: Optional[date] = None,
                       end: Optional[date] = None, reason: str = "") -> bool:
    """后台线程触发一轮自动审计（不阻塞调用方）。返回是否已派发。"""
    if not auto_audit_enabled():
        return False
    wanted = [str(c or "").strip() for c in codes or []]
    wanted = list(dict.fromkeys(c for c in wanted if c))
    if not wanted:
        return False

    def _worker() -> None:
        run_auto_audit(wanted, start=start, end=end, reason=reason)

    threading.Thread(target=_worker, name="ca-auto-audit", daemon=True).start()
    return True


def observability_summary(db: Session) -> dict:
    """只读汇总：公司行为未验证数 + 口径过期标记数 + K 线覆盖摘要。"""
    from core.models import (
        DIAG_UNVERIFIED_EVENT, BacktestRecord, CorporateActionDiagnostic,
    )
    from core.services import kline_coverage_ledger as ledger
    from core.services.backtest_service import caliber_stale_from_snapshot

    rows = (db.query(CorporateActionDiagnostic)
            .filter(CorporateActionDiagnostic.diagnostic_type == DIAG_UNVERIFIED_EVENT)
            .all())
    ca_codes = sorted({r.code for r in rows if r.code})

    stale = 0
    reasons: Dict[str, int] = {}
    total_records = 0
    for (raw,) in db.query(BacktestRecord.params_snapshot).all():
        total_records += 1
        is_stale, reason = caliber_stale_from_snapshot(raw)
        if not is_stale:
            continue
        stale += 1
        key = reason or "unknown"
        reasons[key] = reasons.get(key, 0) + 1

    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "corporate_actions": {
            "enabled": auto_audit_enabled(),
            "unverified": len(rows),
            "codes": ca_codes,
            "last_audit": dict(_LAST_AUDIT) if _LAST_AUDIT else None,
        },
        "caliber": {
            "records": total_records,
            "stale": stale,
            "reasons": reasons,
        },
        "kline_coverage": ledger.status(),
    }
