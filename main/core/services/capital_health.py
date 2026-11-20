"""拼盘实盘健康指标与自动暂停（0020 §5.5 / 0021 G3）。

八类指标（相关性、回撤、ADV 参与率、同票抢单、召回频率、召回 shortfall、
成交率、对账）逐日判定为 ``normal / watch / suspend / skipped``：

* ``skipped``：当日缺观测。缺数据不改变状态，避免“没采集”被当成“恢复”；
* ``watch``：进入 WATCH，只观察，仍可继续（不再新增借出由 SUSPENDED 负责）；
* ``suspend``：进入 SUSPENDED，停止新增借出，但不强制平仓。

状态机（0020 §5.5）::

    ACTIVE → WATCH → SUSPENDED → RETIRED

* 恶化立即生效；恢复必须同时满足“连续 5 个交易日正常”与“人工确认”；
* ``SUSPENDED`` 不自动回升为 ``ACTIVE``，下一次评估若仍异常继续暂停；
* ``RETIRED`` 为永久解除，保留历史账本，重新开启按新绑定处理。

纯函数 ``advance_streaks`` / ``evaluate_health`` 不读库、不读时钟；
落库与状态回写由 ``record_health`` / ``resume_plan`` / ``retire_plan`` 完成。
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional

from sqlalchemy.orm import Session

from core.models import (
    CapitalPairing,
    CapitalPairingHealthDaily,
    CapitalPairingPlan,
)

logger = logging.getLogger(__name__)

HEALTH_VERSION = "capital-health-g3"

STATUS_ACTIVE = "ACTIVE"
STATUS_WATCH = "WATCH"
STATUS_SUSPENDED = "SUSPENDED"
STATUS_RETIRED = "RETIRED"
STATUS_INSUFFICIENT = "INSUFFICIENT"

STATE_NORMAL = "normal"
STATE_WATCH = "watch"
STATE_SUSPEND = "suspend"
STATE_SKIPPED = "skipped"

# 0020 §5.5：这些指标需要“连续 N 个交易日”才升级为 SUSPENDED。
REQUIRED_NORMAL_DAYS = 5
REQUIRED_STREAK_DAYS = 5
RECONCILE_SUSPEND_STREAK = 2
GRAB_SUSPEND_MONTHS = 2

DAILY_STREAK_KEYS = (
    "corr_high", "adv_high", "borrower_fill_low",
    "lender_fill_low", "reconcile_failed",
)

_PLAN_DRAFT = "DRAFT"
_PLAN_QUEUED = "QUEUED"
_PLAN_EXPIRED = "EXPIRED"
_PLAN_ARCHIVED = "ARCHIVED"
_PLAN_APPROVED = "APPROVED"
_PLAN_WATCH = "WATCH"
_PLAN_SUSPENDED = "SUSPENDED"
_PLAN_RETIRED = STATUS_RETIRED

_SEVERITY = {
    STATUS_ACTIVE: 0,
    STATUS_WATCH: 1,
    STATUS_SUSPENDED: 2,
}


class HealthError(ValueError):
    """业务校验失败（API 转 409）。"""


def _num(value) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def _item(code: str, label: str, value, state: str, message: str,
          threshold: str) -> dict:
    return {
        "code": code,
        "label": label,
        "value": value,
        "state": state,
        "message": message,
        "threshold": threshold,
    }


def _corr_value(observations: dict) -> Optional[float]:
    corr60 = _num(observations.get("corr_60d"))
    corr20 = _num(observations.get("corr_20d"))
    if corr60 is not None:
        return corr60
    return corr20


def _below(value: Optional[float], threshold: float) -> bool:
    return value is not None and value < threshold


def advance_streaks(observations: dict, previous: Optional[dict] = None,
                    as_of: Optional[date] = None,
                    prev_as_of: Optional[date] = None) -> Dict[str, int]:
    """按当日观测推进连续计数；缺观测沿用旧值（不把漏采当正常）。"""
    prev = dict(previous or {})
    obs = observations or {}
    out: Dict[str, int] = {}

    derived = {
        "corr_high": (lambda: (_corr_value(obs) or 0) > 0.8),
        "adv_high": (lambda: (_num(obs.get("adv_participation")) or 0) > 0.015),
        "borrower_fill_low": (
            lambda: _below(_num(obs.get("borrower_fill_rate")), 0.90)),
        "lender_fill_low": (
            lambda: _below(_num(obs.get("lender_recovery_fill_rate")), 0.80)),
        "reconcile_failed": (lambda: bool(obs.get("reconcile_failed"))),
    }
    for key in DAILY_STREAK_KEYS:
        raw = obs.get(key)
        if raw is None:
            has_metric = {
                "corr_high": _corr_value(obs) is not None,
                "adv_high": _num(obs.get("adv_participation")) is not None,
                "borrower_fill_low": _num(
                    obs.get("borrower_fill_rate")) is not None,
                "lender_fill_low": _num(
                    obs.get("lender_recovery_fill_rate")) is not None,
                "reconcile_failed": obs.get("reconcile_failed") is not None,
            }[key]
            if not has_metric:
                out[key] = int(prev.get(key) or 0)
                continue
            raw = derived[key]()
        out[key] = (int(prev.get(key) or 0) + 1) if bool(raw) else 0

    prev_months = int(prev.get("grab_high_months") or 0)
    grab = _num(obs.get("grab_ratio_month"))
    if grab is None:
        out["grab_high_months"] = prev_months
    elif grab > 0.10:
        same_month = (as_of is not None and prev_as_of is not None
                      and as_of.year == prev_as_of.year
                      and as_of.month == prev_as_of.month)
        out["grab_high_months"] = prev_months if same_month else prev_months + 1
    else:
        out["grab_high_months"] = 0
    return out


def evaluate_health(observations: dict,
                    streaks: Optional[dict] = None) -> dict:
    """八类指标逐项判定并汇总状态；纯函数，可复现。"""
    obs = observations or {}
    st = dict(streaks or {})
    items: List[dict] = []

    # 1. 20/60 日收益相关性
    corr = _corr_value(obs)
    corr_streak = int(st.get("corr_high") or 0)
    if corr is None:
        items.append(_item("correlation", "收益相关性", None, STATE_SKIPPED,
                           "缺少 20/60 日相关性观测", "≤0.5 / 0.5–0.7 / >0.8 连续5日"))
    elif corr > 0.8:
        items.append(_item(
            "correlation", "收益相关性", corr,
            STATE_SUSPEND if corr_streak >= REQUIRED_STREAK_DAYS else STATE_WATCH,
            "相关性 %.4f，连续 %d 日 >0.8" % (corr, corr_streak),
            "≤0.5 / 0.5–0.7 / >0.8 连续5日"))
    elif corr > 0.5:
        items.append(_item("correlation", "收益相关性", corr, STATE_WATCH,
                           "相关性 %.4f 处于预警区间" % corr,
                           "≤0.5 / 0.5–0.7 / >0.8 连续5日"))
    else:
        items.append(_item("correlation", "收益相关性", corr, STATE_NORMAL,
                           "相关性 %.4f 正常" % corr,
                           "≤0.5 / 0.5–0.7 / >0.8 连续5日"))

    # 2. 组合最大回撤（超出回测值 pp）
    dd = _num(obs.get("drawdown_excess_pp"))
    dd_breach = bool(obs.get("drawdown_limit_breached"))
    if dd is None and obs.get("drawdown_limit_breached") is None:
        items.append(_item("drawdown", "组合最大回撤", None, STATE_SKIPPED,
                           "缺少回撤观测", "≤+2pp / +2~+3pp / >+3pp"))
    elif dd_breach or (dd is not None and dd > 3):
        items.append(_item("drawdown", "组合最大回撤", dd, STATE_SUSPEND,
                           "回撤超过上限或超出回测 +3pp", "≤+2pp / +2~+3pp / >+3pp"))
    elif dd is not None and dd > 2:
        items.append(_item("drawdown", "组合最大回撤", dd, STATE_WATCH,
                           "回撤超出回测 %.2fpp" % dd, "≤+2pp / +2~+3pp / >+3pp"))
    else:
        items.append(_item("drawdown", "组合最大回撤", dd, STATE_NORMAL,
                           "回撤在容忍区间", "≤+2pp / +2~+3pp / >+3pp"))

    # 3. ADV 参与率
    adv = _num(obs.get("adv_participation"))
    adv_streak = int(st.get("adv_high") or 0)
    if adv is None:
        items.append(_item("adv_participation", "ADV 参与率", None, STATE_SKIPPED,
                           "缺少 ADV 参与率观测", "≤1% / 1%–1.5% / >1.5% 连续5日"))
    elif adv > 0.015:
        items.append(_item(
            "adv_participation", "ADV 参与率", adv,
            STATE_SUSPEND if adv_streak >= REQUIRED_STREAK_DAYS else STATE_WATCH,
            "参与率 %.4f，连续 %d 日 >1.5%%" % (adv, adv_streak),
            "≤1% / 1%–1.5% / >1.5% 连续5日"))
    elif adv > 0.01:
        items.append(_item("adv_participation", "ADV 参与率", adv, STATE_WATCH,
                           "参与率 %.4f 处于预警区间" % adv,
                           "≤1% / 1%–1.5% / >1.5% 连续5日"))
    else:
        items.append(_item("adv_participation", "ADV 参与率", adv, STATE_NORMAL,
                           "参与率 %.4f 正常" % adv,
                           "≤1% / 1%–1.5% / >1.5% 连续5日"))

    # 4. 同票抢单事件（月占比）
    grab = _num(obs.get("grab_ratio_month"))
    grab_months = int(st.get("grab_high_months") or 0)
    if grab is None:
        items.append(_item("same_code_grab", "同票抢单", None, STATE_SKIPPED,
                           "缺少同票抢单观测", "≤5%/月 / 5%–10%/月 / >10% 连续2月"))
    elif grab > 0.10:
        items.append(_item(
            "same_code_grab", "同票抢单", grab,
            STATE_SUSPEND if grab_months >= GRAB_SUSPEND_MONTHS else STATE_WATCH,
            "抢单占比 %.4f，连续 %d 个月 >10%%" % (grab, grab_months),
            "≤5%/月 / 5%–10%/月 / >10% 连续2月"))
    elif grab > 0.05:
        items.append(_item("same_code_grab", "同票抢单", grab, STATE_WATCH,
                           "抢单占比 %.4f 处于预警区间" % grab,
                           "≤5%/月 / 5%–10%/月 / >10% 连续2月"))
    else:
        items.append(_item("same_code_grab", "同票抢单", grab, STATE_NORMAL,
                           "抢单占比 %.4f 正常" % grab,
                           "≤5%/月 / 5%–10%/月 / >10% 连续2月"))

    # 5. 召回频率
    recall = _num(obs.get("recall_per_year"))
    if recall is None:
        items.append(_item("recall_frequency", "召回频率", None, STATE_SKIPPED,
                           "缺少召回次数观测", "≤12 / 12–24 / >24 次/年"))
    elif recall > 24:
        items.append(_item("recall_frequency", "召回频率", recall, STATE_SUSPEND,
                           "召回 %.1f 次/年 >24" % recall, "≤12 / 12–24 / >24 次/年"))
    elif recall > 12:
        items.append(_item("recall_frequency", "召回频率", recall, STATE_WATCH,
                           "召回 %.1f 次/年处于预警区间" % recall,
                           "≤12 / 12–24 / >24 次/年"))
    else:
        items.append(_item("recall_frequency", "召回频率", recall, STATE_NORMAL,
                           "召回 %.1f 次/年正常" % recall,
                           "≤12 / 12–24 / >24 次/年"))

    # 6. 召回 shortfall
    shortfall = _num(obs.get("shortfall_ratio"))
    quarter = _num(obs.get("shortfall_quarter_count"))
    if shortfall is None and quarter is None:
        items.append(_item("recall_shortfall", "召回 shortfall", None,
                           STATE_SKIPPED, "缺少 shortfall 观测",
                           "≤2% / 2%–5% / >5% 或单季>2次"))
    elif (shortfall is not None and shortfall > 0.05) or (quarter or 0) > 2:
        items.append(_item("recall_shortfall", "召回 shortfall", shortfall,
                           STATE_SUSPEND,
                           "shortfall 超过 5% 或单季超过 2 次",
                           "≤2% / 2%–5% / >5% 或单季>2次"))
    elif shortfall is not None and shortfall > 0.02:
        items.append(_item("recall_shortfall", "召回 shortfall", shortfall,
                           STATE_WATCH, "shortfall %.4f 处于预警区间" % shortfall,
                           "≤2% / 2%–5% / >5% 或单季>2次"))
    else:
        items.append(_item("recall_shortfall", "召回 shortfall", shortfall,
                           STATE_NORMAL, "shortfall 正常",
                           "≤2% / 2%–5% / >5% 或单季>2次"))

    # 7. 借款人成交率
    bfill = _num(obs.get("borrower_fill_rate"))
    bstreak = int(st.get("borrower_fill_low") or 0)
    if bfill is None:
        items.append(_item("borrower_fill_rate", "借款人成交率", None,
                           STATE_SKIPPED, "缺少借款人成交率观测",
                           "≥95% / 90%–95% / <90% 连续5日"))
    elif bfill < 0.90:
        items.append(_item(
            "borrower_fill_rate", "借款人成交率", bfill,
            STATE_SUSPEND if bstreak >= REQUIRED_STREAK_DAYS else STATE_WATCH,
            "成交率 %.4f，连续 %d 日 <90%%" % (bfill, bstreak),
            "≥95% / 90%–95% / <90% 连续5日"))
    elif bfill < 0.95:
        items.append(_item("borrower_fill_rate", "借款人成交率", bfill,
                           STATE_WATCH, "成交率 %.4f 处于预警区间" % bfill,
                           "≥95% / 90%–95% / <90% 连续5日"))
    else:
        items.append(_item("borrower_fill_rate", "借款人成交率", bfill,
                           STATE_NORMAL, "成交率 %.4f 正常" % bfill,
                           "≥95% / 90%–95% / <90% 连续5日"))

    # 8. 出借人恢复期成交率
    lfill = _num(obs.get("lender_recovery_fill_rate"))
    lstreak = int(st.get("lender_fill_low") or 0)
    if lfill is None:
        items.append(_item("lender_recovery_fill_rate", "出借人恢复期成交率",
                           None, STATE_SKIPPED, "缺少出借人恢复期成交率观测",
                           "≥90% / 80%–90% / <80% 连续5日"))
    elif lfill < 0.80:
        items.append(_item(
            "lender_recovery_fill_rate", "出借人恢复期成交率", lfill,
            STATE_SUSPEND if lstreak >= REQUIRED_STREAK_DAYS else STATE_WATCH,
            "成交率 %.4f，连续 %d 日 <80%%" % (lfill, lstreak),
            "≥90% / 80%–90% / <80% 连续5日"))
    elif lfill < 0.90:
        items.append(_item("lender_recovery_fill_rate", "出借人恢复期成交率",
                           lfill, STATE_WATCH,
                           "成交率 %.4f 处于预警区间" % lfill,
                           "≥90% / 80%–90% / <80% 连续5日"))
    else:
        items.append(_item("lender_recovery_fill_rate", "出借人恢复期成交率",
                           lfill, STATE_NORMAL, "成交率 %.4f 正常" % lfill,
                           "≥90% / 80%–90% / <80% 连续5日"))

    # 9. 资格 params_hash
    hash_match = obs.get("params_hash_match")
    if hash_match is None:
        items.append(_item("params_hash", "资格参数哈希", None, STATE_SKIPPED,
                           "缺少 params_hash 比对结果", "一致 / 不一致立即暂停"))
    elif bool(hash_match):
        items.append(_item("params_hash", "资格参数哈希", True, STATE_NORMAL,
                           "params_hash 一致", "一致 / 不一致立即暂停"))
    else:
        items.append(_item("params_hash", "资格参数哈希", False, STATE_SUSPEND,
                           "params_hash 不一致，资格失效", "一致 / 不一致立即暂停"))

    # 10. 对账恒等式
    failed = obs.get("reconcile_failed")
    rstreak = int(st.get("reconcile_failed") or 0)
    if failed is None:
        items.append(_item("reconcile", "对账恒等式", None, STATE_SKIPPED,
                           "缺少对账结果", "全绿 / 单日告警 / 连续2日失败"))
    elif bool(failed):
        items.append(_item(
            "reconcile", "对账恒等式", True,
            STATE_SUSPEND if rstreak >= RECONCILE_SUSPEND_STREAK else STATE_WATCH,
            "对账失败，连续 %d 日" % rstreak,
            "全绿 / 单日告警 / 连续2日失败"))
    else:
        items.append(_item("reconcile", "对账恒等式", False, STATE_NORMAL,
                           "对账全绿", "全绿 / 单日告警 / 连续2日失败"))

    suspend = [i["code"] for i in items if i["state"] == STATE_SUSPEND]
    watch = [i["code"] for i in items if i["state"] == STATE_WATCH]
    normal = [i for i in items if i["state"] == STATE_NORMAL]
    if suspend:
        status = STATUS_SUSPENDED
    elif watch:
        status = STATUS_WATCH
    elif normal:
        status = STATUS_ACTIVE
    else:
        status = STATUS_INSUFFICIENT

    return {
        "version": HEALTH_VERSION,
        "status": status,
        "items": items,
        "reasons": suspend + watch,
        "counts": {
            "normal": len(normal),
            "watch": len(watch),
            "suspend": len(suspend),
            "skipped": len([i for i in items if i["state"] == STATE_SKIPPED]),
        },
        "streaks": st,
    }


# ---------------------------------------------------------------------------
# 落库
# ---------------------------------------------------------------------------
def _dump(payload) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _load(text, default):
    try:
        value = json.loads(text) if text else None
    except (TypeError, ValueError):
        return default
    return value if value is not None else default


def _edges(db: Session, plan_id: int) -> List[CapitalPairing]:
    return (db.query(CapitalPairing)
            .filter(CapitalPairing.pairing_plan_id == plan_id)
            .order_by(CapitalPairing.id).all())


def _latest_row(db: Session, plan_id: int,
                pairing_id: Optional[int] = None) -> Optional[CapitalPairingHealthDaily]:
    q = db.query(CapitalPairingHealthDaily).filter(
        CapitalPairingHealthDaily.pairing_plan_id == plan_id)
    if pairing_id is not None:
        q = q.filter(CapitalPairingHealthDaily.pairing_id == pairing_id)
    return q.order_by(CapitalPairingHealthDaily.as_of.desc(),
                      CapitalPairingHealthDaily.id.desc()).first()


def _row_payload(row: CapitalPairingHealthDaily) -> dict:
    return {
        "id": row.id,
        "pairing_plan_id": row.pairing_plan_id,
        "pairing_id": row.pairing_id,
        "as_of": row.as_of.isoformat() if row.as_of else None,
        "status": row.status,
        "health_version": row.health_version,
        "metrics": _load(row.metrics_json, {}),
        "items": _load(row.items_json, []),
        "streaks": _load(row.streaks_json, {}),
        "reasons": _load(row.reasons_json, []),
        "normal_streak": int(row.normal_streak or 0),
        "evaluated_at": (
            row.evaluated_at.isoformat(sep=" ") if row.evaluated_at else None),
    }


def _guard_evaluable(plan: CapitalPairingPlan) -> None:
    if plan.status in (_PLAN_DRAFT, _PLAN_QUEUED, _PLAN_EXPIRED, _PLAN_ARCHIVED):
        raise HealthError(
            "仅已批准/观察/暂停的拼盘可评估健康，当前 %s" % plan.status)
    if plan.status == _PLAN_RETIRED:
        raise HealthError("拼盘已退役（RETIRED），不能继续评估")


def _apply_plan_status(db: Session, plan: CapitalPairingPlan,
                       status: str) -> None:
    """把健康状态回写到拼盘头与边；只降不升（恢复走人工 ``resume_plan``）。"""
    if status == STATUS_SUSPENDED:
        plan.status = _PLAN_SUSPENDED
        target = STATUS_SUSPENDED
    elif status == STATUS_WATCH:
        plan.status = _PLAN_WATCH
        target = STATUS_WATCH
    else:
        return
    for edge in _edges(db, plan.id):
        if edge.status in (STATUS_ACTIVE, STATUS_WATCH, STATUS_SUSPENDED):
            edge.status = target


def record_health(db: Session, plan_id: int, observations: dict,
                  as_of: Optional[date] = None,
                  pairing_id: Optional[int] = None,
                  now: Optional[datetime] = None) -> dict:
    """写入当日健康快照并回写拼盘状态。"""
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    _guard_evaluable(plan)
    now = now or datetime.now()
    as_of = as_of or now.date()

    prev = _latest_row(db, plan_id, pairing_id)
    prev_streaks = _load(prev.streaks_json, {}) if prev else {}
    prev_status = prev.status if prev else None
    streaks = advance_streaks(
        observations, prev_streaks, as_of=as_of,
        prev_as_of=prev.as_of if prev else None)
    result = evaluate_health(observations, streaks=streaks)

    if result["status"] == STATUS_INSUFFICIENT:
        normal_streak = int(prev.normal_streak or 0) if prev else 0
        resolved = prev_status or STATUS_ACTIVE
        held = True
    else:
        prev_normal = int(prev.normal_streak or 0) if prev else 0
        normal_streak = (
            prev_normal + 1 if result["status"] == STATUS_ACTIVE else 0)
        held = False
        resolved = result["status"]
        if prev_status in _SEVERITY and resolved in _SEVERITY:
            # 只能人工恢复：WATCH/SUSPENDED 不会被“今天正常”自动抬回。
            resolved = max(
                (prev_status, resolved), key=lambda s: _SEVERITY[s])

    row = CapitalPairingHealthDaily(
        pairing_plan_id=plan.id, pairing_id=pairing_id, as_of=as_of,
        status=resolved, health_version=HEALTH_VERSION,
        metrics_json=_dump(observations or {}),
        items_json=_dump(result["items"]),
        streaks_json=_dump(streaks),
        reasons_json=_dump(result["reasons"]),
        normal_streak=normal_streak, evaluated_at=now)
    db.add(row)
    db.flush()

    _apply_plan_status(db, plan, resolved)
    evidence = _load(plan.evidence_json, {})
    evidence["health"] = {
        "latest_id": row.id,
        "status": resolved,
        "as_of": as_of.isoformat(),
        "normal_streak": normal_streak,
        "held_for_manual_resume": held,
        "evaluated_at": now.isoformat(sep=" ", timespec="seconds"),
    }
    plan.evidence_json = _dump(evidence)
    db.commit()

    data = _row_payload(row)
    data["plan_status"] = plan.status
    data["held_for_manual_resume"] = held
    return data


def health_history(db: Session, plan_id: int, limit: int = 30) -> dict:
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    rows = (db.query(CapitalPairingHealthDaily)
            .filter(CapitalPairingHealthDaily.pairing_plan_id == plan_id)
            .order_by(CapitalPairingHealthDaily.as_of.desc(),
                      CapitalPairingHealthDaily.id.desc())
            .limit(max(1, int(limit))).all())
    latest = _row_payload(rows[0]) if rows else None
    return {
        "plan_id": plan.id,
        "plan_status": plan.status,
        "latest": latest,
        "history": [_row_payload(r) for r in rows],
        "retire_reason": (_load(plan.evidence_json, {})
                          .get("health", {}).get("retire")),
        "resume": (_load(plan.evidence_json, {})
                   .get("health", {}).get("resume")),
    }


def list_health(db: Session) -> list:
    plans = (db.query(CapitalPairingPlan)
             .filter(CapitalPairingPlan.status.in_(
                 [_PLAN_APPROVED, _PLAN_WATCH, _PLAN_SUSPENDED, _PLAN_RETIRED]))
             .order_by(CapitalPairingPlan.id.desc()).all())
    out = []
    for plan in plans:
        row = _latest_row(db, plan.id)
        out.append({
            "plan_id": plan.id, "name": plan.name, "scope": plan.scope,
            "plan_status": plan.status,
            "latest": _row_payload(row) if row else None,
        })
    return out


def resume_plan(db: Session, plan_id: int, *, reason: str,
                valid_until: Optional[str] = None,
                operator: Optional[str] = None,
                now: Optional[datetime] = None) -> dict:
    """人工恢复：必须连续 5 个交易日正常，并记录理由与有效期。"""
    now = now or datetime.now()
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    if plan.status not in (_PLAN_WATCH, _PLAN_SUSPENDED):
        raise HealthError("仅观察/暂停状态可恢复，当前 %s" % plan.status)
    if not (reason or "").strip():
        raise HealthError("恢复必须填写理由")
    latest = _latest_row(db, plan.id)
    streak = int(latest.normal_streak or 0) if latest else 0
    if streak < REQUIRED_NORMAL_DAYS:
        raise HealthError(
            "连续正常交易日 %d < %d，不能恢复"
            % (streak, REQUIRED_NORMAL_DAYS))
    if latest and latest.status not in (
            STATUS_ACTIVE, STATUS_WATCH, STATUS_SUSPENDED):
        raise HealthError("最新健康状态不可恢复：%s" % latest.status)

    plan.status = _PLAN_APPROVED
    for edge in _edges(db, plan.id):
        if edge.status in (STATUS_WATCH, STATUS_SUSPENDED):
            edge.status = STATUS_ACTIVE
    record = {
        "reason": reason.strip(),
        "valid_until": valid_until,
        "operator": operator,
        "normal_streak": streak,
        "resumed_at": now.isoformat(sep=" ", timespec="seconds"),
        "latest_health_id": latest.id if latest else None,
    }
    evidence = _load(plan.evidence_json, {})
    health = evidence.get("health") or {}
    health["resume"] = record
    evidence["health"] = health
    plan.evidence_json = _dump(evidence)
    db.commit()
    data = health_history(db, plan.id, limit=1)
    data["resume"] = record
    return data


def retire_plan(db: Session, plan_id: int, *, reason: str,
                operator: Optional[str] = None,
                now: Optional[datetime] = None) -> dict:
    """永久退役：保留历史账本，不再新增借出。"""
    now = now or datetime.now()
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    if plan.status == _PLAN_RETIRED:
        raise HealthError("拼盘已退役")
    if not (reason or "").strip():
        raise HealthError("退役必须填写理由")
    plan.status = _PLAN_RETIRED
    plan.effective_to = now
    for edge in _edges(db, plan.id):
        edge.status = STATUS_RETIRED
        edge.effective_to = now
    record = {
        "reason": reason.strip(),
        "operator": operator,
        "retired_at": now.isoformat(sep=" ", timespec="seconds"),
    }
    evidence = _load(plan.evidence_json, {})
    health = evidence.get("health") or {}
    health["retire"] = record
    evidence["health"] = health
    plan.evidence_json = _dump(evidence)
    db.commit()
    data = health_history(db, plan.id, limit=1)
    data["retire_reason"] = record
    return data
