"""拼盘服务（0021 B6）：L2 新建 / 评估 / 草稿 / 合并回测绑定。

设计约束（0020 §12.3、§5.1）：

* 拼盘是配置对象：``capital_pairing_plans``（头）+ ``capital_pairings``（边）
  + ``capital_pairing_evals``（冻结评估），不复制任何回测指标；
* 阶段 B 仅同组合 L2、恰好 1 个借款方；跨组合与多借款方留待 E；
* **实盘与回测零继承**：``binding_json`` 显式冻结策略/周期/风控时点/追价/
  单笔上限等绑定参数；评估时按当前策略行重建并逐字段比对，漂移即失效；
* 不触碰实盘会话与交易链路。
"""
import hashlib
import json
import logging
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from core.models import (
    BacktestRecord,
    BacktestTrade,
    CapitalPairing,
    CapitalPairingEval,
    CapitalPairingPlan,
    PortfolioStrategy,
    Strategy,
)
from core.services.pairing_eligibility import (
    STATUS_ELIGIBLE,
    STATUS_INELIGIBLE,
    STATUS_STALE,
    STATUS_WATCH,
    evaluate_eligibility,
)
from core.services.pairing_eligibility import _normalize

logger = logging.getLogger(__name__)

PAIRING_SERVICE_VERSION = "pairing-service-b6"
BINDING_VERSION = "pairing-binding-v1"
EVAL_KIND_L2 = "L2_PRECHECK"

PLAN_DRAFT = "DRAFT"
PLAN_APPROVED = "APPROVED"
PLAN_WATCH = "WATCH"
PLAN_SUSPENDED = "SUSPENDED"
PLAN_ARCHIVED = "ARCHIVED"
PLAN_QUEUED = "QUEUED"
PLAN_EXPIRED = "EXPIRED"

# 0020 §2：L3 硬上限 3 个借款方；超出默认排队，队列 TTL 20 个交易日。
MIN_L3_BORROWERS = 2
MAX_L3_BORROWERS = 3
QUEUE_TTL_TRADING_DAYS = 20
# 交易日→自然日近似（每周 5/7），只用于队列过期比较，不参与收益计算。
_TRADING_TO_CALENDAR = 7.0 / 5.0

STATUS_GO = "GO"
STATUS_NO_GO = "NO_GO"

MIN_SIGNAL_OVERLAP = 0.60
MAX_CASH_DAY_OVERLAP = 0.80

# 显式绑定字段（用户拍板：策略/周期/风控时点/追价/单笔上限；含拆单执行参数）
BINDING_FIELDS = (
    "strategy_id", "name", "role", "formula_id", "period",
    "signal_frequency", "signal_at", "risk_timing",
    "requote_enabled", "requote_max_attempts", "requote_price_mode",
    "requote_max_slippage_ticks", "max_order_amount",
    "risk_chase_ticks", "risk_chase_max_attempts",
    "split_order_enabled", "split_order_threshold",
)


class PairingError(ValueError):
    """业务校验失败（API 转 400/409）。"""


def _num(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _dec(value) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _hash(payload) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True,
                   default=str).encode("utf-8")
    ).hexdigest()


def _dump(payload) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)


def _load(text) -> dict:
    try:
        value = json.loads(text) if text else None
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def binding_snapshot(strategy: Strategy) -> dict:
    """冻结单个策略的绑定参数（实盘/回测零继承的比对基线）。"""
    return {
        "strategy_id": strategy.id,
        "name": strategy.name,
        "role": strategy.role,
        "formula_id": strategy.formula_id,
        "period": strategy.period,
        "signal_frequency": strategy.signal_frequency,
        "signal_at": strategy.signal_at,
        "risk_timing": strategy.risk_timing,
        "requote_enabled": bool(strategy.requote_enabled),
        "requote_max_attempts": strategy.requote_max_attempts,
        "requote_price_mode": strategy.requote_price_mode,
        "requote_max_slippage_ticks": strategy.requote_max_slippage_ticks,
        "risk_chase_ticks": getattr(strategy, "risk_chase_ticks", 0),
        "risk_chase_max_attempts": getattr(strategy, "risk_chase_max_attempts", 0),
        "max_order_amount": _num(strategy.max_order_amount),
        "split_order_enabled": bool(strategy.split_order_enabled),
        "split_order_threshold": _num(strategy.split_order_threshold),
    }


def _latest_record(db: Session, portfolio_strategy_id: int) -> Optional[BacktestRecord]:
    return (
        db.query(BacktestRecord)
        .filter(
            BacktestRecord.portfolio_strategy_id == portfolio_strategy_id,
            BacktestRecord.status == "completed",
        )
        .order_by(BacktestRecord.completed_at.desc(), BacktestRecord.id.desc())
        .first()
    )


def _pairing_version(scope: str, lender: dict, borrower: dict, loan: dict) -> str:
    return _hash({
        "binding_version": BINDING_VERSION, "scope": scope,
        "lender": {k: lender.get(k) for k in BINDING_FIELDS},
        "borrower": {k: borrower.get(k) for k in BINDING_FIELDS},
        "loan": loan,
    })


def _binding_drift(stored: dict, current: dict, side: str) -> List[str]:
    """逐字段比对冻结绑定与当前策略行；数值按语义归一（尾零不误报）。"""
    drift = []
    for field in BINDING_FIELDS:
        if _normalize(stored.get(field)) != _normalize(current.get(field)):
            drift.append("%s.%s" % (side, field))
    return drift


# ---------------------------------------------------------------------------
# 策略级诊断（不依赖 C/D 层，但比组合级更贴近 L2）
# ---------------------------------------------------------------------------
def _strategy_trades(db: Session, record_id: int, strategy_id: int) -> list:
    return (
        db.query(BacktestTrade)
        .filter(
            BacktestTrade.backtest_record_id == record_id,
            BacktestTrade.strategy_id == strategy_id,
        )
        .order_by(BacktestTrade.bar_time, BacktestTrade.id)
        .all()
    )


def _open_pairs(trades: list) -> set:
    return {
        (t.bar_time.date() if isinstance(t.bar_time, datetime) else t.bar_time,
         t.stock_code)
        for t in trades if t.signal_type == "OPEN" and t.bar_time is not None
    }


def _buy_days(trades: list) -> set:
    return {
        t.bar_time.date() if isinstance(t.bar_time, datetime) else t.bar_time
        for t in trades if t.trade_type == "BUY" and t.bar_time is not None
    }


def _holding_days(trades: list) -> Optional[float]:
    lots: Dict[str, List[List]] = {}
    holding: List[int] = []
    for trade in trades:
        day = trade.bar_time.date() if isinstance(trade.bar_time, datetime) else trade.bar_time
        qty = int(trade.quantity or 0)
        if day is None or qty <= 0:
            continue
        if trade.trade_type == "BUY":
            lots.setdefault(trade.stock_code, []).append([day, qty])
            continue
        if trade.trade_type != "SELL":
            continue
        queue = lots.setdefault(trade.stock_code, [])
        remaining = qty
        while remaining > 0 and queue:
            lot = queue[0]
            used = min(remaining, lot[1])
            holding.append((day - lot[0]).days)
            lot[1] -= used
            remaining -= used
            if lot[1] <= 0:
                queue.pop(0)
    return float(median(holding)) if holding else None


def _jaccard(a: set, b: set) -> float:
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def _strategy_metrics(db: Session, record_id: Optional[int],
                      lender_id: int, borrower_id: int) -> dict:
    if not record_id:
        return {}
    lender_trades = _strategy_trades(db, record_id, lender_id)
    borrower_trades = _strategy_trades(db, record_id, borrower_id)
    lender_hold = _holding_days(lender_trades)
    borrower_hold = _holding_days(borrower_trades)
    ratio = None
    if lender_hold and borrower_hold and lender_hold > 0 and borrower_hold > 0:
        ratio = max(lender_hold, borrower_hold) / min(lender_hold, borrower_hold)
    return {
        "lender_trades": len(lender_trades),
        "borrower_trades": len(borrower_trades),
        "signal_overlap": _jaccard(
            _open_pairs(lender_trades), _open_pairs(borrower_trades)),
        "cash_day_overlap": _jaccard(
            _buy_days(lender_trades), _buy_days(borrower_trades)),
        "lender_median_holding_days": lender_hold,
        "borrower_median_holding_days": borrower_hold,
        "holding_ratio": ratio,
    }


# ---------------------------------------------------------------------------
# 评估
# ---------------------------------------------------------------------------
def _precheck(db: Session, plan: CapitalPairingPlan, lender: Strategy,
              borrower: Strategy, now: datetime) -> dict:
    frozen = _load(plan.binding_json)
    current_lender = binding_snapshot(lender)
    current_borrower = binding_snapshot(borrower)
    drift = (
        _binding_drift(frozen.get("lender") or {}, current_lender, "lender")
        + _binding_drift(frozen.get("borrower") or {}, current_borrower, "borrower")
    )
    eligibility = evaluate_eligibility(
        db, plan.portfolio_strategy_id, record_id=plan.lender_record_id,
        scope=plan.scope, now=now)
    metrics = _strategy_metrics(
        db, plan.lender_record_id, lender.id, borrower.id)

    elig_state = eligibility["status"]
    elig_ok = elig_state in (STATUS_ELIGIBLE, STATUS_WATCH)
    hard = [
        {"code": "L2_ELIGIBILITY", "rule": "组合资格通过",
         "state": "pass" if elig_ok else "fail",
         "message": "资格 %s" % elig_state,
         "detail": {"status": elig_state,
                    "codes": [c["code"] for c in eligibility.get("codes", [])
                              if c.get("status") == "reject"]}},
        {"code": "L2_BINDING", "rule": "绑定参数与冻结一致",
         "state": "fail" if drift else "pass",
         "message": "绑定参数漂移：%s" % "、".join(drift) if drift
         else "绑定参数与冻结一致",
         "detail": {"drift_fields": drift, "binding_version": BINDING_VERSION}},
    ]

    soft = []
    signal_overlap = metrics.get("signal_overlap")
    if signal_overlap is None:
        soft.append({"feature": "same_code_signal_overlap", "state": "pending",
                     "value": None, "message": "缺少成交流水"})
    else:
        soft.append({
            "feature": "same_code_signal_overlap",
            "state": "pass" if signal_overlap <= 0.30 else
                     "fail" if signal_overlap > MIN_SIGNAL_OVERLAP else "warn",
            "value": signal_overlap,
            "message": "同日同票 OPEN 信号重叠 %.4f" % signal_overlap})
    cash_overlap = metrics.get("cash_day_overlap")
    if cash_overlap is not None:
        soft.append({
            "feature": "cash_timing_overlap",
            "state": "pass" if cash_overlap <= 0.60 else
                     "fail" if cash_overlap > MAX_CASH_DAY_OVERLAP else "warn",
            "value": cash_overlap,
            "message": "买入日重叠 %.4f" % cash_overlap})
    ratio = metrics.get("holding_ratio")
    if ratio is not None:
        soft.append({
            "feature": "holding_period",
            "state": "pass" if ratio >= 2.0 else
                     "fail" if ratio < 1.2 else "warn",
            "value": ratio,
            "message": "持有周期差异 %.2f 倍" % ratio})

    soft_fail = [s for s in soft if s["state"] == "fail"]
    soft_warn = [s for s in soft if s["state"] == "warn"]
    if not elig_ok or drift:
        status, reasons = STATUS_NO_GO, (
            (["eligibility:%s" % elig_state] if not elig_ok else [])
            + (["binding_drift:%s" % ",".join(drift)] if drift else []))
    elif elig_state == STATUS_WATCH:
        status, reasons = STATUS_WATCH, ["eligibility:%s" % elig_state]
    elif len(soft_fail) >= 2:
        status, reasons = STATUS_NO_GO, [s["feature"] for s in soft_fail]
    elif len(soft_fail) == 1 or len(soft_warn) >= 3:
        status, reasons = STATUS_WATCH, [s["feature"] for s in soft_fail]
    else:
        status, reasons = STATUS_GO, []

    return {
        "status": status,
        "reasons": reasons,
        "hard": hard,
        "soft": soft,
        "metrics": metrics,
        "counts": {
            "hard_failed": sum(1 for h in hard if h["state"] == "fail"),
            "soft_fail": len(soft_fail),
            "soft_warn": len(soft_warn),
        },
        "eligibility": eligibility,
    }


def _store_eval(db: Session, plan: CapitalPairingPlan, result: dict,
                lender_id: int, borrower_id: int,
                now: datetime) -> CapitalPairingEval:
    row = CapitalPairingEval(
        pairing_plan_id=plan.id, scope=plan.scope, eval_kind=EVAL_KIND_L2,
        eval_version=PAIRING_SERVICE_VERSION,
        threshold_version=BINDING_VERSION, status=result["status"],
        reasons_json=_dump(result["reasons"]),
        hard_json=_dump(result["hard"]), soft_json=_dump(result["soft"]),
        metrics_json=_dump(result["metrics"]),
        counts_json=_dump(result["counts"]),
        lender_strategy_id=lender_id, borrower_strategy_id=borrower_id,
        record_id=plan.lender_record_id,
        decided_at=now)
    db.add(row)
    db.flush()
    plan.evidence_json = _dump({
        "latest_eval_id": row.id,
        "status": result["status"],
        "reasons": result["reasons"],
        "decided_at": now.isoformat(sep=" "),
        "binding_version": BINDING_VERSION,
    })
    return row


def evaluate_plan(db: Session, plan_id: int,
                  now: Optional[datetime] = None) -> dict:
    now = now or datetime.now()
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    edge = _edge_of(db, plan.id)
    lender = db.get(Strategy, plan.lender_strategy_id)
    borrower = db.get(Strategy, edge.borrower_id) if edge else None
    if lender is None or borrower is None:
        raise LookupError("拼盘成员策略不存在，无法评估")
    result = _precheck(db, plan, lender, borrower, now)
    row = _store_eval(db, plan, result, lender.id, borrower.id, now)
    if plan.status == PLAN_APPROVED and result["status"] != STATUS_GO:
        plan.status = PLAN_SUSPENDED
    db.commit()
    return _eval_payload(row)


def _eval_payload(row: CapitalPairingEval) -> dict:
    return {
        "id": row.id, "status": row.status, "eval_kind": row.eval_kind,
        "eval_version": row.eval_version,
        "threshold_version": row.threshold_version,
        "decided_at": row.decided_at.isoformat(sep=" ") if row.decided_at else None,
        "reasons": json.loads(row.reasons_json or "[]"),
        "hard": json.loads(row.hard_json or "[]"),
        "soft": json.loads(row.soft_json or "[]"),
        "metrics": json.loads(row.metrics_json or "{}"),
        "counts": json.loads(row.counts_json or "{}"),
    }


def _edge_of(db: Session, plan_id: int) -> Optional[CapitalPairing]:
    return (
        db.query(CapitalPairing)
        .filter(CapitalPairing.pairing_plan_id == plan_id)
        .order_by(CapitalPairing.id)
        .first()
    )


def _latest_eval(db: Session, plan_id: int) -> Optional[CapitalPairingEval]:
    return (
        db.query(CapitalPairingEval)
        .filter(CapitalPairingEval.pairing_plan_id == plan_id)
        .order_by(CapitalPairingEval.id.desc())
        .first()
    )


def _edges_of(db: Session, plan_id: int) -> List[CapitalPairing]:
    return (
        db.query(CapitalPairing)
        .filter(CapitalPairing.pairing_plan_id == plan_id)
        .order_by(CapitalPairing.id)
        .all()
    )


def _latest_eval_for(db: Session, plan_id: int,
                     borrower_id: int) -> Optional[CapitalPairingEval]:
    return (
        db.query(CapitalPairingEval)
        .filter(CapitalPairingEval.pairing_plan_id == plan_id,
                CapitalPairingEval.borrower_strategy_id == borrower_id)
        .order_by(CapitalPairingEval.id.desc())
        .first()
    )


def _borrower_specs(payload: dict, scope: str,
                    lender_portfolio_id: int) -> List[dict]:
    """归一化借款方入参：L3 支持显式 borrowers，也兼容 id 列表（同组合）。"""
    raw = payload.get("borrowers")
    specs: List[dict] = []
    if raw:
        for item in raw:
            if isinstance(item, dict):
                pid = item.get("portfolio_strategy_id") or lender_portfolio_id
                sid = item.get("strategy_id") or item.get("borrower_strategy_id")
                share = item.get("max_share", payload.get("max_share"))
            else:
                pid, sid, share = lender_portfolio_id, item, payload.get("max_share")
            specs.append({"portfolio_strategy_id": pid, "strategy_id": sid,
                          "max_share": share})
        return specs

    ids = payload.get("borrower_strategy_ids") or []
    if isinstance(ids, (int, str)):
        ids = [ids]
    for sid in ids:
        specs.append({"portfolio_strategy_id": lender_portfolio_id,
                      "strategy_id": sid,
                      "max_share": payload.get("max_share")})
    return specs


def _queue_expires(now: datetime, ttl_trading_days: int) -> datetime:
    return now + timedelta(days=ttl_trading_days * _TRADING_TO_CALENDAR)


def _expire_queued(db: Session, now: datetime) -> int:
    """把超过 TTL 的排队草稿标 EXPIRED（只改状态，不删记录）。"""
    rows = (db.query(CapitalPairingPlan)
            .filter(CapitalPairingPlan.status == PLAN_QUEUED).all())
    expired = 0
    for plan in rows:
        binding = _load(plan.binding_json)
        queue = binding.get("queue") or {}
        raw = queue.get("expires_at")
        if not raw:
            continue
        try:
            expires_at = datetime.fromisoformat(raw)
        except ValueError:
            continue
        if expires_at <= now:
            queue["expired_at"] = now.isoformat(sep=" ", timespec="seconds")
            binding["queue"] = queue
            plan.binding_json = _dump(binding)
            plan.status = PLAN_EXPIRED
            expired += 1
    if expired:
        db.commit()
    return expired


def _queue_meta(db: Session, plan: CapitalPairingPlan) -> Optional[dict]:
    queue = _load(plan.binding_json).get("queue")
    if not queue:
        return None
    position = 1 + (
        db.query(CapitalPairingPlan)
        .filter(CapitalPairingPlan.status == PLAN_QUEUED,
                CapitalPairingPlan.id < plan.id,
                CapitalPairingPlan.scope == plan.scope)
        .count()
    )
    data = dict(queue)
    data["position"] = position
    return data


# ---------------------------------------------------------------------------
# CRUD
# ---------------------------------------------------------------------------
def _require_strategy(db: Session, strategy_id, portfolio_id: int,
                      label: str) -> Strategy:
    try:
        sid = int(strategy_id)
    except (TypeError, ValueError):
        raise PairingError("%s 缺失或非法" % label)
    strategy = db.get(Strategy, sid)
    if strategy is None:
        raise LookupError("%s 策略不存在：%s" % (label, sid))
    if strategy.portfolio_id != portfolio_id:
        raise PairingError("%s 不属于该组合（%s）" % (label, portfolio_id))
    return strategy


def create_plan(db: Session, payload: dict,
                now: Optional[datetime] = None) -> dict:
    now = now or datetime.now()
    scope = str(payload.get("scope") or "L2").upper()
    if scope not in ("L2", "L3"):
        raise PairingError("scope 仅支持 L2（同组合）/ L3（跨组合）")
    portfolio_id = (payload.get("portfolio_strategy_id")
                    or payload.get("lender_portfolio_strategy_id"))
    ps = db.get(PortfolioStrategy, portfolio_id) if portfolio_id else None
    if ps is None:
        raise LookupError("组合不存在：%s" % portfolio_id)
    lender_id = payload.get("lender_strategy_id")
    if not lender_id:
        raise PairingError("缺少出借方策略")
    lender = _require_strategy(db, lender_id, portfolio_id, "出借方")

    principal = _dec(payload.get("principal"))
    if principal is None or principal <= 0:
        raise PairingError("借出本金必须大于 0")
    # 借期/召回通知期 0 非法：<=0 回退默认（365 / 2）；下游
    # GateReleaseBook 另有 `max(1, recall_notice_days)` 兜底。
    loan_term_days = int(payload.get("loan_term_days") or 365)
    recall_notice_days = int(payload.get("recall_notice_days") or 2)
    release_delay_days = int(payload.get("release_delay_days") or 0)
    max_share = _dec(payload.get("max_share")) or Decimal("1.0")
    if not (Decimal("0") < max_share <= Decimal("1")):
        raise PairingError("借款方最大占比须在 (0,1]")

    specs = _borrower_specs(payload, scope, portfolio_id)
    if scope == "L2":
        if len(specs) != 1:
            raise PairingError("L2 拼盘仅支持 1 个借款方；多借款方请用 L3")
    else:
        if len(specs) < MIN_L3_BORROWERS:
            raise PairingError(
                "L3 拼盘至少 %d 个借款方" % MIN_L3_BORROWERS)
        if len(specs) > MAX_L3_BORROWERS:
            policy = str(payload.get("queue_policy") or "queue").lower()
            if policy == "reject":
                raise PairingError(
                    "BORROWER_LIMIT_REACHED：L3 最多 %d 个借款方，第 %d 个被拒绝"
                    % (MAX_L3_BORROWERS, MAX_L3_BORROWERS + 1))
            return _create_queued_plan(
                db, payload, now, scope, portfolio_id, lender,
                principal, loan_term_days, recall_notice_days,
                release_delay_days, max_share, specs)

    seen = set()
    pairs = []
    for spec in specs:
        sid = spec["strategy_id"]
        if sid is None:
            raise PairingError("借款方策略缺失")
        if sid in seen:
            raise PairingError("借款方重复：%s" % sid)
        seen.add(sid)
        borrower = _require_strategy(
            db, sid, spec["portfolio_strategy_id"], "借款方")
        if borrower.id == lender.id:
            raise PairingError("出借方与借款方不能是同一策略")
        share = _dec(spec.get("max_share")) or max_share
        if not (Decimal("0") < share <= Decimal("1")):
            raise PairingError("借款方 %s 最大占比须在 (0,1]" % borrower.id)
        record_b = _latest_record(db, spec["portfolio_strategy_id"])
        pairs.append((borrower, share, record_b))

    record = _latest_record(db, portfolio_id)
    lender_binding = binding_snapshot(lender)
    borrower_bindings = [binding_snapshot(b) for b, _, _ in pairs]
    loan = {
        "principal": str(principal), "loan_term_days": loan_term_days,
        "recall_notice_days": recall_notice_days,
        "release_delay_days": release_delay_days,
        "max_share": str(max_share),
        "max_borrowers": len(pairs),
    }
    if scope == "L3":
        binding = {
            "binding_version": BINDING_VERSION,
            "lender": lender_binding,
            "borrowers": [
                {"portfolio_strategy_id": spec["portfolio_strategy_id"],
                 "strategy_id": b.id, "binding": bind}
                for spec, (b, _, _), bind in zip(specs, pairs, borrower_bindings)
            ],
            "loan": loan,
        }
        pairing_version = _hash({
            "binding_version": BINDING_VERSION, "scope": scope,
            "lender": {k: lender_binding.get(k) for k in BINDING_FIELDS},
            "borrowers": [
                {"portfolio_strategy_id": spec["portfolio_strategy_id"],
                 "binding": {k: bind.get(k) for k in BINDING_FIELDS}}
                for spec, bind in zip(specs, borrower_bindings)
            ],
            "loan": loan,
        })
    else:
        binding = {
            "binding_version": BINDING_VERSION,
            "lender": lender_binding, "borrower": borrower_bindings[0],
            "loan": loan,
        }
        pairing_version = _pairing_version(
            scope, lender_binding, borrower_bindings[0], loan)

    plan = CapitalPairingPlan(
        name=str(payload.get("name") or ("%s 拼盘" % scope)).strip(),
        scope=scope, status=PLAN_DRAFT,
        portfolio_strategy_id=portfolio_id,
        lender_strategy_id=lender.id,
        lender_record_id=record.id if record else None,
        max_borrowers=len(pairs), principal=principal,
        loan_term_days=loan_term_days,
        recall_notice_days=recall_notice_days,
        release_delay_days=release_delay_days,
        pool_scope=payload.get("pool_scope"),
        ownership_policy=payload.get("ownership_policy"),
        no_cap_reserve_policy=payload.get("no_cap_reserve_policy"),
        gate_hysteresis_days=payload.get("gate_hysteresis_days"),
        cash_outlet_enabled=0,
        binding_json=_dump(binding),
        pairing_version=pairing_version,
        params_hash=_hash({"lender": lender_binding,
                           "borrowers": borrower_bindings}),
        created_by=payload.get("created_by"),
    )
    db.add(plan)
    db.flush()

    for (borrower, share, record_b) in pairs:
        edge = CapitalPairing(
            pairing_plan_id=plan.id, scope=scope,
            lender_type="strategy", lender_id=lender.id,
            lender_record_id=plan.lender_record_id,
            borrower_type="strategy", borrower_id=borrower.id,
            borrower_record_id=record_b.id if record_b else None,
            status="ACTIVE",
            max_share=share, loan_term_days=loan_term_days,
            recall_notice_days=recall_notice_days,
            release_delay_days=release_delay_days,
            eligibility_backtest_id=(
                record_b.id if record_b else plan.lender_record_id),
            params_hash=plan.params_hash, pairing_version=plan.pairing_version,
            binding_json=_dump({"lender": lender_binding,
                                "borrower": binding_snapshot(borrower)}),
            created_by=payload.get("created_by"),
        )
        db.add(edge)
        db.flush()
        result = _precheck(db, plan, lender, borrower, now)
        row = _store_eval(db, plan, result, lender.id, borrower.id, now)
        edge.evaluation_id = row.id

    db.commit()
    return get_plan(db, plan.id)


def _create_queued_plan(db: Session, payload: dict, now: datetime,
                        scope: str, portfolio_id: int, lender: Strategy,
                        principal: Decimal, loan_term_days: int,
                        recall_notice_days: int, release_delay_days: int,
                        max_share: Decimal, specs: List[dict]) -> dict:
    """超过 3 个借款方：整个申请进入 QUEUED，不预分配额度/不建边。"""
    # 排队有效期 0 非法（0 交易日 = 立即过期），<=0 回退默认。
    ttl = int(payload.get("queue_ttl_days") or QUEUE_TTL_TRADING_DAYS)
    if ttl <= 0:
        ttl = QUEUE_TTL_TRADING_DAYS
    expires_at = _queue_expires(now, ttl)
    binding = {
        "binding_version": BINDING_VERSION,
        "lender": binding_snapshot(lender),
        "borrowers": [
            {"portfolio_strategy_id": s["portfolio_strategy_id"],
             "strategy_id": s["strategy_id"],
             "max_share": str(_dec(s.get("max_share")) or max_share)}
            for s in specs
        ],
        "loan": {
            "principal": str(principal), "loan_term_days": loan_term_days,
            "recall_notice_days": recall_notice_days,
            "release_delay_days": release_delay_days,
            "max_share": str(max_share),
        },
        "queue": {
            "policy": "queue",
            "requested": len(specs),
            "limit": MAX_L3_BORROWERS,
            "ttl_trading_days": ttl,
            "enqueued_at": now.isoformat(sep=" ", timespec="seconds"),
            "expires_at": expires_at.isoformat(sep=" ", timespec="seconds"),
        },
    }
    plan = CapitalPairingPlan(
        name=str(payload.get("name") or "L3 拼盘（排队）").strip(),
        scope=scope, status=PLAN_QUEUED,
        portfolio_strategy_id=portfolio_id,
        lender_strategy_id=lender.id,
        lender_record_id=None,
        max_borrowers=MAX_L3_BORROWERS, principal=principal,
        loan_term_days=loan_term_days,
        recall_notice_days=recall_notice_days,
        release_delay_days=release_delay_days,
        pool_scope=payload.get("pool_scope"),
        ownership_policy=payload.get("ownership_policy"),
        no_cap_reserve_policy=payload.get("no_cap_reserve_policy"),
        gate_hysteresis_days=payload.get("gate_hysteresis_days"),
        cash_outlet_enabled=0,
        binding_json=_dump(binding),
        pairing_version=_hash(binding),
        params_hash=_hash({"lender": binding["lender"],
                           "borrowers": binding["borrowers"]}),
        created_by=payload.get("created_by"),
    )
    db.add(plan)
    db.commit()
    return get_plan(db, plan.id)


def list_plans(db: Session, scope: Optional[str] = None,
               status: Optional[str] = None,
               now: Optional[datetime] = None) -> list:
    _expire_queued(db, now or datetime.now())
    q = db.query(CapitalPairingPlan)
    if scope:
        q = q.filter(CapitalPairingPlan.scope == scope)
    if status:
        q = q.filter(CapitalPairingPlan.status == status)
    rows = q.order_by(CapitalPairingPlan.id.desc()).all()
    return [_plan_summary(db, row) for row in rows]


def _edge_payload(edge: CapitalPairing) -> dict:
    return {
        "id": edge.id, "lender_id": edge.lender_id,
        "borrower_id": edge.borrower_id, "status": edge.status,
        "borrower_record_id": edge.borrower_record_id,
        "max_share": str(edge.max_share) if edge.max_share is not None else None,
        "evaluation_id": edge.evaluation_id,
    }


def _plan_summary(db: Session, plan: CapitalPairingPlan) -> dict:
    edge = _edge_of(db, plan.id)
    latest = _latest_eval(db, plan.id)
    edges = _edges_of(db, plan.id)
    return {
        "id": plan.id, "name": plan.name, "scope": plan.scope,
        "status": plan.status,
        "portfolio_strategy_id": plan.portfolio_strategy_id,
        "lender_strategy_id": plan.lender_strategy_id,
        "borrower_strategy_id": edge.borrower_id if edge else None,
        "borrower_count": len(edges),
        "borrowers": [_edge_payload(e) for e in edges],
        "queue": _queue_meta(db, plan) if plan.status == PLAN_QUEUED else None,
        "principal": str(plan.principal) if plan.principal is not None else None,
        "loan_term_days": plan.loan_term_days,
        "pairing_version": plan.pairing_version,
        "backtest_record_id": plan.backtest_record_id,
        "created_at": plan.created_at.isoformat(sep=" ") if plan.created_at else None,
        "approved_at": plan.approved_at.isoformat(sep=" ") if plan.approved_at else None,
        "eval": _eval_payload(latest) if latest else None,
    }


def get_plan(db: Session, plan_id: int) -> dict:
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    edges = _edges_of(db, plan.id)
    data = _plan_summary(db, plan)
    data["binding"] = _load(plan.binding_json)
    data["evidence"] = _load(plan.evidence_json)
    data["edge"] = _edge_payload(edges[0]) if edges else None
    data["edges"] = [_edge_payload(e) for e in edges]
    try:
        data["loan_model_plan"] = build_loan_model_plan(db, plan_id)
    except PairingError:
        data["loan_model_plan"] = None
    return data


def build_loan_model_plan(db: Session, plan_id: int) -> dict:
    """给 ``POST /api/backtest`` 的 ``loan_model_plan``（B3 口径）。"""
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    edges = _edges_of(db, plan.id)
    if not edges:
        raise PairingError("拼盘缺少借款方边，无法生成回测计划")
    borrowers = []
    for edge in edges:
        strategy = db.get(Strategy, edge.borrower_id)
        borrowers.append({
            "portfolio_strategy_id": (
                strategy.portfolio_id if strategy else plan.portfolio_strategy_id),
            "strategy_id": edge.borrower_id,
            "borrower_record_id": edge.borrower_record_id,
            "max_share": (
                str(edge.max_share) if edge.max_share is not None else None),
        })
    return {
        "enabled": True,
        "lender_strategy_id": plan.lender_strategy_id,
        "borrower_strategy_id": edges[0].borrower_id,
        "principal": str(plan.principal) if plan.principal is not None else None,
        "due_days": plan.loan_term_days,
        "pairing_version": plan.pairing_version,
        "borrowers": borrowers,
    }


def approve_plan(db: Session, plan_id: int,
                 now: Optional[datetime] = None) -> dict:
    now = now or datetime.now()
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    if plan.status not in (PLAN_DRAFT, PLAN_WATCH):
        raise PairingError("仅草稿/观察状态可批准，当前 %s" % plan.status)
    edges = _edges_of(db, plan.id)
    if not edges:
        raise PairingError("拼盘缺少借款方边，不能批准")
    # L3：每个借款方都必须有自己的最新 GO 评估，任一 NO_GO 即整盘不可批。
    for edge in edges:
        latest = _latest_eval_for(db, plan.id, edge.borrower_id)
        if latest is None or latest.status != STATUS_GO:
            raise PairingError(
                "借款方 %s 评估未通过（%s），不能批准"
                % (edge.borrower_id,
                   latest.status if latest else "无评估"))
    if not plan.lender_record_id:
        raise PairingError("缺少资格回测记录，不能批准")
    plan.status = PLAN_APPROVED
    plan.approved_at = now
    db.commit()
    return get_plan(db, plan_id)


def delete_plan(db: Session, plan_id: int) -> dict:
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        raise LookupError("拼盘不存在：%s" % plan_id)
    if plan.status not in (PLAN_DRAFT, PLAN_ARCHIVED):
        raise PairingError("仅草稿/归档可删除，当前 %s" % plan.status)
    db.query(CapitalPairingEval).filter(
        CapitalPairingEval.pairing_plan_id == plan.id).delete()
    db.query(CapitalPairing).filter(
        CapitalPairing.pairing_plan_id == plan.id).delete()
    db.delete(plan)
    db.commit()
    return {"deleted": plan_id}


def link_backtest(db: Session, plan_id: int, record_id: int) -> None:
    """回测完成后绑定记录（由 ``POST /api/backtest`` 的 ``pairing_plan_id`` 触发）。"""
    plan = db.get(CapitalPairingPlan, plan_id)
    if plan is None:
        return
    plan.backtest_record_id = record_id
    evidence = _load(plan.evidence_json)
    evidence["backtest_record_id"] = record_id
    plan.evidence_json = _dump(evidence)
    db.commit()
