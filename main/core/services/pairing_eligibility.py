"""借款方资格（0020 §1.4 E1–E12）：只读计算，不写任何表。

用途：拼盘评估前判断「这个组合/策略能不能作为借款方」。所有判定都从已有回测
记录（``backtest_records``）、日快照、成交、评估、walk-forward 与组合配置读取，
不触发回测、不写业务表、不改交易链路。

12 个拒绝码（0020 §1.4）：

===========  ==========================================  ============
码           含义                                        L2 处置
===========  ==========================================  ============
E1 NO_BACKTEST            没有已完成的回测记录            拒绝
E2 EVALUATION_MISSING     无可复用的冻结评估快照          拒绝
E3 PARAM_STALE            参数哈希与当前配置不一致        拒绝（STALE）
E4 WINDOW_SHORT           回测区间不足 3 年（756 交易日） 拒绝
E5 OOS_MISSING            没有 ≥1 年样本外验证            拒绝
E6 TRADE_SAMPLE_SMALL     成交样本/持仓天数不足           拒绝
E7 GATE_COVERAGE_LOW      门控/空头段覆盖不足             拒绝
E8 PORTFOLIO_INVALID      组合配置不完整（资金/池/槽位）  拒绝
E9 CAPACITY_MISSING       容量/ADV 数据不足               降级（L3 拒绝）
E10 RISK_DEFAULTED        风控参数缺失（用保守默认）      降级
E11 DATA_QUALITY          数据质量不达标                  拒绝
E12 DECISION_TIME_LEAK    记录完成时间晚于当前决策时点    拒绝
===========  ==========================================  ============

状态：任一 reject → ``INELIGIBLE``；E3 不通过 → ``STALE``（优先于 INELIGIBLE）；
仅有 warn → ``WATCH``；否则 ``ELIGIBLE``。
"""
import hashlib
import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Optional

from sqlalchemy import func
from sqlalchemy.orm import Session

from core.models import (
    BacktestDailySnapshot,
    BacktestEvaluation,
    BacktestRecord,
    BacktestTrade,
    PortfolioStrategy,
    StockPoolStock,
    WalkForwardRun,
)
from core.services.backtest_service import (
    _portfolio_strategies,
    build_params_snapshot,
    pool_composition,
)

logger = logging.getLogger(__name__)

STATUS_ELIGIBLE = "ELIGIBLE"
STATUS_WATCH = "WATCH"
STATUS_INELIGIBLE = "INELIGIBLE"
STATUS_STALE = "STALE"

SEVERITY_REJECT = "reject"
SEVERITY_WARN = "warn"

E_NO_BACKTEST = "E1_NO_BACKTEST"
E_EVALUATION_MISSING = "E2_EVALUATION_MISSING"
E_PARAM_STALE = "E3_PARAM_STALE"
E_WINDOW_SHORT = "E4_WINDOW_SHORT"
E_OOS_MISSING = "E5_OOS_MISSING"
E_TRADE_SAMPLE_SMALL = "E6_TRADE_SAMPLE_SMALL"
E_GATE_COVERAGE_LOW = "E7_GATE_COVERAGE_LOW"
E_PORTFOLIO_INVALID = "E8_PORTFOLIO_INVALID"
E_CAPACITY_MISSING = "E9_CAPACITY_MISSING"
E_RISK_DEFAULTED = "E10_RISK_DEFAULTED"
E_DATA_QUALITY = "E11_DATA_QUALITY"
E_DECISION_TIME_LEAK = "E12_DECISION_TIME_LEAK"

# 0020 §1.3 门槛（与文档保持一致；改阈值先改文档）
MIN_TRADING_DAYS = 756          # 3 年 × 252
MIN_OOS_YEARS = 1
MIN_TRADES = 100
MIN_HOLDING_DAYS = 250
MIN_GATE_BULL_DAYS = 60
MIN_GATE_BEAR_DAYS = 60
MIN_DATA_COVERAGE = 0.98

# 快照/配置里必须存在、否则视为「旧记录口径未知」的键
_REQUIRED_FROZEN_KEYS = (
    "strategies",
    "stock_pool_codes",
    "no_trade_bar_policy",
    "stamp_duty_segments",
    "liquidity_caliber",
)


def _check(code, rule, passed, message, severity=SEVERITY_REJECT, detail=None):
    return {
        "code": code,
        "rule": rule,
        "passed": bool(passed),
        "severity": severity if not passed else None,
        "message": message,
        "detail": detail or {},
    }


def _load_json(text):
    try:
        value = json.loads(text) if text else None
    except (TypeError, ValueError):
        return None
    return value if isinstance(value, dict) else None


def _hash(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True,
                   default=str).encode("utf-8")
    ).hexdigest()


def _normalize(value):
    """数值语义归一：同一配置可能被序列化成 ``"0.00025"`` / ``"0.000250"``
    （``_snapshot_value`` 对 Decimal 直接 ``str()``，写入前对象与从库读回对象的
    Decimal 尾零位数不同），逐字比较会把未变化的配置误判为 E3 参数漂移。
    这里把「能解析为数值」的 int/float/Decimal/str 统一按 Decimal 比较与哈希；
    bool/None/非数值字符串保持原样。
    """
    if isinstance(value, dict):
        return {k: _normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, (int, float, Decimal)):
        try:
            number = Decimal(str(value))
        except InvalidOperation:
            return value
        return number.normalize() if number.is_finite() else value
    if isinstance(value, str):
        try:
            number = Decimal(value)
        except InvalidOperation:
            return value
        return number.normalize() if number.is_finite() else value
    return value


def _frozen_snapshot_args(frozen: dict) -> dict:
    """用记录冻结值还原 run 级参数，使重建快照只暴露「配置漂移」。"""
    return dict(
        dividend_mode=frozen.get("dividend_mode", "front"),
        fee_reserve_enabled=bool(frozen.get("fee_reserve_enabled", False)),
        bond_t0_enabled=bool(frozen.get("bond_t0_enabled", False)),
        bond_lot_enabled=bool(frozen.get("bond_lot_enabled", False)),
        adaptive_slippage_enabled=frozen.get("adaptive_slippage_enabled"),
        slippage_impact_coef=frozen.get("slippage_impact_coef"),
        volume_limit_enabled=frozen.get("volume_limit_enabled"),
        volume_limit_ratio=frozen.get("volume_limit_ratio"),
        equity_cap=frozen.get("equity_cap"),
        equity_cap_mode=frozen.get("equity_cap_mode", "cap"),
        capacity_model_version=frozen.get("capacity_model_version"),
        liquidity_caliber=frozen.get("liquidity_caliber", "same_day_bar"),
        daily_close_peak_enabled=bool(frozen.get("daily_close_peak_enabled", False)),
        daily_loss_basis=frozen.get("daily_loss_basis", "equity"),
        data_start=frozen.get("data_start_date"),
        market_gate_enabled=frozen.get("market_gate_enabled"),
        gate_summary=frozen.get("gate_summary"),
        corporate_actions=frozen.get("corporate_actions"),
        pool_timeline=frozen.get("pool_timeline"),
        execution=frozen.get("execution"),
    )


def config_fingerprint(ps, db: Session, frozen: dict) -> dict:
    """记录冻结配置 vs 当前配置：返回哈希、漂移字段与缺失字段（只读）。"""
    current = _normalize(json.loads(build_params_snapshot(
        ps,
        strategies=_portfolio_strategies(ps, db),
        stock_pool_codes=pool_composition(ps, db),
        **_frozen_snapshot_args(frozen),
    )))
    baseline = _normalize(frozen)
    missing = sorted(
        set(k for k in current if k not in baseline)
        | {k for k in _REQUIRED_FROZEN_KEYS if k not in baseline})
    diff = sorted(
        k for k in current if k in baseline and current[k] != baseline[k])
    return {
        "frozen": _hash({k: baseline[k] for k in current if k in baseline}),
        "current": _hash(current),
        "diff_fields": diff,
        "missing_fields": missing,
    }


def _latest_record(db: Session, portfolio_strategy_id: int,
                   record_id: Optional[int]) -> Optional[BacktestRecord]:
    if record_id is not None:
        rec = db.get(BacktestRecord, record_id)
        if rec is None or rec.portfolio_strategy_id != portfolio_strategy_id:
            return None
        return rec
    return (
        db.query(BacktestRecord)
        .filter(
            BacktestRecord.portfolio_strategy_id == portfolio_strategy_id,
            BacktestRecord.status == "completed",
        )
        .order_by(BacktestRecord.completed_at.desc(), BacktestRecord.id.desc())
        .first()
    )


def _trading_days(db: Session, record_id: int) -> int:
    return int(
        db.query(func.count(func.distinct(BacktestDailySnapshot.snap_date)))
        .filter(BacktestDailySnapshot.backtest_record_id == record_id)
        .scalar() or 0
    )


def _holding_days(db: Session, record_id: int) -> int:
    q = db.query(func.count(BacktestDailySnapshot.id)).filter(
        BacktestDailySnapshot.backtest_record_id == record_id,
        BacktestDailySnapshot.positions_json.isnot(None),
        ~BacktestDailySnapshot.positions_json.in_(("", "[]", "null")),
    )
    return int(q.scalar() or 0)


def _gate_days(db: Session, record_id: int):
    base = db.query(func.count(BacktestDailySnapshot.id)).filter(
        BacktestDailySnapshot.backtest_record_id == record_id)
    bull = int(base.filter(
        BacktestDailySnapshot.gate_state.in_((1, 2))).scalar() or 0)
    bear = int(base.filter(BacktestDailySnapshot.gate_state == -1).scalar() or 0)
    return bull, bear


def _latest_oos_years(db: Session, portfolio_strategy_id: int):
    wf = (
        db.query(WalkForwardRun)
        .filter(
            WalkForwardRun.portfolio_strategy_id == portfolio_strategy_id,
            WalkForwardRun.status == "completed",
        )
        .order_by(WalkForwardRun.completed_at.desc(), WalkForwardRun.id.desc())
        .first()
    )
    return (wf.id, int(wf.test_years or 0)) if wf is not None else (None, 0)


def evaluate_eligibility(db: Session, portfolio_strategy_id: int,
                         record_id: Optional[int] = None,
                         scope: str = "L2",
                         now: Optional[datetime] = None) -> dict:
    """计算组合的策略资格（只读）。

    Raises:
        LookupError: 组合不存在。
    """
    ps = db.get(PortfolioStrategy, portfolio_strategy_id)
    if ps is None:
        raise LookupError("组合不存在：%s" % portfolio_strategy_id)
    scope = "L3" if str(scope).upper() == "L3" else "L2"
    now = now or datetime.now()
    record = _latest_record(db, portfolio_strategy_id, record_id)
    checks = []
    metrics = {"scope": scope, "record_id": record.id if record else None}

    # E1 有已完成回测
    ok_e1 = record is not None and record.status == "completed"
    checks.append(_check(
        E_NO_BACKTEST, "已完成回测记录", ok_e1,
        "回测记录 #%s 已完成" % record.id if ok_e1
        else ("回测记录不存在或未完成" if record is None else
              "回测记录 #%s 状态为 %s" % (record.id, record.status))))

    # E2 冻结评估
    has_eval = bool(record and db.query(BacktestEvaluation.id).filter(
        BacktestEvaluation.backtest_record_id == record.id).first())
    checks.append(_check(
        E_EVALUATION_MISSING, "冻结评估快照", has_eval,
        "存在冻结评估" if has_eval else "缺少该回测的评估快照"))

    # E3 参数哈希
    fp = {"frozen": None, "current": None, "diff_fields": [], "missing_fields": []}
    if record is not None and record.params_snapshot:
        frozen = _load_json(record.params_snapshot) or {}
        fp = config_fingerprint(ps, db, frozen)
        ok_e3 = not fp["missing_fields"] and not fp["diff_fields"]
        checks.append(_check(
            E_PARAM_STALE, "参数哈希与当前配置一致", ok_e3,
            "参数哈希一致" if ok_e3 else (
                "参数已变化：缺失 %s，漂移 %s" % (
                    ", ".join(fp["missing_fields"]) or "-",
                    ", ".join(fp["diff_fields"]) or "-")),
            detail=fp))
    else:
        checks.append(_check(
            E_PARAM_STALE, "参数哈希与当前配置一致", False,
            "记录缺少参数快照，无法复现"))

    # E4 区间长度
    days = _trading_days(db, record.id) if record else 0
    metrics["trading_days"] = days
    checks.append(_check(
        E_WINDOW_SHORT, "回测区间 ≥ %d 交易日" % MIN_TRADING_DAYS,
        days >= MIN_TRADING_DAYS,
        "交易日 %d" % days, detail={"trading_days": days}))

    # E5 样本外
    wf_id, years = _latest_oos_years(db, portfolio_strategy_id)
    metrics["oos_years"] = years
    checks.append(_check(
        E_OOS_MISSING, "样本外验证 ≥ %d 年" % MIN_OOS_YEARS,
        years >= MIN_OOS_YEARS,
        "walk-forward #%s 样本外 %s 年" % (wf_id, years) if wf_id else
        "没有已完成的 walk-forward 运行",
        detail={"run_id": wf_id, "test_years": years}))

    # E6 成交样本与持仓天数
    trades = int(db.query(func.count(BacktestTrade.id)).filter(
        BacktestTrade.backtest_record_id == record.id).scalar() or 0) if record else 0
    hold_days = _holding_days(db, record.id) if record else 0
    metrics.update({"trades": trades, "holding_days": hold_days})
    ok_e6 = trades >= MIN_TRADES and hold_days >= MIN_HOLDING_DAYS
    checks.append(_check(
        E_TRADE_SAMPLE_SMALL, "成交 ≥ %d 笔且持仓 ≥ %d 天" % (MIN_TRADES, MIN_HOLDING_DAYS),
        ok_e6, "成交 %d 笔 / 持仓 %d 天" % (trades, hold_days),
        detail={"trades": trades, "holding_days": hold_days}))

    # E7 门控覆盖
    bull, bear = _gate_days(db, record.id) if record else (0, 0)
    metrics.update({"gate_bull_days": bull, "gate_bear_days": bear})
    ok_e7 = bull >= MIN_GATE_BULL_DAYS and bear >= MIN_GATE_BEAR_DAYS
    checks.append(_check(
        E_GATE_COVERAGE_LOW,
        "多空门控覆盖 ≥ %d/%d 天" % (MIN_GATE_BULL_DAYS, MIN_GATE_BEAR_DAYS),
        ok_e7, "多头 %d 天 / 空头 %d 天（未启用门控时为 0）" % (bull, bear),
        detail={"bull_days": bull, "bear_days": bear}))

    # E8 组合完整性
    pool_size = int(db.query(func.count(StockPoolStock.id)).filter(
        StockPoolStock.pool_id == ps.stock_pool_id).scalar() or 0)
    cap = float(ps.initial_capital or 0)
    slots = int(ps.max_holdings or 0)
    ok_e8 = cap > 0 and slots > 0 and pool_size > 0
    metrics.update({"pool_size": pool_size, "initial_capital": cap,
                    "max_holdings": slots})
    checks.append(_check(
        E_PORTFOLIO_INVALID, "组合资金/槽位/股票池完整", ok_e8,
        "初始资金 %.0f / 槽位 %d / 池 %d 只" % (cap, slots, pool_size),
        detail={"initial_capital": cap, "max_holdings": slots,
                "pool_size": pool_size}))

    # E9 容量数据（L2 降级、L3 拒绝）
    validation = _load_json(record.validation_json) if record else None
    capacity = validation.get("capacity") if validation else None
    if capacity is None:
        checks.append(_check(
            E_CAPACITY_MISSING, "容量/ADV 数据可用",
            False, "记录缺少容量评估（旧回测或未接线）",
            severity=SEVERITY_REJECT if scope == "L3" else SEVERITY_WARN))
    else:
        ok_e9 = len(capacity) == 0
        checks.append(_check(
            E_CAPACITY_MISSING, "容量/ADV 数据可用", ok_e9,
            "容量健康" if ok_e9 else "容量预警 %d 条" % len(capacity),
            severity=SEVERITY_REJECT if scope == "L3" else SEVERITY_WARN,
            detail={"warnings": capacity[:5]}))

    # E10 风控参数
    risk_ok = bool(ps.max_drawdown and float(ps.max_drawdown) > 0
                   and ps.daily_loss_limit and float(ps.daily_loss_limit) > 0)
    checks.append(_check(
        E_RISK_DEFAULTED, "风控参数显式配置", risk_ok,
        "风控参数完整" if risk_ok else "风控参数缺失，使用保守默认值",
        severity=SEVERITY_WARN,
        detail={"max_drawdown": float(ps.max_drawdown or 0),
                "daily_loss_limit": float(ps.daily_loss_limit or 0)}))

    # E11 数据质量
    quality = _load_json(record.data_quality_json) if record else None
    if quality is None:
        checks.append(_check(
            E_DATA_QUALITY, "数据质量达标", False,
            "记录缺少数据质量快照（旧回测），无法确认取数完整性"))
    else:
        summary = quality.get("summary") or {}
        coverage = float(summary.get("coverage_ratio") or 0)
        missing_stocks = quality.get("missing_stocks") or []
        ok_e11 = coverage >= MIN_DATA_COVERAGE and not missing_stocks
        checks.append(_check(
            E_DATA_QUALITY, "覆盖率 ≥ %.0f%%" % (MIN_DATA_COVERAGE * 100),
            ok_e11,
            "覆盖率 %.2f%% / 缺失标的 %d" % (coverage * 100, len(missing_stocks)),
            detail={"coverage_ratio": coverage,
                    "missing_stocks": missing_stocks[:10]}))

    # E12 决策时点不泄漏
    completed = record.completed_at if record else None
    ok_e12 = completed is not None and completed <= now
    checks.append(_check(
        E_DECISION_TIME_LEAK, "记录完成时间不晚于决策时点", ok_e12,
        "完成于 %s" % completed.isoformat(sep=" ") if completed else
        "记录缺少完成时间或时间晚于当前决策时点",
        detail={"completed_at": completed.isoformat(sep=" ") if completed else None}))

    failed = [c for c in checks if not c["passed"]]
    codes = [
        {"code": c["code"], "severity": c["severity"], "message": c["message"]}
        for c in failed
    ]
    if any(c["code"] == E_PARAM_STALE for c in failed):
        status = STATUS_STALE
    elif any(c["severity"] == SEVERITY_REJECT for c in failed):
        status = STATUS_INELIGIBLE
    elif failed:
        status = STATUS_WATCH
    else:
        status = STATUS_ELIGIBLE

    return {
        "portfolio_strategy_id": portfolio_strategy_id,
        "record_id": record.id if record else None,
        "scope": scope,
        "decided_at": now.isoformat(sep=" "),
        "status": status,
        "codes": codes,
        "checks": checks,
        "metrics": metrics,
        "fingerprint": fp,
    }
