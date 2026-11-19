"""搭配评估 H1–H8 + 软判据（0020 §3，0021 B5）。

只读：从已完成的回测记录（日快照 / 成交 / 评估）、walk-forward 样本外窗口与
组合配置计算，不触发回测、不写表、不改交易链路。

判定（0020 §3.4，不采用加权总分）：

1. 任一硬约束失败 → ``NO_GO``；
2. 硬约束因数据未就绪无法判定（容量/ADV 属 C 层）→ ``WATCH``（不可进回测）；
3. 软判据「不通过」≥ 2 项 → ``NO_GO``；
4. 「不通过」= 1 项或「警告」≥ 3 项 → ``WATCH``；
5. 其余 → ``GO``（仍需联合回测确认）。

依赖 C 层（容量/ADV/容量曲线）的特征标 ``PENDING_C``，依赖 D 层（策略级门控
时间轴）的标 ``PENDING_D``，依赖联合回测（B6/E）的标 ``PENDING_BACKTEST``。
待定特征不参与通过/不通过计票；``GO`` 的语义是「值得进入联合回测」。
"""
import json
import logging
import math
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from statistics import median
from typing import Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from core.models import (
    BacktestDailySnapshot,
    BacktestEvaluation,
    BacktestRecord,
    BacktestTrade,
    PortfolioStrategy,
    WalkForwardRun,
)
from core.engine.market_gate import build_gate_timeline, gate_off_overlap
from core.services.pairing_eligibility import (
    STATUS_ELIGIBLE,
    STATUS_WATCH as ELIG_WATCH,
    evaluate_eligibility,
)

logger = logging.getLogger(__name__)

PAIRING_EVAL_VERSION = "pairing-eval-d1"

STATUS_GO = "GO"
STATUS_WATCH = "WATCH"
STATUS_NO_GO = "NO_GO"

STATE_PASS = "pass"
STATE_WARN = "warn"
STATE_FAIL = "fail"
STATE_PENDING = "pending"

PENDING_C = "PENDING_C"
PENDING_D = "PENDING_D"
PENDING_BACKTEST = "PENDING_BACKTEST"

# 0020 §3.2 硬约束阈值
MIN_COMMON_OOS_DAYS = 252
MAX_CODE_OVERLAP = 0.60
MAX_CASH_TIMING_OVERLAP = 0.80
COMBINED_DD_EXTRA = 0.01

# 0020 §3.3 软判据阈值
CORR_PASS, CORR_FAIL = 0.30, 0.70
DD_OVERLAP_PASS, DD_OVERLAP_FAIL = 0.30, 0.50
DD_LIMIT_WARN = 0.75
DD_DURATION_PASS, DD_DURATION_FAIL = 120, 180
HOLDING_RATIO_PASS, HOLDING_RATIO_WARN = 2.0, 1.2
TURNOVER_PASS, TURNOVER_FAIL = 45.0, 60.0
SIGNAL_OVERLAP_PASS, SIGNAL_OVERLAP_FAIL = 0.30, 0.60
CASH_COMPLEMENT_PASS, CASH_COMPLEMENT_FAIL = 0.30, 0.10
GATE_OVERLAP_PASS, GATE_OVERLAP_FAIL = 0.50, 0.70   # 0020 §3.3 门控关闭重合

DRAWDOWN_DEPTH = 0.03          # 回撤重叠：跌超 3% 才算「处于回撤」
DEFAULT_DD_LIMIT = 0.20        # 组合风控上限缺失时的保守默认


# ---------------------------------------------------------------------------
# 基础工具
# ---------------------------------------------------------------------------
def _dec(value) -> Decimal:
    if value is None:
        return Decimal("0")
    if isinstance(value, Decimal):
        return value
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _f(value) -> Optional[float]:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _load_json(text) -> dict:
    try:
        value = json.loads(text) if text else None
    except (TypeError, ValueError):
        return {}
    return value if isinstance(value, dict) else {}


def _parse_date(value) -> Optional[date]:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10])
        except ValueError:
            return None
    return None


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


def _oos_window(db: Session, portfolio_strategy_id: int):
    """最近一次已完成 walk-forward 的最后一个窗口样本外区间。"""
    wf = (
        db.query(WalkForwardRun)
        .filter(
            WalkForwardRun.portfolio_strategy_id == portfolio_strategy_id,
            WalkForwardRun.status == "completed",
        )
        .order_by(WalkForwardRun.completed_at.desc(), WalkForwardRun.id.desc())
        .first()
    )
    if wf is None or not wf.summary_json:
        return None
    payload = _load_json(wf.summary_json)
    for window in reversed(payload.get("windows") or []):
        test = (window or {}).get("test") or {}
        start, end = _parse_date(test.get("start")), _parse_date(test.get("end"))
        if start and end:
            return start, end
    return None


def _gate_flag(record: Optional[BacktestRecord]) -> Optional[bool]:
    """该回测口径下组合是否受门控（``None`` = 旧记录未冻结开关，口径未知）。"""
    if record is None:
        return None
    params = _load_json(record.params_snapshot)
    if "market_gate_enabled" not in params:
        return None
    if params.get("market_gate_enabled"):
        return True
    return any((row or {}).get("market_gate_mode") == "on"
               for row in params.get("strategies") or [])


def _side_snapshots(db: Session, record_id: int, window=None) -> list:
    q = db.query(BacktestDailySnapshot).filter(
        BacktestDailySnapshot.backtest_record_id == record_id)
    if window is not None:
        q = q.filter(
            BacktestDailySnapshot.snap_date >= window[0],
            BacktestDailySnapshot.snap_date <= window[1])
    return q.order_by(BacktestDailySnapshot.snap_date).all()


def _returns(snaps: list) -> Dict[date, float]:
    """日收益序列：优先取快照 daily_return，缺失时按 total_value 差分。"""
    out: Dict[date, float] = {}
    prev: Optional[float] = None
    for snap in snaps:
        value = _f(snap.total_value)
        ret = _f(snap.daily_return)
        if ret is None and prev is not None and prev > 0 and value is not None:
            ret = value / prev - 1.0
        if ret is not None:
            out[snap.snap_date] = ret
        prev = value if value is not None else prev
    return out


def _drawdown_series(snaps: list) -> Dict[date, float]:
    out: Dict[date, float] = {}
    peak: Optional[float] = None
    for snap in snaps:
        value = _f(snap.total_value)
        if value is None or value <= 0:
            continue
        peak = value if peak is None else max(peak, value)
        out[snap.snap_date] = value / peak - 1.0
    return out


def _max_drawdown(snaps: list) -> float:
    series = _drawdown_series(snaps)
    return abs(min(series.values())) if series else 0.0


def _drawdown_durations(snaps: list) -> List[int]:
    """峰值 → 恢复（或区间结束）的交易日数；只保留期间跌超 3% 的片段。"""
    durations: List[int] = []
    peak_value: Optional[float] = None
    peak_index = -1
    trough = 0.0
    for index, snap in enumerate(snaps):
        value = _f(snap.total_value)
        if value is None or value <= 0:
            continue
        if peak_value is None or value >= peak_value:
            if peak_value is not None and trough <= peak_value * (1 - DRAWDOWN_DEPTH):
                durations.append(index - peak_index)
            peak_value, peak_index, trough = value, index, value
            continue
        trough = min(trough, value)
    if peak_value is not None and trough <= peak_value * (1 - DRAWDOWN_DEPTH):
        durations.append(len(snaps) - 1 - peak_index)
    return durations


def _percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    idx = max(0, min(len(ordered) - 1, int(math.ceil(q * len(ordered))) - 1))
    return ordered[idx]


def _jaccard(a: set, b: set) -> float:
    union = a | b
    if not union:
        return 0.0
    return len(a & b) / len(union)


def _ranks(values: List[float]) -> List[float]:
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [0.0] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        rank = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[order[k]] = rank
        i = j + 1
    return ranks


def _spearman(xs: List[float], ys: List[float]) -> Optional[float]:
    if len(xs) < 3 or len(xs) != len(ys):
        return None
    rx, ry = _ranks(xs), _ranks(ys)
    n = len(rx)
    mx, my = sum(rx) / n, sum(ry) / n
    cov = sum((a - mx) * (b - my) for a, b in zip(rx, ry))
    vx = sum((a - mx) ** 2 for a in rx)
    vy = sum((b - my) ** 2 for b in ry)
    if vx <= 0 or vy <= 0:
        return None
    return cov / math.sqrt(vx * vy)


def _positions(text) -> Dict[str, float]:
    """positions_json → {code: 市值}（组合层已按 code 聚合）。"""
    if not text:
        return {}
    try:
        rows = json.loads(text)
    except (TypeError, ValueError):
        return {}
    out: Dict[str, float] = {}
    if not isinstance(rows, list):
        return out
    for row in rows:
        if not isinstance(row, dict):
            continue
        code = row.get("stock_code") or row.get("code")
        if not code:
            continue
        out[code] = out.get(code, 0.0) + (_f(row.get("market_value")) or 0.0)
    return out


def _weights(positions: Dict[str, float]) -> Dict[str, float]:
    total = sum(positions.values())
    if total <= 0:
        return {}
    return {code: value / total for code, value in positions.items()}


def _weighted_overlap(days_a: Dict[date, Dict[str, float]],
                      days_b: Dict[date, Dict[str, float]]) -> Optional[float]:
    common = sorted(set(days_a) & set(days_b))
    if not common:
        return None
    values = []
    for day in common:
        wa, wb = _weights(days_a[day]), _weights(days_b[day])
        if not wa or not wb:
            continue
        values.append(sum(min(wa.get(code, 0.0), wb.get(code, 0.0))
                          for code in set(wa) | set(wb)))
    return sum(values) / len(values) if values else None


def _buy_amount_by_day(db: Session, record_id: int) -> Dict[date, float]:
    rows = (
        db.query(BacktestTrade.bar_time, func.sum(BacktestTrade.amount))
        .filter(
            BacktestTrade.backtest_record_id == record_id,
            BacktestTrade.trade_type == "BUY",
        )
        .group_by(BacktestTrade.bar_time)
        .all()
    )
    out: Dict[date, float] = {}
    for bar_time, amount in rows:
        day = bar_time.date() if isinstance(bar_time, datetime) else bar_time
        if day is None:
            continue
        out[day] = out.get(day, 0.0) + (_f(amount) or 0.0)
    return out


def _open_signal_pairs(db: Session, record_id: int) -> set:
    rows = (
        db.query(BacktestTrade.bar_time, BacktestTrade.stock_code)
        .filter(
            BacktestTrade.backtest_record_id == record_id,
            BacktestTrade.signal_type == "OPEN",
        )
        .all()
    )
    pairs = set()
    for bar_time, code in rows:
        day = bar_time.date() if isinstance(bar_time, datetime) else bar_time
        if day is not None:
            pairs.add((day, code))
    return pairs


def _median_holding_days(db: Session, record_id: int) -> Optional[float]:
    """FIFO 配对买卖，返回中位持有交易日数（自然日，含首尾）。"""
    rows = (
        db.query(BacktestTrade.bar_time, BacktestTrade.stock_code,
                 BacktestTrade.trade_type, BacktestTrade.quantity)
        .filter(BacktestTrade.backtest_record_id == record_id)
        .order_by(BacktestTrade.bar_time, BacktestTrade.id)
        .all()
    )
    lots: Dict[str, List[List]] = {}
    holding: List[int] = []
    for bar_time, code, trade_type, quantity in rows:
        day = bar_time.date() if isinstance(bar_time, datetime) else bar_time
        qty = int(quantity or 0)
        if day is None or qty <= 0:
            continue
        if trade_type == "BUY":
            lots.setdefault(code, []).append([day, qty])
            continue
        if trade_type != "SELL":
            continue
        queue = lots.setdefault(code, [])
        remaining = qty
        while remaining > 0 and queue:
            entry = queue[0]
            used = min(remaining, entry[1])
            holding.append((day - entry[0]).days)
            entry[1] -= used
            remaining -= used
            if entry[1] <= 0:
                queue.pop(0)
    return float(median(holding)) if holding else None


def _annual_turnover(db: Session, record_id: int) -> Optional[float]:
    row = (
        db.query(BacktestEvaluation.annual_turnover)
        .filter(
            BacktestEvaluation.backtest_record_id == record_id,
            BacktestEvaluation.target_type == "portfolio",
        )
        .order_by(BacktestEvaluation.id.desc())
        .first()
    )
    return _f(row[0]) if row else None


def _dd_limit(ps: PortfolioStrategy) -> Tuple[float, bool]:
    limit = _f(ps.max_drawdown)
    if limit is None or limit <= 0:
        return DEFAULT_DD_LIMIT, True
    return limit, False


# ---------------------------------------------------------------------------
# 判定构造
# ---------------------------------------------------------------------------
def _hard(code, rule, state, message, detail=None):
    return {"code": code, "rule": rule, "state": state,
            "message": message, "detail": detail or {}}


def _soft(feature, state, value, message, pending_code=None, detail=None):
    return {"feature": feature, "state": state, "value": value,
            "message": message, "pending_code": pending_code,
            "detail": detail or {}}


def _grade(value, pass_max, fail_from, *, lower_is_better=True):
    if value is None:
        return STATE_PENDING
    if lower_is_better:
        if value <= pass_max:
            return STATE_PASS
        return STATE_FAIL if value > fail_from else STATE_WARN
    if value >= pass_max:
        return STATE_PASS
    return STATE_FAIL if value < fail_from else STATE_WARN


def _decide(hard: list, soft: list) -> Tuple[str, list]:
    hard_failed = [h for h in hard if h["state"] == STATE_FAIL]
    if hard_failed:
        return STATUS_NO_GO, [h["code"] for h in hard_failed]
    hard_pending = [h for h in hard if h["state"] == STATE_PENDING]
    if hard_pending:
        return STATUS_WATCH, [h["code"] for h in hard_pending]
    soft_fail = [s for s in soft if s["state"] == STATE_FAIL]
    soft_warn = [s for s in soft if s["state"] == STATE_WARN]
    if len(soft_fail) >= 2:
        return STATUS_NO_GO, [s["feature"] for s in soft_fail]
    if len(soft_fail) == 1 or len(soft_warn) >= 3:
        return STATUS_WATCH, [s["feature"] for s in soft_fail]
    return STATUS_GO, []


# ---------------------------------------------------------------------------
# 主入口
# ---------------------------------------------------------------------------
def evaluate_pairing(db: Session, lender_portfolio_strategy_id: int,
                     borrower_portfolio_strategy_id: int,
                     lender_record_id: Optional[int] = None,
                     borrower_record_id: Optional[int] = None,
                     scope: str = "L2",
                     capacity: Optional[dict] = None,
                     now: Optional[datetime] = None) -> dict:
    """计算两个组合策略能否搭配（只读）。

    ``capacity`` 为 C 层容量模型预计算的结果（None = 尚未接线，相应判据标
    ``PENDING_C``）：

    - ``lender_participation`` / ``borrower_participation`` / ``same_code_participation``
      用于 H4（各自 ≤ 1%、同票合计 ≤ 2%）；
    - ``marginal_1x`` / ``marginal_2x`` 用于 H6（均须 > 0）；
    - ``overlap`` / ``bid_probability`` / ``curve_decay_2x`` 用于软判据。

    Raises:
        LookupError: 组合不存在、指定回测记录不属于该组合，或双方为同一组合。
    """
    scope = "L3" if str(scope).upper() == "L3" else "L2"
    now = now or datetime.now()
    lender_ps = db.get(PortfolioStrategy, lender_portfolio_strategy_id)
    borrower_ps = db.get(PortfolioStrategy, borrower_portfolio_strategy_id)
    if lender_ps is None:
        raise LookupError("组合不存在：%s" % lender_portfolio_strategy_id)
    if borrower_ps is None:
        raise LookupError("组合不存在：%s" % borrower_portfolio_strategy_id)
    if lender_portfolio_strategy_id == borrower_portfolio_strategy_id:
        raise LookupError("出借方与借款方不能是同一组合")

    lender_rec = _latest_record(db, lender_portfolio_strategy_id, lender_record_id)
    borrower_rec = _latest_record(db, borrower_portfolio_strategy_id, borrower_record_id)

    # H1 双方资格
    lender_elig = evaluate_eligibility(
        db, lender_portfolio_strategy_id, record_id=lender_record_id,
        scope=scope, now=now)
    borrower_elig = evaluate_eligibility(
        db, borrower_portfolio_strategy_id, record_id=borrower_record_id,
        scope=scope, now=now)
    hard: list = []
    eligible_states = (STATUS_ELIGIBLE, ELIG_WATCH)
    h1_ok = (lender_elig["status"] in eligible_states
             and borrower_elig["status"] in eligible_states)
    hard.append(_hard(
        "H1", "双方资格通过", STATE_PASS if h1_ok else STATE_FAIL,
        "资格通过（出借方 %s / 借款方 %s）" % (
            lender_elig["status"], borrower_elig["status"]) if h1_ok else
        "资格未通过（出借方 %s / 借款方 %s）" % (
            lender_elig["status"], borrower_elig["status"]),
        {"lender_status": lender_elig["status"],
         "borrower_status": borrower_elig["status"],
         "lender_codes": [c["code"] for c in lender_elig["codes"]],
         "borrower_codes": [c["code"] for c in borrower_elig["codes"]]}))

    # 样本外窗口：两边取交集；任一缺失 → 硬约束无法验证
    lender_window = _oos_window(db, lender_portfolio_strategy_id)
    borrower_window = _oos_window(db, borrower_portfolio_strategy_id)
    window = None
    if lender_window and borrower_window:
        start = max(lender_window[0], borrower_window[0])
        end = min(lender_window[1], borrower_window[1])
        if start <= end:
            window = (start, end)

    lender_snaps = (_side_snapshots(db, lender_rec.id, window)
                    if lender_rec and window else [])
    borrower_snaps = (_side_snapshots(db, borrower_rec.id, window)
                      if borrower_rec and window else [])
    lender_days = _returns(lender_snaps)
    borrower_days = _returns(borrower_snaps)
    common_days = sorted(set(lender_days) & set(borrower_days))

    # H2 共同样本外长度
    h2_state = (STATE_PASS if len(common_days) >= MIN_COMMON_OOS_DAYS
                else STATE_FAIL)
    hard.append(_hard(
        "H2", "共同样本外长度 ≥ %d 交易日" % MIN_COMMON_OOS_DAYS, h2_state,
        "共同样本外 %d 个交易日%s" % (
            len(common_days), "" if window else "（缺少样本外窗口）"),
        {"common_oos_days": len(common_days),
         "window": {"start": window[0].isoformat(), "end": window[1].isoformat()}
         if window else None}))

    # H3 持仓加权重叠
    lender_pos = {s.snap_date: _weights(_positions(s.positions_json))
                  for s in lender_snaps}
    borrower_pos = {s.snap_date: _weights(_positions(s.positions_json))
                    for s in borrower_snaps}
    overlap = _weighted_overlap(lender_pos, borrower_pos)
    h3_state = (STATE_PENDING if overlap is None else
                STATE_PASS if overlap <= MAX_CODE_OVERLAP else STATE_FAIL)
    hard.append(_hard(
        "H3", "持仓加权重叠 ≤ %.2f" % MAX_CODE_OVERLAP,
        h3_state,
        "持仓加权重叠 %.4f" % overlap if overlap is not None
        else "共同交易日无持仓快照，无法计算",
        {"weighted_overlap": overlap}))

    # H4 容量参与率（C 层）
    cap = capacity or {}
    lender_participation = _f(cap.get("lender_participation"))
    borrower_participation = _f(cap.get("borrower_participation"))
    same_code_participation = _f(cap.get("same_code_participation"))
    if None in (lender_participation, borrower_participation,
                same_code_participation):
        hard.append(_hard(
            "H4", "ADV 参与率各自 ≤ 1%、同票合计 ≤ 2%", STATE_PENDING,
            "容量/ADV 参与率数据由 C 层容量模型提供，当前未接线",
            {"pending_code": PENDING_C}))
    else:
        h4_ok = (lender_participation <= 0.01
                 and borrower_participation <= 0.01
                 and same_code_participation <= 0.02)
        hard.append(_hard(
            "H4", "ADV 参与率各自 ≤ 1%、同票合计 ≤ 2%",
            STATE_PASS if h4_ok else STATE_FAIL,
            "参与率 出借方 %.4f / 借款方 %.4f / 同票合计 %.4f" % (
                lender_participation, borrower_participation,
                same_code_participation),
            {"lender_participation": lender_participation,
             "borrower_participation": borrower_participation,
             "same_code_participation": same_code_participation}))

    # H5 现金需求同日重叠
    lender_buys = _buy_amount_by_day(db, lender_rec.id) if lender_rec else {}
    borrower_buys = _buy_amount_by_day(db, borrower_rec.id) if borrower_rec else {}

    def _demand(days, buys, snaps):
        out = {}
        values = {s.snap_date: _f(s.total_value) for s in snaps}
        for day in days:
            value = values.get(day) or 0
            if value > 0:
                out[day] = buys.get(day, 0.0) / value
        return out

    lender_demand = _demand(common_days, lender_buys, lender_snaps)
    borrower_demand = _demand(common_days, borrower_buys, borrower_snaps)
    lender_set = {d for d, r in lender_demand.items() if r > 0}
    borrower_set = {d for d, r in borrower_demand.items() if r > 0}
    cash_overlap = _jaccard(lender_set, borrower_set) if common_days else None
    h5_state = (STATE_PENDING if cash_overlap is None else
                STATE_PASS if cash_overlap <= MAX_CASH_TIMING_OVERLAP
                else STATE_FAIL)
    hard.append(_hard(
        "H5", "现金需求同日重叠 ≤ %.2f" % MAX_CASH_TIMING_OVERLAP, h5_state,
        "现金需求同日重叠 %.4f" % cash_overlap if cash_overlap is not None
        else "无共同交易日，无法计算",
        {"cash_timing_overlap": cash_overlap,
         "lender_demand_days": len(lender_set),
         "borrower_demand_days": len(borrower_set)}))

    # H6 容量曲线边际收益（C 层）
    marginal_1x = _f(cap.get("marginal_1x"))
    marginal_2x = _f(cap.get("marginal_2x"))
    if marginal_1x is None or marginal_2x is None:
        hard.append(_hard(
            "H6", "目标规模 1×/2× 下容量曲线边际收益 > 0", STATE_PENDING,
            "容量曲线由 C 层容量模型与多规模回测提供，当前未接线",
            {"pending_code": PENDING_C}))
    else:
        h6_ok = marginal_1x > 0 and marginal_2x > 0
        hard.append(_hard(
            "H6", "目标规模 1×/2× 下容量曲线边际收益 > 0",
            STATE_PASS if h6_ok else STATE_FAIL,
            "边际收益 1× %.4f / 2× %.4f" % (marginal_1x, marginal_2x),
            {"marginal_1x": marginal_1x, "marginal_2x": marginal_2x}))

    # H7 各自最大回撤 ≤ 上限
    lender_dd = _max_drawdown(lender_snaps)
    borrower_dd = _max_drawdown(borrower_snaps)
    lender_limit, lender_defaulted = _dd_limit(lender_ps)
    borrower_limit, borrower_defaulted = _dd_limit(borrower_ps)
    h7_ok = (lender_snaps and borrower_snaps
             and lender_dd <= lender_limit and borrower_dd <= borrower_limit)
    hard.append(_hard(
        "H7", "各自最大回撤不超过上限", STATE_PASS if h7_ok else STATE_FAIL,
        "出借方回撤 %.2f%%/上限 %.2f%%，借款方 %.2f%%/上限 %.2f%%" % (
            lender_dd * 100, lender_limit * 100,
            borrower_dd * 100, borrower_limit * 100),
        {"lender_max_drawdown": lender_dd,
         "borrower_max_drawdown": borrower_dd,
         "lender_limit": lender_limit, "borrower_limit": borrower_limit,
         "limit_defaulted": lender_defaulted or borrower_defaulted}))

    # H8 组合级预估最大回撤（等权共同日收益）
    combined_dd = None
    if common_days:
        combined_value = 1.0
        peak = 1.0
        worst = 0.0
        for day in common_days:
            combined_value *= (1.0 + (lender_days[day] + borrower_days[day]) / 2.0)
            peak = max(peak, combined_value)
            worst = min(worst, combined_value / peak - 1.0)
        combined_dd = abs(worst)
    combined_limit = min(lender_limit, borrower_limit)
    h8_ok = (combined_dd is not None
             and combined_dd <= combined_limit
             and combined_dd <= min(lender_dd, borrower_dd) + COMBINED_DD_EXTRA)
    hard.append(_hard(
        "H8", "组合预估回撤 ≤ 上限且不高于较优者 +1pp",
        STATE_PENDING if combined_dd is None else
        STATE_PASS if h8_ok else STATE_FAIL,
        "组合预估回撤 %.2f%%（上限 %.2f%%）" % (
            combined_dd * 100, combined_limit * 100) if combined_dd is not None
        else "无共同交易日，无法估算组合回撤",
        {"combined_max_drawdown": combined_dd,
         "combined_limit": combined_limit,
         "better_side_drawdown": min(lender_dd, borrower_dd)}))

    # --- 软判据 -----------------------------------------------------------
    soft: list = []
    xs = [lender_days[d] for d in common_days]
    ys = [borrower_days[d] for d in common_days]
    corr = _spearman(xs, ys)
    soft.append(_soft(
        "return_corr", _grade(corr, CORR_PASS, CORR_FAIL), corr,
        "样本外日收益 Spearman 相关 %.4f" % corr if corr is not None
        else "共同样本不足，无法计算相关性"))

    lender_dd_series = _drawdown_series(lender_snaps)
    borrower_dd_series = _drawdown_series(borrower_snaps)
    lender_deep = {d for d, v in lender_dd_series.items() if v <= -DRAWDOWN_DEPTH}
    borrower_deep = {d for d, v in borrower_dd_series.items() if v <= -DRAWDOWN_DEPTH}
    dd_jaccard = _jaccard(lender_deep, borrower_deep) if (
        lender_dd_series and borrower_dd_series) else None
    soft.append(_soft(
        "drawdown_overlap", _grade(dd_jaccard, DD_OVERLAP_PASS, DD_OVERLAP_FAIL),
        dd_jaccard,
        "回撤时序重叠 %.4f" % dd_jaccard if dd_jaccard is not None
        else "缺少回撤序列"))

    dd_ratio = None
    if lender_snaps and borrower_snaps:
        dd_ratio = max(lender_dd / lender_limit if lender_limit else 0,
                       borrower_dd / borrower_limit if borrower_limit else 0)
    soft.append(_soft(
        "own_max_drawdown",
        STATE_PENDING if dd_ratio is None else
        STATE_PASS if dd_ratio <= DD_LIMIT_WARN else
        STATE_FAIL if dd_ratio > 1.0 else STATE_WARN,
        dd_ratio,
        "最大回撤占上限 %.2f%%" % (dd_ratio * 100) if dd_ratio is not None
        else "缺少快照"))

    lender_dur = _percentile(_drawdown_durations(lender_snaps), 0.9)
    borrower_dur = _percentile(_drawdown_durations(borrower_snaps), 0.9)
    dd_duration = max(filter(None, [lender_dur, borrower_dur]), default=None)
    soft.append(_soft(
        "drawdown_duration_p90",
        STATE_PENDING if dd_duration is None else
        STATE_PASS if dd_duration <= DD_DURATION_PASS else
        STATE_FAIL if dd_duration > DD_DURATION_FAIL else STATE_WARN,
        dd_duration,
        "回撤持续期 p90 %s 日" % dd_duration if dd_duration is not None
        else "无完整回撤片段"))

    lender_hold = _median_holding_days(db, lender_rec.id) if lender_rec else None
    borrower_hold = _median_holding_days(db, borrower_rec.id) if borrower_rec else None
    hold_ratio = None
    if lender_hold and borrower_hold and lender_hold > 0 and borrower_hold > 0:
        hold_ratio = max(lender_hold, borrower_hold) / min(lender_hold, borrower_hold)
    soft.append(_soft(
        "holding_period",
        STATE_PENDING if hold_ratio is None else
        STATE_PASS if hold_ratio >= HOLDING_RATIO_PASS else
        STATE_FAIL if hold_ratio < HOLDING_RATIO_WARN else STATE_WARN,
        hold_ratio,
        "持有周期差异 %.2f 倍" % hold_ratio if hold_ratio is not None
        else "成交样本不足以计算持有周期"))

    lender_turnover = _annual_turnover(db, lender_rec.id) if lender_rec else None
    borrower_turnover = _annual_turnover(db, borrower_rec.id) if borrower_rec else None
    turnover = max(filter(None, [lender_turnover, borrower_turnover]), default=None)
    soft.append(_soft(
        "annual_turnover",
        STATE_PENDING if turnover is None else
        STATE_PASS if turnover <= TURNOVER_PASS else
        STATE_FAIL if turnover > TURNOVER_FAIL else STATE_WARN,
        turnover,
        "年换手率（较大者）%s" % turnover if turnover is not None
        else "缺少组合评估的换手数据"))

    # 容量占用重叠
    cap_overlap = _f(cap.get("overlap"))
    soft.append(_soft(
        "capacity_overlap",
        _grade(cap_overlap, 0.10, 0.25), cap_overlap,
        "容量占用重叠 %.4f" % cap_overlap if cap_overlap is not None
        else "容量占用重叠由 C 层容量模型提供，当前未接线",
        pending_code=None if cap_overlap is not None else PENDING_C))
    # 门控关闭重合（D1）：由日快照 gate_state（已右移的当日生效值）构轴，
    # 只在双方都受门控时才参与判定；一方未受门控 → 关闭期天然互补。
    lender_gate = build_gate_timeline(
        (snap.snap_date, snap.gate_state) for snap in lender_snaps)
    borrower_gate = build_gate_timeline(
        (snap.snap_date, snap.gate_state) for snap in borrower_snaps)
    lender_gated, borrower_gated = _gate_flag(lender_rec), _gate_flag(borrower_rec)
    gate_metric = (gate_off_overlap(lender_gate, borrower_gate)
                   if lender_gate is not None and borrower_gate is not None
                   else None)
    gate_detail = {
        "lender_gated": lender_gated,
        "borrower_gated": borrower_gated,
        "lender": lender_gate.to_dict() if lender_gate is not None else None,
        "borrower": borrower_gate.to_dict() if borrower_gate is not None else None,
    }
    if gate_metric is not None:
        gate_detail.update(gate_metric)
    if lender_gated is False or borrower_gated is False:
        gate_value, gate_state, gate_pending = None, STATE_PASS, None
        gate_message = "一方未启用门控，关闭期不影响其开仓（关闭重合不适用）"
        gate_detail["applicable"] = False
    elif lender_gated is None or borrower_gated is None or gate_metric is None:
        gate_value, gate_state, gate_pending = None, STATE_PENDING, PENDING_D
        gate_message = ("回测记录未冻结门控开关或日快照缺少 gate_state，"
                        "无法生成门控时间轴")
        gate_detail["applicable"] = None
    else:
        gate_value = gate_metric["overlap"]
        gate_state = _grade(gate_value, GATE_OVERLAP_PASS, GATE_OVERLAP_FAIL)
        gate_pending = None
        gate_message = ("门控关闭重合 %.4f（同日关闭 %d / 任一方关闭 %d）"
                        % (gate_value, gate_metric["both_closed"],
                           gate_metric["either_closed"]))
        gate_detail["applicable"] = True
    soft.append(_soft(
        "gate_off_overlap", gate_state, gate_value, gate_message,
        pending_code=gate_pending, detail=gate_detail))

    lender_signals = _open_signal_pairs(db, lender_rec.id) if lender_rec else set()
    borrower_signals = _open_signal_pairs(db, borrower_rec.id) if borrower_rec else set()
    signal_overlap = _jaccard(lender_signals, borrower_signals) if (
        lender_signals or borrower_signals) else None
    soft.append(_soft(
        "same_code_signal_overlap",
        _grade(signal_overlap, SIGNAL_OVERLAP_PASS, SIGNAL_OVERLAP_FAIL),
        signal_overlap,
        "同日同票 OPEN 信号重叠 %.4f" % signal_overlap
        if signal_overlap is not None else "无 OPEN 信号"))

    bid_probability = _f(cap.get("bid_probability"))
    soft.append(_soft(
        "same_code_bid_probability",
        _grade(bid_probability, 0.50, 0.80), bid_probability,
        "同票抢单概率 p90 %.4f" % bid_probability if bid_probability is not None
        else "同票抢单概率需要 ADV 与容量，当前未接线",
        pending_code=None if bid_probability is not None else PENDING_C))
    curve_decay = _f(cap.get("curve_decay_2x"))
    soft.append(_soft(
        "capacity_curve_slope",
        STATE_PENDING if curve_decay is None else
        STATE_PASS if curve_decay <= 0.20 else
        STATE_FAIL if curve_decay > 0.40 or curve_decay < 0 else STATE_WARN,
        curve_decay,
        "2× 规模年化收益衰减 %.2f%%" % (curve_decay * 100)
        if curve_decay is not None
        else "容量曲线斜率需要 C 层多规模回测，当前未接线",
        pending_code=None if curve_decay is not None else PENDING_C))

    complement_days = 0
    if common_days:
        for day in common_days:
            a, b = lender_demand.get(day, 0.0), borrower_demand.get(day, 0.0)
            if (a > 0.05 and b < 0.01) or (b > 0.05 and a < 0.01):
                complement_days += 1
        cash_complement = complement_days / len(common_days)
    else:
        cash_complement = None
    soft.append(_soft(
        "cash_use_complement",
        STATE_PENDING if cash_complement is None else
        STATE_PASS if cash_complement >= CASH_COMPLEMENT_PASS else
        STATE_FAIL if cash_complement < CASH_COMPLEMENT_FAIL else STATE_WARN,
        cash_complement,
        "资金使用互补日占比 %.4f" % cash_complement
        if cash_complement is not None else "无共同交易日"))

    for feature, message in (
        ("combined_sharpe", "组合 Sharpe 改善需要联合回测"),
        ("combined_calmar", "组合 Calmar 改善需要联合回测"),
        ("combined_return", "组合收益对比需要联合回测"),
        ("utilization_gain", "利用率提升需要资金池重放/联合回测"),
        ("recall_frequency", "预期召回频率需要资金池重放"),
        ("recall_shortfall", "预期召回 shortfall 需要资金池重放"),
    ):
        soft.append(_soft(feature, STATE_PENDING, None, message,
                          pending_code=PENDING_BACKTEST))

    status, triggers = _decide(hard, soft)
    hard_failed = sum(1 for h in hard if h["state"] == STATE_FAIL)
    hard_pending = sum(1 for h in hard if h["state"] == STATE_PENDING)
    soft_fail = sum(1 for s in soft if s["state"] == STATE_FAIL)
    soft_warn = sum(1 for s in soft if s["state"] == STATE_WARN)
    soft_pending = sum(1 for s in soft if s["state"] == STATE_PENDING)

    return {
        "version": PAIRING_EVAL_VERSION,
        "scope": scope,
        "decided_at": now.isoformat(sep=" "),
        "status": status,
        "triggers": triggers,
        "lender": {
            "portfolio_strategy_id": lender_portfolio_strategy_id,
            "record_id": lender_rec.id if lender_rec else None,
            "eligibility_status": lender_elig["status"],
        },
        "borrower": {
            "portfolio_strategy_id": borrower_portfolio_strategy_id,
            "record_id": borrower_rec.id if borrower_rec else None,
            "eligibility_status": borrower_elig["status"],
        },
        "window": {"start": window[0].isoformat(), "end": window[1].isoformat()}
        if window else None,
        "common_days": len(common_days),
        "hard": hard,
        "soft": soft,
        "counts": {
            "hard_failed": hard_failed, "hard_pending": hard_pending,
            "soft_fail": soft_fail, "soft_warn": soft_warn,
            "soft_pending": soft_pending,
        },
        "metrics": {
            "lender_max_drawdown": lender_dd,
            "borrower_max_drawdown": borrower_dd,
            "combined_max_drawdown": combined_dd,
            "weighted_code_overlap": overlap,
            "cash_timing_overlap": cash_overlap,
            "return_corr": corr,
            "gate_off_overlap": gate_value,
        },
        "gate": gate_detail,
    }
