from typing import Callable, Dict, List, Optional
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from dataclasses import replace
import json
import logging

from .portfolio import Portfolio
from .position import Position, is_convertible_bond, lot_size_for
from .execution_engine import ExecutionEngine, SimulatedDispatcher, SimulatedT1Checker
from .cash_allocator import MIN_FILL_RATIO, allocate_batch_buys
from .capacity import CapacityBudget
from .session_pool import PoolMember, allocate_session_pool
from .cash_outlet_model import (
    CASH_OUTLET_MODEL_VERSION,
    CashOutletPlanError,
    build_cash_yield_contribution,
)
from .format import fmt_money
from .evaluator import Evaluator
from .event import BarEvent, OrderEvent, TradeEvent
from .decision import DecisionRecorder
from .corporate_actions import (
    apply_day as apply_corporate_day,
    KIND_DELIST_CLOSE,
    KIND_REINVEST,
    KIND_SKIP_CASH,
    KIND_SPLIT,
    KIND_SUSPEND,
)
# 涨跌停幅度口径迁至 market_rules（实盘 TdxTqDispatcher 涨跌停前置校验共用）；
# 此处 re-export 保持既有 import 路径不破。回测改用 limit_rule_for：与实盘
# price_limit.resolve_limit_rule 同源（ST 5% / 创业板 20% / 北交所 30%）。
from .market_rules import (  # noqa: F401
    _limit_ratio_for,
    limit_rule_for,
    resolve_limit_rule,
    session_allows,
    session_allows_bar,
    trading_days_between,
)
from .price_limit import check_bar_consistent_with_prev_close
from .capital_contracts import (
    CAPITAL_POOL_VERSION,
    PortfolioQuotaInput,
    StrategyQuotaInput,
    dec_str,
    to_decimal,
)
from .capital_loan import LoanBook, Owner, REASON_MANUAL, SCOPE_PORTFOLIO
from .capital_gate_release import GateReleaseBook, resolve_gate_status
from .capital_quota import derive_portfolio_quota
from tq_iquant_shared.constants import SignalType, TradeType

logger = logging.getLogger(__name__)

# 资金池 L2 借据建模版本（0021 B3）：随快照写入，便于回测/实盘对照。
LOAN_MODEL_VERSION = "loan-model-b3"
# 会话池动态额度（0021 E2）：仅 run_multi 在显式 plan 下启用，默认 None。
CAPITAL_POOL_MULTI_VERSION = "capital-pool-e2"


# W4 按周期路由：同一时间戳可能同时存在多个周期（1d/1w/1mon 均为 00:00）。
# 固定处理顺序（细 → 粗）：1m > 5m > 15m > 30m > 1h > 1d > 1w > 1mon。
# 与实盘 `core.engine.live.timing.periods_on_boundary` 的派发顺序同序
# （实盘同一 1m bar 到边界时按 5m→15m→30m→1h 依次派发），保证同时间戳
# 多周期同时到边界时回测/实盘先手一致（现金/槽位竞争口径对齐）。
# 每步只处理与当前周期相同的策略，价格索引按 (datetime, period) 键，杜绝跨周期
# 前视（日线 00:00 bar 驱动分钟策略）与同时间戳 OHLC 互相覆盖。
# "60m" 为历史别名（旧参数快照/旧数据可能仍带该串），与 "1h" 同权重。
_PERIOD_ORDER: Dict[str, int] = {
    "1m": 0, "5m": 1, "15m": 2, "30m": 3, "1h": 4, "60m": 4,
    "1d": 5, "1w": 6, "1mon": 7,
}
# 日线及以上周期（D8）：其 bar 的 close 是「周期末价」，在时间轴上却出现在周期
# 起点（00:00）——同一交易日里对分钟 bar 而言是未来价。混合周期回测若拿它抬净值
# 峰值会造成前视（max_drawdown 提前/错误触发）。故这类 bar 只在「日终估值步」
# 才允许抬峰（纯日线/周线/月线回测里该步就是日终步，行为不变）。
_COARSE_PERIODS = ("1d", "1w", "1mon")
# 风控卖出信号类型（回测侧在途卖单去重键与实盘 inflight_sell 同口径）：
# 同一 (策略, 代码, SELL) 存在未结清的风控卖单（含缩量未成交残量）时，
# 不重复产生 trigger 事件与订单，残量由原单继续分批成交。
_RISK_SELL_TYPES = frozenset({
    SignalType.STOP_LOSS, SignalType.TAKE_PROFIT, SignalType.TRAILING_STOP,
})

# 在途单最大存活期（交易日，2026-11-16 核查修复）：停牌/连板时 deferred 单会被
# 无上限地顺延，几个月后恢复交易仍按当日 open 成交——等于凭空补做一笔早已过期的
# 决策，且与实盘不一致（实盘由超时撤单/次日重排兜底，不会挂几个月）。
# 默认 20 个交易日：约一个月，覆盖停牌复牌、连续涨跌停的常见持续期，又不至于把
# 长期停牌票的过期信号永久留在队列。<=0 = 不限制（恢复旧行为）。
PENDING_ORDER_MAX_DAYS = 20


def _ca_mode(portfolio) -> str:
    """公司行为记账口径：J4 exact 档为未复权价空间（backtest_raw），其余沿用
    backtest（复权价 + 比例分红）。"""
    return getattr(portfolio, "corporate_action_mode", None) or "backtest"


def _loan_quota_input(portfolio, equity: Decimal) -> PortfolioQuotaInput:
    """由内存组合装配一次推导输入（只读、无 IO；口径与 A2 一致）。"""
    items = []
    for ctx in portfolio.strategies:
        items.append(StrategyQuotaInput(
            strategy_id=ctx.strategy_id,
            capital_ratio=getattr(ctx, "capital_ratio", None),
            max_positions=getattr(ctx, "max_positions", None),
            single_open_ratio=getattr(ctx, "single_open_ratio", None),
            max_order_amount=getattr(ctx, "max_order_amount", None),
            max_order_pct_equity=getattr(ctx, "max_order_pct_equity", None),
        ))
    return PortfolioQuotaInput(
        portfolio_id=getattr(portfolio, "portfolio_id", None),
        equity=equity,
        equity_cap=getattr(portfolio, "equity_cap", None),
        target_utilization=getattr(portfolio, "target_utilization", None),
        max_holdings=getattr(portfolio, "max_holdings", None),
        max_single_position_pct=getattr(portfolio, "max_single_position_pct", None),
        strategies=items,
    )


def apply_loan_model(portfolio, plan, recorder, at: datetime) -> Optional[dict]:
    """单组合回测内的 L2 借据建模（0021 B3，默认关闭）。

    ``plan`` 为 None / ``enabled=False`` → 返回 None，交易链路零改动。
    仅在组合**同时包含**出借方与借款方两个策略时成立（L2 口径；跨组合留待 E）。
    借方额度与贷方额度均来自 A2 统一推导，注入 B2 的 ``capital_budget``：
    贷方额度 = 自身 Entitlement − 借出本金，借方额度 = 自身 Entitlement + 借入本金。
    权益取 run 起点的当前持仓/现金（不含未来数据），本金由配置给出（无前视）。
    """
    if not plan or plan.get("enabled") is False:
        return None

    lender_id = plan.get("lender_strategy_id")
    borrower_id = plan.get("borrower_strategy_id")
    lender = portfolio._find_strategy_by_id(lender_id)
    borrower = portfolio._find_strategy_by_id(borrower_id)
    if lender is None or borrower is None:
        return {
            "status": "rejected",
            "reason": "strategy_not_in_portfolio",
            "message": "L2 借据要求出借方与借款方同属该组合（跨组合见 E 阶段）",
            "lender_strategy_id": lender_id,
            "borrower_strategy_id": borrower_id,
        }
    if lender is borrower:
        return {
            "status": "rejected", "reason": "self_lending",
            "message": "出借方与借款方不能是同一策略",
        }

    requested = to_decimal(plan.get("principal", plan.get("amount")))
    if requested is None or requested <= 0:
        return {
            "status": "rejected", "reason": "invalid_principal",
            "message": "本金缺失或非正，拒绝建模（不落默认值）",
        }

    equity = portfolio.current_value()
    quota = derive_portfolio_quota(_loan_quota_input(portfolio, equity), enabled=True)
    by_id = {s.strategy_id: s for s in quota.strategies}
    lq = by_id.get(lender_id)
    bq = by_id.get(borrower_id)
    if lq is None or bq is None or lq.entitlement is None or bq.entitlement is None:
        return {
            "status": "rejected", "reason": "quota_not_derivable",
            "message": "双方额度无法推导（见 quota.reasons），不建模借贷",
            "quota": quota.to_dict(),
        }

    # 出借方自有需求优先：只有「额度 − 当前持仓市值」可出借，本金超出部分夹取。
    lendable = lq.entitlement - portfolio._holdings_value(lender)
    if lendable <= 0:
        return {
            "status": "rejected", "reason": "lender_entitlement_exhausted",
            "message": "出借方可用额度不足以出借（额度 %s，持仓市值 %s）" % (
                dec_str(lq.entitlement),
                dec_str(portfolio._holdings_value(lender))),
        }
    amount = min(requested, lendable)
    clamped = amount < requested

    book = LoanBook()
    lender_owner = Owner("strategy", lender_id)
    borrower_owner = Owner("strategy", borrower_id)
    book.deposit_lendable(lender_owner, lendable)
    book.grant_credit(borrower_owner, amount)
    try:
        due_days = int(plan.get("due_days") or 365)
    except (TypeError, ValueError):
        due_days = 365
    lot = book.lend(
        lender=lender_owner,
        borrower=borrower_owner,
        principal=amount,
        scope=SCOPE_PORTFOLIO,
        created_at=at,
        due_at=at + timedelta(days=due_days),
        reason=REASON_MANUAL,
    )

    lender_budget = lq.entitlement - amount
    borrower_budget = bq.entitlement + amount
    # 已有显式额度时取更保守的一侧（不放大外部注入的上限）。
    if getattr(lender, "capital_budget", None) is not None:
        lender_budget = min(lender_budget, Decimal(lender.capital_budget))
    if getattr(borrower, "capital_budget", None) is not None:
        borrower_budget = min(borrower_budget, Decimal(borrower.capital_budget))
    lender.capital_budget = lender_budget
    borrower.capital_budget = borrower_budget

    for ctx, budget, entitlement, role in (
        (lender, lender_budget, lq.entitlement, "lender"),
        (borrower, borrower_budget, bq.entitlement, "borrower"),
    ):
        recorder.record(
            gate="capital_loan_budget", layer="capital_gate", action="trigger",
            portfolio_id=getattr(portfolio, "portfolio_id", None),
            strategy_id=ctx.strategy_id,
            bar_time=at,
            param_name="capital_budget", param_value=float(budget),
            actual_value=float(entitlement),
            message="L2 借据（%s）：本金 %s，额度 %s → %s" % (
                role, dec_str(amount), dec_str(entitlement), dec_str(budget)),
        )

    state = {
        "status": "applied",
        "loan_model_version": LOAN_MODEL_VERSION,
        "capital_pool_version": CAPITAL_POOL_VERSION,
        "lender_strategy_id": lender_id,
        "borrower_strategy_id": borrower_id,
        "principal": dec_str(amount),
        "requested_principal": dec_str(requested),
        "clamped": clamped,
        "entitlements": {
            str(lender_id): dec_str(lq.entitlement),
            str(borrower_id): dec_str(bq.entitlement),
        },
        "budgets": {
            str(lender_id): dec_str(lender_budget),
            str(borrower_id): dec_str(borrower_budget),
        },
        "loan": lot.to_dict(),
        "loan_book": book.snapshot(),
        "violations": book.check_invariants(),
    }
    # D3：把账本对象交给 run() 的释放/召回链路；不进入结果序列化（run 会弹出）。
    state["_ledger"] = book
    if clamped:
        recorder.record(
            gate="capital_loan_clamped", layer="capital_gate", action="clamp",
            portfolio_id=getattr(portfolio, "portfolio_id", None),
            bar_time=at,
            param_name="principal", param_value=float(requested),
            actual_value=float(amount),
            message="本金 %s 超出出借方可用额度，夹取为 %s"
                    % (dec_str(requested), dec_str(amount)),
        )
    return state


GATE_RELEASE_MODEL_VERSION = "gate-release-d3"


class _GateReleaseRunner:
    """D3：把 D2 状态机接到单组合回测（默认不构造，plan=None 时零改动）。

    只调整 B2 ``capital_budget`` 上限与借据账本，不搬运现金、不强制卖出：
    门控关闭确认后把「额度 − 持仓市值 − 已借出」的闲置额在 T+2 释放为对
    借款方的新借据（借款方上限提高、出借方上限等额下降）；恢复确认后立即
    召回，上限回到 B3 起点（已持仓不受影响，只禁新增）。到期借据同样发起
    召回并收回上限。所有输入只用当日及之前的门控状态与持仓，无前视。
    """

    def __init__(self, portfolio, loan_state, ledger, plan, gate_tracker,
                 trading_days, recorder) -> None:
        self.portfolio = portfolio
        self.plan = plan
        self.tracker = gate_tracker
        self.recorder = recorder
        self.book: LoanBook = ledger
        self.lender_id = loan_state["lender_strategy_id"]
        self.borrower_id = loan_state["borrower_strategy_id"]
        self.lender = portfolio._find_strategy_by_id(self.lender_id)
        self.borrower = portfolio._find_strategy_by_id(self.borrower_id)
        self.lender_owner = Owner("strategy", self.lender_id)
        self.borrower_owner = Owner("strategy", self.borrower_id)
        self.base_lender_budget = Decimal(
            loan_state["budgets"][str(self.lender_id)])
        self.base_borrower_budget = Decimal(
            loan_state["budgets"][str(self.borrower_id)])
        self.entitlement = Decimal(
            loan_state["entitlements"][str(self.lender_id)])
        self.mode = (plan.get("lender_gate_mode")
                     or getattr(self.lender, "market_gate_mode", None))
        self.portfolio_gate_enabled = bool(plan.get(
            "portfolio_gate_enabled",
            getattr(portfolio, "market_gate_enabled", False)))
        try:
            self.due_days = int(plan.get("due_days") or 365)
        except (TypeError, ValueError):
            self.due_days = 365
        self.machine = GateReleaseBook(
            trading_days,
            release_confirm_days=int(plan.get("release_confirm_days") or 1),
            release_delay_days=int(plan.get("release_delay_days", 1)),
            reopen_confirm_days=int(plan.get("reopen_confirm_days") or 1),
            recall_notice_days=int(plan.get("recall_notice_days") or 1),
        )
        self.extra_released = Decimal(0)
        self.events: List[dict] = []
        self._by_day: Dict[date, dict] = {}

    # ---- 每日推进 -------------------------------------------------------
    def on_day(self, day: date, at: datetime) -> None:
        gate = resolve_gate_status(
            self.mode, self.portfolio_gate_enabled,
            self.tracker.state_for(day) if self.tracker is not None else None)
        idle = (self.entitlement - self.portfolio._holdings_value(self.lender)
                - self.book.outstanding_for_lender(self.lender_owner))
        if idle < 0:
            idle = Decimal(0)
        event = self.machine.observe(
            self.lender_owner, day, gate, idle_amount=idle,
            loan_book=self.book, now=at, scope=SCOPE_PORTFOLIO)

        released = Decimal(event.get("release_amount") or 0)
        if released > 0:
            self._lend_released(released, at, event)
        if event.get("recalled"):
            self._restore_budgets()
        self._recall_due(day, at, event)
        self._record(event, at)

        state = self.machine.state(self.lender_owner)
        self._by_day[day] = {
            "gate": gate,
            "status": state.status,
            "pending_release": dec_str(state.pending_amount),
            "reopen_reserve": dec_str(state.reopen_reserve),
        }
        self._by_day[day]["budgets"] = {
            str(self.lender_id): dec_str(self.lender.capital_budget),
            str(self.borrower_id): dec_str(self.borrower.capital_budget),
        }

    def day_summary(self, day: date) -> Optional[dict]:
        return self._by_day.get(day)

    # ---- 明细 -----------------------------------------------------------
    def _lend_released(self, amount: Decimal, at: datetime,
                       event: dict) -> None:
        released = self.book.credit(self.borrower_owner) + amount
        self.book.grant_credit(self.borrower_owner, released)
        self.book.lend(
            lender=self.lender_owner, borrower=self.borrower_owner,
            principal=amount, scope=SCOPE_PORTFOLIO, created_at=at,
            due_at=at + timedelta(days=self.due_days),
            reason="gate_closed")
        self.extra_released += amount
        self.lender.capital_budget = max(
            Decimal(0), self.base_lender_budget - self.extra_released)
        self.borrower.capital_budget = (
            self.base_borrower_budget + self.extra_released)
        event["budgets"] = {
            str(self.lender_id): dec_str(self.lender.capital_budget),
            str(self.borrower_id): dec_str(self.borrower.capital_budget),
        }

    def _restore_budgets(self) -> None:
        self.extra_released = Decimal(0)
        self.lender.capital_budget = self.base_lender_budget
        self.borrower.capital_budget = self.base_borrower_budget

    def _recall_due(self, day: date, at: datetime, event: dict) -> None:
        overdue = self.book.overdue_lots(at)
        if not overdue:
            return
        deadline = at + timedelta(days=1)
        for lot in overdue:
            try:
                self.book.request_recall(lot.loan_id, at=at, deadline=deadline)
            except Exception:  # noqa: BLE001 - 借据状态机拒绝即跳过
                continue
        self._restore_budgets()
        event.setdefault("due_recalled", [x.loan_id for x in overdue])
        self.recorder.record(
            gate="capital_loan_due_recall", layer="capital_gate",
            action="recall", portfolio_id=getattr(self.portfolio, "portfolio_id", None),
            strategy_id=self.lender_id, bar_time=at,
            param_name="due_days", param_value=float(self.due_days),
            message="到期借据 %d 笔发起召回，额度上限回到 B3 起点"
                    % len(overdue),
        )

    def _record(self, event: dict, at: datetime) -> None:
        self.events.append(dict(event))
        if not event["reasons"] or event["reasons"] == ["no_change"]:
            return
        self.recorder.record(
            gate="capital_gate_release", layer="capital_gate",
            action=event["status"], portfolio_id=getattr(
                self.portfolio, "portfolio_id", None),
            strategy_id=self.lender_id, bar_time=at,
            param_name="gate", param_value=float(
                {"CLOSED": -1, "OPEN": 1}.get(event["gate"], 0)),
            message=("门控释放/召回：%s（%s）"
                     % (event["status"], ",".join(event["reasons"])))[:200],
        )

    def result(self) -> dict:
        return {
            "version": GATE_RELEASE_MODEL_VERSION,
            "lender_strategy_id": self.lender_id,
            "borrower_strategy_id": self.borrower_id,
            "events": self.events,
            "states": self.machine.snapshot(),
            "final_budgets": {
                str(self.lender_id): dec_str(self.lender.capital_budget),
                str(self.borrower_id): dec_str(self.borrower.capital_budget),
            },
            "loan_book": self.book.snapshot(),
        }


def build_prev_close_by_day(klines: dict) -> Dict[tuple, Decimal]:
    """按 ``(code, 交易日)`` 预计算「上一交易日最后一根 bar 的收盘」（任意周期）。

    依据（2026-11-16 P1 核查修复）：涨跌停价 = 前收 ×(1±幅度)，前收必须是**上一
    交易日收盘**且日内恒定。旧实现键为 ``(code, period)`` 并在每根 bar 收盘后覆写：
    5m/15m/30m/1h 策略当日第二根起「前收」变成上一根同周期收盘 → 涨停价≈1.1×
    5 分钟前收盘，封板买、跌停卖都能成交。

    口径：
      - 每个交易日取「该日最晚时间戳的收盘」（细→粗遍历，同时间戳取先遇到者）——
        日线 00:00 的周期末价与分钟 bar 同日时以分钟 bar 为准；纯周/月线没有分钟
        bar，取该周期末价即真实收盘。
      - 停牌日无 bar → 自然回落到更早的「上一有效交易日」收盘（与本条修复
        「停牌不成交」口径一致）。
    """
    day_closes: Dict[str, Dict[date, tuple]] = {}
    for _code, _periods in klines.items():
        per_day: Dict[date, tuple] = {}
        for _period in sorted((_periods or {}).keys(),
                              key=lambda p: _PERIOD_ORDER.get(p, 99)):
            _df = _periods.get(_period)
            if _df is None or "datetime" not in _df.columns:
                continue
            for _dt, _close in zip(_df["datetime"].to_list(), _df["Close"].to_list()):
                if _dt is None or _close is None or _close <= 0:
                    continue
                _d = _dt.date()
                _prev = per_day.get(_d)
                if _prev is None or _dt > _prev[0]:
                    per_day[_d] = (_dt, _close)
        if per_day:
            day_closes[_code] = per_day

    result: Dict[tuple, Decimal] = {}
    for _code, per_day in day_closes.items():
        _days = sorted(per_day.keys())
        for _i in range(1, len(_days)):
            result[(_code, _days[_i])] = per_day[_days[_i - 1]][1]
    return result


def build_volume_ref_by_day(klines: dict, lookback: int = 5) -> Dict[tuple, int]:
    """按 ``(code, 交易日)`` 预计算「此前 lookback 个有效交易日日均成交量（股）」。

    2026-11-19 口径统一（与实盘 ``LiveEngine._daily_reference_volume`` 对齐）：
      - 只用**严格早于该交易日**的完整日成交量，当日 bar 量绝不进入分母
        （旧实现直接用成交当根 bar 的 Volume，含当日未来信息）；
      - 优先日线；无日线时取最细分钟周期按日累加（避免多周期重复计数）；
        纯周/月线无法还原日量 → 不给键，调用方 fail-open；
      - 不足 lookback 日按已有天数均值；无历史（首日/新股）→ 不给键。
    """
    day_vols: Dict[str, Dict[date, float]] = {}
    intraday_periods = ("1m", "5m", "15m", "30m", "1h")
    for _code, _periods in (klines or {}).items():
        per_day: Dict[date, float] = {}
        _periods = _periods or {}
        if "1d" in _periods:
            _df = _periods.get("1d")
            if _df is not None and "datetime" in _df.columns and "Volume" in _df.columns:
                for _dt, _vol in zip(_df["datetime"].to_list(), _df["Volume"].to_list()):
                    if _dt is None or _vol is None or _vol <= 0:
                        continue
                    per_day[_dt.date()] = per_day.get(_dt.date(), 0.0) + float(_vol)
        else:
            for _p in sorted(_periods.keys(), key=lambda p: _PERIOD_ORDER.get(p, 99)):
                if _p not in intraday_periods:
                    continue
                _df = _periods.get(_p)
                if _df is None or "datetime" not in _df.columns or "Volume" not in _df.columns:
                    break
                for _dt, _vol in zip(_df["datetime"].to_list(), _df["Volume"].to_list()):
                    if _dt is None or _vol is None or _vol <= 0:
                        continue
                    per_day[_dt.date()] = per_day.get(_dt.date(), 0.0) + float(_vol)
                break
        if per_day:
            day_vols[_code] = per_day

    result: Dict[tuple, int] = {}
    for _code, per_day in day_vols.items():
        _days = sorted(per_day.keys())
        for _i, _d in enumerate(_days):
            _hist = [per_day[_x] for _x in _days[max(0, _i - lookback):_i]]
            if _hist:
                result[(_code, _d)] = int(sum(_hist) / len(_hist))
    return result


class BacktestEngine:
    def run(
        self,
        portfolio: Portfolio,
        klines: dict = None,
        signal_cache: dict = None,
        benchmark_data: object = None,
        on_progress: Callable[[int], None] = None,
        regime_tracker=None,
        gate_tracker=None,
        industry_tracker=None,
        daily_close_peak_enabled: bool = False,
        initial_pending_orders: Optional[List[OrderEvent]] = None,
        stock_names: Optional[dict] = None,
        prev_close_by_day: Optional[Dict[tuple, Decimal]] = None,
        pending_order_max_days: int = PENDING_ORDER_MAX_DAYS,
        run_options: Optional[dict] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        loan_model_plan: Optional[dict] = None,
        cash_outlet_plan: Optional[dict] = None,
        gate_release_plan: Optional[dict] = None,
    ) -> dict:
        """逐 bar 回测主链路。

        klines: {stock_code: {period: pl.DataFrame}}，DataFrame 含 datetime + Open/High/Low/Close/Volume 列。
        成交时机：上一 bar 触发的订单在下一 bar open 成交（pending_orders 队列），
        成交价取 price_index 中该 bar 的 Open（open_prices 死参数已移除，#12）。
        on_progress：进度回调（#10），0–100 递增整数，末值 100，节流至最多约 100 次。
        regime_tracker：B5 可选因果市场状态跟踪器（CausalRegimeTracker，默认 None
        不启用）；逐 bar 喂当日基准收盘并把标签注入 portfolio.current_regime。
        gate_tracker：市场多空门控跟踪器（MarketGateTracker，默认 None 不启用）；
        注入 portfolio.current_market_gate，每日快照追加 gate_state/gate_strength
        （None 时不写键）。
        daily_close_peak_enabled：可选保护（默认关）。开启后 peak/max_drawdown 只在
        当日最后一根 bar 更新；跨日 prev_close/daily_loss bookkeeping 仍每 bar 更新。
        纯 1d 行为不变。注意：关闭时**粗周期 bar（1d/1w/1mon）也不再抬峰**——它们
        只在日终估值步抬（见 ``_COARSE_PERIODS`` 注释，避免混合周期前视），分钟
        bar 仍每根抬峰（保留分钟回测原有口径）。
        initial_pending_orders：增量续跑用（默认 None=原行为）。上一段区间末遗留的
        在途单，拷贝进本次 pending 队列，在本段首根可成交 bar 上按原语义成交。
        stock_names：{stock_code: 证券名称}（默认 None=无名称）。涨跌停幅度与实盘
        同源（``price_limit.resolve_limit_rule``）：名称含 ST/*ST 的股票按 5%（主板）
        判定，缺名称时按板块常规幅度（偏宽）。回测无可靠上市日期，新股窗口不生效。
        prev_close_by_day：可选外部注入的 {(code, 交易日): 上一交易日最后一根收盘}。
        为 None 时由本段 klines 预计算；增量续跑传入「未切片」的全区间 klines
        结果，避免续跑首日缺前收（否则该日涨跌停校验整体 fail-open）。
        pending_order_max_days：在途单最大存活期（交易日，默认
        ``PENDING_ORDER_MAX_DAYS``=20，<=0 不限）。超期单在本 bar 记
        ``gate=pending_order_expired`` 决策事件后作废（不再回队），防停牌/连板
        无限顺延后在几个月后按恢复日开盘价「补做」早已过期的决策。依据见常量注释。
        run_options：执行档位展开后的开关（N1，2026-11-18，默认 None=旧行为）：
        ``first_bar_no_fill``（每交易日首根 bar 不生成 BUY）/
        ``partial_fill_enabled``（成交量上限缩量后残量回队续成交）/
        ``liquidity_caliber``（成交量闸门分母：默认/缺失=``same_day_bar``
        成交当根 bar 量，``prev5d_avg``=前 5 个交易日日均量，显式选择才生效）。
        cancel_check：K1 协作式取消（默认 None=不检查）。每根 bar 处理前调用，
        返回 True 即中止循环并返回 ``canceled=True`` + ``progress``，不产出部分结果。
        loan_model_plan：资金池 L2 借据建模（0021 B3，默认 None=不启用）。仅当组合
        同时包含出借/借款两策略时成立；额度来自 A2 推导并注入 B2 ``capital_budget``，
        快照写 ``loan_model_version``。关闭/无效时交易链路逐位不变。
        cash_outlet_plan：现金出口收益率建模（0021 C5，默认 None=不启用）。只按
        日终闲置现金估算利息与可用时点，结果单列在 ``cash_yield_contribution``，
        不改现金/净值/成交/策略指标；plan 非法时降级为 ``enabled=False``。
        gate_release_plan：门控释放/召回接入（0021 D3，默认 None=不启用）。要求
        ``loan_model_plan`` 已生成 L2 借据；逐日按当日生效 gate_state 推进 D2
        状态机，关闭确认后 T+2 把闲置额释放给借款方、恢复确认后召回并收回
        额度上限。只调整 ``capital_budget`` 上限与借据，不搬现金、不强制卖出；
        结果单列 ``gate_release``，快照追加 ``gate_release`` 当日摘要。
        """
        run_options = run_options or {}
        _first_bar_no_fill = bool(run_options.get("first_bar_no_fill", False))
        _partial_fill = bool(run_options.get("partial_fill_enabled", False))
        # 流动性口径（默认 same_day_bar=改动前行为，逐字不变）：只有显式选择
        # prev5d_avg 才启用「前 5 日日均量」分母并预计算索引。
        _liquidity_caliber = (
            run_options.get("liquidity_caliber") or "same_day_bar")
        klines = klines or {}
        signal_cache = signal_cache if signal_cache is not None else {}

        # 决策闸门采集（调参可观测性）：注入组合与执行引擎，run 结束随结果返回
        recorder = DecisionRecorder()
        portfolio.set_recorder(recorder)
        # B5：默认无市场状态（effective_* 全部回落原参数，行为不变）
        portfolio.current_regime = None
        # 市场多空门控：默认不启用（None → _gate_blocks 恒 False，行为不变）
        portfolio.current_market_gate = gate_tracker
        # 行业层决策数据（2026-11-22）：组合开关关闭时引擎传 None，行为逐位不变。
        portfolio.current_industry_gate = industry_tracker

        times = self._build_timeline(klines)
        total_bars = len(times)
        # 资金池 L2 借据（0021 B3，默认关闭）：权益取 run 起点首个 bar 时间作为
        # 起算时点（不用墙钟，回测可复现）；失败只落状态/事件，不改交易链路。
        loan_state = None
        gate_release_runner: Optional[_GateReleaseRunner] = None
        if times:
            loan_state = apply_loan_model(
                portfolio, loan_model_plan, recorder, times[0][0])
            ledger = None
            if loan_state is not None:
                # 账本对象不进入结果序列化（B3 既有结果结构保持不变）
                ledger = loan_state.pop("_ledger", None)
            if loan_state is not None and loan_state.get("status") != "applied":
                recorder.record(
                    gate="capital_loan_rejected", layer="capital_gate",
                    action="reject",
                    portfolio_id=getattr(portfolio, "portfolio_id", None),
                    bar_time=times[0][0],
                    message=(loan_state.get("message") or "")[:200],
                )
            elif (ledger is not None and gate_release_plan
                  and gate_release_plan.get("enabled") is not False):
                gate_release_runner = _GateReleaseRunner(
                    portfolio, loan_state, ledger, gate_release_plan,
                    gate_tracker,
                    sorted({t[0].date() for t in times}), recorder)
        report_every = max(1, total_bars // 100)
        price_index = self._build_price_index(klines)
        # P1-3：沪/北送转新增股落 ex_date+1 交易日桶所需的「下一交易日」查询。
        _trade_days = sorted({_t[0].date() for _t in times})

        def _next_trade_day(_d, _days=_trade_days):
            import bisect
            _i = bisect.bisect_right(_days, _d)
            return _days[_i] if _i < len(_days) else None
        # #2 成交量闸门分母（可选新口径）：成交日之前 5 个交易日日均量
        # （与实盘同口径，无前视）。默认口径不预计算，保持旧行为与耗时。
        volume_ref_by_day = (
            build_volume_ref_by_day(klines)
            if _liquidity_caliber == "prev5d_avg" else {})
        pending_orders: List[OrderEvent] = list(initial_pending_orders or [])
        # 续跑带入的在途单：checkpoint 序列化了 pending_since 时沿用（跨段累计
        # 交易日，寿命上限不被分段重置）；旧 checkpoint 无该字段 → 以本段首根
        # bar 的交易日为起点（保守：最多多给一个上限周期）。
        if pending_orders and times:
            _segment_first_date = times[0][0].date()
            for _o in pending_orders:
                if _o.pending_since is None:
                    _o.pending_since = _segment_first_date
        trades: List[TradeEvent] = []
        snapshots: List[dict] = []
        # #2：已记过 no_market_data 决策事件的订单 id（仅首次延后记 1 条，长停牌不刷屏）
        no_price_recorded: set = set()
        # #1 涨跌停约束：{(code, 交易日) → 上一交易日最后一根 bar 收盘(>0)}，日内恒定；
        # 由全部周期 klines 预计算（见 build_prev_close_by_day 的依据说明）。
        # 旧实现键含 period 且每根 bar 覆写，5m/15m/30m/1h 当日第二根起前收变成上
        # 一根同周期收盘 → 涨停价虚高、跌停价虚低，封板买/跌停卖都能成交。
        if prev_close_by_day is None:
            prev_close_by_day = build_prev_close_by_day(klines)
        price_limit_recorded: set = set()
        # #2 成交量约束：已记过 volume_cap_defer 事件的订单 id（每订单只记 1 条）
        volume_defer_recorded: set = set()
        # 层 1 容量模型（sqrt）：参与率 + 5 日累计 + 同日同票跨策略聚合。
        # 单组合路径一个实例；多组合路径共享同一实例以聚合各策略订单。
        capacity = self._build_capacity_budget(portfolio, klines)
        # 执行统计（BUY 单数 / 单笔金额上限缩量数）→ 服务层 >5% 告警
        run_stats: dict = {}
        # 策略层快照：strategy_id → 快照序列。共享现金按 capital_ratio 归一化分摊。
        strategy_snapshots: Dict[int, List[dict]] = {
            ctx.strategy_id: [] for ctx in portfolio.strategies
        }
        # W4.4 每日一行快照：按日期 flush 上一日末值（pending_*），循环后补最后一日。
        pending_snap: Optional[dict] = None
        pending_strategy_snaps: List[dict] = []
        last_snap_date: Optional[date] = None
        # W4.5 regime：喂入"前一交易日"基准收盘（消除当日未来数据），逐日推进。
        last_regime_date: Optional[date] = None
        last_enter_date: Optional[date] = None
        # D3 门控释放/召回：每交易日只推进一次（混合周期多 bar/日不重复释放）
        _gate_release_day: Optional[date] = None
        # 公司行为（J1/J2，默认未启用）：上次生效日（保证每事件只生效一次、非交易日
        # 顺延）与当日待执行的再投资意图（日终收盘价成交）。
        last_action_date: Optional[date] = None
        ca_reinvest: list = []

        # 日终估值步：每个交易日的**最后时间戳**组里最细周期的那一步。
        # 细→粗排序下即该组第一步；反之（粗→细）为最后一步。用它定义「当日最后一
        # 根 bar」，使日终快照/日末 peak 保护与排序方向解耦：既避免 1w/1mon 的周期末
        # close（对当日是未来价）污染估值，也不会在日内日末退化为粗周期估值。
        day_end_steps = set()
        _cur_date: Optional[date] = None
        _cur_t: Optional[datetime] = None
        _group_first: Optional[int] = None
        for _idx, (_t, _p) in enumerate(times):
            if _t.date() != _cur_date:
                if _group_first is not None:
                    day_end_steps.add(_group_first)
                _cur_date, _cur_t, _group_first = _t.date(), _t, _idx
            elif _t != _cur_t:
                _cur_t, _group_first = _t, _idx
        if _group_first is not None:
            day_end_steps.add(_group_first)

        # 新股窗口判据（修复 8）：A 股新股上市前 5 个交易日无涨跌幅限制，按板块幅度
        # 一律校验会把一字板/大涨误拦成 price_limit 延后。回测**只能用自身数据**判定：
        # 取每 code 首根 bar 日期，从该日起算交易日序（不引入外部上市日期）。
        # run_first_date = 全区间首个交易日：某 code 首根 bar 恰落在该日 → 数据起点
        # 与区间起点重合，无法区分「真新股」与「取数窗口截断」，**不**按新股宽松处理，
        # 只记一次 WARNING（首个 bar 本无前收，涨跌停校验天然跳过，即保守放行一次）。
        first_bar_date: Dict[str, date] = {}
        for _code, _periods in klines.items():
            _dates = [
                _dt.date()
                for _df in (_periods or {}).values() if _df is not None
                for _dt in _df["datetime"].to_list() if _dt is not None
            ]
            if _dates:
                first_bar_date[_code] = min(_dates)
        run_first_date = min(first_bar_date.values()) if first_bar_date else None
        _truncation_warned: set = set()
        # 新股 fail-open 已记录过的订单（每订单只记 1 条 allow 事件）
        new_listing_recorded: set = set()
        # 涨跌停档位缓存：{(code, 交易日) → LimitRule}。同一天同一票的档位只解析一次
        # （滑点夹取与闸门判定共用同一结果，避免重复告警/重复推导）。
        _rule_cache: Dict[tuple, object] = {}

        for i, (t, period) in enumerate(times):
            # K1 协作式取消：每根 bar 前检查一次（粒度=bar，长回测通常秒级响应）
            if cancel_check is not None and cancel_check():
                return {
                    "trades": [], "snapshots": [], "evaluations": {},
                    "strategy_snapshots": {}, "strategy_evaluations": {},
                    "decisions": [], "pending_orders": list(pending_orders),
                    "pending_order_count": len(pending_orders),
                    "pending_order_summary": [],
                    "risk_sell_trigger_count": 0, "risk_sell_fill_attempts": 0,
                    "canceled": True,
                    "progress": {"done_bars": i, "total_bars": len(times)},
                }
            # B5 市场状态闸：在新交易日首根 bar 用"前一交易日"基准收盘推进因果状态机
            # （当日收盘在 00:00 尚未确认，喂当日 = 未来函数）。基准缺失日跳过，沿用标签。
            if regime_tracker is not None:
                if last_regime_date is not None and t.date() != last_regime_date:
                    prev_bench = benchmark_data.get(last_regime_date) if benchmark_data else None
                    if prev_bench is not None:
                        regime_tracker.update(last_regime_date, float(prev_bench))
                portfolio.current_regime = regime_tracker.label
                if last_regime_date is None or t.date() != last_regime_date:
                    last_regime_date = t.date()

            # 0. 跨日补算上一日 daily_loss（在 on_bar 之前）：使日内亏损暂停从
            #    "亏损日的次一交易日"首根 bar 即生效、恢复日首根即解除（消 off-by-one）。
            #    W4：每交易日只调一次（enter_day 同日幂等；混合周期多 bar/日无重复副作用）。
            if last_enter_date != t.date():
                portfolio.risk_manager.enter_day(
                    t.date(), portfolio.account.initial_capital
                )
                last_enter_date = t.date()
                is_first_bar_of_day = True
            else:
                is_first_bar_of_day = False

            # 0a. 门控释放/召回（D3，默认不启用）：当日首根 bar、成交与 on_bar
            #     之前推进，只用当日生效 gate_state 与当前持仓，无前视。
            if gate_release_runner is not None and _gate_release_day != t.date():
                gate_release_runner.on_day(t.date(), t)
                _gate_release_day = t.date()

            # 0b. 公司行为（J1/J2，默认未启用 —— timeline=None 时整段跳过）。
            #     必须在成交步之前：当日待成交的顺延单若碰上送转，股数要先调整，
            #     否则按旧股数入账。分红现金直接进组合账户；再投资意图留到日终
            #     按当日收盘价买入（下方 4d）；退市/长期停牌强平在此即时成交。
            if (portfolio.corporate_timeline is not None
                    and last_action_date != t.date()):
                _ca_actions = apply_corporate_day(
                    portfolio, portfolio.corporate_timeline, t.date(),
                    recorder, mode=_ca_mode(portfolio),
                    reinvest_enabled=portfolio.dividend_reinvest_enabled,
                    price_resolver=lambda code: self._ca_reference_price(
                        code, t, period, price_index, prev_close_by_day),
                    dispose_days=portfolio.suspension_dispose_days,
                    delist_force_close=portfolio.delist_force_close_enabled,
                    last_action_date=last_action_date,
                    next_trade_day=_next_trade_day,
                )
                last_action_date = t.date()
                ca_reinvest.extend(
                    a for a in _ca_actions if a.kind == KIND_REINVEST)
                self._apply_corporate_actions(
                    portfolio, _ca_actions, t, trades, recorder)

            # 1. 成交上一 bar 的 pending_orders（用 t 的 open 价）
            deferred: List[OrderEvent] = []
            if pending_orders:
                bar_open_prices = self._bar_open_prices(price_index, t, period)
                # 滑点越板防护（2026-11-16）：成交价 = open ± 滑点，而闸门只比 open 与
                # 涨跌停价 → 开盘贴板时「含滑点成交价」可越板（买入高于涨停/卖出低于
                # 跌停 = 凭空多付/少收）。这里把本 bar 待成交票的涨跌停区间交给
                # dispatcher 夹取；仅开启涨跌停校验时提供（关闭时不做任何夹取，保持
                # 「关闭=完全不约束」的原语义）。
                bar_price_limits: Dict[str, tuple] = {}
                if portfolio.price_limit_check_enabled:
                    for _pending in pending_orders:
                        _code = _pending.stock_code
                        if _code in bar_price_limits or _code not in bar_open_prices:
                            continue
                        _pc = prev_close_by_day.get((_code, t.date()))
                        if _pc is None or _pc < Decimal("1"):
                            continue
                        _rule = _rule_cache.get((_code, t.date()))
                        if _rule is None:
                            _rule = self._limit_rule_for(
                                _code, t, stock_names, first_bar_date,
                                run_first_date, _truncation_warned,
                            )
                            _rule_cache[(_code, t.date())] = _rule
                        # 新股窗口档位为 None：无涨跌幅限制，不夹取（交由闸门 fail-open）
                        if _rule.ratio is None:
                            continue
                        # P1-2（2026-11-23）：夹价前自洽守卫——只诊断，不改夹价。
                        _ohlcv = (price_index.get((t, period), {}) or {}).get(_code) or {}
                        check_bar_consistent_with_prev_close(
                            code=_code, high=_ohlcv.get("high"),
                            low=_ohlcv.get("low"), prev_close=_pc,
                            ratio=_rule.ratio)
                        bar_price_limits[_code] = (
                            (_pc * (1 - _rule.ratio)).quantize(
                                Decimal("0.01"), rounding=ROUND_HALF_UP),
                            (_pc * (1 + _rule.ratio)).quantize(
                                Decimal("0.01"), rounding=ROUND_HALF_UP),
                        )
                # #4 自适应滑点（可选，默认关）：开启时透传本 bar 各票成交量
                # （手）与冲击系数，dispatcher 按订单量占比动态加滑点；
                # 关闭时不传（dispatcher 走固定滑点，原行为）。
                # 层 1：容量模型 sqrt 时同样需要成交量与 sqrt 冲击参数（k 取
                # capacity_impact_k），并为超限大单提供「不可成交」兜底。
                _use_impact = (
                    portfolio.adaptive_slippage_enabled or capacity is not None)
                dispatcher = SimulatedDispatcher(
                    bar_open_prices,
                    bar_volumes=(
                        {code: ohlcv.get("volume")
                         for code, ohlcv in price_index.get((t, period), {}).items()}
                        if _use_impact else None
                    ),
                    slippage_impact_coef=(
                        portfolio.capacity_impact_k if capacity is not None
                        else (portfolio.slippage_impact_coef
                              if portfolio.adaptive_slippage_enabled else None)
                    ),
                    price_limits=bar_price_limits or None,
                    impact_model=("sqrt" if capacity is not None else "linear"),
                    impact_base=portfolio.capacity_impact_base,
                    impact_cap=portfolio.capacity_impact_cap,
                    **portfolio.cost_params,
                )
                engine = ExecutionEngine(
                    dispatcher, SimulatedT1Checker(), recorder=recorder,
                    # 可转债 10 张取整（可选，默认关）：关闭时恒回 100，行为不变
                    lot_size_of=lambda code: lot_size_for(
                        code, portfolio.bond_lot_enabled
                    ),
                    stats=run_stats,
                )
                # 未来函数闸门：日线级信号（signal_bar_date 非 None）须在
                # t.date() > signal_bar_date 的下一交易日成交，防止当日 00:00
                # 日线 bar 的信号在当日 09:30 分钟 bar 成交。未满足的订单延后。
                for order in pending_orders:
                    # 在途单最大存活期（B1，2026-11-16）：从「首次未能成交的交易日」
                    # 起算交易日数，超期直接作废（不回队）。停牌/连板可让 deferred
                    # 单无限顺延，几个月后按恢复日开盘价成交 = 用过期决策补做一笔
                    # 交易；实盘有超时撤单 + 次日重排兜底，不会挂这么久。
                    if (pending_order_max_days and pending_order_max_days > 0
                            and order.pending_since is not None
                            and trading_days_between(order.pending_since, t.date())
                            >= pending_order_max_days):
                        recorder.record(
                            gate="pending_order_expired", layer="execution",
                            action="cancel",
                            portfolio_id=order.portfolio_id or 0,
                            strategy_id=order.strategy_id,
                            stock_code=order.stock_code,
                            bar_time=t,
                            message=(
                                "在途单超过最大存活期（%d 个交易日）：作废，不再顺延"
                                % pending_order_max_days),
                        )
                        continue
                    # 风控卖单缩量前的原始委托量：成交不足时残量回队续卖
                    # （对齐实盘 inflight_sell：一次触发 + 缩量分批成交）。
                    risk_sell_original = None
                    # W4 周期路由：订单只在其所属周期的 bar 上成交（1d 单等下一根 1d bar，
                    # 5m 单等下一根 5m bar），非本周期延后——杜绝跨周期前视成交。
                    if order.period is not None and order.period != period:
                        deferred.append(order)
                        continue
                    if order.signal_bar_date is not None and t.date() <= order.signal_bar_date:
                        deferred.append(order)
                        continue
                    # #2 静默丢弃修复：停牌/无行情（本 bar 该票无有效 open 价）→
                    # 延后至下一有价 bar 重试，不静默丢弃（此前直接丢弃导致
                    # 停牌期间止损/止盈单永久丢失）。仅首次延后记决策事件。
                    if not bar_open_prices.get(order.stock_code):
                        if id(order) not in no_price_recorded:
                            no_price_recorded.add(id(order))
                            recorder.record(
                                gate="no_market_data", layer="execution", action="defer",
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code,
                                bar_time=t,
                                message="停牌/无行情：本 bar 无开盘价，订单延后至下一有价 bar 重试",
                            )
                        deferred.append(order)
                        continue
                    # #1 涨跌停约束（默认开；组合表显式关闭时不校验）：以本 bar open 判定——BUY open ≥ 涨停价
                    # （前收×(1+幅度)，精确到分）或 SELL open ≤ 跌停价 → 延后至下一 bar
                    # 重试（仿 no_market_data，每订单仅首次记 1 条决策事件）。
                    # 无前收或前收 < 1 元跳过检查：前复权"加性压缩"使早期前收逼近 0/
                    # 失真，比率不可信，宁宽勿错（dual 后复权记账口径比率真实）。
                    if portfolio.price_limit_check_enabled:
                        # 前收 = 上一交易日最后一根 bar 收盘（日内恒定，任意周期），
                        # 见 build_prev_close_by_day 的依据说明。
                        prev_close = prev_close_by_day.get((order.stock_code, t.date()))
                        if prev_close is not None and prev_close >= Decimal("1"):
                            open_px = bar_open_prices[order.stock_code]
                            _rule_key = (order.stock_code, t.date())
                            rule = _rule_cache.get(_rule_key)
                            if rule is None:
                                rule = self._limit_rule_for(
                                    order.stock_code, t, stock_names, first_bar_date,
                                    run_first_date, _truncation_warned,
                                )
                                _rule_cache[_rule_key] = rule
                            ratio = rule.ratio
                            if ratio is None:
                                if rule.source.startswith("new_listing"):
                                    # 新股窗口：fail-open 放行（与桥缺数据同取向），事件标
                                    # limit_rule=new_listing（每订单一次）。
                                    if id(order) not in new_listing_recorded:
                                        new_listing_recorded.add(id(order))
                                        recorder.record(
                                            gate="price_limit", layer="execution", action="allow",
                                            portfolio_id=order.portfolio_id or 0,
                                            strategy_id=order.strategy_id,
                                            stock_code=order.stock_code,
                                            bar_time=t,
                                            message="新股上市初期无涨跌幅限制（%s），跳过本地"
                                                    "涨跌停校验（fail-open，交柜台）；"
                                                    "limit_rule=new_listing" % rule.source,
                                        )
                            else:
                                limit_up = (prev_close * (1 + ratio)).quantize(
                                    Decimal("0.01"), rounding=ROUND_HALF_UP)
                                limit_down = (prev_close * (1 - ratio)).quantize(
                                    Decimal("0.01"), rounding=ROUND_HALF_UP)
                                is_buy = order.trade_type == TradeType.BUY
                                if (is_buy and open_px >= limit_up) or (
                                        not is_buy and open_px <= limit_down):
                                    if id(order) not in price_limit_recorded:
                                        price_limit_recorded.add(id(order))
                                        # 名称缺失 → 幅度是「按板块默认推断」的（修复 7），
                                        # 事件里明确标注，避免误以为已识别 ST。
                                        _suffix = (
                                            "（证券名称缺失，幅度按默认 %s 推断）" % ratio
                                            if "name_unknown" in rule.source else ""
                                        )
                                        recorder.record(
                                            gate="price_limit", layer="execution", action="defer",
                                            portfolio_id=order.portfolio_id or 0,
                                            strategy_id=order.strategy_id,
                                            stock_code=order.stock_code,
                                            bar_time=t,
                                            message="涨跌停约束：%s 开盘价 %s %s（前收 %s ×%s，%s）%s，订单延后至下一 bar 重试"
                                                    % ("涨停买不进" if is_buy else "跌停卖不出",
                                                       open_px,
                                                       ("≥ 涨停价 %s" % limit_up) if is_buy
                                                       else ("≤ 跌停价 %s" % limit_down),
                                                       prev_close, ratio, rule.source, _suffix),
                                        )
                                    deferred.append(order)
                                    continue
                    # #2 成交量/流动性约束（可选，默认关）：单 bar 成交量 × ratio 为
                    # 成交股数上限（TQ Volume 单位=股，直接使用，向下取整手）。
                    # 订单量 ≤ 上限 → 照常；> 上限且上限 ≥ 一手 → 缩量成交（每次记）；
                    # 上限不足一手 → 延后至量足 bar（每订单仅首次记 1 条）。
                    # 4b. 执行档位 partial_fill（N1，默认关）：成交量上限缩量后
                    # 残量回队续成交（仅 BUY；卖出残量由既有风控残量/下一次信号处理，
                    # 避免一次卖出被拆成两笔重复计入卖出信号统计）。
                    partial_residual = 0
                    # 层 1 容量模型（sqrt）：参与率预算不足 → 残量顺延切片（TTL 内
                    # 每根 bar 重校验价格笼子/现金），不整单拒绝；超 TTL 才作废。
                    # 与旧 4% 闸门互斥：sqrt 下一律走本链路，linear/legacy 不变。
                    capacity_residual = 0
                    if capacity is not None:
                        _row = price_index.get((t, period), {}).get(
                            order.stock_code) or {}
                        _is_buy = order.trade_type == TradeType.BUY
                        _requested = order.quantity
                        _allowed, _why = capacity.allow(
                            order.stock_code, t, _row.get("volume"),
                            _row.get("close"), _requested,
                            lot_size_for(order.stock_code,
                                         portfolio.bond_lot_enabled)
                            if _is_buy else 1,
                        )
                        if _allowed < _requested:
                            recorder.record(
                                gate="capacity_slice", layer="execution",
                                action=("shrink" if _allowed > 0 else "defer"),
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code,
                                bar_time=t,
                                requested_qty=_requested,
                                final_qty=_allowed,
                                param_name=_why,
                                message="容量约束（%s）：请求 %d 股，本 bar 可成交 %d 股，"
                                        "残量顺延（TTL %d 根 bar）"
                                        % ({"bar": "本 bar 成交量",
                                            "day": "当日累计成交量",
                                            "5d": "5 日累计成交量",
                                            "no_volume": "无成交量"}.get(_why, _why),
                                           _requested, _allowed,
                                           portfolio.capacity_slice_ttl_bars),
                            )
                            if _allowed <= 0:
                                if (order.capacity_carry_bars + 1
                                        >= portfolio.capacity_slice_ttl_bars):
                                    recorder.record(
                                        gate="capacity_slice_expired",
                                        layer="execution", action="cancel",
                                        portfolio_id=order.portfolio_id or 0,
                                        strategy_id=order.strategy_id,
                                        stock_code=order.stock_code,
                                        bar_time=t,
                                        requested_qty=_requested,
                                        param_name="capacity_slice_ttl_bars",
                                        param_value=float(
                                            portfolio.capacity_slice_ttl_bars),
                                        message="容量约束顺延超过 %d 根 bar 仍未成交，"
                                                "订单作废" % portfolio.capacity_slice_ttl_bars,
                                    )
                                    continue
                                deferred.append(replace(
                                    order,
                                    capacity_carry_bars=order.capacity_carry_bars + 1,
                                ))
                                continue
                            if (not _is_buy
                                    and order.signal_type in _RISK_SELL_TYPES):
                                # 风控卖单残量沿用既有 risk_sell_residual 回队
                                risk_sell_original = _requested
                            else:
                                capacity_residual = _requested - _allowed
                            order.quantity = _allowed
                    elif portfolio.volume_limit_enabled:
                        if _liquidity_caliber == "prev5d_avg":
                            # 显式选择的新口径：分母 = 成交日之前 5 个有效交易日
                            # 日均成交量（股），与实盘同口径；严格不含当日（旧实现
                            # 用成交当根 bar 量，含当日未来信息）。无历史 → fail-open。
                            ref_volume = volume_ref_by_day.get(
                                (order.stock_code, t.date()))
                        else:
                            # 默认口径（与改动前逐字一致）：成交当根 bar 的成交量
                            ref_volume = price_index.get((t, period), {}).get(
                                order.stock_code, {}).get("volume")
                        if ref_volume:
                            # TQ Volume 单位=股（非手），cap = 占比 × 分母
                            cap_shares = int(
                                portfolio.volume_limit_ratio * Decimal(int(ref_volume))
                            )
                            if order.quantity > cap_shares:
                                vol_lot = lot_size_for(
                                    order.stock_code, portfolio.bond_lot_enabled
                                )
                                # A 股规则：买入必须整手，卖出允许零股。买入缩量
                                # 必须先按手取整（否则会产生 137 股这类非整手持仓，
                                # 后续 SELL 手数校验会把它当成不足一手而永远卖不掉）。
                                is_buy = order.trade_type == TradeType.BUY
                                final_qty = (
                                    (cap_shares // vol_lot) * vol_lot
                                    if is_buy else cap_shares
                                )
                                if final_qty >= vol_lot or (not is_buy and final_qty > 0):
                                    if _liquidity_caliber == "prev5d_avg":
                                        _msg = (
                                            "成交量约束：订单 %d 股 > 近5日均量口径上限 %d 股"
                                            "（近5日均量 %s 股 ×%s），缩量至 %d 股成交"
                                            % (order.quantity, cap_shares, ref_volume,
                                               portfolio.volume_limit_ratio, final_qty))
                                    else:
                                        _msg = (
                                            "成交量约束：订单 %d 股 > 本 bar 上限 %d 股"
                                            "（当日成交 %s 股 ×%s），缩量至 %d 股成交"
                                            % (order.quantity, cap_shares, ref_volume,
                                               portfolio.volume_limit_ratio, final_qty))
                                    recorder.record(
                                        gate="volume_cap_shrink", layer="execution",
                                        action="shrink",
                                        portfolio_id=order.portfolio_id or 0,
                                        strategy_id=order.strategy_id,
                                        stock_code=order.stock_code,
                                        bar_time=t,
                                        requested_qty=order.quantity,
                                        final_qty=final_qty,
                                        message=_msg,
                                    )
                                    if (not is_buy
                                            and order.signal_type in _RISK_SELL_TYPES):
                                        risk_sell_original = order.quantity
                                    if _partial_fill and is_buy:
                                        partial_residual = order.quantity - final_qty
                                    order.quantity = final_qty
                                else:
                                    if id(order) not in volume_defer_recorded:
                                        volume_defer_recorded.add(id(order))
                                        if _liquidity_caliber == "prev5d_avg":
                                            _msg = (
                                                "成交量约束：近5日均量口径上限 %d 股不足一手"
                                                "（近5日均量 %s 股 ×%s），订单延后至量足 bar"
                                                % (cap_shares, ref_volume,
                                                   portfolio.volume_limit_ratio))
                                        else:
                                            _msg = (
                                                "成交量约束：本 bar 上限 %d 股不足一手"
                                                "（当日成交 %s 股 ×%s），订单延后至量足 bar"
                                                % (cap_shares, ref_volume,
                                                   portfolio.volume_limit_ratio))
                                        recorder.record(
                                            gate="volume_cap_defer", layer="execution",
                                            action="defer",
                                            portfolio_id=order.portfolio_id or 0,
                                            strategy_id=order.strategy_id,
                                            stock_code=order.stock_code,
                                            bar_time=t,
                                            requested_qty=order.quantity,
                                            message=_msg,
                                        )
                                    deferred.append(order)
                                    continue
                    # 成交日 = 当前 bar 时间 t（T+1 据此判断）
                    order.bar_time = t
                    ctx = portfolio.find_strategy(order.strategy_id)
                    if ctx is None:
                        continue
                    # BUY 首次建仓：确保 Position 存在于 ctx.positions
                    pos = ctx.positions.get(order.stock_code)
                    if pos is None and order.trade_type.value == "BUY":
                        # 可转债 run 级开关（可选，默认关）：T+0 仅标记转债持仓
                        # （混合池股票仍 T+1）；手数按标的定档（转债 10 张 / 股票 100 股）
                        pos = Position(
                            order.stock_code,
                            same_day_sellable=(
                                portfolio.bond_t0_enabled
                                and is_convertible_bond(order.stock_code)
                            ),
                            lot_size=lot_size_for(
                                order.stock_code, portfolio.bond_lot_enabled
                            ),
                        )
                        ctx.positions[order.stock_code] = pos
                    trade = engine.execute(
                        order, portfolio.account, pos,
                        **self._exposure_kwargs(portfolio, ctx),
                    )
                    if trade is not None:
                        trades.append(trade)
                        # 风控冷静期起算（与实盘共用 Portfolio.note_trade）：止损/止盈/
                        # 移动止损成交后登记 (策略, 代码) → 当日/后续 N 交易日禁开新仓。
                        portfolio.note_trade(trade)
                    # 风控卖单缩量后残量不丢弃：挂回原单（quantity=剩余量）继续在
                    # 后续 bar 分批成交。这样一笔风控触发只产生 1 次 trigger 事件，
                    # 不会被下一根 bar 的风控检查重复触发（对齐实盘 inflight_sell）。
                    if risk_sell_original is not None and trade is not None:
                        remaining = (int(risk_sell_original)
                                     - int(getattr(trade, "quantity", 0) or 0))
                        if remaining > 0:
                            order.quantity = remaining
                            deferred.append(order)
                            recorder.record(
                                gate="risk_sell_residual", layer="execution",
                                action="carry",
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code,
                                bar_time=t,
                                requested_qty=remaining,
                                message="风控卖单缩量成交 %d 股，续卖残量 %d 股挂在原单上"
                                        "于后续 bar 分批成交（不重复触发风控）"
                                        % (int(risk_sell_original) - remaining, remaining),
                            )
                    # 4b. 买入 partial_fill 残量回队（默认关）：成交量上限缩量成交后，
                    # 未成交部分作为新在途单挂回，下一根 bar 继续按 open 成交。
                    if (_partial_fill and trade is not None and partial_residual > 0):
                        deferred.append(replace(order, quantity=partial_residual))
                        recorder.record(
                            gate="partial_fill", layer="execution", action="carry",
                            portfolio_id=order.portfolio_id or 0,
                            strategy_id=order.strategy_id,
                            stock_code=order.stock_code,
                            bar_time=t,
                            requested_qty=partial_residual,
                            message="成交量约束部分成交，剩余 %d 股回队于后续 bar 续买"
                                    "（execution_profile=realistic）" % partial_residual,
                        )
                    # 层 1 容量模型：以实际成交量记账（资金/敞口二次缩量后仍准确），
                    # 并把未成交残量作为新在途单回队（TTL 内继续切片；超期作废）。
                    if capacity is not None and trade is not None:
                        capacity.consume(order.stock_code, t,
                                         int(getattr(trade, "quantity", 0) or 0))
                    if capacity_residual > 0 and trade is not None:
                        if (order.capacity_carry_bars + 1
                                >= portfolio.capacity_slice_ttl_bars):
                            recorder.record(
                                gate="capacity_slice_expired", layer="execution",
                                action="cancel",
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code,
                                bar_time=t,
                                requested_qty=capacity_residual,
                                param_name="capacity_slice_ttl_bars",
                                param_value=float(portfolio.capacity_slice_ttl_bars),
                                message="容量切片残量 %d 股顺延超过 %d 根 bar，作废"
                                        % (capacity_residual,
                                           portfolio.capacity_slice_ttl_bars),
                            )
                        else:
                            deferred.append(replace(
                                order, quantity=capacity_residual,
                                capacity_carry_bars=order.capacity_carry_bars + 1,
                            ))
                            recorder.record(
                                gate="capacity_slice", layer="execution",
                                action="carry",
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code,
                                bar_time=t,
                                requested_qty=capacity_residual,
                                message="容量约束部分成交，残量 %d 股顺延至后续 bar 续成交"
                                        % capacity_residual,
                            )

            # 2. 构造 BarEvent（从预建索引 O(1) 查表；含当前周期标注）
            bar = self._build_bar(price_index, t, period)

            # 3. portfolio.on_bar → 新订单入 pending（下一根同周期 bar 成交）
            #    period=P 只驱动 period 相同的策略，杜绝混合周期前视。
            #    deferred（未成交/未到期订单）需携带至后续 bar 合并。
            #    trading_session（full/am/pm）时段闸：非允许时段的日内 bar 跳过本组合
            #    本轮（风控/信号随 bar 跳过，不产生新交易；deferred/既有 pending
            #    照常延后成交）。⚠️ 行为变更：am/pm 组合的日内回测结果会改变
            #    （历史死配置首次接线）；full（默认）无影响。日线及以上 bar
            #    （bar_time=00:00）不受时段过滤——否则 am/pm 组合的日线回测会
            #    被整程跳过（session_allows_bar 的周期判定）。
            if session_allows_bar(portfolio.trading_session, t.time(), period):
                # 在途风控卖单去重（与实盘 inflight_sell 同口径）：deferred 里仍有
                # 未结清的同 (策略, 代码, SELL) 风控单（含刚回队的缩量残量）时，
                # 本 bar 不再重复评估风控/不重复发单，只让残量继续成交。
                inflight_risk_sells: Dict[int, set] = {}
                for _order in deferred:
                    if (_order.trade_type == TradeType.SELL
                            and _order.signal_type in _RISK_SELL_TYPES):
                        inflight_risk_sells.setdefault(
                            _order.strategy_id, set()).add(_order.stock_code)
                new_orders = portfolio.on_bar(
                    bar, signal_cache=signal_cache, period=period,
                    inflight_risk_sells=inflight_risk_sells,
                )
                # 执行档位（N1，默认关）：每交易日首根 bar 不生成 BUY（卖出/风控照常）。
                # 关闭时逐字维持旧行为——不进行任何过滤。
                if _first_bar_no_fill and is_first_bar_of_day and new_orders:
                    _kept_orders = []
                    for _o in new_orders:
                        if _o.trade_type == TradeType.BUY:
                            recorder.record(
                                gate="first_bar_no_fill", layer="execution",
                                action="skip",
                                portfolio_id=_o.portfolio_id or 0,
                                strategy_id=_o.strategy_id,
                                stock_code=_o.stock_code,
                                bar_time=t,
                                requested_qty=_o.quantity,
                                message="执行档位：当日首根 bar 不买入，订单丢弃"
                                        "（execution_profile=realistic）",
                            )
                        else:
                            _kept_orders.append(_o)
                    new_orders = _kept_orders
                # 在途单寿命起点：新单在其信号 bar 当日登记（后续若首根就成交，
                # 该字段不参与任何计算；只有真正被延后的单才会累计交易日）。
                for _o in new_orders:
                    if _o.pending_since is None:
                        _o.pending_since = t.date()
                pending_orders = deferred + new_orders
            else:
                pending_orders = deferred

            # 4. 日终快照（W4.4：每交易日至多一行，取「日终估值步」的末值）
            total_value = portfolio.total_value(bar)
            is_day_end = i in day_end_steps
            if is_day_end and ca_reinvest:
                # 4d. 分红再投资（J1，默认关）：当日收盘价买入整手，不足一手留现金。
                self._execute_ca_reinvest(
                    portfolio, ca_reinvest, bar, t, trades, recorder)
                ca_reinvest = []
            if is_day_end:
                # 4e. equity_cap 复利收口（mode=distribute，默认 cap 无操作）：
                #     日终把超上限部分作为外部流出扣出现金，先于快照落账。
                total_value = self._apply_equity_cap_distribution(
                    portfolio, total_value, t, recorder)
                snap = portfolio.snapshot(t.date(), total_value, bar)
                # 资金池 L2 借据（0021 B3）：建模成立时随快照写版本与额度块；
                # 默认 None → 不新增任何键，旧快照逐位一致。
                if loan_state is not None and loan_state.get("status") == "applied":
                    snap["loan_model_version"] = loan_state["loan_model_version"]
                    snap["capital_pool_version"] = loan_state["capital_pool_version"]
                    snap["entitlement_json"] = json.dumps(
                        {
                            "loan_model_version": loan_state["loan_model_version"],
                            "budgets": loan_state["budgets"],
                            "entitlements": loan_state["entitlements"],
                            "loan_book": loan_state["loan_book"],
                        },
                        ensure_ascii=False, sort_keys=True,
                    )
                # 填基准值：benchmark_data 是 {date: Decimal}，按当前 bar 日期取值；
                # 缺失（基准停牌/非交易日/区间外）直接为 None（不做 carry-forward），
                # Evaluator 喂基准序列时会过滤 None 值，全无则 benchmark_return 退化为 0。
                snap["benchmark_value"] = benchmark_data.get(t.date()) if benchmark_data else None
                # 市场多空门控：快照追加当日门控状态/强度（右移语义由 tracker 处理）；
                # tracker=None 不写键——与「门控关闭不渲染」的前端契约一致。
                if gate_tracker is not None:
                    snap["gate_state"] = gate_tracker.state_for(t.date())
                    snap["gate_strength"] = gate_tracker.strength_for(t.date())
                if gate_release_runner is not None:
                    summary = gate_release_runner.day_summary(t.date())
                    if summary is not None:
                        snap["gate_release"] = dict(summary)

                # 4b. 策略层快照：共享现金按 capital_ratio 归一化分摊，保证可加性
                #     Σ(策略总净值) == 组合总净值。供策略层独立净值曲线与评估。
                #     注入与组合同日的 benchmark_value，使策略层相对指标（FR-1）可算。
                day_strategy_snaps = self._strategy_snapshots(portfolio, bar, t.date())
                for s_snap in day_strategy_snaps:
                    s_snap["benchmark_value"] = snap["benchmark_value"]

            # 4a. W2 现金管理增强（预留触发点，默认关）：命中阈值仅记决策事件，不产生订单。
            #     每交易日内部去重（_cash_enhance_last_date），混合周期多 bar/日只记一次。
            portfolio.maybe_cash_enhancement(bar, total_value)

            # 4c. W4.4 每日一行：日期变化时 flush 上一日的末值快照（组合 + 策略），
            #     当前 bar 落入 pending，循环结束后补最后一日的 pending。
            if last_snap_date is not None and t.date() != last_snap_date:
                snapshots.append(pending_snap)
                for s_snap in pending_strategy_snaps:
                    strategy_snapshots[s_snap["target_id"]].append(s_snap)
            last_snap_date = t.date()
            if is_day_end:
                pending_snap = snap
                pending_strategy_snaps = day_strategy_snaps

            # 5. 熔断检测：更新峰值/检测 max_drawdown，推进熔断次日恢复时序
            #    （daily_loss 已在本 bar 起始 enter_day 补算，此处只做峰值/回撤）
            #    D8：粗周期（1d/1w/1mon）的 close 是周期末价，在时间轴起点（00:00）
            #    对当时的分钟时点是未来价——混合周期下拿它抬峰 = 前视，会让
            #    max_drawdown 提前/错误触发。故粗周期只在「日终估值步」允许抬峰；
            #    分钟周期照旧每根抬（纯分钟/纯日线回测行为不变）。
            #    daily_close_peak_enabled=True 时统一收紧为「只在日终步抬峰」（可选
            #    的更严格模式，默认关）。
            allow_peak_update = (
                is_day_end if period in _COARSE_PERIODS else True)
            if daily_close_peak_enabled:
                allow_peak_update = is_day_end
            portfolio.risk_manager.update_peak(
                total_value, t.date(), allow_peak_update=allow_peak_update,
            )

            # 6. 前收表（涨跌停基准）已在 run 开头按 (code, 交易日) 预计算好，日内恒定，
            #    无需逐 bar 刷新（旧实现此处覆写导致分钟策略前收取错，见上方依据注释）。

            # 7. 进度上报：节流至最多约 100 次，末根 bar 必报 100（#10）。
            if on_progress is not None and (i % report_every == 0 or i == total_bars - 1):
                on_progress(int((i + 1) * 100 / total_bars))

        # 6. flush 最后一日的 pending 快照（无后续跨日触发）
        if pending_snap is not None:
            snapshots.append(pending_snap)
            for s_snap in pending_strategy_snaps:
                strategy_snapshots[s_snap["target_id"]].append(s_snap)

        # 7. 补算最后一日 daily_loss（#8 对齐实盘 14:30 时点：循环内 update 只在
        #    跨日时补算上一日，最后一日无后续跨日触发，需显式收尾）
        portfolio.risk_manager.finalize_last_day(portfolio.account.initial_capital)

        # 基准序列喂 Evaluator
        # 形如 [{"value": Decimal}]。无任何基准值 → 传 None，benchmark_return 退化为 0。
        bench_series = (
            [{"value": s["benchmark_value"]} for s in snapshots if s.get("benchmark_value") is not None]
            or None
        )
        evaluations = Evaluator().evaluate(snapshots, benchmark_data=bench_series, trades=trades)
        # 策略层评估：复用 Evaluator，喂入该策略快照 + 该策略 trades（按 strategy_id 过滤）。
        # 传入 bench_series 使策略层也能计算基准相对指标（beta/alpha/IR/捕获率等）。
        strategy_evaluations: Dict[int, dict] = {}
        for ctx in portfolio.strategies:
            s_snaps = strategy_snapshots.get(ctx.strategy_id, [])
            if len(s_snaps) < 2:
                continue
            s_trades = [t for t in trades if t.strategy_id == ctx.strategy_id]
            strategy_evaluations[ctx.strategy_id] = Evaluator().evaluate(
                s_snaps, benchmark_data=bench_series, trades=s_trades
            )
        # D9：回测结束仍有未成交信号（1w/1mon 等长周期信号需后续同周期 bar 才能
        # 成交，短区间内可能永不触发）。计数随结果返回，服务层追加到 warnings 便于
        # 用户识别「策略信号没做进去」而不是误以为策略没出信号。
        pending_summary = [
            {
                "stock_code": o.stock_code,
                "period": o.period,
                "signal_type": getattr(o, "signal_type", None),
                "bar_time": getattr(o, "bar_time", None),
            }
            for o in pending_orders
        ]
        decision_dicts = [ev.to_dict() for ev in recorder.drain()]
        # 修复 10 统计口径：triggered = 真实风控触发次数；fill_attempts = 风控卖单
        # 实际成交笔数（缩量分批时 > triggered）。正常情况下 1 次触发可对应多笔
        # 成交，但绝不会因「残量未卖完」重复触发。
        risk_trigger_count = sum(
            1 for d in decision_dicts
            if d.get("layer") == "strategy_risk" and d.get("action") == "trigger"
        )
        risk_fill_attempts = sum(
            1 for t in trades if getattr(t, "signal_type", None) in _RISK_SELL_TYPES
        )
        # 0021 C5：现金出口利息（默认 None=不计算）。只读日终快照，单列返回，
        # 不改现金/净值/成交；plan 非法降级为 enabled=False，不中断回测。
        cash_yield = None
        if cash_outlet_plan is not None:
            try:
                cash_yield = build_cash_yield_contribution(
                    snapshots, cash_outlet_plan)
            except CashOutletPlanError as exc:
                cash_yield = {
                    "version": CASH_OUTLET_MODEL_VERSION,
                    "enabled": False, "status": "invalid",
                    "reason": str(exc),
                }
        return {
            "trades": trades,
            "snapshots": snapshots,
            "evaluations": evaluations,
            "strategy_snapshots": strategy_snapshots,
            "strategy_evaluations": strategy_evaluations,
            # 决策闸门事件（止损/止盈/熔断/资金不足/超仓…逐触发一条），供落库与调参统计
            "decisions": decision_dicts,
            # 段末仍未成交的在途单（增量续跑写入 checkpoint，下一段继续尝试）
            "pending_orders": pending_orders,
            # D9 收尾统计：未成交信号数 + 摘要（服务层据此写 warnings）
            "pending_order_count": len(pending_orders),
            "pending_order_summary": pending_summary,
            # 修复 10 汇总：风控触发次数 vs 风控卖单成交笔数（缩量分批可见）
            "risk_sell_trigger_count": risk_trigger_count,
            "risk_sell_fill_attempts": risk_fill_attempts,
            # 层 1：BUY 单数 / 单笔金额上限缩量数（服务层 >5% 告警）
            "execution_stats": run_stats,
            # 资金池 L2 借据（0021 B3）：None=未启用/未成立，不影响任何旧消费方
            "loan_model": loan_state,
            # 门控释放/召回（0021 D3）：None=未启用；只含上限变化与借据时间线
            "gate_release": (gate_release_runner.result()
                             if gate_release_runner is not None else None),
            # 现金出口利息（0021 C5）：None=未启用；与策略指标分离
            "cash_yield_contribution": cash_yield,
        }

    def run_multi(
        self,
        portfolios: List[Portfolio],
        klines: dict,
        signal_cache: dict = None,
        benchmark_data: Optional[dict] = None,
        stock_names: Optional[dict] = None,
        regime_trackers: Optional[Dict[int, object]] = None,
        gate_trackers: Optional[Dict[int, object]] = None,
        industry_tracker=None,
        pending_order_max_days: int = PENDING_ORDER_MAX_DAYS,
        run_options: Optional[dict] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        capital_pool_plan: Optional[dict] = None,
    ) -> Dict[int, dict]:
        """多组合共享现金回测（N1，2026-11-18）。

        与 ``run`` 的关系：单组合路径完全不走这里（``run`` 未改动默认行为）。
        本方法复用同一套辅助方法（涨跌停档位、成交量上限、日终快照、公司行为、
        T+1、熔断），差异只有「资金」一层：

          * 每个组合仍有自己的 ``Account``（回测是纯现金账）；
          * 每个 bar 上，各组合 ``on_bar`` 期间临时把 ``account.cash`` 置为
            **共享可用额**（Σ cash − Σ 在途 BUY 金额），使仓位按共享池而非各自
            账本切分审批；
          * 收集到的新买单一律交给 ``cash_allocator.allocate_batch_buys`` 统一
            逐笔审批（80% 门槛、按组合/信号顺序、不做等比缩量），拒绝单记
            ``insufficient_cash``；
          * 默认各组合现金独立、不跨组合划转（以代码为准；旧注释所述
            「成交时自动转账」不在默认路径实现）。

        返回 ``{portfolio_id: run() 同构结果}``（不含聚合，聚合由服务层拼）。
        取消（K1，``cancel_check``）命中时返回 ``{"_canceled": True,
        "_progress": {...}, "_decisions": []}``，服务层据此把整批标 canceled。

        ``capital_pool_plan``（0021 E2，默认 None=旧行为）：显式开启会话池动态
        额度。每个交易日首 bar 用 ``session_pool.allocate_session_pool`` 按
        「目标仓位 − 持仓市值」的需求做水位分配，把额度写到各策略
        ``capital_budget``；组合自身现金不足时，成交前从其他组合的闲置现金
        临时拆借（只借预算内差额，成交后多借部分立即退回），全程 Σ 现金守恒、
        任一账户现金不为负。关闭或 None 时不计算、不改额度、不转账。
        """
        run_options = run_options or {}
        _pool_plan = capital_pool_plan if isinstance(capital_pool_plan, dict) else None
        _pool_enabled = bool(_pool_plan and _pool_plan.get("enabled") is True)
        _pool_min_share = (
            _pool_plan.get("min_share", "0.05") if _pool_enabled else None)
        _pool_max_share = (
            _pool_plan.get("max_share", "1.0") if _pool_enabled else None)
        _pool_reserve_pct = (
            _pool_plan.get("min_cash_reserve_pct", "0.03")
            if _pool_enabled else None)
        _first_bar_no_fill = bool(run_options.get("first_bar_no_fill", False))
        _partial_fill = bool(run_options.get("partial_fill_enabled", False))
        # 流动性口径：默认 same_day_bar（改动前行为），prev5d_avg 需显式选择
        _liquidity_caliber = (
            run_options.get("liquidity_caliber") or "same_day_bar")
        if not portfolios:
            return {}
        klines = klines or {}
        signal_cache = signal_cache if signal_cache is not None else {}
        recorder = DecisionRecorder()
        for portfolio in portfolios:
            portfolio.set_recorder(recorder)
            portfolio.current_regime = None

        times = self._build_timeline(klines)
        price_index = self._build_price_index(klines)
        prev_close_by_day = build_prev_close_by_day(klines)
        # P1-3：沪/北送转新增股落 ex_date+1 交易日桶所需的「下一交易日」查询。
        _trade_days = sorted({_t[0].date() for _t in times})

        def _next_trade_day(_d, _days=_trade_days):
            import bisect
            _i = bisect.bisect_right(_days, _d)
            return _days[_i] if _i < len(_days) else None
        # #2 成交量闸门分母（可选新口径）：成交日之前 5 个交易日日均量
        # （与实盘同口径，无前视）。默认口径不预计算，保持旧行为与耗时。
        volume_ref_by_day = (
            build_volume_ref_by_day(klines)
            if _liquidity_caliber == "prev5d_avg" else {})
        first_bar_date: Dict[str, date] = {}
        for _code, _periods in klines.items():
            _dates = [
                _dt.date()
                for _df in (_periods or {}).values() if _df is not None
                for _dt in _df["datetime"].to_list() if _dt is not None
            ]
            if _dates:
                first_bar_date[_code] = min(_dates)
        run_first_date = min(first_bar_date.values()) if first_bar_date else None
        _truncation_warned: set = set()
        new_listing_recorded: set = set()
        no_price_recorded: set = set()
        price_limit_recorded: set = set()
        volume_defer_recorded: set = set()
        _rule_cache: Dict[tuple, object] = {}
        # 层 1 容量模型（sqrt）：多组合共享**同一实例**（同日同票跨策略聚合），
        # 参数取首个启用 sqrt 的组合；未启用 sqrt 的组合不持有预算（旧行为）。
        _cap_portfolio = next(
            (p for p in portfolios
             if getattr(p, "capacity_model", "linear") == "sqrt"), None)
        capacity = (self._build_capacity_budget(_cap_portfolio, klines)
                    if _cap_portfolio is not None else None)
        run_stats: dict = {}

        states = []
        for portfolio in portfolios:
            gate = (gate_trackers or {}).get(portfolio.portfolio_id)
            portfolio.current_market_gate = gate
            portfolio.current_industry_gate = industry_tracker
            states.append({
                "portfolio": portfolio,
                "trades": [],
                "snapshots": [],
                "strategy_snapshots": {
                    ctx.strategy_id: [] for ctx in portfolio.strategies},
                "pending": [],
                "pending_snap": None,
                "pending_strategy_snaps": [],
                "last_snap_date": None,
                "last_enter_date": None,
                "last_action_date": None,
                "last_regime_date": None,
                "ca_reinvest": [],
                # 0021 E2：会话池动态额度（仅 _pool_enabled 时读写）
                "last_equity": portfolio.account.initial_capital,
                "pool_budget": Decimal(0),
                "pool_own_available": Decimal(0),
                "pool_reserve": Decimal(0),
                "pool_borrow_left": Decimal(0),
                "pool_borrow_used": Decimal(0),
                "pool_lent": Decimal(0),
                "pool_borrow_hist": Decimal(0),
                "pool_lent_hist": Decimal(0),
                "pool_total": Decimal(0),
                "pool_days": [],
            })

        def _own_available(portfolio: Portfolio, pending,
                           exclude_ids: Optional[set] = None) -> Decimal:
            """单组合可用虚拟现金 = account.cash − 本组合在途 BUY 金额。

            各组合虚拟现金独立（不互相划转，见 0010 §5.1「不做组合间调仓/资金
            划转」）：共享的只是「同一时刻的可用现金池约束」与同一套审批内核。
            ``exclude_ids``：本 bar 刚产生、尚未审批的新单（它们已进 pending，
            不排除会被自己重复预留一次 → 可用额虚低 → 误判资金不足）。
            """
            reserved = Decimal("0")
            for o in pending:
                if exclude_ids and id(o) in exclude_ids:
                    continue
                if o.trade_type == TradeType.BUY and o.price:
                    reserved += o.price * Decimal(int(o.quantity or 0))
            return max(portfolio.account.cash - reserved, Decimal("0"))

        # ---- 0021 E2：会话池动态额度（默认不进入任何分支）----
        def _as_dec(value, default: str = "0") -> Decimal:
            try:
                return Decimal(str(value))
            except Exception:
                return Decimal(default)

        def _apply_session_pool_day(day) -> None:
            """每个交易日首 bar 分配一次额度（需求=目标仓位−持仓，无前视）。"""
            members = []
            for s in states:
                p = s["portfolio"]
                equity = _as_dec(s.get("last_equity")
                                 or p.account.initial_capital or 0)
                reserve = max(Decimal("0"), equity * _as_dec(_pool_reserve_pct))
                own = _own_available(p, s["pending"])
                own = max(Decimal("0"), own - reserve)
                target = _as_dec(
                    p.target_utilization
                    if p.target_utilization is not None else "0.95")
                target = max(Decimal("0"), min(Decimal("1"), target))
                holdings = p._holdings_value()
                # fixed 仓位模型按初始资金定量（与 _strategy_fund 同口径）；
                # dynamic 按上一日收盘净值，均无前视。
                capital_base = (
                    _as_dec(p.account.initial_capital)
                    if getattr(p, "position_sizing", "dynamic") == "fixed"
                    else equity)
                need = max(Decimal("0"), target * capital_base - holdings)
                s["pool_own_available"] = own
                s["pool_reserve"] = reserve
                s["pool_borrow_used"] = Decimal("0")
                s["pool_lent"] = Decimal("0")
                members.append(PoolMember(
                    p.portfolio_id,
                    raw_weight=need,
                    need=need,
                    utilization_cap=target * capital_base,
                ))

            total = sum((s["pool_own_available"] for s in states), Decimal("0"))
            plan = allocate_session_pool(
                total, members,
                min_share=_pool_min_share,
                max_share=_pool_max_share,
            )
            by_id = {m.portfolio_id: m for m in plan.members}
            for s in states:
                p = s["portfolio"]
                budget = by_id[p.portfolio_id].budget
                s["pool_budget"] = budget
                s["pool_total"] = total
                s["pool_borrow_left"] = max(
                    Decimal("0"), budget - s["pool_own_available"])
                ratios = [_as_dec(getattr(ctx, "capital_ratio", 0))
                          for ctx in p.strategies]
                ratio_sum = sum((max(Decimal("0"), r) for r in ratios),
                                Decimal("0"))
                for idx, ctx in enumerate(p.strategies):
                    if ratio_sum > 0:
                        share = max(Decimal("0"), ratios[idx]) / ratio_sum
                    else:
                        share = (Decimal(1) / Decimal(len(p.strategies))
                                 if p.strategies else Decimal("0"))
                    ctx.capital_budget = budget * share
                s["pool_days"].append({
                    "date": day.isoformat(),
                    "budget": f"{budget:f}",
                    "own_available": f"{s['pool_own_available']:f}",
                    "borrow_limit": f"{s['pool_borrow_left']:f}",
                    "session_total": f"{total:f}",
                })

        def _borrow_cash_for_buy(s, order):
            """成交前从其他组合闲置现金拆借预算内差额；无则返回 None。"""
            p = s["portfolio"]
            qty = int(order.quantity or 0)
            price = order.price
            if qty <= 0 or not price:
                return None
            estimate = _as_dec(price) * qty * Decimal("1.002") + Decimal("5")
            own_before = p.account.cash
            short = estimate - own_before
            if short <= 0:
                return None
            short = min(short, s["pool_borrow_left"])
            if short <= 0:
                return None

            moved = Decimal("0")
            donors = []
            for ds in sorted(states, key=lambda x: str(x["portfolio"].portfolio_id)):
                d = ds["portfolio"]
                if d is p:
                    continue
                available = _own_available(d, ds["pending"])
                available = max(Decimal("0"), available - ds["pool_reserve"])
                take = min(short - moved, available)
                if take <= 0:
                    continue
                d.account.cash -= take
                p.account.cash += take
                moved += take
                ds["pool_lent"] += take
                donors.append((ds, d, take))
                if moved >= short:
                    break

            if moved <= 0:
                return None
            s["pool_borrow_left"] -= moved
            s["pool_borrow_used"] += moved
            recorder.record(
                gate="capital_pool_borrow", layer="capital_pool",
                action="borrow",
                portfolio_id=p.portfolio_id or 0,
                strategy_id=order.strategy_id,
                stock_code=order.stock_code,
                bar_time=t,
                param_name="borrowed_cash",
                param_value=float(moved),
                message="会话池拆借 %.2f 元用于买入（预算内，成交后多借部分退回）"
                        % float(moved),
            )
            return own_before, moved, donors

        def _settle_pool_borrow(s, borrow) -> None:
            """成交后只保留实际占用，多借部分立即原路退回。"""
            own_before, moved, donors = borrow
            p = s["portfolio"]
            spend = own_before + moved - p.account.cash
            borrow_used = (
                max(Decimal("0"), spend - own_before) if spend > 0
                else Decimal("0"))
            borrow_used = min(borrow_used, moved)
            excess = moved - borrow_used
            if excess > 0:
                for ds, d, take in reversed(donors):
                    refund = min(excess, take)
                    d.account.cash += refund
                    p.account.cash -= refund
                    ds["pool_lent"] -= refund
                    excess -= refund
                    if excess <= 0:
                        break
            s["pool_borrow_used"] -= (moved - borrow_used)
            s["pool_borrow_left"] += (moved - borrow_used)

        _pool_day = None

        day_end_steps = set()
        _cur_date = _cur_t = None
        _group_first = None
        for _idx, (_t, _p) in enumerate(times):
            if _t.date() != _cur_date:
                if _group_first is not None:
                    day_end_steps.add(_group_first)
                _cur_date, _cur_t, _group_first = _t.date(), _t, _idx
            elif _t != _cur_t:
                _cur_t, _group_first = _t, _idx
        if _group_first is not None:
            day_end_steps.add(_group_first)

        for i, (t, period) in enumerate(times):
            # K1 协作式取消：任一组记录的取消令牌置位 → 整批中止
            if cancel_check is not None and cancel_check():
                return {
                    "_canceled": True,
                    "_progress": {"done_bars": i, "total_bars": len(times)},
                    "_decisions": [],
                }
            for s in states:
                portfolio = s["portfolio"]
                tracker = (regime_trackers or {}).get(portfolio.portfolio_id)
                if tracker is not None:
                    last_regime = s["last_regime_date"]
                    if last_regime is not None and t.date() != last_regime:
                        prev_bench = (benchmark_data.get(last_regime)
                                      if benchmark_data else None)
                        if prev_bench is not None:
                            tracker.update(last_regime, float(prev_bench))
                    portfolio.current_regime = tracker.label
                    if last_regime is None or t.date() != last_regime:
                        s["last_regime_date"] = t.date()
                if s["last_enter_date"] != t.date():
                    portfolio.risk_manager.enter_day(
                        t.date(), portfolio.account.initial_capital)
                    s["last_enter_date"] = t.date()
                    s["is_first_bar"] = True
                else:
                    s["is_first_bar"] = False
                if (portfolio.corporate_timeline is not None
                        and s["last_action_date"] != t.date()):
                    _ca_actions = apply_corporate_day(
                        portfolio, portfolio.corporate_timeline, t.date(),
                        recorder, mode=_ca_mode(portfolio),
                        reinvest_enabled=portfolio.dividend_reinvest_enabled,
                        price_resolver=lambda code: self._ca_reference_price(
                            code, t, period, price_index, prev_close_by_day),
                        dispose_days=portfolio.suspension_dispose_days,
                        delist_force_close=portfolio.delist_force_close_enabled,
                        last_action_date=s["last_action_date"],
                        next_trade_day=_next_trade_day,
                    )
                    s["last_action_date"] = t.date()
                    s["ca_reinvest"].extend(
                        a for a in _ca_actions if a.kind == KIND_REINVEST)
                    self._apply_corporate_actions(
                        portfolio, _ca_actions, t, s["trades"], recorder)

            if _pool_enabled and _pool_day != t.date():
                _apply_session_pool_day(t.date())
                _pool_day = t.date()

            # 1. 成交上一 bar 的在途单（共享价格表；每个组合各自 pending）
            deferred_by_state = []
            bar_open_prices = self._bar_open_prices(price_index, t, period)
            bar_volumes = {
                code: ohlcv.get("volume")
                for code, ohlcv in price_index.get((t, period), {}).items()
            }
            for s in states:
                deferred: List[OrderEvent] = []
                portfolio = s["portfolio"]
                # 本组合是否启用 sqrt 容量模型（run 级开关，但按组合判定以兼容混跑）
                _pf_capacity = (
                    capacity
                    if getattr(portfolio, "capacity_model", "linear") == "sqrt"
                    else None)
                deferred_by_state.append(deferred)
                if not s["pending"]:
                    continue
                bar_price_limits: Dict[str, tuple] = {}
                if portfolio.price_limit_check_enabled:
                    for _pending in s["pending"]:
                        _code = _pending.stock_code
                        if _code in bar_price_limits or _code not in bar_open_prices:
                            continue
                        _pc = prev_close_by_day.get((_code, t.date()))
                        if _pc is None or _pc < Decimal("1"):
                            continue
                        _rule = _rule_cache.get((_code, t.date()))
                        if _rule is None:
                            _rule = self._limit_rule_for(
                                _code, t, stock_names, first_bar_date,
                                run_first_date, _truncation_warned)
                            _rule_cache[(_code, t.date())] = _rule
                        if _rule.ratio is None:
                            continue
                        # P1-2（2026-11-23）：夹价前自洽守卫——只诊断，不改夹价。
                        _ohlcv = (price_index.get((t, period), {}) or {}).get(_code) or {}
                        check_bar_consistent_with_prev_close(
                            code=_code, high=_ohlcv.get("high"),
                            low=_ohlcv.get("low"), prev_close=_pc,
                            ratio=_rule.ratio)
                        bar_price_limits[_code] = (
                            (_pc * (1 - _rule.ratio)).quantize(
                                Decimal("0.01"), rounding=ROUND_HALF_UP),
                            (_pc * (1 + _rule.ratio)).quantize(
                                Decimal("0.01"), rounding=ROUND_HALF_UP),
                        )
                dispatcher = SimulatedDispatcher(
                    bar_open_prices,
                    bar_volumes=(
                        bar_volumes
                        if (portfolio.adaptive_slippage_enabled
                            or _pf_capacity is not None) else None),
                    slippage_impact_coef=(
                        portfolio.capacity_impact_k if _pf_capacity is not None
                        else (portfolio.slippage_impact_coef
                              if portfolio.adaptive_slippage_enabled else None)),
                    price_limits=bar_price_limits or None,
                    impact_model=("sqrt" if _pf_capacity is not None else "linear"),
                    impact_base=portfolio.capacity_impact_base,
                    impact_cap=portfolio.capacity_impact_cap,
                    **portfolio.cost_params,
                )
                engine = ExecutionEngine(
                    dispatcher, SimulatedT1Checker(), recorder=recorder,
                    lot_size_of=lambda code: lot_size_for(
                        code, portfolio.bond_lot_enabled),
                    stats=run_stats,
                )
                for order in s["pending"]:
                    if (pending_order_max_days and pending_order_max_days > 0
                            and order.pending_since is not None
                            and trading_days_between(
                                order.pending_since, t.date())
                            >= pending_order_max_days):
                        recorder.record(
                            gate="pending_order_expired", layer="execution",
                            action="cancel",
                            portfolio_id=order.portfolio_id or 0,
                            strategy_id=order.strategy_id,
                            stock_code=order.stock_code, bar_time=t,
                            message="在途单超过最大存活期（%d 个交易日）：作废，"
                                    "不再顺延" % pending_order_max_days)
                        continue
                    risk_sell_original = None
                    if order.period is not None and order.period != period:
                        deferred.append(order)
                        continue
                    if (order.signal_bar_date is not None
                            and t.date() <= order.signal_bar_date):
                        deferred.append(order)
                        continue
                    if not bar_open_prices.get(order.stock_code):
                        if id(order) not in no_price_recorded:
                            no_price_recorded.add(id(order))
                            recorder.record(
                                gate="no_market_data", layer="execution",
                                action="defer",
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code, bar_time=t,
                                message="停牌/无行情：本 bar 无开盘价，订单延后至"
                                        "下一有价 bar 重试")
                        deferred.append(order)
                        continue
                    if portfolio.price_limit_check_enabled:
                        prev_close = prev_close_by_day.get(
                            (order.stock_code, t.date()))
                        if prev_close is not None and prev_close >= Decimal("1"):
                            open_px = bar_open_prices[order.stock_code]
                            _rule_key = (order.stock_code, t.date())
                            rule = _rule_cache.get(_rule_key)
                            if rule is None:
                                rule = self._limit_rule_for(
                                    order.stock_code, t, stock_names,
                                    first_bar_date, run_first_date,
                                    _truncation_warned)
                                _rule_cache[_rule_key] = rule
                            ratio = rule.ratio
                            if ratio is not None:
                                limit_up = (prev_close * (1 + ratio)).quantize(
                                    Decimal("0.01"), rounding=ROUND_HALF_UP)
                                limit_down = (prev_close * (1 - ratio)).quantize(
                                    Decimal("0.01"), rounding=ROUND_HALF_UP)
                                is_buy = order.trade_type == TradeType.BUY
                                if ((is_buy and open_px >= limit_up)
                                        or (not is_buy and open_px <= limit_down)):
                                    if id(order) not in price_limit_recorded:
                                        price_limit_recorded.add(id(order))
                                        recorder.record(
                                            gate="price_limit", layer="execution",
                                            action="defer",
                                            portfolio_id=order.portfolio_id or 0,
                                            strategy_id=order.strategy_id,
                                            stock_code=order.stock_code,
                                            bar_time=t,
                                            message="涨跌停约束：开盘价 %s 触及限制"
                                                    "（%s），订单延后至下一 bar"
                                                    % (open_px, rule.source))
                                    deferred.append(order)
                                    continue
                    partial_residual = 0
                    # 层 1 容量模型（sqrt）：与单组合同一口径，预算实例在组合间共享
                    # → 同日同票跨策略/跨组合自动聚合。
                    capacity_residual = 0
                    if _pf_capacity is not None:
                        _row = price_index.get((t, period), {}).get(
                            order.stock_code) or {}
                        _is_buy = order.trade_type == TradeType.BUY
                        _requested = order.quantity
                        _allowed, _why = _pf_capacity.allow(
                            order.stock_code, t, _row.get("volume"),
                            _row.get("close"), _requested,
                            lot_size_for(order.stock_code,
                                         portfolio.bond_lot_enabled)
                            if _is_buy else 1,
                        )
                        if _allowed < _requested:
                            recorder.record(
                                gate="capacity_slice", layer="execution",
                                action=("shrink" if _allowed > 0 else "defer"),
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code,
                                bar_time=t,
                                requested_qty=_requested,
                                final_qty=_allowed,
                                param_name=_why,
                                message="容量约束（%s）：请求 %d 股，本 bar 可成交 %d 股，"
                                        "残量顺延（TTL %d 根 bar）"
                                        % ({"bar": "本 bar 成交量",
                                            "day": "当日累计成交量",
                                            "5d": "5 日累计成交量",
                                            "no_volume": "无成交量"}.get(_why, _why),
                                           _requested, _allowed,
                                           portfolio.capacity_slice_ttl_bars),
                            )
                            if _allowed <= 0:
                                if (order.capacity_carry_bars + 1
                                        >= portfolio.capacity_slice_ttl_bars):
                                    recorder.record(
                                        gate="capacity_slice_expired",
                                        layer="execution", action="cancel",
                                        portfolio_id=order.portfolio_id or 0,
                                        strategy_id=order.strategy_id,
                                        stock_code=order.stock_code,
                                        bar_time=t,
                                        requested_qty=_requested,
                                        param_name="capacity_slice_ttl_bars",
                                        param_value=float(
                                            portfolio.capacity_slice_ttl_bars),
                                        message="容量约束顺延超过 %d 根 bar 仍未成交，"
                                                "订单作废" % portfolio.capacity_slice_ttl_bars,
                                    )
                                    continue
                                deferred.append(replace(
                                    order,
                                    capacity_carry_bars=order.capacity_carry_bars + 1,
                                ))
                                continue
                            if (not _is_buy
                                    and order.signal_type in _RISK_SELL_TYPES):
                                risk_sell_original = _requested
                            else:
                                capacity_residual = _requested - _allowed
                            order.quantity = _allowed
                    elif portfolio.volume_limit_enabled:
                        if _liquidity_caliber == "prev5d_avg":
                            # 显式选择的新口径：成交日之前 5 个交易日日均量（股），
                            # 不含当日、无前视；无历史 → fail-open。
                            ref_volume = volume_ref_by_day.get(
                                (order.stock_code, t.date()))
                        else:
                            # 默认口径（与改动前逐字一致）：成交当根 bar 的成交量
                            ref_volume = bar_volumes.get(order.stock_code)
                        if ref_volume:
                            cap_shares = int(
                                portfolio.volume_limit_ratio
                                * Decimal(int(ref_volume)))
                            if order.quantity > cap_shares:
                                vol_lot = lot_size_for(
                                    order.stock_code, portfolio.bond_lot_enabled)
                                is_buy = order.trade_type == TradeType.BUY
                                final_qty = ((cap_shares // vol_lot) * vol_lot
                                             if is_buy else cap_shares)
                                if final_qty >= vol_lot or (
                                        not is_buy and final_qty > 0):
                                    if (not is_buy
                                            and order.signal_type in _RISK_SELL_TYPES):
                                        risk_sell_original = order.quantity
                                    if _partial_fill and is_buy:
                                        partial_residual = (
                                            order.quantity - final_qty)
                                    order.quantity = final_qty
                                else:
                                    if id(order) not in volume_defer_recorded:
                                        volume_defer_recorded.add(id(order))
                                        if _liquidity_caliber == "prev5d_avg":
                                            _msg = (
                                                "成交量约束：近5日均量口径上限不足一手，"
                                                "订单延后至量足 bar（近5日均量 %s 股 ×%s）"
                                                % (ref_volume,
                                                   portfolio.volume_limit_ratio))
                                        else:
                                            _msg = ("成交量约束：本 bar 上限不足一手，"
                                                    "订单延后至量足 bar")
                                        recorder.record(
                                            gate="volume_cap_defer",
                                            layer="execution", action="defer",
                                            portfolio_id=order.portfolio_id or 0,
                                            strategy_id=order.strategy_id,
                                            stock_code=order.stock_code,
                                            bar_time=t,
                                            message=_msg)
                                    deferred.append(order)
                                    continue
                    order.bar_time = t
                    ctx = portfolio.find_strategy(order.strategy_id)
                    if ctx is None:
                        continue
                    pos = ctx.positions.get(order.stock_code)
                    if pos is None and order.trade_type.value == "BUY":
                        pos = Position(
                            order.stock_code,
                            same_day_sellable=(
                                portfolio.bond_t0_enabled
                                and is_convertible_bond(order.stock_code)),
                            lot_size=lot_size_for(
                                order.stock_code, portfolio.bond_lot_enabled),
                        )
                        ctx.positions[order.stock_code] = pos
                    # 各组合虚拟现金默认独立；仅在 E2 显式开启时按预算临时拆借
                    _pool_borrow = (
                        _borrow_cash_for_buy(s, order)
                        if _pool_enabled and order.trade_type == TradeType.BUY
                        else None)
                    trade = engine.execute(
                        order, portfolio.account, pos,
                        **self._exposure_kwargs(portfolio, ctx))
                    if _pool_borrow is not None:
                        _settle_pool_borrow(s, _pool_borrow)
                    if trade is not None:
                        s["trades"].append(trade)
                        portfolio.note_trade(trade)
                    if risk_sell_original is not None and trade is not None:
                        remaining = (int(risk_sell_original)
                                     - int(getattr(trade, "quantity", 0) or 0))
                        if remaining > 0:
                            order.quantity = remaining
                            deferred.append(order)
                    if (_partial_fill and trade is not None
                            and partial_residual > 0):
                        deferred.append(replace(order, quantity=partial_residual))
                    # 层 1 容量模型：实际成交量记账 + 残量顺延（TTL 超期作废）
                    if _pf_capacity is not None and trade is not None:
                        _pf_capacity.consume(
                            order.stock_code, t,
                            int(getattr(trade, "quantity", 0) or 0))
                    if capacity_residual > 0 and trade is not None:
                        if (order.capacity_carry_bars + 1
                                >= portfolio.capacity_slice_ttl_bars):
                            recorder.record(
                                gate="capacity_slice_expired", layer="execution",
                                action="cancel",
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code,
                                bar_time=t,
                                requested_qty=capacity_residual,
                                param_name="capacity_slice_ttl_bars",
                                param_value=float(portfolio.capacity_slice_ttl_bars),
                                message="容量切片残量 %d 股顺延超过 %d 根 bar，作废"
                                        % (capacity_residual,
                                           portfolio.capacity_slice_ttl_bars),
                            )
                        else:
                            deferred.append(replace(
                                order, quantity=capacity_residual,
                                capacity_carry_bars=order.capacity_carry_bars + 1,
                            ))
                            recorder.record(
                                gate="capacity_slice", layer="execution",
                                action="carry",
                                portfolio_id=order.portfolio_id or 0,
                                strategy_id=order.strategy_id,
                                stock_code=order.stock_code,
                                bar_time=t,
                                requested_qty=capacity_residual,
                                message="容量约束部分成交，残量 %d 股顺延至后续 bar 续成交"
                                        % capacity_residual,
                            )

            bar = self._build_bar(price_index, t, period)

            # 3. on_bar：各组合用「共享可用额」审批，随后统一逐笔分配
            for idx, s in enumerate(states):
                portfolio = s["portfolio"]
                if not session_allows_bar(
                        portfolio.trading_session, t.time(), period):
                    # 与 run 的 else 分支一致：时段外只保留未成交单，不重复计数已成交单
                    s["pending"] = deferred_by_state[idx]
                    continue
                deferred = deferred_by_state[idx]
                inflight_risk_sells: Dict[int, set] = {}
                for _order in deferred:
                    if (_order.trade_type == TradeType.SELL
                            and _order.signal_type in _RISK_SELL_TYPES):
                        inflight_risk_sells.setdefault(
                            _order.strategy_id, set()).add(_order.stock_code)
                # 各组合用**自己的**虚拟现金做仓位/审批（不临时借用共享池，
                # 避免按整池规模放大每家的目标仓位）
                new_orders = portfolio.on_bar(
                    bar, signal_cache=signal_cache, period=period,
                    inflight_risk_sells=inflight_risk_sells)
                if _first_bar_no_fill and s["is_first_bar"] and new_orders:
                    new_orders = [o for o in new_orders
                                  if o.trade_type != TradeType.BUY]
                for _o in new_orders:
                    if _o.pending_since is None:
                        _o.pending_since = t.date()
                for _o in deferred:
                    _o.pending_since = _o.pending_since or t.date()
                s["pending"] = deferred + list(new_orders)
                s["new_orders"] = list(new_orders)

            # 3b. 逐组合资金审批（同一内核 cash_allocator）：按状态顺序（= 用户
            #     给出的组合顺序）串行处理，每个组合用「自己的可用虚拟现金」。
            #     组级共享约束：Σ 审批通过的买入金额 ≤ Σ 各组合可用现金（各组合均
            #     不超自身可用，故恒成立；是「同一时刻可用现金池」的形式化保证）。
            lot_of = lambda code: lot_size_for(
                code, any(s["portfolio"].bond_lot_enabled for s in states))
            for idx, s in enumerate(states):
                portfolio = s["portfolio"]
                own_new = [
                    o for o in s.get("new_orders", [])
                    if o.trade_type == TradeType.BUY
                    and o.price is not None and o.price > 0
                    and int(o.quantity or 0) > 0]
                if not own_new:
                    continue
                avail = _own_available(
                    portfolio, s["pending"], {id(o) for o in own_new})
                if _pool_enabled:
                    # E2：审批可用额 = 自有可用 + 会话池预算内可借额
                    avail += s["pool_borrow_left"]
                decisions = allocate_batch_buys(
                    own_new, avail, lot_of=lot_of,
                    sort_key=lambda o: (o.bar_time or datetime.min,),
                )
                rejected = {
                    id(d.order): d for d in decisions if not d.accepted}
                kept = []
                for o in s.get("new_orders", []):
                    d = rejected.get(id(o))
                    if d is None:
                        kept.append(o)
                        continue
                    recorder.record(
                        gate="insufficient_cash", layer="execution",
                        action="skip",
                        portfolio_id=portfolio.portfolio_id,
                        strategy_id=o.strategy_id,
                        stock_code=o.stock_code, bar_time=t,
                        param_name="available_cash",
                        param_value=float(d.target_share or 0),
                        actual_value=float(d.available_before or 0),
                        requested_qty=d.original_qty,
                        final_qty=0,
                        message="可用资金不足：目标 %s，实际可买 %s，低于目标"
                                " %d%% 门槛，本笔不下单"
                                % (fmt_money(d.target_share),
                                   fmt_money(d.available_before),
                                   int(MIN_FILL_RATIO * 100)))
                s["pending"] = deferred_by_state[idx] + kept
            for s in states:
                s.pop("new_orders", None)

            # 4. 日终快照 / 再投资 / 熔断（逐组合）
            for s in states:
                portfolio = s["portfolio"]
                total_value = portfolio.total_value(bar)
                is_day_end = i in day_end_steps
                if is_day_end and s["ca_reinvest"]:
                    self._execute_ca_reinvest(
                        portfolio, s["ca_reinvest"], bar, t, s["trades"],
                        recorder)
                    s["ca_reinvest"] = []
                snap = None
                day_strategy_snaps = []
                if is_day_end:
                    # 4e. equity_cap 复利收口（mode=distribute，默认 cap 无操作）
                    total_value = self._apply_equity_cap_distribution(
                        portfolio, total_value, t, recorder)
                    snap = portfolio.snapshot(t.date(), total_value, bar)
                    snap["benchmark_value"] = (
                        benchmark_data.get(t.date()) if benchmark_data else None)
                    gate = (gate_trackers or {}).get(portfolio.portfolio_id)
                    if gate is not None:
                        snap["gate_state"] = gate.state_for(t.date())
                        snap["gate_strength"] = gate.strength_for(t.date())
                    day_strategy_snaps = self._strategy_snapshots(
                        portfolio, bar, t.date())
                    for s_snap in day_strategy_snaps:
                        s_snap["benchmark_value"] = snap["benchmark_value"]
                    if _pool_enabled:
                        snap["capital_pool"] = {
                            "version": CAPITAL_POOL_MULTI_VERSION,
                            "session_total": f"{s['pool_total']:f}",
                            "budget": f"{s['pool_budget']:f}",
                            "own_available": f"{s['pool_own_available']:f}",
                            "borrowed": f"{s['pool_borrow_used']:f}",
                            "lent": f"{s['pool_lent']:f}",
                        }
                        s["pool_borrow_hist"] += s["pool_borrow_used"]
                        s["pool_lent_hist"] += s["pool_lent"]
                        s["last_equity"] = total_value
                portfolio.maybe_cash_enhancement(bar, total_value)
                if (s["last_snap_date"] is not None
                        and t.date() != s["last_snap_date"]):
                    s["snapshots"].append(s["pending_snap"])
                    for s_snap in s["pending_strategy_snaps"]:
                        s["strategy_snapshots"][s_snap["target_id"]].append(s_snap)
                s["last_snap_date"] = t.date()
                if is_day_end:
                    s["pending_snap"] = snap
                    s["pending_strategy_snaps"] = day_strategy_snaps
                allow_peak_update = (
                    is_day_end if period in _COARSE_PERIODS else True)
                if run_options.get("daily_close_peak_enabled"):
                    allow_peak_update = is_day_end
                portfolio.risk_manager.update_peak(
                    total_value, t.date(), allow_peak_update=allow_peak_update)

        results: Dict[int, dict] = {}
        for s in states:
            portfolio = s["portfolio"]
            if s["pending_snap"] is not None:
                s["snapshots"].append(s["pending_snap"])
                for s_snap in s["pending_strategy_snaps"]:
                    s["strategy_snapshots"][s_snap["target_id"]].append(s_snap)
            portfolio.risk_manager.finalize_last_day(
                portfolio.account.initial_capital)
            # 与 run 同口径的评估：组合层 + 策略层（策略快照 < 2 根跳过）
            bench_series = (
                [{"value": x["benchmark_value"]} for x in s["snapshots"]
                 if x.get("benchmark_value") is not None] or None)
            evaluations = Evaluator().evaluate(
                s["snapshots"], benchmark_data=bench_series, trades=s["trades"])
            strategy_evaluations: Dict[int, dict] = {}
            for ctx in portfolio.strategies:
                s_snaps = s["strategy_snapshots"].get(ctx.strategy_id, [])
                if len(s_snaps) < 2:
                    continue
                strategy_evaluations[ctx.strategy_id] = Evaluator().evaluate(
                    s_snaps, benchmark_data=bench_series,
                    trades=[t for t in s["trades"]
                            if t.strategy_id == ctx.strategy_id])
            results[portfolio.portfolio_id] = {
                "trades": s["trades"],
                "snapshots": s["snapshots"],
                "strategy_snapshots": s["strategy_snapshots"],
                "evaluations": evaluations,
                "strategy_evaluations": strategy_evaluations,
                "pending_orders": s["pending"],
                "pending_order_count": len(s["pending"]),
            }
            if _pool_enabled:
                results[portfolio.portfolio_id]["capital_pool"] = {
                    "version": CAPITAL_POOL_MULTI_VERSION,
                    "days": s["pool_days"],
                    "borrowed_total": f"{s['pool_borrow_hist']:f}",
                    "lent_total": f"{s['pool_lent_hist']:f}",
                }
                for ctx in portfolio.strategies:
                    ctx.capital_budget = None
        results["_decisions"] = [ev.to_dict() for ev in recorder.drain()]
        results["execution_stats"] = run_stats
        return results

    def _exposure_kwargs(self, portfolio: Portfolio, ctx) -> dict:
        """#3 策略级敞口上限（可选，默认关）：开启时给出 execute 的两个 strategy_* 参数。

        上限 = capital_ratio × 组合初始资金；策略持仓市值用 _price_snapshot 估值
        （上一 bar 已刷新，口径与审批用的信号 bar 收盘一致）。关闭返回空 dict，
        execute 走默认 None（行为与旧逻辑一致）。实盘 LiveEngine 不调此方法——
        其 _handle_bar 持有同式 _exposure_kwargs 透传（B-2 敞口实盘化）。

        同时携带策略级单笔最大买入金额（可选 None=不限，照 exposure 同式透传，
        回测/实盘双端 cap_quantity 生效）。
        """
        kwargs = {}
        if getattr(ctx, "exposure_cap_enabled", False):
            kwargs["strategy_position_value"] = portfolio.holdings_value(ctx)
            kwargs["strategy_capital_limit"] = (
                ctx.capital_ratio * portfolio.account.initial_capital
            )
        _moa = getattr(ctx, "max_order_amount", None)
        # 层 1：sqrt 容量模型下，未显式配置单笔上限时按「20 × 单仓目标」兜底，
        # 语义为异常保护（正常单仓目标 = single_open_ratio × 策略资金），不再
        # 充当固定 50 万级别的硬闸门；显式配置仍以配置为准。
        if _moa is None and getattr(portfolio, "capacity_model", "linear") == "sqrt":
            _target = Decimal(str(ctx.single_open_ratio)) * portfolio._strategy_fund(ctx)
            _moa = _target * Decimal("20")
        kwargs["max_order_amount"] = (
            Decimal(str(_moa)) if _moa is not None else None
        )
        return kwargs

    def _build_capacity_budget(self, portfolio, klines):
        """层 1：capacity_model == "sqrt" 时建容量预算，否则返回 None（旧行为）。"""
        if portfolio is None or getattr(portfolio, "capacity_model", "linear") != "sqrt":
            return None
        return CapacityBudget(
            klines,
            daily_small=portfolio.capacity_daily_ratio_small,
            daily_large=portfolio.capacity_daily_ratio_large,
            five_day_small=portfolio.capacity_5d_ratio_small,
            five_day_large=portfolio.capacity_5d_ratio_large,
            large_turnover=portfolio.capacity_large_turnover,
            ttl_bars=portfolio.capacity_slice_ttl_bars,
        )

    def _build_timeline(self, klines: dict) -> List[tuple]:
        """合并所有股票所有周期的时间轴，返回排序去重的 (datetime, period) 列表。

        W4 周期路由：同一 datetime 下不同周期分别成步（不再合并为一个 bar）。
        同 t 内按固定周期顺序（细→粗：1m > 5m > 15m > 30m > 1h > 1d > 1w > 1mon，
        与实盘 periods_on_boundary 同序，见 `_PERIOD_ORDER` 注释），纯排序确定性，
        避免同 00:00 的 1d/1w/1mon 步序随集合遍历次序抖动。00:00 那一步只含
        日线及以上周期（1d→1w→1mon），分钟级 bar 不会落在 00:00，故细→粗改动
        不改变日线 `signal_bar_date` 闸与周期路由。
        """
        pairs = set()
        for _stock_code, periods in klines.items():
            for period, df in periods.items():
                if "datetime" not in df.columns:
                    continue
                for t in df["datetime"].to_list():
                    pairs.add((t, period))
        return sorted(pairs, key=lambda p: (p[0], _PERIOD_ORDER.get(p[1], 99)))

    def _build_price_index(self, klines: dict) -> Dict[tuple, Dict[str, dict]]:
        """预建 {(datetime, period): {stock_code: {open..volume}}} 索引。

        一次 O(total_rows) 扫描替代逐 bar 的 O(n²) polars df.filter()，将每 bar
        的 FFI 查表降为 O(1) 字典查找。

        W4：键含 period，1d/1w/1mon 同为 00:00 不再互相覆盖（原 #5 粗粒度碰撞
        问题由分键天然消解，故删除 _COARSE_PERIODS 特判）。每步只取当前周期的行，
        该周期策略只见本周期股票与价格。
        """
        index: Dict[tuple, Dict[str, dict]] = {}
        for stock_code, periods in klines.items():
            for period, df in periods.items():
                if "datetime" not in df.columns:
                    continue
                if "Open" not in df.columns:
                    continue
                times = df["datetime"].to_list()
                opens = df["Open"].to_list()
                highs = df["High"].to_list()
                lows = df["Low"].to_list()
                closes = df["Close"].to_list()
                volumes = df["Volume"].to_list()
                for i, t in enumerate(times):
                    row = {
                        "open": opens[i],
                        "high": highs[i],
                        "low": lows[i],
                        "close": closes[i],
                        "volume": volumes[i],
                    }
                    index.setdefault((t, period), {})[stock_code] = row
        return index

    # ---------- 公司行为（J1/J2，默认未启用） ----------

    def _ca_reference_price(self, code, t, period, price_index, prev_close_by_day):
        """分红比例口径的参照前收：优先上一交易日收盘，缺失退回当根 bar 收盘。"""
        pc = prev_close_by_day.get((code, t.date()))
        if pc is not None and pc > 0:
            return pc
        row = price_index.get((t, period), {}).get(code) or {}
        close = row.get("close")
        return close if close and close > 0 else None

    def _ca_cost_params(self, portfolio):
        cp = portfolio.cost_params or {}

        def _d(key, default):
            value = cp.get(key)
            if value is None:
                return default
            return value if isinstance(value, Decimal) else Decimal(str(value))

        return {
            "min_commission": _d("min_commission", Decimal("5")),
            "buy_rate": _d("buy_commission_rate", Decimal("0.00025")),
            "sell_rate": _d("sell_commission_rate", Decimal("0.00025")),
            "stamp_rate": _d("stamp_duty_rate", Decimal("0.0005")),
            "transfer_rate": _d("transfer_fee_rate", Decimal("0.00001")),
        }

    def _ca_fees(self, portfolio, amount, is_sell, day):
        from .execution_engine import stamp_duty_rate_on
        p = self._ca_cost_params(portfolio)
        commission = max(p["min_commission"],
                         amount * (p["sell_rate"] if is_sell else p["buy_rate"]))
        stamp = amount * stamp_duty_rate_on(day, p["stamp_rate"]) if is_sell else Decimal("0")
        transfer = amount * p["transfer_rate"]
        return (commission.quantize(Decimal("0.01")),
                stamp.quantize(Decimal("0.01")),
                transfer.quantize(Decimal("0.01")))

    def _apply_corporate_actions(self, portfolio, actions, t, trades, recorder):
        """落账内核发出的意图：送转/分红已在内核改完，这里只做退市强平与审计事件。"""
        for act in actions:
            if act.kind == KIND_DELIST_CLOSE:
                self._execute_ca_force_close(portfolio, act, t, trades, recorder)
            elif act.kind == KIND_SKIP_CASH:
                recorder.record(
                    gate="corporate_action", layer="execution", action="skip",
                    portfolio_id=portfolio.portfolio_id,
                    strategy_id=act.strategy_id, stock_code=act.code, bar_time=t,
                    message="现金分红未生效（%s）" % act.reason)
            elif act.kind == KIND_SPLIT:
                recorder.record(
                    gate="corporate_action", layer="execution", action="apply",
                    portfolio_id=portfolio.portfolio_id,
                    strategy_id=act.strategy_id, stock_code=act.code, bar_time=t,
                    message="送转调整股数 +%d（%s）" % (act.qty, act.reason))
            elif act.kind == "dividend_cash":
                recorder.record(
                    gate="corporate_action", layer="execution", action="apply",
                    portfolio_id=portfolio.portfolio_id,
                    strategy_id=act.strategy_id, stock_code=act.code, bar_time=t,
                    message="现金分红入账 %s 元（%s）" % (act.cash, act.reason))
            elif act.kind == "notice":
                recorder.record(
                    gate="corporate_action", layer="execution", action="notice",
                    portfolio_id=portfolio.portfolio_id,
                    strategy_id=act.strategy_id, stock_code=act.code, bar_time=t,
                    message=act.reason)
            elif act.kind == KIND_SUSPEND:
                recorder.record(
                    gate="corporate_action", layer="execution", action="block",
                    portfolio_id=portfolio.portfolio_id, stock_code=act.code,
                    bar_time=t, message="停牌期间禁止开仓/加仓（%s）" % act.reason)

    def _execute_ca_force_close(self, portfolio, act, t, trades, recorder):
        """退市/长期停牌强平：按最后可交易价卖出整仓（无价退回成本）。"""
        ctx = portfolio.find_strategy(act.strategy_id)
        if ctx is None:
            return
        pos = ctx.positions.get(act.code)
        if pos is None or pos.quantity <= 0:
            return
        qty = min(int(act.qty or 0), pos.quantity)
        if qty <= 0:
            return
        price = act.price if act.price and act.price > 0 else pos.avg_cost
        if price is None or price <= 0:
            recorder.record(
                gate="corporate_action", layer="execution", action="skip",
                portfolio_id=portfolio.portfolio_id, strategy_id=act.strategy_id,
                stock_code=act.code, bar_time=t,
                message="强制了结跳过：无可用的成交价（%s）" % act.reason)
            return
        amount = price * qty
        commission, stamp, transfer = self._ca_fees(portfolio, amount, True, t.date())
        trade = TradeEvent(
            strategy_id=act.strategy_id,
            portfolio_id=portfolio.portfolio_id,
            stock_code=act.code, trade_type=TradeType.SELL, price=price,
            quantity=qty, amount=amount, commission=commission,
            stamp_duty=stamp, transfer_fee=transfer, ref_price=price, trade_time=t,
            signal_type=SignalType.CLOSE, signal_name="corporate_action",
        )
        applied = pos.apply_trade(trade)
        account_trade = trade.scaled_to(applied) if applied != qty else trade
        portfolio.account.apply_trade(account_trade)
        trades.append(account_trade)
        portfolio.note_trade(account_trade)
        recorder.record(
            gate="corporate_action", layer="execution", action="trigger",
            portfolio_id=portfolio.portfolio_id, strategy_id=act.strategy_id,
            stock_code=act.code, bar_time=t, requested_qty=qty, final_qty=applied,
            message="强制了结 %d 股 @ %s（%s）" % (applied, price, act.reason))

    def _apply_equity_cap_distribution(self, portfolio, total_value, t, recorder=None):
        """equity_cap_mode=distribute：日终把「总资产 − cap」作为外部流出扣出现金。

        只在设了 cap 且 mode=distribute 时生效；cap 模式（默认）直接原值返回，
        保证「不设 cap / cap 模式」的结果与改动前逐位一致。

        边界：只扣可用现金，不强制减仓——市值本身超过 cap 或现金不足时，按
        min(超出额, 现金) 出金（最多扣到现金为 0），不产生卖单；出金记一条
        equity_cap 决策事件便于审计（不新增流水表，按约定押后）。
        """
        cap = getattr(portfolio, "equity_cap", None)
        mode = getattr(portfolio, "equity_cap_mode", "cap") or "cap"
        if cap is None or mode != "distribute":
            return total_value
        excess = Decimal(total_value) - Decimal(cap)
        if excess <= 0:
            return total_value
        cash = portfolio.account.cash
        withdraw = min(excess, cash)
        if withdraw <= 0:
            if recorder is not None and bool(recorder):
                recorder.record(
                    gate="equity_cap", layer="capital_gate", action="skip",
                    portfolio_id=portfolio.portfolio_id,
                    bar_time=t, param_name="equity_cap",
                    param_value=float(cap), actual_value=float(total_value),
                    message=("权益上限：总资产超上限但无可分配现金，"
                             "存量市值不强制减仓，本日不出金"),
                )
            return total_value
        portfolio.account.cash = cash - withdraw
        if recorder is not None and bool(recorder):
            recorder.record(
                gate="equity_cap", layer="capital_gate", action="distribute",
                portfolio_id=portfolio.portfolio_id,
                bar_time=t, param_name="equity_cap",
                param_value=float(cap), actual_value=float(total_value),
                message=("权益上限出金：总资产 %s 超上限 %s，"
                         "现金扣出 %s 作为外部流出"
                         % (fmt_money(total_value), fmt_money(cap),
                            fmt_money(withdraw))),
            )
        return total_value - withdraw

    def _execute_ca_reinvest(self, portfolio, intents, bar, t, trades, recorder):
        """分红再投资：当日收盘价买入整手，不足一手（或现金不够）留现金并记事件。"""
        for act in intents:
            ctx = portfolio.find_strategy(act.strategy_id)
            if ctx is None:
                continue
            row = bar.stocks.get(act.code) or {}
            price = row.get("close")
            if price is None or price <= 0:
                recorder.record(
                    gate="corporate_action", layer="execution", action="skip_lot",
                    portfolio_id=portfolio.portfolio_id, strategy_id=act.strategy_id,
                    stock_code=act.code, bar_time=t,
                    message="分红再投资跳过：当日无有效收盘价")
                continue
            lot = lot_size_for(act.code, portfolio.bond_lot_enabled)
            cash = act.cash
            qty = int(cash / price / lot) * lot
            while qty >= lot:
                amount = price * qty
                commission, _stamp, transfer = self._ca_fees(
                    portfolio, amount, False, t.date())
                if amount + commission + transfer <= cash:
                    break
                qty -= lot
            if qty < lot:
                recorder.record(
                    gate="corporate_action", layer="execution", action="skip_lot",
                    portfolio_id=portfolio.portfolio_id, strategy_id=act.strategy_id,
                    stock_code=act.code, bar_time=t,
                    message="分红再投资跳过：现金 %s 不足一手（价 %s ×%d）"
                            % (cash, price, lot))
                continue
            amount = price * qty
            commission, _stamp, transfer = self._ca_fees(
                portfolio, amount, False, t.date())
            pos = ctx.positions.get(act.code)
            if pos is None:
                pos = Position(
                    act.code,
                    same_day_sellable=(
                        portfolio.bond_t0_enabled and is_convertible_bond(act.code)),
                    lot_size=lot,
                )
                ctx.positions[act.code] = pos
            trade = TradeEvent(
                strategy_id=act.strategy_id,
                portfolio_id=portfolio.portfolio_id,
                stock_code=act.code, trade_type=TradeType.BUY, price=price,
                quantity=qty, amount=amount, commission=commission,
                stamp_duty=Decimal("0"), transfer_fee=transfer, ref_price=price,
                trade_time=t,
                signal_type=SignalType.OPEN, signal_name="corporate_action_reinvest",
            )
            pos.apply_trade(trade)
            portfolio.account.apply_trade(trade)
            trades.append(trade)
            portfolio.note_trade(trade)
            recorder.record(
                gate="corporate_action", layer="execution", action="apply",
                portfolio_id=portfolio.portfolio_id, strategy_id=act.strategy_id,
                stock_code=act.code, bar_time=t, requested_qty=qty,
                message="分红再投资买入 %d 股 @ %s" % (qty, price))

    def _bar_open_prices(self, price_index: dict, t: datetime, period: str) -> Dict[str, Decimal]:
        """O(1) 查表取 (t, period) 下所有股票的 open 价。"""
        row = price_index.get((t, period), {})
        return {code: data["open"] for code, data in row.items()}

    def _limit_rule_for(self, code: str, t: datetime, stock_names, first_bar_date,
                        run_first_date, truncation_warned):
        """解析 (code, 交易日) 的涨跌停档位（带新股窗口判定），供闸门与滑点夹取共用。"""
        _name = (stock_names or {}).get(code)
        _first_d = first_bar_date.get(code)
        # 数据起点=区间起点 → 无法判断是否真新股：不套新股宽松，
        # 按板块幅度校验（首根 bar 无前收，天然放行一次），留 WARNING。
        _truncated = (
            _first_d is not None
            and run_first_date is not None
            and _first_d <= run_first_date
        )
        if _truncated:
            if code not in truncation_warned:
                truncation_warned.add(code)
                logger.warning(
                    "涨跌停校验按板块幅度执行：%s 的数据起点 %s 与回测区间起点"
                    "重合，无法判断是否新股（不套前 5 日无限制的宽松判定）；"
                    "首个 bar 无前收，涨跌停校验天然跳过（保守放行一次）",
                    code, _first_d)
            return limit_rule_for(code, name=_name)
        # 从该 code 自身首根 bar 起算已过交易日（首发日=0），
        # 交由 resolve_limit_rule 判「前 5 个交易日无涨跌幅」。
        _listed = (
            trading_days_between(_first_d, t.date())
            if _first_d is not None else None
        )
        rule = resolve_limit_rule(
            code, name=_name,
            listed_bars=_listed,
            earliest_bar_date=_first_d, today=t.date(),
        )
        if rule.ratio is None and rule.source.startswith("new_listing"):
            return rule
        if rule.ratio is None:
            # ETF/转债/未知代码：无可靠档位 → 回落板块常规幅度（旧口径）。
            rule = limit_rule_for(code, name=_name)
        return rule

    def _build_bar(self, price_index: dict, t: datetime, period: str) -> BarEvent:
        """O(1) 查表取 (t, period) 的 OHLCV 构造 BarEvent（含周期标注）。"""
        return BarEvent(stocks=price_index.get((t, period), {}), bar_time=t, period=period)

    def _strategy_snapshots(
        self, portfolio: Portfolio, bar: BarEvent, snap_date: date
    ) -> List[dict]:
        """各策略日终快照。共享现金按 capital_ratio 归一化分摊，保证可加性：
        Σ(策略总净值) == 组合总净值。
        策略总净值 = 策略持股市值 + 分摊现金；分摊现金 = 组合现金 × (该 ratio / Σratio)。
        Σratio=0（极端情况）退化为均分，避免除零。
        """
        sum_ratio = sum((ctx.capital_ratio for ctx in portfolio.strategies), Decimal("0"))
        cash = portfolio.account.cash
        # 与组合层 total_value 同口径：刷新价格快照后按最近已知价估每策略持仓市值，
        # 缺席（停牌/缺 bar）沿用昨收而非按 0，保证 Σ策略市值 == 组合市值。
        portfolio._refresh_price_snapshot(bar)
        snaps: List[dict] = []
        for ctx in portfolio.strategies:
            market_value = portfolio.holdings_value(ctx)
            if sum_ratio > 0:
                allocated_cash = cash * (ctx.capital_ratio / sum_ratio)
            else:
                n = max(len(portfolio.strategies), 1)
                allocated_cash = cash / Decimal(n)
            total_value = market_value + allocated_cash
            snaps.append({
                "target_type": "strategy",
                "target_id": ctx.strategy_id,
                "snap_date": snap_date,
                "total_value": total_value,
                "cash": allocated_cash,
                "market_value": market_value,
                # 策略层持仓明细（只含本策略，供 strategy 维度 positions_json）
                "positions": portfolio.positions_detail(snap_date, ctx=ctx),
            })
        return snaps
