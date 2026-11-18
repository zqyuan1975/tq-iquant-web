from typing import Dict, List, Optional
from datetime import datetime, date
from decimal import Decimal, ROUND_HALF_UP
import logging

from .portfolio import Portfolio
from .position import Position, is_convertible_bond, lot_size_for
from .execution_engine import ExecutionEngine, SimulatedDispatcher, SimulatedT1Checker
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
from tq_iquant_shared.constants import SignalType, TradeType

logger = logging.getLogger(__name__)


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


class BacktestEngine:
    def run(
        self,
        portfolio: Portfolio,
        klines: dict = None,
        signal_cache: dict = None,
        benchmark_data: object = None,
        regime_tracker=None,
        gate_tracker=None,
        daily_close_peak_enabled: bool = False,
        initial_pending_orders: Optional[List[OrderEvent]] = None,
        stock_names: Optional[dict] = None,
        prev_close_by_day: Optional[Dict[tuple, Decimal]] = None,
        pending_order_max_days: int = PENDING_ORDER_MAX_DAYS,
    ) -> dict:
        """逐 bar 回测主链路。

        klines: {stock_code: {period: pl.DataFrame}}，DataFrame 含 datetime + Open/High/Low/Close/Volume 列。
        成交时机：上一 bar 触发的订单在下一 bar open 成交（pending_orders 队列），
        成交价取 price_index 中该 bar 的 Open（open_prices 死参数已移除，#12）。
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
        """
        klines = klines or {}
        signal_cache = signal_cache if signal_cache is not None else {}

        # 决策闸门采集（调参可观测性）：注入组合与执行引擎，run 结束随结果返回
        recorder = DecisionRecorder()
        portfolio.set_recorder(recorder)
        # B5：默认无市场状态（effective_* 全部回落原参数，行为不变）
        portfolio.current_regime = None
        # 市场多空门控：默认不启用（None → _gate_blocks 恒 False，行为不变）
        portfolio.current_market_gate = gate_tracker

        times = self._build_timeline(klines)
        price_index = self._build_price_index(klines)
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

            # 0b. 公司行为（J1/J2，默认未启用 —— timeline=None 时整段跳过）。
            #     必须在成交步之前：当日待成交的顺延单若碰上送转，股数要先调整，
            #     否则按旧股数入账。分红现金直接进组合账户；再投资意图留到日终
            #     按当日收盘价买入（下方 4d）；退市/长期停牌强平在此即时成交。
            if (portfolio.corporate_timeline is not None
                    and last_action_date != t.date()):
                _ca_actions = apply_corporate_day(
                    portfolio, portfolio.corporate_timeline, t.date(),
                    recorder, mode="backtest",
                    reinvest_enabled=portfolio.dividend_reinvest_enabled,
                    price_resolver=lambda code: self._ca_reference_price(
                        code, t, period, price_index, prev_close_by_day),
                    dispose_days=portfolio.suspension_dispose_days,
                    delist_force_close=portfolio.delist_force_close_enabled,
                    last_action_date=last_action_date,
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
                        bar_price_limits[_code] = (
                            (_pc * (1 - _rule.ratio)).quantize(
                                Decimal("0.01"), rounding=ROUND_HALF_UP),
                            (_pc * (1 + _rule.ratio)).quantize(
                                Decimal("0.01"), rounding=ROUND_HALF_UP),
                        )
                # #4 自适应滑点（可选，默认关）：开启时透传本 bar 各票成交量
                # （手）与冲击系数，dispatcher 按订单量占比动态加滑点；
                # 关闭时不传（dispatcher 走固定滑点，原行为）。
                dispatcher = SimulatedDispatcher(
                    bar_open_prices,
                    bar_volumes=(
                        {code: ohlcv.get("volume")
                         for code, ohlcv in price_index.get((t, period), {}).items()}
                        if portfolio.adaptive_slippage_enabled else None
                    ),
                    slippage_impact_coef=(
                        portfolio.slippage_impact_coef
                        if portfolio.adaptive_slippage_enabled else None
                    ),
                    price_limits=bar_price_limits or None,
                    **portfolio.cost_params,
                )
                engine = ExecutionEngine(
                    dispatcher, SimulatedT1Checker(), recorder=recorder,
                    # 可转债 10 张取整（可选，默认关）：关闭时恒回 100，行为不变
                    lot_size_of=lambda code: lot_size_for(
                        code, portfolio.bond_lot_enabled
                    ),
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
                    # 成交股数上限（TDX Volume 单位=手，×100 转股数，向下取整手）。
                    # 订单量 ≤ 上限 → 照常；> 上限且上限 ≥ 一手 → 缩量成交（每次记）；
                    # 上限不足一手 → 延后至量足 bar（每订单仅首次记 1 条）。
                    if portfolio.volume_limit_enabled:
                        bar_volume = price_index.get((t, period), {}).get(
                            order.stock_code, {}).get("volume")
                        if bar_volume is not None:
                            # TQ Volume 单位=股（非手），cap = 占比 × 当日成交股数
                            cap_shares = int(
                                portfolio.volume_limit_ratio * Decimal(int(bar_volume))
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
                                    recorder.record(
                                        gate="volume_cap_shrink", layer="execution",
                                        action="shrink",
                                        portfolio_id=order.portfolio_id or 0,
                                        strategy_id=order.strategy_id,
                                        stock_code=order.stock_code,
                                        bar_time=t,
                                        requested_qty=order.quantity,
                                        final_qty=final_qty,
                                        message="成交量约束：订单 %d 股 > 本 bar 上限 %d 股"
                                                "（当日成交 %s 股 ×%s），缩量至 %d 股成交"
                                                % (order.quantity, cap_shares, bar_volume,
                                                   portfolio.volume_limit_ratio, final_qty),
                                    )
                                    if (not is_buy
                                            and order.signal_type in _RISK_SELL_TYPES):
                                        risk_sell_original = order.quantity
                                    order.quantity = final_qty
                                else:
                                    if id(order) not in volume_defer_recorded:
                                        volume_defer_recorded.add(id(order))
                                        recorder.record(
                                            gate="volume_cap_defer", layer="execution",
                                            action="defer",
                                            portfolio_id=order.portfolio_id or 0,
                                            strategy_id=order.strategy_id,
                                            stock_code=order.stock_code,
                                            bar_time=t,
                                            requested_qty=order.quantity,
                                            message="成交量约束：本 bar 上限 %d 股不足一手"
                                                    "（当日成交 %s 股 ×%s），订单延后至量足 bar"
                                                    % (cap_shares, bar_volume,
                                                       portfolio.volume_limit_ratio),
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
                snap = portfolio.snapshot(t.date(), total_value, bar)
                # 填基准值：benchmark_data 是 {date: Decimal}，按当前 bar 日期取值；
                # 缺失（基准停牌/非交易日/区间外）直接为 None（不做 carry-forward），
                # Evaluator 喂基准序列时会过滤 None 值，全无则 benchmark_return 退化为 0。
                snap["benchmark_value"] = benchmark_data.get(t.date()) if benchmark_data else None
                # 市场多空门控：快照追加当日门控状态/强度（右移语义由 tracker 处理）；
                # tracker=None 不写键——与「门控关闭不渲染」的前端契约一致。
                if gate_tracker is not None:
                    snap["gate_state"] = gate_tracker.state_for(t.date())
                    snap["gate_strength"] = gate_tracker.strength_for(t.date())

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
        }

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
        kwargs["max_order_amount"] = (
            Decimal(str(_moa)) if _moa is not None else None
        )
        return kwargs

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
            stamp_duty=stamp, transfer_fee=transfer, trade_time=t,
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
                stamp_duty=Decimal("0"), transfer_fee=transfer, trade_time=t,
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
