from typing import Callable, List, Dict, Optional, Set
from decimal import Decimal
from datetime import date, datetime, time
from contextlib import nullcontext
import logging

from .account import Account
from .strategy_context import StrategyContext
from .position import Position, lot_size_for
from .risk_manager import PortfolioRiskManager
from .event import BarEvent, SignalEvent, OrderEvent
from .decision import NULL_RECORDER
from .format import fmt_money, fmt_pct, fmt_price
from .industry_gate import INDUSTRY_MODES, INDUSTRY_RELIEF, INDUSTRY_STRICT
from .market_rules import trading_days_between
from tq_iquant_shared.constants import SignalType, TradeType

logger = logging.getLogger(__name__)

# 公式信号类型优先级（同策略内）：CLOSE > REDUCE > ADD > OPEN
_FORMULA_PRIORITY = {
    SignalType.CLOSE: 0,
    SignalType.REDUCE: 1,
    SignalType.ADD: 2,
    SignalType.OPEN: 3,
}
# 风控信号优先级（高于所有公式信号）
_RISK_PRIORITY = {
    SignalType.STOP_LOSS: -1,
    SignalType.TAKE_PROFIT: -1,
    SignalType.TRAILING_STOP: -1,
}
# 全平类信号：生成订单后标记该股票清仓，抑制同 bar 后续信号
_FULL_CLOSE_TYPES = {
    SignalType.CLOSE, SignalType.STOP_LOSS,
    SignalType.TAKE_PROFIT, SignalType.TRAILING_STOP,
}
# 日线及以上周期：日线 bar 的 bar_time 为当日 00:00，close 在收盘后才确认。
# 此类 bar 触发的信号订单须延迟到 t.date() > signal_bar_date 的下一交易日成交，避免未来函数。
_DAILY_PERIODS: set = {"1d", "1w", "1mon"}


def _signal_bar_date_for(period: str, bar: BarEvent) -> Optional[date]:
    """订单的未来函数闸门日期（None = 下一根 bar 即可成交）。

    判别维度 = 触发 bar 时间戳（而非仅策略周期）：日线策略仅在 bar_time 为
    当日 00:00 的日线 bar 触发时打 signal_bar_date（close 未确认，延至次日）；
    分钟 bar 触发的订单（含日线策略的风控/轮动/跟随单）不打——分钟 bar 的
    close 在该 bar 结束时已确认，下一根 bar open 成交严格因果、无未来函数，
    且止损不再无谓延至次日（混合周期回测止损滑点过大，#4）。

    例外：14:50「提前收盘价」轮次（bar.advance_close=True）不打 signal_bar_date
    ——该轮用 14:50 合成收盘价定价、14:52 起始 1m bar 开盘价报单，语义就是当日
    尾盘提交成交（用户口径），若仍顺延次日则「提前收盘价」失去意义。
    收盘落定轮次（1d/1w/1mon）与 B 关闭时的 1d 轮次不受影响。
    """
    if getattr(bar, "advance_close", False):
        return None
    if period in _DAILY_PERIODS and bar.bar_time.time() == time(0, 0):
        return bar.bar_time.date()
    return None


def _order_price(bar: BarEvent, code: str, close):
    """委托价：优先用 ``bar.exec_prices`` 覆盖（B 提前轮次的 14:52/14:51 1m bar 开盘价）。

    仅影响委托价；数量/资金/仓位计算仍用 bar 收盘价 ``close``（与回测「信号 bar
    收盘定规模、下一 bar 开盘成交」一致）。持仓成本由成交回填/券商快照决定，
    与本函数无关。
    """
    override = getattr(bar, "exec_prices", None)
    if not override:
        return close
    px = override.get(code)
    if px is None:
        return close
    try:
        return Decimal(str(px))
    except Exception:  # noqa: BLE001
        return close


def _signal_priority(signal_type: SignalType) -> int:
    if signal_type in _RISK_PRIORITY:
        return _RISK_PRIORITY[signal_type]
    return _FORMULA_PRIORITY.get(signal_type, 99)


class Portfolio:
    def __init__(
        self,
        portfolio_id: int,
        initial_capital: Decimal,
        risk_manager: PortfolioRiskManager,
        cost_params: Optional[Dict] = None,
        recorder=None,
        position_sizing: str = "dynamic",
    ):
        self.portfolio_id = portfolio_id
        self.account = Account(initial_capital)
        self.risk_manager = risk_manager
        self.strategies: List[StrategyContext] = []
        self.benchmark_value: Optional[Decimal] = None
        # W2 仓位模型：dynamic（默认）按组合当前总净值分配策略资金；fixed 按初始资金。
        self.position_sizing: str = position_sizing
        # 本 bar 组合总净值（on_bar 顶部刷新），供 _strategy_fund；未处理 bar 时为 None。
        self._bar_equity: Optional[Decimal] = None
        # W2 现金管理增强（预留触发点，默认关）：命中阈值仅记决策事件，不产生订单。
        self.cash_enhance_enabled: bool = False
        self.cash_enhance_equity_threshold: Optional[Decimal] = None
        self.cash_enhance_position_ratio_threshold: Optional[Decimal] = None
        self._cash_enhance_last_date = None
        # W3 容量上限（可选，默认 None）：dynamic 资金 = capital_ratio × min(净值, 上限)。
        # 净值超上限后仓位封顶、超额现金闲置（等价"超容量撤回/封盘"），
        # 封堵 20%×增长净值 在长周期复利下开出不可实现的天文仓位。fixed 不受影响。
        self.equity_cap: Optional[Decimal] = None
        # 资金池（0021 B3）：组合级目标利用率（None=按 0.95 缺省），仅被额度推导
        # 读取，不参与任何现有交易/风控计算；未注入时行为与改动前逐位一致。
        self.target_utilization: Optional[Decimal] = None
        # W3 上限处理模式：cap（默认，只封顶新开仓定量，与旧行为逐位一致）/
        # distribute（回测日终把「总资产 − cap」当外部流出扣出现金，不强制减仓）。
        self.equity_cap_mode: str = "cap"
        # 资金池（0021 B3）：本 bar 内已排队买单的额度占用（strategy_id → 金额）。
        # 只在 capital_budget 非 None 时写入；每根 bar 开头清空（上一 bar 的挂单
        # 已在成交步落账，本 bar 的持仓市值已包含），避免同 bar 多信号超额。
        self._budget_committed: Dict[int, Decimal] = {}
        # 决策闸门采集器（调参可观测性）：默认空采集器，set_recorder 注入。
        # 同步给 risk_manager（熔断/恢复在其内判定，需同一 recorder）。
        self.recorder = recorder or NULL_RECORDER
        self.risk_manager.recorder = self.recorder
        self.risk_manager.portfolio_id = portfolio_id
        # 实盘账本锁（P0-2，默认 None=回测无锁）：LiveEngine 装配时注入引擎级
        # _book_lock（RLock）。deals 回填池在另一线程插入新仓/镜像 quantity·avg_cost
        # 时持同一把锁；本类所有「遍历 positions + 读数量/成本」的短临界区经
        # _book_guard() 取锁 → 不会撞上 dict changed size during iteration，
        # 止损/减仓数量也取自一致快照（不会读到半更新持仓）。
        # 注意：只包内存读写的短段（风控判定/额度计算/市值合计），绝不包公式
        # 求值或任何 I/O —— deals 回填不能被阻塞。
        self.book_lock = None
        # 卖出范围口径（config.tq_trade.live_sell_scope）：full=按整仓卖（默认，
        # 与改动前逐字一致）；platform_only=只卖 Position.platform_quantity 那部分，
        # 纯外部仓不下单（记 gate=sell_scope 决策事件）。回测不读配置 → 恒 full；
        # 且回测 platform_quantity 与 quantity 等价，两种口径结果一致。
        self.sell_scope: str = "full"
        # platform_only 下「仅外部持仓不代卖」事件的去重键 (strategy_id, stock_code, date)，
        # 同一组合策略同一只票每个交易日只记一条（跨日重记，便于每日核对）。
        self._sell_scope_skipped: set = set()
        # 实盘注入：风控已触发但被 sell_scope 跳过时的显式告警回调
        # 签名 (sig, pos, bar, requested_qty, price, amplitude_pct)；回测保持 None。
        self.scope_skip_notify = None
        # 显式告警去重键 (stock_code, trade_date)：按 (会话, 代码, 交易日) 每日首条。
        self._scope_skip_notified: set = set()
        # 按标的授权代卖（live_position_scope.enabled=1 的代码集合）：命中则
        # platform_only 下不再把数量夹到平台仓，按整仓可卖量放行；空集合（默认）
        # 与旧行为逐字一致。实盘引擎每轮 tick 注入；回测恒空。
        self.sell_scope_authorized: Set[str] = set()
        # 持仓最新价快照（code → close）。BarPoller 逐票完成会发残缺 bar，
        # 实盘分钟 bar 只含本轮新完成的股票；停牌股整天无 bar。估值必须对所有
        # 持仓用最近已知价，不能把缺席持仓当 0（否则组合总市值瞬间塌掉误触发
        # max_drawdown 熔断，见 2026-08-25 对账）。每根 bar 用其有效 close 刷新。
        # 跨日不清理（=「最后已知价，跨日持久」，P1-8 三级兜底第一级）。
        self._price_snapshot: Dict[str, Decimal] = {}
        # 前收快照（P1-8 三级兜底第三级）：跨日滚动时把上一交易日结束时的最新价
        # 拷进来，仅当「最后已知价 → 成本」两级都取不到时使用。
        self._prev_close_snapshot: Dict[str, Decimal] = {}
        self._valuation_date: Optional[date] = None
        # 本轮无法估值的持仓代码（三级全缺 → 按 0 计市值）：引擎据此记 WARNING +
        # risk 事件（gate=valuation_missing_price），避免「零估值就地静默」。
        self._unpriced_codes: set = set()
        self._unpriced_warned: set = set()
        # 交易成本参数（来自组合表），供 BacktestEngine 构建 dispatcher 时透传
        self.cost_params: Dict = cost_params or {}
        # B5 市场状态闸：当前市场状态标签，由引擎在 on_bar 前注入（None=无状态，
        # ctx.effective_* 全部回落原参数，行为与旧逻辑一致）
        self.current_regime: Optional[str] = None
        # 市场多空门控（2026-11-18）：组合级基准指数状态查询器（MarketGateTracker），
        # 由引擎在 on_bar 前注入（None=未启用门控，行为与旧逻辑逐字一致）。
        # 语义：空头状态只禁止**开仓/加仓**（OPEN/ADD），持仓不强平——
        # CLOSE/REDUCE/止损/止盈/移动止损一律不受影响。查询一律右移一日
        # （昨日收盘状态管今日开仓），见 core/engine/market_gate.py。
        self.current_market_gate = None
        # E3 空头减仓档（组合级，默认 0=关闭）：空头连续段内每只持仓减仓一次。
        # 0 时零开销、行为与接线前逐字一致。
        self.gate_bear_reduce_ratio: Decimal = Decimal("0")
        # {(strategy_id, code)}：本空头连续段内已减过仓的持仓，转多/未定时清空。
        self._gate_bear_episode: set = set()
        # 行业层决策（2026-11-22，全部默认关闭）：行业状态轴（IndustryTracker，由
        # 引擎每日/每轮回测注入）+ 组合级开关。关闭时 _gate_decision 不读行业，
        # 行为与接线前逐位一致。
        self.current_industry_gate = None
        self.industry_gate_mode: str = "off"   # off | industry_relief | industry_strict
        self.industry_rank_enabled: bool = False
        self.industry_max_per_industry: int = 0  # 0=不限（仅排序开启时生效）
        self.industry_weight_enabled: bool = False
        self.industry_overbought_block: bool = False
        self.industry_trail_tighten_enabled: bool = False
        self.industry_trail_tighten_ratio: Decimal = Decimal("0.015")
        self.industry_bear_reduce_ratio: Decimal = Decimal("0")
        self.industry_divergence_hint: bool = False
        # 行业数据缺失 WARNING 去重（每只股票每进程只告警一次）
        self._industry_missing_warned: set = set()
        # B 跨策略排序（默认关）：个股 OPEN 信号持续天数（首次出现日记起，缺席即清）
        # 仅排序开启时读写；关闭时零开销。
        self._open_signal_first_seen: Dict[tuple, date] = {}
        self._open_signal_seen: set = set()
        # 公司行为（2026-11-18，J1/J2）：手工事件时间线，None = 未启用（默认）。
        # 由 backtest_service / live_service 在组合开关打开时注入；注入后
        # BacktestEngine / LiveEngine 每交易日调一次 corporate_actions.apply_day。
        self.corporate_timeline = None
        self.corporate_actions_enabled: bool = False
        self.dividend_reinvest_enabled: bool = False
        self.suspension_dispose_days: int = 0
        self.delist_force_close_enabled: bool = False
        # 分红记账口径（J4，2026-11-18）：backtest=复权价 + 比例分红（默认）；
        # backtest_raw=不复权原始价空间（exact 档），优先按每股金额入账。
        self.corporate_action_mode: str = "backtest"
        # 当日停牌代码集合（apply_day 每日刷新）：只拦 OPEN/ADD（持仓不强平，
        # 卖出/风控照常）。默认空集合 → 闸门无任何副作用。
        self.suspended_codes: frozenset = frozenset()
        # 当日除权除息代码集合（2026-11-22 口径切换）：只拦 OPEN/ADD——除权日价格
        # 跳空会制造假突破，开仓/加仓一律次日按新价重评；CLOSE/REDUCE/止损止盈照常。
        # 默认空集合 + data_ok=None（未接线）→ 闸门无副作用（fail-open）。
        self.ex_dividend_codes: frozenset = frozenset()
        # None=未接线/未知，True=事件表可读，False=取不到数据 → fail-open 放行，
        # 但每组合每日至多记一条 ex_dividend_data_unavailable（不得静默）。
        self.ex_dividend_data_ok = None
        self._ex_div_noted_days: set = set()
        # 长期停牌处置的幂等键集合（(code, suspend_ex_date)），防每日重复发强平。
        self._ca_disposed: set = set()
        # 时点股票池（J7，2026-11-18）：None = 未启用（默认）。
        # 注入后只拦 OPEN/ADD；该交易日无任何成员记录时 fail-open 放行（回落
        # 当前池），每组合每日记一条 pool_pit_uncovered 事件。
        self.pool_timeline = None
        self.pool_pit_enabled: bool = False
        self._pool_pit_noted_days: set = set()
        # B4 择优轮动：收盘价历史（code → 最近 N 根有效 close）与去重时间戳
        self._price_history: Dict[str, List[Decimal]] = {}
        self._price_hist_last_t: Dict[str, datetime] = {}
        # #1 涨跌停校验（默认开）：回测成交步拦截"涨停买入/跌停卖出"（由
        # BacktestEngine 判定，仿 no_market_data 延后重试）；实盘下单前置校验
        # （由 LiveEngine 调 dispatcher.check_price_limit，拒单落 rejected +
        # 决策事件）。类默认 True，与组合表 ORM/server 默认一致；builder 仍
        # 从组合表字段装配，用户显式关闭时按库值 False 跳过。
        self.price_limit_check_enabled: bool = True
        # 风控冷静期（2026-11-12）：止损/止盈/移动止损**成交后**，同一 (策略, 代码)
        # 在接下来 N 个交易日内禁止新开仓（当日算第 1 天，N=1=当日不再买回、次日
        # 恢复）；0=关闭（等价改动前行为）。类默认 1，与组合表 ORM/server 默认一致；
        # builder 从组合表装配。回测/实盘共用本层开仓闸，两端自动一致。
        self.risk_cooldown_days: int = 1
        # {(strategy_id, code): 最近一次风控卖出成交日}，由 note_trade 登记
        self._risk_cooldown: Dict[tuple, date] = {}
        # #2 成交量/流动性约束（可选，默认关）：
        # - volume_limit_ratio 为「日线流动性目标」：单笔 ≤ 前 5 个交易日日均量
        #   （股）× 本比例。回测与实盘统一口径，且严格只用 t 日之前的数据（无前视）。
        # - minute_volume_limit_ratio 为「实盘单次可下单量」：单次提交 ≤ 最近完成
        #   1m bar 量 × 本比例。仅实盘消费；回测无分钟执行，不读该字段。
        #   NULL/未配置回落到 volume_limit_ratio（现存组合=4%，与改动前一致）；
        #   10% 只能显式配置，不作为默认（放宽单次可吃比例等同改风险偏好）。
        # 缩量后的残量由实盘补单队列按后续 bar 续发（默认开启，无开关）；
        # 回测按 bar 顺延/残量切片。
        self.volume_limit_enabled: bool = False
        self.volume_limit_ratio: Decimal = Decimal("0.10")
        self.minute_volume_limit_ratio: Decimal = Decimal("0.04")
        # #4 自适应滑点（可选，默认关）：开启后回测成交步按订单量占当根 bar
        # 成交量比例动态加冲击成本（由 BacktestEngine 构造 dispatcher 时透传
        # bar_volumes + 系数）。类默认 False，直接构造 Portfolio 的引擎单测
        # 不受影响；builder 从组合表字段装配。
        self.adaptive_slippage_enabled: bool = False
        self.slippage_impact_coef: Decimal = Decimal("0.10")
        # 层 1 容量模型（默认 linear=旧口径）：sqrt 启用「参与率 + sqrt 冲击 +
        # 残量顺延切片」，替代 4% 成交量闸门与固定单笔金额闸门。参数取保守值，
        # 可由 run 级请求覆盖；capacity_model != "sqrt" 时全部不生效（行为逐位一致）。
        self.capacity_model: str = "linear"
        # 单日参与率：中小盘 0.5% / 超大市值 1%（当日成交额 ≥ capacity_large_turnover）
        self.capacity_daily_ratio_small: Decimal = Decimal("0.005")
        self.capacity_daily_ratio_large: Decimal = Decimal("0.01")
        # 最近 5 个交易日累计参与率：中小盘 1.5% / 超大市值 2%
        self.capacity_5d_ratio_small: Decimal = Decimal("0.015")
        self.capacity_5d_ratio_large: Decimal = Decimal("0.02")
        self.capacity_large_turnover: Decimal = Decimal("5000000000")
        # sqrt 冲击：20bps + k×√参与率，> 上限判不可成交
        self.capacity_impact_base: Decimal = Decimal("0.002")
        self.capacity_impact_k: Decimal = Decimal("0.10")
        self.capacity_impact_cap: Decimal = Decimal("0.05")
        # 残量顺延切片 TTL（根 bar）：超期作废，不整单拒绝
        self.capacity_slice_ttl_bars: int = 3
        # 费用预留（可选，默认关）：开启后买入资金审批含佣金，防现金为负。
        # 经 setter 同步 account.buy_fee_cfg（见下方 property）。
        self._fee_reserve_enabled: bool = False
        # 可转债 run 级可选开关（仅回测装配，默认关=原行为）：
        # bond_t0_enabled：转债当日买入当日可卖（T+0 回转，股票仍强制 T+1）；
        # bond_lot_enabled：转债最小交易单位 10 张（股票仍 100 股）。
        self.bond_t0_enabled: bool = False
        self.bond_lot_enabled: bool = False
        # 组合级最大持仓数（组合表字段，历史死配置首次接线）：OPEN 开新仓前按
        # 组合全部策略持仓去重股票数判定，≥ 上限拒绝新开仓。0=不限（直接构造
        # Portfolio 的引擎单测默认不受限）；builder 从组合表装配。
        self.max_holdings: int = 0
        # 单票集中度上限 / 现金留存下限（组合表字段，**仅实盘接线**，回测恒 None）：
        # max_single_position_pct = 买入后该代码组合市值 ÷ 组合总资产 ≤ 阈值；
        # min_cash_reserve_pct   = 买入后剩余现金 ÷ 组合总资产 ≥ 阈值。
        # None 或 <=0 = 不设限（与接线前行为逐字一致）。回测口径冻结：本类只提供
        # 默认值，装配端由 live_service.build_engine 按组合行显式注入，回测调用点
        # （backtest_service/incremental_backtest）**不读这两列**。
        self.max_single_position_pct: Optional[Decimal] = None
        self.min_cash_reserve_pct: Optional[Decimal] = None
        # 组合交易时段（full/am/pm，组合表字段，历史死配置首次接线）：
        # am=09:30–11:30、pm=13:00–15:00、full=不筛（默认）。回测按 bar 时间、
        # 实盘按墙钟过滤（窗口定义见 market_rules.session_allows）。
        self.trading_session: str = "full"
        # 累计外部净流入（③，正=净入金）：入金/出金/分红/其他流水之和。
        # 虚拟现金随台账增减（入金加现金），但净值/收益率/回撤必须剔除外部
        # 现金流 → 风险口径统一用 risk_value()/risk_equity()（总市值 − 本值）。
        # 回测无外部现金流，恒 0，行为与改动前逐字一致。
        self.external_net_inflow: Decimal = Decimal("0")
        # 峰值持久化钩子（方案 E，2026-11-12）：只在实盘引擎注入
        # ``LiveEngine._note_peak_raised``，把「持仓期最高价」在被抬高时落库
        # （live_position_peaks），重启后 recover 优先读表。回测保持 None → 零影响。
        # 签名：hook(portfolio_id, stock_code, highest_price, bar_time)。
        self.peak_persist_hook: Optional[Callable] = None

    def set_recorder(self, recorder) -> None:
        """注入决策采集器（回测/实盘引擎在装配后调用），同步给 risk_manager。"""
        self.recorder = recorder or NULL_RECORDER
        self.risk_manager.recorder = self.recorder
        self.risk_manager.portfolio_id = self.portfolio_id

    def note_trade(self, trade) -> None:
        """成交回填钩子：风控类卖出成交后登记冷静期起算日（回测/实盘共用）。

        只登记止损/止盈/移动止损（``_RISK_PRIORITY`` 三类）；公式 CLOSE/REDUCE
        不触发冷静期（属策略主动调仓，不是风控出场）。登记粒度 = (策略, 代码)，
        与开仓闸 ``_risk_cooldown_block`` 同键。

        调用点（三处，均为「成交已记账」之后）：
        - ``BacktestEngine.run`` 成交步 ``engine.execute`` 返回成交后；
        - 实盘 ``OrderMachine._apply_fill`` 回填后；
        - ``LiveEngine.recover`` 从 ``live_trades`` 重放时（重启后冷静期不丢）。
        """
        if trade is None:
            return
        if getattr(trade, "trade_type", None) != TradeType.SELL:
            return
        if getattr(trade, "signal_type", None) not in _RISK_PRIORITY:
            return
        t = getattr(trade, "trade_time", None)
        if t is None:
            return
        self._risk_cooldown[(getattr(trade, "strategy_id", None),
                             getattr(trade, "stock_code", None))] = t.date()

    def _risk_cooldown_block(
        self, ctx: StrategyContext, sig: SignalEvent, bar: BarEvent,
    ) -> bool:
        """风控冷静期闸（开新仓路径）：命中返回 True（已记决策事件）。

        按**交易日**推进（``trading_days_between``，周末/节假日正确）。当日卖出
        当日 elapsed=0 → N=1 时拦当日、次日 elapsed=1 放行（「当日算第 1 天」）。
        """
        days = int(self.risk_cooldown_days or 0)
        if days <= 0:
            return False
        last = self._risk_cooldown.get((ctx.strategy_id, sig.stock_code))
        if last is None:
            return False
        elapsed = trading_days_between(last, bar.bar_time.date())
        if elapsed >= days:
            return False
        self._rec("risk_cooldown", "signal_gate", "block", ctx, sig, bar,
                  param_name="risk_cooldown_days",
                  param_value=float(days),
                  actual_value=float(elapsed),
                  message="风控冷静期：%s 于 %s 风控卖出，已过 %d 个交易日 / 剩余 %d 天"
                          "（需 ≥%d 个交易日），不开新仓"
                          % (sig.stock_code, last, elapsed, days - elapsed, days))
        return True

    def _gate_decision(self, trade_time, ctx=None, stock_code=None):
        """开仓/加仓闸门判定：None=放行 / "market"=大盘空头 / "industry"=行业非多头。

        大盘门控语义不变（右移一日、策略级 off 豁免、异常 fail-open）。
        行业门控（2026-11-22，默认关闭）：
        - ``industry_relief``：仅当大盘空头时生效——行业多头放行、行业非多头封堵；
          大盘非空头时不读行业（与关闭时逐位一致）。
        - ``industry_strict``：常开——行业非多头一律封堵；行业多头不覆盖大盘空头。
        行业数据缺失（含个股无法映射）一律 fail-open 放行，并留 WARNING + 决策事件。
        """
        if ctx is not None and getattr(ctx, "market_gate_mode", "inherit") == "off":
            return None
        day = trade_time.date() if hasattr(trade_time, "date") else trade_time
        tracker = self.current_market_gate
        market_blocked = False
        if tracker is not None:
            try:
                market_blocked = bool(tracker.blocks(day))
            except Exception:  # noqa: BLE001
                # 门控查询异常一律放行（fail-open）：不能让基准状态问题变成"永不开仓"
                logger.warning("市场门控查询异常，本次放行（fail-open）", exc_info=True)
                market_blocked = False

        mode = self.industry_gate_mode
        ind = self.current_industry_gate
        if mode == "off" or stock_code is None or ind is None:
            return "market" if market_blocked else None
        if mode == INDUSTRY_RELIEF and not market_blocked:
            return None
        try:
            is_bull = ind.is_bull_for_stock(stock_code, day)
        except Exception:  # noqa: BLE001
            logger.warning("行业状态查询异常，本次放行（fail-open）：%s",
                           stock_code, exc_info=True)
            is_bull = None
        if is_bull is None:
            self._note_industry_missing(stock_code, day, trade_time, ind)
            return "market" if market_blocked else None
        if mode == INDUSTRY_RELIEF:
            return None if is_bull else "industry"
        # industry_strict：大盘空头仍优先，行业多头不覆盖
        if market_blocked:
            return "market"
        return None if is_bull else "industry"

    def _gate_blocks(self, trade_time, ctx=None, stock_code=None) -> bool:
        """市场多空门控闸（含可选行业门控）：True=禁止开仓/加仓。"""
        return self._gate_decision(trade_time, ctx, stock_code) is not None

    def _industry_block_message(self, stock_code, bar, action: str) -> str:
        """行业门控拦截事件文案（含所属行业与当日方向，便于回溯）。"""
        ind = self.current_industry_gate
        if ind is None:
            return "行业门控：行业数据不可用，%s" % action
        day = bar.bar_time.date() if hasattr(bar.bar_time, "date") else bar.bar_time
        code = ind.industry_of(stock_code) or "未知"
        state = ind.state_for_stock(stock_code, day)
        direction = state.direction if state is not None else 0
        return "行业门控(%s)：%s 所属行业 %s 非多头（方向 %d），%s" % (
            self.industry_gate_mode, stock_code, code, direction, action)

    def _industry_state(self, stock_code, bar):
        """个股所属行业的当日生效状态（缺失/异常 → None，调用方按放行处理）。"""
        ind = self.current_industry_gate
        if ind is None:
            return None
        try:
            day = (bar.bar_time.date() if hasattr(bar.bar_time, "date")
                   else bar.bar_time)
            return ind.state_for_stock(stock_code, day)
        except Exception:  # noqa: BLE001
            logger.warning("行业状态查询异常：%s", stock_code, exc_info=True)
            return None

    def _industry_open_weight(self, stock_code, bar) -> Decimal:
        """C 行业强度仓位权重（默认关）：0.5 + 强度/100，夹取 [0.5, 1.5]。

        仅在行业多头时生效；行业非多头/数据缺失 → 1（中性，不放大不缩小）。
        """
        if not self.industry_weight_enabled:
            return Decimal("1")
        state = self._industry_state(stock_code, bar)
        if state is None or not state.is_bull:
            return Decimal("1")
        try:
            weight = Decimal("0.5") + Decimal(str(state.strength)) / Decimal("100")
        except Exception:  # noqa: BLE001
            return Decimal("1")
        return min(Decimal("1.5"), max(Decimal("0.5"), weight))

    def _note_industry_missing(self, stock_code, day, trade_time, tracker) -> None:
        """行业数据缺失留痕（fail-open）：每只股票每进程一条 WARNING + 决策事件。"""
        if stock_code in self._industry_missing_warned:
            return
        if len(self._industry_missing_warned) < 5000:
            self._industry_missing_warned.add(stock_code)
        reason = tracker.missing_for_stock(stock_code, day) or "行业数据缺失"
        logger.warning("行业门控数据缺失，按 fail-open 放行：%s（%s）",
                       stock_code, reason)
        try:
            self.recorder.record(
                gate="industry_gate_missing",
                layer="signal_gate",
                action="skip",
                portfolio_id=self.portfolio_id,
                stock_code=stock_code,
                bar_time=trade_time,
                message=f"行业数据缺失，fail-open 放行：{reason}",
            )
        except Exception:  # noqa: BLE001
            logger.debug("行业门控缺失事件记录失败", exc_info=True)

    def configure_industry(self, settings) -> None:
        """装配组合级行业开关（默认全部关闭 = 不改变任何既有结果）。"""
        cfg = dict(settings or {})
        mode = str(cfg.get("industry_gate_mode") or "off").strip().lower()
        self.industry_gate_mode = mode if mode in INDUSTRY_MODES else "off"
        self.industry_rank_enabled = bool(cfg.get("industry_rank_enabled", False))
        try:
            self.industry_max_per_industry = max(
                0, int(cfg.get("industry_max_per_industry", 0) or 0))
        except (TypeError, ValueError):
            self.industry_max_per_industry = 0
        self.industry_weight_enabled = bool(cfg.get("industry_weight_enabled", False))
        self.industry_overbought_block = bool(
            cfg.get("industry_overbought_block", False))
        self.industry_trail_tighten_enabled = bool(
            cfg.get("industry_trail_tighten_enabled", False))
        try:
            self.industry_trail_tighten_ratio = Decimal(
                str(cfg.get("industry_trail_tighten_ratio", "0.015")))
        except Exception:  # noqa: BLE001
            self.industry_trail_tighten_ratio = Decimal("0.015")
        try:
            self.industry_bear_reduce_ratio = Decimal(
                str(cfg.get("industry_bear_reduce_ratio", "0") or "0"))
        except Exception:  # noqa: BLE001
            self.industry_bear_reduce_ratio = Decimal("0")
        self.industry_divergence_hint = bool(
            cfg.get("industry_divergence_hint", False))

    def _gate_bear_reduce_signals(self, ctx: StrategyContext, bar: BarEvent) -> List[SignalEvent]:
        """空头减仓档：空头连续段内每只持仓减仓一次（默认 0=关闭）。

        大盘档（E3，gate_bear_reduce_ratio）与行业档（F，industry_bear_reduce_ratio）
        共用本入口：同一持仓同日同时命中取 **max(ratio)**，绝不叠加；行业档默认
        关。与开仓闸同源（tracker 右移一日）；策略级 market_gate_mode="off" 跳过。
        只产生 REDUCE 信号，实际数量/整手/T+1/卖出范围仍走既有卖出路径。
        """
        tracker = self.current_market_gate
        market_ratio = self.gate_bear_reduce_ratio or Decimal("0")
        industry_ratio = self.industry_bear_reduce_ratio or Decimal("0")
        ind = self.current_industry_gate
        use_market = tracker is not None and market_ratio > 0
        use_industry = ind is not None and industry_ratio > 0
        if not use_market and not use_industry:
            return []
        if getattr(ctx, "market_gate_mode", "inherit") == "off":
            return []
        market_bear = False
        if use_market:
            try:
                market_bear = bool(tracker.blocks(bar.bar_time.date()))
            except Exception:  # noqa: BLE001
                logger.warning("市场门控减仓查询异常，本 bar 跳过减仓（fail-open）",
                               exc_info=True)
                return []
        if not market_bear and not use_industry:
            # 空头段结束：下次再进空头重新计一次（大盘档单独启用时的原行为）
            self._gate_bear_episode.clear()
            return []
        out: List[SignalEvent] = []
        visited: set = set()
        for stock_code, pos in self._iter_positions(ctx):
            if pos.quantity <= 0 or stock_code not in bar.stocks:
                continue
            key = (ctx.strategy_id, stock_code)
            visited.add(key)
            # 有效减仓比例 = max(大盘档命中, 行业档命中)；禁止叠加。
            ratio = Decimal("0")
            source = None
            if market_bear and market_ratio > 0:
                ratio, source = market_ratio, "market"
            if use_industry and industry_ratio > 0:
                state = self._industry_state(stock_code, bar)
                if (state is not None and state.is_bear
                        and industry_ratio > ratio):
                    ratio, source = industry_ratio, "industry"
            if ratio <= 0:
                # 该持仓已不在任何空头段：下次再进空头重新计一次
                self._gate_bear_episode.discard(key)
                continue
            if key in self._gate_bear_episode:
                continue
            self._gate_bear_episode.add(key)
            sig = SignalEvent(
                strategy_id=ctx.strategy_id,
                stock_code=stock_code,
                signal_name="gate_bear_reduce",
                signal_type=SignalType.REDUCE,
                bar_time=bar.bar_time,
                reduce_ratio=ratio,
            )
            out.append(sig)
            if source == "industry":
                self._rec("industry_bear_reduce", "signal_gate", "reduce",
                          ctx, sig, bar,
                          param_name="industry_bear_reduce_ratio",
                          param_value=float(ratio),
                          message="行业门控空头：本段首次按 %s 减仓" % fmt_pct(ratio))
            else:
                self._rec("market_gate_bear_reduce", "signal_gate", "reduce",
                          ctx, sig, bar,
                          param_name="market_gate_bear_reduce_ratio",
                          param_value=float(ratio),
                          message="市场门控空头：本段首次按 %s 减仓" % fmt_pct(ratio))
        if use_industry:
            # 已清仓的旧键清掉，避免同一股票日后重新买入时被"本段已减"误抑制
            for stale in [k for k in self._gate_bear_episode
                          if k[0] == ctx.strategy_id and k not in visited]:
                self._gate_bear_episode.discard(stale)
        return out

    def _book_guard(self):
        """账本锁上下文（P0-2）：实盘返回注入的 RLock，回测返回 nullcontext。

        只用于包裹「遍历 positions + 读持仓字段」的短临界区；公式求值、下单、
        决策事件落库一律在锁外，避免阻塞 deals 回填。
        """
        lock = self.book_lock
        return lock if lock is not None else nullcontext()

    def _iter_positions(self, ctx: StrategyContext):
        """并发安全的持仓键值快照（P0-2）。

        实盘 deals 池在另一线程插入新仓/镜面数量，直接迭代 dict 会抛
        ``RuntimeError: dictionary changed size during iteration``（被 _on_bar 的
        宽 except 吞成一行 handle_bar error，整批信号与风控丢失）。持账本锁拷
        ``list(...)`` 拿稳定视图；回测无锁时只做拷贝（开销与原来同量级）。
        """
        lock = self.book_lock
        if lock is None:
            return list(ctx.positions.items())
        with lock:
            return list(ctx.positions.items())

    @property
    def fee_reserve_enabled(self) -> bool:
        return self._fee_reserve_enabled

    @fee_reserve_enabled.setter
    def fee_reserve_enabled(self, enabled: bool) -> None:
        """开关与账户买入费率原子同步：置 True 从 cost_params 写入 buy_fee_cfg
        （缺一不写，保持不启用）；置 False 清回 None（审批恢复仅价×量原行为）。

        与 execution_engine 的费用感知兜底（_fit_buy_qty_to_cash）不冲突：
        前者让 approve_order 审批即含佣（更早、更精确），后者是成交前最终
        兜底钳制（防任何路径遗漏）。"""
        self._fee_reserve_enabled = bool(enabled)
        if enabled:
            min_comm = self.cost_params.get("min_commission")
            buy_rate = self.cost_params.get("buy_commission_rate")
            if min_comm is not None and buy_rate is not None:
                self.account.buy_fee_cfg = (min_comm, buy_rate)
                # 过户费（双向）与成交计费同口径，避免审批额度少算费用
                self.account.buy_transfer_fee_rate = Decimal(str(
                    self.cost_params.get("transfer_fee_rate", 0) or 0))
        else:
            self.account.buy_fee_cfg = None
            self.account.buy_transfer_fee_rate = Decimal("0")

    def _lot(self, stock_code: str) -> int:
        """本组合当前配置下该标的的交易手数（转债 10 张 / 股票 100 股）。"""
        return lot_size_for(stock_code, self.bond_lot_enabled)

    def _rec(self, gate: str, layer: str, action: str, ctx: StrategyContext,
             sig: SignalEvent, bar: BarEvent, **kw) -> None:
        """记录一条信号/风控层闸门事件（带策略/股票/bar 时间公共字段）。"""
        self.recorder.record(
            gate=gate, layer=layer, action=action,
            portfolio_id=self.portfolio_id, strategy_id=ctx.strategy_id,
            stock_code=sig.stock_code, bar_time=bar.bar_time, **kw,
        )

    def _scope_sell_quantity(
        self, ctx: StrategyContext, sig: SignalEvent, bar: BarEvent,
        pos: Position, intent: int,
    ) -> int:
        """按卖出范围口径收窄意图数量（full 原样返回 / platform_only 夹到平台仓）。

        - full（默认）：返回意图量，行为与改动前逐字一致。
        - platform_only：若该代码在 ``sell_scope_authorized`` 内（按标的授权代卖，
          见 live_position_scope 表）→ 原样返回意图量（整仓可卖量）；否则返回
          min(意图量, Position.platform_quantity)——只卖平台自己买入且尚未平掉的
          部分；platform_quantity<=0（纯外部/手工仓）返回 0，并记一条
          gate=sell_scope / action=skip 决策事件（同一组合策略同一只票每交易日
          只记一条，避免每根 bar 刷屏）+ 每日首条 warn 告警。
        阈值判定（止损/止盈/移动止损）仍在 _check_risks 按整仓成本计算，本方法只
        收窄卖出数量（已知偏差：可能出现「按整仓成本触发、只卖平台那部分」）。
        """
        intent = max(0, int(intent))
        if self.sell_scope != "platform_only":
            return intent
        if sig.stock_code in self.sell_scope_authorized:
            # 已授权代卖：按整仓可卖量放行；T+1/涨跌停/审批/时点等下游闸门照旧
            return intent
        platform = max(0, getattr(pos, "platform_quantity", 0))
        if platform <= 0:
            day = bar.bar_time.date() if getattr(bar, "bar_time", None) else None
            price = self._bar_close(bar, sig.stock_code)
            amplitude = self._price_amplitude(pos, price)
            rule = self._signal_rule(sig)
            key = (ctx.strategy_id, sig.stock_code, day)
            if key not in self._sell_scope_skipped:
                self._sell_scope_skipped.add(key)
                self._rec(
                    "sell_scope", "signal_gate", "skip", ctx, sig, bar,
                    requested_qty=intent,
                    final_qty=0,
                    actual_value=amplitude,
                    message=(
                        "风控已触发但未代卖：规则=%s，触发价=%s，整仓成本=%.3f，"
                        "触发幅度=%s，整仓=%d 股，请求卖出=%d 股；"
                        "live_sell_scope=platform_only、平台仓=0 且该标的未授权"
                        "代卖，下单前数量收窄为 0（可在持仓表开启「代卖授权」）"
                        % (rule or "—",
                           "%.3f" % price if price is not None else "—",
                           float(getattr(pos, "avg_cost", 0) or 0),
                           "%+.2f%%" % amplitude if amplitude is not None else "—",
                           int(getattr(pos, "quantity", 0) or 0), intent)
                    ),
                )
            # 显式告警：按 (会话, 代码, 交易日) 每日首条（信号事件本身可能每 bar 重触发）
            if day is not None and self.scope_skip_notify is not None:
                nkey = (sig.stock_code, day)
                if nkey not in self._scope_skip_notified:
                    self._scope_skip_notified.add(nkey)
                    try:
                        self.scope_skip_notify(sig, pos, bar, intent, price, amplitude)
                    except Exception:  # noqa: BLE001 — 告警失败不影响风控/交易
                        logger.warning("sell_scope 跳过告警发送失败", exc_info=True)
            return 0
        return min(intent, platform)

    @staticmethod
    def _signal_rule(sig: SignalEvent) -> str:
        """信号规则名（STOP_LOSS / TRAILING_STOP / TAKE_PROFIT…），供告警与事件展示。"""
        rule = getattr(sig, "signal_type", "")
        return str(getattr(rule, "value", rule) or "")

    @staticmethod
    def _bar_close(bar: BarEvent, stock_code: str) -> Optional[float]:
        """本根 bar 该票收盘价（实盘残缺 bar 可能不含该票 → None）。"""
        stocks = getattr(bar, "stocks", None) or {}
        row = stocks.get(stock_code) or {}
        try:
            close = row.get("close")
            return float(close) if close is not None else None
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _price_amplitude(pos: Position, price: Optional[float]) -> Optional[float]:
        """相对整仓成本价（avg_cost）的触发幅度（%）。"""
        avg = float(getattr(pos, "avg_cost", 0) or 0)
        if price is None or avg <= 0:
            return None
        return round((price / avg - 1.0) * 100.0, 2)

    def on_bar(
        self,
        bar: BarEvent,
        signal_cache: Optional[Dict] = None,
        period: Optional[str] = None,
        inflight_opens: Optional[Dict[int, set]] = None,
        mismatched_risk: bool = False,
        inflight_risk_sells: Optional[Dict[int, set]] = None,
    ) -> List[OrderEvent]:
        """处理一根 bar：取信号 + 风控检查 + 优先级排序 → 返回待执行订单列表。

        订单在下一个 bar 的 open 成交（由 BacktestEngine 调度）。
        信号优先级：风控（止损/止盈/移动止损）> 公式；公式内 CLOSE>REDUCE>ADD>OPEN。
        风控清仓后公式信号不再执行。
        period：C6 按周期节拍驱动——非 None 时只处理 period 相同的策略
        （实盘 5m 边界 bar 不触发 1m 策略，避免风控单串周期）；None=全部（回测/旧调用）。
        inflight_opens：strategy_id → 在途未成交 BUY 的股票集合（实盘引擎从
        _pending_orders 注入）。OPEN 闸门把在途单也计入 max_positions；
        回测不传（上一 bar 队列已在成交步全部落账/丢弃），本 bar 内
        排队的 OPEN 由内部累计跟踪。
        mismatched_risk：错周期 bar 是否跑风控。False 时周期不匹配的策略只更新
        价格快照，不评估止损/止盈/移动止损（周线/月线同样只在自身周期边界评估）。
        默认 False：回测与实盘同口径（实盘由会话三态
        `live_sessions.intraday_mismatched_risk_enabled` 解析后传入：NULL（未显式设置）
        一律为关闭，不因会话内存在分钟策略自动开启；显式值优先。
        14:51 提前收盘价轮次同样显式传 False）。
        inflight_risk_sells：strategy_id → 在途未结清的风控卖单股票集合（回测
        BacktestEngine 从 deferred 队列注入，含缩量未成交残量）。命中时该票本 bar
        只刷新峰值、不再评估风控、不重复产生 trigger 事件与订单——残量由原单在
        后续 bar 继续分批成交（对齐实盘 `inflight_sell` 去重语义）。默认 None=旧行为。
        """
        # 工作副本：本 bar 处理过程中每排一个 OPEN 就地登记，同 bar 后续信号可见
        inflight: Dict[int, set] = {
            sid: set(codes) for sid, codes in (inflight_opens or {}).items()
        }
        # 资金池：本 bar 的额度占用从零开始（默认无额度时该 dict 恒空，零开销）。
        self._budget_committed.clear()
        # W2 动态仓位：本 bar 组合总净值（现金 + 持仓按最近已知价市值），
        # _strategy_fund 据此按 capital_ratio 分配策略资金（fixed 模式不使用）。
        self._bar_equity = self.total_value(bar)
        # B4：任一策略开启择优轮动时，逐股票积累收盘价历史供动量计算（未开启零开销
        # 跳过）。close<=0 为停牌/无数据伪影，不采用。cap 取各策略窗口最大值 +2。
        rot_cap = max(
            (c.rotation_momentum_bars for c in self.strategies if c.rotation_enabled),
            default=0,
        )
        if rot_cap:
            self._feed_price_history(bar, rot_cap + 2)
        orders: List[OrderEvent] = self._breaker_disposal_orders(bar)
        # B 跨策略 OPEN 排序（默认关）：仅在开关打开且行业轴可用时收集延迟下单；
        # 否则 open_sink=None，走原逐策略路径（行为逐位不变）。
        rank_opens = bool(self.industry_rank_enabled
                          and self.current_industry_gate is not None)
        deferred_opens: Optional[list] = [] if rank_opens else None
        for ctx in self.strategies:
            skip_risk = (inflight_risk_sells or {}).get(ctx.strategy_id) or set()
            if period is not None and ctx.period != period:
                # 周期不匹配：默认只更新价格快照，公式信号仍锁策略周期（W4 防
                # 混合周期前视语义不变）。mismatched_risk=True 时才按当前 bar 价
                # 盘中核算风控（实盘会话可显式打开）：实盘快照合成 1m bar → 日线
                # 策略持仓获得盘中风控；回测日线数据无 1m bar → 行为与历史一致。
                # 默认 False 两端同口径（AGENTS「错周期风控默认关闭」）。
                if mismatched_risk:
                    orders.extend(self._process_strategy(
                        ctx, bar, signal_cache, inflight, include_signals=False,
                        skip_risk_codes=skip_risk))
                continue
            strat_orders = self._process_strategy(
                ctx, bar, signal_cache, inflight, skip_risk_codes=skip_risk,
                open_sink=deferred_opens)
            orders.extend(strat_orders)
            # follow_master 状态镜像：slave 处理完自身信号后，对"master 已不再持有"的
            # 持仓股全平（本 bar 已自行卖出的股票去重）。以 master 的已成交持仓为准，
            # 故重启安全、跨周期闭环、REDUCE 不触发（master 仍持仓）。
            if ctx.role == "slave" and ctx.slave_exit_mode == "follow_master":
                already_sold = {
                    o.stock_code for o in strat_orders
                    if o.trade_type == TradeType.SELL
                }
                orders.extend(self._follow_master_state(ctx, bar, already_sold))
        # B 跨策略排序：全部策略收集完成后统一排序下单（行业领先天数 → 强度 →
        # 个股信号持续天数 → 信号时间），再交给原有 max_holdings/max_positions 闸门
        # 截断。关闭时 deferred_opens=None，本段不执行。
        if deferred_opens:
            orders.extend(self._execute_ranked_opens(deferred_opens, bar, inflight))
        if rank_opens:
            self._prune_open_signals()
        # 主从同 bar 时序守卫：slave 的 OPEN 门禁查的是 master 的"已成交"持仓
        # （_has_position），当 master 在本 bar 也决定清仓某股时（其 SELL 尚未成交，
        # 门禁误判 master 仍持有）→ slave 会买入 master 正卖出的同一只股票，违背
        # "从策略只能买入主策略当前持有的同一只股票"。此处按"本 bar 生成的订单"后扫：
        # master 全平某股（CLOSE/止损/止盈/移动止损）→ 剥掉同 bar slave 的 OPEN BUY。
        # 覆盖 master 先于/后于 slave 处理的任意顺序（订单已全部生成，顺序无关）。
        if orders:
            master_full_sells = set()
            for o in orders:
                if (
                    o.trade_type == TradeType.SELL
                    and o.signal_type in _FULL_CLOSE_TYPES
                ):
                    oc = self._find_strategy_by_id(o.strategy_id)
                    if oc is not None and oc.role == "master":
                        master_full_sells.add(o.stock_code)
            if master_full_sells:
                kept: List[OrderEvent] = []
                for o in orders:
                    if (
                        o.trade_type == TradeType.BUY
                        and o.signal_type == SignalType.OPEN
                        and o.stock_code in master_full_sells
                    ):
                        ctx = self._find_strategy_by_id(o.strategy_id)
                        if ctx is not None and ctx.role == "slave":
                            self.recorder.record(
                                gate="slave_master_selling", layer="signal_gate",
                                action="block",
                                portfolio_id=self.portfolio_id,
                                strategy_id=o.strategy_id,
                                stock_code=o.stock_code, bar_time=o.bar_time,
                                param_name="master_strategy_id",
                                param_value=ctx.master_strategy_id,
                                message="从策略 OPEN 拦截：主策略本 bar 同步清仓 %s，"
                                        "禁止同 bar 买入" % o.stock_code,
                            )
                            continue
                    kept.append(o)
                orders = kept
        # 熔断/日内亏损暂停期间：剥掉新开仓 BUY，保留 SELL（止损/止盈/CLOSE/REDUCE）。
        # §88：熔断期间不清仓，仅暂停新开仓。DEBUG——熔断期每 bar 每 BUY 都会触发，
        # INFO 会刷屏；DEBUG 供排查"为何 BUY 信号没下单"（被剥的 BUY 不进返回列表，
        # 上层 live_engine 的 signal SSE 也不会发，故此处是唯一可见点）。
        if self.risk_manager.is_trading_halted():
            stripped = [o for o in orders if o.trade_type == TradeType.BUY]
            if stripped:
                logger.debug(
                    "portfolio %s 熔断/日内暂停中：剥掉 %d 个 BUY 新开仓 "
                    "(保留 SELL 止损/平仓)：%s",
                    self.portfolio_id, len(stripped),
                    ", ".join("%s/%s" % (o.stock_code, o.signal_name) for o in stripped),
                )
                # 区分根因熔断（max_drawdown）还是日内亏损暂停，挂对应调参名
                if self.risk_manager.circuit_breaker_active:
                    p_name, p_val = "max_drawdown", float(self.risk_manager.max_drawdown)
                else:
                    p_name, p_val = "daily_loss_limit", float(self.risk_manager.daily_loss_limit)
                for o in stripped:
                    self.recorder.record(
                        gate="halted_buy_strip", layer="portfolio_risk", action="strip",
                        portfolio_id=self.portfolio_id, strategy_id=o.strategy_id,
                        stock_code=o.stock_code, bar_time=o.bar_time,
                        param_name=p_name, param_value=p_val,
                        requested_qty=o.quantity,
                        message="熔断/日内暂停中剥掉 BUY 新开仓 %s（%s）" % (
                            o.stock_code, o.signal_name),
                    )
            orders = [o for o in orders if o.trade_type != TradeType.BUY]
        return orders

    def _breaker_disposal_orders(self, bar: BarEvent) -> List[OrderEvent]:
        """D4（2026-11-18）：熔断/日亏触发后的减半仓或清仓处置单。

        只在 risk_manager.pending_disposal 被置位后的第一根 bar 生成一次；
        `half` 按整手向下取半（100 股一手，转债按组合 lot），不足一手跳过；
        `liquidate` 全平。默认 halt/notify 不生成，行为与改动前一致。
        回测按下一 bar open 成交，实盘当根提交，两端同源。
        """
        action = getattr(self.risk_manager, "pending_disposal", None)
        if action not in ("half", "liquidate"):
            return []
        self.risk_manager.pending_disposal = None
        orders: List[OrderEvent] = []
        for ctx in self.strategies:
            for stock_code, pos in self._iter_positions(ctx):
                quantity = int(getattr(pos, "quantity", 0) or 0)
                if quantity <= 0:
                    continue
                lot = self._lot(stock_code)
                if action == "liquidate":
                    sell_qty = quantity
                else:
                    sell_qty = ((quantity // 2) // lot) * lot
                    if sell_qty <= 0:
                        continue
                close = (bar.stocks.get(stock_code) or {}).get("close")
                price = _order_price(bar, stock_code, close)
                if price is None or price <= 0:
                    continue
                orders.append(OrderEvent(
                    strategy_id=ctx.strategy_id,
                    portfolio_id=self.portfolio_id,
                    stock_code=stock_code,
                    trade_type=TradeType.SELL,
                    signal_type=SignalType.CLOSE,
                    signal_name="breaker_%s" % action,
                    quantity=sell_qty,
                    price=price,
                    bar_time=bar.bar_time,
                    period=bar.period,
                ))
                self.recorder.record(
                    gate="breaker_action", layer="portfolio_risk",
                    action=action, portfolio_id=self.portfolio_id,
                    strategy_id=ctx.strategy_id, stock_code=stock_code,
                    bar_time=bar.bar_time, param_name="breaker_action",
                    requested_qty=quantity, final_qty=sell_qty,
                    message="熔断/日亏触发处置（%s）：%s %s → %d 股" % (
                        action, ctx.strategy_id, stock_code, sell_qty),
                )
        if orders:
            logger.warning(
                "portfolio %s 熔断处置 %s：生成 %d 个卖出单",
                self.portfolio_id, action, len(orders))
        return orders

    def _note_open_signal(self, ctx: StrategyContext, sig: SignalEvent,
                          bar: BarEvent) -> None:
        """登记本 bar 出现的 OPEN 信号（用于「个股信号持续天数」排序键）。"""
        key = (ctx.strategy_id, sig.stock_code)
        day = bar.bar_time.date()
        self._open_signal_first_seen.setdefault(key, day)
        self._open_signal_seen.add(key)

    def _open_signal_age(self, strategy_id: int, code: str, day: date) -> int:
        first = self._open_signal_first_seen.get((strategy_id, code))
        return max(0, (day - first).days) if first is not None else 0

    def _prune_open_signals(self) -> None:
        """清理本 bar 未再出现的信号持续天数，避免旧记录把天数虚高。"""
        stale = [k for k in self._open_signal_first_seen
                 if k not in self._open_signal_seen]
        for key in stale:
            self._open_signal_first_seen.pop(key, None)
        self._open_signal_seen.clear()

    def _industry_exposure(self, ind, inflight) -> Dict[str, int]:
        """当前行业敞口（已有持仓 + 在途买单），用于同行业上限计数。"""
        codes = set()
        for ctx in self.strategies:
            for code, pos in self._iter_positions(ctx):
                if pos.quantity > 0:
                    codes.add(code)
        for pending in (inflight or {}).values():
            codes.update(pending)
        counts: Dict[str, int] = {}
        for code in codes:
            industry = ind.industry_of(code)
            if industry:
                counts[industry] = counts.get(industry, 0) + 1
        return counts

    def _execute_ranked_opens(self, candidates: list, bar: BarEvent,
                              inflight: Dict[int, set]) -> List[OrderEvent]:
        """B：跨策略 OPEN 统一排序后下单（仅 industry_rank_enabled 且行业轴可用时）。

        排序键：行业领先天数↓ → 行业强度↓ → 个股信号持续天数↓ → 信号时间↑。
        同行业持仓上限（含已有持仓与在途）在此处按排序结果逐个占用额度；
        原 max_holdings/max_positions 闸门仍在 ``_signal_to_order`` 内生效。
        """
        ind = self.current_industry_gate
        day = bar.bar_time.date()
        cap = int(self.industry_max_per_industry or 0)
        counts = self._industry_exposure(ind, inflight) if cap > 0 else {}

        def _key(item):
            ctx, sig = item
            try:
                lead = ind.bull_dur_for_stock(sig.stock_code, day)
                strength = ind.strength_for_stock(sig.stock_code, day)
            except Exception:  # noqa: BLE001
                lead, strength = 0, 0.0
            age = self._open_signal_age(ctx.strategy_id, sig.stock_code, day)
            return (-lead, -strength, -age,
                    getattr(sig, "bar_time", bar.bar_time), sig.stock_code)

        orders: List[OrderEvent] = []
        # 组合级名额：已成交持仓 + 本 bar 已排序下单的股票（排序后才截断，
        # 即「6 选 5」的 5）。轮动换出全平则释放名额。
        held_codes = {
            code for c in self.strategies
            for code, pos in self._iter_positions(c) if pos.quantity > 0
        }
        for ctx, sig in sorted(candidates, key=_key):
            industry = ind.industry_of(sig.stock_code)
            if (self.max_holdings and len(held_codes) >= self.max_holdings
                    and sig.stock_code not in held_codes):
                self._rec(
                    "max_holdings", "portfolio", "block", ctx, sig, bar,
                    param_name="max_holdings",
                    param_value=float(self.max_holdings),
                    actual_value=float(len(held_codes)),
                    message="组合持仓数 %d/%d 已达上限，排序后 %s 不开新仓"
                            % (len(held_codes), self.max_holdings, sig.stock_code),
                )
                continue
            if cap > 0 and industry and counts.get(industry, 0) >= cap:
                self._rec(
                    "industry_cap", "signal_gate", "block", ctx, sig, bar,
                    param_name="industry_max_per_industry",
                    param_value=float(cap),
                    actual_value=float(counts.get(industry, 0)),
                    message="同行业持仓上限：行业 %s 已持有/在途 %d ≥ %d，%s 不开新仓"
                            % (industry, counts.get(industry, 0), cap, sig.stock_code),
                )
                continue
            new_orders = self._signal_to_order(ctx, sig, bar, inflight, set())
            if not new_orders:
                continue
            orders.extend(new_orders)
            for order in new_orders:
                if order.trade_type == TradeType.BUY:
                    held_codes.add(order.stock_code)
                elif (order.trade_type == TradeType.SELL
                      and order.signal_type in _FULL_CLOSE_TYPES):
                    held_codes.discard(order.stock_code)
            if industry:
                counts[industry] = counts.get(industry, 0) + 1
        return orders

    def forced_close_orders(
        self, bar: BarEvent, mapping: Dict[int, Set[str]],
        new_open_qty: Optional[Dict[int, Dict[str, int]]] = None,
    ) -> List[OrderEvent]:
        """反转平仓（规范 3.6 ``close_next_open``）：按策略生成 SELL 订单。

        只生成 SELL，不评估公式信号与风控——调用方在 14:50 收盘复核后单独驱动，
        并以 ``post_close=True`` 提交，由既有 deferred 机制在下一交易日首根 bar
        放行（A 股 T+1，次日开盘为最早可卖时点）。三源去重（风控 > 策略 > 反转）
        由调用方在提交前完成；本方法只按持仓生成卖单，不叠加数量。

        护栏①（2026-11-23）：只撤「本轮新开仓」。``new_open_qty`` 为调用方按
        当日 BUY 成交汇总的 (策略 → 代码 → 股数)；缺省时退化为当日持仓分桶
        ``pos._lots[today]``。当日之前就有的既有仓一律不动，可卖量为 0 则跳过。
        """
        orders: List[OrderEvent] = []
        if not mapping:
            return orders
        self._bar_equity = self.total_value(bar)
        bar_day = (bar.bar_time.date()
                   if getattr(bar, "bar_time", None) is not None else None)
        for strategy_id, codes in mapping.items():
            ctx = self.find_strategy(strategy_id)
            if ctx is None or not codes:
                continue
            for code in sorted(codes):
                if code not in bar.stocks:
                    continue
                pos = ctx.positions.get(code)
                if pos is None or pos.quantity <= 0:
                    continue
                if new_open_qty is not None:
                    new_qty = int(
                        (new_open_qty.get(strategy_id) or {}).get(code, 0) or 0)
                else:
                    new_qty = int(
                        (getattr(pos, "_lots", {}) or {}).get(bar_day, 0) or 0)
                if new_qty <= 0:
                    logger.debug(
                        "反转平仓跳过 %s/%s：当日无新开仓（既有仓不动）",
                        strategy_id, code,
                    )
                    continue
                sig = SignalEvent(
                    strategy_id=strategy_id,
                    stock_code=code,
                    signal_name="reversal_close",
                    signal_type=SignalType.CLOSE,
                    bar_time=bar.bar_time,
                )
                orders.extend(self._signal_to_order(
                    ctx, sig, bar, {}, set(), quantity_cap=new_qty))
        return orders

    def _process_strategy(
        self, ctx: StrategyContext, bar: BarEvent, signal_cache,
        inflight: Dict[int, set], include_signals: bool = True,
        skip_risk_codes: Optional[set] = None,
        open_sink: Optional[list] = None,
    ) -> List[OrderEvent]:
        """单策略 bar 处理：公式信号 + 风控检查 → 合并排序 → 转订单。

        include_signals=False（周期解耦盘中风控）：跳过公式信号评估，仅产出
        风控单——非本策略周期的 bar 到来时按当前 bar 价核算止损/止盈/移动止损
        （实盘快照合成 1m bar 盘中风控；信号周期锁不受影响）。
        skip_risk_codes：在途风控卖单未结清的股票（含缩量残量）→ 跳过风控评估，
        避免同一持仓重复触发；峰值仍由 _check_risks_locked 内部刷新。
        open_sink：非 None 时（B 跨策略排序开启）把 OPEN 信号收集到该列表，
        由 on_bar 在全部策略处理完后统一排序下单；其它类型信号仍即时转订单。
        """
        signals = (
            ctx.get_signal(bar, signal_cache=signal_cache)
            if include_signals else []
        )
        # 风控检查：对每只持仓股票检查止损/止盈/移动止损
        risk_signals = self._check_risks(ctx, bar, skip_codes=skip_risk_codes)

        # E3 空头减仓档（默认关）：每空头连续段内每只持仓只减一次
        bear_signals = (
            self._gate_bear_reduce_signals(ctx, bar) if include_signals else []
        )
        # 合并并按优先级排序：风控优先，公式按 CLOSE>REDUCE>ADD>OPEN
        all_signals = risk_signals + signals + bear_signals
        all_signals.sort(key=lambda s: _signal_priority(s.signal_type))

        orders: List[OrderEvent] = []
        # 本 bar 已生成全平订单的股票集合：清仓后抑制该股票后续信号
        cleared: set = set()
        for sig in all_signals:
            if sig.stock_code in cleared:
                # 该股票已清仓，跳过后续公式信号（风控信号不会重复，仍跳过）
                continue
            if open_sink is not None and sig.signal_type == SignalType.OPEN:
                # B 跨策略排序：延迟到全部策略处理完，按行业强弱统一排序下单
                open_sink.append((ctx, sig))
                self._note_open_signal(ctx, sig, bar)
                continue
            new_orders = self._signal_to_order(ctx, sig, bar, inflight, cleared)
            if not new_orders:
                continue
            orders.extend(new_orders)
            # 全平类 SELL（含轮动 rotation_exit）落单后，抑制该股本 bar 后续信号
            for o in new_orders:
                if o.trade_type == TradeType.SELL and o.signal_type in _FULL_CLOSE_TYPES:
                    cleared.add(o.stock_code)
        return orders

    def _check_risks(self, ctx: StrategyContext, bar: BarEvent,
                     skip_codes: Optional[set] = None) -> List[SignalEvent]:
        """风控检查入口：整段持组合账本锁（P0-2）。

        风控判定读 ``quantity/avg_cost/highest_price`` 并写 ``update_high``，必须
        与 deals 回填的镜面更新互斥——否则可能读到半更新持仓（数量已改、成本未改），
        算出的止损卖出数量与账本不一致。本段纯内存计算（无 I/O、无公式求值），
        持锁时间微秒级，不会阻塞回填。
        """
        with self._book_guard():
            return self._check_risks_locked(ctx, bar, skip_codes=skip_codes)

    def _check_risks_locked(self, ctx: StrategyContext, bar: BarEvent,
                            skip_codes: Optional[set] = None) -> List[SignalEvent]:
        """对策略持仓检查止损/止盈/移动止损，返回风控 SignalEvent。

        口径固化（2026-11-11）：峰值 ``highest_price`` 更新与三类风控判定
        **一律只用 ``bar.stocks[code].close``**（缺价则跳过该票）。禁止改成
        high/low/均价：high 是影线极值，会把「持仓期最高价」抬高并放大回撤，
        让移动止损/止损在 close 未触及时误触发；low 反之会漏触发。回测与实盘
        共用本函数，改口径等于同时改两端历史结果，必须走独立评审。
        """
        risk_manager = getattr(ctx, "strategy_risk", None)
        if risk_manager is None:
            # #29：风控未注入不能静默跳过（止损/止盈/移动止损全失效无痕迹）。
            # 告警让失效可见；不 raise 以免中断同组合其他策略的 on_bar。
            logger.warning(
                "strategy %s has no strategy_risk, risk checks skipped",
                ctx.strategy_id,
            )
            self.recorder.record(
                gate="missing_strategy_risk", layer="signal_gate", action="block",
                portfolio_id=self.portfolio_id, strategy_id=ctx.strategy_id,
                bar_time=bar.bar_time,
                message="策略 %s 未注入 strategy_risk，止损/止盈/移动止损全部跳过"
                        % ctx.strategy_id,
            )
            return []
        risks: List[SignalEvent] = []
        for stock_code, pos in self._iter_positions(ctx):
            if pos.quantity == 0 or stock_code not in bar.stocks:
                continue
            close = bar.stocks[stock_code]["close"]
            if close <= Decimal("0"):
                # 停牌/无数据 bar（TQ NaN → close=0）：先算盈亏会得到 -100% 的
                # 假止损并污染信号流（2026-11-06 实测 601989/000939/002013）。
                # 在风控计算之前拦截，只留一条可见的「跳过」记录。
                self.recorder.record(
                    gate="risk_skip_no_price", layer="strategy_risk", action="skip",
                    portfolio_id=self.portfolio_id, strategy_id=ctx.strategy_id,
                    stock_code=stock_code, bar_time=bar.bar_time,
                    message="跳过风控：%s 无有效价格（close=%s），不计算盈亏、不发信号"
                            % (stock_code, fmt_price(close)),
                )
                continue
            # 移动止损基准：持仓期间最高价随每根 bar 收盘价刷新（只升不降）
            prev_high = pos.highest_price
            pos.update_high(close)
            # 方案 E：峰值被抬高时落库（仅实盘注入 hook；回测 None 零开销）。
            # 只升不降语义由 hook 实现侧再校验一次（多策略同代码取 max）。
            if self.peak_persist_hook is not None and pos.highest_price > prev_high:
                try:
                    self.peak_persist_hook(
                        self.portfolio_id, stock_code, pos.highest_price,
                        bar.bar_time,
                    )
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "peak persist hook failed: portfolio=%s code=%s",
                        self.portfolio_id, stock_code, exc_info=True,
                    )
            # B5：移动止损阈值按市场状态解析（未开闸/非趋势态回落策略风控原值，行为不变）
            eff_trailing = ctx.effective_trailing_stop(self.current_regime)
            # E 行业转空：该行业持仓移动止损收紧到配置比例（默认关；不改变
            # 止损/止盈的判定顺序，只在原移动止损阈值上收紧）。
            if (self.industry_trail_tighten_enabled
                    and self.current_industry_gate is not None):
                industry_state = self._industry_state(stock_code, bar)
                if industry_state is not None and industry_state.is_bear:
                    eff_trailing = self.industry_trail_tighten_ratio
            if skip_codes and stock_code in skip_codes:
                # 在途风控卖单（含缩量未成交残量）未结清：本 bar 不重复评估/不重复
                # 发 trigger，残量由原单在后续 bar 继续分批成交（对齐实盘
                # `inflight_sell` 去重：一次触发 + 缩量分批，而非逐日重复触发）。
                # 峰值刷新在其上方完成，移动止损基准不丢。
                continue
            if risk_manager.check_stop_loss(pos, close):
                loss = (pos.avg_cost - close) / pos.avg_cost if pos.avg_cost else Decimal("0")
                risks.append(SignalEvent(
                    strategy_id=ctx.strategy_id, stock_code=stock_code,
                    signal_name="stop_loss", signal_type=SignalType.STOP_LOSS,
                    bar_time=bar.bar_time,
                ))
                self.recorder.record(
                    gate="stop_loss", layer="strategy_risk", action="trigger",
                    portfolio_id=self.portfolio_id, strategy_id=ctx.strategy_id,
                    stock_code=stock_code, bar_time=bar.bar_time,
                    param_name="stop_loss_ratio",
                    param_value=float(risk_manager.stop_loss_ratio),
                    actual_value=float(loss),
                    message="止损：整仓成本 %s 现价 %s 亏 %s%s" % (
                        fmt_price(pos.avg_cost), fmt_price(close), fmt_pct(loss),
                        pos.cost_note()),
                )
            elif risk_manager.check_take_profit(pos, close):
                profit = (close - pos.avg_cost) / pos.avg_cost if pos.avg_cost else Decimal("0")
                risks.append(SignalEvent(
                    strategy_id=ctx.strategy_id, stock_code=stock_code,
                    signal_name="take_profit", signal_type=SignalType.TAKE_PROFIT,
                    bar_time=bar.bar_time,
                ))
                self.recorder.record(
                    gate="take_profit", layer="strategy_risk", action="trigger",
                    portfolio_id=self.portfolio_id, strategy_id=ctx.strategy_id,
                    stock_code=stock_code, bar_time=bar.bar_time,
                    param_name="take_profit_ratio",
                    param_value=float(risk_manager.take_profit_ratio),
                    actual_value=float(profit),
                    message="止盈：整仓成本 %s 现价 %s 盈 %s%s" % (
                        fmt_price(pos.avg_cost), fmt_price(close), fmt_pct(profit),
                        pos.cost_note()),
                )
            elif risk_manager.check_trailing_stop(pos, close, ratio=eff_trailing):
                drawdown = (
                    (pos.highest_price - close) / pos.highest_price
                    if pos.highest_price else Decimal("0")
                )
                risks.append(SignalEvent(
                    strategy_id=ctx.strategy_id, stock_code=stock_code,
                    signal_name="trailing_stop", signal_type=SignalType.TRAILING_STOP,
                    bar_time=bar.bar_time,
                ))
                self.recorder.record(
                    gate="trailing_stop", layer="strategy_risk", action="trigger",
                    portfolio_id=self.portfolio_id, strategy_id=ctx.strategy_id,
                    stock_code=stock_code, bar_time=bar.bar_time,
                    param_name="trailing_stop_ratio",
                    param_value=float(
                        eff_trailing if eff_trailing is not None
                        else risk_manager.trailing_stop_ratio
                    ),
                    actual_value=float(drawdown),
                    message="移动止损：整仓成本 %s 最高 %s 现价 %s 回撤 %s%s" % (
                        fmt_price(pos.avg_cost), fmt_price(pos.highest_price),
                        fmt_price(close), fmt_pct(drawdown), pos.cost_note()),
                )
        return risks

    def maybe_cash_enhancement(self, bar: BarEvent, total_value: Decimal) -> None:
        """现金管理增强（预留触发点）：命中阈值仅记决策事件，不产生任何订单。

        触发条件（OR，阈值非 None 才判）：
        - 组合净资产 total_value ≥ cash_enhance_equity_threshold；
        - 持仓占比 holdings_value / total_value ≤ cash_enhance_position_ratio_threshold。
        每交易日最多记一次（_cash_enhance_last_date 去重）。
        """
        if not self.cash_enhance_enabled:
            return
        day = bar.bar_time.date() if bar.bar_time is not None else None
        if day is not None and self._cash_enhance_last_date == day:
            return
        eq_hit = (
            self.cash_enhance_equity_threshold is not None
            and total_value >= self.cash_enhance_equity_threshold
        )
        ratio_hit = False
        if self.cash_enhance_position_ratio_threshold is not None and total_value > 0:
            ratio = self._holdings_value() / total_value
            ratio_hit = ratio <= self.cash_enhance_position_ratio_threshold
        if not (eq_hit or ratio_hit):
            return
        if day is not None:
            self._cash_enhance_last_date = day
        self.recorder.record(
            gate="cash_enhance_trigger", layer="portfolio_risk", action="trigger",
            portfolio_id=self.portfolio_id,
            bar_time=bar.bar_time,
            actual_value=float(total_value),
            message="现金管理增强为预留接口，待实现",
        )

    def capital_base(self) -> Decimal:
        """组合配置资金口径（起账资金）。

        真实侧占比缩量（live_engine._capital_share）与 G1 资金强制共用同一
        口径；借据净额（借入/贷出）由引擎在此基数上叠加，不回写本方法。
        """
        return self.account.initial_capital

    def _strategy_fund(self, ctx: StrategyContext) -> Decimal:
        """策略可分配资金（下单量基准）。

        dynamic（默认）：capital_ratio × min(组合当前总净值, 容量上限)（随盈亏伸缩，
        但不超过 equity_cap——超出部分视作已撤回/封盘，不再放大仓位，防止长周期
        复利开出不可实现的天文仓位）；fixed（旧行为）：capital_ratio × 组合初始资金。
        _bar_equity 为 None（未处理 bar，如直接调 _signal_to_order 的单测）→ 回落初始资金。
        """
        if self.position_sizing == "fixed":
            return ctx.capital_ratio * self.account.initial_capital
        equity = self._bar_equity
        if equity is None:
            equity = self.account.initial_capital
        if self.equity_cap is not None:
            equity = min(equity, self.equity_cap)
        return ctx.capital_ratio * equity

    def _apply_capital_budget(
        self, ctx: StrategyContext, sig: SignalEvent, bar: BarEvent,
        quantity: int, close: Decimal, lot: int,
    ) -> int:
        """资金池策略额度闸（0021 B2）：本窗口策略总市值 ≤ ``ctx.capital_budget``。

        默认 ``None``（未接入）→ 原样返回，不记事件、零行为变化。
        接入后按「额度 − 当前策略持仓市值」反推可买整手数：超出缩量，
        剩余不足一手直接拦截；不放大、不跨策略借额度（借入由 B3/E 建模）。
        """
        budget = getattr(ctx, "capital_budget", None)
        if budget is None:
            return quantity
        if not isinstance(budget, Decimal):
            try:
                budget = Decimal(str(budget))
            except Exception:  # noqa: BLE001 - 非法额度按 0 处理（保守）
                budget = Decimal(0)
        held = (self._holdings_value(ctx)
                + self._budget_committed.get(ctx.strategy_id, Decimal(0)))
        remaining = budget - held
        if remaining <= 0:
            self._rec("capital_quota_exhausted", "capital_gate", "block",
                      ctx, sig, bar,
                      param_name="capital_budget", param_value=float(budget),
                      actual_value=float(held),
                      message="策略额度 %s 已用尽（当前市值 %s），禁止新开仓/加仓"
                              % (fmt_money(budget), fmt_money(held)))
            return 0
        cap_qty = int(remaining / close / lot) * lot
        if cap_qty < lot:
            self._rec("capital_quota_below_lot", "capital_gate", "block",
                      ctx, sig, bar,
                      param_name="capital_budget", param_value=float(budget),
                      actual_value=float(remaining),
                      message="策略额度剩余 %s 不足一手（价 %s × %d 股），跳过本单"
                              % (fmt_money(remaining), fmt_price(close), lot))
            return 0
        if quantity <= cap_qty:
            self._budget_committed[ctx.strategy_id] = (
                self._budget_committed.get(ctx.strategy_id, Decimal(0))
                + close * quantity)
            return quantity
        self._rec("capital_quota_shrink", "capital_gate", "shrink",
                  ctx, sig, bar,
                  param_name="capital_budget", param_value=float(budget),
                  actual_value=float(cap_qty), requested_qty=int(quantity),
                  message="策略额度 %s 已用 %s，本单 %d 股缩量至 %d 股"
                          % (fmt_money(budget), fmt_money(held),
                             int(quantity), int(cap_qty)))
        self._budget_committed[ctx.strategy_id] = (
            self._budget_committed.get(ctx.strategy_id, Decimal(0))
            + close * cap_qty)
        return cap_qty

    def _signal_to_order(
        self, ctx: StrategyContext, sig: SignalEvent, bar: BarEvent,
        inflight: Optional[Dict[int, set]] = None,
        cleared: Optional[set] = None,
        quantity_cap: Optional[int] = None,
    ) -> List[OrderEvent]:
        """信号转 OrderEvent 列表（B4 轮动一单进两单出）。下单量按策略表参数计算（非硬编码）。
        - 全平类（CLOSE/止损/止盈/移动止损）：量 = 持仓全量
        - REDUCE：量 = 持仓 × reduce_position_ratio
        - OPEN：量 = single_open_ratio × 策略资金 / 价（受 max_positions 约束）
        - ADD：需现价较成本下跌 ≥ add_position_threshold 且 add_count < max_add_count，
               量 = add_position_ratio × 策略资金 / 价
        资金审批（现金/策略上限）仍在 ExecutionEngine 缩减。
        cleared：本 bar 已生成全平订单的股票集合（轮动选轮出候选时排除）。"""
        pos = ctx.positions.get(sig.stock_code)
        close = bar.stocks[sig.stock_code]["close"]
        # 停牌/无数据 bar：close 经 TQ NaN 规整为 0 → 无法计算下单量（除零）
        # 也无有效成交价 → 跳过该 bar 所有订单
        if close <= Decimal("0"):
            self._rec("halted_bar_no_price", "signal_gate", "block", ctx, sig, bar,
                      message="停牌/无数据 bar close=0，无法定价/计算下单量")
            return []
        # 停牌闸（J2，默认集合为空 → 无副作用）：停牌期间禁止开新仓/加仓，
        # 卖出与风控一律不受影响（停牌本想卖也卖不掉，交给柜台/复牌后处理）。
        if (self.suspended_codes
                and sig.stock_code in self.suspended_codes
                and sig.signal_type in (SignalType.OPEN, SignalType.ADD)):
            self._rec("corporate_suspend", "signal_gate", "block", ctx, sig, bar,
                      message="停牌期间禁止开仓/加仓（corporate_actions.suspend）")
            return []
        # 除权除息日闸（2026-11-22）：当日除权的票跳过 OPEN/ADD（不排队、次日按
        # 新价重评）；CLOSE/REDUCE/止损止盈不受影响。取不到事件数据时 fail-open
        # 放行并记一条 ex_dividend_data_unavailable（每日至多一条，不误拦）。
        if sig.signal_type in (SignalType.OPEN, SignalType.ADD):
            bar_day = getattr(bar, "bar_time", None)
            bar_day = bar_day.date() if bar_day is not None else None
            if self.ex_dividend_data_ok is False and bar_day is not None:
                if bar_day not in self._ex_div_noted_days:
                    self._ex_div_noted_days.add(bar_day)
                    self._rec("ex_dividend_data_unavailable", "signal_gate", "notice",
                              ctx, sig, bar,
                              message="除权事件数据不可用，已 fail-open 放行（请补跑公司行为同步）")
            if self.ex_dividend_codes and sig.stock_code in self.ex_dividend_codes:
                self._rec("corporate_ex_dividend", "signal_gate", "block", ctx, sig, bar,
                          message="除权除息日跳过开仓/加仓，次日按新价重评"
                                  "（corporate_actions.ex_date）")
                return []
        # 时点股票池闸（J7，默认 pool_timeline=None → 整段跳过）：
        # 只拦 OPEN/ADD；该交易日无任何成员记录时 fail-open 放行（回落当前池），
        # 每组合每日只记一条 pool_pit_uncovered，避免历史未录段把回测变零交易。
        if (self.pool_timeline is not None
                and sig.signal_type in (SignalType.OPEN, SignalType.ADD)):
            bar_day = getattr(bar, "bar_time", None)
            bar_day = bar_day.date() if bar_day is not None else None
            if bar_day is not None:
                if not self.pool_timeline.covered(bar_day):
                    if bar_day not in self._pool_pit_noted_days:
                        self._pool_pit_noted_days.add(bar_day)
                        self._rec("pool_pit_uncovered", "signal_gate", "notice",
                                  ctx, sig, bar,
                                  message="该交易日无时点成分记录，按当前池放行（fail-open）")
                elif not self.pool_timeline.contains(sig.stock_code, bar_day):
                    self._rec("pool_pit_out_of_pool", "signal_gate", "block",
                              ctx, sig, bar,
                              message="该交易日不在股票池时点成分内，禁止开仓/加仓"
                                      "（pool_pit_enabled）")
                    return []
        # 策略资金 = capital_ratio × 组合当前总净值（dynamic）或初始资金（fixed）
        strategy_fund = self._strategy_fund(ctx)

        # 主从联动（§89）：从策略 OPEN 只能买主策略当前持有的同一只股票；
        # 主策略清仓（含该股）后从策略不可新开仓。仅约束新开仓（OPEN），
        # ADD/REDUCE/全平类不受约束（存量可自行卖出/加减）。
        if sig.signal_type == SignalType.OPEN and ctx.role == "slave":
            master_ctx = self._find_strategy_by_id(ctx.master_strategy_id)
            if master_ctx is None or not self._has_position(master_ctx, sig.stock_code):
                self._rec("slave_master_block", "signal_gate", "block", ctx, sig, bar,
                          param_name="master_strategy_id",
                          param_value=ctx.master_strategy_id,
                          message="从策略 OPEN 需主策略持有同一只股票，主策略无持仓")
                return []

        if sig.signal_type in (SignalType.CLOSE, SignalType.STOP_LOSS,
                               SignalType.TAKE_PROFIT, SignalType.TRAILING_STOP):
            # 全平类：需有持仓
            if pos is None or pos.quantity == 0:
                return []
            # 卖出范围口径：platform_only 只卖平台仓（全平允许剩余零股，故不取整）
            # quantity_cap：反转平仓只撤当日新开仓（护栏①），在口径收窄前先夹总量。
            intent_qty = pos.quantity
            if quantity_cap is not None:
                intent_qty = min(intent_qty, max(0, int(quantity_cap)))
            quantity = self._scope_sell_quantity(ctx, sig, bar, pos, intent_qty)
            if quantity <= 0:
                return []
            trade_type = TradeType.SELL
        elif sig.signal_type == SignalType.REDUCE:
            if pos is None or pos.quantity == 0:
                return []
            lot = self._lot(sig.stock_code)
            # E3：信号级 reduce_ratio（空头减仓档）优先，否则用策略默认减仓比例
            reduce_ratio = (
                sig.reduce_ratio if getattr(sig, "reduce_ratio", None) is not None
                else ctx.reduce_position_ratio
            )
            # 卖出范围口径：先按意图量收窄（platform_only 夹到平台仓），再按整手向下取整
            intent = self._scope_sell_quantity(
                ctx, sig, bar, pos, int(pos.quantity * reduce_ratio))
            quantity = intent // lot * lot
            if quantity < lot:
                logger.debug(
                    "REDUCE 拦截:计算量 %d <%d（%s 持仓 %d × reduce_ratio %s）",
                    quantity, lot, sig.stock_code, pos.quantity, reduce_ratio,
                )
                self._rec("reduce_qty_too_small", "signal_gate", "block", ctx, sig, bar,
                          param_name="reduce_position_ratio",
                          param_value=float(reduce_ratio),
                          actual_value=float(quantity),
                          requested_qty=pos.quantity,
                          message="REDUCE 减仓比例 %s × 持仓 %d 向下取整后 %d 股 <%d"
                                  % (fmt_pct(reduce_ratio), pos.quantity,
                                     quantity, lot))
                return []
            trade_type = TradeType.SELL
        elif sig.signal_type == SignalType.OPEN:
            # OPEN = 开新仓（买入一只当前不持有的股票）。本票已持仓 → 忽略：
            # 加仓是 ADD 的职责（需满足回撤阈值 + max_add_count）。否则公式持续发
            # OPEN（电平信号）会对同一只票每根 bar 加仓一次（实盘 1m 刷屏下单根因）。
            if pos is not None and pos.quantity > 0:
                self._rec("open_already_holding", "signal_gate", "block", ctx, sig, bar,
                          message="OPEN 信号时本票已持仓 %d 股（加仓走 ADD），忽略开仓"
                                  % pos.quantity)
                return []
            # 风控冷静期（回测/实盘共用，2026-11-12）：止损/止盈/移动止损成交后 N 个
            # 交易日内禁止新开仓（当日算第 1 天）。放在 max_holdings/槽位闸之前，
            # 轮动换入（_try_rotation）同受此闸约束——两者都是「开新仓」。
            if self._risk_cooldown_block(ctx, sig, bar):
                return []
            # 市场多空门控（2026-11-18）：基准指数空头状态禁止开新仓（持仓不强平）。
            # 放在冷静期之后、max_holdings/槽位闸之前——与冷静期同属"开新仓"闸；
            # 轮动换入（_try_rotation）同样受此闸约束。
            gate_reason = self._gate_decision(bar.bar_time, ctx, sig.stock_code)
            if gate_reason is not None:
                if gate_reason == "industry":
                    self._rec("industry_gate", "signal_gate", "block", ctx, sig, bar,
                              message=self._industry_block_message(
                                  sig.stock_code, bar, "跳过开仓"))
                else:
                    self._rec("market_gate", "signal_gate", "block", ctx, sig, bar,
                              message="市场门控为空头，跳过开仓")
                return []
            # D 行业超买（z>=2.5）不开新仓（默认关）：只拦 OPEN，不强平已有持仓。
            if self.industry_overbought_block and self.current_industry_gate is not None:
                state = self._industry_state(sig.stock_code, bar)
                if state is not None and state.overbought:
                    self._rec(
                        "industry_overbought", "signal_gate", "block", ctx, sig, bar,
                        param_name="industry_overbought_z",
                        param_value=2.5,
                        actual_value=(None if state.bias_z is None
                                      else float(state.bias_z)),
                        message="行业超买（z=%s ≥ 2.5），%s 不开新仓"
                                % (state.bias_z, sig.stock_code),
                    )
                    return []
            # 组合级 max_holdings 闸（回测/实盘共用 on_bar，一处接线两端生效）：
            # 组合当前去重股票数（跨策略按 stock_code 去重，主从同票合并计数）
            # ≥ max_holdings → 拒绝新开仓。ADD/减仓/清仓类不受影响；
            # max_holdings<=0 视为未启用（直接构造 Portfolio 默认不限）。
            port_codes = {
                code for c in self.strategies
                for code, p in self._iter_positions(c) if p.quantity > 0
            }
            if self.max_holdings and len(port_codes) >= self.max_holdings:
                self._rec("max_holdings", "portfolio", "block", ctx, sig, bar,
                          param_name="max_holdings",
                          param_value=float(self.max_holdings),
                          actual_value=float(len(port_codes)),
                          message="组合持仓数 %d/%d 已达上限，%s 不开新仓"
                                  % (len(port_codes), self.max_holdings, sig.stock_code))
                return []
            # 在途单计入（max_positions 超开修复，回测 id7 实测根因）：held 只数已成交
            # 持仓，同 bar 齐发的 OPEN / 实盘 submitted 未成交单不占位 → 超开后闸门
            # 永久拦截。inflight 含本 bar 已排队 + 上层注入的在途未成交 BUY。
            # 在途票若已持有（ADD 在途）不重复占位——held 已含它。
            # 同票在途重复 OPEN 不在此拦：实盘由 _handle_bar 的 inflight_skip 闸拦
            # （同向在途单未确认），回测上一 bar 队列已全部落账/丢弃不会重复。
            inflight_codes = (inflight or {}).get(ctx.strategy_id, set())
            # 槽位口径（方案 A，回测/实盘共用）：只数「平台自己买入成交过」的持仓
            # （platform_quantity>0）。回测所有持仓都来自平台成交，与原来数 quantity>0
            # 完全等价（行为不变）；实盘的外部/手工导入仓（platform_quantity=0）不占名额。
            held_positions = {
                c: p for c, p in self._iter_positions(ctx)
                if p.quantity > 0 and getattr(p, "platform_quantity", 0) > 0
            }
            held = len(held_positions)
            held_codes = set(held_positions)
            effective = held + len(inflight_codes - held_codes)
            # B5：槽位上限按市场状态解析（未开闸/非趋势态回落 max_positions，行为不变）；
            # None = 该策略未配置 → 跳过槽位闸（不限），实盘 _slot_limit 已告警
            pos_limit = ctx.effective_max_positions(self.current_regime)
            if pos_limit is not None and effective >= pos_limit:
                # B4：开启择优轮动时，尝试以更强信号轮出最弱持仓腾槽位；
                # 未开启（默认）或轮动条件不满足时按原逻辑拦截
                if ctx.rotation_enabled:
                    return self._try_rotation(ctx, sig, bar, inflight, cleared)
                logger.debug(
                    "OPEN 拦截:max_positions 已满（持有 %d + 在途 %d >= 上限 %d，%s 不开新仓）",
                    held, len(inflight_codes - held_codes), pos_limit, sig.stock_code,
                )
                self._rec("max_positions_full", "signal_gate", "block", ctx, sig, bar,
                          param_name="max_positions",
                          param_value=float(pos_limit),
                          actual_value=float(effective),
                          message="持股数已达上限 %d（持有 %d 只 + 在途 %d 只），%s 不开新仓"
                                  % (pos_limit, held,
                                     len(inflight_codes - held_codes), sig.stock_code))
                return []
            # 本 bar 排队登记：同 bar 后续 OPEN 信号按已占用空位计数
            if inflight is not None:
                inflight.setdefault(ctx.strategy_id, set()).add(sig.stock_code)
            lot = self._lot(sig.stock_code)
            # C 行业强度作仓位权重（默认关，weight=1 与改动前逐位一致）
            weight = self._industry_open_weight(sig.stock_code, bar)
            quantity = int(
                ctx.single_open_ratio * strategy_fund * weight / close / lot) * lot
            if quantity < lot:
                logger.debug(
                    "OPEN 拦截:计算量 %d <%d（%s ratio %s × 资金 %s / 价 %s）",
                    quantity, lot, sig.stock_code, ctx.single_open_ratio,
                    strategy_fund, close,
                )
                self._rec("open_qty_too_small", "capital_gate", "block", ctx, sig, bar,
                          param_name="single_open_ratio",
                          param_value=float(ctx.single_open_ratio),
                          actual_value=float(quantity),
                          message="OPEN 单笔开仓比例 %s × 策略资金 %s / 价 %s 取整后 %d 股 <%d"
                                  % (fmt_pct(ctx.single_open_ratio), fmt_money(strategy_fund),
                                     fmt_price(close), quantity, lot))
                return []
            # 资金池策略额度闸（0021 B2，默认 None=不约束）
            quantity = self._apply_capital_budget(
                ctx, sig, bar, quantity, close, lot)
            if quantity <= 0:
                return []
            trade_type = TradeType.BUY
        elif sig.signal_type == SignalType.ADD:
            # ADD：需有持仓，且未超 max_add_count。
            # threshold=-1 特殊值 → 跳过 drop 检查（任何价格都加，含上涨/大涨）；
            # 否则现价较成本下跌 ≥ threshold 才加（逢跌加仓，正常取值 1%~20%）。
            # max_add_count 加仓次数上限始终生效（第一道闸）；资金审批/熔断在执行层兜底。
            if pos is None or pos.quantity == 0:
                return []
            # 成本未知（继承仓桥侧未给 cost 且市值不可用 → avg_cost=0）：ADD 不能走。
            # ① threshold 分支要除以 avg_cost，会抛 Decimal DivisionByZero，冒泡到
            #    live_engine._on_bar 的 per-portfolio 兜底 → 该组合本 bar 全部策略
            #    （含止损/止盈/移动止损）被整体跳过，信号存续时每根 bar 复现；
            # ② 即便 threshold=-1 跳过回撤判断，加仓也会把 0 成本摊进加权均价，
            #    之后止损/止盈基准失真。保守：记事件、不加仓。
            if pos.avg_cost <= 0:
                self._rec("add_cost_unknown", "signal_gate", "block", ctx, sig, bar,
                          message="ADD 跳过：持仓成本未知（avg_cost=0），"
                                  "无法判断回撤且加仓会污染加权成本")
                return []
            # 市场多空门控（2026-11-18）：基准指数空头状态禁止加仓（持仓不强平）。
            # 与 OPEN 同一闸；减仓/清仓/风控卖出不受影响。
            gate_reason = self._gate_decision(bar.bar_time, ctx, sig.stock_code)
            if gate_reason is not None:
                if gate_reason == "industry":
                    self._rec("industry_gate", "signal_gate", "block", ctx, sig, bar,
                              message=self._industry_block_message(
                                  sig.stock_code, bar, "跳过加仓"))
                else:
                    self._rec("market_gate", "signal_gate", "block", ctx, sig, bar,
                              message="市场门控为空头，跳过加仓")
                return []
            if ctx.add_position_threshold != Decimal("-1"):
                drop = (pos.avg_cost - close) / pos.avg_cost
                if drop < ctx.add_position_threshold:
                    self._rec("add_threshold_not_met", "signal_gate", "block", ctx, sig, bar,
                              param_name="add_position_threshold",
                              param_value=float(ctx.add_position_threshold),
                              actual_value=float(drop),
                              message="ADD 逢跌加仓：现价较成本回撤 %s < 阈值 %s（成本 %s 现价 %s）"
                                      % (fmt_pct(drop), fmt_pct(ctx.add_position_threshold),
                                         fmt_price(pos.avg_cost), fmt_price(close)))
                    return []
            if pos.add_count >= ctx.max_add_count:
                self._rec("add_count_exceeded", "signal_gate", "block", ctx, sig, bar,
                          param_name="max_add_count",
                          param_value=float(ctx.max_add_count),
                          actual_value=float(pos.add_count),
                          message="ADD 加仓次数已达上限 %d（已加 %d 次）"
                                  % (ctx.max_add_count, pos.add_count))
                return []
            lot = self._lot(sig.stock_code)
            quantity = int(ctx.add_position_ratio * strategy_fund / close / lot) * lot
            if quantity < lot:
                logger.debug(
                    "ADD 拦截:计算量 %d <%d（%s ratio %s × 资金 %s / 价 %s）",
                    quantity, lot, sig.stock_code, ctx.add_position_ratio,
                    strategy_fund, close,
                )
                self._rec("add_qty_too_small", "capital_gate", "block", ctx, sig, bar,
                          param_name="add_position_ratio",
                          param_value=float(ctx.add_position_ratio),
                          actual_value=float(quantity),
                          message="ADD 加仓比例 %s × 策略资金 %s / 价 %s 取整后 %d 股 <%d"
                                  % (fmt_pct(ctx.add_position_ratio), fmt_money(strategy_fund),
                                     fmt_price(close), quantity, lot))
                return []
            # 资金池策略额度闸（0021 B2，默认 None=不约束）
            quantity = self._apply_capital_budget(
                ctx, sig, bar, quantity, close, lot)
            if quantity <= 0:
                return []
            trade_type = TradeType.BUY
        else:
            return []
        return [OrderEvent(
            strategy_id=ctx.strategy_id,
            portfolio_id=self.portfolio_id,
            stock_code=sig.stock_code,
            trade_type=trade_type,
            signal_type=sig.signal_type,
            signal_name=sig.signal_name,
            quantity=quantity,
            price=_order_price(bar, sig.stock_code, close),
            bar_time=bar.bar_time,
            # 风控单（止损/止盈/移动止损）是保护性离场，**不走 risk_timing 顺延**：
            # 触发即当日提交（若 bar 已在 15:00 之后，仍由 _submit_one_order 的收盘
            # 守卫转 deferred——交易所已闭市）。普通信号单保持原有未来函数闸门口径。
            signal_bar_date=(
                None if sig.signal_type in _RISK_PRIORITY
                else _signal_bar_date_for(ctx.period, bar)
            ),
            period=bar.period,
        )]

    def _feed_price_history(self, bar: BarEvent, cap: int) -> None:
        """把本根 bar 各股票有效 close(>0) 追加进收盘历史（cap 截断）。

        同一 bar_time 重复喂入（实盘残缺 bar 分轮推送同票）只记一次，防动量失真。
        """
        for code, ohlcv in bar.stocks.items():
            close = ohlcv.get("close")
            if close is None or close <= 0:
                continue
            if self._price_hist_last_t.get(code) == bar.bar_time:
                continue
            self._price_hist_last_t[code] = bar.bar_time
            hist = self._price_history.setdefault(code, [])
            hist.append(close)
            if len(hist) > cap:
                del hist[: len(hist) - cap]

    def _momentum(self, code: str, bars: int) -> Optional[Decimal]:
        """近 bars 根 bar 动量 = 最新收盘 / bars 根前收盘 - 1；历史不足返回 None。"""
        hist = self._price_history.get(code)
        if hist is None or len(hist) < bars + 1:
            return None
        base = hist[-1 - bars]
        if base <= 0:
            return None
        return hist[-1] / base - 1

    def _try_rotation(
        self,
        ctx: StrategyContext,
        sig: SignalEvent,
        bar: BarEvent,
        inflight: Optional[Dict[int, set]],
        cleared: Optional[set],
    ) -> List[OrderEvent]:
        """B4 满槽择优轮动（rotation_enabled 时由 _signal_to_order 满槽分支调用）。

        新 OPEN 信号动量 ≥ 最弱持仓动量 + rotation_hysteresis →
        返回 [全平最弱持仓 SELL(rotation_exit), 新信号股 BUY(OPEN 原信号名)]；
        否则记 rotation_signal_weak 闸门事件并返回 []（不开新仓）。
        动量 = 近 rotation_momentum_bars 根 bar 收盘涨幅（历史不足则该侧不可比）。
        """
        new_mom = self._momentum(sig.stock_code, ctx.rotation_momentum_bars)
        # 候选轮出持仓：有量、非信号股、非本 bar 已清仓、本 bar 有有效价（能卖出）、
        # T+1 当日可卖、动量可算；取动量最小者
        weakest_code: Optional[str] = None
        weakest_mom: Optional[Decimal] = None
        cleared = cleared or set()
        bar_date = bar.bar_time.date()
        for code, p in self._iter_positions(ctx):
            if p.quantity == 0 or code == sig.stock_code or code in cleared:
                continue
            w_close = bar.stocks.get(code, {}).get("close")
            if w_close is None or w_close <= Decimal("0"):
                continue
            if p.available_shares_on(bar_date) < p.quantity:
                continue
            mom = self._momentum(code, ctx.rotation_momentum_bars)
            if mom is None:
                continue
            if weakest_mom is None or mom < weakest_mom:
                weakest_code, weakest_mom = code, mom
        if (
            weakest_code is None
            or new_mom is None
            or new_mom < weakest_mom + ctx.rotation_hysteresis
        ):
            self._rec(
                "rotation_signal_weak", "signal_gate", "block", ctx, sig, bar,
                param_name="rotation_hysteresis",
                param_value=float(ctx.rotation_hysteresis),
                actual_value=float(new_mom) if new_mom is not None else None,
                message="满槽轮动不成立：新信号 %s 动量 %s，最弱持仓 %s 动量 %s（需 ≥ 其 +%s）"
                        % (sig.stock_code,
                           fmt_pct(new_mom) if new_mom is not None else "N/A",
                           weakest_code or "无",
                           fmt_pct(weakest_mom) if weakest_mom is not None else "N/A",
                           fmt_pct(ctx.rotation_hysteresis)),
            )
            return []
        close = bar.stocks[sig.stock_code]["close"]
        strategy_fund = self._strategy_fund(ctx)
        lot = self._lot(sig.stock_code)
        quantity = int(ctx.single_open_ratio * strategy_fund / close / lot) * lot
        if quantity < lot:
            # 新仓计算量不足一手：不换仓（避免为小额仓位平掉整只持仓）
            self._rec("open_qty_too_small", "capital_gate", "block", ctx, sig, bar,
                      param_name="single_open_ratio",
                      param_value=float(ctx.single_open_ratio),
                      actual_value=float(quantity),
                      message="OPEN 单笔开仓比例 %s × 策略资金 %s / 价 %s 取整后 %d 股 <%d"
                              % (fmt_pct(ctx.single_open_ratio), fmt_money(strategy_fund),
                                 fmt_price(close), quantity, lot))
            return []
        # 资金池策略额度闸（0021 B2/B3，默认 None=不约束）：换入同样受额度限制；
        # 额度不足则不换仓（避免「先卖后买不成」的单边轮出）。
        quantity = self._apply_capital_budget(
            ctx, sig, bar, quantity, close, lot)
        if quantity <= 0:
            return []
        weak_pos = ctx.positions[weakest_code]
        weak_close = bar.stocks[weakest_code]["close"]
        signal_bar_date = _signal_bar_date_for(ctx.period, bar)
        self._rec(
            "rotation_replaced", "signal_gate", "trigger", ctx, sig, bar,
            param_name="max_positions",
            param_value=float(ctx.effective_max_positions(self.current_regime)),
            actual_value=float(new_mom),
            message="满槽轮动：%s 动量 %s ≥ 最弱持仓 %s 动量 %s + 滞环 %s，轮出 %d 股换入"
                    % (sig.stock_code, fmt_pct(new_mom), weakest_code,
                       fmt_pct(weakest_mom), fmt_pct(ctx.rotation_hysteresis),
                       weak_pos.quantity),
        )
        # 本 bar 排队登记新码：同 bar 后续 OPEN 按已占位计数
        if inflight is not None:
            inflight.setdefault(ctx.strategy_id, set()).add(sig.stock_code)
        # 实盘说明：虚拟现金按成交回填，SELL 未成交前 BUY 可能因现金不足被审批缩减/
        # 拒绝（已知退化）；下一 bar 信号存续时常规 OPEN 仍可按空出的槽位开仓。
        sell_order = OrderEvent(
            strategy_id=ctx.strategy_id,
            portfolio_id=self.portfolio_id,
            stock_code=weakest_code,
            trade_type=TradeType.SELL,
            signal_type=SignalType.CLOSE,
            signal_name="rotation_exit",
            quantity=weak_pos.quantity,
            price=_order_price(bar, weakest_code, weak_close),
            bar_time=bar.bar_time,
            signal_bar_date=signal_bar_date,
            period=bar.period,
        )
        buy_order = OrderEvent(
            strategy_id=ctx.strategy_id,
            portfolio_id=self.portfolio_id,
            stock_code=sig.stock_code,
            trade_type=TradeType.BUY,
            signal_type=SignalType.OPEN,
            signal_name=sig.signal_name,
            quantity=quantity,
            price=_order_price(bar, sig.stock_code, close),
            bar_time=bar.bar_time,
            signal_bar_date=signal_bar_date,
            period=bar.period,
        )
        return [sell_order, buy_order]

    def _follow_master_state(
        self,
        slave: StrategyContext,
        bar: BarEvent,
        already_sold: set,
    ) -> List[OrderEvent]:
        """follow_master 状态镜像：slave 持有的股票若 master **当前不再持有** → 全平。

        以 master 的已成交持仓为准（重启后由 live_trades 重建），而非 master 本 bar 的
        信号。语义（闭环）：master 清仓某股后，slave 在其**第一个自身周期且该股有价的
        bar** 全平该股。特性：
        - 重启安全：无需事件队列，master 持仓重建即自动补上未执行的跟随；
        - 跨周期闭环：slave 在自身下一 bar 自然检出（无需跨调用状态）；
        - 幂等：清完即止，不重复触发；
        - REDUCE 不触发：master 仅减仓时 quantity 仍 > 0 → 不跟随；仅 master 归零才跟随。
        不影响 slave 本 bar 已自行触发的卖出（already_sold 去重）；无价则留待下一 bar。
        """
        if slave.master_strategy_id is None:
            return []
        master = self._find_strategy_by_id(slave.master_strategy_id)
        if master is None:
            return []
        follow_orders: List[OrderEvent] = []
        for stock_code, pos in self._iter_positions(slave):
            if pos.quantity == 0 or stock_code in already_sold:
                continue
            if self._has_position(master, stock_code):
                continue
            close = bar.stocks.get(stock_code, {}).get("close")
            if close is None or close <= Decimal("0"):
                continue
            follow_orders.append(OrderEvent(
                strategy_id=slave.strategy_id,
                portfolio_id=self.portfolio_id,
                stock_code=stock_code,
                trade_type=TradeType.SELL,
                signal_type=SignalType.CLOSE,
                signal_name="follow_master",
                quantity=pos.quantity,
                price=_order_price(bar, stock_code, close),
                bar_time=bar.bar_time,
                signal_bar_date=_signal_bar_date_for(slave.period, bar),
                period=bar.period,
            ))
            self.recorder.record(
                gate="follow_master_sell", layer="signal_gate", action="trigger",
                portfolio_id=self.portfolio_id, strategy_id=slave.strategy_id,
                stock_code=stock_code, bar_time=bar.bar_time,
                param_name="master_strategy_id",
                param_value=slave.master_strategy_id,
                message="follow_master：master 已不再持有 %s，slave 同步全平 %d 股"
                        % (stock_code, pos.quantity),
            )
        return follow_orders

    def check_circuit_breaker(self) -> bool:
        return self.risk_manager.circuit_breaker_active

    def find_strategy(self, strategy_id: int) -> Optional[StrategyContext]:
        """按 strategy_id 在本组合策略列表中找上下文（回测/实盘共用，审计 #25 去重）。

        与 _find_strategy_by_id 同义；后者保留供主从联动内部调用，此为对外公共方法。
        """
        for ctx in self.strategies:
            if ctx.strategy_id == strategy_id:
                return ctx
        return None

    def _refresh_price_snapshot(self, bar: BarEvent) -> None:
        """用本根 bar 的有效 close(>0) 刷新持仓最新价快照。

        close=0 是 TQ NaN 规整而来（停牌/无数据），不得采用、不得覆盖已有快照。
        跨日滚动：新交易日第一次刷新时把上一交易日的最后已知价复制为「前收」
        （P1-8 三级兜底第三级）。
        """
        self._roll_valuation_date(bar.bar_time.date())
        for code, ohlcv in bar.stocks.items():
            close = ohlcv.get("close")
            if close is not None and close > 0:
                self._price_snapshot[code] = close

    def _roll_valuation_date(self, day: date) -> None:
        """估值日滚动：进入新交易日时把当前最新价快照冻结为「前收」。"""
        if self._valuation_date is None:
            self._valuation_date = day
            return
        if day <= self._valuation_date:
            return
        self._prev_close_snapshot = {
            code: px for code, px in self._price_snapshot.items() if px > 0}
        self._valuation_date = day

    def _resolve_price(self, code: str, pos: Position) -> Optional[Decimal]:
        """持仓估值价三级兜底（P1-8）：最后已知价 → 成本 → 前收。

        旧实现只有「最后已知价 → avg_cost」两级，而成本未知（继承仓桥侧未给
        cost，avg_cost=0）时会算出市值 0 → 组合净值瞬间塌掉 = 假回撤/假日亏，
        还会误触 max_drawdown 熔断。三级全缺才返回 None（调用方按 0 计并告警，
        由引擎记 risk 事件），**绝不把 avg_cost<=0 当成「价格 0」**。
        """
        price = self._price_snapshot.get(code)
        if price is not None and price > 0:
            return price
        if pos.avg_cost is not None and pos.avg_cost > 0:
            return pos.avg_cost
        prior = self._prev_close_snapshot.get(code)
        if prior is not None and prior > 0:
            return prior
        return None

    def _holdings_value(self, ctx: Optional[StrategyContext] = None) -> Decimal:
        """持仓按最近已知价的市值合计（不含现金）。须在 _refresh_price_snapshot 之后调。

        - 估值价走 ``_resolve_price`` 三级兜底（最后已知价 → 成本 → 前收，P1-8）。
        - 三级全缺 → 该仓按 0 计并登记到 ``_unpriced_codes``（组合层调用时），
          引擎据此记 WARNING + risk 事件；同一代码只在首次出现时 WARNING 一次。
        - ctx=None 合计全部策略；否则只算该策略持仓（供回测策略层净值，保证与组合层
          同一口径、Σ策略市值 == 组合市值）。
        """
        market_value = Decimal("0")
        contexts = (ctx,) if ctx is not None else self.strategies
        unpriced: set = set()
        # 净值/回撤分母的取数口：持账本锁取值，避免与 deals 镜像并发时读到
        # 半更新持仓（P0-2）。
        with self._book_guard():
            for c in contexts:
                for code, pos in self._iter_positions(c):
                    if pos.quantity == 0:
                        continue
                    price = self._resolve_price(code, pos)
                    if price is None:
                        unpriced.add(code)
                        continue
                    market_value += price * pos.quantity
            if ctx is None:
                self._unpriced_codes = unpriced
            new_codes = unpriced - self._unpriced_warned
            self._unpriced_warned &= unpriced
        for code in sorted(new_codes):
            self._unpriced_warned.add(code)
            logger.warning(
                "估值缺价：%s 无最后已知价/成本未知/无前收，按 0 计市值"
                "（组合 %s；净值将偏低，已记 risk 事件）",
                code, self.portfolio_id,
            )
        return market_value

    def holdings_value(self, ctx: StrategyContext) -> Decimal:
        """单策略持仓按最近已知价的市值（回测策略层快照用，口径同 total_value）。"""
        return self._holdings_value(ctx)

    def unpriced_codes(self) -> set:
        """最近一次组合层估值中「三级兜底全缺、按 0 计市值」的代码（P1-8）。

        引擎每根 bar 估值后取一次，转成 ``gate=valuation_missing_price`` 的
        risk 事件，让工作台能看见「净值为什么偏低」，而不是静默按 0 估值。
        """
        with self._book_guard():
            return set(self._unpriced_codes)

    def positions_detail(self, snap_date: date, ctx: Optional[StrategyContext] = None) -> list:
        """当日持仓明细（供回测快照 positions_json；实盘不调用）。

        - ctx=None：组合层，跨策略按 stock_code 聚合（数量/可卖/市值/成本相加）；
          ctx 指定：仅该策略持仓（策略层快照）。
        - 估值价与 total_value/holdings_value 同口径（三级兜底，P1-8）。
        - sellable_quantity 走 Position T+1 分桶（snap_date 前买入的份额）。
        - Decimal 字段序列化为字符串，保证 json.dumps 安全；按股票代码升序确定输出。
        """
        contexts = (ctx,) if ctx is not None else self.strategies
        agg: Dict[str, dict] = {}
        for c in contexts:
            for code, pos in c.positions.items():
                if pos.quantity == 0:
                    continue
                price = self._resolve_price(code, pos) or Decimal("0")
                slot = agg.get(code)
                if slot is None:
                    slot = {
                        "stock_code": code,
                        "quantity": 0,
                        "sellable_quantity": 0,
                        "market_value": Decimal("0"),
                        "cost_value": Decimal("0"),
                    }
                    agg[code] = slot
                slot["quantity"] += pos.quantity
                slot["sellable_quantity"] += pos.available_shares_on(snap_date)
                slot["market_value"] += price * pos.quantity
                slot["cost_value"] += pos.avg_cost * pos.quantity

        detail = []
        for code in sorted(agg):
            slot = agg[code]
            detail.append({
                "stock_code": slot["stock_code"],
                "quantity": slot["quantity"],
                "sellable_quantity": slot["sellable_quantity"],
                "market_value": str(slot["market_value"]),
                "cost_value": str(slot["cost_value"]),
            })
        return detail

    def _refresh_and_value_holdings(self, bar: BarEvent) -> Decimal:
        """刷新价格快照并返回所有持仓市值合计（不含现金）。

        估值口径：对每只持仓取「最近已知价」——本根 bar 带有效 close 的刷新后采用；
        缺席的（残缺 bar / 停牌全天无 bar）沿用快照里上一次的价；从未报过价的回退
        avg_cost。这样 BarPoller 逐票完成的残缺 1m bar 不会让缺席持仓被当 0，总市值连续。
        """
        self._refresh_price_snapshot(bar)
        return self._holdings_value()

    def total_value(self, bar: BarEvent) -> Decimal:
        """组合总市值 = 现金 + 所有策略持仓按最近已知价的市值（回测/实盘共用，审计 #25 去重）。"""
        return self.account.cash + self._refresh_and_value_holdings(bar)

    def risk_value(self, bar: BarEvent) -> Decimal:
        """风险口径净值 = 总市值 − 累计外部净流入（③，熔断/日亏/回撤的唯一分母）。

        入金/出金/分红会让总市值跳动，但那是外部现金流不是策略表现；回测没有
        这类流水，实盘若直接用总市值，入金当天会被当成「暴涨」重锚峰值、出金
        当天被当成回撤触发熔断。剔除净流入后两端口径一致（回测净流入恒 0）。
        仓位分配/资金审批仍用 ``total_value``（实际购买力，入金理应可用）。
        """
        return self.total_value(bar) - self.external_net_inflow

    def risk_equity(self) -> Decimal:
        """无 bar 时的风险口径净值（日终/重启用）= 现金 + 持仓市值 − 累计净流入。"""
        return self.account.cash + self._holdings_value() - self.external_net_inflow

    def current_value(self) -> Decimal:
        """最近已知价口径的组合总市值（现金 + 持仓市值），无需新 bar。
        日终熔断每日亏损用（#8）：14:30 时 _price_snapshot 已由 on_bar_update 刷新。"""
        return self.account.cash + self._holdings_value()

    def _find_strategy_by_id(self, strategy_id: Optional[int]) -> Optional[StrategyContext]:
        """按 strategy_id 在本组合策略列表中找上下文。"""
        if strategy_id is None:
            return None
        for ctx in self.strategies:
            if ctx.strategy_id == strategy_id:
                return ctx
        return None

    def _has_position(self, ctx: StrategyContext, stock_code: str) -> bool:
        """策略是否持有指定股票（quantity > 0）。用于主从联动约束从策略开仓范围。"""
        pos = ctx.positions.get(stock_code)
        return pos is not None and pos.quantity > 0

    def snapshot(self, snap_date: date, current_value: Decimal, bar: BarEvent = None) -> dict:
        # market_value 复用最近已知价口径（与 total_value 一致）：bar 为 None 时
        # 直接用已积累的价格快照 + avg_cost 兜底，不把缺席持仓当 0。
        if bar is not None:
            self._refresh_price_snapshot(bar)
        market_value = self._holdings_value()
        return {
            "snap_date": snap_date,
            "total_value": current_value,
            "cash": self.account.cash,
            "market_value": market_value,
            # 当日持仓明细（仅回测落 positions_json；只增字段，实盘不读本键）
            "positions": self.positions_detail(snap_date),
        }
