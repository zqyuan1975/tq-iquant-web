from sqlalchemy import Column, Integer, String, Text, DateTime, Numeric, ForeignKey, Boolean
from sqlalchemy.sql import func

from .base import Base


class PortfolioStrategy(Base):
    __tablename__ = "portfolio_strategies"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    stock_pool_id = Column(Integer, ForeignKey("stock_pools.id", ondelete="RESTRICT"), nullable=False, index=True)
    # 仅影响新建行：存量行已落库的基准保持不变（API 新建默认同为 000001.SH）
    benchmark_index = Column(String(20), default="000001.SH", server_default="000001.SH", nullable=False)
    # 市场多空门控（2026-11-18）：基准指数空头状态禁止开仓/加仓，持仓不强平。
    # 判定内核 core/engine/market_gate.py（内置 M 状态机，右移一日）。
    # 空/未定状态一律放行；只拦 OPEN/ADD，CLOSE/REDUCE/止损止盈移动止损不受影响。
    market_gate_enabled = Column(Boolean, default=False, server_default="0", nullable=False)
    # 计算通道：python（默认，内置公式）/ tdx（客户端公式库按名求值）
    market_gate_engine = Column(String(10), default="python", server_default="python", nullable=False)
    # tdx 通道的公式名（空=用内核默认 TQGATE）
    market_gate_formula_name = Column(String(50), nullable=True)
    # E3 门控维度扩展（2026-11-18）：全部默认值 = 旧语义。
    # 多指数投票：JSON 数组文本；NULL/空 = 只用 benchmark_index。
    market_gate_indices = Column(Text, nullable=True)
    # any（任一空头即拦，默认）/ majority（过半）/ all（全部空头）
    market_gate_vote_mode = Column(String(10), default="any", server_default="any",
                                   nullable=False)
    # 空头强度分档：只有空头强度 ≥ 该值才投票拦截；0=不设门槛
    market_gate_strength_threshold = Column(Integer, default=0, server_default="0",
                                            nullable=False)
    # 空头减仓档：0=关闭；>0 时空头连续段内每只持仓按该比例减仓一次
    market_gate_bear_reduce_ratio = Column(Numeric(5, 4), default=0, server_default="0",
                                           nullable=False)
    initial_capital = Column(Numeric(15, 2), default=2000000, server_default="2000000", nullable=False)
    max_drawdown = Column(Numeric(5, 4), default=0.2000, server_default="0.2000", nullable=False)
    daily_loss_limit = Column(Numeric(5, 4), default=0.0500, server_default="0.0500", nullable=False)
    # 日内亏损熔断分母口径：
    # equity（默认）= 上一交易日收盘净值 × daily_loss_limit，阈值随净值滚动（修正口径）；
    # initial = initial_capital × daily_loss_limit，阈值恒定（旧行为——净值涨大后
    # 一天 0.2% 的波动就会锁死整日不开仓，属口径缺陷，故默认已改为 equity）。
    daily_loss_basis = Column(String(10), default="equity", server_default="equity", nullable=False)
    max_holdings = Column(Integer, default=10, server_default="10", nullable=False)
    # 资金基准对齐（2026-11-16）：单票集中度上限（占比，0~1）。
    # NULL 或 0 = 不设限（与接线前逐字一致）。**仅实盘接线**：
    # live_service.build_engine 注入 Portfolio → LiveEngine._apply_allocation_caps
    # 按「买入后该代码组合市值 ÷ 组合总资产」截断；回测/增量回测不读（口径冻结）。
    max_single_position_pct = Column(Numeric(5, 4), nullable=True)
    # 现金留存下限（占比，0~1）：NULL 或 0 = 不设限（与接线前逐字一致）。
    # 按「买入后剩余现金 ÷ 组合总资产」截断买入额；同样只接实盘。
    min_cash_reserve_pct = Column(Numeric(5, 4), nullable=True)
    # 资金池目标仓位上限（占比，0~1）：组合级可配置；NULL = 用默认 0.95
    # （0019 §1.7）。只在额度推导里参与组合上限计算，不改变下单/回测默认行为。
    target_utilization = Column(Numeric(5, 4), nullable=True)
    min_commission = Column(Numeric(10, 2), default=5, server_default="5", nullable=False)
    buy_commission_rate = Column(Numeric(8, 6), default=0.000250, server_default="0.000250", nullable=False)
    sell_commission_rate = Column(Numeric(8, 6), default=0.000250, server_default="0.000250", nullable=False)
    stamp_duty_rate = Column(Numeric(8, 6), default=0.000500, server_default="0.000500", nullable=False)
    # 过户费（双向，2026-11-11）：A 股现行按成交额 0.001% 双边收取，默认 0.000010。
    # 回测买卖两侧都计入（买入摊入成本、卖出从盈亏扣），与实盘柜台成本同口径。
    transfer_fee_rate = Column(Numeric(8, 6), default=0.000010, server_default="0.000010", nullable=False)
    slippage = Column(Numeric(8, 6), default=0.002, server_default="0.002000", nullable=False)
    # 风控冷静期（2026-11-12）：止损/止盈/移动止损**成交后**，同一 (策略, 代码)
    # 在接下来 N 个交易日内禁止新开仓（当日算第 1 天，N=1=当日不再买回、次日恢复）；
    # 0=关闭（等价改动前行为）。公式 CLOSE/REDUCE 不触发。回测/实盘共用引擎实现。
    risk_cooldown_days = Column(Integer, default=1, server_default="1", nullable=False)
    trading_session = Column(String(10), default="full", server_default="full", nullable=False)
    # #1 涨跌停约束（回测成交步）：True=涨停买不进/跌停卖不出（延后重试）。
    # 默认开（含 ORM/server 默认）——无约束回测会系统性高估可成交性；旧组合由迁移置 1。
    price_limit_check_enabled = Column(Boolean, default=True, server_default="1", nullable=False)
    # #2 成交量/流动性约束（回测成交步，可选默认关）：单笔 ≤ 前 5 个交易日日均量
    # （股）× ratio；回测/实盘统一口径，严格不含当日（无前视）。
    volume_limit_enabled = Column(Boolean, default=False, server_default="0", nullable=False)
    volume_limit_ratio = Column(Numeric(4, 2), default=0.10, server_default="0.10", nullable=False)
    # 分钟级「单次可下单量」比例（实盘专用，2026-11-19）：单次提交 ≤ 最近完成
    # 1m bar 量 × 本比例；缩掉的残量进当日补单队列按后续 bar 续发。
    # 与 volume_limit_ratio 分离，避免把日线 4% 直接套到分钟单上。
    # NULL=未配置（引擎回落 0.10）；0=关闭分钟闸门（只保留日线口径）。
    minute_volume_limit_ratio = Column(Numeric(5, 4), nullable=True)
    # #4 自适应滑点（回测成交步，可选默认关）：开启后有效滑点 =
    # slippage + slippage_impact_coef × 订单股数/(bar成交量×100)，封顶 0.10，
    # 模拟市场冲击成本（订单量占当日成交量比例越大滑点越高）；与 volume_limit
    # 天然协同（先缩量后按缩量后 quantity 算冲击）。默认关行为与原逻辑一致。
    adaptive_slippage_enabled = Column(Boolean, default=False, server_default="0", nullable=False)
    slippage_impact_coef = Column(Numeric(6, 4), default=0.10, server_default="0.1000", nullable=False)
    # W2 仓位模型：dynamic=按组合当前总净值分配策略资金（默认，随盈亏伸缩）；
    # fixed=按 initial_capital 固定名义（旧行为，风险调整指标被系统性低估）。
    position_sizing = Column(String(10), default="dynamic", server_default="dynamic", nullable=False)
    # W3 容量上限（可选，默认 None）：dynamic 下资金 = capital_ratio × min(当前净值, 上限)。
    # 净值超上限后仓位封顶、超额现金闲置（等价"超容量部分撤回/封盘"）——封堵
    # "20%×不断增长的净值"在长周期复利下开出不可实现的天文仓位（真实容量天花板）。
    # fixed 模式不受影响（本就按 initial_capital 固定）。
    equity_cap = Column(Numeric(15, 2), nullable=True)
    # W2 现金管理增强（预留触发点，默认关）：命中阈值仅记决策事件，不自动下单。
    cash_enhance_enabled = Column(Boolean, default=False, server_default="0", nullable=False)
    cash_enhance_equity_threshold = Column(Numeric(15, 2), nullable=True)
    cash_enhance_position_ratio_threshold = Column(Numeric(5, 4), nullable=True)
    # 公司行为（2026-11-18，J1/J2）：手工事件表 corporate_actions 的总开关与档位。
    # 全部默认关 —— 关闭时不建 tracker、不改持仓、不写新决策事件（行为逐字一致）。
    corporate_actions_enabled = Column(Boolean, default=False, server_default="0", nullable=False)
    # 分红再投资档（仅总开关开启时有意义）：回测按当日收盘价买入整手；
    # 实盘只记事件 + 告警（真下单属人工/半自动范畴，M1 审批队列上线后再接）。
    dividend_reinvest_enabled = Column(Boolean, default=False, server_default="0", nullable=False)
    # 连续停牌超过 N 个交易日才处置（同退市路径）；0 = 不处置（默认）。
    suspension_dispose_days = Column(Integer, default=0, server_default="0", nullable=False)
    # 退市强制了结（默认关；与总开关 AND）：按最后可交易价生成 SELL。
    delist_force_close_enabled = Column(Boolean, default=False, server_default="0", nullable=False)
    # 时点股票池（J7，2026-11-18）：开启后回测/实盘按 stock_pool_memberships 的
    # 生效区间判定当日成分，只拦 OPEN/ADD；关闭（默认）不读成员表，选股范围仍是
    # stock_pool_stocks 当前成分，行为与改动前逐字一致。
    pool_pit_enabled = Column(Boolean, default=False, server_default="0", nullable=False)
    # 熔断处置动作（D4，2026-11-18）：notify 只通知（仍停开仓）/halt 停开仓（默认）
    # /half 减半仓 /liquidate 清仓。触发后本 bar 生成 SELL（回测下一 bar open，
    # 实盘当根提交），并写入 params_snapshot 冻结口径。
    breaker_action = Column(String(16), default="halt", server_default="halt",
                            nullable=False)
    # A2 跨组合下单优先级（2026-11-18）：数值越大越先处理/成交，默认 0。
    # 同一 bar 多组合共享真实侧资金与通道时，按 priority 降序依次走资金分配与
    # 提交链；默认 0 时保持原有组合顺序（行为与改动前逐字一致）。
    priority = Column(Integer, default=0, server_default="0", nullable=False)
    status = Column(String(10), default="active", server_default="active", nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
