from sqlalchemy import Boolean, Column, Float, Integer, String, ForeignKey, Numeric, DateTime
from sqlalchemy.sql import func, text

from .base import Base


class Strategy(Base):
    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True)
    portfolio_id = Column(Integer, ForeignKey("portfolio_strategies.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String(100), nullable=False)
    formula_id = Column(Integer, ForeignKey("formulas.id", ondelete="RESTRICT"), nullable=False, index=True)
    period = Column(String(5), nullable=False)
    # 信号推送频率（可等于或粗于 period）；NULL = 使用 period。
    signal_frequency = Column(String(10), nullable=True)
    # HH:MM；NULL = 按频率默认（1d=14:30，1w/1mon=09:00）。
    signal_at = Column(String(5), nullable=True)
    # 1-5，仅 1w 使用；NULL = 周一。
    signal_weekday = Column(Integer, nullable=True)
    # completed / current；NULL = 1w/1mon 用 completed，其余 current。
    signal_data_end = Column(String(10), nullable=True)
    role = Column(String(15), nullable=False)
    # 自引用：slave 指向同组合 master。ondelete=RESTRICT（审计 #18）——业务规则禁止
    # "slave 无 master"的孤儿行，delete_strategy 已有 app 层预检，DB 层 RESTRICT 兜底。
    master_strategy_id = Column(Integer, ForeignKey("strategies.id", ondelete="RESTRICT"), nullable=True, index=True)
    # 策略级股票池（可空）：空=继承组合级 stock_pool_id；非空=本策略独立池。
    # 用于"不同股票池独立"模式：每个 slave/independent 可有自己的股票池，跨池不下单。
    stock_pool_id = Column(Integer, ForeignKey("stock_pools.id", ondelete="RESTRICT"), nullable=True, index=True)
    # 从策略卖出模式（仅 role="slave" 时有意义，其他角色忽略）：
    #   independent = slave 自身信号/风控触发卖出，不联动 master
    #   follow_master = master 卖出某股时 slave 强制同步卖出该股（若 slave 持有）
    slave_exit_mode = Column(String(15), nullable=False, default="independent", server_default="independent")
    capital_ratio = Column(Numeric(5, 4), default=0.6000, server_default="0.6000", nullable=False)
    max_positions = Column(Integer, default=5, server_default="5", nullable=False)
    single_open_ratio = Column(Numeric(5, 4), default=0.1000, server_default="0.1000", nullable=False)
    stop_loss_ratio = Column(Numeric(5, 4), default=0.0500, server_default="0.0500", nullable=False)
    take_profit_ratio = Column(Numeric(5, 4), default=0.1500, server_default="0.1500", nullable=False)
    trailing_stop_ratio = Column(Numeric(5, 4), default=0.0300, server_default="0.0300", nullable=False)
    add_position_threshold = Column(Numeric(5, 4), default=0.0500, server_default="0.0500", nullable=False)
    max_add_count = Column(Integer, default=2, server_default="2", nullable=False)
    add_position_ratio = Column(Numeric(5, 4), default=0.1000, server_default="0.1000", nullable=False)
    reduce_position_ratio = Column(Numeric(5, 4), default=0.3000, server_default="0.3000", nullable=False)
    # B4 OPEN 择优轮动（默认关）：满槽时强信号可轮出最弱持仓换入新信号股
    rotation_enabled = Column(Boolean, default=False, server_default=text("0"), nullable=False)
    rotation_momentum_bars = Column(Integer, default=20, server_default="20", nullable=False)
    rotation_hysteresis = Column(Numeric(5, 4), default=0.0500, server_default="0.0500", nullable=False)
    # B5 市场状态闸（默认关）：基准周线趋势态(bull)时用 trend_* 覆盖参数，其余态用原参数
    regime_filter_enabled = Column(Boolean, default=False, server_default=text("0"), nullable=False)
    trend_trailing_stop_ratio = Column(Numeric(5, 4), nullable=True)
    trend_max_positions = Column(Integer, nullable=True)
    # #3 策略级敞口上限（默认关）：回测 BUY 审批加卡策略总持仓市值
    # ≤ capital_ratio × 组合初始资金（仅回测成交步生效；实盘不接）
    exposure_cap_enabled = Column(Boolean, default=False, server_default=text("0"), nullable=False)
    # 实盘执行配置（批次1，全部向后兼容：默认关/保守默认）
    # 单笔最大买入金额（元）：BUY 审批时金额超此值→缩量到该金额能买的整手股数
    max_order_amount = Column(Float, nullable=True)
    # 撤单后有限追价重下：超时撤单成功且未超次数 → 剩余量按 tick 追价重下
    requote_enabled = Column(Boolean, default=False, server_default=text("0"), nullable=False)
    requote_max_attempts = Column(Integer, default=2, server_default="2", nullable=False)
    # H4 追价挂单价源：tick=固定 ±1 tick（默认，原行为）/ adaptive=按最新价追、
    # 受 requote_max_slippage_ticks 限制（无行情回退 tick）
    requote_price_mode = Column(String(16), default="tick", server_default="tick", nullable=False)
    requote_max_slippage_ticks = Column(Integer, default=5, server_default="5", nullable=False)
    # TDX 大额拆单（本批次仅落配置管线，拆单逻辑批次 2 实现）
    split_order_enabled = Column(Boolean, default=False, server_default=text("0"), nullable=False)
    split_order_threshold = Column(Float, default=500000.0, server_default="500000.0", nullable=False)
    # H3 切片增强（None/0 = 沿用按阈值的默认推导）
    split_order_count = Column(Integer, nullable=True)          # 指定子单笔数
    split_order_min_qty = Column(Integer, nullable=True)        # 子单最小股数
    split_order_interval_seconds = Column(Float, nullable=True)  # 子单最小间隔（成交后额外等待）
    split_order_interval_jitter = Column(Boolean, default=False, server_default=text("0"), nullable=False)
    split_order_volume_pct = Column(Float, nullable=True)       # 单笔 ≤ 最近完成 bar 成交量 × 比例
    # 风控/信号执行时点：next_day=当日信号次日执行（回测对齐，默认）/ immediate=当日确认立即执行
    risk_timing = Column(String(15), default="next_day", server_default="next_day", nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
