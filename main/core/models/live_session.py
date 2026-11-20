from sqlalchemy import Boolean, Column, Integer, String, DateTime, Numeric, Text
from sqlalchemy.sql import func

from .base import Base


class LiveSession(Base):
    __tablename__ = "live_sessions"

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    mode = Column(String(10), nullable=False)
    # 交易引擎：iquant(QMT 通道) / tdx(通达信TQ) / ths(同花顺UI)。
    # 为 NULL 表示迁移前的历史会话，运行时按全局默认引擎（ths_ui.enabled 判定）解析，
    # 保持升级前行为不变；新建会话一律显式写入。
    # 历史 qmt 会话（通道已移除）由 session_engine 白名单回退到默认引擎。
    engine = Column(String(10), nullable=True)
    # 实盘资金基准（真实账户总资产口径，元）：语义见 baseline_mode/baseline_value。
    # 兼容旧列：等价于 baseline_value（未写 baseline_value 时回退本列）。
    # **只缩不放**：超过真实总资产的填写值一律拒绝写入，起账时再按 min 截断。
    live_capital = Column(Numeric(18, 2), nullable=True)
    # ---- 资金基准对齐（2026-11-16）----
    # baseline_mode：total_asset=起账即用真实总资产（默认）/ manual=显式填写。
    baseline_mode = Column(String(16), nullable=True, server_default="total_asset")
    # 资金基准（真实账户总资产口径，元）；各组合按 initial_capital 占比切分。
    baseline_value = Column(Numeric(18, 2), nullable=True)
    baseline_set_at = Column(DateTime, nullable=True)
    # 基准版本：每次「收盘后重置」+1，参与增量回测指纹与 deferred 单基线标记，
    # 确保旧 checkpoint / 旧 deferred 行不能把新基准覆盖回去。
    baseline_version = Column(Integer, nullable=False, server_default="0", default=0)
    # 起账时是否把柜台继承仓按当前市值扣减虚拟现金（默认 1；0 仅调试旧口径）
    adopt_inherited_positions = Column(
        Boolean, nullable=False, server_default="1", default=True)
    # ---- C2 账户键独占（2026-11-16）----
    # 账户身份键：iquant/goldminer 取桥 /ping 的 account；tdx/ths 取配置账号；
    # 都取不到时降级为 "engine:<key>"（宁可多拦）。启动成功时落库，供后续会话
    # 做账户级互斥判定；account_key_source 记录来源便于排障。
    account_key = Column(String(128), nullable=True)
    account_key_source = Column(String(16), nullable=True)
    # ---- H2 参数绑定（A7，2026-11-16）----
    # 绑定某条回测记录后，启动时把 backtest_records.params_snapshot 与当前组合
    # 参数比对；差异默认只告警（决策事件 params_drift），不做 FK/不强阻断。
    backtest_record_id = Column(Integer, nullable=True)
    # ---- F1 拼盘 / 运行组绑定（0021，2026-11-20）----
    # 与 backtest_record_id 三选一（至多一个非空）：单记录 / 多组合运行组 /
    # L2·L3 拼盘。软引用（不设 FK）：被引用对象删除不影响会话；仅作理论 vs 实盘
    # 对照基准，不影响交易语义。
    backtest_run_group_id = Column(Integer, nullable=True)
    pairing_plan_id = Column(Integer, nullable=True)
    # 执行模式：alert（半自动，只预警不下单）/ auto（现有自动交易）。
    # 迁移前历史会话写 auto；新建会话由 live_service 显式写 alert。
    execution_mode = Column(String(10), nullable=False, server_default="auto")
    # 外部信号注入（0011）：通达信条件预警输出文件路径。NULL=不启用（走 TQ 公式）；
    # 配置后信号来源替换为文件，TQ 公式链路停用（两条路径互斥）。
    signal_file_path = Column(String(500), nullable=True)
    # 14:50 提前收盘价（仅 1d 日线层，会话级开关，B 口径）：默认 False 表示
    # 1d/1w/1mon 都在**周期收盘价落定后**（15:05 轮次）用完整 bar 算信号、次日开盘
    # 执行。True 时仅 1d 提前：用快照合成 14:50 1m bar 收盘价定信号，委托价取
    # 14:52 起始 1m bar 开盘价（14:51 兜底，两者都无则不下单），**该轮信号当日
    # 提交、尾盘成交**（不受 risk_timing=next_day 顺延）；1w/1mon 不受影响
    # （仍走 15:05 收盘落定 + 次日执行）。
    advance_close_enabled = Column(
        Boolean, default=False, server_default="0", nullable=False)
    # 执行时点 HH:MM（默认 14:51；限定 14:51–14:59，须在 14:50 分钟 bar 闭合后）。
    # 实际轮次不早于 14:52（委托价需要 14:52 起始 1m bar 的开盘价）。
    advance_close_time = Column(
        String(5), nullable=False, server_default="14:51", default="14:51")
    # 幂等标记：该交易日 14:52 提前轮次已执行（YYYY-MM-DD，跨重启不重复跑）
    advance_close_done_date = Column(String(10), nullable=True)
    # 幂等标记：该交易日 15:05「收盘落定」日线级别轮次（1d/1w/1mon）已执行
    # （YYYY-MM-DD，跨重启不重复跑；B 开启时只用于 1w/1mon 周期末轮次）
    daily_bars_done_date = Column(String(10), nullable=True)
    # 错周期 bar 是否跑风控（nullable 三态，2026-11-11）：
    #   NULL = 跟随全局 config.tq_trade.intraday_mismatched_risk_enabled（默认 False）
    #   0/1  = 本会话显式覆盖（关闭时错周期 bar 一律只更新价格快照、不跑风控）
    # 历史会话为 NULL → 立即跟随新默认（对齐回测：风控只在自身周期 bar 上评估）。
    intraday_mismatched_risk_enabled = Column(Boolean, nullable=True)
    # ---- E1 执行前重判门控（2026-11-18，默认关闭）----
    # 开启后：顺延次日单 / 盘前集合竞价单在真正提交前，用「执行日之前最近收盘」
    # 的门控状态再判一次；空头则拦下（保持 deferred 并落决策事件）。
    # 默认关 = 完全沿用「信号日决策即执行」的旧行为。
    gate_recheck_on_execute = Column(
        Boolean, nullable=False, server_default="0", default=False)
    # ---- 账户级风控（D1/D2，2026-11-18，默认关闭）----
    # 会话（账户）级聚合熔断：Σ 各组合 risk_equity() 的口径，与组合级独立生效。
    # enabled=0 时引擎完全不读取/不写入本组字段，行为与接线前一致。
    account_risk_enabled = Column(
        Boolean, nullable=False, server_default="0", default=False)
    # 账户级阈值；NULL = 回退 config.tq_trade.account_max_drawdown / 日亏阈值。
    account_max_drawdown = Column(Numeric(9, 6), nullable=True)
    account_daily_loss_limit = Column(Numeric(9, 6), nullable=True)
    # 账户级内存态 JSON（峰值/昨收/暂停态/已预警级别），跨重启读回。
    account_risk_state = Column(Text, nullable=True)
    # A1 资金池模式（2026-11-18）：off（默认，分账不分池）/ equal（等权池）/
    # risk_budget（按 1/组合最大回撤 分配）/ priority（按组合 priority 分配）。
    # 开启后启动首 bar 做一次现金再分配；rebalance_enabled=1 时每个交易日首 bar
    # 再平衡一次（只挪虚拟现金，不强制卖出）。
    capital_pool_mode = Column(
        String(16), nullable=False, server_default="off", default="off")
    capital_pool_rebalance_enabled = Column(
        Boolean, nullable=False, server_default="0", default=False)
    # ---- G1 资金强制（0021，2026-11-20）----
    # 会话级 kill switch：默认关（零行为变化）。开启后绑定拼盘的借据净额计入
    # 组合可用现金与真实侧占比；关闭当根 bar 即释放全部借据、只回收不新增。
    capital_enforce_enabled = Column(
        Boolean, nullable=False, server_default="0", default=False)
    # ---- F3 资金影子会话级开关（0021，2026-11-20）----
    # 默认关：会话启动时决定是否构建影子账本（只逐 bar 追加
    # live_capital_shadow，不触订单/成交/决策事件，无下单入口）。
    # 与拼盘绑定入口互不覆盖：绑定了 pairing_plan_id / backtest_run_group_id
    # 的会话始终走计划侧（原路径，不受本开关影响）；未绑定时本开关打开，
    # 以会话组合的策略成员构建「无计划侧」影子（只采实际侧）。
    capital_shadow_enabled = Column(
        Boolean, nullable=False, server_default="0", default=False)
    # ---- G4 现金出口分级执行（0021，2026-11-20）----
    # OFF/P1/P2/P3；默认 OFF（只记账不投放）。P1 逐笔人工确认，P2 服务计划
    # + 人工确认，P3 仅对已验证、T+0/≤7 天的出口自动放行（仍只写台账）。
    cash_outlet_mode = Column(
        String(5), nullable=False, server_default="OFF", default="OFF")
    # ---- G2 盘前集合竞价参数会话级覆盖（2026-11-18）----
    # 三列均可空：NULL = 跟随 config.tq_trade.pre_open_auction_* 全局值；
    # 非空 = 本会话显式覆盖（仅作用于本会话，不改全局配置）。
    # 历史会话保持 NULL，行为与升级前逐字一致。
    pre_open_auction_enabled = Column(Boolean, nullable=True)
    pre_open_auction_end_time = Column(String(5), nullable=True)
    pre_open_auction_price_bias_pct = Column(Numeric(9, 6), nullable=True)
    # ---- 存量/外部持仓是否纳入风控（代卖）会话级开关（2026-11-19）----
    # NULL = 历史会话未显式设置 → 解析时回退全局 config.tq_trade.live_sell_scope
    # platform_only = 关闭（默认档位）：只卖平台自建仓，存量/外部仓不代卖
    # full = 开启：按整仓卖（含存量/外部仓）
    # 运行中可切换，引擎每轮 tick 重读本列并同步到各 Portfolio.sell_scope；
    # 全局配置只作新建会话的初始档位来源，本列优先且不反向改写全局配置。
    sell_scope = Column(String(16), nullable=True)
    # B2 主备通道（默认关闭）：NULL=未配置备用引擎；failover_enabled=1 时主通道
    # 心跳失败逐日写 channel_failover 建议事件，实际切换仍需人工确认。
    backup_engine = Column(String(16), nullable=True)
    failover_enabled = Column(
        Boolean, nullable=False, server_default="0", default=False)
    status = Column(String(10), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    started_at = Column(DateTime, nullable=True)
    stopped_at = Column(DateTime, nullable=True)
