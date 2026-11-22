import axios from 'axios'

export const api = axios.create({ baseURL: '/api' })

// #14：响应拦截器 — 业务错误（HTTP 200 但 body.code !== 0）转 reject。
// 后端统一响应 {code,message,data}，code=0 成功；非 0 是业务错误，但 HTTP 仍 200，
// axios 当成功 resolve → 调用方误处理（静默 bug）。此处统一拦下，reject 一个带
// response.data 的错误（让 errMsg 能读 message）。HTTP 错误（404/409/500）由
// axios 自动 reject，不动。
api.interceptors.response.use((response) => {
  const body = response.data as ApiResponse<unknown>
  if (body && typeof body.code === 'number' && body.code !== 0) {
    const err: Error & { response?: unknown } = new Error(body.message || '请求失败')
    err.response = response
    return Promise.reject(err)
  }
  return response
})

export interface ApiResponse<T> {
  code: number
  message?: string
  data: T
}

// ---------------------------------------------------------------------------
// 响应类型（对齐后端 serializer，审计 #26 any 清理）
// ---------------------------------------------------------------------------

// 本地已同步股票池（GET /stock-pools、POST /stock-pools/sync）— 对齐 stock_pools._serialize_pool
export interface StockPoolItem {
  id: number
  code: string
  name: string
  synced_at: string | null
  stock_count: number
}

// 通达信用户板块 + 本地残留合并（GET /stock-pools/tdx）
export interface TdxPoolItem {
  code: string
  name: string
  synced: boolean
  exists_in_tdx: boolean
  stock_count: number
}

// 通达信板块实时成分股（GET /stock-pools/tdx/{code}/stocks）— 对齐 tq.data.get_pool_stocks
export interface TdxPoolStockItem {
  stock_code: string
  stock_name: string | null
}

// 公式信号（响应侧，含 id）— 对齐 formulas._serialize_formula signals 子项
export interface FormulaSignalItem {
  id: number
  signal_name: string
  signal_type: string  // OPEN|ADD|REDUCE|CLOSE
  trigger_value: number  // -2|-1|0|1|2
}

// 公式（GET/POST/PUT /formulas）— 对齐 formulas._serialize_formula
export interface FormulaItem {
  id: number
  name: string
  content: string
  formula_count: number
  engine: string  // tq=通达信计算（默认）/ builtin=内置公式子集解释器（仅回测）
  no_leak_cross?: boolean  // 批次 9a：跨周期引用无泄露开关（默认 false=TDX 语义）
  created_at: string | null
  updated_at: string | null
  signals: FormulaSignalItem[]
}

// 行业门控（组合 JSON 侧配置，2026-11-22）：全部默认关闭
export interface IndustrySettings {
  industry_gate_mode: 'off' | 'industry_relief' | 'industry_strict'
  industry_rank_enabled: boolean
  industry_max_per_industry: number
  industry_weight_enabled: boolean
  industry_overbought_block: boolean
  industry_trail_tighten_enabled: boolean
  industry_trail_tighten_ratio: number
  industry_bear_reduce_ratio: number
  industry_divergence_hint: boolean
}

// 组合策略（GET/POST/PUT /portfolios）— 对齐 strategies._serialize_portfolio
export interface PortfolioItem {
  id: number
  name: string
  stock_pool_id: number
  benchmark_index: string | null
  initial_capital: number | null
  max_drawdown: number | null
  daily_loss_limit: number | null
  max_holdings: number | null
  priority?: number | null  // A2 跨组合下单优先级（数值越大越先处理，默认 0）
  min_commission: number | null
  buy_commission_rate: number | null
  sell_commission_rate: number | null
  stamp_duty_rate: number | null
  transfer_fee_rate: number | null  // 过户费率（买卖双向，默认 0.00001=0.001%）
  slippage: number | null
  risk_cooldown_days: number | null  // 风控冷静期（交易日，默认 1；0=关闭）
  trading_session: string  // full|am|pm
  price_limit_check_enabled: boolean  // #1 涨跌停校验（回测成交 + 实盘下单前置，默认开）
  volume_limit_enabled: boolean       // #2 成交量/流动性约束（回测，默认关）
  volume_limit_ratio: number          // 日线流动性目标：单笔 ≤ 前5日均量 × 本比例（默认 0.10）
  minute_volume_limit_ratio: number | null  // 实盘单次可下单量：≤ 最近完成 1m bar 量 × 本比例（NULL=跟随 volume_limit_ratio，默认 4%；0=关闭分钟闸门；仅实盘读）
  adaptive_slippage_enabled: boolean  // #4 自适应滑点（回测，默认关）：冲击随订单量占成交量比例上浮
  slippage_impact_coef: number        // 滑点冲击系数（默认 0.10，封顶滑点 10%）
  position_sizing: string             // W2 仓位模型：dynamic（默认）|fixed
  daily_loss_basis: string            // 日内亏损熔断分母：equity（默认，昨收净值）|initial（初始资金）
  cash_enhance_enabled: boolean       // W2 现金管理增强（预留，默认关）
  cash_enhance_equity_threshold: number | null
  cash_enhance_position_ratio_threshold: number | null
  equity_cap: number | null            // W3 容量上限（dynamic 仓位资金封顶，None=不设上限）
  equity_cap_mode: string              // 上限处理模式：cap（默认，只封顶定量）| distribute（回测日终出金）
  max_single_position_pct: number | null  // 单票上限（0~1 比例，null=不限制；仅实盘生效）
  min_cash_reserve_pct: number | null     // 现金下限（0~1 比例，null=不限制；仅实盘生效）
  target_utilization: number | null       // 资金池目标仓位上限（0~1 比例，null=默认 0.95；仅额度推导用）
  // 市场多空门控（默认关）：基准指数空头时禁止开仓/加仓，持仓不强平
  market_gate_enabled: boolean
  market_gate_engine: string  // python（默认）| tdx
  market_gate_formula_name: string  // 空串=默认公式名 TQGATE
  // E3 门控维度扩展（默认值=旧语义）
  market_gate_indices: string[]        // 多指数投票；空=仅 benchmark_index
  market_gate_vote_mode: string        // any（默认）| majority | all
  market_gate_strength_threshold: number  // 空头强度门槛 0-100；0=不设门槛
  market_gate_bear_reduce_ratio: number   // 空头减仓比例 0-1；0=关闭
  industry_settings: IndustrySettings     // 行业门控（默认全关）
  status: string  // active|archived
  created_at: string | null
  updated_at: string | null
  strategies: StrategyDetail[]
}

// 回测记录（列表项）— 对齐 backtest._serialize_record
export interface BacktestRecordItem {
  id: number
  portfolio_strategy_id: number
  name: string
  start_date: string | null
  end_date: string | null
  status: string  // running|completed|failed
  error_message: string | null
  warnings: string[]  // 回测非致命告警（行情跳过/零信号/输出线缺失/公式报错）
  evaluation_profile: string  // absolute|relative（历史记录回落 absolute）
  is_oos_gate_enabled: boolean
  oos_gate_min_ratio: number | null
  gate_status: 'passed' | 'rejected' | null  // null=未启用或数据不足
  gate_reasons: string[]
  created_at: string | null
  completed_at: string | null
  // 验证结论（容量健康 + 危机防御 + 样本外引用），回测完成时冻结；老记录为 null
  validation: ValidationVerdict | null
  // F1 数据质量（覆盖率/缺根/被剔除假 bar），老记录为 null
  data_quality: DataQuality | null
  // 0021 E5：所属多组合回测批（拼盘成员），单组合为 null
  run_group_id: number | null
}

// K2 多记录横向对比（对齐 backtest_service.compare_records）
export interface BacktestCompareCurvePoint {
  date: string | null
  total_value: number | null
  cumulative_return: number | null
  index_value: number | null  // 首日归一净值（各记录可叠加）
}

export interface BacktestCompareItem {
  record: BacktestRecordItem
  metrics: Record<string, number | null>
  curve: BacktestCompareCurvePoint[]
  params: Record<string, string | number | boolean | null>
  trade_summary: {
    trades_count: number
    buy_count: number
    sell_count: number
    buy_amount: number
    sell_amount: number
    stocks: Record<string, number>
  }
}

export interface BacktestCompareRow {
  key?: string
  label?: string
  stock_code?: string
  values: (number | string | null)[]
  differs: boolean
}

export interface BacktestCompareResult {
  records: BacktestCompareItem[]
  metric_rows: BacktestCompareRow[]
  param_rows: BacktestCompareRow[]
  stock_rows: BacktestCompareRow[]
  stock_rows_truncated: boolean
  trade_totals: Array<{
    trades_count: number; buy_count: number; sell_count: number
    buy_amount: number; sell_amount: number
  }>
  warnings: string[]
}

// F1 回测数据质量（对齐 backtest_service.summarize_data_quality）
export interface DataQualityPeriod {
  union_bars: number
  expected_bars: number
  kept_bars: number
  raw_bars: number
  missing_bars: number
  coverage_ratio: number
  dropped_artifact_bars: number
  dropped_no_trade_bars: number
}

export interface DataQuality {
  version: string
  window: { start: string; end: string }
  requested_stocks: number
  stocks_with_data: number
  missing_stocks: string[]
  periods: Record<string, DataQualityPeriod>
  summary: {
    coverage_ratio: number
    kept_bars: number
    missing_bars: number
    dropped_artifact_bars: number
    dropped_no_trade_bars: number
  }
}

// 回测"验证结论"（对齐 backtest_service.build_validation_verdict）
export interface CapacityMetrics {
  adv: {
    stock_code: string | null; amount: number; adv: number; ratio: number
    threshold: number; hit: boolean
  } | null
  concentration: { max_single_weight: number | null; threshold: number; hit: boolean }
  exposure: {
    ceiling: number; final_equity: number; ratio: number; threshold: number
    recent_days: number; recent_utilization: number; whole_utilization: number
    hit: boolean
  } | null
  advice: { key: string; text: string }[]
}

export interface ValidationVerdict {
  capacity: string[]
  capacity_metrics?: CapacityMetrics | null  // K7 结构化容量画像（老记录可能缺）
  crisis: { key: string; name: string; start: string; end: string;
    strategy_return: number | null; benchmark_return: number | null;
    max_drawdown: number | null; min_cash_ratio: number | null;
    verdict: string; reason: string }[]
  oos: { run_id: number; name: string; completed_at: string | null;
    windows_passed: string | null; overall_pass: boolean | null } | null
}

// 组合日快照 — 对齐 backtest._serialize_snapshot
export interface BacktestSnapshotItem {
  snap_date: string | null
  total_value: number | null
  cash: number | null
  market_value: number | null
  daily_return: number | null
  cumulative_return: number | null
  benchmark_value: number | null
}

// 回测交易 — 对齐 backtest._serialize_trade_with_name（详情内嵌，含 strategy_name）
export interface BacktestTradeItem {
  id: number
  strategy_id: number
  strategy_name: string
  signal_name: string | null
  signal_type: string | null
  stock_code: string
  trade_type: string  // BUY|SELL
  price: number | null
  quantity: number
  amount: number | null
  commission: number | null
  stamp_duty: number | null
  bar_time: string | null
}

// 回测评估 — 对齐 backtest._serialize_evaluation
export interface BacktestEvaluationItem {
  total_return: number | null
  annual_return: number | null
  max_drawdown: number | null
  volatility: number | null
  sharpe_ratio: number | null
  sortino_ratio: number | null
  downside_volatility: number | null
  calmar_ratio: number | null
  win_rate: number | null
  profit_factor: number | null
  total_trades: number | null
  closed_trades: number | null
  benchmark_return: number | null
  avg_holding_days: number | null
  var_95: number | null
  cvar_95: number | null
  avg_recovery_days: number | null
  max_recovery_days: number | null
  ulcer_index: number | null
  return_stability: number | null
  // 基准相对 8 指标（regime-alpha suite）
  beta: number | null
  jensen_alpha: number | null
  excess_annual_return: number | null
  tracking_error: number | null
  information_ratio: number | null
  up_capture_ratio: number | null
  down_capture_ratio: number | null
  excess_max_drawdown: number | null
  excess_calmar_ratio: number | null
  // 磨损/仓位/结构 6 指标
  annual_turnover: number | null
  total_cost_ratio: number | null
  avg_position_utilization: number | null
  empty_days_ratio: number | null
  max_single_weight: number | null
  hhi: number | null
  // 路径 6 指标
  max_consecutive_loss_days: number | null
  max_consecutive_profit_days: number | null
  worst_daily_return: number | null
  worst_weekly_return: number | null
  return_skewness: number | null
  return_kurtosis: number | null
}

// 策略评估（详情内嵌，含 strategy_id/name）— 对齐 backtest get_record strategy_evaluations
export interface BacktestStrategyEvaluationItem extends BacktestEvaluationItem {
  strategy_id: number
  strategy_name: string
}

// 策略净值曲线（详情内嵌）— 对齐 backtest get_record strategy_snapshots
export interface BacktestStrategySnapshotItem {
  strategy_id: number
  strategy_name: string
  curve: { snap_date: string | null; total_value: number | null }[]
}

// 决策闸门事件（回测/实盘同口径）— 对齐 decision_event 序列化
// 覆盖信号→成交全链路拦截点：止损/止盈/熔断/资金不足/超仓/收盘/去重/桥拒单…
export interface DecisionEventItem {
  id?: number
  gate: string          // 闸门代码（stop_loss/max_positions_full/insufficient_funds/after_close_block…）
  layer: string         // strategy_risk|portfolio_risk|signal_gate|capital_gate|t1|live_gate
  action: string        // trigger|halt|recover|strip|block|reject|shrink|clamp
  portfolio_id: number | null
  strategy_id: number | null
  stock_code: string | null
  bar_time: string | null
  param_name: string | null   // 关联调参名（stop_loss_ratio/max_positions/max_drawdown…）
  param_value: number | null  // 阈值
  actual_value: number | null // 实际值（亏损%/回撤%/持仓数…）
  requested_qty: number | null
  final_qty: number | null
  message: string | null
  created_at?: string | null
  // 处理状态：open 未处理 / acked 人工确认 / archived 过期归档（仍可回看）
  ack_status?: 'open' | 'acked' | 'archived' | null
  ack_note?: string | null
  ack_at?: string | null
  ack_by?: string | null
  archived_at?: string | null
  archive_reason?: string | null
}

// 决策闸门聚合统计（summarize_decisions，按 gate+param_name 分组）
export interface DecisionSummaryItem {
  gate: string
  layer: string
  action: string
  param_name: string | null
  param_value: number | null
  count: number
  first_bar_time: string | null
  last_bar_time: string | null
  stock_count: number
  requested_qty_sum: number | null
  final_qty_sum: number | null
}

// 决策事件查询响应（实盘 /decisions；回测详情内嵌同构字段）
export interface DecisionData {
  summary: DecisionSummaryItem[]
  // 未处理(opened且未归档)/已确认/已归档计数（后端全量口径，不随 ack_status 过滤变化）
  ack_summary?: { open: number; acked: number; archived?: number }
  events: DecisionEventItem[]
}

// L4 关键事件检索（GET /live/decision-events，跨会话只读）
export interface DecisionEventSearchItem extends DecisionEventItem {
  session_id: number
  session_name: string | null
}

export interface DecisionEventSearchFilters {
  session_id?: number
  gate?: string
  layer?: string
  action?: string
  ack_status?: 'open' | 'acked' | 'archived'
  keyword?: string
  start?: string  // YYYY-MM-DD
  end?: string    // YYYY-MM-DD
  limit?: number
  offset?: number
}

export interface DecisionEventSearchResult {
  items: DecisionEventSearchItem[]
  total: number
  limit: number
  offset: number
}

// 月度收益热力图矩阵（年度×12月）
export interface BacktestMonthlyMatrix {
  years: number[]
  months: string[]           // ["1月", "2月", ..., "12月"]
  matrix: (number | null)[][] // matrix[yearIdx][monthIdx] = 月收益% 或 null
  text: string[][]           // 同 shape，格式 "+1.23%"
}

// 年度统计（日收益分箱 + 月收益 + 年累计）
export interface BacktestYearlyStats {
  year: number
  daily_returns: number[]    // 该年日收益（小数）
  monthly_returns: number[]  // 该年月收益（小数）
  annual_return: number | null  // 年度累计收益（小数，复利；无收益年份为 null）
}

// 收益分布桶（月度热力图 + 年度箱线图数据源）
export interface BacktestReturnsBucket {
  monthly_matrix: BacktestMonthlyMatrix
  yearly: BacktestYearlyStats[]
}

export interface CrisisAlphaQuadrant {
  n_crisis: number
  n_down: number
  n_total: number
  hit_rate: number
  up_rate: number
  alpha_ratio: number
  follow_ratio: number
  odds: number | null
  counts: Record<string, number>
  avg_ret: Record<string, number>
  down_avg_s: number
  down_avg_b: number
  win_avg: number | null
  loss_avg: number
}

export interface CrisisAlphaCondition {
  threshold: number
  n: number
  b_avg: number
  s_avg: number
  up_rate: number
}

export interface CrisisAlphaWeekRow {
  日期: string
  基准?: number
  策略?: number
  基准收益?: number
  策略收益?: number
  超额?: number
  分类: string
}

export interface CrisisAlphaEvent {
  start: string
  end: string
  benchmark_ret: number
  strategy_ret: number
  excess: number
  hedged: boolean
  verdict: string
  max_dd: number
  days: number
}

export interface CrisisAlphaPoint {
  date: string
  value: number | null
}

export interface CrisisAlphaMonthState {
  n: number
  s_avg: number
  b_avg: number
  win_rate: number
}

export interface CrisisAlphaAnnual {
  年份: number
  下跌周: number
  危机Alpha: number
  策略收益: number
  基准收益: number
  完整: boolean
}

export interface CrisisAlphaCost {
  drag: number
  sync_up: number
  elasticity: number
  tail_corr: number | null
  q1_corr: number | null
  n_up: number
}

export interface CrisisAlphaPerfSide {
  cum: number
  ann: number
  vol: number | null
  mdd: number
}

export interface CrisisAlphaPerformance {
  s: CrisisAlphaPerfSide
  b: CrisisAlphaPerfSide
  corr: number | null
  start: string
  end: string
}

export interface CrisisAlphaDimension {
  维度: string
  类型: string
  权重: number
  得分: number
  评级: string
  贡献: number
}

export interface CrisisAlphaScore {
  total_score: number
  rating: string
  dimensions: CrisisAlphaDimension[]
}

export interface CrisisAlphaMultiBench {
  key: string
  name: string
  code: string
  alpha_ratio: number
  n_down: number
  up_rate: number
  is_main: boolean
}

export interface CrisisAlphaNav {
  dates: string[]
  strategy: (number | null)[]
  benchmark: (number | null)[]
  shade_spans: string[][]
}

export interface CrisisAlphaData {
  strategy_name: string
  benchmark_code: string
  benchmark_name: string
  alpha_rating: string
  n_weeks: number
  quadrant: CrisisAlphaQuadrant
  conditions: CrisisAlphaCondition[]
  extreme_top: CrisisAlphaWeekRow[]
  deep_dd_events: CrisisAlphaEvent[]
  rolling_corr: CrisisAlphaPoint[]
  expanding_corr: CrisisAlphaPoint[]
  monthly: {
    states: Record<string, CrisisAlphaMonthState>
    hedge_ratio: number
    n_down_month: number
    big_down: Array<{ 日期: string; 基准: number; 策略: number }>
  }
  annual: CrisisAlphaAnnual[]
  cost: CrisisAlphaCost
  performance: CrisisAlphaPerformance
  score: CrisisAlphaScore
  weekly_detail: CrisisAlphaWeekRow[]
  down_weeks: CrisisAlphaWeekRow[]
  nav: CrisisAlphaNav
  multi_bench: CrisisAlphaMultiBench[]
}

export interface BacktestCrisisAlphaItem {
  status: 'completed' | 'insufficient' | 'failed'
  error: string | null
  skipped_benchmarks: string[]
  computed_at: string | null
  benchmark_code: string
  benchmark_name: string
  data: CrisisAlphaData | null
}

// 回测详情（GET /backtest/records/{id}）— 对齐 backtest.get_record
export interface BacktestDetailItem {
  record: BacktestRecordItem
  capacity_model_version: string  // 容量模型版本：v1=旧口径，v2-sqrt-…=参与率+冲击+残量顺延
  liquidity_caliber: string       // 流动性口径：same_day_bar=旧口径（当日 bar 量），prev5d_avg=前 5 日均量
  snapshots: BacktestSnapshotItem[]
  trades: BacktestTradeItem[]
  evaluations: BacktestEvaluationItem | null
  strategy_evaluations: BacktestStrategyEvaluationItem[]
  strategy_snapshots: BacktestStrategySnapshotItem[]
  decision_summary: DecisionSummaryItem[]  // 风控与拦截统计（调参可观测性）
  decisions: DecisionEventItem[]           // 逐闸门事件（下钻）
  returns_buckets: BacktestReturnsBucket | null  // 收益分布（无快照时 null）
  crisis_alpha: BacktestCrisisAlphaItem | null
  market_regime: BacktestMarketRegimeItem | null
  robustness: BacktestRobustnessItem | null  // 稳健性检验（年度/3年滚动/集中度/样本内外）
  persistence: PersistenceAnalysis | null  // 持续性检验（阿尔法衰减 vs 风格钟摆）
  acceptance: AcceptanceResult              // B6 考核口径验收（absolute/relative）
  cost_capacity: BacktestCostCapacity | null  // K5 成本与容量（详情现算，不落库）
  gate_release?: GateReleaseResult | null     // 0021 D4：门控释放/召回时间轴（只读）
}

// K5 成本与容量（对齐 backtest_service.build_cost_capacity）
export interface BacktestCostCapacity {
  explicit: {
    total_cost: number | null
    commission: number | null
    stamp_duty: number | null
    transfer_fee: number | null
    turnover: number | null
    cost_bps: number | null
    avg_equity: number | null
    cost_to_avg_equity: number | null
    net_profit: number | null
    gross_profit: number | null
    cost_drag_ratio: number | null
  }
  slippage: {
    source: 'ref_price' | 'implied_close'
    comparable_to_configured: boolean
    configured_bps: number | null
    realized_slippage_bps: number | null
    realized_deviation_bps: number | null
    excess_bps: number | null
    excess_cost_amount: number | null
    matched_trades: number
    trades_count: number
    coverage_ratio: number | null
    ref_price_missing: number
    matched_amount: number | null
  }
  capacity: {
    trades_count: number
    max_trade_amount: number | null
    max_trade_pct_equity: number | null
    avg_trade_pct_equity: number | null
    large_trade_threshold_pct: number
    large_trade_count: number
    large_trade_amount_ratio: number | null
    pressure_level: string
    structure?: CapacityMetrics | null  // K7 规模约束结构指标（老记录为读取时重算）
  }
}

// B6 考核口径验收（对齐 acceptance.compute_acceptance）
export interface AcceptanceCheck {
  metric: string
  op: string
  threshold: number
  value: number | null
  pass: boolean | null
  label: string
}
export interface AcceptanceResult {
  profile: string  // absolute|relative
  pass: boolean | null  // true 全过 / false 有不过 / null 数据不足
  checks: AcceptanceCheck[]
}

export interface BacktestRobustnessItem {
  status: string
  error?: string | null
  computed_at?: string | null
  data?: {
    total_return: number | null
    benchmark_return: number | null
    years_positive: string
    annual: { year: string; return: number | null; max_drawdown: number | null }[]
    rolling_3y: { start: string; end: string; cagr: number | null; max_drawdown: number | null }[]
    concentration: {
      total_realized_pnl: number
      // 占比字段在毛利/净利 <= 0 时为 null（避免负占比误导）；旧 payload 缺这些键
      top: { code: string; pnl: number; share: number | null;
             share_gross?: number | null; share_net?: number | null }[]
      top20_share: number | null
      gross_profit?: number
      gross_loss?: number
      top20_share_gross?: number | null
      // 期末未平仓：未实现盈亏单列，不计入已实现净利
      unrealized?: {
        total_pnl: number
        cost: number
        market_value: number
        rows: { code: string; quantity: number | null; cost: number;
                market_value: number; pnl: number; share_net: number | null }[]
      }
      profitable_stocks: number
      total_stocks: number
      // 单笔（FIFO 平仓批次）贡献分布
      lots?: {
        count: number
        winners: number
        top: { rank: number; code: string; buy_date: string | null; sell_date: string | null;
               quantity: number; pnl: number;
               share_gross: number | null; share_net: number | null }[]
        cumulative: { n: number; pct_of_lots: number; pnl: number;
                      share_gross: number | null; share_net: number | null }[]
        hhi: number | null
        effective_sources: number | null
      }
    }
    oos: {
      split_year: string
      in_sample: OosSegment
      out_of_sample: OosSegment
      caveat?: string  // W5-O2 伪 OOS 明示（固定切分防时段崩塌，不防全样本调参）
    } | null
  } | null
}

// 持续性检验（阿尔法衰减 vs 风格钟摆）— 对齐 persistence_alpha.compute_persistence
export interface PersistenceYearRow {
  year: number
  is_partial: boolean
  trading_days?: number
  nav_return: number | null
  max_drawdown: number | null
  sharpe: number | null
  calmar: number | null
  win_rate: number | null
  profit_factor: number | null
  turnover: number | null
  cost_ratio: number | null
  avg_position_utilization?: number | null
  excess: Record<string, number | null>
  bench_returns: Record<string, number | null>
}
export interface PersistenceRolling {
  dates: string[]
  strat_6m: (number | null)[]
  strat_12m: (number | null)[]
  bench_12m: (number | null)[]
}
export interface PersistenceBenchmarkMeta { code: string; name: string; is_main: boolean; available: boolean }
export interface PersistenceSignal {
  key: 'excess' | 'risk' | 'struct'
  label: string
  detail: string
  status: 'bad' | 'mixed' | 'good' | 'na'
  word: string
}
export interface PersistenceVerdict {
  level: 'alpha_decay' | 'style_pendulum' | 'stable' | 'mixed' | 'insufficient'
  reasons: string[]
  summary: string
  signals?: PersistenceSignal[]
  window_years?: number[]
  caveat: string
}
export interface PersistenceAnalysis {
  status: string
  error?: string | null
  computed_at?: string | null
  skipped_benchmarks?: string[]
  data?: {
    status: string
    error: string | null
    years: PersistenceYearRow[]
    rolling: PersistenceRolling
    benchmarks: PersistenceBenchmarkMeta[]
    verdict: PersistenceVerdict
  } | null
}
export async function recomputePersistenceAnalysis(id: number, benchmarkCode?: string) {
  const res = await api.post<ApiResponse<PersistenceAnalysis>>(
    `/backtest/records/${id}/persistence/recompute`,
    null,
    { params: benchmarkCode ? { benchmark_code: benchmarkCode } : undefined },
  )
  return res.data.data
}

export interface OosSegment {
  start: string
  end: string
  return: number | null
  cagr: number | null
  max_drawdown: number | null
  benchmark_return: number | null
}

// 市场多空状态卡（GET /market-gate/status）— 对齐 market_gate_service.fetch_gate_status
export interface MarketGateTransition {
  date: string
  state: number  // -1 空头 / 0 未定 / 1 多头 / 2 多头强势
}

export interface MarketGateStatusItem {
  code: string
  benchmark_name: string
  period: string
  as_of: string              // 最新一根 K 线日期（YYYY-MM-DD）
  direction: number          // -1 空头 / 0 未定 / 1 多头 / 2 多头强势
  direction_label: string
  strength: number           // 0–100
  since: string              // 当前状态起始日
  duration_bars: number      // 持续交易日数（含起始日与 as_of）
  transitions: MarketGateTransition[]  // 最近 N 次转折（新→旧）
  bars: number               // 参与计算的 K 线根数
  // E4 数据新鲜度：预期最新交易日 / 落后交易日数 / 是否超阈值（标黄提示）
  latest_trading_day?: string
  stale_days?: number
  stale?: boolean
  cached: boolean
}

export async function getMarketGateStatus(params: {
  code?: string
  period?: string
  count?: number
  history?: number
  refresh?: boolean
} = {}) {
  const res = await api.get<ApiResponse<MarketGateStatusItem>>('/market-gate/status', { params })
  return res.data.data
}

// E5 统一市场状态面板（GET /market-gate/overview）— 只读，聚合两套机制配置
export interface MarketGateScopeNote {
  key: string
  name: string
  scope: string
  effect: string
  timing: string
}

export interface MarketGateOverviewStrategy {
  id: number
  name: string
  regime_filter_enabled: boolean
  trend_trailing_stop_ratio: number | null
  trend_max_positions: number | null
  market_gate_mode: string
}

export interface MarketGateOverviewPortfolio {
  id: number
  name: string
  market_gate_enabled: boolean
  effective_enabled: boolean
  benchmark_index: string
  indices: string[]
  vote_mode: string
  strength_threshold: number
  bear_reduce_ratio: number
  engine: string
  formula_name: string
  strategies: MarketGateOverviewStrategy[]
}

export interface MarketGateOverview {
  scopes: MarketGateScopeNote[]
  portfolios: MarketGateOverviewPortfolio[]
}

export async function getMarketGateOverview() {
  const res = await api.get<ApiResponse<MarketGateOverview>>('/market-gate/overview')
  return res.data.data
}

// ---------------- 投资看板（只读） ----------------

export interface BoardIndustryItem {
  rank: number | null
  code: string
  name: string
  as_of: string | null
  period: string
  direction: number
  direction_label: string
  strength: number | null
  level: string
  level_label: string
  bull_dur: number | null
  bear_dur: number | null
  lead_days: number | null
  turned_at: string | null
  bias: number | null
  bias_z: number | null
  overbought: boolean
  oversold: boolean
  overbought_watch: boolean
  oversold_watch: boolean
  jd: number | null
  jd_filter: number
  channel_filter: number
  up_str: number | null
  dn_str: number | null
  bars: number | null
  stale: boolean
  bar_closed_at: string | null
  effective_from: string | null
  computed_at: string | null
  divergence: string | null
  divergence_label: string | null
}

export interface BoardStockItem {
  rank: number | null
  code: string
  name: string
  industry_code: string
  as_of: string | null
  period: string
  direction: number
  direction_label: string
  strength: number | null
  level: string
  level_label: string
  bull_dur: number | null
  bear_dur: number | null
  lead_days: number | null
  bias: number | null
  bias_z: number | null
  overbought: boolean
  oversold: boolean
  overbought_watch: boolean
  oversold_watch: boolean
  bars: number | null
  stale: boolean
  bar_closed_at: string | null
  effective_from: string | null
  computed_at: string | null
}

export interface BoardRunInfo {
  id: number
  period: string
  scope: string
  as_of: string | null
  trigger: string | null
  status: string
  started_at: string | null
  finished_at: string | null
  total: number
  ok: number
  skipped: number
  error: string | null
  stats: any
}

export interface BoardMeta {
  formula_version: string
  direction_basis: string
  min_bars: number
  bias_z: { window: number; k: number; include_today: boolean; symmetric: boolean }
  periods: string[]
  universe: { count: number; expected: number; ok: boolean; codes: string[] }
  benchmark: string
  level_labels: Record<string, string>
  caliber_note: string
}

export async function getBoardMeta() {
  const res = await api.get<ApiResponse<BoardMeta>>('/investment-board/meta')
  return res.data.data
}

export async function getBoardSummary(params: {
  period?: string
  benchmark_index?: string
  history?: number
  force?: boolean
} = {}) {
  const res = await api.get<ApiResponse<{
    gate: MarketGateStatusItem
    period: string
    as_of: string | null
    level_counts: Record<string, number>
    watch_counts: { overbought: number; oversold: number }
    benchmark_bull_dur: number | null
    formula_version: string
    latest_run: BoardRunInfo | null
  }>>('/investment-board/summary', { params })
  return res.data.data
}

export async function getBoardIndustries(params: {
  period?: string
  date?: string
  sort?: string
  order?: string
  level?: string
  limit?: number
  benchmark?: string
} = {}) {
  const res = await api.get<ApiResponse<{
    as_of: string | null
    benchmark_bull_dur: number | null
    rows: BoardIndustryItem[]
  }>>('/investment-board/industries', { params })
  return res.data.data
}

export async function getBoardIndustry(code: string, params: {
  period?: string
  date?: string
  benchmark?: string
} = {}) {
  const res = await api.get<ApiResponse<BoardIndustryItem>>(
    `/investment-board/industries/${code}`, { params })
  return res.data.data
}

export async function getBoardIndustryStocks(code: string, params: {
  period?: string
  date?: string
  sort?: string
  order?: string
  limit?: number
  compute?: boolean
} = {}) {
  const res = await api.get<ApiResponse<{
    as_of: string | null
    rows: BoardStockItem[]
    reason?: string | null
  }>>(`/investment-board/industries/${code}/stocks`, { params })
  return res.data.data
}

export interface BoardBreakdownItem {
  key: string
  label: string
  score: number | null
  max: number
}

export interface BoardBreakdown {
  available: boolean
  reason?: string
  code?: string
  period?: string
  as_of?: string
  bars?: number
  direction?: number
  direction_label?: string
  side?: 'up' | 'dn'
  strength?: number | null
  items?: BoardBreakdownItem[]
  ma_count?: number
  ma_total?: number
  volume_state?: string
}

export async function getBoardBreakdown(code: string, params: {
  period?: string
  date?: string
} = {}) {
  const res = await api.get<ApiResponse<BoardBreakdown>>(
    `/investment-board/industries/${code}/breakdown`, { params })
  return res.data.data
}

export async function getBoardLeaders(params: {
  period?: string
  date?: string
  limit?: number
  benchmark?: string
} = {}) {
  const res = await api.get<ApiResponse<{
    as_of: string | null
    benchmark_bull_dur: number | null
    sustained: BoardIndustryItem[]
    reversal: BoardIndustryItem[]
    excess: BoardIndustryItem[]
  }>>('/investment-board/leaders', { params })
  return res.data.data
}

export async function getBoardRun(id: number) {
  const res = await api.get<ApiResponse<BoardRunInfo>>(`/investment-board/runs/${id}`)
  return res.data.data
}

export async function refreshBoard(payload: { period: string; scope: string }) {
  const res = await api.post<ApiResponse<{
    run_id: number
    period: string
    scope: string
    status: string
  }>>('/investment-board/refresh', payload)
  return res.data.data
}

export async function getStockPools() {
  const res = await api.get<ApiResponse<StockPoolItem[]>>('/stock-pools')
  return res.data.data
}

// 通达信用户板块 + 本地残留合并：[{code, name, synced, exists_in_tdx, stock_count}]
export async function getTdxPools() {
  const res = await api.get<ApiResponse<TdxPoolItem[]>>('/stock-pools/tdx')
  return res.data.data
}

// 通达信板块实时成分股：[{stock_code, stock_name}]
export async function getTdxPoolStocks(code: string) {
  const res = await api.get<ApiResponse<TdxPoolStockItem[]>>(`/stock-pools/tdx/${code}/stocks`)
  return res.data.data
}

// 按 code 同步（upsert 本地池 + 全量替换成分股）；已同步的可重同步
export async function syncStockPool(req: { code: string }) {
  const res = await api.post<ApiResponse<StockPoolItem>>('/stock-pools/sync', req)
  return res.data.data
}

export async function deleteStockPool(id: number) {
  const res = await api.delete<ApiResponse<null>>(`/stock-pools/${id}`)
  return res.data.data
}

export async function getFormulas() {
  const res = await api.get<ApiResponse<FormulaItem[]>>('/formulas')
  return res.data.data
}

export interface SignalItem {
  signal_name: string
  signal_type: string  // OPEN|ADD|REDUCE|CLOSE
  trigger_value: number  // 1 或 -1
}

export interface FormulaRequest {
  name: string
  content: string
  signals: SignalItem[]
  formula_count: number  // 注入历史根数（公式级，默认 200）
  engine: string  // tq（默认，原行为）/ builtin（内置引擎，仅回测）
  no_leak_cross: boolean  // 批次 9a：跨周期引用无泄露开关（默认 false=TDX 语义）
}

// 语法预检（POST /formulas/precheck）— 对齐 tdx_formula.syntax_check 返回
export interface PrecheckIssue {
  rule: string  // future | realtime | data | index_only | unknown
  level: string  // error | warning
  name: string
  line: number
  col: number
  message: string
}

export interface PrecheckResult {
  ok: boolean
  issues: PrecheckIssue[]
  errors: string[]
  outputs: string[]  // `:` 输出线名（供信号配置对照）
}

export async function precheckFormula(content: string) {
  const res = await api.post<ApiResponse<PrecheckResult>>('/formulas/precheck', { content })
  return res.data.data
}

// ---- 0016 信号标准化向导（POST /formulas/normalize-preview）----
// 选源公式输出线 + 规则 → 生成新公式源码（`:` 线降级 `:=` + 尾部追加 ±1/±2/0 离散标识线）
export interface NormalizePlan {
  line: string
  rule: 'cross_zero' | 'above_threshold' | 'map_values'
  params: Record<string, unknown>
}

export interface NormOutputLine {
  name: string
  source_line: string
  rule: string
  expr: string
  meaning: string  // 触发含义（说明 trigger_value 取值）
}

export interface NormalizePreviewResult {
  new_content: string
  output_lines: NormOutputLine[]
  warnings: string[]
}

export async function normalizePreview(content: string, plans: NormalizePlan[]) {
  const res = await api.post<ApiResponse<NormalizePreviewResult>>(
    '/formulas/normalize-preview', { content, plans })
  return res.data.data
}

export async function getFormulaDetail(id: number) {
  const res = await api.get<ApiResponse<FormulaItem>>(`/formulas/${id}`)
  return res.data.data
}

export async function createFormula(req: FormulaRequest) {
  const res = await api.post<ApiResponse<FormulaItem>>('/formulas', req)
  return res.data.data
}

export async function updateFormula(id: number, req: FormulaRequest) {
  const res = await api.put<ApiResponse<FormulaItem>>(`/formulas/${id}`, req)
  return res.data.data
}

export async function deleteFormula(id: number) {
  const res = await api.delete<ApiResponse<null>>(`/formulas/${id}`)
  return res.data.data
}

// 通达信公式导入三步（对齐 formulas.tdx_list / tdx_info / import）
export interface TdxFormulaListItem {
  acCode: string
  acName: string
  isSys: number | null
  imported: boolean
  formula_id: number | null
}

export interface TdxFormulaInfo {
  acCode: string
  acName?: string
  isSys?: number
  ParaNum?: number
  Para?: { ParaName: string; Min: string; Max: string; Default: string }[]
  LineNum?: number
  Line?: { LineName: string }[]
}

export async function getTdxFormulas(userOnly = true) {
  const res = await api.get<ApiResponse<TdxFormulaListItem[]>>('/formulas/tdx-list', {
    params: { user_only: userOnly },
  })
  return res.data.data
}

export async function getTdxInfo(acCode: string, formulaType = 0) {
  const res = await api.get<ApiResponse<TdxFormulaInfo>>('/formulas/tdx-info', {
    params: { ac_code: acCode, formula_type: formulaType },
  })
  return res.data.data
}

export async function importTdxFormula(acCode: string) {
  const res = await api.post<ApiResponse<FormulaItem>>('/formulas/import', { ac_code: acCode })
  return res.data.data
}

// ---- 1.3.1 公式试算（POST /formulas/test-run）：任选股票/周期/bar 数跑一遍公式 ----
// 单条输出线的统计：n=参与计算 bar 数；ones=值=1 的次数（信号触发次数）；
// last_values=末 3 根 bar 的输出值（字符串，保留原始精度）
export interface TestRunLine {
  n: number
  ones: number
  last_values: string[]
}

// 公式试算结果：按股票代码分组，每股下按输出线名给统计
export interface TestRunResult {
  results: Record<string, Record<string, TestRunLine>>
  warning?: string  // 非致命告警（如个别股票无行情数据），顶部红字提示
}

export interface TestRunRequest {
  content?: string         // 公式源码（可选——通达信导入公式平台库不存源码，留空走 formula_id 按名计算）
  formula_id?: number      // content 为空时必填：平台公式 id（按名调通达信计算，与回测/实盘同路径）
  stock_codes: string[]    // 股票代码（含后缀格式，如 000001.SZ）最多20 只
  period: string           // 1m|5m|15m|30m|1h|1d|1w|1mon
  count?: number           // bar 根数（默认 20）
  dividend_type?: number   // 复权类型（默认 1=前复权）
  formula_type?: number    // 公式类型（默认 0=技术指标）
}

export async function testRunFormula(req: TestRunRequest) {
  const res = await api.post<ApiResponse<TestRunResult>>('/formulas/test-run', req)
  return res.data.data
}

// ---- 1.3.1 通达信用户板块写入（POST /tdx/user-block）：全量替换板块成分股 ----
export interface UserBlockRequest {
  block_code: string       // 板块代码：由 block_name 确定性派生的 ASCII 码（utils/blockCode deriveBlockCode），同名同码保证幂等
  block_name: string       // 板块显示名（用户输入的中文名）
  stock_codes: string[]    // 成分股代码列表
  show?: boolean           // 是否在通达信里显示该板块
  mode?: 'replace' | 'append'  // replace=覆盖写（默认，后端先清空板块再写入）；append=追加（SDK 原生语义）
}

export interface UserBlockResult {
  block_code: string
  count: number  // 实际写入的股票数
}

export async function sendUserBlock(req: UserBlockRequest) {
  const res = await api.post<ApiResponse<UserBlockResult>>('/tdx/user-block', req)
  return res.data.data
}

export async function getPortfolios() {
  const res = await api.get<ApiResponse<PortfolioItem[]>>('/portfolios')
  return res.data.data
}

export interface StrategyItem {
  name: string
  formula_id: number
  period: string  // 1m|5m|15m|30m|1h|1d|1w|1mon
  role: string  // independent|master|slave
  master_strategy_id: number | null  // 0=本批第N个；null=无
  stock_pool_id?: number | null  // null=继承组合级
  slave_exit_mode?: string  // independent|follow_master（仅 slave 有意义）
  capital_ratio: number
  max_positions: number
  single_open_ratio: number
  stop_loss_ratio: number
  take_profit_ratio: number
  trailing_stop_ratio: number
  add_position_threshold: number
  max_add_count: number
  add_position_ratio: number
  reduce_position_ratio: number
  rotation_enabled: boolean
  rotation_momentum_bars: number
  rotation_hysteresis: number
  regime_filter_enabled: boolean
  trend_trailing_stop_ratio: number | null
  trend_max_positions: number | null
  // E3 策略级门控覆盖（inherit=跟随组合（默认）/off=不受门控/on=强制启用）
  market_gate_mode: string
  exposure_cap_enabled: boolean
}

export interface PortfolioRequest {
  name: string
  stock_pool_id: number
  benchmark_index?: string
  initial_capital: number
  max_drawdown: number
  daily_loss_limit: number
  max_holdings: number
  priority?: number  // A2 跨组合下单优先级（数值越大越先处理，默认 0）
  min_commission: number
  buy_commission_rate: number
  sell_commission_rate: number
  stamp_duty_rate: number
  transfer_fee_rate?: number  // 过户费率（买卖双向，默认 0.00001=0.001%）
  slippage: number
  trading_session: string  // full|am|pm
  price_limit_check_enabled?: boolean  // #1 涨跌停校验；None=新建默认开启（实盘仅 TDX/THS 生效，QMT/掘金桥恒放行）/编辑不动
  volume_limit_enabled?: boolean
  volume_limit_ratio?: number  // (0,1]
  adaptive_slippage_enabled?: boolean  // #4 自适应滑点（默认关）
  slippage_impact_coef?: number  // (0,1]，冲击系数，默认 0.10
  position_sizing?: string  // W2: dynamic（默认）|fixed
  daily_loss_basis?: string  // 日内亏损分母：equity（默认，昨收净值）|initial（初始资金）
  cash_enhance_enabled?: boolean
  cash_enhance_equity_threshold?: number | null
  cash_enhance_position_ratio_threshold?: number | null
  equity_cap?: number | null  // W3 容量上限（dynamic 仓位资金封顶，None=不设上限）
  max_single_position_pct?: number | null  // 单票上限（0~1 比例，null=不限制；仅实盘生效）
  min_cash_reserve_pct?: number | null     // 现金下限（0~1 比例，null=不限制；仅实盘生效）
  target_utilization?: number | null       // 资金池目标仓位上限（0~1 比例，null=默认 0.95）
  market_gate_enabled?: boolean | null         // 市场多空门控开关（null=请求未携带，编辑不动）
  market_gate_engine?: string | null           // python（默认）| tdx
  market_gate_formula_name?: string | null     // 通达信公式名，空=默认 TQGATE
  // E3 门控维度扩展（null=请求未携带，编辑不动）
  market_gate_indices?: string[] | null        // 多指数投票；空数组=仅 benchmark_index
  market_gate_vote_mode?: string | null        // any（默认）| majority | all
  market_gate_strength_threshold?: number | null // 空头强度门槛 0-100；0=不设门槛
  market_gate_bear_reduce_ratio?: number | null  // 空头减仓比例 0-1；0=关闭
  // 行业门控（组合 JSON 侧，null/undefined=请求未携带，编辑不动）
  industry_gate_mode?: IndustrySettings['industry_gate_mode'] | null
  industry_rank_enabled?: boolean | null
  industry_max_per_industry?: number | null
  industry_weight_enabled?: boolean | null
  industry_overbought_block?: boolean | null
  industry_trail_tighten_enabled?: boolean | null
  industry_trail_tighten_ratio?: number | null
  industry_bear_reduce_ratio?: number | null
  industry_divergence_hint?: boolean | null
  status: string  // active|archived
  strategies?: StrategyItem[]  // 创建时携带；更新组合时后端忽略（策略单独 CRUD）
}

export async function getPortfolioDetail(id: number) {
  const res = await api.get<ApiResponse<PortfolioItem>>(`/portfolios/${id}`)
  return res.data.data
}

export async function createPortfolio(req: PortfolioRequest) {
  const res = await api.post<ApiResponse<PortfolioItem>>('/portfolios', req)
  return res.data.data
}

export async function updatePortfolio(id: number, req: PortfolioRequest) {
  const res = await api.put<ApiResponse<PortfolioItem>>(`/portfolios/${id}`, req)
  return res.data.data
}

export async function deletePortfolio(id: number) {
  const res = await api.delete<ApiResponse<null>>(`/portfolios/${id}`)
  return res.data.data
}

export interface StrategyDetail {
  id: number
  portfolio_id: number
  name: string
  formula_id: number
  period: string  // 1m|5m|15m|30m|1h|1d|1w|1mon
  role: string  // independent|master|slave
  master_strategy_id: number | null  // 已存在的同组合 master id
  stock_pool_id?: number | null  // null=继承组合级
  slave_exit_mode?: string  // independent|follow_master（仅 slave 有意义）
  capital_ratio: number
  max_positions: number
  single_open_ratio: number
  stop_loss_ratio: number
  take_profit_ratio: number
  trailing_stop_ratio: number
  add_position_threshold: number
  max_add_count: number
  add_position_ratio: number
  reduce_position_ratio: number
  rotation_enabled: boolean
  rotation_momentum_bars: number
  rotation_hysteresis: number
  regime_filter_enabled: boolean
  trend_trailing_stop_ratio: number | null
  trend_max_positions: number | null
  // E3 策略级门控覆盖（inherit=跟随组合（默认）/off=不受门控/on=强制启用）
  market_gate_mode: string
  exposure_cap_enabled: boolean
  max_order_amount?: number | null  // 单笔最大买入金额（元），null=不限
  requote_enabled: boolean
  requote_max_attempts: number
  requote_price_mode: string  // tick|adaptive（H4）
  requote_max_slippage_ticks: number  // H4 最大追价档数
  split_order_enabled: boolean
  split_order_threshold: number
  // H3 切片增强（null/0 = 按阈值默认推导）
  split_order_count: number | null
  split_order_min_qty: number | null
  split_order_interval_seconds: number | null
  split_order_interval_jitter: boolean
  split_order_volume_pct: number | null
  risk_timing: string  // next_day|immediate
  signal_frequency?: string | null
  signal_at?: string | null
  signal_weekday?: number | null
  signal_data_end?: string | null
}

export interface StrategyRequest {
  name: string
  formula_id: number
  period: string  // 1m|5m|15m|30m|1h|1d|1w|1mon
  role: string  // independent|master|slave
  master_strategy_id: number | null  // 已存在的同组合 master id
  stock_pool_id?: number | null  // null=继承组合级
  slave_exit_mode?: string  // independent|follow_master（仅 slave 有意义）
  capital_ratio: number
  max_positions: number
  single_open_ratio: number
  stop_loss_ratio: number
  take_profit_ratio: number
  trailing_stop_ratio: number
  add_position_threshold: number
  max_add_count: number
  add_position_ratio: number
  reduce_position_ratio: number
  rotation_enabled?: boolean
  rotation_momentum_bars?: number
  rotation_hysteresis?: number
  regime_filter_enabled?: boolean
  trend_trailing_stop_ratio?: number | null
  trend_max_positions?: number | null
  // E3 策略级门控覆盖（inherit/off/on；null=请求未携带）
  market_gate_mode?: string | null
  exposure_cap_enabled?: boolean
  max_order_amount?: number | null  // 单笔最大买入金额（元），null=不限
  requote_enabled?: boolean
  requote_max_attempts?: number
  requote_price_mode?: string  // tick|adaptive（H4）
  requote_max_slippage_ticks?: number
  split_order_enabled?: boolean
  split_order_threshold?: number
  split_order_count?: number | null
  split_order_min_qty?: number | null
  split_order_interval_seconds?: number | null
  split_order_interval_jitter?: boolean
  split_order_volume_pct?: number | null
  risk_timing?: string  // next_day|immediate
  signal_frequency?: string | null
  signal_at?: string | null
  signal_weekday?: number | null
  signal_data_end?: string | null
}

// 独立子策略 CRUD（两层设计：组合下单独管理子策略）
export async function getStrategies(pid: number) {
  const res = await api.get<ApiResponse<StrategyDetail[]>>(`/portfolios/${pid}/strategies`)
  return res.data.data
}

export async function createStrategy(pid: number, req: StrategyRequest) {
  const res = await api.post<ApiResponse<StrategyDetail>>(`/portfolios/${pid}/strategies`, req)
  return res.data.data
}

export async function updateStrategy(pid: number, sid: number, req: StrategyRequest) {
  const res = await api.put<ApiResponse<StrategyDetail>>(`/portfolios/${pid}/strategies/${sid}`, req)
  return res.data.data
}

export async function deleteStrategy(pid: number, sid: number) {
  const res = await api.delete<ApiResponse<null>>(`/portfolios/${pid}/strategies/${sid}`)
  return res.data.data
}

export async function getBacktestRecords() {
  const res = await api.get<ApiResponse<BacktestRecordItem[]>>('/backtest/records')
  return res.data.data
}

export interface BacktestRequest {
  portfolio_strategy_id: number
  name: string
  start_date: string  // YYYY-MM-DD
  end_date: string    // YYYY-MM-DD
  dividend_mode?: 'front' | 'back' | 'dual' | 'exact'  // 复权模式：前复权(默认)/后复权/前复权信号+后复权记账
  evaluation_profile?: 'absolute' | 'relative'  // B6 考核口径（默认 absolute）
  is_oos_gate_enabled?: boolean                 // C7 IS/OOS 采纳门槛（默认关）
  oos_gate_min_ratio?: number                   // (0,2]，默认 0.5
  fee_reserve_enabled?: boolean                 // 费用预留（默认关）：买入审批含佣金防负现金
  bond_t0_enabled?: boolean                     // 可转债 T+0 回转（默认关，仅回测）：转债当日买入当日可卖
  bond_lot_enabled?: boolean                    // 可转债 10 张取整（默认关，仅回测）：转债最小交易单位 10 张
  adaptive_slippage_enabled?: boolean           // 自适应滑点 run 级覆盖（undefined=用组合默认）
  slippage_impact_coef?: number                 // (0,1]，run 级覆盖冲击系数（undefined=用组合默认）
  volume_limit_enabled?: boolean                // 成交量约束 run 级覆盖（undefined=用组合默认）
  volume_limit_ratio?: number                   // (0,1]，run 级覆盖占比（undefined=用组合默认）
  capacity_model?: 'linear' | 'sqrt'            // 容量模型 run 级覆盖（undefined=linear 旧口径）
  capacity_impact_k?: number                    // sqrt 冲击系数（0,1]，0.05/0.10/0.20 敏感性对照
  liquidity_caliber?: 'same_day_bar' | 'prev5d_avg'  // 成交量闸门分母（undefined=same_day_bar 旧口径）
  equity_cap?: number                           // 容量上限 run 级覆盖，>0（undefined=用组合默认）
  daily_close_peak_enabled?: boolean            // 日末 peak 保护（默认关）：仅当日最后一根 bar 更新峰值/回撤
  daily_loss_basis?: 'equity' | 'initial' | null   // 日内亏损分母 run 级覆盖（null/undefined=跟随组合）
  market_gate_enabled?: boolean | null             // 市场多空门控 run 级三态覆盖（null/undefined=跟随组合）
  industry_settings?: Partial<IndustrySettings>     // 行业门控 run 级临时覆盖（undefined=跟随组合，不改配置）
  max_order_amount?: number                        // 策略单笔上限 run 级覆盖（undefined=跟随组合；0=本次不限额）
  loan_model_plan?: Record<string, unknown>        // 资金池 B3：L2 拼盘借贷计划（默认不带）
  gate_release_plan?: Record<string, unknown>      // 资金池 D3/D4：门控释放/召回（默认不带）
  pairing_plan_id?: number                         // 资金池 B6：回测完成后绑定该拼盘
}

// 资金池 D4：门控释放/召回时间轴（只读；None=未启用）
export interface GateReleaseEvent {
  day: string
  gate: string          // OPEN / CLOSED / UNKNOWN
  status: string        // IDLE / PENDING_RELEASE / RELEASED / REOPENING
  release_amount: string | null
  recalled: string[]
  shortfall: string
  reasons: string[]     // 原因码（gate_closed_confirmed / release_done / ...）
  budgets?: Record<string, string>
  due_recalled?: string[]
}
export interface GateReleaseResult {
  version: string
  lender_strategy_id: number
  borrower_strategy_id: number
  events: GateReleaseEvent[]
  states: Record<string, unknown>
  final_budgets: Record<string, string>
  loan_book: Record<string, unknown>
}

// C7 IS/OOS 采纳门槛评估结果（回测完成时返回；未启用为 null）
export interface BacktestGateResult {
  status: 'passed' | 'rejected' | null
  reasons: string[]
  min_ratio: number
}
export interface BacktestRunResult {
  record_id: number
  warnings: string[]
  gate: BacktestGateResult | null
  gate_release?: GateReleaseResult | null
}

export async function runBacktest(req: BacktestRequest) {
  const res = await api.post<ApiResponse<BacktestRunResult>>('/backtest', req)
  return res.data.data
}

// B6：详情可按 profile 临时切换验收口径（不改记录存储值）
export async function getBacktestDetail(id: number, profile?: 'absolute' | 'relative') {
  const res = await api.get<ApiResponse<BacktestDetailItem>>(`/backtest/records/${id}`, {
    params: profile ? { profile } : undefined,
  })
  return res.data.data
}

export async function deleteBacktestRecord(id: number) {
  const res = await api.delete<ApiResponse<null>>(`/backtest/records/${id}`)
  return res.data.data
}

// K1 协作式取消：请求中止运行中的回测（引擎下一根 bar 前退出，记录标 canceled）
export async function cancelBacktestRecord(id: number) {
  const res = await api.post<ApiResponse<{ record_id: number; cancel_requested: boolean }>>(
    `/backtest/records/${id}/cancel`)
  return res.data.data
}

// ===== C8 walk-forward 滚动验证 =====
export interface WalkForwardRequest {
  portfolio_strategy_id: number
  name: string
  start_date: string
  end_date: string
  train_years: number
  test_years: number
  oos_gate_min_ratio: number
  dividend_mode: 'front' | 'back' | 'dual' | 'exact'
  evaluation_profile: 'absolute' | 'relative'
}
export interface WalkForwardWindow {
  index: number
  train: { start: string | null; end: string | null }
  test: { start: string | null; end: string | null }
  record_id: number | null
  is_cagr: number | null
  oos_cagr: number | null
  oos_return: number | null
  oos_benchmark_return: number | null
  gate: { status: 'passed' | 'rejected' | null; reasons: string[] }
  error?: string
}
export interface WalkForwardSummary {
  windows: WalkForwardWindow[]
  windows_passed: string  // "k/n"
  windows_failed: number   // W5-L5 失败窗口数
  windows_errored: number  // W5-L5 出错窗口数
  overall_pass: boolean
}
export interface WalkForwardRunItem {
  id: number
  portfolio_strategy_id: number
  name: string
  start_date: string | null
  end_date: string | null
  train_years: number
  test_years: number
  oos_gate_min_ratio: number | null
  status: string
  error_message: string | null
  summary: WalkForwardSummary | null
  created_at: string | null
  completed_at: string | null
}
export async function runWalkForward(req: WalkForwardRequest) {
  const res = await api.post<ApiResponse<{
    run_id: number
    status: string
    windows_passed: string
    overall_pass: boolean
    error: string | null
  }>>('/backtest/walk-forward', req)
  return res.data.data
}
export async function getWalkForwardRuns() {
  const res = await api.get<ApiResponse<WalkForwardRunItem[]>>('/backtest/walk-forward')
  return res.data.data
}
export async function getWalkForwardRun(id: number) {
  const res = await api.get<ApiResponse<WalkForwardRunItem>>(`/backtest/walk-forward/${id}`)
  return res.data.data
}
export async function deleteWalkForwardRun(id: number) {
  const res = await api.delete<ApiResponse<null>>(`/backtest/walk-forward/${id}`)
  return res.data.data
}

// ===== P2b 参数微扰敏感性分析 =====
export interface ParamPerturbRequest {
  portfolio_strategy_id: number
  name: string
  start_date: string
  end_date: string
  perturb_pct: number  // (0,0.5]
  dividend_mode: 'front' | 'back' | 'dual' | 'exact'
  evaluation_profile: 'absolute' | 'relative'
}
export interface PerturbRoundMetrics {
  record_id: number | null
  status: string | null
  annual_return: number | null
  total_return: number | null
  max_drawdown: number | null
  win_rate: number | null
  profit_factor: number | null
  sharpe_ratio: number | null
  total_trades: number | null
  error: string | null
}
export interface PerturbRound extends PerturbRoundMetrics {
  param: string
  direction: 'low' | 'high'
  factor: number
}
export interface PerturbParamBlock {
  param: string
  low: PerturbRound | null
  high: PerturbRound | null
  delta_cagr: number | null  // 实为年化收益差值（high − low）
}
export interface ParamPerturbSummary {
  baseline: PerturbRoundMetrics
  rounds: PerturbRound[]
  params: PerturbParamBlock[]
  most_sensitive: string | null
}
export interface ParamPerturbRunItem {
  id: number
  portfolio_strategy_id: number
  name: string
  start_date: string | null
  end_date: string | null
  perturb_pct: number | null
  status: string
  error_message: string | null
  summary: ParamPerturbSummary | null
  created_at: string | null
  completed_at: string | null
}
export async function runParamPerturb(req: ParamPerturbRequest) {
  const res = await api.post<ApiResponse<{
    run_id: number
    status: string
    rounds_run: string
    most_sensitive: string | null
    error: string | null
  }>>('/backtest/param-perturb', req)
  return res.data.data
}
export async function getParamPerturbRuns() {
  const res = await api.get<ApiResponse<ParamPerturbRunItem[]>>('/backtest/param-perturb')
  return res.data.data
}
export async function getParamPerturbRun(id: number) {
  const res = await api.get<ApiResponse<ParamPerturbRunItem>>(`/backtest/param-perturb/${id}`)
  return res.data.data
}
export async function deleteParamPerturbRun(id: number) {
  const res = await api.delete<ApiResponse<null>>(`/backtest/param-perturb/${id}`)
  return res.data.data
}

export interface CrisisAlphaBenchmarkResolution {
  from_snapshot: boolean
  mismatch: boolean
  benchmark_code: string
  benchmark_name: string
  live_benchmark_code: string
  live_benchmark_name: string
  stored_benchmark_code: string | null
  stored_benchmark_name: string | null
}

export interface CrisisAlphaRecomputeResult {
  status: string
  error: string | null
  computed_at: string | null
  skipped_benchmarks?: string[]
  benchmark_resolution?: CrisisAlphaBenchmarkResolution
}

export async function recomputeCrisisAlpha(id: number) {
  const res = await api.post<ApiResponse<CrisisAlphaRecomputeResult>>(
    `/backtest/records/${id}/crisis-alpha/recompute`)
  return res.data.data
}

// ---- 市场状态×绩效分析（机遇/中性Alpha，regime-alpha 阶段二）----

export interface MarketRegimeStateRow {
  label: string
  label_cn: string
  weeks: number
  ratio: number | null
  strategy_return: number | null
  benchmark_return: number | null
  excess_return: number | null
  win_rate: number | null
  max_drawdown: number | null
  turnover: number | null
  cost_ratio: number | null
}

export interface MarketRegimeBigUpTest {
  threshold: number
  n: number
  avg_strategy: number | null
  avg_benchmark: number | null
  win_rate: number | null
}

export interface MarketRegimeOppCat {
  count: number
  avg_strategy: number | null
}

export interface MarketRegimeReboundSegment {
  start_date: string
  end_date: string
  trough_date: string
  weeks: number
  benchmark_rebound_pct: number | null
  strategy_rebound_pct: number | null
  elasticity: number | null
  benchmark_recover_days: number | null
  benchmark_recovered: boolean
  strategy_recover_days: number | null
  strategy_recovered: boolean
}

export interface MarketRegimeData {
  meta: {
    strategy_name: string
    benchmark_name: string
    n_weeks_labeled: number
    n_weeks_total: number
    state_week_counts: Record<string, number>
  }
  regime_table: Record<string, MarketRegimeStateRow>
  opportunity: {
    n_bull_weeks: number
    up_capture: number | null
    miss_ratio: number
    big_up_tests: MarketRegimeBigUpTest[]
    cats: Record<string, MarketRegimeOppCat>
  }
  rebound: {
    n_segments: number
    avg_elasticity: number | null
    segments: MarketRegimeReboundSegment[]
  }
  neutral: {
    n_range_weeks: number
    range_ratio: number | null
    cumulative_return: number | null
    annualized_return: number | null
    win_rate: number | null
    n_range_days: number
    monthly_trades: number | null
    annual_turnover: number | null
    cost_gross_ratio: number | null
    whipsaw_count: number
    whipsaw_days: number
    avg_trade_pnl: {
      trend: number | null
      trend_count: number
      range: number | null
      range_count: number
    }
  }
  style: StyleBlock
  dispersion: DispersionBlock
  exposure: ExposureBlock
  // 增强A：巨潮四象限纯风格暴露（规模×成长/价值，399372~399377）；老记录无此键
  quadrant_exposure?: ExposureBlock
  industry_attribution: IndustryAttributionBlock
  robustness: RobustnessBlock
}

// ---- 轻量稳健性（FR-20）----

export interface RobustHalfYearRow {
  period: string
  start_date: string
  end_date: string
  strategy_return: number
  benchmark_return: number
  excess_return: number
  max_drawdown: number
  sharpe: number | null
}

export interface RobustDist {
  median: number | null
  min: number | null
  max: number | null
  negative_ratio: number | null
}

export interface RobustWindow {
  window: number
  n_samples: number
  returns: RobustDist
  sharpe: RobustDist
  histogram: { label: string; count: number }[]
}

export interface RobustSensitivityRow {
  code: string
  name: string
  is_primary: boolean
  available: boolean
  n_days: number
  strategy_total_return: number | null
  benchmark_total_return: number | null
  beta: number | null
  jensen_alpha: number | null
  excess_annual_return: number | null
  tracking_error: number | null
  information_ratio: number | null
  up_capture_ratio: number | null
  down_capture_ratio: number | null
  excess_max_drawdown: number | null
}

export interface RobustnessBlock {
  status: 'ok' | 'insufficient' | 'failed'
  error?: string
  n_days?: number
  start_date?: string
  end_date?: string
  half_year_matrix?: RobustHalfYearRow[]
  rolling?: {
    weekly_points: number
    windows: RobustWindow[]
  }
  longest_loss_window?: {
    start_date: string | null
    trough_date: string | null
    end_date: string | null
    days: number
    drawdown: number | null
  }
  benchmark_sensitivity?: RobustSensitivityRow[]
}

// ---- 风格轮动 / 分化度 / 隐含风格暴露（FR-13 ~ FR-16）----

export interface StyleWeekRow {
  date: string
  label: string
  leader_code: string | null
  leader_return: number
  // FR-13 双窗口（Minor-2 修复）：20W 快窗口标签，老记录 payload 无此键
  fast_label?: string
  fast_leader_code?: string | null
}

export interface StyleMatrixRow {
  label: string
  weeks: number
  ratio: number
  strategy_return: number | null
  leader_return: number | null
  excess_return: number | null
  win_rate: number | null
  max_drawdown: number | null
}

export interface StyleBlock {
  available_codes: string[]
  skipped: string[]
  weeks: StyleWeekRow[]
  matrix: Record<string, StyleMatrixRow>
  // FR-13 双窗口：快窗口周数与 20W/60W 标签一致周占比（老记录无此键）
  fast_window?: number
  label_agree_ratio?: number | null
}

export interface DispersionPoint {
  date: string
  dispersion: number
}

export interface DispersionBucket {
  count: number
  mean_excess: number | null
  win_rate: number | null
  threshold: number | null
}

export interface DispersionBlock {
  available_codes: string[]
  skipped: string[]
  series: DispersionPoint[]
  buckets: {
    low: DispersionBucket
    mid: DispersionBucket
    high: DispersionBucket
  }
}

export interface ExposureBlock {
  available_codes: string[]
  available_names: string[]
  skipped: string[]
  dates: string[]
  corr: Record<string, (number | null)[]>
  beta: Record<string, (number | null)[]>
  window: number
}

// ---- 行业归因（FR-17，通达信二级行业 881）----

export interface IndustryExposureSeries {
  code: string
  name: string
  avg_weight: number
  weights: number[]
}

export interface IndustryExposureBlock {
  dates: string[]
  series: IndustryExposureSeries[]
  cash: number[]
}

export interface IndustryHitRatePeriod {
  top_n: number
  periods: number
  periods_holding: number
  hits: number
  hit_rate: number | null
  details: {
    period: string
    held: string[]
    leaders: string[]
    hit: boolean
  }[]
}

export interface IndustryContributionRow {
  code: string
  name: string
  avg_weight: number
  index_return: number | null
  contribution: number
}

export interface IndustryAttributionBlock {
  status: 'ok' | 'needs_rerun' | 'failed'
  taxonomy: string
  reason?: string
  error?: string
  n_days?: number
  unmapped_codes?: string[]
  exposure?: IndustryExposureBlock
  hit_rate?: {
    weekly: IndustryHitRatePeriod
    monthly: IndustryHitRatePeriod
  }
  contribution?: {
    available: boolean
    rows: IndustryContributionRow[]
    total_contribution: number | null
  }
}

export interface BacktestMarketRegimeItem {
  status: 'completed' | 'insufficient' | 'failed'
  error: string | null
  computed_at: string | null
  skipped_benchmarks: string[]
  benchmark_code: string
  benchmark_name: string
  data: MarketRegimeData | null
}

export async function recomputeMarketRegime(id: number) {
  const res = await api.post<ApiResponse<CrisisAlphaRecomputeResult>>(
    `/backtest/records/${id}/market-regime/recompute`)
  return res.data.data
}

export interface RobustnessRecomputeResult {
  status: string
  error?: string | null
  computed_at?: string | null
}

export async function recomputeRobustness(id: number) {
  const res = await api.post<ApiResponse<RobustnessRecomputeResult>>(
    `/backtest/records/${id}/robustness/recompute`)
  return res.data.data
}

export interface PoolCompareRequest {
  portfolio_strategy_id: number
  pool_ids: number[]
  start_date: string
  end_date: string
  name_prefix?: string
  dividend_mode?: 'front' | 'back' | 'dual' | 'exact'
}

export interface PoolCompareItem {
  pool_id: number
  pool_name: string
  record_id: number | null
  status: string
  error?: string | null
  metrics: {
    total_return: number | null
    annual_return: number | null
    max_drawdown: number | null
    sharpe_ratio: number | null
    win_rate: number | null
    profit_factor: number | null
    total_trades: number | null
    avg_position_utilization: number | null
    benchmark_return: number | null
  } | null
  robustness: { oos?: { out_of_sample?: { cagr: number | null } | null } | null } | null
}

export async function compareBacktestPools(req: PoolCompareRequest) {
  const res = await api.post<ApiResponse<{ comparisons: PoolCompareItem[] }>>(
    '/backtest/robustness/compare', req)
  return res.data.data
}

export async function compareBacktestRecords(record_ids: number[]) {
  const res = await api.post<ApiResponse<BacktestCompareResult>>(
    '/backtest/records/compare', { record_ids })
  return res.data.data
}

// ---- B4b: 实盘历史查询（订单/成交/持仓）----

export interface LiveOrderItem {
  id: number
  stock_code: string
  trade_type: string  // BUY|SELL
  order_type: string
  price: number | null
  quantity: number
  filled_quantity: number
  filled_price: number | null
  status: string  // submitted|filled|partial|rejected|canceled
  error_message: string | null
  signal_name: string | null
  signal_type: string | null
  bar_time: string | null
  created_at: string | null
  // H5 进度：拆单分组/序号/总数、追价次数、组级汇总（普通委托为 null）
  split_group?: string | null
  split_seq?: number | null
  split_total?: number | null
  split_summary?: {
    total: number
    filled_children: number
    remaining_quantity: number
  } | null
  requote_attempt?: number | null
}

export interface LiveTradeItem {
  id: number
  stock_code: string
  trade_type: string
  price: number
  quantity: number
  amount: number
  commission: number
  stamp_duty: number
  trade_time: string | null
}

export interface LivePositionItem {
  stock_code: string
  quantity: number
  avg_cost: number
  market_value: number
  // 归属（组合策略/子策略）；旧数据/旧接口可能缺省
  portfolio_id?: number | null
  strategy_id?: number | null
  // 来源：strategy=策略持仓（缺省按 strategy）/ external=账户原有持仓
  source?: string
  // 归属：pool=在组合股票池内 / fallback=不在池内、按兜底规则挂到首个组合；
  // 旧数据/旧接口缺省时按 pool 降级（不显示徽标）
  attribution?: 'pool' | 'fallback'
  // 池外外部仓（代码不在任何组合股票池）：true 时前端显示「池外·不参与风控」徽章；
  // 旧接口缺省/池信息未知时按 false 降级（不显示徽章）
  out_of_pool?: boolean
  // D6 峰值（移动止损基准）：峰值价格 / 峰值日期 / 来源（bar/table/period/daily/
  // snapshot/cost/import）与中文标签；旧接口缺省时前端显示「—」
  highest_price?: number
  peak_bar_time?: string | null
  peak_source?: string | null
  peak_source_label?: string | null
}

export async function getLiveOrders(sessionId: number, status?: string) {
  const params = status ? { status } : {}
  const res = await api.get<ApiResponse<LiveOrderItem[]>>(`/live/sessions/${sessionId}/orders`, { params })
  return res.data.data
}

/** G1：待放行的顺延委托（DB 权威 + 运行中内存可达性标记）。 */
export interface LiveDeferredOrderItem {
  id: number
  portfolio_strategy_id: number | null
  strategy_id: number | null
  stock_code: string
  trade_type: string
  price: number | null
  quantity: number
  signal_type?: string | null
  signal_name?: string | null
  period?: string | null
  bar_time?: string | null
  signal_bar_date?: string | null
  status: string
  retry_count: number
  error_message?: string | null
  in_memory: boolean
}

export async function getLiveDeferredOrders(sessionId: number) {
  const res = await api.get<ApiResponse<{ running: boolean; items: LiveDeferredOrderItem[] }>>(
    `/live/sessions/${sessionId}/deferred-orders`)
  return res.data.data
}

/** G1：人工放行（仅 auto，需 confirm）。 */
export async function releaseLiveDeferredOrder(sessionId: number, orderId: number) {
  const res = await api.post<ApiResponse<{ status: string; order_id: number; live_order_id?: number | null }>>(
    `/live/sessions/${sessionId}/deferred-orders/${orderId}/release`, { confirm: true })
  return res.data.data
}

/** G1：人工放弃（终态 canceled，需 confirm，可选原因）。 */
export async function abandonLiveDeferredOrder(
  sessionId: number, orderId: number, reason?: string,
) {
  const res = await api.post<ApiResponse<{ status: string; order_id: number; reason: string }>>(
    `/live/sessions/${sessionId}/deferred-orders/${orderId}/abandon`,
    { confirm: true, ...(reason ? { reason } : {}) })
  return res.data.data
}

/** G4：盘前效果统计（只读，按日/周）。 */
export interface LivePreOpenBucket {
  bucket: string
  reported: number
  filled: number
  cancel_crossed: number
  requeued: number
  blocked: number
  deferred: number
}

export async function getLivePreOpenStats(
  sessionId: number,
  params: { start?: string; end?: string; group_by?: 'day' | 'week' } = {},
) {
  const res = await api.get<ApiResponse<{
    session_id: number
    start: string
    end: string
    group_by: string
    items: LivePreOpenBucket[]
    totals: Omit<LivePreOpenBucket, 'bucket'>
  }>>(`/live/sessions/${sessionId}/pre-open-stats`, { params })
  return res.data.data
}

/** 成交历史。可选 start_date/end_date（YYYY-MM-DD，北京自然日闭区间）服务端预筛；
 *  不传=全量（与旧行为一致，向后兼容）。 */
export async function getLiveTrades(
  sessionId: number,
  range?: { start_date?: string; end_date?: string },
) {
  const params = range && (range.start_date || range.end_date) ? range : {}
  const res = await api.get<ApiResponse<LiveTradeItem[]>>(
    `/live/sessions/${sessionId}/trades`, { params })
  return res.data.data
}

export async function getLivePositions(sessionId: number) {
  const res = await api.get<ApiResponse<LivePositionItem[]>>(`/live/sessions/${sessionId}/positions`)
  return res.data.data
}

// 决策闸门事件（调参可观测性）：聚合统计 + 逐事件，对齐 live.session_decisions
export async function getLiveDecisions(
  sessionId: number, ackStatus?: 'open' | 'acked' | 'archived',
) {
  const res = await api.get<ApiResponse<DecisionData>>(
    `/live/sessions/${sessionId}/decisions`,
    { params: ackStatus ? { ack_status: ackStatus } : undefined },
  )
  return res.data.data
}

/** 确认/关闭单条决策事件（幂等，可附处理备注与操作者，后端写审计）。 */
export async function ackLiveDecision(
  sessionId: number, eventId: number, note?: string, operator?: string,
) {
  const payload: { note?: string; operator?: string } = {}
  if (note) payload.note = note
  if (operator) payload.operator = operator
  const res = await api.post<ApiResponse<DecisionEventItem>>(
    `/live/sessions/${sessionId}/decisions/${eventId}/ack`,
    payload,
  )
  return res.data.data
}

/** 批量确认筛选条件：必须至少提供一项（后端拒绝「全选所有」）。 */
export interface DecisionBatchFilters {
  gate?: string
  layer?: string
  action?: string
  keyword?: string
  start?: string  // YYYY-MM-DD
  end?: string    // YYYY-MM-DD
}

export interface DecisionBatchPreview {
  matched: number
  high_risk_excluded: number
  by_gate: Record<string, number>
  high_risk_gates: { gate: string; reason: string }[]
}

export interface DecisionBatchAckResult {
  acked: number
  batch_id: string | null
  matched: number
  high_risk_excluded: number
  note: string
  operator: string
}

/** 批量确认预览：返回「将确认 N 条」与高危排除数（高危只能逐条确认）。 */
export async function previewLiveDecisionBatch(
  sessionId: number, filters: DecisionBatchFilters,
) {
  const res = await api.get<ApiResponse<DecisionBatchPreview>>(
    `/live/sessions/${sessionId}/decisions/batch-ack/preview`,
    { params: filters },
  )
  return res.data.data
}

/** 批量确认（高危门类自动排除）；expected_count 不一致时后端返回 409。 */
export async function batchAckLiveDecisions(
  sessionId: number,
  payload: DecisionBatchFilters & {
    note: string; operator?: string; expected_count?: number
  },
) {
  const res = await api.post<ApiResponse<DecisionBatchAckResult>>(
    `/live/sessions/${sessionId}/decisions/batch-ack`,
    payload,
  )
  return res.data.data
}

export interface DecisionAckLogItem {
  id: number
  batch_id: string | null
  live_session_id: number
  event_id: number | null
  action: 'ack' | 'archive' | string
  from_status: string | null
  to_status: string | null
  operator: string | null
  reason: string | null
  filters_json: string | null
  affected_count: number | null
  created_at: string | null
}

/** 处理审计回看：单条确认/批量确认/过期归档（只读）。 */
export async function getLiveDecisionAckLogs(
  sessionId: number, opts?: { limit?: number; eventId?: number },
) {
  const params: Record<string, number> = {}
  if (opts?.limit) params.limit = opts.limit
  if (opts?.eventId) params.event_id = opts.eventId
  const res = await api.get<ApiResponse<{ items: DecisionAckLogItem[]; total: number }>>(
    `/live/sessions/${sessionId}/decisions/ack-logs`, { params },
  )
  return res.data.data
}

/** L4：关键事件检索（跨会话只读，按 created_at 倒序，limit 上限 1000）。 */
export async function searchDecisionEvents(filters: DecisionEventSearchFilters = {}) {
  const res = await api.get<ApiResponse<DecisionEventSearchResult>>(
    '/live/decision-events', { params: filters })
  return res.data.data
}

// ---- 预警信号（alert 模式）：live.session_signals ----
// 字段对齐 live_service.serialize_signal；alert_status: sent|failed|skipped 等
export interface LiveSignalItem {
  id: number
  portfolio_id: number
  strategy_id: number
  stock_code: string
  period: string
  bar_time: string | null
  formula_name: string
  signal_name: string
  signal_type: string  // OPEN|ADD|REDUCE|CLOSE|RISK_*
  trigger_value: number
  price: string | null
  execution_mode: string
  status: string  // new|notified|...
  alert_status: string | null  // sent|failed|skipped
  alert_error: string | null
  tdx_run_id: string | null
  created_at: string | null
  alerted_at: string | null
}

export interface LiveSignalData {
  items: LiveSignalItem[]
  total: number
}

export interface LiveSignalFilters {
  strategy_id?: number
  stock_code?: string
  signal_type?: string
  execution_mode?: string
  alert_status?: string
  start?: string  // ISO 时间
  end?: string    // ISO 时间
  limit?: number
  offset?: number
}

export async function getLiveSignals(sessionId: number, filters: LiveSignalFilters = {}) {
  const res = await api.get<ApiResponse<LiveSignalData>>(
    `/live/sessions/${sessionId}/signals`,
    { params: filters },
  )
  return res.data.data
}

// ---- #27: 实盘会话 CRUD/启停（LiveSessions.vue 曾原生 axios 直连）----
// 列表对齐 live.py list_sessions serializer；启停/create 返回 {id, status}

export interface LiveSessionItem {
  id: number
  name: string
  mode: string  // simulation|live
  engine?: LiveEngineKey  // 交易引擎（旧数据缺省视作 iquant）
  execution_mode?: LiveExecutionMode  // 执行模式（旧数据缺省视作 auto）
  status: string  // stopped|running|stopping（stopping=引擎真正退出中，仍占用引擎/账户）
  engine_running?: boolean  // 该会话引擎是否真的在注册表中运行（status=running 但 false=重启后未启动）
  // 账户身份键（launch 成功后由后端解析写入）：qmt:<account>:<type> / gm:<id> /
  // tdx:<account> / ths:<account> / engine:<engine>（取不到账号时降级）
  account_key?: string | null
  // 账户键来源：bridge_ping|config|engine_fallback；engine_fallback 表示仅同引擎互斥
  account_key_source?: string | null
  // 占用起始时间（直接取 started_at，供引擎下拉置灰/多账户横幅展示）
  occupied_since?: string | null
  // 通道在线态（会话详情接口返回；列表接口通常不返回 → undefined=未知）
  bridge_online?: boolean | null
  live_capital?: number | null  // 实盘资金基准（元）；null=以真实通道总资产为基准（只缩不放）
  started_at: string | null
  stopped_at: string | null
  portfolio_ids: number[]
  // 理论 vs 实盘面板绑定的回测记录（null=未显式绑定，面板按组合自动关联）
  backtest_record_id?: number | null
  portfolios?: { portfolio_id: number; status: string }[]  // 组合级状态(active|circuit_broken)
  reconcile_mismatches?: ReconcileMismatch[]  // 最近一次虚拟持仓 vs 桥实际对账差异（未运行 []）
  advance_close_enabled?: boolean  // 14:50 提前收盘价（B 可选增强，仅 1d）是否开启
  advance_close_time?: string      // 提前收盘价轮次时点（默认 14:51，实际不早于 14:52）
  advance_close_done_date?: string | null  // 提前收盘价轮次已执行日期（幂等标记）
  // 错周期风控三态（原始值）：null=未显式设置（生效＝关闭，纯用户控制）/ true=强制开启 / false=强制关闭
  intraday_mismatched_risk_enabled?: boolean | null
  // 逐 1m 风控生效值与来源（explicit=用户显式设置 / unset=未设置→关闭）
  intraday_mismatched_risk_effective?: boolean
  intraday_mismatched_risk_source?: 'explicit' | 'unset'
  minute_strategy_periods?: string[]
  // A1 资金池（列表/详情回显）
  capital_pool_mode?: 'off' | 'equal' | 'risk_budget' | 'priority'
  capital_pool_rebalance_enabled?: boolean
  // 外部信号文件注入：null/空 = 走 TQ 公式计算
  signal_file_path?: string | null
  // B2 主备通道（后续批次使用；旧数据缺省）
  backup_engine?: LiveEngineKey | null
  failover_enabled?: boolean
  // H2 待补发拆单剩余笔数（>0 显示「补发拆单剩余」入口）
  split_remainder_count?: number
  // E1 执行前重判门控（默认关）：顺延次日单/盘前竞价单提交前再判一次
  gate_recheck_on_execute?: boolean
  // G2 盘前参数会话级覆盖（原始三态，null=跟随全局）
  pre_open_auction_enabled?: boolean | null
  pre_open_auction_end_time?: string | null
  pre_open_auction_price_bias_pct?: number | null
  // 生效值（含来源 source=session|global 与被覆盖字段清单）
  pre_open_auction_effective?: {
    enabled: boolean
    end_time: string
    price_bias_pct: number
    source: 'session' | 'global'
    overrides: string[]
  }
  // 存量/外部持仓是否纳入风控（代卖）会话级开关：
  //   platform_only=关闭（默认，存量/外部仓不代卖）/ full=开启（按整仓卖）
  // sell_scope_source=session 表示本会话显式设置；global 表示未设置、跟随全局默认
  sell_scope?: 'full' | 'platform_only'
  sell_scope_source?: 'session' | 'global'
  sell_scope_global?: 'full' | 'platform_only'
  // F1 拼盘/运行组绑定：与 backtest_record_id 三选一（至多一个非空）
  backtest_run_group_id?: number | null
  pairing_plan_id?: number | null
  // F2 只读继承报告（未绑定 enabled=false；不参与交易）
  capital_inheritance?: CapitalInheritanceReport | null
}

// F2 实盘继承矩阵（计划 → 会话；只读）
export interface CapitalInheritanceItem {
  side: string
  field: string
  mode: 'inherit' | 'confirm' | 'haircut'
  plan: unknown
  live: unknown
  status: 'match' | 'drift' | 'missing' | 'confirm'
}

export interface CapitalInheritanceReport {
  enabled: boolean
  found?: boolean
  reason?: string
  version?: string
  source?: 'pairing_plan' | 'backtest_run_group'
  plan_id?: number | null
  plan_status?: string | null
  run_group_id?: number | null
  pairing_version?: string | null
  matrix?: { key: string; mode: string; note: string }[]
  items?: CapitalInheritanceItem[]
  drift_fields?: string[]
  drift_count?: number
  haircut?: {
    version: string
    composite: number
    factors: {
      key: string
      factor: number
      default: number
      overridden: boolean
      reason: string
    }[]
    warnings: { code: string; key?: string }[]
  }
  warnings?: { code: string; message?: string; side?: string; field?: string }[]
}

// H2 拆单剩余量（停会话时未提交的子单快照）
export interface LiveSplitRemainderItem {
  id: number
  portfolio_strategy_id: number | null
  strategy_id: number | null
  stock_code: string
  trade_type: string
  quantity: number
  price: number | null
  signal_type: string
  period: string
  signal_name: string
  created_at: string | null
}

export interface LiveSplitRemainderList {
  items: LiveSplitRemainderItem[]
  count: number
}

// E2 每日市场门控快照（每组合每日一条；direction 1/0/-1）
export interface LiveGateSnapshotItem {
  id: number
  snap_date: string | null
  portfolio_id: number
  benchmark_index: string | null
  gate_engine: string | null
  direction: number | null
  strength: number | null
  as_of: string | null
}

// I-7（audit）：对账差异摘要（LiveEngine._reconcile_positions 产出）
export interface ReconcileMismatch {
  code: string
  virtual: number
  real: number
  diff: number
  // 本次差异的计算时点（ISO）；旧数据/旧接口可能缺省
  computed_at?: string
}

// 实盘交易引擎：iquant(QMT 通道) / tdx(通达信TQ) / ths(同花顺UI自动化)
export type LiveEngineKey = 'iquant' | 'tdx' | 'ths' | 'goldminer'

// 执行模式：alert(半自动预警,仅 TQ 行情+send_warn,不下单) / auto(全自动,走所选引擎下单)
// / manual(人工审批) / shadow(影子：真实行情+心跳,委托本地模拟成交,不触真实柜台)
export type LiveExecutionMode = 'alert' | 'auto' | 'manual' | 'shadow'

export interface LiveSessionCreate {
  name: string
  mode: string
  engine?: LiveEngineKey
  execution_mode?: LiveExecutionMode
  portfolio_ids: number[]
  live_capital?: number | null  // 实盘资金基准（元）；空=null=以真实通道总资产为基准（只缩不放）
  // 基准口径（文档 §7/§6）：total_asset=以真实总资产为基准 / manual=显式填写 / initial_capital=旧行为
  baseline_mode?: 'total_asset' | 'manual' | 'initial_capital'
  baseline_value?: number | null
  signal_file_path?: string | null  // 外部信号文件路径；空=null=走 TQ 公式计算
  advance_close_enabled?: boolean  // B 可选增强，仅 1d：14:50 定信号、14:52 bar 开盘价报单、当日尾盘成交；默认 false（1d/1w/1mon 均 15:05 收盘落定后算、次日执行）
  advance_close_time?: string      // 执行时点 HH:MM；默认 14:51，限定 14:51–14:59，实际轮次不早于 14:52
  // 错周期风控三态：null=跟随全局（tq_trade.intraday_mismatched_risk_enabled，默认 False=对齐回测）/
  // true=强制开启（任意 bar 都跑风控，旧行为）/ false=强制关闭
  intraday_mismatched_risk_enabled?: boolean | null
  // E1 执行前重判门控（默认关）
  gate_recheck_on_execute?: boolean
  // G2 盘前参数会话级覆盖：null=跟随全局；显式值=本会话覆盖
  pre_open_auction_enabled?: boolean | null
  pre_open_auction_end_time?: string | null
  pre_open_auction_price_bias_pct?: number | null
  // F1 拼盘/运行组绑定（三选一；空=不绑定）
  backtest_run_group_id?: number | null
  pairing_plan_id?: number | null
}

/** 真实总资产预览（GET /live/account-preview）：只读；通道未就绪时后端 503。 */
export interface LiveAccountPreview {
  total_asset: number | null
  available: number | null
  captured_at: string | null
}

export async function getLiveSession(sessionId: number) {
  const res = await api.get<ApiResponse<Record<string, any>>>(`/live/sessions/${sessionId}`)
  return res.data.data
}

/** F2 数据体检 + 非盘中自动补齐（GET /live/sessions/{id}/data-health，只读）。 */
export async function getLiveDataHealth(sessionId: number, autoFill = true) {
  const res = await api.get<ApiResponse<Record<string, any>>>(
    `/live/sessions/${sessionId}/data-health`,
    { params: { auto_fill: autoFill } },
  )
  return res.data.data
}

export async function getLiveSessions() {
  const res = await api.get<ApiResponse<LiveSessionItem[]>>('/live/sessions')
  return res.data.data
}

// ---- 存量/外部仓是否纳入风控（代卖）：会话级开关 ----
export interface SellScopePositionDiff {
  stock_code: string
  counter_qty: number
  virtual_qty: number
}

export interface SellScopeGate {
  passed: boolean
  source: string
  reasons: string[]
  snapshot_at?: string | null
  cash?: Record<string, string | null>
  equity?: Record<string, string | null>
  position_diffs?: SellScopePositionDiff[]
}

export interface SellScopeSwitchResult {
  session_id: number
  old: 'full' | 'platform_only'
  new: 'full' | 'platform_only'
  changed: boolean
  running: boolean
  effective_at: 'now' | 'next_start'
  message: string
  gate: SellScopeGate | null
}

/**
 * 切换会话级卖出范围（存量/外部仓是否代卖）。
 *
 * - 切到 full 会先在后端做全量对账，账不齐返回 409（错误 message 里带差异明细）；
 * - 运行中立即生效（下一轮 tick），不重启会话；已提交未成交委托不撤销。
 */
export async function setLiveSessionSellScope(
  sessionId: number,
  scope: 'full' | 'platform_only',
  reason = '',
) {
  const res = await api.post<ApiResponse<SellScopeSwitchResult>>(
    `/live/sessions/${sessionId}/sell-scope`,
    { scope, reason, confirm: scope === 'full' },
  )
  return res.data.data
}

/** H2 参数漂移（绑定回测记录时）：当前组合参数 vs params_snapshot 的差异。 */
export interface ParamsDriftField {
  field: string
  snapshot: string | null
  current: string | null
}

export interface ParamsDrift {
  backtest_record_id: number | null
  portfolio_strategy_id?: number
  checked: boolean
  drifted: boolean
  fields: ParamsDriftField[]
  block_enabled: boolean
  reason?: string | null
}

export async function getLiveParamsDrift(sessionId: number) {
  const res = await api.get<ApiResponse<ParamsDrift>>(`/live/sessions/${sessionId}/params-drift`)
  return res.data.data
}

// ---- N2/K7 理论 vs 实际对照 ----
export interface TheoryVsLiveCurvePoint {
  date: string
  total_value: number | null
  index_value: number | null
}

export interface TheoryVsLiveSide {
  curve: TheoryVsLiveCurvePoint[]
  metrics: Record<string, number | null>
}

export interface TheoryVsLiveSlippagePair {
  stock_code: string
  date: string
  trade_type: string
  theory_price: number
  live_price: number
  deviation_bps: number
}

export interface PairingMemberCompare {
  strategy_id: number
  strategy_name: string | null
  role: 'lender' | 'borrower' | null
  portfolio_strategy_id: number | null
  theory: {
    record_id: number | null
    record_name: string | null
    status: 'ok' | 'empty' | 'insufficient' | 'record_missing'
    curve: TheoryVsLiveCurvePoint[]
    metrics: Record<string, number | null>
  }
  live: {
    status: 'ok' | 'missing' | 'insufficient'
    curve: TheoryVsLiveCurvePoint[]
    metrics: Record<string, number | null>
  }
  deviation: null | {
    common_days: number
    window_start: string
    window_end: string
    return_gap: number | null
    max_drawdown_gap: number | null
  }
  warnings: { code: string; message?: string }[]
}

// F4 拼盘会话按成员对比
export interface PairingTheoryVsLive {
  bound: boolean
  found: boolean
  source: 'pairing_plan' | 'backtest_run_group'
  plan_id: number | null
  plan_name?: string | null
  plan_status?: string | null
  run_group_id?: number | null
  members: PairingMemberCompare[]
  theory_series_status: string
  warnings: { code: string; message?: string }[]
}

export interface TheoryVsLiveResult {
  bound: boolean
  backtest_record_id: number | null
  backtest_name: string | null
  // manual=会话显式绑定；auto=按会话组合自动关联最近一次已完成回测；
  // F4 起拼盘会话为 pairing_plan / backtest_run_group
  binding_source: 'manual' | 'auto' | 'pairing_plan' | 'backtest_run_group' | null
  binding_note: string | null
  live_series_status: 'missing' | 'ok'
  theory_series_status: 'no_binding' | 'record_missing' | 'empty' | 'insufficient' | 'ok'
  window: { start: string | null; end: string | null }
  theory: TheoryVsLiveSide
  live: TheoryVsLiveSide
  deviation: null | {
    return_gap: number | null
    max_drawdown_gap: number | null
    trades_gap: number
    slippage: {
      matched_pairs: number
      weighted_deviation_bps: number | null
      details: TheoryVsLiveSlippagePair[]
    }
  }
  warnings: string[]
  // F4：绑定拼盘/运行组时的成员对照；非拼盘会话为 null/缺省
  pairing?: PairingTheoryVsLive | null
}

export async function getTheoryVsLive(
  sessionId: number, params: { portfolio_id?: number; start?: string; end?: string } = {},
) {
  const res = await api.get<ApiResponse<TheoryVsLiveResult>>(
    `/live/sessions/${sessionId}/theory-vs-live`, { params })
  return res.data.data
}

// ---- N3 信号回放校验 ----
export interface SignalReplayRow {
  bar_time: string
  date: string | null
  strategy_id: number
  stock_code: string
  signals: Array<{ name: string; signal_type: string | null; value: number }>
  directions: string[]
}

export interface SignalReplayResult {
  portfolio_strategy_id: number
  portfolio_name: string
  start_date: string
  end_date: string
  dividend_mode: string
  session_id: number | null
  rows: SignalReplayRow[]
  summary: {
    signal_bars: number
    signals: number
    by_date: Array<{ date: string; count: number }>
    by_strategy: Record<string, number>
    by_stock: Array<{ stock_code: string; count: number }>
  }
  comparison: null | {
    session_id: number
    replay_keys: number
    live_order_keys: number
    matched: number
    only_replay_count: number
    only_live_count: number
    only_replay: Array<{ date: string; stock_code: string; trade_type: string }>
    only_live: Array<{ date: string; stock_code: string; trade_type: string }>
    truncated: boolean
    note: string
  }
  warnings: string[]
}

export async function replaySignals(payload: {
  portfolio_strategy_id: number
  start_date: string
  end_date: string
  dividend_mode?: string
  session_id?: number | null
}) {
  const res = await api.post<ApiResponse<SignalReplayResult>>(
    '/backtest/signal-replay', payload)
  return res.data.data
}

// ---- 外部现金流台账（入金/出金/分红/其他）----
export type LiveCashFlowType = 'deposit' | 'withdraw' | 'dividend' | 'other'

export interface LiveCashFlowItem {
  id: number
  portfolio_id: number
  flow_type: LiveCashFlowType
  amount: string          // 有符号：>0 流入，<0 流出
  flow_date: string | null
  note: string | null
  created_at: string | null
}

export interface LiveCashFlowList {
  flows: LiveCashFlowItem[]
  net_inflow: { portfolio_id: number; net_inflow: string }[]
  total_net_inflow: string
}

export async function getLiveCashFlows(sessionId: number, portfolioId?: number) {
  const res = await api.get<ApiResponse<LiveCashFlowList>>(
    `/live/sessions/${sessionId}/cash-flows`,
    { params: portfolioId !== undefined ? { portfolio_id: portfolioId } : {} })
  return res.data.data
}

/** 登记一笔外部现金流。deposit/withdraw/dividend 传正数（符号由类型决定）；other 按符号判方向。 */
export async function createLiveCashFlow(
  sessionId: number,
  req: {
    portfolio_id: number
    flow_type: LiveCashFlowType
    amount: number
    flow_date?: string | null
    note?: string | null
  },
) {
  const res = await api.post<ApiResponse<{
    id: number; session_id: number; portfolio_id: number; flow_type: string
    amount: string; flow_date: string | null; note: string | null
    net_inflow: string; engine_applied: boolean
  }>>(`/live/sessions/${sessionId}/cash-flows`, req)
  return res.data.data
}

export async function getLiveAccountPreview(engine?: LiveEngineKey) {
  const res = await api.get<ApiResponse<LiveAccountPreview>>('/live/account-preview', {
    params: engine ? { engine } : {},
  })
  return res.data.data
}

export async function createLiveSession(req: LiveSessionCreate) {
  const res = await api.post<ApiResponse<{
    id: number
    status: string
    warnings?: string[]   // A1 软校验：目标引擎已被其它 running/stopping 会话占用（不阻断）
  }>>('/live/sessions', req)
  return res.data.data
}

/** B1 + A4：编辑会话配置。运行中除 name 外进入待生效队列，下次启动生效。 */
export async function updateLiveSession(id: number, req: Record<string, unknown>) {
  const res = await api.put<ApiResponse<{
    session: LiveSessionItem
    applied: string[]
    pending: string[]
    effective_at: 'now' | 'next_start'
    message: string
  }>>(`/live/sessions/${id}`, req)
  return res.data.data
}

/** H2：停会话时未提交的拆单剩余量（待一键补发）。 */
export async function getLiveSplitRemainders(id: number) {
  const res = await api.get<ApiResponse<LiveSplitRemainderList>>(
    `/live/sessions/${id}/split-remainders`)
  return res.data.data
}

/** E2：每日市场门控快照（按日倒序）。 */
export async function getLiveGateSnapshots(id: number, days = 30) {
  const res = await api.get<ApiResponse<LiveGateSnapshotItem[]>>(
    `/live/sessions/${id}/gate-snapshots`, { params: { days } })
  return res.data.data
}

/** H2：一键补发拆单剩余量（需 confirm=true + 会话运行中）。 */
export async function reorderLiveSplitRemainders(id: number, changeIds?: number[]) {
  const res = await api.post<ApiResponse<{
    results: { id: number | null; status: string; reason?: string }[]
    accepted: number
    remaining: number
  }>>(`/live/sessions/${id}/split-remainders/reorder`,
    { confirm: true, change_ids: changeIds })
  return res.data.data
}

/** B2：主/备通道健康探活（只读）。 */
export interface LiveChannelHealth {
  engine: string
  label: string
  online: boolean | null
  online_source?: string
  engine_running?: boolean
  configured?: boolean
  detail?: string
  stale?: boolean
  checked_at?: string | null
}

export interface LiveSessionChannelHealth {
  session_id: number
  primary: LiveChannelHealth | null
  backup: LiveChannelHealth | null
  failover_enabled: boolean
  advice: string
}

export async function getLiveSessionChannelHealth(id: number) {
  const res = await api.get<ApiResponse<LiveSessionChannelHealth>>(
    `/live/sessions/${id}/channel-health`)
  return res.data.data
}

/** B3：通道级守护检查清单（只读提示，不自动拉起终端）。 */
export interface LiveChannelGuardCheck {
  key: string
  ok: boolean
  label: string
  detail: string
}

export interface LiveChannelGuard {
  session_id: number
  status: string
  engine: string
  checks: LiveChannelGuardCheck[]
  ready_to_switch: boolean
  actions: string[]
  guard_mode: string
  checked_at: string
}

export async function getLiveChannelGuard(id: number) {
  const res = await api.get<ApiResponse<LiveChannelGuard>>(
    `/live/sessions/${id}/channel-guard`)
  return res.data.data
}

/** B3：一键重新探活（清 30s 缓存强制探一次）。 */
export async function reconnectLiveChannel(id: number) {
  const res = await api.post<ApiResponse<{
    session_id: number
    engine: string
    online: boolean | null
    stale: boolean
    message: string
    manual_action: string
  }>>(`/live/sessions/${id}/channel-guard/reconnect`)
  return res.data.data
}

/** B2：人工确认后切换主备通道（confirm 必填；运行中下次启动生效）。 */
export async function failoverLiveSession(id: number, targetEngine?: string) {
  const res = await api.post<ApiResponse<{
    session_id: number
    from_engine: string
    to_engine: string
    effective_at: 'now' | 'next_start'
    message: string
  }>>(`/live/sessions/${id}/failover`, {
    confirm: true,
    ...(targetEngine ? { target_engine: targetEngine } : {}),
  })
  return res.data.data
}

/** 启动实盘会话；键鼠冲突（C3）软拦时带 ack_keyboard_risk=true 二次确认。 */
export async function startLiveSession(id: number, opts?: { ack_keyboard_risk?: boolean }) {
  const res = await api.post<ApiResponse<{
    id: number
    status: string
    account_key?: string | null
    account_key_source?: string | null
    warnings?: string[]   // 通道建议类告警（如推荐通道与所选引擎不一致）
  }>>(`/live/sessions/${id}/start`, opts?.ack_keyboard_risk ? { ack_keyboard_risk: true } : undefined, { timeout: 180_000 })
  return res.data.data
}

export async function stopLiveSession(id: number) {
  const res = await api.post<ApiResponse<{ id: number; status: string }>>(`/live/sessions/${id}/stop`)
  return res.data.data
}

/** 重置资金基准（收盘后）：value=null → 当日收盘总资产；仅缩不放。 */
export async function resetLiveBaseline(
  sessionId: number,
  portfolioId: number,
  req: { value: number | null; note?: string },
) {
  const res = await api.post<ApiResponse<{ portfolio_id: number; baseline: number | null }>>(
    `/live/sessions/${sessionId}/portfolios/${portfolioId}/reset-baseline`, req)
  return res.data.data
}

export async function deleteLiveSession(id: number) {
  const res = await api.delete<ApiResponse<null>>(`/live/sessions/${id}`)
  return res.data.data
}

export async function recoverLiveBreaker(sessionId: number, portfolioId: number) {
  const res = await api.post<ApiResponse<{ portfolio_id: number; status: string; circuit_breaker_count: number }>>(
    `/live/sessions/${sessionId}/portfolios/${portfolioId}/recover`,
  )
  return res.data.data
}

// ---- #27: 系统配置（SystemConfig.vue 曾原生 axios 直连）----
// 对齐 core/config.py _defaults()；GET 全量返回、PUT 全量覆盖写

export interface ThsUiConfig {
  enabled: boolean
  window_title: string
  client_exe_path: string
  /** @deprecated 引擎不使用该配置（UI 自动化固定走键盘/控件读表），界面已移除该项。 */
  driver?: string
  captcha_ocr: boolean
  execute_mode: string             // file_builtin=文件内置执行器 | direct=直接执行
  execute_interval_seconds: number // 内置执行器轮询指令文件间隔（秒）
  work_dir: string                 // 交接目录（相对 main/ 解析），实际文件在 {work_dir}/{session_id}/
  available?: boolean
}

export interface TqTradeConfig {
  account: string
  account_type: string
  price_mode: string   // my|cage|latest|ztj|dtj
  client_path: string
  default_engine: string       // tdx|iquant|ths：新建实盘会话未显式选引擎时使用
  cancel_unfilled_minutes: number  // >0：在途单超时未成交自动撤单；0=禁用
  pre_open_auction_enabled: boolean       // 次日盘前（集合竞价）执行日线信号单，默认 false
  pre_open_auction_end_time: string       // 盘前执行窗口结束时间（默认 09:20，左闭右开）
  pre_open_auction_price_bias_pct: number // 盘前委托价相对信号价的让价幅度（%）
}

export interface BackupConfig {
  enabled: boolean      // 每日自动备份开关
  daily_at: string      // 执行时点 HH:MM（Asia/Shanghai，默认 15:10）
  keep: number          // 自动备份保留份数（手动备份不参与自动清理）
  min_free_mb: number   // 磁盘保护阈值：剩余空间低于该值跳过备份
  dir: string           // 备份目录（相对 main/ 解析，默认 data/backup）
  restore_enabled?: boolean  // 批次21 L2：从备份恢复（破坏性，默认关闭）
}

export interface SystemConfig {
  tdx_path: string
  iquant_path: string
  max_concurrent_backtest: number
  database: { sqlite_path: string }
  backup: BackupConfig
  iquant_bridge: {
    simulation: { base_url: string }
    live: { base_url: string }
  }
  goldminer: {
    base_url: string
    token: string
    account_id: string
  }
  ths_ui: ThsUiConfig
  tq_trade: TqTradeConfig
  incremental_backtest?: IncrementalBacktestConfig
}

export interface BackupRecord {
  id: number | null
  kind: 'auto' | 'manual'
  status: 'success' | 'skipped' | 'failed'
  filename: string | null
  path: string | null
  size_bytes: number | null
  message: string | null
  started_at: string | null   // Asia/Shanghai naive ISO
  finished_at: string | null
}

export interface BackupStatus {
  enabled: boolean
  daily_at: string
  keep: number
  min_free_mb: number
  backup_dir: string
  restore_enabled: boolean   // 恢复功能开关（config.backup.restore_enabled，默认 false）
  last_auto: BackupRecord | null
  last_manual: BackupRecord | null
  auto_done_today: boolean
}

/** 备份文件只读校验结果（POST /system/backup/verify）。 */
export interface BackupVerifyResult {
  ok: boolean
  path: string
  exists: boolean
  size_bytes: number | null
  integrity: string
  table_count: number
  tables: string[]
  row_counts: Record<string, number>
  latest_record: string | null
  message: string
}

/** 恢复结果（POST /system/backup/restore）。 */
export interface BackupRestoreResult {
  ok: boolean
  message: string
  restored_from: string | null
  safety_backup: {
    id: number | null
    filename: string | null
    path: string | null
    status: string | null
  } | null
  verify: BackupVerifyResult | null
  matched: boolean
}

// 下单通道类型：ths_ui / iquant_bridge / tdx_tq 为老三类；goldminer_bridge 为掘金桥，
// alert 为预警模式（只推送不下单）。后端会返回全部取值，类型不得窄化。
export type LiveChannelKind =
  | 'ths_ui' | 'iquant_bridge' | 'tdx_tq' | 'goldminer_bridge' | 'alert'

export interface LiveChannelEngine {
  key: LiveEngineKey
  channel: LiveChannelKind
  label: string
  configured: boolean
  detail: string
  // B4 能力矩阵（下单/撤单/行情/限价/备注认领/拆单/盘前限价）
  capabilities?: Record<string, boolean>
  capability_limits?: string[]
}

export interface LiveChannelInfo {
  engine: LiveEngineKey
  active: LiveChannelKind
  label: string
  online: boolean | null
  engine_running?: boolean  // 无运行引擎时为 false（此时 online=null，不做在线判定）
  online_source?: 'engine' | 'probe' | 'none'
  checked_at?: string | null
  stale?: boolean
  configured?: boolean
  detail?: string
  engines: LiveChannelEngine[]
  // B4 当前引擎能力矩阵与中文标签
  capabilities?: Record<string, boolean>
  capability_labels?: Record<string, string>
  ths_ui: ThsUiConfig
  bridge: { simulation: string; live: string }
  tq_trade?: { available: boolean; account: string; account_type: string; price_mode: string; client_path: string }
}

export interface ChannelTestResult {
  ok: boolean
  channel: LiveChannelKind
  error?: string | null
  window_title?: string
  positions?: number
  base_url?: string
}

export async function getLiveChannel(engine?: LiveEngineKey) {
  const res = await api.get<ApiResponse<LiveChannelInfo>>('/live/channel', { params: engine ? { engine } : {} })
  return res.data.data
}

/** watchdog 活跃告警（通道级在线态；engine_stale 属引擎层，不算通道离线）。 */
export interface WatchdogAlarm {
  session_id: number
  rule: string
  channel?: string | null
}

export interface WatchdogStatus {
  running: boolean
  last_scan_at: string | null
  scan_count: number
  last_scan_duration_ms: number | null
  active_alarms: WatchdogAlarm[]
}

export async function getWatchdogStatus() {
  const res = await api.get<ApiResponse<WatchdogStatus>>('/live/watchdog-status')
  return res.data.data
}

export async function testLiveChannel(
  channel: LiveChannelKind, mode = 'simulation',
) {
  const res = await api.post<ApiResponse<ChannelTestResult>>('/live/channel/test', { channel, mode })
  return res.data.data
}

export interface SystemStatus {
  core: { online: boolean; version: string; uptime: string }
}

export async function getSystemStatus() {
  const res = await api.get<ApiResponse<SystemStatus>>('/status')
  return res.data.data
}

// ---------------------------------------------------------------------------
// 统一健康总览（批次21 L3，GET /system/health-overview）
// ---------------------------------------------------------------------------
export interface HealthChannelInfo {
  configured?: boolean
  online?: boolean | null
  engine?: string
  detail?: string
}

export interface HealthSessionItem {
  id: number
  name: string
  mode: string
  status: string
  engine: string | null
  backup_engine: string | null
  registered: boolean
  bridge: { online: boolean | null; status: string }
  channel: { primary: HealthChannelInfo | null; backup: HealthChannelInfo | null }
  channel_ok: boolean
  channel_guard: { available: boolean; ok: boolean; failed: string[] }
  data: {
    available: boolean
    healthy?: boolean
    unavailable?: boolean
    reason?: string | null
    codes_total?: number | null
    periods?: Record<string, { stale: number; empty: number; total: number }>
    bad_total?: number
  }
  today: { orders: number; trades: number }
  alerts: { open_decisions: number; deferred_orders: number; pending_approvals: number }
}

export interface HealthProblem {
  level: 'warn' | 'error'
  text: string
}

export interface HealthOverview {
  generated_at: string
  level: 'ok' | 'warn' | 'error'
  problems: HealthProblem[]
  core: { online: boolean; version: string | null; uptime: string | null }
  watchdog: {
    running: boolean
    last_scan_at: string | null
    active_alarms: unknown[]
  }
  channels: Record<string, unknown>
  sessions: HealthSessionItem[]
  tasks: {
    trade_date: string
    running_sessions: number
    backup_at: string | null
    backup_done_today: boolean
  }
  backup: Partial<BackupStatus>
  disk: { free_mb: number | null; min_free_mb: number; ok: boolean; path: string }
  notify: { enabled: boolean; usable: boolean; channels: string[]; min_level: string }
  alert_totals: { open_decisions: number; deferred_orders: number; pending_approvals: number }
}

/** 统一健康总览：Core / 通道 / 数据新鲜度 / 当日任务 / 磁盘 / 待处理告警（只读）。 */
export async function getHealthOverview() {
  const res = await api.get<ApiResponse<HealthOverview>>('/system/health-overview')
  return res.data.data
}

export async function getSystemConfigs() {
  const res = await api.get<ApiResponse<SystemConfig>>('/system/configs')
  return res.data.data
}

export async function updateSystemConfigs(cfg: SystemConfig) {
  const res = await api.put<ApiResponse<null>>('/system/configs', cfg)
  return res.data.data
}

export async function getBackupStatus() {
  const res = await api.get<ApiResponse<BackupStatus>>('/system/backup')
  return res.data.data
}

/** 立即备份（手动）。失败时后端返回非 0 code，前端拦截器会 reject。 */
export async function createBackup() {
  const res = await api.post<ApiResponse<BackupRecord>>('/system/backup')
  return res.data.data
}

/** 最近备份记录（恢复入口的选择列表）。 */
export async function listBackupRecords(limit = 20) {
  const res = await api.get<ApiResponse<BackupRecord[]>>('/system/backup/records', {
    params: { limit },
  })
  return res.data.data
}

/** 只读校验备份文件（完整性 + 表数 + 行数），不触碰当前库。 */
export async function verifyBackup(payload: { record_id?: number; path?: string }) {
  const res = await api.post<ApiResponse<BackupVerifyResult>>('/system/backup/verify', payload)
  return res.data.data
}

/** 从备份恢复当前库：需 confirm=true 且后端 backup.restore_enabled=true。 */
export async function restoreBackup(payload: { record_id?: number; path?: string; confirm: boolean }) {
  const res = await api.post<ApiResponse<BackupRestoreResult>>('/system/backup/restore', payload)
  return res.data.data
}

// ---------------------------------------------------------------------------
// 升级 / 回滚路径（批次22 L5）
// ---------------------------------------------------------------------------

export interface UpgradeMigrationPending {
  revision: string
  doc: string | null
  down_revision: string | string[] | null
}

export interface UpgradeCheckResult {
  enabled: boolean
  checked_at: string
  migrations: {
    available: boolean
    error: string | null
    current: string[]
    heads: string[]
    pending: UpgradeMigrationPending[]
    pending_count: number
    single_head: boolean | null
    at_head: boolean | null
  }
  backup: {
    latest: BackupRecord | null
    age_hours: number | null
    max_age_hours: number
    fresh: boolean
    required: boolean
    dir: string
  }
  disk: { free_mb: number; min_free_mb: number; ok: boolean }
  steps: string[]
  rollback: string[]
}

/** L5 升级前只读检查：版本差异 / 备份新鲜度 / 回滚提示（不执行迁移）。 */
export async function getUpgradeCheck() {
  const res = await api.get<ApiResponse<UpgradeCheckResult>>('/system/upgrade-check')
  return res.data.data
}

/** L5 升级前强制备份：需后端 upgrade.enabled=true 且显式 confirm=true。 */
export async function upgradeBackup(confirm: boolean) {
  const res = await api.post<ApiResponse<{
    ok: boolean; status: string; message?: string | null; record: BackupRecord
  }>>('/system/upgrade/backup', { confirm })
  return res.data.data
}

// ---------------------------------------------------------------------------
// 接缝续跑式增量回测（GET /incremental/summary、POST /incremental/validate）
// ---------------------------------------------------------------------------

export interface IncrementalBacktestConfig {
  enabled: boolean
  daily_at: string              // 每日续跑时点（Asia/Shanghai），默认 15:10
  max_catchup_days: number      // 补跑上限（交易日），超过标「断档」
  anchor_on_live_start: boolean // 接缝锚定：用实盘起账状态重锚回测
  validate_weekday: number      // 双跑校验星期（0=周一 … 5=周六）
  validate_at: string           // 双跑校验时点
  dividend_mode: string         // front|back|dual
  tolerances: {
    max_net_dev_pct: number
    mean_net_dev_pct: number
    trade_count_match_ratio: number
    direction_match_ratio: number
    max_drawdown_dev_pct: number
  }
}

export interface IncrementalDayRow {
  trade_date: string
  inc_total_value: string | null
  inc_cash: string | null
  inc_market_value: string | null
  live_total_value: string | null
  live_cash: string | null
  live_market_value: string | null
  net_dev_pct: number | null
  inc_trades_count: number | null
  live_trades_count: number | null
  direction_match: boolean | null
  status: 'ok' | 'warn' | 'gap'
  tags: string[]
}

export interface IncrementalCheckpointInfo {
  seam_date: string
  as_of_date: string
  params_hash: string
  schema_version: number
  updated_at: string | null
}

export interface IncrementalValidationInfo {
  id: number
  from_date: string
  to_date: string
  status: 'pass' | 'fail' | 'error'
  max_net_dev_pct: number | null
  mean_net_dev_pct: number | null
  trade_count_match_ratio: number | null
  direction_match_ratio: number | null
  max_drawdown_dev_pct: number | null
  first_divergence_date: string | null
  criteria: Record<string, any>
  created_at: string | null
}

export interface IncrementalSummary {
  supported: boolean
  reason: string
  seam_date: string | null
  seam_cash: string | null
  checkpoint: IncrementalCheckpointInfo | null
  // K3：最近一次接缝判废（null=从未判废）
  last_invalidation: { at: string | null; message: string } | null
  days: IncrementalDayRow[]
  latest_validation: IncrementalValidationInfo | null
  config: {
    enabled: boolean
    daily_at: string
    max_catchup_days: number
    tolerances: IncrementalBacktestConfig['tolerances']
  }
}

export async function getIncrementalSummary(sessionId: number, portfolioStrategyId: number) {
  const res = await api.get<ApiResponse<IncrementalSummary>>('/incremental/summary', {
    params: { session_id: sessionId, portfolio_strategy_id: portfolioStrategyId },
  })
  return res.data.data
}

export async function runIncrementalValidation(
  sessionId: number, portfolioStrategyId: number,
  fromDate?: string | null, toDate?: string | null,
) {
  const res = await api.post<ApiResponse<{
    status: 'pass' | 'fail' | 'error'
    max_net_dev_pct: number | null
    mean_net_dev_pct: number | null
    trade_count_match_ratio: number | null
    direction_match_ratio: number | null
    max_drawdown_dev_pct: number | null
    first_divergence_date: string | null
    dates_count: number
    from_date: string
    to_date: string
  }>>('/incremental/validate', {
    session_id: sessionId,
    portfolio_strategy_id: portfolioStrategyId,
    from_date: fromDate ?? null,
    to_date: toDate ?? null,
  })
  return res.data.data
}

// K3：一键重建接缝（丢弃 checkpoint 后从接缝日全量重建）
export async function rebuildIncremental(
  sessionId: number, portfolioStrategyId: number,
) {
  const res = await api.post<ApiResponse<{
    status: string
    as_of_date?: string | null
    days?: number
    reason?: string
    checkpoint_invalidated?: { categories: string[]; from_as_of: string | null } | null
  }>>('/incremental/rebuild', {
    session_id: sessionId,
    portfolio_strategy_id: portfolioStrategyId,
  })
  return res.data.data
}

// ---------------------------------------------------------------------------
// 资金池额度推导（0021 A4，GET /capital/derive）— 只读，默认关闭
// ---------------------------------------------------------------------------
export interface CapitalVersionInfo {
  capital_pool_version: string
  quota_algo_version: string
  defaults: Record<string, unknown>
}

// 单条额度约束：金额为 Decimal 序列化字符串（可能为 null）
export interface CapitalLimitItem {
  key: string
  label: string
  amount: string | null
  source: string
  formula: string
  degraded: boolean
}

export interface CapitalStrategyQuota {
  strategy_id: number | null
  status: string  // ok|degraded|not_derivable|disabled
  fund: string | null
  single_position_limit: string | null
  gross_quota: string | null
  entitlement: string | null
  risk_factor: string | null
  limits: CapitalLimitItem[]
  binding: string[]
  degraded: string[]
  warnings: string[]
  reasons: string[]
}

export interface CapitalQuotaDerivation {
  enabled: boolean
  status: string  // disabled|ok|degraded|not_derivable
  portfolio_id?: number
  equity?: string | null
  effective_equity?: string | null
  gross_limit?: string | null
  entitlement?: string | null
  headroom?: string | null
  risk_factor?: string | null
  utilization_cap?: string | null
  binding?: string[]
  limits?: CapitalLimitItem[]
  strategies?: CapitalStrategyQuota[]
  warnings?: string[]
  reasons: string[]
  version: CapitalVersionInfo
}

export async function getCapitalDerive(params: {
  portfolio_id: number
  equity?: number
  gross_exposure?: number
}) {
  const res = await api.get<ApiResponse<CapitalQuotaDerivation>>('/capital/derive', { params })
  return res.data.data
}

// ---------------------------------------------------------------------------
// 资金池拼盘（0021 B6，L2 草稿/评估/批准；默认开关关闭时返回 enabled=false）
// ---------------------------------------------------------------------------
export interface PairingSelectOption {
  id: number
  label: string
  detail?: string
}

export interface PairingHardCheck {
  code: string
  rule: string
  state: string        // pass|fail|pending
  message: string
  detail: Record<string, unknown>
}

export interface PairingSoftFeature {
  feature: string
  state: string        // pass|warn|fail|pending
  value: number | null
  message: string
  pending_code?: string | null
  detail?: Record<string, unknown>
}

export interface PairingEvalResult {
  id: number
  status: string       // GO|WATCH|NO_GO
  eval_kind: string
  eval_version: string | null
  threshold_version: string | null
  decided_at: string | null
  reasons: string[]
  hard: PairingHardCheck[]
  soft: PairingSoftFeature[]
  metrics: Record<string, number | null>
  counts: Record<string, number>
}

export interface PairingBorrowerEdge {
  id: number
  lender_id: number
  borrower_id: number
  status: string
  borrower_record_id: number | null
  max_share: string | null
  evaluation_id: number | null
}

export interface PairingQueueMeta {
  position: number
  expires_at?: string | null
  expired_at?: string | null
  [key: string]: unknown
}

export interface PairingPlanSummary {
  id: number
  name: string
  scope: string        // L2（同组合）| L3（跨组合）
  status: string       // DRAFT|APPROVED|WATCH|SUSPENDED|ARCHIVED|QUEUED|EXPIRED
  portfolio_strategy_id: number
  lender_strategy_id: number | null
  borrower_strategy_id: number | null
  borrower_count: number
  borrowers: PairingBorrowerEdge[]
  queue: PairingQueueMeta | null
  principal: string | null
  loan_term_days: number
  pairing_version: string | null
  backtest_record_id: number | null
  created_at: string | null
  approved_at: string | null
  eval: PairingEvalResult | null
}

export interface PairingPlanDetail extends PairingPlanSummary {
  binding: Record<string, unknown>
  evidence: Record<string, unknown>
  edge: PairingBorrowerEdge | null
  edges: PairingBorrowerEdge[]
  loan_model_plan: Record<string, unknown> | null
}

export interface PairingBorrowerInput {
  portfolio_strategy_id: number
  strategy_id: number
  max_share?: number
}

export async function createPairingPlan(payload: {
  name: string
  portfolio_strategy_id: number
  lender_strategy_id: number
  borrower_strategy_ids?: number[]
  borrowers?: PairingBorrowerInput[]
  queue_policy?: 'queue' | 'reject'
  queue_ttl_days?: number
  principal: number
  loan_term_days?: number
  recall_notice_days?: number
  release_delay_days?: number
  max_share?: number
  scope?: string
}) {
  const res = await api.post<ApiResponse<PairingPlanDetail>>('/pairing/plans', payload)
  return res.data.data
}

export async function listPairingPlans(params?: { scope?: string; status?: string }) {
  const res = await api.get<ApiResponse<PairingPlanSummary[] | { enabled: boolean }>>(
    '/pairing/plans', { params })
  return res.data.data
}

export async function getPairingPlan(planId: number) {
  const res = await api.get<ApiResponse<PairingPlanDetail>>(`/pairing/plans/${planId}`)
  return res.data.data
}

export async function evaluatePairingPlan(planId: number) {
  const res = await api.post<ApiResponse<PairingEvalResult>>(`/pairing/plans/${planId}/evaluate`)
  return res.data.data
}

export async function approvePairingPlan(planId: number) {
  const res = await api.post<ApiResponse<PairingPlanDetail>>(`/pairing/plans/${planId}/approve`)
  return res.data.data
}

export async function deletePairingPlan(planId: number) {
  const res = await api.delete<ApiResponse<{ deleted: number }>>(`/pairing/plans/${planId}`)
  return res.data.data
}

export async function getPairingLoanModelPlan(planId: number) {
  const res = await api.get<ApiResponse<Record<string, unknown>>>(`/pairing/plans/${planId}/loan-model-plan`)
  return res.data.data
}

// ---------------------------------------------------------------------------
// G3 拼盘健康指标（0020 §5.5；开关关闭时返回 enabled=false）
// ---------------------------------------------------------------------------
export interface PairingHealthItem {
  code: string
  label: string
  value: number | boolean | null
  state: string        // normal|watch|suspend|skipped
  message: string
  threshold: string
}

export interface PairingHealthRecord {
  id: number
  pairing_plan_id: number
  pairing_id: number | null
  as_of: string
  status: string      // ACTIVE|WATCH|SUSPENDED|INSUFFICIENT
  health_version: string | null
  metrics: Record<string, unknown>
  items: PairingHealthItem[]
  streaks: Record<string, number>
  reasons: string[]
  normal_streak: number
  evaluated_at: string | null
}

export interface PairingHealthHistory {
  plan_id: number
  plan_status: string
  latest: PairingHealthRecord | null
  history: PairingHealthRecord[]
  retire_reason?: Record<string, unknown> | null
  resume?: Record<string, unknown> | null
}

export interface PairingHealthEntry {
  plan_id: number
  name: string
  scope: string
  plan_status: string
  latest: PairingHealthRecord | null
}

export async function listPairingHealth() {
  const res = await api.get<ApiResponse<PairingHealthEntry[] | { enabled: boolean }>>('/pairing/health')
  return res.data.data
}

export async function getPairingHealth(planId: number, limit = 30) {
  const res = await api.get<ApiResponse<PairingHealthHistory>>(
    `/pairing/plans/${planId}/health`, { params: { limit } })
  return res.data.data
}

export async function recordPairingHealth(
  planId: number,
  metrics: Record<string, unknown>,
  options?: { as_of?: string; pairing_id?: number },
) {
  const res = await api.post<ApiResponse<PairingHealthRecord>>(
    `/pairing/plans/${planId}/health`, { metrics, ...(options || {}) })
  return res.data.data
}

export async function resumePairingHealth(
  planId: number, reason: string,
  options?: { valid_until?: string; operator?: string },
) {
  const res = await api.post<ApiResponse<PairingHealthHistory>>(
    `/pairing/plans/${planId}/health/resume`, { reason, ...(options || {}) })
  return res.data.data
}

export async function retirePairingHealth(
  planId: number, reason: string, operator?: string,
) {
  const res = await api.post<ApiResponse<PairingHealthHistory>>(
    `/pairing/plans/${planId}/health/retire`, { reason, operator })
  return res.data.data
}

// ---------------------------------------------------------------------------
// 多组合回测批（0021 E3/E5，只读；聚合落库后冻结）
// ---------------------------------------------------------------------------

export interface BacktestRunGroupSummary {
  id: number
  name: string
  status: string       // running|completed|failed|canceled
  error_message: string | null
  pairing_plan_id: number | null
  aggregate_frozen: boolean
  params_snapshot: Record<string, unknown> | null
  started_at: string | null
  finished_at: string | null
}

export interface BacktestRunGroupDetail extends BacktestRunGroupSummary {
  aggregate: Record<string, unknown> | null
  records: BacktestRecordItem[]
}

export async function listBacktestRunGroups() {
  const res = await api.get<ApiResponse<BacktestRunGroupSummary[]>>('/backtest/run-groups')
  return res.data.data
}

export async function getBacktestRunGroup(groupId: number) {
  const res = await api.get<ApiResponse<BacktestRunGroupDetail>>(`/backtest/run-groups/${groupId}`)
  return res.data.data
}

// ---------------------------------------------------------------------------
// 现金出口（0021 C4，只读影子；未验证一律 OUTLET_NOT_VERIFIED）
// ---------------------------------------------------------------------------

export interface CashOutletItem {
  id: number
  code: string
  name: string
  category: string
  channel: string
  channel_order_enabled: boolean
  yield_source: string | null
  yield_freq: string | null
  yield_history_days: number | null
  liquidity: string
  term_days: number | null
  settlement_max_days: number
  holiday_postpone: boolean
  min_amount: string | null
  max_single_amount: string | null
  max_daily_amount: string | null
  min_increment: string | null
  redeemable_early: boolean
  early_redeem_penalty: string | null
  auto_mature_return: boolean
  fast_redeem_limit: string | null
  risk_level: string | null
  principal_guaranteed: boolean
  arrival_buffer_days: number
  inflow_certainty: string | null
  verified: boolean
  verified_at: string | null
  verify_amount: string | null
  verify_result: string | null
  verify_expires_at: string | null
  status: string
  notes: string | null
  created_at: string | null
  updated_at: string | null
}

export interface CashOutletCheck {
  code: string
  name: string
  state: 'pass' | 'warn' | 'fail'
  detail: string
}

export interface CashOutletEvaluation {
  enabled?: boolean
  version: string
  outlet_id: number
  code: string
  name: string
  status: 'READY' | 'BLOCKED'
  codes: string[]
  checks: CashOutletCheck[]
  warnings: string[]
  yield_backtestable: boolean
  needed_amount: string | null
  need_date: string | null
  decided_at: string
}

export async function listCashOutlets(params?: { category?: string; status?: string }) {
  const res = await api.get<ApiResponse<{ enabled: boolean; items?: CashOutletItem[]; status?: string }>>(
    '/capital/outlets', { params })
  return res.data.data
}

export async function evaluateCashOutlet(
  outletId: number, payload: { need_date?: string; required_amount?: string } = {},
) {
  const res = await api.post<ApiResponse<CashOutletEvaluation>>(
    `/capital/outlets/${outletId}/evaluate`, payload)
  return res.data.data
}

// 0021 G4：现金出口执行台账（只记账，不划转、不改策略归因）
export interface CashOutletOp {
  id: number
  outlet_id: number
  live_session_id: number | null
  mode: string
  amount: string
  status: string
  idempotency_key: string | null
  executed_at: string | null
  expected_return_date: string | null
  returned_at: string | null
  principal_returned: string | null
  yield_amount: string
  shortfall_amount: string
  shortfall_reason: string | null
  approved_by: string | null
  notes: string | null
  created_at: string | null
}

export interface CashOutletOpsSummary {
  outstanding_principal: string
  returned_principal: string
  yield_contribution: string
  shortfall_total: string
  pending_count: number
  executed_count: number
  returned_count: number
}

export interface CashOutletOpsList {
  enabled?: boolean
  items: CashOutletOp[]
  summary: CashOutletOpsSummary
}

export async function listCashOutletOps(params?: {
  outlet_id?: number; live_session_id?: number; status?: string; limit?: number
}) {
  const res = await api.get<ApiResponse<CashOutletOpsList | { enabled: boolean }>>(
    '/capital/outlet-ops', { params })
  return res.data.data
}

export async function createCashOutletOp(payload: {
  outlet_id: number; amount: string; mode?: string
  live_session_id?: number; need_date?: string
  idempotency_key?: string; notes?: string
}) {
  const res = await api.post<ApiResponse<CashOutletOp & { auto?: boolean; auto_blocked_reasons?: string[] }>>(
    '/capital/outlet-ops', payload)
  return res.data.data
}

export async function confirmCashOutletOp(opId: number, approvedBy: string) {
  const res = await api.post<ApiResponse<CashOutletOp>>(
    `/capital/outlet-ops/${opId}/confirm`, { approved_by: approvedBy })
  return res.data.data
}

export async function rejectCashOutletOp(opId: number, reason: string) {
  const res = await api.post<ApiResponse<CashOutletOp>>(
    `/capital/outlet-ops/${opId}/reject`, { reason })
  return res.data.data
}

export async function returnCashOutletOp(
  opId: number,
  payload: { principal_returned: string; yield_amount?: string; shortfall_reason?: string },
) {
  const res = await api.post<ApiResponse<CashOutletOp>>(
    `/capital/outlet-ops/${opId}/return`, payload)
  return res.data.data
}

export async function reconcileCashOutletOps() {
  const res = await api.get<ApiResponse<Record<string, unknown>>>(
    '/capital/outlet-ops/reconcile')
  return res.data.data
}

// 0021 C2：历史利用率分位（只读，可复算）
export interface CapitalUtilizationSeriesPoint {
  date: string | null
  utilization: string
  equity?: string | null
  drawdown?: string | null
}

export interface CapitalUtilizationStats {
  enabled?: boolean
  status: string
  samples: number
  window: number
  u_mean: string | null
  u_p50: string | null
  u_p90: string | null
  u_p95: string | null
  u_max: string | null
  u_min: string | null
  u_current: string | null
  excluded: Record<string, number>
  degraded: string[]
  reasons: string[]
  series?: CapitalUtilizationSeriesPoint[]
  source: string
  portfolio_id: number
  record_id: number | null
  session_id: number | null
}

export async function getCapitalUtilization(params: {
  portfolio_id: number
  source?: 'backtest' | 'live'
  record_id?: number
  window?: number
}) {
  const res = await api.get<ApiResponse<CapitalUtilizationStats>>(
    '/capital/utilization', { params })
  return res.data.data
}
