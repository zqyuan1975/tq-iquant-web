/**
 * 实盘 SSE 事件格式化 —— 纯函数模块,便于单元测试。
 *
 * 与 backend `core/api/live.py` 的 `/sessions/{id}/stream` 事件 payload 对齐。
 * 事件类型:signal / order / trade / position / risk / decision / ping，
 * 以及 watchdog 的 5 类「恢复」事件（recovered，见 RECOVERY_EVENT_TYPES）。
 */

export type LiveEventType =
  | 'signal' | 'order' | 'trade' | 'position' | 'risk' | 'decision' | 'ping'
  | 'recovered'

export interface LiveEvent {
  id: number
  type: LiveEventType
  time: string
  text: string
  /** 连接关闭 / 会话不存在等非事件流标记,不进日志条 */
  meta?: 'stream-closed' | 'stream-open' | 'network-error'
}

export const EVENT_TYPE_LABEL: Record<string, string> = {
  signal: '信号',
  order: '委托',
  trade: '成交',
  position: '持仓',
  risk: '风控',
  decision: '拦截',
  recovered: '恢复',
}

export const EVENT_TYPE_COLOR: Record<string, string> = {
  signal: 'blue',
  order: 'orange',
  trade: 'green',
  position: 'teal',
  risk: 'red',
  decision: 'purple',
  recovered: 'green',
}

/**
 * watchdog 恢复事件（engine_watchdog 转场时发出）：这些 type 直接走 SSE，
 * 不并入 risk 流——恢复是正向事件，混进红色风控条会误导；独立类型也便于
 * 一眼看出「刚才掉线，现在回来了」。
 */
export const RECOVERY_EVENT_TYPES: readonly string[] = [
  'engine_recovered',
  'execution_recovered',
  'bridge_recovered',
  'market_data_recovered',
  'client_recovered',
]

export const RECOVERY_LABEL: Record<string, string> = {
  engine_recovered: '引擎恢复',
  execution_recovered: '执行恢复',
  bridge_recovered: '交易桥恢复',
  market_data_recovered: '行情恢复',
  client_recovered: '客户端恢复',
}

// 决策闸门代码 → 中文（调参可观测性，对齐 backend decision.py gate 词汇表）
export const GATE_LABEL: Record<string, string> = {
  // 策略风控触发
  stop_loss: '止损',
  take_profit: '止盈',
  trailing_stop: '移动止损',
  // 组合熔断
  max_drawdown: '最大回撤熔断',
  daily_loss: '日内亏损暂停',
  risk_recover: '熔断恢复',
  halted_buy_strip: '熔断期剥买单',
  // 信号闸门
  halted_bar_no_price: '停牌/无价格',
  slave_master_block: '从策略主仓限制',
  open_already_holding: '已持仓不再开仓',
  max_positions_full: '持股数达上限',
  reduce_qty_too_small: '减仓量过小',
  add_threshold_not_met: '加仓阈值未达',
  add_count_exceeded: '加仓次数超限',
  missing_strategy_risk: '缺风控配置',
  // 资金闸门
  open_qty_too_small: '开仓资金不足1手',
  add_qty_too_small: '加仓资金不足1手',
  insufficient_funds: '资金不足拒单',
  order_shrunk: '资金不足缩量',
  // T+1
  t1_clamp: 'T+1可用钳量',
  t1_insufficient: 'T+1可用不足',
  // 实盘专属闸门
  after_close_block: '收盘后拦单',
  non_trading_block: '非交易时段拦单',
  dup_skip: '重复单跳过',
  inflight_skip: '在途单跳过',
  bridge_unavailable: '桥离线拒单',
  bridge_rejected: '桥业务拒单',
  approval_failed: '发单兜底失败',
  // 主从策略 / 现金增强（portfolio.py）
  slave_master_selling: '主策略清仓禁开仓',
  cash_enhance_trigger: '现金增强触发',
  follow_master_sell: '跟随主策略平仓',
  // 人工干预（柜台被人手动动了平台账内仓，只打标不阻断）
  manual_intervention: '人工干预',
  // 盘前 / 顺延 / 价格（live_engine、order_machine）
  pre_open_auction: '盘前集合竞价',
  pre_open_price_mode: '盘前价型不支持',
  deferred_release: '顺延单放行',
  submit_price: '提交价刷新',
  // 点击『发送』已受理但柜台认领不到（180s 复检仍待发送 → 回 deferred 重排）
  order_unconfirmed: '已受理未认领',
  // 周期线 / 日线新鲜度与兜底（market_data、daily_closer）
  period_bar_freshness: '周期线不新鲜',
  period_bar_fallback: '周期线聚合兜底',
  daily_bar_freshness: '日线不新鲜',
  signal_bar_date: '信号 bar 顺延',
  bar_gap: '行情断档',
  // 持仓 / 卖出范围（portfolio、live_engine）
  inflight_sell: '在途卖单去重',
  sell_scope: '卖出范围限制',
  out_of_pool_position: '池外持仓',
  // 风控 / 估值（portfolio、live_engine）
  risk_skip_no_price: '无价格跳过风控',
  valuation_missing_price: '估值缺价',
  // 运行健康与账实核对（live_engine）
  tq_lock_timeout: '通达信锁超时',
  loop_saturation: '主循环饱和',
  equity_deviation: '真实/虚拟净值偏差',
  cash_share_scale: '资金占比缩量',
  cash_diff: '现金差异',
  cash_flow: '外部现金流',
  fill_price_deviation: '成交价偏离',
  claim_price_mismatch: '委托价认领不符',
  // 通道在线态（watchdog / 引擎心跳转场，2026-11-13）
  channel_offline: '通道离线',
  channel_recovered: '通道恢复',
  // 下单闸门里此前未登记的中文标签（工作台不再露英文 key）
  signal: '信号闸',
  trading_session: '非交易时段',
  price_limit: '涨跌停限制',
  no_market_data: '无行情数据',
  volume_cap_shrink: '量能上限缩量',
  volume_cap_defer: '量能上限顺延',
  // 资金基准对齐 / 仓位约束（2026-11-16 契约审计补齐）
  params_drift: '参数漂移',
  exposure_cap: '敞口上限',
  max_single_position: '单票上限',
  min_cash_reserve: '现金下限',
  order_amount_cap: '单笔金额上限',
  risk_sell_residual: '风控卖出余额',
  requote: '追价重下',
  insufficient_cash: '可用资金不足',
  after_close_defer: '收盘后顺延',
  keyboard_exclusive: '键鼠通道独占',
  peak_seed: '峰值种入',
  // _rec 口径补齐（同轮审计扫出）
  risk_cooldown: '风控冷静期',
  add_cost_unknown: '加仓成本未知',
  max_holdings: '持仓数达上限',
  // 拆单队列（stop 时子单未清）
  split_children_lost: '拆单队列丢失',
}

// 闸门层 → 中文
export const LAYER_LABEL: Record<string, string> = {
  strategy_risk: '策略风控',
  portfolio_risk: '组合风控',
  signal_gate: '信号拦截',
  capital_gate: '资金拦截',
  t1: 'T+1',
  live_gate: '实盘闸门',
  formula: '公式计算',
  // 持久化风控事件（_emit_risk_safe(persist=True) 落 layer=risk）
  risk: '风控事件',
}

// 闸门动作 → 中文
export const ACTION_LABEL: Record<string, string> = {
  trigger: '触发',
  halt: '熔断',
  recover: '恢复',
  strip: '剥单',
  block: '拦截',
  reject: '拒单',
  shrink: '缩量',
  clamp: '钳量',
  error: '运行错误',
  notice: '提示',
  apply: '已修正',
  skip: '跳过',
  release: '放行',
  defer: '顺延',
  cancel: '撤单',
  cancel_requeue: '撤单重排',
  bias: '让价',
  fallback: '兜底',
  refresh: '刷新价格',
  replay: '补派',
  partial: '部分执行',
  unrecoverable: '不可恢复',
  timeout: '超时',
  alert: '告警',
  forced: '强制',
  requeue: '重新排队',
  scale: '缩量',
  // 契约审计补齐（2026-11-16）
  ack: '已确认',
  allow: '放行',
  carry: '沿用',
  recompute: '重算',
  approve: '审批通过',
  retry: '重试',
  // 拆单队列（stop 时子单未清）
  drop: '丢弃',
}

/** 闸门代码 → 中文标签（未知 gate 回退原代码）。 */
export function gateLabel(gate: string): string {
  return GATE_LABEL[gate] || gate
}

export const TRADE_TYPE_LABEL: Record<string, string> = {
  BUY: '买入',
  SELL: '卖出',
}

/** 通道在线态 → 中文：true=在线 / false=离线 / null|undefined=未知。 */
export function channelStateLabel(v: boolean | null | undefined): string {
  if (v === true) return '在线'
  if (v === false) return '离线'
  return '未知'
}

/**
 * watchdog 里属于「通道级掉线」的规则；engine_stale 只是引擎主循环停摆，
 * 通道本身可能仍在线，不进通道徽章。
 */
export const CHANNEL_ALARM_RULES: readonly string[] = [
  'client_offline', 'market_data_stalled', 'bridge_unreachable', 'execution_stalled',
]

/**
 * 通道徽章状态：优先看 watchdog 的通道级告警（watchdog 30s 一轮，比心跳
 * 60–90s 的 ``bridge_online`` 翻转更早），无告警再回退心跳值。
 * 返回 true=在线 / false=离线 / null=未知（无运行会话或尚未取到）。
 */
export function channelBadgeState(
  bridgeOnline: boolean | null | undefined,
  alarms: readonly { session_id: number; rule: string }[] | null | undefined,
  sessionId: number | null | undefined,
): boolean | null {
  if (sessionId === null || sessionId === undefined) return null
  const hit = (alarms || []).some(
    (a) => a.session_id === sessionId && CHANNEL_ALARM_RULES.includes(a.rule))
  if (hit) return false
  if (bridgeOnline === true || bridgeOnline === false) return bridgeOnline
  return null
}

/** trade_type → 中文标签（大小写不敏感；未知值回退原值）。 */
export function tradeTypeLabel(v: unknown): string {
  const raw = fmt(v)
  return TRADE_TYPE_LABEL[raw.toUpperCase()] || raw
}

export const ORDER_STATUS_LABEL: Record<string, string> = {
  submitted: '已提交',
  filled: '成交',
  partial: '部分成交',
  rejected: '拒绝',
  canceled: '已撤单',
}

export const RISK_RULE_LABEL: Record<string, string> = {
  max_drawdown: '最大回撤熔断',
  daily_loss: '日内亏损熔断',
  reconcile_mismatch: '持仓对账差异',
  manual_intervention: '人工干预',
  quote_polling_paused: '行情轮询暂停',
  quote_polling_resumed: '行情轮询恢复',
  // 行情 / 周期线健康
  bar_gap: '行情断档',
  reconnect: '断线重连',
  no_quote_capability: '行情源不可回补',
  period_bar_fallback_aggregated: '周期线聚合兜底',
  daily_bar_freshness: '日线不新鲜',
  // 运行健康
  tick_too_slow: '主循环单轮过慢',
  formula_injection: '通达信锁超时',
  // 估值 / 账实核对
  price_unavailable: '估值价缺失',
  equity_deviation: '真实/虚拟净值偏差',
  cash_diff: '现金差异',
  external_cash_flow: '外部现金流',
  fill_price_deviation: '成交价偏离',
  out_of_pool_position: '池外持仓',
  // 盘前 / 资金
  pre_open_price_capability: '盘前定价能力不足',
  real_account_insufficient: '真实可用资金不足',
  // 确认点击未生效（行仍在『发送』）后按 deferred 重排
  order_requeued: '点击未生效重排',
  // watchdog 运行健康（engine_watchdog 每 30s 扫描）
  engine_stale: '引擎主循环停摆',
  client_offline: '交易客户端离线',
  market_data_stalled: '行情停滞',
  bridge_unreachable: '交易桥不可达',
  execution_stalled: '执行通道停滞',
  stop_inflight_orders: '停止时撤在途单',
}

const fmt = (v: unknown): string => (v === null || v === undefined ? '' : String(v))

/** 成本价展示：统一 3 位小数（后端计算/存储仍全精度）；
 *  avg_cost<=0 = 柜台成本为负/未知（B5），标注「成本未知」而非漂移现价。 */
function fmtCost(v: unknown): string {
  const n = Number(v)
  if (v === null || v === undefined || !Number.isFinite(n)) return ''
  if (n <= 0) return '未知'
  return n.toFixed(3)
}

/** 金额（市值）展示：统一 2 位小数（分为精度；后端计算/存储仍全精度）。 */
function fmtMoney(v: unknown): string {
  const n = Number(v)
  if (v === null || v === undefined || !Number.isFinite(n)) return ''
  return n.toFixed(2)
}

/** 委托:已提交/成交/部分成交/拒绝/撤单,读 trade_type + quantity/price 或 filled_*。 */
function formatOrder(data: Record<string, unknown>): string {
  const status = String(data.status || 'submitted')
  const stock = fmt(data.stock_code)
  const label = ORDER_STATUS_LABEL[status] || status
  if (status === 'rejected') {
    return `${label} ${stock} ${fmt(data.error_message)}`
  }
  if (status === 'submitted') {
    const t = tradeTypeLabel(data.trade_type)
    const price = data.price ? ` @ ${data.price}` : ''
    return `${t} ${stock} ${fmt(data.quantity)}股${price}`
  }
  // filled / partial
  return `${label} ${stock} ${fmt(data.filled_quantity)}股 @ ${fmt(data.filled_price)}`
}

/** 数值格式化：保留有效位，去掉多余尾零。 */
function fmtNum(v: unknown): string {
  if (v === null || v === undefined || v === '') return ''
  const n = Number(v)
  if (!Number.isFinite(n)) return fmt(v)
  // 比率类（0~1）转百分比更直观；其余保留至多 4 位有效小数。
  if (Math.abs(n) > 0 && Math.abs(n) < 1) return `${(n * 100).toFixed(2)}%`
  return String(Math.round(n * 10000) / 10000)
}

/**
 * 决策闸门事件 → 人类可读文本，例：
 * 「资金不足拒单 000001.SZ 需1000股/实发0股 阈值— 实际现金不足 — 开仓资金不足1手」
 * 调参时一眼看出：哪个闸门、哪只票、阈值 vs 实际、拦掉多少量、原因。
 */
function formatDecision(data: Record<string, unknown>): string {
  const gate = String(data.gate || '')
  // 人工干预不是「拦掉多少股」的闸门：requested/final 是虚拟/真实持仓量，
  // 直接展示后端组好的 message（含代码、虚拟、真实、差额、方向、原因），
  // 避免被渲染成「需0股/实发100股」误导。
  if (gate === 'manual_intervention') {
    return fmt(data.message) || gateLabel(gate)
  }
  const parts: string[] = [gateLabel(gate)]
  const stock = fmt(data.stock_code)
  if (stock) parts.push(stock)

  // 拦截量：请求 vs 实发（final=0 整单被拦；0<final<请求 缩量；final 缺省只报请求量）
  const req = data.requested_qty
  const fin = data.final_qty
  if (req !== null && req !== undefined && req !== '') {
    const finN = fin === null || fin === undefined || fin === '' ? null : Number(fin)
    if (finN === null) {
      parts.push(`${fmt(req)}股被拦`)
    } else if (finN === 0) {
      parts.push(`${fmt(req)}股被拦`)
    } else if (finN !== Number(req)) {
      parts.push(`需${fmt(req)}股/实发${fmt(fin)}股`)
    }
  }

  // 阈值 vs 实际（比率/数量）
  const pv = fmtNum(data.param_value)
  const av = fmtNum(data.actual_value)
  if (pv || av) {
    parts.push(`阈值${pv || '—'} 实际${av || '—'}`)
  }

  const msg = fmt(data.message)
  if (msg) parts.push(msg)
  return parts.join(' ')
}

/** SSE 事件 → 人类可读日志文本。ping 返回空串(心跳不进日志)。 */
export function formatEvent(type: string, data: Record<string, unknown>): string {
  switch (type) {
    case 'signal':
      return `[${fmt(data.signal_type)}] ${fmt(data.signal_name)} ${fmt(data.stock_code)}`
    case 'order':
      return formatOrder(data)
    case 'trade': {
      const t = tradeTypeLabel(data.trade_type)
      const amount = data.amount ? ` 金额${data.amount}` : ''
      return `${t} ${fmt(data.stock_code)} ${fmt(data.quantity)}股 @ ${fmt(data.price)}${amount}`
    }
    case 'position':
      return `${fmt(data.stock_code)} ${fmt(data.quantity)}股 成本${fmtCost(data.avg_cost)} 市值${fmtMoney(data.market_value)}`
    case 'risk': {
      // rule 优先；rule 未登记时退回 gate 标签（同一批新增闸门的 rule 取值不唯一，
      // 例如 bar_gap 的 rule 是断线原因），最后才显示原始 key。
      const rule = fmt(data.rule)
      const label =
        RISK_RULE_LABEL[rule] ||
        (data.gate ? gateLabel(fmt(data.gate)) : '') ||
        rule
      return `${label}: ${fmt(data.message)}`
    }
    case 'decision':
      return formatDecision(data)
    case 'recovered': {
      // watchdog 恢复：rule 即事件名（engine_recovered / bridge_recovered …）
      const rule = fmt(data.rule)
      const label = RECOVERY_LABEL[rule] || '恢复'
      const msg = fmt(data.message)
      return msg ? `${label}: ${msg}` : label
    }
    case 'ping':
      return ''
    default:
      return `${type}: ${JSON.stringify(data)}`
  }
}

let _id = 0

/** 生成全局递增日志条 id(SSE 事件本身无 id 字段,前端自增)。 */
export function nextEventId(): number {
  _id += 1
  return _id
}
