/**
 * 实盘工作台纯函数 —— 订单/成交/持仓行构建 + 历史合并 + SSE 事件增量。
 *
 * 与 backend `core/api/live.py` 三个历史查询端点 + `/stream` SSE payload 对齐。
 * 纯函数便于单元测试;key 用负 id 表示历史行(稳定),SSE 事件行用正自增 id。
 */
import { nextEventId } from './liveEvents'
import type { LiveOrderItem, LiveTradeItem, LivePositionItem, DecisionEventItem, LiveSessionItem } from '../api'

export interface OrderRow {
  key: number
  // 后端 live_order.id（SSE order 事件与历史行共用）；用于同一笔委托的
  // 多次状态事件原地 upsert，避免「已提交/已成交/已撤单」各占一行。
  order_id?: number | null
  time: string
  /** 事件时间 epoch（ms）：筛选区间用；旧数据/无时区 → null（降级显示、不筛掉）。 */
  ts?: number | null
  stock_code: string
  trade_type: string
  status: string
  quantity: number
  price: number | null
  filled_quantity: number | null
  filled_price: number | null
  error_message: string | null
  // H5 进度：拆单子单 x/N、组级汇总（已成交笔数/剩余股数）、追价第 j 次
  split_group?: string | null
  split_seq?: number | null
  split_total?: number | null
  split_summary?: { total: number; filled_children: number; remaining_quantity: number } | null
  requote_attempt?: number | null
}

export interface TradeRow {
  key: number
  time: string
  /** 北京日期标注：今天 / 昨天 / MM-DD；旧数据无时区 → ''（只显示时分秒）。 */
  day?: string
  ts?: number | null
  stock_code: string
  trade_type: string
  price: number
  quantity: number
  amount: number
}

export interface PositionRow {
  stock_code: string
  quantity: number
  avg_cost: number
  market_value: number
  // 归属（组合策略/子策略）；同票多子策略持有时各一行；外部持仓（原有持仓）为 null
  portfolio_id: number | null
  strategy_id: number | null
  // 来源：strategy=策略持仓 / external=账户原有持仓（非策略买入，仅供监视）
  source: string
  // 归属：pool=在组合股票池内 / fallback=不在池内、按兜底规则挂到首个组合；
  // 缺省（旧接口/SSE 事件未带）时不显示徽标，按 pool 降级
  attribution?: 'pool' | 'fallback'
  // 池外外部仓（后端 out_of_pool）：只展示/对账，不进风控、不占槽位、不代卖
  out_of_pool?: boolean
  // D6 峰值（移动止损基准）：价格 / 日期 / 来源标记
  highest_price?: number
  peak_bar_time?: string | null
  peak_source?: string | null
  peak_source_label?: string | null
}

/** 决策闸门事件行（调参可观测性）。 */
export interface DecisionRow {
  key: number
  id?: number | null
  time: string
  ts?: number | null
  gate: string
  layer: string
  action: string
  stock_code: string
  strategy_id: number | null
  param_name: string | null
  param_value: number | null
  actual_value: number | null
  requested_qty: number | null
  final_qty: number | null
  message: string
  // M4 确认/关闭状态
  ack_status?: 'open' | 'acked' | null
  ack_note?: string | null
}

type Ev = Record<string, unknown>

/** ISO datetime → HH:MM:SS（无则空串）。仅用于无时区旧数据的降级显示。 */
const shortTime = (iso?: string | null): string =>
  iso && iso.length >= 19 ? iso.slice(11, 19) : ''

// 时间口径统一为 Asia/Shanghai：后端已给 created_at 标 +00:00、bar_time/trade_time
// 标 +08:00，前端按显式时区换算成北京时钟；naive 旧数据不猜时区、原样降级显示。
const BJ_CLOCK = new Intl.DateTimeFormat('en-GB', {
  timeZone: 'Asia/Shanghai', hour: '2-digit', minute: '2-digit',
  second: '2-digit', hourCycle: 'h23',
})
const BJ_DAY = new Intl.DateTimeFormat('en-CA', {
  timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
})

/** 当前北京时钟 HH:MM:SS（SSE 实时事件用，与历史行同一口径）。 */
export function beijingNowHms(): string {
  return BJ_CLOCK.format(new Date())
}

/** 解析 ISO 时间（需带时区）→ 北京时钟文本 + epoch(ms)。非法/naive → 降级。 */
export function parseBeijingTime(iso?: string | null): { ts: number | null; text: string } {
  if (!iso) return { ts: null, text: '' }
  const norm = iso.includes('T') ? iso : iso.replace(' ', 'T')
  if (!/(?:Z|[+-]\d{2}:?\d{2})$/.test(norm)) {
    // 无时区旧数据：不猜（无法判断是 UTC 还是上海墙钟），原样显示且不参与筛选
    return { ts: null, text: shortTime(norm) }
  }
  const ts = Date.parse(norm)
  if (Number.isNaN(ts)) return { ts: null, text: shortTime(norm) }
  return { ts, text: BJ_CLOCK.format(new Date(ts)) }
}

/** epoch(ms) → 北京 ISO（YYYY-MM-DDTHH:mm:ss，聚合表首次/末次列用）。 */
export function beijingIso(ts: number): string {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Shanghai', year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23',
  }).formatToParts(new Date(ts))
  const v = (t: string) => parts.find((p) => p.type === t)?.value ?? ''
  return `${v('year')}-${v('month')}-${v('day')}T${v('hour')}:${v('minute')}:${v('second')}`
}

// ---- 时间区间筛选（今日 / 近3日 / 近7日 / 全部，按北京自然日）----

export type TimeRange = 'today' | '3d' | '7d' | 'all'

export const TIME_RANGE_OPTIONS: Array<{ value: TimeRange; label: string }> = [
  { value: 'today', label: '今日' },
  { value: '3d', label: '近3日' },
  { value: '7d', label: '近7日' },
  { value: 'all', label: '全部' },
]

export const TIME_RANGE_LABEL: Record<TimeRange, string> = {
  today: '今日', '3d': '近3日', '7d': '近7日', all: '全部',
}

/** 北京自然日键 YYYY-MM-DD。 */
export function beijingDayKey(ts: number): string {
  const parts = BJ_DAY.formatToParts(new Date(ts))
  const v = (t: string) => parts.find((p) => p.type === t)?.value ?? ''
  return `${v('year')}-${v('month')}-${v('day')}`
}

/** 成交日期标注：今天 / 昨天 / MM-DD；ts 不可解析（旧数据无时区）→ ''（只显示时分秒）。 */
export function tradeDayLabel(ts?: number | null, nowTs: number = Date.now()): string {
  if (ts == null) return ''
  const day = beijingDayKey(ts)
  if (day === beijingDayKey(nowTs)) return '今天'
  if (day === beijingDayKey(nowTs - 86400000)) return '昨天'
  return day.slice(5)
}

/** 区间起点 epoch：今日=北京当日 00:00；近N日=含今日往前 N-1 天的 00:00；全部=null。 */
export function rangeStartTs(range: TimeRange, nowTs: number = Date.now()): number | null {
  if (range === 'all') return null
  const days = range === 'today' ? 1 : range === '3d' ? 3 : 7
  const key = beijingDayKey(nowTs - (days - 1) * 86400000)
  const [y, m, d] = key.split('-').map(Number)
  // 中国无夏令时：北京当日 00:00 = 前一日 UTC 16:00
  return Date.UTC(y, m - 1, d) - 8 * 3600 * 1000
}

/** 按区间过滤；ts 缺失/不可解析（旧数据）不筛掉，保证能降级看到。 */
export function filterRowsByRange<T extends { ts?: number | null }>(
  rows: T[], range: TimeRange, nowTs: number = Date.now(),
): T[] {
  const start = rangeStartTs(range, nowTs)
  if (start === null) return rows
  return rows.filter((r) => (r.ts == null ? true : r.ts >= start))
}

/** 聚合统计行（口径对齐后端 summarize_decisions，但按当前筛选区间重算）。 */
export interface DecisionSummaryRow {
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
  /** 成交笔数：本组股票命中的 filled/partial 委托，按 order_id 去重（重复触发不重复计）。
   *  未传委托行（旧调用/旧数据）→ null，前端降级显示「—」。 */
  filled_count?: number | null
  /** 转化率 = 成交笔数 / 触发次数；无触发或未统计 → null。 */
  conversion_rate?: number | null
}

/**
 * 按 (gate, param_name) 聚合当前筛选后的决策行；首次/末次用事件时间（北京）。
 *
 * 成交转化（传入当前筛选区间的委托行时计算）：
 * - 触发次数 = 决策事件条数（同一订单重复触发算多次触发）
 * - 成交笔数 = 本组股票代码命中的、终态 filled/partial 的委托，按 order_id 去重
 *   （同一订单的多次触发/多次状态事件只算 1 笔）
 * - 转化率 = 成交笔数 / 触发次数
 * 未传 orders（旧调用）→ 两列为 null，前端降级显示「—」。
 */
export function summarizeDecisionRows(
  rows: DecisionRow[], orders?: OrderRow[],
): DecisionSummaryRow[] {
  const groups = new Map<string, DecisionSummaryRow & {
    _stocks: Set<string>; _minTs: number | null; _maxTs: number | null
    _hasReq: boolean; _hasFin: boolean
  }>()
  for (const r of rows) {
    const key = `${r.gate}|${r.param_name ?? ''}`
    let g = groups.get(key)
    if (!g) {
      g = {
        gate: r.gate, layer: r.layer, action: r.action, param_name: r.param_name,
        param_value: r.param_value, count: 0, first_bar_time: null,
        last_bar_time: null, stock_count: 0, requested_qty_sum: null,
        final_qty_sum: null, _stocks: new Set(), _minTs: null, _maxTs: null,
        _hasReq: false, _hasFin: false,
      }
      groups.set(key, g)
    }
    g.count += 1
    if (g.param_value == null) g.param_value = r.param_value
    if (r.ts != null) {
      if (g._minTs == null || r.ts < g._minTs) g._minTs = r.ts
      if (g._maxTs == null || r.ts > g._maxTs) g._maxTs = r.ts
    }
    if (r.stock_code) g._stocks.add(r.stock_code)
    if (r.requested_qty != null) {
      g.requested_qty_sum = (g.requested_qty_sum ?? 0) + r.requested_qty
      g._hasReq = true
    }
    if (r.final_qty != null) {
      g.final_qty_sum = (g.final_qty_sum ?? 0) + r.final_qty
      g._hasFin = true
    }
  }
  // 成交笔数：按组内股票代码匹配终态 filled/partial 委托，order_id 去重。
  // 同一订单被多次触发（多次决策事件）只算 1 笔成交；未传 orders → null（旧数据降级）。
  const fillsByKey = new Map<string, number>()
  if (orders !== undefined) {
    const filled = orders.filter((o) => o.status === 'filled' || o.status === 'partial')
    for (const [key, g] of groups) {
      const ids = new Set<string>()
      for (const o of filled) {
        if (!o.stock_code || !g._stocks.has(o.stock_code)) continue
        ids.add(o.order_id != null ? `oid:${o.order_id}` : `row:${o.key}`)
      }
      fillsByKey.set(key, ids.size)
    }
  }
  const out: DecisionSummaryRow[] = []
  for (const [key, g] of groups) {
    const filledCount = orders === undefined ? null : (fillsByKey.get(key) ?? 0)
    out.push({
      gate: g.gate, layer: g.layer, action: g.action, param_name: g.param_name,
      param_value: g.param_value, count: g.count,
      first_bar_time: g._minTs == null ? null : beijingIso(g._minTs),
      last_bar_time: g._maxTs == null ? null : beijingIso(g._maxTs),
      stock_count: g._stocks.size,
      requested_qty_sum: g._hasReq ? g.requested_qty_sum : null,
      final_qty_sum: g._hasFin ? g.final_qty_sum : null,
      filled_count: filledCount,
      conversion_rate: filledCount == null || g.count <= 0 ? null : filledCount / g.count,
    })
  }
  return out.sort((a, b) => (b.count - a.count)
    || String(a.gate).localeCompare(String(b.gate))
    || String(a.param_name ?? '').localeCompare(String(b.param_name ?? '')))
}

/** SSE order 事件 → 委托行；time 优先取外部日志时间,缺省回退 bar_time。 */
export function orderEventToRow(ev: Ev, liveTime = ''): OrderRow {
  const oid = (ev.order_id as number) ?? null
  const t = parseBeijingTime(ev.bar_time as string | undefined)
  return {
    key: oid !== null ? oid : nextEventId(),
    order_id: oid,
    time: liveTime || t.text,
    ts: liveTime ? Date.now() : t.ts,
    stock_code: (ev.stock_code as string) || '',
    trade_type: (ev.trade_type as string) || '',
    status: (ev.status as string) || 'submitted',
    quantity: (ev.quantity as number) ?? (ev.filled_quantity as number) ?? 0,
    price: (ev.price as number) ?? (ev.filled_price as number) ?? null,
    filled_quantity: (ev.filled_quantity as number) ?? null,
    filled_price: (ev.filled_price as number) ?? null,
    error_message: (ev.error_message as string) ?? null,
  }
}

/** 委托历史 → 行（key=-id，稳定标识历史单）。 */
export function orderHistoryToRows(items: LiveOrderItem[]): OrderRow[] {
  return items.map((i) => {
    const bt = parseBeijingTime(i.bar_time)
    const ct = parseBeijingTime(i.created_at)
    return {
      key: -i.id,
      order_id: i.id,
      time: bt.text || ct.text,
      ts: bt.ts ?? ct.ts,
      stock_code: i.stock_code,
      trade_type: i.trade_type,
      status: i.status,
      quantity: i.quantity,
      price: i.price,
      filled_quantity: i.filled_quantity,
      filled_price: i.filled_price,
      error_message: i.error_message,
      split_group: i.split_group ?? null,
      split_seq: i.split_seq ?? null,
      split_total: i.split_total ?? null,
      split_summary: i.split_summary ?? null,
      requote_attempt: i.requote_attempt ?? null,
    }
  })
}

/** SSE trade 事件 → 成交行。 */
export function tradeEventToRow(ev: Ev, liveTime = ''): TradeRow {
  const ts = liveTime ? Date.now() : null
  return {
    key: nextEventId(),
    time: liveTime,
    day: tradeDayLabel(ts),
    ts,
    stock_code: (ev.stock_code as string) || '',
    trade_type: (ev.trade_type as string) || '',
    price: (ev.price as number) ?? 0,
    quantity: (ev.quantity as number) ?? 0,
    amount: (ev.amount as number) ?? 0,
  }
}

/** 成交历史 → 行（key=-id）。 */
export function tradeHistoryToRows(items: LiveTradeItem[]): TradeRow[] {
  return items.map((i) => {
    const t = parseBeijingTime(i.trade_time)
    return {
      key: -i.id,
      time: t.text,
      day: tradeDayLabel(t.ts),
      ts: t.ts,
      stock_code: i.stock_code,
      trade_type: i.trade_type,
      price: i.price,
      quantity: i.quantity,
      amount: i.amount,
    }
  })
}

/** 持仓行复合键：策略行为 股票+组合+子策略（同票多子策略各占一行）；
 *  外部行（组合/子策略均为 null 的原有持仓）用股票代码作键。 */
export function positionRowKey(r: Pick<PositionRow, 'stock_code' | 'portfolio_id' | 'strategy_id'>): string {
  if ((r.portfolio_id ?? null) === null && (r.strategy_id ?? null) === null) {
    return `ext|${r.stock_code}`
  }
  return `${r.stock_code}|${r.portfolio_id ?? null}|${r.strategy_id ?? null}`
}

/** SSE position 事件 → 持仓行（按 股票+组合+子策略 复合键原地 upsert，不重复）。
 *  清仓（quantity<=0）→ 移除该行（与历史口径 quantity!=0 一致，不留幽灵持仓行）。
 *  payload 无 source（SSE 仅推策略持仓）→ 默认 strategy。 */
export function upsertPositionRows(rows: PositionRow[], ev: Ev): PositionRow[] {
  const row: PositionRow = {
    stock_code: (ev.stock_code as string) || '',
    quantity: (ev.quantity as number) ?? 0,
    avg_cost: (ev.avg_cost as number) ?? 0,
    market_value: (ev.market_value as number) ?? 0,
    portfolio_id: (ev.portfolio_id as number) ?? null,
    strategy_id: (ev.strategy_id as number) ?? null,
    source: (ev.source as string) || 'strategy',
    attribution: (ev.attribution as PositionRow['attribution']) || undefined,
    out_of_pool: ev.out_of_pool === true ? true : undefined,
  }
  const idx = rows.findIndex((r) => positionRowKey(r) === positionRowKey(row))
  // SSE position 事件不携带归属 → 保留已有行的标注，避免徽标闪没
  if (!row.attribution && idx >= 0) row.attribution = rows[idx].attribution
  if (!row.out_of_pool && idx >= 0) row.out_of_pool = rows[idx].out_of_pool
  if (row.quantity <= 0) {
    if (idx < 0) return rows
    const next = [...rows]
    next.splice(idx, 1)
    return next
  }
  if (idx >= 0) {
    const next = [...rows]
    next[idx] = row
    return next
  }
  return [...rows, row]
}

/** 持仓历史 → 行（归属 id 缺省为 null；source 缺省 strategy，外部行透传 external）。 */
export function positionHistoryToRows(items: LivePositionItem[]): PositionRow[] {
  return items.map((i) => ({
    stock_code: i.stock_code,
    quantity: i.quantity,
    avg_cost: i.avg_cost,
    market_value: i.market_value,
    portfolio_id: i.portfolio_id ?? null,
    strategy_id: i.strategy_id ?? null,
    source: i.source || 'strategy',
    attribution: i.attribution,
    out_of_pool: i.out_of_pool === true ? true : undefined,
    highest_price: i.highest_price,
    peak_bar_time: i.peak_bar_time,
    peak_source: i.peak_source,
    peak_source_label: i.peak_source_label,
  }))
}

/** SSE decision 事件 → 决策闸门行（key 正自增）。 */
export function decisionEventToRow(ev: Ev, liveTime = ''): DecisionRow {
  const t = parseBeijingTime(ev.bar_time as string | undefined)
  return {
    key: nextEventId(),
    time: liveTime || t.text,
    ts: liveTime ? Date.now() : t.ts,
    gate: (ev.gate as string) || '',
    layer: (ev.layer as string) || '',
    action: (ev.action as string) || '',
    stock_code: (ev.stock_code as string) || '',
    strategy_id: (ev.strategy_id as number) ?? null,
    param_name: (ev.param_name as string) ?? null,
    param_value: (ev.param_value as number) ?? null,
    actual_value: (ev.actual_value as number) ?? null,
    requested_qty: (ev.requested_qty as number) ?? null,
    final_qty: (ev.final_qty as number) ?? null,
    message: (ev.message as string) || '',
  }
}

/**
 * 决策行去重签名：与后端 `live_decision_events` 落库字段同口径
 * （gate / layer / action / 代码 / 策略 / message 前 200 字）。
 *
 * 后端把同一逻辑事件的两种投影都落库：`_rec_live` → layer=live_gate，
 * `_emit_risk_safe(persist=True)` → layer=risk，所以 layer 必须进签名，
 * 否则实时去重后与整页刷新拉到的历史行数对不上。message 截 200 字是因为
 * 后端落库 `(ev.message or "")[:200]`，SSE payload 是全文。
 */
export function decisionRowSignature(
  r: Pick<DecisionRow, 'gate' | 'layer' | 'action' | 'stock_code' | 'strategy_id' | 'message'>,
): string {
  return [
    r.gate, r.layer, r.action, r.stock_code,
    r.strategy_id == null ? '' : String(r.strategy_id),
    (r.message || '').slice(0, 200),
  ].join('|')
}

/**
 * SSE risk 事件 → 决策行（后端持久化风控事件：现金差异 / 缺价 / 净值偏差 /
 * 日线新鲜度 / 拆单队列丢失…）。字段映射对齐 `_persist_risk_decision`：
 * gate 取 payload.gate 否则 rule，layer 恒 risk，action 缺省 notice，
 * param_name 取 payload.param_name 否则 rule。
 */
export function riskEventToRow(ev: Ev, liveTime = ''): DecisionRow {
  const t = parseBeijingTime(ev.bar_time as string | undefined)
  const rule = (ev.rule as string) || ''
  return {
    key: nextEventId(),
    time: liveTime || t.text,
    ts: liveTime ? Date.now() : t.ts,
    gate: (ev.gate as string) || rule,
    layer: 'risk',
    action: (ev.action as string) || 'notice',
    stock_code: (ev.stock_code as string) || '',
    strategy_id: (ev.strategy_id as number) ?? null,
    param_name: (ev.param_name as string) ?? (rule || null),
    param_value: (ev.param_value as number) ?? null,
    actual_value: (ev.actual_value as number) ?? null,
    requested_qty: (ev.requested_qty as number) ?? null,
    final_qty: (ev.final_qty as number) ?? null,
    message: (ev.message as string) || '',
  }
}

/**
 * 决策表 upsert：同一签名（见 `decisionRowSignature`）已存在则不新增，
 * 只把既有行的时间推进（SSE 重复推送 / 与历史行撞车时不会出现双行）。
 * 未命中则头部插入并封顶。
 */
export function upsertDecisionRow(rows: DecisionRow[], incoming: DecisionRow, cap = 500): DecisionRow[] {
  const sig = decisionRowSignature(incoming)
  const idx = rows.findIndex((r) => decisionRowSignature(r) === sig)
  if (idx < 0) return prependCapped(rows, incoming, cap)
  const prev = rows[idx]
  // 时间只前进不后退（历史行 created_at 可能晚于 SSE 到达的本地时刻）
  if (incoming.ts != null && (prev.ts == null || incoming.ts > prev.ts)) {
    const next = [...rows]
    next[idx] = { ...prev, time: incoming.time || prev.time, ts: incoming.ts }
    return next
  }
  return rows
}

/** 决策历史 → 行（key=-id，稳定）；历史按 bar_time 升序返回，前端展示倒序（最新在上）。

 * 时间列用 created_at（事件落库时刻）——日线事件 bar_time 恒 00:00:00，无法区分
 * 何时触发（2026-11-03 止损 defer 57 次无法排重问题）；bar_time 兜底。 */
export function decisionHistoryToRows(items: DecisionEventItem[]): DecisionRow[] {
  return items.map((i) => {
    const ct = parseBeijingTime(i.created_at || undefined)
    const bt = parseBeijingTime(i.bar_time || undefined)
    return {
      key: -(i.id ?? 0),
      id: i.id ?? null,
      time: ct.text || bt.text,
      ts: ct.ts ?? bt.ts,
      gate: i.gate,
      layer: i.layer,
      action: i.action,
      stock_code: i.stock_code || '',
      strategy_id: i.strategy_id ?? null,
      param_name: i.param_name ?? null,
      param_value: i.param_value ?? null,
      actual_value: i.actual_value ?? null,
      requested_qty: i.requested_qty ?? null,
      final_qty: i.final_qty ?? null,
      message: i.message || '',
      ack_status: i.ack_status ?? 'open',
      ack_note: i.ack_note ?? null,
    }
  }).reverse()
}

/** 头部插入 + 封顶（订单/成交/决策表）。 */
export function prependCapped<T>(list: T[], row: T, cap = 200): T[] {
  return [row, ...list].slice(0, cap)
}

/** SSE order 事件 → 委托表（按 order_id 原地 upsert，同一笔委托多状态只占一行）。
 *
 * 后端对同一笔 live_order 依次推 submitted → partial/filled → canceled/rejected，
 * 每次 payload 都带 order_id。原实现每条事件新建一行，导致一笔委托出现 2~4 行、
 * 且旧行状态永久停留在「已提交」。历史行（key=-id）与 SSE 行共用 order_id，
 * 因此命中历史行时也能原地推进状态。
 */
export function upsertOrderRow(
  rows: OrderRow[], ev: Ev, liveTime = '',
): OrderRow[] {
  const oid = (ev.order_id as number) ?? null
  if (oid === null) return prependCapped(rows, orderEventToRow(ev, liveTime))
  const idx = rows.findIndex((r) => r.order_id === oid || r.key === -oid)
  if (idx < 0) return prependCapped(rows, orderEventToRow(ev, liveTime))
  const prev = rows[idx]
  const incoming = orderEventToRow(ev, liveTime)
  const next = [...rows]
  next[idx] = {
    ...prev,
    time: incoming.time || prev.time,
    ts: incoming.ts ?? prev.ts,
    status: incoming.status,
    // 成交回报事件不带 price（只有 filled_price）→ 保留原委托价
    price: (ev.price as number) ?? prev.price,
    quantity: incoming.quantity || prev.quantity,
    filled_quantity: incoming.filled_quantity ?? prev.filled_quantity,
    filled_price: incoming.filled_price ?? prev.filled_price,
    error_message: incoming.error_message ?? prev.error_message,
  }
  return next
}

// ---------------------------------------------------------------------------
// 实盘并发约束：C1 引擎类型独占 / C2 账户独占 / C3 键鼠互斥 / C4 多账户提醒
// + 资金基准重置窗口（文档《实盘资金基准对齐-修复方案》§3.2 / §3.3 / §7）
// ---------------------------------------------------------------------------

/** 占用中的会话状态：stopping 仍占用引擎/账户，只有 stopped 才释放。 */
export function isSessionActive(s: { status?: string }): boolean {
  return s.status === 'running' || s.status === 'stopping'
}

/** 生效引擎：老数据 engine 为空 → 按前端默认 iquant 解析（与 engineLabel 一致）。 */
export function resolvedSessionEngine(s: LiveSessionItem): string {
  return s.engine || 'iquant'
}

export interface SessionOccupancy {
  session_id: number
  name: string
  engine: string
  account_key: string | null
  since: string | null
  status: string
}

function toOccupancy(s: LiveSessionItem): SessionOccupancy {
  return {
    session_id: s.id,
    name: s.name,
    engine: resolvedSessionEngine(s),
    account_key: s.account_key ?? null,
    since: s.occupied_since ?? s.started_at ?? null,
    status: s.status,
  }
}

/** 被占用的引擎集合（engine key → 占用方）；同引擎后到者覆盖前者（正常数据不会出现两个）。 */
export function engineOccupants(sessions: LiveSessionItem[]): Record<string, SessionOccupancy> {
  const map: Record<string, SessionOccupancy> = {}
  for (const s of sessions) {
    if (!isSessionActive(s)) continue
    map[resolvedSessionEngine(s)] = toOccupancy(s)
  }
  return map
}

const ENGINE_TEXT: Record<string, string> = {
  tdx: '通达信', ths: '同花顺', iquant: 'QMT', goldminer: '掘金量化',
}

/** 占用起始时间 → 北京 HH:MM（缺省空串）。 */
function beijingHm(iso?: string | null): string {
  const t = parseBeijingTime(iso).text
  return t ? t.slice(0, 5) : ''
}

/** 引擎下拉禁用文案（文档 §7）：`tdx（被 会话 3「TDX 主策略」占用，09:15 起）`。 */
export function engineOccupiedText(engineText: string, occ: SessionOccupancy): string {
  const since = beijingHm(occ.since)
  return `${engineText}（被 会话 ${occ.session_id}「${occ.name}」占用${since ? `，${since} 起` : ''}）`
}

/** 账户键压缩展示（文档 §7 示例）：`tdx:410038…` / `gm:0e5343…`（冒号后保留 6 位）。 */
export function shortAccountKey(key: string): string {
  const i = key.indexOf(':')
  if (i > 0 && key.length > i + 7) return `${key.slice(0, i + 1)}${key.slice(i + 1, i + 7)}…`
  return key.length > 10 ? `${key.slice(0, 10)}…` : key
}

/**
 * C4 多账户并行横幅：running 会话 ≥2 且账户键去重 ≥2 才提示（不阻断）。
 * 账户键缺省（旧后端未返回 account_key）→ 按 engine:<生效引擎> 降级。
 */
export function multiAccountBannerText(sessions: LiveSessionItem[]): string | null {
  const running = sessions.filter((s) => s.status === 'running')
  if (running.length < 2) return null
  const keys = Array.from(new Set(
    running.map((s) => s.account_key || `engine:${resolvedSessionEngine(s)}`),
  ))
  if (keys.length < 2) return null
  return `当前同时运行 ${running.length} 个实盘会话（${keys.map(shortAccountKey).join(' / ')}）。`
    + '多账户同时下单建议分设备操作，下单前先确认当前窗口对应的账户。'
}

/**
 * C3 键鼠冲突对手：目标为 tdx/ths 时，返回另一类键鼠引擎的 running/stopping 会话。
 * 非键鼠引擎（iquant/goldminer）恒返回空数组。
 */
export function keyboardConflictOpponents(
  sessions: LiveSessionItem[], targetEngine: string,
): SessionOccupancy[] {
  const other = targetEngine === 'tdx' ? 'ths' : targetEngine === 'ths' ? 'tdx' : null
  if (!other) return []
  return sessions
    .filter((s) => isSessionActive(s) && resolvedSessionEngine(s) === other)
    .map(toOccupancy)
}

export interface StartConflict {
  rule: string
  message: string
}

/**
 * 结构化解析 start 的 409：按 data.rule 给不同话术（文档 §7 指名文案）。
 * 后端 message 已指名占用者时直接采用；缺失时按 rule + occupied_by 兜底拼接。
 */
export function parseStartConflict(err: unknown): StartConflict {
  const e = err as { response?: { data?: any }; message?: string } | null
  const body = e?.response?.data
  const data = body && typeof body === 'object' ? (body.data ?? {}) : {}
  const rule = typeof data?.rule === 'string' ? data.rule : ''
  const backendMsg = body && typeof body.message === 'string' ? body.message.trim() : ''
  // 参数漂移需按字段中文名重排（后端 message 只列英文 key），故跳过直取
  if (backendMsg && rule !== 'params_drift') return { rule, message: backendMsg }
  const occ = data?.occupied_by ?? null
  const who = occ ? `会话 ${occ.session_id}「${occ.name}」` : '另一会话'
  const engineText = ENGINE_TEXT[occ?.engine] || occ?.engine || '该'
  switch (rule) {
    case 'engine_exclusive':
      return {
        rule,
        message: `引擎独占：${who}正在使用${engineText}通道（engine=${occ?.engine ?? '—'}），请先停止后再启动`,
      }
    case 'account_exclusive':
      return {
        rule,
        message: occ?.status === 'stopping'
          ? `账户独占：${who}正在停止中（同一实盘账户 ${occ?.account_key ?? '—'}），请稍候再启动`
          : `账户独占：${who}正在使用同一实盘账户（${occ?.account_key ?? '—'}），请先停止后再启动`,
      }
    case 'keyboard_exclusive':
      return {
        rule,
        message: `键鼠冲突：${who}与当前会话同为键鼠 UI 通道，不建议同时启动`,
      }
    case 'params_drift': {
      // tq_trade.params_drift_block=true：组合参数与回测快照不一致 → 409 阻断启动
      const recId = data?.backtest_record_id ?? '—'
      const fields = Array.isArray(data?.fields)
        ? data.fields.map((f: any) => paramFieldLabel(String(f?.field ?? ''))).filter(Boolean)
        : []
      return {
        rule,
        message: `参数漂移阻断：当前组合参数与回测快照 #${recId} 不一致`
          + (fields.length > 0 ? `（${fields.join('、')}）` : '')
          + '，请先对齐参数后重试（tq_trade.params_drift_block=true）',
      }
    }
    default:
      return { rule, message: e?.message?.trim() || '启动失败' }
  }
}

export const BASELINE_RESET_BLOCKED_REASON = '交易时段禁止重置，请 15:00 后操作'

export interface ResetWindow {
  allowed: boolean
  reason: string
}

const BJ_PARTS = new Intl.DateTimeFormat('en-US', {
  timeZone: 'Asia/Shanghai', weekday: 'short',
  hour: '2-digit', minute: '2-digit', hourCycle: 'h23',
})

/**
 * 资金基准重置窗口：交易日收盘后（>= 15:00）或非交易日（周末）任意时刻可重置；
 * 其余时间禁用并给出原因。半日市/临时休市由后端 403 最终裁决。
 */
export function baselineResetAllowed(nowTs: number = Date.now()): ResetWindow {
  const parts = BJ_PARTS.formatToParts(new Date(nowTs))
  const v = (t: string) => parts.find((p) => p.type === t)?.value ?? ''
  const weekday = v('weekday')
  if (weekday === 'Sat' || weekday === 'Sun') return { allowed: true, reason: '' }
  const hm = `${v('hour')}:${v('minute')}`
  if (hm >= '15:00') return { allowed: true, reason: '' }
  return { allowed: false, reason: BASELINE_RESET_BLOCKED_REASON }
}

/** 金额展示：千分位 + 2 位小数（超限提示与基准展示）。 */
export function formatMoney2(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return '—'
  return Number(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

// ===== 参数漂移（H2 / A7）=====
// 后端 compute_params_drift 比对的是 PARAMS_SNAPSHOT_FIELDS（回测快照冻结的组合参数），
// 下面标签与之 1:1 对齐；出现未知字段时回退英文 key（不静默丢弃）。
export const PARAM_FIELD_LABEL: Record<string, string> = {
  benchmark_index: '基准指数',
  initial_capital: '初始资金',
  max_drawdown: '最大回撤',
  daily_loss_limit: '日亏损限',
  max_holdings: '最大持仓数',
  min_commission: '最低佣金',
  buy_commission_rate: '买佣金率',
  sell_commission_rate: '卖佣金率',
  stamp_duty_rate: '印花税率',
  transfer_fee_rate: '过户费率',
  slippage: '滑点',
  trading_session: '交易时段',
  stock_pool_id: '股票池',
  position_sizing: '仓位模型',
  price_limit_check_enabled: '涨跌停校验',
  volume_limit_enabled: '成交量约束',
  volume_limit_ratio: '成交量占比上限',
  adaptive_slippage_enabled: '自适应滑点',
  slippage_impact_coef: '滑点冲击系数',
  cash_enhance_enabled: '现金管理增强',
  cash_enhance_equity_threshold: '净资产阈值',
  cash_enhance_position_ratio_threshold: '仓位占比阈值',
  equity_cap: '容量上限',
  risk_cooldown_days: '风控冷静期',
}

const PARAM_RATIO_FIELDS = new Set([
  'max_drawdown', 'daily_loss_limit',
  'buy_commission_rate', 'sell_commission_rate', 'stamp_duty_rate',
  'transfer_fee_rate', 'slippage', 'volume_limit_ratio', 'slippage_impact_coef',
  'cash_enhance_position_ratio_threshold',
])

export function paramFieldLabel(field: string): string {
  return PARAM_FIELD_LABEL[field] || field
}

/** 快照值/当前值展示：比例字段转百分比，其余原样（空值显示「空」）。 */
export function formatParamValue(field: string, raw: unknown): string {
  if (raw === null || raw === undefined || raw === '') return '空'
  const n = Number(raw)
  if (PARAM_RATIO_FIELDS.has(field) && Number.isFinite(n)) {
    return `${Number((n * 100).toFixed(4))}%`
  }
  return String(raw)
}

export interface ParamsDriftLike {
  drifted: boolean
  checked: boolean
  block_enabled?: boolean
  fields?: { field: string; snapshot: string | null; current: string | null }[]
  reason?: string | null
}

/**
 * 漂移差异行：「最大回撤：20% → 25%」。drifted=false 或未检查时返回空数组。
 */
export function paramsDriftLines(d: ParamsDriftLike | null | undefined): string[] {
  if (!d || !d.drifted || !Array.isArray(d.fields)) return []
  return d.fields.map((f) => {
    const label = paramFieldLabel(f.field)
    return `${label}：${formatParamValue(f.field, f.snapshot)} → ${formatParamValue(f.field, f.current)}`
  })
}

