<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { formatEvent, nextEventId, EVENT_TYPE_COLOR, tradeTypeLabel, ORDER_STATUS_LABEL, gateLabel, LAYER_LABEL, ACTION_LABEL, channelStateLabel, channelBadgeState, RECOVERY_EVENT_TYPES, type LiveEvent } from '../utils/liveEvents'
import {
  orderHistoryToRows, upsertOrderRow,
  tradeEventToRow, tradeHistoryToRows,
  upsertPositionRows, positionHistoryToRows, prependCapped,
  decisionEventToRow, decisionHistoryToRows, riskEventToRow, upsertDecisionRow,
  filterRowsByRange, summarizeDecisionRows, beijingNowHms, TIME_RANGE_OPTIONS,
  beijingDayKey, rangeStartTs, parseBeijingTime,
  positionRowKey,
  isSessionActive, resolvedSessionEngine, engineOccupants, engineOccupiedText,
  multiAccountBannerText, keyboardConflictOpponents, parseStartConflict,
  baselineResetAllowed, formatMoney2, shortAccountKey, paramsDriftLines,
  type OrderRow, type TradeRow, type PositionRow, type DecisionRow, type TimeRange,
  type SessionOccupancy,
} from '../utils/liveWorkbench'
import {
  getLiveOrders, getLiveTrades, getLivePositions, getLiveDecisions, getLiveSignals, getPortfolios,
  ackLiveDecision, ackAllLiveDecisions,
  getLiveSessions, getLiveSession, createLiveSession, startLiveSession, stopLiveSession, deleteLiveSession,
  recoverLiveBreaker, getLiveChannel, testLiveChannel, sendUserBlock, getFormulas, getWatchdogStatus,
  getLiveAccountPreview, resetLiveBaseline,
  getLiveParamsDrift, getLiveCashFlows, createLiveCashFlow,
  type PortfolioItem, type LiveSessionItem,
  type LiveChannelInfo, type LiveChannelEngine, type LiveEngineKey, type LiveExecutionMode, type LiveSignalItem,
  type FormulaItem, type WatchdogAlarm, type ParamsDrift, type LiveCashFlowList, type LiveCashFlowType,
} from '../api'
import PageHeader from '../components/PageHeader.vue'
import { deriveBlockCode } from '../utils/blockCode'

const sessions = ref<LiveSessionItem[]>([])
const loading = ref(true)
// 当前生效的下单通道（全局配置 ths_ui.enabled 决定，切换需重启运行中 session）
const channel = ref<LiveChannelInfo | null>(null)
const channelTesting = ref(false)
const channelTestMsg = ref<{ ok: boolean; text: string } | null>(null)
const portfolios = ref<PortfolioItem[]>([])  // 全量组合策略,供新建实盘多选 + 会话列表解析名称
const formulas = ref<FormulaItem[]>([])  // 公式列表（formula_id → name 映射，用于组合策略标注公式名）
const showCreate = ref(false)
// M2 实盘配置指南弹窗
const showGuide = ref(false)
// 默认与后端 create_session 对齐：新会话默认 alert（半自动预警，仅 TQ 行情+send_warn，不下单）
// 组合策略多选时标注公式名称——外部信号注入需按公式名建立一一对应关系
function portfolioFormulas(pid: number): string {
  const p = portfolios.value.find(x => x.id === pid)
  if (!p || !(p as any).strategies) return ''
  const names = ((p as any).strategies as { formula_id?: number }[])
    .map((s) => {
      const f = formulas.value.find(f => f.id === s.formula_id)
      return f ? f.name : null
    })
    .filter(Boolean)
  return [...new Set(names)].join(' + ')
}

const form = ref({
  name: '', mode: 'simulation',
  engine: 'tdx' as LiveEngineKey,
  execution_mode: 'alert' as LiveExecutionMode,
  portfolio_ids: [] as number[],
  // 实盘资金基准（元）：null/空=以真实通道总资产为基准（只缩不放）；number=固定上限
  live_capital: null as number | string | null,
  // 外部信号注入：通达信条件预警输出的信号文件路径（空=走 TQ 公式计算）
  signal_file_path: '' as string,
  // 14:50 提前收盘价（B，可选增强，仅 1d）：默认关 → 1d/1w/1mon 都在 15:05
  // 收盘落定后用完整 bar 算信号、次日开盘执行；勾选后仅 1d 提前到 14:50 定信号、
  // 14:52 起始 1m bar 开盘价报单、当日尾盘成交（1w/1mon 不受影响）
  advance_close_enabled: false as boolean,
  // 提前收盘价轮次时点 HH:MM（默认 14:51，限定 14:51–14:59；委托价需要 14:52
  // 起始 1m bar 的开盘价，故实际轮次不早于 14:52）
  advance_close_time: '14:51' as string,
  // 错周期风控三态：null=跟随全局（tq_trade.intraday_mismatched_risk_enabled，默认
  // False=对齐回测：错周期 bar 只更新价格快照，不评估止损/止盈/移动止损）/
  // true=强制开启（任意 bar 都跑风控）/ false=强制关闭
  intraday_mismatched_risk_enabled: null as boolean | null,
})

/** live_capital 归一化：空串/未填 → null（以真实总资产为基准），否则转数值透传。 */
function normalizedLiveCapital(): number | null {
  const v = form.value.live_capital
  if (v === null || v === undefined || v === '') return null
  return Number(v)
}

// ---- 资金基准 / 并发约束（文档《实盘资金基准对齐-修复方案》§3.2 / §3.3 / §7）----
// 新建弹窗的真实总资产预览（GET /live/account-preview；通道未就绪 → 仅提示不拦保存）
const accountPreview = ref<{ total_asset: number | null; available: number | null; captured_at: string | null } | null>(null)
const accountPreviewError = ref('')
// 新建弹窗内联校验错误（如基准超上限），不静默 alert
const createError = ref('')
// start 409 的行内红字（按 data.rule 给指名占用者的不同话术）
const startErrors = ref<Record<number, string>>({})
// C3 键鼠冲突二次确认（tdx ↔ ths 同时启动）
const keyboardRisk = ref<{ session: LiveSessionItem; opponents: SessionOccupancy[] } | null>(null)
const ackKeyboardRisk = ref(false)
// 资金基准重置弹窗（收盘后可用；留空 = 当日收盘总资产，也可再次手动填写更小值）
const baselineModal = ref<{ session: LiveSessionItem } | null>(null)
const baselineValue = ref<number | string>('')
const baselineError = ref('')
// C4 多账户并行横幅（可关闭，不阻断任何操作）
const bannerDismissed = ref(false)
// 重置窗口判定的时钟（随 15s 轮询刷新，跨过 15:00 后按钮自动解禁）
const nowTick = ref(Date.now())

// ---- H2/A7 参数漂移（绑定回测记录时：当前组合参数 vs params_snapshot）----
// 后端 `GET /sessions/{id}/params-drift`；drifted=true 才出横幅，block_enabled 时标注会拦启动
const paramsDrifts = ref<Record<number, ParamsDrift>>({})
const driftBanners = computed(() =>
  Object.entries(paramsDrifts.value)
    .map(([sid, d]) => ({
      sessionId: Number(sid),
      name: sessions.value.find((s) => s.id === Number(sid))?.name ?? `#${sid}`,
      blockEnabled: !!d.block_enabled,
      lines: paramsDriftLines(d),
    }))
    // 只有真漂移才出横幅（未绑定回测/未完成比对不打扰）
    .filter((b) => b.lines.length > 0),
)
// start 返回的通道建议告警（按会话展示，非阻断）
const startWarnings = ref<Record<number, string[]>>({})
// 新建会话返回的占用类软告警（A1，非阻断）
const createWarnings = ref<string[]>([])

// ---- 外部现金流台账（入金/出金/分红/其他）----
// 不记账 → 净值/收益率/回撤/日亏被外部资金扭曲（后端按「总市值 − 累计净流入」计风险指标）
const cashFlowModal = ref<{ session: LiveSessionItem } | null>(null)
const cashFlows = ref<LiveCashFlowList | null>(null)
const cashFlowError = ref('')
const cashFlowSaving = ref(false)
const CASH_FLOW_TYPES: { value: LiveCashFlowType; label: string }[] = [
  { value: 'deposit', label: '入金' },
  { value: 'withdraw', label: '出金' },
  { value: 'dividend', label: '分红' },
  { value: 'other', label: '其他' },
]
const cashFlowForm = ref({
  portfolio_id: null as number | null,
  flow_type: 'deposit' as LiveCashFlowType,
  amount: '' as number | string,
  flow_date: '' as string,
  note: '' as string,
})
const CASH_FLOW_TYPE_LABEL: Record<string, string> = {
  deposit: '入金', withdraw: '出金', dividend: '分红', other: '其他',
}
/** 台账表格「方向」列：deposit/dividend/正数 other = 流入，withdraw/负数 other = 流出。 */
function cashFlowDirection(f: { flow_type: string; amount: string | number }): string {
  const n = Number(f.amount)
  if (f.flow_type === 'withdraw') return '流出'
  if (n < 0) return '流出'
  if (n === 0) return '—'
  return '流入'
}

// C1：当前被占用的引擎（running/stopping）→ 建会话下拉置灰 + 显示占用方
const occupants = computed(() => engineOccupants(sessions.value))
// C4：多账户并行横幅文案（<2 个 running 或同账户 → null）
const multiAccountBanner = computed(() => multiAccountBannerText(sessions.value))
// 重置窗口：交易日 15:00 前禁用（后端 403 兜底半日市/临时休市）
const resetWindow = computed(() => baselineResetAllowed(nowTick.value))
// 被占用的引擎清单（新建弹窗引导「先停止该会话」）
const occupiedEngineList = computed(() =>
  (channel.value?.engines || [])
    .filter((e) => occupants.value[e.key])
    .map((e) => ({ engine: e, occ: occupants.value[e.key] as SessionOccupancy })),
)

function engineOptionText(e: LiveChannelEngine): string {
  const occ = occupants.value[e.key]
  // 文档 §3.2.3.1 / §7 文案：`tdx（被 会话 3「TDX 主策略」占用，09:15 起）`
  if (occ) return engineOccupiedText(e.key, occ)
  return `${e.label}${e.configured ? '' : '（未配置）'}`
}
function engineOptionDisabled(e: LiveChannelEngine): boolean {
  return !e.configured || !!occupants.value[e.key]
}
/** 键鼠冲突 modal 的对方会话信息：引擎 / 账户键 / 占用起始时间。 */
function opponentAccountText(o: SessionOccupancy): string {
  return o.account_key ? shortAccountKey(o.account_key) : `engine:${o.engine}`
}
function occupiedSinceText(o: SessionOccupancy): string {
  const t = parseBeijingTime(o.since).text
  return t ? `${t.slice(0, 5)} 起` : '时间未知'
}

/** 会话的组合策略 id 列表（portfolios 有状态时优先，缺省回退 portfolio_ids）。 */
function sessionPortfolioIds(s: LiveSessionItem): number[] {
  const ids = (s.portfolios || []).map((p) => p.portfolio_id)
  return ids.length > 0 ? ids : (s.portfolio_ids || [])
}

async function loadAccountPreview(engine: LiveEngineKey) {
  accountPreview.value = null
  accountPreviewError.value = ''
  try {
    accountPreview.value = await getLiveAccountPreview(engine)
  } catch (e: any) {
    accountPreviewError.value = e?.response?.data?.message || e?.message || '通道未就绪'
  }
}
function onEngineChange() {
  createError.value = ''
  loadAccountPreview(form.value.engine)
}
// 引擎中文名（列表与弹窗共用；后端 engines 清单里有 label 时优先）
const ENGINE_LABEL: Record<string, string> = {
  goldminer: '掘金量化', iquant: 'QMT 通道', tdx: '通达信', ths: '同花顺',
}
function engineLabel(key?: string): string {
  if (!key) return ENGINE_LABEL.iquant
  return channel.value?.engines?.find((e) => e.key === key)?.label || ENGINE_LABEL[key] || key
}

// 执行模式中文名（alert=预警/auto=自动）；老数据缺省视作 auto
const EXECUTION_MODE_LABEL: Record<string, string> = { alert: '预警', auto: '自动' }
function executionModeLabel(m?: string): string {
  return EXECUTION_MODE_LABEL[m || 'auto'] || m || '自动'
}
function isAlert(s: LiveSessionItem): boolean {
  return (s.execution_mode || 'auto') === 'alert'
}

// 通道条前缀：有运行中会话=运行中通道（跟随该会话引擎）；否则=默认引擎（default_engine）
const runningEngine = ref<LiveEngineKey | null>(null)
const channelLabelPrefix = computed(() => (runningEngine.value ? '运行中通道' : '默认引擎'))
// 引擎清单去掉默认引擎（首徽章已展示，避免重复渲染两个相同徽章）
const otherEngines = computed(
  () => (channel.value?.engines || []).filter((e) => e.key !== channel.value?.engine),
)
const channelBadgeTitle =
  '默认引擎：engine 为空的历史会话/旧 qmt(miniQMT) 会话回退使用；可在 系统配置 → 通达信 TQ 通道 → 默认交易引擎 修改'
// 新建实盘默认引擎：跟随后端 default_engine（SystemConfig 可配，当前默认 tdx）
function defaultEngineKey(): LiveEngineKey {
  return (channel.value?.engine as LiveEngineKey) || 'tdx'
}

// M1 多会话并行：B6 全局限 1 已移除（后端改 THS 通道互斥），启动按钮恒可点，
// 多会话冲突由后端 409 兜底（同花顺通道键鼠独占）。

// 组合级状态列表：优先用后端 portfolios 字段，缺省时兜底 portfolio_ids(无状态视为 active)
function portfolioStatuses(s: LiveSessionItem): { portfolio_id: number; status: string }[] {
  if (s.portfolios && s.portfolios.length > 0) return s.portfolios
  return (s.portfolio_ids || []).map((id) => ({ portfolio_id: id, status: 'active' }))
}

function portfolioName(id: number): string {
  return portfolios.value.find((p) => p.id === id)?.name || `#${id}`
}

/** 子策略名：从已加载的 portfolios 嵌套 strategies 里解析；查不到兜底 #id。 */
function strategyName(pid: number | null, sid: number | null): string {
  if (sid === null || sid === undefined) return '—'
  const p = portfolios.value.find((x) => x.id === pid)
  const s = p?.strategies.find((y) => y.id === sid)
  return s?.name || `#${sid}`
}

async function recoverBreaker(s: LiveSessionItem, pid: number) {
  if (!confirm(`确定手动恢复组合「${portfolioName(pid)}」的熔断？将清零熔断计数并恢复开仓。`)) return
  try {
    await recoverLiveBreaker(s.id, pid)
    await load()
  } catch (e: any) {
    alert(e?.message || '恢复失败')
  }
}

// ---- B4a: 实时事件日志面板（SSE，多会话并行订阅）----
type EventRow = LiveEvent & { sessionId?: number; sessionLabel?: string }
const events = ref<EventRow[]>([])
const connState = ref<'closed' | 'connecting' | 'open' | 'error'>('closed')
const connLabel: Record<string, string> = {
  closed: '未连接', connecting: '连接中', open: '已连接', error: '连接失败',
}
// 多会话并行：每个运行中会话一条 SSE（事件按来源流标会话前缀）
const esMap = new Map<number, EventSource>()
// 工作台聚焦的会话（持仓/委托/成交跟随它；事件日志不受限）
let activeSessionId: number | null = null
const EVENT_TYPES = ['signal', 'order', 'trade', 'position', 'risk', 'decision'] as const
const LOG_CAP = 200

// ---- B4b: 工作台（持仓/委托/成交/决策拦截/预警信号）----
const positions = ref<PositionRow[]>([])
// 池外仓单列（②「池外（不参与业绩）」）：out_of_pool=true 的柜台外部仓只进展示/对账，
// 不进虚拟现金、不进净值、不进回撤分母——与参与业绩的策略持仓分表渲染，避免市值被
// 误读为组合净值的一部分。
const poolPositions = computed(() => positions.value.filter((p) => p.out_of_pool !== true))
const outOfPoolPositions = computed(() => positions.value.filter((p) => p.out_of_pool === true))
const orders = ref<OrderRow[]>([])
const trades = ref<TradeRow[]>([])
const decisions = ref<DecisionRow[]>([])
// 时间区间筛选（默认今日；聚合统计随筛选区间重算，不再显示全时段累计）
const orderRange = ref<TimeRange>('today')
const decRange = ref<TimeRange>('today')
const filteredOrders = computed(() => filterRowsByRange(orders.value, orderRange.value))
// 成交默认只看今日（前一交易日的成交仍在库里、切「全部」可查——历史用于重启后
// 重放重算虚拟持仓/现金，平台不删除任何成交记录）
const tradeRange = ref<TimeRange>('today')
const filteredTrades = computed(() => filterRowsByRange(trades.value, tradeRange.value))
const filteredDecisions = computed(() => filterRowsByRange(decisions.value, decRange.value))
// 成交转化与决策聚合使用同一筛选区间（decRange），不跟随委托表的 orderRange
const decRangeOrders = computed(() => filterRowsByRange(orders.value, decRange.value))
const decisionSummaryView = computed(
  () => summarizeDecisionRows(filteredDecisions.value, decRangeOrders.value))
const signals = ref<LiveSignalItem[]>([])
const signalsTotal = ref(0)
// 账户全貌（真实账户快照）：多会话并行 → 每个运行中会话一张卡（不同账号分列）
const accountSnapshots = ref<Record<number, any>>({})
// 交易日历来源/退化状态（详情接口 trading_calendar；仅聚焦会话拉取，不额外轮询）
const calendarStatus = ref<Record<number, any>>({})
// 真实账户总资产 vs 虚拟净值（详情接口 equity_check；>20% 后端已告警一次）
const equityChecks = ref<Record<number, any>>({})
// 通道在线态（会话详情 bridge_online）：null=未知（未运行/未取到）。15s 轮询，
// 让「客户端退出/通道掉线」在工作台可见，而不是只留在后端日志里（2026-11-13）。
const bridgeOnline = ref<boolean | null>(null)
// watchdog 活跃告警（通道级）：比心跳更早反映掉线（watchdog 30s 一轮 vs 心跳 60–90s）
const watchdogAlarms = ref<WatchdogAlarm[]>([])
// 徽章最终态：通道级告警优先，无告警回退心跳值
const channelBadge = computed(
  () => channelBadgeState(bridgeOnline.value, watchdogAlarms.value, watchedSessionId.value),
)
// 告警项自带 channel（watchdog 回传的通道名）此前未被读取 → 在徽章 title 里点名告警通道
const sessionChannelBadgeTitle = computed(() => {
  const base = '当前聚焦会话的通道状态：watchdog 通道告警优先（30 秒扫描，客户端退出即翻红），无告警回退心跳态；未知=会话未运行或尚未取到'
  const channels = [...new Set(watchdogAlarms.value.map((a) => a.channel).filter(Boolean))]
  return channels.length > 0 ? `${base}。活跃告警通道：${channels.join(' / ')}` : base
})
let bridgeTimer: ReturnType<typeof setInterval> | null = null

/** 单年日历摘要：正常=来源/交易日数/覆盖区间；退化=来源+标记。 */
function calendarYearText(year: string, y: any): string {
  if (!y) return year
  if (y.degraded) return `${year}：${y.source || '退化'}（退化）`
  const days = y.trading_days ?? '—'
  const cov = Array.isArray(y.coverage) && y.coverage.length >= 2
    ? `${y.coverage[0]} ~ ${y.coverage[1]}` : '—'
  return `${year}：${y.source || '—'}，${days} 个交易日，覆盖 ${cov}`
}

/** 多年度按年份升序拼接，最多展示最近 2 年（避免撑破会话卡）。 */
function calendarInfoText(cal: any): string {
  if (!cal || !cal.years) return ''
  const years = Object.keys(cal.years).sort().slice(-2)
  return '日历 ' + years.map((y) => calendarYearText(y, cal.years[y])).join('；')
}

/** 退化徽标 title：逐年列出退化来源。 */
function calendarDegradedTitle(cal: any): string {
  if (!cal || !cal.years) return '交易日历已退化'
  const parts = Object.keys(cal.years).sort()
    .filter((y) => cal.years[y]?.degraded)
    .map((y) => calendarYearText(y, cal.years[y]))
  return parts.length > 0 ? parts.join('；') : '交易日历已退化'
}

/**
 * 账户快照采集提示按引擎区分：仅同花顺（ths）是读 xiadan 下单窗口表格，
 * 需要窗口打开；goldminer / iquant / tdx 都是后端定时拉柜台/桥接口，与窗口无关。
 */
function snapshotHint(engine: string | null | undefined): string {
  if ((engine || '').toLowerCase() === 'ths') {
    return 'THS 通道 60s/次，读表需下单窗口打开'
  }
  return '后台每 60s 采集一次，无需打开下单窗口'
}
type WbTab = 'positions' | 'orders' | 'trades' | 'decisions' | 'signals'
const wbTab = ref<WbTab>('positions')
const TAB_LABEL: Record<WbTab, string> = {
  positions: '持仓', orders: '委托', trades: '成交', decisions: '决策/拦截', signals: '预警信号',
}

// ---- 决策/拦截页签：自带刷新 + 切页签重拉 ----
// 旧版只有挂载/切会话/reconnect/「刷新持仓」按钮会重拉历史，收盘后看当天移动止损与
// 顺延事件常常是空表（页签没数据也没入口）——这里给决策页签独立刷新入口。
const decisionsLoading = ref(false)
/** M4：未处理/已确认计数（后端全量口径）。 */
const decisionAckSummary = ref<{ open: number; acked: number }>({ open: 0, acked: 0 })
/** 正在确认的事件 id（按钮防重）。 */
const ackingEventId = ref<number | null>(null)
/** 顺延待次日提交筛选开关（汇总条点击切换；只筛事件表，不动上方聚合统计）。 */
const decOnlyDeferred = ref(false)
/** 顺延待次日提交（gate=signal_bar_date / action=defer）的笔数（随筛选区间重算）。 */
const deferredNextDayCount = computed(
  () => filteredDecisions.value.filter(
    (d) => d.gate === 'signal_bar_date' && d.action === 'defer').length,
)
/** 事件表实际渲染行：开了顺延筛选只留那一类（聚合表仍看全区间）。 */
const decisionRowsView = computed(() => (decOnlyDeferred.value
  ? filteredDecisions.value.filter((d) => d.gate === 'signal_bar_date' && d.action === 'defer')
  : filteredDecisions.value))

async function reloadDecisions() {
  const id = activeSessionId
  if (id === null) return
  decisionsLoading.value = true
  try {
    const dec = await getLiveDecisions(id).catch(() => null)
    if (activeSessionId !== id) return
    decisions.value = dec ? decisionHistoryToRows(dec.events) : []
    decisionAckSummary.value = dec?.ack_summary || { open: 0, acked: 0 }
  } finally {
    decisionsLoading.value = false
  }
}

/** M4：确认单条事件（附带「人工确认」备注，刷新计数与状态）。 */
async function ackDecisionRow(d: DecisionRow) {
  const sid = activeSessionId
  if (sid === null || !d.id || ackingEventId.value === d.id) return
  ackingEventId.value = d.id
  try {
    await ackLiveDecision(sid, d.id, '人工确认')
    await reloadDecisions()
  } catch {
    /* 失败保持原状，用户可重试 */
  } finally {
    ackingEventId.value = null
  }
}

/** M4：日终清零——把所有未处理事件批量标记为已确认。 */
async function ackAllDayEnd() {
  const sid = activeSessionId
  if (sid === null) return
  if (!window.confirm('将当前所有未处理的决策/拦截事件标记为已确认（日终清零）？')) return
  decisionsLoading.value = true
  try {
    await ackAllLiveDecisions(sid, { day_end: true })
    await reloadDecisions()
  } catch {
    /* 失败保持原状，用户可重试 */
  } finally {
    decisionsLoading.value = false
  }
}

/** 页签切换：切到决策/拦截时重拉一次历史（其它页签维持既有行为）。 */
function setWbTab(tab: WbTab) {
  if (tab === wbTab.value) return
  wbTab.value = tab
  decOnlyDeferred.value = false
  if (tab === 'decisions') void reloadDecisions()
}

// 时间区间变更后顺延筛选可能已空 → 自动收起，避免「打开筛选像没数据」
watch(decRange, () => { decOnlyDeferred.value = false })

// 信号类型 / 预警推送状态中文映射
const SIGNAL_TYPE_LABEL: Record<string, string> = {
  OPEN: '开仓', ADD: '加仓', REDUCE: '减仓', CLOSE: '清仓',
}
function signalTypeLabel(t: string): string {
  return SIGNAL_TYPE_LABEL[t] || t
}
const ALERT_STATUS_LABEL: Record<string, string> = {
  sent: '已发送', failed: '发送失败', skipped: '已跳过',
}
function alertStatusLabel(s: string | null): string {
  if (!s) return '—'
  return ALERT_STATUS_LABEL[s] || s
}
function alertStatusClass(s: string | null): string {
  if (s === 'sent') return 'badge-green'
  if (s === 'failed') return 'badge-red'
  return 'badge-gray'
}
function fmtTime(t: string | null): string {
  return t ? t.replace('T', ' ').slice(0, 19) : '—'
}
// 成本价展示统一 3 位小数（后端计算/存储仍全精度）；
// B5：avg_cost<=0 表示柜台成本为负/未知（或外部导入仓无成本基准），
// 不显示漂移的现价 / 负数，统一标注「成本未知」（该票不参与风控阈值判断）。
function fmtCost(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  if (n <= 0) return '成本未知'
  return n.toFixed(3)
}
// 金额（市值）展示统一 2 位小数（分为精度；后端计算/存储仍全精度）
function fmtMoney(v: number | null | undefined): string {
  if (v === null || v === undefined || !Number.isFinite(Number(v))) return '—'
  return Number(v).toFixed(2)
}

// ---- 1.3.1 预警信号股同步到通达信自选板块（当前信号列表股票代码去重）----
async function syncSignalsBlock() {
  const codes = Array.from(new Set(signals.value.map((s) => s.stock_code).filter(Boolean)))
  if (codes.length === 0) {
    alert('暂无信号股票')
    return
  }
  const name = window.prompt('通达信自选板块名', '创懿预警')
  if (!name) return
  try {
    const r = await sendUserBlock({ block_code: deriveBlockCode(name), block_name: name, stock_codes: codes, mode: 'replace' })
    alert(`已写入 ${r.count} 只到「${r.block_code}」`)
  } catch (e: any) {
    alert(e?.response?.data?.message || e?.message || '同步失败')
  }
}

// 决策闸门阈值/实际：比率（0<|v|<1）转百分比，数量原样。
function fmtGateVal(v: number | null | undefined): string {
  if (v === null || v === undefined) return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return '—'
  if (n !== 0 && Math.abs(n) < 1) return `${(n * 100).toFixed(2)}%`
  return String(Math.round(n * 10000) / 10000)
}
// 成交转化率：0~1 → 百分比；未统计（旧数据）→「—」
function fmtRate(r: number | null | undefined): string {
  if (r === null || r === undefined || !Number.isFinite(Number(r))) return '—'
  return `${(Number(r) * 100).toFixed(1)}%`
}
function qtyPair(req: number | null | undefined, fin: number | null | undefined): string {
  if (req === null || req === undefined) return '—'
  if (fin === null || fin === undefined) return `${req}（全拦）`
  if (Number(fin) === Number(req)) return `${req}`
  if (Number(fin) === 0) return `${req}（全拦）`
  return `${req} → ${fin}`
}
const layerLabel = (l: string) => LAYER_LABEL[l] || l || '—'
const actionLabel = (a: string) => ACTION_LABEL[a] || a || '—'

const nowTime = () => beijingNowHms()

function pushLog(type: string, data: Record<string, unknown>, time: string, sessionLabel = '', sessionId?: number) {
  const text = formatEvent(type, data)
  if (!text) return // ping 心跳不进日志
  events.value.push({
    id: nextEventId(),
    type: type as LiveEvent['type'],
    time,
    text: sessionLabel ? `[${sessionLabel}] ${text}` : text,
    sessionLabel,
    sessionId,
  })
  if (events.value.length > LOG_CAP) events.value = events.value.slice(-LOG_CAP)
}

// 多会话事件过滤：全部 / 按会话（运行中会话 >1 时显示下拉）
const eventFilter = ref<number | 'all'>('all')
const runningSessions = computed(() => sessions.value.filter((s) => s.status === 'running'))
const filteredEvents = computed(() => {
  if (eventFilter.value === 'all') return events.value
  return events.value.filter((e) => e.sessionId === eventFilter.value)
})

// 预警信号节流重拉：SSE signal 事件到达后 2s 窗口内合并为一次列表重拉。
// 不直接把 SSE payload 映射成行——payload 是精简形状（signal_id 而非 id），
// 缺 period/formula_name/trigger_value 等列，重拉才完整。
let signalsRefreshTimer: ReturnType<typeof setTimeout> | null = null
function scheduleSignalsRefresh() {
  if (signalsRefreshTimer !== null || activeSessionId === null) return
  signalsRefreshTimer = setTimeout(async () => {
    signalsRefreshTimer = null
    if (activeSessionId === null) return
    // await 期间用户可能已切到别的会话：响应回来必须丢弃，否则旧会话
    // 信号会写进新会话的预警表（与 loadHistory 的 activeSessionId 校验同理）。
    const id = activeSessionId
    try {
      const sig = await getLiveSignals(id, { limit: 200 })
      if (activeSessionId !== id) return
      signals.value = sig.items
      signalsTotal.value = sig.total
    } catch { /* 会话已删除等 → 忽略，与 loadHistory 兜底一致 */ }
  }, 2000)
}

// loadHistory 与 SSE 竞态计数（audit C1）：五请求在途期间到达的 workbench 事件数。
// 只计 position/order/trade——三类历史 resolve 时会整体覆盖 SSE 已插的行；
// decision/signals 不计（signals 走节流重拉，decision 逐事件流可接受毫秒级窗口）。
let workbenchEventsSinceLoad = 0

function applyToWorkbench(type: string, data: Record<string, unknown>, time: string) {
  if (type === 'position') {
    workbenchEventsSinceLoad += 1
    positions.value = upsertPositionRows(positions.value, data)
  }
  else if (type === 'order') {
    workbenchEventsSinceLoad += 1
    orders.value = upsertOrderRow(orders.value, data, time)
  }
  else if (type === 'trade') {
    workbenchEventsSinceLoad += 1
    trades.value = prependCapped(trades.value, tradeEventToRow(data, time))
  }
  else if (type === 'decision') decisions.value = upsertDecisionRow(decisions.value, decisionEventToRow(data, time))
  // 后端以 type=risk 推送的持久化风控事件（现金差异/缺价/净值偏差/日线新鲜度/拆单队列丢失…）
  // 与 decision 事件落同一张 live_decision_events（layer=risk），因此同样并入决策表；
  // 去重签名与历史行同口径，SSE 重复推送与随后整页刷新都不会出双行。
  else if (type === 'risk') decisions.value = upsertDecisionRow(decisions.value, riskEventToRow(data, time))
  else if (type === 'signal') scheduleSignalsRefresh()
}

/** 成交区间 → 接口日期参数（今日/近N日只传起点；「全部」不传=服务端全量）。 */
function tradeDateParams(): { start_date?: string } {
  const start = rangeStartTs(tradeRange.value)
  return start == null ? {} : { start_date: beijingDayKey(start) }
}

/** 成交页签切区间：按新区间重拉。老后端忽略未知查询参数时，仍由前端
 *  filterRowsByRange 兜底，历史照样能看（不会因后端未重启而丢失入口）。 */
async function reloadTrades() {
  const id = activeSessionId
  if (id === null) return
  try {
    const trd = await getLiveTrades(id, tradeDateParams())
    if (activeSessionId !== id) return
    trades.value = tradeHistoryToRows(trd)
  } catch { /* 保持现状 */ }
}

async function loadHistory(id: number) {
  workbenchEventsSinceLoad = 0
  try {
    const [pos, ord, trd, dec, sig] = await Promise.all([
      getLivePositions(id), getLiveOrders(id), getLiveTrades(id, tradeDateParams()),
      getLiveDecisions(id).catch(() => null),
      getLiveSignals(id, { limit: 200 }).catch(() => null),
    ])
    // M1 多会话监控切换：五请求在途期间用户已切走（activeSessionId 变更）→
    // 丢弃过期结果，防旧会话历史覆盖新会话工作台。
    if (activeSessionId !== id) return
    positions.value = positionHistoryToRows(pos)
    orders.value = orderHistoryToRows(ord)
    trades.value = tradeHistoryToRows(trd)
    // 决策历史：逐事件（旧库/迁移未应用时端点 404 → 兜底空）；聚合统计前端按筛选区重算
    decisions.value = dec ? decisionHistoryToRows(dec.events) : []
    decisionAckSummary.value = dec?.ack_summary || { open: 0, acked: 0 }
    // 预警信号：alert 模式主产物；auto 模式下也可能有信号落库
    signals.value = sig?.items ?? []
    signalsTotal.value = sig?.total ?? 0
    // 账户全貌快照 + 交易日历来源/退化状态（同一详情请求，采集于 deals 节拍）——失败静默（辅助卡）
    try {
      getLiveSession(id).then(d => {
        if (activeSessionId !== id) return
        accountSnapshots.value = { ...accountSnapshots.value, [id]: d?.account_snapshot ?? null }
        calendarStatus.value = { ...calendarStatus.value, [id]: d?.trading_calendar ?? null }
        equityChecks.value = { ...equityChecks.value, [id]: d?.equity_check ?? null }
        bridgeOnline.value = typeof d?.bridge_online === 'boolean' ? d.bridge_online : null
      }).catch(() => {})
    } catch { /* api 缺失等 → 辅助卡静默 */ }
    // 竞态补拉（audit C1）：五请求在途期间 SSE 已推过 position/order/trade 时，
    // 上面整体覆盖会丢这些事件 → 计数 >0 则补拉一次三表历史兜底（补拉前清零；
    // 补拉期间再进的事件仍会漏，残余窗口毫秒级可接受，不无限递归）。
    if (workbenchEventsSinceLoad > 0) {
      workbenchEventsSinceLoad = 0
      const [pos2, ord2, trd2] = await Promise.all([
        getLivePositions(id), getLiveOrders(id), getLiveTrades(id, tradeDateParams()),
      ])
      // 补拉同样先验会话未切换（同上）
      if (activeSessionId !== id) return
      positions.value = positionHistoryToRows(pos2)
      orders.value = orderHistoryToRows(ord2)
      trades.value = tradeHistoryToRows(trd2)
    }
  } catch {
    // 会话已删除等 → 保持现状
  }
}

function startEventStreams() {
  closeEventStreams()
  // stopping 仍占用引擎/账户，且后端会在停止过程中推 canceled 终态事件 → 流保持到真正 stopped
  const running = sessions.value.filter(isSessionActive)
  if (running.length === 0) {
    connState.value = 'closed'
    return
  }
  for (const s of running) openOneStream(s)
}

/** 单会话 SSE（唯一实现：首连与心跳 reconnect 重连共用）。 */
function openOneStream(s: { id: number; name: string; status: string }) {
  const id = s.id
  const label = `#${s.id} ${s.name}`
  const source = new EventSource(`/api/live/sessions/${id}/stream`)
  // D-5（audit 前端 I4）：心跳流升级——收到 reconnect 关当前流重接（按流隔离，
  // 旧流迟到的 reconnect 不影响其它会话）；切换间隙可能漏事件，聚焦会话重拉兜底。
  source.addEventListener('reconnect', () => {
    const cur = esMap.get(id)
    if (cur !== source) return
    cur.close()
    esMap.delete(id)
    openOneStream(s)
    if (activeSessionId === id) loadHistory(id)
  })
  EVENT_TYPES.forEach((t) => {
    source.addEventListener(t, (e: MessageEvent) => {
      const data = JSON.parse(e.data)
      const time = nowTime()
      pushLog(t, data, time, label, id)
      // 工作台（持仓/委托/成交…）只跟随聚焦会话
      if (id === activeSessionId) applyToWorkbench(t, data, time)
    })
  })
  // watchdog 恢复事件（engine_recovered / bridge_recovered …）：统一落「恢复」日志条。
  // 不并入 risk 流：恢复是正向事件，混进红色风控会误导，也会污染风控统计口径。
  RECOVERY_EVENT_TYPES.forEach((t) => {
    source.addEventListener(t, (e: MessageEvent) => {
      const data = e.data ? JSON.parse(e.data) : {}
      pushLog('recovered', { ...data, rule: t }, nowTime(), label, id)
    })
  })
  source.onopen = () => { connState.value = 'open' }
  source.onerror = () => {
    // EventSource 规范：readyState=2(CLOSED) 才是终态不再重连（如后端 404），
    // 0/1 都仍在自动重连中（显示「连接中」而非 closed/未连接）
    connState.value = source.readyState === 2 ? 'error' : 'connecting'
  }
  esMap.set(id, source)
}

function closeEventStreams() {
  for (const s of esMap.values()) s.close()
  esMap.clear()
  connState.value = 'closed'
}

/** 切换工作台聚焦会话：清空工作台、拉该会话历史；事件流不动（多会话并行常开）。 */
const watchedSessionId = ref<number | null>(null)

function watchSession(id: number) {
  watchedSessionId.value = id
  activeSessionId = id
  bridgeOnline.value = null  // 切换会话先置未知，避免显示上一个会话的在线态
  const target = sessions.value.find((s) => s.id === id)
  if (target) {
    runningEngine.value = (target.engine as LiveEngineKey) || null
    loadChannel(target.engine)
  }
  // 清空工作台（事件日志保留——多会话合并流）
  positions.value = []
  orders.value = []
  trades.value = []
  decisions.value = []
  signals.value = []
  signalsTotal.value = 0
  decOnlyDeferred.value = false
  loadHistory(id)
}

async function load() {
  try {
    sessions.value = await getLiveSessions()
  } catch {
    sessions.value = []
  } finally {
    loading.value = false
    nowTick.value = Date.now()
  }
  portfolios.value = await getPortfolios().catch(() => [])
  formulas.value = await getFormulas().catch(() => [])
  // 多会话并行监控：为所有运行中会话开 SSE（事件日志合并带会话前缀）；
  // 工作台聚焦「监控中」会话（优先续接，无则回退第一个 running）。
  const running = sessions.value.filter((s) => s.status === 'running')
  const target = running.find((s) => s.id === watchedSessionId.value) || running[0] || null
  watchedSessionId.value = target ? target.id : null
  runningEngine.value = (target?.engine as LiveEngineKey) || null
  startEventStreams()
  if (target) {
    activeSessionId = target.id
    loadHistory(target.id)
  } else {
    // 无运行会话 → 断开事件流、工作台聚焦清空
    closeEventStreams()
    activeSessionId = null
    bridgeOnline.value = null
  }
  // 通道条跟随监控中 session 的引擎；无运行会话时跟随默认引擎
  loadChannel(target?.engine)
  // 参数漂移（绑定回测记录时）：随列表刷新，drifted 才出横幅
  void loadParamsDrift()
}

function openCreate() {
  // C1：默认引擎若已被占用，自动选第一个未占用且已配置的引擎（避免默认值即非法）
  const occ = occupants.value
  const engines = channel.value?.engines || []
  const preferred = defaultEngineKey()
  const engine = (!occ[preferred]
    ? preferred
    : (engines.find((e) => e.configured && !occ[e.key])?.key as LiveEngineKey | undefined)) || preferred
  form.value = { name: '', mode: 'simulation', engine, execution_mode: 'alert', portfolio_ids: [], live_capital: null, signal_file_path: '', advance_close_enabled: false, advance_close_time: '14:51', intraday_mismatched_risk_enabled: null }
  createError.value = ''
  showCreate.value = true
  loadAccountPreview(engine)
}

async function create() {
  if (form.value.portfolio_ids.length === 0) {
    alert('请至少选择一个组合策略')
    return
  }
  const cap = normalizedLiveCapital()
  // 只缩不放：填写值超过真实总资产 → 提示超上限并拒绝保存（通道未就绪取不到时不拦）
  const totalAsset = accountPreview.value?.total_asset
  if (cap !== null && totalAsset !== null && totalAsset !== undefined && cap > Number(totalAsset)) {
    createError.value = `填写值 ${formatMoney2(cap)} 超过当前真实总资产 ${formatMoney2(totalAsset)}，请填写更小的值`
    return
  }
  if (cap !== null && (!Number.isFinite(cap) || cap <= 0)) {
    createError.value = '实盘资金基准需大于 0，或留空以真实通道总资产为基准'
    return
  }
  createError.value = ''
  try {
    const res = await createLiveSession({
      ...form.value,
      live_capital: cap,
      baseline_mode: cap === null ? 'total_asset' : 'manual',
      baseline_value: cap,
    })
    // A1 软校验：目标引擎已被其它会话占用等提示（不阻断创建，但必须让用户看见）
    createWarnings.value = Array.isArray(res?.warnings) ? res.warnings : []
    showCreate.value = false
    form.value = { name: '', mode: 'simulation', engine: defaultEngineKey(), execution_mode: 'alert', portfolio_ids: [], live_capital: null, signal_file_path: '', advance_close_enabled: false, advance_close_time: '14:51', intraday_mismatched_risk_enabled: null }
    load()
  } catch (e: any) {
    // 后端参数校验失败（名称空/资金≤0 等）返回 code!==0 → 拦截器 reject，
    // 必须提示并保留弹窗，否则点「确认」看起来完全无反应。
    createError.value = e?.message || '创建失败'
  }
}

/** 启动会话：tdx ↔ ths 键鼠冲突时先弹二次确认（需勾选风险），确认后带 ack 提交。 */
function startSession(id: number, ack = false) {
  if (!ack) {
    const target = sessions.value.find((s) => s.id === id)
    const opponents = target ? keyboardConflictOpponents(sessions.value, resolvedSessionEngine(target)) : []
    if (opponents.length > 0) {
      keyboardRisk.value = { session: target as LiveSessionItem, opponents }
      ackKeyboardRisk.value = false
      return
    }
  }
  void doStart(id, ack)
}

async function doStart(id: number, ack: boolean) {
  clearStartError(id)
  try {
    const r = ack
      ? await startLiveSession(id, { ack_keyboard_risk: true })
      : await startLiveSession(id)
    // 通道建议类告警（非阻断）：如所选引擎与推荐通道不一致，展示在行内
    if (Array.isArray(r?.warnings) && r.warnings.length > 0) {
      startWarnings.value = { ...startWarnings.value, [id]: r.warnings }
    }
    load() // load 内自动连接新运行 session 的流
  } catch (e: any) {
    // 结构化 409：按 data.rule（引擎占用/账户占用/键鼠硬拦/参数漂移阻断）给指名话术
    const info = parseStartConflict(e)
    startErrors.value = { ...startErrors.value, [id]: info.message }
    alert(info.message)
  }
}

function clearStartError(id: number) {
  if (startErrors.value[id] === undefined) return
  const next = { ...startErrors.value }
  delete next[id]
  startErrors.value = next
}

function confirmKeyboardRisk() {
  if (!ackKeyboardRisk.value || !keyboardRisk.value) return
  const id = keyboardRisk.value.session.id
  keyboardRisk.value = null
  startSession(id, true)
}

/** 打开重置弹窗；交易时段置灰不可点（原因见 title）。 */
function openBaseline(s: LiveSessionItem) {
  if (!resetWindow.value.allowed) return
  baselineModal.value = { session: s }
  baselineValue.value = ''
  baselineError.value = ''
}

async function confirmBaseline() {
  if (!baselineModal.value) return
  const s = baselineModal.value.session
  const v = baselineValue.value === '' ? null : Number(baselineValue.value)
  if (v !== null && (!Number.isFinite(v) || v <= 0)) {
    baselineError.value = '请填写大于 0 的金额，或留空使用当日收盘总资产'
    return
  }
  const pids = sessionPortfolioIds(s)
  if (pids.length === 0) {
    baselineError.value = '该会话没有可重置的组合策略'
    return
  }
  try {
    // 文档 §7 的重置端点是按组合，一个会话的多组合逐个重置
    for (const pid of pids) await resetLiveBaseline(s.id, pid, { value: v })
    baselineModal.value = null
    load()
  } catch (e: any) {
    baselineError.value = e?.response?.data?.message || e?.message || '重置失败'
  }
}

// ---- 参数漂移（随会话列表刷新；只查 running/stopping）----
async function loadParamsDrift() {
  const active = sessions.value.filter(isSessionActive)
  if (active.length === 0) {
    paramsDrifts.value = {}
    return
  }
  const pairs = await Promise.all(active.map(async (s) => {
    try {
      return [s.id, await getLiveParamsDrift(s.id)] as const
    } catch {
      return [s.id, null] as const
    }
  }))
  const next: Record<number, ParamsDrift> = {}
  for (const [id, d] of pairs) if (d) next[id] = d
  paramsDrifts.value = next
}

// ---- 外部现金流台账（入金/出金/分红/其他）----
function openCashFlows(s: LiveSessionItem) {
  cashFlowModal.value = { session: s }
  cashFlowError.value = ''
  cashFlows.value = null
  const pids = sessionPortfolioIds(s)
  cashFlowForm.value = {
    portfolio_id: pids[0] ?? null,
    flow_type: 'deposit',
    amount: '',
    flow_date: beijingDayKey(Date.now()),
    note: '',
  }
  void loadCashFlows(s.id)
}

async function loadCashFlows(sessionId: number) {
  try {
    const d = await getLiveCashFlows(sessionId)
    if (cashFlowModal.value?.session.id !== sessionId) return
    cashFlows.value = d
  } catch (e: any) {
    cashFlowError.value = e?.response?.data?.message || e?.message || '台账加载失败'
  }
}

async function submitCashFlow() {
  if (!cashFlowModal.value) return
  const sid = cashFlowModal.value.session.id
  const pid = cashFlowForm.value.portfolio_id
  const amount = Number(cashFlowForm.value.amount)
  if (pid === null) {
    cashFlowError.value = '请选择组合策略'
    return
  }
  if (!Number.isFinite(amount) || amount === 0) {
    cashFlowError.value = '金额需为非 0 数字（入金/出金/分红传正数，其他可按正负判方向）'
    return
  }
  cashFlowSaving.value = true
  cashFlowError.value = ''
  try {
    await createLiveCashFlow(sid, {
      portfolio_id: pid,
      flow_type: cashFlowForm.value.flow_type,
      amount,
      flow_date: cashFlowForm.value.flow_date || null,
      note: cashFlowForm.value.note.trim() || null,
    })
    cashFlowForm.value.amount = ''
    cashFlowForm.value.note = ''
    await loadCashFlows(sid)
  } catch (e: any) {
    cashFlowError.value = e?.response?.data?.message || e?.message || '登记失败'
  } finally {
    cashFlowSaving.value = false
  }
}

function closeCashFlows() {
  cashFlowModal.value = null
  cashFlows.value = null
  cashFlowError.value = ''
}

/** 台账净流入（按组合；弹窗底部汇总）。 */
function netInflowOf(pid: number): string {
  const row = cashFlows.value?.net_inflow?.find((x) => x.portfolio_id === pid)
  return row ? formatMoney2(Number(row.net_inflow)) : '—'
}

async function stopSession(id: number) {
  // 先停后端成功再关流（audit M3）：stop 后端会经流推 canceled 终态事件，
  // 先关流会错过清扫；API 失败则保持流开着（会话实际仍在运行）。
  try {
    await stopLiveSession(id)
    load() // 多会话并行：load 会重开所有 running 流、关停已停会话的流
  } catch (e: any) {
    alert(e?.message || '停止失败')
  }
}

async function deleteSession(s: LiveSessionItem) {
  if (!confirm(`确定删除实盘 session「${s.name}」？该 session 的历史委托/成交将一并清除，且不可恢复。`)) return
  try {
    await deleteLiveSession(s.id)
    load()
  } catch (e: any) {
    alert(e?.message || '删除失败')
  }
}

async function loadChannel(engine?: LiveEngineKey) {
  try {
    channel.value = await getLiveChannel(engine)
  } catch {
    channel.value = null
  }
}

/** 定时刷新聚焦会话的通道在线态（只在 running 会话上取；未知=null）。
 *
 * 同时取 watchdog 活跃告警：``channel_offline`` 是即时的（30s 扫描），而
 * ``bridge_online`` 要等心跳失败（60–90s）才翻转，徽章优先用前者（2026-11-13）。
 */
async function refreshBridgeOnline() {
  const id = activeSessionId
  if (id === null) {
    bridgeOnline.value = null
    watchdogAlarms.value = []
    return
  }
  const target = sessions.value.find((s) => s.id === id)
  if (!target || target.status !== 'running') {
    bridgeOnline.value = null
    watchdogAlarms.value = []
    return
  }
  try {
    const d = await getLiveSession(id)
    if (activeSessionId !== id) return  // 在途期间已切走 → 丢弃
    bridgeOnline.value = typeof d?.bridge_online === 'boolean' ? d.bridge_online : null
  } catch { /* 保持现状 */ }
  try {
    const w = await getWatchdogStatus()
    if (activeSessionId !== id) return
    watchdogAlarms.value = Array.isArray(w?.active_alarms) ? w.active_alarms : []
  } catch { /* 保持现状 */ }
}

async function testChannel() {
  if (!channel.value) return
  channelTesting.value = true
  channelTestMsg.value = null
  try {
    const r = await testLiveChannel(channel.value.active)
    const label = channel.value.label
    channelTestMsg.value = r.ok
      ? { ok: true, text: `${label} 已连接${r.positions !== undefined ? `，读到 ${r.positions} 条持仓` : r.base_url ? `（${r.base_url}）` : ''}` }
      : { ok: false, text: r.error || '连接失败' }
  } catch (e: any) {
    channelTestMsg.value = { ok: false, text: e?.message || '连接失败' }
  } finally {
    channelTesting.value = false
  }
}

onMounted(() => {
  load()
  // 通道在线态 15s 轮询（与决策流 SSE 互补：SSE 掉线时仍能看到状态）；
  // 同时刷新 nowTick，使「重置资金基准」在跨过 15:00 后自动解禁。
  bridgeTimer = setInterval(() => {
    nowTick.value = Date.now()
    refreshBridgeOnline()
  }, 15000)
})
onUnmounted(() => {
  closeEventStreams()
  if (bridgeTimer !== null) {
    clearInterval(bridgeTimer)
    bridgeTimer = null
  }
})
</script>

<template>
  <PageHeader title="实盘交易">
    <button class="btn btn-sm" @click="showGuide = true" data-field="live-guide-toggle">📖 配置指南</button>
    <button @click="openCreate" class="btn btn-primary">+ 新建实盘</button>
  </PageHeader>

  <!-- A1 新建会话软告警（如目标引擎已被其它会话占用）：不阻断，但必须可见 -->
  <div v-if="createWarnings.length" class="card create-warning-banner" data-test="create-warnings">
    <div v-for="w in createWarnings" :key="w">{{ w }}</div>
    <button class="btn btn-sm" @click="createWarnings = []">关闭</button>
  </div>

  <!-- C4 多账户并行提示：不阻断任何操作，可关闭 -->
  <div v-if="multiAccountBanner && !bannerDismissed" class="card multi-account-banner" data-test="multi-account-banner">
    <span>{{ multiAccountBanner }}</span>
    <button class="btn btn-sm" @click="bannerDismissed = true">关闭</button>
  </div>

  <!-- A7 参数漂移：绑定回测记录的会话，当前组合参数与快照不一致（drifted）时提示差异字段 -->
  <div
    v-for="b in driftBanners" :key="`drift-${b.sessionId}`"
    class="card drift-banner" data-test="params-drift-banner"
  >
    <div class="drift-head">
      <strong>参数漂移</strong>
      <span class="badge" :class="b.blockEnabled ? 'badge-red' : 'badge-gray'">
        会话 {{ b.sessionId }}「{{ b.name }}」{{ b.blockEnabled ? '启动将被阻断' : '仅告警' }}
      </span>
    </div>
    <div v-if="b.lines.length" class="drift-fields">
      <ul>
        <li v-for="line in b.lines" :key="line">{{ line }}</li>
      </ul>
    </div>
    <div class="drift-hint">
      当前组合参数与回测参数快照不一致，实盘行为可能已偏离该回测结果；请先对齐参数（或重新回测）后再启动。
    </div>
  </div>

  <div class="card channel-bar">
    <span class="channel-label">{{ channelLabelPrefix }}</span>
    <span class="badge" :class="channel?.online ? 'badge-green' : 'badge-gray'"
      :title="channelBadgeTitle">
      {{ channel?.label || '未知' }}
    </span>
    <span v-for="e in otherEngines" :key="e.key" class="badge"
      :class="e.configured ? 'badge-gray' : 'badge-red'"
      :title="`${e.detail}（自动模式可在新建实盘时另选）`">{{ e.label }}{{ e.configured ? '' : '（未配置）' }}</span>
    <!-- 引擎未运行 → 不做在线判定（此前无引擎时探活只看客户端是否开着，会把
         「引擎未运行」误显示为绿色在线；重启后尤其误导）。 -->
    <span
      v-if="channel?.engine_running === false"
      class="badge badge-gray"
      title="引擎未运行：重启后需重新启动会话，启动后才会实时检测通道连通性"
    >未检测（会话未启动）</span>
    <span
      v-else-if="channel?.online !== null && channel?.online !== undefined"
      class="badge" :class="channel.online ? 'badge-green' : 'badge-red'"
    >{{ channel.online ? '在线' : '离线' }}</span>
    <!-- 聚焦会话的通道态：watchdog 告警（30s）优先，回退会话详情心跳（60–90s） -->
    <span
      v-if="watchedSessionId !== null"
      class="badge"
      :class="channelBadge === true ? 'badge-green' : channelBadge === false ? 'badge-red' : 'badge-gray'"
      :title="sessionChannelBadgeTitle"
    >通道：{{ channelStateLabel(channelBadge) }}</span>
    <button class="btn btn-sm" :disabled="channelTesting" @click="testChannel">
      {{ channelTesting ? '测试中…' : '测试连接' }}
    </button>
    <span
      v-if="channelTestMsg"
      :style="{ color: channelTestMsg.ok ? 'var(--green)' : 'var(--red)', fontSize: '13px' }"
    >{{ channelTestMsg.text }}</span>
    <span class="channel-hint">自动模式可在「新建实盘」时另选引擎；默认引擎在 系统配置 → 通达信 TQ 通道 修改</span>
    <span class="channel-hint">支持跨引擎多会话并行（不同账户）；同一引擎类型同时仅一个会话；同一实盘账户跨引擎也仅一个会话；同时运行键鼠通道（通达信/同花顺）需二次确认风险</span>
    <router-link to="/system" style="margin-left:auto;font-size:13px">通道配置 →</router-link>
  </div>

  <div v-if="loading" class="card" style="padding:12px"><p>加载中…</p></div>
  <div v-else class="card table-wrap">
    <table>
      <thead><tr><th>ID</th><th>名称</th><th>组合策略</th><th>执行模式</th><th>引擎</th><th>模式</th><th>资金基准</th><th>状态</th><th>操作</th></tr></thead>
      <tbody>
        <tr v-for="s in sessions" :key="s.id">
          <td style="color:var(--ink-soft)">#{{ s.id }}</td>
          <td>{{ s.name }}</td>
          <td style="color:var(--ink-soft)">
            <span v-for="p in portfolioStatuses(s)" :key="p.portfolio_id" class="portfolio-chip">
              {{ portfolioName(p.portfolio_id) }}
              <span v-if="p.status === 'circuit_broken'" class="badge badge-red" style="margin-left:4px">熔断</span>
              <button v-if="p.status === 'circuit_broken'" @click="recoverBreaker(s, p.portfolio_id)" class="btn btn-sm" style="margin-left:4px">恢复</button>
            </span>
            <span v-if="portfolioStatuses(s).length === 0">-</span>
          </td>
          <td>
            <span class="badge" :class="isAlert(s) ? 'badge-blue' : 'badge-purple'"
              :title="isAlert(s) ? '半自动预警：仅 TQ 行情 + send_warn 推送预警，不下单' : '全自动：按交易引擎下单'"
            >{{ executionModeLabel(s.execution_mode) }}</span>
          </td>
          <td>
            <span class="badge badge-gray" :title="isAlert(s) ? '预警模式：固定走通达信 TQ 行情计算信号，不下单' : '全自动：按交易引擎下单'">{{ engineLabel(s.engine) }}</span>
          </td>
          <td>{{ s.mode === 'simulation' ? '仿真' : '实盘' }}</td>
          <td>
            <span :title="s.live_capital != null ? '手动填写的实盘资金基准（只缩不放）' : '以真实通道总资产为基准（只缩不放）'">
              {{ s.live_capital != null ? formatMoney2(s.live_capital) : '真实总资产' }}
            </span>
            <button
              class="btn btn-sm" style="margin-left:6px" data-test="baseline-reset"
              :disabled="!resetWindow.allowed"
              :title="resetWindow.allowed ? '重置资金基准（留空=当日收盘总资产，只能小于真实总资产）' : resetWindow.reason"
              @click="openBaseline(s)"
            >重置</button>
          </td>
          <td>
            <span class="badge"
              :class="s.status === 'running' ? 'badge-green' : s.status === 'stopping' ? 'badge-orange' : 'badge-gray'"
            >{{ s.status === 'running' ? '运行中' : s.status === 'stopping' ? '停止中' : '已停止' }}</span>
            <!-- C4：真的在操作多个账户 → 徽标提示（不阻断） -->
            <span v-if="s.status === 'running' && multiAccountBanner"
              class="badge badge-orange" style="margin-left:4px"
              title="当前有多个实盘账户并行运行，下单前请确认窗口对应的账户">多账户并行中</span>
            <!-- 状态陈旧保护：后端重启后引擎未恢复时，DB 可能仍留 running；以引擎
                 注册表（engine_running）为准提示，避免误以为在自动交易。 -->
            <span v-if="s.status === 'running' && s.engine_running === false"
              class="badge badge-red" style="margin-left:4px"
              title="状态陈旧：后端重启后引擎未恢复，实际未运行；请重新启动会话">引擎未启动</span>
            <!-- 「工作台聚焦」≠「是否被监控」：所有 running 会话都并行订阅 SSE、并行监控；
                 该徽标仅表示工作台当前聚焦展示哪一个会话。 -->
            <span v-if="s.status === 'running' && s.id === watchedSessionId"
              class="badge badge-blue" style="margin-left:4px"
              title="工作台聚焦：仅表示工作台当前聚焦展示该会话；所有运行中会话都在并行监控与订阅">工作台聚焦</span>
            <!-- 监控正常：引擎在册（engine_running=true）才算真的在被监控；后端未返回该字段时不显示，避免误报。 -->
            <span v-if="s.status === 'running' && s.engine_running === true"
              class="badge badge-green" style="margin-left:4px"
              title="监控正常：引擎在册，轮询与风控在线">监控正常</span>
          </td>
          <td>
            <button v-if="s.status === 'stopped'" @click="startSession(s.id)" class="btn btn-sm btn-primary">启动</button>
            <button v-if="s.status === 'running'" @click="stopSession(s.id)" class="btn btn-sm btn-danger">停止</button>
            <button v-if="s.status === 'stopping'" class="btn btn-sm" disabled
              title="引擎正在退出中，停止完成后才会释放引擎/账户占用">停止中…</button>
            <button v-if="s.status === 'running' && s.id !== watchedSessionId" @click="watchSession(s.id)" class="btn btn-sm">监控</button>
            <button
              v-if="s.status !== 'stopping'"
              @click="openCashFlows(s)"
              class="btn btn-sm" data-test="cash-flow-open"
              title="登记入金/出金/分红等外部现金流（不记台账会扭曲净值、收益率、回撤、日亏）"
            >存取款/分红</button>
            <button v-if="s.status === 'stopped'" @click="deleteSession(s)" class="btn btn-sm">删除</button>
            <!-- 结构化 409：按 data.rule 指名占用者（C1 引擎占用 / C2 账户占用 / C3 键鼠硬拦 / 参数漂移阻断） -->
            <div v-if="startErrors[s.id]" class="start-error" data-test="start-error">{{ startErrors[s.id] }}</div>
            <!-- start 返回的通道建议类告警（非阻断）：如所选引擎与推荐通道不一致 -->
            <div v-if="startWarnings[s.id]?.length" class="start-warning" data-test="start-warning">
              <div v-for="w in startWarnings[s.id]" :key="w">{{ w }}</div>
            </div>
          </td>
        </tr>
      </tbody>
    </table>
    <div v-if="sessions.length === 0" class="empty-state"><p>暂无实盘 session</p></div>
  </div>

  <!-- B4b: 工作台（持仓/委托/成交） -->
  <div class="card" style="margin-top:16px">
    <div class="wb-head">
      <h3 style="margin:0">
        实盘工作台
        <span v-if="watchedSessionId" style="font-size:13px;color:var(--ink-soft);font-weight:normal;margin-left:8px">
          聚焦：#{{ watchedSessionId }} {{ sessions.find((s) => s.id === watchedSessionId)?.name }}
        </span>
      </h3>
      <div class="wb-tabs">
        <select
          v-if="runningSessions.length > 1"
          class="input" style="width:auto;margin-right:8px;padding:4px 8px"
          :value="watchedSessionId ?? ''"
          @change="watchSession(Number(($event.target as HTMLSelectElement).value))"
        >
          <option v-for="s in runningSessions" :key="s.id" :value="s.id">#{{ s.id }} {{ s.name }}（{{ s.engine }}）</option>
        </select>
        <button
          v-for="(label, key) in TAB_LABEL" :key="key"
          class="btn btn-sm" :class="wbTab === key ? 'btn-primary' : ''"
          @click="setWbTab(key)"
        >{{ label }}</button>
        <button
          v-if="wbTab === 'positions'"
          class="btn btn-sm" style="margin-left:8px"
          :disabled="loading || activeSessionId === null"
          @click="loadHistory(activeSessionId!)"
          title="重新拉取全量持仓（原有持仓服务端缓存 60s）"
        >↻ 刷新持仓</button>
        <button
          v-if="wbTab === 'decisions'"
          class="btn btn-sm" style="margin-left:8px"
          data-test="decisions-refresh"
          :disabled="decisionsLoading || activeSessionId === null"
          @click="reloadDecisions()"
          title="重新拉取决策/拦截历史（含 move-stop、顺延、持久化风控事件）"
        >{{ decisionsLoading ? '刷新中…' : '↻ 刷新决策' }}</button>
      </div>
    </div>

    <!-- 账户全貌（多会话并行：每个运行中会话一张卡，不同账号分列） -->
    <div v-if="wbTab === 'positions' && runningSessions.length > 0" class="card" style="margin-bottom:12px;padding:10px 14px">
      <div style="display:flex;gap:16px;flex-wrap:wrap">
        <div
          v-for="s in runningSessions" :key="s.id"
          style="min-width:280px;flex:1;border:1px solid var(--line,#ddd);border-radius:8px;padding:8px 12px"
          :style="s.id === watchedSessionId ? 'border-color:var(--accent,#3b82f6)' : ''"
        >
          <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:center">
            <strong style="margin:0">#{{ s.id }} {{ s.name }}<span style="color:var(--ink-soft);font-weight:normal;margin-left:6px">{{ s.engine }}</span></strong>
            <span
              v-if="s.status === 'running' && calendarStatus[s.id]?.degraded === true"
              class="badge badge-red" data-test="calendar-degraded"
              :title="calendarDegradedTitle(calendarStatus[s.id])"
            >交易日历退化</span>
          </div>
          <p
            v-if="s.status === 'running' && calendarStatus[s.id] && calendarStatus[s.id].degraded !== true"
            data-test="calendar-info"
            style="margin:4px 0 0;color:var(--ink-soft);font-size:12px"
            :title="calendarInfoText(calendarStatus[s.id])"
          >{{ calendarInfoText(calendarStatus[s.id]) }}</p>
          <template v-if="accountSnapshots[s.id]">
            <div style="display:flex;gap:16px;flex-wrap:wrap;margin-top:4px">
              <span>总资产 <b>{{ accountSnapshots[s.id].total_asset ?? '—' }}</b></span>
              <span>可用资金 <b>{{ accountSnapshots[s.id].available_cash ?? '—' }}</b></span>
              <span>持仓市值 <b>{{ accountSnapshots[s.id].market_value ?? '—' }}</b></span>
            </div>
            <p style="margin:4px 0 0;color:var(--ink-soft);font-size:12px">
              采集于 {{ accountSnapshots[s.id].captured_at }}；BUY 审批额度 = min(虚拟现金, 真实可用 × 0.98)
            </p>
          </template>
          <div
            v-if="equityChecks[s.id] && equityChecks[s.id].virtual_total"
            data-test="equity-check"
            style="display:flex;gap:16px;flex-wrap:wrap;margin-top:4px;font-size:13px"
          >
            <span>虚拟净值 <b>{{ equityChecks[s.id].virtual_total }}</b></span>
            <span>真实总资产 <b>{{ equityChecks[s.id].real_total ?? '—' }}</b></span>
            <span
              v-if="equityChecks[s.id].deviation_pct !== null"
              :style="{ color: Math.abs(Number(equityChecks[s.id].deviation_pct)) > 0.2 ? 'var(--red)' : 'var(--ink-soft)' }"
            >偏差 <b>{{ (Number(equityChecks[s.id].deviation_pct) * 100).toFixed(2) }}%</b></span>
            <span style="color:var(--ink-soft)">
              （真实侧已剔除池外 {{ equityChecks[s.id].out_of_pool_value ?? '—' }}；偏差 &gt;20% 需人工核对账本）
            </span>
          </div>
          <p v-if="!accountSnapshots[s.id]" style="margin:4px 0 0;color:var(--ink-soft);font-size:12px">
            账户快照采集中…（{{ snapshotHint(s.engine) }}）
          </p>
        </div>
      </div>
      <p style="margin:8px 0 0;color:var(--ink-soft);font-size:12px">
        池内原有持仓已自动导入对应会话的虚拟账本纳入风控；池外持仓只展示/对账（下表单列「池外（不参与业绩）」）。不同引擎=不同账户，持仓/资金按会话隔离。
      </p>
    </div>

    <!-- 持仓（序号 + 组合/子策略归属，同票多子策略分多行；来源分级：策略/原有持仓） -->
    <div v-if="wbTab === 'positions'" class="table-wrap">
      <table>
        <thead><tr><th>序号</th><th>组合策略</th><th>子策略</th><th>代码</th><th>数量</th><th>成本价</th><th>市值</th><th>峰值</th><th>峰值日期</th><th>峰值来源</th><th>来源</th></tr></thead>
        <tbody>
          <tr v-for="(p, i) in poolPositions" :key="positionRowKey(p)">
            <td style="color:var(--ink-soft)">{{ i + 1 }}</td>
            <td>{{ p.portfolio_id !== null && p.portfolio_id !== undefined ? portfolioName(p.portfolio_id) : '—' }}</td>
            <td>{{ strategyName(p.portfolio_id, p.strategy_id) }}</td>
            <td>{{ p.stock_code }}</td><td>{{ p.quantity }}</td><td>{{ fmtCost(p.avg_cost) }}</td><td>{{ fmtMoney(p.market_value) }}</td>
            <td>{{ p.highest_price ? fmtCost(p.highest_price) : '—' }}</td>
            <td>{{ p.peak_bar_time ? String(p.peak_bar_time).replace('T', ' ').slice(0, 16) : '—' }}</td>
            <td>
              <span v-if="p.peak_source_label" class="badge badge-gray" :title="`峰值来源：${p.peak_source_label}（${p.peak_source}）`">
                {{ p.peak_source_label }}
              </span>
              <span v-else style="color:var(--ink-soft)">—</span>
            </td>
            <td>
              <span class="badge" :class="p.source === 'external' ? 'badge-gray' : 'badge-blue'">
                {{ p.source === 'external' ? '原有持仓' : '策略持仓' }}
              </span>
              <span v-if="p.out_of_pool === true || p.attribution === 'fallback'" class="badge badge-orange" style="margin-left:4px"
                    title="池外外部仓：只进持仓展示与对账，不进风控、不占槽位、不代卖（每日一条 out_of_pool_position 汇总事件）">
                池外·不参与风控
              </span>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-if="poolPositions.length === 0" class="empty-state">
        <p>暂无持仓</p>
        <p>账户原有持仓（非策略买入）以灰色标注为原有持仓；属于本会话股票池的已导入虚拟账本并按真实成本纳入风控，池外外部仓仅展示/对账、不进风控、不占槽位、不代卖；成本 ≤0 显示「成本未知」且不参与风控。</p>
      </div>
    </div>

    <!-- 池外（不参与业绩）：柜台有仓但代码不在任何组合股票池 → 不进虚拟账本、
         不进虚拟现金/净值/回撤分母，仅展示与对账 -->
    <div v-if="wbTab === 'positions' && outOfPoolPositions.length > 0" class="table-wrap">
      <p style="margin:0 0 8px;font-weight:600">池外（不参与业绩）</p>
      <table>
        <thead><tr><th>序号</th><th>代码</th><th>数量</th><th>成本价</th><th>市值（参考）</th><th>说明</th></tr></thead>
        <tbody>
          <tr v-for="(p, i) in outOfPoolPositions" :key="positionRowKey(p)">
            <td style="color:var(--ink-soft)">{{ i + 1 }}</td>
            <td>{{ p.stock_code }}</td><td>{{ p.quantity }}</td><td>{{ fmtCost(p.avg_cost) }}</td><td>{{ fmtMoney(p.market_value) }}</td>
            <td>
              <span class="badge badge-orange"
                    title="池外外部仓：只进持仓展示与对账，不进虚拟现金/净值/回撤分母，不进风控、不占槽位、不代卖（每日一条 out_of_pool_position 汇总事件）">
                池外·不参与业绩
              </span>
            </td>
          </tr>
        </tbody>
      </table>
      <p style="margin:8px 0 0;color:var(--ink-soft);font-size:12px">
        这些代码不在本会话任何组合股票池：不计入虚拟净值、不参与风控、不占槽位、平台不代卖。
        如需纳入策略管理，请把代码加入对应组合股票池。
      </p>
    </div>

    <!-- 委托 -->
    <div v-if="wbTab === 'orders'" class="table-wrap">
      <div style="display:flex;align-items:center;gap:8px;margin:0 0 8px">
        <span style="font-size:12px;color:var(--ink-soft)">时间范围</span>
        <select v-model="orderRange" style="font-size:12px;padding:2px 6px" aria-label="委托时间范围">
          <option v-for="opt in TIME_RANGE_OPTIONS" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
        </select>
        <span style="font-size:12px;color:var(--ink-faint)">按北京时间自然日 · 共 {{ filteredOrders.length }} 条</span>
      </div>
      <table>
        <thead><tr><th>时间（北京时间）</th><th>方向</th><th>代码</th><th>状态</th><th>数量</th><th>价格</th><th>已成交</th><th>错误</th></tr></thead>
        <tbody>
          <tr v-for="o in filteredOrders" :key="o.key">
            <td style="color:var(--ink-soft)">{{ o.time || '—' }}</td>
            <td>{{ tradeTypeLabel(o.trade_type) }}</td>
            <td>{{ o.stock_code }}</td>
            <td><span class="badge" :class="o.status === 'filled' ? 'badge-green' : o.status === 'rejected' ? 'badge-red' : 'badge-gray'">{{ ORDER_STATUS_LABEL[o.status] || o.status }}</span></td>
            <td>{{ o.quantity }}</td>
            <td>{{ o.price ?? '-' }}</td>
            <td>{{ o.filled_quantity ?? '-' }}</td>
            <td style="color:var(--red);max-width:280px;white-space:normal">{{ o.error_message ?? '-' }}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="filteredOrders.length === 0" class="empty-state">
        <p>{{ orderRange === 'today' ? '今日暂无委托' : '该时间范围暂无委托' }}</p>
      </div>
    </div>

    <!-- 成交 -->
    <div v-if="wbTab === 'trades'" class="table-wrap">
      <div style="display:flex;align-items:center;gap:8px;margin:0 0 8px">
        <span style="font-size:12px;color:var(--ink-soft)">时间范围</span>
        <select v-model="tradeRange" style="font-size:12px;padding:2px 6px"
                aria-label="成交时间范围" @change="reloadTrades">
          <option v-for="opt in TIME_RANGE_OPTIONS" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
        </select>
        <span style="font-size:12px;color:var(--ink-faint)">
          按北京时间自然日 · 默认只看今日 · 共 {{ filteredTrades.length }} 条（历史成交保留，不删除）
        </span>
      </div>
      <table>
        <thead><tr><th>时间（北京时间）</th><th>方向</th><th>代码</th><th>价格</th><th>数量</th><th>金额</th></tr></thead>
        <tbody>
          <tr v-for="t in filteredTrades" :key="t.key">
            <td style="color:var(--ink-soft)">
              <span v-if="t.day" style="color:var(--ink-faint)">{{ t.day }} </span>{{ t.time }}
            </td>
            <td>{{ tradeTypeLabel(t.trade_type) }}</td>
            <td>{{ t.stock_code }}</td>
            <td>{{ t.price }}</td><td>{{ t.quantity }}</td><td>{{ t.amount }}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="filteredTrades.length === 0" class="empty-state">
        <p>{{ tradeRange === 'today' ? '今日暂无成交（历史成交请切「全部」查看）' : '该时间范围暂无成交' }}</p>
      </div>
    </div>

    <!-- 决策/拦截（调参可观测性：闸门触发聚合 + 逐事件流） -->
    <div v-if="wbTab === 'decisions'" class="table-wrap">
      <div style="display:flex;align-items:center;gap:8px;margin:0 0 8px">
        <span style="font-size:12px;color:var(--ink-soft)">时间范围</span>
        <select v-model="decRange" style="font-size:12px;padding:2px 6px" aria-label="决策时间范围">
          <option v-for="opt in TIME_RANGE_OPTIONS" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
        </select>
        <span style="font-size:12px;color:var(--ink-faint)">按北京时间自然日 · 聚合与统计随筛选重算 · 共 {{ filteredDecisions.length }} 条事件</span>
        <span style="margin-left:auto;font-size:12px;color:var(--ink-soft)">
          未处理 {{ decisionAckSummary.open }} 条 · 已确认 {{ decisionAckSummary.acked }} 条
        </span>
        <button
          class="btn ghost"
          data-test="decisions-ack-all"
          style="font-size:12px;padding:2px 10px"
          :disabled="decisionsLoading || decisionAckSummary.open === 0"
          title="把所有未处理的决策/拦截事件标记为已确认（日终清零）"
          @click="ackAllDayEnd"
        >全部确认（日终清零）</button>
      </div>
      <!-- 顺延待次日提交汇总：gate=signal_bar_date / action=defer，N=0 不显示。
           点击=把下方事件表筛到这一类（不隐藏入口，再点一次取消），非行高亮。 -->
      <div
        v-if="deferredNextDayCount > 0"
        class="defer-banner"
        :class="{ 'defer-banner--active': decOnlyDeferred }"
        data-test="defer-summary"
        @click="decOnlyDeferred = !decOnlyDeferred"
        :title="decOnlyDeferred ? '点击取消筛选' : '点击只看顺延待次日提交的事件'"
      >
        顺延待次日提交（{{ deferredNextDayCount }} 笔）
        <span class="defer-hint">
          {{ decOnlyDeferred ? '已筛选 · 点击取消' : '点击只看这一类' }}
          · 这些单在信号 bar 当日未报，等下一交易日开盘（盘前竞价或 09:31 首根 1m bar）再提交
        </span>
      </div>
      <!-- 聚合统计（按当前筛选区间重算，不再是全时段累计） -->
      <table v-if="decisionSummaryView.length > 0" style="margin-bottom:14px">
        <thead><tr>
          <th>闸门</th><th>层</th><th>动作</th><th>参数</th><th>阈值</th>
          <th>次数</th><th>成交笔数</th><th>转化率</th><th>首次</th><th>末次</th><th>股票数</th><th>请求/实发</th>
        </tr></thead>
        <tbody>
          <tr v-for="(s, i) in decisionSummaryView" :key="s.gate + (s.param_name || '') + i">
            <td>{{ gateLabel(s.gate) }} <span class="gate-code">{{ s.gate }}</span></td>
            <td>{{ layerLabel(s.layer) }}</td>
            <td>{{ actionLabel(s.action) }}</td>
            <td style="color:var(--accent);font-family:monospace">{{ s.param_name || '—' }}</td>
            <td>{{ fmtGateVal(s.param_value) }}</td>
            <td style="text-align:right;font-weight:700;color:var(--red)">{{ s.count }}</td>
            <td style="text-align:right">{{ s.filled_count == null ? '—' : s.filled_count }}</td>
            <td style="text-align:right">{{ fmtRate(s.conversion_rate) }}</td>
            <td style="color:var(--ink-soft);font-size:11px">{{ s.first_bar_time ? s.first_bar_time.replace('T',' ').slice(11,19) : '—' }}</td>
            <td style="color:var(--ink-soft);font-size:11px">{{ s.last_bar_time ? s.last_bar_time.replace('T',' ').slice(11,19) : '—' }}</td>
            <td style="text-align:right">{{ s.stock_count }}</td>
            <td style="text-align:right">{{ qtyPair(s.requested_qty_sum, s.final_qty_sum) }}</td>
          </tr>
        </tbody>
      </table>

      <!-- 逐事件流 -->
      <table>
        <thead><tr><th>时间（北京时间）</th><th>闸门</th><th>层</th><th>代码</th><th>策略</th><th>阈值</th><th>实际</th><th>请求/实发</th><th>原因</th><th>确认</th></tr></thead>
        <tbody>
          <tr v-for="d in decisionRowsView" :key="d.key">
            <td style="color:var(--ink-soft)">{{ d.time || '—' }}</td>
            <td><span class="badge badge-purple">{{ gateLabel(d.gate) }}</span></td>
            <td style="color:var(--ink-soft);font-size:11px">{{ layerLabel(d.layer) }}</td>
            <td>{{ d.stock_code || '—' }}</td>
            <td>{{ d.strategy_id ? `#${d.strategy_id}` : '组合' }}</td>
            <td>{{ fmtGateVal(d.param_value) }}</td>
            <td>{{ fmtGateVal(d.actual_value) }}</td>
            <td style="text-align:right">{{ qtyPair(d.requested_qty, d.final_qty) }}</td>
            <td style="color:var(--ink-soft);white-space:normal;min-width:160px" :title="d.message">{{ d.message || '—' }}</td>
            <td style="white-space:nowrap">
              <span v-if="d.ack_status === 'acked'" class="badge badge-gray" :title="d.ack_note || '已确认'">已确认</span>
              <button
                v-else-if="d.id"
                class="btn ghost"
                style="font-size:11px;padding:1px 8px"
                :disabled="ackingEventId === d.id"
                @click="ackDecisionRow(d)"
              >确认</button>
              <span v-else style="color:var(--ink-soft)">—</span>
            </td>
          </tr>
        </tbody>
      </table>
      <div v-if="decisionRowsView.length === 0" class="empty-state">
        <p v-if="decOnlyDeferred">当前时间范围内没有顺延待次日提交的事件。</p>
        <p v-else-if="decRange === 'today'">今日暂无决策事件。止损/止盈/熔断/资金不足/超仓/收盘拦单等事件会实时显示在这里。</p>
        <p v-else>该时间范围暂无决策事件。止损/止盈/熔断/资金不足/超仓/收盘拦单等事件会实时显示在这里。</p>
      </div>
    </div>

    <!-- 预警信号：alert 模式主产物（半自动）；auto 模式信号同样落库并默认经 send_warn 推送（审计旁路） -->
    <div v-if="wbTab === 'signals'" class="table-wrap">
      <div style="display:flex;justify-content:flex-end;align-items:center;margin:0 0 6px">
        <button @click="syncSignalsBlock" class="btn btn-sm"
                title="将当前信号列表的股票代码去重后写入通达信自选板块；覆盖写入板块">同步信号股到自选</button>
      </div>
      <p v-if="signalsTotal > signals.length" style="font-size:12px;color:var(--ink-faint);margin:0 0 6px">
        共 {{ signalsTotal }} 条，仅显示最新 {{ signals.length }} 条
      </p>
      <table>
        <thead><tr>
          <th>bar 时间</th><th>组合/策略</th><th>代码</th><th>公式</th><th>信号</th>
          <th>类型</th><th>触发值</th><th>价格</th>
          <th>预警推送</th><th>推送时间</th><th>错误</th>
        </tr></thead>
        <tbody>
          <tr v-for="s in signals" :key="s.id">
            <td style="color:var(--ink-soft)">{{ fmtTime(s.bar_time) }}</td>
            <td style="color:var(--ink-soft);font-size:12px">
              {{ portfolioName(s.portfolio_id) }} / {{ strategyName(s.portfolio_id, s.strategy_id) }}
            </td>
            <td>{{ s.stock_code }}</td>
            <td style="color:var(--ink-soft)">{{ s.formula_name }}</td>
            <td>{{ s.signal_name }}</td>
            <td><span class="badge badge-blue">{{ signalTypeLabel(s.signal_type) }}</span></td>
            <td>{{ s.trigger_value }}</td>
            <td>{{ s.price ?? '—' }}</td>
            <td>
              <span class="badge" :class="alertStatusClass(s.alert_status)"
                :title="s.tdx_run_id ? `tdx_run_id: ${s.tdx_run_id}` : ''"
              >{{ alertStatusLabel(s.alert_status) }}</span>
            </td>
            <td style="color:var(--ink-soft)">{{ fmtTime(s.alerted_at) }}</td>
            <td style="color:var(--red);font-size:11px;white-space:normal;min-width:120px">{{ s.alert_error || '—' }}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="signals.length === 0" class="empty-state">
        <p>暂无预警信号。alert（半自动）模式信号触发后落库并经通达信 send_warn 推送；auto（全自动）模式默认同样推送（审计旁路），信号均落库供本页查看。</p>
      </div>
    </div>
  </div>

  <!-- B4a: 实时事件日志（多会话合并流，带会话前缀 + 过滤） -->
  <div class="card" style="margin-top:16px">
    <div class="wb-head">
      <h3 style="margin:0">实时事件日志</h3>
      <div style="display:flex;align-items:center;gap:8px">
        <select
          v-if="runningSessions.length > 1"
          class="input" style="width:auto;padding:4px 8px"
          v-model="eventFilter"
        >
          <option value="all">全部会话</option>
          <option v-for="s in runningSessions" :key="s.id" :value="s.id">#{{ s.id }} {{ s.name }}</option>
        </select>
        <span class="badge" :class="connState === 'open' ? 'badge-green' : 'badge-gray'">{{ connLabel[connState] }}</span>
      </div>
    </div>
    <div class="event-log" data-test="event-log">
      <div v-if="filteredEvents.length === 0" class="empty-state"><p>运行中 session 的信号 / 委托 / 成交 / 持仓 / 风控事件将实时显示在这里（多会话并行时事件带 [#id 名称] 前缀）</p></div>
      <div v-for="ev in filteredEvents" :key="ev.id" class="event-row">
        <span class="event-time">{{ ev.time }}</span>
        <span class="badge" :class="`badge-${EVENT_TYPE_COLOR[ev.type] || 'gray'}`">{{ ev.type }}</span>
        <span class="event-text">{{ ev.text }}</span>
      </div>
    </div>
  </div>

  <div v-if="showCreate" class="modal-overlay modal-lg" @click.self="showCreate = false">
    <div class="modal-content">
      <h3>新建实盘</h3>
      <div class="modal-body">
      <label>名称</label><input v-model="form.name" placeholder="例如：模拟盘A" />
      <label>模式</label><select v-model="form.mode"><option value="simulation">仿真</option><option value="live">实盘</option></select>
      <label>执行模式</label>
      <select v-model="form.execution_mode">
        <option value="alert">预警（半自动，仅 TQ 行情 + send_warn 推送，不下单）</option>
        <option value="auto">自动（按交易引擎下单）</option>
      </select>
      <label>交易引擎（决定信号计算的数据源）</label>
      <select v-model="form.engine" data-field="create-engine" @change="onEngineChange">
        <option v-for="e in channel?.engines || []" :key="e.key" :value="e.key" :disabled="engineOptionDisabled(e)">
          {{ engineOptionText(e) }}
        </option>
      </select>
      <div v-for="item in occupiedEngineList" :key="`occ-${item.engine.key}`" class="engine-occupied-hint" data-test="engine-occupied-hint">
        <span>{{ item.engine.label }} 已被 会话 #{{ item.occ.session_id }}「{{ item.occ.name }}」占用（{{ opponentAccountText(item.occ) }}，{{ occupiedSinceText(item.occ) }}，{{ item.occ.status === 'stopping' ? '停止中' : '运行中' }}）。</span>
        <button class="btn btn-sm" :disabled="item.occ.status === 'stopping'" @click="stopSession(item.occ.session_id)">先停止该会话</button>
      </div>
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
        模式只对 QMT 通道选桥：simulation / live 两个模式位当前都指向 8790（410038214257，STOCK；
        该账户是 QMT 仿真账户、账户内为仿真资金）。8791 信用账户桥位保留但未启用。其它引擎的账户由通道自身决定，模式不改变下单账户。
        <span v-if="form.execution_mode === 'alert'">
          预警模式：固定走通达信 TQ 行情计算信号（与所选引擎无关）并通过 send_warn 推送，不下单。
        </span>
        <span v-else>
          自动模式：下单由所选引擎执行——通达信 TQ 直连客户端（确认面板可自动处置）；同花顺走 UI 自动化（file_builtin 文件交接）；掘金量化桥/QMT 桥须在对应终端内运行桥策略。引擎未就绪时启动直接 503 报错，不会静默换通道。
        </span>
      </p>
      <label>实盘资金基准（元）</label>
      <input v-model="form.live_capital" type="number" min="0" placeholder="留空 = 以真实通道总资产为基准" data-field="live-capital" />
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
        默认取真实通道总资产，<b>只缩不放</b>：填写值只能小于当前真实总资产（超过会被拒绝保存）；留空 = 直接以真实总资产为基准。
        起账现金 = 基准 × 组合资金占比 − 该组合继承仓成本（起账按成本估值，首日浮亏即真实持仓浮亏）；
        多组合共享同一真实账户、各自独立记账，池外持仓只展示/对账，不进虚拟现金与净值。
      </p>
      <div v-if="accountPreview?.total_asset != null" class="baseline-preview" data-test="account-preview">
        当前真实总资产：<b>{{ formatMoney2(accountPreview.total_asset) }}</b>
        <span v-if="accountPreview.captured_at" style="color:var(--ink-faint)">（采集于 {{ accountPreview.captured_at }}）</span>
        ，填写值不能超过该金额。
      </div>
      <div v-else-if="accountPreviewError" class="baseline-preview warn" data-test="account-preview-error">
        通道未就绪，无法读取真实总资产；可先用固定金额起账，通道就绪后手动重置。
      </div>
      <p v-if="createError" class="form-error" data-test="baseline-error">{{ createError }}</p>
      <label>外部信号文件路径（通达信条件预警输出，可选）</label>
      <input v-model="form.signal_file_path" placeholder="留空 = 走 TQ 公式计算信号；填写完整文件路径" />
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
        填写后（如 D:\project\分时突击.txt）引擎定时读取该文件的新增信号行并注入交易链路——绕过 TQ 公式引擎的周期限制（如 1m 级别）。文件由通达信「条件预警」功能动态输出（GBK 追加写入）。所有级别公式均可走此链路。填写即<strong>替换信号来源</strong>：该会话不再跑 TQ 公式计算（风控/仓位/资金等限制条件仍沿用回测口径）；仅自动模式生效，预警模式不读取该文件。
      </p>
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
        <strong>买卖方向</strong>：信号动作分买入类（OPEN / ADD）与卖出类（REDUCE / CLOSE），方向由该策略公式信号配置的 <code>signal_type</code> 决定（预警文件本身不含方向列）。<br />
        ① <strong>买入类</strong>：沿用原有开仓闸门（资金占比 / 最大持仓数 / 单仓金额 / 敞口上限 / 可用现金），满槽或已持仓跳过；槽位上限取各组合自己的 <code>strategies.max_positions</code>（缺失则跳过槽位闸并告警，不写死 5）。<br />
        ② <strong>卖出类</strong>：不做仓位与资金检查，但受两条硬约束——<strong>无持仓不卖</strong>（不得卖空）、<strong>T+1 实际可用为 0 时拒绝</strong>（沿用实盘 T+1 可用股数校验，不绕过）。<br />
        ③ <strong>减仓比例</strong>：预警文件 8 列没有数量/比例列，<strong>REDUCE 未给出比例时按清仓执行</strong>（日志与风控事件明确标注「未给出比例，按清仓执行」，不静默猜测百分比）；CLOSE 恒清仓。<br />
        ④ <strong>去重</strong>：同代码 + 同日 + 同方向各认第一条（买卖互不吞并；同方向重复推送不重复下单）。未知/不可解析的动作一律忽略并节流告警，绝不猜测方向或默认成买入。
      </p>
      <label style="display:flex;align-items:center;gap:8px;margin-top:4px">
        <input v-model="form.advance_close_enabled" type="checkbox" style="width:auto" />
        14:50 提前收盘价（B 可选增强，仅日线 1d，当日尾盘报单）
      </label>
      <template v-if="form.advance_close_enabled">
        <label>提前收盘价执行时点</label>
        <select v-model="form.advance_close_time">
          <option value="14:51">14:51（默认）</option>
          <option value="14:52">14:52</option>
          <option value="14:55">14:55</option>
          <option value="14:57">14:57</option>
        </select>
        <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
          <strong>默认（不勾选）</strong>：1d/1w/1mon 都等周期收盘价落定后（15:05）用完整 bar 算信号，次一交易日开盘执行；14:30 只做日终估值/日亏/熔断恢复，不再产生日线级信号。周线/月线仅在周期最后一个交易日的 15:05 轮次计算。<br />
          <strong>勾选后（仅 1d 提前）</strong>：14:50 用快照合成 1m bar 的收盘价定信号，<strong>委托价取 14:52 起始 1m bar 的开盘价</strong>（考虑报价时延，取不到回退 14:51 起始 bar 的开盘价并推送 fallback 风控事件；两者都取不到则该股本日不下单），该轮 1d 信号<strong>当日尾盘提交成交</strong>（不受 risk_timing=next_day 的次日顺延）。<strong>周线/月线不受影响</strong>（仍收盘落定 + 次日执行）。数量/资金/仓位仍按 14:50 收盘价计算；成本价以柜台成交回填为准。外部信号文件模式下 1d 信号仍来自文件，本时点只补跑日线风控。仅自动模式、仅实盘；当日只执行一次（重启不重复）；会话须在 14:50 前启动。
        </p>
      </template>
      <label>错周期风控</label>
      <select v-model="form.intraday_mismatched_risk_enabled" data-field="intraday_mismatched_risk_enabled">
        <option :value="null">跟随全局（默认关闭，对齐回测）</option>
        <option :value="true">强制开启（任意 bar 都跑风控）</option>
        <option :value="false">强制关闭</option>
      </select>
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
        关闭时（全局默认）止损/止盈/移动止损只在策略<strong>自身周期</strong>的边界 bar 上评估：1m/5m 等错周期 bar 只更新价格快照，周线/月线同样只在自身边界评估——与回测口径一致。开启则任何 bar 到达都按收盘价评估持仓风险（旧行为，盘中反应更早但回测无法复现）。历史会话（未设置）恒跟随全局。
      </p>
      <label>组合策略（可多选）</label>
      <div class="portfolio-check-list">
        <label v-for="p in portfolios" :key="p.id" class="portfolio-check">
          <input v-model="form.portfolio_ids" type="checkbox" :value="p.id" data-portfolio-check />
          {{ p.name }}<span style="color:var(--ink-faint)">（#{{ p.id }}）</span>
          <span v-if="portfolioFormulas(p.id)" class="formula-tag">{{ portfolioFormulas(p.id) }}</span>
        </label>
      </div>
      <div v-if="portfolios.length === 0" class="empty-state" style="padding:8px">
        <p>暂无组合策略，请先在"组合管理"页创建</p>
      </div>
      </div>
      <div class="modal-actions">
        <button @click="create" class="btn btn-primary">确认</button>
        <button @click="showCreate = false" class="btn">取消</button>
      </div>
    </div>
  </div>

  <!-- C3 键鼠冲突二次确认：勾选风险后才可带 ack_keyboard_risk 提交 -->
  <div v-if="keyboardRisk" class="modal-overlay" @click.self="keyboardRisk = null">
    <div class="modal-content">
      <h3>键鼠冲突风险确认</h3>
      <div class="modal-body">
        <p data-test="keyboard-risk-text">
          通达信与同花顺都是键鼠 UI 自动化，同时运行可能因抢焦点/看错窗口导致误操作。
          建议先停止会话 {{ keyboardRisk.opponents[0].session_id }}「{{ keyboardRisk.opponents[0].name }}」。仍要继续吗？
        </p>
        <p style="font-size:12px;color:var(--ink-faint)">
          当前冲突会话：
          <span v-for="o in keyboardRisk.opponents" :key="o.session_id">
            #{{ o.session_id }} {{ o.name }}（{{ engineLabel(o.engine) }}，{{ opponentAccountText(o) }}，{{ occupiedSinceText(o) }}）
          </span>
        </p>
        <label style="display:flex;align-items:center;gap:8px">
          <input v-model="ackKeyboardRisk" type="checkbox" style="width:auto" data-test="keyboard-risk-ack" />
          已知键盘鼠标冲突风险
        </label>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" :disabled="!ackKeyboardRisk" data-test="keyboard-risk-confirm" @click="confirmKeyboardRisk">我已了解风险，仍要启动（不推荐）</button>
        <button class="btn" @click="keyboardRisk = null">取消</button>
      </div>
    </div>
  </div>

  <!-- 资金基准重置（收盘后）：留空=当日收盘总资产，也可再次手动填写更小值 -->
  <div v-if="baselineModal" class="modal-overlay" @click.self="baselineModal = null">
    <div class="modal-content">
      <h3>重置资金基准</h3>
      <div class="modal-body">
        <p>会话 #{{ baselineModal.session.id }}「{{ baselineModal.session.name }}」</p>
        <label>实盘资金基准（元）</label>
        <input v-model="baselineValue" type="number" min="0" placeholder="留空 = 当日收盘总资产" data-field="baseline-reset-value" />
        <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
          默认取当日收盘总资产，只缩不放：填写值只能小于真实总资产；重置不解除熔断、不清零熔断次数。
        </p>
        <p v-if="baselineError" class="form-error" data-test="baseline-reset-error">{{ baselineError }}</p>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" @click="confirmBaseline">确认重置</button>
        <button class="btn" @click="baselineModal = null">取消</button>
      </div>
    </div>
  </div>

  <!-- ============ 外部现金流台账（入金/出金/分红/其他）============ -->
  <div v-if="cashFlowModal" class="modal-overlay modal-lg" @click.self="closeCashFlows">
    <div class="modal-content">
      <h3>存取款 / 分红台账</h3>
      <div class="modal-body">
        <p>会话 #{{ cashFlowModal.session.id }}「{{ cashFlowModal.session.name }}」</p>
        <p class="cash-flow-risk" data-test="cash-flow-risk">
          重要：外部资金进出必须记台账。后端按「总市值 − 累计净流入」计算净值、收益率、回撤与日亏，
          漏记会让这些指标被外部资金扭曲（可能误触熔断，或掩盖真实亏损）。
        </p>
        <div class="form-group">
          <label>登记一笔</label>
          <div class="cash-flow-form">
            <select v-model="cashFlowForm.portfolio_id" data-field="cash-flow-portfolio">
              <option :value="null" disabled>选择组合</option>
              <option v-for="pid in sessionPortfolioIds(cashFlowModal.session)" :key="pid" :value="pid">
                {{ portfolioName(pid) }}
              </option>
            </select>
            <select v-model="cashFlowForm.flow_type" data-field="cash-flow-type">
              <option v-for="t in CASH_FLOW_TYPES" :key="t.value" :value="t.value">{{ t.label }}</option>
            </select>
            <input v-model="cashFlowForm.amount" type="number" step="0.01"
              placeholder="金额（元）" data-field="cash-flow-amount" />
            <input v-model="cashFlowForm.flow_date" type="date" data-field="cash-flow-date" />
            <input v-model="cashFlowForm.note" type="text"
              placeholder="备注（可选）" data-field="cash-flow-note" />
            <button class="btn btn-primary" :disabled="cashFlowSaving" @click="submitCashFlow">
              {{ cashFlowSaving ? '提交中…' : '登记' }}
            </button>
          </div>
          <p class="field-hint" style="font-size:12px;color:var(--ink-faint)">
            入金 / 出金 / 分红填正数（方向由类型决定）；「其他」按正负判方向（正 = 流入）。
          </p>
          <p v-if="cashFlowError" class="form-error" data-test="cash-flow-error">{{ cashFlowError }}</p>
        </div>
        <table v-if="cashFlows?.flows?.length" class="table">
          <thead>
            <tr><th>日期</th><th>组合</th><th>类型</th><th>方向</th><th>金额（元）</th><th>备注</th></tr>
          </thead>
          <tbody>
            <tr v-for="f in cashFlows.flows" :key="f.id">
              <td>{{ f.flow_date || '—' }}</td>
              <td>{{ portfolioName(f.portfolio_id) }}</td>
              <td>{{ CASH_FLOW_TYPE_LABEL[f.flow_type] || f.flow_type }}</td>
              <td>{{ cashFlowDirection(f) }}</td>
              <td>{{ formatMoney2(Number(f.amount)) }}</td>
              <td>{{ f.note || '—' }}</td>
            </tr>
          </tbody>
        </table>
        <div v-else class="empty-state"><p>暂无台账记录</p></div>
        <div v-if="cashFlows" class="cash-flow-summary" data-test="cash-flow-summary">
          <span v-for="pid in sessionPortfolioIds(cashFlowModal.session)" :key="pid">
            {{ portfolioName(pid) }} 累计净流入：{{ netInflowOf(pid) }} 元
          </span>
        </div>
      </div>
      <div class="modal-actions">
        <button class="btn" @click="closeCashFlows">关闭</button>
      </div>
    </div>
  </div>

  <!-- ============ 实盘配置指南 Modal（M2）============ -->
  <div v-if="showGuide" class="modal-overlay modal-lg" @click.self="showGuide = false">
    <div class="modal-content">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:8px">
        <h3 style="margin:0">实盘配置指南</h3>
        <button class="btn btn-sm" @click="showGuide = false">关闭</button>
      </div>
      <div class="modal-body" style="margin-top:12px">
        <div class="form-group">
          <label class="group-label">一、交易通道选择（四引擎对比）</label>
          <div class="guide-block">
            <p class="guide-step"><b>通达信 TQ（tdx）</b>：同进程 TQ SDK 直调，撤单/价格校验/大额拆单全支持；
              下单确认面板可自动处置（config.tq_trade.auto_confirm_dialog，默认开）——<b>无人值守首选</b>。</p>
            <p class="guide-step"><b>同花顺（ths）</b>：UI 自动化（键鼠模拟），execute_mode 双模式——
              file_builtin（默认，指令落 JSONL 交内置执行器）/ direct（同步直呼）；price_mode=approval（默认）/
              latest（本机 latest，按最新价键入）；撤单已自动化（F3 撤单页双击）。
              运行期间勿触碰键鼠（UI 独占前台），同花顺通道同时仅一个会话。</p>
            <p class="guide-step"><b>掘金量化（goldminer）</b>：HTTP 桥（goldminer_bridge.py 跑在掘金终端内，
              127.0.0.1:7801）；持仓/资金/委托经桥实时读回。</p>
            <p class="guide-step"><b>QMT 通道（iquant）</b>：<b>通道定位</b>——四条生产通道之一，与掘金、
              同花顺、通达信同级，平台侧正式接入并按生产标准维护；<b>账户资金属性</b>——当前
              iquant_bridge.py（127.0.0.1:8790）绑定的 410038214257（STOCK）是 <b>QMT 仿真账户，
              账户内是仿真资金</b>，不是真实资金。会话「模式」simulation / live 两个位都指向这一个桥；
              iquant_bridge_live.py（8791 信用账户 CREDIT 410009993017）仓库保留但<b>非生产、未启用</b>，
              无需部署。切真实资金须改桥文件顶部 ACCOUNT / ACCOUNT_TYPE（必要时 TICK_TAG / DRY_RUN）
              并重跑实盘验证，不能只改 config.yaml。其它引擎的「模式」不改变下单账户。</p>
            <p class="guide-note">· 委托认领：QMT/掘金桥把 bridge_order_id 写进柜台 remark，按 remark 精确认领；
              通达信/同花顺不支持 remark，走「代码+方向+数量+提交时间窗」模糊匹配（陌生遗留单会降级排除）。</p>
            <p class="guide-note">· 撤单能力：仅通达信/同花顺实现 cancel_order；平台对 QMT/掘金桥不下发撤单
              （掘金桥虽有 /cancel 端点但平台未接），超时在途单按 expire 处理（柜台可能仍挂单，需人工撤），
              requote 追价重下对桥通道不生效。</p>
            <p class="guide-note">· 默认引擎：系统配置 → 通达信 TQ 通道 → 默认交易引擎（tq_trade.default_engine，
              当前 tdx）。engine 为空的历史会话、已移除的旧 `qmt`（miniQMT）会话键都回退该默认；
              未配置/非法时回退 ths_ui.enabled 且依赖可用→ths，否则 iquant。</p>
            <p class="guide-note">· 就绪门槛：启动前探活；客户端未启动/未登录、依赖缺失、桥未运行、路径账号未配置时
              直接 503 并给出可操作中文提示，不会静默换通道。</p>
            <p class="guide-note">· 信号前置：实盘公式必须是通达信导入公式（engine=tq）；全自动仅支持
              signal_frequency == period（跨周期信号请改预警模式）。</p>
          </div>
        </div>

        <div class="form-group">
          <label class="group-label">二、桥策略部署（掘金桥 / QMT 生产桥）</label>
          <div class="guide-block">
            <p class="guide-step"><b>掘金量化桥</b>：文件 live/bridge/goldminer_bridge.py，部署在掘金终端
              「我的策略」内；顶部 TOKEN / ACCOUNT / STRATEGY_ID 必须替换为新账户与终端分配的策略 ID
              （仓库内 STRATEGY_ID 是占位符）；HOST/PORT 默认 127.0.0.1:7801，须与 config.goldminer.base_url
              一致。gm SDK 装在系统 Python（掘金终端使用的解释器），<b>禁止装进 main/.venv</b>。</p>
            <p class="guide-step"><b>QMT 生产桥</b>：只部署 iquant_bridge.py（127.0.0.1:8790，账号
              410038214257，STOCK；<b>该账户是 QMT 仿真账户，账户内为仿真资金</b>），文件粘贴进 QMT
              客户端策略编辑器运行（客户端自带 Python 3.6.8）。切真实资金须改桥文件顶部
              ACCOUNT / ACCOUNT_TYPE（必要时 TICK_TAG / DRY_RUN）并重跑实盘验证，不能只改平台配置。
              iquant_bridge_live.py（8791，信用账户 CREDIT 410009993017）<b>非生产、未启用</b>，
              换机不必部署；但两份文件须保持镜像（除 PORT / ACCOUNT / ACCOUNT_TYPE / TICK_TAG /
              /ping 的 service / 日志前缀 / strategyName 外逐字一致），改动须手动同步两份。</p>
            <p class="guide-note">· <b>启动方式</b>：桥策略只能由对应终端点「运行」启动。禁止用 uv run / 平台 venv
              启动掘金桥（会把 gm 装进平台环境，下次 uv sync 删掉后桥真断）；掘金终端关闭 = 桥停 = 报单按超时
              expire 兜底。改完桥文件必须停止再启动策略才生效。</p>
            <p class="guide-note">· <b>平台侧配置</b>（config.yaml，只读核对）：iquant_bridge.simulation/live.base_url、
              goldminer.base_url（掘金另含 request_timeout_seconds=30 / slow_response_seconds=15 /
              slow_warn_interval_seconds=300）、tq_trade.*、ths_ui.work_dir/execute_mode/price_mode。
              本机当前实际：simulation=8790；live=8790（两位都指向 QMT 仿真账户 410038214257，不使用 8791 信用腿）；
              goldminer=7801；ths execute_mode=file_builtin、price_mode=latest、work_dir=data/ths。</p>
            <p class="guide-note">· <b>卖出范围</b>（tq_trade.live_sell_scope）：full＝按整仓卖（默认，含外部仓）；
              platform_only＝只卖平台自建仓，纯外部仓不代卖并记一条 gate=sell_scope / action=skip。
              本机 platform_only；买入侧不受影响。</p>
            <p class="guide-note">· <b>次日盘前集合竞价</b>（tq_trade.pre_open_auction_*）：enabled 默认 <b>true</b>、
              end_time 默认 09:20、price_bias_pct 默认 2.0。窗口 09:15 ≤ now &lt; end_time；当日 due 的
              <b>1d/1w/1mon</b> deferred 单统一生效（due 判定 = signal_bar_date 的下一交易日）；价＝信号价×(1±bias%)
              并夹进当日涨跌停；每单每日至多一次（at-most-once）。未报出的单由 09:31 首根 1m bar 兜底：
              <b>限价 = 当日首根 1m 开盘价</b>（取不到退回实时 close 并记 deferred_release/fallback 事件，
              夹涨跌停后 quantize 到分）；tq_trade.deferred_release_ensure_fill（默认 false）开启才改对手价
              「保证成交」。开盘后若开盘价已越过限价（买限价 &lt; 开盘价 / 卖限价 &gt; 开盘价）即判必然未成交 →
              <b>主动撤单</b>并记 pre_open_auction/cancel（09:20–09:25 为交易所不可撤窗口，该段内不发撤单，
              顺延到 09:30 之后）。盘前单<b>按单</b>下发 price_type=limit，
              <b>无需</b>会话级或 config.yaml 里的 price_type=limit；HTTP 桥通道的闸门判据为桥 /ping 自述支持 limit 或提供
              /price_limit（通达信/同花顺按各自通道能力），若本会话盘前单 /order 回显非 limit 则直接拦下该单。</p>
            <p class="guide-note">· <b>错周期风控</b>（会话三态 intraday_mismatched_risk_enabled / 全局
              tq_trade.intraday_mismatched_risk_enabled，默认 false＝对齐回测）：关闭时止损/止盈/移动止损只在
              策略自身周期的边界 bar 上评估，1m/5m 等错周期 bar 只更新价格快照（周线/月线同样只在自身边界评估）；
              会话置 true 可恢复「任意 bar 都跑风控」的旧行为。会话级 NULL＝跟随全局。</p>
            <p class="guide-note">· <b>桥超时</b>：live.bridge_read_timeout_seconds=15（单次读取）、
              bridge_connect_timeout_seconds=3（建连）、bridge_timeout_seconds=90（持续离线判死阈值，语义不同）；
              iquant_bridge.request_timeout_seconds 默认 15，掘金保持 30（/quote 首次走云端历史服务慢）。</p>
            <p class="guide-note">· <b>安全</b>：桥文件与 config.yaml 均含明文 TOKEN / 账户号，且桥绑 127.0.0.1 无鉴权；
              分发/拷贝时只在可信机器间进行，勿提交到公开仓库或聊天群。</p>
          </div>
        </div>

        <div class="form-group">
          <label class="group-label">三、会话与组合</label>
          <div class="guide-block">
            <p class="guide-step"><b>一个会话 = 一个引擎 + N 个组合</b>（新建实盘多选）；组合内多策略并行。</p>
            <p class="guide-step"><b>多会话并行</b>：跨引擎可并行（不同账户）；<b>同一引擎同时仅一个会话</b>
              （tdx/iquant/goldminer/ths 同理，同引擎双会话会双份记账交易同一账户；同花顺另有键鼠独占）。</p>
            <p class="guide-step"><b>复制交易</b>：同一组合挂多个会话需选<b>不同引擎</b>（= 不同账户），
              各会话独立记账、同一信号各账户各执行一次（执行偏差自担）；
              live_capital 按各账户「总资产」设置（可用资金 + 持仓市值），只作真实侧总闸上限
              （桥快照缺失时兜底，不足时按各组合配置资金占比缩量）；各组合虚拟起账恒取自身
              initial_capital，继承持仓市值从该组合起账现金扣除。</p>
            <p class="guide-note" style="color:var(--red)"><b>禁止</b>：同一资金账号挂两个会话（资金竞争）。</p>
            <p class="guide-step"><b>日线级别默认口径（1d/1w/1mon）</b>：都等<b>周期收盘价落定</b>后
              在 15:05 轮次用完整 bar 算信号，<b>次一交易日开盘执行</b>；14:30 只做日终估值 /
              日亏 / 熔断恢复，不再驱动日线级信号。1w 只在每周最后一个交易日、1mon 只在每月
              最后一个交易日的 15:05 轮次计算（周五 / 月末休市自动前移到前一交易日），其余日子跳过。</p>
            <p class="guide-step"><b>14:50 提前收盘价（B，可选增强，仅 1d，advance_close_enabled）</b>：
              会话级开关，默认关。开启后仅 1d 提前——14:50 用快照合成 1m bar 收盘价定信号，
              <b>委托价取 14:52 起始 1m bar 的开盘价</b>（考虑报价时延；取不到回退 14:51 起始
              bar 的开盘价并推送 fallback 风控事件；两者都取不到则该股本日不下单），<b>该轮信号
              当日尾盘提交成交</b>（不受 risk_timing 的次日顺延）。<b>周线/月线不受影响</b>
              （仍走 15:05 收盘落定 + 次日执行）。时点限定 14:51–14:59，实际轮次不早于 14:52。
              四引擎通用、仅实盘 auto 生效、回测忽略；当日只执行一次（跨重启幂等，DB
              advance_close_done_date）。外部信号文件模式下 1d 信号仍来自文件，该时点只补跑日线风控。
              14:52/14:51 开盘价都取不到则不下单（不伪造价格，ERROR 日志 + risk 事件可见）；
              会话须在 14:50 前启动。</p>
            <p class="guide-note">· 与策略级「风控执行时点 risk_timing」的区别：risk_timing 决定
              <b>15:05 收盘落定轮次 / 盘中轮次</b>算出的单是否顺延次日（策略级）；提前收盘价的
              1d 轮次则<b>固定当日提交</b>（等价于该轮强制 immediate）。两者不冲突：未开启提前
              收盘价时按 risk_timing 执行；开启后该轮不受 next_day 影响。</p>
          </div>
        </div>

        <div class="form-group">
          <label class="group-label">四、实盘执行配置（策略表单「实盘执行」分组）</label>
          <div class="guide-block">
            <p class="guide-step"><b>撤单后追价重下 requote</b>：超时未成交 → 自动撤单 → 追 1 tick 重下
              （最多 requote_max_attempts 次）；默认关。依赖通道撤单能力（通达信/同花顺），桥通道不生效。</p>
            <p class="guide-step"><b>大额拆单（仅 TDX）</b>：单笔 ≥ split_order_threshold → 拆 N 子单，成交驱动续发；默认关。</p>
            <p class="guide-step"><b>风控执行时点 risk_timing</b>：next_day（默认，与回测一致——日线信号
              次日开盘执行）/ immediate（当日确认当日执行，仅盘中周期有效；15:05 收盘落定轮次与
              提前收盘价之外的日线单仍按本设置）。仅实盘生效，回测忽略（回测固定 T+1）；
              15:05 收盘落定轮次产生的单<b>强制次日执行</b>（收盘后报单报不进交易所，immediate 也不例外）；
              开启「14:50 提前收盘价」后，该轮 1d 信号固定当日提交，不受此设置影响（见第三节）。</p>
            <p class="guide-note">· 上述默认关闭 = 与回测行为一致；开启后建议先仿真观察。</p>
            <p class="guide-note">· <b>卖出同票在途去重</b>（gate=inflight_sell）：同（策略, 代码）已有未结卖单时，
              目标量先减在途未成交量——≤0 整单跳过，&gt;0 按差额收窄（收窄后记 reduce）。</p>
            <p class="guide-note">· <b>事件去重</b>：order/submitted 不再重复推送两条；受理未认领时只补一条
              order_unconfirmed 决策事件说明状态。</p>
          </div>
        </div>

        <div class="form-group">
          <label class="group-label">五、原有持仓</label>
          <div class="guide-block">
            <p class="guide-step">账户存量持仓中、属于本会话股票池的部分会导入虚拟账本，按<b>真实成本</b>纳入止损/止盈/移动止损风控；
              非策略买入的部分在工作台以灰色「<b>原有持仓</b>」标注来源，与蓝色「策略持仓」并列展示。</p>
            <p class="guide-note">· <b>池外外部仓不再兜底归属</b>：只进持仓展示与对账，不进风控、不占槽位、不代卖；
              每会话每日落一条汇总决策事件（gate=out_of_pool_position / action=notice）+ 日志 + SSE，
              逐票明细只在日志/SSE；对账差异含原有持仓属正常。</p>
            <p class="guide-note">· <b>成本未知</b>：柜台成本 ≤ 0（送转分红后为负，或柜台返回 0）视为成本未知，
              不参与止损/止盈/移动止损，持仓表显示「成本未知」（不再用市值÷数量推算）。</p>
          </div>
        </div>

        <div class="form-group">
          <label class="group-label">六、执行模式</label>
          <div class="guide-block">
            <p class="guide-step"><b>预警（alert）</b>：固定走通达信 TQ 行情计算信号 + send_warn 推送 + 落库，
              不下单——半自动（此时所选引擎只用于记录，不参与取数）。</p>
            <p class="guide-step"><b>自动（auto）</b>：全套自动下单——按所选引擎执行；同花顺 file_builtin 下
              指令/回执落在 {ths_ui.work_dir}/{session_id}/（signals/risk/orders/results + cursor.json）。</p>
            <p class="guide-note">· THS 自动模式已真机验证（下单→成交→撤单全链路）；首次实盘建议小单先行；
              运行期间勿触碰键鼠。</p>
          </div>
        </div>

        <div class="form-group">
          <label class="group-label">七、风险提示</label>
          <div class="guide-block">
            <p class="guide-note">· 同一资金账号禁止挂两个会话。</p>
            <p class="guide-note">· 同花顺真实账户操作请先小单验证。</p>
            <p class="guide-note">· 佣金/印花税按柜台真实回报记账；下单前费用为估算。</p>
            <p class="guide-note">· 内置休市表目前只覆盖 2026 年；2027 年及以后无权威日历时按工作日 fail-open
              并在启动时打 WARNING，需在交易所公布休市安排后补表。</p>
            <p class="guide-note">· <b>前端生效</b>：前端改动需 npm run build 重建 web/dist 才生效
              （FastAPI 直接托管 dist，无需重启后端）。</p>
          </div>
        </div>
      </div>
      <div class="modal-actions">
        <button @click="showGuide = false" class="btn btn-primary">知道了</button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.channel-bar {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  padding: 10px 14px;
  margin-bottom: 12px;
}
.channel-label { font-size: 13px; color: var(--ink-soft); }
.channel-hint { font-size: 12px; color: var(--ink-faint); }
/* C4 多账户并行横幅：提示不阻断，可关闭 */
.multi-account-banner {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 14px;
  margin-bottom: 12px;
  font-size: 13px;
  border-left: 4px solid var(--orange, #e67e22);
  background: rgba(230, 126, 34, 0.08);
}
/* 引擎下拉占用引导（C1） */
.engine-occupied-hint {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin: 2px 0 10px;
  font-size: 12px;
  color: var(--ink-soft);
}
/* 新建弹窗真实总资产预览 / 内联校验错误 */
.baseline-preview { font-size: 12px; color: var(--ink-soft); margin: -6px 0 12px; }
.baseline-preview.warn { color: var(--orange, #e67e22); }
.form-error { font-size: 12px; color: var(--red); margin: -6px 0 12px; }
/* start 409 行内红字：指名占用者，不笼统报「启动失败」 */
.start-error {
  margin-top: 6px;
  max-width: 320px;
  font-size: 12px;
  line-height: 1.5;
  color: var(--red);
  white-space: normal;
}
/* start 返回的通道建议类告警（非阻断） */
.start-warning {
  margin-top: 6px;
  max-width: 320px;
  font-size: 12px;
  line-height: 1.5;
  color: var(--orange, #e67e22);
  white-space: normal;
}
/* A1 新建会话软告警横幅 */
.create-warning-banner {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 14px;
  margin-bottom: 12px;
  font-size: 13px;
  line-height: 1.6;
  border-left: 4px solid var(--orange, #e67e22);
  background: rgba(230, 126, 34, 0.08);
}
/* A7 参数漂移横幅：差异字段逐条列出 */
.drift-banner {
  padding: 10px 14px;
  margin-bottom: 12px;
  border-left: 4px solid var(--red);
  background: rgba(231, 76, 60, 0.06);
  font-size: 13px;
}
.drift-head { display: flex; align-items: center; gap: 10px; margin-bottom: 6px; }
.drift-fields { margin: 4px 0 6px; }
.drift-fields ul { margin: 0; padding-left: 20px; line-height: 1.7; }
.drift-hint { font-size: 12px; color: var(--ink-faint); }
/* 顺延待次日提交汇总条（点击切换事件表筛选） */
.defer-banner {
  margin: 0 0 10px;
  padding: 8px 12px;
  border-left: 4px solid #d97706;
  background: rgba(217, 119, 6, 0.08);
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  user-select: none;
}
.defer-banner--active { background: rgba(217, 119, 6, 0.18); }
.defer-hint { font-size: 12px; font-weight: normal; color: var(--ink-faint); margin-left: 8px; }
/* 外部现金流台账弹窗 */
.cash-flow-risk {
  font-size: 12px;
  line-height: 1.6;
  color: var(--orange, #e67e22);
  background: rgba(230, 126, 34, 0.08);
  padding: 8px 10px;
  border-radius: 4px;
  margin: 4px 0 12px;
}
.cash-flow-form { display: flex; flex-wrap: wrap; gap: 8px; align-items: center; }
.cash-flow-form input, .cash-flow-form select { width: auto; min-width: 110px; }
.cash-flow-summary {
  display: flex;
  flex-wrap: wrap;
  gap: 16px;
  margin-top: 8px;
  font-size: 12px;
  color: var(--ink-soft);
}
.formula-tag { font-size: 11px; color: var(--accent); background: var(--accent-light); padding: 1px 6px; border-radius: 3px; margin-left: 4px; }
.event-log {
  max-height: 420px;
  overflow-y: auto;
  border: 1px solid var(--line);
  border-radius: 6px;
  padding: 4px 8px;
  font-size: 13px;
  background: var(--hover-bg);
}
.event-row {
  display: flex;
  align-items: baseline;
  gap: 8px;
  padding: 4px 0;
  border-bottom: 1px dashed var(--line);
}
.event-row:last-child { border-bottom: none; }
.event-time { color: var(--ink-faint); font-variant-numeric: tabular-nums; flex-shrink: 0; }
.event-text { word-break: break-all; }
.portfolio-check-list {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
  margin: 4px 0 12px;
}
.portfolio-check {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  padding: 6px 10px;
  border: 1px solid var(--line);
  border-radius: 6px;
  cursor: pointer;
  font-size: 13px;
  background: var(--hover-bg);
}
.portfolio-chip {
  display: inline-block;
  margin-right: 6px;
}
.gate-code {
  margin-left: 6px;
  padding: 0 6px;
  border-radius: 4px;
  background: var(--hover-bg);
  color: var(--ink-faint);
  font-size: 11px;
  font-weight: 400;
}
/* 工作台标题行与小屏适配：标题与标签/下拉/按钮允许换行，按钮不被挤出可视区 */
.wb-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-bottom: 8px;
}
.wb-tabs { display: flex; align-items: center; flex-wrap: wrap; gap: 4px; margin-left: auto; }
.wb-tabs select { margin-right: 8px; }
@media (max-width: 1180px) {
  .channel-hint { flex-basis: 100%; }
  .wb-tabs { margin-left: 0; }
}
/* 视口高度不足时事件日志不再霸占过多高度（其余内容在 .content 内滚动） */
@media (max-height: 820px) {
  .event-log { max-height: min(420px, 34vh); }
}
</style>
