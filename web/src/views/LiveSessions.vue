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
  ackLiveDecision, previewLiveDecisionBatch, batchAckLiveDecisions,
  type DecisionBatchFilters, type DecisionBatchPreview,
  getLiveSessions, getLiveSession, createLiveSession, updateLiveSession, startLiveSession, stopLiveSession, deleteLiveSession,
  setLiveSessionSellScope,
  getLiveSessionChannelHealth, failoverLiveSession,
  getLiveDataHealth,
  getLiveChannelGuard, reconnectLiveChannel,
  getLiveSplitRemainders, reorderLiveSplitRemainders,
  getLiveDeferredOrders, releaseLiveDeferredOrder, abandonLiveDeferredOrder,
  getLivePreOpenStats,
  type LiveDeferredOrderItem, type LivePreOpenBucket,
  getLiveGateSnapshots,
  type LiveSplitRemainderItem,
  type LiveGateSnapshotItem,
  type LiveSessionChannelHealth, type LiveChannelGuard,
  recoverLiveBreaker, getLiveChannel, testLiveChannel, sendUserBlock, getFormulas, getWatchdogStatus,
  getLiveAccountPreview, resetLiveBaseline,
  getLiveParamsDrift, getLiveCashFlows, createLiveCashFlow,
  getTheoryVsLive, getBacktestRecords, listPairingPlans,
  type TheoryVsLiveResult, type BacktestRecordItem, type PairingPlanSummary,
  type CapitalInheritanceReport,
  type PortfolioItem, type LiveSessionItem,
  type LiveChannelInfo, type LiveChannelEngine, type LiveEngineKey, type LiveExecutionMode, type LiveSignalItem,
  type FormulaItem, type WatchdogAlarm, type ParamsDrift, type LiveCashFlowList, type LiveCashFlowType,
} from '../api'
import PageHeader from '../components/PageHeader.vue'
import ApprovalPanel from '../components/ApprovalPanel.vue'
import { deriveBlockCode } from '../utils/blockCode'

const sessions = ref<LiveSessionItem[]>([])
const loading = ref(true)
// 当前生效的下单通道（全局配置 ths_ui.enabled 决定，切换需重启运行中 session）
const channel = ref<LiveChannelInfo | null>(null)
const channelTesting = ref(false)
const channelTestMsg = ref<{ ok: boolean; text: string } | null>(null)
const portfolios = ref<PortfolioItem[]>([])  // 全量组合策略,供新建实盘多选 + 会话列表解析名称
const formulas = ref<FormulaItem[]>([])  // 公式列表（formula_id → name 映射，用于组合策略标注公式名）
// F5 拼盘会话：仅展示已批准（APPROVED）的拼盘，供新建/编辑绑定
const pairingPlans = ref<PairingPlanSummary[]>([])
const pairingPlansLoading = ref(false)
async function loadPairingPlans(force = false) {
  if (pairingPlansLoading.value) return
  if (!force && pairingPlans.value.length > 0) return
  pairingPlansLoading.value = true
  try {
    const data = await listPairingPlans({ status: 'APPROVED' })
    pairingPlans.value = Array.isArray(data) ? data : []
  } catch {
    pairingPlans.value = []
  } finally {
    pairingPlansLoading.value = false
  }
}
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
  // A1 资金池：off 默认；其余模式启动首 bar 按权重分配各组合虚拟现金
  capital_pool_mode: 'off' as 'off' | 'equal' | 'risk_budget' | 'priority',
  capital_pool_rebalance_enabled: false,
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
  // E1：执行前重判门控（默认关）——顺延次日单/盘前竞价单提交前再判一次
  gate_recheck_on_execute: false as boolean,
  // G2：盘前参数会话级覆盖（null / '' = 跟随全局）
  pre_open_auction_enabled: null as boolean | null,
  pre_open_auction_end_time: '' as string,
  pre_open_auction_price_bias_pct: null as number | string | null,
  // F1/F5：绑定已批准拼盘（三选一；空=不绑定，走普通实盘）
  pairing_plan_id: null as number | null,
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
// 启动中的行（防止重复点击 + 给出明确「启动中…」反馈；后端可能因数据体检耗时）
const startingIds = ref<Set<number>>(new Set())
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
// start 返回的通道建议 / 启动体检告警（按会话展示，非阻断）。
// 提示类行为：展示 START_WARNING_TTL_MS 后自动收起；原文同时写入「实时事件日志」
// 长期留存，收起后仍可回看（此前只存在这个 ref 里，不切页面重建组件就永远常驻）。
const startWarnings = ref<Record<number, string[]>>({})
const START_WARNING_TTL_MS = 45_000
const startWarningTimers = new Map<number, ReturnType<typeof setTimeout>>()
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
// 分钟级策略提示：组合含日线以下周期时，实盘 1m 由快照合成，与回测 TQ 原生 1m 不等价。
const MINUTE_LIVE_PERIODS = ['1m', '5m', '15m', '30m', '1h']
const minuteLivePeriods = computed(() => {
  const hit = new Set<string>()
  for (const p of portfolios.value as any[]) {
    for (const s of ((p as any)?.strategies || [])) {
      const period = String(s?.period || '')
      if (MINUTE_LIVE_PERIODS.includes(period)) hit.add(period)
    }
  }
  return Array.from(hit).sort()
})
const minuteNoticeDismissed = ref(false)
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
// B4 能力矩阵：随选中引擎展示支持/不支持项，避免配置了通道做不到的选项
const selectedEngineCaps = computed<Record<string, boolean>>(() =>
  channel.value?.engines?.find((e) => e.key === form.value.engine)?.capabilities || {})
function capabilityLabel(key: string): string {
  return channel.value?.capability_labels?.[key] || key
}
// 引擎中文名（列表与弹窗共用；后端 engines 清单里有 label 时优先）
const ENGINE_LABEL: Record<string, string> = {
  goldminer: '掘金量化', iquant: 'QMT 通道', tdx: '通达信', ths: '同花顺',
}
function engineLabel(key?: string): string {
  if (!key) return ENGINE_LABEL.iquant
  return channel.value?.engines?.find((e) => e.key === key)?.label || ENGINE_LABEL[key] || key
}

// 执行模式中文名（alert=预警/auto=自动/manual=审批/shadow=影子）；老数据缺省视作 auto
const EXECUTION_MODE_LABEL: Record<string, string> = { alert: '预警', auto: '自动', manual: '审批', shadow: '影子' }
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
// G1：待放行的顺延委托（运行中人工放行/放弃；DB 权威 + 内存可达性）
const deferredOrders = ref<LiveDeferredOrderItem[]>([])
const deferredRunning = ref(false)
const deferredBusy = ref<number | null>(null)
// G4：盘前效果统计（按日/周，只读）
const preOpenStats = ref<LivePreOpenBucket[]>([])
const preOpenTotals = ref<Omit<LivePreOpenBucket, 'bucket'> | null>(null)
const preOpenGroupBy = ref<'day' | 'week'>('day')
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
// 原生分钟数据源（会话详情 minute_source）：默认关闭时展示启用说明；开启后
// 常驻展示最后前进时刻，停顿时按滞后秒数升级为醒目告警。
const minuteSource = ref<any>(null)
const minuteSourceAlert = computed(
  () => minuteSource.value?.enabled === true && minuteSource.value?.stalled === true,
)
const minuteSourceText = computed(() => {
  const s = minuteSource.value || {}
  const last = s.last_advance_at ? String(s.last_advance_at) : '从未前进'
  const lag = typeof s.lag_seconds === 'number' ? `${s.lag_seconds} 秒` : '未知'
  return { last, lag }
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
type WbTab = 'positions' | 'orders' | 'approvals' | 'trades' | 'decisions' | 'signals' | 'gate'
const wbTab = ref<WbTab>('positions')
let initialTabFromQuery = false
const TAB_LABEL: Record<WbTab, string> = {
  positions: '持仓', orders: '委托', approvals: '待审批', trades: '成交', decisions: '决策/拦截',
  signals: '预警信号', gate: '门控快照',
}

// ---- 决策/拦截页签：自带刷新 + 切页签重拉 ----
// 旧版只有挂载/切会话/reconnect/「刷新持仓」按钮会重拉历史，收盘后看当天移动止损与
// 顺延事件常常是空表（页签没数据也没入口）——这里给决策页签独立刷新入口。
const decisionsLoading = ref(false)
/** 未处理（open 且未归档）/已确认/已归档计数（后端全量口径）。 */
const decisionAckSummary = ref<{ open: number; acked: number; archived: number }>(
  { open: 0, acked: 0, archived: 0 })
/** 正在确认的事件 id（按钮防重）。 */
const ackingEventId = ref<number | null>(null)
/** 高危门类：与后端 HIGH_RISK_DECISION_GATES 对齐，只能逐条确认。 */
const HIGH_RISK_GATES = new Set([
  'cash_diff', 'equity_deviation', 'engine_owner_mismatch',
  'sell_scope', 'split_children_lost', 'out_of_pool_position',
])
/** 批量确认筛选（必须至少一项；不提供「全选所有」）。 */
const decBatch = ref({
  gate: '', layer: '', start: '', end: '', keyword: '', note: '',
})
const decBatchPreview = ref<DecisionBatchPreview | null>(null)
const decBatchLoading = ref(false)
const decBatchMsg = ref('')
const decBatchError = ref('')
const decBatchFilterActive = computed(() => Boolean(
  decBatch.value.gate || decBatch.value.layer || decBatch.value.start
  || decBatch.value.end || decBatch.value.keyword.trim(),
))
const decBatchGateOptions = computed(() => Array.from(
  new Set(decisions.value.map((d) => d.gate).filter(Boolean))).sort())
const decBatchLayerOptions = computed(() => Array.from(
  new Set(decisions.value.map((d) => d.layer).filter(Boolean))).sort())
watch(
  () => [
    decBatch.value.gate, decBatch.value.layer, decBatch.value.start,
    decBatch.value.end, decBatch.value.keyword,
  ],
  () => {
    // 筛选变化后旧的「将确认 N 条」失效，必须重新预览；改原因不触发。
    decBatchPreview.value = null
    decBatchMsg.value = ''
    decBatchError.value = ''
  },
)
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
    decisionAckSummary.value = dec?.ack_summary
      ? { open: dec.ack_summary.open, acked: dec.ack_summary.acked,
          archived: dec.ack_summary.archived || 0 }
      : { open: 0, acked: 0, archived: 0 }
  } finally {
    decisionsLoading.value = false
  }
}

/** 确认单条事件（高危门类也必须逐条人工确认，备注与操作者写入审计）。 */
async function ackDecisionRow(d: DecisionRow) {
  const sid = activeSessionId
  if (sid === null || !d.id || ackingEventId.value === d.id) return
  ackingEventId.value = d.id
  try {
    await ackLiveDecision(sid, d.id, '人工确认', 'web')
    await reloadDecisions()
  } catch {
    /* 失败保持原状，用户可重试 */
  } finally {
    ackingEventId.value = null
  }
}

function decisionBatchFilters(): DecisionBatchFilters {
  const out: DecisionBatchFilters = {}
  if (decBatch.value.gate) out.gate = decBatch.value.gate
  if (decBatch.value.layer) out.layer = decBatch.value.layer
  if (decBatch.value.start) out.start = decBatch.value.start
  if (decBatch.value.end) out.end = decBatch.value.end
  if (decBatch.value.keyword.trim()) out.keyword = decBatch.value.keyword.trim()
  return out
}

/** 批量确认第一步：预览「将确认 N 条」；高危门类自动排除，不参与统计。 */
async function previewDecisionBatch() {
  const sid = activeSessionId
  if (sid === null) return
  if (!decBatchFilterActive.value) {
    decBatchError.value = '请至少选择一个筛选条件（门类/层级/时间范围/关键字），不支持全选'
    decBatchPreview.value = null
    return
  }
  decBatchLoading.value = true
  decBatchError.value = ''
  decBatchMsg.value = ''
  try {
    const p = await previewLiveDecisionBatch(sid, decisionBatchFilters())
    decBatchPreview.value = p
    decBatchMsg.value = `将确认 ${p.matched} 条` + (p.high_risk_excluded
      ? `；另有 ${p.high_risk_excluded} 条高危门类需逐条查看后手动确认` : '')
  } catch (e) {
    decBatchPreview.value = null
    decBatchError.value = '预览失败：' + (e instanceof Error ? e.message : String(e))
  } finally {
    decBatchLoading.value = false
  }
}

/** 批量确认第二步：二次确认后提交（带 expected_count 防集合漂移）。 */
async function submitDecisionBatch() {
  const sid = activeSessionId
  const p = decBatchPreview.value
  if (sid === null || !p) return
  const note = decBatch.value.note.trim()
  if (!note) {
    decBatchError.value = '请填写确认原因（写入逐条审计，不能留空）'
    return
  }
  if (p.matched <= 0) {
    decBatchError.value = '没有可批量确认的事件；高危门类需逐条手动确认'
    return
  }
  const extra = p.high_risk_excluded
    ? `\n高危门类 ${p.high_risk_excluded} 条不参与批量，需逐条查看后确认。` : ''
  if (!window.confirm(
    `将确认 ${p.matched} 条事件（按当前筛选条件）。${extra}\n原因：${note}\n确认提交？`,
  )) return
  decBatchLoading.value = true
  decBatchError.value = ''
  try {
    const result = await batchAckLiveDecisions(sid, {
      ...decisionBatchFilters(), note, operator: 'web',
      expected_count: p.matched,
    })
    decBatchPreview.value = null
    decBatchMsg.value = `已确认 ${result.acked} 条（审计批次 ${result.batch_id || '—'}）`
      + (result.high_risk_excluded
        ? `；跳过高危 ${result.high_risk_excluded} 条` : '')
    await reloadDecisions()
  } catch (e) {
    decBatchError.value = '提交失败：' + (e instanceof Error ? e.message : String(e))
  } finally {
    decBatchLoading.value = false
  }
}

/** 页签切换：切到决策/拦截时重拉一次历史（其它页签维持既有行为）。 */
function setWbTab(tab: WbTab) {
  if (tab === wbTab.value) return
  wbTab.value = tab
  decOnlyDeferred.value = false
  if (tab === 'decisions') void reloadDecisions()
  if (tab === 'gate') void reloadGateSnapshots()
}

// ---- E2：每日市场门控快照（每组合每日一条，工作台回看门控判定依据）----
const gateSnapshots = ref<LiveGateSnapshotItem[]>([])
const gateSnapshotsLoading = ref(false)

async function reloadGateSnapshots() {
  const id = activeSessionId
  if (id === null) return
  gateSnapshotsLoading.value = true
  try {
    const rows = await getLiveGateSnapshots(id, 30).catch(() => [])
    if (activeSessionId !== id) return
    gateSnapshots.value = rows
  } finally {
    gateSnapshotsLoading.value = false
  }
}

function gateSnapshotLabel(d: number | null): string {
  if (d === 1) return '多头'
  if (d === -1) return '空头'
  if (d === 0) return '中性'
  return '—'
}
function gateSnapshotBadge(d: number | null): string {
  if (d === 1) return 'badge-green'
  if (d === -1) return 'badge-red'
  return 'badge-gray'
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
    decisionAckSummary.value = dec?.ack_summary
      ? { open: dec.ack_summary.open, acked: dec.ack_summary.acked,
          archived: dec.ack_summary.archived || 0 }
      : { open: 0, acked: 0, archived: 0 }
    // 预警信号：alert 模式主产物；auto 模式下也可能有信号落库
    signals.value = sig?.items ?? []
    signalsTotal.value = sig?.total ?? 0
    // G1：待放行顺延委托（独立端点；失败静默，不阻塞主工作台）
    loadDeferredOrders(id)
    // G4：盘前效果统计（与委托同屏，失败静默）
    loadPreOpenStats(id)
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

/** G1：待放行顺延委托列表；停止态返回 in_memory=false 的只读视图。 */
async function loadDeferredOrders(id: number | null = activeSessionId) {
  if (id === null) {
    deferredOrders.value = []
    deferredRunning.value = false
    return
  }
  try {
    const data = await getLiveDeferredOrders(id)
    if (activeSessionId !== id) return
    deferredOrders.value = data.items || []
    deferredRunning.value = !!data.running
  } catch {
    deferredOrders.value = []
    deferredRunning.value = false
  }
}

async function onReleaseDeferred(o: LiveDeferredOrderItem) {
  if (activeSessionId === null) return
  const label = `${o.stock_code} ${tradeTypeLabel(o.trade_type)} ${o.quantity} 股`
  if (!window.confirm(
    `放行顺延委托：${label}？\n将按当前价重走完整闸门链（资金/槽位/门控），被拦下会自动回队。`,
  )) return
  deferredBusy.value = o.id
  try {
    const r = await releaseLiveDeferredOrder(activeSessionId, o.id)
    if (r.status === 'blocked') {
      alert('已尝试提交但被闸门拦下，已回队等待自动重试（详见决策/拦截）。')
    }
    await loadDeferredOrders()
    await loadHistory(activeSessionId)
  } catch (e: any) {
    alert(e?.response?.data?.message || e?.message || '放行失败')
  } finally {
    deferredBusy.value = null
  }
}

async function onAbandonDeferred(o: LiveDeferredOrderItem) {
  if (activeSessionId === null) return
  const reason = window.prompt(
    `放弃顺延委托 ${o.stock_code} ${o.quantity} 股，请填写原因（写入审计）：`, '')
  if (reason === null) return
  if (!reason.trim()) {
    alert('必须填写放弃原因')
    return
  }
  deferredBusy.value = o.id
  try {
    await abandonLiveDeferredOrder(activeSessionId, o.id, reason.trim())
    await loadDeferredOrders()
    await loadHistory(activeSessionId)
  } catch (e: any) {
    alert(e?.response?.data?.message || e?.message || '放弃失败')
  } finally {
    deferredBusy.value = null
  }
}

/** G4：盘前效果统计（只读；日期区间缺省=服务端最近 7 天）。 */
async function loadPreOpenStats(id: number | null = activeSessionId) {
  if (id === null) {
    preOpenStats.value = []
    preOpenTotals.value = null
    return
  }
  try {
    const data = await getLivePreOpenStats(id, { group_by: preOpenGroupBy.value })
    if (activeSessionId !== id) return
    preOpenStats.value = data.items || []
    preOpenTotals.value = data.totals || null
  } catch {
    preOpenStats.value = []
    preOpenTotals.value = null
  }
}

function onPreOpenGroupChange() {
  loadPreOpenStats()
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

// ---- F2：数据体检（只读；非盘中自动重读本地已落盘数据，不触发下载）----
const dataHealth = ref<Record<string, any> | null>(null)
const dataHealthLoading = ref(false)
// 逐周期「缺失/陈旧」摘要（stale + empty）
const dataHealthProblems = computed(() => {
  const periods = dataHealth.value?.health?.periods as Record<string, any> | undefined
  if (!periods) return []
  return Object.entries(periods)
    .filter(([, s]) => (Number(s?.stale) || 0) + (Number(s?.empty) || 0) > 0)
    .map(([p, s]) => `${p} 缺/陈旧 ${(Number(s?.stale) || 0) + (Number(s?.empty) || 0)} 只`)
})

async function checkDataHealth() {
  const id = activeSessionId
  if (id === null) return
  dataHealthLoading.value = true
  try {
    const d = await getLiveDataHealth(id, true)
    if (activeSessionId !== id) return
    dataHealth.value = d ?? null
  } catch (e: any) {
    if (activeSessionId !== id) return
    dataHealth.value = {
      available: false,
      reason: e?.response?.data?.error || e?.message || '体检请求失败',
    }
  } finally {
    dataHealthLoading.value = false
  }
}

// ---- N2/K7 理论 vs 实际对照 ----
const showTvl = ref(false)
const tvlLoading = ref(false)
const tvlError = ref('')
const theoryVsLive = ref<TheoryVsLiveResult | null>(null)
// F5 影子健康（只读）：继承矩阵漂移 + live_haircut，不提供任何写操作
const tvlInheritance = ref<CapitalInheritanceReport | null>(null)
// 面板内绑定：auto=按会话组合自动关联；数字字符串=显式绑定记录 id
const tvlRecords = ref<BacktestRecordItem[]>([])
const tvlRecordsLoading = ref(false)
const tvlBindingValue = ref<string>('auto')
const tvlBindingSaving = ref(false)
const tvlBindingMsg = ref('')
// 当前面板对应的会话（用于标注「实盘已纳入存量仓 → 理论对照解释力下降」）
const tvlSessionId = ref<number | null>(null)
const tvlSellScopeFull = computed(() => {
  const s = sessions.value.find((x) => x.id === tvlSessionId.value)
  return (s?.sell_scope || 'platform_only') === 'full'
})

const _tvlPct = (v: number | null) => (v === null ? '—' : `${(v * 100).toFixed(2)}%`)

async function loadTvlRecords() {
  if (tvlRecords.value.length || tvlRecordsLoading.value) return
  tvlRecordsLoading.value = true
  try {
    tvlRecords.value = await getBacktestRecords()
  } catch {
    tvlRecords.value = []
  } finally {
    tvlRecordsLoading.value = false
  }
}

const tvlRecordOptions = computed(() => {
  const sid = watchedSessionId.value ?? activeSessionId
  const pids = new Set(
    sessions.value.find((s) => s.id === sid)?.portfolio_ids || [])
  return [...tvlRecords.value].sort((a, b) => {
    const am = pids.has(a.portfolio_strategy_id) ? 1 : 0
    const bm = pids.has(b.portfolio_strategy_id) ? 1 : 0
    return bm - am || b.id - a.id
  })
})

function recordStatusLabel(status: string): string {
  return status === 'completed' ? '已完成' : status
}

async function openTheoryVsLive() {
  const sid = watchedSessionId.value ?? activeSessionId
  if (sid === null) return
  tvlSessionId.value = sid
  showTvl.value = true
  tvlLoading.value = true
  tvlError.value = ''
  tvlBindingMsg.value = ''
  theoryVsLive.value = null
  tvlInheritance.value = null
  await loadTvlRecords()
  try {
    theoryVsLive.value = await getTheoryVsLive(sid)
    // F5：拼盘会话顺带取只读继承/影子健康；失败不阻断对照主视图
    getLiveSession(sid).then((d) => {
      tvlInheritance.value = (d as LiveSessionItem).capital_inheritance ?? null
    }).catch(() => { tvlInheritance.value = null })
    // 显式绑定才回填下拉；自动关联保持 auto，切回自动即清除绑定
    tvlBindingValue.value = (
      theoryVsLive.value.binding_source === 'manual' && theoryVsLive.value.backtest_record_id
        ? String(theoryVsLive.value.backtest_record_id)
        : 'auto'
    )
  } catch (e: any) {
    tvlError.value = '加载失败：' + (e?.response?.data?.message || e?.message || e)
  } finally {
    tvlLoading.value = false
  }
}

async function saveTvlBinding() {
  const sid = watchedSessionId.value ?? activeSessionId
  if (sid === null || tvlBindingSaving.value) return
  tvlBindingSaving.value = true
  tvlBindingMsg.value = ''
  try {
    const recordId = tvlBindingValue.value === 'auto'
      ? null : Number(tvlBindingValue.value)
    const updated = await updateLiveSession(sid, { backtest_record_id: recordId })
    const idx = sessions.value.findIndex((s) => s.id === sid)
    if (idx >= 0) sessions.value[idx] = updated.session
    theoryVsLive.value = await getTheoryVsLive(sid)
    tvlBindingMsg.value = recordId === null
      ? '已切回自动关联' : `已绑定回测记录 #${recordId}`
  } catch (e: any) {
    tvlBindingMsg.value = '保存失败：' + (e?.response?.data?.message || e?.message || e)
  } finally {
    tvlBindingSaving.value = false
  }
}

const tvlChart = computed(() => {
  const d = theoryVsLive.value
  if (!d) return null
  const series = [
    { key: 'theory', label: '理论', color: 'var(--blue)', points: d.theory.curve },
    { key: 'live', label: '实盘', color: 'var(--red)', points: d.live.curve },
  ]
  const dates = Array.from(new Set(
    series.flatMap(s => s.points.map(p => p.date)))).sort()
  const values = series.flatMap(s => s.points.map(p => p.index_value))
    .filter((v): v is number => v !== null && v !== undefined)
  if (!dates.length || !values.length) return null
  const W = 560
  const H = 150
  const pad = 8
  const min = Math.min(...values)
  const max = Math.max(...values)
  const span = (max - min) || 1
  const xOf = (date: string) => {
    const i = dates.indexOf(date)
    return pad + (dates.length === 1 ? 0 : (i / (dates.length - 1)) * (W - 2 * pad))
  }
  const paths = series.map(s => ({
    ...s,
    path: s.points
      .filter(p => p.index_value !== null)
      .map((p, i) => {
        const y = H - pad - ((p.index_value as number) - min) / span * (H - 2 * pad)
        return `${i === 0 ? 'M' : 'L'}${xOf(p.date).toFixed(1)},${y.toFixed(1)}`
      })
      .join(' '),
  }))
  return { W, H, min, max, paths }
})

function tvlMetric(side: 'theory' | 'live', key: string): string {
  const v = theoryVsLive.value?.[side].metrics?.[key]
  if (v === null || v === undefined) return '—'
  return String(v)
}

/** 切换工作台聚焦会话：清空工作台、拉该会话历史；事件流不动（多会话并行常开）。 */
const watchedSessionId = ref<number | null>(null)

function watchSession(id: number) {
  watchedSessionId.value = id
  activeSessionId = id
  bridgeOnline.value = null  // 切换会话先置未知，避免显示上一个会话的在线态
  dataHealth.value = null  // 数据体检结果按会话隔离
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
  // 健康总览「待审批」计数深链到本工作台审批页签（仅首次进入时生效，
  // 之后用户手动切换页签不会被定时刷新抢回）。
  if (!initialTabFromQuery) {
    initialTabFromQuery = true
    if (new URLSearchParams(window.location.search).get('tab') === 'approvals') {
      wbTab.value = 'approvals'
    }
  }
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
  scheduleStoppingRefresh()
}

// stopping 是后端中间态：engine.stop() 收敛完成后 DB 才置 stopped。
// 本页列表没有自动轮询，不补一个短轮询会让「停止中…」永远停在那里。
let stoppingTimer: ReturnType<typeof setTimeout> | null = null
function scheduleStoppingRefresh() {
  if (stoppingTimer !== null) return
  if (!sessions.value.some((s) => s.status === 'stopping')) return
  stoppingTimer = setTimeout(async () => {
    stoppingTimer = null
    await load()
  }, 5000)
}

function openCreate() {
  // C1：默认引擎若已被占用，自动选第一个未占用且已配置的引擎（避免默认值即非法）
  const occ = occupants.value
  const engines = channel.value?.engines || []
  const preferred = defaultEngineKey()
  const engine = (!occ[preferred]
    ? preferred
    : (engines.find((e) => e.configured && !occ[e.key])?.key as LiveEngineKey | undefined)) || preferred
  form.value = { name: '', mode: 'simulation', engine, execution_mode: 'alert', capital_pool_mode: 'off', capital_pool_rebalance_enabled: false, portfolio_ids: [], live_capital: null, signal_file_path: '', advance_close_enabled: false, advance_close_time: '14:51', intraday_mismatched_risk_enabled: null, gate_recheck_on_execute: false, pre_open_auction_enabled: null, pre_open_auction_end_time: '', pre_open_auction_price_bias_pct: null, pairing_plan_id: null }
  createError.value = ''
  showCreate.value = true
  void loadPairingPlans()
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
  const biasRaw = form.value.pre_open_auction_price_bias_pct
  const bias = (biasRaw === null || biasRaw === undefined || biasRaw === '')
    ? null : Number(biasRaw)
  try {
    const res = await createLiveSession({
      ...form.value,
      live_capital: cap,
      pre_open_auction_price_bias_pct: bias,
      baseline_mode: cap === null ? 'total_asset' : 'manual',
      baseline_value: cap,
    })
    // A1 软校验：目标引擎已被其它会话占用等提示（不阻断创建，但必须让用户看见）
    createWarnings.value = Array.isArray(res?.warnings) ? res.warnings : []
    showCreate.value = false
    form.value = { name: '', mode: 'simulation', engine: defaultEngineKey(), execution_mode: 'alert', capital_pool_mode: 'off', capital_pool_rebalance_enabled: false, portfolio_ids: [], live_capital: null, signal_file_path: '', advance_close_enabled: false, advance_close_time: '14:51', intraday_mismatched_risk_enabled: null, gate_recheck_on_execute: false, pre_open_auction_enabled: null, pre_open_auction_end_time: '', pre_open_auction_price_bias_pct: null, pairing_plan_id: null }
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
  if (startingIds.value.has(id)) return
  clearStartError(id)
  dismissStartWarnings(id)
  startingIds.value = new Set(startingIds.value).add(id)
  try {
    const r = ack
      ? await startLiveSession(id, { ack_keyboard_risk: true })
      : await startLiveSession(id)
    // 通道建议类告警（非阻断）：如所选引擎与推荐通道不一致，展示在行内并留档事件日志
    if (Array.isArray(r?.warnings) && r.warnings.length > 0) {
      showStartWarnings(id, r.warnings)
    }
    load() // load 内自动连接新运行 session 的流
  } catch (e: any) {
    // 结构化 409：按 data.rule（引擎占用/账户占用/键鼠硬拦/参数漂移阻断）给指名话术
    const info = parseStartConflict(e)
    startErrors.value = { ...startErrors.value, [id]: info.message }
    alert(info.message)
  } finally {
    const next = new Set(startingIds.value)
    next.delete(id)
    startingIds.value = next
  }
}

function clearStartError(id: number) {
  if (startErrors.value[id] === undefined) return
  const next = { ...startErrors.value }
  delete next[id]
  startErrors.value = next
}

/** 展示启动告警：原文写事件日志留档，并在 TTL 后自动收起（同会话再次启动会重置计时）。 */
function showStartWarnings(id: number, list: string[]) {
  const session = sessions.value.find((s) => s.id === id)
  const label = session ? `#${session.id} ${session.name}` : `#${id}`
  const prev = startWarningTimers.get(id)
  if (prev) clearTimeout(prev)
  startWarnings.value = { ...startWarnings.value, [id]: list }
  startWarningTimers.set(id, setTimeout(() => dismissStartWarnings(id), START_WARNING_TTL_MS))
  for (const w of list) pushLog('notice', { message: w }, nowTime(), label, id)
}

/** 收起某会话的启动告警（自动 TTL 到期或用户点 × 均走这里）。 */
function dismissStartWarnings(id: number) {
  const timer = startWarningTimers.get(id)
  if (timer) {
    clearTimeout(timer)
    startWarningTimers.delete(id)
  }
  if (startWarnings.value[id] === undefined) return
  const next = { ...startWarnings.value }
  delete next[id]
  startWarnings.value = next
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
  // 二次确认（2026-11-20）：停止按钮紧挨会话行、一点就停，且停止会跳过当日日终，
  // 必须先让操作者看到后果再发请求；取消则不发任何请求。
  const name = sessions.value.find((s) => s.id === id)?.name
  const label = name ? `「${name}」` : ` #${id}`
  if (!window.confirm(
    `确定停止会话${label}？\n\n`
    + '停止后当日 14:30 日终估值与 15:05 收盘清扫、风控都不再执行；\n'
    + '需重新启动该会话才能补跑。\n\n确认停止？'
  )) return
  // 先停后端成功再关流（audit M3）：stop 后端会经流推 canceled 终态事件，
  // 先关流会错过清扫；API 失败则保持流开着（会话实际仍在运行）。
  try {
    await stopLiveSession(id)
    load() // 多会话并行：load 会重开所有 running 流、关停已停会话的流
  } catch (e: any) {
    alert(e?.message || '停止失败')
  }
}

// ---- 存量/外部仓是否纳入风控（代卖）：会话级开关（运行中可切，不重启）----
const sellScopeBusy = ref<number | null>(null)
function sellScopeOf(s: LiveSessionItem): 'full' | 'platform_only' {
  return s.sell_scope === 'full' ? 'full' : 'platform_only'
}
function sellScopeTitle(s: LiveSessionItem): string {
  const cur = sellScopeOf(s)
  const src = s.sell_scope_source === 'session'
    ? '本会话已显式设置' : `本会话未设置，跟随全局默认（${s.sell_scope_global || 'platform_only'}）`
  return cur === 'full'
    ? `存量/外部持仓已纳入风控代卖：风控卖出按整仓口径（${src}）`
    : `仅代卖平台自建仓，存量/外部持仓不纳入风控（${src}）`
}
async function toggleSellScope(s: LiveSessionItem) {
  if (sellScopeBusy.value === s.id) return
  const next: 'full' | 'platform_only' = sellScopeOf(s) === 'full' ? 'platform_only' : 'full'
  if (next === 'full') {
    const ok = window.confirm(
      `即将把「${s.name}」的存量/外部持仓纳入风控代卖。\n\n` +
      '⚠ 开启后会扩大代卖范围，可能立刻对存量仓产生止损单。\n' +
      '• 系统会先做全量对账（真实现金 / 持仓市值 / 逐只持仓），账不齐会拒绝开启；\n' +
      '• 运行中立即生效（下一轮 tick），无需重启会话；\n' +
      '• 已提交未成交的委托不会撤销，只是后续新卖单按整仓口径。\n\n' +
      '确认开启？',
    )
    if (!ok) return
  }
  sellScopeBusy.value = s.id
  try {
    const res = await setLiveSessionSellScope(s.id, next, next === 'full' ? '界面开启存量仓纳入风控' : '界面关闭存量仓纳入风控')
    // 就地更新（列表对象与工作台同源）
    s.sell_scope = res.new
    s.sell_scope_source = 'session'
    alert(res.message)
  } catch (e: any) {
    alert(e?.response?.data?.message || e?.message || '切换失败')
  } finally {
    sellScopeBusy.value = null
  }
}

// ---- B3 通道守护（只读检查清单 + 一键重新探活）----
const guardModal = ref<LiveSessionItem | null>(null)
const guard = ref<LiveChannelGuard | null>(null)
const guardLoading = ref(false)
const guardMessage = ref('')
async function openGuard(s: LiveSessionItem) {
  guardModal.value = s
  guard.value = null
  guardMessage.value = ''
  guardLoading.value = true
  try {
    guard.value = await getLiveChannelGuard(s.id)
  } catch (e: any) {
    guardMessage.value = e?.response?.data?.message || e?.message || '读取检查清单失败'
  } finally {
    guardLoading.value = false
  }
}
async function recheckChannel() {
  if (!guardModal.value) return
  guardLoading.value = true
  guardMessage.value = ''
  try {
    const res = await reconnectLiveChannel(guardModal.value.id)
    guardMessage.value = res.online
      ? `${res.message}（${guardModal.value.name}）`
      : `${res.message}：${res.manual_action}`
    guard.value = await getLiveChannelGuard(guardModal.value.id)
  } catch (e: any) {
    guardMessage.value = e?.response?.data?.message || e?.message || '重新探活失败'
  } finally {
    guardLoading.value = false
  }
}
async function switchFromGuard() {
  if (!guardModal.value) return
  await openEdit(guardModal.value)
  guardModal.value = null
  editResult.value = { effective_at: 'now', message: '请在编辑弹窗确认备用通道后点「一键切换备用通道」' }
}

// ---- H2 拆单剩余量补发 ----
const splitModal = ref<LiveSessionItem | null>(null)
const splitItems = ref<LiveSplitRemainderItem[]>([])
const splitLoading = ref(false)
const splitMessage = ref('')
async function openSplitRemainders(s: LiveSessionItem) {
  splitModal.value = s
  splitItems.value = []
  splitMessage.value = ''
  splitLoading.value = true
  try {
    const res = await getLiveSplitRemainders(s.id)
    splitItems.value = res.items
    if (res.count === 0) splitMessage.value = '当前没有待补发的拆单剩余量'
  } catch (e: any) {
    splitMessage.value = e?.response?.data?.message || e?.message || '读取拆单剩余量失败'
  } finally {
    splitLoading.value = false
  }
}
async function submitSplitReorder() {
  if (!splitModal.value || splitItems.value.length === 0) return
  splitLoading.value = true
  splitMessage.value = ''
  try {
    const res = await reorderLiveSplitRemainders(splitModal.value.id)
    splitMessage.value = `已提交 ${res.results.length} 笔，成功 ${res.accepted} 笔，剩余 ${res.remaining} 笔待补发`
    const refreshed = await getLiveSplitRemainders(splitModal.value.id)
    splitItems.value = refreshed.items
    load()
  } catch (e: any) {
    splitMessage.value = e?.response?.data?.message || e?.message || '补发失败'
  } finally {
    splitLoading.value = false
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

// ---- B1 + A4 编辑会话配置（运行中除名称外下次启动生效）----
const editModal = ref<LiveSessionItem | null>(null)
const editSaving = ref(false)
const editError = ref('')
const editResult = ref<{ effective_at: string; message: string } | null>(null)
const editForm = ref({
  name: '',
  engine: 'tdx' as LiveEngineKey,
  backup_engine: '' as '' | LiveEngineKey,
  failover_enabled: false,
  execution_mode: 'alert' as LiveExecutionMode,
  signal_file_path: '',
  capital_pool_mode: 'off' as 'off' | 'equal' | 'risk_budget' | 'priority',
  capital_pool_rebalance_enabled: false,
  gate_recheck_on_execute: false,
  // G2：盘前参数会话级覆盖（null / '' = 跟随全局）
  pre_open_auction_enabled: null as boolean | null,
  pre_open_auction_end_time: '',
  pre_open_auction_price_bias_pct: null as number | string | null,
  portfolio_ids: [] as number[],
  // F1/F5：拼盘绑定（与回测记录三选一；空=不绑定）
  pairing_plan_id: null as number | null,
})
const editHealth = ref<LiveSessionChannelHealth | null>(null)
const editSwitching = ref(false)
async function loadEditHealth(sessionId: number) {
  editHealth.value = null
  try {
    editHealth.value = await getLiveSessionChannelHealth(sessionId)
  } catch {
    editHealth.value = null
  }
}
function openEdit(s: LiveSessionItem) {
  editModal.value = s
  editError.value = ''
  editResult.value = null
  editForm.value = {
    name: s.name,
    engine: (s.engine || defaultEngineKey()) as LiveEngineKey,
    backup_engine: (s.backup_engine || '') as '' | LiveEngineKey,
    failover_enabled: !!s.failover_enabled,
    execution_mode: (s.execution_mode || 'auto') as LiveExecutionMode,
    signal_file_path: s.signal_file_path || '',
    capital_pool_mode: (s.capital_pool_mode || 'off') as 'off' | 'equal' | 'risk_budget' | 'priority',
    capital_pool_rebalance_enabled: !!s.capital_pool_rebalance_enabled,
    gate_recheck_on_execute: !!s.gate_recheck_on_execute,
    // G2：原始三态回填（null=跟随全局）
    pre_open_auction_enabled: s.pre_open_auction_enabled ?? null,
    pre_open_auction_end_time: s.pre_open_auction_end_time || '',
    pre_open_auction_price_bias_pct: s.pre_open_auction_price_bias_pct ?? null,
    portfolio_ids: [...(s.portfolio_ids || [])],
    pairing_plan_id: s.pairing_plan_id ?? null,
  }
  loadEditHealth(s.id)
  void loadPairingPlans()
}
async function switchToBackup() {
  if (!editModal.value || !editForm.value.backup_engine) return
  if (!confirm(`确认把会话「${editModal.value.name}」切换到备用通道 ${engineLabel(editForm.value.backup_engine)}？运行中会话将在下次启动生效。`)) return
  editSwitching.value = true
  editError.value = ''
  try {
    const res = await failoverLiveSession(editModal.value.id, editForm.value.backup_engine)
    editResult.value = { effective_at: res.effective_at, message: res.message }
    load()
  } catch (e: any) {
    editError.value = e?.response?.data?.message || e?.message || '切换失败'
  } finally {
    editSwitching.value = false
  }
}
async function submitEdit() {
  if (!editModal.value) return
  editSaving.value = true
  editError.value = ''
  editResult.value = null
  try {
    const res = await updateLiveSession(editModal.value.id, {
      name: editForm.value.name,
      engine: editForm.value.engine,
      backup_engine: editForm.value.backup_engine || null,
      failover_enabled: editForm.value.failover_enabled,
      execution_mode: editForm.value.execution_mode,
      signal_file_path: editForm.value.signal_file_path,
      capital_pool_mode: editForm.value.capital_pool_mode,
      capital_pool_rebalance_enabled: editForm.value.capital_pool_rebalance_enabled,
      gate_recheck_on_execute: editForm.value.gate_recheck_on_execute,
      // G2：空串/null = 清除覆盖（跟随全局）
      pre_open_auction_enabled: editForm.value.pre_open_auction_enabled,
      pre_open_auction_end_time: editForm.value.pre_open_auction_end_time || null,
      pre_open_auction_price_bias_pct: (
        editForm.value.pre_open_auction_price_bias_pct === '' ||
        editForm.value.pre_open_auction_price_bias_pct === null
          ? null : Number(editForm.value.pre_open_auction_price_bias_pct)),
      portfolio_ids: editForm.value.portfolio_ids,
      // F5：绑定拼盘时同时清空单记录绑定，避免三选一冲突被后端 400
      pairing_plan_id: editForm.value.pairing_plan_id,
      ...(editForm.value.pairing_plan_id
        ? { backtest_record_id: null, backtest_run_group_id: null } : {}),
    })
    editResult.value = { effective_at: res.effective_at, message: res.message }
    load()
    if (res.effective_at === 'now') {
      editModal.value = null
    }
  } catch (e: any) {
    editError.value = e?.response?.data?.message || e?.message || '保存失败'
  } finally {
    editSaving.value = false
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
    minuteSource.value = null
    return
  }
  const target = sessions.value.find((s) => s.id === id)
  if (!target || target.status !== 'running') {
    bridgeOnline.value = null
    watchdogAlarms.value = []
    minuteSource.value = null
    return
  }
  try {
    const d = await getLiveSession(id)
    if (activeSessionId !== id) return  // 在途期间已切走 → 丢弃
    bridgeOnline.value = typeof d?.bridge_online === 'boolean' ? d.bridge_online : null
    minuteSource.value = (d as any)?.minute_source ?? null
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
  if (stoppingTimer !== null) {
    clearTimeout(stoppingTimer)
    stoppingTimer = null
  }
  // 启动告警自动收起计时器：组件销毁时清掉，避免定时器悬挂
  startWarningTimers.forEach((t) => clearTimeout(t))
  startWarningTimers.clear()
})
</script>

<template>
  <PageHeader title="实盘交易">
    <button class="btn btn-sm" @click="showGuide = true" data-field="live-guide-toggle">📖 配置指南</button>
    <button @click="openCreate" class="btn btn-primary">+ 新建实盘</button>
  </PageHeader>

  <!-- 盘中数据轮询常驻提示（不可关闭）：默认关闭原生源时给出启用方式与差异；
       开启后展示最后前进时刻，minute_source.stalled 时升级为醒目告警。 -->
  <div class="card minute-source-banner"
       :class="{ 'minute-source-alert': minuteSourceAlert }"
       data-test="minute-source-banner">
    <div>
      <strong>盘中数据轮询，请确认已启用数据下载工具</strong>
      <div class="minute-source-body">
        <template v-if="!minuteSource?.enabled">
          当前为默认快照合成：1m 由行情快照合成，5m/15m/30m/1h 由 1m 聚合，与回测原生 K 线不等价。
          如需与回测同源，在 config.yaml 设置 <code>live.native_minute.enabled: true</code>
          （可选 <code>stall_timeout_seconds</code>，默认 180）并<b>重启实盘会话</b>生效。
          开启后：1m 读本地原生 <code>.lc1</code>、5m 读 <code>.lc5</code>，15m/30m/1h 走原生周期；
          只使用标签 ≤ 当前墙钟的已闭合 bar（未完成桶不进入信号与风控）；判新只看 bar 标签前进；
          原生数据超过阈值不前进会自动回落快照合成并留痕。
        </template>
        <template v-else-if="minuteSourceAlert">
          <b>原生分钟数据已停止前进</b>：最后前进 {{ minuteSourceText.last }}，已滞后
          {{ minuteSourceText.lag }}，已自动回落快照合成。请检查「TDX数据工具.exe」是否在运行、
          清单 CCJX.blk 是否已更新；恢复前进后自动切回原生。
        </template>
        <template v-else>
          原生分钟数据源已启用：最后前进 {{ minuteSourceText.last }}
          （滞后 {{ minuteSourceText.lag }}），当前 1m/5m 读本地原生文件；停滞将自动回落快照合成。
        </template>
      </div>
    </div>
  </div>

  <!-- A1 新建会话软告警（如目标引擎已被其它会话占用）：不阻断，但必须可见 -->
  <div v-if="createWarnings.length" class="card create-warning-banner" data-test="create-warnings">
    <div v-for="w in createWarnings" :key="w">{{ w }}</div>
    <button class="btn btn-sm" @click="createWarnings = []">关闭</button>
  </div>

  <!-- 分钟级口径提示：不阻断任何操作，可关闭；只提示不改装配 -->
  <div v-if="minuteLivePeriods.length && !minuteNoticeDismissed" class="card minute-live-banner"
       data-test="minute-live-banner">
    <div>
      <strong v-if="minuteSource?.enabled && !minuteSourceAlert">分钟级策略：实盘已启用原生分钟数据源（.lc1/.lc5），与回测同源</strong>
      <strong v-else>分钟级策略：实盘用快照合成 bar，与回测 TQ 原生 1m 不等价</strong>
      <div v-if="minuteSource?.enabled && !minuteSourceAlert" class="minute-live-body">
        实盘 1m 读本地原生 .lc1、5m 读 .lc5、15m/30m/1h 走原生周期，只使用已闭合 bar
        （未完成桶不进入信号与风控）。成交按信号 bar 收盘价挂限价且默认不追价，
        成交时点与均价仍可能与回测不同；回测绑定不继承风控执行时点 / 追价 / 单笔上限 / 复权口径。
      </div>
      <div v-else class="minute-live-body">
        当前组合含 {{ minuteLivePeriods.join(' / ') }} 周期。当前未启用原生分钟源（默认）：
        实盘 1m 由行情快照合成（约 5–30s 采样、60s 发布、只发已完成 bar），缺采样会整根丢 bar，
        恢复后首根可能是半根，成交量按日累计差分、跨分钟边界会串位；5m/15m/30m/1h 由 1m 聚合，
        缺一分钟整桶丢弃。开启 live.native_minute.enabled 后改为 1m 读 .lc1、5m 读 .lc5、
        15m/30m/1h 走原生周期（与回测同源），只取已闭合 bar，上述采样类问题消失，但下载工具漏写
        仍会缺 bar。成交按信号 bar 收盘价挂限价且默认不追价（180s 超时撤单），离线统计约 7.2% 买单 /
        7.9% 卖单在信号价无成交，成交均价系统性劣于回测（回测按下一根开盘必成交）。
        回测绑定只是绑定关系，不继承策略 / 周期 / 风控执行时点 / 追价 / 单笔上限 / 复权口径。
      </div>
    </div>
    <button class="btn btn-sm" @click="minuteNoticeDismissed = true">关闭</button>
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
  <div v-else class="card table-wrap sessions-wrap">
    <table class="sessions-table">
      <thead>
        <tr>
          <th class="col-id">ID · 名称</th>
          <th class="col-strategy">组合策略<span class="th-sub">执行模式 · 引擎 · 模式</span></th>
          <th class="col-baseline">资金基准</th>
          <th class="col-status">状态</th>
          <th class="col-scope">存量仓风控</th>
          <th class="col-actions">操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="s in sessions" :key="s.id">
          <td class="col-id" data-label="ID · 名称">
            <div class="sess-id">#{{ s.id }}</div>
            <div class="sess-name">{{ s.name }}</div>
          </td>
          <td class="col-strategy" data-label="组合策略">
            <div class="strat-chips">
              <span v-for="p in portfolioStatuses(s)" :key="p.portfolio_id" class="portfolio-chip">
                {{ portfolioName(p.portfolio_id) }}
                <span v-if="p.status === 'circuit_broken'" class="badge badge-red">熔断</span>
                <button v-if="p.status === 'circuit_broken'" @click="recoverBreaker(s, p.portfolio_id)" class="btn btn-sm">恢复</button>
              </span>
              <span v-if="portfolioStatuses(s).length === 0" class="muted">-</span>
            </div>
            <div class="sess-meta">
              <span class="badge" :class="isAlert(s) ? 'badge-blue' : 'badge-purple'"
                :title="isAlert(s) ? '半自动预警：仅 TQ 行情 + send_warn 推送预警，不下单' : '全自动：按交易引擎下单'"
              >{{ executionModeLabel(s.execution_mode) }}</span>
              <span class="badge badge-gray" :title="isAlert(s) ? '预警模式：固定走通达信 TQ 行情计算信号，不下单' : '全自动：按交易引擎下单'">{{ engineLabel(s.engine) }}</span>
              <span class="meta-mode" :title="`模式：${s.mode === 'simulation' ? '仿真' : '实盘'}`">{{ s.mode === 'simulation' ? '仿真' : '实盘' }}</span>
            </div>
          </td>
          <td class="col-baseline" data-label="资金基准">
            <div class="baseline-cell">
              <span
                class="baseline-value num"
                :class="{ 'is-default': s.live_capital == null }"
                :title="s.live_capital != null ? '手动填写的实盘资金基准（只缩不放）' : '以真实通道总资产为基准（只缩不放）'"
              >{{ s.live_capital != null ? formatMoney2(s.live_capital) : '真实总资产' }}</span>
              <button
                class="btn btn-sm baseline-reset" data-test="baseline-reset"
                :disabled="!resetWindow.allowed"
                :title="resetWindow.allowed ? '重置资金基准（留空=当日收盘总资产，只能小于真实总资产）' : resetWindow.reason"
                @click="openBaseline(s)"
              >重置</button>
            </div>
          </td>
          <td class="col-status" data-label="状态">
            <div class="status-cell">
              <span class="badge"
                :class="s.status === 'running' ? 'badge-green' : s.status === 'stopping' ? 'badge-orange' : 'badge-gray'"
              >{{ s.status === 'running' ? '运行中' : s.status === 'stopping' ? '停止中' : '已停止' }}</span>
              <div v-if="s.status === 'running'" class="status-tags">
                <!-- C4：真的在操作多个账户 → 徽标提示（不阻断） -->
                <span v-if="multiAccountBanner"
                  class="badge badge-orange"
                  title="当前有多个实盘账户并行运行，下单前请确认窗口对应的账户">多账户并行中</span>
                <!-- 状态陈旧保护：后端重启后引擎未恢复时，DB 可能仍留 running；以引擎
                     注册表（engine_running）为准提示，避免误以为在自动交易。 -->
                <span v-if="s.engine_running === false"
                  class="badge badge-red"
                  title="状态陈旧：后端重启后引擎未恢复，实际未运行；请重新启动会话">引擎未启动</span>
                <!-- 「工作台聚焦」≠「是否被监控」：所有 running 会话都并行订阅 SSE、并行监控；
                     该徽标仅表示工作台当前聚焦展示哪一个会话。 -->
                <span v-if="s.id === watchedSessionId"
                  class="badge badge-blue"
                  title="工作台聚焦：仅表示工作台当前聚焦展示该会话；所有运行中会话都在并行监控与订阅">工作台聚焦</span>
                <!-- 监控正常：引擎在册（engine_running=true）才算真的在被监控；后端未返回该字段时不显示，避免误报。 -->
                <span v-if="s.engine_running === true"
                  class="badge badge-green"
                  title="监控正常：引擎在册，轮询与风控在线">监控正常</span>
              </div>
            </div>
          </td>
          <td class="col-scope" data-label="存量仓风控">
            <div class="scope-cell">
              <!-- 存量/外部持仓是否纳入风控（代卖）：会话级开关，默认关闭 -->
              <span
                class="badge"
                :class="sellScopeOf(s) === 'full' ? 'badge-orange' : 'badge-gray'"
                :title="sellScopeTitle(s)"
                data-test="sell-scope-badge"
              >{{ sellScopeOf(s) === 'full' ? '存量仓已纳入' : '仅平台自建仓' }}</span>
              <button
                class="btn btn-sm" data-test="sell-scope-toggle"
                :disabled="s.status === 'stopping' || sellScopeBusy === s.id"
                :title="sellScopeOf(s) === 'full' ? '关闭后不再对存量/外部仓下新卖单（已提交未成交委托不撤销）' : '开启前会先做全量对账，账不齐会拒绝'"
                @click="toggleSellScope(s)"
              >{{ sellScopeBusy === s.id ? '切换中…' : (sellScopeOf(s) === 'full' ? '关闭' : '开启') }}</button>
            </div>
          </td>
          <td class="col-actions" data-label="操作">
            <div class="op-cell">
              <div class="op-primary">
                <button v-if="s.status === 'stopped'" @click="startSession(s.id)" class="btn btn-sm btn-primary"
                  :disabled="startingIds.has(s.id)">{{ startingIds.has(s.id) ? '启动中…' : '启动' }}</button>
                <button v-if="s.status === 'running'" @click="stopSession(s.id)" class="btn btn-sm btn-danger">停止</button>
                <button v-if="s.status === 'stopping'" class="btn btn-sm" disabled
                  title="引擎正在退出中，停止完成后才会释放引擎/账户占用">停止中…</button>
                <button v-if="s.status === 'running' && s.id !== watchedSessionId" @click="watchSession(s.id)" class="btn btn-sm">监控</button>
              </div>
              <div class="op-secondary">
                <button
                  v-if="s.status !== 'stopping'"
                  @click="openCashFlows(s)"
                  class="btn btn-sm" data-test="cash-flow-open"
                  title="登记入金/出金/分红等外部现金流（不记台账会扭曲净值、收益率、回撤、日亏）"
                >存取款/分红</button>
                <button
                  v-if="s.status !== 'stopping'"
                  @click="openGuard(s)" class="btn btn-sm" data-test="channel-guard-open"
                  title="通道级守护：检查客户端/桥、运行态引擎、备用通道与待生效配置"
                >通道守护</button>
                <button
                  v-if="(s.split_remainder_count || 0) > 0"
                  @click="openSplitRemainders(s)" class="btn btn-sm" data-test="split-reorder-open"
                  title="停止时丢失的拆单子单剩余量；需会话运行中才能补发"
                >补发拆单剩余（{{ s.split_remainder_count }}）</button>
                <button
                  v-if="s.status !== 'stopping'"
                  @click="openEdit(s)" class="btn btn-sm" data-test="session-edit-open"
                  title="编辑会话配置；运行中除名称外，其余修改下次启动生效"
                >编辑</button>
                <button v-if="s.status === 'stopped'" @click="deleteSession(s)" class="btn btn-sm btn-danger">删除</button>
              </div>
              <!-- 结构化 409：按 data.rule 指名占用者（C1 引擎占用 / C2 账户占用 / C3 键鼠硬拦 / 参数漂移阻断） -->
              <div v-if="startErrors[s.id]" class="start-error" data-test="start-error">{{ startErrors[s.id] }}</div>
              <!-- start 返回的通道建议类告警（非阻断）：如所选引擎与推荐通道不一致 -->
              <div v-if="startWarnings[s.id]?.length" class="start-warning" data-test="start-warning">
                <div v-for="w in startWarnings[s.id]" :key="w">{{ w }}</div>
                <button
                  class="start-warning-close" data-test="start-warning-close"
                  title="收起提示（原文已记录到下方实时事件日志）"
                  @click="dismissStartWarnings(s.id)"
                >×</button>
              </div>
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
          class="field-select" style="width:auto;margin-right:8px"
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
        <button
          class="btn btn-sm" style="margin-left:8px"
          :disabled="watchedSessionId === null"
          @click="openTheoryVsLive"
          title="绑定回测记录（理论）与实盘净值/成交同窗对照"
        >理论 vs 实盘</button>
      </div>
    </div>

    <!-- F2 数据体检：本地数据缺失/陈旧自查；非盘中自动重读一次已落盘数据 -->
    <div
      v-if="watchedSessionId !== null"
      style="display:flex;gap:10px;align-items:center;flex-wrap:wrap;margin:0 0 12px;font-size:13px"
    >
      <button class="btn btn-sm" :disabled="dataHealthLoading" @click="checkDataHealth">
        {{ dataHealthLoading ? '体检中…' : '数据体检' }}
      </button>
      <template v-if="dataHealth">
        <span v-if="dataHealth.available === false" class="badge badge-red">不可用：{{ dataHealth.reason }}</span>
        <template v-else>
          <span class="badge" :class="dataHealth.health?.healthy ? 'badge-green' : 'badge-orange'">
            {{ dataHealth.health?.healthy ? '本地数据齐全' : `缺失/陈旧 ${dataHealth.health?.missing_count ?? 0} 只` }}
          </span>
          <span style="color:var(--ink-soft)">
            {{ (dataHealth.periods_checked || []).join('/') }} · {{ dataHealth.codes_total }} 只
            <template v-if="dataHealth.health?.intraday">（盘中只报告，不自动补齐）</template>
          </span>
          <span v-if="dataHealthProblems.length" style="color:var(--orange, #b45309)">{{ dataHealthProblems.join('；') }}</span>
          <span v-if="dataHealth.auto_fill?.triggered" style="color:var(--ink-soft)">
            非盘中已重读 {{ dataHealth.auto_fill.warmed }}/{{ dataHealth.auto_fill.requested }} 项
          </span>
          <span v-if="!dataHealth.health?.healthy" style="color:var(--ink-soft)">
            缺失：{{ (dataHealth.health?.missing_codes || []).slice(0, 10).join('、') }}{{ (dataHealth.health?.missing_count || 0) > 10 ? ' 等' : '' }}；请在通达信客户端补下载对应周期数据
          </span>
        </template>
      </template>
      <span v-else style="color:var(--ink-soft)">检查本地是否已下载策略周期数据（平台只重读已落盘数据，不能代替通达信下载）</span>
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
      <!-- G1：待放行的顺延委托（人工放行/放弃，全部写审计） -->
      <div style="border:1px solid var(--line);border-radius:6px;padding:10px;margin-bottom:14px" data-test="deferred-panel">
        <div style="display:flex;align-items:center;gap:8px;margin:0 0 6px;flex-wrap:wrap">
          <strong style="font-size:13px">待放行委托</strong>
          <span class="badge" :class="deferredOrders.length ? 'badge-orange' : 'badge-gray'">{{ deferredOrders.length }} 笔</span>
          <span style="font-size:12px;color:var(--ink-faint)">
            信号日未成交、等下一交易日放行的委托；{{ deferredRunning ? '运行中可人工放行/放弃' : '会话未运行，仅可查看' }}
          </span>
          <button class="btn btn-sm" type="button" style="margin-left:auto" @click="loadDeferredOrders()">刷新</button>
        </div>
        <table v-if="deferredOrders.length">
          <thead><tr><th>信号日</th><th>代码</th><th>方向</th><th>数量</th><th>限价</th><th>重试</th><th>队列</th><th>说明</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="o in deferredOrders" :key="o.id">
              <td>{{ o.signal_bar_date || '—' }}</td>
              <td>{{ o.stock_code }}</td>
              <td>{{ tradeTypeLabel(o.trade_type) }}</td>
              <td>{{ o.quantity }}</td>
              <td>{{ o.price ?? '-' }}</td>
              <td>{{ o.retry_count }}</td>
              <td>
                <span class="badge" :class="o.in_memory ? 'badge-green' : 'badge-gray'">
                  {{ o.in_memory ? '可达' : '不在队列' }}
                </span>
              </td>
              <td style="max-width:280px;white-space:normal;color:var(--ink-soft)">{{ o.error_message || '—' }}</td>
              <td style="white-space:nowrap">
                <button
                  class="btn btn-sm" type="button"
                  :disabled="!deferredRunning || !o.in_memory || deferredBusy === o.id"
                  @click="onReleaseDeferred(o)"
                >放行</button>
                <button
                  class="btn btn-sm" type="button" style="margin-left:6px"
                  :disabled="!deferredRunning || deferredBusy === o.id"
                  @click="onAbandonDeferred(o)"
                >放弃</button>
              </td>
            </tr>
          </tbody>
        </table>
        <div v-else class="empty-state" style="padding:6px 0">
          <p style="margin:0">当前没有待放行的顺延委托</p>
        </div>
      </div>
      <!-- G4：盘前效果统计（只读；报出/成交/越价撤单/重排/积压） -->
      <div style="border:1px solid var(--line);border-radius:6px;padding:10px;margin-bottom:14px" data-test="pre-open-stats">
        <div style="display:flex;align-items:center;gap:8px;margin:0 0 6px;flex-wrap:wrap">
          <strong style="font-size:13px">盘前效果</strong>
          <span style="font-size:12px;color:var(--ink-faint)">最近 7 天（北京时间；只读聚合，口径与决策/委托一致）</span>
          <select
            class="field-select" style="margin-left:auto"
            v-model="preOpenGroupBy"
            aria-label="盘前效果聚合粒度" @change="onPreOpenGroupChange"
          >
            <option value="day">按日</option>
            <option value="week">按周</option>
          </select>
          <button class="btn btn-sm" type="button" @click="loadPreOpenStats()">刷新</button>
        </div>
        <table v-if="preOpenStats.length">
          <thead><tr><th>{{ preOpenGroupBy === 'week' ? '周' : '日期' }}</th><th>报出</th><th>成交</th><th>越价撤单</th><th>重排</th><th>拦截</th><th>积压</th></tr></thead>
          <tbody>
            <tr v-for="row in preOpenStats" :key="row.bucket">
              <td>{{ row.bucket }}</td>
              <td>{{ row.reported }}</td>
              <td>{{ row.filled }}</td>
              <td>{{ row.cancel_crossed }}</td>
              <td>{{ row.requeued }}</td>
              <td>{{ row.blocked }}</td>
              <td>{{ row.deferred }}</td>
            </tr>
            <tr v-if="preOpenTotals" style="font-weight:600">
              <td>合计</td>
              <td>{{ preOpenTotals.reported }}</td>
              <td>{{ preOpenTotals.filled }}</td>
              <td>{{ preOpenTotals.cancel_crossed }}</td>
              <td>{{ preOpenTotals.requeued }}</td>
              <td>{{ preOpenTotals.blocked }}</td>
              <td>{{ preOpenTotals.deferred }}</td>
            </tr>
          </tbody>
        </table>
        <div v-else class="empty-state" style="padding:6px 0">
          <p style="margin:0">区间内没有盘前活动</p>
        </div>
      </div>
      <div style="display:flex;align-items:center;gap:8px;margin:0 0 8px">
        <span style="font-size:12px;color:var(--ink-soft)">时间范围</span>
        <select class="field-select field-select--compact" v-model="orderRange" aria-label="委托时间范围">
          <option v-for="opt in TIME_RANGE_OPTIONS" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
        </select>
        <span style="font-size:12px;color:var(--ink-faint)">按北京时间自然日 · 共 {{ filteredOrders.length }} 条</span>
      </div>
      <table>
        <thead><tr><th>时间（北京时间）</th><th>方向</th><th>代码</th><th>状态</th><th>数量</th><th>价格</th><th>已成交</th><th>进度</th><th>错误</th></tr></thead>
        <tbody>
          <tr v-for="o in filteredOrders" :key="o.key">
            <td style="color:var(--ink-soft)">{{ o.time || '—' }}</td>
            <td>{{ tradeTypeLabel(o.trade_type) }}</td>
            <td>{{ o.stock_code }}</td>
            <td><span class="badge" :class="o.status === 'filled' ? 'badge-green' : o.status === 'rejected' ? 'badge-red' : 'badge-gray'">{{ ORDER_STATUS_LABEL[o.status] || o.status }}</span></td>
            <td>{{ o.quantity }}</td>
            <td>{{ o.price ?? '-' }}</td>
            <td>{{ o.filled_quantity ?? '-' }}</td>
            <td style="font-size:12px;color:var(--ink-soft);white-space:nowrap" data-test="order-progress">
              <template v-if="o.split_seq && o.split_summary">
                子单 {{ o.split_seq }}/{{ o.split_summary.total }} · 成交 {{ o.split_summary.filled_children }} 笔 · 剩 {{ o.split_summary.remaining_quantity }} 股
              </template>
              <template v-else>—</template>
              <span
                v-if="o.requote_attempt"
                class="badge badge-gray" style="margin-left:4px"
              >追价 {{ o.requote_attempt }} 次</span>
            </td>
            <td style="color:var(--red);max-width:280px;white-space:normal">{{ o.error_message ?? '-' }}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="filteredOrders.length === 0" class="empty-state">
        <p>{{ orderRange === 'today' ? '今日暂无委托' : '该时间范围暂无委托' }}</p>
      </div>
    </div>

    <!-- 待审批（人工审批队列） -->
    <ApprovalPanel
      v-if="wbTab === 'approvals'"
      :session-id="watchedSessionId"
      :running="runningSessions.some((s) => s.id === watchedSessionId)"
    />

    <!-- 成交 -->
    <div v-if="wbTab === 'trades'" class="table-wrap">
      <div style="display:flex;align-items:center;gap:8px;margin:0 0 8px">
        <span style="font-size:12px;color:var(--ink-soft)">时间范围</span>
        <select class="field-select field-select--compact" v-model="tradeRange"
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
        <select class="field-select field-select--compact" v-model="decRange" aria-label="决策时间范围">
          <option v-for="opt in TIME_RANGE_OPTIONS" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
        </select>
        <span style="font-size:12px;color:var(--ink-faint)">按北京时间自然日 · 聚合与统计随筛选重算 · 共 {{ filteredDecisions.length }} 条事件</span>
        <span style="margin-left:auto;font-size:12px;color:var(--ink-soft)">
          未处理 {{ decisionAckSummary.open }} 条 · 已确认 {{ decisionAckSummary.acked }} 条 · 已归档 {{ decisionAckSummary.archived }} 条
        </span>
      </div>
      <!-- 批量确认：必须带筛选条件（不提供「全选所有」）；高危门类永远排除，只能逐条确认。
           先预览「将确认 N 条」，再二次确认提交；原因必填并写入逐条审计。 -->
      <div
        data-test="decisions-batch"
        style="border:1px solid var(--line);border-radius:8px;padding:8px 10px;margin:0 0 10px;display:flex;flex-wrap:wrap;align-items:center;gap:6px"
      >
        <span style="font-size:12px;font-weight:700;color:var(--ink-soft)">批量确认（必须筛选）</span>
        <select class="field-select field-select--compact" v-model="decBatch.gate" aria-label="批量确认门类">
          <option value="">门类 gate · 请选择</option>
          <option v-for="g in decBatchGateOptions" :key="g" :value="g">{{ gateLabel(g) }} ({{ g }})</option>
        </select>
        <select class="field-select field-select--compact" v-model="decBatch.layer" aria-label="批量确认层级">
          <option value="">层级 layer · 请选择</option>
          <option v-for="l in decBatchLayerOptions" :key="l" :value="l">{{ layerLabel(l) }} ({{ l }})</option>
        </select>
        <input style="font-size:12px;padding:2px 6px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--ink)" type="date" v-model="decBatch.start" aria-label="批量确认开始日期" />
        <input style="font-size:12px;padding:2px 6px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--ink)" type="date" v-model="decBatch.end" aria-label="批量确认结束日期" />
        <input style="font-size:12px;padding:2px 6px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--ink)" v-model="decBatch.keyword" placeholder="关键字（消息/股票/参数）" aria-label="批量确认关键字" />
        <input style="font-size:12px;padding:2px 6px;border:1px solid var(--line);border-radius:6px;background:var(--bg);color:var(--ink)" v-model="decBatch.note" placeholder="确认原因（必填，写入审计）" aria-label="批量确认原因" />
        <button
          class="btn ghost"
          data-test="decisions-batch-preview"
          style="font-size:12px;padding:2px 10px"
          :disabled="decisionsLoading || decBatchLoading || !decBatchFilterActive"
          :title="decBatchFilterActive ? '按筛选条件统计将确认条数' : '请先选择至少一个筛选条件（不支持全选）'"
          @click="previewDecisionBatch"
        >预览</button>
        <button
          class="btn"
          data-test="decisions-batch-submit"
          style="font-size:12px;padding:2px 10px"
          :disabled="decisionsLoading || decBatchLoading || !decBatchPreview || decBatchPreview.matched <= 0 || !decBatch.note.trim()"
          @click="submitDecisionBatch"
        >确认 {{ decBatchPreview ? decBatchPreview.matched : 0 }} 条</button>
        <span v-if="decBatchMsg" style="font-size:12px;color:var(--ink-soft)">{{ decBatchMsg }}</span>
        <span v-if="decBatchError" style="font-size:12px;color:var(--red)">{{ decBatchError }}</span>
        <span style="flex-basis:100%;font-size:11px;color:var(--ink-faint)">
          高危门类（cash_diff / equity_deviation / engine_owner_mismatch / sell_scope / split_children_lost / out_of_pool_position）不参与批量，需逐条查看后确认；超过 7 天未处理的普通事件自动归档（仍可查询回看）。
        </span>
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
              <span v-if="d.ack_status === 'archived'" class="badge badge-gray"
                    :title="d.archive_reason || '超过 7 天未处理，已自动归档（可查询回看）'">已归档</span>
              <span v-else-if="d.ack_status === 'acked'" class="badge badge-gray"
                    :title="[d.ack_note, d.ack_by ? '操作者 ' + d.ack_by : ''].filter(Boolean).join(' · ') || '已确认'">已确认</span>
              <button
                v-else-if="d.id"
                class="btn ghost"
                style="font-size:11px;padding:1px 8px"
                :disabled="ackingEventId === d.id"
                :title="HIGH_RISK_GATES.has(d.gate) ? '高危门类：请先核对事件内容再逐条确认（不参与批量）' : '确认此事件'"
                @click="ackDecisionRow(d)"
              >确认{{ HIGH_RISK_GATES.has(d.gate) ? '（高危·逐条）' : '' }}</button>
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

    <!-- E2 门控快照：每组合每交易日一条，回看当日生效方向/强度与数据截至日 -->
    <div v-if="wbTab === 'gate'" class="table-wrap">
      <div style="display:flex;justify-content:flex-end;align-items:center;margin:0 0 6px">
        <button class="btn btn-sm" :disabled="gateSnapshotsLoading"
                data-test="reload-gate-snapshots" @click="reloadGateSnapshots()">
          {{ gateSnapshotsLoading ? '刷新中…' : '↻ 刷新快照' }}
        </button>
      </div>
      <p style="font-size:12px;color:var(--ink-faint);margin:0 0 6px">
        仅启用市场门控的组合在每个交易日首根 bar 落一条；方向为「当日生效」的右移一日收盘状态（1 多头 / 0 中性 / -1 空头）。
      </p>
      <table v-if="gateSnapshots.length > 0">
        <thead><tr>
          <th>日期</th><th>组合</th><th>基准</th><th>方向</th><th>强度</th>
          <th>数据截至</th><th>引擎</th>
        </tr></thead>
        <tbody>
          <tr v-for="g in gateSnapshots" :key="g.id" :data-test="`gate-snapshot-${g.id}`">
            <td>{{ g.snap_date || '—' }}</td>
            <td>{{ portfolioName(g.portfolio_id) }}</td>
            <td>{{ g.benchmark_index || '—' }}</td>
            <td><span class="badge" :class="gateSnapshotBadge(g.direction)">{{ gateSnapshotLabel(g.direction) }}</span></td>
            <td>{{ g.strength ?? '—' }}</td>
            <td>{{ g.as_of || '—' }}</td>
            <td>{{ g.gate_engine || '—' }}</td>
          </tr>
        </tbody>
      </table>
      <div v-else class="empty-state">
        <p>暂无门控快照：仅启用市场门控的组合会在每个交易日首根 bar 落一条记录（最近 30 天）。</p>
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
          class="field-select" style="width:auto"
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

  <!-- N2/K7 理论 vs 实际对照 -->
  <div v-if="showTvl" class="modal-overlay modal-lg" @click.self="showTvl = false">
    <div class="modal-content">
      <div style="display:flex;align-items:center;justify-content:space-between;gap:8px">
        <h3 style="margin:0">理论 vs 实盘</h3>
        <button class="btn btn-sm" @click="showTvl = false">关闭</button>
      </div>
      <div class="modal-body">
        <div v-if="tvlLoading"><p>加载中…</p></div>
        <div v-else-if="tvlError" style="color:var(--red)">{{ tvlError }}</div>
        <template v-else-if="theoryVsLive">
          <!-- F5：拼盘/运行组会话按成员对照（只读）；单记录绑定区不适用 -->
          <template v-if="theoryVsLive.pairing?.bound">
            <div class="baseline-preview" data-test="pairing-compare">
              <div>
                拼盘对照：
                {{ theoryVsLive.pairing.source === 'pairing_plan' ? '拼盘' : '运行组' }}
                #{{ theoryVsLive.pairing.plan_id }} {{ theoryVsLive.pairing.plan_name || '' }}
                <span class="badge badge-gray">{{ theoryVsLive.pairing.plan_status || '—' }}</span>
                <span v-if="theoryVsLive.pairing.run_group_id" class="label-note">
                  运行组 #{{ theoryVsLive.pairing.run_group_id }}
                </span>
              </div>
              <div v-if="!theoryVsLive.pairing.found" class="note" style="color:var(--red)">
                绑定的拼盘已缺失：理论侧无法生成，实盘侧仍按会话展示。
              </div>
            </div>
            <div v-if="tvlInheritance?.enabled" class="baseline-preview" data-test="shadow-health">
              <div>
                影子健康（只读）：
                <span class="badge" :class="tvlInheritance.drift_count ? 'badge-orange' : 'badge-green'">
                  漂移 {{ tvlInheritance.drift_count ?? 0 }} 项
                </span>
                <span v-if="tvlInheritance.haircut">
                  合成折扣 {{ tvlInheritance.haircut.composite }}
                </span>
                <span v-if="tvlInheritance.found === false" class="badge badge-red">拼盘缺失</span>
              </div>
              <div v-if="tvlInheritance.drift_fields?.length" class="note" style="color:var(--orange)">
                漂移字段：{{ tvlInheritance.drift_fields.join('、') }}
              </div>
              <div v-for="(w, i) in tvlInheritance.warnings || []" :key="i" class="note" style="color:var(--orange)">
                {{ w.message || w.code }}<span v-if="w.field">（{{ w.field }}）</span>
              </div>
            </div>
            <div class="table-container" data-test="pairing-members">
              <table>
                <thead>
                  <tr><th>成员</th><th>理论</th><th>实盘</th><th>同日收益差</th><th>同日回撤差</th><th>提示</th></tr>
                </thead>
                <tbody>
                  <tr v-for="m in theoryVsLive.pairing.members" :key="m.strategy_id">
                    <td>
                      {{ m.role === 'lender' ? '出借方' : '借款方' }}
                      #{{ m.strategy_id }} {{ m.strategy_name || '' }}
                    </td>
                    <td>
                      <span v-if="m.theory.record_id">#{{ m.theory.record_id }}</span>
                      <span v-else>—</span>
                      <span class="label-note">{{ m.theory.status }}</span>
                    </td>
                    <td>{{ m.live.status }}（{{ m.live.curve.length }} 天）</td>
                    <td>{{ _tvlPct(m.deviation?.return_gap ?? null) }}</td>
                    <td>{{ _tvlPct(m.deviation?.max_drawdown_gap ?? null) }}</td>
                    <td>
                      <span v-for="(w, i) in m.warnings" :key="i" class="note" style="color:var(--orange)">
                        {{ w.message || w.code }}
                      </span>
                      <span v-if="!m.warnings.length">—</span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p class="note">
              拼盘会话只做成员级对照，不做顶层聚合；实盘借据与强制借贷在阶段 G 才启用。
            </p>
          </template>

          <template v-if="!theoryVsLive.pairing?.bound">
          <div class="tvl-bind">
            <label>理论基准</label>
            <select v-model="tvlBindingValue" :disabled="tvlRecordsLoading || tvlBindingSaving">
              <option value="auto">自动关联（同组合最近一次已完成回测）</option>
              <option v-for="r in tvlRecordOptions" :key="r.id" :value="String(r.id)">
                #{{ r.id }} {{ r.name }} · {{ portfolioName(r.portfolio_strategy_id) }}
                （{{ r.start_date || '?' }} ~ {{ r.end_date || '?' }}，{{ recordStatusLabel(r.status) }}）
              </option>
            </select>
            <button class="btn btn-sm" :disabled="tvlBindingSaving" @click="saveTvlBinding">
              {{ tvlBindingSaving ? '保存中…' : '保存绑定' }}
            </button>
            <span v-if="tvlBindingMsg" class="note">{{ tvlBindingMsg }}</span>
          </div>
          <p v-if="tvlSellScopeFull" class="note" style="color:var(--orange)">
            实盘已纳入存量/外部仓代卖：理论对照仍按平台自建仓口径计算，
            解释力下降，仅作趋势参考，不可据此判定策略失效。
          </p>
          <p v-if="!theoryVsLive.bound" class="note" style="color:var(--red)">
            未绑定回测记录，且未找到该会话组合的已完成回测：理论侧无法生成
            （实盘侧不受影响）。可在上方选择回测记录并保存。
          </p>
          <p v-else class="note">
            理论基准：#{{ theoryVsLive.backtest_record_id }} {{ theoryVsLive.backtest_name }}
            <span v-if="theoryVsLive.window.start">
              （{{ theoryVsLive.window.start }} ~ {{ theoryVsLive.window.end }}）
            </span>
            <span v-if="theoryVsLive.binding_source === 'auto'" class="badge badge-gray">
              自动关联
            </span>
            <span v-else class="badge badge-green">手动绑定</span>
          </p>
          <p v-for="(w, i) in theoryVsLive.warnings" :key="i" class="note" style="color:var(--orange)">
            {{ w }}
          </p>

          <h4 style="margin:8px 0">归一净值叠加（首日=1）</h4>
          <div v-if="tvlChart" style="overflow-x:auto">
            <svg :width="tvlChart.W" :height="tvlChart.H" role="img">
              <line :x1="0" :y1="tvlChart.H / 2" :x2="tvlChart.W" :y2="tvlChart.H / 2"
                    stroke="var(--line)" stroke-dasharray="3,3" />
              <path v-for="s in tvlChart.paths" :key="s.key" :d="s.path"
                    fill="none" :stroke="s.color" stroke-width="1.5" />
            </svg>
            <div style="display:flex;gap:16px;font-size:12px;color:var(--ink-soft)">
              <span v-for="s in tvlChart.paths" :key="s.key">
                <span :style="{ color: s.color }">■</span> {{ s.label }}
              </span>
              <span>区间 {{ tvlChart.min.toFixed(3) }} ~ {{ tvlChart.max.toFixed(3) }}</span>
            </div>
          </div>
          <p v-else class="note">
            暂无可绘曲线：
            <span v-if="theoryVsLive.theory_series_status === 'insufficient'">
              理论侧仅 1 个快照，无法成线；
            </span>
            <span v-else-if="theoryVsLive.theory_series_status !== 'ok'">
              理论侧无可用日快照；
            </span>
            <span v-if="theoryVsLive.live_series_status !== 'ok'">实盘日序列未生成；</span>
          </p>

          <h4 style="margin:16px 0 8px">指标对照</h4>
          <div class="table-container">
            <table>
              <thead><tr><th>指标</th><th>理论</th><th>实盘</th><th>差异</th></tr></thead>
              <tbody>
                <tr>
                  <td>累计收益</td>
                  <td>{{ _tvlPct(theoryVsLive.theory.metrics.total_return ?? null) }}</td>
                  <td>{{ _tvlPct(theoryVsLive.live.metrics.total_return ?? null) }}</td>
                  <td>{{ _tvlPct(theoryVsLive.deviation?.return_gap ?? null) }}</td>
                </tr>
                <tr>
                  <td>最大回撤</td>
                  <td>{{ _tvlPct(theoryVsLive.theory.metrics.max_drawdown ?? null) }}</td>
                  <td>{{ _tvlPct(theoryVsLive.live.metrics.max_drawdown ?? null) }}</td>
                  <td>{{ _tvlPct(theoryVsLive.deviation?.max_drawdown_gap ?? null) }}</td>
                </tr>
                <tr>
                  <td>成交笔数</td>
                  <td>{{ tvlMetric('theory', 'trades_count') }}</td>
                  <td>{{ tvlMetric('live', 'trades_count') }}</td>
                  <td>{{ theoryVsLive.deviation?.trades_gap ?? '—' }}</td>
                </tr>
                <tr>
                  <td>委托 / 完全成交 / 部分成交</td>
                  <td>—</td>
                  <td>
                    {{ tvlMetric('live', 'orders_count') }} /
                    {{ tvlMetric('live', 'filled_orders') }} /
                    {{ tvlMetric('live', 'partial_orders') }}
                  </td>
                  <td>{{ _tvlPct(theoryVsLive.live.metrics.fill_rate ?? null) }} 成交率</td>
                </tr>
                <tr>
                  <td>成交滑移（买贵/卖贱为正）</td>
                  <td>—</td>
                  <td colspan="2">
                    <span v-if="theoryVsLive.deviation
                      && theoryVsLive.deviation.slippage.weighted_deviation_bps != null">
                      {{ theoryVsLive.deviation.slippage.weighted_deviation_bps }} bps
                      （匹配 {{ theoryVsLive.deviation.slippage.matched_pairs }} 对）
                    </span>
                    <span v-else-if="!theoryVsLive.bound">未绑定回测记录，无法计算滑移</span>
                    <span v-else-if="!theoryVsLive.deviation?.slippage.matched_pairs">
                      无可匹配成交（无同日/同股票/同方向的理论与实盘成交样本）
                    </span>
                    <span v-else>—</span>
                  </td>
                </tr>
              </tbody>
            </table>
          </div>
          <p class="note">
            理论指标按回测区间、实盘指标按实盘区间计算；「差异」列只在两者重叠
            ≥2 个交易日时计算，否则显示「—」并由上方告警说明原因。
          </p>

          <template v-if="theoryVsLive.deviation?.slippage.details.length">
            <h4 style="margin:16px 0 8px">滑移明细（前 20 对）</h4>
            <div class="table-container" style="max-height:260px;overflow:auto">
              <table>
                <thead>
                  <tr><th>日期</th><th>股票</th><th>方向</th><th>理论价</th><th>实盘价</th><th>偏差 bps</th></tr>
                </thead>
                <tbody>
                  <tr v-for="(p, i) in theoryVsLive.deviation.slippage.details" :key="i">
                    <td>{{ p.date }}</td>
                    <td>{{ p.stock_code }}</td>
                    <td>{{ tradeTypeLabel(p.trade_type) }}</td>
                    <td>{{ p.theory_price }}</td>
                    <td>{{ p.live_price }}</td>
                    <td :style="{ color: p.deviation_bps > 0 ? 'var(--red)' : 'var(--green)' }">
                      {{ p.deviation_bps }}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </template>
          </template>
        </template>
      </div>
      <div class="modal-actions">
        <button class="btn" @click="showTvl = false">关闭</button>
      </div>
    </div>
  </div>

  <div v-if="showCreate" class="modal-overlay modal-lg modal-dense" @click.self="showCreate = false">
    <div class="modal-content">
      <h3>新建实盘</h3>
      <div class="modal-body">
      <label>名称</label><input v-model="form.name" placeholder="例如：模拟盘A" />
      <label>模式</label><select v-model="form.mode"><option value="simulation">仿真</option><option value="live">实盘</option></select>
      <label>执行模式</label>
      <select v-model="form.execution_mode">
        <option value="alert">预警（半自动，仅 TQ 行情 + send_warn 推送，不下单）</option>
        <option value="auto">自动（按交易引擎下单）</option>
        <option value="manual">人工审批（信号入队，人工确认后下单）</option>
        <option value="shadow">影子（真实行情 + 本地模拟成交，不发真实委托）</option>
      </select>
      <label>交易引擎<span class="label-note">（决定信号计算的数据源）</span></label>
      <select v-model="form.engine" data-field="create-engine" @change="onEngineChange">
        <option v-for="e in channel?.engines || []" :key="e.key" :value="e.key" :disabled="engineOptionDisabled(e)">
          {{ engineOptionText(e) }}
        </option>
      </select>
      <!-- B4 通道能力矩阵：拆单仅通达信支持，备注认领仅 QMT/掘金桥支持 -->
      <div v-if="Object.keys(selectedEngineCaps).length" class="capability-row" data-test="channel-capabilities">
        <span
          v-for="(ok, key) in selectedEngineCaps" :key="key"
          class="capability-chip" :class="{ off: !ok }"
          :title="ok ? '支持' : '该通道不支持'"
        >{{ ok ? '✓' : '✗' }} {{ capabilityLabel(String(key)) }}</span>
      </div>
      <p
        v-if="selectedEngineCaps.split_order === false"
        class="capability-hint" data-test="split-unsupported-hint"
      >
        该通道不支持大额拆单：组合策略里的「拆单」开关在此通道不会生效（仅通达信 TQ 支持）。
      </p>
      <label>资金池模式<span class="label-note">（可选增强，默认关闭）</span></label>
      <select v-model="form.capital_pool_mode" data-field="create-capital-pool">
        <option value="off">关闭（分账不分池）</option>
        <option value="equal">等权池（各组合均分现金）</option>
        <option value="risk_budget">风险预算（按 1/最大回撤分配）</option>
        <option value="priority">优先级池（按组合下单优先级分配）</option>
      </select>
      <label v-if="form.capital_pool_mode !== 'off'" style="display:flex;align-items:center;gap:8px">
        <input v-model="form.capital_pool_rebalance_enabled" type="checkbox" style="width:auto" />
        每交易日首 bar 再平衡（只挪虚拟现金，不强制卖出）
      </label>
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
      <label>实盘资金基准<span class="label-note">（元）</span></label>
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
      <label>外部信号文件路径<span class="label-note">（通达信条件预警输出，可选）</span></label>
      <input v-model="form.signal_file_path" placeholder="留空 = 走 TQ 公式计算信号；示例 D:\TDX\T0002\export\BDXH.txt" />
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
        填写后（如 D:\project\分时突击.txt）引擎每 5 秒（<code>tq_trade.external_signal_poll_interval_seconds</code>，1–60s）检查该文件是否有新增行并注入交易链路——绕过 TQ 公式引擎的周期限制（如 1m 级别）。文件由通达信「条件预警」动态输出（<strong>GBK + CRLF + Tab 分隔，追加写入</strong>），每行形如 <code>300456	赛微电子	2026-09-08 23:08	37.61	-2.77%	5387	波段加仓</code>（行尾多余 Tab 可容忍；旧版带行号列的文件也兼容）。公式名必须与该会话组合策略里的公式名一致，否则该行被忽略。填写即<strong>替换信号来源</strong>：该会话不再跑 TQ 公式计算（风控/仓位/资金等限制条件仍沿用回测口径）；仅自动模式生效，预警模式不读取该文件。路径不存在不会静默失效——启动日志会报错并持续轮询等待文件出现。
      </p>
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
        <strong>买卖方向</strong>：信号动作分买入类（OPEN / ADD）与卖出类（REDUCE / CLOSE），方向由该策略公式信号配置的 <code>signal_type</code> 决定（预警文件本身不含方向列）。<br />
        ① <strong>买入类</strong>：沿用原有开仓闸门（资金占比 / 最大持仓数 / 单仓金额 / 敞口上限 / 可用现金），满槽或已持仓跳过；槽位上限取各组合自己的 <code>strategies.max_positions</code>（缺失则跳过槽位闸并告警，不写死 5）。<br />
        ② <strong>卖出类</strong>：不做仓位与资金检查，但受两条硬约束——<strong>无持仓不卖</strong>（不得卖空）、<strong>T+1 实际可用为 0 时拒绝</strong>（沿用实盘 T+1 可用股数校验，不绕过）。<br />
        ③ <strong>归属过滤</strong>：代码不在该会话任何目标策略的股票池内且无持仓 → 不产生委托，留 <code>external signal out of scope</code> 日志。<br />
        ④ <strong>减仓比例</strong>：预警文件没有数量/比例列，<strong>REDUCE 未给出比例时按清仓执行</strong>（日志与风控事件明确标注「未给出比例，按清仓执行」，不静默猜测百分比）；CLOSE 恒清仓。<br />
        ⑤ <strong>去重</strong>：同代码 + 同日 + 同方向各认第一条（买卖互不吞并；同方向重复推送不重复下单）。去重键 = 代码 + 交易日 + 方向（会话内生效，跨会话不共享）。未知/不可解析的动作一律忽略并节流告警，绝不猜测方向或默认成买入。
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
      <label style="display:flex;align-items:center;gap:8px">
        <input v-model="form.gate_recheck_on_execute" type="checkbox" style="width:auto" data-field="gate_recheck_on_execute" />
        执行前重判门控（顺延次日单 / 盘前竞价单提交前再判一次）
      </label>
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 12px">
        默认关闭：信号日门控通过即按原决策执行。开启后，顺延次日单与盘前集合竞价单在真正提交前用「执行日前一收盘」的门控状态再判一次；若为空头则直接取消并落决策事件（gate=market_gate_recheck），不拿旧决策硬执行。仅对启用市场门控的组合生效。
      </p>
      <label>盘前集合竞价参数<span class="label-note">（会话级覆盖）</span></label>
      <select v-model="form.pre_open_auction_enabled" data-field="pre_open_auction_enabled">
        <option :value="null">跟随全局（默认开启，09:15–09:20 / 让价 2%）</option>
        <option :value="true">本会话开启</option>
        <option :value="false">本会话关闭（走次日首根 1m bar 放行）</option>
      </select>
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 8px">
        仅覆盖本会话，不改全局 config.yaml。关闭后该会话日线级信号不再报集合竞价，统一等次一交易日首根完成 1m bar（约 09:31）放行。
      </p>
      <label>盘前窗口结束时间<span class="label-note">（HH:MM，留空跟随全局 09:20）</span></label>
      <input v-model="form.pre_open_auction_end_time" placeholder="09:20（可选 09:15–09:30）" data-field="pre_open_auction_end_time" />
      <label>盘前让价 %<span class="label-note">（留空跟随全局 2%）</span></label>
      <input v-model="form.pre_open_auction_price_bias_pct" type="number" step="0.1" min="0" max="10" placeholder="2（0–10）" data-field="pre_open_auction_price_bias_pct" />
      <label>组合策略<span class="label-note">（可多选）</span></label>
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
      <label>绑定拼盘<span class="label-note">（阶段 F 影子，可选）</span></label>
      <select v-model="form.pairing_plan_id" :disabled="pairingPlansLoading" data-field="pairing-plan">
        <option :value="null">不绑定（普通实盘）</option>
        <option v-for="p in pairingPlans" :key="p.id" :value="p.id">
          #{{ p.id }} {{ p.name }}（{{ p.scope }} · {{ p.status }}）
        </option>
      </select>
      <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 8px">
        绑定后仅用于理论 vs 实盘按成员对照与影子健康记录；不改下单、数量、拆单或审批。
        与「理论基准」单回测绑定三选一，绑定拼盘会清空单回测绑定。阶段 G 前不执行强制借贷。
      </p>
      </div>
      <div class="modal-actions">
        <button @click="create" class="btn btn-primary">确认</button>
        <button @click="showCreate = false" class="btn">取消</button>
      </div>
    </div>
  </div>

  <!-- B3 通道守护：前置检查清单 + 一键重新探活（不自动拉起终端/重启会话） -->
  <div v-if="guardModal" class="modal-overlay" data-test="channel-guard-modal" @click.self="guardModal = null">
    <div class="modal-content">
      <h3>通道守护「{{ guardModal.name }}」（{{ engineLabel(guard?.engine || guardModal.engine) }}）</h3>
      <div class="modal-body">
        <p v-if="guardLoading" class="muted">检查中…</p>
        <template v-else-if="guard">
          <ul class="guard-check-list">
            <li v-for="c in guard.checks" :key="c.key" :class="{ ok: c.ok }" data-guard-check>
              <b>{{ c.ok ? '✓' : '✗' }} {{ c.label }}</b>
              <span>{{ c.detail }}</span>
            </li>
          </ul>
          <div v-if="guard.actions.length" class="baseline-preview warn" data-test="channel-guard-actions">
            <div v-for="(a, i) in guard.actions" :key="i">· {{ a }}</div>
          </div>
          <p v-else class="muted">未发现阻断项。</p>
          <p v-if="guardMessage" class="baseline-preview" data-test="channel-guard-message">{{ guardMessage }}</p>
        </template>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" :disabled="guardLoading" data-test="channel-guard-reconnect" @click="recheckChannel">
          {{ guardLoading ? '检查中…' : '重新探活' }}
        </button>
        <button
          v-if="guard?.ready_to_switch"
          class="btn" data-test="channel-guard-switch"
          @click="switchFromGuard"
        >去切换备用通道</button>
        <button class="btn" @click="guardModal = null">关闭</button>
      </div>
    </div>
  </div>

  <!-- H2 拆单剩余量：停止时未提交的子单，运行中可一键补发 -->
  <div v-if="splitModal" class="modal-overlay" data-test="split-reorder-modal" @click.self="splitModal = null">
    <div class="modal-content">
      <h3>补发拆单剩余「{{ splitModal.name }}」</h3>
      <div class="modal-body">
        <p v-if="splitModal.status !== 'running'" class="baseline-preview warn" data-test="split-reorder-need-running">
          会话当前未运行：请先启动会话，补发会逐笔重走资金/风控/T+1 闸门。
        </p>
        <p v-if="splitLoading" class="muted">读取中…</p>
        <table v-else-if="splitItems.length" class="data-table" data-test="split-reorder-table">
          <thead>
            <tr><th>股票</th><th>方向</th><th>剩余数量</th><th>参考价</th><th>信号</th><th>丢失时间</th></tr>
          </thead>
          <tbody>
            <tr v-for="it in splitItems" :key="it.id">
              <td>{{ it.stock_code }}</td>
              <td>{{ it.trade_type === 'SELL' ? '卖出' : '买入' }}</td>
              <td>{{ it.quantity }}</td>
              <td>{{ it.price ?? '—' }}</td>
              <td>{{ it.signal_name || it.signal_type || '—' }}</td>
              <td>{{ it.created_at || '—' }}</td>
            </tr>
          </tbody>
        </table>
        <p v-if="splitMessage" class="baseline-preview" data-test="split-reorder-message">{{ splitMessage }}</p>
      </div>
      <div class="modal-actions">
        <button
          class="btn btn-primary" data-test="split-reorder-submit"
          :disabled="splitLoading || splitItems.length === 0 || splitModal.status !== 'running'"
          @click="submitSplitReorder"
        >确认补发</button>
        <button class="btn" @click="splitModal = null">关闭</button>
      </div>
    </div>
  </div>

  <!-- B1 + A4 编辑会话：未运行立即生效；运行中除名称外入待生效队列 -->
  <div v-if="editModal" class="modal-overlay modal-lg modal-dense" data-test="session-edit-modal" @click.self="editModal = null">
    <div class="modal-content">
      <h3>编辑会话 #{{ editModal.id }}「{{ editModal.name }}」</h3>
      <div class="modal-body">
        <p v-if="editModal.status !== 'stopped'" class="baseline-preview warn" data-test="session-edit-running-hint">
          会话运行中：名称立即生效；引擎 / 执行模式 / 资金池 / 外部信号 / 组合增删将在下次启动前应用，
          不影响当前正在跑的策略与持仓。
        </p>
        <label>名称</label>
        <input v-model="editForm.name" data-field="edit-name" />
        <label>交易引擎</label>
        <select v-model="editForm.engine" data-field="edit-engine">
          <option v-for="e in channel?.engines || []" :key="e.key" :value="e.key" :disabled="engineOptionDisabled(e)">
            {{ engineOptionText(e) }}
          </option>
        </select>
        <label>执行模式</label>
        <select v-model="editForm.execution_mode" data-field="edit-execution-mode">
          <option value="alert">预警（半自动，不下单）</option>
          <option value="auto">自动（按交易引擎下单）</option>
          <option value="manual">人工审批（信号入队，人工确认后下单）</option>
          <option value="shadow">影子（真实行情 + 本地模拟成交）</option>
        </select>
        <label>备用通道<span class="label-note">（主备切换，可选）</span></label>
        <select v-model="editForm.backup_engine" data-field="edit-backup-engine">
          <option value="">未配置</option>
          <option v-for="e in channel?.engines || []" :key="e.key" :value="e.key" :disabled="e.key === editForm.engine">
            {{ e.label }}
          </option>
        </select>
        <label style="display:flex;align-items:center;gap:8px">
          <input v-model="editForm.failover_enabled" type="checkbox" style="width:auto" data-field="edit-failover-enabled" />
          主通道离线时提示切换备用通道（默认仅提示，实际切换需人工确认）
        </label>
        <div v-if="editHealth" class="baseline-preview" data-test="channel-health">
          主通道 {{ editHealth.primary?.label || '—' }}：
          <b :style="{ color: editHealth.primary?.online ? 'var(--green)' : 'var(--red)' }">
            {{ editHealth.primary?.online ? '在线' : '离线' }}
          </b>
          <span v-if="editHealth.backup">
            ｜备用 {{ editHealth.backup.label }}：
            <b :style="{ color: editHealth.backup.online ? 'var(--green)' : 'var(--red)' }">
              {{ editHealth.backup.online ? '在线' : '离线' }}
            </b>
          </span>
          <button
            v-if="editHealth.backup"
            class="btn btn-sm" style="margin-left:8px"
            :disabled="editSwitching" data-test="channel-failover"
            @click="switchToBackup"
          >{{ editSwitching ? '切换中…' : '一键切换备用通道' }}</button>
          <div style="font-size:11px;color:var(--ink-faint);margin-top:4px">{{ editHealth.advice }}</div>
        </div>
        <label>资金池模式</label>
        <select v-model="editForm.capital_pool_mode" data-field="edit-capital-pool">
          <option value="off">关闭（分账不分池）</option>
          <option value="equal">等权池（各组合均分现金）</option>
          <option value="risk_budget">风险预算（按 1/最大回撤分配）</option>
          <option value="priority">优先级池（按组合下单优先级分配）</option>
        </select>
        <label v-if="editForm.capital_pool_mode !== 'off'" style="display:flex;align-items:center;gap:8px">
          <input v-model="editForm.capital_pool_rebalance_enabled" type="checkbox" style="width:auto" />
          每交易日首 bar 再平衡（只挪虚拟现金，不强制卖出）
        </label>
        <label>外部信号文件路径<span class="label-note">（可选）</span></label>
        <input v-model="editForm.signal_file_path" placeholder="留空 = 走 TQ 公式计算信号；示例 D:\TDX\T0002\export\BDXH.txt" data-field="edit-signal-path" />
        <label style="display:flex;align-items:center;gap:8px">
          <input v-model="editForm.gate_recheck_on_execute" type="checkbox" style="width:auto" data-field="edit-gate-recheck" />
          执行前重判门控（顺延次日单 / 盘前竞价单提交前再判一次，默认关）
        </label>
        <label>盘前集合竞价参数<span class="label-note">（会话级覆盖）</span></label>
        <select v-model="editForm.pre_open_auction_enabled" data-field="edit-pre-open-enabled">
          <option :value="null">跟随全局（默认开启，09:15–09:20 / 让价 2%）</option>
          <option :value="true">本会话开启</option>
          <option :value="false">本会话关闭（走次日首根 1m bar 放行）</option>
        </select>
        <label>盘前窗口结束时间<span class="label-note">（HH:MM，留空跟随全局）</span></label>
        <input v-model="editForm.pre_open_auction_end_time" placeholder="09:20（可选 09:15–09:30）" data-field="edit-pre-open-end-time" />
        <label>盘前让价 %<span class="label-note">（留空跟随全局）</span></label>
        <input v-model="editForm.pre_open_auction_price_bias_pct" type="number" step="0.1" min="0" max="10" placeholder="2（0–10）" data-field="edit-pre-open-bias" />
        <label>组合策略<span class="label-note">（可多选，运行中下次启动生效）</span></label>
        <div class="portfolio-check-list">
          <label v-for="p in portfolios" :key="p.id" class="portfolio-check">
            <input v-model="editForm.portfolio_ids" type="checkbox" :value="p.id" data-edit-portfolio-check />
            {{ p.name }}<span style="color:var(--ink-faint)">（#{{ p.id }}）</span>
            <span v-if="portfolioFormulas(p.id)" class="formula-tag">{{ portfolioFormulas(p.id) }}</span>
          </label>
        </div>
        <label>绑定拼盘<span class="label-note">（阶段 F 影子，可选；运行中立即生效）</span></label>
        <select v-model="editForm.pairing_plan_id" :disabled="pairingPlansLoading" data-field="edit-pairing-plan">
          <option :value="null">不绑定（普通实盘）</option>
          <option v-for="p in pairingPlans" :key="p.id" :value="p.id">
            #{{ p.id }} {{ p.name }}（{{ p.scope }} · {{ p.status }}）
          </option>
        </select>
        <p style="font-size:12px;color:var(--ink-faint);margin:2px 0 8px">
          绑定拼盘会同时清空「理论基准」的单回测绑定（三选一）。只影响对照与影子记录，
          不改下单/数量/审批；阶段 G 前不执行强制借贷。
        </p>
        <p v-if="editError" class="form-error" data-test="session-edit-error">{{ editError }}</p>
        <p v-if="editResult" class="baseline-preview" data-test="session-edit-result">
          {{ editResult.message }}
          <span v-if="editResult.effective_at === 'next_start'">（下次启动自动应用并留痕）</span>
        </p>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" :disabled="editSaving" data-test="session-edit-save" @click="submitEdit">
          {{ editSaving ? '保存中…' : '保存' }}
        </button>
        <button class="btn" @click="editModal = null">关闭</button>
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
        <label>实盘资金基准<span class="label-note">（元）</span></label>
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
            <p class="guide-note">· <b>卖出范围</b>（tq_trade.live_sell_scope）：仅作<b>新建会话的初始档位</b>，
              会话级开关优先且运行中可切（不重启会话、不反向改写本配置）：
              platform_only＝只卖平台自建仓（默认关闭，纯外部仓不代卖并记一条 gate=sell_scope / action=skip）；
              full＝按整仓卖（含存量/外部仓）。开启 full 前会先做全量对账，账不齐拒绝开启；
              已提交未成交委托不撤销。买入侧不受影响。</p>
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
              live_capital 按各账户「总资产」设置（可用资金 + 持仓市值）：起账时按各组合
              initial_capital 占比切分为虚拟起账资金（单组合 = 整额），并同时作真实侧总闸上限
              （桥快照缺失时兜底，不足时按各组合配置资金占比缩量）；继承持仓市值从该组合
              起账现金扣除。</p>
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
              （最多 requote_max_attempts 次）；默认关。requote_price_mode=tick（默认）/adaptive（按盘口追价，
              受 requote_max_slippage_ticks 限制，无行情回退 tick）。依赖通道撤单能力（通达信/同花顺），桥通道不生效。</p>
            <p class="guide-step"><b>大额拆单（仅 TDX）</b>：单笔 ≥ split_order_threshold → 拆 N 子单，成交驱动续发；默认关。
              split_order_count/min_qty/interval_seconds/volume_pct 默认 NULL、interval_jitter 默认 false
              （NULL/false＝按阈值与成交量推导的旧口径）。</p>
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
            <p class="guide-step"><b>人工审批（manual）</b>：信号先入审批队列，人工确认后才下单；支持人工补单/改账，
              受 tq_trade.approval_exempt_sells（默认 true，卖出豁免）、manual_order_max_amount（默认 0 不限）、
              manual_action_min_interval_ms（默认 1000）约束。非 manual 会话的审批端点返回 409。</p>
            <p class="guide-step"><b>影子（shadow）</b>：只按真实行情记账、生成决策与持仓，<b>不触发任何真实委托</b>，
              用于验证策略与执行偏差；必须显式选择，能力矩阵不会自动开启。</p>
            <p class="guide-note">· THS 自动模式已真机验证（下单→成交→撤单全链路）；首次实盘建议小单先行；
              运行期间勿触碰键鼠。</p>
          </div>
        </div>

        <div class="form-group">
          <label class="group-label">七、本轮新增能力与开关（2026-11-18，默认全部关闭）</label>
          <div class="guide-block">
            <p class="guide-step"><b>告警外发（notify）</b>：总开关 notify.enabled 默认 false；飞书
              notify.channel=feishu + webhook_url/secret，企微 notify.wecom.webhook_url，邮件
              notify.email.smtp_host/smtp_port(465)/username/password/from/to[]/use_ssl(true)（密码可用
              TQ_NOTIFY_EMAIL_PASSWORD），本地提示音 notify.sound.enabled(false)/path。公共项 min_level=warn、
              silence_seconds=300、timeout_seconds 夹 1~5 秒。仅 config.yaml 可配，改后<b>即时生效</b>；
              关闭＝不建线程、不发请求，与接入前逐字一致。</p>
            <p class="guide-step"><b>日志轮转 / 升级校验 / 业务心跳 / 备份恢复</b>：logging.daily_rotate 默认
              false、backup_count=14（<b>改后需重启后端</b>，关闭＝单文件追加）；upgrade.enabled 默认 false、
              require_backup=true、max_backup_age_hours=24（preflight 即时，启动校验需重启；关闭＝不校验版本）；
              health.business_probe_enabled 默认 false、business_stall_seconds=180、business_grace_seconds=120
              （即时读取，关闭＝/health 不带 business 字段）；backup.restore_enabled 默认 false
              （即时读取，关闭＝恢复接口/前端入口不可用，只能手工替换库）。</p>
            <p class="guide-step"><b>行情降级 / 盘后交叉校验 / 回测 K 线缓存</b>：live.quote_fallback.enabled
              默认 false、chain=[tq,iquant,goldminer]（<b>需重启会话</b>，关闭＝只用主行情源）；
              live.cross_check.enabled 默认 false、sample_size=5、max_dev_pct=1.0（次日生效，关闭＝不校验）；
              backtest.kline_cache.enabled 默认 false、dir=data/kline_cache、today_ttl_seconds=300
              （每次回测即时读取，关闭＝每次直连数据源）。</p>
            <p class="guide-step"><b>原生分钟数据源（可选，默认关）</b>：live.native_minute.enabled
              默认 false、stall_timeout_seconds=180（<b>需重启会话</b>）。开启＝盘中 1m 读本地原生
              .lc1、5m 读 .lc5、15m/30m/1h 走原生周期（与回测同源），只取标签 ≤ 墙钟的已闭合 bar，
              判新只看 bar 标签前进；依赖外部「TDX数据工具.exe」持续写入 D:\zd_zyb 且清单已更新，
              超过 stall_timeout_seconds 不前进 → 自动回落快照合成并留痕（WARNING + risk 事件），
              恢复前进自动切回。关闭＝行为与接入前一致（快照合成）。</p>
            <p class="guide-step"><b>会话级默认（重启会话生效）</b>：execution_mode 新建默认 alert（历史会话
              auto），shadow 仅影子记账不下单，manual 才启用审批队列；gate_recheck_on_execute=0（关闭＝按信号日
              决策执行）；account_risk_enabled=0、account_max_drawdown/account_daily_loss_limit=NULL（回退
              tq_trade 全局 0.15/0.05）；failover_enabled=0、backup_engine=NULL（关闭＝只走主通道）；
              capital_pool_mode=off、capital_pool_rebalance_enabled=0（关闭＝分账不分池、不日再平衡）；
              pre_open_auction_enabled/end_time/price_bias_pct=NULL（NULL＝跟随全局 true/09:20/2.0）。</p>
            <p class="guide-step"><b>组合/策略级（重启会话生效）</b>：portfolio priority=0（0＝默认顺序）；
              market_gate_indices=NULL、vote_mode=any、strength_threshold=0、bear_reduce_ratio=0（默认＝单指数
              任一空头即拦、无强度门槛、不减仓）；breaker_action=halt（只停开仓）；requote_price_mode=tick
              （adaptive 才按盘口追价）；split_order_count/min_qty/interval_seconds/volume_pct=NULL、
              interval_jitter=false（NULL/false＝按阈值推导或旧口径）。</p>
            <p class="guide-step"><b>回测档位</b>：dividend_mode 默认 front，exact＝不复权 + 显式公司行为（仅作
              对账基准）；execution_profile 默认 legacy，realistic 才启用涨跌停/成交量/自适应滑点/日终抬峰。
              二者按每次回测显式选择，无需重启。</p>
            <p class="guide-note">· <b>通道能力矩阵</b>：GET /api/live/channel 的 engines[].capabilities /
              capability_limits 为静态声明（下单/撤单/行情/限价/备注认领/拆单/盘前限价），前端据此置灰不支持的
              配置；无需配置，也不改变既有行为。影子模式必须显式选择，不会因能力矩阵自动开启。</p>
          </div>
        </div>

        <div class="form-group">
          <label class="group-label">八、风险提示</label>
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
/* 理论 vs 实盘：面板内直接绑定回测记录，避免用户去找隐蔽入口 */
.tvl-bind {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin: 0 0 8px;
  font-size: 13px;
}
.tvl-bind label { color: var(--ink-soft); }
.tvl-bind select { max-width: 520px; }
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
/* 分钟级实盘口径提示：与回测不等价（只提示，不改装配） */
.minute-live-banner {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
  padding: 10px 14px;
  margin-bottom: 12px;
  font-size: 12px;
  line-height: 1.6;
  border-left: 4px solid var(--amber, #d48806);
  background: rgba(212, 136, 6, 0.08);
}
.minute-live-banner strong { display: block; margin-bottom: 4px; }
.minute-live-body { color: var(--ink-soft); }
/* 盘中数据轮询常驻提示：默认中性；原生分钟源停滞时升级为红色告警 */
.minute-source-banner {
  padding: 10px 14px;
  margin-bottom: 12px;
  font-size: 12px;
  line-height: 1.6;
  border-left: 4px solid var(--line, #555);
  background: rgba(140, 140, 140, 0.08);
}
.minute-source-banner strong { display: block; margin-bottom: 4px; }
.minute-source-body { color: var(--ink-soft); }
.minute-source-banner code {
  padding: 0 3px;
  border-radius: 3px;
  background: rgba(255, 255, 255, 0.08);
}
.minute-source-alert {
  border-left-color: var(--red, #d93026);
  background: rgba(217, 48, 38, 0.14);
}
.minute-source-alert strong { color: var(--red, #d93026); }
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
.muted { font-size: 12px; color: var(--ink-faint); }
/* B3 通道守护检查清单 */
.guard-check-list { list-style: none; padding: 0; margin: 0 0 12px; }
.guard-check-list li {
  display: flex; justify-content: space-between; gap: 12px;
  font-size: 12px; padding: 4px 0; border-bottom: 1px dashed var(--line, #2a2f3a);
  color: var(--red);
}
.guard-check-list li.ok { color: var(--ink-soft); }
.guard-check-list li span { color: var(--ink-faint); text-align: right; }
/* B4 通道能力矩阵 */
.capability-row { display: flex; flex-wrap: wrap; gap: 6px; margin: 4px 0 10px; }
.capability-chip {
  font-size: 11px; padding: 2px 8px; border-radius: 10px;
  border: 1px solid var(--line, #2a2f3a); color: var(--ink-soft);
}
.capability-chip.off { color: var(--ink-faint); opacity: .65; text-decoration: line-through; }
.capability-hint { font-size: 12px; color: var(--ink-faint); margin: 2px 0 10px; }
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
  position: relative;
  margin-top: 6px;
  max-width: 320px;
  padding-right: 18px;
  font-size: 12px;
  line-height: 1.5;
  color: var(--orange, #e67e22);
  white-space: normal;
}
/* 提示可手动收起（TTL 到期也会自动收起，原文已留档事件日志） */
.start-warning-close {
  position: absolute;
  top: -2px;
  right: -4px;
  padding: 2px 4px;
  border: none;
  background: transparent;
  color: var(--ink-faint);
  font-size: 14px;
  line-height: 1;
  cursor: pointer;
}
.start-warning-close:hover { color: var(--ink); }
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

/* ===== 实盘会话列表：10 项信息收敛为 6 列 =====
   原表把「执行模式 / 引擎 / 模式」各占一列、状态徽标与 5 个按钮全挤在单行，
   页面宽度有限时单元格内文字/按钮互相粘连。此处只改展现层：
   1) 会话身份（ID + 名称）合并；
   2) 组合策略与执行模式/引擎/模式合为一列，徽标上下分组；
   3) 资金基准值与「重置」按钮改为上下排布，不再连成一串数字；
   4) 状态主徽标与次要徽标分行，行内用 gap 分隔；
   5) 操作分主操作（启动/停止/监控）与次级入口两行。
   所有按钮、可见性条件、接口调用与权限判定均保持原样。 */
.sessions-table { min-width: 1080px; }
.sessions-table thead th { white-space: normal; line-height: 1.3; }
.sessions-table thead th.col-id { width: 128px; }
.sessions-table thead th.col-strategy { width: 250px; }
.sessions-table thead th.col-baseline { width: 132px; }
.sessions-table thead th.col-status { width: 210px; }
.sessions-table thead th.col-scope { width: 132px; }
.sessions-table thead th.col-actions { width: 236px; }
.sessions-table tbody td { vertical-align: top; padding: 10px 12px; }
.sessions-table .th-sub {
  display: block;
  margin-top: 2px;
  font-size: 10px;
  font-weight: 500;
  letter-spacing: .04em;
  text-transform: none;
  color: var(--ink-faint);
}
.sess-id {
  font-size: 12px;
  color: var(--ink-soft);
  font-variant-numeric: tabular-nums;
}
.sess-name {
  font-weight: 600;
  color: var(--ink);
  line-height: 1.35;
  word-break: break-word;
}
.strat-chips { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
.strat-chips .portfolio-chip {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  margin-right: 0;
  font-size: 13px;
  color: var(--ink-soft);
}
.sess-meta {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 6px;
  margin-top: 7px;
}
.meta-mode { font-size: 12px; color: var(--ink-soft); }
.baseline-cell { display: flex; flex-direction: column; align-items: flex-start; gap: 5px; }
.baseline-value { font-size: 14px; font-weight: 600; white-space: nowrap; }
.baseline-value.is-default { font-weight: 500; color: var(--ink-soft); }
.baseline-reset { color: var(--ink-soft); }
.status-cell { display: flex; flex-direction: column; align-items: flex-start; gap: 6px; }
.status-tags { display: flex; flex-wrap: wrap; align-items: center; gap: 5px; }
.scope-cell { display: flex; flex-direction: column; align-items: flex-start; gap: 6px; }
.op-cell { display: flex; flex-direction: column; align-items: flex-start; gap: 7px; }
.op-primary, .op-secondary { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; }
/* 次级入口弱化边框与文字，仍保持可点；删除保留危险色 */
.op-secondary .btn:not(.btn-danger) {
  border-color: var(--line);
  color: var(--ink-soft);
}
.op-secondary .btn:not(.btn-danger):hover {
  background: var(--ink);
  color: var(--bg);
  border-color: var(--ink);
}
/* 窄屏（≤760px）：表头隐藏，每行改为「标签 + 内容」的卡片式堆叠，免横向挤压 */
@media (max-width: 760px) {
  .sessions-table { min-width: 0; }
  .sessions-table thead { display: none; }
  .sessions-table, .sessions-table tbody, .sessions-table tr, .sessions-table td { display: block; width: auto; }
  .sessions-table tbody tr {
    padding: 10px 12px;
    border-bottom: 1px solid var(--line);
  }
  .sessions-table tbody tr:last-child { border-bottom: none; }
  .sessions-table tbody tr:hover { background: var(--hover-bg); }
  .sessions-table tbody tr:hover td { background: transparent; }
  .sessions-table tbody td {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 4px 0;
    border-bottom: none;
  }
  .sessions-table tbody td::before {
    content: attr(data-label);
    flex: 0 0 84px;
    padding-top: 3px;
    font-size: 11px;
    letter-spacing: .06em;
    color: var(--ink-faint);
  }
  .sess-id { font-size: 11px; }
}
</style>
