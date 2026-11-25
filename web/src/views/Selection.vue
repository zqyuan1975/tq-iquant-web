<script setup lang="ts">
import { computed, onMounted, onUnmounted, reactive, ref } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import {
  activateSelectionVersion,
  bindSelectionSession,
  computeSelectionRecall,
  createSelectionRuns,
  createSelectionScheme,
  createSelectionVersion,
  deliverSelection,
  freezeSelectionSnapshot,
  getSelectionDataStatus,
  getSelectionBuildStatus,
  getSelectionDefaults,
  getSelectionFactorValidation,
  getSelectionFormulas,
  getSelectionGpAudit,
  getSelectionMeta,
  getSelectionPools,
  getSelectionRun,
  getSelectionRuns,
  getSelectionScheme,
  getSelectionSchemes,
  getSelectionSessionBinding,
  getSelectionSnapshot,
  getSelectionSnapshots,
  getSelectionWideDbStatus,
  importSelectionWarehouse,
  publishSelectionSnapshot,
  resolveSelectionPool,
  seedSelectionSchemes,
  startSelectionBuild,
  unbindSelectionSession,
  type SelectionBindResult,
  type SelectionBuildStatus,
  type SelectionDataStatus,
  type SelectionDeliverResult,
  type SelectionFactorValidation,
  type SelectionFieldSpec,
  type SelectionFormulaItem,
  type SelectionImportResult,
  type SelectionMeta,
  type SelectionPoolItem,
  type SelectionPoolRecall,
  type SelectionPoolResolve,
  type SelectionQuality,
  type SelectionRunDetail,
  type SelectionRunItem,
  type SelectionRunResult,
  type SelectionSchemeItem,
  type SelectionSessionBinding,
  type SelectionSnapshotDetail,
  type SelectionSnapshotItem,
  type SelectionSpec,
  type SelectionStockRow,
  type SelectionVersionItem,
  type WideDbStatus,
} from '../api'
import {
  POOL_GROUPS,
  groupPoolItems,
  optgroupLabel,
  poolGroupKey,
  poolKeyFromCode,
  poolLabel,
} from '../utils/selectionPoolGroups'

type TabKey = 'schemes' | 'filters' | 'results' | 'snapshots' | 'data' | 'guide'

const TABS: { key: TabKey; label: string }[] = [
  { key: 'schemes', label: '选股方案' },
  { key: 'filters', label: '筛选条件' },
  { key: 'results', label: '选股结果' },
  { key: 'snapshots', label: '快照与历史' },
  { key: 'data', label: '数据体检' },
  { key: 'guide', label: '使用说明' },
]

const TRACK_LABEL: Record<string, string> = {
  both: '双轨（前瞻+历史）', forward: '仅前瞻', historical: '仅历史',
  cash: '现金分红', quality: '质量', value: '价值',
}

//: 与后端 DEFAULT_GATES 一一对应；只做展示与编辑，后端仍会二次校验。
const GATES: { key: string; label: string; step: number }[] = [
  { key: 'min_float_mv', label: '流通市值下限（亿元）', step: 1 },
  { key: 'max_pe', label: 'PE 上限', step: 10 },
  { key: 'min_div_streak', label: '连续分红年数下限', step: 1 },
  { key: 'max_payout', label: '分红率上限（%）', step: 5 },
  { key: 'min_fcf_cover', label: '自由现金流覆盖下限', step: 0.1 },
  { key: 'max_roic_trend_drop', label: 'ROIC 趋势否决下限', step: 1 },
]

const QUALITY_LABEL: Record<string, string> = {
  field_set_version: '字段集版本',
  field_fingerprint: '字段指纹',
  warehouse_as_of: '导入仓快照日',
  warehouse_imported_at: '导入时间',
  is_financial_source: '金融股识别',
  valuation_source: '估值口径',
  kline_source: 'K 线口径',
  missing_policy: '缺失处理',
  survivorship: '幸存者偏差',
  uncalibrated: '权重校准',
}

const tab = ref<TabKey>('schemes')
const loading = ref(true)
const errorMsg = ref('')
const infoMsg = ref('')

const meta = ref<SelectionMeta | null>(null)
const schemes = ref<SelectionSchemeItem[]>([])
const defaults = ref<Record<string, SelectionSpec>>({})
const wideDb = ref<WideDbStatus | null>(null)
const buildStatus = ref<SelectionBuildStatus | null>(null)
const buildStarting = ref(false)
const buildMode = ref<'incremental' | 'full'>('incremental')
let buildTimer: number | undefined

const BUILD_PHASE_LABEL: Record<string, string> = {
  universe: '名单', profile: '估值/市值', fin: '财报', dividend: '分红',
  sector: '行业归属', daily: '日线回补', publish: '发布 as_of',
}

const selected = ref<{
  id: number; code: string; name: string; pool_key: string
  versions: SelectionVersionItem[]
} | null>(null)
const newScheme = reactive({ code: '', name: '', pool_key: 'value', description: '' })
const filterPool = ref('')

// 版本编辑器（保存时总是新建版本，绝不改写历史版本）
const editor = reactive({
  track: 'both' as 'both' | 'forward' | 'historical',
  top_n: 100,
  weights: {} as Record<string, number>,
  weights_hist: {} as Record<string, number>,
  gates: {} as Record<string, number>,
  pe_mode: 'strict' as 'strict' | 'loss_discount',
  note: '',
})

const PE_MODE_LABEL: Record<string, string> = {
  strict: '严格（PE≤0 一律挡在候选外）',
  loss_discount: '亏损股降权（PE≤0 进候选，按亏损幅度打折）',
}
const addFieldKey = ref('')
const saving = ref(false)
const seeding = ref(false)

// 运行与结果
const running = ref(false)
const runs = ref<SelectionRunItem[]>([])
const lastRunResult = ref<SelectionRunResult | null>(null)
const runDetail = ref<SelectionRunDetail | null>(null)
const stockFilter = ref<'selected' | 'all'>('selected')
const expandedStock = ref<string>('')

// 快照
const snapshots = ref<SelectionSnapshotItem[]>([])
const snapshotDetail = ref<SelectionSnapshotDetail | null>(null)
const publishForm = reactive({ code: '', name: '' })
const publishResult = ref<{
  pool_id: number; pool_code: string; pool_name: string; stock_count: number; kind?: string
} | null>(null)

// 数据体检
const dataStatus = ref<SelectionDataStatus | null>(null)
const importing = ref(false)
const importResult = ref<SelectionImportResult | null>(null)
const gpAuditDay = ref(new Date().toISOString().slice(0, 10))
const gpAudit = ref<Record<string, unknown> | null>(null)

// 交付 / 池接入
const delivering = ref(false)
const deliverResult = ref<SelectionDeliverResult | null>(null)
const pools = ref<SelectionPoolItem[]>([])
const resolvePoolId = ref<number | null>(null)
const resolveDay = ref('')
const poolResolve = ref<SelectionPoolResolve | null>(null)
const sessionId = ref<number | null>(null)
const sessionBinding = ref<SelectionSessionBinding | null>(null)
const bindResult = ref<SelectionBindResult | null>(null)
const factorValidation = ref<SelectionFactorValidation | null>(null)

// 池子有效性（召回）——主口径；IC 仅参考
const formulas = ref<SelectionFormulaItem[]>([])
const recallSnapshotId = ref<number | null>(null)
const recallFormulaId = ref<number | null>(null)
const recallHorizon = ref(20)
// 入选判定窗口（交易 bar）：择时 OPEN 脉冲稀疏，严格单日口径常为 0/0
const recallWindow = ref(20)
const recallResult = ref<SelectionPoolRecall | null>(null)
const computingRecall = ref(false)
const missedExpanded = ref(false)
const frozenSnapshots = computed(() => snapshots.value.filter((s) => s.frozen))

const scorable = computed(() => new Set(meta.value?.scorable_fields || []))
const fields = computed<SelectionFieldSpec[]>(() => meta.value?.fields || [])

const filteredFields = computed<SelectionFieldSpec[]>(() => {
  if (!filterPool.value) return fields.value
  return fields.value.filter((f) => f.pools.includes(filterPool.value))
})

const activeVersion = computed<SelectionVersionItem | null>(() => {
  if (!selected.value) return null
  return selected.value.versions.find((v) => v.status === 'active') || null
})

const industryPools = computed(() => meta.value?.industry_pools || [])
const industryPoolMap = computed(
  () => new Map(industryPools.value.map((p) => [p.key, p])))
const poolOptions = computed(() => [
  ...(meta.value?.pools || []),
  ...industryPools.value.map((p) => p.key),
])
const industryMembershipText = computed(() => {
  const counts = meta.value?.industry_membership || {}
  const keys = Object.keys(counts)
  if (!keys.length) return ''
  const order = ['周期', '消费', '科技', '医药', '高端制造', '金融地产',
    '公用事业', '农业', '其他综合']
  return order.filter((k) => k in counts)
    .map((k) => `${k} ${counts[k]}`).join(' · ')
})

function isGroupOnlyPool(key: string | null | undefined): boolean {
  if (!key) return false
  const def = industryPoolMap.value.get(key)
  return Boolean(def?.group_only || def?.kind === 'l2')
}

/**
 * 方案列表分组：因子风格 → 行业·一级 → 行业·二级标签。
 * 组内保持后端返回顺序，不跨组混排；折叠状态落在 localStorage，刷新/切换
 * Tab/操作后仍保持，默认全部展开。
 */
const SCHEME_GROUP_STORAGE = 'selection:scheme-groups-collapsed'
const collapsedGroups = ref<Record<string, boolean>>(loadCollapsedGroups())

function loadCollapsedGroups(): Record<string, boolean> {
  try {
    const raw = localStorage.getItem(SCHEME_GROUP_STORAGE)
    return raw ? JSON.parse(raw) : {}
  } catch {
    return {}
  }
}

function toggleSchemeGroup(key: string) {
  const next = { ...collapsedGroups.value, [key]: !collapsedGroups.value[key] }
  if (!next[key]) delete next[key]
  collapsedGroups.value = next
  try {
    localStorage.setItem(SCHEME_GROUP_STORAGE, JSON.stringify(next))
  } catch {
    /* 隐私模式等存储不可用时只影响持久化，不影响本次折叠 */
  }
}

/**
 * 列表分组：列表非空时三组恒定输出（空组 items=[]，便于表头/下拉结构稳定），
 * 列表为空时交给各表自己的空态提示，不再渲染三行 0 条组头。
 */
function pooledGroups<T>(items: T[], keyOf: (item: T) => string | null | undefined) {
  return items.length ? groupPoolItems(items, keyOf, true, true) : []
}

const schemeGroups = computed(() =>
  pooledGroups(schemes.value, (s) => s.pool_key))

/** 运行列表 / 快照列表 / 池列表共用同一套分组定义。 */
const runGroups = computed(() => pooledGroups(runs.value, (r) => r.pool_key))
const snapshotGroups = computed(() =>
  pooledGroups(snapshots.value, (s) => s.pool_key))
const poolGroups = computed(() =>
  pooledGroups(pools.value, (p) => poolKeyFromCode(p.code)))
const frozenSnapshotGroups = computed(() =>
  pooledGroups(frozenSnapshots.value, (s) => s.pool_key))

/** 所有池下拉统一用分组选项：风格 / 行业一级 / 行业二级标签（注明可重叠）。 */
const poolOptionGroups = computed(() => {
  const known = new Set(poolOptions.value)
  return POOL_GROUPS
    .map((def) => ({
      def,
      keys: poolOptions.value.filter((key) => known.has(key) && poolGroupKey(key) === def.key),
    }))
    .filter((g) => g.keys.length > 0)
})

function isCollapsed(key: string): boolean {
  return Boolean(collapsedGroups.value[key])
}

const weightSum = computed(() =>
  Object.values(editor.weights).reduce((a, b) => a + (Number(b) || 0), 0))
const histSum = computed(() =>
  Object.values(editor.weights_hist).reduce((a, b) => a + (Number(b) || 0), 0))
const sumOk = computed(() => {
  if (isGroupOnlyPool(selected.value?.pool_key)) return true
  if (Math.abs(weightSum.value - 1) >= 1e-6) return false
  const dualTrack = (selected.value?.pool_key === 'growth'
    || selected.value?.pool_key === 'industry:tech') && editor.track === 'both'
  return dualTrack ? Math.abs(histSum.value - 1) < 1e-6 : true
})

const weightKeys = computed(() => Object.keys(editor.weights))
const histKeys = computed(() => Object.keys(editor.weights_hist))

/** 可选字段：已接入打分 + 属于当前池（行业池不限）+ 尚未使用的权重字段。 */
const addableFields = computed(() => {
  if (!selected.value) return []
  const pool = selected.value.pool_key
  const industry = pool.startsWith('industry:')
  const used = new Set([...weightKeys.value, ...histKeys.value])
  return fields.value
    .filter((f) => scorable.value.has(f.key)
      && (industry || f.pools.includes(pool)) && !used.has(f.key))
    .map((f) => f.key)
})

const visibleStocks = computed<SelectionStockRow[]>(() => {
  const rows = runDetail.value?.stocks || []
  return stockFilter.value === 'selected' ? rows.filter((r) => r.selected) : rows
})

const runValidation = computed(() => runDetail.value?.quality?.factor_validation || null)
const validationRows = computed(() =>
  Object.entries(factorValidation.value?.factors || {}))

function errMsg(e: any): string {
  const d = e?.response?.data
  if (d?.message) return d.message
  if (Array.isArray(d?.detail)) return d.detail.map((x: any) => `${(x.loc || []).join('.')}: ${x.msg}`).join('; ')
  if (typeof d?.detail === 'string') return d.detail
  return e?.message || '请求失败'
}

function trackLabel(key: string | null): string {
  if (!key) return '—'
  return TRACK_LABEL[key] || key
}

function statusClass(status: string): string {
  if (status === 'succeeded') return 'badge badge-green'
  if (status === 'failed') return 'badge badge-red'
  return 'badge badge-gray'
}

function fmtNum(v: number | null | undefined, digits = 4): string {
  if (v === null || v === undefined || Number.isNaN(v)) return '—'
  return Number(v).toFixed(digits)
}

function addField() {
  if (!addFieldKey.value) return
  const target = selected.value?.pool_key === 'growth' && activeTarget.value === 'hist'
    ? editor.weights_hist : editor.weights
  target[addFieldKey.value] = 0
  addFieldKey.value = ''
}

// 添加字段时选择加到哪一轨（成长双轨时才有意义）
const activeTarget = ref<'forward' | 'hist'>('forward')

function removeField(key: string) {
  delete editor.weights[key]
}

function removeHistField(key: string) {
  delete editor.weights_hist[key]
}

function fillGates(gates?: Record<string, number>) {
  editor.gates = {}
  for (const g of GATES) editor.gates[g.key] = Number(gates?.[g.key] ?? 0)
}

function buildEditor(spec: SelectionSpec | undefined) {
  const fallback = selected.value ? defaults.value[selected.value.pool_key] : undefined
  const src = spec || fallback
  editor.track = (src?.track as any) || 'both'
  editor.top_n = src?.top_n ?? 100
  editor.weights = { ...(src?.weights || {}) }
  editor.weights_hist = { ...(src?.weights_hist || {}) }
  fillGates(src?.gates)
  editor.pe_mode = (src?.pe_mode as 'strict' | 'loss_discount') || 'strict'
  editor.note = ''
  addFieldKey.value = ''
  activeTarget.value = 'forward'
}

async function loadAll() {
  loading.value = true
  errorMsg.value = ''
  const failed: string[] = []
  // 局部数据失败不阻塞整页，但必须把失败项汇总出来，避免静默空页面
  const part = <T>(label: string, p: Promise<T>, fallback: T): Promise<T> =>
    p.catch(() => { failed.push(label); return fallback })
  // 兜底超时：接口挂住时也要退出 loading，不能一直转圈
  const withTimeout = <T>(p: Promise<T>, ms = 15000): Promise<T> => Promise.race([
    p,
    new Promise<T>((_, reject) => setTimeout(() => reject(new Error(`请求超时（${ms / 1000} 秒）`)), ms)),
  ])
  try {
    const [metaRes, schemeRes, wideRes, defRes, dataRes, runRes, snapRes, poolRes, valRes, formulaRes] = await Promise.all([
      withTimeout(getSelectionMeta()),
      withTimeout(getSelectionSchemes(true)),
      part<WideDbStatus | null>('宽库状态', withTimeout(getSelectionWideDbStatus()), null),
      part<Record<string, SelectionSpec>>('默认参数', withTimeout(getSelectionDefaults()), {}),
      part<SelectionDataStatus | null>('数据体检', withTimeout(getSelectionDataStatus()), null),
      part<SelectionRunItem[]>('运行记录', withTimeout(getSelectionRuns(50)), []),
      part<SelectionSnapshotItem[]>('快照', withTimeout(getSelectionSnapshots(50)), []),
      part<SelectionPoolItem[]>('选股池', withTimeout(getSelectionPools()), []),
      part<SelectionFactorValidation | null>('因子回验', withTimeout(getSelectionFactorValidation()), null),
      part<SelectionFormulaItem[]>('公式', withTimeout(getSelectionFormulas()), []),
    ])
    meta.value = metaRes
    schemes.value = schemeRes
    wideDb.value = wideRes
    defaults.value = defRes
    dataStatus.value = dataRes
    runs.value = runRes
    snapshots.value = snapRes
    pools.value = poolRes
    factorValidation.value = valRes
    formulas.value = formulaRes
    if (!recallFormulaId.value && formulaRes.length) {
      recallFormulaId.value = formulaRes[0].formula_id
    }
    if (!resolvePoolId.value && poolRes.length) {
      resolvePoolId.value = poolRes[0].pool_id
    }
    if (failed.length) {
      errorMsg.value = `以下数据加载失败：${failed.join('、')}。常见原因是后端进程未加载最新路由（需重启后端）或接口异常；其余内容可正常查看，修复后请点「重试」。`
    }
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  } finally {
    loading.value = false
  }
}

async function refreshSchemes() {
  schemes.value = await getSelectionSchemes(true)
  if (selected.value) {
    const detail = await getSelectionScheme(selected.value.id)
    selected.value = {
      id: detail.id, code: detail.code, name: detail.name,
      pool_key: detail.pool_key, versions: detail.versions,
    }
  }
}

async function openScheme(item: SelectionSchemeItem | { id: number }) {
  errorMsg.value = ''
  try {
    const detail = await getSelectionScheme(item.id)
    selected.value = {
      id: detail.id, code: detail.code, name: detail.name,
      pool_key: detail.pool_key, versions: detail.versions,
    }
    const active = detail.versions.find((v) => v.status === 'active')
    buildEditor((active?.spec as SelectionSpec) || undefined)
    infoMsg.value = active
      ? `已载入 v${active.version}（当前启用）参数，修改后保存为新版本`
      : '该方案还没有启用版本，已载入内置默认参数'
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function submitScheme() {
  errorMsg.value = ''
  try {
    await createSelectionScheme({ ...newScheme })
    newScheme.code = ''
    newScheme.name = ''
    newScheme.description = ''
    await refreshSchemes()
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function seedDefaults() {
  errorMsg.value = ''
  seeding.value = true
  try {
    const res = await seedSelectionSchemes()
    await refreshSchemes()
    infoMsg.value = `已初始化 ${res.created} 个默认方案（四风格池），每个方案带 v1 未校准参数`
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  } finally {
    seeding.value = false
  }
}

async function saveVersion() {
  if (!selected.value) return
  if (!sumOk.value) {
    errorMsg.value = '权重必须非负且和为 1（成长双轨时两条轨分别求和）'
    return
  }
  errorMsg.value = ''
  saving.value = true
  const spec: SelectionSpec = {
    pool_key: selected.value.pool_key,
    top_n: editor.top_n,
    weights: { ...editor.weights },
    gates: { ...editor.gates },
    pe_mode: editor.pe_mode,
  }
  if (selected.value.pool_key === 'growth') {
    spec.track = editor.track
    if (editor.track === 'both') spec.weights_hist = { ...editor.weights_hist }
  }
  try {
    const version = await createSelectionVersion(selected.value.id, {
      spec, note: editor.note,
    })
    await refreshSchemes()
    buildEditor(spec)
    infoMsg.value = `已保存 v${version.version}（草稿）。历史版本不可改写；如需生效请点「启用」。`
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  } finally {
    saving.value = false
  }
}

async function activate(version: SelectionVersionItem) {
  if (!selected.value) return
  errorMsg.value = ''
  try {
    await activateSelectionVersion(selected.value.id, version.id)
    await refreshSchemes()
    infoMsg.value = `v${version.version} 已启用；同方案其他版本自动归档（激活互斥）`
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function runAll() {
  errorMsg.value = ''
  infoMsg.value = ''
  running.value = true
  try {
    const res = await createSelectionRuns({})
    lastRunResult.value = res
    runs.value = await getSelectionRuns(50)
    snapshots.value = await getSelectionSnapshots(50)
    const ok = res.runs.filter((r) => r.status === 'succeeded')
    infoMsg.value = ok.length
      ? `已按 ${res.as_of} 运行 ${ok.length} 个方案：` + ok.map((r) => `${poolLabel(r.pool_key)} ${r.stock_count} 只`).join('，')
      : '没有方案产出结果，请检查启用版本与数据状态'
    if (ok.length) await openRun(ok[0])
  } catch (e: any) {
    errorMsg.value = `${errMsg(e)}（可点上方「重试」，或按空态指引先构建宽库/初始化方案）`
    dataStatus.value = await getSelectionDataStatus().catch(() => dataStatus.value)
  } finally {
    running.value = false
  }
}

async function openRun(run: SelectionRunItem) {
  errorMsg.value = ''
  stockFilter.value = 'selected'
  expandedStock.value = ''
  try {
    runDetail.value = await getSelectionRun(run.id, {
      includeStocks: true, selectedOnly: false, limit: 600,
    })
    if (runDetail.value.snapshot_id) {
      recallSnapshotId.value = runDetail.value.snapshot_id
    }
    recallResult.value = null
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function loadPools() {
  pools.value = await getSelectionPools()
}

async function deliverAll() {
  errorMsg.value = ''
  infoMsg.value = ''
  delivering.value = true
  try {
    const res = await deliverSelection()
    deliverResult.value = res
    runs.value = await getSelectionRuns(50)
    snapshots.value = await getSelectionSnapshots(50)
    pools.value = await getSelectionPools()
    infoMsg.value = res.delivered.length
      ? `收盘交付完成：${res.delivered.length} 个池已冻结并发布版本池 + 序列池`
      : '没有可交付的方案，请检查启用版本与数据状态'
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  } finally {
    delivering.value = false
  }
}

async function doResolvePool() {
  if (!resolvePoolId.value) return
  errorMsg.value = ''
  try {
    poolResolve.value = await resolveSelectionPool(
      resolvePoolId.value, resolveDay.value || undefined)
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function loadBinding() {
  if (!sessionId.value) {
    errorMsg.value = '先填写实盘会话 id'
    return
  }
  errorMsg.value = ''
  try {
    sessionBinding.value = await getSelectionSessionBinding(sessionId.value)
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function doBind() {
  if (!sessionId.value || !resolvePoolId.value) {
    errorMsg.value = '先填写会话 id 并选择版本池'
    return
  }
  errorMsg.value = ''
  try {
    const pool = pools.value.find((p) => p.pool_id === resolvePoolId.value)
    if (pool && pool.kind !== 'version') {
      errorMsg.value = '实盘只能挂版本池（SEL_风格_YYYYMMDD），序列池会随新快照漂移'
      return
    }
    bindResult.value = await bindSelectionSession(sessionId.value, {
      pool_id: resolvePoolId.value,
    })
    await loadBinding()
    infoMsg.value = `会话已挂到 ${bindResult.value.pool_code}（${bindResult.value.stock_count} 只），可用「回退」恢复`
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function doUnbind() {
  if (!sessionId.value || !bindResult.value) return
  errorMsg.value = ''
  try {
    await unbindSelectionSession(sessionId.value, bindResult.value.portfolios)
    bindResult.value = null
    await loadBinding()
    infoMsg.value = '已按绑定前快照回退组合股票池'
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function openSnapshot(item: SelectionSnapshotItem) {
  errorMsg.value = ''
  publishResult.value = null
  publishForm.code = item.published_pool_code || ''
  publishForm.name = item.published_pool_name || ''
  try {
    snapshotDetail.value = await getSelectionSnapshot(item.id, {
      includeStocks: true, selectedOnly: true, limit: 600,
    })
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function doFreeze(item: SelectionSnapshotItem) {
  errorMsg.value = ''
  try {
    await freezeSelectionSnapshot(item.id)
    snapshots.value = await getSelectionSnapshots(50)
    if (snapshotDetail.value?.id === item.id) {
      snapshotDetail.value = await getSelectionSnapshot(item.id, {
        includeStocks: true, selectedOnly: true, limit: 600,
      })
    }
    infoMsg.value = `快照 #${item.id} 已冻结，内容不可再变`
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function doPublish(item: SelectionSnapshotItem) {
  errorMsg.value = ''
  publishResult.value = null
  try {
    const res = await publishSelectionSnapshot(item.id, {
      code: publishForm.code || undefined,
      name: publishForm.name || undefined,
    })
    publishResult.value = res
    snapshots.value = await getSelectionSnapshots(50)
    infoMsg.value = `已发布到股票池 ${res.pool_code}（${res.stock_count} 只，含 PIT 成员区间）`
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function doImport() {
  errorMsg.value = ''
  infoMsg.value = ''
  importResult.value = null
  importing.value = true
  try {
    const res = await importSelectionWarehouse()
    importResult.value = res
    dataStatus.value = await getSelectionDataStatus()
    wideDb.value = await getSelectionWideDbStatus().catch(() => wideDb.value)
    infoMsg.value = `导入完成：快照日 ${res.as_of}，universe ${res.counts.universe ?? '—'} 只`
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  } finally {
    importing.value = false
  }
}

async function loadBuildStatus() {
  try {
    buildStatus.value = await getSelectionBuildStatus()
  } catch {
    return
  }
  if (buildStatus.value?.running) startBuildPolling()
}

function startBuildPolling() {
  if (buildTimer !== undefined) return
  buildTimer = window.setInterval(async () => {
    try {
      buildStatus.value = await getSelectionBuildStatus()
      if (!buildStatus.value?.running && buildTimer !== undefined) {
        window.clearInterval(buildTimer)
        buildTimer = undefined
        dataStatus.value = await getSelectionDataStatus().catch(() => dataStatus.value)
      }
    } catch {
      /* 轮询失败保留上一次进度，下轮再试 */
    }
  }, 5000)
}

async function doBuild() {
  if (buildMode.value === 'full'
    && !window.confirm('full 是灾备模式，会重取全量并可能耗时数小时。确认执行？')) {
    return
  }
  errorMsg.value = ''
  infoMsg.value = ''
  buildStarting.value = true
  try {
    const res = await startSelectionBuild({ mode: buildMode.value })
    if (res.status === 'busy') {
      infoMsg.value = `已有构建在运行（${res.run_id}），下方进度会继续刷新`
    } else {
      infoMsg.value = `已启动内置${buildMode.value === 'full' ? '全量' : '增量'}构建（${res.run_id}）`
    }
    await loadBuildStatus()
    startBuildPolling()
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  } finally {
    buildStarting.value = false
  }
}

async function runGpAudit() {
  errorMsg.value = ''
  gpAudit.value = null
  try {
    gpAudit.value = await getSelectionGpAudit(gpAuditDay.value)
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

function stockFactorEntries(row: SelectionStockRow): [string, number][] {
  return Object.entries(row.factors || {}) as [string, number][]
}

function qualityEntries(q?: SelectionQuality): [string, string][] {
  if (!q) return []
  const out: [string, string][] = []
  for (const key of Object.keys(QUALITY_LABEL)) {
    const val = (q as Record<string, unknown>)[key]
    if (val === undefined || val === null || val === '') continue
    out.push([QUALITY_LABEL[key], String(val)])
  }
  return out
}

async function computeRecall() {
  if (!recallSnapshotId.value || !recallFormulaId.value) {
    infoMsg.value = '请先选择已冻结快照与公式'
    return
  }
  infoMsg.value = ''
  computingRecall.value = true
  try {
    recallResult.value = await computeSelectionRecall({
      snapshot_id: recallSnapshotId.value,
      formula_id: recallFormulaId.value,
      horizon_days: recallHorizon.value,
      signal_window_days: recallWindow.value,
      refresh: true,
    })
    missedExpanded.value = false
    infoMsg.value = recallResult.value.recall === null
      ? '公式当期无入选，无法计算召回'
      : `召回率 ${(recallResult.value.recall * 100).toFixed(1)}%（池内入选 ${recallResult.value.formula_selected_pool} / 全市场入选 ${recallResult.value.formula_selected_market}），漏票 ${recallResult.value.missed_count}`
  } catch (e: any) {
    infoMsg.value = '召回验证失败：' + errMsg(e)
  } finally {
    computingRecall.value = false
  }
}

function causeLabel(key: string): string {
  if (key === 'rank_cut') return '排名截断（Top N 太小）'
  if (key === 'not_candidate') return '未进入候选（闸门/非universe）'
  if (key.startsWith('gate:')) return '闸门 ' + key.slice(5)
  return key
}

onMounted(() => {
  loadAll()
  loadBuildStatus()
})
onUnmounted(() => {
  if (buildTimer !== undefined) window.clearInterval(buildTimer)
})
</script>

<template>
  <PageHeader title="条件选股">
    <button class="btn" :disabled="loading" @click="loadAll">刷新</button>
  </PageHeader>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:12px">
    <div class="form-row" style="align-items:center;justify-content:space-between">
      <span>{{ errorMsg }}</span>
      <button class="btn btn-sm" :disabled="loading" @click="loadAll">重试</button>
    </div>
  </div>
  <div v-if="infoMsg" class="card" style="padding:12px;margin-bottom:12px">
    {{ infoMsg }}
  </div>

  <div class="tabs">
    <button
      v-for="t in TABS" :key="t.key"
      class="btn btn-sm" :class="{ 'btn-primary': tab === t.key }"
      @click="tab = t.key"
    >{{ t.label }}</button>
  </div>

  <div v-if="loading" class="card" style="padding:12px"><p>加载中…</p></div>

  <template v-else>
    <!-- 1. 选股方案：方案列表 + 版本（不可变/激活互斥）+ 参数编辑 -->
    <div v-show="tab === 'schemes'">
      <div class="card" style="padding:12px;margin-bottom:12px">
        <div class="form-row">
          <button class="btn btn-sm" :disabled="seeding" @click="seedDefaults">
            {{ seeding ? '初始化中…' : '初始化默认方案（四风格 + 行业池）' }}
          </button>
          <span class="hint">幂等：已有方案不会被覆盖；四风格 + 9 个行业主池 + 14 个标签池各补一个 v1（未校准）。</span>
        </div>
        <div v-if="industryMembershipText" class="hint" style="margin-top:6px">
          行业归属（当前 as_of）：{{ industryMembershipText }}
        </div>
        <div v-if="schemes.length === 0" class="form-row" style="margin-top:8px">
          <button class="btn btn-primary btn-sm" @click="tab = 'data'">去「数据体检」构建宽库</button>
          <span class="hint">导入约 60–90 秒，导入后再运行选股。</span>
        </div>
      </div>

      <div class="card" style="padding:12px;margin-bottom:12px">
        <h3 style="margin-top:0">新建方案</h3>
        <div class="form-row">
          <input v-model="newScheme.code" placeholder="code，如 value_core" />
          <input v-model="newScheme.name" placeholder="名称" />
          <select v-model="newScheme.pool_key">
            <optgroup v-for="g in poolOptionGroups" :key="g.def.key" :label="optgroupLabel(g.def)">
              <option v-for="p in g.keys" :key="p" :value="p">{{ poolLabel(p) }}</option>
            </optgroup>
          </select>
          <button class="btn btn-primary btn-sm" @click="submitScheme">创建</button>
        </div>
      </div>

      <div class="card table-wrap">
        <table>
          <thead><tr><th>名称</th><th>code</th><th>池</th><th>状态</th><th>最新版本</th><th>操作</th></tr></thead>
          <tbody>
            <template v-for="g in schemeGroups" :key="g.def.key">
              <tr class="group-row" @click="toggleSchemeGroup(g.def.key)">
                <td colspan="6">
                  <span class="group-toggle">{{ isCollapsed(g.def.key) ? '▶' : '▼' }}</span>
                  <strong>{{ g.def.label }}</strong>
                  <span class="badge badge-gray" style="margin-left:6px">{{ g.items.length }}</span>
                  <span class="hint" style="margin-left:8px">{{ g.def.hint }}</span>
                </td>
              </tr>
              <template v-if="!isCollapsed(g.def.key)">
                <tr v-if="!g.items.length">
                  <td colspan="6" class="hint" style="padding-left:28px">该分组暂无方案</td>
                </tr>
                <tr v-for="s in g.items" :key="s.id">
                  <td>{{ s.name }}</td>
                  <td style="color:var(--ink-soft)">{{ s.code }}</td>
                  <td>{{ poolLabel(s.pool_key) }}</td>
                  <td><span class="badge badge-gray">{{ s.status }}</span></td>
                  <td>
                    v{{ s.latest_version ?? '—' }}
                    <span v-if="s.latest_status === 'active'" class="badge badge-green" style="margin-left:6px">启用中</span>
                  </td>
                  <td><button class="btn btn-sm" @click="openScheme(s)">编辑/版本</button></td>
                </tr>
              </template>
            </template>
          </tbody>
        </table>
        <div v-if="schemes.length === 0" class="empty-state">
          <p>暂无方案，点上方「初始化默认方案（四风格 + 行业池）」开始</p>
        </div>
      </div>

      <template v-if="selected">
        <div class="card" style="padding:12px;margin-top:12px">
          <h3 style="margin-top:0">
            {{ selected.name }}（{{ selected.code }} · {{ poolLabel(selected.pool_key) }}）的版本
          </h3>
          <div class="card table-wrap" style="margin-bottom:12px">
            <table>
              <thead><tr><th>版本</th><th>状态</th><th>权重和</th><th>spec_hash</th><th>字段指纹</th><th>校准</th><th>说明</th><th>操作</th></tr></thead>
              <tbody>
                <tr v-for="v in selected.versions" :key="v.id">
                  <td>v{{ v.version }}</td>
                  <td>
                    <span :class="v.status === 'active' ? 'badge badge-green' : 'badge badge-gray'">
                      {{ v.status === 'active' ? '启用' : (v.status === 'archived' ? '归档' : '草稿') }}
                    </span>
                  </td>
                  <td>{{ v.weight_sum ?? '—' }}</td>
                  <td style="color:var(--ink-soft)">{{ v.spec_hash }}</td>
                  <td style="color:var(--ink-soft)">{{ v.field_fingerprint }}</td>
                  <td>
                    <span v-if="v.uncalibrated" class="badge" style="background:var(--line);color:var(--ink-soft)" title="排序未经全买型验证；本池按候选域口径验证召回">排序未验证（候选域口径）</span>
                    <span v-else class="badge badge-green">已校准</span>
                  </td>
                  <td style="color:var(--ink-soft)">{{ v.note || '—' }}</td>
                  <td>
                    <button
                      v-if="v.status !== 'active'" class="btn btn-sm"
                      @click="activate(v)"
                    >启用</button>
                    <span v-else class="hint">当前启用</span>
                  </td>
                </tr>
              </tbody>
            </table>
            <div v-if="selected.versions.length === 0" class="empty-state"><p>该方案还没有版本</p></div>
          </div>
          <p class="hint">
            版本一经创建不可修改；启用某个版本会让同方案其他启用版本自动归档（激活互斥）。
          </p>
        </div>

        <div class="card" style="padding:12px;margin-top:12px">
          <h3 style="margin-top:0">参数编辑（保存为新版本）</h3>
          <div class="form-row" style="margin-bottom:8px">
            <label class="hint">Top N（每轨上限）
              <input v-model.number="editor.top_n" type="number" min="1" max="1000" style="width:80px" />
            </label>
            <span class="hint">
              目标池规模 ≈
              {{ selected.pool_key === 'growth' && editor.track === 'both' ? editor.top_n * 2 : editor.top_n }}
              只（上限；成长双轨 = 2×Top N，实际还受闸门后可选数量限制）。
            </span>
            <label v-if="selected.pool_key === 'growth'" class="hint">轨道
              <select v-model="editor.track">
                <option value="both">双轨（前瞻+历史）</option>
                <option value="forward">仅前瞻</option>
                <option value="historical">仅历史</option>
              </select>
            </label>
          </div>

          <h4 style="margin-bottom:6px">
            权重（前瞻/主轨）
            <span class="hint" :style="{ color: Math.abs(weightSum - 1) < 1e-6 ? 'var(--ink-soft)' : 'var(--red)' }">
              合计 {{ weightSum.toFixed(4) }}
            </span>
          </h4>
          <div class="weight-grid">
            <div v-for="key in weightKeys" :key="key" class="weight-item">
              <span class="weight-name">{{ key }}</span>
              <input v-model.number="editor.weights[key]" type="number" step="0.01" min="0" max="1" />
              <button class="btn btn-sm" @click="removeField(key)">移除</button>
            </div>
          </div>

          <template v-if="selected.pool_key === 'growth' && editor.track === 'both'">
            <h4 style="margin-bottom:6px">
              权重（历史轨）
              <span class="hint" :style="{ color: Math.abs(histSum - 1) < 1e-6 ? 'var(--ink-soft)' : 'var(--red)' }">
                合计 {{ histSum.toFixed(4) }}
              </span>
            </h4>
            <div class="weight-grid">
              <div v-for="key in histKeys" :key="key" class="weight-item">
                <span class="weight-name">{{ key }}</span>
                <input v-model.number="editor.weights_hist[key]" type="number" step="0.01" min="0" max="1" />
                <button class="btn btn-sm" @click="removeHistField(key)">移除</button>
              </div>
            </div>
          </template>

          <div class="form-row" style="margin-top:8px">
            <select v-model="addFieldKey">
              <option value="">选择要添加的可打分字段</option>
              <option v-for="k in addableFields" :key="k" :value="k">{{ k }}</option>
            </select>
            <select v-if="selected.pool_key === 'growth' && editor.track === 'both'" v-model="activeTarget">
              <option value="forward">加到主轨</option>
              <option value="hist">加到历史轨</option>
            </select>
            <button class="btn btn-sm" :disabled="!addFieldKey" @click="addField">添加字段</button>
          </div>
          <p class="hint">
            只有「筛选条件」里标记为可打分的字段能进权重；权重非负且每条轨合计为 1，后端会二次校验。
          </p>

          <h4 style="margin:14px 0 6px">硬闸门</h4>
          <div class="gate-grid">
            <label v-for="g in GATES" :key="g.key" class="gate-item">
              <span class="hint">{{ g.label }}</span>
              <input v-model.number="editor.gates[g.key]" type="number" :step="g.step" />
            </label>
          </div>

          <h4 style="margin:14px 0 6px">PE 处理口径</h4>
          <div class="form-row">
            <select v-model="editor.pe_mode">
              <option value="strict">strict — 严格（PE≤0 一律挡在候选外）</option>
              <option value="loss_discount">loss_discount — 亏损股降权（PE≤0 进候选，按亏损幅度打折）</option>
            </select>
            <span class="hint">
              PE &gt; 上限两种口径都挡；降权只乘综合得分（亏损&lt;1%×0.90、1%~3.3%×0.75、
              3.3%~10%×0.55、&gt;10%×0.35），不改因子与权重。
            </span>
          </div>

          <div class="form-row" style="margin-top:12px">
            <input v-model="editor.note" placeholder="变更说明（可选）" style="flex:1;min-width:220px" />
            <button class="btn btn-primary btn-sm" :disabled="saving" @click="saveVersion">
              {{ saving ? '保存中…' : '保存为新版本' }}
            </button>
          </div>
          <p v-if="!sumOk" class="hint" style="color:var(--red)">
            权重合计不为 1，无法保存。请调整后再试。
          </p>
        </div>
      </template>
    </div>

    <!-- 2. 筛选条件：字段字典 + 当前启用参数 -->
    <div v-show="tab === 'filters'">
      <div class="card" style="padding:12px;margin-bottom:12px">
        <div class="form-row">
          <select v-model="filterPool">
            <option value="">全部池</option>
            <optgroup v-for="g in poolOptionGroups" :key="g.def.key" :label="optgroupLabel(g.def)">
              <option v-for="p in g.keys" :key="p" :value="p">{{ poolLabel(p) }}</option>
            </optgroup>
          </select>
          <span class="hint">字段集版本：{{ meta?.field_set_version }} · 可打分字段 {{ meta?.scorable_fields?.length || 0 }} 个</span>
        </div>
      </div>

      <div class="card table-wrap">
        <table>
          <thead><tr><th>字段</th><th>来源</th><th>原始列</th><th>单位</th><th>0 视为缺失</th><th>可打分</th><th>可用于历史</th><th>说明</th></tr></thead>
          <tbody>
            <tr v-for="f in filteredFields" :key="f.key">
              <td>{{ f.key }}</td>
              <td style="color:var(--ink-soft)">{{ f.source }}</td>
              <td style="color:var(--ink-soft)">{{ f.column }}</td>
              <td>{{ f.unit }}</td>
              <td>{{ f.zero_is_missing ? '是' : '否' }}</td>
              <td>
                <span v-if="scorable.has(f.key)" class="badge badge-green">是</span>
                <span v-else class="badge badge-gray">否</span>
              </td>
              <td>
                <span v-if="f.pit_safe" class="badge badge-green">是</span>
                <span v-else class="badge badge-gray">否（仅当期）</span>
              </td>
              <td style="color:var(--ink-soft)">{{ f.note }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <div v-if="activeVersion" class="card" style="padding:12px;margin-top:12px">
        <h3 style="margin-top:0">
          当前启用参数：{{ selected?.name }} v{{ activeVersion.version }}
          <span v-if="activeVersion.uncalibrated" class="badge" style="margin-left:8px;background:var(--line);color:var(--ink-soft)" title="排序未经全买型验证；本池按候选域口径验证召回">排序未验证（候选域口径）</span>
        </h3>
        <div class="card table-wrap" style="margin-bottom:10px">
          <table>
            <thead><tr><th>因子</th><th>权重</th><th>轨道</th></tr></thead>
            <tbody>
              <tr v-for="(w, k) in (activeVersion.spec as SelectionSpec).weights" :key="'f-' + k">
                <td>{{ k }}</td><td>{{ w }}</td><td>主轨</td>
              </tr>
              <tr v-for="(w, k) in ((activeVersion.spec as SelectionSpec).weights_hist || {})" :key="'h-' + k">
                <td>{{ k }}</td><td>{{ w }}</td><td>历史轨</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div class="card table-wrap">
          <table>
            <thead><tr><th>闸门</th><th>取值</th></tr></thead>
            <tbody>
              <tr v-for="g in GATES" :key="g.key">
                <td>{{ g.label }}</td>
                <td>{{ (activeVersion.spec as SelectionSpec).gates?.[g.key] ?? '—' }}</td>
              </tr>
              <tr>
                <td>PE 处理口径</td>
                <td>{{ PE_MODE_LABEL[(activeVersion.spec as SelectionSpec).pe_mode || 'strict'] }}</td>
              </tr>
              <tr>
                <td>Top N（每轨上限）</td>
                <td>
                  {{ (activeVersion.spec as SelectionSpec).top_n ?? 100 }}
                  <span class="hint">
                    （{{ (activeVersion.spec as SelectionSpec).track === 'both' ? '双轨，池规模≈2×' : '池规模=' }}Top N，实际受可选数量限制）
                  </span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
      <div v-else class="card" style="padding:12px;margin-top:12px">
        <p class="hint">在「选股方案」里选择一个方案并启用版本后，这里显示它的生效参数。</p>
      </div>
    </div>

    <!-- 3. 选股结果 -->
    <div v-show="tab === 'results'">
      <div v-if="!dataStatus?.ready" class="card empty-state" style="padding:16px;margin-bottom:12px">
        <p>平台导入仓还没有数据，暂时无法运行选股。按下面两步补齐：</p>
        <div class="form-row" style="justify-content:center">
          <button class="btn btn-primary btn-sm" @click="tab = 'data'">去「数据体检」构建宽库</button>
          <button class="btn btn-sm" :disabled="seeding" @click="seedDefaults">
            {{ seeding ? '初始化中…' : '初始化默认方案（四风格 + 行业池）' }}
          </button>
        </div>
        <p class="hint">
          导入约 60–90 秒（整表替换、可重跑）；初始化约 1 秒。完成后回到本页点「运行全部启用方案」，四池合计约 40 秒。
        </p>
      </div>
      <div v-else-if="schemes.length === 0" class="card empty-state" style="padding:16px;margin-bottom:12px">
        <p>宽库已导入（{{ dataStatus?.as_of }}），但还没有选股方案，无法运行。</p>
        <div class="form-row" style="justify-content:center">
          <button class="btn btn-primary btn-sm" :disabled="seeding" @click="seedDefaults">
            {{ seeding ? '初始化中…' : '初始化默认方案（四风格 + 行业池）' }}
          </button>
        </div>
        <p class="hint">初始化约 1 秒，生成成长/质量/价值/红利四套 v1 方案（未校准参数）。</p>
      </div>

      <div class="card" style="padding:12px;margin-bottom:12px">
        <div class="form-row">
          <button class="btn btn-primary btn-sm" :disabled="running" @click="runAll">
            {{ running ? '运行中…（四池约 40 秒）' : '运行全部启用方案' }}
          </button>
          <button class="btn btn-sm" :disabled="delivering" @click="deliverAll">
            {{ delivering ? '交付中…' : '收盘交付（运行→冻结→双池）' }}
          </button>
          <span class="hint">
            运行读取平台导入仓的 {{ dataStatus?.as_of || '导入日' }} 快照；只支持该日，历史回放暂不支持。
          </span>
        </div>
        <p v-if="lastRunResult?.skipped?.length" class="hint" style="color:var(--red)">
          跳过：{{ lastRunResult.skipped.map((s) => `${s.scheme_name}（${s.error}）`).join('；') }}
        </p>
      </div>

      <div class="card table-wrap">
        <table>
          <thead><tr><th>#</th><th>池</th><th>方案</th><th>as_of</th><th>状态</th><th>命中</th><th>快照</th><th>时间</th><th>操作</th></tr></thead>
          <tbody>
            <template v-for="g in runGroups" :key="g.def.key">
              <tr class="group-row">
                <td colspan="9">
                  <strong>{{ g.def.label }}</strong>
                  <span class="badge badge-gray" style="margin-left:6px">{{ g.items.length }}</span>
                  <span class="hint" style="margin-left:8px">{{ g.def.hint }}</span>
                </td>
              </tr>
              <tr v-if="!g.items.length">
                <td colspan="9" class="hint" style="padding-left:28px">该分组暂无运行记录</td>
              </tr>
              <tr v-for="r in g.items" :key="r.id">
                <td>{{ r.id }}</td>
                <td>{{ poolLabel(r.pool_key) }}</td>
                <td>{{ r.scheme_name }} <span style="color:var(--ink-soft)">v{{ r.version ?? '—' }}</span></td>
                <td>{{ r.as_of }}</td>
                <td><span :class="statusClass(r.status)">{{ r.status }}</span></td>
                <td>{{ r.stock_count }}</td>
                <td>{{ r.snapshot_id ?? '—' }}<span v-if="r.frozen" class="badge badge-green" style="margin-left:6px">冻结</span></td>
                <td style="color:var(--ink-soft)">{{ r.finished_at || r.created_at || '—' }}</td>
                <td><button class="btn btn-sm" @click="openRun(r)">查看</button></td>
              </tr>
            </template>
          </tbody>
        </table>
        <div v-if="runs.length === 0" class="empty-state"><p>还没有运行记录</p></div>
      </div>

      <div v-if="runDetail" class="card" style="padding:12px;margin-top:12px">
        <h3 style="margin-top:0">
          运行 #{{ runDetail.id }} · {{ runDetail.scheme_name }} · {{ poolLabel(runDetail.pool_key) }}
          <span :class="statusClass(runDetail.status)" style="margin-left:8px">{{ runDetail.status }}</span>
        </h3>
        <p v-if="runDetail.error" class="hint" style="color:var(--red)">{{ runDetail.error }}</p>

        <div class="stat-grid">
          <div><span class="hint">全市场</span><b>{{ runDetail.stats?.universe ?? '—' }}</b></div>
          <div><span class="hint">过闸门</span><b>{{ runDetail.stats?.gate_passed ?? '—' }}</b></div>
          <div><span class="hint">可打分</span><b>{{ runDetail.stats?.eligible ?? '—' }}</b></div>
          <div><span class="hint">命中</span><b>{{ runDetail.stats?.selected ?? '—' }}</b></div>
          <div><span class="hint">Top N（每轨）</span><b>{{ runDetail.stats?.top_n ?? '—' }}</b></div>
          <div v-if="runDetail.stats?.track_mode"><span class="hint">轨道</span><b>{{ trackLabel(runDetail.stats.track_mode) }}</b></div>
        </div>

        <div class="two-col">
          <div>
            <h4>闸门淘汰计数</h4>
            <div class="card table-wrap" style="max-height:220px;overflow:auto">
              <table>
                <thead><tr><th>原因</th><th>数量</th></tr></thead>
                <tbody>
                  <tr v-for="(n, k) in (runDetail.stats?.gate_drops || {})" :key="k">
                    <td>{{ k }}</td><td>{{ n }}</td>
                  </tr>
                </tbody>
              </table>
              <div v-if="!Object.keys(runDetail.stats?.gate_drops || {}).length" class="empty-state"><p>无淘汰</p></div>
            </div>
          </div>
          <div>
            <h4>因子覆盖（过闸门后非空）</h4>
            <div class="card table-wrap" style="max-height:220px;overflow:auto">
              <table>
                <thead><tr><th>因子</th><th>覆盖</th></tr></thead>
                <tbody>
                  <tr v-for="(n, k) in (runDetail.stats?.factor_coverage || {})" :key="k">
                    <td>{{ k }}</td><td>{{ n }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>

        <h4>数据质量披露</h4>
        <div class="card table-wrap" style="margin-bottom:12px">
          <table>
            <tbody>
              <tr v-for="[label, val] in qualityEntries(runDetail.quality)" :key="label">
                <td style="width:180px;color:var(--ink-soft)">{{ label }}</td>
                <td>{{ val }}</td>
              </tr>
            </tbody>
          </table>
        </div>

        <h4>
          池子有效性（召回）
          <span class="hint">池是候选域，最终买入由公式择时决定</span>
        </h4>
        <div class="card" style="padding:12px;margin-bottom:12px">
          <div class="form-row" style="flex-wrap:wrap;gap:8px;margin-bottom:8px">
            <select v-model.number="recallSnapshotId">
              <option :value="null">选择已冻结快照</option>
              <optgroup v-for="g in frozenSnapshotGroups" :key="g.def.key" :label="optgroupLabel(g.def)">
                <option v-if="!g.items.length" disabled>（暂无冻结快照）</option>
                <option v-for="s in g.items" :key="s.id" :value="s.id">
                  #{{ s.id }} {{ s.scheme_name }} · {{ s.as_of }}
                </option>
              </optgroup>
            </select>
            <select v-model.number="recallFormulaId">
              <option :value="null">选择择时公式</option>
              <option v-for="f in formulas" :key="f.formula_id" :value="f.formula_id">
                {{ f.name }}（{{ f.open_signals.join(' / ') || '无 OPEN 线' }}）
              </option>
            </select>
            <label class="hint">
              事后
              <input v-model.number="recallHorizon" type="number" min="1" max="120" style="width:64px" />
              个交易日
            </label>
            <label class="hint">
              入选窗口
              <input v-model.number="recallWindow" type="number" min="1" max="120" style="width:64px" />
              个交易 bar
            </label>
            <button class="btn btn-sm btn-primary" :disabled="computingRecall" @click="computeRecall">
              {{ computingRecall ? '计算中…' : '计算召回' }}
            </button>
          </div>
          <p class="hint">
            口径：同一期取「公式在全市场入选集合」与「公式在池内入选集合」，
            召回率 = 池内入选 / 全市场入选；漏票为公式选中但落在池外的票，
            成因按「排名截断 / 闸门 / 未进候选」分类计数。择时 OPEN 是稀疏脉冲
            （实测单票每年 1-3 次），故「入选」指 as_of 前最近 N 个交易 bar 内触发，
            窗口填 1 为严格单日口径。
          </p>
          <template v-if="recallResult">
            <div class="form-row" style="gap:16px;margin-bottom:8px">
              <span>
                <b>召回率</b>
                {{ recallResult.recall === null ? '—' : (recallResult.recall * 100).toFixed(1) + '%' }}
              </span>
              <span class="hint">全市场入选 {{ recallResult.formula_selected_market }}</span>
              <span class="hint">池内入选 {{ recallResult.formula_selected_pool }}</span>
              <span class="hint">池 {{ recallResult.pool_size }} / universe {{ recallResult.universe_size }}</span>
              <span class="hint">漏票 {{ recallResult.missed_count }}</span>
              <span v-if="recallResult.cached" class="badge badge-gray">缓存</span>
            </div>
            <div class="form-row" style="gap:8px;margin-bottom:8px;flex-wrap:wrap">
              <span v-for="(n, k) in recallResult.cause_counts" :key="k" class="badge badge-gray">
                {{ causeLabel(String(k)) }} {{ n }}
              </span>
            </div>
            <p v-if="recallResult.eval_error_count" class="hint" style="color:var(--red)">
              有 {{ recallResult.eval_error_count }} 只公式求值失败（不计入召回分子/分母），
              样本：{{ recallResult.eval_error_sample.join('、') }}
            </p>
            <button v-if="recallResult.missed.length" class="btn btn-sm" @click="missedExpanded = !missedExpanded">
              {{ missedExpanded ? '收起漏票清单' : '展开漏票清单（' + recallResult.missed.length + '）' }}
            </button>
            <div v-if="missedExpanded" class="card table-wrap" style="margin-top:8px">
              <table>
                <thead>
                  <tr>
                    <th>代码</th><th>成因</th><th>事后{{ recallResult.horizon_days }}日%</th>
                    <th>排名</th><th>得分</th><th>主要因子（分位）</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="m in recallResult.missed" :key="m.code">
                    <td>{{ m.code }}</td>
                    <td>{{ causeLabel(m.cause) }}</td>
                    <td :style="{ color: (m.forward_return_pct ?? 0) >= 0 ? 'var(--green)' : 'var(--red)' }">
                      {{ m.forward_return_pct === null ? '数据不足' : m.forward_return_pct.toFixed(2) }}
                    </td>
                    <td>{{ m.rank ?? '—' }}</td>
                    <td>{{ m.score === null || m.score === undefined ? '—' : m.score.toFixed(4) }}</td>
                    <td style="font-size:12px;color:var(--ink-soft)">
                      {{ (m.top_factors || []).map((f) => `${f.factor} ${f.pct !== undefined ? (f.pct * 100).toFixed(0) + '%' : ''}`).join(' · ') || m.note || '—' }}
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
            <p class="hint" style="margin-top:8px">{{ recallResult.notes.join(' ') }}</p>
          </template>
        </div>

        <details class="card" style="padding:12px;margin-bottom:12px">
          <summary style="cursor:pointer">
            <b>参考：因子排序 IC / 分组单调性</b>
            <span class="hint">（全买型假设，仅参考，不参与任何决策）</span>
          </summary>
          <div style="margin-top:8px">
            <p v-if="!runValidation" class="hint">尚未导入验证摘要</p>
            <template v-else-if="!runValidation.available">
              <p class="hint" style="color:var(--red)">不可用：{{ runValidation.reason }}</p>
            </template>
            <template v-else>
              <p class="hint">
                调仓 {{ runValidation.n_periods }} 期 · 前向 {{ runValidation.horizon_days }} 个交易日 ·
                as_of {{ runValidation.as_of }}
              </p>
              <div class="card table-wrap" style="margin-bottom:8px">
                <table>
                  <thead><tr><th>因子</th><th>期数</th><th>均值 IC</th><th>ICIR</th><th>IC&gt;0 占比</th><th>分组单调</th><th>分组均值%</th></tr></thead>
                  <tbody>
                    <tr v-for="(v, k) in (runValidation.factors || {})" :key="k">
                      <td>{{ k }}</td>
                      <td>{{ v.n_periods ?? '—' }}</td>
                      <td>{{ v.mean_ic !== undefined ? v.mean_ic.toFixed(4) : '—' }}</td>
                      <td>{{ v.icir ?? '—' }}</td>
                      <td>{{ v.positive_ratio !== undefined ? (v.positive_ratio * 100).toFixed(0) + '%' : '—' }}</td>
                      <td>{{ v.insufficient ? '样本不足' : (v.monotonic ? '是' : '否') }}</td>
                      <td>{{ (v.quintile_means_pct || []).map((x) => x === null ? '—' : x.toFixed(2)).join(' / ') }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <p v-if="(runValidation.unvalidated_used || []).length" class="hint">
                本轮权重中无历史快照可回验的因子（不假装有证据）：
                {{ (runValidation.unvalidated_used || []).join('、') }}
              </p>
            </template>
          </div>
        </details>

        <div class="form-row" style="margin-bottom:8px">
          <label class="hint"><input v-model="stockFilter" type="radio" value="selected" /> 只看命中</label>
          <label class="hint"><input v-model="stockFilter" type="radio" value="all" /> 含候补/排除</label>
          <span class="hint">共 {{ visibleStocks.length }} 行</span>
        </div>

        <div class="card table-wrap">
          <table>
            <thead><tr><th>排名</th><th>代码</th><th>名称</th><th>行业</th><th>轨道</th><th>得分</th><th>覆盖</th><th>状态</th></tr></thead>
            <tbody>
              <template v-for="row in visibleStocks" :key="row.code">
                <tr style="cursor:pointer" @click="expandedStock = expandedStock === row.code ? '' : row.code">
                  <td>{{ row.rank ?? '—' }}</td>
                  <td>{{ row.code }}</td>
                  <td>{{ row.name || '—' }}</td>
                  <td style="color:var(--ink-soft)">{{ row.sector || '—' }}</td>
                  <td>{{ trackLabel(row.track) }}</td>
                  <td>{{ fmtNum(row.score) }}</td>
                  <td>{{ row.coverage !== null && row.coverage !== undefined ? (row.coverage * 100).toFixed(0) + '%' : '—' }}</td>
                  <td>
                    <span v-if="row.selected" class="badge badge-green">命中</span>
                    <span v-else class="badge badge-gray">{{ row.exclude_reason || row.gate || '候补' }}</span>
                  </td>
                </tr>
                <tr v-if="expandedStock === row.code">
                  <td colspan="8" style="background:var(--bg-soft);font-size:12px">
                    <div style="margin-bottom:4px">
                      <b>命中解释</b>
                      <span v-if="!(row.explain || []).length" class="hint">（未落盘解释）</span>
                      <span
                        v-for="e in (row.explain || []).slice(0, 5)" :key="e.factor"
                        style="margin-right:14px"
                      >
                        {{ e.factor }}：分位 {{ (e.pct * 100).toFixed(1) }}% × 权重 {{ e.weight }}
                        = {{ e.contribution.toFixed(4) }}
                      </span>
                    </div>
                    <div>
                      <b>因子值</b>
                      <span v-if="!stockFactorEntries(row).length" class="hint">（未落盘因子明细）</span>
                      <span v-for="[k, v] in stockFactorEntries(row)" :key="k" style="margin-right:12px">
                        {{ k }}={{ typeof v === 'number' ? fmtNum(v, 4) : v }}
                      </span>
                    </div>
                  </td>
                </tr>
              </template>
            </tbody>
          </table>
          <div v-if="visibleStocks.length === 0" class="empty-state"><p>没有可展示的股票</p></div>
        </div>
        <p class="hint">
          门槛淘汰的个股只留计数不落明细；「候补」为未命中但通过闸门且因子覆盖达标的股票（最多 2×Top N）。
        </p>
      </div>
    </div>

    <!-- 4. 快照与历史 -->
    <div v-show="tab === 'snapshots'">
      <div class="card" style="padding:12px;margin-bottom:12px">
        <p class="hint" style="margin:0">
          快照冻结后只读；发布到股票池会同时写当前成分与 PIT 成员区间（生效日 = 快照日）。
          指纹由字段集版本、方案参数与导入仓版本共同决定，任一不同即不可直接比较。
        </p>
      </div>

      <div class="card" style="padding:12px;margin-bottom:12px">
        <h3 style="margin-top:0">池接入（回测取成分 / 实盘钉版本）</h3>
        <div class="form-row" style="margin-bottom:8px">
          <button class="btn btn-sm" @click="loadPools">刷新池清单</button>
          <span class="hint">序列池 SEL_风格 供回测按日取成分；版本池 SEL_风格_日期 供实盘挂接。</span>
        </div>
        <div class="card table-wrap" style="margin-bottom:10px">
          <table>
            <thead><tr><th>池</th><th>类型</th><th>as_of</th><th>冻结</th><th>当前成分</th><th>PIT 区间</th><th>操作</th></tr></thead>
            <tbody>
              <template v-for="g in poolGroups" :key="g.def.key">
                <tr class="group-row">
                  <td colspan="7">
                    <strong>{{ g.def.label }}</strong>
                    <span class="badge badge-gray" style="margin-left:6px">{{ g.items.length }}</span>
                    <span class="hint" style="margin-left:8px">{{ g.def.hint }}</span>
                  </td>
                </tr>
                <tr v-if="!g.items.length">
                  <td colspan="7" class="hint" style="padding-left:28px">该分组暂无池</td>
                </tr>
                <tr v-for="p in g.items" :key="p.pool_id">
                  <td>{{ p.code }}</td>
                  <td>
                    <span :class="p.kind === 'series' ? 'badge badge-gray' : 'badge badge-green'">
                      {{ p.kind === 'series' ? '序列（回测）' : '版本（实盘）' }}
                    </span>
                  </td>
                  <td>{{ p.as_of || '—' }}</td>
                  <td>{{ p.frozen ? '是' : '—' }}</td>
                  <td>{{ p.stock_count }}</td>
                  <td>{{ p.interval_count }}</td>
                  <td>
                    <button class="btn btn-sm" @click="resolvePoolId = p.pool_id; doResolvePool()">解析</button>
                  </td>
                </tr>
              </template>
            </tbody>
          </table>
          <div v-if="pools.length === 0" class="empty-state"><p>还没有条件选股池，先做一次「收盘交付」</p></div>
        </div>

        <div class="form-row" style="margin-bottom:8px">
          <select v-model.number="resolvePoolId">
            <option :value="null">选择池</option>
            <optgroup v-for="g in poolGroups" :key="g.def.key" :label="optgroupLabel(g.def)">
              <option v-if="!g.items.length" disabled>（暂无池）</option>
              <option v-for="p in g.items" :key="p.pool_id" :value="p.pool_id">{{ p.code }}</option>
            </optgroup>
          </select>
          <input v-model="resolveDay" type="date" />
          <button class="btn btn-sm" :disabled="!resolvePoolId" @click="doResolvePool">按日解析成分</button>
        </div>
        <p v-if="poolResolve" class="hint">
          {{ poolResolve.code }} @ {{ poolResolve.day }}：
          <b :style="{ color: poolResolve.available ? 'var(--green)' : 'var(--red)' }">
            {{ poolResolve.available ? '有该日 PIT 成分' : '该日无历史（fail-open）' }}
          </b>
          · 来源 {{ poolResolve.source }} · {{ poolResolve.codes.length }} 只
          <span v-if="poolResolve.note">（{{ poolResolve.note }}）</span>
          <br />前 10 只：{{ poolResolve.codes.slice(0, 10).join('、') }}
        </p>

        <h4 style="margin:14px 0 6px">实盘会话挂接</h4>
        <div class="form-row">
          <input v-model.number="sessionId" type="number" placeholder="实盘会话 id" style="width:120px" />
          <button class="btn btn-sm" @click="loadBinding">查绑定</button>
          <button class="btn btn-primary btn-sm" :disabled="!sessionId || !resolvePoolId" @click="doBind">挂所选池</button>
          <button class="btn btn-sm" :disabled="!bindResult" @click="doUnbind">回退</button>
        </div>
        <p v-if="sessionBinding" class="hint">
          {{ sessionBinding.session_name }} 当前：
          <span v-for="it in sessionBinding.items" :key="it.portfolio_strategy_id" style="margin-right:12px">
            {{ it.portfolio_name }} → {{ it.pool_code || '（未设置）' }}
            <span v-if="it.pool_kind">[{{ it.pool_kind === 'version' ? '条件池版本' : it.pool_kind === 'series' ? '条件池序列' : '既有池' }}]</span>
            · PIT {{ it.pool_pit_enabled ? '开' : '关' }}
          </span>
        </p>
        <p class="hint">
          只能挂版本池（frozen 快照），挂接后组合的 pool_pit_enabled 自动打开；
          「回退」按挂接前的池恢复，避免影响既有四池配置。
        </p>
      </div>

      <div class="card table-wrap">
        <table>
          <thead><tr><th>#</th><th>池</th><th>方案</th><th>as_of</th><th>命中</th><th>冻结</th><th>已发布池</th><th>指纹</th><th>操作</th></tr></thead>
          <tbody>
            <template v-for="g in snapshotGroups" :key="g.def.key">
              <tr class="group-row">
                <td colspan="9">
                  <strong>{{ g.def.label }}</strong>
                  <span class="badge badge-gray" style="margin-left:6px">{{ g.items.length }}</span>
                  <span class="hint" style="margin-left:8px">{{ g.def.hint }}</span>
                </td>
              </tr>
              <tr v-if="!g.items.length">
                <td colspan="9" class="hint" style="padding-left:28px">该分组暂无快照</td>
              </tr>
              <tr v-for="s in g.items" :key="s.id">
                <td>{{ s.id }}</td>
                <td>{{ poolLabel(s.pool_key) }}</td>
                <td>{{ s.scheme_name }} <span style="color:var(--ink-soft)">v{{ s.version ?? '—' }}</span></td>
                <td>{{ s.as_of }}</td>
                <td>{{ s.stock_count ?? '—' }}</td>
                <td>
                  <span :class="s.frozen ? 'badge badge-green' : 'badge badge-gray'">{{ s.frozen ? '已冻结' : '未冻结' }}</span>
                </td>
                <td>{{ s.published_pool_code || '—' }}</td>
                <td style="color:var(--ink-soft)">{{ s.fingerprint }}</td>
                <td>
                  <button class="btn btn-sm" @click="openSnapshot(s)">查看</button>
                  <button v-if="!s.frozen" class="btn btn-sm" @click="doFreeze(s)">冻结</button>
                </td>
              </tr>
            </template>
          </tbody>
        </table>
        <div v-if="snapshots.length === 0" class="empty-state"><p>还没有快照，先在「选股结果」运行一次</p></div>
      </div>

      <div v-if="snapshotDetail" class="card" style="padding:12px;margin-top:12px">
        <h3 style="margin-top:0">
          快照 #{{ snapshotDetail.id }} · {{ snapshotDetail.scheme_name }} · {{ snapshotDetail.as_of }}
        </h3>
        <div class="stat-grid">
          <div><span class="hint">全市场</span><b>{{ snapshotDetail.stats?.universe ?? '—' }}</b></div>
          <div><span class="hint">可打分</span><b>{{ snapshotDetail.stats?.eligible ?? '—' }}</b></div>
          <div><span class="hint">命中</span><b>{{ snapshotDetail.stock_count ?? '—' }}</b></div>
          <div><span class="hint">冻结</span><b>{{ snapshotDetail.frozen ? '是' : '否' }}</b></div>
        </div>

        <div class="card table-wrap" style="margin:10px 0">
          <table>
            <thead><tr><th>排名</th><th>代码</th><th>名称</th><th>行业</th><th>得分</th><th>覆盖</th></tr></thead>
            <tbody>
              <tr v-for="row in (snapshotDetail.stocks || [])" :key="row.code">
                <td>{{ row.rank ?? '—' }}</td>
                <td>{{ row.code }}</td>
                <td>{{ row.name || '—' }}</td>
                <td style="color:var(--ink-soft)">{{ row.sector || '—' }}</td>
                <td>{{ fmtNum(row.score) }}</td>
                <td>{{ row.coverage !== null && row.coverage !== undefined ? (row.coverage * 100).toFixed(0) + '%' : '—' }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="!(snapshotDetail.stocks || []).length" class="empty-state"><p>该快照没有命中股票</p></div>
        </div>

        <h4>发布到平台股票池</h4>
        <div class="form-row">
          <input v-model="publishForm.code" placeholder="池 code（留空自动 SEL_池_日期）" />
          <input v-model="publishForm.name" placeholder="池名称（留空自动生成）" />
          <button class="btn btn-primary btn-sm" :disabled="!snapshotDetail.frozen" @click="doPublish(snapshotDetail)">
            发布
          </button>
          <span v-if="!snapshotDetail.frozen" class="hint">先冻结才能发布</span>
        </div>
        <p v-if="publishResult" class="hint" style="color:var(--green)">
          已发布：{{ publishResult.pool_code }}（{{ publishResult.pool_name }}），{{ publishResult.stock_count }} 只，
          pool_id={{ publishResult.pool_id }}
        </p>
        <p class="hint">
          重复发布同一快照会复用同一股票池并刷新当前成分；PIT 区间不会为同一 (股票, 生效日) 重复插行。
        </p>
      </div>
    </div>

    <!-- 5. 数据体检 -->
    <div v-show="tab === 'data'">
      <div class="card" style="padding:12px;margin-bottom:12px">
        <h3 style="margin-top:0">内置宽库构建（默认数据源）</h3>
        <div class="form-row" style="margin-bottom:8px;align-items:center;gap:8px">
          <select v-model="buildMode" :disabled="buildStarting || buildStatus?.running" style="width:180px">
            <option value="incremental">增量（只补缺口）</option>
            <option value="full">全量（灾备，需确认）</option>
          </select>
          <button class="btn btn-primary btn-sm" :disabled="buildStarting || buildStatus?.running" @click="doBuild">
            {{ buildStatus?.running ? '构建中…' : '开始构建' }}
          </button>
          <span class="hint">后台线程执行，可离开本页；页面每 5 秒刷新阶段与进度。</span>
        </div>
        <template v-if="buildStatus">
          <div class="stat-grid">
            <div><span class="hint">快照 as_of</span><b>{{ buildStatus.as_of || '—' }}</b></div>
            <div><span class="hint">日线覆盖</span><b>{{ buildStatus.daily.min_date || '—' }} ~ {{ buildStatus.daily.max_date || '—' }}</b></div>
            <div><span class="hint">日线行数</span><b>{{ buildStatus.daily.rows }}</b></div>
            <div><span class="hint">运行号</span><b>{{ buildStatus.run_id }}</b></div>
            <div><span class="hint">状态</span><b>{{ buildStatus.running ? '运行中' : (buildStatus.last_build?.status || '空闲') }}</b></div>
            <div><span class="hint">最近完成</span><b>{{ buildStatus.last_build?.last_run_at || '—' }}</b></div>
          </div>
          <div v-if="buildStatus.phases.length" class="card table-wrap" style="margin:10px 0">
            <table>
              <thead><tr><th>阶段</th><th>批次完成</th><th>累计行数</th><th>进度</th><th>错误</th></tr></thead>
              <tbody>
                <tr v-for="p in buildStatus.phases" :key="p.phase">
                  <td>{{ BUILD_PHASE_LABEL[p.phase] || p.phase }}</td>
                  <td>{{ p.done_batches }}/{{ p.batches }}<span v-if="p.failed_batches" style="color:var(--red)">（失败 {{ p.failed_batches }}）</span></td>
                  <td>{{ p.rows_done }}</td>
                  <td>{{ p.percent }}%</td>
                  <td class="hint">{{ p.errors.join('；') || '—' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
          <p v-else class="hint">暂无构建批次（点「开始构建」，或等待交易日 15:30 调度）。</p>
          <p v-if="buildStatus.last_build?.error" class="hint" style="color:var(--red)">
            最近失败：{{ buildStatus.last_build.error }}
          </p>
        </template>
        <p v-else class="hint">未获取到构建状态（后端未加载新路由时需重启后端）。</p>
      </div>

      <div class="card" style="padding:12px;margin-bottom:12px">
        <h3 style="margin-top:0">平台导入仓（运行期读取层）</h3>
        <div class="form-row" style="margin-bottom:8px">
          <button class="btn btn-sm" :disabled="importing" @click="doImport">
            {{ importing ? '导入中…（约 60–90 秒，请勿离开）' : '从外部宽库导入（对账/兜底）' }}
          </button>
          <span class="hint">默认走上方内置构建；此入口仅用于外部宽库对账或灾备兜底，整表替换、可重跑。</span>
        </div>
        <p v-if="dataStatus?.ready" class="hint" style="color:var(--ink-soft)">
          导入仓已就绪（{{ dataStatus.as_of }}）。下一步：到「选股方案」初始化默认方案（四风格 + 行业池），再到「选股结果」点「运行全部启用方案」。
        </p>

        <template v-if="dataStatus">
          <p v-if="!dataStatus.ready" class="hint" style="color:var(--red)">
            {{ dataStatus.message || '尚未导入宽库' }}
          </p>
          <template v-else>
            <div class="stat-grid">
              <div><span class="hint">快照日</span><b>{{ dataStatus.as_of }}</b></div>
              <div><span class="hint">滞后</span><b>{{ dataStatus.stale_days ?? '—' }} 天</b></div>
              <div><span class="hint">成分</span><b>{{ dataStatus.stock_count ?? '—' }}</b></div>
              <div><span class="hint">财报</span><b>{{ dataStatus.fin_count ?? '—' }}</b></div>
              <div><span class="hint">分红</span><b>{{ dataStatus.dividend_count ?? '—' }}</b></div>
              <div><span class="hint">导入时间</span><b>{{ dataStatus.imported_at || '—' }}</b></div>
            </div>
            <p class="hint">来源：{{ dataStatus.source_path }}（{{ dataStatus.source_size_mb ?? '—' }} MB）</p>

            <div class="card table-wrap" style="margin-bottom:10px">
              <table>
                <thead><tr><th>项</th><th>数量</th></tr></thead>
                <tbody>
                  <tr v-for="[k, v] in Object.entries(dataStatus.counts || {}).filter(([k]) => k !== 'notes')" :key="k">
                    <td>{{ k }}</td><td>{{ v }}</td>
                  </tr>
                </tbody>
              </table>
            </div>
            <div v-if="(dataStatus.counts?.notes || []).length">
              <h4 style="margin-bottom:4px">口径与缺口披露</h4>
              <ul class="hint" style="margin:0;padding-left:18px">
                <li v-for="(n, i) in dataStatus.counts?.notes" :key="i">{{ n }}</li>
              </ul>
            </div>
          </template>
        </template>
        <p v-else class="hint">未获取到导入仓状态</p>

        <template v-if="importResult">
          <h4>本次导入</h4>
          <p class="hint">
            快照日 {{ importResult.as_of }} · 导入时间 {{ importResult.imported_at }} ·
            universe {{ importResult.counts.universe ?? '—' }} · fin {{ importResult.counts.fin ?? '—' }} ·
            dividend {{ importResult.counts.dividend ?? '—' }} · go {{ importResult.counts.go ?? '—' }}
          </p>
        </template>
      </div>

      <div class="card" style="padding:12px;margin-bottom:12px">
        <h3 style="margin-top:0">
          参考：因子排序 IC（全买型假设）
          <span class="hint">仅参考，不参与任何决策；池子有效性请看「选股结果」的召回验证</span>
        </h3>
        <p v-if="!factorValidation" class="hint">尚未导入验证摘要（重新从宽库导入会一并回算）</p>
        <template v-else-if="!factorValidation.available">
          <p class="hint" style="color:var(--red)">不可用：{{ factorValidation.reason }}</p>
        </template>
        <template v-else>
          <p class="hint">
            as_of {{ factorValidation.as_of }} · 调仓 {{ factorValidation.n_periods }} 期 ·
            前向 {{ factorValidation.horizon_days }} 个交易日 · 步长 {{ factorValidation.step_days }} ·
            计算于 {{ factorValidation.computed_at || '—' }}
          </p>
          <div class="card table-wrap" style="margin-bottom:10px">
            <table>
              <thead>
                <tr>
                  <th>因子</th><th>期数</th><th>均值 IC</th><th>ICIR</th>
                  <th>IC&gt;0 占比</th><th>分组单调</th><th>分组均值（低→高，%）</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="[k, v] in validationRows" :key="k">
                  <td>{{ k }}</td>
                  <td>{{ v.n_periods ?? '—' }}</td>
                  <td v-if="v.insufficient" colspan="5" style="color:var(--red)">
                    样本不足：{{ v.reason }}
                  </td>
                  <template v-else>
                    <td :style="{ color: (v.mean_ic ?? 0) >= 0 ? 'var(--green)' : 'var(--red)' }">
                      {{ fmtNum(v.mean_ic, 4) }}
                    </td>
                    <td>{{ v.icir === null || v.icir === undefined ? '—' : fmtNum(v.icir, 3) }}</td>
                    <td>{{ v.positive_ratio === undefined ? '—' : (v.positive_ratio * 100).toFixed(0) + '%' }}</td>
                    <td>
                      {{ v.monotonic === undefined ? '—' : (v.monotonic ? '是' : '否') }}
                      <span v-if="v.monotonic_rho !== null && v.monotonic_rho !== undefined" class="hint">
                        （ρ={{ fmtNum(v.monotonic_rho, 2) }}）
                      </span>
                    </td>
                    <td style="color:var(--ink-soft)">
                      {{ (v.quintile_means_pct || []).map(x => x === null ? '—' : (x * 100).toFixed(2)).join(' / ') || '—' }}
                    </td>
                  </template>
                </tr>
              </tbody>
            </table>
            <div v-if="validationRows.length === 0" class="empty-state"><p>没有可回验因子</p></div>
          </div>
          <div v-if="Object.keys(factorValidation.unvalidated || {}).length">
            <h4 style="margin-bottom:4px">不可回验因子（不参与调权）</h4>
            <ul class="hint" style="margin:0;padding-left:18px">
              <li v-for="(reason, k) in factorValidation.unvalidated" :key="k">{{ k }}：{{ reason }}</li>
            </ul>
          </div>
          <p class="hint">
            口径：用宽库不复权日线与财报 announce_time 做严格 PIT 回算，只评估「因子分位 → 前向收益」的秩相关；
            IC 弱或期数不足时不据此调默认权重，仅作证据披露。估值类因子无历史快照，暂不回验。
          </p>
        </template>
      </div>

      <div class="card" style="padding:12px;margin-bottom:12px">
        <h3 style="margin-top:0">外部宽库（只读导入源）</h3>
        <p v-if="!wideDb" class="hint">未获取到宽库状态</p>
        <template v-else>
          <p class="hint">
            路径 {{ wideDb.path }} · 大小 {{ wideDb.size_mb ?? '—' }} MB ·
            {{ wideDb.ready ? '可读' : ('不可用：' + (wideDb.error || 'unknown')) }}
          </p>
          <div class="card table-wrap">
            <table>
              <thead><tr><th>表</th><th>列数</th><th>有数据</th><th>行数估计</th></tr></thead>
              <tbody>
                <tr v-for="t in wideDb.tables" :key="t.name">
                  <td>{{ t.name }}</td>
                  <td>{{ t.columns }}</td>
                  <td>{{ t.has_rows ? '是' : '否' }}</td>
                  <td>{{ t.rows_estimate ?? '—' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </template>
      </div>

      <div class="card" style="padding:12px">
        <h3 style="margin-top:0">GP16 缺口与除权断点核对</h3>
        <div class="form-row">
          <input v-model="gpAuditDay" type="date" />
          <button class="btn btn-sm btn-primary" @click="runGpAudit">核对</button>
        </div>
        <p class="hint">
          只读核对：停牌缺值不算真缺口；跳变超阈值但当日无除权事件记为 unexplained。
          取数入库需在导入窗口执行，页面不提供触发入口，避免实盘中占用 TQ 通道。
        </p>
        <pre v-if="gpAudit" class="card" style="padding:12px;overflow:auto;max-height:360px">{{ JSON.stringify(gpAudit, null, 2) }}</pre>
      </div>
    </div>

    <!-- 6. 使用说明：口径与当前实现逐条对应，未实现的能力不写入 -->
    <div v-show="tab === 'guide'">
      <div class="card" style="padding:14px;margin-bottom:12px">
        <h3 style="margin-top:0">最短使用路径</h3>
        <ol style="margin:0;padding-left:20px;line-height:1.9">
          <li>
            <b>宽库构建</b>：「数据体检」→ 选「增量（只补缺口）」→「开始构建」。默认数据源是内置构建，
            后台执行、可离开本页；「从外部宽库导入」只用于对账/灾备，整表替换、可重跑。
          </li>
          <li>
            <b>方案初始化</b>：「选股方案」→「初始化默认方案（四风格 + 行业池）」。幂等：已有方案不覆盖，
            为 4 个风格池 + 9 个 L1 + 14 个 L2 各补一个启用版 v1（首发参数未校准）。
          </li>
          <li>
            <b>运行选股</b>：「选股结果」→「运行全部启用方案」。只读平台导入仓的当前 as_of；
            页面不提供指定历史日的运行入口（历史由回放补建，见下文）。
          </li>
          <li>
            <b>冻结发布</b>：「快照与历史」→ 快照「冻结」→「发布」（发布版本池或序列池）。
            「收盘交付（运行→冻结→双池）」可一次完成运行、冻结、发版本池 + 序列池。
          </li>
          <li>
            <b>挂接实盘</b>：同页「实盘会话挂接」→ 选一个<b>已冻结的版本池</b> →「挂所选池」，
            组合的 pool_pit_enabled 自动打开；「回退」按挂接前快照还原。回测侧选序列池按日解析成分。
          </li>
        </ol>
      </div>

      <div class="card" style="padding:14px;margin-bottom:12px">
        <h3 style="margin-top:0">当期池与历史池</h3>
        <div class="card table-wrap">
          <table>
            <thead><tr><th>池</th><th>code 形态</th><th>内容</th><th>用途</th></tr></thead>
            <tbody>
              <tr>
                <td><b>版本池</b></td>
                <td><code>SEL_&lt;池&gt;_&lt;YYYYMMDD&gt;</code></td>
                <td>单日快照的成分，冻结后不可变</td>
                <td>实盘钉版本；单期复核/对照</td>
              </tr>
              <tr>
                <td><b>序列池</b></td>
                <td><code>SEL_&lt;池&gt;</code></td>
                <td>跨期 PIT 成员区间（selection_pool_intervals）</td>
                <td>回测/历史研究，按任意交易日解析成分</td>
              </tr>
            </tbody>
          </table>
        </div>
        <ul class="hint" style="margin:8px 0 0;padding-left:18px;line-height:1.8">
          <li><b>当期池</b> = 最新 as_of 冻结并发布的版本池，反映构建日可见数据。</li>
          <li>
            <b>历史池</b> = 回放补建的季末快照（11 期：2024-04-30 ~ 2026-09-30）
            以及历次发布累积的版本池。回放区间为封闭区间，最新一期为开口（effective_to 为空）。
          </li>
          <li>
            解析优先级：先 stock_pool_memberships（发布写入），该日无覆盖时回落 selection_pool_intervals
            （回放补建），仍无则返回当前成分并标 <code>source=current_fallback / available=false</code>，
            调用方按 fail-open 处理、不冒充历史成分。
          </li>
        </ul>
      </div>

      <div class="card" style="padding:14px;margin-bottom:12px">
        <h3 style="margin-top:0">三类池的口径</h3>
        <div class="card table-wrap">
          <table>
            <thead><tr><th>类别</th><th>数量</th><th>口径</th></tr></thead>
            <tbody>
              <tr>
                <td><b>因子风格</b></td>
                <td>4</td>
                <td>
                  成长 / 质量 / 价值 / 红利，每池 TopN=100；四个风格池各自独立运行，同一股票可同时命中多个池。
                  成长池为双轨：前瞻轨用 GO 一致预期（g_fwd_2y / neg_peg_fwd / g_rev_2y / accel / roic_trend），
                  历史 as_of 无 GO 数据时自动切历史轨（np_yoy / neg_peg_hist / rev_yoy / g_hist_3y / roic_trend）。
                </td>
              </tr>
              <tr>
                <td><b>行业一级（L1）</b></td>
                <td>9</td>
                <td>
                  周期、消费、科技、医药、高端制造、金融地产、公用事业、农业、其他综合；
                  <b>互斥主池</b>，每只股票只归一个 L1。金融地产第一阶段只分组不打分。
                </td>
              </tr>
              <tr>
                <td><b>行业二级（L2 标签）</b></td>
                <td>14</td>
                <td>
                  上游资源、中游材料、交运周期、产能周期、军工、农产品周期、可选消费、品牌、地产链、
                  平台、必选消费、金融单列、高研发、高股息防御；<b>可重叠标签池</b>，同一股票可属多个 L2。
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <p class="hint" style="margin:8px 0 0">
          行业映射为平台内置表（129 个二级行业 → L1/L2），运行期不读取外部目录；行业池与四风格池 key 正交，互不覆盖。
        </p>
      </div>

      <div class="card" style="padding:14px;margin-bottom:12px">
        <h3 style="margin-top:0">数据新鲜度与 as_of</h3>
        <ul style="margin:0;padding-left:18px;line-height:1.8">
          <li>
            <b>as_of</b> = 宽库最后一次发布（publish）的交易日，落 selection_wh_meta.as_of；
            构建失败不改变旧 as_of 的语义（新增行只补缺口，不改写旧日期已覆盖的值）。
          </li>
          <li>
            「数据体检」展示 as_of、日线覆盖区间与行数、stale 天数；增量构建补到最近交易日，
            非交易日/未构建时 as_of 停留在上一交易日（stale=1 属正常）。
          </li>
          <li>运行选股、冻结快照都按 as_of 读取；快照指纹由字段集版本 + 方案参数 + 导入仓版本共同决定，任一不同即不可直接比较。</li>
        </ul>
      </div>

      <div class="card" style="padding:14px;margin-bottom:12px">
        <h3 style="margin-top:0">回放的 PIT 边界与已知偏差</h3>
        <ul style="margin:0;padding-left:18px;line-height:1.8">
          <li>财报：只取 <code>announce_time &lt;= as_of</code> 的披露，逐期截断，同 tag 取截止日前最后一次披露。</li>
          <li>市值：GP16（万元 → 亿元）按「年 + 月日」取 as_of 当时值；分红：除权日 &lt;= as_of。</li>
          <li>K 线：<b>不复权收盘</b>，取 as_of 前 252 个交易日；除权日附近动量/回撤/均线类因子存在偏差，送转与分红未复权。</li>
          <li>
            <b>pit_unavailable（置空、绝不用导入日值冒充）</b>：<code>pe</code>、<code>pb</code>、
            <code>ltsz</code>（无历史流通市值，规模用 GP16 总市值代理）、
            <code>g_fwd_2y / g_rev_2y / neg_peg_fwd</code>（GO 无历史序列）、
            <code>is_st</code>、<code>is_quit</code>（ST/退市只有当前值，回放置否并在此披露）。
            因此历史成长池自动走历史轨。
          </li>
          <li><b>名单为构建时的当前上市名单，不含已退市股票</b>，历史回放存在幸存者偏差。</li>
          <li>
            <b>行业归属按 as_of 累积</b>：分类接口只有当前值，只能从首次刷新起逐期落库；更早的 as_of
            回退最近一期归属（当前仅 2026-11-13/24 两期有记录），早期行业池的归属等同「按当前名单回看」。
          </li>
        </ul>
      </div>

      <div class="card" style="padding:14px">
        <h3 style="margin-top:0">验证口径：池只是候选域</h3>
        <ul style="margin:0;padding-left:18px;line-height:1.8">
          <li><b>池只是候选域，最终买什么由导入公式择时决定</b>；池的作用是缩小范围，不构成买卖信号。</li>
          <li>
            召回率 = 池内入选 / 全市场入选。「入选」= 公式 OPEN 信号线在 as_of 前最近 N 个交易 bar 内触发
            （择时脉冲稀疏，单日口径常为 0/0；N=1 即严格单日口径）。
          </li>
          <li>漏票收益 = as_of 起第 N 个交易日的<b>不复权</b>涨幅，取不到时置 null；漏票成因优先取快照留痕，未留痕按仓库快照重放基础闸门。</li>
          <li>IC / 分组单调性仅作参考，不参与任何决策与调权。</li>
        </ul>
      </div>
    </div>
  </template>
</template>

<style scoped>
.tabs { display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }
.form-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.form-row input, .form-row select { padding: 4px 8px; }
.hint { color: var(--ink-soft); font-size: 12px; }
.weight-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(230px, 1fr)); gap: 8px; }
.weight-item { display: flex; gap: 6px; align-items: center; }
.weight-item input { width: 80px; padding: 4px 6px; }
.weight-name { min-width: 96px; font-size: 13px; }
.gate-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 8px; }
.gate-item { display: flex; flex-direction: column; gap: 4px; }
.gate-item input { padding: 4px 6px; }
.stat-grid { display: flex; gap: 18px; flex-wrap: wrap; margin: 8px 0 4px; }
.stat-grid > div { display: flex; flex-direction: column; }
.stat-grid b { font-size: 16px; }
.two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
@media (max-width: 900px) { .two-col { grid-template-columns: 1fr; } }
.badge-red { background: var(--red); color: #fff; }
.group-row td { background: var(--bg-soft, #f6f7f9); cursor: pointer; user-select: none; }
.group-row:hover td { background: var(--bg-hover, #eef0f4); }
.group-toggle { display: inline-block; width: 14px; color: var(--ink-soft); font-size: 11px; }
</style>
