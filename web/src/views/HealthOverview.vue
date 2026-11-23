<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import {
  getDataObservabilitySummary, getHealthOverview,
  getKlineCoverageStatus, listKlineCoverage, getKlineCoveragePlan,
  getBacktestRecords, getBacktestDetail,
  type DataObservabilitySummary, type HealthOverview, type HealthSessionItem,
  type KlineCoverageStatus, type KlineCoverageRecord, type KlineCoveragePlan,
  type BacktestRecordItem, type DataFingerprint,
} from '../api'

const data = ref<HealthOverview | null>(null)
const obs = ref<DataObservabilitySummary | null>(null)
const obsError = ref('')
const loading = ref(true)
const errorMsg = ref('')
const busy = ref(false)

const LEVEL_TEXT: Record<string, string> = {
  ok: '健康', warn: '需关注', error: '异常',
}
const LEVEL_BADGE: Record<string, string> = {
  ok: 'badge-green', warn: 'badge-blue', error: 'badge-red',
}
const ENGINE_LABEL: Record<string, string> = {
  tdx: '通达信 TQ', ths: '同花顺', iquant: 'QMT 通道', goldminer: '掘金量化',
}
const NOTIFY_LABEL: Record<string, string> = {
  feishu: '飞书', wecom: '企微', email: '邮件', sound: '提示音',
}

const alertTotal = computed(() => {
  const t = data.value?.alert_totals
  return t ? t.open_decisions + t.deferred_orders + t.pending_approvals : 0
})

function badgeLevel(level: string): string {
  return LEVEL_BADGE[level] || 'badge-gray'
}

function statusBadge(status: string): string {
  if (status === 'running') return 'badge-green'
  if (status === 'stopping') return 'badge-blue'
  return 'badge-gray'
}

function onlineText(v: boolean | null | undefined): string {
  if (v === true) return '在线'
  if (v === false) return '离线'
  return '—'
}

// 数据新鲜度口径：未运行=不适用；缺失只看 stale+empty；截断/未完成单独表述，
// 绝不把「未校验」写成「有缺失/陈旧」。
function dataText(s: HealthSessionItem): string {
  if (s.data.not_running) return '未运行（不需要）'
  if (s.data.unavailable) return '不可用'
  if (!s.data.available) return '—'
  if (Number(s.data.bad_total || 0) > 0) return '有缺失/陈旧'
  if (s.data.incomplete) return '未校验（抽样）'
  return '正常'
}

function dataBadge(s: HealthSessionItem): string {
  if (s.data.not_running) return 'badge-gray'
  if (s.data.unavailable || !s.data.available) return 'badge-gray'
  if (Number(s.data.bad_total || 0) > 0) return 'badge-red'
  if (s.data.incomplete) return 'badge-blue'
  return 'badge-green'
}

/** 抽样未覆盖时补一行「已校验 X/Y 只」，说明是没验完而不是缺数据。 */
function dataNote(s: HealthSessionItem): string {
  if (s.data.not_running) return ''  // 徽标已写「未运行（不需要）」
  if (s.data.incomplete && s.data.codes_total) {
    return `已校验 ${s.data.codes_checked ?? 0}/${s.data.codes_total} 只，其余未校验、不计缺失`
  }
  return ''
}

interface GuardCheck {
  key?: string
  label?: string
  ok?: boolean
  detail?: string
  applicable?: boolean
}

/** 后端在 channel_guard 内返回逐项 checks（5 项）；旧数据无该字段时返回空数组。 */
function guardChecks(s: HealthSessionItem): GuardCheck[] {
  const raw = (s.channel_guard as unknown as { checks?: GuardCheck[] }).checks
  return Array.isArray(raw) ? raw : []
}

/** 逐周期新鲜度（1d/1m…），替代原先单一聚合数字。 */
function periodEntries(s: HealthSessionItem): [string, { stale: number; empty: number; total: number }][] {
  return Object.entries(s.data.periods ?? {})
}

function fmtTime(iso: string | null): string {
  return iso && iso.length >= 19 ? iso.replace('T', ' ').slice(0, 19) : '—'
}

async function load() {
  busy.value = true
  errorMsg.value = ''
  obsError.value = ''
  // 主总览失败才阻断页面；观测汇总失败只在卡片内提示（两者互相独立）。
  const [healthRes, obsRes] = await Promise.allSettled([
    getHealthOverview(), getDataObservabilitySummary(),
  ])
  if (healthRes.status === 'fulfilled') {
    data.value = healthRes.value
  } else {
    const e = healthRes.reason
    errorMsg.value = '加载失败: ' + (e instanceof Error ? e.message : String(e))
  }
  if (obsRes.status === 'fulfilled') {
    obs.value = obsRes.value
  } else {
    const e = obsRes.reason
    obsError.value = '观测汇总加载失败: ' + (e instanceof Error ? e.message : String(e))
  }
  loading.value = false
  busy.value = false
}

/** 口径过期原因 → 「原因 × N」文本（后端已给中文原因，原样展示）。 */
function caliberReasonText(): string {
  const reasons = obs.value?.caliber.reasons || {}
  const parts = Object.entries(reasons).map(([k, n]) => `${k} × ${n}`)
  return parts.length ? parts.join('；') : '—'
}

// ---- 台账详情 / 回测指纹对比（只读；查看不会开启台账）----
const showLedger = ref(false)
const ledgerTab = ref<'coverage' | 'fingerprint'>('coverage')
const ledgerLoading = ref(false)
const ledgerError = ref('')
const ledgerStatus = ref<KlineCoverageStatus | null>(null)
const ledgerRecords = ref<KlineCoverageRecord[]>([])
const ledgerPage = ref(1)
const LEDGER_PAGE_SIZE = 20
const expandedKey = ref('')
const planCache = ref<Record<string, KlineCoveragePlan | 'loading' | 'error'>>({})

function recordKey(r: KlineCoverageRecord): string {
  return `${r.code}|${r.period}|${r.field}`
}

function ledgerRange(r: KlineCoverageRecord): [string, string] | null {
  if (!r.ranges.length) return null
  return [r.ranges[0][0], r.ranges[r.ranges.length - 1][1]]
}

const ledgerTotalPages = computed(() =>
  Math.max(1, Math.ceil(ledgerRecords.value.length / LEDGER_PAGE_SIZE)))
const pagedRecords = computed(() => {
  const start = (ledgerPage.value - 1) * LEDGER_PAGE_SIZE
  return ledgerRecords.value.slice(start, start + LEDGER_PAGE_SIZE)
})

/** 当前页已加载计划的汇总：停牌（数据源确认无行情）/ 未发布（需补数）。 */
const pageGapTotals = computed(() => {
  let stop = 0, missing = 0, expected = 0, covered = 0, loaded = 0
  for (const r of pagedRecords.value) {
    const p = planCache.value[recordKey(r)]
    if (!p || p === 'loading' || p === 'error') continue
    loaded += 1
    expected += p.expected_days
    covered += p.covered_days
    missing += p.missing_days.length
    stop += p.empty_days.length
  }
  return { stop, missing, expected, covered, loaded }
})

async function loadLedger() {
  ledgerLoading.value = true
  ledgerError.value = ''
  try {
    const [status, list] = await Promise.all([
      getKlineCoverageStatus(), listKlineCoverage({ limit: 500 }),
    ])
    ledgerStatus.value = status
    ledgerRecords.value = list.records || []
    ledgerPage.value = 1
    expandedKey.value = ''
    planCache.value = {}
    await loadPagePlans()
  } catch (e) {
    ledgerError.value = '台账加载失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    ledgerLoading.value = false
  }
}

/** 逐票计划只按台账窗口查询（不传 local_days：缺口三分类由窗口内缺日推导）。 */
async function loadPagePlans() {
  const tasks = pagedRecords.value
    .filter((r) => ledgerRange(r) !== null)
    .map(async (r) => {
      const key = recordKey(r)
      if (planCache.value[key]) return
      const [start, end] = ledgerRange(r) as [string, string]
      planCache.value = { ...planCache.value, [key]: 'loading' }
      try {
        const plan = await getKlineCoveragePlan({
          code: r.code, period: r.period, start, end,
          source: r.source, field: r.field,
        })
        planCache.value = { ...planCache.value, [key]: plan }
      } catch {
        planCache.value = { ...planCache.value, [key]: 'error' }
      }
    })
  await Promise.all(tasks)
}

function planOf(r: KlineCoverageRecord): KlineCoveragePlan | null {
  const p = planCache.value[recordKey(r)]
  return p && p !== 'loading' && p !== 'error' ? p : null
}

function gapCell(r: KlineCoverageRecord, kind: 'stop' | 'missing'): string {
  const p = planOf(r)
  if (!p) return '—'
  if (!p.stamp_valid) return '台账不可信'
  if (kind === 'stop') return String(p.empty_days.length)
  return String(p.missing_days.length)
}

function coverageCell(r: KlineCoverageRecord): string {
  const p = planOf(r)
  if (!p) return '—'
  if (!p.stamp_valid) return '逐日核对'
  return `${p.covered_days}/${p.expected_days}`
}

function toggleRecord(r: KlineCoverageRecord) {
  const key = recordKey(r)
  expandedKey.value = expandedKey.value === key ? '' : key
}

/** 缺口日期列表：默认只显示前 60 天（防止单票数百天撑爆弹窗）。 */
function daysText(days?: string[], limit = 60): string {
  if (!days || !days.length) return '无'
  const head = days.slice(0, limit).join('，')
  return days.length > limit ? `${head} …共 ${days.length} 天` : head
}

function rangesText(r: KlineCoverageRecord): string {
  if (!r.ranges.length) return '—'
  return r.ranges.map((x) => `${x[0]} ~ ${x[1]}`).join('，')
}

function trustText(r: KlineCoverageRecord): string {
  const p = planOf(r)
  if (!p) return '计划加载中…'
  return p.trusted ? '可信任台账' : `需逐日核对（${p.reason}）`
}

function changeLedgerPage(delta: number) {
  const next = ledgerPage.value + delta
  if (next < 1 || next > ledgerTotalPages.value) return
  ledgerPage.value = next
  expandedKey.value = ''
  loadPagePlans()
}

function switchLedgerTab(tab: 'coverage' | 'fingerprint') {
  ledgerTab.value = tab
  if (tab === 'coverage' && !ledgerRecords.value.length && !ledgerLoading.value) {
    loadLedger()
  }
  if (tab === 'fingerprint' && !fpRecords.value.length) {
    loadFingerprintRecords()
  }
}

function openLedger() {
  showLedger.value = true
  switchLedgerTab(ledgerTab.value)
}

// ---- 回测记录指纹对比 ----
const fpRecords = ref<BacktestRecordItem[]>([])
const fpLoading = ref(false)
const fpError = ref('')
const fpComparing = ref(false)
const fpLeftId = ref<number | null>(null)
const fpRightId = ref<number | null>(null)
const fpLeft = ref<DataFingerprint | null>(null)
const fpRight = ref<DataFingerprint | null>(null)
const fpLeftLabel = ref('')
const fpRightLabel = ref('')

interface FpRow {
  key: string
  left: string
  right: string
  status: 'same' | 'diff' | 'left_only' | 'right_only'
}

function fpText(v?: { bars?: number; first?: string; last?: string }): string {
  if (!v) return '—'
  return `${v.bars ?? '—'} 根 · ${v.first || '—'} → ${v.last || '—'}`
}

const fpRows = computed<FpRow[]>(() => {
  const l = fpLeft.value?.per_code || {}
  const r = fpRight.value?.per_code || {}
  const keys = new Set<string>([...Object.keys(l), ...Object.keys(r)])
  const rows: FpRow[] = []
  for (const code of [...keys].sort()) {
    const periods = new Set<string>([
      ...Object.keys(l[code] || {}), ...Object.keys(r[code] || {}),
    ])
    for (const period of [...periods].sort()) {
      const lv = (l[code] || {})[period]
      const rv = (r[code] || {})[period]
      let status: FpRow['status'] = 'diff'
      if (!lv) status = 'right_only'
      else if (!rv) status = 'left_only'
      else if (lv.bars === rv.bars && lv.first === rv.first && lv.last === rv.last) {
        status = 'same'
      }
      rows.push({
        key: `${code} ${period}`,
        left: fpText(lv), right: fpText(rv), status,
      })
    }
  }
  return rows
})

async function loadFingerprintRecords() {
  fpLoading.value = true
  fpError.value = ''
  try {
    const list = await getBacktestRecords()
    fpRecords.value = (list || []).filter((r) => r.status !== 'running')
    if (!fpLeftId.value && fpRecords.value.length) {
      fpLeftId.value = fpRecords.value[0].id
    }
    if (!fpRightId.value && fpRecords.value.length > 1) {
      fpRightId.value = fpRecords.value[1].id
    }
  } catch (e) {
    fpError.value = '回测记录加载失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    fpLoading.value = false
  }
}

async function compareFingerprints() {
  if (!fpLeftId.value || !fpRightId.value) {
    fpError.value = '请选择两条回测记录'
    return
  }
  fpComparing.value = true
  fpError.value = ''
  try {
    const [a, b] = await Promise.all([
      getBacktestDetail(fpLeftId.value), getBacktestDetail(fpRightId.value),
    ])
    fpLeft.value = a.data_fingerprint || null
    fpRight.value = b.data_fingerprint || null
    fpLeftLabel.value = `#${a.record.id} ${a.record.name}`
    fpRightLabel.value = `#${b.record.id} ${b.record.name}`
    if (!fpLeft.value && !fpRight.value) {
      fpError.value = '两条记录都没有数据指纹（老记录或不含台账快照）'
    }
  } catch (e) {
    fpError.value = '指纹对比失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    fpComparing.value = false
  }
}

let timer: ReturnType<typeof setInterval> | null = null
onMounted(() => {
  load()
  // 30s 静默刷新：与「状态总览」的通道复探同频，页面常开也能看到通道/告警变化
  timer = setInterval(load, 30000)
})
onUnmounted(() => {
  if (timer !== null) {
    clearInterval(timer)
    timer = null
  }
})
</script>

<template>
  <div class="dash-hero">
    <div class="dash-hero-main">
      <div class="dash-hero-stats" v-if="data">
        <span>
          <span class="badge" :class="badgeLevel(data.level)">{{ LEVEL_TEXT[data.level] || data.level }}</span>
        </span>
        <span><b>{{ data.sessions.length }}</b> 会话</span>
        <span><b>{{ data.tasks.running_sessions }}</b> 运行中</span>
        <span><b class="db-accent">{{ alertTotal }}</b> 待处理</span>
        <span>生成于 {{ fmtTime(data.generated_at) }}</span>
      </div>
    </div>
    <div class="dash-hero-actions">
      <button class="btn" :disabled="busy" @click="load">{{ busy ? '刷新中…' : '刷新' }}</button>
    </div>
  </div>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:16px">
    {{ errorMsg }}
  </div>

  <div v-if="loading" class="card" style="padding:16px"><p>加载中…</p></div>
  <template v-else-if="data">
    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">01</span>
        <span class="frame-title">核心 / 当日任务 / 磁盘</span>
      </div>
      <div class="frame-body">
        <div class="health-stats">
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">Core</span>
              <b :style="{ color: data.core.online ? 'var(--green)' : 'var(--red)' }">{{ data.core.online ? '在线' : '离线' }}</b>
            </div>
            <i class="hs-sub" :title="`v${data.core.version || '—'} · ${data.core.uptime || '—'}`">v{{ data.core.version || '—' }} · {{ data.core.uptime || '—' }}</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">引擎 watchdog</span>
              <b>{{ data.watchdog.running ? '运行中' : '未运行' }}</b>
            </div>
            <i class="hs-sub" :title="`告警 ${data.watchdog.active_alarms.length} 条 · 扫描 ${fmtTime(data.watchdog.last_scan_at)}`">告警 {{ data.watchdog.active_alarms.length }} 条 · 扫描 {{ fmtTime(data.watchdog.last_scan_at) }}</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">自动备份</span>
              <b>{{ data.tasks.backup_done_today ? '今日已完成' : '今日未完成' }}</b>
            </div>
            <i class="hs-sub" :title="`计划 ${data.tasks.backup_at || '—'}`">计划 {{ data.tasks.backup_at || '—' }}</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">磁盘剩余</span>
              <b :style="{ color: data.disk.ok ? 'var(--green)' : 'var(--red)' }">
                {{ data.disk.free_mb === null ? '—' : `${data.disk.free_mb} MB` }}
              </b>
            </div>
            <i class="hs-sub" :title="`保护阈值 ${data.disk.min_free_mb} MB`">保护阈值 {{ data.disk.min_free_mb }} MB</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">待处理告警</span>
              <b :style="{ color: alertTotal ? 'var(--red)' : 'var(--ink)' }">{{ alertTotal }}</b>
            </div>
            <i class="hs-sub" :title="`未处理告警 ${data.alert_totals.open_decisions} · 顺延 ${data.alert_totals.deferred_orders} · 待审批 ${data.alert_totals.pending_approvals}`">
              <RouterLink to="/events?ack_status=open" class="alert-link">未处理告警 {{ data.alert_totals.open_decisions }}</RouterLink>
              · 顺延 {{ data.alert_totals.deferred_orders }} ·
              <RouterLink to="/live?tab=approvals" class="alert-link">待审批 {{ data.alert_totals.pending_approvals }}</RouterLink>
            </i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">告警外发</span>
              <b>{{ data.notify.usable ? '已配置' : (data.notify.enabled ? '未就绪' : '关闭') }}</b>
            </div>
            <i class="hs-sub" :title="`${data.notify.channels.map((c) => NOTIFY_LABEL[c] || c).join(' / ') || '未配置渠道'} · 最低 ${data.notify.min_level}`">
              {{ data.notify.channels.map((c) => NOTIFY_LABEL[c] || c).join(' / ') || '未配置渠道' }}
              · 最低 {{ data.notify.min_level }}
            </i>
          </div>
        </div>
      </div>
    </div>

    <div v-if="data.problems.length" class="frame">
      <div class="frame-head">
        <span class="frame-num">02</span>
        <span class="frame-title">需要关注（{{ data.problems.length }}）</span>
      </div>
      <div class="frame-body">
        <ul class="problem-list">
          <li v-for="(p, i) in data.problems" :key="i" :class="p.level">
            <span class="badge" :class="p.level === 'error' ? 'badge-red' : 'badge-blue'">
              {{ p.level === 'error' ? '异常' : '注意' }}
            </span>
            <span>
              {{ p.text }}
              <RouterLink v-if="p.fix" :to="p.fix.to" class="alert-link">
                {{ p.fix.label }} →
              </RouterLink>
              <i v-else-if="p.observe" class="hs-sub">（需外部处理：{{ p.observe }}）</i>
            </span>
          </li>
        </ul>
      </div>
    </div>

    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">03</span>
        <span class="frame-title">实盘会话</span>
      </div>
      <div class="frame-body">
        <div class="table-wrap">
          <table class="health-table">
            <colgroup>
              <col class="c-name" /><col class="c-status" /><col class="c-engine" />
              <col class="c-channel" /><col class="c-bridge" /><col class="c-data" />
              <col class="c-today" /><col class="c-alerts" /><col class="c-deferred" />
              <col class="c-approval" />
            </colgroup>
            <thead>
              <tr>
                <th>名称</th><th>状态</th><th>引擎</th><th>通道</th><th>桥</th>
                <th>数据新鲜度</th><th class="t-right">今日委托/成交</th>
                <th class="t-right">未处理告警</th><th class="t-right">顺延</th><th class="t-right">待审批</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="s in data.sessions" :key="s.id">
                <td>{{ s.name }}</td>
                <td><span class="badge" :class="statusBadge(s.status)">{{ s.status }}</span></td>
                <td>{{ ENGINE_LABEL[s.engine || ''] || s.engine || '—' }}</td>
                <td :style="{ color: s.channel_ok ? 'var(--green)' : 'var(--red)' }">
                  {{ s.channel_ok ? onlineText(s.channel.primary?.online) : '不可用' }}
                </td>
                <td>{{ onlineText(s.bridge.online) }}</td>
                <td class="data-cell">
                  <span class="badge" :class="dataBadge(s)">{{ dataText(s) }}</span>
                  <div v-if="s.data.unavailable" class="period-line">
                    {{ s.data.reason || '行情通道不可用' }}
                  </div>
                  <div v-if="dataNote(s)" class="period-line">{{ dataNote(s) }}</div>
                  <div v-for="[p, v] in periodEntries(s)" :key="p" class="period-line">
                    {{ p }}：陈旧 {{ v.stale }} / 空 {{ v.empty }} / 共 {{ v.total }}
                  </div>
                </td>
                <td class="num">{{ s.today.orders }} / {{ s.today.trades }}</td>
                <td class="num">
                  <RouterLink
                    v-if="s.alerts.open_decisions"
                    :to="`/events?ack_status=open&session_id=${s.id}`"
                    class="alert-link"
                    title="在关键事件检索中定位该会话的未处理事件"
                  >{{ s.alerts.open_decisions }}</RouterLink>
                  <span v-else>0</span>
                </td>
                <td class="num">
                  <RouterLink
                    v-if="s.alerts.deferred_orders"
                    to="/live?tab=orders"
                    class="alert-link"
                    title="前往实盘工作台处理顺延委托"
                  >{{ s.alerts.deferred_orders }}</RouterLink>
                  <span v-else>0</span>
                </td>
                <td class="num">
                  <RouterLink
                    v-if="s.alerts.pending_approvals"
                    to="/live?tab=approvals"
                    class="alert-link"
                    title="前往实盘工作台处理审批"
                  >{{ s.alerts.pending_approvals }}</RouterLink>
                  <span v-else>0</span>
                </td>
              </tr>
            </tbody>
          </table>
          <div v-if="data.sessions.length === 0" class="empty-state"><p>暂无实盘会话</p></div>
        </div>
      </div>
    </div>

    <div v-if="data.sessions.length" class="frame">
      <div class="frame-head">
        <span class="frame-num">04</span>
        <span class="frame-title">通道检查明细（逐项）</span>
      </div>
      <div class="frame-body">
        <div v-for="s in data.sessions" :key="s.id" class="guard-block">
          <div class="guard-head">
            <b>{{ s.name }}</b>
            <span class="badge" :class="s.channel_guard.ok ? 'badge-green' : 'badge-orange'">
              {{ s.channel_guard.available
                ? (s.channel_guard.ok ? '全部通过'
                  : `未通过 ${s.channel_guard.failed.length} 项`)
                : '检查不可用' }}
            </span>
          </div>
          <ul v-if="guardChecks(s).length" class="guard-list">
            <li v-for="c in guardChecks(s)" :key="c.key || c.label"
                :class="{ bad: c.ok === false && c.applicable !== false }">
              <span class="badge"
                    :class="c.applicable === false ? 'badge-gray' : (c.ok ? 'badge-green' : 'badge-red')">
                {{ c.applicable === false ? '不适用' : (c.ok ? '通过' : '未通过') }}
              </span>
              <span class="guard-label">{{ c.label || c.key || '—' }}</span>
              <span class="guard-detail">{{ c.detail || '—' }}</span>
            </li>
          </ul>
          <p v-else class="guard-detail">该会话没有可用的通道检查结果。</p>
        </div>
      </div>
    </div>
    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">05</span>
        <span class="frame-title">数据与口径观测</span>
      </div>
      <div class="frame-body">
        <div v-if="obsError" class="obs-error">{{ obsError }}</div>
        <div v-else-if="obs" class="health-stats obs-stats">
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">公司行为未验证</span>
              <b :style="{ color: obs.corporate_actions.unverified ? 'var(--red)' : 'var(--ink)' }">{{ obs.corporate_actions.unverified }}</b>
            </div>
            <i class="hs-sub" :title="obs.corporate_actions.codes.join(' ')">
              自动审计{{ obs.corporate_actions.enabled ? '已开启' : '已关闭' }} · 涉及 {{ obs.corporate_actions.codes.length }} 只
            </i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">最近一轮审计</span>
              <b>{{ obs.corporate_actions.last_audit ? (obs.corporate_actions.last_audit.reason || '—') : '未运行' }}</b>
            </div>
            <i class="hs-sub">{{ obs.corporate_actions.last_audit ? fmtTime(obs.corporate_actions.last_audit.at || null) : '暂无本轮结果' }}</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">口径过期标记</span>
              <b :style="{ color: obs.caliber.stale ? 'var(--red)' : 'var(--ink)' }">{{ obs.caliber.stale }}</b>
            </div>
            <i class="hs-sub" :title="caliberReasonText()">共 {{ obs.caliber.records }} 条记录</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">K 线覆盖台账</span>
              <b>{{ obs.kline_coverage.enabled ? (obs.kline_coverage.exists ? '已有台账' : '未生成') : '未开启' }}</b>
            </div>
            <i class="hs-sub" :title="obs.kline_coverage.ledger_path">
              覆盖 {{ obs.kline_coverage.coverage }} · 空日 {{ obs.kline_coverage.empty_days }} · 指纹 {{ obs.kline_coverage.stamps }}
            </i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">台账最近更新</span>
              <b>{{ fmtTime(obs.kline_coverage.latest_updated_at) }}</b>
            </div>
            <i class="hs-sub" :title="`生成于 ${fmtTime(obs.generated_at)}`">只读汇总 · 不改计算</i>
          </div>
        </div>
        <div style="margin-top:10px;display:flex;align-items:center;gap:8px;flex-wrap:wrap">
          <button class="btn btn-sm" data-test="ledger-open" @click="openLedger">
            台账详情 / 指纹对比
          </button>
          <span style="font-size:12px;color:var(--ink-soft)">
            只读查看：展示覆盖率、缺口三分类与逐票明细，不会开启台账
          </span>
        </div>
      </div>
    </div>
  </template>

  <!-- 台账详情 + 回测指纹对比（只读；查看不写入、不启用台账） -->
  <div v-if="showLedger" class="modal-overlay modal-lg" data-test="ledger-modal"
       @click.self="showLedger = false">
    <div class="modal-content">
      <h3>数据台账与指纹对比</h3>
      <div class="modal-body">
        <div style="display:flex;gap:8px;margin-bottom:12px;flex-wrap:wrap">
          <button class="btn btn-sm" :class="ledgerTab === 'coverage' ? 'btn-primary' : ''"
                  data-test="ledger-tab-coverage" @click="switchLedgerTab('coverage')">
            K 线覆盖台账
          </button>
          <button class="btn btn-sm" :class="ledgerTab === 'fingerprint' ? 'btn-primary' : ''"
                  data-test="ledger-tab-fingerprint" @click="switchLedgerTab('fingerprint')">
            回测记录指纹对比
          </button>
        </div>

        <p v-if="ledgerError" style="color:var(--red);font-size:13px">{{ ledgerError }}</p>
        <p v-if="fpError" style="color:var(--red);font-size:13px">{{ fpError }}</p>

        <!-- 台账 -->
        <template v-if="ledgerTab === 'coverage'">
          <p v-if="ledgerLoading">加载中…</p>
          <template v-else>
            <div class="health-stats" style="margin-bottom:10px">
              <div class="health-stat">
                <div class="hs-head">
                  <span class="hs-label">台账开关</span>
                  <b>{{ ledgerStatus?.enabled ? '已开启' : '未开启' }}</b>
                </div>
                <i class="hs-sub" :title="ledgerStatus?.ledger_path">
                  {{ ledgerStatus?.exists ? '库已生成' : '库未生成（只读查看不创建）' }}
                </i>
              </div>
              <div class="health-stat">
                <div class="hs-head">
                  <span class="hs-label">覆盖记录</span>
                  <b>{{ ledgerStatus?.coverage ?? 0 }}</b>
                </div>
                <i class="hs-sub">空日 {{ ledgerStatus?.empty_days ?? 0 }} · 指纹 {{ ledgerStatus?.stamps ?? 0 }}</i>
              </div>
              <div class="health-stat">
                <div class="hs-head">
                  <span class="hs-label">当前页缺口</span>
                  <b>{{ pageGapTotals.stop + pageGapTotals.missing }}</b>
                </div>
                <i class="hs-sub">
                  停牌 {{ pageGapTotals.stop }} · 未发布 {{ pageGapTotals.missing }}
                  （已算 {{ pageGapTotals.loaded }} 条）
                </i>
              </div>
              <div class="health-stat">
                <div class="hs-head">
                  <span class="hs-label">当前页覆盖</span>
                  <b>{{ pageGapTotals.covered }}/{{ pageGapTotals.expected }}</b>
                </div>
                <i class="hs-sub">非交易日按交易日历剔除，不计缺口</i>
              </div>
            </div>

            <p v-if="!ledgerRecords.length" style="color:var(--ink-soft);font-size:13px">
              暂无台账记录。台账写入口默认关闭，开启与否由配置
              <code>backtest.kline_ledger.enabled</code> 控制；此处查看不会启用台账。
            </p>
            <template v-else>
              <table class="table">
                <thead>
                  <tr>
                    <th>代码</th><th>周期</th><th>字段</th><th>覆盖段</th>
                    <th>已覆盖/应有</th><th>停牌</th><th>未发布</th>
                    <th>指纹</th><th>最近更新</th>
                  </tr>
                </thead>
                <tbody>
                  <template v-for="r in pagedRecords" :key="recordKey(r)">
                    <tr style="cursor:pointer" @click="toggleRecord(r)">
                      <td>{{ r.code }}</td>
                      <td>{{ r.period }}</td>
                      <td>{{ r.field }}</td>
                      <td>{{ r.ranges.length }} 段</td>
                      <td>{{ coverageCell(r) }}</td>
                      <td>{{ gapCell(r, 'stop') }}</td>
                      <td>{{ gapCell(r, 'missing') }}</td>
                      <td>
                        <span class="badge" :class="r.stamp_valid ? 'badge-green' : 'badge-red'">
                          {{ r.stamp_valid ? '有效' : (r.stamp ? '失效' : '无') }}
                        </span>
                      </td>
                      <td>{{ fmtTime(r.updated_at) }}</td>
                    </tr>
                    <tr v-if="expandedKey === recordKey(r)">
                      <td colspan="9" style="background:var(--bg-soft, #fafafa)">
                        <div style="font-size:12px;line-height:1.7">
                          <div><b>覆盖区间：</b>{{ rangesText(r) }}</div>
                          <div><b>停牌（数据源确认无行情）：</b>
                            {{ daysText(planOf(r)?.empty_days) }}
                          </div>
                          <div><b>未发布/未下载：</b>
                            {{ daysText(planOf(r)?.missing_days) }}
                          </div>
                          <div><b>可信判定：</b>{{ trustText(r) }}</div>
                        </div>
                      </td>
                    </tr>
                  </template>
                </tbody>
              </table>
              <div style="display:flex;align-items:center;gap:8px;margin-top:10px;font-size:12px">
                <button class="btn btn-sm" :disabled="ledgerPage <= 1"
                        @click="changeLedgerPage(-1)">上一页</button>
                <span>第 {{ ledgerPage }} / {{ ledgerTotalPages }} 页 · 共 {{ ledgerRecords.length }} 条</span>
                <button class="btn btn-sm" :disabled="ledgerPage >= ledgerTotalPages"
                        @click="changeLedgerPage(1)">下一页</button>
              </div>
            </template>
          </template>
        </template>

        <!-- 指纹对比 -->
        <template v-else>
          <p v-if="fpLoading">回测记录加载中…</p>
          <template v-else>
            <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:flex-end">
              <label style="font-size:13px">记录 A
                <select v-model.number="fpLeftId" class="input" style="min-width:260px">
                  <option v-for="r in fpRecords" :key="r.id" :value="r.id">
                    #{{ r.id }} {{ r.name }}（{{ r.start_date }} ~ {{ r.end_date }}）
                  </option>
                </select>
              </label>
              <label style="font-size:13px">记录 B
                <select v-model.number="fpRightId" class="input" style="min-width:260px">
                  <option v-for="r in fpRecords" :key="r.id" :value="r.id">
                    #{{ r.id }} {{ r.name }}（{{ r.start_date }} ~ {{ r.end_date }}）
                  </option>
                </select>
              </label>
              <button class="btn btn-sm" :disabled="fpComparing" @click="compareFingerprints">
                {{ fpComparing ? '比对中…' : '比对指纹' }}
              </button>
            </div>

            <template v-if="fpLeft || fpRight">
              <p style="font-size:12px;margin:10px 0 6px">
                A: {{ fpLeftLabel }} · window
                {{ fpLeft?.window ? `${fpLeft.window.start} ~ ${fpLeft.window.end}` : '—' }}
                · hash {{ fpLeft?.hash || '—' }}<br>
                B: {{ fpRightLabel }} · window
                {{ fpRight?.window ? `${fpRight.window.start} ~ ${fpRight.window.end}` : '—' }}
                · hash {{ fpRight?.hash || '—' }}
              </p>
              <p v-if="!fpRows.length" style="font-size:13px;color:var(--ink-soft)">
                无可比对的逐票指纹（记录可能为老版本或无台账快照）。
              </p>
              <table v-else class="table">
                <thead>
                  <tr><th>代码 / 周期</th><th>记录 A</th><th>记录 B</th><th>结论</th></tr>
                </thead>
                <tbody>
                  <tr v-for="row in fpRows" :key="row.key">
                    <td>{{ row.key }}</td>
                    <td>{{ row.left }}</td>
                    <td>{{ row.right }}</td>
                    <td>
                      <span class="badge"
                            :class="row.status === 'same' ? 'badge-green'
                              : row.status === 'diff' ? 'badge-red' : 'badge-gray'">
                        {{ row.status === 'same' ? '一致'
                          : row.status === 'diff' ? '不一致'
                          : row.status === 'left_only' ? '仅 A 有' : '仅 B 有' }}
                      </span>
                    </td>
                  </tr>
                </tbody>
              </table>
            </template>
            <p v-else style="font-size:13px;color:var(--ink-soft)">
              选择两条回测记录后点「比对指纹」：按代码+周期对比首根/最后一根/行数。
              老记录无指纹时显示为空。
            </p>
          </template>
        </template>
      </div>
      <div class="modal-actions">
        <button class="btn" @click="showLedger = false">关闭</button>
      </div>
    </div>
  </div>
</template>

<style scoped>
.dash-hero {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 20px;
  padding: 4px 2px 20px;
  margin-bottom: 18px;
  border-bottom: 1px solid var(--line);
}
.dash-hero-stats { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 22px; font-size: 12px; color: var(--ink-soft) }
.dash-hero-stats b { font-family: var(--font-serif); font-size: 18px; font-weight: 800; color: var(--ink); margin-right: 5px; font-variant-numeric: tabular-nums }
.dash-hero-stats .db-accent { color: var(--red) }
.dash-hero-actions { flex-shrink: 0 }
/* 本页密度：frame 区块与状态条整体收紧，不依赖全局 style.css */
.frame { margin-bottom: 10px }
.frame-head { padding: 6px 12px }
.frame-body { padding: 8px 12px }
/* 01 区单行数字条：主值一行 + 副信息小字（对齐首页状态总览 01 的形态） */
.health-stats { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px 0 }
/* 05 数据与口径观测：5 项单行（比 01 少一格，单独收列宽） */
.obs-stats { grid-template-columns: repeat(5, minmax(0, 1fr)) }
.obs-error { font-size: 12px; color: var(--red) }
.health-stat { display: flex; flex-direction: column; gap: 1px; min-width: 0; padding: 0 12px; border-left: 1px solid var(--line) }
.health-stat:first-child { padding-left: 0; border-left: none }
.hs-head { display: flex; align-items: baseline; gap: 6px; min-width: 0 }
.hs-label { font-size: 11.5px; color: var(--ink-soft); white-space: nowrap; flex: 0 0 auto }
.health-stat > b { min-width: 0; font-family: var(--font-serif); font-size: 16px; font-weight: 800; color: var(--ink); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-variant-numeric: tabular-nums }
.health-stat > i { font-style: normal; font-size: 11px; color: var(--ink-faint); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; font-variant-numeric: tabular-nums }
/* 10 列与 10 个 td 一一对应：固定 table-layout 后列宽只由 colgroup 决定，
   窄屏改为表格内部横向滚动，不会因自动布局挤压而让单元格内容跑位。
   数据新鲜度列容纳「徽标 + 逐周期明细」，仍全部落在同一个 td 内。 */
.health-table { font-size: 13px; table-layout: fixed; min-width: 920px }
.health-table .c-name { width: 8% }
.health-table .c-status { width: 10% }
.health-table .c-engine { width: 12% }
.health-table .c-channel { width: 8% }
.health-table .c-bridge { width: 5% }
.health-table .c-data { width: 18% }
.health-table .c-today { width: 15% }
.health-table .c-alerts { width: 9% }
.health-table .c-deferred { width: 7% }
.health-table .c-approval { width: 8% }
.health-table th, .health-table td { padding: 4px 10px }
.health-table td { vertical-align: top }
.health-table td.num { white-space: nowrap }
.problem-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 4px }
.problem-list li { display: flex; align-items: flex-start; gap: 8px; font-size: 13px; line-height: 1.4 }
.num { font-variant-numeric: tabular-nums; text-align: right }
.period-line { font-size: 11px; color: var(--ink-soft); line-height: 1.5; margin-top: 3px; font-variant-numeric: tabular-nums }
.guard-block { padding: 6px 0; border-top: 1px dashed var(--line) }
.guard-block:first-child { padding-top: 0; border-top: none }
.guard-head { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; font-size: 13px }
.guard-list { list-style: none; margin: 0; padding: 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 3px 14px }
.guard-list li { display: flex; align-items: baseline; gap: 8px; font-size: 12px; line-height: 1.4 }
.guard-list li.bad .guard-label { color: var(--red) }
.guard-label { font-weight: 600; flex-shrink: 0 }
.guard-detail { color: var(--ink-soft); overflow-wrap: anywhere }
.alert-link { color: var(--accent); text-decoration: underline dotted; text-underline-offset: 2px }
.alert-link:hover { text-decoration: underline }
@media (max-width: 1100px) {
  .dash-hero { flex-wrap: wrap; align-items: center; row-gap: 10px; padding-bottom: 12px; margin-bottom: 14px; }
  /* 01 数字条：6 列 -> 3 列（两行），保持「主值 + 副信息」两行结构 */
  .health-stats { grid-template-columns: repeat(3, minmax(0, 1fr)); row-gap: 10px }
  .health-stat { padding-left: 12px; border-left: 1px solid var(--line) }
  .health-stat:nth-child(3n + 1) { padding-left: 0; border-left: none }
}
@media (max-width: 760px) {
  /* 01 数字条：3 列 -> 2 列 */
  .health-stats { grid-template-columns: repeat(2, minmax(0, 1fr)) }
  .health-stat { padding-left: 12px; border-left: 1px solid var(--line) }
  .health-stat:nth-child(2n + 1) { padding-left: 0; border-left: none }
}
</style>
