<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import {
  getIncrementalSummary, getLiveSessions, getPortfolios, runIncrementalValidation,
  rebuildIncremental,
  type IncrementalSummary, type LiveSessionItem, type PortfolioItem,
} from '../api'
import PageHeader from '../components/PageHeader.vue'

const loading = ref(false)
const validating = ref(false)
const rebuilding = ref(false)
const errorMsg = ref('')
const sessions = ref<LiveSessionItem[]>([])
const portfolios = ref<PortfolioItem[]>([])
const sessionId = ref<number | null>(null)
const portfolioId = ref<number | null>(null)
const summary = ref<IncrementalSummary | null>(null)

const portfolioName = computed(() => {
  const map = new Map(portfolios.value.map(p => [p.id, p.name]))
  return (id: number) => map.get(id) || `#${id}`
})

const availablePortfolioIds = computed(() => {
  const s = sessions.value.find(x => x.id === sessionId.value)
  return s ? s.portfolio_ids || [] : []
})

function fmtNum(v: string | number | null | undefined): string {
  if (v === null || v === undefined || v === '') return '-'
  const n = Number(v)
  if (!Number.isFinite(n)) return '-'
  return n.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function fmtPct(v: number | null | undefined): string {
  if (v === null || v === undefined) return '-'
  return `${v.toFixed(2)}%`
}

function rowClass(row: { status: string }): string {
  if (row.status === 'gap') return 'row-gap'
  if (row.status === 'warn') return 'row-warn'
  return ''
}

function tagLabel(tag: string): string {
  const map: Record<string, string> = {
    gap: '断档',
    net_dev: '净值偏差',
    trade_mismatch: '笔数不符',
    direction_mismatch: '方向不符',
    validation_fail: '校验超容差',
  }
  return map[tag] || tag
}

async function loadSummary() {
  if (!sessionId.value || !portfolioId.value) return
  loading.value = true
  errorMsg.value = ''
  try {
    summary.value = await getIncrementalSummary(sessionId.value, portfolioId.value)
  } catch (e: any) {
    summary.value = null
    errorMsg.value = e?.message || '加载失败'
  } finally {
    loading.value = false
  }
}

async function onValidate() {
  if (!sessionId.value || !portfolioId.value) return
  validating.value = true
  errorMsg.value = ''
  try {
    await runIncrementalValidation(sessionId.value, portfolioId.value)
    await loadSummary()
  } catch (e: any) {
    errorMsg.value = e?.message || '双跑校验失败'
  } finally {
    validating.value = false
  }
}

// K3：一键重建接缝（丢弃 checkpoint，从接缝日全量重建）
async function onRebuild() {
  if (!sessionId.value || !portfolioId.value) return
  if (!confirm('确定重建接缝？将丢弃当前 checkpoint 并从接缝日全量重算（耗时较长）。')) return
  rebuilding.value = true
  errorMsg.value = ''
  try {
    await rebuildIncremental(sessionId.value, portfolioId.value)
    await loadSummary()
  } catch (e: any) {
    errorMsg.value = e?.message || '重建接缝失败'
  } finally {
    rebuilding.value = false
  }
}

async function onSessionChange() {
  portfolioId.value = availablePortfolioIds.value[0] ?? null
  summary.value = null
  await loadSummary()
}

async function onPortfolioChange() {
  summary.value = null
  await loadSummary()
}

onMounted(async () => {
  const [ss, ps] = await Promise.all([
    getLiveSessions().catch(() => [] as LiveSessionItem[]),
    getPortfolios().catch(() => [] as PortfolioItem[]),
  ])
  sessions.value = ss
  portfolios.value = ps
  if (ss.length > 0) {
    sessionId.value = ss[0].id
    await onSessionChange()
  }
})
</script>

<template>
  <div>
    <PageHeader title="增量回测">
      <button class="btn" :disabled="validating || !summary?.supported" @click="onValidate">
        {{ validating ? '校验中…' : '立即双跑校验' }}
      </button>
      <button class="btn" style="margin-left:6px" @click="loadSummary">刷新</button>
    </PageHeader>

    <div class="card" style="padding:12px; margin-bottom:12px; display:flex; gap:16px; flex-wrap:wrap; align-items:center">
      <label>
        实盘会话：
        <select class="field-select" v-model.number="sessionId" @change="onSessionChange">
          <option v-for="s in sessions" :key="s.id" :value="s.id">{{ s.name }}（{{ s.status }}）</option>
        </select>
      </label>
      <label>
        组合策略：
        <select class="field-select" v-model.number="portfolioId" @change="onPortfolioChange">
          <option v-for="id in availablePortfolioIds" :key="id" :value="id">{{ portfolioName(id) }}</option>
        </select>
      </label>
      <span v-if="summary" class="muted">
        接缝日 {{ summary.seam_date || '未建立' }} ·
        截止 {{ summary.checkpoint?.as_of_date || '-' }} ·
        口径版本 v{{ summary.checkpoint?.schema_version ?? '-' }}
      </span>
    </div>

    <div v-if="errorMsg" class="card" style="padding:12px; color:var(--red)">{{ errorMsg }}</div>

    <div v-if="summary && !summary.supported" class="card" style="padding:12px; color:var(--warn)">
      {{ summary.reason || '该组合策略暂不支持续跑' }}
    </div>

    <div v-if="summary?.last_invalidation" class="card" data-test="invalidation-banner"
         style="padding:12px; margin-bottom:12px; border-left:4px solid var(--red); color:var(--red)">
      <strong>接缝判废：</strong>
      <span>{{ summary.last_invalidation.message || '参数/口径变化，checkpoint 已作废' }}</span>
      <span v-if="summary.last_invalidation.at" class="muted" style="margin-left:8px">
        {{ summary.last_invalidation.at }}
      </span>
      <button class="btn" style="margin-left:12px" :disabled="rebuilding" @click="onRebuild">
        {{ rebuilding ? '重建中…' : '立即重建接缝' }}
      </button>
    </div>

    <div v-if="summary?.latest_validation" class="card" style="padding:12px; margin-bottom:12px">
      <strong>最近一次双跑校验：</strong>
      <span :style="{ color: summary.latest_validation.status === 'pass' ? '#2f9e44' : '#e5484d' }">
        {{ summary.latest_validation.status === 'pass' ? '通过' : (summary.latest_validation.status === 'fail' ? '未通过' : '异常') }}
      </span>
      <span class="muted" style="margin-left:12px">
        {{ summary.latest_validation.from_date }} ~ {{ summary.latest_validation.to_date }} ·
        净值偏差 max {{ fmtPct(summary.latest_validation.max_net_dev_pct) }} /
        mean {{ fmtPct(summary.latest_validation.mean_net_dev_pct) }} ·
        笔数一致率 {{ fmtPct((summary.latest_validation.trade_count_match_ratio ?? 0) * 100) }} ·
        方向一致率 {{ fmtPct((summary.latest_validation.direction_match_ratio ?? 0) * 100) }} ·
        回撤偏差 {{ fmtPct(summary.latest_validation.max_drawdown_dev_pct) }}
        <template v-if="summary.latest_validation.first_divergence_date">
          · 首处偏差 {{ summary.latest_validation.first_divergence_date }}
        </template>
      </span>
    </div>

    <div class="card table-wrap">
      <table>
        <thead>
          <tr>
            <th>日期</th>
            <th>续跑净值</th>
            <th>实盘净值</th>
            <th>偏差</th>
            <th>成交笔数（续跑/实盘）</th>
            <th>方向</th>
            <th>标签</th>
          </tr>
        </thead>
        <tbody>
          <tr v-if="loading"><td colspan="7">加载中…</td></tr>
          <tr v-else-if="!summary || summary.days.length === 0">
            <td colspan="7" class="muted">暂无对照数据（需实盘启动建立接缝日后，每日 15:10 续跑生成）</td>
          </tr>
          <tr v-for="row in summary?.days || []" :key="row.trade_date" :class="rowClass(row)">
            <td>{{ row.trade_date }}</td>
            <td>{{ fmtNum(row.inc_total_value) }}</td>
            <td>{{ fmtNum(row.live_total_value) }}</td>
            <td :style="{ color: (row.net_dev_pct ?? 0) > (summary?.config.tolerances.max_net_dev_pct ?? 0.5) ? '#e5484d' : undefined }">
              {{ fmtPct(row.net_dev_pct) }}
            </td>
            <td>{{ row.inc_trades_count ?? '-' }} / {{ row.live_trades_count ?? '-' }}</td>
            <td>{{ row.direction_match === null ? '-' : (row.direction_match ? '一致' : '不一致') }}</td>
            <td>
              <span v-for="tag in row.tags" :key="tag" class="tag">{{ tagLabel(tag) }}</span>
              <span v-if="row.tags.length === 0" class="muted">正常</span>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
/* 顶部会话/组合下拉：与站点其它下拉同一规格，标签与控件垂直居中 */
.card > label { display: inline-flex; align-items: center; gap: 6px; }
.card > label .field-select { min-width: 200px; }
@media (max-width: 640px) {
  .card > label { flex: 1 1 100%; }
  .card > label .field-select { min-width: 0; flex: 1; }
}
.table-wrap { padding: 0; overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid var(--border-color, #eee); white-space: nowrap; }
th { font-weight: 600; }
.muted { color: var(--text-muted, #888); }
.row-gap { background: rgba(229, 72, 77, 0.10); }
.row-warn { background: rgba(229, 72, 77, 0.05); }
.tag { display: inline-block; padding: 1px 6px; margin-right: 4px; border-radius: 4px; background: var(--red-wash); color: var(--red); font-size: 12px; }
</style>
