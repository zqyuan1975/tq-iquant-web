<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { api } from '../api'

interface PoolCoverage {
  pool_id: number
  pool_name: string
  total: number
  covered: number
  no_events: number
  failed: number
  missing: number
}

interface CoverageTotals {
  total: number
  covered: number
  no_events: number
  failed: number
  missing: number
}

interface HoldingsCoverage extends CoverageTotals {
  codes: string[]
}

interface SyncResult {
  requested: number
  covered: string[]
  no_events: string[]
  failed: string[]
  added: number
  updated: number
  rights: number
}

interface SyncState {
  last_run_at?: string
  last_success_date?: string
  trigger?: string
  status?: string
  result?: {
    requested: number
    covered: number
    no_events: number
    failed: string[]
    added: number
    updated: number
    rights: number
  }
  message?: string
}

interface CoverageResp {
  pools: PoolCoverage[]
  holdings: HoldingsCoverage
  totals: CoverageTotals
  last_sync: SyncState
  scheduler: {
    enabled: boolean
    daily_at: string
    p2_allowed: boolean | null
    next_p2_open_at: string | null
  }
  generated_at: string
}

const coverage = ref<CoverageResp | null>(null)
const loading = ref(false)
const syncing = ref(false)
const errorMsg = ref('')
const notice = ref('')
/** 'all' = 全部池；数字 = 单个池 */
const scope = ref<'all' | number>('all')
const lastResult = ref<SyncResult | null>(null)

const scopeOptions = computed(() => coverage.value?.pools ?? [])

function fmtTime(raw?: string | null): string {
  if (!raw) return '—'
  return raw.replace('T', ' ').slice(0, 19)
}

function statusBadge(status?: string): string {
  if (status === 'success' || status === 'skipped') return 'badge-green'
  if (status === 'failed') return 'badge-red'
  return 'badge-gray'
}

function triggerText(trigger?: string): string {
  if (trigger === 'scheduled') return '定时'
  if (trigger === 'manual') return '手工'
  if (trigger === 'cli') return '命令行'
  return trigger || '—'
}

async function loadCoverage() {
  loading.value = true
  errorMsg.value = ''
  try {
    const res = await api.get('/corporate-actions/coverage')
    coverage.value = res.data.data as CoverageResp
  } catch (e) {
    errorMsg.value = e instanceof Error ? e.message : '覆盖状态加载失败'
  } finally {
    loading.value = false
  }
}

async function syncMissing() {
  if (syncing.value) return
  syncing.value = true
  errorMsg.value = ''
  notice.value = ''
  try {
    const payload: Record<string, unknown> = { only_missing: true }
    if (scope.value === 'all') {
      const ids = (coverage.value?.pools ?? []).map((p) => p.pool_id)
      if (ids.length) payload.pool_ids = ids
    } else {
      payload.pool_ids = [scope.value]
    }
    const res = await api.post('/corporate-actions/sync', payload)
    const data = res.data.data as SyncResult & { skipped?: boolean; message?: string }
    lastResult.value = data
    notice.value = data.skipped
      ? (data.message || '当前无缺失代码')
      : `同步完成：新增 ${data.added} 条、更新 ${data.updated} 条、失败 ${data.failed.length} 只`
    await loadCoverage()
  } catch (e) {
    errorMsg.value = e instanceof Error ? e.message : '同步失败'
  } finally {
    syncing.value = false
  }
}

onMounted(loadCoverage)
</script>

<template>
  <div class="page-head">
    <div class="page-head-left">
      <span class="page-head-crumb">系统 / 公司行为</span>
      <span class="page-head-title">公司行为同步</span>
    </div>
    <div class="page-head-actions">
      <button class="btn" :disabled="loading || syncing" @click="loadCoverage">
        {{ loading ? '刷新中…' : '刷新状态' }}
      </button>
    </div>
  </div>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:16px">
    {{ errorMsg }}
  </div>
  <div v-if="notice" class="card" style="padding:12px;color:var(--green);margin-bottom:16px">
    {{ notice }}
  </div>

  <div class="frame">
    <div class="frame-head">
      <span class="frame-num">01</span>
      <span class="frame-title">覆盖状态</span>
      <span class="frame-caption" v-if="coverage">
        统计时间 {{ fmtTime(coverage.generated_at) }}
      </span>
    </div>
    <div class="frame-body">
      <div v-if="loading && !coverage">加载中…</div>
      <div v-else-if="!coverage || !coverage.pools.length" class="empty-state">
        暂无股票池数据
      </div>
      <div v-else class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>股票池</th>
              <th>总数</th>
              <th>covered</th>
              <th>no_events</th>
              <th>failed</th>
              <th>missing</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="p in coverage.pools" :key="p.pool_id">
              <td>{{ p.pool_name }} <span class="ca-dim">#{{ p.pool_id }}</span></td>
              <td>{{ p.total }}</td>
              <td>{{ p.covered }}</td>
              <td>{{ p.no_events }}</td>
              <td>
                <span class="badge" :class="p.failed ? 'badge-red' : 'badge-gray'">
                  {{ p.failed }}
                </span>
              </td>
              <td>
                <span class="badge" :class="p.missing ? 'badge-orange' : 'badge-green'">
                  {{ p.missing }}
                </span>
              </td>
            </tr>
            <tr v-if="coverage.holdings.codes.length">
              <td>池外持仓 <span class="ca-dim">{{ coverage.holdings.codes.length }} 只</span></td>
              <td>{{ coverage.holdings.total }}</td>
              <td>{{ coverage.holdings.covered }}</td>
              <td>{{ coverage.holdings.no_events }}</td>
              <td>
                <span class="badge" :class="coverage.holdings.failed ? 'badge-red' : 'badge-gray'">
                  {{ coverage.holdings.failed }}
                </span>
              </td>
              <td>
                <span class="badge" :class="coverage.holdings.missing ? 'badge-orange' : 'badge-green'">
                  {{ coverage.holdings.missing }}
                </span>
              </td>
            </tr>
            <tr>
              <td><b>合计</b></td>
              <td><b>{{ coverage.totals.total }}</b></td>
              <td><b>{{ coverage.totals.covered }}</b></td>
              <td><b>{{ coverage.totals.no_events }}</b></td>
              <td><b>{{ coverage.totals.failed }}</b></td>
              <td>
                <b class="badge" :class="coverage.totals.missing ? 'badge-orange' : 'badge-green'">
                  {{ coverage.totals.missing }}
                </b>
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div class="ca-toolbar">
        <label class="ca-label">同步范围</label>
        <select v-model="scope" class="field-select" :disabled="syncing">
          <option value="all">全部池（只补缺失）</option>
          <option v-for="p in scopeOptions" :key="p.pool_id" :value="p.pool_id">
            {{ p.pool_name }}（missing {{ p.missing }}）
          </option>
        </select>
        <button class="btn btn-primary" :disabled="syncing || loading" @click="syncMissing">
          {{ syncing ? '同步中…' : '同步公司行为' }}
        </button>
        <span class="ca-hint">
          幂等：已覆盖的票自动跳过；单票失败记 failed，下轮自动重试。
        </span>
      </div>

      <div class="ca-scheduler" v-if="coverage">
        自动同步：{{ coverage.scheduler.enabled ? '已启用' : '已关闭' }}
        · 交易日 {{ coverage.scheduler.daily_at }} 后
        · TQ 闸 P2
        {{ coverage.scheduler.p2_allowed === true ? '放行中' : coverage.scheduler.p2_allowed === false ? '未放行' : '状态未知' }}
        <template v-if="coverage.scheduler.next_p2_open_at">
          （下次可用 {{ fmtTime(coverage.scheduler.next_p2_open_at) }}）
        </template>
      </div>
    </div>
  </div>

  <div class="frame">
    <div class="frame-head">
      <span class="frame-num">02</span>
      <span class="frame-title">最近一次同步</span>
    </div>
    <div class="frame-body">
      <div v-if="!coverage || !coverage.last_sync || !coverage.last_sync.last_run_at" class="empty-state">
        暂无同步记录
      </div>
      <template v-else>
        <div class="ca-summary">
          <span>
            <span class="ca-dim">时间</span>
            {{ fmtTime(coverage.last_sync.last_run_at) }}
          </span>
          <span>
            <span class="ca-dim">触发</span>
            {{ triggerText(coverage.last_sync.trigger) }}
          </span>
          <span>
            <span class="ca-dim">结果</span>
            <span class="badge" :class="statusBadge(coverage.last_sync.status)">
              {{ coverage.last_sync.status || '—' }}
            </span>
          </span>
          <template v-if="coverage.last_sync.result">
            <span><span class="ca-dim">requested</span> {{ coverage.last_sync.result.requested }}</span>
            <span><span class="ca-dim">covered</span> {{ coverage.last_sync.result.covered }}</span>
            <span><span class="ca-dim">no_events</span> {{ coverage.last_sync.result.no_events }}</span>
            <span><span class="ca-dim">failed</span> {{ coverage.last_sync.result.failed.length }}</span>
            <span><span class="ca-dim">added</span> {{ coverage.last_sync.result.added }}</span>
            <span><span class="ca-dim">updated</span> {{ coverage.last_sync.result.updated }}</span>
            <span><span class="ca-dim">rights</span> {{ coverage.last_sync.result.rights }}</span>
          </template>
        </div>
        <div v-if="coverage.last_sync.result?.failed.length" class="ca-failed">
          失败代码（{{ coverage.last_sync.result.failed.length }}）：
          <code>{{ coverage.last_sync.result.failed.slice(0, 30).join(', ') }}</code>
          <template v-if="coverage.last_sync.result.failed.length > 30">…</template>
        </div>
        <div v-if="lastResult" class="ca-scheduler">
          本次结果：requested {{ lastResult.requested }} · added {{ lastResult.added }}
          · updated {{ lastResult.updated }} · failed {{ lastResult.failed.length }}
        </div>
      </template>
    </div>
  </div>
</template>

<style scoped>
.ca-dim {
  color: var(--ink-faint);
  font-size: 11px;
  letter-spacing: 0.04em;
}
.ca-toolbar {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 10px;
  margin-top: 16px;
  padding-top: 14px;
  border-top: 1px solid var(--line);
}
.ca-label {
  font-size: 12px;
  color: var(--ink-soft);
}
.ca-hint {
  font-size: 11px;
  color: var(--ink-faint);
}
.ca-scheduler {
  margin-top: 10px;
  font-size: 12px;
  color: var(--ink-soft);
}
.ca-summary {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 6px 18px;
  font-size: 13px;
}
.ca-failed {
  margin-top: 10px;
  font-size: 12px;
  color: var(--red);
  word-break: break-all;
}
</style>
