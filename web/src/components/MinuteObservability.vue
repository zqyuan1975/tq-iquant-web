<template>
  <div class="minute-obs" data-test="minute-obs">
    <template v-if="hasAny">
      <span class="minute-obs-label">取数路由</span>
      <template v-if="routeTotal > 0">
        <span class="badge badge-gray" data-test="route-store">自有存储 {{ route.store }}</span>
        <span class="badge badge-gray" data-test="route-inner">原生/快照 {{ route.inner }}</span>
        <span style="color:var(--ink-soft)">
          本轮请求 {{ routeTotal }}（存储占比 {{ storePct }}%）
        </span>
      </template>
      <span v-else style="color:var(--ink-soft)">本轮暂无取数采样</span>

      <span class="minute-obs-label">下载线程</span>
      <template v-if="downloadEnabled">
        <span class="badge" :class="threadClass" data-test="thread-badge">{{ threadText }}</span>
        <span style="color:var(--ink-soft)" data-test="thread-age">{{ ageText }}</span>
        <span v-if="download.thread_error" class="minute-obs-error" data-test="thread-error">
          {{ download.thread_error }}
        </span>
        <span v-if="gaveUp" class="badge badge-red" data-test="gave-up">
          重建已放弃（{{ attempts }} 次）
        </span>
      </template>
      <span v-else-if="downloadKnown" style="color:var(--ink-soft)">内置下载器未启用</span>
      <span v-else style="color:var(--ink-soft)">下载线程状态不可用</span>

      <span class="minute-obs-label">覆盖缺口</span>
      <template v-if="outside !== null">
        <button
          class="btn btn-sm"
          :disabled="!codes.length"
          data-test="coverage-toggle"
          @click="showCodes = !showCodes"
        >池外 {{ outside }} 只{{ codes.length ? '（看样例）' : '' }}</button>
        <span v-if="showCodes && codes.length" class="minute-obs-codes" data-test="coverage-codes">
          {{ codes.join('、') }}
        </span>
      </template>
      <span v-else style="color:var(--ink-soft)">暂无覆盖采样</span>

      <span class="minute-obs-label">保护窗兜底</span>
      <template v-if="fallbackEnabled">
        <span
          class="badge"
          :class="fallbackTriggered ? 'badge-red' : 'badge-gray'"
          data-test="fallback-status"
        >{{ fallbackTriggered ? '已触发' : '未触发' }}</span>
        <template v-if="fallbackTriggered">
          <span class="badge badge-gray" data-test="fallback-reason">{{ fallbackReasonText }}</span>
          <span style="color:var(--ink-soft)" data-test="fallback-detail">
            库 lag {{ fallbackLagText }} · 窗口 {{ fallback?.window_state || '—' }}
            · 最近 {{ fallback?.last_trigger_at || '—' }}
          </span>
        </template>
        <span v-else style="color:var(--ink-soft)" data-test="fallback-window">
          允许 {{ fallbackWindowText }}
        </span>
      </template>
      <span v-else-if="fallbackKnown" style="color:var(--ink-soft)" data-test="fallback-disabled">
        未启用
      </span>
      <span v-else style="color:var(--ink-soft)">状态不可用</span>

      <span class="minute-obs-label">限频利用率</span>
      <template v-if="utilKnown">
        <span class="badge" :class="utilClass" data-test="util-rps">{{ utilRpsText }}</span>
        <span class="badge badge-gray" data-test="util-round">{{ utilRoundText }}</span>
        <span style="color:var(--ink-soft)" data-test="util-streak">
          连续顶格 {{ utilization?.high_streak ?? 0 }} · 告警 {{ utilization?.alerts ?? 0 }}
        </span>
        <span class="badge" :class="raiseClass" data-test="util-raise">{{ raiseText }}</span>
      </template>
      <span v-else style="color:var(--ink-soft)" data-test="util-na">暂无采样</span>

      <span class="minute-obs-label">规模建议</span>
      <button
        v-if="sizingRows.length"
        class="btn btn-sm"
        data-test="sizing-toggle"
        @click="showSizing = !showSizing"
      >{{ showSizing ? '收起' : `建议 ${sizingRows.length} 档` }}</button>
      <span v-else style="color:var(--ink-soft)">不可用</span>
      <div v-if="showSizing && sizingRows.length" class="minute-obs-table-wrap" data-test="sizing-table">
        <table class="minute-obs-table">
          <thead>
            <tr>
              <th>池规模</th><th>连接</th><th>rps</th><th>单主站</th><th>备注</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="row in sizingRows"
              :key="row.tier_index"
              :class="{ current: row.tier_index === currentTier }"
            >
              <td>{{ row.tier }}</td>
              <td>{{ row.connections }}</td>
              <td>{{ row.max_req_per_second }}</td>
              <td>{{ row.single_host_sustainable ? '可续' : '需分片' }}</td>
              <td>{{ row.note }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </template>
    <span v-else style="color:var(--ink-soft)" data-test="minute-obs-empty">
      暂无分钟下载观测数据（会话启动后可见）
    </span>
  </div>
</template>

<script setup lang="ts">
// 分钟下载观测（只读展示）：取数路由 / 下载线程 / 覆盖缺口。
// 口径是观测信息，不是故障：只有线程已死或监督 gave_up 才用警示色。
import { computed, ref } from 'vue'

interface RouteLike { requests?: number; store?: number; inner?: number }
interface CoverageLike {
  outside_pool?: number
  outside_codes?: string[]
  pool_known?: boolean
}
interface SupervisionLike { attempts_in_window?: number; gave_up?: boolean }
interface FallbackLike {
  enabled?: boolean
  triggered?: boolean
  reason?: string
  store_lag_minutes?: number | null
  window_state?: string
  last_trigger_at?: string | null
  window?: { start?: string; end?: string; state?: string }
}
interface UtilizationLike {
  actual_rps?: number | null
  rps_cap?: number | null
  rps_ratio?: number | null
  round_seconds?: number | null
  interval_seconds?: number | null
  elapsed_ratio?: number | null
  high_streak?: number
  saturated?: boolean
  alerts?: number
  last_alert_at?: string | null
  last_alert_reason?: string
  auto_raised_tiers?: number
  max_raise_tiers?: number
  raise_exhausted?: boolean
  explicit_override?: boolean
}
interface SizingRowLike {
  tier_index?: number
  tier?: string
  connections?: number
  max_req_per_second?: number
  single_host_sustainable?: boolean
  note?: string
}
interface SizingLike {
  tier_index?: number | null
  tier?: string
  table?: SizingRowLike[]
}
interface DownloadLike {
  enabled?: boolean
  alive?: boolean
  stalled?: boolean
  last_round_age_seconds?: number | null
  thread_error?: string | null
  supervision?: SupervisionLike | null
  fallback?: FallbackLike | null
  utilization?: UtilizationLike | null
  sizing?: SizingLike | null
}

const props = defineProps<{
  route?: RouteLike | null
  coverage?: CoverageLike | null
  download?: DownloadLike | null
}>()

const showCodes = ref(false)
const showSizing = ref(false)

const route = computed(() => props.route || {})
const routeTotal = computed(() => {
  const n = Number(route.value.requests)
  return Number.isFinite(n) && n > 0 ? n : 0
})
const storePct = computed(() => {
  if (routeTotal.value <= 0) return 0
  return Math.round((Number(route.value.store) || 0) / routeTotal.value * 100)
})

const download = computed<DownloadLike>(() => props.download || {})
const downloadKnown = computed(() => props.download != null)
const downloadEnabled = computed(() => download.value.enabled === true)
const gaveUp = computed(() => download.value.supervision?.gave_up === true)
const attempts = computed(() => Number(download.value.supervision?.attempts_in_window) || 0)
const dead = computed(
  () => downloadEnabled.value
    && (download.value.alive === false || !!download.value.thread_error),
)
// 字段缺失（alive 既非 true 也非 false）按「未知」处理，不误报红色。
const unknown = computed(
  () => downloadEnabled.value
    && download.value.alive !== true && download.value.alive !== false
    && !download.value.thread_error,
)
const stalled = computed(
  () => downloadEnabled.value && download.value.alive === true && download.value.stalled === true,
)
const threadText = computed(() => {
  if (dead.value) return '已停止'
  if (unknown.value) return '状态未知'
  if (stalled.value) return '停滞'
  return '存活'
})
// 只有「线程已死 / 监督放弃重建」用警示色；停滞与未知仍是观测项，用中性灰。
const threadClass = computed(() => {
  if (dead.value || gaveUp.value) return 'badge-red'
  if (stalled.value || unknown.value) return 'badge-gray'
  return 'badge-green'
})
const ageText = computed(() => {
  const age = download.value.last_round_age_seconds
  if (age === null || age === undefined || !Number.isFinite(Number(age))) {
    return '尚未完成一轮'
  }
  const n = Math.max(0, Math.round(Number(age)))
  if (n < 60) return `最近一轮 ${n} 秒前`
  return `最近一轮 ${Math.floor(n / 60)} 分 ${n % 60} 秒前`
})

const outside = computed(() => {
  const n = props.coverage?.outside_pool
  return typeof n === 'number' && Number.isFinite(n) ? n : null
})
const codes = computed(() => {
  const list = props.coverage?.outside_codes
  return Array.isArray(list) ? list.filter((c) => !!c) : []
})

// --- 保护窗兜底（只读、P3）：未触发也要明确展示，不空白 ---
const fallback = computed<FallbackLike | null>(() => download.value.fallback || null)
const fallbackKnown = computed(() => fallback.value != null)
const fallbackEnabled = computed(() => fallback.value?.enabled === true)
const fallbackTriggered = computed(
  () => fallbackEnabled.value && fallback.value?.triggered === true,
)
const fallbackReasonText = computed(() => {
  const raw = String(fallback.value?.reason || '')
  const labels: Record<string, string> = {
    store_read_error: '库读失败',
    store_stale: '库落后',
    native_stalled: '原生停滞',
  }
  const parts = raw.split(',').filter(Boolean).map((r) => labels[r] || r)
  return parts.length ? parts.join(' + ') : '原因未知'
})
const fallbackLagText = computed(() => {
  const lag = fallback.value?.store_lag_minutes
  if (lag === null || lag === undefined) return '未知'
  return `${lag} 分钟`
})
const fallbackWindowText = computed(() => {
  const win = fallback.value?.window
  if (win?.start && win?.end) return `${win.start}–${win.end}`
  return '—'
})

// --- 限频利用率：实际 rps / 上限、轮次耗时 / interval ---
const utilization = computed<UtilizationLike | null>(
  () => download.value.utilization || null,
)
const utilKnown = computed(
  () => utilization.value != null && utilization.value.rps_ratio != null,
)
const utilRpsText = computed(() => {
  const u = utilization.value
  const actual = Number(u?.actual_rps ?? 0)
  const cap = Number(u?.rps_cap ?? 0)
  const ratio = Number(u?.rps_ratio ?? 0)
  return `rps ${actual.toFixed(1)} / ${cap.toFixed(0)}（${Math.round(ratio * 100)}%）`
})
const utilRoundText = computed(() => {
  const u = utilization.value
  const round = Number(u?.round_seconds ?? 0)
  const interval = Number(u?.interval_seconds ?? 0)
  const ratio = Number(u?.elapsed_ratio ?? 0)
  return `轮次 ${round.toFixed(1)}s / ${interval.toFixed(0)}s（${Math.round(ratio * 100)}%）`
})
const utilClass = computed(
  () => (utilization.value?.saturated === true ? 'badge-red' : 'badge-gray'),
)
const raiseText = computed(() => {
  const u = utilization.value
  if (u?.explicit_override) return '人工配置'
  if (u?.raise_exhausted) return '已达上限，仅告警'
  const raised = Number(u?.auto_raised_tiers ?? 0)
  const limit = Number(u?.max_raise_tiers ?? 0)
  return raised > 0 ? `已抬 ${raised}/${limit} 档` : `可抬 0/${limit} 档`
})
const raiseClass = computed(() => {
  const u = utilization.value
  if (u?.raise_exhausted) return 'badge-red'
  return Number(u?.auto_raised_tiers ?? 0) > 0 ? 'badge-green' : 'badge-gray'
})

// --- 按池规模建议配置表（由后端 sizing.table 提供，前端不写死） ---
const sizingRows = computed<SizingRowLike[]>(() => {
  const rows = download.value.sizing?.table
  return Array.isArray(rows) ? rows.filter((r) => r && r.tier) : []
})
const currentTier = computed(() => download.value.sizing?.tier_index ?? null)

const hasAny = computed(
  () => routeTotal.value > 0 || downloadKnown.value || outside.value !== null
    || fallbackKnown.value || utilKnown.value || sizingRows.value.length > 0,
)
</script>

<style scoped>
.minute-obs {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin-top: 8px;
  font-size: 12px;
}
.minute-obs-label {
  color: var(--ink-soft);
  font-weight: 600;
}
.minute-obs-codes {
  color: var(--ink-soft);
  word-break: break-all;
}
.minute-obs-error {
  color: var(--red);
  word-break: break-all;
}
.minute-obs-table-wrap {
  width: 100%;
  overflow-x: auto;
}
.minute-obs-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 11px;
}
.minute-obs-table th,
.minute-obs-table td {
  padding: 4px 6px;
  border-bottom: 1px solid var(--line, rgba(127, 127, 127, 0.2));
  text-align: left;
  white-space: nowrap;
}
.minute-obs-table tr.current {
  background: rgba(64, 158, 255, 0.12);
  font-weight: 600;
}
</style>
