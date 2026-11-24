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
interface DownloadLike {
  enabled?: boolean
  alive?: boolean
  stalled?: boolean
  last_round_age_seconds?: number | null
  thread_error?: string | null
  supervision?: SupervisionLike | null
}

const props = defineProps<{
  route?: RouteLike | null
  coverage?: CoverageLike | null
  download?: DownloadLike | null
}>()

const showCodes = ref(false)

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

const hasAny = computed(
  () => routeTotal.value > 0 || downloadKnown.value || outside.value !== null,
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
</style>
