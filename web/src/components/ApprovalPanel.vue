<script setup lang="ts">
/**
 * 实盘工作台「待审批」面板（2026-11-19）。
 *
 * 复用后端既有 `live_approval_queue` 与 approvals 接口：
 * - 列表：标的/方向/数量/金额/策略与组合/发起时间/有效期（过期可辨识）
 * - 通过：按当前价重走完整闸门；返回 expired / rejected 时给出明确说明
 * - 拒绝：可填原因，写入审批队列审计
 * 未启用人工审批（自动/影子）或会话未运行时显示空态，不阻塞既有流程。
 */
import { computed, onMounted, onUnmounted, ref, watch } from 'vue'
import {
  approveLiveApproval, getLiveApprovals, rejectLiveApproval,
  type LiveApprovalItem,
} from '../api/approvals'
import { formatMoney2, parseBeijingTime } from '../utils/liveWorkbench'

const props = defineProps<{
  sessionId: number | null
  running: boolean
}>()

const loading = ref(false)
const loadError = ref('')
const manualMode = ref(true)
const items = ref<LiveApprovalItem[]>([])
const notice = ref('')
const actionError = ref('')
const busyId = ref<number | null>(null)
const rejectingId = ref<number | null>(null)
const rejectReason = ref('')
const nowMs = ref(Date.now())
let ticker: number | undefined

const pendingCount = computed(() => items.value.length)

function errText(e: unknown): string {
  const err = e as {
    response?: { status?: number; data?: { message?: string } }
    message?: string
  }
  const status = err.response?.status
  const msg = err.response?.data?.message || err.message || '请求失败'
  if (status === 409) return `审批单状态已变化（可能已被其他操作处理）：${msg}`
  if (status === 404) return '审批单不存在或已被清理。'
  return msg
}

function isExpired(item: LiveApprovalItem): boolean {
  if (item.expired) return true
  if (!item.expires_at) return false
  const ts = Date.parse(item.expires_at)
  return Number.isFinite(ts) && ts <= nowMs.value
}

function expireText(item: LiveApprovalItem): string {
  if (isExpired(item)) return '已过期'
  if (!item.expires_at) return '—'
  const ms = Date.parse(item.expires_at) - nowMs.value
  if (!Number.isFinite(ms)) return '—'
  return `剩 ${Math.max(1, Math.ceil(ms / 60000))} 分钟`
}

function timeText(v: string | null | undefined): string {
  if (!v) return '—'
  const parsed = parseBeijingTime(v)
  return parsed.text || v.replace('T', ' ').slice(0, 19)
}

function dirText(v: string): string {
  if (v === 'BUY') return '买入'
  if (v === 'SELL') return '卖出'
  return v
}

function removeRow(id: number) {
  items.value = items.value.filter((it) => it.id !== id)
}

async function load() {
  actionError.value = ''
  if (props.sessionId === null || !props.running) {
    items.value = []
    loadError.value = ''
    manualMode.value = true
    return
  }
  loading.value = true
  loadError.value = ''
  try {
    const d = await getLiveApprovals(props.sessionId)
    manualMode.value = d.manual_mode
    items.value = d.approvals
  } catch (e) {
    items.value = []
    loadError.value = errText(e)
  } finally {
    loading.value = false
  }
}

function startReject(item: LiveApprovalItem) {
  rejectingId.value = item.id
  rejectReason.value = ''
  actionError.value = ''
}

function cancelReject() {
  rejectingId.value = null
  rejectReason.value = ''
}

async function approve(item: LiveApprovalItem) {
  const sid = props.sessionId
  if (sid === null || busyId.value !== null) return
  if (isExpired(item)) {
    actionError.value = '该审批单已过期（超过审批时限），不能再通过。'
    return
  }
  busyId.value = item.id
  actionError.value = ''
  notice.value = ''
  try {
    const r = await approveLiveApproval(sid, item.id)
    if (r.status === 'expired') {
      notice.value = '该审批单已过期（超过审批时限），未提交任何委托。'
    } else if (r.status === 'rejected') {
      notice.value = `已批准，但重走闸门未通过：${r.reason || '详见决策事件'}。`
    } else {
      notice.value = `已通过并提交，委托 #${r.live_order_id ?? '—'}。`
    }
    removeRow(item.id)
  } catch (e) {
    const status = (e as { response?: { status?: number } }).response?.status
    actionError.value = errText(e)
    if (status === 409 || status === 404) removeRow(item.id)
  } finally {
    busyId.value = null
  }
}

async function confirmReject(item: LiveApprovalItem) {
  const sid = props.sessionId
  if (sid === null || busyId.value !== null) return
  busyId.value = item.id
  actionError.value = ''
  notice.value = ''
  try {
    await rejectLiveApproval(sid, item.id, rejectReason.value.trim() || '人工拒绝')
    notice.value = '已拒绝该审批单，未提交委托。'
    removeRow(item.id)
    cancelReject()
  } catch (e) {
    const status = (e as { response?: { status?: number } }).response?.status
    actionError.value = errText(e)
    if (status === 409 || status === 404) {
      removeRow(item.id)
      cancelReject()
    }
  } finally {
    busyId.value = null
  }
}

watch(
  () => [props.sessionId, props.running] as const,
  () => {
    notice.value = ''
    cancelReject()
    void load()
  },
  { immediate: true },
)

onMounted(() => {
  ticker = window.setInterval(() => { nowMs.value = Date.now() }, 30_000)
})

onUnmounted(() => {
  if (ticker !== undefined) window.clearInterval(ticker)
})
</script>

<template>
  <div class="table-wrap">
    <div class="approval-head">
      <strong>人工审批队列</strong>
      <span class="badge badge-gray">待批 {{ pendingCount }} 单</span>
      <span class="approval-hint">通过后按当前价重走完整闸门；拒绝会写审计原因。</span>
      <button class="btn btn-sm approval-refresh" :disabled="loading || sessionId === null" @click="load()">
        {{ loading ? '刷新中…' : '↻ 刷新' }}
      </button>
    </div>

    <p v-if="notice" class="approval-notice">{{ notice }}</p>
    <p v-if="actionError" class="approval-error">{{ actionError }}</p>

    <div v-if="loadError" class="empty-state">
      <p>{{ loadError }}</p>
      <button class="btn btn-sm" @click="load()">重试</button>
    </div>
    <div v-else-if="sessionId === null" class="empty-state">
      <p>当前没有运行中的实盘会话。审批队列在会话停止时作废，重启不重放。</p>
    </div>
    <div v-else-if="!running" class="empty-state">
      <p>会话 #{{ sessionId }} 未运行，审批队列已清空，无法操作。</p>
    </div>
    <div v-else-if="loading && items.length === 0" class="empty-state">
      <p>加载审批队列…</p>
    </div>
    <div v-else-if="!manualMode" class="empty-state">
      <p>当前会话未启用人工审批流程（自动/影子模式），不会产生待审批单。</p>
    </div>
    <div v-else-if="items.length === 0" class="empty-state">
      <p>当前没有待审批单。人工审批模式下，开仓信号会先进入这里等待确认。</p>
    </div>

    <table v-else>
      <thead>
        <tr>
          <th>标的</th>
          <th>方向</th>
          <th>数量</th>
          <th>金额（信号价×量）</th>
          <th>策略 / 组合</th>
          <th>发起时间</th>
          <th>有效期</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="item in items" :key="item.id" :class="{ 'approval-expired': isExpired(item) }">
          <td>
            <b>{{ item.stock_code }}</b>
            <div class="approval-sub">{{ item.signal_name || item.signal_type || '—' }}</div>
          </td>
          <td>{{ dirText(item.trade_type) }}</td>
          <td class="num">{{ item.quantity }}</td>
          <td class="num">
            {{ item.amount != null ? formatMoney2(item.amount)
              : (item.price != null ? `${item.price} × ${item.quantity}` : '—') }}
          </td>
          <td>
            {{ item.strategy_name || `策略#${item.strategy_id}` }}
            <div class="approval-sub">{{ item.portfolio_name || `组合#${item.portfolio_strategy_id}` }}</div>
          </td>
          <td>{{ timeText(item.created_at) }}</td>
          <td>
            <span class="badge" :class="isExpired(item) ? 'badge-red' : 'badge-gray'">
              {{ expireText(item) }}
            </span>
            <div class="approval-sub">截止 {{ timeText(item.expires_at) }}</div>
          </td>
          <td class="approval-actions">
            <template v-if="rejectingId === item.id">
              <input
                v-model="rejectReason"
                class="approval-reason"
                maxlength="200"
                placeholder="拒绝原因（可选）"
                @keyup.enter="confirmReject(item)"
              >
              <button class="btn btn-sm btn-danger" :disabled="busyId === item.id" @click="confirmReject(item)">
                确认拒绝
              </button>
              <button class="btn btn-sm" :disabled="busyId === item.id" @click="cancelReject">取消</button>
            </template>
            <template v-else>
              <button
                class="btn btn-sm btn-primary"
                :disabled="busyId === item.id || isExpired(item)"
                :title="isExpired(item) ? '已过期，无法通过' : ''"
                @click="approve(item)"
              >
                通过
              </button>
              <button
                class="btn btn-sm"
                :disabled="busyId === item.id || isExpired(item)"
                @click="startReject(item)"
              >
                拒绝
              </button>
            </template>
          </td>
        </tr>
      </tbody>
    </table>
  </div>
</template>

<style scoped>
.approval-head {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  margin: 0 0 8px;
}
.approval-hint { font-size: 12px; color: var(--ink-soft) }
.approval-refresh { margin-left: auto }
.approval-sub { font-size: 11px; color: var(--ink-soft); margin-top: 2px }
.approval-notice {
  margin: 0 0 8px;
  padding: 6px 10px;
  font-size: 12px;
  color: var(--ink);
  background: var(--green-wash);
  border: 1px solid var(--green);
  border-radius: 6px;
}
.approval-error {
  margin: 0 0 8px;
  padding: 6px 10px;
  font-size: 12px;
  color: var(--red);
  background: var(--red-wash);
  border: 1px solid var(--red);
  border-radius: 6px;
}
.approval-expired { opacity: 0.72 }
.approval-actions { white-space: nowrap }
.approval-actions .btn + .btn { margin-left: 6px }
.approval-reason {
  width: 130px;
  margin-right: 6px;
  padding: 3px 6px;
  font-size: 12px;
  border: 1px solid var(--line);
  border-radius: 5px;
}
</style>
