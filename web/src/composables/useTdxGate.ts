/**
 * TQ 通道闸（tq_gate）状态 — 前端统一入口。
 *
 * 通达信单机只能开一个实例，回测与实盘共用同一通道。交易时段后台重任务（P2）
 * 会被后端直接拒绝（409 + 下次可用时刻），所以回测类按钮要提前置灰并说明原因，
 * 而不是让用户点了以后转圈/报错。
 *
 * 状态在模块级共享（多个视图共用一份 60s 轮询），组件卸载不销毁轮询。
 */
import { computed, ref } from 'vue'
import { getTdxGateStatus, type TdxGateStatus } from '../api'

const status = ref<TdxGateStatus | null>(null)
const loading = ref(false)
let timer: ReturnType<typeof setInterval> | null = null
let inflight: Promise<void> | null = null

async function refresh(): Promise<void> {
  if (inflight) return inflight
  loading.value = true
  inflight = (async () => {
    try {
      status.value = await getTdxGateStatus()
    } catch {
      // 状态接口不可用时保持上一次结果，不阻断页面其他功能
    } finally {
      loading.value = false
      inflight = null
    }
  })()
  return inflight
}

function ensurePolling() {
  if (timer !== null) return
  void refresh()
  timer = setInterval(() => { void refresh() }, 60_000)
}

function _fmtNext(iso: string | null): string {
  if (!iso) return '收盘后'
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})/)
  if (!m) return iso
  return `${m[2]}-${m[3]} ${m[4]}:${m[5]}`
}

export function useTdxGate() {
  ensurePolling()
  const enabled = computed(() => status.value?.enabled !== false)
  // 只有「有实盘在跑」才谈得上让路：没有实盘引擎时时段限制不生效，
  // 不能只读 p2_allowed（旧后端版本无 live_running 字段时保持原行为）。
  const liveRunning = computed(() => status.value?.policy.live_running !== false)
  const p2Blocked = computed(() =>
    enabled.value && liveRunning.value && !!status.value &&
    !status.value.policy.p2_allowed)
  const p2Reason = computed(() => {
    const p = status.value?.policy
    if (!p2Blocked.value || !p) return ''
    if (p.protected) {
      return '实盘收盘保护窗口（14:55–15:10）内暂停回测类任务，15:10 后恢复'
    }
    return `实盘运行中，回测类任务（P2）暂不可用；${_fmtNext(p.next_p2_open_at)} 后自动恢复`
  })
  return { status, loading, enabled, liveRunning, p2Blocked, p2Reason, refresh }
}
