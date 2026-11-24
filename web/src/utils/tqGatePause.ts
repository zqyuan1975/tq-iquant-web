/**
 * TQ 通道闸「暂停」与「真失败」的区分（仅展示层用）。
 *
 * 后端闸门拒绝沿用统一 envelope：HTTP 200 + code=409 + data（下次可用时刻）。
 * 这是设计上的让路，不是故障；页面必须用中性提示（.tq-gate-notice）展示，
 * 不能落到「加载失败 / 红色错误条」里。真正的失败（5xx、网络、解析）没有
 * data.next_available_at，仍按原错误路径处理。
 */

const GATE_PAUSE_REASONS = new Set(['protected_window', 'trading_window'])

export interface GatePauseInfo {
  reason: string
  resumeAt: string | null
  /** 中性文案（含恢复时点），可直接渲染到 .tq-gate-notice */
  text: string
}

function pad(n: number): string {
  return String(n).padStart(2, '0')
}

/** ISO 时刻 → 「HH:MM」（当天）或「MM-DD HH:MM」（跨天）；无法解析时原样返回。 */
export function formatGateResume(iso: string | null): string {
  if (!iso) return ''
  const m = iso.match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})/)
  if (!m) return iso
  const now = new Date()
  const today = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`
  const day = `${m[1]}-${m[2]}-${m[3]}`
  return day === today ? `${m[4]}:${m[5]}` : `${m[2]}-${m[3]} ${m[4]}:${m[5]}`
}

/**
 * 从接口错误里识别「闸门暂停」；不是闸门暂停（含并发槽等其他 409）返回 null。
 * 判据：envelope code=409，且 reason 属于闸门集合或带 next_available_at。
 */
export function gatePauseOf(e: unknown): GatePauseInfo | null {
  const d = (e as { response?: { data?: { code?: unknown; data?: unknown } } })?.response?.data
  if (!d || d.code !== 409) return null
  const info = (d.data || {}) as { reason?: unknown; next_available_at?: unknown }
  const reason = typeof info.reason === 'string' ? info.reason : ''
  const resumeAt = typeof info.next_available_at === 'string' ? info.next_available_at : null
  if (!GATE_PAUSE_REASONS.has(reason) && !resumeAt) return null
  const when = formatGateResume(resumeAt)
  const text = reason === 'protected_window'
    ? `实盘收盘保护窗口内已暂停${when ? `，${when} 后自动恢复` : '，窗口结束后自动恢复'}`
    : `实盘运行中，TQ 后台任务暂不可用${when ? `；${when} 后自动恢复` : '；收盘后自动恢复'}`
  return { reason: reason || 'p2_window', resumeAt, text }
}

/** 便捷版：命中闸门暂停返回中性文案，否则 null。 */
export function gatePauseText(e: unknown): string | null {
  return gatePauseOf(e)?.text ?? null
}
