import { describe, it, expect } from 'vitest'
import { formatGateResume, gatePauseOf, gatePauseText } from '../utils/tqGatePause'

function envelope(code: number, data: unknown = null, message = '') {
  return { response: { data: { code, message, data } } }
}

describe('tqGatePause', () => {
  it('保护窗口：识别为暂停，文案中性且带恢复时点', () => {
    const info = gatePauseOf(envelope(409, {
      reason: 'protected_window',
      next_available_at: '2026-11-24T15:10:00+08:00',
    }, '实盘收盘保护窗口（14:55–15:10）内暂停实盘辅助 TQ 任务；15:10 后恢复'))

    expect(info).not.toBeNull()
    expect(info!.reason).toBe('protected_window')
    expect(info!.resumeAt).toBe('2026-11-24T15:10:00+08:00')
    expect(info!.text).toContain('保护窗口内已暂停')
    expect(info!.text).toContain('15:10')
    expect(info!.text).toContain('后自动恢复')
    expect(info!.text).not.toContain('失败')
    expect(info!.text).not.toContain('拒绝')
  })

  it('实盘优先（仅有 next_available_at）：识别为暂停', () => {
    const text = gatePauseText(envelope(409, {
      next_available_at: '2026-11-24T15:15:00+08:00',
    }, '看板全量刷新在交易时段暂停（实盘优先），11-24 15:15 后可用'))

    expect(text).toContain('实盘运行中')
    expect(text).toContain('15:15')
    expect(text).not.toContain('加载失败')
  })

  it('真失败：5xx / 无提示时点的 409 → 不按暂停处理', () => {
    expect(gatePauseText(envelope(500, null, '服务器内部错误'))).toBeNull()
    expect(gatePauseText(envelope(409, null, '已有刷新任务在运行，请稍后再试'))).toBeNull()
    expect(gatePauseText({ message: 'Network Error' })).toBeNull()
    expect(gatePauseText(null)).toBeNull()
  })

  it('formatGateResume：当天只显示 HH:MM，跨天带 MM-DD', () => {
    const now = new Date()
    const pad = (n: number) => String(n).padStart(2, '0')
    const today = `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`
    expect(formatGateResume(`${today}T15:10:00+08:00`)).toBe('15:10')
    expect(formatGateResume('2026-11-25T09:31:00+08:00')).toBe('11-25 09:31')
    expect(formatGateResume(null)).toBe('')
  })
})
