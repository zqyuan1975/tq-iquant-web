import { describe, expect, it } from 'vitest'
import { mount } from '@vue/test-utils'
import MinuteObservability from '../components/MinuteObservability.vue'

const route = { requests: 10, store: 7, inner: 3 }

describe('MinuteObservability', () => {
  it('正常态：路由占比、线程存活与覆盖缺口均可读', async () => {
    const w = mount(MinuteObservability, {
      props: {
        route,
        coverage: { outside_pool: 2, outside_codes: ['000001', '600000'], pool_known: true },
        download: {
          enabled: true, alive: true, stalled: false,
          last_round_age_seconds: 12, thread_error: null,
          supervision: { attempts_in_window: 0, gave_up: false },
        },
      },
    })

    expect(w.text()).toContain('自有存储 7')
    expect(w.text()).toContain('原生/快照 3')
    expect(w.text()).toContain('本轮请求 10（存储占比 70%）')
    expect(w.text()).toContain('存活')
    expect(w.text()).toContain('最近一轮 12 秒前')
    expect(w.text()).toContain('池外 2 只')
    expect(w.find('[data-test="thread-badge"]').classes()).toContain('badge-green')

    await w.find('[data-test="coverage-toggle"]').trigger('click')
    expect(w.find('[data-test="coverage-codes"]').text()).toContain('000001、600000')
    w.unmount()
  })

  it('停滞态：中性灰徽标，不误报红色', () => {
    const w = mount(MinuteObservability, {
      props: {
        route,
        download: {
          enabled: true, alive: true, stalled: true,
          last_round_age_seconds: 600, thread_error: null,
          supervision: { attempts_in_window: 1, gave_up: false },
        },
      },
    })

    expect(w.text()).toContain('停滞')
    expect(w.text()).toContain('最近一轮 10 分 0 秒前')
    const badge = w.find('[data-test="thread-badge"]')
    expect(badge.classes()).toContain('badge-gray')
    expect(badge.classes()).not.toContain('badge-red')
    w.unmount()
  })

  it('线程已死：红色徽标 + 异常信息 + 监督放弃', () => {
    const w = mount(MinuteObservability, {
      props: {
        download: {
          enabled: true, alive: false, stalled: false,
          last_round_age_seconds: null, thread_error: 'TimeoutError: read timeout',
          supervision: { attempts_in_window: 4, gave_up: true },
        },
      },
    })

    expect(w.text()).toContain('已停止')
    expect(w.text()).toContain('尚未完成一轮')
    expect(w.text()).toContain('TimeoutError: read timeout')
    expect(w.text()).toContain('重建已放弃（4 次）')
    expect(w.find('[data-test="thread-badge"]').classes()).toContain('badge-red')
    expect(w.find('[data-test="gave-up"]').exists()).toBe(true)
    w.unmount()
  })

  it('空值兜底：字段缺失不崩，按未知/不可用中性展示', () => {
    const empty = mount(MinuteObservability)
    expect(empty.text()).toContain('暂无分钟下载观测数据')
    expect(empty.find('[data-test="minute-obs-empty"]').exists()).toBe(true)
    empty.unmount()

    const partial = mount(MinuteObservability, {
      props: {
        coverage: { outside_pool: 1 },
        download: { enabled: true },
      },
    })
    expect(partial.text()).toContain('状态未知')
    expect(partial.text()).not.toContain('已停止')
    const toggle = partial.find('[data-test="coverage-toggle"]')
    expect(toggle.text()).toContain('池外 1 只')
    expect(toggle.attributes('disabled')).toBeDefined()
    partial.unmount()
  })
})
