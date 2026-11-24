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

  it('保护窗兜底未触发时明确显示「未触发」，不空白', () => {
    const w = mount(MinuteObservability, {
      props: {
        download: {
          enabled: true, alive: true,
          fallback: {
            enabled: true, triggered: false,
            window: { start: '14:55', end: '14:57', state: 'outside' },
          },
        },
      },
    })

    expect(w.find('[data-test="fallback-status"]').text()).toBe('未触发')
    expect(w.text()).toContain('允许 14:55–14:57')
    w.unmount()
  })

  it('保护窗兜底触发：原因/lag/窗口/时间均可见', () => {
    const w = mount(MinuteObservability, {
      props: {
        download: {
          enabled: true, alive: true,
          fallback: {
            enabled: true, triggered: true,
            reason: 'store_stale,native_stalled',
            store_lag_minutes: 5,
            window_state: 'p0_only',
            last_trigger_at: '2026-11-24 14:55:10',
            window: { start: '14:55', end: '14:57', state: 'open' },
          },
        },
      },
    })

    expect(w.find('[data-test="fallback-status"]').text()).toBe('已触发')
    expect(w.find('[data-test="fallback-reason"]').text()).toBe('库落后 + 原生停滞')
    const detail = w.find('[data-test="fallback-detail"]').text()
    expect(detail).toContain('库 lag 5 分钟')
    expect(detail).toContain('p0_only')
    expect(detail).toContain('2026-11-24 14:55:10')
    w.unmount()
  })

  it('限频利用率与自动抬档：展示实际/上限、轮次/interval 与抬档状态', () => {
    const w = mount(MinuteObservability, {
      props: {
        download: {
          enabled: true, alive: true,
          utilization: {
            actual_rps: 11.95, rps_cap: 12, rps_ratio: 0.996,
            round_seconds: 21.2, interval_seconds: 60, elapsed_ratio: 0.353,
            high_streak: 2, alerts: 1, saturated: true,
            auto_raised_tiers: 1, max_raise_tiers: 1, raise_exhausted: false,
          },
        },
      },
    })

    expect(w.find('[data-test="util-rps"]').text()).toContain('11.9 / 12')
    expect(w.find('[data-test="util-rps"]').text()).toContain('100%')
    expect(w.find('[data-test="util-round"]').text()).toContain('21.2s / 60s')
    expect(w.find('[data-test="util-rps"]').classes()).toContain('badge-red')
    expect(w.find('[data-test="util-raise"]').text()).toBe('已抬 1/1 档')
    w.unmount()
  })

  it('按池规模建议表：由后端 sizing.table 提供，展开后高亮当前档', async () => {
    const w = mount(MinuteObservability, {
      props: {
        download: {
          enabled: true, alive: true,
          sizing: {
            tier_index: 1,
            table: [
              { tier_index: 0, tier: '≤150', connections: 2, max_req_per_second: 6, single_host_sustainable: true, note: '小池不浪费连接' },
              { tier_index: 1, tier: '151–400', connections: 4, max_req_per_second: 12, single_host_sustainable: true, note: '251 只实测档' },
              { tier_index: 4, tier: '2001–6000', connections: 10, max_req_per_second: 30, single_host_sustainable: false, note: '必须多主站分片' },
            ],
          },
        },
      },
    })

    expect(w.find('[data-test="sizing-toggle"]').text()).toContain('建议 3 档')
    await w.find('[data-test="sizing-toggle"]').trigger('click')
    const table = w.find('[data-test="sizing-table"]')
    expect(table.text()).toContain('151–400')
    expect(table.text()).toContain('需分片')
    expect(table.find('tr.current').text()).toContain('151–400')
    w.unmount()
  })
})
