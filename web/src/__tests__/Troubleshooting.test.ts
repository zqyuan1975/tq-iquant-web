import { describe, expect, it, vi, beforeEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import Troubleshooting from '../views/Troubleshooting.vue'

const {
  mockGetHealthOverview, mockGetTdxGateStatus, mockSetTdxGateOverride,
  mockClearTdxGateOverride, mockLiquidateAllSessions,
} = vi.hoisted(() => ({
  mockGetHealthOverview: vi.fn(),
  mockGetTdxGateStatus: vi.fn(),
  mockSetTdxGateOverride: vi.fn(),
  mockClearTdxGateOverride: vi.fn(),
  mockLiquidateAllSessions: vi.fn(),
}))
vi.mock('../api', () => ({
  getHealthOverview: mockGetHealthOverview,
  getTdxGateStatus: mockGetTdxGateStatus,
  setTdxGateOverride: mockSetTdxGateOverride,
  clearTdxGateOverride: mockClearTdxGateOverride,
  liquidateAllSessions: mockLiquidateAllSessions,
}))

function _gate(override: any = null) {
  return {
    enabled: true,
    policy: {
      now: '2026-11-23T20:00:00+08:00', trading_day: false, live_running: true,
      window: 'after_close', override: !!override, protected: false,
      p1_allowed: true, p2_allowed: false, p2_quota_active: false,
      p1_quota_active: false, next_p2_open_at: null,
    },
    override,
    channel_held: false,
    stats: {},
  }
}

const RouterLinkStub = {
  props: ['to'],
  template: '<a :href="to"><slot /></a>',
}

function _overview(problems: any[]) {
  return {
    generated_at: '2026-11-23T20:03:23+08:00',
    level: 'warn', problems, sessions: [], channels: {},
    core: {}, watchdog: {}, tasks: {}, disk: {}, backup: {}, notify: {},
    alert_totals: { open_decisions: 0, deferred_orders: 0, pending_approvals: 0 },
  }
}

describe('故障向导：可修复与观察项分开', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockGetTdxGateStatus.mockResolvedValue(_gate())
  })

  it('只有带 fix 的进故障列表且动作可点击；外部处理项归观察', async () => {
    mockGetHealthOverview.mockResolvedValue(_overview([
      { level: 'warn', text: '会话「01」有 3 条未处理告警', kind: 'open_decisions',
        impact: '告警堆积会掩盖新问题', fix: { label: '去决策页签批量确认',
                                             to: '/live?tab=decisions' } },
      { level: 'warn', text: '会话「01」数据体检未完成：抽样未覆盖', kind: 'data_incomplete',
        impact: '本次抽样未覆盖全部标的，非数据缺失',
        observe: '盘后可在实盘工作台重跑数据体检' },
    ]))
    const w = mount(Troubleshooting, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
    await flushPromises()
    const text = w.text()
    expect(text).toContain('可修复故障（1）')
    expect(text).toContain('观察项（1')
    expect(text).toContain('去决策页签批量确认')
    expect(w.find('a[href="/live?tab=decisions"]').exists()).toBe(true)
    expect(text).toContain('影响：告警堆积会掩盖新问题')
    expect(text).toContain('外部处理：盘后可在实盘工作台重跑数据体检')
    // 观察项不应渲染成可点击的修复按钮
    expect(w.findAll('a[href="/live?tab=decisions"]')).toHaveLength(1)
    w.unmount()
  })
})

describe('故障向导：高危运维入口（二次确认 + 结果反馈）', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockGetHealthOverview.mockResolvedValue(_overview([]))
    mockGetTdxGateStatus.mockResolvedValue(_gate())
  })

  it('TQ 闸覆盖：原因必填，确认后带 confirm=true 调接口并回显结果', async () => {
    mockSetTdxGateOverride.mockResolvedValue({ override: {}, decision_event_id: 1 })
    const w = mount(Troubleshooting, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
    await flushPromises()
    await w.find('[data-test="gate-override-open"]').trigger('click')
    expect(w.find('[data-test="gate-modal"]').exists()).toBe(true)

    // 未填原因 → 拦在本地，不调接口
    await w.find('[data-test="gate-confirm"]').trigger('click')
    await flushPromises()
    expect(mockSetTdxGateOverride).not.toHaveBeenCalled()
    expect(w.find('[data-test="gate-modal-error"]').text()).toContain('覆盖原因')

    await w.find('[data-test="gate-reason"]').setValue('盘后补算')
    await w.find('[data-test="gate-confirm"]').trigger('click')
    await flushPromises()
    expect(mockSetTdxGateOverride).toHaveBeenCalledWith(
      { confirm: true, reason: '盘后补算', minutes: 30 })
    expect(w.find('[data-test="ops-feedback"]').text()).toContain('TQ 闸已临时覆盖 30 分钟')
    w.unmount()
  })

  it('紧急全平：未输入 LIQUIDATE 不调接口；确认后展示每会话提交/跳过数', async () => {
    mockLiquidateAllSessions.mockResolvedValue({
      results: [{ session_id: 1, submitted: 3, skipped: 1 }],
    })
    const w = mount(Troubleshooting, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
    await flushPromises()
    await w.find('[data-test="liquidate-open"]').trigger('click')
    expect(w.find('[data-test="liquidate-modal"]').exists()).toBe(true)

    await w.find('[data-test="liquidate-confirm"]').trigger('click')
    await flushPromises()
    expect(mockLiquidateAllSessions).not.toHaveBeenCalled()
    expect(w.find('[data-test="liquidate-error"]').text()).toContain('LIQUIDATE')

    await w.find('[data-test="liquidate-confirm-text"]').setValue('LIQUIDATE')
    await w.find('[data-test="liquidate-confirm"]').trigger('click')
    await flushPromises()
    expect(mockLiquidateAllSessions).toHaveBeenCalledWith('LIQUIDATE', undefined)
    const fb = w.find('[data-test="ops-feedback"]').text()
    expect(fb).toContain('#1 提交 3 笔卖单')
    expect(fb).toContain('跳过 1 笔')
    w.unmount()
  })

  it('TQ 闸覆盖失败时给出原因', async () => {
    mockGetTdxGateStatus.mockResolvedValue(_gate({
      minutes: 30, reason: '旧覆盖', operator: 'ops',
      started_at: '2026-11-23T19:00:00+08:00',
      expires_at: '2026-11-23T19:30:00+08:00', active: true,
    }))
    mockClearTdxGateOverride.mockRejectedValue(new Error('网络不通'))
    const w = mount(Troubleshooting, {
      global: { stubs: { RouterLink: RouterLinkStub } },
    })
    await flushPromises()
    expect(w.find('[data-test="gate-override-active"]').text()).toContain('旧覆盖')
    await w.find('[data-test="gate-clear"]').trigger('click')
    await flushPromises()
    expect(w.find('[data-test="ops-feedback"]').text()).toContain('解除覆盖失败：网络不通')
    w.unmount()
  })
})
