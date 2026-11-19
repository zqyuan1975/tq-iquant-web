import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import ApprovalPanel from '../components/ApprovalPanel.vue'
import {
  approveLiveApproval, getLiveApprovals, rejectLiveApproval,
  type LiveApprovalItem,
} from '../api/approvals'

vi.mock('../api/approvals', () => ({
  getLiveApprovals: vi.fn(),
  approveLiveApproval: vi.fn(),
  rejectLiveApproval: vi.fn(),
}))

const mockGet = vi.mocked(getLiveApprovals)
const mockApprove = vi.mocked(approveLiveApproval)
const mockReject = vi.mocked(rejectLiveApproval)

function item(overrides: Partial<LiveApprovalItem> = {}): LiveApprovalItem {
  return {
    id: 1,
    portfolio_strategy_id: 11,
    strategy_id: 22,
    stock_code: '600000',
    trade_type: 'BUY',
    price: 10,
    quantity: 100,
    signal_type: 'OPEN',
    signal_name: '突破',
    period: '1m',
    bar_time: null,
    signal_bar_date: null,
    status: 'pending',
    reason: null,
    created_at: '2026-11-19T09:30:00',
    expires_at: '2099-11-19T09:45:00',
    strategy_name: '策略A',
    portfolio_name: '组合A',
    amount: 1000,
    expired: false,
    ...overrides,
  }
}

function buttons(w: ReturnType<typeof mount>, label: string) {
  return w.findAll('button').filter((b) => b.text() === label)
}

describe('ApprovalPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    vi.useFakeTimers({ toFake: ['Date'] })
    vi.setSystemTime(new Date('2026-11-19T09:35:00'))
    vi.stubGlobal('confirm', vi.fn(() => true))
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.unstubAllGlobals()
  })

  it('渲染列表字段，并通过/拒绝可操作', async () => {
    mockGet.mockResolvedValue({ manual_mode: true, approvals: [item()] })
    const w = mount(ApprovalPanel, { props: { sessionId: 1, running: true } })
    await flushPromises()
    expect(w.text()).toContain('600000')
    expect(w.text()).toContain('策略A')
    expect(w.text()).toContain('组合A')
    expect(w.text()).toContain('1,000.00')
    expect(w.text()).toContain('待批 1 单')
    expect(buttons(w, '通过')[0].attributes('disabled')).toBeUndefined()
    expect(buttons(w, '拒绝')[0].attributes('disabled')).toBeUndefined()
  })

  it('通过成功后行内移除并刷新计数', async () => {
    mockGet.mockResolvedValue({ manual_mode: true, approvals: [item()] })
    mockApprove.mockResolvedValue({ status: 'approved', approval_id: 1, live_order_id: 88 })
    const w = mount(ApprovalPanel, { props: { sessionId: 1, running: true } })
    await flushPromises()
    await buttons(w, '通过')[0].trigger('click')
    await flushPromises()
    expect(mockApprove).toHaveBeenCalledWith(1, 1)
    expect(w.text()).toContain('已通过并提交，委托 #88')
    expect(w.find('table').exists()).toBe(false)
    expect(w.text()).toContain('待批 0 单')
  })

  it('通过时后端判定过期，提示明确且不移交下单', async () => {
    mockGet.mockResolvedValue({ manual_mode: true, approvals: [item()] })
    mockApprove.mockResolvedValue({ status: 'expired', approval_id: 1 })
    const w = mount(ApprovalPanel, { props: { sessionId: 1, running: true } })
    await flushPromises()
    await buttons(w, '通过')[0].trigger('click')
    await flushPromises()
    expect(w.text()).toContain('该审批单已过期（超过审批时限），未提交任何委托。')
    expect(w.find('table').exists()).toBe(false)
  })

  it('过期单在列表可辨识且操作禁用', async () => {
    mockGet.mockResolvedValue({
      manual_mode: true,
      approvals: [item({ expired: true, expires_at: '2026-11-19T09:34:00' })],
    })
    const w = mount(ApprovalPanel, { props: { sessionId: 1, running: true } })
    await flushPromises()
    expect(w.text()).toContain('已过期')
    expect(buttons(w, '通过')[0].attributes('disabled')).toBeDefined()
    expect(buttons(w, '拒绝')[0].attributes('disabled')).toBeDefined()
  })

  it('拒绝可填原因并调用接口', async () => {
    mockGet.mockResolvedValue({ manual_mode: true, approvals: [item()] })
    mockReject.mockResolvedValue({ status: 'rejected', approval_id: 1 })
    const w = mount(ApprovalPanel, { props: { sessionId: 1, running: true } })
    await flushPromises()
    await buttons(w, '拒绝')[0].trigger('click')
    const input = w.find('input.approval-reason')
    expect(input.exists()).toBe(true)
    await input.setValue('风险太高')
    await buttons(w, '确认拒绝')[0].trigger('click')
    await flushPromises()
    expect(mockReject).toHaveBeenCalledWith(1, 1, '风险太高')
    expect(w.text()).toContain('已拒绝该审批单，未提交委托。')
    expect(w.find('table').exists()).toBe(false)
  })

  it('未启用人工审批时显示空态说明', async () => {
    mockGet.mockResolvedValue({ manual_mode: false, approvals: [] })
    const w = mount(ApprovalPanel, { props: { sessionId: 1, running: true } })
    await flushPromises()
    expect(w.text()).toContain('未启用人工审批流程')
  })

  it('会话未运行时显示空态且不请求接口', async () => {
    const w = mount(ApprovalPanel, { props: { sessionId: 1, running: false } })
    await flushPromises()
    expect(mockGet).not.toHaveBeenCalled()
    expect(w.text()).toContain('未运行，审批队列已清空')
  })
})
