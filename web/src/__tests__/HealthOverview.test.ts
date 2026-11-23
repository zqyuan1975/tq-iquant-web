import { describe, expect, it, vi, beforeEach } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'
import HealthOverview from '../views/HealthOverview.vue'

const {
  mockGetHealthOverview, mockGetObsSummary,
  mockGetKlineCoverageStatus, mockListKlineCoverage, mockGetKlineCoveragePlan,
  mockGetBacktestRecords, mockGetBacktestDetail,
} = vi.hoisted(() => ({
  mockGetHealthOverview: vi.fn(),
  mockGetObsSummary: vi.fn(),
  mockGetKlineCoverageStatus: vi.fn(),
  mockListKlineCoverage: vi.fn(),
  mockGetKlineCoveragePlan: vi.fn(),
  mockGetBacktestRecords: vi.fn(),
  mockGetBacktestDetail: vi.fn(),
}))
vi.mock('../api', () => ({
  getHealthOverview: mockGetHealthOverview,
  getDataObservabilitySummary: mockGetObsSummary,
  getKlineCoverageStatus: mockGetKlineCoverageStatus,
  listKlineCoverage: mockListKlineCoverage,
  getKlineCoveragePlan: mockGetKlineCoveragePlan,
  getBacktestRecords: mockGetBacktestRecords,
  getBacktestDetail: mockGetBacktestDetail,
}))

function _overview(sessions: any[]) {
  return {
    generated_at: '2026-11-23T20:03:23+08:00',
    level: 'warn',
    problems: [],
    core: { online: true, version: 'v1.0.6', uptime: '0h7m' },
    watchdog: { running: true, last_scan_at: null, active_alarms: [] },
    channels: {},
    sessions,
    tasks: { trade_date: '2026-11-23', running_sessions: 1,
             backup_at: '15:10', backup_done_today: true },
    backup: {},
    disk: { free_mb: 90297, min_free_mb: 500, ok: true, path: 'D:' },
    notify: { enabled: true, usable: true, channels: ['feishu'], min_level: 'warn' },
    alert_totals: { open_decisions: 0, deferred_orders: 0, pending_approvals: 0 },
  }
}

function _session(overrides: Record<string, any> = {}) {
  return {
    id: 1, name: '02', status: 'stopped', engine: 'iquant',
    channel: {}, channel_ok: false,
    bridge: { online: null },
    today: { orders: 0, trades: 0 },
    channel_guard: {
      available: true, ok: true, failed: [],
      checks: [
        { key: 'primary_configured', label: '主通道已配置', ok: true, detail: 'x' },
        { key: 'client_online', label: '客户端/桥在线', ok: true,
          applicable: false, detail: '未运行（不需要）' },
        { key: 'backup_ready', label: '备用通道可用', ok: true,
          applicable: false, detail: '未配置（可选，未启用备用通道）' },
      ],
    },
    data: { available: false, not_running: true, incomplete: false,
            periods: {}, bad_total: 0 },
    alerts: { open_decisions: 0, deferred_orders: 0, pending_approvals: 0 },
    today_orders: 0, today_trades: 0,
    ...overrides,
  }
}

describe('HealthOverview 数据新鲜度与通道结论', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockGetObsSummary.mockResolvedValue({
      generated_at: '2026-11-23T20:00:00+08:00',
      corporate_actions: { enabled: true, unverified: 0, codes: [], last_audit: null },
      caliber: { records: 0, stale: 0, reasons: {} },
      kline_coverage: { enabled: false, ledger_path: '', exists: false,
                        coverage: 0, empty_days: 0, stamps: 0,
                        latest_updated_at: null },
    })
    mockGetKlineCoverageStatus.mockResolvedValue({
      enabled: false, ledger_path: 'D:/data/kline_ledger.db', exists: true,
      coverage: 1, empty_days: 1, stamps: 1,
      latest_updated_at: '2026-11-23T15:00:00',
    })
    mockListKlineCoverage.mockResolvedValue({
      ledger_path: 'D:/data/kline_ledger.db', enabled: false, count: 1,
      records: [{
        code: '000001.SZ', period: '1d', field: 'all', source: 'tdx',
        ranges: [['2026-01-05', '2026-01-10'], ['2026-01-12', '2026-01-20']],
        empty_days: ['2026-01-11'],
        stamp: { path: 'x', mtime_ns: 1, size: 1 }, stamp_valid: true,
        updated_at: '2026-11-23T15:00:00',
      }],
    })
    mockGetKlineCoveragePlan.mockResolvedValue({
      code: '000001.SZ', period: '1d', field: 'all', source: 'tdx',
      ranges: [['2026-01-05', '2026-01-10'], ['2026-01-12', '2026-01-20']],
      empty_days: ['2026-01-11'], stamp_valid: true, trusted: true,
      reason: 'complete', missing_days: ['2026-01-20'],
      tail_gap: false, expected_days: 14, covered_days: 13,
    })
    mockGetBacktestRecords.mockResolvedValue([
      { id: 1, name: 'A', status: 'done', start_date: '2026-01-01',
        end_date: '2026-03-01' },
      { id: 2, name: 'B', status: 'done', start_date: '2026-01-01',
        end_date: '2026-03-01' },
    ])
    mockGetBacktestDetail.mockImplementation(async (id: number) => ({
      record: { id, name: id === 1 ? 'A' : 'B' },
      data_fingerprint: {
        window: { start: '2026-01-01', end: '2026-03-01' },
        hash: id === 1 ? 'h1' : 'h2',
        per_code: {
          '000001.SZ': { '1d': { bars: 40, first: '2026-01-02', last: '2026-03-01' } },
          ...(id === 1 ? {} : {
            '000002.SZ': { '1d': { bars: 39, first: '2026-01-02', last: '2026-02-28' } },
          }),
        },
      },
    }))
  })

  it('停止态会话：未运行（不需要），通道项显示不适用', async () => {
    mockGetHealthOverview.mockResolvedValue(_overview([_session()]))
    const w = mount(HealthOverview)
    await flushPromises()
    expect(w.find('.data-cell').text()).toContain('未运行（不需要）')
    expect(w.text()).toContain('不适用')
    expect(w.text()).not.toContain('有缺失/陈旧')
    w.unmount()
  })

  it('零缺失但抽样截断：只写未校验，不写有缺失/陈旧', async () => {
    mockGetHealthOverview.mockResolvedValue(_overview([_session({
      status: 'running',
      data: { available: true, healthy: false, incomplete: true,
              truncated: true, codes_total: 251, codes_checked: 22,
              periods: {}, bad_total: 0, data_missing_count: 0 },
    })]))
    const w = mount(HealthOverview)
    await flushPromises()
    const cell = w.find('.data-cell').text()
    expect(cell).toContain('未校验（抽样）')
    expect(cell).toContain('已校验 22/251 只')
    expect(cell).not.toContain('有缺失/陈旧')
    w.unmount()
  })

  it('真有缺失（stale）才显示有缺失/陈旧', async () => {
    mockGetHealthOverview.mockResolvedValue(_overview([_session({
      status: 'running',
      data: { available: true, healthy: false, incomplete: false,
              periods: { '1d': { stale: 2, empty: 0, total: 30 } },
              bad_total: 2, data_missing_count: 2 },
    })]))
    const w = mount(HealthOverview)
    await flushPromises()
    expect(w.find('.data-cell').text()).toContain('有缺失/陈旧')
    w.unmount()
  })

  it('台账详情：只读摘要 + 缺口三分类 + 展开逐票明细', async () => {
    mockGetHealthOverview.mockResolvedValue(_overview([_session()]))
    const w = mount(HealthOverview)
    await flushPromises()

    await w.find('[data-test="ledger-open"]').trigger('click')
    await flushPromises()

    const modal = w.find('[data-test="ledger-modal"]')
    expect(modal.exists()).toBe(true)
    // 台账开关状态如实展示（未开启），且只读查看不触发任何写接口
    expect(modal.text()).toContain('未开启')
    expect(modal.text()).toContain('停牌 1')
    expect(modal.text()).toContain('未发布 1')
    expect(modal.text()).toContain('13/14')

    // 展开逐票明细：列出停牌日与未发布日
    await modal.find('tbody tr').trigger('click')
    await flushPromises()
    expect(modal.text()).toContain('2026-01-11')
    expect(modal.text()).toContain('2026-01-20')
    expect(modal.text()).toContain('可信任台账')
    w.unmount()
  })

  it('指纹对比：按代码+周期给出 一致/不一致/仅一侧', async () => {
    mockGetHealthOverview.mockResolvedValue(_overview([_session()]))
    const w = mount(HealthOverview)
    await flushPromises()

    await w.find('[data-test="ledger-open"]').trigger('click')
    await flushPromises()
    await w.find('[data-test="ledger-tab-fingerprint"]').trigger('click')
    await flushPromises()

    const modal = w.find('[data-test="ledger-modal"]')
    const buttons = modal.findAll('button')
    const compare = buttons.find((b) => b.text().includes('比对指纹'))
    expect(compare).toBeTruthy()
    await compare!.trigger('click')
    await flushPromises()

    expect(mockGetBacktestDetail).toHaveBeenCalledWith(1)
    expect(mockGetBacktestDetail).toHaveBeenCalledWith(2)
    const text = modal.text()
    expect(text).toContain('000001.SZ 1d')
    expect(text).toContain('一致')
    expect(text).toContain('000002.SZ 1d')
    expect(text).toContain('仅 B 有')
    w.unmount()
  })
})
