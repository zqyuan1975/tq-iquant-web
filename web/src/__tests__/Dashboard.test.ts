import { describe, it, expect, vi, beforeEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import Dashboard from '../views/Dashboard.vue'

const mocks = vi.hoisted(() => ({
  mockGetStockPools: vi.fn(),
  mockGetFormulas: vi.fn(),
  mockGetPortfolios: vi.fn(),
  mockGetBacktestRecords: vi.fn(),
  mockGetLiveSessions: vi.fn(),
  mockGetSystemConfigs: vi.fn(),
  mockGetSystemStatus: vi.fn(),
  mockGetLiveChannel: vi.fn(),
}))

vi.mock('../api', () => ({
  getStockPools: mocks.mockGetStockPools,
  getFormulas: mocks.mockGetFormulas,
  getPortfolios: mocks.mockGetPortfolios,
  getBacktestRecords: mocks.mockGetBacktestRecords,
  getLiveSessions: mocks.mockGetLiveSessions,
  getSystemConfigs: mocks.mockGetSystemConfigs,
  getSystemStatus: mocks.mockGetSystemStatus,
  getLiveChannel: mocks.mockGetLiveChannel,
}))

import {
  getStockPools, getFormulas, getPortfolios, getBacktestRecords,
  getLiveSessions, getSystemConfigs, getSystemStatus, getLiveChannel,
} from '../api'

const mockCfg = {
  tdx_path: 'D:\\new_tdx64',
  iquant_path: 'D:\\iquant',
  max_concurrent_backtest: 1,
  database: { sqlite_path: 'data/dev.db' },
  goldminer: { base_url: 'http://127.0.0.1:7801', token: '', account_id: '0e5343e4' },
  iquant_bridge: {
    simulation: { base_url: 'http://127.0.0.1:8790' },
    live: { base_url: 'http://127.0.0.1:8791' },
  },
}

beforeEach(() => {
  vi.clearAllMocks()
  ;(getStockPools as any).mockResolvedValue([{ id: 1, code: 'CS', name: '自选', synced_at: null, stock_count: 0 }])
  ;(getFormulas as any).mockResolvedValue([{ id: 1, name: '均线', content: '', formula_count: 1, created_at: null, updated_at: null, signals: [] }])
  ;(getPortfolios as any).mockResolvedValue([])
  ;(getBacktestRecords as any).mockResolvedValue([{
    id: 1, portfolio_strategy_id: 1, name: '回测A', start_date: '2026-01-01', end_date: '2026-01-31',
    status: 'completed', error_message: null, created_at: null, completed_at: null,
  }])
  ;(getLiveSessions as any).mockResolvedValue([])
  ;(getSystemConfigs as any).mockResolvedValue(mockCfg)
  ;(getSystemStatus as any).mockResolvedValue({ core: { online: true, version: '1.0', uptime: '0h0m' } })
  ;(getLiveChannel as any).mockImplementation((engine: string) => Promise.resolve({
    engine,
    label: engine === 'tdx' ? '通达信 TQ'
      : engine === 'ths' ? '同花顺'
      : engine === 'goldminer' ? '掘金量化' : 'QMT 通道',
    online: null,
  }))
})

describe('Dashboard.vue', () => {
  it('挂载后并行拉取各统计源并渲染统计卡', async () => {
    const w = mount(Dashboard)
    await flushPromises()

    expect(getStockPools).toHaveBeenCalled()
    expect(getFormulas).toHaveBeenCalled()
    expect(getPortfolios).toHaveBeenCalled()
    expect(getBacktestRecords).toHaveBeenCalled()
    expect(getLiveSessions).toHaveBeenCalled()
    expect(getSystemConfigs).toHaveBeenCalled()
    expect(getSystemStatus).toHaveBeenCalled()
    expect(getLiveChannel).toHaveBeenCalledTimes(4)   // tdx/ths/goldminer/iquant 四通道
    expect(getLiveChannel).toHaveBeenCalledWith('iquant')

    // 统计卡：股票池=1、公式=1、回测=1；系统卡在线；配置路径
    expect(w.text()).toContain('股票池')
    expect(w.text()).toContain('回测记录')
    expect(w.text()).toContain('Core 状态')
    expect(w.text()).toContain('在线')
    expect(w.text()).toContain('回测A')
    expect(w.text()).toContain('D:\\new_tdx64')
    expect(w.text()).toContain('http://127.0.0.1:7801')
    w.unmount()
  })

  it('四通道同一套版式：名称/状态/次要信息齐全，QMT 桥不可达显示离线红', async () => {
    ;(getLiveChannel as any).mockImplementation((engine: string) => Promise.resolve({
      engine,
      label: engine === 'tdx' ? '通达信 TQ'
        : engine === 'ths' ? '同花顺'
        : engine === 'goldminer' ? '掘金量化' : 'QMT 通道',
      online: engine === 'iquant' ? false : true,
      detail: engine === 'iquant' ? 'HTTP 桥 http://127.0.0.1:8790' : '次要信息',
      checked_at: '2026-11-11 10:57:32',
    }))
    const w = mount(Dashboard)
    await flushPromises()

    const cards = w.findAll('.channel-card')
    expect(cards).toHaveLength(4)
    expect(cards.map((c) => c.find('.metric-label').text())).toEqual(
      ['通达信 TQ', '同花顺', '掘金量化', 'QMT 通道'],
    )
    for (const c of cards) {
      expect(c.find('.channel-status').exists()).toBe(true)
      expect(c.find('.channel-meta').text()).not.toBe('')
    }
    // 前三个通道探活在线上线：同一套「在线」绿
    for (const c of cards.slice(0, 3)) {
      expect(c.find('.channel-status').text()).toBe('在线')
      expect(c.find('.channel-status').classes()).toContain('ok')
    }
    // QMT 桥不可达：唯一的另一套词「离线」红，不再有第三种表述
    const qmt = cards[3]
    expect(qmt.find('.channel-status').text()).toBe('离线')
    expect(qmt.find('.channel-status').classes()).toContain('bad')
    expect(qmt.find('.channel-meta').text()).toContain('127.0.0.1:8790')
    // 协议头在 200px 最小列宽下会挤掉后半句，统一剥离（原文保留在 title 悬浮提示里）
    expect(qmt.find('.channel-meta').text()).not.toContain('http://')
    expect(qmt.find('.channel-meta').text()).toContain('心跳 10:57:32')
    expect(qmt.find('.channel-meta').attributes('title')).toContain('8790')
    expect(qmt.text()).not.toContain('8791')
    expect(qmt.text()).not.toContain('未检测')
    w.unmount()
  })

  it('次要信息固定两行：桥地址去协议头一行、心跳单列一行（200px 列宽不截断）', async () => {
    ;(getLiveChannel as any).mockImplementation((engine: string) => Promise.resolve({
      engine,
      label: engine === 'tdx' ? '通达信 TQ'
        : engine === 'ths' ? '同花顺'
        : engine === 'goldminer' ? '掘金量化' : 'QMT 通道',
      online: false,
      detail: engine === 'goldminer' ? 'http://127.0.0.1:7801'
        : engine === 'iquant' ? 'HTTP 桥 http://127.0.0.1:8790'
        : engine === 'ths' ? '网上股票交易系统5.0' : 'TQ 资金账号 未配置',
      checked_at: '2026-11-11 11:27:00',
    }))
    const w = mount(Dashboard)
    await flushPromises()

    const cards = w.findAll('.channel-card')
    expect(cards).toHaveLength(4)
    const metas = cards.map((c) => c.find('.channel-meta'))
    // 主体行：不含协议头；掘金只留 host:port
    expect(metas[2].findAll('.meta-line')[0].text()).toBe('127.0.0.1:7801')
    expect(metas[2].text()).not.toContain('http://')
    expect(metas[3].findAll('.meta-line')[0].text()).toBe('HTTP 桥 127.0.0.1:8790')
    // 第二行固定是心跳，四卡同一口径
    for (const m of metas) {
      const lines = m.findAll('.meta-line')
      expect(lines).toHaveLength(2)
      expect(lines[1].text()).toBe('心跳 11:27:00')
      expect(lines[1].classes()).toContain('meta-sub')
    }
    w.unmount()
  })

  it('旧接口 online=null/无 detail → 统一离线红，次要信息降级为「—」', async () => {
    const w = mount(Dashboard)
    await flushPromises()
    const cards = w.findAll('.channel-card')
    expect(cards).toHaveLength(4)
    for (const c of cards) {
      expect(c.find('.channel-meta').text()).toBe('—')
      expect(c.find('.channel-status').text()).toBe('离线')
      expect(c.find('.channel-status').classes()).toContain('bad')
    }
    w.unmount()
  })

  it('通道接口慢 → 主区块先渲染，通道卡片 5s 后出超时占位', async () => {
    vi.useFakeTimers()
    ;(getLiveChannel as any).mockImplementation(() => new Promise(() => {}))  // 永不返回
    const w = mount(Dashboard)
    await vi.advanceTimersByTimeAsync(10)

    // 主数据（<100ms 的真实接口）已渲染，不被通道探活 gate 卡住
    expect(w.text()).toContain('核心指标')
    expect(w.text()).toContain('最近回测')
    expect(w.text()).toContain('加载中…')   // 通道卡片独立占位

    await vi.advanceTimersByTimeAsync(5000)
    // 超时不再造第三种状态词：统一按「离线」展示，原因写在次要信息
    expect(w.text()).toContain('离线')
    expect(w.text()).toContain('探测超时，按离线显示')

    w.unmount()
    vi.useRealTimers()
  })

  it('某查询失败 → 其余卡片仍渲染,仅提示部分失败', async () => {
    ;(getLiveSessions as any).mockRejectedValue(new Error('network down'))
    const w = mount(Dashboard)
    await flushPromises()

    expect(w.text()).toContain('部分数据加载失败')
    expect(w.text()).toContain('回测记录')  // 其余卡片不受影响
    expect(w.text()).toContain('在线')
    w.unmount()
  })

  it('对账差异 tooltip 标注 computed_at；旧数据缺字段时降级不报错', async () => {
    ;(getLiveSessions as any).mockResolvedValue([
      {
        id: 1, name: '实盘A', mode: 'live', status: 'running', engine: 'tdx',
        execution_mode: 'auto', started_at: null, stopped_at: null, portfolio_ids: [1],
        reconcile_mismatches: [{
          code: '002745.SZ', virtual: 0, real: 5300, diff: -5300,
          computed_at: '2026-11-05T11:41:18+08:00',
        }],
      },
      {
        id: 2, name: '实盘B', mode: 'live', status: 'running', engine: 'tdx',
        execution_mode: 'auto', started_at: null, stopped_at: null, portfolio_ids: [1],
        reconcile_mismatches: [{ code: '600000.SH', virtual: 100, real: 0, diff: 100 }],
      },
    ])
    const w = mount(Dashboard)
    await flushPromises()

    expect(w.html()).toContain('算于 11:41:18')
    expect(w.text()).toContain('1 项')
    w.unmount()
  })

  it('Hero 保留统计与刷新，已移除品牌标题', async () => {
    const w = mount(Dashboard)
    await flushPromises()
    expect(w.find('.dash-hero').exists()).toBe(true)
    expect(w.find('.dash-hero-stats').exists()).toBe(true)
    expect(w.find('.dash-hero-stats').text()).toContain('股票池')
    expect(w.find('.dash-hero-actions').find('button').text()).toContain('刷新')
    expect(w.find('.dash-brand').exists()).toBe(false)
    expect(w.find('.dash-brand-sub').exists()).toBe(false)
    w.unmount()
  })
})
