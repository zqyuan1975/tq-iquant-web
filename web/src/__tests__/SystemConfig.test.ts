import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'

// SystemConfig.vue 走 ../api 客户端（getSystemConfigs/updateSystemConfigs）
const {
  mockGetSystemConfigs, mockUpdateSystemConfigs, mockTestLiveChannel,
  mockGetBackupStatus, mockCreateBackup,
} = vi.hoisted(() => ({
  mockGetSystemConfigs: vi.fn(),
  mockUpdateSystemConfigs: vi.fn(),
  mockTestLiveChannel: vi.fn(),
  mockGetBackupStatus: vi.fn(),
  mockCreateBackup: vi.fn(),
}))
vi.mock('../api', () => ({
  getSystemConfigs: mockGetSystemConfigs,
  updateSystemConfigs: mockUpdateSystemConfigs,
  testLiveChannel: mockTestLiveChannel,
  getBackupStatus: mockGetBackupStatus,
  createBackup: mockCreateBackup,
}))

import SystemConfig from '../views/SystemConfig.vue'
import {
  getSystemConfigs, updateSystemConfigs, testLiveChannel,
  getBackupStatus, createBackup,
} from '../api'

// 与 config.yaml / core/config.py 一致的真实字段
const mockConfig = {
  tdx_path: 'D:\\zd_zyb',
  iquant_path: 'D:\\iquant',
  max_concurrent_backtest: 1,
  database: { sqlite_path: 'data/dev.db' },
  backup: {
    enabled: true,
    daily_at: '15:10',
    keep: 10,
    min_free_mb: 500,
    dir: 'data/backup',
  },
  iquant_bridge: {
    simulation: { base_url: 'http://127.0.0.1:8790' },
    live: { base_url: 'http://127.0.0.1:8791' },
  },
  ths_ui: {
    enabled: false,
    window_title: '网上股票交易系统5.0',
    client_exe_path: 'D:/同花顺远航版/transaction/xiadan.exe',
    driver: 'clipboard',
    captcha_ocr: false,
    execute_mode: 'file_builtin',
    execute_interval_seconds: 5,
    work_dir: 'data/ths',
  },
  tq_trade: {
    account: '88866666',
    account_type: 'stock',
    price_mode: 'my',
    client_path: 'D:\\zd_zyb',
    default_engine: 'tdx',
    cancel_unfilled_minutes: 3,
  },
}

// 内嵌数据备份状态（无历史记录时的初始态）
const emptyBackupStatus = {
  enabled: true,
  daily_at: '15:10',
  keep: 10,
  min_free_mb: 500,
  backup_dir: 'D:/project/tq-iquant-web/main/data/backup',
  last_auto: null,
  last_manual: null,
  auto_done_today: false,
}

beforeEach(() => {
  vi.clearAllMocks()
  ;(getSystemConfigs as any).mockResolvedValue(mockConfig)
  ;(updateSystemConfigs as any).mockResolvedValue(null)
  ;(testLiveChannel as any).mockResolvedValue({ ok: true, channel: 'ths_ui', positions: 3 })
  ;(getBackupStatus as any).mockResolvedValue(emptyBackupStatus)
  ;(createBackup as any).mockResolvedValue({
    filename: 'dev-manual-20261110-153000.db', status: 'success',
  })
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('SystemConfig.vue', () => {
  it('挂载后 GET /api/system/configs 并渲染各字段值', async () => {
    const w = mount(SystemConfig)
    await flushPromises()

    expect(getSystemConfigs).toHaveBeenCalled()
    const inputs = w.findAll('input')
    const v = (el: any) => (el.element as HTMLInputElement).value
    expect(v(inputs.find(i => i.attributes('placeholder')?.includes('tdx')))).toBe('D:\\zd_zyb')
    expect(v(inputs.find(i => i.attributes('placeholder')?.includes('dev.db')))).toBe('data/dev.db')
    expect(v(inputs.find(i => i.attributes('placeholder')?.includes('7801')))).toBe('http://127.0.0.1:7801')
    expect(w.text()).toContain('最大并发回测')
    w.unmount()
  })

  it('渲染同花顺 UI 通道配置并可只读测试连接', async () => {
    const w = mount(SystemConfig)
    await flushPromises()

    expect(w.text()).toContain('同花顺 UI 下单通道')
    const inputs = w.findAll('input')
    const clientInput = inputs.find(i => i.attributes('placeholder')?.includes('xiadan.exe'))
    expect((clientInput!.element as HTMLInputElement).value).toBe('D:/同花顺远航版/transaction/xiadan.exe')

    await w.findAll('button').find(b => b.text().includes('测试连接（只读）'))!.trigger('click')
    await flushPromises()

    expect(testLiveChannel).toHaveBeenCalledWith('ths_ui')
    expect(w.text()).toContain('连接成功，读到 3 条持仓')
    w.unmount()
  })

  it('不渲染已废弃的 database.host / database.port 字段', async () => {
    const w = mount(SystemConfig)
    await flushPromises()

    expect(w.text()).not.toContain('数据库主机')
    expect(w.text()).not.toContain('数据库端口')
    w.unmount()
  })

  it('渲染 TQ 通道默认引擎下拉与超时撤单分钟数（A-6/A-7）', async () => {
    const w = mount(SystemConfig)
    await flushPromises()

    expect(w.text()).toContain('默认交易引擎')
    expect(w.text()).toContain('超时撤单(分钟)')
    // 默认引擎 select 选中 tdx
    const selects = w.findAll('select')
    const engineSelect = selects.find(s => s.findAll('option').some(o => o.element.value === 'tdx'))
    expect(engineSelect).toBeTruthy()
    expect((engineSelect!.element as HTMLSelectElement).value).toBe('tdx')
    // 超时撤单 number 输入回显 3
    const numInput = w.findAll('input').find(i => i.attributes('type') === 'number' && (i.element as HTMLInputElement).value === '3')
    expect(numInput).toBeTruthy()
    w.unmount()
  })

  it('盘前集合竞价独立分组：标题含「全引擎生效」、开关有「启用」字样（A-8）', async () => {
    const w = mount(SystemConfig)
    await flushPromises()

    // 独立分组标题明确全引擎生效
    expect(w.text()).toContain('盘前集合竞价（全引擎生效）')
    // 勾选框有显式「启用」标签（不是裸勾选框）
    const checkbox = w.findAll('input').find(i => i.attributes('type') === 'checkbox'
      && ((i.element as HTMLInputElement).closest('label')?.textContent || '').includes('启用盘前集合竞价'))
    expect(checkbox).toBeTruthy()
    // 已从通达信 TQ 分组移出：TQ 分组内不再出现「盘前」
    const tqSection = w.findAll('section').find(s => s.find('h3').exists()
      && s.find('h3').text().includes('通达信 TQ 交易通道'))
    expect(tqSection).toBeTruthy()
    expect(tqSection!.text()).not.toContain('盘前')
    w.unmount()
  })

  it('渲染同花顺文件交接层三字段（执行模式/轮询间隔/工作目录）', async () => {
    const w = mount(SystemConfig)
    await flushPromises()

    expect(w.text()).toContain('执行模式')
    expect(w.text()).toContain('执行器轮询间隔')
    expect(w.text()).toContain('交接文件工作目录')
    // 执行模式 select 选中 file_builtin，含 direct 选项
    const selects = w.findAll('select')
    const modeSelect = selects.find(s => s.findAll('option').some(o => o.element.value === 'file_builtin'))
    expect(modeSelect).toBeTruthy()
    expect((modeSelect!.element as HTMLSelectElement).value).toBe('file_builtin')
    expect(modeSelect!.findAll('option').map(o => o.element.value)).toContain('direct')
    // 轮询间隔 number 回显 5；工作目录回显 data/ths
    const intervalInput = w.findAll('input').find(
      i => i.attributes('type') === 'number' && (i.element as HTMLInputElement).value === '5')
    expect(intervalInput).toBeTruthy()
    const workDirInput = w.findAll('input').find(i => (i.element as HTMLInputElement).value === 'data/ths')
    expect(workDirInput).toBeTruthy()
    // hint 说明 {work_dir}/{session_id}/ 目录结构
    expect(w.text()).toContain('{work_dir}/{session_id}/')
    w.unmount()
  })

  it('保存 → PUT 提交与 config.yaml 一致的完整配置并提示已保存', async () => {
    const w = mount(SystemConfig)
    await flushPromises()

    await w.findAll('button').find(b => b.text().includes('保存'))!.trigger('click')
    await flushPromises()

    expect(updateSystemConfigs).toHaveBeenCalledWith(expect.objectContaining({
      tdx_path: 'D:\\zd_zyb',
      iquant_path: 'D:\\iquant',
      max_concurrent_backtest: 1,
      database: expect.objectContaining({ sqlite_path: 'data/dev.db' }),
      backup: expect.objectContaining({
        enabled: true,
        daily_at: '15:10',
        keep: 10,
        min_free_mb: 500,
      }),
      iquant_bridge: expect.objectContaining({
        simulation: expect.objectContaining({ base_url: 'http://127.0.0.1:8790' }),
        live: expect.objectContaining({ base_url: 'http://127.0.0.1:8791' }),
      }),
      ths_ui: expect.objectContaining({
        enabled: false,
        window_title: '网上股票交易系统5.0',
        client_exe_path: 'D:/同花顺远航版/transaction/xiadan.exe',
        execute_mode: 'file_builtin',
        execute_interval_seconds: 5,
        work_dir: 'data/ths',
      }),
      tq_trade: expect.objectContaining({
        account: '88866666',
        default_engine: 'tdx',
        cancel_unfilled_minutes: 3,
      }),
    }))
    expect(w.text()).toContain('已保存')
    w.unmount()
  })

  it('保存失败 → 显示错误信息,不显示已保存', async () => {
    ;(updateSystemConfigs as any).mockRejectedValue(new Error('network down'))
    const w = mount(SystemConfig)
    await flushPromises()

    await w.findAll('button').find(b => b.text().includes('保存'))!.trigger('click')
    await flushPromises()

    expect(w.text()).not.toContain('已保存')
    expect(w.text()).toContain('保存失败')
    w.unmount()
  })

  it('GET 失败 → 显示加载失败信息,页面不崩', async () => {
    ;(getSystemConfigs as any).mockRejectedValue(new Error('连接被拒绝'))
    const w = mount(SystemConfig)
    await flushPromises()

    expect(w.text()).toContain('加载配置失败')
    // 模板仍安全渲染(默认值),无抛错
    expect(w.findAll('input').length).toBeGreaterThan(0)
    w.unmount()
  })

  it('渲染数据备份区块：上次自动/手动备份时间与大小、今日已自动备份提示', async () => {
    ;(getBackupStatus as any).mockResolvedValue({
      ...emptyBackupStatus,
      // 启动晚于 15:10 → 补备逻辑实际在 19:42 执行；标签必须显示实际时间而非配置点位
      last_auto: {
        id: 11, kind: 'auto', status: 'success',
        filename: 'dev-20261110-194234.db', path: '/data/backup/dev-20261110-194234.db',
        size_bytes: 3 * 1024 * 1024, message: null,
        started_at: '2026-11-10T19:42:34', finished_at: '2026-11-10T19:42:36',
      },
      last_manual: {
        id: 12, kind: 'manual', status: 'success',
        filename: 'dev-manual-20261110-152000.db', path: '/data/backup/dev-manual-20261110-152000.db',
        size_bytes: 2048, message: null,
        started_at: '2026-11-10T15:20:00', finished_at: '2026-11-10T15:20:01',
      },
      auto_done_today: true,
    })
    const w = mount(SystemConfig)
    await flushPromises()

    expect(getBackupStatus).toHaveBeenCalled()
    expect(w.text()).toContain('数据备份')
    expect(w.text()).toContain('上次自动备份')
    expect(w.text()).toContain('2026-11-10 19:42:34 / 3.0 MB')
    expect(w.text()).toContain('上次手动备份')
    expect(w.text()).toContain('2026-11-10 15:20:00 / 2.0 KB')
    // 标签取当日自动备份记录的实际时间，而非配置的 daily_at(15:10)
    expect(w.text()).toContain('今日已自动备份（19:42）')
    expect(w.text()).not.toContain('今日已自动备份（15:10）')
    expect(w.text()).toContain('D:/project/tq-iquant-web/main/data/backup')
    // 自动备份配置字段回显（保存走同一表单）
    const dailyInput = w.findAll('input').find(
      i => (i.element as HTMLInputElement).value === '15:10')
    expect(dailyInput).toBeTruthy()
    w.unmount()
  })

  it('无历史备份 → 显示「暂无」且不显示今日已自动备份提示', async () => {
    const w = mount(SystemConfig)
    await flushPromises()

    expect(w.text()).toContain('暂无')
    expect(w.text()).not.toContain('今日已自动备份')
    w.unmount()
  })

  it('当日无自动备份记录（上次自动备份是昨天） → 不显示今日已自动备份提示', async () => {
    ;(getBackupStatus as any).mockResolvedValue({
      ...emptyBackupStatus,
      last_auto: {
        id: 9, kind: 'auto', status: 'success',
        filename: 'dev-20261109-151000.db', path: '/data/backup/dev-20261109-151000.db',
        size_bytes: 3 * 1024 * 1024, message: null,
        started_at: '2026-11-09T15:10:00', finished_at: '2026-11-09T15:10:02',
      },
      auto_done_today: false,
    })
    const w = mount(SystemConfig)
    await flushPromises()

    expect(w.text()).toContain('2026-11-09 15:10:00 / 3.0 MB')
    expect(w.text()).not.toContain('今日已自动备份')
    w.unmount()
  })

  it('「立即备份」调用手动备份接口并刷新备份状态', async () => {
    const w = mount(SystemConfig)
    await flushPromises()
    expect(getBackupStatus).toHaveBeenCalledTimes(1)

    await w.findAll('button').find(b => b.text().includes('立即备份'))!.trigger('click')
    await flushPromises()

    expect(createBackup).toHaveBeenCalledTimes(1)
    expect(getBackupStatus).toHaveBeenCalledTimes(2)
    expect(w.text()).toContain('备份完成：dev-manual-20261110-153000.db')
    w.unmount()
  })

  it('立即备份失败（如磁盘不足） → 显示后端错误信息且不显示完成', async () => {
    ;(createBackup as any).mockRejectedValue(new Error('磁盘剩余空间不足，已跳过本次手动备份'))
    const w = mount(SystemConfig)
    await flushPromises()

    await w.findAll('button').find(b => b.text().includes('立即备份'))!.trigger('click')
    await flushPromises()

    expect(w.text()).toContain('备份失败')
    expect(w.text()).toContain('磁盘剩余空间不足')
    expect(w.text()).not.toContain('备份完成')
    w.unmount()
  })
})
