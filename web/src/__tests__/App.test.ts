import { describe, it, expect, vi, beforeEach, afterEach, type Mock } from 'vitest'
import { mount, flushPromises } from '@vue/test-utils'
import { reactive } from 'vue'
import { ICONS } from '../icons'
import { resetTheme } from '../composables/useTheme'
import { NAV_RESELECT_EVENT } from '../utils/navReselect'

// 路由 mock：App.vue 调 useRoute() 且模板用 <router-link>，
// 不装 router 插件时 useRoute() 返回 undefined 会直接抛错。
const routeState = reactive({ path: '/dashboard', name: 'dashboard' })
vi.mock('vue-router', () => ({
  useRoute: () => routeState,
  RouterLink: {
    props: ['to'],
    template: '<a class="router-link-stub" :href="to"><slot /></a>',
  },
}))

vi.mock('../api', () => ({
  getSystemStatus: vi.fn().mockResolvedValue({
    core: { online: true, version: '1.0', uptime: '0h53m' },
  }),
}))

import App from '../App.vue'

const KEYS = ['dashboard', 'pools', 'formula', 'portfolio', 'backtest', 'live', 'health', 'system', 'sun', 'moon', 'search']

describe('icons.ts', () => {
  it('导出 11 个图标 key，值均为合法 path d 字符串', () => {
    expect(Object.keys(ICONS).sort()).toEqual([...KEYS].sort())
    for (const k of KEYS) {
      expect(typeof ICONS[k]).toBe('string')
      expect(ICONS[k].length).toBeGreaterThan(4)
      expect(ICONS[k]).toMatch(/^[MmLlHhVvCcSsQqTtAaZz0-9eE\s,.\-+]+$/)
    }
  })

  it('无 Emoji 混入（专业终端不用 Emoji 图标）', () => {
    for (const k of KEYS) {
      expect(ICONS[k]).not.toMatch(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u)
    }
  })
})

describe('App.vue — 图标渲染', () => {
  beforeEach(() => {
    resetTheme()
    localStorage.clear()
  })

  it('侧边栏 12 个导航项各渲染一个内联 SVG 图标（非 Emoji 文本节点）', async () => {
    const w = mount(App)
    await flushPromises()
    const items = w.findAll('.sidebar-nav .nav-item')
    expect(items).toHaveLength(12)
    items.forEach((it) => {
      const svg = it.find('svg.icon')
      expect(svg.exists()).toBe(true)
      expect(svg.findAll('path')).toHaveLength(1)
      expect(svg.text()).toBe('')
    })
    expect(w.text()).not.toMatch(/[\u{1F300}-\u{1FAFF}\u{2600}-\u{27BF}]/u)
  })
})

describe('App.vue — 外壳结构', () => {
  beforeEach(() => {
    resetTheme()
    localStorage.clear()
  })

  it('侧边栏导航分三组（研究/交易/系统）并有组标签', async () => {
    const w = mount(App)
    await flushPromises()
    const labels = w.findAll('.nav-group-label').map((e) => e.text())
    expect(labels).toEqual(['研究', '交易', '系统'])
    expect(w.findAll('.nav-group-label')).toHaveLength(3)
  })

  it('主题切换按钮在侧边栏底部（与 Core 状态同区）', async () => {
    const w = mount(App)
    await flushPromises()
    const foot = w.find('.sidebar-footbar')
    expect(foot.exists()).toBe(true)
    expect(foot.find('.theme-toggle').exists()).toBe(true)
    expect(w.find('.sidebar-footer').exists()).toBe(false)
  })

  it('Core 在线时状态点为 online 且显示版本与运行时长', async () => {
    const w = mount(App)
    await flushPromises()
    const dot = w.find('.status-dot')
    expect(dot.exists()).toBe(true)
    expect(dot.classes()).toContain('online')
    expect(w.find('.status-chip').text()).toContain('Core 在线')
    expect(w.find('.status-chip').text()).toContain('v1.0')
    expect(w.find('.status-chip').text()).toContain('0h53m')
  })

  it('顶栏横幅显示平台大标题（居中双色）', async () => {
    const w = mount(App)
    await flushPromises()
    const banner = w.find('.topbar .app-banner')
    expect(banner.exists()).toBe(true)
    expect(banner.text()).toContain('创懿私募基金量化投资平台')
  })
})

describe('App.vue — 左侧导航重复点击回入口页', () => {
  let spy: Mock<(e: Event) => void>
  // addEventListener 需要 (e: Event) => void 的具名函数，vi.fn 的 Mock 类型
  // 不满足 EventListener 重载（TS2769），用包一层的 handler 保持类型正确。
  let handler: (e: Event) => void

  beforeEach(() => {
    resetTheme()
    localStorage.clear()
    routeState.path = '/dashboard'
    routeState.name = 'dashboard'
    spy = vi.fn()
    handler = (e: Event) => spy(e)
    window.addEventListener(NAV_RESELECT_EVENT, handler)
  })

  afterEach(() => {
    window.removeEventListener(NAV_RESELECT_EVENT, handler)
  })

  async function clickNav(label: string) {
    const w = mount(App)
    await flushPromises()
    const item = w.findAll('.sidebar-nav .nav-item').find((i) => i.text().includes(label))!
    expect(item.exists()).toBe(true)
    await item.trigger('click')
    w.unmount()
  }

  it('已在回测管理路由时点击「回测管理」→ 派发重选事件（供详情页回列表）', async () => {
    routeState.path = '/backtest'
    routeState.name = 'backtest'
    await clickNav('回测管理')
    expect(spy).toHaveBeenCalledTimes(1)
    expect((spy.mock.calls[0][0] as CustomEvent).detail).toEqual({ to: '/backtest' })
  })

  it('在其它路由点击「回测管理」→ 不派发事件（保持正常跳转）', async () => {
    routeState.path = '/dashboard'
    routeState.name = 'dashboard'
    await clickNav('回测管理')
    expect(spy).not.toHaveBeenCalled()
  })

  it('已在其它菜单路由时重复点击该菜单项 → 不派发事件（不影响其它菜单项）', async () => {
    routeState.path = '/live'
    routeState.name = 'live'
    await clickNav('实盘交易')
    expect(spy).not.toHaveBeenCalled()
  })
})

