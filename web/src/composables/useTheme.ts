import { ref, type Ref } from 'vue'

export type Theme = 'light' | 'dark'

const STORAGE_KEY = 'theme'

/** 系统偏好：仅在没有已保存选择、也没有外部预设时作为首屏兜底 */
function systemTheme(): Theme {
  return window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'
}

function savedTheme(): Theme | null {
  try {
    const saved = localStorage.getItem(STORAGE_KEY)
    return saved === 'light' || saved === 'dark' ? saved : null
  } catch {
    return null
  }
}

/** 首屏前由外壳/内联脚本写入的预设（属性不存在时返回 null，不能当成 light） */
function presetTheme(): Theme | null {
  const preset = document.documentElement.dataset.theme
  return preset === 'light' || preset === 'dark' ? preset : null
}

/** 当前实际渲染的主题：以 <html data-theme> 为准，避免状态与界面脱节 */
function renderedTheme(): Theme {
  return document.documentElement.dataset.theme === 'dark' ? 'dark' : 'light'
}

function applyTheme(val: Theme) {
  document.documentElement.dataset.theme = val
}

/**
 * 模块级单例。切换直接同步写 DOM，不依赖 watch：
 * watch 建立在本模块首次被调用时的组件作用域里，一旦该组件重建（路由/热更新）会被自动停止，
 * 之后只会改 ref 不再改 DOM，表现为「按钮文字变了、页面风格不变」。
 */
const theme: Ref<Theme> = ref('light')
let initialized = false

/**
 * 首屏初始化：已保存的选择 > 外部预设 > 系统偏好。
 * 只写 DOM、不回写 localStorage，避免首次访问就把系统偏好固化成用户选择。
 */
export function initTheme(): Theme {
  const resolved: Theme = savedTheme() ?? presetTheme() ?? systemTheme()
  theme.value = resolved
  applyTheme(resolved)
  initialized = true
  return resolved
}

export function useTheme() {
  if (!initialized) initTheme()

  function toggle() {
    // 以真实渲染值为基准翻转：即使外部状态曾被改动，第一次点击也一定产生可见变化
    const next: Theme = renderedTheme() === 'light' ? 'dark' : 'light'
    applyTheme(next)
    theme.value = next
    try {
      localStorage.setItem(STORAGE_KEY, next)
    } catch {
      /* 隐私模式等写入失败不影响本次切换 */
    }
  }
  return { theme, toggle }
}

/** 仅测试用：重置单例与 DOM 标记 */
export function resetTheme() {
  initialized = false
  theme.value = 'light'
  delete document.documentElement.dataset.theme
}
