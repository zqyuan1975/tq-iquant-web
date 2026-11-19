/**
 * 左侧导航「当前已在该路由」时再次点击的重选事件。
 *
 * 同址 router-link 跳转会被 vue-router 忽略，页面内的二级视图（如回测详情）
 * 不会因此回到入口页。App 在检测到「重复点击当前菜单项」时派发本事件，
 * 由需要响应的视图自行重置（目前只有回测管理：详情 → 列表）。
 */
export const NAV_RESELECT_EVENT = 'app:nav-reselect'

interface NavReselectDetail { to: string }

export function emitNavReselect(to: string): void {
  window.dispatchEvent(new CustomEvent<NavReselectDetail>(NAV_RESELECT_EVENT, { detail: { to } }))
}

/** 订阅重选事件，返回取消订阅函数（组件卸载时调用） */
export function onNavReselect(handler: (to: string) => void): () => void {
  const listener = (e: Event) => {
    const detail = (e as CustomEvent<NavReselectDetail>).detail
    handler(detail?.to ?? '')
  }
  window.addEventListener(NAV_RESELECT_EVENT, listener)
  return () => window.removeEventListener(NAV_RESELECT_EVENT, listener)
}
