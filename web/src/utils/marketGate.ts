// 市场多空门控（TQGATE 公式）前端公用的枚举与展示口径。
// 后端方向：-1 空头 / 0 未定 / 1 多头 / 2 多头（强势）。

export type GateTone = 'bear' | 'bull' | 'neutral'

export function gateDirectionLabel(direction: number | null | undefined): string {
  if (direction === -1) return '空头'
  if (direction === 1) return '多头'
  if (direction === 2) return '多头（强势）'
  return '未定'
}

export function gateDirectionTone(direction: number | null | undefined): GateTone {
  if (direction === -1) return 'bear'
  if (direction === 1 || direction === 2) return 'bull'
  return 'neutral'
}

// 门控判别引擎（与后端 VALID_MARKET_GATE_ENGINES 同集合）
export const MARKET_GATE_ENGINES = [
  { value: 'python', label: '内置公式（python，默认）' },
  { value: 'tdx', label: '通达信公式库（tdx）' },
]

// 回测 run 级三态覆盖：空串=跟随组合配置，提交时展开为布尔
export const MARKET_GATE_OVERRIDES = [
  { value: '', label: '跟随组合' },
  { value: 'true', label: '强制开启' },
  { value: 'false', label: '强制关闭' },
]
