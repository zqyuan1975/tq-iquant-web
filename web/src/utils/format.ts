export const DASH = '—'

type Num = number | string | null | undefined

function toNum(v: Num): number | null {
  if (v === null || v === undefined) return null
  const n = typeof v === 'string' ? Number(v) : v
  return Number.isFinite(n) ? n : null
}

function quantize(v: Num, places: number): number | null {
  const n = toNum(v)
  if (n === null) return null
  const f = Math.pow(10, places)
  return Math.round((n + Number.EPSILON) * f) / f
}

export function fmtMoney(v: Num): string {
  const n = quantize(v, 2)
  if (n === null) return DASH
  return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

export function fmtPrice(v: Num): string {
  const n = quantize(v, 3)
  if (n === null) return DASH
  return n.toFixed(3)
}

export function fmtQty(v: Num): string {
  const n = quantize(v, 0)
  if (n === null) return DASH
  return Math.round(n).toLocaleString('en-US')
}

export function fmtPct(v: Num, digits = 2): string {
  const n = toNum(v)
  if (n === null) return DASH
  const q = quantize(n * 100, digits)
  if (q === null) return DASH
  return q.toFixed(digits) + '%'
}

export function fmtSignedPct(v: Num, digits = 2): string {
  const n = toNum(v)
  if (n === null) return DASH
  const s = fmtPct(Math.abs(n), digits)
  if (s === DASH) return DASH
  return (n >= 0 ? '+' : '-') + s
}

export function fmtRatio(v: Num, digits = 2): string {
  const n = quantize(v, digits)
  if (n === null) return DASH
  return n.toFixed(digits)
}

/** 字节数 → 人类可读（B/KB/MB/GB/TB）。 */
export function fmtBytes(v: Num): string {
  const n = toNum(v)
  if (n === null || n < 0) return DASH
  if (n < 1024) return `${Math.round(n)} B`
  const units = ['KB', 'MB', 'GB', 'TB']
  let val = n / 1024
  let i = 0
  while (val >= 1024 && i < units.length - 1) {
    val /= 1024
    i += 1
  }
  return `${val.toFixed(val >= 100 ? 0 : 1)} ${units[i]}`
}