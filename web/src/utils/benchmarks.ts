// 基准指数可选集（TQ 指数日线可取，与回测页基准 guard 同集合）。
// 000001.SH（上证指数）置首位：市场多空门控默认基准 + 组合新建默认基准
// （通达信可自动下载日线；中证全指 000985.CSI 需手工下载，缺数据会导致门控算不出来）。
export interface BenchmarkOption {
  code: string
  name: string
}

export const DEFAULT_BENCHMARK = '000001.SH'

export const BENCHMARKS: BenchmarkOption[] = [
  { code: '000001.SH', name: '上证指数' },
  { code: '000985.CSI', name: '中证全指' },
  { code: '000300.SH', name: '沪深300' },
  { code: '000905.SH', name: '中证500' },
  { code: '000852.SH', name: '中证1000' },
  { code: '000688.SH', name: '科创50' },
  { code: '399006.SZ', name: '创业板指' },
  { code: '000001.SH', name: '上证指数' },
  { code: '399001.SZ', name: '深证成指' },
]
