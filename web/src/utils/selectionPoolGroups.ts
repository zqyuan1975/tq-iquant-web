/**
 * 条件选股池的共享分组定义。
 *
 * 全站所有涉及「池」的列表、下拉与徽标都必须引用本文件，避免各页面各写一套
 * 顺序/名称/提示而逐渐不一致。三组固定顺序：
 *   因子风格 → 行业·一级（互斥）→ 行业·二级标签（可重叠）。
 */

export const STYLE_POOL_KEYS = ['growth', 'quality', 'value', 'dividend'] as const

export type PoolGroupKey = 'style' | 'industry-l1' | 'industry-l2' | 'other'

export interface PoolGroupDef {
  key: PoolGroupKey
  label: string
  hint: string
  /** 排序权重，数字小的在前 */
  order: number
  /** 用于徽标/分组条的语义色，具体颜色由页面 CSS 变量决定 */
  tone: string
}

export const POOL_GROUPS: PoolGroupDef[] = [
  {
    key: 'style', label: '因子风格', hint: '四风格池，彼此独立',
    order: 0, tone: 'style',
  },
  {
    key: 'industry-l1', label: '行业·一级', hint: '互斥主池，一只票只归一个',
    order: 1, tone: 'industry-l1',
  },
  {
    key: 'industry-l2', label: '行业·二级标签',
    hint: '可重叠，一只票可同时属于多个标签',
    order: 2, tone: 'industry-l2',
  },
]

/** 未识别池（历史遗留 / 手工命名）兜底分组，始终排在最后。 */
export const OTHER_GROUP: PoolGroupDef = {
  key: 'other', label: '其他', hint: '未纳入三组分类的历史池',
  order: 99, tone: 'other',
}

export const POOL_LABELS: Record<string, string> = {
  growth: '成长', quality: '质量', value: '价值', dividend: '红利',
  'industry:cycle': '行业·周期', 'industry:consumer': '行业·消费',
  'industry:tech': '行业·科技', 'industry:pharma': '行业·医药',
  'industry:manufacturing': '行业·高端制造', 'industry:financial': '行业·金融地产',
  'industry:utility': '行业·公用事业', 'industry:agriculture': '行业·农业',
  'industry:other': '行业·其他综合',
  'industry:l2:staple_consumer': '标签·必选消费',
  'industry:l2:discretionary_consumer': '标签·可选消费',
  'industry:l2:brand': '标签·品牌',
  'industry:l2:upstream_resource': '标签·上游资源',
  'industry:l2:midstream_material': '标签·中游材料',
  'industry:l2:capacity_cycle': '标签·产能周期',
  'industry:l2:high_rd': '标签·高研发',
  'industry:l2:high_dividend_defense': '标签·高股息防御',
  'industry:l2:financial_standalone': '标签·金融单列',
  'industry:l2:realestate_chain': '标签·地产链',
  'industry:l2:defense': '标签·军工',
  'industry:l2:platform': '标签·平台',
  'industry:l2:transport_cycle': '标签·交运周期',
  'industry:l2:agri_cycle': '标签·农产品周期',
}

export function poolLabel(key: string | null | undefined): string {
  if (!key) return '—'
  return POOL_LABELS[key] || key
}

export function poolGroupKey(poolKey: string | null | undefined): PoolGroupKey | null {
  if (!poolKey) return null
  if ((STYLE_POOL_KEYS as readonly string[]).includes(poolKey)) return 'style'
  if (poolKey.startsWith('industry:l2:')) return 'industry-l2'
  if (poolKey.startsWith('industry:')) return 'industry-l1'
  return null
}

export interface PoolGroup<T> {
  def: PoolGroupDef
  items: T[]
}

/**
 * 按三组顺序切分任意池列表；组内保持传入顺序（即后端返回顺序）。
 * ``includeOther`` 为真时把无法归类的条目收进「其他」，避免行凭空消失。
 * ``keepEmpty`` 为真时三组恒定输出（空组 items=[]），让各页面组头结构一致，
 * 不因当前数据只覆盖部分组而少显示组头。
 */
export function groupPoolItems<T>(
  items: T[],
  keyOf: (item: T) => string | null | undefined,
  includeOther = true,
  keepEmpty = false,
): PoolGroup<T>[] {
  const buckets = new Map<PoolGroupKey, T[]>()
  const other: T[] = []
  for (const item of items) {
    const group = poolGroupKey(keyOf(item))
    if (!group) {
      other.push(item)
      continue
    }
    const bucket = buckets.get(group)
    if (bucket) bucket.push(item)
    else buckets.set(group, [item])
  }
  const out: PoolGroup<T>[] = []
  for (const def of [...POOL_GROUPS].sort((a, b) => a.order - b.order)) {
    const rows = buckets.get(def.key)
    if (rows && rows.length) out.push({ def, items: rows })
    else if (keepEmpty) out.push({ def, items: [] })
  }
  if (includeOther && other.length) out.push({ def: OTHER_GROUP, items: other })
  return out
}

/** 下拉分组标签：二级标签显式注明可重叠。 */
export function optgroupLabel(def: PoolGroupDef): string {
  return def.key === 'industry-l2' ? `${def.label}（可重叠）` : def.label
}

/**
 * 由池代码反推 pool_key（池清单接口只回 code）。
 * 序列池 ``SEL_<TAG>``；版本池 ``SEL_<TAG>_<YYYYMMDD>``；
 * TAG = pool_key 大写、冒号换下划线。按标签长度倒序匹配避免前缀误判。
 */
const CODE_TAGS: { tag: string; key: string }[] = Object.keys(POOL_LABELS)
  .map((key) => ({ key, tag: key.toUpperCase().replace(/:/g, '_') }))
  .sort((a, b) => b.tag.length - a.tag.length)

export function poolKeyFromCode(code: string | null | undefined): string | null {
  if (!code) return null
  const body = code.toUpperCase().replace(/^SEL_/, '')
  for (const { tag, key } of CODE_TAGS) {
    if (body === tag) return key
    if (body.startsWith(tag + '_') && /^\d{8}$/.test(body.slice(tag.length + 1))) {
      return key
    }
  }
  return null
}
