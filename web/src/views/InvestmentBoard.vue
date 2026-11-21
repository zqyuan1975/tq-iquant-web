<script setup lang="ts">
// 投资看板（只读）：市场多空状态（整体迁移自备选股池）+ 128 个 881 二级行业
// 相对大盘的多头先后 + 行业成分股相对行业指数的领先 + 日线/30m 双视图。
// 不接下单、不进 market_gate、不改变任何策略与风控行为。
import { computed, onMounted, ref } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import MarketGatePanel from '../components/board/MarketGatePanel.vue'
import {
  getBoardIndustries, getBoardIndustry, getBoardIndustryStocks,
  getBoardLeaders, getBoardMeta, getBoardRun, getBoardSummary, refreshBoard,
  type BoardIndustryItem, type BoardMeta, type BoardRunInfo, type BoardStockItem,
} from '../api'
import { gateDirectionTone } from '../utils/marketGate'

type Period = '1d' | '30m'

const period = ref<Period>('1d')
const meta = ref<BoardMeta | null>(null)
const levelCounts = ref<Record<string, number>>({})
const industries = ref<BoardIndustryItem[]>([])
const leaders = ref<{ sustained: BoardIndustryItem[]; reversal: BoardIndustryItem[]; excess: BoardIndustryItem[] }>({
  sustained: [], reversal: [], excess: [],
})
const asOf = ref<string | null>(null)
const effectiveFrom = ref<string | null>(null)
const loading = ref(false)
const errorMsg = ref('')

const sortKey = ref('bull_dur')
const sortOrder = ref<'desc' | 'asc'>('desc')
const levelFilter = ref('')

const active = ref<BoardIndustryItem | null>(null)
const activeStocks = ref<BoardStockItem[]>([])
const drawerLoading = ref(false)

const refreshScope = ref<'industries' | 'all'>('industries')
const refreshing = ref(false)
const run = ref<BoardRunInfo | null>(null)

function errMsg(e: any): string {
  const d = e?.response?.data
  if (d?.message) return d.message
  if (typeof d?.detail === 'string') return d.detail
  return e?.message || '请求失败'
}

function fmt(v: number | null | undefined, digits = 0): string {
  if (v === null || v === undefined || Number.isNaN(v)) return '—'
  return digits > 0 ? v.toFixed(digits) : String(Math.round(v))
}

function signed(v: number | null | undefined): string {
  if (v === null || v === undefined) return '—'
  return v > 0 ? `+${v}` : String(v)
}

function toneOf(row: BoardIndustryItem): string {
  return `dir-${gateDirectionTone(row.direction)}`
}

const dataTime = computed(() => {
  if (!industries.value.length) return asOf.value || '—'
  const row = industries.value[0]
  return row.bar_closed_at || row.as_of || '—'
})

const universeText = computed(() => {
  const u = meta.value?.universe
  if (!u) return '—'
  return `${u.count} / ${u.expected}${u.ok ? '' : '（名单不一致）'}`
})

async function loadAll() {
  loading.value = true
  errorMsg.value = ''
  try {
    const p = period.value
    const [ind, lead, m] = await Promise.all([
      getBoardIndustries({ period: p, sort: sortKey.value, order: sortOrder.value, level: levelFilter.value, limit: 200 }),
      getBoardLeaders({ period: p, limit: 5 }),
      meta.value ? Promise.resolve(meta.value) : getBoardMeta(),
    ])
    industries.value = ind.rows
    asOf.value = ind.as_of
    effectiveFrom.value = ind.rows[0]?.effective_from || null
    leaders.value = { sustained: lead.sustained, reversal: lead.reversal, excess: lead.excess }
    meta.value = m
    const summary = await getBoardSummary({ period: p }).catch(() => null)
    levelCounts.value = summary?.level_counts || {}
  } catch (e) {
    errorMsg.value = '加载失败：' + errMsg(e)
  } finally {
    loading.value = false
  }
}

function switchPeriod(p: Period) {
  if (period.value === p) return
  period.value = p
  active.value = null
  loadAll()
}

function toggleSort(key: string) {
  if (sortKey.value === key) {
    sortOrder.value = sortOrder.value === 'desc' ? 'asc' : 'desc'
  } else {
    sortKey.value = key
    sortOrder.value = 'desc'
  }
  loadAll()
}

function applyLevel() {
  loadAll()
}

async function openIndustry(row: BoardIndustryItem) {
  active.value = row
  activeStocks.value = []
  drawerLoading.value = true
  try {
    const [detail, stocks] = await Promise.all([
      getBoardIndustry(row.code, { period: period.value }).catch(() => row),
      getBoardIndustryStocks(row.code, { period: period.value, sort: 'lead_days', limit: 200 }),
    ])
    active.value = detail || row
    activeStocks.value = stocks.rows
  } catch (e) {
    errorMsg.value = '加载成分股失败：' + errMsg(e)
  } finally {
    drawerLoading.value = false
  }
}

function closeDrawer() {
  active.value = null
  activeStocks.value = []
}

async function doRefresh() {
  if (refreshing.value) return
  refreshing.value = true
  errorMsg.value = ''
  try {
    const r = await refreshBoard({ period: period.value, scope: refreshScope.value })
    await pollRun(r.run_id)
  } catch (e) {
    errorMsg.value = '刷新失败：' + errMsg(e)
  } finally {
    refreshing.value = false
  }
}

async function pollRun(runId: number) {
  for (let i = 0; i < 180; i += 1) {
    const info = await getBoardRun(runId)
    run.value = info
    if (info.status !== 'running') break
    await new Promise((resolve) => setTimeout(resolve, 2000))
  }
  await loadAll()
}

function sortMark(key: string): string {
  if (sortKey.value !== key) return ''
  return sortOrder.value === 'desc' ? ' ↓' : ' ↑'
}

onMounted(loadAll)
</script>

<template>
  <PageHeader title="投资看板">
    <span class="board-tag">只读</span>
    <select v-model="refreshScope" class="board-select">
      <option value="industries">刷新行业榜</option>
      <option value="all">行业 + 成分股</option>
    </select>
    <button class="btn btn-primary" :disabled="refreshing" @click="doRefresh">
      {{ refreshing ? '刷新中…' : '刷新' }}
    </button>
  </PageHeader>

  <!-- A 口径条 -->
  <div class="card board-meta">
    <span class="meta-pill">公式 <b>{{ meta?.formula_version || '—' }}</b></span>
    <span class="meta-pill">方向口径 <b>{{ meta?.direction_basis === 'm_enh' ? 'ENH' : (meta?.direction_basis || '—') }}</b></span>
    <span class="meta-pill">超买超卖 <b>z({{ meta?.bias_z.window ?? 250 }}, k={{ meta?.bias_z.k ?? 2.5 }})</b></span>
    <span class="meta-pill">最小根数 <b>{{ meta?.min_bars ?? 300 }}</b></span>
    <span class="meta-pill">881 名单 <b>{{ universeText }}</b></span>
    <span class="meta-pill">数据 <b>{{ dataTime }}</b></span>
    <span v-if="effectiveFrom" class="meta-pill">生效 <b>{{ effectiveFrom }}</b></span>
    <span v-if="run" class="meta-pill" :class="{ 'is-run': run.status === 'running' }">
      任务 #{{ run.id }} <b>{{ run.status }}</b>
      <template v-if="run.status === 'running' && run.total">（{{ run.ok }}/{{ run.total }}）</template>
    </span>
  </div>

  <div v-if="errorMsg" class="card board-error">{{ errorMsg }}</div>

  <!-- B 大盘多空状态卡 + 统一市场状态面板（整体迁移） -->
  <MarketGatePanel />

  <!-- 周期切换 -->
  <div class="board-tabs">
    <button class="board-tab" :class="{ active: period === '1d' }" @click="switchPeriod('1d')">日线</button>
    <button class="board-tab" :class="{ active: period === '30m' }" @click="switchPeriod('30m')">30 分钟</button>
    <span class="board-tabs-hint">
      30m 只使用已闭合 bar，闭合后下一根 bar 生效（effective_from）
    </span>
  </div>

  <!-- C 三张分类卡 -->
  <div class="leader-grid">
    <div class="card leader-card">
      <div class="leader-head">持续领先 <span>按多头持续天数</span></div>
      <ol class="leader-list">
        <li v-for="r in leaders.sustained" :key="`s-${r.code}`" @click="openIndustry(r)">
          <span class="leader-name">{{ r.name }}</span>
          <span class="leader-num num">{{ fmt(r.bull_dur) }} 天</span>
          <span class="leader-strength num">{{ fmt(r.strength) }}</span>
        </li>
        <li v-if="!leaders.sustained.length" class="leader-empty">暂无多头行业</li>
      </ol>
    </div>
    <div class="card leader-card">
      <div class="leader-head">由空转多 <span>bull_dur = 1</span></div>
      <ol class="leader-list">
        <li v-for="r in leaders.reversal" :key="`r-${r.code}`" @click="openIndustry(r)">
          <span class="leader-name">{{ r.name }}</span>
          <span class="leader-num num">强度 {{ fmt(r.strength) }}</span>
          <span class="leader-strength num">{{ r.lead_days === null ? '—' : '+' + r.lead_days }}</span>
        </li>
        <li v-if="!leaders.reversal.length" class="leader-empty">暂无新转多行业</li>
      </ol>
    </div>
    <div class="card leader-card">
      <div class="leader-head">超额领先 <span>领先大盘天数</span></div>
      <ol class="leader-list">
        <li v-for="r in leaders.excess" :key="`e-${r.code}`" @click="openIndustry(r)">
          <span class="leader-name">{{ r.name }}</span>
          <span class="leader-num num">{{ signed(r.lead_days) }} 天</span>
          <span class="leader-strength num">{{ fmt(r.strength) }}</span>
        </li>
        <li v-if="!leaders.excess.length" class="leader-empty">暂无领先行业</li>
      </ol>
    </div>
  </div>

  <!-- D 行业榜单 -->
  <div class="card table-wrap board-table">
    <div class="board-table-head">
      <h3>行业榜单<span v-if="asOf" class="board-table-date num">（{{ asOf }}）</span></h3>
      <div class="board-filters">
        <span v-for="(n, lv) in levelCounts" :key="lv" class="badge badge-gray">
          {{ meta?.level_labels[lv] || lv }} {{ n }}
        </span>
        <select v-model="levelFilter" class="board-select" @change="applyLevel">
          <option value="">全部等级</option>
          <option value="strong_bull">强多</option>
          <option value="medium_bull">中多</option>
          <option value="weak_bull">弱多</option>
          <option value="strong_bear">强空</option>
          <option value="medium_bear">中空</option>
          <option value="weak_bear">弱空</option>
          <option value="none">无信号</option>
        </select>
      </div>
    </div>

    <div v-if="loading" class="board-loading">正在加载行业榜单…</div>
    <table v-else>
      <thead>
        <tr>
          <th>#</th>
          <th>行业</th>
          <th>方向</th>
          <th class="sortable" @click="toggleSort('strength')">强度{{ sortMark('strength') }}</th>
          <th>等级</th>
          <th class="sortable" @click="toggleSort('bull_dur')">持续{{ sortMark('bull_dur') }}</th>
          <th class="sortable" @click="toggleSort('bias_z')">偏离 z{{ sortMark('bias_z') }}</th>
          <th>超买超卖</th>
          <th class="sortable" @click="toggleSort('lead_days')">领先大盘{{ sortMark('lead_days') }}</th>
          <th>数据时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="r in industries" :key="r.code" class="board-row" @click="openIndustry(r)">
          <td class="num">{{ r.rank }}</td>
          <td>
            <div class="ind-name">{{ r.name }}</div>
            <div class="ind-code num">{{ r.code }}</div>
          </td>
          <td><span :class="toneOf(r)" class="dir-badge">{{ r.direction_label }}</span></td>
          <td class="num">{{ fmt(r.strength) }}</td>
          <td><span class="level-tag" :class="`level-${r.level}`">{{ r.level_label }}</span></td>
          <td class="num">{{ r.bull_dur ? r.bull_dur + ' 天' : (r.bear_dur ? '空 ' + r.bear_dur : '—') }}</td>
          <td class="num">{{ fmt(r.bias_z, 2) }}</td>
          <td>
            <span v-if="r.overbought" class="flag flag-ob">超买</span>
            <span v-else-if="r.oversold" class="flag flag-os">超卖</span>
            <span v-else class="muted">—</span>
          </td>
          <td class="num">{{ signed(r.lead_days) }}</td>
          <td class="muted">{{ r.bar_closed_at || r.as_of || '—' }}</td>
        </tr>
        <tr v-if="!industries.length">
          <td colspan="10" class="board-empty">
            暂无快照。请点击右上角「刷新」生成（首次建议选「行业 + 成分股」）。
          </td>
        </tr>
      </tbody>
    </table>
  </div>

  <!-- E 口径说明 -->
  <details class="card board-caliber">
    <summary>口径说明（本项目实现 vs 行业评级系统）</summary>
    <ul>
      <li><b>公式唯一来源</b>：本项目 <code>core/engine/rating_core.py</code>（{{ meta?.formula_version }}），方向取 ENH（<code>m_enh</code>）；不移植行业评级系统的 Python 版本。</li>
      <li><b>三处已修正缺陷</b>：<code>WO</code> 补 ROUND；角度用 <code>180/3.1416</code>；补 <code>偏离分_空</code>（<code>IF(偏差率&lt;-3,10,IF(偏差率&lt;-1,…))</code>）。</li>
      <li><b>超买超卖重定义</b>：乖离率 250 日滚动 z 分数、不含当日、多空对称、k=2.5；JD 与 CH/CL 轨道只作二级过滤展示，不作必要条件。</li>
      <li><b>名单</b>：严格 128 个 881 二级行业（剔除 881999 类）；不足 {{ meta?.min_bars ?? 300 }} 根不输出方向。</li>
      <li><b>30m</b>：只用已闭合 bar，闭合后下一根 bar 生效；个股 30m 仅计算「日线由空转多」的行业。</li>
      <li>{{ meta?.caliber_note }}</li>
    </ul>
  </details>

  <!-- F 行业详情抽屉 -->
  <div v-if="active" class="board-drawer-overlay" @click.self="closeDrawer">
    <aside class="board-drawer">
      <div class="drawer-head">
        <div>
          <div class="drawer-title">{{ active.name }}</div>
          <div class="drawer-code num">{{ active.code }}</div>
        </div>
        <button class="btn btn-sm" @click="closeDrawer">关闭</button>
      </div>
      <div class="drawer-stats">
        <div><span>方向</span><b :class="`dir-${gateDirectionTone(active.direction)}`">{{ active.direction_label }}</b></div>
        <div><span>强度</span><b class="num">{{ fmt(active.strength) }}</b></div>
        <div><span>等级</span><b>{{ active.level_label }}</b></div>
        <div><span>多头持续</span><b class="num">{{ fmt(active.bull_dur) }}</b></div>
        <div><span>偏离 z</span><b class="num">{{ fmt(active.bias_z, 2) }}</b></div>
        <div><span>乖离率</span><b class="num">{{ fmt(active.bias, 2) }}%</b></div>
        <div><span>JD 过滤</span><b class="num">{{ active.jd_filter > 0 ? '过热' : (active.jd_filter < 0 ? '过冷' : '中性') }}（{{ fmt(active.jd, 1) }}）</b></div>
        <div><span>轨道过滤</span><b>{{ active.channel_filter > 0 ? '上轨外' : (active.channel_filter < 0 ? '下轨外' : '轨内') }}</b></div>
        <div><span>领先大盘</span><b class="num">{{ signed(active.lead_days) }} 天</b></div>
        <div><span>数据 / 生效</span><b class="num">{{ active.bar_closed_at || '—' }} → {{ active.effective_from || '—' }}</b></div>
      </div>
      <div class="drawer-sub">
        成分股领先榜（相对行业指数）
        <span class="muted">领先天数 = 个股多头持续 − 行业多头持续</span>
      </div>
      <div v-if="drawerLoading" class="board-loading">正在加载成分股…</div>
      <div v-else class="drawer-table-wrap">
        <table>
          <thead>
            <tr><th>#</th><th>个股</th><th>方向</th><th>强度</th><th>持续</th><th>领先</th><th>超买超卖</th></tr>
          </thead>
          <tbody>
            <tr v-for="s in activeStocks" :key="s.code">
              <td class="num">{{ s.rank }}</td>
              <td>
                <div>{{ s.name }}</div>
                <div class="ind-code num">{{ s.code }}</div>
              </td>
              <td><span :class="`dir-${gateDirectionTone(s.direction)}`">{{ s.direction_label }}</span></td>
              <td class="num">{{ fmt(s.strength) }}</td>
              <td class="num">{{ s.bull_dur ? s.bull_dur + ' 天' : (s.bear_dur ? '空 ' + s.bear_dur : '—') }}</td>
              <td class="num">{{ signed(s.lead_days) }}</td>
              <td>
                <span v-if="s.overbought" class="flag flag-ob">超买</span>
                <span v-else-if="s.oversold" class="flag flag-os">超卖</span>
                <span v-else class="muted">—</span>
              </td>
            </tr>
            <tr v-if="!activeStocks.length">
              <td colspan="7" class="board-empty">
                {{ period === '30m'
                  ? '30m 个股未计算（仅计算日线由空转多行业的成分股）'
                  : '暂无成分股快照，请用「行业 + 成分股」刷新' }}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </aside>
  </div>
</template>

<style scoped>
.board-tag {
  padding: 2px 8px; border: 1px solid var(--line); border-radius: 99px;
  font-size: 10.5px; letter-spacing: .05em; color: var(--ink-soft);
}
.board-select {
  height: 27px; padding: 0 8px; border: 1px solid var(--border);
  border-radius: var(--radius); background: var(--bg); color: var(--ink); font-size: 12px;
}

/* A 口径条 */
.board-meta {
  display: flex; flex-wrap: wrap; gap: 6px 10px; align-items: center;
  padding: 9px 14px; margin-bottom: 12px; font-size: 11.5px; color: var(--ink-soft);
}
.meta-pill { padding: 2px 9px; border: 1px solid var(--line); border-radius: 99px; }
.meta-pill b { font-weight: 600; color: var(--ink); }
.meta-pill.is-run b { color: var(--accent); }

.board-error {
  padding: 10px 14px; margin-bottom: 12px; color: var(--red);
  background: var(--red-wash); font-size: 12.5px;
}

/* 周期切换 */
.board-tabs { display: flex; align-items: center; gap: 8px; margin: 4px 0 12px; }
.board-tab {
  padding: 6px 16px; border: 1px solid var(--line); border-radius: 99px;
  background: transparent; color: var(--ink-soft); font-size: 12.5px; cursor: pointer;
}
.board-tab.active {
  border-color: var(--accent); background: var(--accent-light); color: var(--ink); font-weight: 600;
}
.board-tabs-hint { font-size: 11px; color: var(--ink-faint); }

/* C 分类卡 */
.leader-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 12px; }
.leader-card { padding: 12px 14px; }
.leader-head { font-size: 13px; font-weight: 700; margin-bottom: 8px; }
.leader-head span { font-size: 10.5px; font-weight: 400; color: var(--ink-faint); margin-left: 6px; }
.leader-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 4px; }
.leader-list li {
  display: flex; align-items: center; gap: 8px; padding: 4px 6px;
  border-radius: 6px; cursor: pointer; font-size: 12.5px;
}
.leader-list li:hover { background: var(--hover-bg); }
.leader-name { flex: 1 1 auto; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.leader-num { color: var(--ink-soft); font-size: 11.5px; }
.leader-strength { min-width: 44px; text-align: right; font-weight: 600; }
.leader-empty { color: var(--ink-faint); font-size: 12px; cursor: default; }

/* D 榜单 */
.board-table { padding: 0; margin-bottom: 12px; }
.board-table-head {
  display: flex; align-items: center; justify-content: space-between; gap: 10px;
  flex-wrap: wrap; padding: 12px 16px; border-bottom: 1px solid var(--line);
}
.board-table-head h3 { margin: 0; font-size: 14px; }
.board-table-date { font-size: 11.5px; font-weight: 400; color: var(--ink-faint); }
.board-filters { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.board-table table { font-size: 12.5px; }
.board-table th.sortable { cursor: pointer; user-select: none; white-space: nowrap; }
.board-table th.sortable:hover { color: var(--ink); }
.board-row { cursor: pointer; }
.board-row:hover { background: var(--hover-bg); }
.ind-name { font-weight: 600; }
.ind-code { font-size: 10.5px; color: var(--ink-faint); }
.dir-badge { font-weight: 600; }
.dir-bear { color: var(--green); }
.dir-bull { color: var(--red); }
.dir-neutral { color: var(--ink-soft); }
.level-tag { font-size: 11px; padding: 1px 7px; border-radius: 99px; border: 1px solid var(--line); }
.level-strong_bull, .level-medium_bull, .level-weak_bull { color: var(--red); border-color: var(--red); }
.level-strong_bear, .level-medium_bear, .level-weak_bear { color: var(--green); border-color: var(--green); }
.level-none { color: var(--ink-faint); }
.flag { font-size: 10.5px; padding: 1px 7px; border-radius: 99px; }
.flag-ob { color: var(--red); background: var(--red-wash); }
.flag-os { color: var(--green); background: var(--green-wash); }
.muted { color: var(--ink-faint); }
.board-loading { padding: 18px 16px; font-size: 12.5px; color: var(--ink-soft); }
.board-empty { padding: 20px 16px; text-align: center; color: var(--ink-faint); font-size: 12.5px; }

/* E 口径说明 */
.board-caliber { padding: 10px 16px; margin-bottom: 12px; font-size: 12px; color: var(--ink-soft); }
.board-caliber summary { cursor: pointer; font-weight: 600; color: var(--ink); }
.board-caliber ul { margin: 8px 0 2px; padding-left: 18px; line-height: 1.75; }
.board-caliber code { font-size: 11.5px; }

/* F 抽屉 */
.board-drawer-overlay {
  position: fixed; inset: 0; z-index: 40; background: rgba(0, 0, 0, .28);
  display: flex; justify-content: flex-end;
}
.board-drawer {
  width: min(760px, 94vw); height: 100%; overflow-y: auto;
  background: var(--bg); border-left: 1px solid var(--line);
  padding: 16px 18px; box-shadow: -8px 0 24px rgba(0, 0, 0, .12);
}
.drawer-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; }
.drawer-title { font-size: 18px; font-weight: 700; }
.drawer-code { font-size: 11.5px; color: var(--ink-faint); }
.drawer-stats {
  display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px 14px;
  margin: 14px 0; padding: 12px 14px; border: 1px solid var(--line); border-radius: 8px;
  font-size: 12px; color: var(--ink-soft);
}
.drawer-stats div { display: flex; justify-content: space-between; gap: 10px; }
.drawer-stats b { color: var(--ink); font-weight: 600; }
.drawer-sub { font-size: 12.5px; font-weight: 700; margin: 6px 0 8px; }
.drawer-sub .muted { font-weight: 400; font-size: 10.5px; margin-left: 8px; }
.drawer-table-wrap { overflow-x: auto; }
.drawer-table-wrap table { width: 100%; border-collapse: collapse; font-size: 12px; }
.drawer-table-wrap th, .drawer-table-wrap td {
  text-align: left; padding: 7px 8px; border-top: 1px solid var(--line); vertical-align: top;
}
.drawer-table-wrap th { color: var(--ink-faint); font-size: 11px; font-weight: 600; }

@media (max-width: 900px) {
  .leader-grid { grid-template-columns: minmax(0, 1fr); }
  .drawer-stats { grid-template-columns: minmax(0, 1fr); }
}
</style>
