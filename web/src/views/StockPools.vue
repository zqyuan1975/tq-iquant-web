<script setup lang="ts">
import { computed, ref, onMounted } from 'vue'
import {
  getTdxPools, getTdxPoolStocks, syncStockPool, getStockPools, deleteStockPool,
  getMarketGateStatus,
  type TdxPoolItem, type TdxPoolStockItem, type MarketGateStatusItem,
} from '../api'
import PageHeader from '../components/PageHeader.vue'
import { BENCHMARKS, DEFAULT_BENCHMARK } from '../utils/benchmarks'
import { gateDirectionLabel, gateDirectionTone } from '../utils/marketGate'

const pools = ref<TdxPoolItem[]>([])
const localIdByCode = ref<Record<string, number>>({})  // 删除用：code → 本地池 id
const loading = ref(true)
const errorMsg = ref('')
const showStocks = ref(false)
const stocksList = ref<TdxPoolStockItem[]>([])
const stocksPoolName = ref('')

// ===== 市场多空状态卡（独立取数：失败只影响本卡片，不影响股票池列表）=====
const gateBenchmark = ref(DEFAULT_BENCHMARK)
const gateStatus = ref<MarketGateStatusItem | null>(null)
const gateLoading = ref(false)
const gateError = ref('')

// 卡片态色调（空头/多头/未定）：同时驱动左标条、方向大字与强度条填充色
const gateTone = computed(() => `gate-tone-${gateDirectionTone(gateStatus.value?.direction)}`)
const gateCardTone = computed(() => `gate-card-${gateDirectionTone(gateStatus.value?.direction)}`)
const gateStrengthWidth = computed(() => {
  const v = gateStatus.value?.strength ?? 0
  return Math.max(0, Math.min(100, v)) + '%'
})

async function loadGateStatus(force = false) {
  gateLoading.value = true
  gateError.value = ''
  try {
    gateStatus.value = await getMarketGateStatus({ code: gateBenchmark.value, refresh: force })
  } catch (e) {
    gateStatus.value = null
    gateError.value = '市场状态加载失败：' + errMsg(e)
  } finally {
    gateLoading.value = false
  }
}

// 从 axios 错误里提取后端错误消息（统一响应 {code,message} 或 Pydantic 422 detail）
function errMsg(e: any): string {
  const d = e?.response?.data
  if (d?.message) return d.message
  if (Array.isArray(d?.detail)) return d.detail.map((x: { loc?: unknown[]; msg?: string }) => `${(x.loc || []).join('.')}: ${x.msg}`).join('; ')
  if (typeof d?.detail === 'string') return d.detail
  return e?.message || '请求失败'
}

async function load() {
  errorMsg.value = ''
  try {
    // 并行拉通达信板块 + 本地已同步池（取 id 供删除）
    const [tdxRes, localRes] = await Promise.all([
      getTdxPools(),
      getStockPools().catch(() => []),  // 本地列表失败不阻塞
    ])
    pools.value = tdxRes
    localIdByCode.value = {}
    for (const p of localRes) {
      localIdByCode.value[p.code] = p.id
    }
  } catch (e) {
    // getTdxPools 失败（拦截器 reject 或 HTTP 错误）→ 提示，清空列表
    errorMsg.value = '加载失败：' + errMsg(e)
    pools.value = []
  } finally {
    loading.value = false
  }
}

async function viewStocks(p: TdxPoolItem) {
  try {
    stocksPoolName.value = p.name
    stocksList.value = await getTdxPoolStocks(p.code)
    showStocks.value = true
  } catch (e) {
    alert(`查看成分股失败：${errMsg(e)}`)
  }
}

async function syncPool(p: TdxPoolItem) {
  if (!confirm(`确认从通达信同步「${p.name}」的股票清单？将全量替换现有股票。`)) return
  try {
    await syncStockPool({ code: p.code })
  } catch (e) {
    alert(`同步失败：${errMsg(e)}`)
    return
  }
  load()
}

async function remove(p: TdxPoolItem) {
  const id = localIdByCode.value[p.code]
  if (!id) return
  if (!confirm(`确认删除股票池「${p.name}」？`)) return
  try {
    await deleteStockPool(id)
  } catch (e) {
    alert(`删除失败：${errMsg(e)}`)
    return
  }
  load()
}

onMounted(() => {
  load()
  loadGateStatus()
})
</script>

<template>
  <PageHeader title="备选股池">
    <button @click="load" class="btn">刷新</button>
  </PageHeader>

  <!-- 市场多空状态（放在股票池加载态之外：卡片取数失败只影响自身） -->
  <div class="card gate-card" :class="gateCardTone">
    <div class="gate-card-head">
      <div class="gate-card-title">
        <span class="gate-dot" :class="gateTone" aria-hidden="true"></span>
        市场多空状态
      </div>
      <div class="gate-card-actions">
        <select
          v-model="gateBenchmark"
          class="field-select"
          data-field="gate-benchmark"
          @change="loadGateStatus(false)"
        >
          <option v-for="b in BENCHMARKS" :key="b.code" :value="b.code">{{ b.name }}（{{ b.code }}）</option>
        </select>
        <button class="btn btn-sm" data-field="gate-refresh" :disabled="gateLoading" @click="loadGateStatus(true)">
          {{ gateLoading ? '计算中…' : '刷新' }}
        </button>
      </div>
    </div>

    <div v-if="gateLoading" class="gate-state">
      <span class="gate-spinner" aria-hidden="true"></span>正在计算市场状态…
    </div>
    <div v-else-if="gateError" class="gate-error" data-test="gate-error">{{ gateError }}</div>
    <template v-else-if="gateStatus">
      <div class="gate-hero">
        <div class="gate-hero-block">
          <span class="gate-label">当前方向</span>
          <span class="gate-dir-value" :class="gateTone">{{ gateDirectionLabel(gateStatus.direction) }}</span>
          <span class="gate-hero-note">截至 <span class="num">{{ gateStatus.as_of }}</span></span>
        </div>
        <div class="gate-hero-block">
          <span class="gate-label">强度分</span>
          <span class="gate-strength-line">
            <span class="gate-strength-value">{{ gateStatus.strength }}</span>
            <span class="gate-strength-max">/ 100</span>
          </span>
          <span class="gate-strength-track">
            <i class="gate-strength-fill" :class="gateTone" :style="{ width: gateStrengthWidth }"></i>
          </span>
        </div>
        <div class="gate-hero-block">
          <span class="gate-label">持续</span>
          <span class="gate-duration-line">
            <b class="gate-duration-value num">{{ gateStatus.duration_bars }}</b>
            <span class="gate-duration-unit">个交易日</span>
          </span>
          <span class="gate-hero-note">起始 <span class="num">{{ gateStatus.since }}</span></span>
        </div>
      </div>

      <!-- E4：状态数据陈旧提示（超过 N 个交易日未见新 bar → 标黄，提示可能不准） -->
      <div v-if="gateStatus.stale" class="gate-error" data-test="gate-stale"
           style="background:#fff7e6;color:#ad6800;border-color:#ffd591">
        门控数据截至 <span class="num">{{ gateStatus.as_of }}</span>，已落后
        <b>{{ gateStatus.stale_days }}</b> 个交易日（预期最新
        <span class="num">{{ gateStatus.latest_trading_day }}</span>）。
        请先在通达信补齐 {{ gateStatus.benchmark_name }} 日线，再点「刷新」重算；
        陈旧数据下的多空判断可能不准。
      </div>

      <div class="gate-transitions">
        <div class="gate-transitions-head">
          <span class="gate-label">最近转折</span>
          <span class="gate-transitions-hint">新 → 旧</span>
        </div>
        <div class="gate-chips">
          <span v-if="gateStatus.transitions.length === 0" class="gate-empty">暂无转折记录</span>
          <span
            v-for="(t, i) in gateStatus.transitions"
            :key="`${t.date}-${t.state}`"
            class="gate-chip"
            :class="[`gate-chip-${gateDirectionTone(t.state)}`, { 'is-latest': i === 0 }]"
          >
            <span class="gate-chip-dot" :class="`gate-tone-${gateDirectionTone(t.state)}`" aria-hidden="true"></span>
            <span class="gate-chip-state" :class="`gate-tone-${gateDirectionTone(t.state)}`">{{ gateDirectionLabel(t.state) }}</span>
            <span class="gate-chip-date num">{{ t.date }}</span>
          </span>
        </div>
      </div>

      <div class="gate-foot">
        <span>基准 <b>{{ gateStatus.benchmark_name }}</b>（{{ gateStatus.code }}）</span>
        <span class="gate-foot-sep">·</span>
        <span>周期 {{ gateStatus.period }}</span>
        <span class="gate-foot-sep">·</span>
        <span><span class="num">{{ gateStatus.bars }}</span> 根 K 线</span>
        <span v-if="gateStatus.cached" class="gate-foot-tag">缓存结果</span>
      </div>
    </template>
    <div v-else class="gate-state">暂无数据</div>
  </div>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:12px">
    {{ errorMsg }}
  </div>

  <div v-if="loading" class="card" style="padding:12px"><p>加载中…</p></div>
  <div v-else class="card table-wrap">
    <table>
      <thead><tr><th>名称</th><th>代码</th><th>状态</th><th>股票数</th><th>操作</th></tr></thead>
      <tbody>
        <tr v-for="p in pools" :key="p.code">
          <td>{{ p.name }}</td>
          <td style="color:var(--ink-soft)">{{ p.code }}</td>
          <td>
            <span v-if="!p.exists_in_tdx" class="badge" style="background:var(--line);color:var(--ink-soft)">通达信已删除</span>
            <span v-else-if="p.synced" class="badge badge-green">已同步</span>
            <span v-else class="badge badge-gray">未同步</span>
          </td>
          <td>{{ p.synced ? p.stock_count + ' 只' : '-' }}</td>
          <td>
            <button v-if="p.exists_in_tdx" @click="viewStocks(p)" class="btn btn-sm">查看</button>
            <button v-if="p.exists_in_tdx" @click="syncPool(p)" class="btn btn-sm btn-primary" style="margin-left:6px">
              {{ p.synced ? '同步' : '同步' }}
            </button>
            <button v-if="p.synced" @click="remove(p)" class="btn btn-sm btn-danger" style="margin-left:6px">删除</button>
          </td>
        </tr>
      </tbody>
    </table>
    <div v-if="pools.length === 0 && !errorMsg" class="empty-state"><p>暂无股票池</p></div>
  </div>

  <!-- 成分股清单 Modal -->
  <div v-if="showStocks" class="modal-overlay modal-lg" @click.self="showStocks = false">
    <div class="modal-content">
      <h3>{{ stocksPoolName }} 成分股（{{ stocksList.length }}）</h3>
      <div class="modal-body">
        <div class="card table-wrap" style="margin-bottom:12px">
          <table>
            <thead><tr><th>股票代码</th><th>名称</th></tr></thead>
            <tbody>
              <tr v-for="s in stocksList" :key="s.stock_code">
                <td>{{ s.stock_code }}</td>
                <td style="color:var(--ink-soft)">{{ s.stock_name || '-' }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="stocksList.length === 0" class="empty-state"><p>该板块暂无成分股</p></div>
        </div>
      </div>
      <div class="modal-actions">
        <button @click="showStocks = false" class="btn">关闭</button>
      </div>
    </div>
  </div>
</template>

<style scoped>
/* ===== 市场多空状态卡 ===== */
.gate-card { padding: 0; margin-bottom: 14px; border-left-width: 3px; }
.gate-card-bear { border-left-color: var(--green); }
.gate-card-bull { border-left-color: var(--red); }
.gate-card-neutral { border-left-color: var(--line); }

.gate-card-head {
  display: flex; align-items: center; justify-content: space-between; gap: 12px;
  flex-wrap: wrap; padding: 11px 18px; border-bottom: 1px solid var(--line);
}
.gate-card-title {
  display: inline-flex; align-items: center; gap: 8px;
  font-size: 14px; font-weight: 700; letter-spacing: .02em; color: var(--ink);
}
.gate-dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; flex: 0 0 auto; }
.gate-card-actions { display: flex; align-items: center; gap: 8px; }
/* 基准下拉：套上站点控件外观（全局已把原生箭头替换为内联 SVG），与右侧按钮同高同节奏 */
.gate-card-actions .field-select {
  min-width: 210px; height: 25px;
  padding: 0 28px 0 10px;
  border: 1px solid var(--border); border-radius: var(--radius);
  background-color: var(--bg); background-position: right 9px center; background-size: 9px 6px;
  color: var(--ink); font-size: 12px; line-height: 23px;
  text-overflow: ellipsis;
}
.gate-card-actions .field-select:hover { border-color: var(--line-strong); }
.gate-card-actions .field-select:focus {
  outline: none; border-color: var(--accent); box-shadow: 0 0 0 2px var(--accent-light);
}

/* 三栏主区：方向 / 强度 / 持续（竖细线分隔，层级一眼分明） */
.gate-hero {
  display: grid;
  grid-template-columns: minmax(180px, 1fr) minmax(200px, 1.1fr) minmax(150px, .85fr);
  gap: 20px; padding: 18px; border-bottom: 1px solid var(--line);
}
.gate-hero-block { display: flex; flex-direction: column; gap: 7px; min-width: 0; }
.gate-hero-block + .gate-hero-block { border-left: 1px solid var(--line); padding-left: 20px; }
.gate-label { font-size: 10.5px; font-weight: 600; letter-spacing: .13em; text-transform: uppercase; color: var(--ink-faint); }
.gate-hero-note { font-size: 11.5px; color: var(--ink-faint); }

.gate-dir-value { font-size: 28px; font-weight: 800; line-height: 1.08; letter-spacing: .04em; }

.gate-strength-line { display: flex; align-items: baseline; gap: 5px; }
.gate-strength-value { font-family: var(--font-serif); font-size: 26px; font-weight: 700; line-height: 1; font-variant-numeric: tabular-nums; color: var(--ink); }
.gate-strength-max { font-size: 11px; color: var(--ink-faint); }
.gate-strength-track { display: block; height: 8px; border-radius: 99px; background: var(--line); overflow: hidden; }
.gate-strength-fill { display: block; height: 100%; border-radius: 99px; background: currentColor; transition: width .35s ease; }

.gate-duration-line { display: flex; align-items: baseline; gap: 5px; }
.gate-duration-value { font-size: 20px; font-weight: 700; line-height: 1; color: var(--ink); }
.gate-duration-unit { font-size: 11.5px; color: var(--ink-soft); }

/* 转折记录：胶囊时间轴（新 → 旧），最近一次用同色 wash 高亮 */
.gate-transitions { display: flex; flex-direction: column; gap: 9px; padding: 14px 18px 16px; border-bottom: 1px solid var(--line); }
.gate-transitions-head { display: flex; align-items: baseline; gap: 10px; }
.gate-transitions-hint { font-size: 10.5px; letter-spacing: .05em; color: var(--ink-faint); }
.gate-chips { display: flex; flex-wrap: wrap; gap: 8px; }
.gate-chip {
  display: inline-flex; align-items: center; gap: 6px; padding: 4px 10px;
  border: 1px solid var(--line); border-radius: 99px; background: transparent;
  font-size: 12px; line-height: 1.45; color: var(--ink-soft);
}
.gate-chip.is-latest { font-weight: 600; }
.gate-chip.is-latest.gate-chip-bear { background: var(--green-wash); border-color: var(--green); }
.gate-chip.is-latest.gate-chip-bull { background: var(--red-wash); border-color: var(--red); }
.gate-chip.is-latest.gate-chip-neutral { background: var(--hover-bg); border-color: var(--ink-faint); }
.gate-chip-dot { width: 6px; height: 6px; border-radius: 50%; background: currentColor; flex: 0 0 auto; }
.gate-chip-state { font-weight: 700; }
.gate-chip-date { font-size: 11.5px; color: var(--ink-soft); opacity: .85; }
.gate-empty { font-size: 12px; color: var(--ink-faint); }

/* 数据来源脚注：独立底条，右侧挂缓存标记 */
.gate-foot {
  display: flex; align-items: center; flex-wrap: wrap; gap: 4px 8px;
  padding: 9px 18px; background: var(--hover-bg); font-size: 11px; color: var(--ink-faint);
}
.gate-foot b { font-weight: 600; color: var(--ink-soft); }
.gate-foot-sep { opacity: .55; }
.gate-foot-tag {
  margin-left: auto; padding: 1px 8px; border: 1px solid var(--line); border-radius: 99px;
  font-size: 10.5px; letter-spacing: .04em; color: var(--ink-soft);
}

/* 载入 / 空态 / 错误态 */
.gate-state { display: flex; align-items: center; gap: 8px; padding: 16px 18px; font-size: 12.5px; color: var(--ink-soft); }
.gate-spinner {
  width: 11px; height: 11px; border-radius: 50%; flex: 0 0 auto;
  border: 2px solid var(--line); border-top-color: var(--accent);
  animation: gate-spin .8s linear infinite;
}
@keyframes gate-spin { to { transform: rotate(360deg); } }
.gate-error { padding: 13px 18px; font-size: 12.5px; line-height: 1.6; color: var(--red); background: var(--red-wash); }

/* 方向语义色（A 股红涨绿跌口径，与全站一致） */
.gate-tone-bear { color: var(--green); }
.gate-tone-bull { color: var(--red); }
.gate-tone-neutral { color: var(--ink-soft); }

@media (max-width: 720px) {
  .gate-card-actions { width: 100%; }
  .gate-card-actions .field-select { min-width: 0; flex: 1 1 auto; }
  .gate-hero { grid-template-columns: minmax(0, 1fr); gap: 14px; }
  .gate-hero-block + .gate-hero-block { border-left: none; padding-left: 0; border-top: 1px solid var(--line); padding-top: 13px; }
  .gate-foot { flex-direction: column; align-items: flex-start; }
  .gate-foot-tag { margin-left: 0; }
}

</style>
