<script setup lang="ts">
// 市场多空状态卡 + 统一市场状态面板（自备选股池整体迁移，功能等价）：
// 基准切换、刷新按钮、陈旧提示、转折记录、作用域/配置概览全部保留可操作。
import { computed, ref, onMounted } from 'vue'
import {
  getMarketGateStatus, getMarketGateOverview,
  type MarketGateStatusItem, type MarketGateOverview,
} from '../../api'
import { BENCHMARKS, DEFAULT_BENCHMARK } from '../../utils/benchmarks'
import { gateDirectionLabel, gateDirectionTone } from '../../utils/marketGate'

const gateBenchmark = ref(DEFAULT_BENCHMARK)
const gateStatus = ref<MarketGateStatusItem | null>(null)
const gateLoading = ref(false)
const gateError = ref('')

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

const gateOverview = ref<MarketGateOverview | null>(null)
const gateOverviewError = ref('')

async function loadGateOverview() {
  gateOverviewError.value = ''
  try {
    gateOverview.value = await getMarketGateOverview()
  } catch (e) {
    gateOverview.value = null
    gateOverviewError.value = '统一市场状态加载失败：' + errMsg(e)
  }
}

function errMsg(e: any): string {
  const d = e?.response?.data
  if (d?.message) return d.message
  if (Array.isArray(d?.detail)) return d.detail.map((x: { loc?: unknown[]; msg?: string }) => `${(x.loc || []).join('.')}: ${x.msg}`).join('; ')
  if (typeof d?.detail === 'string') return d.detail
  return e?.message || '请求失败'
}

onMounted(() => {
  loadGateStatus()
  loadGateOverview()
})
</script>

<template>
  <div class="gate-card card" :class="gateCardTone">
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

      <div v-if="gateStatus.stale" class="gate-error" data-test="gate-stale"
           style="background:var(--warn-wash);color:var(--warn);border-color:var(--warn)">
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

  <div class="card gate-overview">
    <div class="gate-overview-head">
      <h3>市场状态统一面板</h3>
      <span class="gate-overview-hint">market_gate 与 regime_filter 的作用域/优先级与当前配置（只读）</span>
    </div>
    <div v-if="gateOverviewError" class="gate-error">{{ gateOverviewError }}</div>
    <template v-else-if="gateOverview">
      <div class="scope-grid">
        <div v-for="s in gateOverview.scopes" :key="s.key" class="scope-item">
          <div class="scope-title">{{ s.name }}</div>
          <div class="scope-row"><span>作用域</span>{{ s.scope }}</div>
          <div class="scope-row"><span>效果</span>{{ s.effect }}</div>
          <div class="scope-row"><span>时点</span>{{ s.timing }}</div>
        </div>
      </div>
      <div v-if="gateOverview.portfolios.length === 0" class="gate-empty">暂无组合配置</div>
      <table v-else class="gate-overview-table">
        <thead>
          <tr>
            <th>组合</th><th>门控</th><th>指数 / 投票</th>
            <th>强度门槛</th><th>空头减仓</th><th>子策略（状态闸 / 门控覆盖）</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="p in gateOverview.portfolios" :key="p.id">
            <td>{{ p.name }}</td>
            <td>
              <span class="badge" :class="p.effective_enabled ? 'badge-green' : 'badge-gray'">
                {{ p.effective_enabled ? '启用' : '关闭' }}
              </span>
              <div v-if="!p.market_gate_enabled && p.effective_enabled" class="gate-overview-note">由策略 on 强制</div>
            </td>
            <td>
              <div class="num">{{ p.indices.join(' / ') }}</div>
              <div class="gate-overview-note">{{ p.vote_mode }} · {{ p.engine }}</div>
            </td>
            <td class="num">{{ p.strength_threshold > 0 ? p.strength_threshold : '不限' }}</td>
            <td class="num">{{ p.bear_reduce_ratio > 0 ? Math.round(p.bear_reduce_ratio * 100) + '%' : '关闭' }}</td>
            <td>
              <div v-for="s in p.strategies" :key="s.id" class="scope-strategy">
                <span>{{ s.name }}</span>
                <span class="badge" :class="s.regime_filter_enabled ? 'badge-green' : 'badge-gray'">
                  状态闸{{ s.regime_filter_enabled ? '开' : '关' }}
                </span>
                <span class="badge" :class="s.market_gate_mode === 'off' ? 'badge-gray' : (s.market_gate_mode === 'on' ? 'badge-green' : '')">
                  门控 {{ s.market_gate_mode }}
                </span>
              </div>
              <div v-if="p.strategies.length === 0" class="gate-overview-note">无子策略</div>
            </td>
          </tr>
        </tbody>
      </table>
      <div class="gate-overview-foot">
        运行中修改组合/策略配置需停止并重启实盘会话后生效；本面板只读，不改变任何判定。
      </div>
    </template>
    <div v-else class="gate-state">加载中…</div>
  </div>
</template>

<style scoped>
/* ===== 市场多空状态卡（口径与样式自备选股池原样迁移） ===== */
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

.gate-state { display: flex; align-items: center; gap: 8px; padding: 16px 18px; font-size: 12.5px; color: var(--ink-soft); }
.gate-spinner {
  width: 11px; height: 11px; border-radius: 50%; flex: 0 0 auto;
  border: 2px solid var(--line); border-top-color: var(--accent);
  animation: gate-spin .8s linear infinite;
}
@keyframes gate-spin { to { transform: rotate(360deg); } }
.gate-error { padding: 13px 18px; font-size: 12.5px; line-height: 1.6; color: var(--red); background: var(--red-wash); }

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
  .scope-grid { grid-template-columns: minmax(0, 1fr); }
  .gate-overview-table { display: block; overflow-x: auto; }
}

/* ===== E5 统一市场状态面板 ===== */
.gate-overview { margin-bottom: 12px; }
.gate-overview-head { display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap; padding: 14px 18px 10px; }
.gate-overview-head h3 { margin: 0; font-size: 14px; }
.gate-overview-hint { font-size: 11.5px; color: var(--ink-faint); }
.scope-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; padding: 0 18px 14px; }
.scope-item { border: 1px solid var(--line); border-radius: 8px; padding: 10px 12px; background: var(--surface-2, transparent); }
.scope-title { font-size: 12.5px; font-weight: 600; margin-bottom: 6px; }
.scope-row { font-size: 11.5px; line-height: 1.55; color: var(--ink-soft); }
.scope-row > span { display: inline-block; min-width: 44px; color: var(--ink-faint); }
.gate-overview-table { width: 100%; border-collapse: collapse; font-size: 12px; }
.gate-overview-table th, .gate-overview-table td { text-align: left; padding: 8px 12px; border-top: 1px solid var(--line); vertical-align: top; }
.gate-overview-table th { font-size: 11px; font-weight: 600; color: var(--ink-faint); }
.gate-overview-note { font-size: 10.5px; color: var(--ink-faint); margin-top: 3px; }
.scope-strategy { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; margin-bottom: 4px; }
.gate-overview-foot { padding: 10px 18px 14px; font-size: 11px; color: var(--ink-faint); }
</style>
