<script setup lang="ts">
import { ref, onMounted, onUnmounted } from 'vue'
import {
  getStockPools, getFormulas, getPortfolios, getBacktestRecords,
  getLiveSessions, getSystemConfigs, getSystemStatus, getLiveChannel,
  type BacktestRecordItem, type SystemConfig, type SystemStatus,
  type LiveSessionItem,
} from '../api'

const loading = ref(true)
const errorMsg = ref('')

const poolCount = ref(0)
const formulaCount = ref(0)
const portfolioCount = ref(0)
const backtestCount = ref(0)
const liveCount = ref(0)
const liveRunning = ref(0)
const recentBacktests = ref<BacktestRecordItem[]>([])
const cfg = ref<SystemConfig | null>(null)
const status = ref<SystemStatus | null>(null)
const liveSessions = ref<LiveSessionItem[]>([])

/** 通道卡片三态：loading（占位）/timeout（5s 未回，先占位）/error/ok */
type ChannelState = 'loading' | 'ok' | 'timeout' | 'error'
interface ChannelItem {
  key: string; label: string; online: boolean | null; state: ChannelState
  detail: string; checkedAt: string | null
}
const CHANNEL_DEFS = [
  { key: 'tdx', label: '通达信 TQ' },
  { key: 'ths', label: '同花顺' },
  { key: 'goldminer', label: '掘金量化' },
  { key: 'iquant', label: 'QMT 通道' },
] as const
/** 通道探活独立分块：5s 内没回就出占位文案，不再卡整页 */
const CHANNEL_TIMEOUT_MS = 5000
const channels = ref<ChannelItem[]>([])
let channelSeq = 0

const ENGINE_LABEL: Record<string, string> = {
  tdx: '通达信 TQ', ths: '同花顺', iquant: 'QMT 通道', goldminer: '掘金量化',
}
const EXEC_MODE_LABEL: Record<string, string> = {
  alert: '预警', auto: '自动',
}

/** 对账差异 tooltip：逐条列出，并标注该差异是几点算的（旧接口无 computed_at 则省略）。 */
function mismatchTitle(s: LiveSessionItem): string {
  return (s.reconcile_mismatches || [])
    .map((m) => {
      const base = `${m.code} 虚${m.virtual}/实${m.real}`
      const t = m.computed_at && m.computed_at.length >= 19 ? m.computed_at.slice(11, 19) : ''
      return t ? `${base}（算于 ${t}）` : base
    })
    .join('；')
}

/** 通道卡片单独加载：互不阻塞，超时先占位、迟到结果仍会刷新 */
function loadChannels(silent = false) {
  const seq = ++channelSeq
  // 静默刷新（30s 轮询）不重置卡片，避免每次轮询闪一下「加载中」
  if (!silent || channels.value.length !== CHANNEL_DEFS.length) {
    channels.value = CHANNEL_DEFS.map((c) => ({
      key: c.key, label: c.label, online: null, state: 'loading' as ChannelState,
      detail: '', checkedAt: null,
    }))
  }
  CHANNEL_DEFS.forEach((c, i) => {
    let settled = false
    const timer = setTimeout(() => {
      if (seq !== channelSeq || settled) return
      channels.value[i] = { ...channels.value[i], state: 'timeout' }
    }, CHANNEL_TIMEOUT_MS)
    getLiveChannel(c.key)
      .then((p) => {
        settled = true
        clearTimeout(timer)
        if (seq !== channelSeq) return
        channels.value[i] = {
          key: c.key, label: p.label || c.label, online: p.online, state: 'ok',
          detail: p.detail || '', checkedAt: p.checked_at || null,
        }
      })
      .catch(() => {
        settled = true
        clearTimeout(timer)
        if (seq !== channelSeq) return
        channels.value[i] = { ...channels.value[i], state: 'error' }
      })
  })
}

async function load() {
  errorMsg.value = ''
  // 主数据全等（实测 2–86ms）；通道探活单独走 loadChannels，不参与整页 gate
  const results = await Promise.allSettled([
    getStockPools(), getFormulas(), getPortfolios(), getBacktestRecords(),
    getLiveSessions(), getSystemConfigs(), getSystemStatus(),
  ])
  const [pools, formulas, portfolios, backtests, lives, configs, sys] = results

  if (pools.status === 'fulfilled') poolCount.value = pools.value.length
  if (formulas.status === 'fulfilled') formulaCount.value = formulas.value.length
  if (portfolios.status === 'fulfilled') portfolioCount.value = portfolios.value.length
  if (backtests.status === 'fulfilled') {
    backtestCount.value = backtests.value.length
    // 最近 5 条回测记录，新的在前
    recentBacktests.value = backtests.value.slice(0, 5)
  }
  if (lives.status === 'fulfilled') {
    liveSessions.value = lives.value
    liveCount.value = lives.value.length
    liveRunning.value = lives.value.filter((s) => s.status === 'running').length
  }
  if (configs.status === 'fulfilled') cfg.value = configs.value
  if (sys.status === 'fulfilled') status.value = sys.value

  const failed = results.filter(r => r.status === 'rejected')
  if (failed.length > 0) {
    errorMsg.value = `部分数据加载失败：${failed.length} 项（其余卡片正常）`
  }
  loading.value = false
  loadChannels()
}

function channelText(c: ChannelItem): string {
  if (c.state === 'loading') return '加载中…'
  // 状态词只有「在线 / 离线」：探测超时/失败/旧接口未回一律按离线展示（红），
  // 不另造第三种词，也不靠会话是否启动推断。
  if (c.state !== 'ok') return '离线'
  return c.online === true ? '在线' : '离线'
}

/** 状态色：在线绿 / 离线红（四通道同一套）；加载中灰 */
function channelClass(c: ChannelItem): string {
  if (c.state === 'loading') return 'idle'
  return c.state === 'ok' && c.online === true ? 'ok' : 'bad'
}

/** 次要信息主体：去掉 http(s):// 协议头——200px 列宽下带协议头必被省略号切掉 */
function channelDetail(c: ChannelItem): string {
  if (c.state === 'loading') return ''
  return (c.detail || '').replace(/https?:\/\//gi, '').replace(/\s+/g, ' ').trim()
}

/** 次要信息第二行：最近心跳；探测失败时改为「按离线显示」的原因（整行完整可见） */
function channelSub(c: ChannelItem): string {
  if (c.state === 'loading') return ''
  if (c.state === 'timeout') return '探测超时，按离线显示'
  if (c.state === 'error') return '探测失败，按离线显示'
  const hb = c.checkedAt && c.checkedAt.length >= 19 ? c.checkedAt.slice(11, 19) : ''
  return hb ? `心跳 ${hb}` : ''
}

/** 悬浮提示：两行合并成一条，鼠标悬停可看完整原文 */
function channelMeta(c: ChannelItem): string {
  return [channelDetail(c), channelSub(c)].filter(Boolean).join(' · ')
}

function fmtDate(d: string | null): string {
  return d ? d.slice(0, 10) : '-'
}

// 通道卡定时复探（30s）：页面常开时客户端退出/桥断开也能及时从绿转红，
// 不再只在首屏探一次（2026-11-13）
let channelTimer: ReturnType<typeof setInterval> | null = null

onMounted(() => {
  load()
  channelTimer = setInterval(() => loadChannels(true), 30000)
})
onUnmounted(() => {
  if (channelTimer !== null) {
    clearInterval(channelTimer)
    channelTimer = null
  }
})
</script>

<template>
  <div class="dash-hero">
    <div class="dash-hero-main">
      <div class="dash-hero-stats">
        <span><b>{{ poolCount }}</b> 股票池</span>
        <span><b>{{ formulaCount }}</b> 公式</span>
        <span><b>{{ portfolioCount }}</b> 组合</span>
        <span><b>{{ backtestCount }}</b> 回测</span>
        <span><b class="db-accent">{{ liveCount }}</b> 实盘</span>
      </div>
    </div>
    <div class="dash-hero-actions">
      <router-link class="btn" to="/guide">使用说明</router-link>
      <button @click="load" class="btn">刷新</button>
    </div>
  </div>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:16px">
    {{ errorMsg }}
  </div>

  <div v-if="loading" class="card" style="padding:16px"><p>加载中…</p></div>
  <template v-else>
    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">01</span>
        <span class="frame-title">核心指标</span>
      </div>
      <div class="frame-body">
        <div class="dash-stats">
          <div class="dash-stat">
            <span class="dash-stat-label">股票池</span>
            <b class="dash-stat-value">{{ poolCount }}</b>
          </div>
          <div class="dash-stat">
            <span class="dash-stat-label">公式</span>
            <b class="dash-stat-value">{{ formulaCount }}</b>
          </div>
          <div class="dash-stat">
            <span class="dash-stat-label">组合策略</span>
            <b class="dash-stat-value">{{ portfolioCount }}</b>
          </div>
          <div class="dash-stat">
            <span class="dash-stat-label">回测记录</span>
            <b class="dash-stat-value">{{ backtestCount }}</b>
          </div>
          <div class="dash-stat">
            <span class="dash-stat-label">实盘会话</span>
            <b class="dash-stat-value">运行中 {{ liveRunning }} · 共 {{ liveCount }}</b>
          </div>
          <div class="dash-stat">
            <span class="dash-stat-label">Core 状态</span>
            <b class="dash-stat-value">{{ status ? (status.core.online ? '在线' : '离线') : '-' }}</b>
            <span class="dash-stat-sub">
              {{ status ? `v${status.core.version} · 运行 ${status.core.uptime}` : '未获取到状态' }}
            </span>
          </div>
        </div>
      </div>
    </div>

    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">02</span>
        <span class="frame-title">实盘会话</span>
      </div>
      <div class="frame-body">
        <div class="table-wrap">
          <table class="dash-table">
            <thead><tr><th>名称</th><th>引擎</th><th>执行模式</th><th>组合</th><th>状态</th><th>对账差异</th></tr></thead>
            <tbody>
              <tr v-for="s in liveSessions" :key="s.id">
                <td>{{ s.name }}</td>
                <td>{{ ENGINE_LABEL[s.engine ?? ''] || s.engine || '—' }}</td>
                <td>{{ EXEC_MODE_LABEL[s.execution_mode ?? ''] || s.execution_mode || '—' }}</td>
                <td style="color:var(--ink-soft)">{{ (s.portfolio_ids || []).length }} 个组合</td>
                <td>
                  <span v-if="s.status === 'running'" class="badge badge-green">运行中</span>
                  <span v-else class="badge badge-gray">已停止</span>
                </td>
                <td>
                  <span v-if="(s.reconcile_mismatches || []).length"
                        class="badge badge-red"
                        :title="mismatchTitle(s)">
                    {{ (s.reconcile_mismatches || []).length }} 项
                  </span>
                  <span v-else style="color:var(--ink-soft)">—</span>
                </td>
              </tr>
            </tbody>
          </table>
          <div v-if="liveSessions.length === 0" class="empty-state"><p>暂无实盘会话</p></div>
        </div>
      </div>
    </div>

    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">03</span>
        <span class="frame-title">交易通道</span>
      </div>
      <div class="frame-body">
        <div class="channel-wrap">
          <div class="channel-grid">
            <div v-for="c in channels" :key="c.key" class="channel-card">
              <span class="channel-dot" :class="channelClass(c)"></span>
              <span class="metric-label channel-name" :title="c.label">{{ c.label }}</span>
              <span class="channel-status" :class="channelClass(c)">{{ channelText(c) }}</span>
              <span class="channel-meta" :title="channelMeta(c)">
                <span class="meta-line">{{ channelDetail(c) || '—' }}</span>
                <span v-if="channelSub(c)" class="meta-line meta-sub">{{ channelSub(c) }}</span>
              </span>
            </div>
          </div>
        </div>
      </div>
    </div>

    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">04</span>
        <span class="frame-title">最近回测</span>
      </div>
      <div class="frame-body">
        <div class="table-wrap">
          <table class="dash-table">
            <thead><tr><th>名称</th><th>区间</th><th>状态</th></tr></thead>
            <tbody>
              <tr v-for="r in recentBacktests" :key="r.id">
                <td>{{ r.name }}</td>
                <td style="color:var(--ink-soft);font-variant-numeric:tabular-nums">{{ fmtDate(r.start_date) }} ~ {{ fmtDate(r.end_date) }}</td>
                <td>
                  <span v-if="r.status === 'completed'" class="badge badge-green">完成</span>
                  <span v-else-if="r.status === 'running'" class="badge badge-blue">运行中</span>
                  <span v-else class="badge badge-gray">{{ r.status }}</span>
                </td>
              </tr>
            </tbody>
          </table>
          <div v-if="recentBacktests.length === 0" class="empty-state"><p>暂无回测记录</p></div>
        </div>
      </div>
    </div>

    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">05</span>
        <span class="frame-title">系统配置</span>
      </div>
      <div class="frame-body">
        <p v-if="cfg" class="dash-cfg">
          通达信路径：{{ cfg.tdx_path }}<br>
          掘金桥：{{ cfg.goldminer?.base_url ?? '—' }}<br>
          掘金账户：{{ cfg.goldminer?.account_id || '—' }}
        </p>
        <p v-else class="dash-cfg">配置加载失败</p>
      </div>
    </div>
  </template>
</template>

<style scoped>
.dash-hero {
  display: flex;
  align-items: flex-end;
  justify-content: space-between;
  gap: 20px;
  padding: 4px 2px 20px;
  margin-bottom: 18px;
  border-bottom: 1px solid var(--line);
}
.dash-hero-stats { display: flex; flex-wrap: wrap; gap: 8px 22px; font-size: 12px; color: var(--ink-soft) }
.dash-hero-stats b { font-family: var(--font-serif); font-size: 18px; font-weight: 800; color: var(--ink); margin-right: 5px; font-variant-numeric: tabular-nums }
.dash-hero-stats .db-accent { color: var(--red) }
.dash-hero-actions { flex-shrink: 0; display: flex; align-items: center; gap: 8px }

/* 本页密度：全局 .metric-grid/.metric-card 已被回测详情占用（12 列 + span 6），
   这里不再复用，改用本页自有紧凑类，避免被全局规则撑成大卡。 */
.frame { margin-bottom: 10px }
.frame-head { padding: 8px 14px }
.frame-body { padding: 10px 14px }

/* 01 核心指标：一行数字条，标签与数字同处一个紧凑单元，项间细线分隔 */
.dash-stats { display: flex; flex-wrap: wrap; align-items: baseline; row-gap: 5px }
.dash-stat { display: flex; align-items: baseline; gap: 6px; padding: 0 12px; min-width: 0 }
.dash-stat:first-child { padding-left: 0 }
.dash-stat + .dash-stat { border-left: 1px solid var(--line) }
.dash-stat-label { font-size: 11.5px; color: var(--ink-soft); white-space: nowrap }
.dash-stat-value {
  font-family: var(--font-serif); font-size: 16px; font-weight: 800; line-height: 1.3;
  color: var(--ink); font-variant-numeric: tabular-nums; white-space: nowrap;
}
.dash-stat-sub { font-size: 10.5px; color: var(--ink-faint); white-space: nowrap }

/* 02/04 表格：收紧行高与内边距，保留全部列 */
.dash-table { font-size: 12.5px }
.dash-table thead th { padding: 6px 10px; font-size: 10px }
.dash-table tbody td { padding: 5px 10px }
.dash-table .badge { padding: 1px 7px; font-size: 10.5px }
.empty-state { padding: 14px 10px; font-size: 12px }

/* 05 系统配置 */
.dash-cfg { margin: 0; color: var(--ink-soft); font-size: 12.5px; line-height: 1.6 }

/* 03 交易通道：四框一行（≥1100px），<1100px 两列、<760px 单列。
   卡内为紧凑网格：圆点+名称一行、状态一行、附加信息（地址/账号、心跳）两行；
   宽屏名称预留两行高度，四卡等高且状态行对齐，不因长名称错位。 */
.channel-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px }
.channel-card {
  display: grid; grid-template-columns: 7px minmax(0, 1fr); column-gap: 7px; row-gap: 3px;
  align-content: start; align-items: start; min-width: 0; padding: 8px 10px 9px;
  border: 1px solid var(--line); border-radius: var(--radius);
  background: var(--bg); box-shadow: none; grid-column: auto;
}
.channel-dot {
  grid-column: 1; grid-row: 1; width: 7px; height: 7px; border-radius: 50%;
  flex: 0 0 auto; background: var(--ink-faint); margin-top: 5px;
}
.channel-dot.ok { background: var(--green) }
.channel-dot.bad { background: var(--red) }
.channel-dot.idle { background: var(--ink-faint) }
.channel-name {
  grid-column: 2; grid-row: 1; min-height: 2.7em;
  font-size: 12.5px; font-weight: 600; line-height: 1.35;
  letter-spacing: 0; text-transform: none; color: var(--ink);
  white-space: normal; overflow-wrap: anywhere;
}
.channel-status {
  grid-column: 2; grid-row: 2;
  font-size: 12px; font-weight: 700; line-height: 1.3;
  white-space: nowrap; overflow-wrap: normal;
}
.channel-status.ok { color: var(--green) }
.channel-status.bad { color: var(--red) }
.channel-status.idle { color: var(--ink-soft) }
.channel-meta {
  grid-column: 2; grid-row: 3; display: flex; flex-direction: column; gap: 1px;
  min-width: 0; font-size: 11px; line-height: 1.35; color: var(--ink-soft);
  overflow-wrap: anywhere; font-variant-numeric: tabular-nums;
}
.meta-line { display: block }
.meta-sub { color: var(--ink-faint) }
@media (max-width: 1100px) {
  .dash-hero { flex-wrap: wrap; align-items: center; row-gap: 10px; padding-bottom: 12px; margin-bottom: 14px; }
  .channel-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px }
  .channel-name { min-height: 0 }
}
@media (max-width: 760px) {
  .channel-grid { grid-template-columns: minmax(0, 1fr) }
  .dash-stat { padding: 0 9px }
}
</style>
