<script setup lang="ts">
import { ref, onMounted } from 'vue'
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
function loadChannels() {
  const seq = ++channelSeq
  channels.value = CHANNEL_DEFS.map((c) => ({
    key: c.key, label: c.label, online: null, state: 'loading' as ChannelState,
    detail: '', checkedAt: null,
  }))
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

onMounted(load)
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
        <div class="metric-grid" style="gap:12px;margin-bottom:0">
          <div class="metric-card">
            <span class="metric-label">股票池</span>
            <span class="metric-value">{{ poolCount }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">公式</span>
            <span class="metric-value">{{ formulaCount }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">组合策略</span>
            <span class="metric-value">{{ portfolioCount }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">回测记录</span>
            <span class="metric-value">{{ backtestCount }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">实盘会话</span>
            <span class="metric-value" :style="{ fontSize: '15px' }">
              运行中 {{ liveRunning }} · 共 {{ liveCount }}
            </span>
          </div>
          <div class="metric-card">
            <span class="metric-label">Core 状态</span>
            <span class="metric-value" :style="{ fontSize: '15px' }">
              {{ status ? (status.core.online ? '在线' : '离线') : '-' }}
            </span>
            <span class="metric-label" style="font-size:11px">
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
          <table>
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
          <div class="metric-grid channel-grid">
            <div v-for="c in channels" :key="c.key" class="metric-card channel-card">
              <span class="metric-label">{{ c.label }}</span>
              <span class="metric-value channel-status" :class="channelClass(c)" :style="{ fontSize: '15px' }">
                {{ channelText(c) }}
              </span>
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
          <table>
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
        <p v-if="cfg" style="margin:0;color:var(--ink-soft);line-height:1.8">
          通达信路径：{{ cfg.tdx_path }}<br>
          掘金桥：{{ cfg.goldminer?.base_url ?? '—' }}<br>
          掘金账户：{{ cfg.goldminer?.account_id || '—' }}
        </p>
        <p v-else style="margin:0;color:var(--ink-soft)">配置加载失败</p>
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
.dash-hero-actions { flex-shrink: 0 }
/* 交易通道四卡：只允许「4 列一行」「2 列」「1 列」三种排布，永不出现 3+1 孤行。
   判据用容器查询按「内容宽」判定（侧栏展开/收起时媒体查询会出现 1280 反而 3 列的怪相）：
   · ≥ 836px（4×200 + 3×12）→ 4 列一行
   · ≥ 416px（2×200 + 12，留 4px 余量）→ 2 列
   · 更窄 → 1 列（<416px 时两列每卡不足 200px，文字会碎成多行，单列更舒服） */
.channel-wrap { container-type: inline-size; }
.channel-grid {
  grid-template-columns: minmax(0, 1fr);
  gap: 12px;
  margin-bottom: 0;
}
@container (min-width: 416px) {
  .channel-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@container (min-width: 836px) {
  .channel-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
}
.channel-card { min-width: 0 }
/* 名称/状态允许折行（窄卡不裁切）；通道名最长「同花顺独立交易（UI 自动化）」 */
.channel-card .metric-label {
  white-space: normal;
  line-height: 1.35;
  overflow-wrap: anywhere;
}
.channel-status {
  white-space: normal;
  line-height: 1.3;
  overflow-wrap: anywhere;
}
.channel-status.ok { color: var(--green) }
.channel-status.bad { color: var(--red) }
.channel-status.idle { color: var(--ink-soft) }
/* 次要信息（桥地址 / 账号 / 心跳）：强制两行——主体一行、心跳或降级原因一行，
   200px 最小列宽下两条都能完整显示，不再依赖省略号或 line-clamp 截断。 */
.channel-meta {
  display: flex;
  flex-direction: column;
  gap: 1px;
  font-size: 11px;
  line-height: 1.35;
  min-height: 2.7em;
  color: var(--ink-soft);
  overflow-wrap: anywhere;
  font-variant-numeric: tabular-nums;
}
.meta-line { display: block }
.meta-sub { color: var(--ink-faint) }
@media (max-width: 1100px) {
  .dash-hero { flex-wrap: wrap; align-items: center; row-gap: 10px; padding-bottom: 12px; margin-bottom: 14px; }
}
</style>
