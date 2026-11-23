<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import {
  getDataObservabilitySummary, getHealthOverview,
  type DataObservabilitySummary, type HealthOverview, type HealthSessionItem,
} from '../api'

const data = ref<HealthOverview | null>(null)
const obs = ref<DataObservabilitySummary | null>(null)
const obsError = ref('')
const loading = ref(true)
const errorMsg = ref('')
const busy = ref(false)

const LEVEL_TEXT: Record<string, string> = {
  ok: '健康', warn: '需关注', error: '异常',
}
const LEVEL_BADGE: Record<string, string> = {
  ok: 'badge-green', warn: 'badge-blue', error: 'badge-red',
}
const ENGINE_LABEL: Record<string, string> = {
  tdx: '通达信 TQ', ths: '同花顺', iquant: 'QMT 通道', goldminer: '掘金量化',
}
const NOTIFY_LABEL: Record<string, string> = {
  feishu: '飞书', wecom: '企微', email: '邮件', sound: '提示音',
}

const alertTotal = computed(() => {
  const t = data.value?.alert_totals
  return t ? t.open_decisions + t.deferred_orders + t.pending_approvals : 0
})

function badgeLevel(level: string): string {
  return LEVEL_BADGE[level] || 'badge-gray'
}

function statusBadge(status: string): string {
  if (status === 'running') return 'badge-green'
  if (status === 'stopping') return 'badge-blue'
  return 'badge-gray'
}

function onlineText(v: boolean | null | undefined): string {
  if (v === true) return '在线'
  if (v === false) return '离线'
  return '—'
}

function dataText(s: HealthSessionItem): string {
  if (s.data.unavailable) return '不可用'
  if (!s.data.available) return '—'
  return s.data.healthy ? '正常' : '有缺失/陈旧'
}

function dataBadge(s: HealthSessionItem): string {
  if (s.data.unavailable || !s.data.available) return 'badge-gray'
  return s.data.healthy ? 'badge-green' : 'badge-red'
}

interface GuardCheck {
  key?: string
  label?: string
  ok?: boolean
  detail?: string
}

/** 后端在 channel_guard 内返回逐项 checks（5 项）；旧数据无该字段时返回空数组。 */
function guardChecks(s: HealthSessionItem): GuardCheck[] {
  const raw = (s.channel_guard as unknown as { checks?: GuardCheck[] }).checks
  return Array.isArray(raw) ? raw : []
}

/** 逐周期新鲜度（1d/1m…），替代原先单一聚合数字。 */
function periodEntries(s: HealthSessionItem): [string, { stale: number; empty: number; total: number }][] {
  return Object.entries(s.data.periods ?? {})
}

function fmtTime(iso: string | null): string {
  return iso && iso.length >= 19 ? iso.replace('T', ' ').slice(0, 19) : '—'
}

async function load() {
  busy.value = true
  errorMsg.value = ''
  obsError.value = ''
  // 主总览失败才阻断页面；观测汇总失败只在卡片内提示（两者互相独立）。
  const [healthRes, obsRes] = await Promise.allSettled([
    getHealthOverview(), getDataObservabilitySummary(),
  ])
  if (healthRes.status === 'fulfilled') {
    data.value = healthRes.value
  } else {
    const e = healthRes.reason
    errorMsg.value = '加载失败: ' + (e instanceof Error ? e.message : String(e))
  }
  if (obsRes.status === 'fulfilled') {
    obs.value = obsRes.value
  } else {
    const e = obsRes.reason
    obsError.value = '观测汇总加载失败: ' + (e instanceof Error ? e.message : String(e))
  }
  loading.value = false
  busy.value = false
}

/** 口径过期原因 → 「原因 × N」文本（后端已给中文原因，原样展示）。 */
function caliberReasonText(): string {
  const reasons = obs.value?.caliber.reasons || {}
  const parts = Object.entries(reasons).map(([k, n]) => `${k} × ${n}`)
  return parts.length ? parts.join('；') : '—'
}

let timer: ReturnType<typeof setInterval> | null = null
onMounted(() => {
  load()
  // 30s 静默刷新：与「状态总览」的通道复探同频，页面常开也能看到通道/告警变化
  timer = setInterval(load, 30000)
})
onUnmounted(() => {
  if (timer !== null) {
    clearInterval(timer)
    timer = null
  }
})
</script>

<template>
  <div class="dash-hero">
    <div class="dash-hero-main">
      <div class="dash-hero-stats" v-if="data">
        <span>
          <span class="badge" :class="badgeLevel(data.level)">{{ LEVEL_TEXT[data.level] || data.level }}</span>
        </span>
        <span><b>{{ data.sessions.length }}</b> 会话</span>
        <span><b>{{ data.tasks.running_sessions }}</b> 运行中</span>
        <span><b class="db-accent">{{ alertTotal }}</b> 待处理</span>
        <span>生成于 {{ fmtTime(data.generated_at) }}</span>
      </div>
    </div>
    <div class="dash-hero-actions">
      <button class="btn" :disabled="busy" @click="load">{{ busy ? '刷新中…' : '刷新' }}</button>
    </div>
  </div>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:16px">
    {{ errorMsg }}
  </div>

  <div v-if="loading" class="card" style="padding:16px"><p>加载中…</p></div>
  <template v-else-if="data">
    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">01</span>
        <span class="frame-title">核心 / 当日任务 / 磁盘</span>
      </div>
      <div class="frame-body">
        <div class="health-stats">
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">Core</span>
              <b :style="{ color: data.core.online ? 'var(--green)' : 'var(--red)' }">{{ data.core.online ? '在线' : '离线' }}</b>
            </div>
            <i class="hs-sub" :title="`v${data.core.version || '—'} · ${data.core.uptime || '—'}`">v{{ data.core.version || '—' }} · {{ data.core.uptime || '—' }}</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">引擎 watchdog</span>
              <b>{{ data.watchdog.running ? '运行中' : '未运行' }}</b>
            </div>
            <i class="hs-sub" :title="`告警 ${data.watchdog.active_alarms.length} 条 · 扫描 ${fmtTime(data.watchdog.last_scan_at)}`">告警 {{ data.watchdog.active_alarms.length }} 条 · 扫描 {{ fmtTime(data.watchdog.last_scan_at) }}</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">自动备份</span>
              <b>{{ data.tasks.backup_done_today ? '今日已完成' : '今日未完成' }}</b>
            </div>
            <i class="hs-sub" :title="`计划 ${data.tasks.backup_at || '—'}`">计划 {{ data.tasks.backup_at || '—' }}</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">磁盘剩余</span>
              <b :style="{ color: data.disk.ok ? 'var(--green)' : 'var(--red)' }">
                {{ data.disk.free_mb === null ? '—' : `${data.disk.free_mb} MB` }}
              </b>
            </div>
            <i class="hs-sub" :title="`保护阈值 ${data.disk.min_free_mb} MB`">保护阈值 {{ data.disk.min_free_mb }} MB</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">待处理告警</span>
              <b :style="{ color: alertTotal ? 'var(--red)' : 'var(--ink)' }">{{ alertTotal }}</b>
            </div>
            <i class="hs-sub" :title="`未处理告警 ${data.alert_totals.open_decisions} · 顺延 ${data.alert_totals.deferred_orders} · 待审批 ${data.alert_totals.pending_approvals}`">
              未处理告警 {{ data.alert_totals.open_decisions }} · 顺延 {{ data.alert_totals.deferred_orders }} ·
              <RouterLink to="/live?tab=approvals" class="alert-link">待审批 {{ data.alert_totals.pending_approvals }}</RouterLink>
            </i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">告警外发</span>
              <b>{{ data.notify.usable ? '已配置' : (data.notify.enabled ? '未就绪' : '关闭') }}</b>
            </div>
            <i class="hs-sub" :title="`${data.notify.channels.map((c) => NOTIFY_LABEL[c] || c).join(' / ') || '未配置渠道'} · 最低 ${data.notify.min_level}`">
              {{ data.notify.channels.map((c) => NOTIFY_LABEL[c] || c).join(' / ') || '未配置渠道' }}
              · 最低 {{ data.notify.min_level }}
            </i>
          </div>
        </div>
      </div>
    </div>

    <div v-if="data.problems.length" class="frame">
      <div class="frame-head">
        <span class="frame-num">02</span>
        <span class="frame-title">需要关注（{{ data.problems.length }}）</span>
      </div>
      <div class="frame-body">
        <ul class="problem-list">
          <li v-for="(p, i) in data.problems" :key="i" :class="p.level">
            <span class="badge" :class="p.level === 'error' ? 'badge-red' : 'badge-blue'">
              {{ p.level === 'error' ? '异常' : '注意' }}
            </span>
            <span>{{ p.text }}</span>
          </li>
        </ul>
      </div>
    </div>

    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">03</span>
        <span class="frame-title">实盘会话</span>
      </div>
      <div class="frame-body">
        <div class="table-wrap">
          <table class="health-table">
            <colgroup>
              <col class="c-name" /><col class="c-status" /><col class="c-engine" />
              <col class="c-channel" /><col class="c-bridge" /><col class="c-data" />
              <col class="c-today" /><col class="c-alerts" /><col class="c-deferred" />
              <col class="c-approval" />
            </colgroup>
            <thead>
              <tr>
                <th>名称</th><th>状态</th><th>引擎</th><th>通道</th><th>桥</th>
                <th>数据新鲜度</th><th class="t-right">今日委托/成交</th>
                <th class="t-right">未处理告警</th><th class="t-right">顺延</th><th class="t-right">待审批</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="s in data.sessions" :key="s.id">
                <td>{{ s.name }}</td>
                <td><span class="badge" :class="statusBadge(s.status)">{{ s.status }}</span></td>
                <td>{{ ENGINE_LABEL[s.engine || ''] || s.engine || '—' }}</td>
                <td :style="{ color: s.channel_ok ? 'var(--green)' : 'var(--red)' }">
                  {{ s.channel_ok ? onlineText(s.channel.primary?.online) : '不可用' }}
                </td>
                <td>{{ onlineText(s.bridge.online) }}</td>
                <td class="data-cell">
                  <span class="badge" :class="dataBadge(s)">{{ dataText(s) }}</span>
                  <div v-if="s.data.unavailable" class="period-line">
                    {{ s.data.reason || '行情通道不可用' }}
                  </div>
                  <div v-for="[p, v] in periodEntries(s)" :key="p" class="period-line">
                    {{ p }}：陈旧 {{ v.stale }} / 空 {{ v.empty }} / 共 {{ v.total }}
                  </div>
                </td>
                <td class="num">{{ s.today.orders }} / {{ s.today.trades }}</td>
                <td class="num">{{ s.alerts.open_decisions }}</td>
                <td class="num">{{ s.alerts.deferred_orders }}</td>
                <td class="num">
                  <RouterLink
                    v-if="s.alerts.pending_approvals"
                    to="/live?tab=approvals"
                    class="alert-link"
                    title="前往实盘工作台处理审批"
                  >{{ s.alerts.pending_approvals }}</RouterLink>
                  <span v-else>0</span>
                </td>
              </tr>
            </tbody>
          </table>
          <div v-if="data.sessions.length === 0" class="empty-state"><p>暂无实盘会话</p></div>
        </div>
      </div>
    </div>

    <div v-if="data.sessions.length" class="frame">
      <div class="frame-head">
        <span class="frame-num">04</span>
        <span class="frame-title">通道检查明细（逐项）</span>
      </div>
      <div class="frame-body">
        <div v-for="s in data.sessions" :key="s.id" class="guard-block">
          <div class="guard-head">
            <b>{{ s.name }}</b>
            <span class="badge" :class="s.channel_guard.ok ? 'badge-green' : 'badge-orange'">
              {{ s.channel_guard.available
                ? (s.channel_guard.ok ? '全部通过'
                  : `未通过 ${s.channel_guard.failed.length} 项`)
                : '检查不可用' }}
            </span>
          </div>
          <ul v-if="guardChecks(s).length" class="guard-list">
            <li v-for="c in guardChecks(s)" :key="c.key || c.label" :class="{ bad: c.ok === false }">
              <span class="badge" :class="c.ok ? 'badge-green' : 'badge-red'">
                {{ c.ok ? '通过' : '未通过' }}
              </span>
              <span class="guard-label">{{ c.label || c.key || '—' }}</span>
              <span class="guard-detail">{{ c.detail || '—' }}</span>
            </li>
          </ul>
          <p v-else class="guard-detail">该会话没有可用的通道检查结果。</p>
        </div>
      </div>
    </div>
    <div class="frame">
      <div class="frame-head">
        <span class="frame-num">05</span>
        <span class="frame-title">数据与口径观测</span>
      </div>
      <div class="frame-body">
        <div v-if="obsError" class="obs-error">{{ obsError }}</div>
        <div v-else-if="obs" class="health-stats obs-stats">
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">公司行为未验证</span>
              <b :style="{ color: obs.corporate_actions.unverified ? 'var(--red)' : 'var(--ink)' }">{{ obs.corporate_actions.unverified }}</b>
            </div>
            <i class="hs-sub" :title="obs.corporate_actions.codes.join(' ')">
              自动审计{{ obs.corporate_actions.enabled ? '已开启' : '已关闭' }} · 涉及 {{ obs.corporate_actions.codes.length }} 只
            </i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">最近一轮审计</span>
              <b>{{ obs.corporate_actions.last_audit ? (obs.corporate_actions.last_audit.reason || '—') : '未运行' }}</b>
            </div>
            <i class="hs-sub">{{ obs.corporate_actions.last_audit ? fmtTime(obs.corporate_actions.last_audit.at || null) : '暂无本轮结果' }}</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">口径过期标记</span>
              <b :style="{ color: obs.caliber.stale ? 'var(--red)' : 'var(--ink)' }">{{ obs.caliber.stale }}</b>
            </div>
            <i class="hs-sub" :title="caliberReasonText()">共 {{ obs.caliber.records }} 条记录</i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">K 线覆盖台账</span>
              <b>{{ obs.kline_coverage.enabled ? (obs.kline_coverage.exists ? '已有台账' : '未生成') : '未开启' }}</b>
            </div>
            <i class="hs-sub" :title="obs.kline_coverage.ledger_path">
              覆盖 {{ obs.kline_coverage.coverage }} · 空日 {{ obs.kline_coverage.empty_days }} · 指纹 {{ obs.kline_coverage.stamps }}
            </i>
          </div>
          <div class="health-stat">
            <div class="hs-head">
              <span class="hs-label">台账最近更新</span>
              <b>{{ fmtTime(obs.kline_coverage.latest_updated_at) }}</b>
            </div>
            <i class="hs-sub" :title="`生成于 ${fmtTime(obs.generated_at)}`">只读汇总 · 不改计算</i>
          </div>
        </div>
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
.dash-hero-stats { display: flex; flex-wrap: wrap; align-items: center; gap: 8px 22px; font-size: 12px; color: var(--ink-soft) }
.dash-hero-stats b { font-family: var(--font-serif); font-size: 18px; font-weight: 800; color: var(--ink); margin-right: 5px; font-variant-numeric: tabular-nums }
.dash-hero-stats .db-accent { color: var(--red) }
.dash-hero-actions { flex-shrink: 0 }
/* 本页密度：frame 区块与状态条整体收紧，不依赖全局 style.css */
.frame { margin-bottom: 10px }
.frame-head { padding: 6px 12px }
.frame-body { padding: 8px 12px }
/* 01 区单行数字条：主值一行 + 副信息小字（对齐首页状态总览 01 的形态） */
.health-stats { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px 0 }
/* 05 数据与口径观测：5 项单行（比 01 少一格，单独收列宽） */
.obs-stats { grid-template-columns: repeat(5, minmax(0, 1fr)) }
.obs-error { font-size: 12px; color: var(--red) }
.health-stat { display: flex; flex-direction: column; gap: 1px; min-width: 0; padding: 0 12px; border-left: 1px solid var(--line) }
.health-stat:first-child { padding-left: 0; border-left: none }
.hs-head { display: flex; align-items: baseline; gap: 6px; min-width: 0 }
.hs-label { font-size: 11.5px; color: var(--ink-soft); white-space: nowrap; flex: 0 0 auto }
.health-stat > b { min-width: 0; font-family: var(--font-serif); font-size: 16px; font-weight: 800; color: var(--ink); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; font-variant-numeric: tabular-nums }
.health-stat > i { font-style: normal; font-size: 11px; color: var(--ink-faint); white-space: nowrap; overflow: hidden; text-overflow: ellipsis; min-width: 0; font-variant-numeric: tabular-nums }
/* 10 列与 10 个 td 一一对应：固定 table-layout 后列宽只由 colgroup 决定，
   窄屏改为表格内部横向滚动，不会因自动布局挤压而让单元格内容跑位。
   数据新鲜度列容纳「徽标 + 逐周期明细」，仍全部落在同一个 td 内。 */
.health-table { font-size: 13px; table-layout: fixed; min-width: 920px }
.health-table .c-name { width: 8% }
.health-table .c-status { width: 10% }
.health-table .c-engine { width: 12% }
.health-table .c-channel { width: 8% }
.health-table .c-bridge { width: 5% }
.health-table .c-data { width: 18% }
.health-table .c-today { width: 15% }
.health-table .c-alerts { width: 9% }
.health-table .c-deferred { width: 7% }
.health-table .c-approval { width: 8% }
.health-table th, .health-table td { padding: 4px 10px }
.health-table td { vertical-align: top }
.health-table td.num { white-space: nowrap }
.problem-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 4px }
.problem-list li { display: flex; align-items: flex-start; gap: 8px; font-size: 13px; line-height: 1.4 }
.num { font-variant-numeric: tabular-nums; text-align: right }
.period-line { font-size: 11px; color: var(--ink-soft); line-height: 1.5; margin-top: 3px; font-variant-numeric: tabular-nums }
.guard-block { padding: 6px 0; border-top: 1px dashed var(--line) }
.guard-block:first-child { padding-top: 0; border-top: none }
.guard-head { display: flex; align-items: center; gap: 8px; margin-bottom: 4px; font-size: 13px }
.guard-list { list-style: none; margin: 0; padding: 0; display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 3px 14px }
.guard-list li { display: flex; align-items: baseline; gap: 8px; font-size: 12px; line-height: 1.4 }
.guard-list li.bad .guard-label { color: var(--red) }
.guard-label { font-weight: 600; flex-shrink: 0 }
.guard-detail { color: var(--ink-soft); overflow-wrap: anywhere }
.alert-link { color: var(--accent); text-decoration: underline dotted; text-underline-offset: 2px }
.alert-link:hover { text-decoration: underline }
@media (max-width: 1100px) {
  .dash-hero { flex-wrap: wrap; align-items: center; row-gap: 10px; padding-bottom: 12px; margin-bottom: 14px; }
  /* 01 数字条：6 列 -> 3 列（两行），保持「主值 + 副信息」两行结构 */
  .health-stats { grid-template-columns: repeat(3, minmax(0, 1fr)); row-gap: 10px }
  .health-stat { padding-left: 12px; border-left: 1px solid var(--line) }
  .health-stat:nth-child(3n + 1) { padding-left: 0; border-left: none }
}
@media (max-width: 760px) {
  /* 01 数字条：3 列 -> 2 列 */
  .health-stats { grid-template-columns: repeat(2, minmax(0, 1fr)) }
  .health-stat { padding-left: 12px; border-left: 1px solid var(--line) }
  .health-stat:nth-child(2n + 1) { padding-left: 0; border-left: none }
}
</style>
