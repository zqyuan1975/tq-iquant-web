<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { getHealthOverview, type HealthOverview, type HealthSessionItem } from '../api'

const data = ref<HealthOverview | null>(null)
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
  if (s.data.healthy) return '正常'
  return `缺失/陈旧 ${s.data.bad_total ?? 0}`
}

function dataBadge(s: HealthSessionItem): string {
  if (s.data.unavailable || !s.data.available) return 'badge-gray'
  return s.data.healthy ? 'badge-green' : 'badge-red'
}

function fmtTime(iso: string | null): string {
  return iso && iso.length >= 19 ? iso.replace('T', ' ').slice(0, 19) : '—'
}

async function load() {
  busy.value = true
  errorMsg.value = ''
  try {
    data.value = await getHealthOverview()
  } catch (e) {
    errorMsg.value = '加载失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    loading.value = false
    busy.value = false
  }
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
        <div class="metric-grid" style="gap:12px;margin-bottom:0">
          <div class="metric-card">
            <span class="metric-label">Core</span>
            <span class="metric-value">{{ data.core.online ? '在线' : '离线' }}</span>
            <span class="metric-sub">v{{ data.core.version || '—' }} · {{ data.core.uptime || '—' }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">引擎 watchdog</span>
            <span class="metric-value">{{ data.watchdog.running ? '运行中' : '未运行' }}</span>
            <span class="metric-sub">
              告警 {{ data.watchdog.active_alarms.length }} 条 · 扫描 {{ fmtTime(data.watchdog.last_scan_at) }}
            </span>
          </div>
          <div class="metric-card">
            <span class="metric-label">自动备份</span>
            <span class="metric-value">{{ data.tasks.backup_done_today ? '今日已完成' : '今日未完成' }}</span>
            <span class="metric-sub">计划 {{ data.tasks.backup_at || '—' }}</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">磁盘剩余</span>
            <span class="metric-value" :style="{ color: data.disk.ok ? 'var(--green)' : 'var(--red)' }">
              {{ data.disk.free_mb === null ? '—' : `${data.disk.free_mb} MB` }}
            </span>
            <span class="metric-sub">保护阈值 {{ data.disk.min_free_mb }} MB</span>
          </div>
          <div class="metric-card">
            <span class="metric-label">待处理告警</span>
            <span class="metric-value" :style="{ color: alertTotal ? 'var(--red)' : 'var(--ink)' }">{{ alertTotal }}</span>
            <span class="metric-sub">
              未确认 {{ data.alert_totals.open_decisions }} · 顺延 {{ data.alert_totals.deferred_orders }} ·
              待审批 {{ data.alert_totals.pending_approvals }}
            </span>
          </div>
          <div class="metric-card">
            <span class="metric-label">告警外发</span>
            <span class="metric-value">{{ data.notify.usable ? '已配置' : (data.notify.enabled ? '未就绪' : '关闭') }}</span>
            <span class="metric-sub">
              {{ data.notify.channels.map((c) => NOTIFY_LABEL[c] || c).join(' / ') || '未配置渠道' }}
              · 最低 {{ data.notify.min_level }}
            </span>
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
          <table>
            <thead>
              <tr>
                <th>名称</th><th>状态</th><th>引擎</th><th>通道</th><th>桥</th>
                <th>数据新鲜度</th><th>今日委托/成交</th>
                <th>未确认</th><th>顺延</th><th>待审批</th>
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
                <td><span class="badge" :class="dataBadge(s)">{{ dataText(s) }}</span></td>
                <td class="num">{{ s.today.orders }} / {{ s.today.trades }}</td>
                <td class="num">{{ s.alerts.open_decisions }}</td>
                <td class="num">{{ s.alerts.deferred_orders }}</td>
                <td class="num">{{ s.alerts.pending_approvals }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="data.sessions.length === 0" class="empty-state"><p>暂无实盘会话</p></div>
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
.metric-sub { font-size: 11px; color: var(--ink-soft); line-height: 1.4; overflow-wrap: anywhere }
.problem-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 8px }
.problem-list li { display: flex; align-items: flex-start; gap: 8px; font-size: 13px; line-height: 1.5 }
.num { font-variant-numeric: tabular-nums; text-align: right }
@media (max-width: 1100px) {
  .dash-hero { flex-wrap: wrap; align-items: center; row-gap: 10px; padding-bottom: 12px; margin-bottom: 14px; }
}
</style>
