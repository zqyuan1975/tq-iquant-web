<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue'
import { RouterLink } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import { getHealthOverview, type HealthOverview } from '../api'

interface PlaybookCheck {
  label: string
  detail: string
  to?: string
  linkText?: string
}

interface PlaybookItem {
  key: string
  title: string
  symptom: string
  checks: PlaybookCheck[]
}

const overview = ref<HealthOverview | null>(null)
const loading = ref(false)
const errorMsg = ref('')
let timer: ReturnType<typeof setInterval> | null = null

const PLAYBOOK: PlaybookItem[] = [
  {
    key: 'channel',
    title: '交易通道断线 / 主备切换',
    symptom: '实盘工作台出现「通道离线」告警，下单失败或回执不再返回。',
    checks: [
      { label: '主通道状态', detail: '健康总览「通道」区确认 primary.online 与 configured；主通道不可用会亮 error。', to: '/health', linkText: '查看健康总览' },
      { label: '客户端进程', detail: '通达信/同花顺客户端是否仍在运行；ths 通道还需确认窗口标题与交接目录未被改动。', to: '/system', linkText: '核对通道配置' },
      { label: '通道守卫检查', detail: '健康总览「通道检查明细」逐项列出 5 项检查（主通道配置 / 客户端在线 / 运行态引擎注册 / 备用通道 / 无待生效配置）；未通过项也会出现在「需要关注」。停止态会话不判备用通道。', to: '/health', linkText: '查看逐项明细' },
      { label: '近期通道事件', detail: '关键事件检索里 gate=channel_offline / channel_recovered，确认断线持续时间与恢复时间。', to: '/events', linkText: '检索通道事件' },
    ],
  },
  {
    key: 'market_stall',
    title: '行情停滞 / 数据陈旧',
    symptom: '引擎仍在运行，但最近 bar 很久没有推进，策略不产生新信号。',
    checks: [
      { label: '最近 bar 时间', detail: '健康总览「数据新鲜度」按周期列出 stale/empty 数量；业务心跳开启时 /health 也带最近 bar。', to: '/health', linkText: '查看数据体检' },
      { label: '行情链路', detail: '确认行情源（TQ / 桥）在线；bridge_online=false 或客户端退出都会先表现为行情停摆。', to: '/health', linkText: '查看通道状态' },
      { label: '盘外误报', detail: '午休 11:30–13:00、盘后与非交易日不判停滞；先确认当前是否在连续竞价时段。', to: '/health', linkText: '查看当日任务' },
      { label: '关键事件', detail: '检索 gate=channel_offline，关键字填 market_data_stalled（事件 message 含「判据=market_data_stalled」；行情停滞类事件统一落在 channel_offline）。', to: '/events', linkText: '检索行情事件' },
    ],
  },
  {
    key: 'engine_stale',
    title: '引擎卡死 / 轮次不推进',
    symptom: '进程还在、/health 正常，但引擎长期没有新的 tick 或账户快照。',
    checks: [
      { label: '业务心跳', detail: '开启 health.business_probe_enabled 后，/health 的 business.sessions 会给出 last_tick_at / last_bar_at / 停滞秒数。', to: '/health', linkText: '查看健康总览' },
      { label: 'watchdog 扫描', detail: 'watchdog-status 的 last_scan_at 停止前进 = 扫描线程卡死；活跃告警明细在实盘工作台的通道/风险提示区查看（健康总览只给计数）。', to: '/live', linkText: '查看告警明细' },
      { label: '引擎层告警', detail: 'engine_stale 只进 watchdog 告警，不落决策事件；到实盘工作台看当前告警，通道类离线事件再去关键事件检索 gate=channel_offline。', to: '/live', linkText: '查看 watchdog 告警' },
      { label: '重启会话', detail: '确认无未完成委托后，在实盘工作台停止并重新启动该会话（本向导不代操作）。', to: '/live', linkText: '进入实盘工作台' },
    ],
  },
  {
    key: 'order_stuck',
    title: '委托卡住 / 待放行 / 待审批',
    symptom: '委托长期处于 deferred 或审批队列 pending，未成交也未撤销。',
    checks: [
      { label: '待放行委托', detail: '健康总览 alerts.deferred_orders 按会话计数；点进实盘工作台查看具体委托。', to: '/live', linkText: '处理委托' },
      { label: '待审批单', detail: 'alerts.pending_approvals 非 0 时到实盘工作台「待审批」页签处理：可查看标的/方向/数量/金额/策略与组合/剩余有效期，支持通过或填写原因拒绝；过期单会标记并禁用操作。超时过期只在会话运行中的引擎 tick 内执行。', to: '/live?tab=approvals', linkText: '前往审批' },
      { label: '执行为停滞', detail: '检索 gate=channel_offline，关键字填 execution_stalled（message 含「判据=execution_stalled」，ths 积压/回执未消费会触发）。', to: '/events', linkText: '检索执行事件' },
    ],
  },
  {
    key: 'data_stale',
    title: '数据缺失 / 补齐失败',
    symptom: '数据体检出现 stale/empty，策略回测或实盘选股少了股票。',
    checks: [
      { label: '按周期排查', detail: '健康总览 data.periods 逐周期给出 stale/empty/total，定位是分钟线还是日线缺口。', to: '/health', linkText: '查看数据体检' },
      { label: '行情通道', detail: '数据体检 unavailable 时先修通道（行情源不可用无法补齐）。', to: '/health', linkText: '查看通道' },
      { label: '股票池 / 数据体检', detail: '备选股池可核对股票池同步；数据缺口到实盘工作台点「数据体检」查看。非盘中会自动重读本地落盘，但平台无法程序化触发通达信下载，需在通达信客户端人工补下载。', to: '/live', linkText: '进入实盘工作台' },
    ],
  },
  {
    key: 'backup',
    title: '备份失败 / 磁盘不足',
    symptom: '今日自动备份未完成，或备份记录显示 failed / skipped。',
    checks: [
      { label: '备份状态', detail: '系统配置「数据备份」查看上次自动/手动备份与失败原因。', to: '/system', linkText: '查看备份面板' },
      { label: '磁盘空间', detail: '健康总览 disk 区给出剩余空间与保护阈值；低于阈值备份会跳过而不是写坏库。', to: '/health', linkText: '查看磁盘' },
      { label: '手动补备', detail: '清理磁盘后在备份面板点「立即备份」，并校验备份完整性。', to: '/system', linkText: '立即备份' },
    ],
  },
  {
    key: 'upgrade',
    title: '升级前检查 / 回滚',
    symptom: '升级后启动异常，或数据库版本落后于代码 head。',
    checks: [
      { label: '版本差异', detail: '系统配置「升级与回滚」查看当前 revision、代码 head 与待执行迁移。', to: '/system', linkText: '查看升级检查' },
      { label: '升级前备份', detail: '开启 upgrade.enabled 后可用「执行升级前备份」；未开启时用数据备份面板手动备份。', to: '/system', linkText: '执行备份' },
      { label: '回滚提示', detail: '按页面「回滚与恢复」执行 alembic downgrade 或替换备份文件；本平台不会自动迁移。', to: '/system', linkText: '查看回滚步骤' },
    ],
  },
  {
    key: 'events',
    title: '告警堆积 / 事件未确认',
    symptom: '未处理决策事件持续增长，盘后无人清零。',
    checks: [
      { label: '未处理事件', detail: '健康总览 alerts.open_decisions 按会话统计，事件检索默认只看「未处理」。', to: '/events', linkText: '检索未处理事件' },
      { label: '逐条确认', detail: '在实盘工作台的决策事件面板确认或批量确认；日终可一键清零。', to: '/live', linkText: '处理事件' },
    ],
  },
]

async function load() {
  loading.value = true
  errorMsg.value = ''
  try {
    overview.value = await getHealthOverview()
  } catch (e) {
    errorMsg.value = '读取健康总览失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    loading.value = false
  }
}

onMounted(() => {
  load()
  timer = setInterval(load, 30000)
})

onUnmounted(() => {
  if (timer) clearInterval(timer)
})
</script>

<template>
  <PageHeader title="故障处置向导" />

  <div class="card" style="padding:14px;margin-bottom:12px">
    <div class="ts-head">
      <div>
        <b>当前总体状态：</b>
        <span
          class="badge"
          :class="overview?.level === 'error' ? 'badge-red'
            : overview?.level === 'warn' ? 'badge-orange' : 'badge-green'"
        >
          {{ loading ? '读取中…' : overview?.level === 'error' ? '存在故障'
            : overview?.level === 'warn' ? '有告警' : '正常' }}
        </span>
        <span v-if="overview?.generated_at" class="ts-time">
          更新于 {{ overview.generated_at.replace('T', ' ').slice(0, 19) }}
        </span>
      </div>
      <button class="btn btn-sm" :disabled="loading" @click="load">
        {{ loading ? '刷新中…' : '刷新状态' }}
      </button>
    </div>
    <div v-if="errorMsg" style="color:var(--red);font-size:13px;margin-top:8px">{{ errorMsg }}</div>
    <ul v-else-if="overview?.problems.length" class="ts-problems">
      <li v-for="(p, i) in overview.problems" :key="i" :class="p.level">
        <span class="badge" :class="p.level === 'error' ? 'badge-red' : 'badge-orange'">
          {{ p.level === 'error' ? '故障' : '告警' }}
        </span>
        {{ p.text }}
      </li>
    </ul>
    <p v-else-if="overview" class="ts-ok">当前没有检测到异常，按下方手册进行预防性巡检。</p>
  </div>

  <div class="ts-grid">
    <details v-for="item in PLAYBOOK" :key="item.key" class="card ts-item">
      <summary>
        <span class="ts-title">{{ item.title }}</span>
        <span class="ts-symptom">{{ item.symptom }}</span>
      </summary>
      <ol class="ts-checks">
        <li v-for="(c, i) in item.checks" :key="i">
          <div class="ts-check-label">{{ c.label }}</div>
          <div class="ts-check-detail">{{ c.detail }}</div>
          <RouterLink v-if="c.to" :to="c.to" class="ts-link">{{ c.linkText || '前往处理' }} →</RouterLink>
        </li>
      </ol>
    </details>
  </div>
</template>

<style scoped>
.ts-head { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.ts-time { margin-left: 10px; font-size: 12px; color: var(--ink-soft); }
.ts-problems { margin: 10px 0 0; padding: 0; list-style: none; display: grid; gap: 6px; }
.ts-problems li { font-size: 13px; line-height: 1.6; }
.ts-problems li.error { color: var(--red); }
.ts-problems li.warn { color: var(--ink); }
.ts-ok { margin: 8px 0 0; font-size: 13px; color: var(--ink-soft); }
.ts-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 12px; }
.ts-item { padding: 12px 14px; }
.ts-item summary { cursor: pointer; list-style: none; }
.ts-item summary::-webkit-details-marker { display: none; }
.ts-title { display: block; font-size: 14px; font-weight: 600; }
.ts-title::before { content: '▸ '; color: var(--accent); }
.ts-item[open] .ts-title::before { content: '▾ '; }
.ts-symptom { display: block; margin-top: 4px; font-size: 12px; color: var(--ink-soft); line-height: 1.6; }
.ts-checks { margin: 10px 0 0; padding-left: 18px; display: grid; gap: 10px; }
.ts-checks li { font-size: 13px; line-height: 1.6; }
.ts-check-label { font-weight: 600; }
.ts-check-detail { color: var(--ink-soft); }
.ts-link { font-size: 12px; color: var(--accent); text-decoration: none; }
.ts-link:hover { text-decoration: underline; }
</style>
