<script setup lang="ts">
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { RouterLink } from 'vue-router'
import PageHeader from '../components/PageHeader.vue'
import {
  getHealthOverview, getTdxGateStatus, setTdxGateOverride,
  clearTdxGateOverride, liquidateAllSessions,
  type HealthOverview, type TdxGateStatus,
} from '../api'

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

// ---- 高危与通道运维：TQ 闸覆盖 / 紧急全平（二次确认 + 后端审计）----
const opsFeedback = ref<{ ok: boolean; text: string } | null>(null)

const gate = ref<TdxGateStatus | null>(null)
const gateLoading = ref(false)
const gateError = ref('')
const gateClearing = ref(false)
const gateModal = ref(false)
const gateMinutes = ref<number | string>(30)
const gateReason = ref('')
const gateBusy = ref(false)
const gateModalError = ref('')

async function loadGate() {
  gateLoading.value = true
  gateError.value = ''
  try {
    gate.value = await getTdxGateStatus()
  } catch (e) {
    gateError.value = '读取 TQ 闸状态失败：' + (e instanceof Error ? e.message : String(e))
  } finally {
    gateLoading.value = false
  }
}

function openGateModal() {
  gateMinutes.value = 30
  gateReason.value = ''
  gateModalError.value = ''
  gateModal.value = true
}

async function submitGateOverride() {
  if (gateBusy.value) return
  const minutes = Number(gateMinutes.value)
  if (!Number.isInteger(minutes) || minutes < 1 || minutes > 240) {
    gateModalError.value = '覆盖时长须为 1–240 分钟的整数'; return
  }
  if (!gateReason.value.trim()) {
    gateModalError.value = '必须填写覆盖原因（写审计事件）'; return
  }
  gateBusy.value = true
  gateModalError.value = ''
  try {
    await setTdxGateOverride({
      confirm: true, reason: gateReason.value.trim(), minutes,
    })
    gateModal.value = false
    opsFeedback.value = {
      ok: true,
      text: `TQ 闸已临时覆盖 ${minutes} 分钟：P2 后台任务可抢占通道，到期自动恢复`,
    }
    await loadGate()
  } catch (e: any) {
    gateModalError.value = e?.response?.data?.message || e?.message || '覆盖失败'
  } finally {
    gateBusy.value = false
  }
}

async function clearGate() {
  if (gateClearing.value) return
  gateClearing.value = true
  try {
    await clearTdxGateOverride('故障向导手动解除')
    opsFeedback.value = { ok: true, text: 'TQ 闸覆盖已解除' }
    await loadGate()
  } catch (e: any) {
    opsFeedback.value = {
      ok: false,
      text: '解除覆盖失败：' + (e?.response?.data?.message || e?.message || String(e)),
    }
  } finally {
    gateClearing.value = false
  }
}

// 紧急全平（全部运行中会话；后端要求 confirm=LIQUIDATE）
const liquidateModal = ref(false)
const liquidateConfirmText = ref('')
const liquidateReason = ref('')
const liquidating = ref(false)
const liquidateError = ref('')

function openLiquidateModal() {
  liquidateConfirmText.value = ''
  liquidateReason.value = ''
  liquidateError.value = ''
  liquidateModal.value = true
}

async function submitLiquidate() {
  if (liquidating.value) return
  if (liquidateConfirmText.value.trim() !== 'LIQUIDATE') {
    liquidateError.value = '请输入 LIQUIDATE 以确认全平'; return
  }
  liquidating.value = true
  liquidateError.value = ''
  try {
    const res = await liquidateAllSessions(
      'LIQUIDATE', liquidateReason.value.trim() || undefined)
    liquidateModal.value = false
    const lines = (res.results || []).map((r) => {
      const sid = r.session_id
      const err = (r as Record<string, unknown>).error
      if (err) return `#${sid} 失败：${String(err)}`
      const n = Number((r as Record<string, unknown>).submitted ?? 0)
      const skipped = Number((r as Record<string, unknown>).skipped ?? 0)
      return `#${sid} 提交 ${n} 笔卖单${skipped ? `（跳过 ${skipped} 笔，原因见日志）` : ''}`
    })
    opsFeedback.value = {
      ok: true,
      text: lines.length ? `紧急全平：${lines.join('；')}` : '紧急全平：无运行中会话',
    }
  } catch (e: any) {
    liquidateError.value = e?.response?.data?.message || e?.message || '紧急全平失败'
  } finally {
    liquidating.value = false
  }
}

// 故障向导/健康总览/关键事件同源：都用 getHealthOverview() 的这一份事实。
// 只有带 fix（站内可执行、执行后该条会消失）的才进「故障」；其余是观察项。
const fixableProblems = computed(
  () => (overview.value?.problems || []).filter((p) => !!p.fix))
const observeProblems = computed(
  () => (overview.value?.problems || []).filter((p) => !p.fix))

const PLAYBOOK: PlaybookItem[] = [
  {
    key: 'channel',
    title: '交易通道断线 / 主备切换',
    symptom: '实盘工作台出现「通道离线」告警，下单失败或回执不再返回。',
    checks: [
      { label: '主通道状态', detail: '健康总览「通道」区确认 primary.online 与 configured；主通道不可用会亮 error。', to: '/health', linkText: '查看健康总览' },
      { label: '客户端进程', detail: '通达信/同花顺客户端是否仍在运行；ths 通道还需确认窗口标题与交接目录未被改动。', to: '/system', linkText: '核对通道配置' },
      { label: '通道守卫检查', detail: '健康总览「通道检查明细」逐项列出 5 项检查（主通道配置 / 客户端在线 / 运行态引擎注册 / 备用通道 / 无待生效配置）。停止态会话整体不判在线与备用通道，显示「不适用」；运行中未装备用通道属配置选择，也显示「不适用」而不是未通过。', to: '/health', linkText: '查看逐项明细' },
      { label: '近期通道事件', detail: '关键事件检索里 gate=channel_offline / channel_recovered，确认断线持续时间与恢复时间。', to: '/events', linkText: '检索通道事件' },
    ],
  },
  {
    key: 'market_stall',
    title: '行情停滞 / 数据陈旧',
    symptom: '引擎仍在运行，但最近 bar 很久没有推进，策略不产生新信号。',
    checks: [
      { label: '最近 bar 时间', detail: '健康总览「数据新鲜度」按周期列出 stale（落后）/empty（无 bar）数量；「未校验（抽样）」表示本次体检没覆盖到，不等于数据缺失。业务心跳开启时 /health 也带最近 bar。', to: '/health', linkText: '查看数据体检' },
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
      { label: '按周期排查', detail: '健康总览 data.periods 逐周期给出 stale/empty/total，定位是分钟线还是日线缺口；只有 stale/empty 才算缺失，查询异常/超时/未抽查单列且不计缺失。', to: '/health', linkText: '查看数据体检' },
      { label: '行情通道', detail: '数据体检 unavailable 时先修通道（行情源不可用无法补齐）。', to: '/health', linkText: '查看通道' },
      { label: '股票池 / 数据体检', detail: '总览的体检是抽样校验（受抽样上限与时间预算限制），显示「未校验（抽样）X/Y」只代表没验完，不是缺数据；盘后可在实盘工作台重跑体检。平台无法程序化触发通达信下载，确需补下载的标的只能在通达信客户端人工补下载（属观察项，不在故障向导里放按钮）。', to: '/live', linkText: '进入实盘工作台' },
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
      { label: '未处理事件', detail: '健康总览 alerts.open_decisions 与事件检索「未处理」同一口径：ack_status=open 且未归档，两处数字应永远一致。', to: '/events', linkText: '检索未处理事件' },
      { label: '批量确认', detail: '在实盘工作台决策面板按门类/层级/时间/关键字筛选后先预览「将确认 N 条」再二次确认；不支持全选，原因必填并逐条留痕。', to: '/live', linkText: '批量确认事件' },
      { label: '高危门类', detail: 'cash_diff / equity_deviation / engine_owner_mismatch / sell_scope / split_children_lost / out_of_pool_position 不参与批量与自动归档，必须逐条核对后确认。', to: '/live', linkText: '逐条处理' },
      { label: '过期归档', detail: '普通事件超过 7 天未处理自动归档（config: live.decision_archive_days 可调），移出未处理计数但仍可按「已归档」查询回看，不是删除。', to: '/events', linkText: '查看已归档' },
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
  void loadGate()
  timer = setInterval(() => {
    load()
    void loadGate()
  }, 30000)
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
    <template v-else-if="overview">
      <p class="ts-src">
        本页与健康总览、关键事件同源（同一份健康总览快照），不各自计算；
        「故障」仅列本平台内可执行修复的项，执行后该条会自动消失。
      </p>
      <div v-if="fixableProblems.length" class="ts-section">
        <div class="ts-section-title">可修复故障（{{ fixableProblems.length }}）</div>
        <ul class="ts-problems">
          <li v-for="(p, i) in fixableProblems" :key="'fix' + i" :class="p.level">
            <span class="badge" :class="p.level === 'error' ? 'badge-red' : 'badge-orange'">
              {{ p.level === 'error' ? '故障' : '告警' }}
            </span>
            <span>
              {{ p.text }}
              <i v-if="p.impact" class="ts-impact">影响：{{ p.impact }}</i>
              <RouterLink v-if="p.fix" :to="p.fix.to" class="ts-link">
                {{ p.fix.label }} →
              </RouterLink>
            </span>
          </li>
        </ul>
      </div>
      <div v-if="observeProblems.length" class="ts-section">
        <div class="ts-section-title">
          观察项（{{ observeProblems.length }}，需外部处理或无需操作，不列入故障）
        </div>
        <ul class="ts-problems">
          <li v-for="(p, i) in observeProblems" :key="'obs' + i" class="warn">
            <span class="badge badge-gray">观察</span>
            <span>
              {{ p.text }}
              <i v-if="p.impact" class="ts-impact">影响：{{ p.impact }}</i>
              <i v-if="p.observe" class="ts-impact">外部处理：{{ p.observe }}</i>
            </span>
          </li>
        </ul>
      </div>
      <p v-if="!overview.problems.length" class="ts-ok">
        当前没有检测到异常，按下方手册进行预防性巡检。
      </p>
    </template>
  </div>

  <!-- 高危与通道运维：TQ 闸覆盖（临时放开 P2）/ 紧急全平 -->
  <div class="card" style="padding:14px;margin-bottom:12px" data-test="ops-card">
    <div class="ts-head">
      <b>高危与通道运维</b>
      <button class="btn btn-sm" :disabled="gateLoading" @click="loadGate">
        {{ gateLoading ? '刷新中…' : '刷新 TQ 闸状态' }}
      </button>
    </div>
    <p class="ts-src">
      TQ 通道单实例：实盘（P0）优先，回测类后台任务（P2）在交易时段被拦。
      临时覆盖会让 P2 抢占通道，仅在确需盘后补算时使用；所有变更写审计事件。
    </p>

    <div class="ts-section">
      <div class="ts-section-title">TQ 通道闸覆盖</div>
      <template v-if="gate">
        <div class="gate-line">
          <span class="badge" :class="gate.enabled ? 'badge-green' : 'badge-gray'">
            {{ gate.enabled ? '闸门启用' : '闸门关闭' }}
          </span>
          <span>时段：{{ gate.policy.window }}</span>
          <span>P1：{{ gate.policy.p1_allowed ? '可执行' : '被拦' }}</span>
          <span>P2：{{ gate.policy.p2_allowed ? '可执行' : '被拦' }}</span>
          <span v-if="gate.channel_held">通道占用中</span>
        </div>
        <p v-if="gate.override && gate.override.active" class="ts-impact" data-test="gate-override-active">
          覆盖生效至 {{ gate.override.expires_at }}（{{ gate.override.minutes }} 分钟 ·
          操作者：{{ gate.override.operator || '—' }} · 原因：{{ gate.override.reason }}）
        </p>
        <p v-else class="ts-impact">当前无覆盖。</p>
        <div class="gate-actions">
          <button class="btn btn-sm" data-test="gate-override-open" @click="openGateModal">临时覆盖…</button>
          <button
            v-if="gate.override && gate.override.active"
            class="btn btn-sm" data-test="gate-clear" :disabled="gateClearing"
            @click="clearGate"
          >{{ gateClearing ? '解除中…' : '立即解除覆盖' }}</button>
        </div>
      </template>
      <p v-else-if="gateError" class="ts-impact" style="color:var(--red)" data-test="gate-error">{{ gateError }}</p>
      <p v-else class="ts-impact">读取中…</p>
    </div>

    <div class="ts-section">
      <div class="ts-section-title" style="color:var(--red)">紧急全平</div>
      <p class="ts-src">
        对<b>全部运行中会话</b>的持仓按整仓提交卖单（不受 sell_scope / 代卖授权限制），
        仍走既有提交闸门（T+1 / 时点 / 审批）；需输入 LIQUIDATE 确认，动作写审计事件。
      </p>
      <button
        class="btn btn-sm" style="color:var(--red);border-color:var(--red)"
        data-test="liquidate-open" @click="openLiquidateModal"
      >紧急全平…</button>
    </div>

    <p v-if="opsFeedback" style="margin:10px 0 0;font-size:12px"
       :style="{ color: opsFeedback.ok ? 'var(--ink-soft)' : 'var(--red)' }"
       data-test="ops-feedback">{{ opsFeedback.text }}</p>
  </div>

  <!-- TQ 闸覆盖：二次确认（展示时长与后果） -->
  <div v-if="gateModal" class="modal-overlay" data-test="gate-modal" @click.self="gateModal = false">
    <div class="modal-content">
      <h3>临时覆盖 TQ 通道闸</h3>
      <div class="modal-body">
        <p class="ts-src">
          覆盖期间回测类后台任务（P2）可抢占通达信通道，可能与实盘（P0）争用。
          到期自动恢复；变更写审计事件。
        </p>
        <div class="form-group">
          <label>覆盖时长（分钟，1–240）</label>
          <input v-model="gateMinutes" type="number" min="1" max="240" data-test="gate-minutes" />
        </div>
        <div class="form-group">
          <label>原因（必填，写审计）</label>
          <input v-model="gateReason" type="text" placeholder="如：盘后补算需放开 P2" data-test="gate-reason" />
        </div>
        <p v-if="gateModalError" class="form-error" data-test="gate-modal-error">{{ gateModalError }}</p>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" :disabled="gateBusy" data-test="gate-confirm" @click="submitGateOverride">
          {{ gateBusy ? '提交中…' : '确认覆盖' }}
        </button>
        <button class="btn" :disabled="gateBusy" @click="gateModal = false">取消</button>
      </div>
    </div>
  </div>

  <!-- 紧急全平：必须键入 LIQUIDATE -->
  <div v-if="liquidateModal" class="modal-overlay" data-test="liquidate-modal" @click.self="liquidateModal = false">
    <div class="modal-content">
      <h3 style="color:var(--red)">紧急全平</h3>
      <div class="modal-body">
        <p class="ts-src" style="color:var(--red)">
          将对<b>全部运行中会话</b>的持仓按整仓提交卖单（不受 sell_scope / 代卖授权限制）。
          该动作立即生效、不可撤销，仅用于紧急处置；后端写审计事件。
        </p>
        <div class="form-group">
          <label>确认口令</label>
          <input v-model="liquidateConfirmText" type="text" placeholder="键入 LIQUIDATE 以确认"
                 data-test="liquidate-confirm-text" />
        </div>
        <div class="form-group">
          <label>原因（可选）</label>
          <input v-model="liquidateReason" type="text" placeholder="如：通道异常 / 风控紧急降险"
                 data-test="liquidate-reason" />
        </div>
        <p v-if="liquidateError" class="form-error" data-test="liquidate-error">{{ liquidateError }}</p>
      </div>
      <div class="modal-actions">
        <button class="btn btn-primary" style="background:var(--red);border-color:var(--red)"
                :disabled="liquidating" data-test="liquidate-confirm" @click="submitLiquidate">
          {{ liquidating ? '提交中…' : '确认全平' }}
        </button>
        <button class="btn" :disabled="liquidating" @click="liquidateModal = false">取消</button>
      </div>
    </div>
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
.ts-src { margin: 8px 0 0; font-size: 12px; color: var(--ink-soft); line-height: 1.6; }
.ts-section { margin-top: 12px; }
.ts-section-title { font-size: 13px; font-weight: 600; margin-bottom: 6px; }
.ts-impact { display: block; margin-top: 2px; font-size: 12px; color: var(--ink-soft); font-style: normal; }
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
.ts-link { font-size: 12px; color: var(--link); text-decoration: none; }
.ts-link:hover { color: var(--link-hover); text-decoration: underline; }
.gate-line { display: flex; flex-wrap: wrap; gap: 10px; align-items: center; font-size: 12px; color: var(--ink-soft); }
.gate-actions { display: flex; gap: 8px; margin-top: 8px; }
</style>
