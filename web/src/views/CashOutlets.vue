<script setup lang="ts">
import { ref, onMounted } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import {
  listCashOutlets, evaluateCashOutlet,
  listCashOutletOps, createCashOutletOp, confirmCashOutletOp,
  rejectCashOutletOp, returnCashOutletOp, reconcileCashOutletOps,
} from '../api'
import type {
  CashOutletItem, CashOutletEvaluation,
  CashOutletOp, CashOutletOpsSummary,
} from '../api'

const items = ref<CashOutletItem[]>([])
const loading = ref(true)
const disabled = ref(false)
const errorMessage = ref('')

const amount = ref('')
const needDate = ref('')
const evaluating = ref<number | null>(null)
const evaluation = ref<CashOutletEvaluation | null>(null)
const evaluationError = ref('')

const CATEGORY_LABEL: Record<string, string> = {
  repo: '逆回购', etf_money: '货基 ETF', other: '其他',
}
const LIQUIDITY_LABEL: Record<string, string> = {
  t0: 'T+0', t1: 'T+1', term: '到期',
}
const OP_STATUS_LABEL: Record<string, string> = {
  PENDING_CONFIRM: '待确认', EXECUTED: '已投放',
  RETURNED: '已回流', REJECTED: '已驳回', CANCELLED: '已取消',
}

// G4 执行台账（只记账，不划转；出口收益不进入策略归因）
const ops = ref<CashOutletOp[]>([])
const opsSummary = ref<CashOutletOpsSummary | null>(null)
const opsError = ref('')
const opsBusy = ref(false)
const opOutletId = ref<number | null>(null)
const opAmount = ref('')
const opMode = ref('P1')
const opSessionId = ref('')

function errMsg(e: unknown): string {
  const anyErr = e as any
  return anyErr?.response?.data?.message || anyErr?.message || '请求失败'
}

async function load() {
  loading.value = true
  errorMessage.value = ''
  try {
    const data = await listCashOutlets()
    disabled.value = data.enabled === false
    items.value = data.items ?? []
    if (opOutletId.value === null && items.value.length) {
      opOutletId.value = items.value[0].id
    }
    if (!disabled.value) await loadOps()
  } catch (e) {
    errorMessage.value = errMsg(e)
  } finally {
    loading.value = false
  }
}

async function loadOps() {
  opsError.value = ''
  try {
    const data = await listCashOutletOps({ limit: 100 })
    if ((data as any).enabled === false) return
    const list = data as { items: CashOutletOp[]; summary: CashOutletOpsSummary }
    ops.value = list.items ?? []
    opsSummary.value = list.summary ?? null
  } catch (e) {
    opsError.value = errMsg(e)
  }
}

async function submitOp() {
  if (!opOutletId.value) return
  opsBusy.value = true
  opsError.value = ''
  try {
    const created = await createCashOutletOp({
      outlet_id: opOutletId.value,
      amount: opAmount.value,
      mode: opMode.value,
      live_session_id: opSessionId.value ? Number(opSessionId.value) : undefined,
    })
    if (created.auto_blocked_reasons?.length) {
      opsError.value = `P3 条件不满足，已转人工确认：${created.auto_blocked_reasons.join('、')}`
    }
    opAmount.value = ''
    await loadOps()
  } catch (e) {
    opsError.value = errMsg(e)
  } finally {
    opsBusy.value = false
  }
}

async function confirmOp(row: CashOutletOp) {
  const who = prompt('确认操作人（必填）：')
  if (!who || !who.trim()) return
  try {
    await confirmCashOutletOp(row.id, who.trim())
    await loadOps()
  } catch (e) {
    opsError.value = errMsg(e)
  }
}

async function rejectOp(row: CashOutletOp) {
  const reason = prompt('驳回理由（必填）：')
  if (!reason || !reason.trim()) return
  try {
    await rejectCashOutletOp(row.id, reason.trim())
    await loadOps()
  } catch (e) {
    opsError.value = errMsg(e)
  }
}

async function returnOp(row: CashOutletOp) {
  const principal = prompt(`登记回流本金（计划 ${row.amount}）：`, row.amount)
  if (principal === null || !principal.trim()) return
  const gain = prompt('出口收益（元，可留空）：', '0') || '0'
  const shortfall = Number(row.amount) - Number(principal)
  let reason: string | undefined
  if (shortfall > 0) {
    const text = prompt(`存在短款 ${shortfall.toFixed(2)}，请填写原因（必填）：`)
    if (!text || !text.trim()) return
    reason = text.trim()
  }
  try {
    await returnCashOutletOp(row.id, {
      principal_returned: principal.trim(),
      yield_amount: gain.trim() || '0',
      shortfall_reason: reason,
    })
    await loadOps()
  } catch (e) {
    opsError.value = errMsg(e)
  }
}

async function checkOps() {
  try {
    const result = await reconcileCashOutletOps()
    opsError.value = result.ok ? '台账对账通过：无逾期、无未说明短款'
      : `对账异常：${JSON.stringify(result.overdue || [])} ${JSON.stringify(result.anomalies || [])}`
  } catch (e) {
    opsError.value = errMsg(e)
  }
}

async function evaluate(row: CashOutletItem) {
  evaluating.value = row.id
  evaluation.value = null
  evaluationError.value = ''
  try {
    evaluation.value = await evaluateCashOutlet(row.id, {
      required_amount: amount.value || undefined,
      need_date: needDate.value || undefined,
    })
  } catch (e) {
    evaluationError.value = errMsg(e)
  } finally {
    evaluating.value = null
  }
}

function limitText(row: CashOutletItem): string {
  const parts: string[] = []
  if (row.min_amount) parts.push(`起 ${row.min_amount}`)
  if (row.max_single_amount) parts.push(`单笔 ≤${row.max_single_amount}`)
  if (row.max_daily_amount) parts.push(`单日 ≤${row.max_daily_amount}`)
  return parts.join(' / ') || '—'
}

onMounted(load)
</script>

<template>
  <div>
    <PageHeader title="现金出口" :crumbs="['交易', '资金池', '现金出口']">
      <button class="btn" @click="load">刷新</button>
    </PageHeader>

    <div v-if="disabled" class="card panel">
      现金出口开关未启用（<code>capital_pool.cash_outlet_enabled=false</code>）。
      本页仅展示与评估配置，不发起任何委托或划转。
    </div>
    <div v-else-if="errorMessage" class="card panel error">{{ errorMessage }}</div>

    <template v-else>
      <div class="card panel">
        <div class="eval-bar">
          <label>评估金额（元）<input v-model="amount" placeholder="留空只校验配置" /></label>
          <label>用钱日<input v-model="needDate" type="date" /></label>
          <span class="muted">评估只读，不产生交易；未验证出口一律不可借。</span>
        </div>
      </div>

      <div class="card">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>代码</th><th>名称</th><th>类别</th><th>通道</th>
                <th>流动性</th><th>风险</th><th>限额</th><th>验证</th>
                <th>状态</th><th>评估</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="loading"><td colspan="10" class="muted">加载中…</td></tr>
              <tr v-else-if="!items.length"><td colspan="10" class="muted">暂无已登记的现金出口</td></tr>
              <tr v-for="row in items" :key="row.id">
                <td class="num">{{ row.code }}</td>
                <td>{{ row.name }}</td>
                <td>{{ CATEGORY_LABEL[row.category] || row.category }}</td>
                <td>{{ row.channel }}{{ row.channel_order_enabled ? '' : '（人工）' }}</td>
                <td>{{ LIQUIDITY_LABEL[row.liquidity] || row.liquidity }}<span v-if="row.term_days" class="muted"> / {{ row.term_days }}天</span></td>
                <td>{{ row.risk_level || '—' }}</td>
                <td class="num">{{ limitText(row) }}</td>
                <td>
                  <span v-if="row.verified" class="badge badge-green">已验</span>
                  <span v-else class="badge badge-gray">未验</span>
                  <span v-if="row.verify_expires_at" class="muted expires">至 {{ row.verify_expires_at }}</span>
                </td>
                <td>{{ row.status === 'active' ? '启用' : '归档' }}</td>
                <td>
                  <button class="btn" :disabled="evaluating === row.id" @click="evaluate(row)">
                    {{ evaluating === row.id ? '评估中…' : '评估' }}
                  </button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>

      <div v-if="evaluationError" class="card panel error">{{ evaluationError }}</div>

      <div v-if="evaluation" class="card panel">
        <div class="eval-head">
          <strong>{{ evaluation.code }} {{ evaluation.name }}</strong>
          <span :class="['badge', evaluation.status === 'READY' ? 'badge-green' : 'badge-red']">
            {{ evaluation.status }}
          </span>
          <span v-if="evaluation.codes.length" class="muted">
            拒绝：{{ evaluation.codes.join('、') }}
          </span>
          <span v-else class="muted">C1–C8 全部通过</span>
        </div>
        <ul class="checks">
          <li v-for="c in evaluation.checks" :key="c.code + c.name" :class="'state-' + c.state">
            <span class="badge" :class="{
              'badge-green': c.state === 'pass',
              'badge-orange': c.state === 'warn',
              'badge-red': c.state === 'fail',
            }">{{ c.state }}</span>
            <span class="check-name">{{ c.name }}</span>
            <span class="muted">{{ c.detail }}</span>
          </li>
        </ul>
        <div v-if="evaluation.warnings.length" class="muted">
          提示：{{ evaluation.warnings.join('、') }}
        </div>
        <div class="muted">
          收益率可回测：{{ evaluation.yield_backtestable ? '是' : '否' }}；
          判定时间 {{ evaluation.decided_at }}
        </div>
      </div>

      <!-- 0021 G4：执行台账（只记账，不划转；出口收益单列） -->
      <div class="card panel">
        <div class="eval-bar">
          <label>出口
            <select v-model="opOutletId">
              <option v-for="row in items" :key="row.id" :value="row.id">
                {{ row.code }} {{ row.name }}
              </option>
            </select>
          </label>
          <label>金额（元）<input v-model="opAmount" placeholder="如 10000" /></label>
          <label>模式
            <select v-model="opMode">
              <option value="P1">P1 人工</option>
              <option value="P2">P2 半自动</option>
              <option value="P3">P3 自动</option>
            </select>
          </label>
          <label>会话 ID（P3 必填）<input v-model="opSessionId" placeholder="可留空" /></label>
          <button class="btn btn-primary" :disabled="opsBusy || !opAmount" @click="submitOp">
            登记计划
          </button>
          <button class="btn" @click="checkOps">台账对账</button>
        </div>
        <div class="muted">
          P1/P2 登记后需人工确认；P3 仅对已验证、T+0/≤7 天出口自动放行。
          台账不划转、不下单，出口收益单独统计，不进入策略归因。
        </div>
      </div>

      <div v-if="opsError" class="card panel error">{{ opsError }}</div>

      <div v-if="opsSummary" class="card panel">
        <div class="eval-head">
          <span>在途本金 <b class="num">{{ opsSummary.outstanding_principal }}</b></span>
          <span>已回流 <b class="num">{{ opsSummary.returned_principal }}</b></span>
          <span>出口收益（单列）<b class="num">{{ opsSummary.yield_contribution }}</b></span>
          <span :class="{ error: opsSummary.shortfall_total !== '0' && opsSummary.shortfall_total !== '0.00' }">
            短款合计 <b class="num">{{ opsSummary.shortfall_total }}</b>
          </span>
          <span class="muted">待确认 {{ opsSummary.pending_count }} / 已投放 {{ opsSummary.executed_count }}</span>
        </div>
      </div>

      <div class="card">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>ID</th><th>出口</th><th>模式</th><th>金额</th><th>状态</th>
                <th>预计回流</th><th>已回本金</th><th>收益</th><th>短款</th><th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="!ops.length"><td colspan="10" class="muted">暂无出口台账</td></tr>
              <tr v-for="op in ops" :key="op.id">
                <td class="num">{{ op.id }}</td>
                <td class="num">{{ op.outlet_id }}</td>
                <td>{{ op.mode }}</td>
                <td class="num">{{ op.amount }}</td>
                <td>{{ OP_STATUS_LABEL[op.status] || op.status }}</td>
                <td>{{ op.expected_return_date || '—' }}</td>
                <td class="num">{{ op.principal_returned || '—' }}</td>
                <td class="num">{{ op.yield_amount }}</td>
                <td class="num">
                  {{ op.shortfall_amount }}
                  <span v-if="op.shortfall_reason" class="muted">（{{ op.shortfall_reason }}）</span>
                </td>
                <td>
                  <template v-if="op.status === 'PENDING_CONFIRM'">
                    <button class="btn" @click="confirmOp(op)">确认</button>
                    <button class="btn" @click="rejectOp(op)">驳回</button>
                  </template>
                  <button v-else-if="op.status === 'EXECUTED'" class="btn" @click="returnOp(op)">
                    登记回流
                  </button>
                  <span v-else class="muted">—</span>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.panel { padding: 14px 16px; margin-bottom: 14px; }
.error { color: var(--red); }
.eval-bar { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 14px; }
.eval-bar label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--ink-soft); }
.eval-bar input { padding: 5px 8px; border: 1px solid var(--line); border-radius: 6px; background: var(--card-bg); color: var(--ink); }
.eval-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; margin-bottom: 10px; }
.checks { list-style: none; margin: 0 0 10px; padding: 0; display: flex; flex-direction: column; gap: 6px; }
.checks li { display: flex; align-items: baseline; gap: 10px; font-size: 13px; }
.check-name { min-width: 96px; color: var(--ink); }
.state-fail .check-name { color: var(--red); }
.state-warn .check-name { color: var(--warn); }
.expires { margin-left: 6px; font-size: 11px; }
.muted { color: var(--ink-faint); font-size: 12px; }
</style>
