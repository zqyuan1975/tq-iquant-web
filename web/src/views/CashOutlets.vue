<script setup lang="ts">
import { ref, onMounted } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import {
  listCashOutlets, evaluateCashOutlet,
} from '../api'
import type { CashOutletItem, CashOutletEvaluation } from '../api'

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
  } catch (e) {
    errorMessage.value = errMsg(e)
  } finally {
    loading.value = false
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
