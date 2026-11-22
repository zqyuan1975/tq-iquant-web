<script setup lang="ts">
import { ref, onMounted } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import {
  listCashOutlets, evaluateCashOutlet, registerCashOutlet,
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
const MODE_LABEL: Record<string, string> = {
  OFF: '未启用', P1: 'P1 · 人工确认', P2: 'P2 · 半自动', P3: 'P3 · 自动放行',
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

// 登记标的：独立入口，后端校验代码格式与通达信行情，落库为未验证
const showRegister = ref(false)
const regBusy = ref(false)
const regError = ref('')
const regNotice = ref('')
const reg = ref({
  code: '', name: '', category: '', channel: 'manual',
  channel_order_enabled: false, liquidity: 't0', term_days: 1,
  risk_level: 'R1', inflow_certainty: 'certain', notes: '',
})

function openRegister() {
  regError.value = ''
  showRegister.value = true
}

function closeRegister() {
  if (regBusy.value) return
  showRegister.value = false
}

function regErrMsg(e: unknown): string {
  const anyErr = e as any
  if (anyErr?.response?.status === 404) {
    return '登记接口暂不可用：新增路由需重启 Core 后才会加载，请重启后重试'
      + '（前端已上线，无需重新构建）。'
  }
  return anyErr?.response?.data?.message || anyErr?.message || '登记失败，请稍后重试'
}

async function submitRegister() {
  const code = reg.value.code.trim()
  if (!code) {
    regError.value = '请填写标的代码'
    return
  }
  regBusy.value = true
  regError.value = ''
  regNotice.value = ''
  try {
    const created = await registerCashOutlet({
      code,
      name: reg.value.name.trim() || undefined,
      category: reg.value.category || undefined,
      channel: reg.value.channel,
      channel_order_enabled: reg.value.channel !== 'manual' && reg.value.channel_order_enabled,
      liquidity: reg.value.liquidity,
      term_days: reg.value.liquidity === 'term' ? Number(reg.value.term_days) : undefined,
      risk_level: reg.value.risk_level || undefined,
      inflow_certainty: reg.value.inflow_certainty || undefined,
      notes: reg.value.notes.trim() || undefined,
    })
    showRegister.value = false
    await load()
    opOutletId.value = created.item.id
    const warn = created.warnings?.length ? `；提示：${created.warnings.join('；')}` : ''
    regNotice.value = `已登记 ${created.item.code} ${created.item.name}（未验证）；`
      + '已自动选中，可直接登记计划。' + warn
  } catch (e) {
    regError.value = regErrMsg(e)
  } finally {
    regBusy.value = false
  }
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
      opsError.value = `P3 · 自动放行条件不满足，已转人工确认：${created.auto_blocked_reasons.join('、')}`
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
  const gain = prompt('现金管理收益（元，可留空）：', '0') || '0'
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
    <PageHeader title="现金管理" :crumbs="['交易', '资金池', '现金管理']">
      <button class="btn btn-primary" @click="openRegister">登记标的</button>
      <button class="btn" @click="load">刷新</button>
    </PageHeader>

    <div v-if="disabled" class="card panel">
      现金管理开关未启用（<code>capital_pool.cash_outlet_enabled=false</code>）。
      本页仅展示与评估配置，不发起任何委托或划转。
    </div>
    <div v-else-if="errorMessage" class="card panel error">{{ errorMessage }}</div>

    <template v-else>
      <div class="card panel">
        <div class="eval-bar">
          <label>评估金额（元）<input v-model="amount" placeholder="留空只校验配置" /></label>
          <label>用钱日<input v-model="needDate" type="date" /></label>
          <span class="muted">评估只读，不产生交易；未验证标的一律不可出借。</span>
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
              <tr v-else-if="!items.length"><td colspan="10" class="muted">暂无已登记的现金管理标的</td></tr>
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
        <div class="eval-bar eval-bar-ops">
          <label class="ops-field ops-outlet">标的
            <select v-model="opOutletId" :disabled="!items.length">
              <option v-if="!items.length" :value="null">暂无可选标的</option>
              <option v-for="row in items" :key="row.id" :value="row.id">
                {{ row.code }} {{ row.name }}
              </option>
            </select>
          </label>
          <label class="ops-field ops-amount">金额（元）<input v-model="opAmount" placeholder="如 10000" /></label>
          <label class="ops-field ops-mode">模式
            <select v-model="opMode">
              <option value="P1">P1 · 人工确认</option>
              <option value="P2">P2 · 半自动</option>
              <option value="P3">P3 · 自动放行</option>
            </select>
          </label>
          <label class="ops-field ops-session">会话 ID（P3 必填）<input v-model="opSessionId" placeholder="可留空" /></label>
          <div class="ops-actions">
            <button class="btn btn-primary" :disabled="opsBusy || !opAmount || !opOutletId" @click="submitOp">
              登记计划
            </button>
            <button class="btn" @click="checkOps">台账对账</button>
          </div>
        </div>
        <div v-if="!items.length" class="muted ops-empty">
          暂无可用标的：需先登记并验证现金管理标的，才可登记计划。
          <button class="link-btn" @click="openRegister">点此登记</button>
        </div>
        <div class="muted mode-help">
          <div><b>P1 · 人工确认</b>：登记后进入「待确认」，人工确认后才投放。</div>
          <div><b>P2 · 半自动</b>：登记后同样需人工确认；差别在实盘会话授权档位，会话档位需 ≥ P2 才允许。</div>
          <div><b>P3 · 自动放行</b>：须绑定 P3 会话，且标的已验证、通道可下单、T+0 或期限 ≤7 天、结算与回流确定，才自动放行；任一条不满足则转人工确认。</div>
          <div>台账不划转、不下单；现金管理收益单独统计，不进入策略归因。</div>
        </div>
      </div>

      <div v-if="opsError" class="card panel error">{{ opsError }}</div>
      <div v-if="regNotice" class="card panel reg-ok">{{ regNotice }}</div>

      <div v-if="opsSummary" class="card panel">
        <div class="eval-head">
          <span>在途本金 <b class="num">{{ opsSummary.outstanding_principal }}</b></span>
          <span>已回流 <b class="num">{{ opsSummary.returned_principal }}</b></span>
          <span>现金管理收益（单列）<b class="num">{{ opsSummary.yield_contribution }}</b></span>
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
                <th>ID</th><th>标的</th><th>模式</th><th>金额</th><th>状态</th>
                <th>预计回流</th><th>已回本金</th><th>收益</th><th>短款</th><th>操作</th>
              </tr>
            </thead>
            <tbody>
              <tr v-if="!ops.length"><td colspan="10" class="muted">暂无现金管理台账</td></tr>
              <tr v-for="op in ops" :key="op.id">
                <td class="num">{{ op.id }}</td>
                <td class="num">{{ op.outlet_id }}</td>
                <td>{{ MODE_LABEL[op.mode] || op.mode }}</td>
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

      <div v-if="showRegister" class="modal-overlay" @click.self="closeRegister">
        <div class="modal-content">
          <h3>登记现金管理标的</h3>
          <details class="reg-guide">
            <summary>怎么填（3 步）</summary>
            <ol>
              <li><b>选代码</b>：填 6 位代码，系统会校验格式并确认通达信能取到行情。</li>
              <li><b>选通道与流动性</b>：要走程序化/自动放行就选 tq/qmt/gj 并勾选程序化下单；只人工登记就选 manual。</li>
              <li><b>点「登记」</b>：列表立即刷新并自动选中，可马上登记计划。</li>
            </ol>
            <p>登记后为「未验证」：P1/P2 可人工确认后投放；P3 需先完成 C8 小额实盘验证才会自动放行。</p>
            <p>详细说明见项目文档 <code>docs/cash-management-guide.md</code>。</p>
          </details>
          <div class="reg-grid">
            <label>代码
              <input v-model="reg.code" placeholder="6 位，如 511880" maxlength="9" />
              <span class="hint">逆回购 204xxx（沪）/1318xx（深），场内货基 51xxxx（沪）/159xxx（深）；如 204001、511880。</span>
            </label>
            <label>名称
              <input v-model="reg.name" placeholder="可留空，默认用代码" />
              <span class="hint">可留空，默认用代码。</span>
            </label>
            <label>类型
              <select v-model="reg.category">
                <option value="">按代码自动识别</option>
                <option value="repo">逆回购</option>
                <option value="etf_money">货基 ETF</option>
                <option value="other">其他</option>
              </select>
              <span class="hint">按代码自动识别：204/1318 记为逆回购，51/159 记为货基 ETF；也可手动指定。</span>
            </label>
            <label>通道
              <select v-model="reg.channel"
                      @change="reg.channel === 'manual' && (reg.channel_order_enabled = false)">
                <option value="manual">manual（仅人工）</option>
                <option value="tq">tq</option>
                <option value="qmt">qmt</option>
                <option value="gj">gj</option>
              </select>
              <span class="hint">manual 只做人工登记、不走程序化下单，P3 不会自动放行；tq/qmt/gj 为程序化通道。</span>
            </label>
            <label>流动性
              <select v-model="reg.liquidity">
                <option value="t0">T+0</option>
                <option value="t1">T+1</option>
                <option value="term">到期</option>
              </select>
              <span class="hint">T+0 当日可用；T+1 次日可用；期限为到期回款（1–7 天，需填天数）。</span>
            </label>
            <label v-if="reg.liquidity === 'term'">到期天数
              <input v-model.number="reg.term_days" type="number" min="1" max="7" />
              <span class="hint">仅「期限」需要填，1–7 天。</span>
            </label>
            <label>风险等级
              <select v-model="reg.risk_level">
                <option value="R1">R1</option>
                <option value="R2">R2</option>
                <option value="">未评（评估 C6 会拦住）</option>
              </select>
              <span class="hint">优先选 R1；R2 次之；选「未评」会被评估 C6 拦住。</span>
            </label>
            <label>回流确定性
              <select v-model="reg.inflow_certainty">
                <option value="certain">确定</option>
                <option value="probable">可能（C7 会提示）</option>
              </select>
              <span class="hint">回流时间有把握选「确定」；只能估计选「可能」，C7 只作提示，不能当确定额度。</span>
            </label>
            <label class="full">备注
              <input v-model="reg.notes" placeholder="可留空" />
              <span class="hint">可留空；可写用途或联系人，便于日后对账。</span>
            </label>
            <label class="reg-check full">
              <input v-model="reg.channel_order_enabled" type="checkbox"
                     :disabled="reg.channel === 'manual'" />
              该标的所在通道支持程序化下单（manual 不支持）
              <span class="check-hint">勾选表示该通道支持程序化下单（manual 不可勾）；勾选后仍需 C8 小额实盘验证才可能自动放行。</span>
            </label>
          </div>
          <div class="muted reg-tip">
            登记只写配置，落库为未验证：P1/P2 仍需人工确认，P3 不会自动放行；
            需完成小额实盘验证（C8）后才可自动放行。
          </div>
          <div v-if="regError" class="error reg-msg">{{ regError }}</div>
          <div class="modal-actions">
            <button class="btn" :disabled="regBusy" @click="closeRegister">取消</button>
            <button class="btn btn-primary" :disabled="regBusy || !reg.code.trim()" @click="submitRegister">
              {{ regBusy ? '登记中…' : '登记' }}
            </button>
          </div>
        </div>
      </div>
    </template>
  </div>
</template>

<style scoped>
.panel { padding: 14px 16px; margin-bottom: 14px; }
.error { color: var(--red); }
.eval-bar { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 12px 14px; }
.eval-bar label { display: flex; flex-direction: column; gap: 4px; font-size: 12px; color: var(--ink-soft); min-width: 0; }
.eval-bar input, .eval-bar select {
  box-sizing: border-box; height: 32px; font-size: 13px;
  padding: 5px 8px; border: 1px solid var(--line); border-radius: 6px;
  background: var(--card-bg); color: var(--ink);
}
/* 台账登记行：标的展示码+名称，占剩余宽度；其余控件等宽，窄屏整体换行不重叠 */
.eval-bar-ops .ops-outlet { flex: 1 1 240px; min-width: 200px; max-width: 360px; }
.eval-bar-ops .ops-outlet select { width: 100%; }
.eval-bar-ops select { min-width: 168px; }
.eval-bar-ops input { width: 150px; }
.eval-bar-ops select:disabled { opacity: .7; cursor: not-allowed; }
.ops-actions { display: flex; flex: 0 0 auto; gap: 8px; }
.ops-actions .btn:disabled { cursor: not-allowed; opacity: .55; }
.ops-empty { margin-top: 8px; color: var(--warn); }
.mode-help { margin-top: 10px; line-height: 1.7; }
.mode-help b { color: var(--ink); font-weight: 600; }
.eval-head { display: flex; flex-wrap: wrap; align-items: center; gap: 10px; margin-bottom: 10px; }
.checks { list-style: none; margin: 0 0 10px; padding: 0; display: flex; flex-direction: column; gap: 6px; }
.checks li { display: flex; align-items: baseline; gap: 10px; font-size: 13px; }
.check-name { min-width: 96px; color: var(--ink); }
.state-fail .check-name { color: var(--red); }
.state-warn .check-name { color: var(--warn); }
.expires { margin-left: 6px; font-size: 11px; }
.muted { color: var(--ink-faint); font-size: 12px; }
.link-btn {
  background: none; border: none; padding: 0; margin-left: 4px; cursor: pointer;
  color: var(--accent); font-size: 12px; text-decoration: underline;
}
.reg-ok { color: var(--green, #1a7f37); }
.reg-guide {
  margin: -4px 0 14px; padding: 8px 12px; border: 1px solid var(--line);
  border-radius: 6px; background: rgba(127, 127, 127, .06);
  font-size: 12px; color: var(--ink-soft);
}
.reg-guide summary { cursor: pointer; font-weight: 600; color: var(--ink); }
.reg-guide ol { margin: 8px 0 6px; padding-left: 18px; line-height: 1.7; }
.reg-guide p { margin: 4px 0; line-height: 1.6; }
.reg-guide code { font-size: 11px; }
.reg-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 0 14px; }
.reg-grid .full { grid-column: 1 / -1; }
.reg-grid label { margin-bottom: 10px; }
.reg-grid .hint {
  display: block; margin: -6px 0 8px; font-size: 11px; line-height: 1.5;
  color: var(--ink-faint); font-weight: 400;
}
.modal-content .reg-check { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; font-weight: 400; }
.modal-content .reg-check input { width: auto; margin: 0; }
.reg-check .check-hint {
  flex-basis: 100%; margin-top: 2px; font-size: 11px; line-height: 1.5;
  color: var(--ink-faint); font-weight: 400;
}
.reg-tip { margin: 4px 0 8px; line-height: 1.6; }
.reg-msg { margin: 8px 0 0; font-size: 12px; }
@media (max-width: 560px) {
  .reg-grid { grid-template-columns: 1fr; }
}
</style>
