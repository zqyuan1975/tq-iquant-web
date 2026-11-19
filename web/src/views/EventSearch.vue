<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import TablePager from '../components/TablePager.vue'
import {
  getLiveSessions, searchDecisionEvents,
  type DecisionEventSearchFilters, type DecisionEventSearchItem,
  type LiveSessionItem,
} from '../api'

const sessions = ref<LiveSessionItem[]>([])
const items = ref<DecisionEventSearchItem[]>([])
const total = ref(0)
const page = ref(1)
const pageSize = ref(100)
const loading = ref(false)
const errorMsg = ref('')

const sessionId = ref('')
const gate = ref('')
const layer = ref('')
const action = ref('')
const ackStatus = ref<'' | 'open' | 'acked' | 'archived'>('open')
const keyword = ref('')
const start = ref('')
const end = ref('')

const totalPages = computed(() => Math.max(1, Math.ceil(total.value / (pageSize.value || 100))))

function fmtTime(iso: string | null | undefined): string {
  return iso && iso.length >= 19 ? iso.replace('T', ' ').slice(0, 19) : '—'
}

function buildFilters(): DecisionEventSearchFilters {
  const filters: DecisionEventSearchFilters = {
    limit: pageSize.value || 100,
    offset: (page.value - 1) * (pageSize.value || 100),
  }
  if (sessionId.value !== '') filters.session_id = Number(sessionId.value)
  if (gate.value.trim()) filters.gate = gate.value.trim()
  if (layer.value.trim()) filters.layer = layer.value.trim()
  if (action.value.trim()) filters.action = action.value.trim()
  if (ackStatus.value) filters.ack_status = ackStatus.value
  if (keyword.value.trim()) filters.keyword = keyword.value.trim()
  if (start.value) filters.start = start.value
  if (end.value) filters.end = end.value
  return filters
}

async function load() {
  loading.value = true
  errorMsg.value = ''
  try {
    const data = await searchDecisionEvents(buildFilters())
    items.value = data.items
    total.value = data.total
  } catch (e) {
    errorMsg.value = '加载失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    loading.value = false
  }
}

function search() {
  page.value = 1
  load()
}

function reset() {
  sessionId.value = ''
  gate.value = ''
  layer.value = ''
  action.value = ''
  ackStatus.value = 'open'
  keyword.value = ''
  start.value = ''
  end.value = ''
  search()
}

function onPage(p: number) {
  page.value = p
  load()
}

function onPageSize(size: number) {
  pageSize.value = size || 100
  page.value = 1
  load()
}

onMounted(async () => {
  try {
    sessions.value = await getLiveSessions()
  } catch {
    sessions.value = []
  }
  load()
})
</script>

<template>
  <PageHeader title="关键事件检索" />
  <div class="card" style="padding:12px;margin-bottom:12px">
    <div class="filter-grid">
      <label class="field">
        <span>会话</span>
        <select class="field-select field-select--compact" v-model="sessionId">
          <option value="">全部</option>
          <option v-for="s in sessions" :key="s.id" :value="String(s.id)">{{ s.name }}</option>
        </select>
      </label>
      <label class="field">
        <span>闸门门类 gate</span>
        <input v-model="gate" placeholder="如 stop_loss" />
      </label>
      <label class="field">
        <span>层级 layer</span>
        <input v-model="layer" placeholder="如 strategy_risk" />
      </label>
      <label class="field">
        <span>动作 action</span>
        <input v-model="action" placeholder="如 trigger" />
      </label>
      <label class="field">
        <span>处理状态</span>
        <select class="field-select field-select--compact" v-model="ackStatus">
          <option value="">全部</option>
          <option value="open">未处理</option>
          <option value="acked">已确认</option>
          <option value="archived">已归档</option>
        </select>
      </label>
      <label class="field">
        <span>关键字</span>
        <input v-model="keyword" placeholder="消息 / 股票 / 参数名" />
      </label>
      <label class="field">
        <span>开始日期</span>
        <input v-model="start" type="date" />
      </label>
      <label class="field">
        <span>结束日期</span>
        <input v-model="end" type="date" />
      </label>
    </div>
    <div class="filter-actions">
      <button class="btn btn-primary" :disabled="loading" @click="search">
        {{ loading ? '查询中…' : '查询' }}
      </button>
      <button class="btn" :disabled="loading" @click="reset">重置</button>
      <span class="filter-total">共 {{ total }} 条</span>
    </div>
  </div>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:12px">
    {{ errorMsg }}
  </div>

  <div class="card table-wrap">
    <table>
      <thead>
        <tr>
          <th>时间</th><th>会话</th><th>门类</th><th>层级</th><th>动作</th>
          <th>股票</th><th>参数</th><th>消息</th><th>状态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="e in items" :key="e.id ?? `${e.session_id}-${e.created_at}-${e.gate}`">
          <td>{{ fmtTime(e.created_at) }}</td>
          <td>{{ e.session_name || `#${e.session_id}` }}</td>
          <td>{{ e.gate }}</td>
          <td>{{ e.layer }}</td>
          <td>{{ e.action }}</td>
          <td>{{ e.stock_code || '—' }}</td>
          <td>{{ e.param_name || '—' }}</td>
          <td class="msg">{{ e.message || '—' }}</td>
          <td>
            <span class="badge" :class="(e.ack_status || 'open') === 'open' ? 'badge-blue' : 'badge-gray'"
                  :title="e.archive_reason || e.ack_note || ''">
              {{ (e.ack_status || 'open') === 'open' ? '未处理'
                : (e.ack_status === 'archived' ? '已归档' : '已确认') }}
            </span>
          </td>
        </tr>
      </tbody>
    </table>
    <div v-if="!loading && items.length === 0" class="empty-state"><p>没有匹配的事件</p></div>
  </div>

  <TablePager
    :page="page" :page-size="pageSize" :total-pages="totalPages"
    :displayed-count="items.length" :total-count="total"
    @update:page="onPage" @update:page-size="onPageSize"
  />
</template>

<style scoped>
.filter-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 10px;
}
.field { display: flex; flex-direction: column; gap: 4px; min-width: 0; font-size: 12px; color: var(--ink-soft); }
.field input, .field select {
  width: 100%; min-width: 0; height: 30px; box-sizing: border-box;
  padding: 5px 8px; border: 1px solid var(--border); border-radius: var(--radius);
  background-color: var(--card-bg); color: var(--ink);
}
.field select { padding-right: 22px; background-position: right 7px center; }
.filter-actions { display: flex; align-items: center; gap: 8px; margin-top: 10px; }
.filter-total { margin-left: auto; font-size: 12px; color: var(--ink-soft); }
.msg { max-width: 420px; overflow-wrap: anywhere; }
</style>
