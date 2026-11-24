<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'
import PageHeader from '../components/PageHeader.vue'
import {
  activateSelectionVersion,
  createSelectionScheme,
  createSelectionVersion,
  getSelectionGpAudit,
  getSelectionMeta,
  getSelectionScheme,
  getSelectionSchemes,
  getSelectionWideDbStatus,
  type SelectionFieldSpec,
  type SelectionMeta,
  type SelectionSchemeItem,
  type SelectionVersionItem,
  type WideDbStatus,
} from '../api'

type TabKey = 'schemes' | 'filters' | 'results' | 'snapshots' | 'data'

const TABS: { key: TabKey; label: string }[] = [
  { key: 'schemes', label: '选股方案' },
  { key: 'filters', label: '筛选条件' },
  { key: 'results', label: '选股结果' },
  { key: 'snapshots', label: '快照与历史' },
  { key: 'data', label: '数据体检' },
]

const POOL_LABEL: Record<string, string> = {
  growth: '成长', quality: '质量', value: '价值', dividend: '红利',
}

const tab = ref<TabKey>('schemes')
const loading = ref(true)
const errorMsg = ref('')

const meta = ref<SelectionMeta | null>(null)
const schemes = ref<SelectionSchemeItem[]>([])
const selected = ref<{ id: number; versions: SelectionVersionItem[] } | null>(null)
const wideDb = ref<WideDbStatus | null>(null)

const newScheme = reactive({ code: '', name: '', pool_key: 'value', description: '' })
const newVersion = reactive({ spec: '', note: '' })
const fieldPoolFilter = ref('')
const gpAuditDay = ref(new Date().toISOString().slice(0, 10))
const gpAudit = ref<Record<string, unknown> | null>(null)

const filteredFields = computed<SelectionFieldSpec[]>(() => {
  const fields = meta.value?.fields || []
  if (!fieldPoolFilter.value) return fields
  return fields.filter((f) => f.pools.includes(fieldPoolFilter.value))
})

function errMsg(e: any): string {
  const d = e?.response?.data
  if (d?.message) return d.message
  if (Array.isArray(d?.detail)) return d.detail.map((x: any) => `${(x.loc || []).join('.')}: ${x.msg}`).join('; ')
  if (typeof d?.detail === 'string') return d.detail
  return e?.message || '请求失败'
}

function poolLabel(key: string): string {
  return POOL_LABEL[key] || key
}

async function loadAll() {
  loading.value = true
  errorMsg.value = ''
  try {
    const [metaRes, schemeRes, wideRes] = await Promise.all([
      getSelectionMeta(),
      getSelectionSchemes(true),
      getSelectionWideDbStatus().catch(() => null),
    ])
    meta.value = metaRes
    schemes.value = schemeRes
    wideDb.value = wideRes
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  } finally {
    loading.value = false
  }
}

async function openScheme(item: SelectionSchemeItem) {
  errorMsg.value = ''
  try {
    const detail = await getSelectionScheme(item.id)
    selected.value = { id: detail.id, versions: detail.versions }
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function submitScheme() {
  errorMsg.value = ''
  try {
    await createSelectionScheme({ ...newScheme })
    newScheme.code = ''
    newScheme.name = ''
    newScheme.description = ''
    schemes.value = await getSelectionSchemes(true)
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function submitVersion() {
  if (!selected.value) return
  errorMsg.value = ''
  let spec: Record<string, unknown>
  try {
    spec = JSON.parse(newVersion.spec || '{}')
  } catch {
    errorMsg.value = '参数不是合法 JSON'
    return
  }
  try {
    await createSelectionVersion(selected.value.id, {
      spec, note: newVersion.note,
    })
    newVersion.spec = ''
    newVersion.note = ''
    await openScheme({ id: selected.value.id } as SelectionSchemeItem)
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function activate(version: SelectionVersionItem) {
  if (!selected.value) return
  errorMsg.value = ''
  try {
    await activateSelectionVersion(selected.value.id, version.id)
    await openScheme({ id: selected.value.id } as SelectionSchemeItem)
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

async function runGpAudit() {
  errorMsg.value = ''
  gpAudit.value = null
  try {
    gpAudit.value = await getSelectionGpAudit(gpAuditDay.value)
  } catch (e: any) {
    errorMsg.value = errMsg(e)
  }
}

onMounted(loadAll)
</script>

<template>
  <PageHeader title="条件选股">
    <button class="btn" @click="loadAll">刷新</button>
  </PageHeader>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:12px">
    {{ errorMsg }}
  </div>

  <div class="tabs">
    <button
      v-for="t in TABS" :key="t.key"
      class="btn btn-sm" :class="{ 'btn-primary': tab === t.key }"
      @click="tab = t.key"
    >{{ t.label }}</button>
  </div>

  <div v-if="loading" class="card" style="padding:12px"><p>加载中…</p></div>

  <template v-else>
    <!-- 1. 选股方案 + 版本 -->
    <div v-show="tab === 'schemes'">
      <div class="card" style="padding:12px;margin-bottom:12px">
        <h3 style="margin-top:0">新建方案</h3>
        <div class="form-row">
          <input v-model="newScheme.code" placeholder="code，如 value_core" />
          <input v-model="newScheme.name" placeholder="名称" />
          <select v-model="newScheme.pool_key">
            <option v-for="p in meta?.pools || []" :key="p" :value="p">{{ poolLabel(p) }}</option>
          </select>
          <button class="btn btn-primary btn-sm" @click="submitScheme">创建</button>
        </div>
        <p class="hint">版本一旦创建不可修改；改权重请新建版本，旧版本保留供回看。</p>
      </div>

      <div class="card table-wrap">
        <table>
          <thead><tr><th>名称</th><th>code</th><th>池</th><th>状态</th><th>最新版本</th><th>操作</th></tr></thead>
          <tbody>
            <tr v-for="s in schemes" :key="s.id">
              <td>{{ s.name }}</td>
              <td style="color:var(--ink-soft)">{{ s.code }}</td>
              <td>{{ poolLabel(s.pool_key) }}</td>
              <td><span class="badge badge-gray">{{ s.status }}</span></td>
              <td>{{ s.latest_version ?? '—' }}</td>
              <td><button class="btn btn-sm" @click="openScheme(s)">版本</button></td>
            </tr>
          </tbody>
        </table>
        <div v-if="schemes.length === 0" class="empty-state"><p>暂无方案</p></div>
      </div>

      <div v-if="selected" class="card" style="padding:12px;margin-top:12px">
        <h3 style="margin-top:0">方案 #{{ selected.id }} 的版本</h3>
        <div class="card table-wrap" style="margin-bottom:12px">
          <table>
            <thead><tr><th>版本</th><th>状态</th><th>权重和</th><th>spec_hash</th><th>字段指纹</th><th>校准</th><th>操作</th></tr></thead>
            <tbody>
              <tr v-for="v in selected.versions" :key="v.id">
                <td>v{{ v.version }}</td>
                <td><span class="badge badge-gray">{{ v.status }}</span></td>
                <td>{{ v.weight_sum ?? '—' }}</td>
                <td style="color:var(--ink-soft)">{{ v.spec_hash }}</td>
                <td style="color:var(--ink-soft)">{{ v.field_fingerprint }}</td>
                <td>
                  <span v-if="v.uncalibrated" class="badge" style="background:var(--line);color:var(--ink-soft)">未校准</span>
                  <span v-else class="badge badge-green">已校准</span>
                </td>
                <td>
                  <button v-if="v.status !== 'active'" class="btn btn-sm" @click="activate(v)">启用</button>
                </td>
              </tr>
            </tbody>
          </table>
          <div v-if="selected.versions.length === 0" class="empty-state"><p>该方案还没有版本</p></div>
        </div>

        <h4>新建版本</h4>
        <textarea
          v-model="newVersion.spec" rows="5" style="width:100%"
          placeholder='{"pool_key":"value","weights":{"ep":0.6,"bp":0.4},"top_n":50}'
        ></textarea>
        <div class="form-row" style="margin-top:8px">
          <input v-model="newVersion.note" placeholder="变更说明（可选）" />
          <button class="btn btn-primary btn-sm" @click="submitVersion">保存为新版本</button>
        </div>
        <p class="hint">权重必须非负且和为 1；未登记字段会被拒绝。</p>
      </div>
    </div>

    <!-- 2. 筛选条件（字段字典） -->
    <div v-show="tab === 'filters'">
      <div class="card" style="padding:12px;margin-bottom:12px">
        <div class="form-row">
          <select v-model="fieldPoolFilter">
            <option value="">全部池</option>
            <option v-for="p in meta?.pools || []" :key="p" :value="p">{{ poolLabel(p) }}</option>
          </select>
          <span class="hint">字段集版本：{{ meta?.field_set_version }}</span>
        </div>
      </div>
      <div class="card table-wrap">
        <table>
          <thead><tr><th>字段</th><th>来源</th><th>原始列</th><th>单位</th><th>0 视为缺失</th><th>可用于历史</th><th>说明</th></tr></thead>
          <tbody>
            <tr v-for="f in filteredFields" :key="f.key">
              <td>{{ f.key }}</td>
              <td style="color:var(--ink-soft)">{{ f.source }}</td>
              <td style="color:var(--ink-soft)">{{ f.column }}</td>
              <td>{{ f.unit }}</td>
              <td>{{ f.zero_is_missing ? '是' : '否' }}</td>
              <td>
                <span v-if="f.pit_safe" class="badge badge-green">是</span>
                <span v-else class="badge badge-gray">否（仅当期）</span>
              </td>
              <td style="color:var(--ink-soft)">{{ f.note }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

    <!-- 3. 选股结果（待 A2） -->
    <div v-show="tab === 'results'">
      <div class="card" style="padding:16px">
        <h3 style="margin-top:0">结果表（未启用）</h3>
        <p class="hint">
          打分引擎（A2）依赖 D3 字段集定稿，本批次不产出选股结果。
          届时此处展示：排名 / 得分 / 各因子值 / 命中或排除原因。
        </p>
      </div>
    </div>

    <!-- 4. 快照与历史 -->
    <div v-show="tab === 'snapshots'">
      <div class="card" style="padding:16px">
        <h3 style="margin-top:0">快照与历史回看（未启用）</h3>
        <p class="hint">
          快照冻结后只读，回测按生效日引用当时的成分。
          每次快照记录字段集版本、方案指纹、配置指纹三项，任一不同即标记「不可比」。
        </p>
      </div>
    </div>

    <!-- 5. 数据体检 -->
    <div v-show="tab === 'data'">
      <div class="card" style="padding:12px;margin-bottom:12px">
        <h3 style="margin-top:0">外部宽库（只读导入源）</h3>
        <p v-if="!wideDb" class="hint">未获取到宽库状态</p>
        <template v-else>
          <p class="hint">
            路径 {{ wideDb.path }} · 大小 {{ wideDb.size_mb ?? '—' }} MB ·
            {{ wideDb.ready ? '可读' : ('不可用：' + (wideDb.error || 'unknown')) }}
          </p>
          <div class="card table-wrap">
            <table>
              <thead><tr><th>表</th><th>列数</th><th>有数据</th><th>行数估计</th></tr></thead>
              <tbody>
                <tr v-for="t in wideDb.tables" :key="t.name">
                  <td>{{ t.name }}</td>
                  <td>{{ t.columns }}</td>
                  <td>{{ t.has_rows ? '是' : '否' }}</td>
                  <td>{{ t.rows_estimate ?? '—' }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </template>
      </div>

      <div class="card" style="padding:12px">
        <h3 style="margin-top:0">GP16 缺口与除权断点核对</h3>
        <div class="form-row">
          <input v-model="gpAuditDay" type="date" />
          <button class="btn btn-sm btn-primary" @click="runGpAudit">核对</button>
        </div>
        <p class="hint">
          只读核对：停牌缺值不算真缺口；跳变超阈值但当日无除权事件记为 unexplained。
          取数入库需在导入窗口执行，页面不提供触发入口，避免实盘中占用 TQ 通道。
        </p>
        <pre v-if="gpAudit" class="card" style="padding:12px;overflow:auto;max-height:360px">{{ JSON.stringify(gpAudit, null, 2) }}</pre>
      </div>
    </div>
  </template>
</template>

<style scoped>
.tabs { display: flex; gap: 8px; margin-bottom: 12px; flex-wrap: wrap; }
.form-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
.form-row input, .form-row select { padding: 4px 8px; }
.hint { color: var(--ink-soft); font-size: 12px; }
</style>
