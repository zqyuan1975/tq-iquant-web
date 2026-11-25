<script setup lang="ts">
import { ref, onMounted } from 'vue'
import {
  getTdxPools, getTdxPoolStocks, syncStockPool, getStockPools, deleteStockPool,
  pushSelectionPoolsToTdx,
  type TdxPoolItem, type TdxPoolStockItem,
} from '../api'
import PageHeader from '../components/PageHeader.vue'
import { gatePauseText } from '../utils/tqGatePause'

const pools = ref<TdxPoolItem[]>([])
const localIdByCode = ref<Record<string, number>>({})  // 删除用：code → 本地池 id
const loading = ref(true)
const errorMsg = ref('')
// 收盘保护窗口 / 实盘优先导致的「暂停」不是故障：用中性提示，别落到加载失败
const pausedMsg = ref('')
const showStocks = ref(false)
const stocksList = ref<TdxPoolStockItem[]>([])
const stocksPoolName = ref('')
// 推送到通达信：结果面板（板块数 / 股票数 / 落盘文件 / 备份目录）
const tdxPushMsg = ref('')
const tdxPushErr = ref('')
const pushingCode = ref('')

// 市场多空状态卡与统一市场状态面板已整体迁移至「投资看板」(/investment-board)。

// 序列池 = SEL_ 开头且末段不是 8 位日期；版本池（带日期）不推送，避免板块泛滥
function isSeriesPool(code: string) {
  if (!code.startsWith('SEL_')) return false
  const tail = code.split('_').pop() || ''
  return !/^\d{8}$/.test(tail)
}

// 从 axios 错误里提取后端错误消息（统一响应 {code,message} 或 Pydantic 422 detail）
function errMsg(e: any): string {
  const d = e?.response?.data
  if (d?.message) return d.message
  if (Array.isArray(d?.detail)) return d.detail.map((x: { loc?: unknown[]; msg?: string }) => `${(x.loc || []).join('.')}: ${x.msg}`).join('; ')
  if (typeof d?.detail === 'string') return d.detail
  return e?.message || '请求失败'
}

async function load() {
  errorMsg.value = ''
  pausedMsg.value = ''
  try {
    // 并行拉通达信板块 + 本地已同步池（取 id 供删除）
    const [tdxRes, localRes] = await Promise.all([
      getTdxPools(),
      getStockPools().catch(() => []),  // 本地列表失败不阻塞
    ])
    pools.value = tdxRes
    localIdByCode.value = {}
    for (const p of localRes) {
      localIdByCode.value[p.code] = p.id
    }
  } catch (e) {
    // 闸门暂停（保护窗口/实盘优先）→ 中性提示；真失败才走错误条
    const paused = gatePauseText(e)
    if (paused) {
      pausedMsg.value = paused
    } else {
      // getTdxPools 失败（拦截器 reject 或 HTTP 错误）→ 提示，清空列表
      errorMsg.value = '加载失败：' + errMsg(e)
    }
    pools.value = []
  } finally {
    loading.value = false
  }
}

async function viewStocks(p: TdxPoolItem) {
  try {
    stocksPoolName.value = p.name
    stocksList.value = await getTdxPoolStocks(p.code)
    showStocks.value = true
    pausedMsg.value = ''
  } catch (e) {
    const paused = gatePauseText(e)
    if (paused) {
      pausedMsg.value = paused
      return
    }
    alert(`查看成分股失败：${errMsg(e)}`)
  }
}

async function syncPool(p: TdxPoolItem) {
  if (!confirm(`确认从通达信同步「${p.name}」的股票清单？将全量替换现有股票。`)) return
  try {
    await syncStockPool({ code: p.code })
  } catch (e) {
    const paused = gatePauseText(e)
    if (paused) {
      pausedMsg.value = paused
      return
    }
    alert(`同步失败：${errMsg(e)}`)
    return
  }
  load()
}

async function pushTdx(p: TdxPoolItem) {
  if (!confirm(`确认把「${p.name}」（${p.stock_count} 只）写入通达信自定义板块？\n只会创建/覆盖平台自己的 SEL_ 板块，不动你的其他板块。`)) return
  pushingCode.value = p.code
  tdxPushMsg.value = ''
  tdxPushErr.value = ''
  try {
    const r = await pushSelectionPoolsToTdx({ pool_codes: [p.code] })
    const blocks = r.blocks.filter(b => b.status !== 'skipped_empty')
    const parts = blocks.map(b => `${b.name}(${b.block_code}) ${b.verified}/${b.requested} 只`)
    tdxPushMsg.value = `已写入 ${r.blocks_written} 个板块、${r.stocks_written} 只股票：${parts.join('；')}`
      + `。文件目录：${r.target_dir}；备份：${r.backup_dir}`
    if (r.blocks_partial) {
      tdxPushErr.value = `有 ${r.blocks_partial} 个板块回读数量与请求不一致，通达信可能过滤了部分代码`
    }
  } catch (e) {
    tdxPushErr.value = `推送到通达信失败：${errMsg(e)}`
  } finally {
    pushingCode.value = ''
  }
}

async function remove(p: TdxPoolItem) {
  const id = localIdByCode.value[p.code]
  if (!id) return
  if (!confirm(`确认删除股票池「${p.name}」？`)) return
  try {
    await deleteStockPool(id)
  } catch (e) {
    alert(`删除失败：${errMsg(e)}`)
    return
  }
  load()
}

onMounted(() => {
  load()
})
</script>

<template>
  <PageHeader title="备选股池">
    <button @click="load" class="btn">刷新</button>
  </PageHeader>

  <!-- 市场多空状态卡与统一市场状态面板已迁移至「投资看板」(/investment-board) -->
  <p class="hint" style="margin:-4px 0 12px">
    「同步」只把通达信用户板块<b>导入</b>到本平台，不会反向写入通达信；
    「已同步 / 未同步 / 通达信已删除」为本地与通达信板块的实时比对结果。
    SEL_ 前缀为条件选股生成的<b>平台内部池</b>，不参与通达信同步；
    需要看盘/手工下单时点该行「推送到通达信」，只写平台自己的 SEL_ 板块（写入前自动备份，
    不动你原有的自选与板块）。
  </p>

  <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:12px">
    {{ errorMsg }}
  </div>

  <div v-if="tdxPushMsg" class="card" style="padding:12px;color:var(--green);margin-bottom:12px">{{ tdxPushMsg }}</div>
  <div v-if="tdxPushErr" class="card" style="padding:12px;color:var(--red);margin-bottom:12px">{{ tdxPushErr }}</div>

  <!-- 闸门暂停：中性提示（不是失败），带上恢复时点 -->
  <div v-if="pausedMsg" class="tq-gate-notice">{{ pausedMsg }}</div>

  <div v-if="loading" class="card" style="padding:12px"><p>加载中…</p></div>
  <div v-else class="card table-wrap">
    <table>
      <thead><tr><th>名称</th><th>代码</th><th>状态</th><th>股票数</th><th>操作</th></tr></thead>
      <tbody>
        <tr v-for="p in pools" :key="p.code">
          <td>{{ p.name }}</td>
          <td style="color:var(--ink-soft)">{{ p.code }}</td>
          <td>
            <span v-if="p.origin === 'internal'" class="badge badge-blue" title="条件选股生成的平台内部池，可点「推送到通达信」写入自定义板块">平台内部池</span>
            <span v-else-if="!p.exists_in_tdx" class="badge" style="background:var(--line);color:var(--ink-soft)">通达信已删除</span>
            <span v-else-if="p.synced" class="badge badge-green">已同步</span>
            <span v-else class="badge badge-gray">未同步</span>
          </td>
          <td>{{ p.synced ? p.stock_count + ' 只' : '-' }}</td>
          <td>
            <button v-if="p.exists_in_tdx" @click="viewStocks(p)" class="btn btn-sm">查看</button>
            <button v-if="p.exists_in_tdx" @click="syncPool(p)" class="btn btn-sm btn-primary" style="margin-left:6px" title="从通达信导入到本平台（不反向写入）">
              {{ p.synced ? '同步' : '同步' }}
            </button>
            <button v-if="p.origin === 'internal' && isSeriesPool(p.code)" @click="pushTdx(p)" class="btn btn-sm" :disabled="!!pushingCode" title="写入通达信自定义板块，看盘/手工下单可直接调用">
              {{ pushingCode === p.code ? '推送中…' : '推送到通达信' }}
            </button>
            <button v-if="p.synced" @click="remove(p)" class="btn btn-sm btn-danger" style="margin-left:6px">删除</button>
          </td>
        </tr>
      </tbody>
    </table>
    <div v-if="pools.length === 0 && !errorMsg && !pausedMsg" class="empty-state"><p>暂无股票池</p></div>
  </div>

  <!-- 成分股清单 Modal -->
  <div v-if="showStocks" class="modal-overlay modal-lg" @click.self="showStocks = false">
    <div class="modal-content">
      <h3>{{ stocksPoolName }} 成分股（{{ stocksList.length }}）</h3>
      <div class="modal-body">
        <div class="card table-wrap" style="margin-bottom:12px">
          <table>
            <thead><tr><th>股票代码</th><th>名称</th></tr></thead>
            <tbody>
              <tr v-for="s in stocksList" :key="s.stock_code">
                <td>{{ s.stock_code }}</td>
                <td style="color:var(--ink-soft)">{{ s.stock_name || '-' }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="stocksList.length === 0" class="empty-state"><p>该板块暂无成分股</p></div>
        </div>
      </div>
      <div class="modal-actions">
        <button @click="showStocks = false" class="btn">关闭</button>
      </div>
    </div>
  </div>
</template>

