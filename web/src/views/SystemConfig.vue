<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import {
  getSystemConfigs, updateSystemConfigs, testLiveChannel,
  getBackupStatus, createBackup, listBackupRecords, verifyBackup, restoreBackup,
  getUpgradeCheck, upgradeBackup,
  type SystemConfig, type BackupStatus, type BackupRecord, type BackupVerifyResult,
  type UpgradeCheckResult,
} from '../api'
import { fmtBytes } from '../utils/format'
import PageHeader from '../components/PageHeader.vue'

// 与 core/config.py _defaults() 对齐：回测字段 tdx_path、QMT 字段 iquant_path、
// max_concurrent_backtest(回测并发上限)、database.sqlite_path(数据库路径)、
// iquant_bridge.simulation/live.base_url(两个模式位当前都指 8790；该账户为 QMT 仿真账户/仿真资金，8791 信用腿未启用)
// SystemConfig 接口定义见 api/index.ts（对齐 config.py _defaults()）
const DEFAULTS: SystemConfig = {
  tdx_path: '',
  iquant_path: '',
  max_concurrent_backtest: 1,
  database: { sqlite_path: '' },
  backup: {
    enabled: true,
    daily_at: '15:10',
    keep: 10,
    min_free_mb: 500,
    dir: 'data/backup',
    restore_enabled: false,
  },
  iquant_bridge: {
    simulation: { base_url: '' },
    live: { base_url: '' },
  },
  ths_ui: {
    enabled: false,
    window_title: '网上股票交易系统5.0',
    client_exe_path: '',
    captcha_ocr: false,
    execute_mode: 'file_builtin',
    execute_interval_seconds: 5,
    work_dir: 'data/ths',
  },
  tq_trade: {
    account: '',
    account_type: 'stock',
    price_mode: 'my',
    client_path: '',
    default_engine: 'tdx',
    cancel_unfilled_minutes: 3,
    pre_open_auction_enabled: false,
    pre_open_auction_end_time: '09:20',
    pre_open_auction_price_bias_pct: 2,
  },
  goldminer: {
    base_url: 'http://127.0.0.1:7801',
    token: '',
    account_id: '',
  },
}

const config = ref<SystemConfig>(JSON.parse(JSON.stringify(DEFAULTS)))
const loading = ref(true)
const saved = ref(false)
const errorMsg = ref('')

// 左侧分组导航：分组多时一屏到底难定位，改为「导航 + 单分组面板」。
// 面板用 v-show 保持 DOM 常驻（输入态由 config 持有，切换不丢）。
const GROUPS = [
  { key: 'base', label: '基础环境' },
  { key: 'ths', label: '同花顺通道' },
  { key: 'tq', label: '通达信 TQ 通道' },
  { key: 'preopen', label: '盘前集合竞价' },
  { key: 'misc', label: '回测与数据' },
  { key: 'backup', label: '数据备份' },
  { key: 'upgrade', label: '升级与回滚' },
] as const
type GroupKey = (typeof GROUPS)[number]['key']
const activeGroup = ref<GroupKey>('base')

onMounted(async () => {
  try {
    const data = await getSystemConfigs()
    config.value = {
      tdx_path: data.tdx_path ?? DEFAULTS.tdx_path,
      iquant_path: data.iquant_path ?? DEFAULTS.iquant_path,
      max_concurrent_backtest: data.max_concurrent_backtest ?? DEFAULTS.max_concurrent_backtest,
      database: { sqlite_path: data.database?.sqlite_path ?? DEFAULTS.database.sqlite_path },
      backup: {
        enabled: data.backup?.enabled ?? DEFAULTS.backup.enabled,
        daily_at: data.backup?.daily_at ?? DEFAULTS.backup.daily_at,
        keep: data.backup?.keep ?? DEFAULTS.backup.keep,
        min_free_mb: data.backup?.min_free_mb ?? DEFAULTS.backup.min_free_mb,
        dir: data.backup?.dir ?? DEFAULTS.backup.dir,
        restore_enabled:
          data.backup?.restore_enabled ?? DEFAULTS.backup.restore_enabled,
      },
      iquant_bridge: {
        simulation: { base_url: data.iquant_bridge?.simulation?.base_url ?? DEFAULTS.iquant_bridge.simulation.base_url },
        live: { base_url: data.iquant_bridge?.live?.base_url ?? DEFAULTS.iquant_bridge.live.base_url },
      },
      goldminer: {
        base_url: data.goldminer?.base_url ?? DEFAULTS.goldminer!.base_url,
        token: data.goldminer?.token ?? DEFAULTS.goldminer!.token,
        account_id: data.goldminer?.account_id ?? DEFAULTS.goldminer!.account_id,
      },
      ths_ui: {
        enabled: data.ths_ui?.enabled ?? DEFAULTS.ths_ui.enabled,
        window_title: data.ths_ui?.window_title ?? DEFAULTS.ths_ui.window_title,
        client_exe_path: data.ths_ui?.client_exe_path ?? DEFAULTS.ths_ui.client_exe_path,
        captcha_ocr: data.ths_ui?.captcha_ocr ?? DEFAULTS.ths_ui.captcha_ocr,
        execute_mode: data.ths_ui?.execute_mode ?? DEFAULTS.ths_ui.execute_mode,
        execute_interval_seconds:
          data.ths_ui?.execute_interval_seconds ?? DEFAULTS.ths_ui.execute_interval_seconds,
        work_dir: data.ths_ui?.work_dir ?? DEFAULTS.ths_ui.work_dir,
      },
      tq_trade: {
        account: data.tq_trade?.account ?? DEFAULTS.tq_trade!.account,
        account_type: data.tq_trade?.account_type ?? DEFAULTS.tq_trade!.account_type,
        price_mode: data.tq_trade?.price_mode ?? DEFAULTS.tq_trade!.price_mode,
        client_path: data.tq_trade?.client_path ?? DEFAULTS.tq_trade!.client_path,
        default_engine: data.tq_trade?.default_engine ?? DEFAULTS.tq_trade!.default_engine,
        cancel_unfilled_minutes:
          data.tq_trade?.cancel_unfilled_minutes ?? DEFAULTS.tq_trade!.cancel_unfilled_minutes,
        pre_open_auction_enabled:
          data.tq_trade?.pre_open_auction_enabled
          ?? DEFAULTS.tq_trade!.pre_open_auction_enabled,
        pre_open_auction_end_time:
          data.tq_trade?.pre_open_auction_end_time
          ?? DEFAULTS.tq_trade!.pre_open_auction_end_time,
        pre_open_auction_price_bias_pct:
          data.tq_trade?.pre_open_auction_price_bias_pct
          ?? DEFAULTS.tq_trade!.pre_open_auction_price_bias_pct,
      },
    }
  } catch (e) {
    errorMsg.value = '加载配置失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    loading.value = false
  }
  await loadBackupStatus()
  await loadBackupRecords()
  await loadUpgradeCheck()
})

// ---- 数据备份（内嵌：VACUUM INTO 一致性副本 + WAL 收敛）----
// 状态单独读取（含上次自动/手动备份记录），与配置表单互不影响；
// 备份线程在平台进程内，点击「立即备份」为同步 HTTP（后端线程池执行，不阻塞事件循环）。
const backupStatus = ref<BackupStatus | null>(null)
const backupBusy = ref(false)
const backupMsg = ref('')
const backupErr = ref('')

async function loadBackupStatus() {
  try {
    backupStatus.value = await getBackupStatus()
  } catch (e) {
    backupErr.value = '读取备份状态失败: ' + (e instanceof Error ? e.message : String(e))
  }
}

// ---- 升级与回滚（批次22 L5，只读检查 + 升级前强制备份）----
const upgrade = ref<UpgradeCheckResult | null>(null)
const upgradeBusy = ref(false)
const upgradeMsg = ref('')
const upgradeErr = ref('')

async function loadUpgradeCheck() {
  try {
    upgrade.value = await getUpgradeCheck()
  } catch (e) {
    upgradeErr.value = '读取升级检查失败: ' + (e instanceof Error ? e.message : String(e))
  }
}

function upgradeRevision(revs: string[] | null | undefined): string {
  return revs && revs.length ? revs.join(', ') : '—'
}

async function prepareUpgradeBackup() {
  if (!window.confirm('执行升级前备份？将生成一份手动备份，不会修改当前库。')) return
  upgradeBusy.value = true
  upgradeMsg.value = ''
  upgradeErr.value = ''
  try {
    const res = await upgradeBackup(true)
    upgradeMsg.value = `升级前备份完成${res.record?.filename ? '：' + res.record.filename : ''}`
    await loadUpgradeCheck()
    await loadBackupStatus()
    await loadBackupRecords()
  } catch (e) {
    upgradeErr.value = '升级前备份失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    upgradeBusy.value = false
  }
}

/** 备份记录一行文案：时间 / 大小（非成功状态追加标注）。 */
function backupLine(rec: BackupRecord | null | undefined): string {
  if (!rec) return '暂无'
  const t = rec.started_at ? rec.started_at.replace('T', ' ').slice(0, 19) : '—'
  const size = rec.size_bytes == null ? '—' : fmtBytes(rec.size_bytes)
  const tag = rec.status === 'success'
    ? ''
    : `（${rec.status === 'skipped' ? '已跳过' : '失败'}）`
  return `${t} / ${size}${tag}`
}

/**
 * 今日自动备份的**实际执行时间**（HH:MM），取自当日自动备份记录的 started_at。
 * 不能用配置项 backup.daily_at：启动晚于 daily_at 时由补备逻辑执行，实际时间晚于点位。
 */
const autoDoneTodayAt = computed(() => {
  const s = backupStatus.value
  if (!s?.auto_done_today) return ''
  const started = s.last_auto?.started_at
  if (!started) return ''
  const m = /T(\d{2}):(\d{2})/.exec(started)
  return m ? `${m[1]}:${m[2]}` : ''
})

async function runBackup() {
  backupBusy.value = true
  backupMsg.value = ''
  backupErr.value = ''
  try {
    const rec = await createBackup()
    backupMsg.value = `备份完成${rec?.filename ? '：' + rec.filename : ''}`
    await loadBackupStatus()
    await loadBackupRecords()
  } catch (e) {
    backupErr.value = '备份失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    backupBusy.value = false
  }
}

// ---- 从备份恢复（批次21 L2，破坏性操作，需后端 backup.restore_enabled=true）----
// 交互锁定：先选备份 → 只读校验 → 勾选确认 → 恢复；恢复前后端会自动备份当前库。
const backupRecords = ref<BackupRecord[]>([])
const selectedRecordId = ref<number | null>(null)
const verifyResult = ref<BackupVerifyResult | null>(null)
const restoreConfirm = ref(false)
const restoreBusy = ref(false)
const restoreMsg = ref('')
const restoreErr = ref('')

const restoreEnabled = computed(() => backupStatus.value?.restore_enabled === true)
const selectedRecord = computed(
  () => backupRecords.value.find((r) => r.id === selectedRecordId.value) ?? null,
)

async function loadBackupRecords() {
  try {
    const rows = await listBackupRecords(20)
    backupRecords.value = rows.filter((r) => r.status === 'success' && !!r.path)
  } catch {
    backupRecords.value = []  // 记录读取失败不阻塞配置页其它功能
  }
}

function backupOptionLabel(r: BackupRecord): string {
  const t = r.started_at ? r.started_at.replace('T', ' ').slice(0, 19) : '—'
  const kind = r.kind === 'auto' ? '自动' : '手动'
  return `${t} / ${kind} / ${r.filename || '—'}`
}

function onSelectBackup() {
  verifyResult.value = null
  restoreConfirm.value = false
  restoreMsg.value = ''
  restoreErr.value = ''
}

async function doVerify() {
  if (!selectedRecordId.value) return
  restoreBusy.value = true
  restoreMsg.value = ''
  restoreErr.value = ''
  try {
    verifyResult.value = await verifyBackup({ record_id: selectedRecordId.value })
  } catch (e) {
    verifyResult.value = null
    restoreErr.value = '校验失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    restoreBusy.value = false
  }
}

async function doRestore() {
  const rec = selectedRecord.value
  if (!rec?.id || !restoreConfirm.value) return
  const label = backupOptionLabel(rec)
  if (!window.confirm(`确认用以下备份覆盖当前数据库？\n${label}\n\n恢复前会自动备份当前库。`)) {
    return
  }
  restoreBusy.value = true
  restoreMsg.value = ''
  restoreErr.value = ''
  try {
    const res = await restoreBackup({ record_id: rec.id, confirm: true })
    restoreMsg.value = res.message || '恢复完成'
    restoreConfirm.value = false
    verifyResult.value = res.verify ?? verifyResult.value
    await loadBackupStatus()
    await loadBackupRecords()
  } catch (e) {
    restoreErr.value = '恢复失败: ' + (e instanceof Error ? e.message : String(e))
  } finally {
    restoreBusy.value = false
  }
}

async function save() {
  saved.value = false
  errorMsg.value = ''
  try {
    await updateSystemConfigs(config.value)
    saved.value = true
    setTimeout(() => saved.value = false, 2000)
  } catch (e) {
    errorMsg.value = '保存失败: ' + (e instanceof Error ? e.message : String(e))
  }
}

// 通道连通性探测（只读：连接窗口/账户 + 读一次持仓，不下单）。
// 后端读取已保存的 config.yaml，故修改路径/账号后需先「保存」再测试。
// 测试结果按通道分别持有，切换分组互不影响。
const testing = ref('')
const testResults = ref<Record<string, { ok: boolean; text: string }>>({})

async function testChannel(channel: 'ths_ui' | 'tdx_tq') {
  testing.value = channel
  try {
    const r = await testLiveChannel(channel)
    testResults.value[channel] = r.ok
      ? { ok: true, text: `连接成功，读到 ${r.positions ?? 0} 条持仓` }
      : { ok: false, text: r.error || '连接失败' }
  } catch (e) {
    testResults.value[channel] = { ok: false, text: e instanceof Error ? e.message : String(e) }
  } finally {
    testing.value = ''
  }
}

function testThs() { return testChannel('ths_ui') }
function testTdx() { return testChannel('tdx_tq') }
</script>

<template>
  <PageHeader title="系统配置" />
  <div class="card cfg-card">
    <div v-if="loading" class="empty-state"><p>加载中…</p></div>

    <div v-else>
      <div v-if="errorMsg" class="card" style="padding:12px;color:var(--red);margin-bottom:16px">
        {{ errorMsg }}
      </div>

      <div class="cfg-layout">
        <nav class="cfg-nav">
          <button
            v-for="g in GROUPS" :key="g.key"
            class="cfg-nav-item" :class="{ active: activeGroup === g.key }"
            @click="activeGroup = g.key"
          >{{ g.label }}</button>
        </nav>

        <div class="cfg-pane">
          <section v-show="activeGroup === 'base'">
            <h3 class="cfg-h">通达信</h3>
            <p class="cfg-desc">
              通达信客户端安装目录，回测行情 / 日线 / 周线数据的来源（通达信规范）
            </p>
            <div class="field">
              <label>通达信目录</label>
              <input v-model="config.tdx_path" placeholder="D:\new_tdx64" />
            </div>

            <h3 class="cfg-h">掘金量化桥（goldminer）</h3>
            <p class="cfg-desc">
              桥策略（goldminer_bridge.py）跑在掘金终端内，本地 HTTP 服务供平台下单/查询。
              下方 Token / 账户 ID 仅用于核对展示，<b>不会</b>传给桥；真正生效的是桥文件顶部的
              TOKEN / ACCOUNT / STRATEGY_ID 常量。保存后新建会话生效，运行中的会话需停止再启动。
            </p>
            <div class="field">
              <label>桥服务地址</label>
              <input v-model="config.goldminer.base_url" placeholder="http://127.0.0.1:7801" />
            </div>
            <div class="field">
              <label>掘金 Token</label>
              <input v-model="config.goldminer.token" placeholder="掘金终端「账户管理」中的 token" />
            </div>
            <div class="field">
              <label>账户 ID</label>
              <input v-model="config.goldminer.account_id" placeholder="仿真账户 ID（长线法宝）" />
            </div>
          </section>

          <section v-show="activeGroup === 'ths'">
            <h3 class="cfg-h">同花顺 UI 下单通道</h3>
            <p class="cfg-desc">
              同花顺通道是每会话可选引擎：在「新建实盘」选择同花顺便由独立交易客户端（xiadan.exe）
              执行下单，投资平台只生成信号。此开关仅影响 engine 为空的历史会话在 default_engine
              未配置/非法时的回退判定（启用→ths，否则→iquant），<b>不会</b>强制切换或禁用已有会话；
              切换引擎需停止再启动会话。
            </p>
            <div class="field">
              <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
                <input v-model="config.ths_ui.enabled" type="checkbox" style="width:auto" />
                启用同花顺通道回退（仅影响 engine 为空的历史会话，不代表启用/禁用该引擎）
              </label>
            </div>
            <div class="field">
              <label>交易客户端路径</label>
              <input v-model="config.ths_ui.client_exe_path" placeholder="D:/同花顺远航版/transaction/xiadan.exe" />
            </div>
            <div class="field">
              <label>窗口标题</label>
              <input v-model="config.ths_ui.window_title" placeholder="网上股票交易系统5.0" />
            </div>
            <div class="field">
              <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
                <input v-model="config.ths_ui.captcha_ocr" type="checkbox" style="width:auto" />
                启用验证码 OCR（使用 Windows 内置 OCR，默认关闭）
              </label>
            </div>
            <div class="field">
              <label>执行模式</label>
              <select v-model="config.ths_ui.execute_mode">
                <option value="file_builtin">文件内置执行器（信号/指令落 JSONL，后台按频率执行）</option>
                <option value="direct">直接执行（同步 UI 自动化）</option>
              </select>
              <p class="cfg-hint">
                file_builtin：place_order 写入指令文件秒回，内置执行器按轮询间隔执行键鼠自动化；
                成交/持仓仍走 UI 读回回填。撤单已自动化（F3 撤单页双击委托行）；
                读后游标即推进（at-most-once），执行前崩溃的单不重复执行，超时走 expire rejected。
              </p>
            </div>
            <div class="field">
              <label>执行器轮询间隔(秒)</label>
              <input
                v-model.number="config.ths_ui.execute_interval_seconds"
                type="number" min="1" style="max-width:180px"
              />
              <p class="cfg-hint">内置执行器每 N 秒读一次指令文件（默认 5 秒）</p>
            </div>
            <div class="field">
              <label>交接文件工作目录</label>
              <input v-model="config.ths_ui.work_dir" placeholder="data/ths" />
              <p class="cfg-hint">
                相对服务进程目录解析（默认 main/data/ths），每个 session 一个子目录
                {work_dir}/{session_id}/：signals.jsonl（信号）/ risk.jsonl（风控决策）/
                orders.jsonl（下单指令）/ results.jsonl（执行回执）/ cursor.json（消费游标）
              </p>
            </div>
            <div class="cfg-test">
              <button @click="testThs" class="btn btn-sm" :disabled="testing !== ''">
                {{ testing === 'ths_ui' ? '测试中…' : '测试连接（只读）' }}
              </button>
              <span v-if="testResults.ths_ui" :style="{ color: testResults.ths_ui.ok ? 'var(--green)' : 'var(--red)', fontSize: '13px' }">
                {{ testResults.ths_ui.text }}
              </span>
            </div>
          </section>

          <section v-show="activeGroup === 'tq'">
            <h3 class="cfg-h">通达信 TQ 交易通道</h3>
            <p class="cfg-desc">
              走通达信客户端内置的 TQ 交易接口（tqcenter），与回测取数同一条连接。
              前置条件：通达信客户端已启动并登录资金账号，PYPlugins 下存在 tqcenter.py。
              下单确认面板默认自动处置（config.tq_trade.auto_confirm_dialog，默认 true）；
              若关闭该自动处置，则需在客户端关闭「下单确认」，否则会被判定为半自动模式拒单。
            </p>
            <div class="field">
              <label>资金账号</label>
              <input v-model="config.tq_trade!.account" placeholder="留空则用客户端当前登录账号" />
            </div>
            <div class="field">
              <label>账号类型</label>
              <input v-model="config.tq_trade!.account_type" placeholder="stock" />
            </div>
            <div class="field">
              <label>下单价格模式</label>
              <select v-model="config.tq_trade!.price_mode">
                <option value="my">自填价（策略价，默认）</option>
                <option value="latest">最新价</option>
                <option value="ztj">涨停价</option>
                <option value="dtj">跌停价</option>
              </select>
            </div>
            <div class="field">
              <label>TQ 插件目录（留空用上方通达信目录）</label>
              <input v-model="config.tq_trade!.client_path" placeholder="D:\zd_zyb" />
            </div>
            <div class="field">
              <label>默认交易引擎</label>
              <select v-model="config.tq_trade!.default_engine">
                <option value="tdx">通达信 TQ（tdx）</option>
                <option value="goldminer">掘金量化 HTTP 桥（goldminer）</option>
                <option value="ths">同花顺 UI 自动化（ths）</option>
              </select>
              <p class="cfg-hint">engine 为空的历史会话/旧 qmt(miniQMT) 会话回退使用；未配置或非法时回退 ths_ui.enabled 且依赖可用→ths，否则 iquant</p>
            </div>
            <div class="field">
              <label>超时撤单(分钟)</label>
              <input
                v-model.number="config.tq_trade!.cancel_unfilled_minutes"
                type="number" min="0" style="max-width:180px"
              />
              <p class="cfg-hint">&gt;0：在途单超时未成交自动撤单并释放同向闸；0=禁用</p>
            </div>
            <div class="cfg-test">
              <button @click="testTdx" class="btn btn-sm" :disabled="testing !== ''">
                {{ testing === 'tdx_tq' ? '测试中…' : '测试连接（只读）' }}
              </button>
              <span v-if="testResults.tdx_tq" :style="{ color: testResults.tdx_tq.ok ? 'var(--green)' : 'var(--red)', fontSize: '13px' }">
                {{ testResults.tdx_tq.text }}
              </span>
            </div>
          </section>

          <section v-show="activeGroup === 'preopen'">
            <h3 class="cfg-h">盘前集合竞价（全引擎生效）</h3>
            <p class="cfg-desc">
              对通达信 TQ / 同花顺 / 掘金 / QMT 四条交易通道均生效，与各通道自身参数相互独立。
              默认开启；关闭后行为与旧版一致（次日首根完成 1m bar 约 09:31 放行）。
              开启后在交易日 09:15–结束时间 把「当日 due」的 deferred 日线信号单直接
              报进集合竞价，不依赖 1 分钟 bar；未报成/闸门拦截的单回 deferred 队列，
              由 09:31 首根 bar 路径兜底重试（同一单不会两次报出）。
            </p>
            <div class="field">
              <label class="cfg-check">
                <input v-model="config.tq_trade!.pre_open_auction_enabled" type="checkbox" />
                启用盘前集合竞价
              </label>
            </div>
            <div class="field">
              <label>盘前执行结束时间</label>
              <input
                v-model="config.tq_trade!.pre_open_auction_end_time"
                placeholder="09:20" style="max-width:180px"
              />
              <p class="cfg-hint">09:15 起、左闭右开；默认 09:20（之后进入不可撤单撮合段，不再新报）</p>
            </div>
            <div class="field">
              <label>盘前让价幅度(%)</label>
              <input
                v-model.number="config.tq_trade!.pre_open_auction_price_bias_pct"
                type="number" min="0" step="0.1" style="max-width:180px"
              />
              <p class="cfg-hint">
                委托价 = 信号价 × (1 ± 幅度)，并夹进当日涨跌停（涨停−1 跳 / 跌停+1 跳）。
                买向上让价提高竞价成交率，卖向下让价；不推到涨跌停（避免余量挂板追价）
              </p>
            </div>
          </section>

          <section v-show="activeGroup === 'misc'">
            <h3 class="cfg-h">回测</h3>
            <p class="cfg-desc">
              同一时刻最多允许运行的回测任务数，超出返回 HTTP 409
            </p>
            <div class="field">
              <label>最大并发回测</label>
              <input v-model.number="config.max_concurrent_backtest" type="number" min="1" style="max-width:180px" />
            </div>

            <h3 class="cfg-h">数据库</h3>
            <p class="cfg-desc">
              SQLite 数据库文件路径（相对 main/ 目录解析，纯单用户本地工具）
            </p>
            <div class="field">
              <label>数据库文件路径</label>
              <input v-model="config.database.sqlite_path" placeholder="data/dev.db" />
              <p class="cfg-hint">修改后需重启后端进程生效（数据库连接在进程启动时建立）</p>
            </div>
          </section>

          <section v-show="activeGroup === 'backup'">
            <h3 class="cfg-h">自动备份</h3>
            <p class="cfg-desc">
              每日收盘后在平台进程内用 <code>VACUUM INTO</code> 生成一致性副本并收敛一次 WAL，
              不停服、不影响运行中的实盘会话；自动备份只保留最近 N 份，手动备份不参与自动清理。
            </p>
            <div class="field">
              <label class="cfg-check">
                <input v-model="config.backup.enabled" type="checkbox" />
                启用每日自动备份
              </label>
            </div>
            <div class="field">
              <label>每日备份时间</label>
              <input v-model="config.backup.daily_at" placeholder="15:10" style="max-width:180px" />
              <p class="cfg-hint">24 小时制 HH:MM（Asia/Shanghai），默认 15:10 收盘后</p>
            </div>
            <div class="field">
              <label>自动备份保留份数</label>
              <input v-model.number="config.backup.keep" type="number" min="1" style="max-width:180px" />
              <p class="cfg-hint">超出后删除最旧的自动备份；手动备份不参与自动清理</p>
            </div>
            <div class="field">
              <label>磁盘保护阈值(MB)</label>
              <input v-model.number="config.backup.min_free_mb" type="number" min="0" style="max-width:180px" />
              <p class="cfg-hint">备份前剩余空间低于该值则跳过本次备份并记 WARNING，不写坏库</p>
            </div>
            <p class="cfg-hint">以上配置随下方「保存」生效；「立即备份」按当前生效的配置执行。</p>

            <h3 class="cfg-h">备份记录</h3>
            <div class="backup-status">
              <div class="backup-row">
                <span class="backup-label">上次自动备份</span>
                <span>{{ backupLine(backupStatus?.last_auto) }}</span>
              </div>
              <div class="backup-row">
                <span class="backup-label">上次手动备份</span>
                <span>{{ backupLine(backupStatus?.last_manual) }}</span>
              </div>
              <div class="backup-row">
                <span class="backup-label">备份目录</span>
                <span>{{ backupStatus?.backup_dir || '—' }}</span>
              </div>
            </div>
            <div class="cfg-test">
              <button @click="runBackup" class="btn btn-sm" :disabled="backupBusy">
                {{ backupBusy ? '备份中…' : '立即备份' }}
              </button>
              <span v-if="autoDoneTodayAt" class="cfg-hint">
                今日已自动备份（{{ autoDoneTodayAt }}）
              </span>
              <span v-if="backupMsg" style="color:var(--green);font-size:13px">{{ backupMsg }}</span>
              <span v-if="backupErr" style="color:var(--red);font-size:13px">{{ backupErr }}</span>
            </div>

            <h3 class="cfg-h">从备份恢复</h3>
            <p class="cfg-desc">
              恢复会用所选备份<b>覆盖当前数据库</b>（破坏性）。需在 config.yaml 设置
              <code>restore_enabled: true</code>；存在运行中/停止中的实盘会话时后端直接拒绝；
              恢复前会先自动备份当前库，备份失败即中止，恢复后重开连接校验表数与行数。
            </p>
            <div class="field">
              <label>选择备份</label>
              <select v-model="selectedRecordId" @change="onSelectBackup" style="max-width:460px">
                <option :value="null">— 请选择 —</option>
                <option v-for="r in backupRecords" :key="r.id ?? r.path ?? ''" :value="r.id">
                  {{ backupOptionLabel(r) }}
                </option>
              </select>
              <p class="cfg-hint">仅列出成功且带文件路径的备份（最近 20 条）</p>
            </div>
            <div class="cfg-test">
              <button class="btn btn-sm" @click="doVerify" :disabled="!selectedRecordId || restoreBusy">
                {{ restoreBusy ? '处理中…' : '校验所选备份（只读）' }}
              </button>
            </div>
            <div v-if="verifyResult" class="backup-status">
              <div class="backup-row">
                <span class="backup-label">完整性</span>
                <span>{{ verifyResult.integrity || '—' }}</span>
              </div>
              <div class="backup-row">
                <span class="backup-label">表数</span>
                <span>{{ verifyResult.table_count }}</span>
              </div>
              <div class="backup-row">
                <span class="backup-label">最新备份记录</span>
                <span>{{ verifyResult.latest_record || '—' }}</span>
              </div>
              <div class="backup-row">
                <span class="backup-label">行数预览</span>
                <span>
                  {{ Object.entries(verifyResult.row_counts).slice(0, 6)
                    .map(([k, v]) => `${k}=${v}`).join('，') || '—' }}
                </span>
              </div>
            </div>
            <div class="field">
              <label class="cfg-check">
                <input v-model="restoreConfirm" type="checkbox" :disabled="!restoreEnabled" />
                我已了解恢复会覆盖当前数据库（恢复前会自动备份当前库）
              </label>
            </div>
            <div class="cfg-test">
              <button
                class="btn btn-sm btn-danger" @click="doRestore"
                :disabled="!restoreEnabled || !restoreConfirm || !selectedRecordId || restoreBusy"
              >
                {{ restoreBusy ? '恢复中…' : '从所选备份恢复' }}
              </button>
              <span v-if="!restoreEnabled" class="cfg-hint">
                恢复功能未启用（config.yaml backup.restore_enabled=false）
              </span>
              <span v-if="restoreMsg" style="color:var(--green);font-size:13px">{{ restoreMsg }}</span>
              <span v-if="restoreErr" style="color:var(--red);font-size:13px">{{ restoreErr }}</span>
            </div>
          </section>

          <section v-show="activeGroup === 'upgrade'">
            <h3 class="cfg-h">升级前检查（只读）</h3>
            <p class="cfg-desc">
              对比数据库 <code>alembic_version</code> 与代码迁移 head，并检查最近备份是否新鲜。
              <b>本页不会执行任何迁移</b>；升级仍按台账人工执行 <code>alembic upgrade head</code>。
              自动启动校验与「升级前备份」需在 config.yaml 设置 <code>upgrade.enabled: true</code>。
            </p>
            <div v-if="upgradeErr" style="color:var(--red);font-size:13px;margin-bottom:8px">
              {{ upgradeErr }}
            </div>
            <div class="backup-status">
              <div class="backup-row">
                <span class="backup-label">能力开关</span>
                <span>{{ upgrade?.enabled ? '已开启' : '未开启（upgrade.enabled=false）' }}</span>
              </div>
              <div class="backup-row">
                <span class="backup-label">当前版本</span>
                <span>{{ upgradeRevision(upgrade?.migrations.current) }}</span>
              </div>
              <div class="backup-row">
                <span class="backup-label">代码 head</span>
                <span>{{ upgradeRevision(upgrade?.migrations.heads) }}</span>
              </div>
              <div class="backup-row">
                <span class="backup-label">版本状态</span>
                <span>
                  <template v-if="upgrade?.migrations.available === false">
                    读取失败：{{ upgrade?.migrations.error || '未知原因' }}
                  </template>
                  <template v-else-if="upgrade?.migrations.at_head">已在 head</template>
                  <template v-else>
                    待执行 {{ upgrade?.migrations.pending_count ?? 0 }} 条迁移
                  </template>
                  <span v-if="upgrade?.migrations.single_head === false" style="color:var(--red)">
                    （检测到多个 head，请先合并迁移）
                  </span>
                </span>
              </div>
              <div class="backup-row">
                <span class="backup-label">最近成功备份</span>
                <span>
                  {{ backupLine(upgrade?.backup.latest) }}
                  <template v-if="upgrade?.backup.age_hours != null">
                    （{{ upgrade?.backup.age_hours }} 小时前）
                  </template>
                  <span v-if="upgrade?.backup.required" style="color:var(--red)"> · 需先备份</span>
                </span>
              </div>
              <div class="backup-row">
                <span class="backup-label">磁盘</span>
                <span>
                  剩余 {{ upgrade?.disk.free_mb ?? '—' }} MB / 保护阈值
                  {{ upgrade?.disk.min_free_mb ?? '—' }} MB
                  <span v-if="upgrade && !upgrade.disk.ok" style="color:var(--red)"> · 不足</span>
                </span>
              </div>
            </div>
            <div v-if="upgrade?.migrations.pending.length" class="backup-status">
              <div class="backup-row">
                <span class="backup-label">待执行迁移</span>
                <span>
                  {{ upgrade.migrations.pending
                    .slice(0, 10)
                    .map((p) => `${p.revision}${p.doc ? ' ' + p.doc : ''}`)
                    .join('；') }}
                </span>
              </div>
            </div>
            <div class="cfg-test">
              <button class="btn btn-sm" @click="loadUpgradeCheck" :disabled="upgradeBusy">
                刷新检查
              </button>
              <button
                class="btn btn-sm" @click="prepareUpgradeBackup"
                :disabled="upgradeBusy || upgrade?.enabled !== true"
              >
                {{ upgradeBusy ? '备份中…' : '执行升级前备份' }}
              </button>
              <span v-if="upgrade?.enabled !== true" class="cfg-hint">
                未启用（config.yaml upgrade.enabled=false）
              </span>
              <span v-if="upgradeMsg" style="color:var(--green);font-size:13px">{{ upgradeMsg }}</span>
            </div>

            <h3 class="cfg-h">升级步骤</h3>
            <ol class="upgrade-list">
              <li v-for="(s, i) in upgrade?.steps || []" :key="i">{{ s }}</li>
            </ol>
            <h3 class="cfg-h">回滚与恢复</h3>
            <ul class="upgrade-list">
              <li v-for="(s, i) in upgrade?.rollback || []" :key="i">{{ s }}</li>
            </ul>
          </section>

          <div class="cfg-foot">
            <button @click="save" class="btn btn-primary">保存</button>
            <span v-if="saved" style="color:var(--green);font-size:13px">已保存</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.cfg-card { padding: 24px; max-width: 900px; }
.cfg-layout { display: flex; gap: 20px; align-items: flex-start; }
.cfg-nav {
  width: 150px;
  flex: none;
  display: grid;
  gap: 4px;
  position: sticky;
  top: 12px;
}
.cfg-nav-item {
  padding: 8px 12px;
  border-radius: 8px;
  border: 1px solid transparent;
  background: transparent;
  color: var(--ink-soft);
  font-size: 13px;
  text-align: left;
  cursor: pointer;
}
.cfg-nav-item:hover { background: var(--hover-bg); }
.cfg-nav-item.active {
  background: var(--accent-light);
  color: var(--accent);
  font-weight: 600;
  box-shadow: inset 2px 0 0 var(--accent);
}
.cfg-pane { flex: 1; min-width: 0; }
.upgrade-list {
  margin: 0 0 8px;
  padding-left: 20px;
  color: var(--ink-soft);
  font-size: 13px;
  line-height: 1.9;
}
.cfg-h { margin: 18px 0 4px; font-size: 14px; }
.cfg-h:first-child { margin-top: 0; }
.cfg-desc { margin: 0 0 12px; font-size: 12px; color: var(--ink-soft); }
.cfg-hint { margin: 4px 0 0; font-size: 12px; color: var(--ink-soft); }
.cfg-test { display: flex; align-items: center; gap: 10px; margin: 4px 0 8px; }
.cfg-check {
  display: flex;
  align-items: center;
  gap: 8px;
  cursor: pointer;
}
.cfg-check input { width: auto; }
.backup-status {
  display: grid;
  gap: 6px;
  padding: 12px 14px;
  margin-bottom: 12px;
  border: 1px solid var(--line);
  border-radius: 8px;
  background: var(--hover-bg);
  font-size: 13px;
}
.backup-row { display: flex; gap: 12px; }
.backup-label { flex: none; width: 88px; color: var(--ink-soft); }
.cfg-foot {
  display: flex;
  align-items: center;
  gap: 8px;
  margin-top: 16px;
  padding-top: 14px;
  border-top: 1px solid var(--line);
}
@media (max-width: 1100px) {
  /* 侧栏抽屉化后内容区变宽，但 125% 缩放机器实际可视宽仍偏窄：导航改顶部横排 */
  .cfg-layout { flex-direction: column; gap: 12px; }
  .cfg-nav { width: 100%; display: flex; flex-wrap: wrap; position: static; gap: 6px; }
  .cfg-pane { width: 100%; }
  .cfg-card { padding: 16px; }
}
@media (max-height: 820px) {
  .cfg-card { padding: 16px 18px; }
  .cfg-h { margin-top: 12px; }
}
@media (max-width: 720px) {
  .cfg-layout { flex-direction: column; }
  .cfg-nav { width: 100%; display: flex; flex-wrap: wrap; position: static; }
}
</style>
