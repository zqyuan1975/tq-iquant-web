<#
.SYNOPSIS
  0021 G5 资金池灰度前置检查（只读，不启停会话、不改配置、不写业务表）。

.DESCRIPTION
  按运维手册 docs/ops/capital_pool_g5_runbook.md 的「每日/推进前检查」执行：
  1) 生产库 revision 是否等于预期 head、PRAGMA quick_check 是否 ok；
  2) G1–G4 新表/新列是否齐全；
  3) 三个实盘会话的状态与两个会话级开关（只读展示，运行中仅提示）；
  4) 现金出口台账完整性（金额非负、状态合法、幂等键唯一、引用有效）；
  5) 拼盘健康状态分布；
  6) config.yaml 四个资金池开关的实际值（只读，不修改）；
  7) main/data/backups 最近备份及其 revision；
  8) 可选：对运行中的服务做只读 GET 探活（-ApiBase）。

  任一项 FAIL → 退出码 1；WARN 不影响退出码（-Strict 时也视为失败）。

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File docs/ops/capital_pool_g5_preflight.ps1
  powershell -ExecutionPolicy Bypass -File docs/ops/capital_pool_g5_preflight.ps1 -ApiBase http://127.0.0.1:8001

.NOTES
  - 本脚本不调用 /start、/stop，不写 config.yaml，不执行 alembic upgrade/downgrade。
  - 迁移回滚演练请用 docs/ops/capital_pool_rollback_drill.ps1（F6）。
#>
param(
    [string]$AppRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$Python = "main\.venv\Scripts\python.exe",
    [string]$DbRel = "main\data\dev.db",
    [string]$ExpectedRevision = "g4a5b6c7d8e9",
    [string]$ApiBase = "",
    [switch]$Strict
)

$ErrorActionPreference = "Stop"

$pythonPath = Join-Path $AppRoot $Python
$dbPath = Join-Path $AppRoot $DbRel
$configPath = Join-Path $AppRoot "config.yaml"
$backupDir = Join-Path $AppRoot "main\data\backups"
$stamp = [Guid]::NewGuid().ToString("N")
$logDb = Join-Path $env:TEMP ("capital_g5_db_$stamp.log")
$logCfg = Join-Path $env:TEMP ("capital_g5_cfg_$stamp.log")

Write-Host "== 0021 G5 资金池灰度前置检查（只读）=="
Write-Host "AppRoot   : $AppRoot"
Write-Host "生产库    : $dbPath"
Write-Host "预期 head : $ExpectedRevision"
Write-Host ""

$failed = $false
$warned = $false

if (-not (Test-Path $pythonPath)) { Write-Host "[FAIL] 找不到 Python：$pythonPath"; exit 1 }
if (-not (Test-Path $dbPath)) { Write-Host "[FAIL] 找不到数据库：$dbPath"; exit 1 }
if ((Get-Item $dbPath).Length -le 0) { Write-Host "[FAIL] 数据库为 0 字节：$dbPath"; exit 1 }

$pyDb = @'
import sqlite3, sys, os

db_path, expected_rev, backup_dir, out_path = sys.argv[1:5]
fails, warns, logs = [], [], []

def emit(msg):
    logs.append(msg)

def pass_(msg): emit("[PASS] " + msg)
def warn(msg):
    warns.append(msg); emit("[WARN] " + msg)
def fail(msg):
    fails.append(msg); emit("[FAIL] " + msg)

c = sqlite3.connect("file:%s?mode=ro" % db_path.replace("\\", "/"), uri=True)
try:
    rev = (c.execute("select version_num from alembic_version").fetchone() or [None])[0]
    if rev == expected_rev:
        pass_("alembic revision = %s" % rev)
    else:
        fail("alembic revision = %s，预期 %s" % (rev, expected_rev))

    qc = c.execute("pragma quick_check").fetchone()[0]
    if qc == "ok":
        pass_("PRAGMA quick_check = ok")
    else:
        fail("PRAGMA quick_check = %s" % qc)

    for t in ("capital_pairings", "capital_pairing_plans",
              "capital_pairing_health_daily", "cash_outlet_ops",
              "live_capital_shadow", "live_sessions"):
        row = c.execute("select name from sqlite_master where type='table' and name=?",
                        (t,)).fetchone()
        if row:
            pass_("表存在：%s" % t)
        else:
            fail("缺少表：%s" % t)

    cols = {r[1] for r in c.execute("pragma table_info(live_sessions)")}
    for col in ("capital_enforce_enabled", "cash_outlet_mode",
                "pairing_plan_id", "backtest_run_group_id"):
        if col in cols:
            pass_("live_sessions.%s 存在" % col)
        else:
            fail("live_sessions 缺少列：%s" % col)

    ops_cols = {r[1] for r in c.execute("pragma table_info(cash_outlet_ops)")}
    for col in ("amount", "yield_amount", "shortfall_amount",
                "idempotency_key", "status"):
        if col in ops_cols:
            pass_("cash_outlet_ops.%s 存在" % col)
        else:
            fail("cash_outlet_ops 缺少列：%s" % col)

    rows = c.execute(
        "select id, name, status, capital_enforce_enabled, cash_outlet_mode, "
        "pairing_plan_id from live_sessions order by id").fetchall()
    emit("[INFO] 会话：id/name/status/enforce/outlet_mode/pairing_plan_id")
    running = 0
    enforced = 0
    for r in rows:
        emit("       %s | %s | %s | %s | %s | %s" % r)
        if str(r[2]).lower() == "running":
            running += 1
        if r[3]:
            enforced += 1
    if running:
        warn("%d 个会话处于 running；本脚本不启停会话，变更开关前先确认归属" % running)
    else:
        pass_("全部会话均非 running（%d 个）" % len(rows))
    if enforced:
        warn("%d 个会话已开启资金强制（capital_enforce_enabled=1），请核对是否在灰度名单内"
             % enforced)
    else:
        pass_("无会话开启资金强制（灰度起点正确）")

    statuses = dict(c.execute(
        "select status, count(*) from cash_outlet_ops group by status").fetchall())
    emit("[INFO] 出口台账状态分布：%s" % (statuses or "空"))
    bad_status = set(statuses) - {"PENDING", "EXECUTED", "RETURNED",
                                  "REJECTED", "CANCELLED"}
    if bad_status:
        fail("台账存在非法状态：%s" % sorted(bad_status))
    else:
        pass_("台账状态合法")

    bad_amt = c.execute(
        "select count(*) from cash_outlet_ops where amount < 0 "
        "or yield_amount < 0 or shortfall_amount < 0").fetchone()[0]
    if bad_amt:
        fail("台账存在负金额记录：%d 条" % bad_amt)
    else:
        pass_("台账金额均非负")

    dup = c.execute(
        "select count(*) from (select idempotency_key from cash_outlet_ops "
        "where idempotency_key is not null group by idempotency_key "
        "having count(*) > 1)").fetchone()[0]
    if dup:
        fail("幂等键重复：%d 组" % dup)
    else:
        pass_("幂等键无重复")

    orphan = c.execute(
        "select count(*) from cash_outlet_ops o left join live_sessions s "
        "on s.id = o.live_session_id where o.live_session_id is not null "
        "and s.id is null").fetchone()[0]
    if orphan:
        fail("台账引用了不存在的会话：%d 条" % orphan)
    else:
        pass_("台账会话引用有效")

    has_outlet = c.execute(
        "select count(*) from sqlite_master where type='table' and name='cash_outlets'"
    ).fetchone()[0]
    if has_outlet:
        orphan2 = c.execute(
            "select count(*) from cash_outlet_ops o left join cash_outlets t "
            "on t.id = o.outlet_id where t.id is null").fetchone()[0]
        if orphan2:
            fail("台账引用了不存在的出口：%d 条" % orphan2)
        else:
            pass_("台账出口引用有效")

    try:
        plans = dict(c.execute(
            "select coalesce(status,'(null)'), count(*) from capital_pairing_plans "
            "group by status").fetchall())
        emit("[INFO] 拼盘状态分布：%s" % plans)
        bad_plan = set(plans) - {"draft", "evaluated", "approved", "rejected",
                                 "ACTIVE", "WATCH", "SUSPENDED", "RETIRED",
                                 "(null)"}
        if bad_plan:
            warn("拼盘存在未识别状态：%s" % sorted(bad_plan))
        else:
            pass_("拼盘状态可识别")
    except Exception as exc:
        warn("读取拼盘状态失败：%s" % exc)
finally:
    c.close()

if os.path.isdir(backup_dir):
    files = sorted((os.path.join(backup_dir, f) for f in os.listdir(backup_dir)
                    if f.lower().endswith(".db")),
                   key=os.path.getmtime, reverse=True)[:3]
    if not files:
        warn("备份目录无 .db 备份：%s" % backup_dir)
    for f in files:
        try:
            b = sqlite3.connect("file:%s?mode=ro" % f.replace("\\", "/"), uri=True)
            brev = (b.execute("select version_num from alembic_version").fetchone()
                    or ["(none)"])[0]
            b.close()
            emit("[INFO] 备份 %s | %.0f MB | revision=%s"
                 % (os.path.basename(f), os.path.getsize(f) / 1048576, brev))
        except Exception as exc:
            warn("备份不可读：%s（%s）" % (os.path.basename(f), exc))
else:
    warn("备份目录不存在：%s" % backup_dir)

emit("")
if fails:
    emit("检查失败：%d 项 FAIL，%d 项 WARN" % (len(fails), len(warns)))
else:
    emit("检查通过：0 项 FAIL，%d 项 WARN" % len(warns))

open(out_path, "w", encoding="utf-8").write("\n".join(logs) + "\n")
sys.exit(1 if fails else 0)
'@

Write-Host "[1/2] 数据库与台账检查"
$tmpPyDb = Join-Path $env:TEMP ("capital_g5_db_$stamp.py")
Set-Content -Path $tmpPyDb -Value $pyDb -Encoding UTF8
& $pythonPath $tmpPyDb $dbPath $ExpectedRevision $backupDir $logDb
$dbExit = $LASTEXITCODE
if (Test-Path $logDb) {
    $dbLines = Get-Content -Path $logDb -Encoding UTF8
    $dbLines | ForEach-Object { Write-Host $_ }
    if ($dbLines -match '\[WARN\]') { $warned = $true }
}
if ($dbExit -ne 0) { $failed = $true }

Write-Host ""
Write-Host "[2/2] 配置开关（只读）"
$pyCfg = @'
import sys

out_path = sys.argv[2]
logs = []
try:
    import yaml
except Exception as exc:
    logs.append("[WARN] 无法导入 yaml：%s" % exc)
    open(out_path, "w", encoding="utf-8").write("\n".join(logs) + "\n")
    sys.exit(0)

cfg = yaml.safe_load(open(sys.argv[1], encoding="utf-8")) or {}
pool = cfg.get("capital_pool") or {}
for k in ("enabled", "pairing_enabled", "loan_model_enabled", "cash_outlet_enabled"):
    logs.append("[INFO] capital_pool.%s = %s" % (k, pool.get(k, "(缺省)")))
if not pool.get("cash_outlet_enabled"):
    logs.append("[INFO] 现金出口接口当前返回 enabled=false（灰度未开启）")
if not pool.get("enabled"):
    logs.append("[INFO] 资金池总开关关闭：推导/健康接口返回 enabled=false")
if pool.get("enabled") and not pool.get("loan_model_enabled"):
    logs.append("[WARN] 总开关已开但 loan_model_enabled=false：回测/实盘借据不生效")
open(out_path, "w", encoding="utf-8").write("\n".join(logs) + "\n")
'@
$tmpPyCfg = Join-Path $env:TEMP ("capital_g5_cfg_$stamp.py")
Set-Content -Path $tmpPyCfg -Value $pyCfg -Encoding UTF8
& $pythonPath $tmpPyCfg $configPath $logCfg
$cfgExit = $LASTEXITCODE
if (Test-Path $logCfg) {
    $cfgLines = Get-Content -Path $logCfg -Encoding UTF8
    $cfgLines | ForEach-Object { Write-Host $_ }
    if ($cfgLines -match '\[WARN\]') { $warned = $true }
}
if ($cfgExit -ne 0) { $failed = $true }

if ($ApiBase) {
    Write-Host ""
    Write-Host "[可选] 只读接口探活：$ApiBase"
    $probes = @(
        "/api/capital/outlet-ops?limit=1",
        "/api/capital/outlet-ops/reconcile",
        "/api/pairing/health"
    )
    foreach ($p in $probes) {
        try {
            $resp = Invoke-RestMethod -Uri ($ApiBase.TrimEnd("/") + $p) -Method Get -TimeoutSec 10
            if ($resp.code -eq 0) { Write-Host "[PASS] GET $p" }
            else { Write-Host "[WARN] GET $p 返回 code=$($resp.code)：$($resp.message)" }
        } catch {
            Write-Host "[WARN] GET $p 失败：$($_.Exception.Message)"
        }
    }
}

Remove-Item -Force $logDb, $logCfg, $tmpPyDb, $tmpPyCfg -ErrorAction SilentlyContinue

if ($Strict -and $warned) { $failed = $true }

Write-Host ""
if ($failed) {
    if ($Strict -and $warned) {
        Write-Host "前置检查失败：存在 WARN 且指定了 -Strict。" -ForegroundColor Red
    } else {
        Write-Host "前置检查失败：先按上方 FAIL 修复，再进入灰度。" -ForegroundColor Red
    }
    exit 1
}
Write-Host "前置检查完成：无 FAIL。灰度推进仍需按手册逐档确认（见 docs/ops/capital_pool_g5_runbook.md）。"
exit 0
