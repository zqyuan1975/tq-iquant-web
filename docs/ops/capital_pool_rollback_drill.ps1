<#
.SYNOPSIS
  0021 F6 资金池备份 / 回滚演练。

.DESCRIPTION
  不新增业务代码，也不修改生产库内容：
  1) 用 sqlite3 在线备份 API 把 main/data/dev.db 复制到 main/data/backups/（读源库，不写源库）；
  2) 在 %TEMP% 下复制备份与 config.yaml，设置 TQ_APP_ROOT 指向临时目录；
  3) 在临时库上执行 alembic current → downgrade <目标版本> → upgrade head，
     验证 F1/F3 迁移可回退、可重放；
  4) 默认只演练，不动生产库；-Execute 也只在临时副本上做 downgrade/upgrade。

.PARAMETER TargetRevision
  回滚目标版本，默认 v8a9b0c1d2e3（F1 之前），会同时回退 F1 两列与 F3 影子表。

.PARAMETER DryRun
  仅打印步骤，不复制、不执行 alembic。

.EXAMPLE
  powershell -ExecutionPolicy Bypass -File docs/ops/capital_pool_rollback_drill.ps1
  powershell -ExecutionPolicy Bypass -File docs/ops/capital_pool_rollback_drill.ps1 -Execute

.NOTES
  - 生产服务运行中也可以跑：备份走 sqlite backup，升级/回退只发生在临时副本。
  - 演练失败会以非 0 退出并保留临时目录，便于排查。
#>
param(
    [string]$AppRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
    [string]$TargetRevision = "v8a9b0c1d2e3",
    [string]$Python = "main\.venv\Scripts\python.exe",
    # 源码运行时 sqlite_path 锚 main/（core/db.py），真实库在 main/data/dev.db；
    # 根目录 data/dev.db 可能是历史残留空文件，不能当生产库。
    [string]$DbRel = "main\data\dev.db",
    [string]$BackupRel = "main\data\backups",
    [switch]$Execute,
    [switch]$DryRun,
    [switch]$KeepTemp
)

$ErrorActionPreference = "Stop"

if ($DryRun -and $Execute) {
    throw "-DryRun 与 -Execute 不能同时使用"
}
$mode = if ($Execute) { "执行" } else { "演练（仅检查）" }

$pythonPath = Join-Path $AppRoot $Python
$dbPath = Join-Path $AppRoot $DbRel
$backupDir = Join-Path $AppRoot $BackupRel
$configPath = Join-Path $AppRoot "config.yaml"
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$backupPath = Join-Path $backupDir ("dev_{0}.db" -f $stamp)

Write-Host "== 0021 F6 资金池备份/回滚 $mode =="
Write-Host "AppRoot     : $AppRoot"
Write-Host "Python      : $pythonPath"
Write-Host "生产库      : $dbPath"
Write-Host "备份目标    : $backupPath"
Write-Host "回滚目标    : $TargetRevision"
Write-Host ""

if (-not (Test-Path $pythonPath)) { throw "找不到 Python：$pythonPath" }
if (-not (Test-Path $configPath)) { throw "找不到 config.yaml：$configPath" }
if (-not (Test-Path $dbPath)) {
    # 兼容打包/便携布局：data/dev.db 直接在应用根下
    $alt = Join-Path $AppRoot "data\dev.db"
    if ((Test-Path $alt) -and ((Get-Item $alt).Length -gt 0)) {
        $dbPath = $alt
        Write-Host "      默认库不存在，改用：$dbPath"
    } else {
        throw "找不到数据库：$dbPath（可先用 -DbRel 指定）"
    }
}
$dbSize = (Get-Item $dbPath).Length
if ($dbSize -le 0) { throw "数据库为空文件（0 字节）：$dbPath；真实库通常在 main\data\dev.db" }

Write-Host "[1/5] 检查迁移入口"
Push-Location (Join-Path $AppRoot "main")
try {
    & $pythonPath -m alembic --help | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "alembic 不可用（退出码 $LASTEXITCODE）" }
} finally {
    Pop-Location
}

if ($DryRun -or -not $Execute) {
    Write-Host "[2/5] 跳过备份（演练模式）"
    Write-Host "[3/5] 跳过临时副本（演练模式）"
    Write-Host "[4/5] 跳过 downgrade/upgrade（演练模式）"
    Write-Host "[5/5] 跳过清理（演练模式）"
    Write-Host ""
    Write-Host "演练通过：命令与路径检查正常。加 -Execute 才会真正备份并在临时副本上回滚。"
    exit 0
}

Write-Host "[2/5] 在线备份生产库（sqlite backup，不写源库）"
New-Item -ItemType Directory -Path $backupDir -Force | Out-Null
& $pythonPath -c "import sqlite3,sys; s=sqlite3.connect(sys.argv[1]); d=sqlite3.connect(sys.argv[2]); s.backup(d); d.close(); s.close()" $dbPath $backupPath
if ($LASTEXITCODE -ne 0) { throw "备份失败（退出码 $LASTEXITCODE）" }
# 备份副本必须带 alembic_version，否则说明源库选错（如根目录空库），后续 downgrade 会不可达
$backupRev = & $pythonPath -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); print(c.execute('select version_num from alembic_version').fetchone()[0]); c.close()" $backupPath
if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($backupRev)) {
    throw "备份副本缺少 alembic_version：$backupPath（请用 -DbRel 指定真实库）"
}
Write-Host "      备份完成：$backupPath（revision=$backupRev）"

Write-Host "[3/5] 准备临时副本"
$tempRoot = Join-Path $env:TEMP ("capital_rollback_" + $stamp)
New-Item -ItemType Directory -Path $tempRoot | Out-Null
New-Item -ItemType Directory -Path (Join-Path $tempRoot "data") | Out-Null
Copy-Item $configPath $tempRoot
Copy-Item $backupPath (Join-Path $tempRoot "data\dev.db")
Write-Host "      临时根目录：$tempRoot"

$oldAppRoot = $env:TQ_APP_ROOT
$failed = $false
Write-Host "[4/5] 在临时库上演练：current → downgrade → current → upgrade head → current"
Push-Location (Join-Path $AppRoot "main")
try {
    $env:TQ_APP_ROOT = $tempRoot

    & $pythonPath -m alembic current
    if ($LASTEXITCODE -ne 0) { throw "alembic current 失败（$LASTEXITCODE）" }

    & $pythonPath -m alembic downgrade $TargetRevision
    if ($LASTEXITCODE -ne 0) { throw "downgrade $TargetRevision 失败（$LASTEXITCODE）" }

    & $pythonPath -m alembic current
    if ($LASTEXITCODE -ne 0) { throw "downgrade 后 current 失败（$LASTEXITCODE）" }

    & $pythonPath -m alembic upgrade head
    if ($LASTEXITCODE -ne 0) { throw "upgrade head 失败（$LASTEXITCODE）" }

    & $pythonPath -m alembic current
    if ($LASTEXITCODE -ne 0) { throw "upgrade 后 current 失败（$LASTEXITCODE）" }
} catch {
    $failed = $true
    Write-Host "演练失败：$_" -ForegroundColor Red
    throw
} finally {
    Pop-Location
    $env:TQ_APP_ROOT = $oldAppRoot
    Write-Host "[5/5] 清理"
    if ($failed -or $KeepTemp) {
        Write-Host "      保留临时目录：$tempRoot"
    } else {
        Remove-Item -Recurse -Force $tempRoot
        Write-Host "      临时目录已删除"
    }
}

Write-Host ""
Write-Host "F6 演练通过：临时库 downgrade → upgrade 成功，生产库未被修改。"
Write-Host "备份保留在：$backupPath"
Write-Host "回滚生产（如需）：停会话后执行 alembic downgrade $TargetRevision，再启动由 init_db 升级；本脚本不会自动回滚生产。"
