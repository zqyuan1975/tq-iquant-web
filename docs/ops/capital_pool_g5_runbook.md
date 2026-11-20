# 0021 G5 资金池灰度与运维手册

- 适用：0021 资金池 G1–G4 已上线后的灰度推进、日常对账与应急回退
- 基线：[0021-资金池实施任务计划](../plans/0021-资金池实施任务计划.md)（G5 段落）
- 配套脚本：
  - 前置检查：[capital_pool_g5_preflight.ps1](capital_pool_g5_preflight.ps1)（只读）
  - 备份/回滚演练：[capital_pool_rollback_drill.ps1](capital_pool_rollback_drill.ps1)（F6，默认只演练）
- 硬约束：
  1. 灰度只动「会话级」两个开关，不改 `config.yaml`，不启停实盘会话；
  2. 任何阶段都不得用 `/stop` 充当 kill switch（会跳过当日日终与收盘清扫）；
  3. 出口开关 P1/P2/P3 的推进必须逐档人工确认，禁止跳档；
  4. 迁移只加不删；回滚演练只在临时副本执行。

---

## 0. 当前状态快照（2026-11-20）

| 项 | 实际值 | 说明 |
|---|---|---|
| 生产库 revision | `g4a5b6c7d8e9` | G1–G4 迁移全部就位，`PRAGMA quick_check=ok` |
| `capital_pool.enabled` | `true` | 由另一条线在 `config.yaml` 打开；本手册不代改 |
| `capital_pool.cash_outlet_enabled` | `true` | 同上；接口可读、可登记台账 |
| `capital_pool.loan_model_enabled` | `false` | 回测/实盘借据尚未在全局开启 |
| `capital_pool.pairing_enabled` | `false` | 拼盘评估开关 |
| 实盘会话 | 3 个，均 `stopped` | 未启停 |
| `capital_enforce_enabled` | 全部 `0` | 灰度起点：强制未开 |
| `cash_outlet_mode` | 全部 `OFF` | 灰度起点：出口仅拒绝登记 |
| 出口台账 | 0 行 | 无在途、无收益、无短款 |
| 当前状态备份 | `main/data/backups/dev_20261120_105015_g4head.db` | revision 校验一致 |

> 注意：全局两个开关已打开，但「会话级强制」与「出口分级」默认仍是关闭/`OFF`。
> 灰度推进只改会话行，不改全局配置；全局配置的变更走配置评审，不属本手册操作范围。

---

## 1. 灰度四档

推进顺序固定：**S0 观察 → S1 影子 → S2 P1 人工 → S3 P2 半自动 → S4 P3 自动**。
每档至少 5 个交易日，且每日对账全绿才可进入下一档。

### S0 观察（当前）

- 会话：`capital_enforce_enabled=0`、`cash_outlet_mode=OFF`。
- 动作：只读查看健康与对账；不登记出口。
- 通过标准：连续 5 个交易日 `/api/capital/outlet-ops/reconcile` 无 `overdue/shortfalls/anomalies`，`/api/pairing/health` 全部 `ACTIVE`。

### S1 小资金影子

- 前置：目标会话已绑定已批准的拼盘（`pairing_plan_id`），资金为最小可用规模。
- 开关：保持 `capital_enforce_enabled=0`。
- 动作：观察 `live_capital_shadow` 与「理论 vs 实盘」成员对照（F3/F4），确认额度/借据口径与回测一致。
- 通过标准：影子记录逐日生成、无 `pairing_plan_missing`/`member_live_missing`；理论 vs 实盘偏差方向可解释。
- 回退：解绑拼盘或保持开关不动，零交易影响。

### S2 小资金强制（P1 人工）

- 开关（会话级，立即生效）：

```powershell
# 只改会话行，不启停会话
$body = @{ capital_enforce_enabled = $true; cash_outlet_mode = "P1" } | ConvertTo-Json
Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:8001/api/live/sessions/<id>" `
  -ContentType "application/json" -Body $body
```

- 动作：每笔出口先在「现金出口」页登记，核对 C1–C8 判定与单日限额后人工确认；回流逐笔登记本金/收益，短款必须写原因。
- 通过标准（连续 5 日）：
  - 无超额委托（`capital_quota_shrink` 可解释，`capital_quota_exhausted` 未集中出现）；
  - 对账阻断事件 `capital_reconcile/identity_block` 为 0；
  - 健康 `ACTIVE`，无 `SUSPENDED`；
  - 台账 `shortfall_total=0`，收益只出现在 `yield_contribution`，策略归因无变化。
- 回退：把 `capital_enforce_enabled` 置回 `false`、`cash_outlet_mode=OFF`（当根 bar 生效，借据只回收不新增）。

### S3 半自动（P2）

- 开关：同一会话 `cash_outlet_mode="P2"`。
- 动作：服务生成计划，人工确认后执行；至少覆盖 3 笔不同出口。
- 通过标准：计划→确认→回流全链路可追溯；确认人、时间、幂等键完整；无重复登记。
- 回退：`cash_outlet_mode="P1"`（未确认的计划可驳回，已投放的正常回流）。

### S4 自动（P3）

- 入口条件（全部满足）：
  1. S3 连续 5 日通过；
  2. 目标出口已完成 `verify` 且在有效期内；
  3. 出口满足 R1/R2、T+0/T+1 或期限 ≤7 天、回流确定性非 `probable`；
  4. 单日上限先设为最小档（建议 ≤1 万元），且不超过出口 `max_daily_amount`。
- 开关：`cash_outlet_mode="P3"`；P3 必须绑定实盘会话（否则接口 422）。
- 观察：`auto:p3` 审计记录、`cash_outlet/mode_change` 风险事件、每日对账。
- 立即回退条件（任一命中即降回 P2 或 OFF）：
  - 任一笔短款（`shortfall_amount>0`）；
  - 逾期未回 `overdue` 非空；
  - 单日累计触顶拒绝；
  - 健康 `SUSPENDED` 或对账阻断。

> 资金规模、灰度会话归属由运维指定；本手册只给开关动作与判定标准。

---

## 2. Kill switch（应急）

按顺序执行，**不要启停会话**：

1. **会话级（第一优先，立即生效）**

```powershell
$body = @{ capital_enforce_enabled = $false; cash_outlet_mode = "OFF" } | ConvertTo-Json
Invoke-RestMethod -Method Put -Uri "http://127.0.0.1:8001/api/live/sessions/<id>" `
  -ContentType "application/json" -Body $body
```

- 关闭强制：当根 bar 起回到原资金口径；既有借据只回收、不新增。
- `cash_outlet_mode=OFF`：拒绝新登记；已投放台账仍可正常登记回流与对账。

2. **全局（次优先，需重启读取，影响所有会话）**

- `config.yaml → capital_pool.cash_outlet_enabled=false`：关闭出口接口（返回 `enabled=false`）。
- `config.yaml → capital_pool.enabled=false`：关闭推导/健康/拼盘接口。
- 本手册不代改配置；变更需走配置评审并记录。

3. **禁止项**：不得用 `/api/live/sessions/{id}/stop` 做 kill switch；停会话会跳过当日日终/收盘清扫，且不属于“资金池开关”语义。

---

## 3. 日常对账（每交易日收盘后）

### 3.1 接口

| 检查 | 请求 | 通过标准 |
|---|---|---|
| 出口对账 | `GET /api/capital/outlet-ops/reconcile` | `overdue`、`shortfalls`、`anomalies` 均为空 |
| 台账汇总 | `GET /api/capital/outlet-ops` | `summary.shortfall_total=0`；`yield_contribution` 单列；在途与本日计划一致 |
| 拼盘健康 | `GET /api/pairing/health` | 灰度拼盘 `ACTIVE`；无新增 `SUSPENDED/RETIRED` |
| 会话开关 | `GET /api/live/sessions/{id}` | `capital_enforce_enabled`、`cash_outlet_mode` 与灰度档位一致 |
| 理论 vs 实盘 | `GET /api/live/sessions/{id}/theory-vs-live` | 成员对照齐全，偏差可解释 |

### 3.2 SQL（只读，`main/data/dev.db`）

```sql
-- 在途与本日计划
SELECT status, COUNT(*), SUM(amount) FROM cash_outlet_ops GROUP BY status;
SELECT outlet_id, SUM(amount) FROM cash_outlet_ops
 WHERE executed_at LIKE date('now') || '%' AND status IN ('PENDING','EXECUTED')
 GROUP BY outlet_id;
-- 短款与收益（收益只允许出现在 yield_amount，不得混入本金）
SELECT id, outlet_id, amount, principal_returned, yield_amount, shortfall_amount, shortfall_reason
  FROM cash_outlet_ops WHERE shortfall_amount > 0 OR yield_amount > 0;
-- 逾期未回
SELECT id, outlet_id, executed_at, expected_return_date FROM cash_outlet_ops
 WHERE status = 'EXECUTED' AND expected_return_date < date('now');
-- 会话开关
SELECT id, name, status, capital_enforce_enabled, cash_outlet_mode FROM live_sessions;
```

### 3.3 恒等式口径

- 引擎侧 (1)–(10) 中，当前可实算 (2)(4)(5)(6)(8)(10)；(1)(3)(7)(9) 因缺持久化数据源暂 `skipped`，不得当作“账平”。
- 阻断项 (2)(3)(4)(6) 任一失败：下一个买入审批返回 0，并写 `capital_reconcile/identity_block` 风险事件；对账恢复后自动解除。
- `skipped` 项需在日报中单列，不能省略。

---

## 4. 备份、迁移与回滚

### 4.1 变更前备份（在线，不写源库）

```powershell
$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
New-Item -ItemType Directory -Path main\data\backups -Force | Out-Null
main\.venv\Scripts\python.exe -c "import sqlite3,sys; s=sqlite3.connect(sys.argv[1]); d=sqlite3.connect(sys.argv[2]); s.backup(d); d.close(); s.close()" main\data\dev.db "main\data\backups\dev_$stamp.db"
main\.venv\Scripts\python.exe -c "import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); print(c.execute('select version_num from alembic_version').fetchone()[0])" "main\data\backups\dev_$stamp.db"
```

备份必须能读出 `alembic_version`；读不出说明选错了库（根目录 `data/dev.db` 可能是 0 字节残留）。

### 4.2 回滚路径

| 范围 | 命令（在 `main/` 下） | 说明 |
|---|---|---|
| 只退 G4 | `alembic downgrade z2e3f4a5b6c7` | 删 `cash_outlet_ops`、`live_sessions.cash_outlet_mode` |
| 退 G3+G4 | `alembic downgrade y1d2e3f4a5b6` | 再删健康日表 |
| 退全部 G | `alembic downgrade x0c1d2e3f4a5` | 回到 pre-G，与 `dev_20261120_094234.db` 一致 |

回滚前：先把灰度会话两个开关置回关闭/`OFF`，再停会话（正常停，不用 kill switch），最后执行 downgrade。
回滚后：`alembic current` 核对版本；启动前跑一遍 `capital_pool_g5_preflight.ps1`。

### 4.3 演练（不碰生产库）

```powershell
# 只检查路径与迁移入口
powershell -ExecutionPolicy Bypass -File docs/ops/capital_pool_rollback_drill.ps1
# 在线备份 + 在临时副本上 downgrade/upgrade
powershell -ExecutionPolicy Bypass -File docs/ops/capital_pool_rollback_drill.ps1 -Execute
```

脚本只对临时副本执行迁移，生产库全程只读。

---

## 5. 演练记录

### 5.1 前置检查（2026-11-20）

执行：

```powershell
powershell -ExecutionPolicy Bypass -File docs/ops/capital_pool_g5_preflight.ps1
```

结果摘要（2026-11-20，`-Strict` 下因一条 WARN 返回 1，属预期）：

- revision `g4a5b6c7d8e9`、`quick_check=ok`；
- G1–G4 表/列齐全（`cash_outlet_ops`、`capital_pairing_health_daily`、`live_capital_shadow`、`live_sessions.cash_outlet_mode` 等）；
- 3 个会话（01/02/03）均 `stopped`；`capital_enforce_enabled=0`、`cash_outlet_mode=OFF`；
- 台账 0 行，无负金额、无重复幂等键、无孤儿引用；
- 备份最近两份：`dev_20261120_105015_g4head.db`（revision `g4a5b6c7d8e9`）、`dev_20261120_094234.db`（revision `x0c1d2e3f4a5`）；
- 配置：`enabled=true`、`cash_outlet_enabled=true`、`loan_model_enabled=false`、`pairing_enabled=false`。

末项 WARN（总开关已开但 `loan_model_enabled=false`）是当前真实状态，不是故障：全局开关由配置评审打开，灰度只动会话级字段；`-Strict` 把它当失败，用于“必须连全局都就绪”的发布前检查。

### 5.2 回滚演练（2026-11-20，F6）

`x0c1d2e3f4a5 → w9b0c1d2e3f4 → v8a9b0c1d2e3` 下行成功，再重放至 head 成功；生产库未被修改。

### 5.3 待运维确认

- 灰度会话与资金规模（S1 起）；
- S4 单日上限的具体数值；
- 每档观察期的起止日期与日报模板归属。
