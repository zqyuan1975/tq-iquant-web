# 0021 G6 总验收与回归报告

- 文档编号：0021-G6
- 日期：2026-11-20
- 分支：`fix/model-fixes`（基线 commit `083f45c`，G5 已完成）
- 环境：Windows / Python 3.13.14 / Intel i5-13500 / 16 GB
- 范围：`docs/plans/0021-资金池实施任务计划.md` G6「全量测试、#4 对照、实盘影子对照、性能」
- 约束：未启停任何实盘会话；未打开 P1/P2/P3；未修改 `config.yaml` 全局开关；未写入 `data/` 之外的运行数据。

## 1. 结论摘要

| 验收项 | 口径 | 结果 |
|---|---|---|
| 全量后端测试 | `pytest main/core/tests -q` | 3834 passed / 11 skipped / 285.70 s（修复 3 项失败后） |
| #4 对照 | B7 黄金指纹 + #4 自适应滑点关闭逐位一致 | 通过（3/3） |
| 默认路径回归（单组合） | 旧配置 vs 新字段存在但开关关闭：快照/成交/现金指纹一致 | 通过 |
| 默认路径回归（多组合） | `capital_pool_plan` 未传 vs `enabled=false`：规范哈希一致 | 通过（3/3） |
| 实盘影子对照 | 影子只记录计划 vs 实际、不触碰委托；理论 vs 实盘对照逻辑 | 测试通过；实盘样本缺口见 §6 |
| 资金池相关回归组 | `-k "capital or theory_vs_live or shadow_mode"` | 214 passed |
| 前端构建 | `cd web; npm run build` | 通过（773 modules，788 ms） |
| 性能 | 开关全关默认路径 vs 借贷开启 | 见 §5 |
| 生产库与开关状态 | `main/data/dev.db` | revision `g4a5b6c7d8e9`，quick_check ok，3 会话 stopped，强制 0 / 出口 OFF |

工程验收结论：G1–G6 的代码路径、回归基线与默认路径零变化均有可重复测试支撑；G6 判定 **DONE（工程验收）**。业务 KPI（§7）与实盘影子样本（§6）不满足「可用测试取证」条件，按约束列为缺口，不自行放宽。

## 2. 全量测试

```powershell
main\.venv\Scripts\python.exe -m pytest main/core/tests -q -p no:cacheprovider
```

### 2.1 首轮结果与修复

首轮全量：**3 failed / 3831 passed / 11 skipped / 309.06 s**。三项失败均与资金池默认路径行为无关，按最小改动修复后复跑全绿：

| 失败用例 | 根因 | 修复 | 性质 |
|---|---|---|---|
| `test_params_snapshot_round_trips_source_attributes` | 本机 `config.yaml` 的 `capital_pool.enabled=true`（部署配置，gitignored），A5 按设计多写 `capital_pool` 版本块；用例对键集合做全等断言 | 断言允许「开关开启时多出 `capital_pool`」，其余键仍严格要求 | 测试契约随 A5 更新 |
| `test_bridge_state_transitions_persist_once_each` | `_bare_engine` 用 `LiveEngine.__new__` 绕过 `__init__`，B2 新增的 `_failover_enabled` / `backup_engine` 未初始化 | 测试助手补齐这两个默认值（`False` / `None`） | 测试助手滞后于 B2 |
| `test_failure_log_masks_webhook_and_secret` | `_log_fail` 先写 `_last_fail_log` 再 `logger.warning`，测试等待时间戳后立即读 `caplog`，存在记录未入 handler 的竞态（单独运行/整文件运行均通过，仅全量触发） | 改为先写日志、后置时间戳（发送线程内串行调用，节流语义不变） | 日志竞态 |

修复后复跑：**3834 passed / 11 skipped / 285.70 s**；11 项 skip 为既有用例的环境性跳过（与资金池无关）。

## 3. #4 对照

对应 B7 基线 [test_capital_regression.py](file:///d:/project/tq-iquant-web/main/core/tests/unit/test_capital_regression.py)：

1. `test_switches_off_legacy_vs_new_fields_bit_identical`：同一 40 根日线回放样本，旧配置与「新字段存在但开关关闭」的「快照 + 成交 + 现金」规范化 sha256 指纹一致；新快照不出现 `loan_model_version` / `capital_pool_version` / `entitlement_json`，`loan_model` 为 `None`。
2. `test_issue4_adaptive_slippage_off_matches_legacy`：`slippage_impact_coef` / `volume_limit_*` 字段存在但 `adaptive_slippage_enabled=False`、`volume_limit_enabled=False` 时，成交类型、价格、数量、金额与旧配置逐位一致，成交量不被流动性字段缩减。
3. `test_live_replay_sample_golden_fingerprint`：回放样本默认路径指纹固定为黄金常量 `f081b1e8b2f0c0f3ae6de7f1a52e0634dbe78aa9b2066b4d3233c0474ae86dd9`，任何无意改变默认路径的提交都会在此失败。

实测：3 passed。多组合口径另见 [test_capital_pool_regression.py](file:///d:/project/tq-iquant-web/main/core/tests/unit/test_capital_pool_regression.py)：`capital_pool_plan` 未传与 `enabled=false` 的 trades / snapshots / evaluations / aggregate 哈希一致，且 `capital_pool_borrow` 门控不出现；3 passed。

> 说明：G6 的「#4 对照」以代码级黄金基线为准。§3.3 的利用率/回撤 KPI 需要真实 #4 组合回测样本，见 §3.1。

### 3.1 #4 资金池回测实测（2026-11-20，记录 31）

- 组合与区间：PS3（`04` 同组合），2014-01-01 → 2026-11-19，与既有 #4（`backtest_records` id=4）**区间一致**；新开记录 id=31 `G6-#4-capital-pool-20261120`，`status=completed`（11:42:32 → 11:45:46），3131 个交易日。既有 27 条记录未改动（现共 28 条）。
- 资金池参数：`params_snapshot.capital_pool = {capital_pool_version: capital-pool-a1, quota_algo_version: quota-1.0.0, defaults:{max_holdings:10, min_cash_reserve_pct:0.03, target_utilization:0.95}}`（config `live.capital_pool.enabled=true` 时由 A5 写入）。单组合回测不注入额度分配/借据（需 `loan_model_plan` 两策略同组合或 E2 多组合 `capital_pool_plan`），故该块为版本戳，KPI 反映当前 PS3 参数下的实际曲线。
- 指标（逐日 `market_value/total_value` 均值；`total_value` 峰值回撤）：

| 记录 | 平均利用率 | 最大回撤 | 期末净值 |
|---|---|---|---|
| id=4 `04`（既有 #4） | 24.51% | 6.08% | 38,841,725.55 |
| id=26 `REF-04-default-replay` | 24.51% | 6.08% | 38,841,696.26 |
| **id=31 本轮（开资金池参数）** | **18.62%** | **8.41%** | **84,005,599.17** |

- KPI 对照结论：**未达标**。利用率 18.62% 低于 35%–45% 下限；最大回撤 8.41% 高于 ≤8% 上限（超 0.41pp）。
- 可比性说明（未自行放宽）：区间一致，但 PS3 配置在 id=4 之后已变更——`max_positions` 10 → 20、`single_open_ratio` 10% → 5%、`dividend_mode` back → front、`fee_reserve_enabled` true → false。故 31 与 4/26 只能按区间对照，不能视为同参数 A/B；利用率下降与回撤上升主要来自上述配置差异，不能单独归因于资金池机制。

## 4. 默认路径与 kill switch 回归

- 资金池相关回归组（`capital` / `theory_vs_live` / `shadow_mode`）：**214 passed**，5.06 s。覆盖额度推导、LoanLot、预算闸、门控释放/召回、对账恒等式、健康状态机、强制借贷、影子、现金出口、API 默认关闭语义。
- kill switch：`test_capital_enforce.py` 覆盖「关闭即释放借据、可用现金/真实侧占比恢复原口径、只回收不新增」；`test_capital_reconcile.py` 覆盖 kill switch 关闭时清空阻断态。
- 现金出口默认关闭：API `enabled=false`；`test_cash_outlet_ops.py` 覆盖 OFF 一律拒绝登记。

## 5. 性能

口径：4000 根日线 × 10 只标的 × 双策略组合，三档交错执行（奇数轮换起始顺序）、每档预热 1 次后测 7 次（Python 3.13.14 / i5-13500）。

| 配置档 | 中位数 | 最小值 | 最大值 |
|---|---|---|---|
| `off/legacy`（不注入新字段） | 0.742 s | 0.689 s | 0.814 s |
| `off/new-fields`（新字段存在、开关全关） | 0.707 s | 0.682 s | 0.828 s |
| `on/L2-loan`（L2 借贷开启） | 0.810 s | 0.725 s | 1.016 s |

- 新字段（关闭开关）相对 legacy：中位 **−4.7%**、最小 **−1.1%** → 落在台架噪声内，无可测开销。
- L2 借贷开启相对 legacy：中位 **+9.2%**、最小 **+5.2%**。
- 三种配置成交逐笔一致（`trades identical: True`），各 4000 个快照。台架为三档交错、奇偶轮换顺序、每档 7 次，预热 1 次；本机同时有实盘服务进程在跑，噪声约 ±5%。

结论：

- 默认路径（开关全关）与改动前逐位一致，耗时差在测量噪声范围内；新字段存在本身不引入可测开销。
- 开启 L2 借贷后增加额度推导、预算闸与快照 `entitlement_json` 序列化开销，属于开关开启后的预期成本，不影响默认路径。

## 6. 实盘影子对照

测试层（全部通过）：

- [test_capital_shadow.py](file:///d:/project/tq-iquant-web/main/core/tests/unit/test_capital_shadow.py)：`build_shadow_payload` 产出计划 vs 实际、haircut 后可用本金、gap、召回字段；`orders_unchanged=True`；影子关闭不写库、失败被吞掉不影响交易主链路、非成员组合跳过。
- [test_live_theory_vs_live.py](file:///d:/project/tq-iquant-web/main/core/tests/unit/test_live_theory_vs_live.py)：理论 vs 实盘曲线、指标、未绑定会话的兜底、自动绑定最新组合记录、成员缺失状态。
- `test_live_shadow_mode.py`：影子执行模式不调用真实 `place_order/cancel_order`，不占用实盘会话名额。

数据缺口（未自行放宽）：

- 「计划 vs 实际偏差 < 5%、门控一致 100%」需要实盘影子期逐 bar 数据。当前生产库 `live_capital_shadow` 0 行、`capital_pairing_health_daily` 0 行。
- **F3 会话级影子开关不存在**：`live_service.start_session` 仅在会话绑定 `pairing_plan_id`（或 `backtest_run_group_id` → 组上计划）时才构建 `capital_shadow`；`capital_pool_mode` 取值 off/equal/risk_budget/priority，不是影子开关。生产库 `capital_pairing_plans` / `capital_pairings` / `backtest_run_groups` 均 0 行，故 F3 无法在会话 1 单独开启。用户拍板选 A：先以实盘 auto 启动会话 1、不启 F3，F3 记为待补缺口（待有真实拼盘或运行组再补），不计通过。
- 补齐方式：按 G5 手册 S1「小资金影子」运行一个交易日以上，再用 `GET /api/live/sessions/{id}/theory-vs-live` 与影子台账取偏差。

### 6.1 实盘会话 1 只读核对（2026-11-20 11:49，未调用 start）

| 项 | 值 |
|---|---|
| 状态 / 执行模式 | `running` / `auto`（tdx，template live，`bridge_online=true`） |
| 启动时刻 | 2026-11-20T11:45:54.621011（用户自行在通达信登录后启动） |
| 会话资金 / 基准 | 263,515.51 / 263,515.51（baseline_version=1） |
| 账户快照 | 11:48:34 total_asset 253,861.35，available_cash 55,018.35，持仓 4 只 |
| `minute_source` | `enabled=true`、`mode=native`、`last_advance_at=null`、`last_label=null`、`lag_seconds=0`、`stalled=false`、`stall_timeout_seconds=180` |
| 日终钩子 | 无停止截断：11:45:54 启动成功，`DailyCloser` 14:30 估值 / 15:05 日线轮次 + 收盘清扫随 tick 执行；`advance_close_enabled=false` 仅关 14:50 提前收盘，与 14:30/15:05 无关 |
| 开关 | `capital_enforce_enabled=false`、`cash_outlet_mode=OFF`、`sell_scope=platform_only`（global）、`capital_pool_mode=off`、`pairing_plan_id/backtest_run_group_id=null` |
| 今日委托 / 成交 | 0 / 0（仅 2 条 `data_health` notify 决策事件） |
| 今日日终行 | `live_portfolio_daily` 2026-11-20 组合 1 行 total_value 250,894.51（启动时 backfill 写入）；2026-11-19 行仍为 258,471.51 |

原生分钟源核对（墙钟 11:49，午休 11:30–13:00）：

- 持仓 4 只 `.lc1` mtime 11:29:45–11:33:45，今日在写；最新闭合标签 1m=11:29、5m=11:25，均 ≤ 墙钟，`latest_closed_bar` 正确丢弃未闭合桶，判新只看标签。
- 会话启动时正处于午休，行情轮询整段暂停（日志：`行情轮询已暂停（会话 1）：午休 11:30–13:00…下次恢复 2026-11-20 13:00`），`MinuteSourceHealth` 尚无观测，故 `last_label/last_advance_at` 为 null、`lag_seconds=0`、非回落——属预期，不是停滞。

已知风险（离线复现，待 13:00 实测确认）：

- 跨午休首轮误判：13:00:05 首次 `observe` 看到的最大闭合标签仍是 11:30，`_evaluate` 以该标签为参考，会立即判 `no_advance`（lag 5405s）→ WARNING + risk 事件 + 回落快照合成；13:01 首根下午 bar 到达后自动恢复（回落约 1 分钟，自愈）。用注入时钟复现结果：13:00:05 `mode=fallback/stalled=true`，13:01:20 `mode=native/stalled=false`。
- 影响面：回落期间仅行情取数走快照合成（与默认行为一致），不影响下单/日终；但会产生一次误报的回落留痕。建议保留 `native_minute.enabled=true`（自愈且日终不受影响），后续单独修 `_session_anchor` 跨午休参考点（以下午 13:00 为锚）。**未自行改回 false**。

## 7. 未覆盖项与需拍板事项

1. **#4 业务 KPI（已执行，未达标）**：§3.3 要求「#4 类组合利用率 24.5% → 35%–45%，最大回撤 ≤ 8%」。2026-11-20 已按用户授权新开记录 31 实测：利用率 18.62%、最大回撤 8.41%，两项均未达标；区间与 #4 一致但策略参数已变更，不能视为同参数 A/B（详见 §3.1）。是否在恢复 id=4 参数后重跑，待拍板。
2. **召回次数 ≤ 12 次/年、shortfall ≤ 2%/年**：需要实盘或长周期回测样本，当前无数据源。
3. **恒等式 (1)(3)(9)**：运行时因「持久化虚拟现金账本 / 当日开盘现金 / 会话级策略预算」数据源缺失记 `skipped`（G2 已记录），模块层有用例覆盖；按手册口径，`skipped` 不等于账平。
4. **迁移回退**：本阶段未在生产库执行 `downgrade`（G4/G5 已在临时副本演练）；生产库保持在 `g4a5b6c7d8e9`。需要回退时按 G5 手册 §4.2 的三条路径执行。

## 8. 生产库快照（只读核对）

| 项 | 值 |
|---|---|
| `alembic_version` | `g4a5b6c7d8e9` |
| `PRAGMA quick_check` | `ok` |
| `live_sessions` | 01 `running`（`auto` / tdx，11:45:54 启动）、02 / 03 `stopped`；三会话均 `capital_enforce_enabled=0`、`cash_outlet_mode=OFF` |
| `capital_pairing_plans` / `capital_pairings` / `backtest_run_groups` | 0 / 0 / 0 行（F3 无绑定来源） |
| `backtest_records` | 28 条（新增 id=31 `G6-#4-capital-pool-20261120`，既有 27 条未动） |
| `cash_outlet_ops` | 0 行 |
| `live_capital_shadow` | 0 行 |
| `capital_pairing_health_daily` | 0 行 |
| 本机 `live.native_minute` | `enabled=true`、`stall_timeout_seconds=180`（config.yaml，gitignored） |
| 备份 | `main/data/backups/dev_20261120_105015_g4head.db`（同 revision） |

## 9. 复跑命令

```powershell
# 全量
main\.venv\Scripts\python.exe -m pytest main/core/tests -q -p no:cacheprovider
# 资金池回归组
main\.venv\Scripts\python.exe -m pytest main/core/tests/unit main/core/tests/integration/test_capital_api.py -q -k "capital or theory_vs_live or shadow_mode"
# 前端
cd web; npm run build
# 只读前置检查（G5）
powershell -ExecutionPolicy Bypass -File docs/ops/capital_pool_g5_preflight.ps1
```

性能脚本为一次性临时脚本（未入库）：固定样本、无随机、无时钟，可按 §5 口径重现。
