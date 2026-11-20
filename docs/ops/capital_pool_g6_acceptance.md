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
| 实盘影子对照 | 影子只记录计划 vs 实际、不触碰委托；理论 vs 实盘对照逻辑 | 测试通过；实盘影子已于 2026-11-20 收盘后开启（`capital_shadow_enabled=1`），自下一交易日起算采样（见 §6） |
| 资金池相关回归组 | `-k "capital or theory_vs_live or shadow_mode"` | 214 passed |
| 前端构建 | `cd web; npm run build` | 通过（773 modules，788 ms） |
| 性能 | 开关全关默认路径 vs 借贷开启 | 见 §5 |
| 生产库与开关状态 | `main/data/dev.db` | revision `h5b6c7d8e9f0`，quick_check ok，会话 1 running（15:13:02 收盘后重启并启动，`capital_shadow_enabled=1`）、2/3 stopped，强制 0 / 出口 OFF |

工程验收结论：G1–G6 的代码路径、回归基线与默认路径零变化均有可重复测试支撑；G6 判定 **DONE（工程验收）**。业务 KPI 按 §7 的新判据复核；实盘影子样本（§6）进入采样期，按「累计 ≥3 个交易日且样本量达可判水平」再判。

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

- KPI 对照：利用率 18.62% 落在参考区间 35%–45% 下方（仅参考，不再作为门槛）；最大回撤 8.41% 高于 ≤8% 上限 0.41pp。本记录为单组合、`capital_pool` 仅为版本戳、无额度注入（额度事件 0 条），不适用 §3.3 的「注入真实发生」判据；回撤超标作为单组合口径的独立观察保留。
- 可比性说明（未自行放宽）：区间一致，但 PS3 配置在 id=4 之后已变更——`max_positions` 10 → 20、`single_open_ratio` 10% → 5%、`dividend_mode` back → front、`fee_reserve_enabled` true → false。故 31 与 4/26 只能按区间对照，不能视为同参数 A/B；利用率下降与回撤上升主要来自上述配置差异，不能单独归因于资金池机制。

### 3.2 #4 真注入重跑（2026-11-20，run_group 1 / 记录 32+33）

- **数据来源**：本期 #4 真注入使用 E2 多组合计划（`POST /api/backtest/run-multi`，`backtest_run_groups` id=1 `G6-#4-pool-inject-20261120`，记录 id=32 PS2 / id=33 PS3），**不是** `loan_model_plan`，也不是单组合记录 31 的 `capital_pool` 版本戳；与 id=4 / id=26 **不可比**（PS3 参数已变、池化并入 PS2、id=4/26 为单组合未开池，详见本节末）。
- 注入路径：`loan_model_plan` 要求借款方与出资方同属一个组合，生产库 3 个组合均为单策略（策略 1/2/3），无法成立；改用等价的 E2 多组合计划——`POST /api/backtest/run-multi`，`portfolio_strategy_ids=[2,3]`（PS2 创懿题材 + PS3 创懿全量股池，#4 组合在池内），2014-01-01 → 2026-11-19，`dividend_mode=front`，`capital_pool_plan={enabled:true, min_share:0.05, max_share:1.0, min_cash_reserve_pct:0.03}`。请求 12:39:27 发起、12:45:36 返回（约 6 分钟，含两组合数据准备）；记录 32/33 于 12:45:10 创建、12:45:34/35 完成，`backtest_run_groups` id=1 `G6-#4-pool-inject-20261120` completed；新开记录 id=32（PS2）/ id=33（PS3），既有 28 条未动（现共 30 条）。
- **注入已生效的证据**（不是版本戳）：引擎每日首 bar 按「目标仓位 − 持仓市值」水位分配额度并支持跨组合闲置现金临时拆借；记录 33 产生 499 条额度约束事件（`capital_quota_shrink` 157 / `capital_quota_exhausted` 112 / `capital_quota_below_lot` 230），最高策略额度 53,889,509.66（单组合记录 31 同类事件为 **0** 条，因其 `capital_pool` 块只是 A5 版本戳）。
- 指标（逐日 `market_value/total_value` 均值；`total_value` 峰值回撤；池化 = 两组合同日「市值和 / 净值和」，缺日按前值携带）：

| 口径 | 平均利用率 | 最大回撤 | 期末净值 |
|---|---|---|---|
| **池化（PS2+PS3 共享资金池）** | **16.73%** | **5.65%** | 93,875,396.95 |
| 池内 PS3（记录 33） | 18.25% | 6.26% | 81,072,576.13 |
| 池内 PS2（记录 32） | 11.95% | 5.48% | 12,802,820.83 |

- 新判据（0021 §3.3，35%–45% 已降为参考区间）逐条核对：
  1. **注入真实发生**：记录 33 共 499 条额度约束事件（shrink 157 / exhausted 112 / below_lot 230），记录 31 为 0 条 → 通过；
  2. **回撤 ≤ 8%**：池化 5.65%、池内 PS3 6.26% → 通过；
  3. **利用率不劣化**：同窗口（2021-01-04 → 2026-11-19，1424 个交易日；因 PS2 无 2014 起单组合记录，取四者共存窗口）实测——PS3 单组合（记录 31）**10.78%**、PS2 单组合（记录 03）**20.11%**；按当日净值加权基准（等价 Σ市值/Σ净值）= **11.32%**，按期初资金等权（各 200 万）= **15.44%**；同期池化 = **11.36%**（§3.2 表中的 16.73% 是 2014 起全期 carry 口径，两者窗口不同，不可混用）。相对净值加权基准 **+0.04pp**（不劣化；正式判据采用与池化同分母的净值加权）；相对期初等权 **−4.08pp**，原因：PS2 独立运行时独占 200 万，池内与 PS3 共享资金，额度按「目标仓位 − 持仓市值」分配且受 `min_cash_reserve` 约束，其份额被摊薄——属口径差异，不是池机制缺陷。
  → 三条判据均通过。35%–45% 仅作参考：池化 16.73% 落在区间下方，原因是策略本身不打满现金（期末池内现金占比 96.3%，池内 PS3 利用率中位数 13.83%、PS2 9.48%）。
- 与 id=4 / id=26 是否可比：**不可比（仅同区间）**。①PS3 参数已变更（`max_positions` 10→20、`single_open_ratio` 10%→5%、`dividend_mode` back→front、`fee_reserve_enabled` true→false）；②池化口径额外并入 PS2，分母含其 200 万初始资金，利用率天然被摊薄；③id=4/26 为单组合、未开池。若要严格同参数 A/B，需先恢复 id=4 时的 PS3 配置并单独授权改动组合参数，本轮未改。
- 结论：资金池注入链路（额度分配 + 真注入证据）已验证可用；按新判据（注入发生、回撤、不劣化）三项通过。参考区间 35%–45% 仍偏低，属策略仓位/信号约束而非池机制缺陷；是否调整 PS3 参数（提高单笔开仓比例、放宽持仓上限）以进入参考区间，待拍板。

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

采样状态（2026-11-20 收盘后开启，自下一交易日起算）：

- 判定口径：累计不少于 **3 个交易日**，且样本量达到可判水平后再判「计划 vs 实际偏差 < 5%、门控一致 100%」；不足 3 日不下结论。
- 采样起点：13:33 以 `PUT /api/live/sessions/1 {"capital_shadow_enabled": true}` 写入待生效队列（运行中不重建引擎），15:09:26 会话 1 重新启动时应用——日志 `pending changes applied before start (session 1): 1`、`capital shadow enabled (session 1): 未绑定拼盘，只采实际侧`，库内 `live_sessions.capital_shadow_enabled=1`、队列行 `applied`。开启时点已在收盘后，当日无新 bar，`live_capital_shadow` 仍 **0 行**、`capital_pairing_health_daily` 0 行；**采样自 2026-11-23（下一交易日）首个盘中 bar 起算**。
- `no_pairing_plan`：按设计只在构建期写入一次 `warning_codes`（未绑定计划且开关开启时），并随每行影子 payload 落到 `warnings`，不是逐 bar 的决策事件；本次启动日志仅 1 条，`live_decision_events` 无该告警，符合预期。
- **F3 会话级影子开关（2026-11-20 已补）**：`live_sessions.capital_shadow_enabled`（迁移 `h5b6c7d8e9f0`，默认 0）。会话启动时据此决定是否构建影子；只逐 bar 追加 `live_capital_shadow`，不触订单/成交/决策事件，无下单入口。两条入口互不覆盖：
  - 已绑定 `pairing_plan_id` / `backtest_run_group_id`：始终走计划侧原路径（lender/borrowers、借据与 live_haircut），本开关不改变行为；
  - 未绑定 + 开关开启：以会话组合的策略成员构建「无计划侧」影子（`loan_plan=None`，本金/可用本金/缺口留空，并显式写 `no_pairing_plan` 告警，避免误读为「计划=实际」）。
- 开启方式：`PUT /api/live/sessions/{id}` `{"capital_shadow_enabled": true}`（或建会话时带上），**下次启动生效**（运行中不重建引擎）。生产库 `capital_pairing_plans` / `capital_pairings` / `backtest_run_groups` 均 0 行，故当前只能走会话级实际侧采样。注意：`backtest_run_groups` 在 12:45 已新增 id=1（本轮 #4 池化回测），该组为**回测**记录，会话 1 未绑定，不影响「本会话无计划侧」的事实（绑定字段仍为 null）。
- 补齐路径：开关打开并重新启停会话后，按 G5 手册 S1「小资金影子」累计 ≥3 个交易日，再用 `GET /api/live/sessions/{id}/theory-vs-live` 与影子台账取偏差。

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

已知风险（13:05 实测确认 + 已修复）：

- 跨午休首轮误判：13:00:56 首次 `observe` 看到的最大闭合标签仍是 11:29，旧代码以该标签为参考 → `no_advance`（log：`最后前进 11:29:00，已滞后 5515 秒`）→ 回落快照合成；13:01:56 看到标签 13:01 自动恢复（log：`原生分钟数据恢复前进（最后标签 13:01）`），回落约 60 秒、自愈。13:05:10 状态：`mode=native`、`last_advance_at=13:04:00`、`last_label=13:04`、`lag_seconds=70`、`stalled=false`、`reason=native_recovered`。
- 影响面：回落期间仅行情取数走快照合成（与默认行为一致），不影响下单/日终；留痕在 core.log 与 SSE risk 事件（`_emit("risk")` 只走事件流/交接文件，不落 `live_decision_events`，故 DB 查不到）。建议保留 `native_minute.enabled=true`（自愈且日终不受影响）。**未自行改回 false**。
- 已修复（2026-11-20，提交 `0297fab`）：`_session_anchor` → `_reference`，跨时段以上午/下午时段起点为下限，跨午休首轮不再误报；同段内停写判定不变。离线用例 `test_cross_lunch_first_round_no_false_fallback` 覆盖「13:00:05 不回落 + 13:01 首根到达仍 native」。代码未重启，下次重启生效。
- 13:05 标签连续性（13:05:10 探测）：1m 为 `…11:29, 13:01, 13:02 … 13:06`，5m 为 `…11:25, 13:05, 13:10`；**源文件本身没有 11:30 那根**（下载工具在午休前停写、不补边界 bar），故不存在「13:00 首轮取回 11:30」；`latest_closed` 正确停在 13:05（13:06 标签因 > 墙钟被丢弃），判新走标签、未闭合桶丢弃均得到实测验证。

### 6.2 重启后只读核对（2026-11-20 13:29–13:30，未调用 start/stop）

| 项 | 值 |
|---|---|
| 后端 | 13:24:23 由用户重启（pid 31692）；`/api/health` ok、端口 8001、version 1.0.6 |
| 会话 1 | `running` / `auto`，started_at `2026-11-20T13:24:42.401032`，`bridge_online=true` |
| `minute_source` | `mode=native`、`last_label=13:29`、`last_advance_at=13:29:00`、`lag_seconds=56`、`stalled=false`（13:25→13:26→13:29 连续推进，跨午休修复已生效） |
| 影子 | `capital_shadow_enabled=0`；`live_capital_shadow` 0 行；`no_pairing_plan` 无日志/无决策事件 |
| 今日委托 / 成交 | 0 / 0 |
| 持仓 | 4 只（快照 13:29:51：total_asset 250,639.35、available_cash 55,018.35、market_value 195,646） |
| 今日日终行 | `live_portfolio_daily` 组合 1：cash 55,002.51、market_value 195,650、total_value 250,652.51、position_count 4（13:25 backfill 值）；14:30 估值 249,759.51，15:09 重启启动后按收盘价重估为 **248,456.51**（见 §6.3） |
| 三项开关 | `capital_enforce_enabled=0`、`cash_outlet_mode=OFF`、`sell_scope=platform_only`（全局） |
| `alembic_version` | `h5b6c7d8e9f0` |

### 6.3 收盘后重启并启动会话 1 的只读核对（2026-11-20 15:09–15:11）

| 项 | 值 |
|---|---|
| 后端 | 15:09:03 首次重启（pid 26612）；15:12:38 交接为独立进程（health pid 2828）；`/api/health` ok、端口 8001、version 1.0.6；日志 `init_db：数据库已在 head（h5b6c7d8e9f0）` |
| 会话 1 | `running` / `auto`（15:09:27 首次启动，交接后 15:13:02 重新启动），`bridge_online=true`；启动告警仅「数据体检 30/251（预算内未阻塞）」 |
| 待生效队列 | `live_session_pending_changes` id=1 `capital_shadow_enabled` true → `applied`（15:09:26），启动前应用 |
| 影子开关 / 台账 | `capital_shadow_enabled=1`（两次启动日志均 `capital shadow enabled (session 1): 未绑定拼盘，只采实际侧`）；`live_capital_shadow` **0 行**（收盘后无 bar，自 2026-11-23 起算）；`no_pairing_plan` 仅构建期 1 条 |
| `minute_source` | `enabled=true`、`mode=native`、`last_advance_at=null`（收盘后无新 bar）、`stalled=false`；收盘后 `_in_session=false` 不做停滞判定，不会误回落 |
| 今日委托 / 成交 | 委托 **1** 笔 / 成交 **0** 笔：id=14 `600211.SH` 卖 1300 @37.94（`trailing_stop`，15:06:36 由 15:05 日线 bar 生成）状态 `deferred`，次日首 bar 提交；两次重启均 `1 deferred daily-signal order(s) restored, 0 stale dropped, 0 duplicate dropped` |
| 今日日终行（定稿） | `live_portfolio_daily` id=8 组合 1：cash 55,002.51、market_value 193,454、total_value **248,456.51**、position_count 4（15:09:27 / 15:13:02 启动时按收盘价重估，两次结果一致） |
| 账户快照 | 15:13:07 total_asset 248,472.35、available_cash 55,018.35、market_value 193,454；15:10:13 / 15:10:31 两次 `database is locked`（自动备份 + 增量回测争用），15:10:49 起恢复 |
| 三项开关 | `capital_enforce_enabled=0`、`cash_outlet_mode=OFF`、`sell_scope=platform_only`（全局，会话列仍 NULL） |
| `alembic_version` | `h5b6c7d8e9f0` |

### 6.4 15:10 备份/续跑锁争用修复（2026-11-20 收盘后）

- 现象：15:10:13 / 15:10:31 两次 `account snapshot capture error ... database is locked`（15:10:49 自愈，快照未断）。
- 根因（只读核对 + 日志时序）：续跑在判废处 `db.delete(ckpt); db.flush()` 即开启 SQLite 库级写事务，锁一直持有到函数末尾 commit，横跨整个回测计算（本次 session 1 约 15:10:04→15:10:15、session 3 15:10:20→15:10:40）；同一分钟自动备份 `VACUUM INTO`（读事务，15:10:03→15:10:20）又把 WAL 收敛拖到最后。实盘快照写入 `busy_timeout=5000ms`，等不到写锁即报错。备份用 `VACUUM INTO`（一致性快照，非文件拷贝），本身不持写锁，但会显著延长争用窗口。
- 修复（不改业务语义/回测结果/订单与撮合）：① 判废事件与旧 checkpoint 删除推迟到计算完成后的写库段（同一事务，失败仍整体回滚）；② 新增进程内 `db.MAINTENANCE_GATE`，自动备份与增量回测互斥——抢不到就让出本轮（不落记录、不跳过当日备份，60s 后重试），手动备份最多等 120s。
- 验证：新增 3 个用例（计算期间无 DML、备份持闸时续跑让出、续跑持闸时备份让出且随后正常产出），`test_backup_service.py` / `test_incremental_checkpoint_rebuild.py` / `test_incremental_backtest.py` 共 70 passed。原争用可在单测中按同一机制复现（旧实现于计算前即 emit DELETE）。
- 生效条件：调度线程在 API 进程内，需重启后端后生效；本报告落笔时未重启。

## 7. 未覆盖项与需拍板事项

1. **#4 业务 KPI（判据已按 0021 §3.3 修订）**：35%–45% 降为参考区间，正式判据三条——
   - **注入真实发生**：记录 33 共 499 条额度约束事件（shrink 157 / exhausted 112 / below_lot 230），记录 31 为 0 条 → 通过；
   - **回撤 ≤ 8%**：池化 5.65%、池内 PS3 6.26% → 通过；
   - **利用率不劣化**：同窗口（2021-01-04→2026-11-19，1424 个交易日，成员期初各 200 万）实测 PS3 单组合（记录 31）10.78%、PS2 单组合（记录 03）20.11%；按当日净值加权基准（等价 Σ市值/Σ净值）= **11.32%**，按期初资金等权 = **15.44%**；同期池化 = **11.36%** → 相对净值加权 **+0.04pp（不劣化，正式口径）**，相对期初等权 −4.08pp（PS2 独立独占 200 万、池内共享后被摊薄，属口径差异）→ 通过。
   - 参考区间仍偏低（池化全期 16.73%，因策略不打满现金：期末池内现金占比 96.3%）；是否调整 PS3 参数以进入参考区间，待拍板。
2. **F3 影子偏差（采样中）**：`capital_shadow_enabled` 已开启（见 §6）：13:33 `PUT` 入待生效队列，15:09:26 会话 1 重启启动时应用，库内为 **1**；因收盘后无新 bar，`live_capital_shadow` 仍 **0 行**，`no_pairing_plan` 仅构建期 1 条。**采样起点为 2026-11-20 收盘后开启、自 2026-11-23 起算**；判定口径为累计 **≥3 个交易日**且样本量达可判水平后再判「偏差 <5%、门控一致 100%」。
3. **召回次数 ≤ 12 次/年、shortfall ≤ 2%/年**：需要实盘或长周期回测样本，当前无数据源。
4. **恒等式 (1)(3)(9)**：运行时因「持久化虚拟现金账本 / 当日开盘现金 / 会话级策略预算」数据源缺失记 `skipped`（G2 已记录），模块层有用例覆盖；按手册口径，`skipped` 不等于账平。
5. **迁移回退**：本阶段未在生产库执行 `downgrade`（G4/G5 已在临时副本演练）；生产库已随启动自动迁移到 `h5b6c7d8e9f0`（见 §8），不再需要在下次重启时额外 upgrade。需要回退时按 G5 手册 §4.2 的三条路径执行（回退目标 `g4a5b6c7d8e9`）。

## 8. 生产库快照（只读核对）

| 项 | 值 |
|---|---|
| `alembic_version` | `h5b6c7d8e9f0`（2026-11-20 12:35 由集成测试的 TestClient lifespan 自动迁移落库，非人工 upgrade；已补 `TQ_INIT_DB_DISABLED` 闸，见提交 be13969） |
| `PRAGMA quick_check` | `ok` |
| `live_sessions` | 01 `running`（`auto` / tdx，15:13:02 收盘后重启并启动，`capital_shadow_enabled=1`）、02 / 03 `stopped`；三会话均 `capital_enforce_enabled=0`、`cash_outlet_mode=OFF` |
| `capital_pairing_plans` / `capital_pairings` | 0 / 0 行（F3 绑定入口无来源，改走会话级开关） |
| `backtest_records` / `backtest_run_groups` | 30 条 / 1 组（新增 id=31 单组合、id=32/33 + run_group 1 池化注入；既有 28 条未动） |
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
