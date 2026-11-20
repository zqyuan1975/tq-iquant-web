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

> 说明：G6 的「#4 对照」以代码级黄金基线为准。§3.3 的利用率/回撤 KPI 需要真实 #4 组合回测样本，见 §7。

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

- 「计划 vs 实际偏差 < 5%、门控一致 100%」需要实盘影子期逐 bar 数据。当前生产库 `live_capital_shadow` 0 行、`capital_pairing_health_daily` 0 行，三个会话均 stopped；按约束未启停会话，故只能证明采集/对照链路正确，不能给出偏差数值。
- 补齐方式：按 G5 手册 S1「小资金影子」运行一个交易日以上，再用 `GET /api/live/sessions/{id}/theory-vs-live` 与影子台账取偏差；是否需要启动会话由用户拍板。

## 7. 未覆盖项与需拍板事项

1. **#4 业务 KPI**：§3.3 要求「#4 类组合利用率 24.5% → 35%–45%，最大回撤 ≤ 8%」。这需要以真实 #4 组合跑一次开启资金池的回测（会向 `backtest_records` 写新记录，并与当前 `data/`、REF-04 基线共存）。G6 未擅自执行，需用户确认是否允许写库跑该回测。
2. **召回次数 ≤ 12 次/年、shortfall ≤ 2%/年**：需要实盘或长周期回测样本，当前无数据源。
3. **恒等式 (1)(3)(9)**：运行时因「持久化虚拟现金账本 / 当日开盘现金 / 会话级策略预算」数据源缺失记 `skipped`（G2 已记录），模块层有用例覆盖；按手册口径，`skipped` 不等于账平。
4. **迁移回退**：本阶段未在生产库执行 `downgrade`（G4/G5 已在临时副本演练）；生产库保持在 `g4a5b6c7d8e9`。需要回退时按 G5 手册 §4.2 的三条路径执行。

## 8. 生产库快照（只读核对）

| 项 | 值 |
|---|---|
| `alembic_version` | `g4a5b6c7d8e9` |
| `PRAGMA quick_check` | `ok` |
| `live_sessions` | 01 / 02 / 03 均 `stopped`，`capital_enforce_enabled=0`，`cash_outlet_mode=OFF` |
| `cash_outlet_ops` | 0 行 |
| `live_capital_shadow` | 0 行 |
| `capital_pairing_health_daily` | 0 行 |
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
