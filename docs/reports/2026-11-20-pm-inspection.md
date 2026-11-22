# 收盘后只读巡检（2026-11-20 周五）

**结论：今日 session 1（TDX）盘中运行基本正常，收盘清扫完成；存在 4 条 ERROR（无 Traceback 展开）、桥 12:41–12:44 短暂 offline 后已恢复但当前 7801 端口已消失；昨日遗留 deferred sell 单（600211.SH）未下单、session 2 reconcile mismatch 3 条未修复、watchdog 已 4 天未运行 —— 均需用户知悉或处置。**

---

## 1. 收盘清扫与轮询暂停

### 1.1 轮询暂停（15:05 截止）

| 会话 | 事件 | 时间戳 |
|---|---|---|
| session 1 | 行情轮询已暂停：已过收盘轮询截止 15:05，下次恢复 2026-11-23 09:15 | `2026-11-20 15:05:43` |
| session 1（引擎重启后再次进入收盘态） | 同上 | `2026-11-20 15:09:27` |
| session 2 | 无今日轮询记录（会话昨日已 stopped） | — |
| session 3 | 无今日轮询记录（会话昨日已 stopped） | — |

> 日志原文（15:09:27 第二次，引擎重启后触发）：
> ```
> 2026-11-20 15:09:27 [INFO] core.engine.live_engine: 行情轮询已暂停（会话 1）：已过收盘轮询截止 15:05，下次恢复 2026-11-23 09:15；账户快照/成交回填/收盘清扫照常
> ```

15:05 之后今日再无 `period=1m` 字段命中（全文件 grep 为 0）、也无 `/quote` 请求命中（今日 0 条）。午休 11:45–13:00 也正常暂停并恢复。

### 1.2 收盘清扫（daily_closer）

| 步骤 | 时间戳 | 结果 |
|---|---|---|
| close sweep（清 pending_orders） | `2026-11-20 15:06:36` | **no pending orders, nothing to do** |
| daily close done（组合估值） | `2026-11-20 15:09:27` | **1 portfolio(s) valuated** |
| close sweep（引擎重启后再走一遍） | `2026-11-20 15:09:27` | **no pending orders, nothing to do** |

> 日志原文：
> ```
> 2026-11-20 15:06:36 [INFO] core.engine.live.daily_closer: close sweep: no pending orders, nothing to do (session 1, 2026-11-20)
> 2026-11-20 15:09:27 [INFO] core.engine.live.daily_closer: daily close done (session 1, 2026-11-20): 1 portfolio(s) valuated
> ```

### 1.3 Reconcile 执行

- **今日无任何 reconcile 日志**（全量 grep `2026-11-20.*reconcile` 为 0）
- 昨日遗留（session 2 昨 23:56:36）仍有 3 条 mismatch，session 2 当前 stopped 状态，不会自动修复
  ```
  2026-11-19 23:56:36 [WARNING] core.engine.live_engine: reconcile mismatch (session 2) 301538.SZ: virtual=0 real=1100 diff=-1100
  2026-11-19 23:56:36 [WARNING] core.engine.live_engine: reconcile mismatch (session 2) 601989.SH: virtual=0 real=17500 diff=-17500
  2026-11-19 23:56:36 [WARNING] core.engine.live_engine: reconcile mismatch (session 2) 603325.SH: virtual=0 real=800 diff=-800
  ```

### 1.4 增量回测（盘后）

| 会话 | 时间戳 | 结果 |
|---|---|---|
| session 1 | `2026-11-20 15:10:15` | incremental backtest done, 5 day(s), gap=0, fast=False |
| session 2 | `2026-11-20 15:10:20` | incremental backtest done, 2 day(s), gap=0, fast=False |
| session 3 | `2026-11-20 15:10:40` | incremental backtest done, 2 day(s), gap=0, fast=False |
| 自动备份 | `2026-11-20 15:10:20` | dev.db → dev-20261120-151004.db（341.9 MB, WAL=ok） |

---

## 2. 会话状态

### 2.1 三会话总览

| Session | Name | Engine | Status | Engine Running | 启动时间 | 停时间 |
|---|---|---|---|---|---|---|
| 1 | 01 | tdx | **running** | true | 2026-11-20 15:09:27 | — |
| 2 | 02 | iquant | stopped | false | 2026-11-19 23:56:36 | 2026-11-20 00:07:41 |
| 3 | 03 | goldminer | stopped | false | 2026-11-19 23:31:35 | 2026-11-19 23:43:46 |

> session 1 今日启动于 15:09:27（盘后引擎重启），core.status.uptime = 0h3m（巡检时刻 15:12）。

### 2.2 Session 1 细项（运行中）

| 字段 | 实测值 |
|---|---|
| status | running |
| engine_running | true |
| pending_orders | **0** |
| reconcile_mismatches | **[]（空）** |
| last_backfill_time | 2026-11-20 15:12:10 |
| bridge_online | **true** |
| trading_calendar.degraded | **false** |
| trading_calendar.source | primary, coverage [2026-01-05, 2026-11-20] |
| minute_source.enabled | true, mode=native, lag=165s, stalled=false |
| account_snapshot.captured_at | 2026-11-20 15:12:10 |
| live_capital | 263,515.51 |
| account_key | engine:tdx, **source=degraded**（注意：degraded 状态 —— 没桥也能工作，但账户快照退化为 engine tag 占位） |
| capital_shadow_enabled | true |
| capital_pool_mode | off |
| sell_scope | platform_only, source=global |
| gate_recheck_on_execute | false |

### 2.3 Session 2（已停）

| 字段 | 实测值 |
|---|---|
| status | stopped |
| bridge_online | null |
| pending_orders | 0 |
| reconcile_mismatches | **[]**（当前 API 已清空；日志显示昨日有 3 条） |
| account_key | iquant:410038214257, source=bridge |
| trading_calendar | **null**（会话未运行） |
| minute_source.enabled | false |
| account_snapshot.captured_at | 2026-11-20 00:07:18（最后快照） |
| account_snapshot.total_asset | 237,673,477.57 |
| 持仓 | 9 只（601989.SH 17500、603325.SH 800、000001.SZ 100、000939.SZ 93300、002013.SZ 5900、300413.SZ 20、300722.SZ 89、301383.SZ 2000、301538.SZ 1100） |

> 注意：session 2 昨 23:56:36 的 reconcile mismatch（virtual=0 vs real>0 的 3 只：301538、601989、603325），API 返回 reconcile_mismatches 已清空（可能启动 recover 时做了清理），但根因（柜台持仓未纳入虚拟）仍未解决。

### 2.4 Session 3（已停）

| 字段 | 实测值 |
|---|---|
| status | stopped |
| engine | goldminer |
| account_snapshot.captured_at | 2026-11-19 23:40:27 |
| account_snapshot.total_asset | 5,000,000.00（空仓） |
| reconcile_mismatches | **[]** |

---

## 3. 今日探针单收口

**结论：今日未执行过任何"双通道交易验证"探针单，live_orders 与 live_trades 今日条目均无带 probe/marker/channel 标识的委托。收口项不适用。**

今日 live_orders 最近一条是 **id=14**（600211.SH SELL 1300，trailing_stop），状态 **deferred**：
- `bar_time`: 2026-11-20 00:00:00（日信号）
- `created_at`: 2026-11-20 15:06:36+08:00（盘后生成，被 deferred 是正确行为 —— 收盘后不下单）
- `order_ref`: null，`bridge_order_id`: null
- 引擎重启后 recover 恢复了 deferred 单（`2026-11-20 15:09:27 recover: 1 deferred daily-signal order(s) restored`）
- 次日（2026-11-23）盘前 09:31 放行窗口将顺延执行

今日 live_trades 无新成交（今日最近成交是 2026-11-19 09:15:35 的 301600.SZ SELL 700 filled）。

---

## 4. 资金与持仓

### 4.1 Session 1 柜台 vs 虚拟持仓对比

| 标的 | 柜台数量 | 柜台市值 | 虚拟（API /positions）数量 | 差异 |
|---|---|---|---|---|
| 002745.SZ | 5,300 | 51,728.00 | **0** | **-5300（未纳入虚拟）** |
| 002922.SZ | 2,500 | 44,525.00 | **0** | **-2500（未纳入虚拟）** |
| 300286.SZ | 2,900 | 47,879.00 | **0** | **-2900（未纳入虚拟）** |
| 600211.SH | 1,300 | 49,322.00 | **1,300** | 0（策略持仓） |
| **合计** | 12,000 | 193,454.00 | 1,300 | **-10,700** |

> equity_check 显示：`virtual_total=248,456.51` vs `real_total=248,472.35`，偏差 0.0063% —— 系统当前认定这部分偏差可接受。3 只未纳入虚拟的柜台持仓（002745、002922、300286）来源是实盘继承，capital_shadow_enabled=true 但未把这 3 只的数量入 shadow。

### 4.2 Session 1 资金

| 字段 | 实测值 |
|---|---|
| 柜台 total_asset | 248,472.35 |
| 柜台 available_cash | 55,018.35 |
| 柜台 market_value | 193,454.00 |
| 虚拟 live_capital | 263,515.51 |
| baseline_value | 263,515.51（baseline_version=1, set_at=2026-11-16） |

### 4.3 Session 2 资金（昨日快照）

| 字段 | 实测值 |
|---|---|
| total_asset | 237,673,477.57 |
| available_cash | 237,381,540.07 |
| market_value | 290,231.66 |

> 昨日 session 2 reconcile 时日志告警：`cash diff: 柜台可用 237,381,540.07 vs 虚拟现金合计 1,957,120.00 偏差 235,424,420.07 —— 仅告警，不改账`。session 2 今日未运行，此偏差原样保留。

### 4.4 探针单影响评估

今日无探针单、无新开仓。session 1 持仓、资金与盘前基线一致（301600.SZ 已于 11-19 卖出，今日无新变化）。

---

## 5. 健康度与桥

### 5.1 ERROR 计数（2026-11-20）

| 时间戳 | ERROR 消息 | 类型 |
|---|---|---|
| `12:41:13` | deals loop unexpected error | Bridge 异常（桥 offline 后 deals 轮询失败） |
| `12:44:03` | deals loop unexpected error | 同上 |
| `15:10:13` | account snapshot capture error (session 1) | SQLAlchemy execute（SQLite 锁） |
| `15:10:31` | account snapshot capture error (session 1) | 同上 |

**Traceback 展开**（均为截断片段，未完整爆栈）：
- deals loop → `live_engine.py:4329 _poll_deals()` 内部异常
- snapshot → `sqlalchemy engine.base do_execute()` SQLite 层面锁冲突

全量 ERROR 今日 **4 条**，无 WARNING 之外的异常级日志。Traceback 展开今日 **0 条**（日志里全局 122 条，但今日未新增）。

### 5.2 Bridge offline

| 时间戳 | 事件 | 后续 |
|---|---|---|
| `12:41:25` | `bridge offline, pause trading (session 1)` | — |
| `12:44:25` | 同上（重试后仍 offline） | — |
| `13:24:44` | `bridge mirror applied to 1 holding(s)` | **恢复** |
| `15:09:27` | `bridge mirror applied to 1 holding(s)`（引擎重启后） | 正常 |
| 巡检时刻 15:12 | **7801 端口已无 LISTENING** | **当前状态：桥进程已退出** |

**bridge offline 总次数：2 次**（都是 session 1，同一根因的两个 retry 周期）。恢复时间点在 13:24（约 40 分钟后自愈）。但现在 7801 已经不在了——bridge 进程很可能随后端主进程重启（15:09）而重新拉起，随后又退出或未重启成功。

### 5.3 7801/ping

```
❌ FAIL: 无法连接到远程服务器
```

同时 netstat `:7801` **无 LISTENING**。但 session 1 API 返回 `bridge_online=true`——说明 live_engine 内的桥健康检查在引擎重启后做了一次内部状态检查（或者 bridge_online 字段是上次 mirror 时的残留值），与真实端口状态不一致。

### 5.4 /orders 可读性

- GET `/api/live/sessions/1/orders` ✅ 可读，返回 8 条历史 order（含今日 deferred 的 id=14）
- GET `/api/live/sessions/1/trades` ✅ 可读，返回 3 条历史 trade

### 5.5 watchdog 状态

watchdog_state.json **最后更新 2026-11-16 13:51:34，已 4 天未运行**。last_alert_at 是 2026-11-16 09:11:34，说明今天桥 offline 事件 watchdog 也没捕捉到。

### 5.6 进程快照

```
PID 2828   python  2026/11/20 15:12:36  （刚启动，应是巡检相关）
PID 22948  python  2026/11/19 15:03:31  （昨日起长期运行）
PID 33508  python  2026/11/20 15:12:35  （刚启动）
```

后端主进程 PID 26612（8001 端口）不在 python 命名空间里显示——可能是用不同方式启动。

---

## 6. 明日待办（需用户决定或关注）

### 6.1 立即关注

| # | 事项 | 风险 | 建议 |
|---|---|---|---|
| A | **7801 桥当前离线**（ping 失败，netstat 无监听） | 明日开盘前若不恢复，session 1 将在无桥状态下运行，账户快照退化为 `account_key_source=degraded`，成交回填可能不全 | 盘前手动检查/重启掘金桥进程，确认 7801 端口监听 |
| B | **watchdog 已停 4 天**（2026-11-16 起不再更新状态） | 今日 bridge offline 事件 watchdog 未告警；后续同类故障可能静默 | 盘前重新启动 watchdog，确认状态文件恢复更新 |
| C | **deferred sell 单（600211.SH SELL 1300）明日放行** | 若明日开盘市场急跌/涨停，该 trailing_stop 可能在错误时机被触发 | 盘前确认该标的现价与预期止盈/止损位，是否需要手动调整或预放行 |
| D | **session 2 reconcile mismatch 3 条仍在**（301538.SZ 1100、601989.SH 17500、603325.SH 800） | session 2 已 stopped，但下次重启后会继续报；根因是柜台持仓未入虚拟 | 确认是否要重启 session 2 触发 reconcile 修复，或接受 stopped 状态暂不处理 |
| E | **session 1 account_key_source=degraded**（"engine:tdx" 而非 "bridge"） | 说明桥当前的账户查询能力未被引擎识别 | 与 A 项一起处理，桥恢复后应该自动切回 bridge |

### 6.2 用户之前列出的遗留项（需择机处置）

| # | 事项 | 说明 |
|---|---|---|
| F | 两处前端/事件改动需重启后端才生效 | 具体指什么改动需再核对 commit 范围 |
| G | 方案 A 超窗收口仍待真实场景验证 | 收盘后 deferred 顺延是否会在节假日后正确进入放行窗口（下次是 2026-11-23 周一） |
| H | 桥性能根治需非交易日 | 今日 bridge offline 40 分钟自愈但根因未查；建议周末排障 |
| I | request_queue_size 与超时参数待定 | 昨日 reconcile cash diff 235M 异常、SQLite lock 都是潜在放大因素 |
| J | order_ref 列宽迁移 | 之前 69 条里的尾项，已完成 |
| K | main/dist 封装待指令 | 需用户明确打包内容与交付方式 |

### 6.3 今日新发现问题

| # | 新问题 | 建议 |
|---|---|---|
| L | **engine_running=true 但 7801 不在，bridge_online 字段疑似陈旧值** | 盘前启动 session 时强制刷新 bridge health |
| M | **account snapshot SQLAlchemy SQLite lock（15:10 两条）** | 盘后 engine 重启 + 增量回测 + 自动备份三个并发写，可能触发 WAL 竞争；考虑错开自动备份时间 |
| N | **今日无 reconcile 日志（session 1）** | daily_closer 做了 close sweep 但没调 reconcile —— 检查是否盘中 reconcile 正常（grep 今日全量 reconcile 为 0，疑似盘中 reconcile 也未触发） |

---

*巡检时间：2026-11-20 15:12–15:20（只读，未做任何修改或操作）*
