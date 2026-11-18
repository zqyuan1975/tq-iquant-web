# 创懿私募基金量化投资平台 — AGENTS.md

## 项目状态

**已实现（脚手架 + 主链路已通）** — 31 个模型文件（33 张表）、50 个引擎源码文件、11 个 API 模块（10 路由 + `response.py`）、11 个前端视图、66 个 Alembic 迁移（head `f6d7e8f9a0b1`）均已落地；回测/实盘主链路已接线。完整实现状态见 [CLAUDE.md](CLAUDE.md)「实现状态」小节。本文档记录业务规则与设计依据，实现细节以代码与 CLAUDE.md 为准。

**单用户系统**：无用户鉴权，所有接口无需登录。

## 仓库

`github.com/nigelming/tq-iquant-web`

## 核心架构（4 模块）

```
Core (main, Python 3.13) ──HTTP──→ QMT 桥策略 (live/bridge/iquant_bridge.py, 8790/STOCK, QMT 客户端内 Python 3.6.8)
                          ──HTTP──→ 掘金桥策略 (live/bridge/goldminer_bridge.py, 掘金终端进程内, 127.0.0.1:7801)
  ├─ 同进程嵌入 TQ 模块（通达信，直接函数调用；tdx/ths/alert 取数 + 掘金行情快照合成）
  └─ 同花顺 UI 自动化（xiadan.exe，键鼠 + 文件交接）
Web 前端 (Vue 3 + Vite + Pinia) ←HTTP/SSE→ Core (FastAPI)
```

**关键约束**：两个独立的 uv 虚拟环境。`main/` 用 Python 3.13；`live/` 目录下 QMT 桥在 QMT 客户端自带 Python 3.6.8 内运行（桥代码须兼容 3.6，纯标准库实现），掘金桥在掘金终端使用的系统 Python 内运行。

> **Python 版本说明**：QMT 通道自带 Python 3.6.8，桥策略直接在该解释器内运行（不引入 nats-py 等需 3.7+ 的第三方库，HTTP 层用标准库 `socket` 手写）。

> **架构变更（2026-08）**：原 NATS 网关方案已废弃，Core↔QMT 改为 HTTP 桥。现行为 `init` 只做初始化并注册 `C.schedule_run(tick, ...)` 定时回调后立即返回（禁止在 `init` 内写阻塞死循环，否则客户端会挂起桥）。详见 [docs/plans/0009-iquant-http-bridge.md](docs/plans/0009-iquant-http-bridge.md)。原 `live/iguant_gateway/` NATS mock 网关已删除。

> **shared 包兼容性**：`shared/` 包被 main(3.13) 和 live(3.7) 共同引用，代码必须兼容 Python 3.7（不可用 walrus `:=`、match、pydantic v2 等）。数据结构用 dataclasses 或 pydantic v1。

> **时区**：所有时间按 Asia/Shanghai 处理，bar 时间判断基于上海时间。

> **数据库迁移**：使用 Alembic 管理 schema 变更，禁止手动改表结构。

## 开发命令

| 命令 | 说明 |
|---|---|
| `uv run pytest core/tests/` | 后端测试（main 环境） |
| `uv run pytest core/tests/unit/` | 仅单元测试 |
| `uv run pytest core/tests/integration/` | 仅集成测试 |
| `npx vitest` | 前端测试（web/ 目录） |
| `uv run uvicorn core.main:app --reload` | 启动后端 dev server |
| `npm run dev` | 启动前端 dev server（web/ 目录） |

开发期前端通过 Vite proxy 代理 API 到 FastAPI；生产期由 FastAPI 直接托管 `web/dist/` 静态文件。

## 进程自愈与部署（2026-11-16）

- **进程内看门狗只告警**：`core/engine/live/engine_watchdog.py`（30s daemon）只检测通道停摆/轮次卡住并告警，**不重启进程**（进程消失它也消失）；文档/注释里禁止再写"靠它兜底进程"。
- **进程级自愈 = 程序自带父进程**：`core/supervisor.py`，入口 `tq-iquant.exe --supervise`（源码 `python -m core.supervisor` 或 `desktop.py --supervise`）。父进程拉起 `--serve` 子进程，每 10s 探活 `/health`；子进程退出或连续 3 次无响应 → 退避重启（3s 起翻倍、上限 60s）；连续 5 次失败 → 停止重试并写明确日志，不无限刷屏。日志：`logs/supervisor.log`（父）/`logs/supervisor-child.log`（子进程输出）。
- **端口禁止硬编码**：打包版端口 8888~8898 动态，实际端口/pid/版本写 `data/runtime/server.json`（正常退出清理）；父进程、桌面壳、二次双击都读它探活。`/health` 无鉴权，返回版本/运行时长/端口/pid/app 标记。
- **单实例**：`data/runtime/supervisor.lock`（只允许一个父进程）+ `server.lock`（只允许一个后端，防两后端同连一个 SQLite），均为 OS 级文件锁，进程强杀自动释放；已有健康实例时二次启动只复用界面，不起第二个后端。
- **默认双击行为不变**：不加参数仍是单进程（后端 + 界面；`--no-supervise` 等价）；自愈模式只由安装器注册的登录计划任务或显式 `--supervise` 触发。子进程内父进程看门狗保证父进程被杀后 5s 内自行退出，不留孤儿端口。
- **禁止用 Windows 服务托管**：服务在会话 0、无交互桌面，通达信/同花顺键鼠自动化会失效。安装器只用「登录时计划任务」（任务名 `TQ-IQuant-Supervisor`，`/SC ONLOGON /RL LIMITED`，卸载删除）。

## 开发范式

**TDD**：先写测试，再写实现。pytest（后端）+ vitest（前端）。

**开发顺序**（在 `docs/system-plan-draft.md` 第 10 章有完整依赖图）：

1. 基础设施 → 2. 数据层 → 3. TQ 模块 → 4. 核心引擎 → 5. 回测 → 6. 实盘 → 7. 收尾

## API 规范

- 基础路径：`/api`
- 无鉴权，API 直接可访问
- 统一返回格式：`{ "code": 0, "message": "ok", "data": { ... } }`（code=0 成功，非 0 错误）
- 80 条 HTTP 路由（11 个路由模块；**全仓无 WebSocket 实现**），详见设计文档 5.6 节
- 实盘实时推送通过 SSE（`GET /api/live/sessions/{id}/stream`），非轮询

## 数据库（SQLite）

详见设计文档 5.4 节。关键点：
- 纯单用户本地工具，统一用 SQLite（`main/data/dev.db`），零配置，不切 PostgreSQL
- `config.yaml`（项目根目录）存储系统路径配置，无数据库密码（SQLite 本地文件）
- 每日快照 `backtest_daily_snapshots` 是评估指标的原始数据来源
- `backtest_evaluations` 18 个指标由 Evaluator 从快照序列计算
- `backtest_records.params_snapshot` 冻结回测时的组合/策略级参数、股票池成分与口径版本标记，确保结果可复现、可比
- 实盘交易记录：`live_orders`（订单状态跟踪）+ `live_trades`（成交记录）
- 多组合策略实盘：`live_session_portfolios` 关联表，一个 session 可含多个组合策略，共享同一资金账户（实际账户由所选引擎通道决定），各组合策略独立维护虚拟持仓和虚拟现金

## 并发模型

- **回测**：同步内联执行（在请求处理线程内跑 `BacktestEngine.run`）+ 全局锁 `_BACKTEST_LOCK`，同一时刻最多 1 个回测运行（并发启动返回 HTTP 409）。前端通过记录状态轮询获取进度。
- **实盘**：`BarPoller` 同步轮询所选引擎行情源（`tdx` / `ths` / 预警 / `goldminer` 走通达信快照合成，`iquant` 走 QMT HTTP 桥），`_loop`（60s 主循环：bar/approve/submit/超时收割）与 `_deals_loop`（5s 成交回填）各自投**独立单 worker `ThreadPoolExecutor`**（非 `run_coroutine_threadsafe`——TQ 回调线程方案已废弃，实际为 HTTP 拉取轮询），两池真并发。
- **实盘记账锁**：跨池记账（`_pending_orders` 增删/遍历、backfill/apply、持仓现金更新）由引擎级 `_book_lock`（`threading.RLock`）在调用点串行化——覆盖 `_tick_main` 超时收割段（expire + cancel_unfilled + commit）、收盘清扫（`_maybe_close_sweep`）、deals 回填（`_tick_deals`）与 `_handle_bar` 的在途单遍历；锁序恒为 `_book_lock` → `get_tdx_lock`，无反向获取，无死锁。start/stop 接口另有 `_START_LOCK`（asyncio）串行化 check-then-act 窗口，防双引擎并发下单。
- FastAPI 主事件循环不能被同步 I/O 阻塞（行情/订单轮询走线程池 worker）。

## 业务规则（容易遗漏）

- **股票代码格式**：统一带后缀（如 `000001.SZ`），通达信规范
- **复权方式**：回测支持三档复权模式（`dividend_mode`：`front` 前复权默认 / `back` 后复权 / `dual` 前复权信号+后复权记账，推荐）。前复权"加性压缩"会让长区间历史价逼近 0/转负（假收益），取数层已过滤 `Open/Close≤0` 与近零价（<0.01）伪影行
- **实盘桥行情不复权（已知口径差异，显式标记，不按缺陷处理）**：`/quote` 返回柜台原始价（`kline_t` 直出，`http_bridge_dispatcher.py` 注释自述），而回测（含 `dual` 的信号腿）用前复权。影响仅限 **1d/1w/1mon** 这类跨除权日的周期公式（除权日前后同一公式可能给出不同信号）；1m/5m 等日内周期与风控（止损/止盈/移动止损只用当日价格序列）不受影响。桥侧不做复权（QMT/掘金均无稳定前复权序列），故不回改信号口径；已知差异只做标记：`backtest_records.params_snapshot.dividend_mode` 记录回测口径，实盘会话按原始价执行，不可用「回测信号条数」直接与实盘对齐核对。
- **TQ 数据传递**：polars DataFrame 进程内传递，通过 tqcenter SDK 直连运行中通达信
- **Core↔交易通道通信**：QMT 通道走 HTTP 桥（`127.0.0.1:8790`，账号 410038214257 / `STOCK`；QMT 是四条生产通道之一、平台按生产标准维护，但**该账户是 QMT 仿真账户、账户内为仿真资金**，不是真实资金；切真实资金须改桥文件 `ACCOUNT` / `ACCOUNT_TYPE`（必要时 `TICK_TAG` / `DRY_RUN`）并重跑实盘验证。`iquant_bridge_live.py` 的 `8791` + 信用账户 `CREDIT` 保留但**非生产、未启用**），掘金走 HTTP 桥（`127.0.0.1:7801`）；桥策略受理下单/查单/持仓/资金，QMT 桥另提供行情拉取（掘金行情由平台走通达信快照合成）。端点见 [live/bridge/README.md](live/bridge/README.md)
- **信号优先级**：风控信号（止损/止盈/移动止损）> 公式信号（OPEN/ADD/REDUCE/CLOSE）
- **ADD 加仓阈值特殊值**：`add_position_threshold = -1` 表示跳过回撤检查，只要有 ADD 信号即加仓（含上涨/大涨）；`max_add_count` 加仓次数上限仍生效。正常取值 1%~20%（逢跌加仓，现价较成本下跌 ≥ 阈值才加）。0 表示"不涨即加"（drop ≥ 0）。风控优先级仍高于 ADD（涨到止盈点先止盈清仓，不加）
- **资金模型**：策略资金占比是持仓上限（非预分），多策略上限之和可超过 100%
- **T+1 约束**：回测强制 T+1，实盘实时查可用股票
- **熔断规则**：max_drawdown 触发 → 次日恢复（累计触发 3 次后转手动恢复）；daily_loss_limit 触发 → 当日暂停，次日恢复。熔断期间不清仓，仅暂停新开仓
- **账户级聚合熔断（2026-11-18，默认关闭）**：`live_sessions.account_risk_enabled=1` 时按「Σ 各组合 `risk_equity()`」聚合账户净值，账户最大回撤 / 日亏触发 → 该会话所有组合停新开仓（订单组装后剥 BUY，SELL/风控卖出不受影响），次日自动恢复、累计 3 次转手动（`POST /api/live/sessions/{id}/account-risk/recover`）。60%/80%/100% 分级预警（SSE `risk` + `notify` + `live_decision_events`，`gate=account_risk`）。阈值缺省取 `tq_trade.account_max_drawdown` / `account_daily_loss_limit`（15% / 5%），会话列显式值优先；与组合级熔断独立叠加（任一触发都停开仓）
- **组合资金日序列（A3，2026-11-18）**：14:30 日终每组合落一行 `live_portfolio_daily`（现金/持仓市值/总市值/风险净值/账户权重/逐仓明细，唯一键 session+portfolio+date，upsert）；`GET /api/live/sessions/{id}/portfolio-daily?days=30` 回看。只做观测，不参与账务或风控判定
- **主从策略**：从策略只能买入主策略当前持有的同一只股票；主策略清仓（含该股）后从策略不可新开仓但存量可自行卖出
- **一个 5m bar 可能同时触发多个周期**（如 10:30 同时触发 5m+15m+30m+1h 公式；回测同时间戳亦按细→粗依次处理，与实盘 `periods_on_boundary` 同序）
- **周期边界新鲜度闸（5m/15m/30m/1h）**：边界分发前校验「最新完成 bar 的交易日 == boundary 当日（Asia/Shanghai）」。手册 TQ 1.3.1 本地模式分钟线依赖盘后下载根数 + Level2，且 `refresh_kline` 明说盘中 1m/5m 只到上一交易日——**绝不用上一交易日的完成 bar 冒充当日信号**。不等则跳过该 code（fail-safe），记 WARNING + risk 事件（`gate=period_bar_freshness`，字段含 period/code/boundary_time/bar_date）；**boundary 只在成功派发后登记**，整批被拦时挂待补偿 `_pending_boundaries`，后续 1m bar 重试，超 15 分钟或跨日放弃（记 WARNING）。1m（快照合成）/`1d`（DailyCloser 同口径 date==今日校验）/`1w`·`1mon`（启动注入）不走此闸
- **实盘卖出与持仓口径（2026-11-10）**：`tq_trade.live_sell_scope` ∈ `full`（默认，卖整仓，含外部仓）/ `platform_only`（只卖平台自建仓；纯外部仓不代卖，记 `gate=sell_scope/action=skip`）。池外外部仓**不再兜底归属**：只进持仓展示与对账，不进风控、不占槽位、不代卖，每会话每日一条 `out_of_pool_position/notice` 汇总事件（逐票明细只在日志/SSE）。槽位上限取各组合自己的 `strategies.max_positions`（缺失则跳过槽位闸并告警，不写死 5）。成本 `cost ≤ 0`（送转分红后为负或柜台返回 0）或 `cost=None`（桥取不到成本属性）视为**成本未知**，不参与止损/止盈（阈值判断无成本基准），展示「成本未知」，不回退市值÷数量；但**移动止损照常生效**——它只用「持仓期最高价」这条价格序列，不依赖成本，是负成本/未知成本仓唯一的保护（2026-11-10 修；旧实现整票跳过 = 这类仓完全裸奔）。同理，镜面覆盖时若「真实量增加但合并均价摊薄、总成本未增」（`cost×qty ≈ 旧cost×旧qty`）判定为**送转/拆股等股本变动**，不扣虚拟现金（旧实现按增量市值扣现金 → 虚拟净值凭空少掉增量市值 = 假回撤，可能误触 max_drawdown 熔断）。卖出侧同 `(策略, 代码)` 未结单去重 `gate=inflight_sell`（目标量减在途未成交 ≤0 整单跳过、>0 按差额收窄）。`order/submitted` 不再重复推送两条（unconfirmed 分支只留 `order_unconfirmed` 决策事件）。
- **人工干预标记（2026-11-11）**：柜台被人手动动了**平台账内**的仓时，落 `gate=manual_intervention` 决策事件（SSE `risk` 同名 rule）+ WARNING 日志，字段 = 代码 / 虚拟量 / 真实量 / 差额（`param_value`）/ 方向（增加·减少）/ 原因，`requested_qty`=虚拟、`final_qty`=真实。判定点：①镜面覆盖时「柜台量 ≠ 虚拟量」（在途单代码已跳过、平台自身成交已记账，剩差即人工）→ `action=notice`；②柜台整行消失（人工清仓）连续 2 次采集确认 → 清虚拟仓 + 按账面成本退回虚拟现金 + `action=apply`；③收盘对账兜底 `_apply_close_counter_correction` 再清一次并留审计。**只打标不阻断**：不改风控优先级、不拦单；人工减仓按成本回补现金，避免虚拟净值凭空少一截（假回撤误触熔断）。与既有标记分工：`out_of_pool_position`=代码不在池（平台从未管过）、`external_deal`（`bridge_order_id=ext:*`）=柜台成交无平台委托（只留痕）、`bridge_order_manual_review`=平台委托柜台状态不可核（不判 rejected）。该闸不新增表/列，审计走既有 `live_decision_events`。
- **多组合策略实盘**：一个 live session 可含多个组合策略，共享同一资金账户下单（实际账户由所选引擎通道决定），各组合策略独立维护虚拟持仓（Per Portfolio）和虚拟现金（基于成本，非市值）。Core 重启时从 `live_trades` 按 `portfolio_strategy_id` 聚合重算虚拟持仓和虚拟现金
- **回测仅支持单组合策略**：每次回测针对一个 `portfolio_strategy_id`，多组合策略交互行为仅在实盘体现
- **次日盘前（集合竞价）执行日线信号单（默认开启）**：`tq_trade.pre_open_auction_enabled`（代码默认 `true`；`config.yaml` 未写时取此默认，`PreOpenAuctionConfig.from_config` 兜底同为 `true`）+ `pre_open_auction_end_time`（默认 `09:20`）+ `pre_open_auction_price_bias_pct`（默认 `2.0`）。关闭时行为与旧版逐字一致——`status=deferred` 单只由「次一交易日首根完成 1m bar（约 09:31）」放行。开启后交易日 **09:15 ≤ now < end_time（左闭右开）** 把「当日 due（`signal_bar_date` 的下一交易日 == 今天）的 deferred 单」直接报进集合竞价：**不依赖 1 分钟 bar、不做快照合成**；周期范围含 `1d`/`1w`/`1mon`（旧版 `1w`/`1mon` 跳过已取消），未到 due 的顺延单不处理。委托价 = 信号价（deferred 行记录的 `price` = 信号 bar 收盘）× (1 ± bias%)，再夹进当日涨跌停（买 `涨停−1 跳`、卖 `跌停+1 跳`；不推到板上追价；桥无涨跌停数据 fail-open 只按让价）。资金/整手/在途预留/T+1/熔断/风控优先级全部复用 `_submit_one_order` 同一条闸门链（`_allocate_batch_buys`）。**at-most-once**：每单每日只尝试一次（逻辑键幂等，与 DB 行 id 解耦）；已报出（`submitted`/`partial`，含受理未确认）不回队 → 09:31 不重报；闸门拦截/异常回滚/桥明确拒单 → 保持或重落 `deferred` 行并回内存队列，由 09:31 首根 bar 路径兜底重试（最坏是「盘前没报成、09:31 报一次」，绝无两次）。**09:31 兜底为限价**：`_refresh_release_price` 取当日首根 1m bar 的 `open` 作限价（快照通道优先读内存合成器 `SnapshotBarSynthesizer.open_at()`，不新增 TQ 查询；仅无合成器的 iquant 桥才回退 `count=250` 的 `/quote` 拉取并按 `parse_bar_time` 筛当日最早一根），夹涨跌停后 quantize 到分；取不到 open 退回实时 close 并记 `gate=deferred_release / action=fallback` 风险事件；`tq_trade.deferred_release_ensure_fill`（默认 `false`）开启才改对手价「保证成交」（可复现优先）。**开盘后越价撤单**：`_check_pre_open_orders_at_open` 在当日首根 1m bar（≤09:32，每日一次）比对登记的开盘价——买限价 < 开盘价 / 卖限价 > 开盘价即判必然未成交 → `cancel_order` 主动撤单并记 `gate=pre_open_auction / action=cancel / rule=open_crossed_limit`；**09:20–09:25 为交易所不可撤窗口**，该段内不发撤单（顺延 09:30 后由超时路径处理）。**撤单后回 deferred 重试（2026-11-11 取舍反转，推翻旧「开盘越价不自动重报」）**：撤单收敛为 `canceled` 后，把该单 `bar_time` 复原为原信号 bar 并放回 `_signal_bar_deferred`，由**同一交易日 09:31 首根 1m bar** 按当日首根 `open` 重定限价重试，次数上限 `_DEFERRED_RETRY_MAX=2`（与 P1-3 共用，计数落 `error_message=deferred_retry:n=<k>`）；**本次改为回 deferred 重试、受次数上限约束**——动机是回测按次日 `open` 无条件成交，只撤不重报等于系统性少做一笔已决策交易，而重排是拿当日真实开盘价重定限价，不是盲目挂回前日信号价、也不追价；撤单**未收敛**（`pending`/`failed`/`rejected`）不重排，宁可漏一次也不冒双份成交风险。审计事件 `gate=pre_open_auction`、`action=release|block|cancel|cancel_requeue` 进工作台决策流，日志前缀 `[pre-open]` 与首根 bar 路径区分。盘前委托单**按单**下发 `price_type=limit`（不再要求会话级 `price_type: limit`）；闸门判据为桥 `/ping` 自述支持 `limit` 或提供 `/price_limit`，并保留「本会话盘前单 `/order` 回显非 `limit` 就拦」的兜底。`price_limit_bounds` 由通道实现（`HttpBridgeDispatcher` 走 `GET /price_limit`，`TdxTqDispatcher` 用最近一根已完成日线收盘推价，同花顺委托通达信）。
- **错周期风控（默认关闭，对齐回测）**：`tq_trade.intraday_mismatched_risk_enabled`（默认 `false`）→ 错周期 bar **一律不跑风控**（1m/5m 等只更新价格快照，周线/月线同样只在自身周期边界评估）。会话级三态 `live_sessions.intraday_mismatched_risk_enabled`（`Boolean(nullable=True)`，`NULL`=跟随全局，`0/1`=覆盖），由 `live_service.normalize_mismatched_risk` 解析、`build_engine(session_mismatch_risk=...)` 传入引擎；API 仅接受 `null/true/false/0/1`。历史会话（列为 `NULL`）立即跟随新默认。`_check_risks` 口径固化：峰值与触发**只用 `close`**，禁止改 high/low/均价（回归测试 `test_check_risks_uses_close_only_not_high_low`）。
- **回测成本含费（2026-11-11）**：`portfolio_strategies.transfer_fee_rate`（`Numeric(8,6)`，默认 `0.000010`＝A 股现行双边 0.001%，可配 0 复现旧结果）；`SimulatedDispatcher` 成交时计过户费并写入 `TradeEvent.transfer_fee`；`Position.buy/sell(fees=)` 把佣金+印花税+过户费摊入/扣除成本与盈亏；`note_strategy_trade` 归因成本同口径；`backtest_trades.transfer_fee` 落库；`evaluator`/`persistence_alpha`/`regime_alpha`/`backtest_robustness` 汇总均含该字段；`portfolio_builder.cost_params` 透传（缺键会恒走 dispatcher 默认，用户置 0 不生效——已修）。实盘 `TradeEvent.transfer_fee` 恒 0（柜台成本已含费）。
- **外部现金流台账（2026-11-11）**：`live_cash_flows`（`live_session_id`/`portfolio_strategy_id`/`flow_type`=deposit|withdraw|dividend|other/`amount` **有符号**（>0 流入，<0 流出）/`flow_date`/`note`）。API `GET|POST /api/live/sessions/{id}/cash-flows`（`deposit|dividend|withdraw` 传正数，符号由类型决定；`other` 按符号判向）。虚拟现金随台账加减（运行中 `LiveEngine.apply_cash_flow` 即时生效，停止时 `recover` 从台账重放），**但净值/收益率/回撤/日亏按「总市值 − 累计净流入」计算**（`Portfolio.risk_value()/risk_equity()` 与 `BreakerService.on_daily_update` 同口径；回测净流入恒 0，两端可比）。柜台现金与虚拟现金不一致（扣在途买入冻结后）**只记 `cash_diff` 告警（10 分钟节流），绝不改账**；改账只能走台账或成交回填。
- **熔断恢复与真实-虚拟比对（2026-11-11）**：max_drawdown 次日首 bar 自动恢复、累计 3 次转手动（`BreakerService.restore_counts` 重启兜底）；手动恢复**默认不重锚峰值**（回撤基准不变，再次触发属预期），仅 `tq_trade.breaker_manual_reset_reanchor_peak=true` 才把峰值重锚到最近估值。账户采集后比对「真实总资产（剔除池外市值）vs 虚拟净值」，偏差 >20% 每会话只告警一次（`gate=equity_deviation`，不改账），会话详情 `equity_check` 字段展示。起账虚拟现金 = 组合配置资金（不自动采用桥真实总资产）。桥 `/deals` 缺佣金（`commission=0`，或 `/orders` 全成兜底合成成交）时按组合费率 `max(amount×方向费率, min_commission)` 补算佣金，与回测计费对齐（过户费实盘仍恒 0）。
- **估值兜底与派发边界（2026-11-11）**：持仓估值价 = **最后已知价（跨日持久）→ 成本 → 前收**（`Portfolio._resolve_price`），三级全缺才按 0 计并记 WARNING + `gate=valuation_missing_price` 风险事件（每 code 每日一次）——`avg_cost<=0`（继承仓成本未知）**绝不等同于市值 0**，否则停牌/无 bar 当日会制造假回撤、假日亏甚至误触熔断。`recover` 从最近一次账户快照的 `current_price` 回填「最后已知价」（跨重启持久，`market_value/quantity` 兜底）。回测买入费用兜底判据含 `transfer_fee`（与 `account.apply_trade` 扣款同口径，否则极小负现金）。下单时段闸：交易日 09:30–11:30 / 13:00–15:00（**11:30–13:00 午休禁报**，`TradingCalendar.is_trading_allowed`）。`BarPoller` 的 baseline 只拦「种子日早于当日」的陈旧 bar，当日首根完成 1m bar（快照通道 09:31）正常派发公式与风控（旧实现每天稳定丢一次首分钟信号/止损）。日线轮次因「取数为空 / 最新日线非当日」跳过时记 `gate=daily_bar_freshness`（每交易日一条），工作台可见「今天日线策略没跑」。
- **桥拒单可重排（2026-11-11）**：`BridgeOrderRejected` 分两类——「价格闸/涨跌停」类（未命中 `_REJECT_TERMINAL_HINTS`）视为**可重排**，订单不落终态而是回 `deferred`（`live_orders.status="deferred"`），`error_message` 写 `deferred_retry:n=<k>; <柜台原因>`，计数上限 `_DEFERRED_RETRY_MAX=2`（首报 + 2 次重排 = 最多 3 次尝试），`recover` 从该标记读回计数（跨重启仍受上限约束）；命中终态提示（资金不足/可用数量/数量价格非法/非交易时间/停牌/重复委托/通道不支持 `not supported`）或 `BridgeOrderIdError`（认领键冲突）→ 仍 `rejected` 终态。**与 at-most-once 不冲突**：重排只发生在「次日 due 窗口」或「同一交易日 09:31 首根 bar」各一次，当日绝不重复报；`LiveBarOrderMark` 仍按 `(session, 组合, 策略, 代码, bar_time, 方向, seq)` 独立成键、不看终态。终态分支同时把该单从 `_signal_bar_deferred` 内存队列摘除，避免终态单被放行路径二次提交。
- **分钟单提交价让 1 跳（2026-11-11）**：`requote_enabled=true` 时，分钟级策略（非 `1d`/`1w`/`1mon`）提交价在信号 bar 收盘价上 BUY `+1 tick` / SELL `−1 tick`（`tick_size(code)`，经 `_clamp_release_price` 夹当日涨跌停并 quantize 到分），记 `gate=submit_price/action=bias` 决策事件。豁免：日线及以上周期、deferred 放行价（`_release_priced`，由 `_refresh_release_price` 取首根 open 后不再让价）、盘前集合竞价单（`_pre_open_bar_time`，按 bias% 定价）、追价重下（`_requote`，已自带 ±1 跳）。默认 `requote_enabled=false` → 与旧版逐字一致；未成交仍由既有 180s 超时撤单 + 追价路径兜底（`requote_max_attempts`）。

## 回测口径（2026-11-16 核查收口）

- **无成交假 bar 剔除**：`NO_TRADE_BAR_POLICY = "drop_volume0_carryforward_v1"`（`backtest_service.py`）。取数层把「`Volume==0` 且 `OHLC` 全等于相邻有效收盘」的补行（停牌/上市前占位行）整行剔除，不参与信号、成交、估值与日快照；口径版本写 `params_snapshot["no_trade_bar_policy"]`，**旧结果（含假 bar）不与新结果混比**。
- **卖出印花税按成交日分段**：`STAMP_DUTY_SEGMENTS_VERSION = "cn_a_share_sell_2008_20230828_v2"`（`execution_engine.py`）；2023-08-28 起 0.05%，之前 0.1%（2008-09-19 后单边 0.1% 兜底）。组合 `stamp_duty_rate` 显式配成非默认值（≠0.0005）时**全区间照用**（保住自定义费率语义）。版本写 `params_snapshot["stamp_duty_segments"]`。
- **涨跌停前收按 `(code, 交易日)`**：`build_prev_close_by_day`（`backtest_engine.py`）= 上一交易日**最晚一根** bar 收盘（任意周期），**日内恒定**。旧实现键含 period 且逐 bar 覆写，5m/15m/30m/1h 当日第二根起「前收」变成上一根同周期收盘 → 涨停虚高/跌停虚低，封板买、跌停卖都能成交。增量续跑传「未切片全区间」结果，避免续跑首日缺前收导致整体 fail-open。
- **在途单最大存活期**：`backtest.pending_order_max_days`（`config.yaml`；缺键 = `PENDING_ORDER_MAX_DAYS = 20` 个交易日，`<=0` 不限）。超期单记 `gate=pending_order_expired / action=cancel` 后作废不回队——防停牌/连板把 deferred 单无限顺延、几个月后按恢复日开盘价「补做」早已过期的决策（实盘有超时撤单 + 次日重排，不会挂这么久）。
- **`params_snapshot` 冻结范围**：组合级参数 + run 级覆盖（费用预留/债券 T+0/整手/自适应滑点/成交量与权益上限/复权/日终抬峰/日亏基准）之外，本轮新增**策略级参数**（`strategies[]`：止损止盈移动止损/开仓比例/加仓阈值与次数/槽位/主从角色/周期/公式）与**股票池成分**（`pool`：pool_id → 代码列表），另含三个口径标记 `no_trade_bar_policy` / `stamp_duty_segments` / `fee_threshold_basis`。
- **back/dual 下费用阈值在复权价空间判定**：`fee_threshold_basis = "adjusted_price_space_v1"`——最低佣金 5 元等金额阈值判据用后复权记账价而非原始价，后复权价更高时「未达 5 元」判定偏保守（成本略高估）。**已知限制**，不与原始价空间逐笔对齐。
- **评估口径**：年化因子按快照日期间隔中位数推断（`infer_periods_per_year`：≥20 天 → 12、≥5 天 → 52、否则 252；分钟快照先聚合到日），Sharpe/Sortino/年化超额/跟踪误差不再被 √5~√21 放大；**无有效样本时 sharpe/sortino/calmar 返回 `None`**（界面「—」）；`total_return` / 最大回撤 / ulcer 有分母保护（首末净值或回撤基准为 0 时给 None/0，不出 inf/NaN）。
- **市场状态预热**：`_MARKET_REGIME_WARMUP_DAYS = 280`（约 40 周）仅用于打标预热，须从回测起点之前提前取基准收盘；样本量闸 `_MARKET_REGIME_MIN_DAYS = 210`（≈ MA30 周 + 2 周确认），不足返回 `status=insufficient`（「样本不足」），**不退回 range 冒充结论**。
- **可转债涨跌停与已知限制**：`market_rules.backtest_limit_rule` 对转债（`BOARD_BOND`）兜底 **±20%**（自 2022-08-01 统一口径；旧口径回落 10% → 板上追价成交）。已知限制：①转债**上市首日** -43.3%/+57.3% 特例未建模（按 20% 推偏窄，方向 fail-safe＝延后而非误成交）；②**历史 ST 名单不可回溯**——回测用当前名称判 ST，历史某段为 ST 而当前不是时按板块默认幅度推宽（主板 10%）。
- **增量 checkpoint v3 判定依据**：`SCHEMA_VERSION = 3` / `_ENGINE_SCHEMA = "backtest_engine_v3"`。指纹分项 = `params` / `strategies` / `formula`（正文哈希/engine/no_leak_cross/信号线）/ `pool` / `baseline`（baseline_version + baseline_value），叠加成本口径 `fee_inclusive_v2`、成交 `next_bar_open_v1`、T+1 `t1_forced_v1`；state 新增 `risk_cooldown`（冷静期）与接缝游标 `seam_bar_time`。任一分项变化 → checkpoint **判废**；**判废不自动重建**，由用户/调度器决定（下一轮 `run_incremental` 按常规路径从接缝日重建）。
- **实盘侧重放并入 `live_cash_flows`**：接缝 JSON 记 `flow_cursor`（`live_cash_flows.id` **右开**，表示该水位已计入 `seam_cash`）——重放只取 `id > flow_cursor` 的流水。不用 `flow_date`（只有日期，同日无法排序）或 `created_at`（SQLite UTC，与本地接缝时刻差 8 小时），两者都会把接缝日流水误判为「已计入」；旧接缝无该字段时回退 `flow_date > seam_date` 并告警。

## 实盘链 P1 收口（2026-11-16）

- **日线轮次幂等**：`live_sessions.daily_bars_done_date` **只在整轮驱动成功后写**（旧实现驱动前就写，一次短暂掉线/崩溃就永久丢剩余组合的 1d/1w/1mon）。桥离线（`BridgeUnavailableError`）只跳过该组合、继续驱动其余组合并计「未完成」，其它异常同样跳过该组合；本轮有未完成则不标完成，由 60s tick 重试，已成功的 `(交易日, 周期, 组合)` 记在 `_daily_driven_keys` 不再重复驱动。
- **增量调度异常可审计**：进程内 `IncrementalBacktestScheduler` 的日更/周校验异常除 `logger.exception` 外，同落 `live_decision_events`（`gate=incremental_backtest|incremental_validation`、`layer=live_gate`、`action=error`、`message` 取 `异常类型: 内容` 前 200 字），按 `(session, ps, gate, 交易日)` 去重，避免 60s 重试刷屏；写事件失败只告警，不掩盖原始异常、不阻断调度。
- **拆单队列可观测**：`stop` 时未提交子单随队列丢弃（预期行为），逐队列落 `gate=split_children_lost / action=drop` 决策事件（含剩余笔数与股数，供人工判断是否补单）；`recover` 按 `live_bar_order_marks(seq>0)` + 在途子单反查剩余量并记 `action=notice` 告警，**不自动补单、不落库重放、不新增列**。
- **risk 事件落库清单**：`valuation_missing_price` / `deferred_release fallback` / `deferred_dropped_stale` / `cash_diff` / `equity_deviation` / `daily_bar_freshness` 经 `_emit_risk_safe(persist=True)` **同落 `live_decision_events`（layer=risk）**，刷新页面仍可查；节流口径不变（每 code 每日一次 / 10 分钟 / 每会话一次 / 每交易日一条）。
- **周期边界补偿**：`dispatch_period_bar` 返回 `dispatched | stale | empty | no_strategy`；`stale`（全部被新鲜度闸拦下）与 `empty`（有该周期策略但一只 code 都取不到可用 bar）**同等挂待补偿**（15 分钟或跨日放弃），`no_strategy`（实例无该周期策略）不进补偿。
- **`bought_today` 口径**：BUY-only、区间 `[当日 0 点, 次日 0 点)`（旧实现只有下界，一条未来时间脏行会让该策略该股被永久判定「当日已买过」）；**同一笔单的续做豁免**——`_requote` / `_requeued_for_retry` / `_deferred_retry_count>0` 带的是原单未成交剩余量，追价与 deferred 重排**不再被信号级排重吞掉**（partial 成交后剩量不丢）；槽位/在途/bar 幂等闸照旧，新信号仍严格（同日同策略同代码只买一次）。
- **记账锁（BUY 临界区）**：BUY 的「资金上限计算（读在途预留）+ 审批 + 拆单 + 落库 + 在途注册」在**同一个 `_book_lock` 临界区**内完成（旧实现 cap 在锁外算、注册在锁内，deals 池追价与新单并发时双份占用同一笔现金＝超买）；锁序恒为 `_book_lock → get_tdx_lock`。
- **判重键**：桥可用持仓刷新按 **`bar_time`** 判重（`_available_bar_time`，不用 bar 对象身份——1m 节拍与周期边界可能是同一时刻的两个 `BarEvent`）；**缺价告警去重键 = `(portfolio_id, code)`**（旧键只用 code，多组合持同一缺价票时只有第一个组合报一次）；`trade_type` 读取统一走 `_is_buy()` 大小写归一（大写行漏计在途 BUY → 超开仓）。
- **隐式契约**：`live_orders` **无 `signal_bar_date` 列**，deferred 的信号日靠 `bar_time.date()` 重建；`orig_quantity` / `baseline_version` **只写不读**（放行时按真实可用动态缩量；基准识别靠 deferred 行 + `bar_time` 口径，不靠这两列）。

## 通达信「交易信号」确认收口（2026-11-18）

针对「点击没生效却被记成已受理、180s 后静默吃 rejected 终态」的闭环缺口（真机 2026-11-18 11:08 实证：窗口刚从隐藏被强制显示约 1 秒即点击，该帧价格列整体缺失、点击未生效）：

- **点之前等画面就绪**：`_force_show` 记 `_force_shown_at`（`snapshot_rows` 与 confirm 共用同一 confirmer 实例，跨调用有效）；confirm 循环在刚强制显示后的 `_READY_GRACE=10s` 内先 `_wait_frame_ready`——判据 = 行非空 + `_price_column_band` 可推（价格列区间 + 行尾『发送』锚点）+ **连续两帧行指纹一致**，上限 `_READY_TIMEOUT=4s`、间隔 `_READY_INTERVAL=0.3s`；超时只 WARNING 并继续现有路径（不改变失败语义）。
- **点击后复核不通过补点一次**：`_verify_clicked_row` 返回 False（行尾仍读作『发送』）→ `_reclick_pending_row` 重取一帧、按 `(code, volume, seconds)` 校验**必须还是同一行**（防点到别的待发委托）后补点 **1 次**（`_CLICK_RETRY_MAX=1`）并再复核；仍 False 落回既有失败路径。返回 None 的语义不变（不补点）；行已转『已发送』(`row.sent`) 绝不补点。
- **别把「点过」当「已报」**：`_expire_stale_orders` 在 `OrderStateMachine.expire_stale_orders` 之前调 `_recheck_unconfirmed_pending_rows`——仅对提供 `pending_row_state` 的通道（TDX）生效，条件 = `order_ref is None` + `status ∈ submitted/partial` + `bridge_order_id` 非空 + `created_at` 距今 ≥ `_ORDER_REF_MATCH_TIMEOUT`(180s)；复核 `pending`（行仍在『发送』）→ 重建 `OrderEvent` 调 `_requeue_rejected_order(gate=order_unconfirmed)` 回 deferred；`absent`（行已消失/已转『已发送』）与 `unknown`（无窗口/OCR 不可判/多候选）**保持现行为**交 expire。重排沿用 P1-3 同一套约束：`_DEFERRED_RETRY_MAX=2`、`error_message=deferred_retry:n=<k>` 跨重启读回、`signal_bar_date=bar_time.date()` 使放行必须跨到次一交易日（**当日绝不重报**）。残留未发送行**只告警、不自动点/撤**（日志提示人工核对后手动『取消』）。
- **`last_unconfirmed_message`**：TDX dispatcher 补齐该属性（此前仅 `HttpBridgeDispatcher` 定义），unconfirmed 分支写入 `confirm()` 的具体 detail（如「已点击『发送』，但该行仍为待发送状态…」），引擎既有 `getattr(..., "last_unconfirmed_message", None)` 口径不变，`live_orders.error_message` 不再只有通用文案。

## 公司行为与退市/停牌处置（2026-11-18）

手工事件表 `corporate_actions`（分红/送转/配股/退市/停牌，唯一键 `(code, event_type, ex_date)`），**不做自动抓取**；表为空或组合开关关闭时回测/实盘执行路径与改动前逐字一致。

- **判定内核唯一**：`core/engine/corporate_actions.py` 的 `apply_day(...)` 被 `BacktestEngine` 与 `LiveEngine` 共用（`CORPORATE_ACTION_VERSION = "manual_events_v1"`）。内核只算「当日应生效什么」并落到 `Position`/`Account`，再返回 `AppliedAction` 意图；两端适配层各自决定再投资成交、退市强平、停牌闸与告警。
- **组合开关**（`portfolio_strategies` 四列，全默认关）：`corporate_actions_enabled` / `dividend_reinvest_enabled` / `suspension_dispose_days`（0=不处置）/ `delist_force_close_enabled`（与总开关 AND）。
- **回测接入点**：`BacktestEngine.run` 主循环内、`risk_manager.enter_day` 之后、pending 成交之前（送转必须先改股数，否则顺延单按旧股数入账）；每交易日一次（`last_action_date` 水位）。再投资在 `is_day_end` 分支按当日收盘价买整手，不足一手留现金记 `skip_lot`。
- **实盘接入点**：`LiveEngine._maybe_apply_corporate_actions`（`_run_daily_schedule` + 每根 `_handle_bar` 开头，每组合每日一次）。**送转不手工改虚拟持仓**（柜台镜面已吸收，`_is_share_count_change` 不扣现金），只记事件；**现金分红 `account.add_cash` + 落 `live_corporate_actions` 台账**（`recover` 按 `applied_date > seam_date` 重放；**绝不走 `live_cash_flows`**，否则被当外部入金从净值抹掉）；再投资只记 `action=notice`（审批队列上线前不下单）；退市/长期停牌强平走 `_submit_one_order` 同一闸门链。
- **分红口径**：回测只认 `cash_pct`（前复权加性压缩，每股金额不可还原），实盘只认 `cash_per_share`；字段缺失记 `skip_cash`，不猜。事件日为非交易日 → 顺延到下一交易日，只生效一次。
- **停牌闸**：`Portfolio.suspended_codes` 只拦 `OPEN`/`ADD`（`gate=corporate_suspend`），`CLOSE`/`REDUCE`/止损/止盈一律放行；估值仍走「最后已知价」三级兜底。
- **口径冻结**：`params_snapshot.corporate_actions`（version/enabled/档位/event_count/events_digest）+ 增量指纹 `corporate_actions` 分项；开关/档位/事件集合任一变化 → 旧接缝判废（不自动重建）。**旧记录缺键 ≠ 关闭**，不混比。
- **运行中改开关需停止再启动会话**（`build_engine` 只读一次）；事件表内容每日现读，改事件不必重启，当日已应用的不重算。

## 市场多空门控（2026-11-18）

组合级开关：**基准指数空头状态禁止开仓/加仓，持仓不强平**。回测/实盘同一套闸门（`Portfolio._gate_blocks`），语义只拦 `OPEN`/`ADD`，`CLOSE`/`REDUCE`/止损/止盈/移动止损一律不受影响。

- **判别内核** `core/engine/market_gate.py`：`compute_gate_states(ohlcv)` 对通达信公式的 numpy/pandas 精确重实现（MA26/55/144/288 均线带、`jd` 角度线、`s1/s2/d/q` 多头触发、`s3/e/i` 空头触发、`M_IN` 优先、carry-forward、`jj` → 状态 2），输出 `states`（`-1` 空头 / `0` 未定 / `1` 多头 / `2` 多头强势）与 `strengths`（0–100）。`GATE_FORMULA_VERSION = "tqgate_py_v1"`，**改动公式必须同步改版本号**（冻结入 `params_snapshot`，用于跨记录口径核对；公式实现以参考版单测逐位对齐为验收基线，禁止"顺手优化" `_round_half_away` / `_if` NaN 语义 / `_count` 窗口）。
- **右移一日**：`MarketGateTracker._last_state_before` 用 `bisect_left` 取**严格早于当日**的最近状态日 → 昨日收盘状态管今日开仓，绝不用当日收盘决定当日成交（`blocks(d) = state == -1`，`0` 未定**放行**）。
- **配置**（`portfolio_strategies` 三列）：`market_gate_enabled`（默认 0）/ `market_gate_engine`（`python` 默认 / `tdx`）/ `market_gate_formula_name`（tdx 公式名，空=`TQGATE`）。迁移 `a4b5c6d7e8f9`（紧接 `e2f3a4b5c6d8`）另给 `backtest_daily_snapshots` 加 `gate_state`/`gate_strength`（NULL＝未启用门控）。
- **取数**：`backtest_service.build_gate_ohlcv` 走 `TQData().get_history_raw(... dividend_type="front", count=-1)`，取 OHLCV 五列（公式需 High/Low/Open/Volume，单 close 不够）；`build_gate_tracker(ps, start, end, enabled=None)` 把起点前推 `GATE_WARMUP_DAYS=500` 自然日做 MA288 预热，`enabled=None` 跟随组合配置。取数/公式无输出抛 `MarketGateUnavailableError`（`ValueError` 子类）→ 回测路由映射 **HTTP 400**；**不静默放行**（门控开启却全 0 等于没开）。
- **回测**：`BacktestEngine.run(gate_tracker=)` 注入 `portfolio.current_market_gate`，日快照追加 `gate_state`/`gate_strength`（`None` 不写键）；`POST /api/backtest/run` 的 `market_gate_enabled` 为 run 级三态覆盖（`null` 跟随组合 / `true` / `false`）。`params_snapshot` 写 `market_gate_enabled` / `gate_index` / `gate_formula_version` / `gate_summary`。
- **增量续跑**：`run_incremental` 与校验侧（`run_validation`）**都必须**带 `build_gate_tracker`（同一口径），`schema_fingerprint` 含 `market_gate = GATE_FORMULA_VERSION` → 开关或公式版本变化判废旧接缝，防接缝前后半段口径不一致。
- **实盘**：`live_service` 组装组合时收集**标量**配置 `gate_specs`（只收启用门控的组合，**不持有 ORM 实例**——避免 `build_engine` 之后 commit/expire 触发 DetachedInstanceError），传 `gate_provider` + `gate_portfolio_ids`；`LiveEngine._maybe_update_gate` 在**每组合每交易日首根非 baseline bar** 重建 tracker，调用点必须在 `_release_signal_bar_deferred` **之前**；provider 异常保留旧 tracker（fail-open，绝不因取数失败变成永不开仓）。**运行中改门控配置需停止再启动会话**（配置只在 `build_engine` 读一次）。
- **状态卡**：`GET /api/market-gate/status`（`core/services/market_gate_service.py`）实时算、不落库；进程内 **5 分钟缓存**（`(code, period)` 键，`refresh=true` 绕过）以避免每次进备选股池页都冻结通达信客户端。返回 `direction(-1/0/1/2)`、`strength`、`as_of`、`since`、`duration_bars`、`transitions`（最近 N 次转折）。前端入口（2026-11-18）：「备选股池」页顶部状态卡（基准下拉默认 `000985.CSI`、刷新、方向/强度分/截至/持续与起始日/最近转折，**独立取数失败只影响卡片**）；组合编辑弹窗末组「市场多空门控」；「发起回测」弹窗三态下拉（`''`=跟随组合 / `'true'` / `'false'`，提交时非空才展开为布尔）。
- **已知边界**：①门控只拦新信号，已排队的次日 deferred 单与盘前集合竞价单**不重判**（沿用信号日已决策，与参考版对齐）；②默认基准仅**新建组合**取 `000985.CSI`（中证全指），既有组合保留库值；③本地指数日线若有缺口（实证 `000985.CSI` 在 `2026-10-28~11-17` 缺失），状态靠 carry-forward 维持，卡片「数据截至」仍显示最新一根，**不报错也不拦截**。

## 时点股票池与多组合回测（2026-11-18）

**J7 时点股票池**：`stock_pool_memberships`（`pool_id`/`stock_code`/`effective_from`/`effective_to`，**两端含当日**，`effective_to=NULL` 仍有效）+ `portfolio_strategies.pool_pit_enabled`（默认 **0**）。判定内核 `core/engine/pool_timeline.py`（`PoolTimeline.contains/covered/codes_on/digest`，`POOL_TIMELINE_VERSION="manual_memberships_v1"`）；插入点 `Portfolio._signal_to_order` 停牌闸之后，**只拦 `OPEN`/`ADD`**（`CLOSE`/`REDUCE`/风控卖出不受限）。关闭时 `pool_timeline is None` → 闸门第一行即返回，取数也不传 `pool_codes`（与改动前逐字一致）。**该交易日无任何区间覆盖 → fail-open 放行**并记 `pool_pit_uncovered`（每交易日一条），绝不因历史未录变成零交易；出池票记 `pool_pit_out_of_pool/block`。成分同步按**用户操作当日**写区间（新增开区间、消失 `effective_to=当日`），**不回溯改历史区间**。API：`GET|POST /api/stock-pools/{id}/memberships`、`PUT|DELETE /api/stock-pools/memberships/{id}`。迁移 `f1e2d3c4b5a6`（紧接 `f0e1d2c3b4a6`，回填既有 `stock_pool_stocks` 为 1970 起开区间）。口径冻结 + 增量指纹分项 `pool_timeline`。

**N1 唯一资金审批内核**：`core/engine/cash_allocator.py` 的 `allocate_batch_buys`（目标份额 + **80% 门槛** + **不做等比缩量** + `caps` 回调 + 同代码累计）是实盘 `LiveEngine._allocate_batch_buys` 与回测（`run` / `run_multi`）的**同一份判定**，禁止在任一端另写一套资金规则。改门槛/缩量语义必须两端同时回归。

**N1 多组合回测**：`BacktestEngine.run_multi` 单时间轴驱动多个 `Portfolio`；服务 `run_multi_backtest` 落一个 `backtest_run_groups` + 每组合一条 `backtest_records`（`run_group_id`），聚合曲线（各组同日总市值求和）写 `aggregate_json`。**资金口径 = 各组合虚拟现金独立、互不划转**（0010 §5.1），每笔买单可用额 = 本组合 `cash − 在途 BUY`，按请求组合顺序串行审批（组级不变量：Σ 通过金额 ≤ Σ 可用现金）。**不要实现跨组合转账**——会让出资组合虚拟净值凭空下跌（假回撤可能误触熔断）。API：`POST /api/backtest/run-multi`（≥2 组合、共用单组合并发槽、超限 409）、`GET /api/backtest/run-groups[/{id}[/aggregate]]`。迁移 `f2e3d4c5b6a7`（紧接 `f1e2d3c4b5a6`）。

**N1 执行档位**：`core/engine/execution_profile.py`（`EXECUTION_PROFILE_VERSION="execution_profile_v1"`）；`legacy`（默认，旧成交口径）/ `realistic`（涨跌停校验 + 成交量约束 10% + 自适应滑点 + 日终抬峰），逐开关 `partial_fill_enabled` / `first_bar_no_fill` 可覆盖（**默认 false**）。`partial_fill` **只作用于 BUY**（卖出残量仍走既有顺延/超时撤单）。档位写 `params_snapshot.execution` 并进增量指纹 `execution` 分项（切档位判废旧接缝，不自动重建）。缺省值必须与改动前逐字一致——回归基线是「不传任何新参数时结果与旧版相同」。

## 半自动审批队列与人工操作（2026-11-18）

- **M1 审批队列（默认关闭）**：`live_sessions.execution_mode="manual"` 时，买入信号在 `LiveEngine._submit_one_order` 的「① 审批+拆单+落库注册」之前写入 `live_approval_queue`（pending，`expires_at=now+approval_ttl_seconds`）并返回，**不写 `live_orders`**；`alert`/`auto` 会话入队分支第一行即返回，行为与改动前逐字一致。`GET|POST /api/live/sessions/{id}/approvals[/{aid}/approve|reject]`，未运行会话返回 409。
- **豁免口径**：风控信号（止损/止盈/移动止损）与 `CLOSE`/`REDUCE` **永不入队**（人工延迟止损不可接受）；其余卖出按 `tq_trade.approval_exempt_sells`（默认 `true`）。人工补单（`_manual_order`）、拆单子单、追价重下、批准回放（`_approval_passed`）均不重复入队。
- **批准 = 以当下重走全部闸门**：重建 `OrderEvent(bar_time=now, signal_bar_date=行内原值, period=行内周期)` 调 `_submit_one_order`（资金/在途/槽位/T+1/涨跌停/拆单/bought_today 全生效）；`signal_bar_date` **必须随行落库**——按 `bar_time.date()` 重建会把 15:05 日线信号误顺延次日、分钟信号误当每日信号。过期 → `expired`；`stop()` 把 pending 置 `dropped`（重启不重放）；`recover()` 只告警残留 pending 计数。
- **I1 人工补单/平仓/改账**（`live_manual_actions` 审计，`tq_trade.manual_order_max_amount` 默认 0 不限、`manual_action_min_interval_ms` 默认 1000）：
  - `submit_manual_order` 仅 `manual` 会话开放；`_manual_order=True` → 不入审批队列、不参与 `bought_today` 信号级排重、**不让价**（人工定价原样报出），但仍走全部闸门与拆单/落库链；归因策略 = 请求 `strategy_id` 或组合内第一个策略。
  - `force_close_position` 按虚拟持仓全额 SELL CLOSE，**不绕过 `live_sell_scope`**（`platform_only` 下纯外部仓 409 + `blocked` 审计）。
  - 现金改账复用 `record_cash_flow`（运行中即时生效）；**持仓改账仅停止的会话**：写一条合成 `live_trades`（`live_order_id=NULL`、费用 0），重启 `recover` 重放即生效——虚拟持仓/现金的唯一权威来源就是 `live_trades` 重放，新增账本会造成双口径。

## 对账差异台账与现金流红冲（2026-11-18）

- **C1 差异台账（默认关闭）**：`live_reconciliation_diffs`（迁移 `f4b5c6d7e8f9`）记录 `position` / `cash` / `equity` 三类差异，状态 `pending → accepted | resolved | ignored`。开关 `tq_trade.reconciliation_ledger_enabled`（**默认 false**，关闭时 `_note_reconciliation_diff` 第一行返回，既有告警链逐字不变）。插入点全部**旁路在既有告警发射之后**：`_note_cash_diff` / `_note_equity_deviation` / `_note_manual_intervention`（后者 `detail` 带 `price=` 供改账取价）。同一 `(session, diff_type, stock_code)` 的 pending 行复用更新，不逐次追加。API：`GET /api/live/sessions/{id}/diffs?status=`、`POST .../diffs/{did}/resolve`（`action=accept|resolve|ignore` + `resolution` + 可选 `price`）。**运行中改该项需停止再启动会话**。
- **I2 以柜台为准**：`position` + accept 按「柜台量 − 期望量」写合成成交改虚拟持仓（运行中 `engine.accept_counter_truth` 同步内存，停止时 `adjust_position_virtual` 重启重放生效）；`cash`/`equity` + accept **只确认口径不改账**（现金差仍按既有「只告警不改账」规则）。已处理行不能再次处理；改账价格优先请求 `price`，其次 `detail` 的 `price=` 提示。
- **C2 现金流红冲**：`live_cash_flows` 加 `reversal_of_id` / `reversed_by_id`（自引用 FK SET NULL）/ `voided`。**禁止物理删除**——红冲 = 反向补一笔（金额取反、`reversal_of_id=原行`）+ 原行 `voided=1`；Σamount 自动归零，净值口径无需额外处理。同一笔只能冲一次、红冲行不能再红冲。API：`POST .../cash-flows/{fid}/reverse`。
- **C3 现金流日期边界**：`flow_date` 晚于今天 → 拒绝；早于会话起账日（`live_sessions.created_at.date()`）→ 拒绝，除非显式 `allow_backfill=true`（会改写历史净值口径，必须操作者确认）。`POST /cash-flows` 透传 `allow_backfill`。
- **C4 每日对账日报（默认开启，只读）**：`live_reconciliation_reports`（迁移 `f5c6d7e8f9a0`，唯一键 `(session, report_date)`），`_tick_main` 在 `close_sweep`/`reconcile_after_close` 之后每交易日 15:05 生成一次（内存幂等 + 唯一键兜底，重复生成覆盖更新）。柜台侧取最新 `live_account_snapshots`，虚拟侧取运行中引擎内存；**只在运行中生成**。报表不写 `live_trades`、不改现金/差异台账；API：`GET .../reports`、`POST .../reports`（运行中立即生成，未运行 409）。失败只告警，不影响收盘流程。
- **I4 手工成交录入**：`submit_manual_trade` 落 `live_orders(order_type="manual", status="filled")` + `live_trades`，再走与真实成交回报完全相同的 `_apply_filled_trade`（持仓/现金/成本/冷静期/归因一次到位）。**不经过桥、不占审批、不做买入闸门**（成交已发生）；仅 `execution_mode=manual` 开放；卖出须有虚拟持仓且 ≤ 持仓（超卖拒绝，不静默缩量）；买入允许负现金（差异进对账）；`commission` 缺省 0。API：`POST /sessions/{id}/manual/trades`。不做撤销——错误用反向手工成交修正，历史只追加。
- **I3 池外外部仓处置**：`live_out_of_pool_decisions`（迁移 `f6d7e8f9a0b1`，唯一键 `(session, stock_code)`）。默认语义不变（池外=不归属/不风控/不代卖）。`include` 写 `stock_pool_stocks`（PIT 开启时补 `effective_from=今天` 成员区间），下一轮采集按池内继承仓导入；`ignore` 只抑制 `out_of_pool_position` 告警。运行中处置即时刷新引擎内存（`reload_out_of_pool_decisions` / `include_pool_code`），不重启。API：`GET/POST .../out-of-pool/actions`。
- **C5 启发式送转落台账**：镜面识别「数量增 + 均价摊薄 + 总成本未增」时写 `live_corporate_actions`（`event_type=split`、`ex_date=applied_date=当天`、`qty_delta`、`amount=0`、note 标明启发式）。**只记事件不改账**；唯一键幂等（同一会话/组合/代码/事件日一次）；`begin_nested()` 保存点隔离，台账失败不影响镜面主链路。查询：`GET .../corporate-actions`。

## 模块复用

引擎层核心逻辑（风控、信号优先级排序、资金审批、持仓更新）回测/实盘共用，集中在 `portfolio.py`/`risk_manager.py`/`strategy_context.py`。实盘独有逻辑（对账告警、熔断次日恢复、SSE 事件流、周期边界 1m/1d 14:30/1w·1mon 处理）在 `live_engine.py`。差异通过策略模式隔离下单与 T+1 检查：

| 接口 | 回测实现 | 实盘实现 |
|------|----------|----------|
| `OrderDispatcher` | `SimulatedDispatcher` 按 next_bar.open 模拟成交 | `HttpBridgeDispatcher`（QMT `iquant` / 掘金 `goldminer`，同构协议）/ `TdxTqDispatcher`（通达信 TQ）/ `ThsUiDispatcher`（同花顺 UI 自动化），按 `live_sessions.engine` 选择 |
| `T1Checker` | `SimulatedT1Checker` 直接返回持仓量 | `LiveT1Checker` 查实际可用股数（各引擎同一口径：持仓表的「可用」列，0 为合法锁定值） |

执行引擎 `ExecutionEngine` 持有这两个接口，不感知具体实现。

**实盘交易引擎按会话可选**：`live_sessions.engine` ∈ `goldminer` / `tdx` / `ths` / `iquant`（旧 miniQMT 的 `qmt` 键已不在白名单；历史 `qmt` 会话与 NULL 会话均由 `session_engine` 回退到全局默认）。全局默认解析顺序：`tq_trade.default_engine` 优先（SystemConfig 可配，默认 tdx）；未配置/非法时回退旧行为（`ths_ui.enabled` 且依赖可用 → ths，否则 iquant）。旧 `qmt` 会话不会自动改指 `iquant`（miniQMT 与 QMT 是不同终端/账户，避免打到非预期账户）。引擎未就绪（客户端未启动/未登录/依赖缺失、桥策略未运行、未配置路径账号）时 `/sessions/{id}/start` 直接返回 503 并给出可操作中文提示，**不会静默回退到其它通道**。委托认领：`supports_remark=False` 的通道（`TdxTqDispatcher`、`ThsUiDispatcher`）走 legacy 模糊匹配（代码+方向+数量+提交时间窗；陌生 ref 补记的时间标记 `insert_fabricated`，匹配时降级排除，防遗留单误绑）；`supports_remark=True` 的 HTTP 桥（`iquant`、`goldminer`，同一 `HttpBridgeDispatcher`）把确定性 bridge_order_id 写进柜台 remark，按 remark 精确认领（柜台 `m_strRemark` 上限 20 字符：`bridge_order_id` 为 32 位 MD5 落库，写入柜台的是**前 20 位**认领键；认领键生成失败或同会话撞键一律**拒单**（`BridgeOrderIdError`），不静默截断）。撤单能力：`TdxTqDispatcher` / `ThsUiDispatcher` / `HttpBridgeDispatcher` 均实现 `cancel_order`（桥通道走 `POST /cancel`，按柜台委托号撤单，返回区分 `canceled`/`pending`/`not_found`/`rejected`/`failed`，失败绝不误报成功）；**老版本桥**无该端点（HTTP 404）时 dispatcher 自动置 `supports_cancel=False`，调用方按「通道不支持撤单」跳过，退回 180s 超时 expire 兜底（柜台可能仍挂单，需人工撤）。`requote` 追价重下**对所有通道生效**（引擎层无通道判断）：撤单收敛为 `canceled` 后按跳价 ±1 tick、次数上限 `requote_max_attempts` 重新走 `_submit_one_order` → 同一 `dispatcher.place_order`，桥通道即再次 `POST /order`（QMT `passorder` / 掘金 `order_volume`），TDX/THS 同路径。涨跌停校验：桥提供 `GET /price_limit`（前收 + 交易所涨跌停价），`HttpBridgeDispatcher.check_price_limit` 本地判定买 ≥ 涨停 / 卖 ≤ 跌停；桥缺数据或端点不存在（老桥 404）一律 **fail-open** 放行。

**同花顺文件交接层（THS file_builtin，默认）**：同花顺通道是纯 UI 自动化，`ThsUiDispatcher` 双模式 `execute_mode` ∈ `file_builtin`（默认）/ `direct`（同步直呼，原行为）。file_builtin 下：引擎 signal/decision/risk 事件同步落 `{ths_ui.work_dir}/{session_id}/` 下 `signals.jsonl`/`risk.jsonl`（LiveEngine `_emit` 钩子，写失败仅告警）；`place_order` 只做参数校验后把指令追加进 `orders.jsonl` 并**秒回 None**（LiveEngine 按 `last_enqueued_order_id` 把 None 识别为「已入队」，订单保持 submitted 等回填，不判 approval failed）；内置后台执行线程（daemon，惰性启动，`execute_interval_seconds` 默认 5s）按游标（`cursor.json`：消费行数 + done 幂等表）读指令逐单执行 UI 自动化，回执落 `results.jsonl`（accepted+合同编号 / rejected+错误详情），成功单补记 `_known_refs`/`_submit_times`（批次 A 口径）。OCR/验证码守卫移入执行器（file 模式 place_order 不做 OCR 前置拒单，direct 模式维持），OCR 未启用不再阻断 file 会话启动。THS 撤单已自动化（撤单页定位目标委托行后双击；失败 fail-closed，不误报成功）：file 模式超时在途单仍走 180s expire rejected；读后游标即推进（at-most-once），执行前崩溃的单不重复执行，由 expire 兜底。执行器 rejected 回执由引擎 deals 轮询即时消费（`execution_receipts()` 弹出 → 在途单即时 rejected + order 事件，工作台秒级可见），不等 180s expire。**执行契约 schema 文档**（四文件/游标逐字段定义、seq 语义、执行器要求、通道接入指南、版本策略）见 [docs/execution-contract.md](docs/execution-contract.md)。

**同花顺 UI 键入真机校准（2026-10-28 实证，勿回退）**：xiadan 下单页字段为自绘控件（WM_GETTEXT/ValuePattern/剪贴板均不可读、UIA 焦点报告与可见字段不对应），键入必须走「**鼠标点击进字段 + kb_send（SendInput）**」：①字段定位 UIA control_id 实测 代码=1032、价格=1033、数量=1034；②代码框是「输入区+下拉按钮」组合，点击必须落**输入区左起 8px**（中心点点到下拉按钮、键入丢失，像素差异扫描校准）；③代码键入后**等待 4s**——THS 异步用最新价覆盖价格框（真机实证委托价 4.415≠键入 4.153）；④`_key` 每次 `_focus()` 重置主窗口焦点打掉字段焦点、`Timings.fast()` 丢字符——键入路径禁用；⑤撤单=撤单页**双击委托行**（真机实证有效，页面提示「双击或回车即可撤单」；开盘前委托未报不可撤）；⑥窗口级键入会被 THS 热键层吃掉（整窗 OCR 实证屏幕零变化）。

## 关键路径与配置文件

| 路径 | 说明 |
|---|---|
| `main/core/main.py` | FastAPI 入口 |
| `main/core/engine/` | 自研回测/交易框架（事件驱动，polars 核心） |
| `main/core/tq/` | 通达信 TQ 模块（嵌入 Core 同进程） |
| `main/core/engine/http_bridge_dispatcher.py` | HTTP 桥下单通道（通用：QMT 桥/掘金桥同构协议） |
| `main/core/engine/tdx_tq_dispatcher.py` | 通达信 TQ 下单通道（与回测取数同一 TQ 连接；确认弹窗自动处置） |
| `main/core/engine/ths_ui_dispatcher.py` | 同花顺独立交易 UI 自动化通道（file_builtin 文件交接层 + direct 直呼双模式） |
| `main/core/engine/live/file_handoff.py` | 执行契约文件交接层（通道无关；signals/risk/orders/results JSONL + cursor.json 游标） |
| `main/core/engine/live/win_dialog_scope.py` | 双引擎弹窗归属识别 + UI 自动化互斥锁（tdx/ths 并行防串扰） |
| `main/core/engine/live/snapshot_bars.py` | 快照合成 1m bar（TQ 盘中分钟线不可得的盘中数据源） |
| `main/core/engine/corporate_actions.py` | 公司行为/停牌/退市判定内核（回测与实盘共用 `apply_day`） |
| `main/core/services/corporate_action_service.py` | 手工事件台账 CRUD 与校验（`/api/corporate-actions`） |
| `main/core/runtime.py` | 运行期实例信息 `data/runtime/server.json` + 单实例锁 + 动态端口发现（`/health` 载荷） |
| `main/core/supervisor.py` | 自带自愈父进程（`--supervise` 父进程 / `--serve` 子进程；探活 + 退避重启 + 孤儿防护） |
| `main/core/services/notify_service.py` | 告警外发（飞书自定义机器人 Webhook；默认关闭、后台线程、超时 ≤5s、失败只写日志） |
| `main/desktop.py` | 打包版/桌面壳入口（默认单进程；`--supervise` 自愈；`--serve` 纯后端） |
| `live/bridge/` | HTTP 桥策略：`iquant_bridge.py`（QMT 通道生产桥，`127.0.0.1:8790`，账号 410038214257 / `STOCK`，**在用**；账户为 **QMT 仿真账户/仿真资金**，切真实资金须改 `ACCOUNT` / `ACCOUNT_TYPE` 并重跑实盘验证）/ `iquant_bridge_live.py`（`8791` + 信用账户 `CREDIT` 410009993017，**非生产、未启用**，仓库保留以维持镜像护栏）/ `goldminer_bridge.py`（掘金量化，掘金终端内运行，`127.0.0.1:7801`）。QMT 与掘金、同花顺、通达信并列为四条生产通道；Core 的 `simulation` / `live` 两个模式位均指向 8790 |
| `web/` | Vue 3 + Vite 前端 |
| `config.yaml` | 系统配置（TDX 路径、`ths_ui` / `tq_trade` / `goldminer` / `iquant_bridge` 交易通道） |

## 告警外发（2026-11-18）

- **默认关闭**：`notify.enabled`（默认 `false`）+ `notify.webhook_url` 非空才生效；任一不满足 → `notify()` 立即返回 False（**不建线程、不发请求**），行为与接入前逐字一致。测试/调试用 `TQ_NOTIFY_DISABLED=1` 强制关闭（`conftest.py` 已默认设置，防测试打到真实机器人）。
- **绝不影响交易主链路**（硬约束）：`main/core/services/notify_service.py` 的 `notify()` 只做「级别过滤 → 静默期去重 → 入队」，HTTP 发送在独立 daemon 线程（队列 `QUEUE_MAX=200`，满则丢弃新事件），超时强制夹 1~5 秒；**所有异常内部吞掉**，失败只写日志且按 600 秒节流。任何接入点都必须在既有事件已经发射/落库**之后**调用，禁止因告警失败改变任何闸门、订单状态机或落库结果。
- **分级与去重**：`info < warn < error < critical`，低于 `min_level`（默认 `warn`）丢弃；静默期键 = `(gate, code, trade_date)`，`silence_seconds`（默认 300）内同键只发一条。
- **接入点**（只旁路加一条，不改既有语义）：熔断（`breaker.py` 的 max_drawdown 触发/转手动/自动恢复/手动恢复、daily_loss 触发与次日解除）、通道与桥离线/恢复（`engine_watchdog.py`，只在「进入告警」时发一次）、`cash_diff` 与 `equity_deviation`（`live_engine._note_*`）、拆单子单丢失（`stop` 丢弃队列 / `recover` 重启遗留）、订单未确认超时重排（`_recheck_unconfirmed_pending_rows`，含重排耗尽 `critical`）、增量回测续跑与双跑校验失败（`incremental_backtest._record_scheduler_failure` 旁）。
- **敏感信息**：`webhook_url` / `secret` **只写本机 `config.yaml`（未入 git）**；`packaging/config.template.yaml` 只留空占位。日志与异常消息经 `mask_sensitive` 脱敏（只留尾部 4~6 位），`_send_one` 失败日志会替换掉文本中出现的完整地址/secret。签名：`timestamp`（秒）+ `sign`=base64(HMAC-SHA256(key=`timestamp\nsecret`, msg=空))。

## 掘金量化桥（2026-11-04 接入，与 QMT 桥并存）

- **架构**：桥策略 `goldminer_bridge.py` 跑在**掘金终端进程内**（掘金终端用系统
  Python 解释器执行策略，gm SDK 装在**系统 Python 的 site-packages**，与终端
  本地服务 gmterm-serv v5.2.158 协议天然匹配），实现 QMT 桥同构 HTTP 协议
  （`127.0.0.1:7801`），平台 HttpBridgeDispatcher 零改动接入。
- **环境纯净规则（强制）**：**禁止把 gm SDK 安装进平台 venv（main/.venv）**——
  PyPI gm 3.0.192 的 C SDK（NATS 协议）与 goldminer3 终端 v5.2 服务协议不匹配
  （get_cash 无限挂起实证），且其依赖链（protobuf==3.20.x / onnxruntime 降级 /
  typing / wheel / cachetools）会污染平台环境（2026-11-05 清理实证，备份
  `main/data/backup/pyproject.toml.*.bak`）。掘金集成只允许「终端内桥策略」模式。
- **启动方式规则（强制）**：桥策略**只能由掘金终端启动**（终端内点击运行）。
  **禁止用 `uv run` / 平台 venv 启动掘金桥**——那会把桥拉进项目 venv，下次
  `uv sync` 会把 gm 删掉，桥真断（Core 只 HTTP 访问 7801，从不 import gm，
  平台 venv 无 gm 不影响桥与 Core）。
- 引擎键 `goldminer`；桥策略必须保持运行（掘金终端里不停，终端关闭 = 桥停），
  停止 = 平台报单 rejected（expire 兜底）。

## 测试注意事项

- 后端测试需要 `conftest.py` 提供测试数据库 fixtures（内存 SQLite）
- 前端测试在 `web/src/__tests__/` 目录
- 引擎模块的单元测试应使用 Mock 数据，无需连接通达信
- 集成测试需小数据集端到端验证
## 修复与部署协作规则（2026-11-04 起）

- **代码修复后只做冒烟测试**：运行本次改动涉及的聚焦 pytest（`uv run pytest core/tests/unit/test_<涉及模块>.py`）/ `npx vitest run <相关测试>` 确认 pass 即止。
- **禁止由 AI 执行重启/部署**：杀后端进程、启动 `uvicorn`、重开实盘会话一律由用户择时手动执行（启动预热与 TQ 调用会冻结通达信客户端，AI 不掌握用户盘中节奏）。
- **AI 汇报格式**：改动文件清单 + 冒烟测试结果 + 「需要重启生效」标注；用户重启后如有验证需求，只做只读 API/日志检查。
- **全量测试**（`uv run pytest core/tests/unit/`，~80s，部分用例触真实 TQ）同样会卡通达信——仅在用户明确要求或停会话窗口执行。
- **文档与代码同一轮收口**：修复任务凡涉及配置项、参数、接口或库结构变更，必须同步更新 [docs/用户使用说明.md](docs/用户使用说明.md)（配置指南：键名、默认值、取值范围、实盘/回测生效范围、界面入口）与本文件对应规则，不得只改代码；迁移 revision 与新增库列要写进该文档「九、系统配置」。
