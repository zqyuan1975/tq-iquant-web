# 创懿私募基金量化投资平台 — AGENTS.md

## 项目状态

**已实现（脚手架 + 主链路已通）** — 24 个模型文件、48 个引擎源码文件、9 个 API 路由模块、10 个前端视图、41 个 Alembic 迁移均已落地；回测/实盘主链路已接线。完整实现状态见 [CLAUDE.md](CLAUDE.md)「实现状态」小节。本文档记录业务规则与设计依据，实现细节以代码与 CLAUDE.md 为准。

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

## 开发范式

**TDD**：先写测试，再写实现。pytest（后端）+ vitest（前端）。

**开发顺序**（在 `docs/system-plan-draft.md` 第 10 章有完整依赖图）：

1. 基础设施 → 2. 数据层 → 3. TQ 模块 → 4. 核心引擎 → 5. 回测 → 6. 实盘 → 7. 收尾

## API 规范

- 基础路径：`/api`
- 无鉴权，API 直接可访问
- 统一返回格式：`{ "code": 0, "message": "ok", "data": { ... } }`（code=0 成功，非 0 错误）
- 44 个 HTTP 接口 + 1 个 WebSocket，9 组（详见设计文档 5.6 节）
- 实盘实时推送通过 SSE（`GET /api/live/sessions/{id}/stream`），非轮询

## 数据库（SQLite）

详见设计文档 5.4 节。关键点：
- 纯单用户本地工具，统一用 SQLite（`main/data/dev.db`），零配置，不切 PostgreSQL
- `config.yaml`（项目根目录）存储系统路径配置，无数据库密码（SQLite 本地文件）
- 每日快照 `backtest_daily_snapshots` 是评估指标的原始数据来源
- `backtest_evaluations` 18 个指标由 Evaluator 从快照序列计算
- `backtest_records.params_snapshot` 冻结回测时的策略参数，确保结果可复现
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
- **TQ 数据传递**：polars DataFrame 进程内传递，通过 tqcenter SDK 直连运行中通达信
- **Core↔交易通道通信**：QMT 通道走 HTTP 桥（`127.0.0.1:8790`，账号 410038214257 / `STOCK`；QMT 是四条生产通道之一、平台按生产标准维护，但**该账户是 QMT 仿真账户、账户内为仿真资金**，不是真实资金；切真实资金须改桥文件 `ACCOUNT` / `ACCOUNT_TYPE`（必要时 `TICK_TAG` / `DRY_RUN`）并重跑实盘验证。`iquant_bridge_live.py` 的 `8791` + 信用账户 `CREDIT` 保留但**非生产、未启用**），掘金走 HTTP 桥（`127.0.0.1:7801`）；桥策略受理下单/查单/持仓/资金，QMT 桥另提供行情拉取（掘金行情由平台走通达信快照合成）。端点见 [live/bridge/README.md](live/bridge/README.md)
- **信号优先级**：风控信号（止损/止盈/移动止损）> 公式信号（OPEN/ADD/REDUCE/CLOSE）
- **ADD 加仓阈值特殊值**：`add_position_threshold = -1` 表示跳过回撤检查，只要有 ADD 信号即加仓（含上涨/大涨）；`max_add_count` 加仓次数上限仍生效。正常取值 1%~20%（逢跌加仓，现价较成本下跌 ≥ 阈值才加）。0 表示"不涨即加"（drop ≥ 0）。风控优先级仍高于 ADD（涨到止盈点先止盈清仓，不加）
- **资金模型**：策略资金占比是持仓上限（非预分），多策略上限之和可超过 100%
- **T+1 约束**：回测强制 T+1，实盘实时查可用股票
- **熔断规则**：max_drawdown 触发 → 次日恢复（累计触发 3 次后转手动恢复）；daily_loss_limit 触发 → 当日暂停，次日恢复。熔断期间不清仓，仅暂停新开仓
- **主从策略**：从策略只能买入主策略当前持有的同一只股票；主策略清仓（含该股）后从策略不可新开仓但存量可自行卖出
- **一个 5m bar 可能同时触发多个周期**（如 10:30 同时触发 5m+30m+60m 公式）
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
| `live/bridge/` | HTTP 桥策略：`iquant_bridge.py`（QMT 通道生产桥，`127.0.0.1:8790`，账号 410038214257 / `STOCK`，**在用**；账户为 **QMT 仿真账户/仿真资金**，切真实资金须改 `ACCOUNT` / `ACCOUNT_TYPE` 并重跑实盘验证）/ `iquant_bridge_live.py`（`8791` + 信用账户 `CREDIT` 410009993017，**非生产、未启用**，仓库保留以维持镜像护栏）/ `goldminer_bridge.py`（掘金量化，掘金终端内运行，`127.0.0.1:7801`）。QMT 与掘金、同花顺、通达信并列为四条生产通道；Core 的 `simulation` / `live` 两个模式位均指向 8790 |
| `web/` | Vue 3 + Vite 前端 |
| `config.yaml` | 系统配置（TDX 路径、`ths_ui` / `tq_trade` / `goldminer` / `iquant_bridge` 交易通道） |

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
