# 0017 多引擎并发与多实例部署：现状核查与方案

> 性质：只读核查 + 方案设计，未改任何代码/配置，未启停会话、未下单。
> 核查时间：2026-11-10。

## 0. 结论速览

| 问题 | 结论 |
|---|---|
| 一个后端同时跑多个 live session？ | **能**，但仅限「跨引擎」；每会话一个 LiveEngine + 独立 dispatcher/BarPoller |
| 同一引擎开两个会话？ | **被 409 硬拦**（`_check_channel_conflict`），现状不是「可能踩」而是「不允许」 |
| tdx 与 ths 能否同时开？ | **技术上能，实际不建议盘中同时下单**：共用一把进程内 UI 锁，TDX 确认预算会被 THS 占锁吃掉 |
| SQLite 多线程并发写？ | 已由 WAL + `busy_timeout=5000` + 每 tick 短事务吸收，风险点为「长写事务 > 5s」 |
| SQLite 多进程并发写？ | **不可共享同一 db**：应用启动会无条件把 DB 里 running 会话置 stopped，会互杀 |
| 封装版能否多开？ | 默认**不能**（端口被占即复用已有实例并退出）；无 AppMutex，靠 `TQ_PORT` + 独立 `TQ_APP_ROOT` 可做「便携多开」 |
| 两个进程能同时用通达信 TQ？ | **不能**：协议层只允许一个 TQ 在线（已知约束，非未知项），第二进程无法持有 tqcenter 连接 |
| 推荐路线 | **优先跨机部署（每台机器一套完整栈）**；次选单机多实例（需明确划分 TQ 持有者）；A（单引擎多开）搁置 |

---

## 1. 现状核查

### 1.1 多会话并发：支持，粒度是「会话」

`ENGINES` 是进程级注册表，key 为 `session_id`：

- [live_service.py#L72-L75](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L72-L75)（`ENGINES: Dict[int, LiveEngine] = {}`，注释明确「进程级 dict，单用户系统」）
- [live.py#L41](file:///d:/project/tq-iquant-web/main/core/api/live.py#L41)（`_ENGINES = svc.ENGINES`，API 层直接引用同一对象）

引擎实例**按会话创建**，不是全局单例：

- [live_service.py#L748-L760](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L748-L760) `build_engine(session_id, db, ...)` → 一个 LiveEngine
- [live_service.py#L781](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L781) `dispatcher = make_dispatcher(engine, mode, session_id)`
- [live_service.py#L937-L947](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L937-L947) 每个会话一个新 BarPoller
- 启动串行化：[live.py#L207-L242](file:///d:/project/tq-iquant-web/main/core/api/live.py#L207-L242)（`_START_LOCK` 覆盖「幂等检查→建引擎→注册→commit」）

**共享状态清单（关键）**：

| 状态 | 位置 | 粒度 | 多会话影响 |
|---|---|---|---|
| `ENGINES` | live_service.py:72-75 | 进程 | 注册表，本身安全 |
| `_START_LOCK` | api/live.py:32 | 进程（asyncio） | start/stop 串行，安全 |
| `_BACKTEST_LOCK` | backtest_service | 进程 | 回测串行；与实盘不同资源 |
| dispatcher | 每会话 `make_dispatcher` | **会话** | 同引擎两会话→两个对象指向同一桥/同一柜台 |
| `_pending_orders` | live_engine.py:846-852（委托 OrderStateMachine） | **会话** | 不跨会话 |
| `_book_lock` | live_engine.py:691 | **会话 RLock** | 不跨会话 |
| `_deals_executor` | live_engine.py:681-683 | **会话 1 worker** | 不跨会话 |
| `_known_refs` / `_submit_times` | tdx_tq_dispatcher.py:191、195 | **dispatcher 实例（会话）** | 同引擎两会话各一份 → 互把对方委托当「陌生 ref」 |
| TQ 单例 `_tq` | tq/utils.py:17、59-77 | **进程单连接** | 所有 tdx/ths/goldminer 会话共用一个 tqcenter 连接 |
| `get_tdx_lock()` | tq/utils.py:13、21 | **进程单锁** | 所有 TQ 调用（含其它会话）串行 |
| `UI_AUTO_LOCK` | live/win_dialog_scope.py:27 | **进程单锁** | tdx 与 ths 的键鼠段落全局串行 |
| THS `_GRID_LOCK` | ths_ui_dispatcher.py:130 | 类级（进程） | 同进程 THS 实例共享 |
| OCR 引擎 | ths_ui_dispatcher.py:43 | 进程 | 无状态，安全 |
| `_ACTIVE_WATCHDOG` | live/engine_watchdog.py:49、191 | 进程 | 单例看门狗，监控全部会话 |
| DB engine | db.py:28-31 | 进程 | 每 tick 新建 Session，短事务 |

补充：**账号不是会话维度**。`live_sessions` 没有任何账号字段（[live_session.py#L10-L49](file:///d:/project/tq-iquant-web/main/core/models/live_session.py#L10-L49)），账号来自全局配置/桥常量：

- tdx：`config.yaml` 的 `tq_trade.account`（[config.yaml#L61](file:///d:/project/tq-iquant-web/config.yaml#L61)）→ [live_service.py#L184](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L184)
- goldminer：`goldminer.account_id`（config.yaml:15）→ live_service.py:163
- iquant/goldminer 桥：账号是**桥文件常量** `ACCOUNT`（[goldminer_bridge.py#L46](file:///d:/project/tq-iquant-web/live/bridge/goldminer_bridge.py#L46)），Core 传不进去

### 1.2 同引擎两会话：现状是硬拦（409），不是「可能踩」

[live_service.py#L1290-L1320](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L1290-L1320)：

- 注释即设计：「同一引擎（=同一交易通道/同一真实账户）同时仅允许一个运行中会话」
- tdx/iquant 的理由写明：「双会话 = 两套虚拟账本交易同一真实账户（双份记账/互相吃单/快照对齐失真）」
- 返回 409 拒绝，`start_session` 在 [L1353-L1355](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L1353-L1355) 直接返回该错误
- 文档口径一致：[用户使用说明.md#L275](file:///d:/project/tq-iquant-web/docs/用户使用说明.md#L275)

**若把这道护栏拿掉**，实际会发生什么（按引擎）：

- **iquant / goldminer**：两个 dispatcher 各自 new，但 `base_url` 来自同一份全局配置（[live_service.py#L281-L307](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L281-L307)）→ 同一桥、同一 `ACCOUNT`。两个会话各自按「全账户资产」起账虚拟现金 → 双方都高估可用资金 → 超买。认领侧 remark 是 oid 前 20 位，**撞键护栏只在单 dispatcher 内检查**（[http_bridge_dispatcher.py#L428-L443](file:///d:/project/tq-iquant-web/main/core/engine/http_bridge_dispatcher.py#L428-L443)），跨会话撞键不会被拦，但两会话 oid 大概率不同，故不会互相认领；真正的错在账本，不在认领。
- **tdx**：`TQ` 是进程单例（tq/utils.py:59-77），两个 dispatcher 共用同一 `account_id`（各自 `connect()` 查同一个全局账号）；`_known_refs`/`_submit_times` 各一份 → 会互把对方委托当陌生 ref 补记 fabricated，导致 legacy 匹配降级/漏认；T+1 可用量读的是同一账户 → 可能双卖。
- **ths**：两个 dispatcher 都按 `window_title` 定位**同一个 xiadan 窗口**（两次启动会抢同一个 UI），文件交接目录按 `{work_dir}/{session_id}` 隔离（ths_ui_dispatcher.py:246），但**提交都落到同一个登录账号**；`_GRID_LOCK` 与 `UI_AUTO_LOCK` 只保证不交叉键入，不保证账本正确。
- **同账号 vs 不同账号**：现状根本无法「不同账号」（账号不是会话属性）；即便改造加字段，桥端仍是「一桥一账号一端口」的物理约束，同端口第二个桥会被 `SO_EXCLUSIVEADDRUSE` 拒绝（[iquant_bridge.py#L57-L60](file:///d:/project/tq-iquant-web/live/bridge/iquant_bridge.py#L57-L60)）。

### 1.3 tdx + ths 同时开：能开，但 TDX 确认会退化

互斥机制到底是什么（[win_dialog_scope.py#L1-L33](file:///d:/project/tq-iquant-web/main/core/engine/live/win_dialog_scope.py#L1-L33)）：

- 一把**进程级 `threading.RLock`**（`UI_AUTO_LOCK`），不是按窗口/按账号多把锁
- 作用：**只串行化「物理键鼠段落」**，不禁止两个通道同时存在；弹窗归属另由 `dialog_matches_channel()` 按进程映像名 fail-closed 过滤（win_dialog_scope.py:73-93）
- 明确限定作用域：**只在同一后端进程内互斥**（ths_ui_dispatcher.py:891-893 注释「另一个后端进程…都不会排队」）

实际代价（代码级）：

1. THS 每次下单分两段持锁，单段目标 8–10s：[ths_ui_dispatcher.py#L2539-L2586](file:///d:/project/tq-iquant-web/main/core/engine/ths_ui_dispatcher.py#L2539-L2586)（注释原文「单次占锁压到 8–10s，避免双引擎并行时长期饥饿 TDX 的 5s 确认预算」）。持仓/委托读表也整段持锁：[L1591-L1607](file:///d:/project/tq-iquant-web/main/core/engine/ths_ui_dispatcher.py#L1591-L1607)、[L1655-L1678](file:///d:/project/tq-iquant-web/main/core/engine/ths_ui_dispatcher.py#L1655-L1678)。
2. TDX 弹窗确认 `deadline = time.time() + timeout` **先于** 取锁计算，然后 `with UI_AUTO_LOCK:` 无限期等锁：[tdx_tq_dispatcher.py#L1220-L1223](file:///d:/project/tq-iquant-web/main/core/engine/tdx_tq_dispatcher.py#L1220-L1223)。等锁超过 5s → 进循环时 deadline 已过 → 直接 `False`。
3. 确认失败不是拒单，而是走「受理未认领」分支返回 `None`：[tdx_tq_dispatcher.py#L1334-L1365](file:///d:/project/tq-iquant-web/main/core/engine/tdx_tq_dispatcher.py#L1334-L1365)，订单保持 submitted，最坏 180s expire；客户端「交易信号」窗口可能残留未发送行，需人工取消。
4. THS 侧反向也有风险：`UI_LOCK_TIMEOUT = 60s`（ths_ui_dispatcher.py:883），超时 fail-closed（不点击不键入），单子走 expire。

`file_builtin` 解耦到哪一步：

- 解耦的是**调用链**（place_order 只写 `orders.jsonl` 秒回 None：ths_ui_dispatcher.py:2014-2038），后台执行器仍是**每 dispatcher 一个 daemon 线程**（[L2089-L2092](file:///d:/project/tq-iquant-web/main/core/engine/ths_ui_dispatcher.py#L2089-L2092)），执行时照样抢 `_GRID_LOCK` → `UI_AUTO_LOCK`。
- 未解耦的是**物理资源**：同一个桌面、同一套键鼠、同一个 `xiadan.exe` 窗口。

**结论**：能同时开，且跨引擎并行是当前设计目标；但「tdx 与 ths 都在盘中自动下单」会让 TDX 确认经常落到「受理未认领 → 180s expire」，实际是可用性事故。行情/信号/风控不受影响（行情走快照合成，不碰 UI 锁）。

### 1.4 SQLite 健壮性

现状（[db.py#L28-L48](file:///d:/project/tq-iquant-web/main/core/db.py#L28-L48)）：

- `create_engine(..., connect_args={"check_same_thread": False})`，**未显式设连接池参数**（默认 QueuePool size=5 / overflow=10）
- 逐连接 PRAGMA：`foreign_keys=ON`、`journal_mode=WAL`、`busy_timeout=5000`
- 每请求一个 Session：[db.py#L84-L89](file:///d:/project/tq-iquant-web/main/core/db.py#L84-L89)；LiveEngine 每个 tick 新建 Session 并短事务 commit（[live_engine.py#L1453](file:///d:/project/tq-iquant-web/main/core/engine/live_engine.py#L1453)、L1647、L1825 等 20+ 处）

**多线程写（同进程）**：`_loop` 与 `_deals_loop` 两池 + 回测内联。写事务都是「查到就改就 commit」的短事务，WAL 允许读不阻塞写，`busy_timeout=5000` 是写锁重试窗口。真实风险只有一种：**某次写事务超过 5s**（回测批量落库、大结果集写入），此时并发写会拿到 `database is locked`。目前靠「回测串行 + 短事务」压住，没有分库/重试逻辑。

**多进程写（共享 db）**：SQLite 只允许单写者，跨进程靠 `busy_timeout` 排队，无应用层互斥。两个更硬的问题：

1. **启动恢复会互杀**：[main.py#L49-L55](file:///d:/project/tq-iquant-web/main/core/main.py#L49-L55) 冷启动时**无条件**把所有 `status=running` 的 live session 置 stopped（依据是「本进程注册表必为空」——这个前提在多实例下不成立）。第二个实例一启动就把第一个实例正在跑的会话标成 stopped，工作台显示停止，而引擎还在真实下单。回测/批量任务的同类恢复（main.py:40-48）同理。
2. **Alembic 并发**：[db.py#L51-L81](file:///d:/project/tq-iquant-web/main/core/db.py#L51-L81) 每次启动 `command.upgrade(cfg, "head")`，无跨进程锁；已到 head 时是只读 no-op（安全），但**有待执行迁移时两实例同时升级**会竞争 DDL / `alembic_version`（busy_timeout 只能降低概率）。
3. 附带影响：`live_sessions` 等表没有实例归属列，两实例共享 db 时彼此可见 → 上述互杀无解，除非加 owner 列。

**结论**：多线程没问题；**多进程必须一实例一 db 文件**（见方案 B）。

### 1.5 端口与封装

| 项 | 现状 | 证据 |
|---|---|---|
| dev 后端端口 | **硬编码 8001**（代码里没有端口配置，靠启动参数） | [manage.ps1#L7](file:///d:/project/tq-iquant-web/manage.ps1#L7)、[#L95](file:///d:/project/tq-iquant-web/manage.ps1#L95) |
| 前端 proxy | 硬编码 `http://localhost:8001` | [vite.config.ts#L8](file:///d:/project/tq-iquant-web/web/vite.config.ts#L8) |
| 封装版端口 | `TQ_PORT` 或默认 8888；被占则 8889…8898 递增；**若该端口已有本程序在跑 → 直接复用并退出** | [desktop.py#L24](file:///d:/project/tq-iquant-web/main/desktop.py#L24)、[#L55-L67](file:///d:/project/tq-iquant-web/main/desktop.py#L55-L67)、[#L160-L162](file:///d:/project/tq-iquant-web/main/desktop.py#L160-L162) |
| 单实例锁 | **无 Mutex**，靠「端口 + /health」软判断 | desktop.py:55-67；installer.iss 全文无 `AppMutex` |
| 数据目录 | 封装版：exe 同级（可写）否则 `%LOCALAPPDATA%\创懿基金量化投资平台`；可用 `TQ_APP_ROOT` 强制指定 | [config.py#L52-L81](file:///d:/project/tq-iquant-web/main/core/config.py#L52-L81) |
| 安装目录 | 固定 `D:\创懿基金量化投资平台`，AppId 固定（升级沿用），`data/`、`logs/` 在 app 目录 | [installer.iss#L16-L21](file:///d:/project/tq-iquant-web/packaging/installer.iss#L16-L21)、[#L48-L51](file:///d:/project/tq-iquant-web/packaging/installer.iss#L48-L51) |
| 桥端口 | 写死在桥文件常量：8790/8791（QMT）、7801（掘金）；Core 侧从 config.yaml 读 base_url | [goldminer_bridge.py#L49](file:///d:/project/tq-iquant-web/live/bridge/goldminer_bridge.py#L49)、[config.yaml#L4-L13](file:///d:/project/tq-iquant-web/config.yaml#L4-L13) |
| THS 交接目录 | `{ths_ui.work_dir}/{session_id}`（相对路径按进程 CWD 解析） | ths_ui_dispatcher.py:246、config.yaml:35 |
| 日志 | `app_root()/logs/core.log`（RotatingFileHandler） | [logging_config.py#L27-L36](file:///d:/project/tq-iquant-web/main/core/logging_config.py#L27-L36) |

**封装版多开现状**：双击第二个快捷方式 → 首选端口 8888 已被第一个实例健康占用 → `already=True` → 打开窗口后**自身直接退出**（desktop.py:160-162、188-190）。即「默认单实例」；只有显式 `TQ_PORT=8889` 才会起第二个进程，但两者共用同一 `app_root`（同一 db/配置/日志/THS 工作目录）→ 落到 1.4 的互杀。

**额外共享资源（多实例的硬边界）**：tdx/ths/goldminer/alert 四个引擎的**盘中行情都走通达信快照合成**（[live_service.py#L910-L940](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L910-L940)），而**通达信在协议层只允许一个 TQ 在线**——第二个进程无法持有 tqcenter 连接。所以单机多实例不是「共用是否安全」的问题，而是**根本不允许第二个进程持有 TQ**。三条应对路径见 §2.3。

---

## 2. 方案

### 2.1 方案 A：单引擎多开（同一平台内，同一引擎多会话，各填不同账号）

需要贯穿的改造（按工作量升序）：

| # | 改造 | 说明 | 风险 |
|---|---|---|---|
| A1 | `live_sessions` 增加 `account_id`/`account_type` | Alembic 迁移；唯一约束 `(engine, account_id)` 且「同账号仅一个运行中会话」 | 低 |
| A2 | dispatcher 接受会话级账号 | `make_dispatcher` 增加 account 参数，config 只作默认；tdx 需在每次调用前按账号切 `account_id`（`TQ` 仍是进程单例，靠 `get_tdx_lock` 串行） | 中：TQ 的 `stock_account()` 切换与缓存一致性需实测 |
| A3 | 桥侧「账号 → base_url」映射 | 桥的 `ACCOUNT` 是文件常量，一账号必须一桥一端口（`SO_EXCLUSIVEADDRUSE`），config 增加 account→url 表，桥文件按账号复制 N 份 | 高：桥文件必须手工镜像复制，且每份跑在不同终端实例里 |
| A4 | 认领键全局唯一 | remark 撞键检查从「单 dispatcher」上移到「进程级」 | 低 |
| A5 | 账户快照按账号聚合 | 现按 `live_session_id`（live_account_snapshot.py:25）；同账号多会话会各自采集同一账户 → 外部仓重复导入 | 中 |
| A6 | UI 通道按账号隔离窗口 | THS 一个 `xiadan.exe` 实例 = 一个登录账号；`window_title` 相同 → 必须按进程路径/账号区分窗口定位 | 高：需要真机校准，且用户手工操作会串 |
| A7 | 撤销 `_check_channel_conflict` 的同引擎拦截 | 改成「同 (engine, account) 拦，不同 account 放行」 | 低（但一旦 A6 没做好，UI 通道会真串） |

**判定：不建议**。收益只有一个界面看全部账户；成本是 6 处贯穿改造 + 桥/终端多开，且最终仍受「一登录终端一账户」的物理约束，最坏情况下（同账号两会话）依然必须禁止。

### 2.2 方案 B：单机多开平台实例（次选：仅在无法加机器时使用）

原则：**引擎维持现状（一实例内同引擎仍只允许一个会话），把隔离放到进程外**；同时必须按 §2.3 解决「谁持有 TQ」。

#### B1 端口规划（建议编号）

| 实例 | 平台端口（封装/源码） | QMT 桥 | 掘金桥 | 前端 |
|---|---|---|---|---|
| 1 | 8888 / 8001 | 8790（现状） | 7801（现状） | 封装版由 FastAPI 托管；源码 5173 |
| 2 | 8889 / 8002 | 8800（桥文件 PORT 改） | 7802（桥文件 PORT 改） | 源码 5174 |

规则：平台端口 +1；桥端口「实例序号 ×10 + 基址」或直接顺序分配 8800/8801、7802/7803。**不要**复用 8791（那是保留的信用腿）。

#### B2 数据库分离（必须独立，不共享）

- 每实例一个可写根：`TQ_APP_ROOT=<实例目录>`（[config.py#L67-L69](file:///d:/project/tq-iquant-web/main/core/config.py#L67-L69) 第一优先级，源码与封装版都生效），`data/dev.db`、`logs/`、`config.yaml`、`data/ths/` 随之各自独立（[db.py#L18-L21](file:///d:/project/tq-iquant-web/main/core/db.py#L18-L21)）
- **不接受共享 db + 实例键**：启动恢复会互杀（main.py:49-55），加上 Alembic 并发升级，改造量大于收益
- 代价：跨实例看不到对方会话/回测历史；备份要备 N 份（可用同一父目录 + 定时 robocopy 汇总）

#### B3 配置分离

- 每实例一份 `config.yaml`：`database.sqlite_path`、`ths_ui.work_dir`（**改绝对路径**，避免两个实例 CWD 相同导致 `{work_dir}/{session_id}` 撞目录）、`iquant_bridge.*.base_url`、`goldminer.base_url`、`tq_trade.account`
- 桥文件按实例复制并改顶部常量（`PORT` / `ACCOUNT` / `ACCOUNT_TYPE` / `TICK_TAG`），每个桥跑在**各自的终端进程**里

#### B4 引擎归属（避免抢同一终端）

| 资源 | 约束 | 规避 |
|---|---|---|
| 通达信 TQ（行情快照 + tdx 下单） | **协议层只允许一个 TQ 在线（已知约束）**：第二个进程无法持有 tqcenter 连接 | 整机只让**一个**实例持有 TQ，承载 tdx/ths/goldminer/alert；另一实例只跑 iquant（QMT 桥自带行情） |
| THS `xiadan.exe` | 一个窗口=一个登录账号，`window_title` 定位 | 同一时刻只允许一个实例跑 ths；如需两账号，需两套客户端 + 按进程路径定位窗口（成本高，暂不做） |
| QMT / 掘金桥 | 一桥一端口一账号 | 每实例各自桥端口，互不监听 |

#### B5 封装版多开

- 现状：无 AppMutex；同安装目录 + 默认端口 → 第二实例直接退出复用
- 可选做法（择一）：
  1. **便携多开（推荐，零安装改造）**：把 `dist/tq-iquant` 复制两份，分别用 `TQ_PORT` / `TQ_APP_ROOT` 启动（可加 `启动-实例2.bat`）
  2. **安装器多实例模式**：`DefaultDirName` 允许用户改（`DisableDirPage=no`）、安装器写两个快捷方式并带环境变量；**不加 AppMutex**（加了就彻底不能多开）
- 日志/备份随 `TQ_APP_ROOT` 自动隔离（logging_config.py:27-36），无需改代码

#### B6 改造清单（按工作量与风险排序）

| 优先级 | 改造 | 工作量 | 风险 |
|---|---|---|---|
| P0-1 | 平台端口可配：源码运行也支持 `TQ_PORT`/`--port`（现仅封装版支持）；`vite.config.ts` proxy 改读 `VITE_API_TARGET` | 小 | 低 |
| P0-2 | 启动脚本/文档化「实例目录 + TQ_APP_ROOT + TQ_PORT」 | 小 | 低 |
| P0-3 | `ths_ui.work_dir` 绝对化 + 校验两实例不共享 | 小 | 低 |
| P0-4 | 桥文件按实例复制 + 端口/账号编号表（运维文档 + 校验脚本：`/ping` 自述端口/账号比对 config） | 小 | 中（人工易错，靠 `/ping` 校验） |
| P1-1 | 实例级「允许引擎集」校验：启动会话时校验 `live.allowed_engines`，越权 409 并给中文提示 | 中 | 低 |
| P1-2 | 日志/事件加 `instance` 标识（便于两实例并排查错） | 小 | 低 |
| P2-1 | 跨实例只读汇总（导出/汇总库） | 中 | 低 |
| P2-2 | 若单机多实例且不止一个实例需要 TQ：按 §2.3 路径 2（sidecar）或路径 3（主从广播）改造 | 中～中偏高 | 中（路径 3 强耦合主实例存活） |

### 2.3 TQ 单实例约束下的三条路径（单机多实例场景）

前提（已知约束）：**通达信协议层只允许一个 TQ 在线**，第二个进程无法持有 tqcenter 连接。因此单机多实例的问题不是「并发访问是否安全」，而是「谁持有 TQ」。三条路径按改造成本排序：

#### 路径 1（零改造，先落地）：一机一个 TQ 持有者

- 持有 TQ 的实例承载 tdx / ths / goldminer / 预警（这四个引擎的盘中行情都靠快照合成，见 [live_service.py#L910-L940](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L910-L940)），并可承担 tdx 下单与 THS UI 自动化（键鼠、`UI_AUTO_LOCK` 也都在这一进程内，天然不跨进程竞争）。
- 另一实例**只跑 iquant**：QMT 桥自带实时 bar（`poller = BarPoller(dispatcher, ...)`，[live_service.py#L945-L949](file:///d:/project/tq-iquant-web/main/core/services/live_service.py#L945-L949)），不碰 TQ。
- 成本：零代码。限制：TQ 侧能力全部集中在持有者；另一实例不能开 tdx/ths/goldminer/预警，且若持有者停机，另一实例无法临时接管这些引擎。
- 落地要点：靠 `live.allowed_engines`（P1-1）把「谁不能开什么」变成硬校验，而不是靠运维记忆。

#### 路径 2（中等改造）：行情服务化 sidecar

- 独立 sidecar 进程**唯一持有 TQ**，对外（127.0.0.1）提供快照 / 合成 1m bar / 可选下单代理；各平台实例从它取数，不再 `import tqcenter`。
- `get_tdx_lock` 随之上移：TQ 调用集中到 sidecar，进程内单锁变成「sidecar 内部串行 + 客户端侧连接复用」。
- 改造面：抽出 `SnapshotBarSynthesizer` / `MarketDataService` 的取数边界做成可替换数据源（[snapshot_bars.py#L85-L104](file:///d:/project/tq-iquant-web/main/core/engine/live/snapshot_bars.py#L85-L104)）、定义 sidecar 协议、`TdxTqDispatcher` 的下单与查单同样走 sidecar（下单本身也是 TQ 调用）、`UI_AUTO_LOCK` 与弹窗处置随 tdx/ths 落到 sidecar 进程。
- 收益：所有实例平等获取行情，不再需要「引擎归属划分」；缺点：sidecar 成为单点（需要看门狗 + 自动重启），本机 loopback 时延可接受。

#### 路径 3（中偏高）：主实例持 TQ，向从实例广播合成 bar

- 主实例照旧持有 TQ 并合成 bar，把完成 bar 通过本机通道（HTTP/SSE/进程间队列）推给从实例；从实例完全不碰 TQ，只消费 bar。
- 缺点：**强耦合主实例存活**——主实例挂掉时从实例没有任何行情，风控/信号停摆；且只解决行情，**不解决 tdx 下单**（从实例仍不能用 tdx）。
- 实现量：bar 广播协议 + 断线重连 + 时序一致性（同一 bar 在两侧只处理一次），中偏高。

**取舍**：先按路径 1 落地（零改造、可立即用）；只有当「一机多实例且多个实例都需要 TQ 行情」成为硬需求时，才评估路径 2（推荐）或路径 3（不推荐，主从耦合）。

### 2.4 跨机部署（推荐的多开形态）

**结论：需要多开就多准备机器。** 每台机器一套完整栈：平台实例 + 独立 `data/dev.db` + 本机桥（QMT/掘金）+ 该机唯一的 TQ。机器之间不共享任何运行时资源，只共享「同一份代码版本」。

这样自动消除的风险：

| 单机多实例的问题 | 跨机后为什么消失 |
|---|---|
| 共享 db 互杀：冷启动把 `running` 会话置 stopped（[main.py#L40-L55](file:///d:/project/tq-iquant-web/main/core/main.py#L40-L55)） | 每机独立 `dev.db`，恢复逻辑只作用于本机库，看不到也改不到别机的会话 |
| 平台端口冲突（8001 / 8888、`TQ_PORT` 递增） | 平台端口都是本机 `127.0.0.1`，两台机器可完全同号，零端口规划 |
| 桥端口 `SO_EXCLUSIVEADDRUSE` 冲突（8790 / 7801） | 桥绑本机环回，两台机器各绑各的 8790/7801，互不可见 |
| TQ 单实例约束（协议层只允许一个 TQ 在线） | 每机只有一个平台进程持有本机通达信 TQ，天然满足 |
| Alembic 迁移跨进程并发升级 | 每机的 `alembic upgrade head` 只对本地库，不存在并发 DDL |
| 进程内互斥锁（`UI_AUTO_LOCK` / `get_tdx_lock`）跨进程无效 | 每机的 UI 自动化与 TQ 调用都只在本进程内，锁语义恢复完整 |

跨机形态下**仍然存在**的运维问题：

1. **无跨机统一报表**：各机独立库，跨账户汇总需要导出/汇总侧工具（可在各机导出，汇总端合并）。
2. **备份分散**：每机各自备份（可内嵌到平台里，平台已计划内嵌备份功能）；需要统一的保留策略与校验（备份可恢复性）。
3. **时钟需 NTP 对齐**：交易信号、bar 边界、委托时间匹配都按 Asia/Shanghai 判定（`sent_at ±45s` 类匹配见 [tdx_tq_dispatcher.py#L1288-L1294](file:///d:/project/tq-iquant-web/main/core/engine/tdx_tq_dispatcher.py#L1288-L1294)），机器间时钟漂移会直接影响认领与风控时点。
4. **版本需一致**：所有机器部署同一 commit（含 Alembic 迁移链一致），升级时按机滚动、逐台验证，避免「新库旧代码」。
5. 运维可见性：每机一份 `logs/core.log`，排障需按机定位（可在 P1-2 加实例标识）。

### 2.5 推荐

**优先级：跨机（每台一套）> 单机多实例（路径 1 划分 TQ 持有者）> A 路线（搁置）**。

理由：

1. 跨机把「TQ 单实例、桥端口独占、db 单写者、UI 锁仅进程内」四条约束一次性变成非问题，没有新增代码。
2. 单机多实例只在「无法加机器」时使用，且必须接受路径 1 的能力划分（一个实例不能跑 tdx/ths/goldminer）或承担路径 2/3 的改造。
3. A（单引擎多开）改造成本最高（见 §2.1）且收益仅是「一个界面看多账户」，在跨机形态下更无必要。
4. 现有护栏（`_check_channel_conflict`）已经把「一引擎=一账户」写成不变量；B/跨机顺着不变量做隔离，A 要拆掉它。

---

## 3. 需要用户决策的点

### 3.1 已定（2026-11-10 用户确认，不再讨论）

| 决策项 | 结论 |
|---|---|
| 多开形态 | **跨机优先**：需要多开就多准备机器，每台一套完整栈（平台 + 独立 `dev.db` + 本机桥 + 唯一 TQ）；详见 §2.4 |
| 数据库 | **一实例一 db 文件**，不共享、不加实例键（跨机形态下天然独立） |
| TQ 持有者 | **每机只允许一个进程持有 TQ**（协议层约束），单机多实例须按 §2.3 路径 1 划分引擎归属 |
| A 路线（单引擎多开） | **搁置**，不作为近期方案 |

### 3.2 仍未定（需要拍板）

1. **桥端口编号规则**（仅在「单机多实例」或「同机第二套桥」场景需要）：用「实例序号 → 桥 8800/7802 起顺序分配」，还是沿用 8790/8791 语义（信用腿）另辟区间？跨机场景可直接沿用 8790/7801，不涉及编号。
2. **封装版是否需要「单机多实例模式」**：跨机优先后这可能不再是必需项——若确认不需要，则保持现状（默认单实例、端口被占即复用），不做便携/多目录改造；若需要，选「便携目录复制（零安装改造）」还是「安装器支持多目录（`DisableDirPage=no` + 双快捷方式，且不能加 AppMutex）」。
3. **同实例内 tdx + ths 是否继续允许同时自动下单**：现状允许但会退化（TDX 确认可能 unconfirmed → 180s expire）。建议加启动告警或直接 409 禁止，需确认取哪一种。
4. **THS 双账号**：是否有真实需求？如有，需要两套同花顺客户端 + 按进程路径定位窗口（当前按 `window_title` 定位），属于独立项目量级，需单独立项。
