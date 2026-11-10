# 打包与安装器

面向 Windows 目标机（**无需安装 Python**）。采用 **Nuitka standalone（onedir，非 onefile）**
+ **Inno Setup 6** 安装包。

## 日常策略

**平时源码运行、不打安装包；只在稳定节点出包。** 源码开发用 `manage.ps1 start`
（uvicorn 8001 + vite 5173），改代码热更新，与安装包无关。

出包按改动范围走三条路径，只有正式发版才需要 Nuitka 全量重编：

| 场景 | 命令 | 量级 |
|---|---|---|
| 只改前端（文案/样式/交互） | ①`cd web; npm run build` ②`python main\build_package.py --web-only` ③`ISCC packaging\installer_web.iss` | 秒级复制 + 十秒级打包，补丁包约 3 MB |
| 只改后端（Python） | ①`python main\build_package.py`（复用导入分析缓存）②`ISCC packaging\installer.iss` | 仍需完整 Nuitka 构建（见下方说明），建议集中到稳定节点/夜间 |
| 正式发版 / 缓存可疑 | ①`python main\build_package.py --clean`（清缓存）②`ISCC packaging\installer.iss` | 全量 2h 级 |

```powershell
# 只改前端（推荐，无需 Nuitka）
cd web; npm run build; cd ..
main\.venv\Scripts\python.exe main\build_package.py --web-only
& "C:\Users\<用户名>\AppData\Local\Programs\Inno Setup 6\ISCC.exe" packaging\installer_web.iss

# 只改后端（增量编译，不要加 --clean）
main\.venv\Scripts\python.exe main\build_package.py
& "C:\Users\<用户名>\AppData\Local\Programs\Inno Setup 6\ISCC.exe" packaging\installer.iss

# 正式出包（全量重编，换版本号前先改 packaging\app-version.iss）
main\.venv\Scripts\python.exe main\build_package.py --clean
& "C:\Users\<用户名>\AppData\Local\Programs\Inno Setup 6\ISCC.exe" packaging\installer.iss
```

> **Nuitka 缓存的真实边界（本机实测）**：`dist\build\nuitka-build\module-cache` 只缓存「导入分析」
> （依赖扫描结果），**不缓存 C 代码生成与编译**。实测同一模块、同一构建目录连续编译两次：
> 首次 19.7s → 再编 15.0s → 15.0s / 14.3s，基本无差别；Nuitka 4.2.2 官方也明确没有增量编译
> （`--devel-recompile-c-only` 仅供 Nuitka 自身开发）。因此：
> - 默认**保留缓存**（旧脚本每次清空，白丢依赖扫描结果），只省依赖扫描的几分钟，**不会**把 2h 变成分钟级；
> - 后端改动仍按「稳定节点集中出包」处理，日常源码运行（`manage.ps1 start`）；
> - 只有改 `--include-package` 等构建参数或产物诡异（旧代码生效/启动崩溃）时才加 `--clean`
>   （清 `module-cache` + `desktop.build`，下一次回到全量）。
>
> **进一步提速（可选，需在构建机自行验证）**：
> ①把 `dist\build`、`main\.venv` 加入 Windows Defender 实时扫描排除项——C 编译产物多、文件碎，
> 杀软实时扫描常数倍拖慢（需管理员操作，属机器配置不在仓库内）；
> ②内存充足时可试去掉 `build_package.py` 里的 `--low-memory`（该参数会减少并行 C 编译任务数），
> 若出现 OOM 再恢复——本机 20 核 / 16 GB，是否安全需实际跑一次观察。
>
> **若要让后端也做到分钟级**，只有换分发形态才成立，且都需要一次完整构建窗口做验证，暂不启用：
> ①把变化最频繁的 `core` 包排除出 Nuitka 编译（`--nofollow-import-to=core` + 外置源码目录 +
> `desktop.py` 注入 `sys.path`），改后端只替换 `.py`——代价是源码在安装目录明文可见；
> ②改用 CPython embed + `.pyc` 分发（不再有 C 编译，构建分钟级）——代价是放弃 Nuitka 的启动性能与
> 与既有安装包的兼容性，属于发行形态变更，需单独评审。
>
> **分层原理**：前端 `web/dist` 是明文静态文件（`_internal\web\dist`），与 exe 无关，
> 因此改前端只需替换这个目录。`--web-only` 只做「清旧 dist + 复制新 dist + 更新 build-info.json」，
> 不触发 Nuitka；`installer_web.iss` 只打包这几 MB 前端文件，且**不删除** `_internal` 其它内容。
>
> **同一台机器手工更新**：也可以不装补丁包，直接
> `robocopy web\dist "<安装目录>\_internal\web\dist" /MIR`（先关程序），效果相同。
>
> **版本号唯一来源**：`packaging\app-version.iss`（`build_package.py` 与两个 `.iss` 共用）。
> 前端补丁包沿用当前版本号，不单独升版本；正式发版才 bump。
>
> ISCC 路径视 Inno Setup 安装位置而定（winget 装到 `%LOCALAPPDATA%\Programs\Inno Setup 6\`）。
> 官方安装包**不自带**简体中文语言文件，`packaging\lang\ChineseSimplified.isl` 随仓库提供
> （来源：Inno Setup 官方 issrc `Files\Languages\`，版本 6.5.0+，已转 UTF-8 BOM）。

## 版本、变更清单与回滚

- **版本号**：只改 `packaging\app-version.iss` 一处，`build_package.py` 编译出的 exe 属性、
  两个安装包文件名/卸载项都随之变化。
- **构建档案**：每次出包在 `dist\tq-iquant\build-info.json`（随安装包落到安装目录根）写入
  `app_version` / `build_mode`（full-clean / full-incremental / web-only）/ `build_time` /
  `git_commit` / `git_dirty` / `frontend_dist_mtime`。装到目标机后可直接核对「这版到底是什么」。
  建议每版把一行摘要记到 [CHANGELOG.md](CHANGELOG.md)。
- **回滚**：
  1. 历史安装包都在 `dist\installer\`，**不要删**；回滚 = 直接运行上一版全量安装包
     （同 AppId 覆盖安装，`data/`、`logs/`、`config.yaml`、`bridge/` 均保留）。
  2. 前端单独回滚：重新 `npm run build`（切回旧 git 提交的前端源码）→ `--web-only` → 补丁包。
  3. **数据库迁移不可回退**：Alembic 迁移只前进。跨版本回滚前先备份
     `<安装目录>\data\dev.db`（或 `%LOCALAPPDATA%\创懿基金量化投资平台\data\dev.db`），
     旧版 exe 遇到新 schema 可能直接报错。
  4. 出包前的旧 `dist\tq-iquant` 会被覆盖，需要保留旧分发目录时先手工改名备份。

## 产物与约定

| 路径 | 说明 |
|---|---|
| `dist/tq-iquant/` | onedir 分发目录：`tq-iquant.exe` + `_internal/`（含 `web/dist` 前端）+ `alembic/` + `config.template.yaml` + `build-info.json` |
| `dist/installer/` | Inno 安装包：全量 `创懿基金量化投资平台-<版本>-<时间戳>-安装程序.exe`；前端补丁 `…-前端更新包.exe` |
| `dist/build/` | Nuitka 中间产物与模块缓存（不入库；`nuitka-build` 为增量缓存，勿随手删） |

- 安装位置默认 `D:\创懿基金量化投资平台`，安装需管理员权限；**程序日常运行不提权**。
- 数据根目录：优先 `<安装目录>`（需可写），不可写时自动回落
  `%LOCALAPPDATA%\创懿基金量化投资平台`。`data/`（SQLite、同花顺交接文件）、
  `logs/`、`config.yaml` 都在该根目录下；首次启动从 `config.template.yaml` 生成
  `config.yaml`，此后用户修改不会被覆盖。
- 卸载默认**保留**数据目录（卸载时可选删除）；升级安装不触碰 `data/`。
- `bridge\` 目录随包附带**明文**桥脚本（`iquant_bridge.py` 8790 股票账户、
  `goldminer_bridge.py` 7801）与部署说明；桥脚本**不编译进 exe**，必须在
  QMT 客户端 Python 3.6.8 / 掘金终端内运行。信用账户腿 `iquant_bridge_live.py`（8791）
  保留在仓库但不随包。
- VC++ 运行库（`msvcp140.dll` / `msvcp140_1.dll` / `vcruntime140*.dll`）从构建机
  `System32` 补齐到分发根目录，目标机无需预装 VC++ 2015-2022 Redistributable。
- 不要求代码签名，因此首次运行会触发 SmartScreen 提示（已知取舍）。

## 授权/许可证机制的预留位置

本轮不含任何授权校验。将来引入时，唯一侵入点是**启动校验 + 写操作中间件**：

1. **启动校验**：`main/desktop.py::main()` 在 `_start_server()` 之前插入校验，
   校验失败直接弹窗并退出，不启动后端（WebView2/浏览器均拿不到界面）。
2. **写操作中间件**：`main/core/main.py` 的 FastAPI app 上挂一个全局依赖
   （或 `@app.middleware("http")`），统一拦截 `POST/PUT/PATCH/DELETE`，
   校验通过后放行；读接口与 SSE 不受影响。

这样授权逻辑不侵入业务路由与引擎，现有结构无需为将来改动。
