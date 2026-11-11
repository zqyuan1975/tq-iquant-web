import os
import sys
import threading
from pathlib import Path
from typing import Optional

import yaml


def is_frozen_app() -> bool:
    """是否为打包后的独立程序（exe）。

    Nuitka 与 PyInstaller 不同：standalone 编译后**不会**设置 sys.frozen，
    必须额外用 argv[0] 是否为 .exe 判断；否则所有"包内资源定位"都会错误
    回退到 __file__ 推算路径（实测导致读到空配置、tdx_path 回退错误目录）。

    pytest 下 argv[0] 可能是 pytest.exe，需显式排除，避免测试误判为打包环境。
    """
    if os.environ.get("PYTEST_CURRENT_TEST") is not None or "pytest" in sys.modules:
        return False
    return hasattr(sys, "frozen") or Path(sys.argv[0]).suffix.lower() == ".exe"


def _install_dir() -> Path:
    return Path(sys.argv[0]).resolve().parent


def use_app_root() -> bool:
    """相对路径（data/dev.db、data/ths）是否锚定到 app_root()。

    打包版恒为 True；源码运行时默认锚 main/（历史行为），
    仅当显式设置 TQ_APP_ROOT（冒烟测试/便携）时改锚该目录。
    """
    return is_frozen_app() or bool(os.environ.get("TQ_APP_ROOT"))


def _is_writable_dir(path: Path) -> bool:
    """实际写探测（创建目录 + 写删临时文件），比 os.access 可靠。"""
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write_probe"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
        return True
    except OSError:
        return False


_APP_ROOT: Optional[Path] = None


def app_root() -> Path:
    """应用可写根目录：data/、logs/、config.yaml 都落在这里。

    优先级：
    1. 环境变量 TQ_APP_ROOT（冒烟测试/便携场景显式指定）
    2. 打包版：exe 同级目录（安装目录对当前用户可写时）
    3. 打包版回落：%LOCALAPPDATA%\\创懿基金量化投资平台
       （装到 Program Files 或 D:\\ 根目录且普通用户无写权限时，
       绝不把 SQLite/配置写到只读目录）
    4. 源码运行：仓库根目录（main/ 的上一级）
    """
    global _APP_ROOT
    if _APP_ROOT is not None:
        return _APP_ROOT

    env = os.environ.get("TQ_APP_ROOT")
    if env:
        _APP_ROOT = Path(env).expanduser().resolve()
    elif is_frozen_app():
        base = _install_dir()
        if _is_writable_dir(base / "data"):
            _APP_ROOT = base
        else:
            local = os.environ.get("LOCALAPPDATA") or str(Path.home())
            fallback = Path(local) / "创懿基金量化投资平台"
            _is_writable_dir(fallback / "data")
            _APP_ROOT = fallback
    else:
        _APP_ROOT = Path(__file__).resolve().parent.parent.parent
    return _APP_ROOT


def _default_config_path() -> Path:
    if use_app_root():
        return app_root() / "config.yaml"
    return Path(__file__).resolve().parent.parent.parent / "config.yaml"


def ensure_config() -> Path:
    """首次启动生成 config.yaml（从打包附带的模板复制，模板只读不覆盖用户配置）。"""
    path = _default_config_path()
    if path.exists():
        return path
    template = None
    candidates = []
    if is_frozen_app():
        candidates.append(_install_dir() / "config.template.yaml")
    candidates.append(Path(__file__).resolve().parent.parent.parent / "config.template.yaml")
    candidates.append(
        Path(__file__).resolve().parent.parent.parent / "packaging" / "config.template.yaml"
    )
    for c in candidates:
        if c.exists():
            template = c
            break
    path.parent.mkdir(parents=True, exist_ok=True)
    with _CONFIG_WRITE_LOCK:
        if template is not None:
            path.write_text(template.read_text(encoding="utf-8"), encoding="utf-8")
        else:
            path.write_text(
                yaml.dump(_defaults(), default_flow_style=False, allow_unicode=True),
                encoding="utf-8",
            )
    return path


def get_config_path() -> Path:
    return _default_config_path()


def _defaults() -> dict:
    return {
        "tdx_path": "",
        "iquant_path": "",
        "max_concurrent_backtest": 1,
        "database": {
            "sqlite_path": "data/dev.db",
        },
        # 内嵌数据备份（core/services/backup_service.py，进程内 daemon 调度）。
        # 每日 daily_at（Asia/Shanghai，默认 15:10 收盘后）用 VACUUM INTO 生成
        # 一致性副本并执行一次 wal_checkpoint(TRUNCATE)；自动备份只保留最近 keep 份，
        # 手动备份不参与自动清理；剩余空间低于 min_free_mb 跳过（不写坏库）。
        "backup": {
            "enabled": True,
            "daily_at": "15:10",
            "keep": 10,
            "min_free_mb": 500,
            "dir": "data/backup",
        },
        "iquant_bridge": {
            "simulation": {"base_url": "http://127.0.0.1:8790"},
            "live": {"base_url": "http://127.0.0.1:8791"},
            # 桥请求超时（秒，= 读/写/池超时）：桥内全局锁/GIL 排队下 /ping 也要
            # 5~8s，15s 留足余量；超时按在途处理，不判拒单。缺省/非法回退该值
            # （live_service.iquant_config）。建连超时见 live.bridge_connect_timeout_seconds。
            "request_timeout_seconds": 15.0,
            # 慢响应告警阈值（秒）：须明显小于请求超时；超阈值记 WARNING + risk/bridge_slow。
            "slow_response_seconds": 5.0,
            # 同一会话慢告警冷却窗（秒），避免轮询刷屏。
            "slow_warn_interval_seconds": 300.0,
            # 委托价格类型（下发给桥的 /order.price_type）：
            #   opponent（默认，桥侧 prType=14 对手价——与改动前逐字一致）
            #   limit    （prType=11，Core 审批价原样进 passorder，桥内不改写/不跟价）
            #   market_best5（prType=42 沪 / 47 深，五档即时成交剩余撤销）
            # 默认值刻意保持旧行为；切真实资金前改 limit 即可，无需改代码。
            "price_type": "opponent",
        },
        # 掘金量化桥（2026-11-04 替代 QMT 桥）：桥策略跑在掘金终端内
        # （live/bridge/goldminer_bridge.py），base_url 指向其本地 HTTP 服务。
        "goldminer": {
            "base_url": "http://127.0.0.1:7801",
            "token": "",
            "account_id": "",
            # 桥请求超时（秒）：桥内全局锁 + gm C SDK 持 GIL 串行，实测单发
            # 8~13s、多请求叠加可到 20s+；45s 既覆盖并发叠加又小于主循环 60s
            # （超时后按在途处理，不判拒单）。旧的 30s 贴着实测上界，易误超时
            # 导致客户端提前断连（桥侧 WinError 10054 刷屏）。
            "request_timeout_seconds": 45.0,
            # 慢响应告警阈值（秒）：须明显小于请求超时；超阈值记 WARNING +
            # risk/bridge_slow 事件（按 endpoint 冷却去重）。
            "slow_response_seconds": 15.0,
            # 同一 endpoint 慢告警冷却窗（秒），避免 5s 轮询刷屏。
            "slow_warn_interval_seconds": 300.0,
        },
        "ths_ui": {
            "enabled": False,
            "window_title": "网上股票交易系统5.0",
            "client_exe_path": "D:/同花顺远航版/transaction/xiadan.exe",
            "driver": "clipboard",
            "captcha_ocr": False,
            # 委托价模式：approval=键入审批价（信号价）限价，委托价≠审批价自动撤单；
            # latest=不键价格，让 THS 代码键入后自动跟出最新价（日线信号跨日跳空补救）。
            "price_mode": "approval",
            # 文件交接层：file_builtin=信号/风控/指令落 JSONL + 内置后台执行器按
            # 频率执行（place_order 秒回）；direct=同步 UI 自动化直呼（原行为）。
            "execute_mode": "file_builtin",
            "execute_interval_seconds": 5,
            # 交接目录（相对路径按服务进程 CWD=main/ 解析），实际文件在
            # {work_dir}/{session_id}/ 下。
            "work_dir": "data/ths",
        },
        # 实盘引擎存活监控（进程内 EngineWatchdog，单位秒；非法/非正回退默认）。
        # heartbeat_timeout_seconds：引擎 tick/账户快照双信号静默阈值；
        # execution_stall_seconds：ths 文件交接层积压/cursor 停滞阈值；
        # bridge_timeout_seconds：HTTP 桥持续离线阈值；
        # market_data_stall_seconds：tdx 最后一根 bar 停滞阈值。
        "live": {
            "heartbeat_timeout_seconds": 90,
            "execution_stall_seconds": 180,
            "bridge_timeout_seconds": 90,
            "market_data_stall_seconds": 180,
            # HTTP 桥客户端超时（秒；通道未显式配 request_timeout_seconds 时生效）：
            # 读/写/池默认 15（掘金桥 /ping 实测 5~8s，10s 贴太近会误判断链）；
            # 建连默认 3（本机 loopback，无需等满读超时）。/order /orders /deals
            # /positions /account /price_limit /cancel /quote 共用同一 client。
            "bridge_read_timeout_seconds": 15.0,
            "bridge_connect_timeout_seconds": 3.0,
            # 成交价偏离监控：桥通道回填成交价相对审批价的绝对偏离超过该百分比（%）
            # 记 risk 事件（gate/rule=fill_price_deviation）；<=0 关闭监控。
            "fill_price_deviation_pct": 1.0,
            # 行情轮询窗口（Asia/Shanghai，左闭右开）：窗口外（交易日 15:05 之后 /
            # 次日 09:15 之前 / 非交易日）完全停 BarPoller 行情拉取，消除收盘后对
            # 掘金桥 /quote 与通达信 TQ 的无谓持续压力；账户快照/成交回填/收盘清扫
            # 不受影响。enabled=false 恢复旧行为（全天轮询）。缺该键 → 安全默认
            # （即下列值：收盘后停轮询）。
            "quote_polling": {
                "enabled": True,
                "resume_time": "09:15",
                "pause_time": "15:05",
            },
        },
        "tq_trade": {
            "account": "",
            "account_type": "stock",
            "price_mode": "my",
            "client_path": "",
            # A-6：新建实盘会话未显式选引擎时的默认交易引擎（用户主通道 tdx）。
            "default_engine": "tdx",
            # A-2：在途单超时未成交自动撤单（分钟）；<=0 = 禁用。
            "cancel_unfilled_minutes": 3,
            # 次日盘前（集合竞价）执行日线级信号单：默认开启（对齐回测「次日开盘
            # 执行」口径）。关闭 → 行为与旧版一致（deferred 单等次一交易日首根完成
            # 1m bar——约 09:31——放行，用首根 bar 价格报单）。
            # 开启 → 交易日 09:15–09:20（end_time 可配，左闭右开）把「当日 due 的
            # deferred 日线信号单」直接报进集合竞价（柜台接受竞价期委托，实测
            # 竞价期可成交/可撤单）。不依赖任何 1 分钟 bar，不需要快照合成。
            # 价位规则见 live_engine.pre_open_auction_price_bias_pct 注释。
            "pre_open_auction_enabled": True,
            "pre_open_auction_end_time": "09:20",
            # 盘前委托价相对「信号价（前一日 deferred 单记录的 price，即信号 bar
            # 收盘价）」的让价幅度（%）：买 +bias / 卖 −bias，再夹到当日涨跌停
            # 之内（涨停价−1 跳 / 跌停价+1 跳，避免被本地涨跌停校验直接拦下）。
            # 不推到涨停价/跌停价：集合竞价按统一开盘价成交，限价只作上限；
            # 但未成交的余量会以该限价挂在连续竞价簿上，推到涨跌停等于全天追价。
            "pre_open_auction_price_bias_pct": 2.0,
            # 卖出范围口径（止损/止盈/移动止损与 SELL/REDUCE/CLOSE 公式卖单）：
            #   full          （默认）按整仓卖出——与改动前逐字一致，行为零变化
            #   platform_only 只卖平台自己买入的股数（Position.platform_quantity）；
            #                 纯外部/手工仓（platform_quantity=0）不下单，只记一条
            #                 gate=sell_scope 决策事件。买入侧不受影响。
            # 阈值判定（止损/止盈/回撤）仍按整仓成本，本项只收窄卖出数量。
            "live_sell_scope": "full",
            # 错周期 bar 是否跑风控（全局默认，会话可用
            # live_sessions.intraday_mismatched_risk_enabled 三态覆盖）：
            # False（默认，对齐回测）→ 风控只在策略自身周期 bar 上评估：1m/5m 等
            # 错周期 bar 不再触发止损/止盈/移动止损（周线/月线同样只在自身边界）。
            # True → 旧行为：任何 bar 都按收盘价评估持仓风险。
            "intraday_mismatched_risk_enabled": False,
            # 09:31 deferred 兜底放行的「保证成交」开关：False（默认，可复现优先）
            # → 限价 = 当日首根 1m bar 开盘价（夹涨跌停、quantize 到分）；True →
            # 不按首根 open 定价，改用通道默认对手价，优先成交而非价格可复现。
            "deferred_release_ensure_fill": False,
        },
        # 接缝续跑式增量回测：组合策略从接缝日（进入实盘那天）接着往后延，
        # 每天只跑新交易日，与实盘逐日对照；结果按特征级容差与全量重跑对齐。
        "incremental_backtest": {
            "enabled": True,
            # 每日续跑触发时刻（Asia/Shanghai）：晚于 DailyCloser 的 15:05 日线取数，
            # 直接复用那轮已取到的日线，不再额外卡通达信行情。
            "daily_at": "15:10",
            # 后端某天没开/续跑失败时，下次启动从最后 checkpoint 逐日补齐的上限
            # （交易日）。超过上限标「断档」并在对照表标红，同时从接缝日重建。
            "max_catchup_days": 5,
            # 接缝锚定（默认开）：回测用实盘该组合的起账状态重锚——现金=虚拟起账
            # 现金、存量仓按成本注入、highest_price 按成本种下，两边同起点同分母。
            # 关闭 → 用回测自身在接缝日的状态（空账户从接缝日起跑）。
            "anchor_on_live_start": True,
            # 每周双跑校验：0=周一 … 5=周六（默认）。到点后从接缝日全量重算对账。
            "validate_weekday": 5,
            "validate_at": "18:00",
            # 续跑复权口径（front/back/dual），默认 dual（前复权信号 + 后复权记账）。
            # 该值参与参数快照哈希，变更即作废 checkpoint 并从接缝日重跑。
            "dividend_mode": "dual",
            # 双跑校验的特征级容差（判据依据见 incremental_backtest 服务注释）。
            "tolerances": {
                # 逐日净值相对偏差（%）上限：取整/费用/过户费/T+1 边界噪声远小于此
                "max_net_dev_pct": 0.5,
                "mean_net_dev_pct": 0.2,
                # 逐日成交笔数一致率 / 方向一致率下限（允许极少数边界单跨日漂移）
                "trade_count_match_ratio": 0.9,
                "direction_match_ratio": 0.9,
                # 最大回撤偏差（百分点）：回撤是路径极值，对单日噪声更敏感
                "max_drawdown_dev_pct": 1.0,
            },
        },
    }


def _deep_merge(base: dict, override: dict) -> dict:
    """递归合并：override 覆盖 base，嵌套 dict 逐层合并而非整体替换。"""
    merged = base.copy()
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = _deep_merge(merged[k], v)
        else:
            merged[k] = v
    return merged


def load_config(config_path: Optional[str] = None) -> dict:
    path = Path(config_path) if config_path else _default_config_path()
    if not path.exists():
        return _defaults()
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return _deep_merge(_defaults(), cfg)


_CONFIG_WRITE_LOCK = threading.Lock()


def save_config(cfg: dict, config_path: Optional[str] = None) -> None:
    """原子写配置（临时文件 + os.replace）并串行化并发写。

    直接 open("w") 会先截断原文件：进程在写入中崩溃/被强杀，config.yaml
    变成空文件，下次加载静默回退全默认（tdx_path/iquant_path 等丢失）。
    保存路由跑在线程池，并发请求互相截断还会写出错乱内容 → 进程内写锁。
    """
    path = Path(config_path) if config_path else _default_config_path()
    text = yaml.dump(cfg, default_flow_style=False, allow_unicode=True)
    with _CONFIG_WRITE_LOCK:
        tmp = path.with_name(path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
