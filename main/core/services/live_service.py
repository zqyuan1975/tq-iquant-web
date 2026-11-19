"""实盘 service 层 — 业务逻辑（从 core.api.live 提取）。

与 core.services.backtest_service 同一范式：路由层仅留 HTTP 壳（参数校验 +
守卫/调度 + 调 service + ok/err 包装），业务逻辑下沉到本模块。本模块承接：

- 引擎组装：build_engine（查 link/PS/strategy/股票池/Formula，算 code_period_count，
  建 dispatcher/poller/LiveEngine）
- 进程注册表：ENGINES（session_id -> LiveEngine）+ B6 资源互斥守卫
  （M1 多会话并行：仅 THS 通道键鼠独占，tdx/iquant 可并行，组合重叠=复制交易）
- session CRUD：list/create/get/delete + start/stop 业务体
- 熔断手动恢复：recover_breaker（运行中双写 / 未运行只改 DB）
- 历史查询 + 序列化：orders/trades/positions（运行中读内存，停止后从 live_trades 重放）

SSE 流（_sse_line/_heartbeat_stream/session_stream）是 HTTP 传输层关注点，留路由。
asyncio 任务调度（create_task(engine.start/stop)）同样留 async 路由——本模块的
start_session 只返回构造好、heartbeat 通过、recover 已执行但尚未 start() 的引擎，
由路由负责调度与注册。

纯重构，行为不变。
"""

import json
import logging
import math
import re
import threading
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Dict, List, Optional, Tuple

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from core.config import load_config
from core.db import SessionLocal
from core.runtime import process_tag
from core.models import (
    LiveSession, LiveSessionPortfolio, PortfolioStrategy, Strategy,
    StockPoolStock, StockPoolMembership, Formula, FormulaSignal, LiveOrder, LiveTrade, LiveDecisionEvent,
    LiveSignal, LiveAccountSnapshot, LiveCashFlow, LiveBaselineReset,
    LiveManualAction, LiveReconciliationDiff, LiveReconciliationReport,
    LiveOutOfPoolDecision, LiveCorporateAction,
    LivePortfolioDaily,
    LiveGateSnapshot,
    LivePositionPeak,
    LiveSessionPendingChange,
    LiveGlobalSwitch,
    IncrementalBacktestCheckpoint, BacktestRecord,
    BacktestDailySnapshot, BacktestTrade,
)
from core.engine.portfolio_builder import assemble_portfolio
try:
    from core.engine.http_bridge_dispatcher import HttpBridgeDispatcher
except Exception:
    HttpBridgeDispatcher = None  # type: ignore
try:
    from core.engine.ths_ui_dispatcher import ThsUiDispatcher
except Exception:
    ThsUiDispatcher = None  # type: ignore
try:
    from core.engine.tdx_tq_dispatcher import TdxTqDispatcher
except Exception:
    TdxTqDispatcher = None  # type: ignore
from core.engine.bar_poller import BarPoller
from core.engine.live_engine import LiveEngine
from core.engine.live.breaker import manual_reset_reanchor_enabled
from core.engine.position import Position
from core.engine.format import dec_money
from core.engine.decision import summarize_decisions
from core.engine.crisis_alpha import DEFAULT_BENCHMARK_CODE
from core.engine.market_gate import gate_enabled_for
from core.engine.event import TradeEvent
from core.tq.formula import TQFormula
from core.tq.alert import TQAlert
from core.engine.live.alert_dispatcher import AlertDataDispatcher
from core.engine.execution_engine import ShadowDispatcher
from core.engine.live.market_data import MarketDataService
from core.engine.live.fallback_quote import FallbackQuoteDispatcher
from core.engine.live.timing import now_shanghai
from core.services.live_data_health import (
    health_summary_text,
    run_health_check,
)
from core.services.signal_schedule import (
    effective_signal_frequency, validate_signal_config,
)
from tq_iquant_shared.constants import TradeType

logger = logging.getLogger(__name__)

# 进程内运行中的 LiveEngine 注册表：session_id -> LiveEngine。
# 实盘引擎需长期持有 dispatcher/poller/portfolio 内存态，不能按请求建销，
# 故放进程级 dict（单用户系统，无需分布式协调）。
ENGINES: Dict[int, LiveEngine] = {}


# ---------------------------------------------------------------------------
# 桥配置 / 股票池 / 引擎组装
# ---------------------------------------------------------------------------

def iquant_config() -> dict:
    """读 QMT 通道桥配置（config.iquant_bridge 段）。

    simulation / live 两个模式位当前都指向 8790 的 QMT 仿真账户（410038214257 / STOCK，账户内为仿真资金），各一个
    base_url；桥绑 loopback、无鉴权。超时/慢告警阈值与掘金桥同构配置，
    缺省回退（本机 loopback 桥默认 10s / 5s / 300s），不写死在调用点。
    """
    cfg = load_config()
    br = cfg.get("iquant_bridge", {}) if isinstance(cfg, dict) else {}
    br = br if isinstance(br, dict) else {}
    sim = br.get("simulation", {}) if isinstance(br.get("simulation"), dict) else {}
    liv = br.get("live", {}) if isinstance(br.get("live"), dict) else {}
    return {
        "available": HttpBridgeDispatcher is not None,
        "simulation": (sim.get("base_url")
                       or "http://127.0.0.1:8790"),
        "live": (liv.get("base_url")
                 or "http://127.0.0.1:8791"),
        "request_timeout_seconds": _positive_float(
            br.get("request_timeout_seconds"), 15.0),
        "slow_response_seconds": _positive_float(
            br.get("slow_response_seconds"), 5.0),
        "slow_warn_interval_seconds": _positive_float(
            br.get("slow_warn_interval_seconds"), 300.0),
    }


def bridge_config() -> dict:
    """读桥地址：simulation/live 两个模式位当前都指向 8790 的 QMT 仿真账户。

    8791 信用腿（CREDIT）保留但非生产、未启用，见 live/bridge/README.md。
    模拟/实盘（信号 vs 真实下单）不由 Core 控制，由 QMT 客户端启动按钮决定。
    """
    q = iquant_config()
    return {"simulation": q["simulation"], "live": q["live"]}


def ths_ui_config() -> dict:
    """读同花顺 UI 通道配置（config.ths_ui 段），缺省值对齐 core.config._defaults()。"""
    cfg = load_config()
    ths = cfg.get("ths_ui", {}) if isinstance(cfg, dict) else {}
    ths = ths if isinstance(ths, dict) else {}
    return {
        "enabled": bool(ths.get("enabled")),
        "available": ThsUiDispatcher is not None,
        "window_title": ths.get("window_title") or "网上股票交易系统5.0",
        "client_exe_path": ths.get("client_exe_path") or "",
        "driver": ths.get("driver") or "clipboard",
        "captcha_ocr": bool(ths.get("captcha_ocr")),
        # 委托价模式（2026-11-07 修：此前该配置项从未透传给 dispatcher，
        # config.ths_ui.price_mode 是死配置；实盘恒走 approval）
        "price_mode": ths.get("price_mode") or "approval",
        # 文件交接层双模式（file_builtin=内置执行器 / direct=同步直呼）
        "execute_mode": ths.get("execute_mode") or "file_builtin",
        "execute_interval_seconds": int(ths.get("execute_interval_seconds") or 5),
        "work_dir": ths.get("work_dir") or "data/ths",
    }


def _positive_float(value, default: float) -> float:
    """配置数值安全解析：缺失/非法/<=0 一律回退默认（配置问题不得抛异常）。"""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if v > 0 else default


def goldminer_config() -> dict:
    """读掘金量化桥配置（config.goldminer 段）。

    桥策略（live/bridge/goldminer_bridge.py）跑在掘金终端内，本平台经
    HttpBridgeDispatcher 以 QMT 桥同构协议访问其本地 HTTP 服务。
    """
    cfg = load_config()
    g = cfg.get("goldminer", {}) if isinstance(cfg, dict) else {}
    g = g if isinstance(g, dict) else {}
    return {
        "available": HttpBridgeDispatcher is not None,
        "base_url": g.get("base_url") or "http://127.0.0.1:7801",
        "token_set": bool(g.get("token")),
        "account_id": g.get("account_id") or "",
        # 桥请求超时（默认 30s：≥30 且 < 主循环 60s；超时按在途处理不判拒单）
        "request_timeout_seconds": _positive_float(
            g.get("request_timeout_seconds"), 30.0),
        # 慢响应告警阈值（默认 15s，明显小于超时；超阈值 risk/bridge_slow）
        "slow_response_seconds": _positive_float(
            g.get("slow_response_seconds"), 15.0),
        # 慢告警窗口（默认 300s）：同一会话每窗口最多一条，窗口内后续慢请求
        # 累计成计数与最慢值，在窗口过期后的首条里汇总（防多端点各报一条）
        "slow_warn_interval_seconds": _positive_float(
            g.get("slow_warn_interval_seconds"), 300.0),
    }


def tq_trade_config() -> dict:
    """读通达信 TQ 交易配置（config.tq_trade 段）。"""
    cfg = load_config()
    tq = cfg.get("tq_trade", {}) if isinstance(cfg, dict) else {}
    tq = tq if isinstance(tq, dict) else {}
    return {
        "available": TdxTqDispatcher is not None,
        "account": tq.get("account") or "",
        "account_type": tq.get("account_type") or "stock",
        "price_mode": tq.get("price_mode") or "my",
        "client_path": tq.get("client_path") or "",
        "auto_confirm_dialog": bool(tq.get("auto_confirm_dialog", True)),
        # 「交易信号」自动确认单笔总时限（秒）：定位窗口+OCR+点击+等委托表认领。
        "confirm_timeout": float(tq.get("confirm_timeout") or 12.0),
    }


def live_sell_scope() -> str:
    """卖出范围口径（config.tq_trade.live_sell_scope）。

    full（默认，缺键/非法值回退）= 风控与公式卖单按整仓卖出，行为与旧版一致；
    platform_only = 只卖平台自己买入的部分（Position.platform_quantity），纯外部/
    手工仓不代卖。回测不走本函数（assemble_portfolio 默认 full）。
    """
    cfg = load_config()
    tq = cfg.get("tq_trade", {}) if isinstance(cfg, dict) else {}
    value = str((tq.get("live_sell_scope") if isinstance(tq, dict) else "") or "").strip()
    if value in ("full", "platform_only"):
        return value
    if value:
        logger.warning(
            "config.tq_trade.live_sell_scope=%r 非法（仅支持 full/platform_only），回退 full",
            value,
        )
    return "full"


def _pct_or_none(value) -> Optional[Decimal]:
    """比例型护栏字段归一：None / 0 / 负数 / 非数字 → None（不设限）。

    ``portfolio_strategies.max_single_position_pct`` / ``min_cash_reserve_pct``
    的默认语义：「NULL 或 0 一律视为不设限」，保证未填写时与接线前行为逐字一致。
    超出 (0, 1] 的值按原值使用（>1 等价不约束，但保留用户填写值便于审计）。
    """
    if value is None:
        return None
    try:
        dec = Decimal(str(value))
    except (TypeError, ValueError, ArithmeticError):
        return None
    if not dec.is_finite() or dec <= 0:
        return None
    return dec


#: 实盘交易引擎：key -> (渠道标识, 中文名)。渠道标识是 Core 状态机/前端识别的通道名。
#: 四引擎：QMT 通道 / 掘金量化桥 / 通达信 TQ / 同花顺 UI 自动化。
ENGINES_META = {
    "goldminer": ("goldminer_bridge", "掘金量化（HTTP 桥）"),
    "tdx": ("tdx_tq", "通达信 TQ 交易"),
    "ths": ("ths_ui", "同花顺独立交易（UI 自动化）"),
    "iquant": ("iquant_bridge", "QMT 通道"),
}


def default_engine() -> str:
    """未显式选引擎时的默认值：tq_trade.default_engine 优先（A-6，用户主通道 tdx）；
    未配置/空/非法值时沿用旧行为（ths_ui.enabled → ths，否则 iquant）。"""
    cfg = load_config()
    tq = cfg.get("tq_trade", {}) if isinstance(cfg, dict) else {}
    value = str((tq.get("default_engine") if isinstance(tq, dict) else "") or "").strip()
    if value in ENGINES_META:
        return value
    ths = ths_ui_config()
    return "ths" if (ths["enabled"] and ths["available"]) else "iquant"


def session_engine(session) -> str:
    """session 生效引擎：显式值优先；NULL（升级前的历史会话）回退全局默认。

    全局默认自 A-6 起为 tq_trade.default_engine（SystemConfig 可配，默认 tdx）；
    未配置时沿用旧行为（ths_ui.enabled → ths，否则 iquant）。
    qmt 通道已移除：历史 qmt 会话的显式值不再进白名单，同样回退默认引擎。
    **不自动改指 iquant**——miniQMT 与 QMT 通道是不同终端与不同账户，
    静默改指可能把委托打到非预期账户；需要切换请显式修改会话 engine。
    """
    explicit = getattr(session, "engine", None)
    return explicit if explicit in ENGINES_META else default_engine()


# ---------------------------------------------------------------------------
# 资金基准（2026-11-16 实盘资金基准对齐）
# ---------------------------------------------------------------------------

def session_baseline_value(session_row) -> Optional[float]:
    """会话资金基准：baseline_value 优先，回退旧列 live_capital。"""
    if session_row is None:
        return None
    for attr in ("baseline_value", "live_capital"):
        value = getattr(session_row, attr, None)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def latest_account_total_asset(db: Session, session_id: int) -> Optional[float]:
    """最近一次账户快照的真实总资产（供「只缩不放」的 min 截断与重置锚点）。"""
    try:
        row = (
            db.query(LiveAccountSnapshot)
            .filter(LiveAccountSnapshot.live_session_id == session_id)
            .order_by(LiveAccountSnapshot.id.desc())
            .first()
        )
    except Exception:  # noqa: BLE001 — 取不到按无数据，不阻断启动
        return None
    if row is None:
        return None
    try:
        total = float(row.total_asset or 0)
    except (TypeError, ValueError):
        return None
    return total if total > 0 else None


def resolve_effective_baseline(db: Session, session_row, session_id: int) -> Optional[float]:
    """起账基准 = min(填写基准, 真实总资产)——只缩不放，且**只在未起账时截断一次**。

    填写值超过真实总资产时，起账按真实总资产截断（写入侧另由 API 拒绝）；
    无填写值/无真实快照时返回填写值（可能为 None → 沿用各组合 initial_capital）。

    P0（2026-11-18）：基准一经设定（``baseline_set_at`` 非空）必须冻结，后续重启
    只重读账本。旧实现每次 build_engine 都拿「当前最新账户快照总资产」重新截断，
    而快照总资产随盘中持仓市值波动 → 每次重启都把虚拟现金重新锚到当日的真实
    总资产，表现为「重启一次账本变一个数」（实测同一会话柜台可用恒 10,414.71，
    虚拟现金 7,464.19 → 7,134.19 → 8,183.19 → 6,942.19）。需要重设基准只能走
    reset_baseline（会递增 baseline_version 并重捕接缝）。
    """
    value = session_baseline_value(session_row)
    if value is None or value <= 0:
        return None
    if getattr(session_row, "baseline_set_at", None) is not None:
        return value
    total = latest_account_total_asset(db, session_id)
    if total is not None and total < value:
        return total
    return value


def make_dispatcher(engine: str, mode: str = "simulation", session_id: int = 0):
    """按引擎标识构造 dispatcher（tdx/ths 两个独立通道 + QMT/掘金 HTTP 桥）。

    依赖缺失/未配置时抛 RuntimeError，由 start_session 转成 503 明确告警，
    绝不静默回退到别的通道（避免"以为在通达信下单、实际打到 QMT"）。

    QMT 双桥：mode == "live" 走真实资金桥（8791），其余一律走仿真桥
    （8790）——未知 mode 绝不落到真实资金桥。
    """
    if engine == "ths":
        if ThsUiDispatcher is None:
            raise RuntimeError("未安装同花顺 UI 自动化依赖（pywinauto/pyperclip）")
        ths = ths_ui_config()
        return ThsUiDispatcher(
            window_title=ths["window_title"], client_exe_path=ths["client_exe_path"],
            captcha_ocr=ths["captcha_ocr"],
            execute_mode=ths["execute_mode"],
            execute_interval_seconds=ths["execute_interval_seconds"],
            work_dir=ths["work_dir"],
            session_id=session_id,
            price_mode=ths["price_mode"],
        )
    if engine == "tdx":
        if TdxTqDispatcher is None:
            raise RuntimeError("通达信 TQ 交易通道不可用")
        cfg = tq_trade_config()
        return TdxTqDispatcher(
            account=cfg["account"], account_type=cfg["account_type"],
            price_mode=cfg["price_mode"], client_path=cfg["client_path"],
            auto_confirm_dialog=cfg["auto_confirm_dialog"],
            confirm_timeout=cfg["confirm_timeout"],
        )
    if engine == "goldminer":
        if HttpBridgeDispatcher is None:
            raise RuntimeError("HttpBridgeDispatcher not available")
        g = goldminer_config()
        # 掘金桥 /quote 走 gm history_n（云端历史服务，首次 >10s）+ 桥内全局锁
        # 串行 → 超时/慢阈值读 config.goldminer（默认 30s / 15s）。
        return HttpBridgeDispatcher(
            base_url=g["base_url"],
            timeout=g["request_timeout_seconds"],
            slow_threshold=g["slow_response_seconds"],
            slow_warn_interval=g["slow_warn_interval_seconds"],
            channel="goldminer_bridge",
        )
    if engine == "iquant":
        q = iquant_config()
        if HttpBridgeDispatcher is None:
            raise RuntimeError("HttpBridgeDispatcher not available")
        # mode 语义显式：live=真实资金桥（8791），其余（simulation/未知）一律
        # 仿真桥（8790）——配置错误不得把请求打到真实资金桥。
        base_url = q["live"] if mode == "live" else q["simulation"]
        return HttpBridgeDispatcher(
            base_url=base_url,
            timeout=q["request_timeout_seconds"],
            slow_threshold=q["slow_response_seconds"],
            slow_warn_interval=q["slow_warn_interval_seconds"],
            channel="iquant_bridge",
        )
    # 未知引擎明确报错，绝不静默回退
    raise RuntimeError(
        "未知交易引擎：%s（可用：%s）" % (engine, "/".join(ENGINES_META)))


#: engine 标识 → 该会话主行情源在降级链中的名称（避免把同一源重复挂在链上）。
_ENGINE_QUOTE_SOURCE = {
    "tdx": "tq", "ths": "tq", "alert": "tq",
    "iquant": "iquant", "goldminer": "goldminer",
}


def build_quote_fallback(primary, engine: str):
    """J5：按 live.quote_fallback 配置包装 1m 行情源（默认关闭 → 原样返回）。

    只影响 BarPoller 的行情读取；交易/持仓/撤单仍走 ``primary``，降级不改变
    下单通道。备用源构造失败只告警并跳过，链为空时保持单源运行。
    """
    try:
        live_cfg = (load_config() or {}).get("live") or {}
    except Exception:  # noqa: BLE001 — 配置异常按未启用
        return primary
    section = live_cfg.get("quote_fallback") or {}
    if not isinstance(section, dict) or not bool(section.get("enabled", False)):
        return primary
    chain = section.get("chain") or []
    own = _ENGINE_QUOTE_SOURCE.get(str(engine or ""), "")
    providers = [("primary", primary)]
    for raw_name in chain:
        name = str(raw_name or "").strip().lower()
        if not name or name == own:
            continue
        try:
            disp = _build_quote_provider(name)
        except Exception:  # noqa: BLE001 — 备用源不可构造不影响主链路
            logger.warning("quote fallback: 备用源 %s 构造失败", name, exc_info=True)
            continue
        if disp is not None:
            providers.append((name, disp))
    if len(providers) <= 1:
        logger.warning(
            "quote fallback: 已启用但没有可用备用源（chain=%s），按单源运行", chain)
        return primary
    logger.info("quote fallback enabled: %s", [n for n, _ in providers])
    return FallbackQuoteDispatcher(providers)


def _build_quote_provider(name: str):
    """构造备用行情源（只读行情；交易方法不参与降级链）。"""
    if name == "tq":
        if TdxTqDispatcher is None:
            return None
        return TdxTqDispatcher()
    if name == "iquant":
        if HttpBridgeDispatcher is None:
            return None
        q = iquant_config()
        return HttpBridgeDispatcher(
            base_url=q["simulation"], channel="iquant_bridge")
    if name == "goldminer":
        if HttpBridgeDispatcher is None:
            return None
        g = goldminer_config()
        return HttpBridgeDispatcher(
            base_url=g["base_url"],
            timeout=g["request_timeout_seconds"],
            slow_threshold=g["slow_response_seconds"],
            slow_warn_interval=g["slow_warn_interval_seconds"],
            channel="goldminer_bridge",
        )
    logger.warning(
        "quote fallback: 未知行情源 %s（支持 tq/iquant/goldminer）", name)
    return None


def bridge_probe_message(channel: str, reason: Optional[str], base_url: str) -> str:
    """桥启动闸提示（按探测失败类型分级，避免把人误导成「策略没加载」）。

    reason 来自 HttpBridgeDispatcher.last_probe_error：
    refused=端口无监听 / timeout=已监听但无响应 / protocol=应答格式不符。
    """
    label = ("掘金桥" if channel == "goldminer_bridge"
             else "QMT 通道" if channel == "iquant_bridge" else "桥")
    if reason == "refused":
        return ("%s进程未启动（%s 端口无监听）：桥策略可能未运行，"
                "或启动时卡在 SDK 查询" % (label, base_url))
    if reason == "timeout":
        return ("%s端口已监听但无响应（%s）：桥策略可能卡在 SDK 查询"
                "（终端显示“运行中”不代表查询可用）" % (label, base_url))
    if reason == "protocol":
        return "%s应答格式不符（%s，期望 HTTP 桥协议）" % (label, base_url)
    return "%s不可达（%s）：请检查桥策略是否运行及端口配置" % (label, base_url)


#: B4：通道能力矩阵（前端据此置灰不支持的配置，后端在启动时软提示）。
#: 口径来自各 dispatcher 实现：备注精确认领仅 HTTP 桥（iquant/goldminer）支持；
#: 拆单仅通达信 TQ 通道支持；其余能力四通道一致。
CHANNEL_CAPABILITIES: Dict[str, Dict[str, bool]] = {
    "tdx": {
        "place_order": True, "cancel_order": True, "quotes": True,
        "limit_price": True, "remark_claim": False, "split_order": True,
        "premarket_limit": True,
    },
    "ths": {
        "place_order": True, "cancel_order": True, "quotes": True,
        "limit_price": True, "remark_claim": False, "split_order": False,
        "premarket_limit": True,
    },
    "iquant": {
        "place_order": True, "cancel_order": True, "quotes": True,
        "limit_price": True, "remark_claim": True, "split_order": False,
        "premarket_limit": True,
    },
    "goldminer": {
        "place_order": True, "cancel_order": True, "quotes": True,
        "limit_price": True, "remark_claim": True, "split_order": False,
        "premarket_limit": True,
    },
}

#: 能力中文名（顺序即前端展示顺序）。
CHANNEL_CAPABILITY_LABELS = {
    "place_order": "下单", "cancel_order": "撤单", "quotes": "行情",
    "limit_price": "限价", "remark_claim": "备注认领",
    "split_order": "拆单", "premarket_limit": "盘前限价",
}


def engine_status(engine: Optional[str]) -> dict:
    """引擎清单 + 已配置情况（前端下拉与通道条共用）。"""
    ths = ths_ui_config()
    tq = tq_trade_config()
    items = []
    for key, (channel, label) in ENGINES_META.items():
        configured = True
        detail = ""
        if key == "ths":
            configured = ths["available"]
            detail = "依赖未安装（pywinauto/pyperclip）" if not configured else ths["window_title"]
        elif key == "tdx":
            configured = tq["available"]
            detail = "通达信 TQ（需客户端已登录资金账号）" if configured else "通道不可用"
        elif key == "goldminer":
            g = goldminer_config()
            configured = g["available"]
            detail = "%s（token%s）" % (g["base_url"], "已配" if g["token_set"] else "未配")
        elif key == "iquant":
            q = iquant_config()
            configured = q["available"]
            detail = "仿真 %s / 实盘 %s" % (q["simulation"], q["live"])
        else:
            continue
        caps = dict(CHANNEL_CAPABILITIES.get(key, {}))
        items.append({
            "key": key, "channel": channel, "label": label,
            "configured": configured, "detail": detail,
            "capabilities": caps,
            "capability_limits": [CHANNEL_CAPABILITY_LABELS[k]
                                  for k, ok in caps.items() if not ok],
        })
    current = engine or default_engine()
    # 未知引擎（如历史 qmt）回退 tdx 展示，避免前端 active 为 None
    channel, label = ENGINES_META.get(current, ENGINES_META["tdx"])
    caps_key = current if current in CHANNEL_CAPABILITIES else "tdx"
    return {
        "engine": current, "active": channel, "label": label, "engines": items,
        "capabilities": dict(CHANNEL_CAPABILITIES.get(caps_key, {})),
        "capability_labels": dict(CHANNEL_CAPABILITY_LABELS),
        "ths_ui": ths, "tq_trade": tq, "bridge": bridge_config(),
    }


#: 通道探活 TTL：同一通道 30s 内复用上次结果，页面请求不重复打桥
_PROBE_TTL_SECONDS = 30.0
#: 单次探活硬超时：心跳最多等 1s，超时沿用上次已知值（绝不阻塞页面请求）
_PROBE_TIMEOUT_SECONDS = 1.0
#: {通道键: (单调时钟时点, 是否在线)}
_PROBE_CACHE: Dict[str, Tuple[float, bool]] = {}
_PROBE_CACHE_LOCK = threading.Lock()


def _known_engine_online(engine) -> Optional[bool]:
    """引擎内存里最近一次已知的在线状态（不新发探针；未知返回 None）。

    ``LiveEngine._tick_main`` 每轮都调 dispatcher.heartbeat() 并写
    ``_bridge_online``，页面请求只需读这个值，避免与引擎共用慢桥排队。
    """
    for attr in ("bridge_online", "_bridge_online"):
        val = getattr(engine, attr, None)
        if val is not None:
            return bool(val)
    return None


def _live_bridge_online(engine) -> bool:
    """运行中引擎的桥在线状态：优先引擎内存最近已知值（不新发 /ping）。

    兜底（引擎无 ``bridge_online`` 的替身/旧实现）才直接探活一次，保留旧语义。
    """
    known = _known_engine_online(engine)
    if known is not None:
        return known
    return bool(engine.dispatcher.heartbeat())


def _engine_state_freshness(engine) -> Tuple[Optional[str], bool]:
    """引擎状态新鲜度：(checked_at, stale)。

    ``checked_at`` 取引擎最近一次主循环 tick 时点（``_last_tick_at``，naive 上海
    时间）；超过 ``max(180s, 3×poll_interval)`` 未更新视为可能过期（引擎卡住/
    线程池饿死），置 ``stale=True``。取不到 tick 时点时返回 (None, False)。
    """
    last_tick = getattr(engine, "_last_tick_at", None)
    if not isinstance(last_tick, datetime):
        return None, False
    if last_tick.tzinfo is None:
        now = datetime.now(_SH_TZ).replace(tzinfo=None)
    else:
        now = datetime.now(_SH_TZ)
    try:
        poll = float(getattr(engine, "_poll_interval", 60) or 60)
    except (TypeError, ValueError):
        poll = 60.0
    stale = (now - last_tick).total_seconds() > max(180.0, 3.0 * poll)
    return last_tick.strftime("%Y-%m-%d %H:%M:%S"), stale


def _window_present(engine_key: str) -> bool:
    """该通道客户端是否有可见顶层窗口（进程映像名匹配）。失败一律判不在。

    实现已下沉到 ``win_dialog_scope.client_window_present``（watchdog 盘前判据
    共用同一套 ctypes 探测），行为不变：**刻意不碰 TQ / 同花顺 UI**。
    """
    from core.engine.live.win_dialog_scope import client_window_present
    return client_window_present(engine_key)


class _WindowProbe:
    """把「客户端窗口是否存在」包装成 heartbeat()，复用探活 TTL/超时机制。"""

    def __init__(self, engine_key: str):
        self._engine_key = engine_key

    def heartbeat(self) -> bool:
        return _window_present(self._engine_key)


def _probe_dispatcher_for(engine_key: Optional[str]):
    """无运行引擎时该通道的探针对象；依赖缺失/未支持返回 None。

    - tdx / ths：本机客户端窗口存在性（不碰 TQ / UI 自动化）；
    - goldminer / iquant：对应 HTTP 桥 /ping（常驻服务，与会话无关）。
    """
    if engine_key in ("tdx", "ths"):
        return _WindowProbe(engine_key)
    if engine_key == "goldminer" and HttpBridgeDispatcher is not None:
        return HttpBridgeDispatcher(base_url=goldminer_config()["base_url"])
    if engine_key == "iquant" and HttpBridgeDispatcher is not None:
        return HttpBridgeDispatcher(base_url=iquant_config()["simulation"])
    return None


def _probe_online_quick(engine_key: str, dispatcher) -> Tuple[Optional[bool], bool]:
    """兜底探活：30s TTL 缓存 + 1s 硬超时，返回 (online, stale)。

    仅用于没有 ``bridge_online`` 的引擎替身/旧实现；真实 LiveEngine 走引擎状态，
    不会进入这里。心跳跑在守护线程里并 ``join(1s)``：超时/异常时沿用上一次已知
    值（无缓存则判离线）并置 ``stale=True``，保证调用方最坏 1s 返回。
    """
    with _PROBE_CACHE_LOCK:
        cached = _PROBE_CACHE.get(engine_key)
    if cached is not None and time.monotonic() - cached[0] < _PROBE_TTL_SECONDS:
        return cached[1], False

    box: List[bool] = []

    def _run() -> None:
        try:
            box.append(bool(dispatcher.heartbeat()))
        except Exception:  # noqa: BLE001 - 探活失败只影响在线展示
            box.append(False)

    t = threading.Thread(target=_run, name="channel-probe", daemon=True)
    t.start()
    t.join(_PROBE_TIMEOUT_SECONDS)
    if t.is_alive():
        return (cached[1] if cached is not None else False), True
    online = box[0] if box else False
    with _PROBE_CACHE_LOCK:
        _PROBE_CACHE[engine_key] = (time.monotonic(), online)
    return online, False


def channel_status(engine: Optional[LiveEngine] = None,
                   engine_key: Optional[str] = None,
                   engines: Optional[Dict[int, LiveEngine]] = None) -> dict:
    """当前生效的下单通道 + 在线状态（实盘页顶部通道条 / 状态总览通道卡）。

    在线语义（2026-11-04 修正，2026-11-05 探活脱钩）：
    - **有运行中的 LiveEngine**（``engine`` 直接传入，或按 ``engine_key`` 在当前
      注册表中找到同通道引擎）→ ``online`` 取引擎内存里**最近一次已知**的在线
      状态（``LiveEngine.bridge_online``，由主循环每 60s 心跳刷新），**不新发
      /ping**。掘金桥单次查询 4–40s（桥内全局锁串行），页面请求里现打心跳会把
      「状态总览」整页卡住十几到几十秒（实证 2026-11-05：channel?engine=goldminer
      耗时 7.7s，其余接口 2–86ms）。
    - 引擎没有已知状态（无 ``bridge_online`` 的替身/旧实现）才回退到探活：30s
      TTL 缓存 + 1s 硬超时，超时沿用上一次已知值并置 ``stale=True``。
    - **没有运行引擎**（``engine_key`` 显式传入）→ 仍按通道真实探测，状态词只有
      在线/离线：tdx/ths 看客户端窗口是否存在（不碰 TQ/UI，避免冻结客户端），
      goldminer 探 7801、iquant 探 8790 的 HTTP 桥 /ping；探不到判离线。
      显式 ``engine_key`` 为 None（旧调用）才保留 ``online=None``。
      ``engine_running`` 单独输出，供前端说明「会话未启动」——不参与在线判定。

    新增可选字段：``online_source``（engine/probe/none）、``checked_at``（引擎
    最近一次心跳时点，未知为 None）、``stale``（状态可能已过期）。

    ``engines`` 仅供测试注入注册表，缺省全局 ``ENGINES``。
    """
    data = engine_status(engine_key)
    registry = ENGINES if engines is None else engines
    running = engine if engine is not None else _running_engine_for(registry, engine_key)
    online = None
    source = "none"
    checked_at = None
    stale = False
    if running is not None:
        known = _known_engine_online(running)
        if known is not None:
            online = known
            source = "engine"
            checked_at, stale = _engine_state_freshness(running)
        else:
            # 兜底探活（仅无引擎状态的替身/旧实现）：TTL 缓存 + 1s 硬超时。
            online, stale = _probe_online_quick(engine_key or "", running.dispatcher)
            source = "probe"
    elif engine_key is not None:
        # 无运行引擎也必须真实探测——「在线/离线」不能靠会话是否启动推断，更不能
        # 默认亮绿。tdx/ths 看客户端窗口是否在，goldminer/iquant 探对应 HTTP 桥；
        # 探不到（依赖缺失/异常）一律判离线。复用 30s TTL + 1s 硬超时，不新起轮询。
        try:
            probe = _probe_dispatcher_for(engine_key)
        except Exception:  # noqa: BLE001 - 配置读取失败只影响在线展示
            probe = None
        if probe is not None:
            online, stale = _probe_online_quick(engine_key, probe)
        else:
            online, stale = False, False
        source = "probe"
    data["engine_running"] = running is not None
    data["online"] = online
    data["online_source"] = source
    data["checked_at"] = checked_at
    data["stale"] = stale

    # 四通道卡片统一口径：configured + 一行次要信息（端口/账号/窗口）
    key = engine_key or data.get("engine")
    meta = next(
        (it for it in data.get("engines", []) if it.get("key") == key), None)
    data["configured"] = bool(meta.get("configured")) if meta else False
    detail = ""
    if key == "tdx":
        detail = "TQ 资金账号 %s" % (tq_trade_config()["account"] or "未配置")
    elif key == "ths":
        detail = ths_ui_config()["window_title"] or "同花顺下单窗口"
    elif key == "goldminer":
        detail = goldminer_config()["base_url"]
    elif key == "iquant":
        # 只报生产的 8790 仿真桥；8791 信用腿非生产、未启用，不进状态卡
        detail = "HTTP 桥 %s" % iquant_config()["simulation"]
    data["detail"] = detail
    return data


#: 引擎键 → dispatcher.channel（用于在注册表中找同通道的运行中引擎）
_ENGINE_CHANNEL = {
    "goldminer": "goldminer_bridge",
    "tdx": "tdx_tq",
    "ths": "ths_ui",
    "iquant": "iquant_bridge",
}


def _running_engine_for(engines: Dict[int, LiveEngine],
                        engine_key: Optional[str]) -> Optional[LiveEngine]:
    """引擎注册表中该通道的运行中实例（无则 None，多个时取第一个）。"""
    target = _ENGINE_CHANNEL.get(engine_key or "")
    if not target:
        return None
    for eng in list(engines.values()):
        if getattr(getattr(eng, "dispatcher", None), "channel", None) == target:
            return eng
    return None


def real_position_count(rows: list) -> int:
    """真实持仓条数：按「持仓数量」判定，0 股残行不计。

    通达信/同花顺卖出后持仓表仍保留该行（代码+名称），只是数量归零；按
    行数计数会把残行算成持仓。字段取 ``volume``（持仓数量/股票余额），
    **不取** ``available``（可卖数量）：当日买入 T+1 锁仓 ``available=0`` 但
    ``volume>0``，属真实持仓，不能用可卖数剔除。
    """
    count = 0
    for r in rows or []:
        if not isinstance(r, dict):
            continue
        try:
            volume = int(r.get("volume") or 0)
        except (TypeError, ValueError):
            continue
        if volume > 0:
            count += 1
    return count


def test_channel(channel: str = "ths_ui", mode: str = "simulation") -> dict:
    """只读连通性探测（实盘页/系统配置页「测试连接」按钮）。

    ths_ui：连接 xiadan.exe 窗口并读一次持仓（只读，不下单）；
    iquant_bridge：请求对应模式桥的 /ping；
    tdx_tq：连通达信 TQ 交易账户并读一次持仓（只读）。
    """
    if channel == "tdx_tq":
        if TdxTqDispatcher is None:
            return {"ok": False, "channel": "tdx_tq", "error": "通达信 TQ 交易通道不可用"}
        cfg = tq_trade_config()
        dispatcher = TdxTqDispatcher(
            account=cfg["account"], account_type=cfg["account_type"],
            price_mode=cfg["price_mode"], client_path=cfg["client_path"],
            auto_confirm_dialog=cfg["auto_confirm_dialog"],
            confirm_timeout=cfg["confirm_timeout"],
        )
        try:
            if not dispatcher.heartbeat():
                return {"ok": False, "channel": "tdx_tq",
                        "error": dispatcher.last_error or "通达信交易未就绪"}
            try:
                rows = dispatcher.query_positions()
            except Exception as e:
                # 持仓查询失败会抛 BridgeUnavailableError（读失败≠零持仓）；
                # 探测接口必须转成 ok=False，不能漏成 500。
                return {"ok": False, "channel": "tdx_tq",
                        "error": str(e) or "通达信持仓读取失败"}
            if dispatcher.last_error:
                return {"ok": False, "channel": "tdx_tq", "error": dispatcher.last_error}
            return {"ok": True, "channel": "tdx_tq",
                    "positions": real_position_count(rows)}
        finally:
            dispatcher.close()
    if channel == "ths_ui":
        if ThsUiDispatcher is None:
            return {"ok": False, "channel": "ths_ui", "error": "未安装 UI 自动化依赖（pywinauto/pyperclip）"}
        cfg = ths_ui_config()
        dispatcher = ThsUiDispatcher(
            window_title=cfg["window_title"], client_exe_path=cfg["client_exe_path"],
            captcha_ocr=cfg["captcha_ocr"],
            # 只读探测：direct 模式不启文件交接执行器线程（否则每次「测试连接」
            # 都会在 data/ths/0 挂一个消费 orders 的后台线程）。
            execute_mode="direct",
        )
        try:
            if not dispatcher.connect():
                return {"ok": False, "channel": "ths_ui",
                        "error": dispatcher.last_error or "未找到同花顺交易客户端窗口"}
            if not cfg["captcha_ocr"]:
                return {"ok": False, "channel": "ths_ui",
                        "error": "同花顺复制表格会触发验证码，请先在系统配置启用验证码 OCR"}
            rows = dispatcher.query_positions()
            if rows is None or dispatcher.last_error:
                return {"ok": False, "channel": "ths_ui",
                        "error": dispatcher.last_error or "同花顺持仓读取失败（未登录/验证码/切页异常）"}
            return {"ok": True, "channel": "ths_ui", "window_title": cfg["window_title"],
                    "positions": real_position_count(rows)}
        finally:
            dispatcher.close()
    if channel == "goldminer_bridge":
        g = goldminer_config()
        base_url = g["base_url"]
        try:
            dispatcher = HttpBridgeDispatcher(base_url=base_url)
            online = dispatcher.heartbeat()
            if not online:
                return {"ok": False, "channel": "goldminer_bridge", "base_url": base_url,
                        "error": "掘金桥未响应（终端内桥策略未运行，日志应显示 [GM-BRIDGE] listening）"}
            return {"ok": True, "channel": "goldminer_bridge", "base_url": base_url, "error": None}
        except Exception as e:
            return {"ok": False, "channel": "goldminer_bridge", "base_url": base_url, "error": str(e)}
    q = iquant_config()
    # 与 make_dispatcher 同一 mode 语义：仅 live 走真实资金桥，其余兜底仿真桥
    base_url = q["live"] if mode == "live" else q["simulation"]
    try:
        dispatcher = HttpBridgeDispatcher(base_url=base_url)
        online = dispatcher.heartbeat()
        return {"ok": bool(online), "channel": "iquant_bridge", "base_url": base_url,
                "error": None if online else "QMT 桥未响应（客户端未启动或桥策略未加载）"}
    except Exception as e:
        return {"ok": False, "channel": "iquant_bridge", "base_url": base_url, "error": str(e)}


def resolve_stock_codes(db: Session, portfolio_strategy_id: int) -> list:
    """取某组合股票池的成分股代码列表（BarPoller 行情订阅范围）。"""
    ps = db.query(PortfolioStrategy).filter_by(id=portfolio_strategy_id).first()
    if ps is None:
        return []
    stocks = (
        db.query(StockPoolStock)
        .filter_by(pool_id=ps.stock_pool_id)
        .all()
    )
    return [s.stock_code for s in stocks if s.stock_code]


def _session_strategies(db: Session, session_id: int):
    links = (
        db.query(LiveSessionPortfolio)
        .filter(LiveSessionPortfolio.session_id == session_id)
        .all()
    )
    rows = []
    for link in links:
        strategies = (
            db.query(Strategy)
            .filter(Strategy.portfolio_id == link.portfolio_strategy_id)
            .order_by(Strategy.id)
            .all()
        )
        rows.extend((link.portfolio_strategy_id, st) for st in strategies)
    return rows


def _preflight_tq_alert():
    msg = TQAlert().preflight()
    return (503, msg) if msg else None


def _smoke_formula_outputs(db: Session, session_id: int):
    """alert 启动前用 1 只股票试算公式，确认输出线覆盖 FormulaSignal。"""
    formula = TQFormula()
    seen = set()
    for pid, st in _session_strategies(db, session_id):
        smoke_key = (st.formula_id, st.period)
        if st.formula_id is None or smoke_key in seen:
            continue
        seen.add(smoke_key)
        f = db.query(Formula).filter_by(id=st.formula_id).first()
        if f is None:
            return (503, "策略 %s 引用的公式不存在" % st.name)
        signals = db.query(FormulaSignal).filter_by(formula_id=f.id).all()
        if not signals:
            return (503, "公式 %s 未配置信号输出线" % f.name)
        codes = resolve_stock_codes(db, pid)
        if not codes:
            continue
        code = sorted(codes)[0]
        # 至少拉 200 根，避免周线 MACD 等长周期输出线因历史不足变 NaN
        count = max(f.formula_count or 200, 200)
        try:
            raw = formula.compute(
                f.name, "", [code], period=st.period,
                count=count, end_time=now_shanghai().strftime("%Y%m%d"),
            )
            if (not isinstance(raw, dict)
                    or str(raw.get("ErrorId", "0")) not in ("0", "19")):
                detail = raw.get("Error") if isinstance(raw, dict) else "无返回"
                return (503, "公式 %s 试算失败：%s" % (
                    f.name, detail or "无返回"))
            produced = MarketDataService._extract_latest_signal(raw, code)
            names = {o["name"] for o in produced}
        except Exception as exc:
            return (503, "公式 %s 试算失败：%s" % (f.name, exc))
        missing = [s.signal_name for s in signals if s.signal_name not in names]
        if missing:
            return (503, "公式 %s 输出缺少信号线：%s" % (
                f.name, "、".join(missing)))
    return None


def preflight_signal_config(db: Session, session_id: int, execution_mode: str):
    """启动前检查；返回 None 或 (http_status, message)。

    alert: TQ-only + 频率规则 + send_warn 可用 + 公式试算输出线。
    auto（P0 兼容）: 只查 TQ engine + 频率规则，不做 TQ 试算/告警探测，
    避免历史自动会话因客户端未就绪而无法启动。
    """
    if execution_mode not in ("alert", "auto", "manual", "shadow"):
        return (400, "execution_mode 必须为 alert/auto/manual/shadow")
    for pid, st in _session_strategies(db, session_id):
        if st.formula_id is None:
            if execution_mode == "alert":
                return (503, "策略 %s 未绑定公式" % st.name)
            continue
        f = db.query(Formula).filter_by(id=st.formula_id).first()
        if f is None:
            return (503, "策略 %s 引用的公式不存在" % st.name)
        if f.engine != "tq":
            return (503, "实盘只支持通达信导入公式（%s.engine=%s）" % (
                f.name, f.engine))
        msg = validate_signal_config(
            st.period, st.signal_frequency, st.signal_at,
            st.signal_weekday, st.signal_data_end,
        )
        if msg:
            return (503, msg)
        freq = effective_signal_frequency(st.period, st.signal_frequency)
        if execution_mode in ("auto", "shadow") and freq != st.period:
            return (503, "全自动 P0 仅支持 signal_frequency == period；"
                         "请改用 alert 或等待 P2（策略 %s）" % st.name)
    if execution_mode == "alert":
        err = _preflight_tq_alert()
        if err:
            return err
        err = _smoke_formula_outputs(db, session_id)
        if err:
            return err
    return None


def split_channel_warnings(
    db: Session, session_id: int, execution_mode: str, channel: str,
) -> List[str]:
    """H1：启用拆单但通道不支持时的启动软提示（不阻断）。

    拆单在 ``LiveEngine._maybe_split_order`` 里只对 ``tdx_tq`` 生效；alert 模式
    本身不下单，无需提示。返回空列表 = 无需提示。
    """
    if execution_mode == "alert" or channel == "tdx_tq":
        return []
    names = [
        st.name for _, st in _session_strategies(db, session_id)
        if getattr(st, "split_order_enabled", False)
    ]
    if not names:
        return []
    return [
        "通道 %s 不支持拆单：策略 %s 已启用大额拆单，启动后不会拆单"
        "（拆单仅通达信 TQ 支持）" % (channel or "-", "、".join(names))
    ]


def build_engine(
    session_id: int,
    db: Session,
    mode: str = "simulation",
    db_session_factory=SessionLocal,
    dispatcher_cls=None,
    engine: Optional[str] = None,
    execution_mode: str = "auto",
    signal_file_path: Optional[str] = None,
    advance_close_enabled: Optional[bool] = None,
    advance_close_time: Optional[str] = None,
    session_mismatch_risk: Optional[bool] = None,
) -> LiveEngine:
    """组装一个 session 的 LiveEngine：各组合 Portfolio + dispatcher + BarPoller。

    公式注入（0010）：遍历各组合策略，按 Strategy.formula_id → Formula.name 预加载
    {strategy_id: formula_name}，连同 TQFormula 实例传入 LiveEngine，供 _fill_signal_cache
    逐 bar 内存注入算公式信号。无公式的策略不入映射（_fill_signal_cache 跳过）。

    dispatcher_cls：注入 dispatcher 类（测试 monkeypatch 用）；None 时按 engine 选：
    engine=None → 沿用旧行为（ths_ui.enabled → 同花顺，否则 QMT 桥）；
    engine=tdx → 通达信 TQ。

    execution_mode=alert 时固定使用 AlertDataDispatcher（TQ 行情、禁止下单），
    优先于 dispatcher_cls/engine 选择；auto 保持既有下单通道行为。

    外部信号注入（0011）：signal_file_path 参数非空 → LiveEngine 建
    ExternalSignalFileSource（通达信条件预警文件时间窗口增量读取）；None 时回退读
    session.signal_file_path（会话级配置，start_session/api 组装路径无需显式传），
    两者皆空 → 不启用，行为不变。
    """
    if execution_mode == "alert":
        dispatcher = AlertDataDispatcher()
    elif execution_mode == "shadow":
        # B5 影子模式：包裹真实通道取行情/心跳，委托本地模拟成交（不触真实柜台）
        dispatcher = ShadowDispatcher(
            make_dispatcher(engine or default_engine(), mode, session_id))
    elif dispatcher_cls is None:
        dispatcher = make_dispatcher(engine or default_engine(), mode, session_id)
    elif dispatcher_cls is ThsUiDispatcher:
        ths_cfg = load_config().get("ths_ui", {})
        ths_cfg = ths_cfg if isinstance(ths_cfg, dict) else {}
        dispatcher = dispatcher_cls(
            window_title=ths_cfg.get("window_title") or "网上股票交易系统5.0",
            client_exe_path=ths_cfg.get("client_exe_path"),
            captcha_ocr=bool(ths_cfg.get("captcha_ocr")),
            execute_mode=ths_cfg.get("execute_mode") or "file_builtin",
            execute_interval_seconds=int(ths_cfg.get("execute_interval_seconds") or 5),
            work_dir=ths_cfg.get("work_dir") or "data/ths",
            session_id=session_id,
            price_mode=ths_cfg.get("price_mode") or "approval",
        )
    elif dispatcher_cls is TdxTqDispatcher and TdxTqDispatcher is not None:
        cfg = tq_trade_config()
        dispatcher = dispatcher_cls(
            account=cfg["account"], account_type=cfg["account_type"],
            price_mode=cfg["price_mode"], client_path=cfg["client_path"],
            auto_confirm_dialog=cfg["auto_confirm_dialog"],
        )
    else:
        br = bridge_config()
        dispatcher = dispatcher_cls(base_url=br.get(mode, br["simulation"]))
    links = (
        db.query(LiveSessionPortfolio)
        .filter(LiveSessionPortfolio.session_id == session_id)
        .all()
    )
    # 会话级「资金基准（元）」：2026-11-16 起 = 真实账户总资产口径，只缩不放。
    # 起账时按各组合 initial_capital 占比切分给各组合（独占模型下只缩到
    # 「会话内多组合」；单组合=整额）。取不到填写值时沿用各组合 initial_capital。
    session_row = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    live_capital = getattr(session_row, "live_capital", None)
    baseline_effective = resolve_effective_baseline(db, session_row, session_id)
    # 外部信号注入（0011）：参数优先；未传回退 session.signal_file_path（会话级配置）
    if signal_file_path is None and session_row is not None:
        signal_file_path = getattr(session_row, "signal_file_path", None)
    # 14:50 提前收盘价（仅 1d）：参数优先；未传回退会话级配置（默认关/14:51）
    if advance_close_enabled is None and session_row is not None:
        advance_close_enabled = bool(getattr(session_row, "advance_close_enabled", False))
    if advance_close_time is None and session_row is not None:
        advance_close_time = getattr(session_row, "advance_close_time", None)
    portfolios = []
    stock_codes: set = set()
    # B5 市场状态闸：组合 → 基准指数代码（供 LiveEngine 逐日算 regime）
    benchmark_codes: Dict[int, str] = {}
    # 市场多空门控（2026-11-18）：组合 → **标量配置**（不持有 ORM 实例，避免
    # build_engine 之后 commit/expire 触发 DetachedInstanceError）。
    # 只收集启用了门控的组合；未启用的组合不进 provider，零额外 TQ 取数。
    gate_specs: Dict[int, dict] = {}
    # 公司行为（2026-11-18，J1/J2）：组合 → 标量配置；事件内容由 provider 现读
    # 手工事件表（同一张 corporate_actions，与回测同口径）。默认空 = 未启用。
    ca_specs: Dict[int, dict] = {}
    formula_ids: set = set()
    strategy_formula: Dict[int, int] = {}  # strategy_id -> formula_id
    # 预热/分发按 (code, period) 取该股票该周期所有公式的 formula_count 最大值（跨组合跨策略）。
    # 无公式兜底 DEFAULT_COUNT(200)。预热只拉实际有策略的 (code,period)，按需不浪费。
    # 在此同一循环累加（已查 ps/strategies/股票池，不二次遍历）。
    DEFAULT_COUNT = 200
    code_period_count: Dict[tuple, int] = {}
    # 注：formula_map 在循环后批量查，此处先记 strategy_id -> period，待 formula_map 就绪再算 max。
    # 简化：先记 (code, period) -> [formula_id...]，formula_map 就绪后取 max(formula_count)。
    code_period_formula_ids: Dict[tuple, set] = {}
    pool_codes_by_portfolio: Dict[int, set] = {}
    # 卖出范围口径（config.tq_trade.live_sell_scope）：full=整仓卖（默认，旧行为），
    # platform_only=只卖平台自己买入的部分（外部手工仓不代卖）
    sell_scope = live_sell_scope()
    # (port, 尽分权重=该组合 initial_capital)：会话基准按此占比切分
    baseline_weights: list = []
    for link in links:
        ps = db.query(PortfolioStrategy).filter_by(id=link.portfolio_strategy_id).first()
        if ps is None:
            continue
        strategies = db.query(Strategy).filter_by(portfolio_id=ps.id).order_by(Strategy.id).all()
        port = assemble_portfolio(
            ps, strategies, db,
            sell_scope=sell_scope,
        )
        # 单票集中度上限 / 现金留存下限（2026-11-16 接线，**仅实盘**）：
        # 两列在 portfolio_strategies 上，NULL 或 0 视为不设限（与接线前逐字一致）。
        # 回测侧口径冻结——assemble_portfolio 不读这两列，回测调用点也不注入，
        # 因此历史回测结果不会随填写值漂移。实际约束在
        # LiveEngine._allocate_batch_buys → _apply_allocation_caps 内执行。
        port.max_single_position_pct = _pct_or_none(
            getattr(ps, "max_single_position_pct", None))
        port.min_cash_reserve_pct = _pct_or_none(
            getattr(ps, "min_cash_reserve_pct", None))
        try:
            weight = float(ps.initial_capital or 0)
        except (TypeError, ValueError):
            weight = 0.0
        logger.info(
            "组合 %s 起账权重 %s（会话资金基准 %s，按占比切分）",
            ps.id, weight, baseline_effective,
        )
        portfolios.append(port)
        baseline_weights.append((port, weight))
        benchmark_codes[ps.id] = getattr(ps, "benchmark_index", None) or DEFAULT_BENCHMARK_CODE
        if gate_enabled_for(ps, strategies):
            gate_specs[ps.id] = {
                "portfolio_id": ps.id,
                "market_gate_enabled": True,
                "benchmark_index": (getattr(ps, "benchmark_index", None)
                                    or DEFAULT_BENCHMARK_CODE),
                "market_gate_engine": getattr(ps, "market_gate_engine", None) or "python",
                "market_gate_formula_name": getattr(ps, "market_gate_formula_name", None),
                # E3 多指数投票 / 强度分档 / 空头减仓（默认值=旧语义）
                "market_gate_indices": getattr(ps, "market_gate_indices", None),
                "market_gate_vote_mode": (
                    getattr(ps, "market_gate_vote_mode", None) or "any"),
                "market_gate_strength_threshold": int(
                    getattr(ps, "market_gate_strength_threshold", 0) or 0),
                "market_gate_bear_reduce_ratio": getattr(
                    ps, "market_gate_bear_reduce_ratio", 0),
            }
        # 公司行为（2026-11-18，J1/J2）：只收集启用组合的标量配置（不持有 ORM）。
        if getattr(ps, "corporate_actions_enabled", False):
            ca_specs[ps.id] = {
                "portfolio_id": ps.id,
                "dividend_reinvest_enabled": bool(
                    getattr(ps, "dividend_reinvest_enabled", False)),
                "suspension_dispose_days": int(
                    getattr(ps, "suspension_dispose_days", 0) or 0),
                "delist_force_close_enabled": bool(
                    getattr(ps, "delist_force_close_enabled", False)),
            }
        # 时点股票池（J7，默认关）：注入时间线与「历史成分并集」——
        # 当日不在池的票不开仓/加仓，但已持仓的卖出/风控不受影响；行情轮询
        # 也必须覆盖历史成分，否则日后重新入池时没有可用的价格序列。
        _pit_enabled = bool(getattr(ps, "pool_pit_enabled", False))
        if _pit_enabled:
            try:
                from core.services.backtest_service import build_pool_timeline
                _pit_timeline = build_pool_timeline(db, ps)
                if len(_pit_timeline):
                    port.pool_timeline = _pit_timeline
                    port.pool_pit_enabled = True
            except Exception:  # noqa: BLE001 — 取数失败只告警，不影响启动
                logger.exception("组合 %s 时点股票池加载失败，按当前池运行", ps.id)
        pool_codes = resolve_stock_codes(db, ps.id)
        if _pit_enabled:
            pool_codes = sorted(set(pool_codes) | set(
                row[0] for row in db.query(StockPoolMembership.stock_code)
                .filter(StockPoolMembership.pool_id == ps.stock_pool_id)
                .distinct().all()))
        stock_codes.update(pool_codes)
        pool_codes_by_portfolio[ps.id] = set(pool_codes)
        for strat in strategies:
            if strat.formula_id is not None:
                formula_ids.add(strat.formula_id)
                strategy_formula[strat.id] = strat.formula_id
            # 累加 (code, period) -> formula_id（跨组合跨策略，同 key 收集所有公式）
            for code in pool_codes:
                code_period_formula_ids.setdefault((code, strat.period), set()).add(strat.formula_id)

    # 资金基准切分（2026-11-16）：会话基准 = 各组合起账资金之和，按各自
    # initial_capital 占比分配（最大余数法落到分，最后一组承接余数）。
    # 只改内存起账，不回写 portfolio_strategies.initial_capital（那是策略配置/
    # 回测口径；实盘基准是会话级覆盖）。取不到基准时保持旧行为。
    if baseline_effective is not None and baseline_weights:
        base_dec = Decimal(str(baseline_effective)).quantize(Decimal("0.01"))
        total_weight = sum(w for _, w in baseline_weights)
        remaining = base_dec
        for idx, (port, weight) in enumerate(baseline_weights):
            last = idx == len(baseline_weights) - 1
            if total_weight > 0:
                if last:
                    amt = remaining
                else:
                    amt = (base_dec * Decimal(str(weight)) / Decimal(str(total_weight))
                           ).quantize(Decimal("0.01"))
                    if amt > remaining:
                        amt = remaining
                    remaining -= amt
            else:
                amt = (base_dec / Decimal(len(baseline_weights))).quantize(Decimal("0.01"))
                if last:
                    amt = remaining
                remaining -= amt
            port.account.initial_capital = amt
            port.account.cash = amt
        logger.info(
            "资金基准切分：会话 #%s 基准 %s → 各组起账 %s",
            session_id, base_dec,
            [str(p.account.initial_capital) for p, _ in baseline_weights],
        )

    # 批量查 Formula，建 {strategy_id: formula_name} + {formula_name: formula_count}
    # formula_count（Q4 决策4）为公式级注入根数：实盘注入 count 来自该字段，非全局 200。
    formula_by_strategy: Dict[int, str] = {}
    formula_count_by_name: Dict[str, int] = {}
    # formula_id -> formula_count（供 code_period_count 取 max）
    formula_count_by_id: Dict[int, int] = {}
    if formula_ids:
        formula_map = {
            f.id: f
            for f in db.query(Formula).filter(Formula.id.in_(formula_ids)).all()
        }
        for sid, fid in strategy_formula.items():
            f = formula_map.get(fid)
            if f:
                formula_by_strategy[sid] = f.name
                if f.formula_count:
                    formula_count_by_name[f.name] = f.formula_count
                    formula_count_by_id[fid] = f.formula_count

    for port in portfolios:
        for ctx in port.strategies:
            ctx.formula_name = formula_by_strategy.get(ctx.strategy_id, "")
            ctx.formula_id = strategy_formula.get(ctx.strategy_id)

    # (code, period) -> 该股票该周期所有公式的 formula_count 最大值（跨组合跨策略）。
    # formula_id 为 None 的策略（无公式）兜底 DEFAULT_COUNT。预热/分发按此拉，按需不浪费。
    for (code, period), fids in code_period_formula_ids.items():
        counts = [formula_count_by_id.get(fid, DEFAULT_COUNT) for fid in fids if fid is not None]
        # 无任何带公式的策略 → 兜底；有则取 max
        code_period_count[(code, period)] = max(counts) if counts else DEFAULT_COUNT

    def _regime_provider(code: str, end_date):
        """B5：拉基准收盘历史（end_date 前 ~620 天 ~ end_date），供因果 regime 跟踪。
        局部 import 避免 live_service ↔ backtest_service 模块级耦合；仅在有策略
        开启 regime_filter_enabled 时被 LiveEngine 调用（默认完全不触发）。"""
        from core.services.backtest_service import build_index_close_series
        return build_index_close_series(
            [code], end_date - timedelta(days=620), end_date,
        ).get(code, {})

    def _gate_provider(portfolio_id: int, end_date):
        """市场多空门控：按组合标量配置现算 tracker（每组合每交易日一次）。

        只捕获标量字典（不持有 ORM 实例）——build_engine 之后的 commit/expire
        不会让闭包失效。返回 None 表示该组合未启用门控（引擎据此不注入）。
        取数失败抛 MarketGateUnavailableError，由 LiveEngine._maybe_update_gate
        捕获并保留旧状态（fail-open，不做「全禁开仓」）。
        """
        spec = gate_specs.get(portfolio_id)
        if not spec:
            return None
        from types import SimpleNamespace
        from core.services.backtest_service import build_gate_tracker
        # build_gate_tracker 内部再前推 GATE_WARMUP_DAYS(500) 自然日，
        # 这里传 end_date-365 天 → 实际回看约 865 天（≈600 根日线），MA288 余量充足。
        return build_gate_tracker(
            SimpleNamespace(**spec), end_date - timedelta(days=365), end_date)

    def _corporate_timeline_provider(portfolio_id: int):
        """公司行为（J1/J2）：现读手工事件表返回时间线（每组合每日一次）。

        每次调用自带会话（引擎在 worker 线程里调用，不能复用请求级 Session）；
        读失败抛异常 → ``_maybe_apply_corporate_actions`` 捕获后跳过本轮，
        不影响交易主链路。
        """
        from core.services.backtest_service import build_corporate_timeline
        session = SessionLocal()
        try:
            return build_corporate_timeline(session)
        finally:
            session.close()

    # 股票池为空时给一个占位，避免空列表；首期实盘一般有成分股
    poll_codes = sorted(stock_codes) or ["000001.SZ"]
    # 盘中 1m 数据源（快照合成）：TQ 通道盘中分钟 K 线只到上一交易日
    # （实测 + 手册 refresh_kline 限制），快照通道实时——tdx/ths/alert 的
    # poller 改读快照合成 bar（deals 节拍 5s 采样，_tick_deals 驱动）；
    # iquant 桥行情自带实时 bar，保持 K 线轮询。合成器构建失败回退 K 线
    # 轮询（盘中 1m 退化，行为同旧版）。
    snapshot_synth = None
    poller = None
    try:
        from core.tq.data import TQData
        from core.engine.live.snapshot_bars import (
            SnapshotBarSynthesizer, SnapshotQuoteDispatcher,
        )
        from core.engine.ths_ui_dispatcher import ThsUiDispatcher as _ThsDisp
        from core.engine.tdx_tq_dispatcher import TdxTqDispatcher as _TdxDisp
        _tq_quote_types = tuple(
            d for d in (_TdxDisp, _ThsDisp, AlertDataDispatcher)
            if d is not None
        )
        # goldminer 引擎：交易走掘金桥，**行情走通达信快照合成**（行情与交易
        # 分离——gm history_n 单只 ~13s 云端往返，120 只全池轮询会饿死主循环）
        _goldminer_quotes = engine == "goldminer"
        if _goldminer_quotes or isinstance(dispatcher, _tq_quote_types):
            snapshot_synth = SnapshotBarSynthesizer(
                poll_codes,
                fetch_snapshot=lambda codes: TQData().get_market_snapshot_batch(
                    list(codes), []),
            )
            poller = BarPoller(
                SnapshotQuoteDispatcher(snapshot_synth), poll_codes,
                period="1m", count=10,
            )
    except Exception:
        logger.exception("快照合成器构建失败，回退 K 线轮询（盘中 1m 将不可用）")
        snapshot_synth = None
        poller = None
    if poller is None:
        # J5：行情降级链（默认关闭 → build_quote_fallback 原样返回 dispatcher）。
        # 仅包装 poller 的行情读取；dispatcher 本体仍用于下单/持仓/撤单。
        quote_dispatcher = build_quote_fallback(dispatcher, engine)
        try:
            poller = BarPoller(
                quote_dispatcher, poll_codes, period="1m", count=10)
        except Exception:
            poller = None  # type: ignore
    # 账户全貌快照采集节拍：THS UI 读表重 → 60s；goldminer 交易通道走掘金云
    # （gm RPC 每查询 ~10s）→ 60s；tdx/iquant/alert 本地快照 → 5s
    try:
        from core.engine.ths_ui_dispatcher import ThsUiDispatcher as _ThsDisp2
        from core.engine.http_bridge_dispatcher import HttpBridgeDispatcher as _HttpDisp2
        if isinstance(dispatcher, _ThsDisp2) or engine == "goldminer":
            _snap_interval = 60.0
        elif isinstance(dispatcher, _HttpDisp2):
            _snap_interval = 60.0
        else:
            _snap_interval = 5.0
    except Exception:
        _snap_interval = 5.0
    # 引擎标识（快照表落库用）：alert 模式固定；auto 按会话解析（None → 全局默认）
    if execution_mode == "alert":
        _engine_key = "alert"
    else:
        _engine_key = engine or default_engine()
    # 交易日历多源回退链：主源=当前通道 query_calendar；非掘金会话再挂掘金桥
    # /calendar 作备用源（内置官方休市表 BUILTIN_CLOSURES 为末级兜底）。
    from core.engine.live.calendar import TradingCalendar, goldminer_calendar_provider
    _tq_cfg = load_config().get("tq_trade") or {}
    if not isinstance(_tq_cfg, dict):
        _tq_cfg = {}
    # G2：会话级盘前参数覆盖（NULL=跟随全局；非空=本会话覆盖）。
    _pre_open_cfg, _pre_open_overrides = resolve_pre_open_auction_config(
        session_row, _tq_cfg)
    # 错周期 bar 风控：会话三态 NULL → 跟随全局默认（默认 False，对齐回测）
    if session_mismatch_risk is None:
        session_mismatch_risk = bool(
            _tq_cfg.get("intraday_mismatched_risk_enabled", False))
    _calendar_fallbacks = []
    if _engine_key != "goldminer":
        _calendar_fallbacks.append(("goldminer_bridge", goldminer_calendar_provider))
    trading_calendar = TradingCalendar(
        dispatcher.query_calendar, fallback_providers=_calendar_fallbacks)
    eng = LiveEngine(
        session_id=session_id,
        portfolios=portfolios,
        dispatcher=dispatcher,
        bar_poller=poller,
        db_session_factory=db_session_factory,
        tq_formula=TQFormula(),
        formula_by_strategy=formula_by_strategy,
        formula_count=200,
        formula_count_by_name=formula_count_by_name,
        code_period_count=code_period_count,
        trading_calendar=trading_calendar,
        regime_provider=_regime_provider,
        benchmark_codes=benchmark_codes,
        gate_provider=_gate_provider if gate_specs else None,
        gate_portfolio_ids=set(gate_specs),
        # E2：每日门控快照（带基准指数/引擎元信息）
        gate_specs=gate_specs,
        # E1（默认关）：顺延次日单/盘前竞价单提交前重判门控
        gate_recheck_on_execute=bool(
            getattr(session_row, "gate_recheck_on_execute", False)),
        corporate_action_specs=ca_specs or None,
        corporate_timeline_provider=(
            _corporate_timeline_provider if ca_specs else None),
        # D1/D2（2026-11-18）：账户级聚合风控配置（默认关闭）。
        # 阈值优先级：会话列显式值 > config.tq_trade.account_max_drawdown /
        # account_daily_loss_limit > 引擎默认 15% / 5%。
        account_risk_config={
            "enabled": bool(getattr(session_row, "account_risk_enabled", False)),
            "max_drawdown": (
                getattr(session_row, "account_max_drawdown", None)
                or _tq_cfg.get("account_max_drawdown")),
            "daily_loss_limit": (
                getattr(session_row, "account_daily_loss_limit", None)
                or _tq_cfg.get("account_daily_loss_limit")),
            "state": getattr(session_row, "account_risk_state", None),
        },
        execution_mode=execution_mode,
        signal_file_path=signal_file_path,
        advance_close_enabled=bool(advance_close_enabled),
        advance_close_time=advance_close_time or "14:51",
        formula_id_by_strategy=strategy_formula,
        enable_signal_monitor=True,
        snapshot_synth=snapshot_synth,
        engine_key=_engine_key,
        account_snapshot_interval=_snap_interval,
        pool_codes_by_portfolio=pool_codes_by_portfolio,
        # 行情轮询窗口（config.live.quote_polling）：缺键时引擎侧回落安全默认
        # （交易日 09:15–15:05 之外不轮询，收盘后停对桥/TQ 的行情拉取）
        quote_polling=(load_config().get("live") or {}).get("quote_polling"),
        # 次日盘前（集合竞价）执行日线级信号单：全局 config.tq_trade.pre_open_auction*
        # + 会话级覆盖（G2）。缺键 → 引擎侧 PreOpenAuctionConfig 默认开启。
        pre_open_auction=_pre_open_cfg,
        # G2：来源标记（session=至少一项被会话覆盖），启动时落一条生效事件。
        pre_open_config_source=(
            "session" if _pre_open_overrides else "global"),
        pre_open_config_overrides=tuple(_pre_open_overrides),
        # G3：顺延单积压告警阈值（0=关闭；默认全关）
        deferred_backlog_warn_count=int(
            _tq_cfg.get("deferred_backlog_warn_count") or 0),
        deferred_backlog_warn_days=int(
            _tq_cfg.get("deferred_backlog_warn_days") or 0),
        # 错周期 bar 风控（会话三态：NULL=跟随全局默认；0/1=显式覆盖）
        intraday_mismatched_risk_enabled=(
            bool(session_mismatch_risk)
        ),
        # 09:31 deferred 放行「保证成交」开关（默认关=首根 open 限价）
        deferred_release_ensure_fill=bool(
            _tq_cfg.get("deferred_release_ensure_fill", False)),
        # 真实账户总资产口径的资金基准（baseline_value/live_capital，min 截断后）：
        # 只作真实侧总闸兜底上限，不重复覆盖组合起账（起账已按占比切分）。
        account_total_capital=(
            baseline_effective if baseline_effective is not None else live_capital),
        # A1 资金池：会话级开关（默认 off/不分池），首 bar 分配一次；
        # 开启再平衡后每交易日首 bar 再平衡（只挪虚拟现金）。
        capital_pool_mode=(getattr(session_row, "capital_pool_mode", None) or "off"),
        capital_pool_rebalance_enabled=bool(
            getattr(session_row, "capital_pool_rebalance_enabled", False)),
        # B2 主备通道：主通道心跳失败时按需写建议切换事件（不自动热切换）
        backup_engine=getattr(session_row, "backup_engine", None),
        failover_enabled=bool(getattr(session_row, "failover_enabled", False)),
    )
    # 桥慢响应告警接引擎事件流（risk/bridge_slow，dispatcher 侧已按 endpoint
    # 冷却去重）；仅 HTTP 桥 dispatcher 有 on_slow 钩子，其它通道不受影响。
    if hasattr(dispatcher, "on_slow"):
        dispatcher.on_slow = eng._on_bridge_slow
    # 交易日历退化告警接引擎事件流（risk/trading_calendar_degraded，每自然年一次）
    cal = getattr(eng, "_calendar", None)
    if cal is not None and hasattr(cal, "on_degraded"):
        cal.on_degraded = lambda payload: eng._emit(
            "risk", dict(payload, session_id=session_id, engine=_engine_key))
    return eng


# ---------------------------------------------------------------------------
# session CRUD
# ---------------------------------------------------------------------------

def list_sessions(db: Session, engines: Optional[Dict[int, LiveEngine]] = None) -> list:
    """session 列表。engines 缺省取本模块注册表（I-7：并入引擎内存对账差异摘要）。"""
    if engines is None:
        engines = ENGINES
    sessions = db.query(LiveSession).all()
    # 一次取全部分组关系,避免 N+1(单用户规模可忽略,但分组写更干净)
    links = db.query(LiveSessionPortfolio).all()
    by_session: Dict[int, list] = {}
    for l in links:
        by_session.setdefault(l.session_id, []).append(
            {"portfolio_id": l.portfolio_strategy_id, "status": l.status}
        )
    # H2：各会话待补发拆单剩余笔数（仅 pending；列表/编辑弹窗提示用）
    split_counts: Dict[int, int] = dict(
        db.query(LiveSessionPendingChange.live_session_id,
                 func.count(LiveSessionPendingChange.id))
        .filter(
            LiveSessionPendingChange.field == "split_remainder",
            LiveSessionPendingChange.status == "pending",
        )
        .group_by(LiveSessionPendingChange.live_session_id)
        .all()
    )
    # G2：全局盘前配置只读一次（避免每个会话重复读文件）
    _tq_raw = load_config().get("tq_trade") or {}
    _tq_cfg = _tq_raw if isinstance(_tq_raw, dict) else {}
    return [
        {
            "id": s.id,
            "name": s.name,
            "mode": s.mode,
            "engine": session_engine(s),
            "execution_mode": s.execution_mode,
            # 会话级实盘资金基准：None=各组合用自身 initial_capital
            "live_capital": s.live_capital,
            # 外部信号注入（0011）：None=不启用
            "signal_file_path": s.signal_file_path,
            # 14:50 提前收盘价（仅 1d）：会话级开关 + 时点 + 当日幂等标记
            "advance_close_enabled": bool(s.advance_close_enabled),
            "advance_close_time": s.advance_close_time or "14:51",
            "advance_close_done_date": s.advance_close_done_date,
            # 错周期 bar 风控（三态原始值：None=跟随全局，true/false=会话覆盖）
            "intraday_mismatched_risk_enabled":
                s.intraday_mismatched_risk_enabled,
            # E1：执行前重判门控（默认关）
            "gate_recheck_on_execute": bool(
                getattr(s, "gate_recheck_on_execute", False)),
            # G2：盘前参数会话级覆盖（原始三态）+ 生效值（含来源）
            "pre_open_auction_enabled": getattr(
                s, "pre_open_auction_enabled", None),
            "pre_open_auction_end_time": getattr(
                s, "pre_open_auction_end_time", None),
            "pre_open_auction_price_bias_pct": (
                None if getattr(s, "pre_open_auction_price_bias_pct", None) is None
                else float(s.pre_open_auction_price_bias_pct)),
            "pre_open_auction_effective": pre_open_auction_effective(s, _tq_cfg),
            # A1 资金池：列表回显，编辑弹窗据此预填
            "capital_pool_mode": getattr(s, "capital_pool_mode", None) or "off",
            "capital_pool_rebalance_enabled": bool(
                getattr(s, "capital_pool_rebalance_enabled", False)),
            # B2 主备通道
            "backup_engine": getattr(s, "backup_engine", None),
            "failover_enabled": bool(getattr(s, "failover_enabled", False)),
            # H2：待补发拆单剩余笔数（>0 时会话行显示「补发拆单剩余」入口）
            "split_remainder_count": int(split_counts.get(s.id, 0)),
            "status": s.status,
            # 引擎是否真的在本进程注册表中运行（工作台据此区分「状态陈旧」与
            # 真正运行中：status=running 但 engine_running=false = 后端重启后
            # 引擎未恢复的残留状态，见 recover_stale_running_live_sessions）。
            "engine_running": s.id in engines,
            "started_at": s.started_at,
            "stopped_at": s.stopped_at,
            # C4（2026-11-16）：账户键 + 占用起始时间，供前端做「引擎/账户占用」
            # 置灰与多账户并行横幅聚合（running/stopping 才算占用，否则 None）。
            "account_key": getattr(s, "account_key", None),
            "account_key_source": getattr(s, "account_key_source", None),
            "occupied_since": (
                _occupied_since(s).isoformat()
                if s.status in ("running", "stopping") and _occupied_since(s)
                else None
            ),
            # 资金基准（真实总资产口径）：baseline_value 优先，回退旧列 live_capital
            "baseline_mode": getattr(s, "baseline_mode", None),
            "baseline_value": (
                float(getattr(s, "baseline_value", None))
                if getattr(s, "baseline_value", None) is not None
                else (float(getattr(s, "live_capital", None))
                      if getattr(s, "live_capital", None) is not None else None)
            ),
            "baseline_version": int(getattr(s, "baseline_version", 0) or 0),
            "portfolio_ids": sorted(p["portfolio_id"] for p in by_session.get(s.id, [])),
            "portfolios": by_session.get(s.id, []),
            # I-7（audit）：运行中引擎最近一次对账差异摘要；未运行（无引擎）为 []
            "reconcile_mismatches": (
                list(engines[s.id].reconcile_mismatches) if s.id in engines else []
            ),
        }
        for s in sessions
    ]


def _status_change_log(session: LiveSession, old: Optional[str], new_status: str,
                       caller: str, reason: str = "") -> None:
    """``live_sessions.status`` 每次变化的统一留痕（old→new + 调用方 + 进程）。"""
    logger.warning(
        "实盘会话状态变更 #%s「%s」: %s → %s（调用方=%s，%s，原因=%s）",
        getattr(session, "id", "?"), getattr(session, "name", ""),
        old, new_status, caller, process_tag(), reason or "-")


def set_session_status(session: LiveSession, new_status: str, *,
                       caller: str, reason: str = "") -> bool:
    """统一写 ``live_sessions.status``：所有写入点必须走这里。

    2026-11-19 事故：会话启动 0.38 秒后被写成 ``stopped``，但四处写入点各写
    各的、``finalize_stopped`` 不留痕，查不到是谁写的。本函数把 old→new、
    调用方标识、进程 tag（pid/port）一并打进日志（``logs/core.log`` 可查），
    并返回是否真的发生了变化（相同状态不写日志、不触发 updated_at）。
    """
    old = getattr(session, "status", None)
    if old == new_status:
        return False
    session.status = new_status
    _status_change_log(session, old, new_status, caller, reason)
    return True


#: 本进程内各会话的启动序号（不落库：跨进程代际以 ``started_at`` 判定，
#: 仅有排查价值，如需持久化需新增列——先报批再加）。
_START_SEQ: Dict[int, int] = {}


def next_start_seq(session_id: int) -> int:
    """本进程内该会话的第几次启动（1 起）；仅用于日志/排障。"""
    seq = _START_SEQ.get(int(session_id), 0) + 1
    _START_SEQ[int(session_id)] = seq
    return seq



def mark_session_started(session: LiveSession, *,
                         caller: str = "api.start_session") -> int:
    """启动成功后的统一落库前处理：running + started_at + 清上一轮 stopped_at。

    返回本进程内启动序号。启动序号只进日志（不新增列）：跨进程的代际以
    ``started_at`` 判定（见 ``engine_watchdog._orphan_evidence``）。
    """
    seq = next_start_seq(int(session.id))
    changed = set_session_status(
        session, "running", caller=caller,
        reason="启动成功（本进程内第 %d 次，清空上一轮 stopped_at）" % seq)
    session.started_at = datetime.now()
    session.stopped_at = None
    if not changed:
        # 已是 running（重复启动/状态未落库）：状态未变化也要留一条启动痕迹
        logger.warning(
            "实盘会话启动 #%s「%s」: 本进程内第 %d 次（调用方=%s，%s）",
            session.id, getattr(session, "name", ""), seq, caller,
            process_tag())
    return seq


def recover_stale_running_live_sessions(
    db: Session, engines: Optional[Dict[int, LiveEngine]] = None,
) -> int:
    """启动时清理僵尸 running 实盘会话（进程崩溃/重启残留）。

    与 ``recover_stale_running_backtests`` 同思路：引擎注册表 ``ENGINES`` 是
    进程内状态，重启后必然为空；DB 若仍留 ``status="running"``，工作台会显示
    「运行中」而实际没有任何引擎在跑（下单循环/行情轮询/成交回填全部停止，
    且 UI 无从分辨）。这里统一置 ``stopped`` 并逐条记日志（可读原因），由用户
    确认后手动重新启动。

    边界：``engines``（缺省全局注册表）非空时直接返回 0——存在运行中引擎说明
    不是冷启动，绝不能误改真实运行态。幂等：只匹配 ``status="running"``，重复
    调用返回 0。返回本次被清理的会话数。
    """
    registry = ENGINES if engines is None else engines
    if registry:
        return 0
    stale = (
        db.query(LiveSession)
        .filter(LiveSession.status.in_(("running", "stopping")))
        .all()
    )
    if not stale:
        return 0
    now = datetime.now()
    for session in stale:
        set_session_status(
            session, "stopped", caller="recover_stale_running_live_sessions",
            reason="冷启动：进程内注册表为空")
        session.stopped_at = now
    db.commit()
    for session in stale:
        logger.warning(
            "启动恢复：实盘会话 #%s「%s」残留运行态（引擎未随重启恢复，实际未运行），"
            "已置 stopped；如需继续交易请在界面重新启动",
            session.id, session.name,
        )
    return len(stale)


# ---- G2 盘前集合竞价参数会话级覆盖（2026-11-18）----
# 三列均可空：NULL = 跟随 config.tq_trade.pre_open_auction_* 全局值；
# 非空 = 本会话显式覆盖。仅作用于本会话，不改全局配置。
_PRE_OPEN_OVERRIDE_FIELDS = (
    "pre_open_auction_enabled",
    "pre_open_auction_end_time",
    "pre_open_auction_price_bias_pct",
)


def _normalize_pre_open_end_time(value) -> Optional[str]:
    """HH:MM → 规范化文本；仅接受 09:15–09:30（窗口起点固定 09:15）。"""
    text = str(value or "").strip()
    try:
        hh, mm = text.split(":", 1)
        h, m = int(hh), int(mm)
    except (TypeError, ValueError):
        return None
    if not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    if (h, m) < (9, 15) or (h, m) > (9, 30):
        return None
    return "%02d:%02d" % (h, m)


def normalize_pre_open_overrides(data: dict):
    """G2：校验三个覆盖字段；返回 (patch, (status, message) | None)。

    缺键 = 不改；显式 null = 清除覆盖（跟随全局）。
    """
    patch: dict = {}
    if "pre_open_auction_enabled" in data:
        raw = data.get("pre_open_auction_enabled")
        if raw is None:
            patch["pre_open_auction_enabled"] = None
        elif isinstance(raw, bool):
            patch["pre_open_auction_enabled"] = raw
        elif isinstance(raw, int) and raw in (0, 1):
            patch["pre_open_auction_enabled"] = bool(raw)
        else:
            return None, (
                400, "pre_open_auction_enabled 仅支持 null / true / false")
    if "pre_open_auction_end_time" in data:
        raw = data.get("pre_open_auction_end_time")
        if raw is None or str(raw).strip() == "":
            patch["pre_open_auction_end_time"] = None
        else:
            text = _normalize_pre_open_end_time(raw)
            if text is None:
                return None, (
                    400, "pre_open_auction_end_time 须为 09:15–09:30 的 HH:MM")
            patch["pre_open_auction_end_time"] = text
    if "pre_open_auction_price_bias_pct" in data:
        raw = data.get("pre_open_auction_price_bias_pct")
        if raw is None:
            patch["pre_open_auction_price_bias_pct"] = None
        else:
            try:
                bias = float(raw)
            except (TypeError, ValueError):
                return None, (
                    400, "pre_open_auction_price_bias_pct 须为 0–10 的数字")
            if not (0 <= bias <= 10):
                return None, (
                    400, "pre_open_auction_price_bias_pct 须在 0–10 之间")
            patch["pre_open_auction_price_bias_pct"] = (
                Decimal(str(bias)).quantize(Decimal("0.000001")))
    return patch, None


def resolve_pre_open_auction_config(session_row, tq_cfg: dict) -> tuple:
    """G2：全局 tq_trade 配置 + 会话非 NULL 覆盖 → (effective, overrides)。

    overrides 只含被会话覆盖的字段名，供启动事件/前端说明「哪些值是会话级」。
    """
    cfg = dict(tq_cfg) if isinstance(tq_cfg, dict) else {}
    overrides: List[str] = []
    if session_row is not None:
        enabled = getattr(session_row, "pre_open_auction_enabled", None)
        if enabled is not None:
            cfg["pre_open_auction_enabled"] = bool(enabled)
            overrides.append("enabled")
        end_time = getattr(session_row, "pre_open_auction_end_time", None)
        if end_time:
            cfg["pre_open_auction_end_time"] = str(end_time)
            overrides.append("end_time")
        bias = getattr(session_row, "pre_open_auction_price_bias_pct", None)
        if bias is not None:
            cfg["pre_open_auction_price_bias_pct"] = float(bias)
            overrides.append("price_bias_pct")
    return cfg, overrides


def pre_open_auction_effective(session_row, tq_cfg: Optional[dict] = None) -> dict:
    """G2：会话详情回显的生效盘前配置（含来源标记）。

    source=session 表示至少一项被会话覆盖；global 表示全部跟随全局。
    """
    if tq_cfg is None:
        raw = load_config().get("tq_trade") or {}
        tq_cfg = raw if isinstance(raw, dict) else {}
    cfg, overrides = resolve_pre_open_auction_config(session_row, tq_cfg)
    default = {"enabled": True, "end_time": "09:20", "price_bias_pct": 2.0}
    return {
        "enabled": bool(cfg.get("pre_open_auction_enabled", default["enabled"])),
        "end_time": str(cfg.get("pre_open_auction_end_time")
                        or default["end_time"]),
        "price_bias_pct": float(cfg.get("pre_open_auction_price_bias_pct")
                                if cfg.get("pre_open_auction_price_bias_pct")
                                is not None else default["price_bias_pct"]),
        "source": "session" if overrides else "global",
        "overrides": overrides,
    }


def normalize_advance_close(data: dict) -> tuple:
    """14:50 提前收盘价配置归一：开关默认 False；时点默认 14:51，限定 14:51–14:59。

    时点必须晚于 14:50 分钟 bar 闭合（stime=14:51:00）且早于收盘 15:00，避免
    「用 14:50 价执行」被配置到取不到价的窗口。非法一律回退默认值。
    """
    enabled = bool(data.get("advance_close_enabled"))
    raw = str(data.get("advance_close_time") or "14:51").strip()
    try:
        hh, mm = raw.split(":")
        minutes = int(hh) * 60 + int(mm)
        if not (14 * 60 + 51 <= minutes <= 14 * 60 + 59):
            raw = "14:51"
    except (TypeError, ValueError):
        raw = "14:51"
    return enabled, raw


def normalize_mismatched_risk(value) -> Optional[bool]:
    """错周期风控三态归一：None/auto → NULL（跟随全局）；0/1/true/false 显式覆盖。"""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    text = str(value).strip().lower()
    if text in ("", "null", "none", "auto", "global"):
        return None
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return None


def create_session(db: Session, data: dict, mode: str) -> dict:
    """创建 session。新会话默认 alert；老会话由迁移保持 auto。"""
    execution_mode = data.get("execution_mode") or "alert"
    # 外部信号注入（0011）：空串/纯空白 → None（不启用）；非空原样存储（API 层同规则）
    signal_file_path = data.get("signal_file_path")
    if signal_file_path is not None:
        signal_file_path = str(signal_file_path).strip() or None
    advance_close_enabled, advance_close_time = normalize_advance_close(data)
    # G2：盘前参数会话级覆盖（非法值由 API 层 400；服务层只取合法键）
    pre_open_patch, _ = normalize_pre_open_overrides(data)
    # 资金基准（2026-11-16）：live_capital 语义 = 实盘资金基准（真实总资产口径）。
    # baseline_mode：total_asset=起账即用真实总资产（默认）/ manual=显式填写；
    # 兼容旧叫法 adopt_actual（统一映射到 total_asset），不出现两套口径。
    baseline_value = data.get("live_capital")
    if baseline_value is None and data.get("baseline_value") is not None:
        baseline_value = data.get("baseline_value")
    baseline_mode = str(data.get("baseline_mode") or "").strip()
    if baseline_mode == "adopt_actual":
        baseline_mode = "total_asset"
    if baseline_mode not in ("total_asset", "manual"):
        baseline_mode = "manual" if baseline_value is not None else "total_asset"
    # H2 参数绑定（A7）：绑定某条回测记录后，启动时比对 params_snapshot 与实际参数。
    # 软引用（不设 FK）：回测记录被删不影响会话；非法值静默忽略（API 层已校验存在性）。
    backtest_record_id = data.get("backtest_record_id")
    if backtest_record_id is not None:
        try:
            backtest_record_id = int(backtest_record_id)
        except (TypeError, ValueError):
            backtest_record_id = None
    # A1 资金池模式（默认 off；非法值静默回退 off，API 层已做 400 校验）
    pool_mode = str(data.get("capital_pool_mode") or "off").strip()
    if pool_mode not in ("off", "equal", "risk_budget", "priority"):
        pool_mode = "off"
    pool_rebalance = bool(data.get("capital_pool_rebalance_enabled"))
    session = LiveSession(
        name=data["name"], mode=mode, status="stopped",
        engine=data.get("engine") or default_engine(),
        execution_mode=execution_mode,
        # A1 资金池：off 默认（分账不分池）；其余模式首 bar 按权重分配
        capital_pool_mode=pool_mode,
        capital_pool_rebalance_enabled=pool_rebalance,
        live_capital=baseline_value,
        baseline_mode=baseline_mode,
        baseline_value=baseline_value,
        baseline_set_at=(datetime.now() if baseline_value is not None else None),
        baseline_version=0,
        adopt_inherited_positions=bool(data.get("adopt_inherited_positions", True)),
        # 外部信号注入（0011）：None=不启用；非空=通达信条件预警输出文件路径
        signal_file_path=signal_file_path,
        # 14:50 提前收盘价（仅 1d）：默认关 + 14:51
        advance_close_enabled=advance_close_enabled,
        advance_close_time=advance_close_time,
        # 错周期 bar 风控（三态）：NULL=跟随全局默认，0/1=会话覆盖
        intraday_mismatched_risk_enabled=normalize_mismatched_risk(
            data.get("intraday_mismatched_risk_enabled")),
        # E1：执行前重判门控（默认关）
        gate_recheck_on_execute=bool(data.get("gate_recheck_on_execute")),
        # G2：盘前参数会话级覆盖（NULL=跟随全局）
        pre_open_auction_enabled=pre_open_patch.get(
            "pre_open_auction_enabled"),
        pre_open_auction_end_time=pre_open_patch.get(
            "pre_open_auction_end_time"),
        pre_open_auction_price_bias_pct=pre_open_patch.get(
            "pre_open_auction_price_bias_pct"),
        backtest_record_id=backtest_record_id,
    )
    db.add(session)
    db.flush()
    for pid in data.get("portfolio_ids", []):
        link = LiveSessionPortfolio(session_id=session.id, portfolio_strategy_id=pid)
        db.add(link)
    db.commit()
    # A1 软校验（不阻断）：目标引擎已被其它 running/stopping 会话占用时提示
    # 「启动前需先停止」，避免用户建完才发现启动 409。
    return {"id": session.id, "status": session.status,
            "engine": session_engine(session),
            "execution_mode": session.execution_mode,
            "backtest_record_id": backtest_record_id,
            "capital_pool_mode": pool_mode,
            "capital_pool_rebalance_enabled": pool_rebalance,
            "gate_recheck_on_execute": bool(
                session.gate_recheck_on_execute),
            # G2：盘前参数（原始三态 + 生效值）
            "pre_open_auction_enabled": session.pre_open_auction_enabled,
            "pre_open_auction_end_time": session.pre_open_auction_end_time,
            "pre_open_auction_price_bias_pct": (
                None if session.pre_open_auction_price_bias_pct is None
                else float(session.pre_open_auction_price_bias_pct)),
            "pre_open_auction_effective": pre_open_auction_effective(session),
            "warnings": _engine_occupancy_warnings(db, session)}


def get_session(db: Session, session_id: int, engine: Optional[LiveEngine] = None) -> Optional[dict]:
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None
    portfolios = (
        db.query(LiveSessionPortfolio)
        .filter(LiveSessionPortfolio.session_id == session_id)
        .all()
    )
    # ③ 外部现金流累计净流入（一次查询，供各组合展示）
    net_map = cash_flow_net_inflow(db, session_id)
    data = {
        "id": session.id,
        "name": session.name,
        "mode": session.mode,
        "engine": session_engine(session),
        "execution_mode": session.execution_mode,
        # 会话级实盘资金基准：None=各组合用自身 initial_capital
        "live_capital": session.live_capital,
        "baseline_mode": getattr(session, "baseline_mode", None),
        "baseline_value": (
            float(session.baseline_value)
            if getattr(session, "baseline_value", None) is not None else None),
        "baseline_version": int(getattr(session, "baseline_version", 0) or 0),
        "baseline_set_at": getattr(session, "baseline_set_at", None),
        "adopt_inherited_positions": bool(
            getattr(session, "adopt_inherited_positions", True)),
        # H2 参数绑定（A7）：绑定的回测记录 + 当前参数与快照差异（None=未绑定）
        "backtest_record_id": getattr(session, "backtest_record_id", None),
        "params_drift": compute_params_drift(db, session),
        # 外部信号注入（0011）：None=不启用
        "signal_file_path": session.signal_file_path,
        # A1 资金池模式与日再平衡开关（默认 off/false）
        "capital_pool_mode": getattr(session, "capital_pool_mode", None) or "off",
        "capital_pool_rebalance_enabled": bool(
            getattr(session, "capital_pool_rebalance_enabled", False)),
        # B2 主备通道（默认 NULL / 0）
        "backup_engine": getattr(session, "backup_engine", None),
        "failover_enabled": bool(getattr(session, "failover_enabled", False)),
        # H2：待补发拆单剩余笔数
        "split_remainder_count": int(
            db.query(func.count(LiveSessionPendingChange.id))
            .filter(
                LiveSessionPendingChange.live_session_id == session_id,
                LiveSessionPendingChange.field == "split_remainder",
                LiveSessionPendingChange.status == "pending",
            )
            .scalar() or 0),
        # 14:50 提前收盘价（仅 1d）
        "advance_close_enabled": bool(session.advance_close_enabled),
        "advance_close_time": session.advance_close_time or "14:51",
        "advance_close_done_date": session.advance_close_done_date,
        # 错周期 bar 风控（三态原始值：None=跟随全局，true/false=会话覆盖）
        "intraday_mismatched_risk_enabled":
            session.intraday_mismatched_risk_enabled,
        # E1：执行前重判门控（默认关）
        "gate_recheck_on_execute": bool(
            getattr(session, "gate_recheck_on_execute", False)),
        # G2：盘前参数会话级覆盖（原始三态）+ 生效值（含来源）
        "pre_open_auction_enabled": getattr(
            session, "pre_open_auction_enabled", None),
        "pre_open_auction_end_time": getattr(
            session, "pre_open_auction_end_time", None),
        "pre_open_auction_price_bias_pct": (
            None if getattr(session, "pre_open_auction_price_bias_pct", None)
            is None else float(session.pre_open_auction_price_bias_pct)),
        "pre_open_auction_effective": pre_open_auction_effective(session),
        "status": session.status,
        "started_at": session.started_at,
        "stopped_at": session.stopped_at,
        "portfolio_ids": sorted(p.portfolio_strategy_id for p in portfolios),
        "portfolios": [
            {
                "portfolio_id": p.portfolio_strategy_id,
                "status": p.status,
                # ③ 外部现金流累计净流入（>0 净入金）：净值/回撤按剔除该值计算
                "net_inflow": str(
                    net_map.get(p.portfolio_strategy_id, Decimal("0"))),
            }
            for p in portfolios
        ],
    }
    # G7（0011 §5.11）：桥状态并入 session API。运行中取引擎实时态：
    # bridge_online 为引擎内存最近已知心跳态（不新发 /ping，避免页面请求被慢桥阻塞），
    # pending_orders 在途单计数，last_backfill_time 最近一次成交回报回填时点；未运行返回空值。
    # I-7（audit）：reconcile_mismatches = 引擎内存最近一次对账差异摘要（未运行 []）。
    if engine is not None:
        data["bridge_online"] = _live_bridge_online(engine)
        data["pending_orders"] = engine.pending_orders_count
        data["last_backfill_time"] = (
            engine.last_backfill_time.strftime("%Y-%m-%d %H:%M:%S")
            if engine.last_backfill_time is not None else None
        )
        data["reconcile_mismatches"] = list(engine.reconcile_mismatches)
        # 交易日历来源/退化状态（"是否在用退化日历"必须可见；未加载年份为空）
        cal = getattr(engine, "_calendar", None)
        data["trading_calendar"] = (
            cal.status() if cal is not None and hasattr(cal, "status") else None)
    else:
        data["bridge_online"] = None
        data["pending_orders"] = 0
        data["last_backfill_time"] = None
        data["reconcile_mismatches"] = []
        data["trading_calendar"] = None
    # 账户全貌对齐：最新真实账户快照（总资产/可用资金/市值/持仓明细）。
    # 运行中优先取引擎内存最近一次快照组装（与采集同源），否则查表最近一行。
    data["account_snapshot"] = _latest_account_snapshot(db, session_id, engine)
    # ⑥ 真实账户总资产 vs 虚拟净值：运行中由引擎在账户采集时比对（偏差 >20%
    # 每会话告警一次）；未运行无法重算虚拟净值，只回真实侧最后快照。
    check = getattr(engine, "_latest_equity_check", None) if engine is not None else None
    if check is None:
        snap_total = (data.get("account_snapshot") or {}).get("total_asset")
        check = {
            "real_total": None if snap_total is None else str(snap_total),
            "out_of_pool_value": None,
            "expected_total": None,
            "virtual_total": None,
            "deviation_pct": None,
            "computed_at": None,
            "note": "会话未运行：虚拟净值需启动后计算",
        }
    data["equity_check"] = check
    # 序列化兜底：任何路径混进来的 inf/-inf/nan 在这里归一为 None，
    # 保证 GET /api/live/sessions/{id} 不会因一个脏字段整接口 500。
    return _json_safe(data)


# ---------------------------------------------------------------------------
# B1 + A4：会话配置编辑（未运行全量可改；运行中入待生效队列，下次启动生效）
# ---------------------------------------------------------------------------
#: PUT /sessions/{id} 允许编辑的字段（白名单，未知字段忽略）。
SESSION_EDIT_FIELDS = (
    "name", "engine", "backup_engine", "failover_enabled",
    "execution_mode", "signal_file_path",
    "advance_close_enabled", "advance_close_time",
    "intraday_mismatched_risk_enabled", "capital_pool_mode",
    "capital_pool_rebalance_enabled", "portfolio_ids",
    "gate_recheck_on_execute",
    # G2：盘前参数会话级覆盖（NULL=跟随全局）
    "pre_open_auction_enabled", "pre_open_auction_end_time",
    "pre_open_auction_price_bias_pct",
)
#: 运行中只能「下次启动生效」的字段（name 为纯展示字段，立即生效）。
SESSION_NEXT_START_FIELDS = frozenset(SESSION_EDIT_FIELDS) - {"name"}


def _sync_session_portfolios(db: Session, session_id: int, pids: List[int]) -> None:
    """A4：把会话的组合关联同步为目标集合（保留会话与历史成交，只增删关联行）。"""
    wanted = set(pids)
    rows = (
        db.query(LiveSessionPortfolio)
        .filter(LiveSessionPortfolio.session_id == session_id)
        .all()
    )
    existing = {row.portfolio_strategy_id: row for row in rows}
    for pid in sorted(wanted - set(existing)):
        db.add(LiveSessionPortfolio(
            session_id=session_id, portfolio_strategy_id=pid))
    for pid, row in existing.items():
        if pid not in wanted:
            db.delete(row)


def _apply_session_field(db: Session, session: LiveSession, field: str, value) -> None:
    """写入单个会话字段（portfolio_ids 同步关联表）；调用方保证 field 已白名单。"""
    if field == "portfolio_ids":
        _sync_session_portfolios(db, session.id, [int(p) for p in value])
        return
    setattr(session, field, value)


def _normalize_session_patch(db: Session, data: dict,
                             current_engine: Optional[str] = None):
    """校验并归一 PUT body；返回 (patch, (status, message) | None)。"""
    patch: dict = {}
    if "name" in data:
        name = str(data.get("name") or "").strip()
        if not name:
            return None, (400, "会话名称不能为空")
        patch["name"] = name
    if "engine" in data:
        engine = str(data.get("engine") or "").strip()
        if engine not in ENGINES_META:
            return None, (400, "engine 非法，仅支持 %s" % " / ".join(ENGINES_META))
        patch["engine"] = engine
    if "backup_engine" in data:
        raw = data.get("backup_engine")
        if raw is None or str(raw).strip() == "":
            patch["backup_engine"] = None
        else:
            backup = str(raw).strip()
            if backup not in ENGINES_META:
                return None, (400, "backup_engine 非法，仅支持 %s"
                              % " / ".join(ENGINES_META))
            if backup == patch.get("engine", current_engine):
                return None, (400, "备用引擎不能与主引擎相同")
            patch["backup_engine"] = backup
    if "failover_enabled" in data:
        patch["failover_enabled"] = bool(data.get("failover_enabled"))
    if "execution_mode" in data:
        execution_mode = str(data.get("execution_mode") or "").strip()
        if execution_mode not in ("alert", "auto", "manual", "shadow"):
            return None, (400, "execution_mode 非法，仅支持 alert / auto / manual / shadow")
        patch["execution_mode"] = execution_mode
    if "signal_file_path" in data:
        raw = data.get("signal_file_path")
        patch["signal_file_path"] = (
            str(raw).strip() or None if raw is not None else None)
    if "advance_close_enabled" in data:
        patch["advance_close_enabled"] = bool(data.get("advance_close_enabled"))
    if "advance_close_time" in data:
        raw = str(data.get("advance_close_time") or "14:51").strip()
        try:
            hh, mm = raw.split(":")
            minutes = int(hh) * 60 + int(mm)
        except (TypeError, ValueError):
            return None, (400, "advance_close_time 须为 HH:MM 格式")
        if not (14 * 60 + 51 <= minutes <= 14 * 60 + 59):
            return None, (400, "advance_close_time 仅支持 14:51–14:59")
        patch["advance_close_time"] = raw
    if "intraday_mismatched_risk_enabled" in data:
        raw = data.get("intraday_mismatched_risk_enabled")
        if raw is None:
            patch["intraday_mismatched_risk_enabled"] = None
        elif isinstance(raw, bool):
            patch["intraday_mismatched_risk_enabled"] = raw
        elif isinstance(raw, int) and raw in (0, 1):
            patch["intraday_mismatched_risk_enabled"] = bool(raw)
        else:
            return None, (
                400, "intraday_mismatched_risk_enabled 仅支持 null / true / false")
    if "pre_open_auction_enabled" in data or \
            "pre_open_auction_end_time" in data or \
            "pre_open_auction_price_bias_pct" in data:
        pre_open_patch, pre_open_error = normalize_pre_open_overrides(data)
        if pre_open_error:
            return None, pre_open_error
        patch.update(pre_open_patch)
    if "capital_pool_mode" in data:
        mode = str(data.get("capital_pool_mode") or "off").strip()
        if mode not in ("off", "equal", "risk_budget", "priority"):
            return None, (400, "capital_pool_mode 非法，仅支持 off/equal/risk_budget/priority")
        patch["capital_pool_mode"] = mode
    if "capital_pool_rebalance_enabled" in data:
        patch["capital_pool_rebalance_enabled"] = bool(
            data.get("capital_pool_rebalance_enabled"))
    if "gate_recheck_on_execute" in data:
        patch["gate_recheck_on_execute"] = bool(
            data.get("gate_recheck_on_execute"))
    if "portfolio_ids" in data:
        pids = data.get("portfolio_ids")
        if not isinstance(pids, list):
            return None, (400, "portfolio_ids 必须为数组")
        try:
            pids = [int(p) for p in pids]
        except (TypeError, ValueError):
            return None, (400, "portfolio_ids 只能包含整数组合 id")
        if len(set(pids)) != len(pids):
            return None, (400, "portfolio_ids 存在重复项")
        if pids:
            existing = {
                row.id for row in
                db.query(PortfolioStrategy.id)
                .filter(PortfolioStrategy.id.in_(pids)).all()
            }
            missing = [str(p) for p in pids if p not in existing]
            if missing:
                return None, (400, "组合不存在：%s" % "、".join(missing))
        patch["portfolio_ids"] = pids
    if not patch:
        return None, (400, "没有可更新的字段（支持：%s）"
                      % " / ".join(SESSION_EDIT_FIELDS))
    return patch, None


def _enqueue_session_pending(db: Session, session: LiveSession,
                             field: str, value) -> LiveSessionPendingChange:
    """运行中会话的字段变更入待生效队列（同会话同字段只保留一条 pending）。"""
    row = (
        db.query(LiveSessionPendingChange)
        .filter(LiveSessionPendingChange.live_session_id == session.id,
                LiveSessionPendingChange.portfolio_strategy_id.is_(None),
                LiveSessionPendingChange.field == field,
                LiveSessionPendingChange.status == "pending")
        .first()
    )
    if row is None:
        row = LiveSessionPendingChange(
            live_session_id=session.id, portfolio_strategy_id=None,
            strategy_id=None, field=field)
        db.add(row)
    old_value = getattr(session, field, None)
    row.old_value = json.dumps(old_value, ensure_ascii=False, default=str)
    row.new_value = json.dumps(value, ensure_ascii=False)
    row.note = "会话配置编辑（下次启动生效）"
    row.status = "pending"
    row.applied_at = None
    return row


def update_session(db: Session, session_id: int, data: dict,
                   engines: Dict[int, LiveEngine]):
    """B1 + A4：编辑会话配置。

    - 未运行：全部字段立即写库（组合增删同步关联表，保留会话与历史成交）；
    - 运行中：`name` 立即生效，其余字段入 D3 待生效队列，下次启动前应用。
    返回 (ok, data_or_(status, message))。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return False, (404, "资源不存在")
    patch, error = _normalize_session_patch(db, data, session_engine(session))
    if error:
        return False, error
    running = (session_id in engines
               or session.status in ("running", "stopping"))
    applied: List[str] = []
    pending: List[str] = []
    for field, value in patch.items():
        if running and field in SESSION_NEXT_START_FIELDS:
            _enqueue_session_pending(db, session, field, value)
            pending.append(field)
        else:
            _apply_session_field(db, session, field, value)
            applied.append(field)
    db.commit()
    result = get_session(db, session_id, engines.get(session_id))
    return True, {
        "session": result,
        "applied": applied,
        "pending": pending,
        "effective_at": "next_start" if pending else "now",
        "message": ("会话运行中：%s 已加入待生效队列，下次启动生效"
                    % "、".join(pending)) if pending else "已立即生效",
    }


# ---------------------------------------------------------------------------
# B2：主备通道健康探活 + 人工确认切换
# ---------------------------------------------------------------------------

def _channel_health_one(engine_key: Optional[str],
                        engines: Optional[Dict[int, LiveEngine]]) -> Optional[dict]:
    if not engine_key:
        return None
    data = channel_status(engine_key=engine_key, engines=engines)
    return {
        "engine": engine_key,
        "label": data.get("label") or ENGINES_META.get(engine_key, (None, engine_key))[1],
        "online": data.get("online"),
        "online_source": data.get("online_source"),
        "engine_running": bool(data.get("engine_running")),
        "configured": bool(data.get("configured")),
        "detail": data.get("detail") or "",
        "stale": bool(data.get("stale")),
        "checked_at": data.get("checked_at"),
    }


def session_channel_health(db: Session, session_id: int,
                           engines: Optional[Dict[int, LiveEngine]] = None):
    """B2：主/备通道健康探活（只读，不新建轮询）。

    - 主通道：会话生效引擎；运行中优先读引擎内存在线态，未运行真实探活；
    - 备用通道：未配置返回 None；配置后同样真实探活（tdx/ths 看窗口，
      iquant/goldminer 探 HTTP 桥）。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return None
    primary = session_engine(session)
    backup = getattr(session, "backup_engine", None)
    return {
        "session_id": session_id,
        "primary": _channel_health_one(primary, engines),
        "backup": _channel_health_one(backup, engines),
        "failover_enabled": bool(getattr(session, "failover_enabled", False)),
        "advice": ("主通道离线时可一键切换备用通道；运行中切换于下次启动生效"
                   if backup else "未配置备用通道（编辑会话可设置）"),
    }


def session_data_health(db: Session, session_id: int,
                        engines: Optional[Dict[int, LiveEngine]] = None,
                        auto_fill: bool = True,
                        now: Optional[datetime] = None):
    """F2：会话数据体检 + 非盘中自动补齐（只读，不改交易语义）。

    - 运行中：用引擎 dispatcher/日历（与实盘同一通道口径）；
    - 未运行：按会话引擎临时构造 dispatcher（tdx/ths 走本机 TQ；桥走 HTTP），
      构造失败返回 available=False 与原因；
    - 周期取会话内策略实际使用的 period（至少 1d），代码取组合股票池并集。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return None
    periods = sorted({
        getattr(st, "period", None)
        for _pid, st in _session_strategies(db, session_id)
        if getattr(st, "period", None)
    }) or ["1d"]
    links = (
        db.query(LiveSessionPortfolio)
        .filter(LiveSessionPortfolio.session_id == session_id)
        .all()
    )
    pool_codes: set = set()
    for link in links:
        ps = db.query(PortfolioStrategy).filter_by(
            id=link.portfolio_strategy_id).first()
        if ps is not None:
            pool_codes.update(resolve_stock_codes(db, ps.id))
    engine = (engines or {}).get(session_id)
    dispatcher = getattr(engine, "dispatcher", None) if engine is not None else None
    calendar = getattr(engine, "_calendar", None) if engine is not None else None
    if dispatcher is None:
        try:
            dispatcher = make_dispatcher(
                session_engine(session),
                getattr(session, "mode", None) or "simulation", session_id)
        except Exception as exc:  # noqa: BLE001 — 通道依赖缺失如实返回
            return {
                "session_id": session_id, "available": False,
                "engine": session_engine(session), "engine_running": False,
                "reason": str(exc) or "交易通道未就绪",
                "periods_checked": periods,
            }
    result = run_health_check(
        dispatcher, sorted(pool_codes), periods,
        auto_fill=auto_fill, now=now, calendar=calendar)
    return {
        "session_id": session_id,
        "available": True,
        "engine": session_engine(session),
        "engine_running": engine is not None,
        "periods_checked": periods,
        "codes_total": len(pool_codes),
        "health": result["health"],
        "auto_fill": result["auto_fill"],
    }


def switch_session_channel(db: Session, session_id: int,
                           target_engine: Optional[str] = None,
                           engines: Optional[Dict[int, LiveEngine]] = None):
    """B2：人工确认后切换主通道。

    - 目标默认取会话 `backup_engine`；运行中入待生效队列（下次启动建新引擎），
      未运行立即改 engine；
    - 切换写一条 `channel_failover` 决策事件留痕（谁在何时切到哪条通道）；
    - 不做自动热切换：dispatcher 不能在持有在途委托/持仓内存态时被替换。
    """
    engines = engines or ENGINES
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return False, (404, "资源不存在")
    current = session_engine(session)
    target = str(target_engine or getattr(session, "backup_engine", None) or "").strip()
    if not target:
        return False, (400, "未配置备用通道，无法切换")
    if target not in ENGINES_META:
        return False, (400, "target_engine 非法，仅支持 %s"
                       % " / ".join(ENGINES_META))
    if target == current:
        return False, (400, "目标通道与当前主通道相同")
    if session.status == "stopping":
        return False, (409, "会话正在停止中，请等待停止完成后再切换")
    running = session_id in engines or session.status == "running"
    if running:
        _enqueue_session_pending(db, session, "engine", target)
        effective_at = "next_start"
        message = ("已提交切换：%s → %s，运行中会话将在下次启动生效"
                   % (current, target))
    else:
        session.engine = target
        effective_at = "now"
        message = "已切换主通道：%s → %s" % (current, target)
    db.add(LiveDecisionEvent(
        live_session_id=session_id,
        gate="channel_failover",
        layer="live_gate",
        action=("switch_pending" if running else "switch"),
        param_name="engine",
        message=("%s（操作人=人工确认，时点=%s）"
                 % (message, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))[:200],
    ))
    db.commit()
    return True, {
        "session_id": session_id,
        "from_engine": current,
        "to_engine": target,
        "effective_at": effective_at,
        "message": message,
    }


# ---------------------------------------------------------------------------
# B3：通道级守护检查清单 + 一键重新探活（不自动拉起终端）
# ---------------------------------------------------------------------------

_MANUAL_CHANNEL_HINT = {
    "tdx": "打开并登录通达信客户端，确认「交易信号」窗口可用后点「重新探活」",
    "ths": "打开并登录同花顺网上股票交易系统，确认窗口标题匹配后点「重新探活」",
    "iquant": "启动 QMT 客户端并确认 8790 桥服务可访问后点「重新探活」",
    "goldminer": "启动掘金终端并确认桥策略服务可访问后点「重新探活」",
}


def _force_channel_probe(engine_key: str) -> Tuple[Optional[bool], bool]:
    """清掉 30s 探活缓存后强制探一次（B3「一键重新探活」）。"""
    with _PROBE_CACHE_LOCK:
        _PROBE_CACHE.pop(engine_key, None)
    dispatcher = _probe_dispatcher_for(engine_key)
    if dispatcher is None:
        return None, False
    return _probe_online_quick(engine_key, dispatcher)


def session_channel_guard(db: Session, session_id: int,
                          engines: Optional[Dict[int, LiveEngine]] = None):
    """B3：前置通道检查清单（只读）。

    只提示、不自动拉起终端、不自动重启会话；每项给 ``ok`` 与人工处置建议。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return None
    engines = engines or ENGINES
    health = session_channel_health(db, session_id, engines)
    primary = health["primary"] or {}
    backup = health["backup"]
    running_status = session.status in ("running", "stopping")
    engine_key = primary.get("engine") or session_engine(session)
    registered = session_id in engines
    pending_count = (
        db.query(LiveSessionPendingChange)
        .filter(LiveSessionPendingChange.live_session_id == session_id,
                LiveSessionPendingChange.field.isnot(None),
                LiveSessionPendingChange.portfolio_strategy_id.is_(None),
                LiveSessionPendingChange.status == "pending")
        .count()
    )
    checks = [
        {"key": "primary_configured", "ok": bool(primary.get("configured")),
         "label": "主通道已配置",
         "detail": primary.get("detail") or engine_key},
        {"key": "client_online", "ok": bool(primary.get("online")),
         "label": "客户端/桥在线",
         "detail": ("在线" if primary.get("online") else
                    (primary.get("detail") or "离线"))},
        {"key": "engine_registered", "ok": (not running_status) or registered,
         "label": "运行态引擎已注册",
         "detail": ("未运行（不需要）" if not running_status else
                    ("已注册" if registered else "会话运行中但后端无引擎实例，需停止后重新启动"))},
        # 停止态不判备用通道：未运行会话没有「切换」需求，硬判未配置会制造
        # 假告警（2026-11-19：停止会话仍报「备用通道可用」失败）。
        {"key": "backup_ready",
         "ok": (not running_status) or bool(backup and backup.get("online")),
         "label": "备用通道可用",
         "detail": ("未运行（不需要）" if not running_status else
                    ("未配置" if backup is None else
                     ("在线" if backup.get("online") else "离线")))},
        {"key": "pending_changes", "ok": pending_count == 0,
         "label": "无待生效配置",
         "detail": ("无" if pending_count == 0 else
                    "%d 项待下次启动生效" % pending_count)},
    ]
    actions: List[str] = []
    if not primary.get("online"):
        actions.append(_MANUAL_CHANNEL_HINT.get(
            engine_key, "确认交易终端已启动并登录后点「重新探活」"))
    if running_status and not registered:
        actions.append("后端可能重启过：先在页面停止该会话，再重新启动以恢复引擎")
    if backup is not None and not backup.get("online"):
        actions.append("备用通道当前离线，暂不建议切换")
    ready_to_switch = bool(
        not primary.get("online") and backup and backup.get("online")
        and session.status != "stopping")
    return {
        "session_id": session_id,
        "status": session.status,
        "engine": engine_key,
        "checks": checks,
        "ready_to_switch": ready_to_switch,
        "actions": actions,
        "guard_mode": "advisory",
        "checked_at": datetime.now().isoformat(timespec="seconds"),
    }


def reconnect_session_channel(db: Session, session_id: int,
                              engines: Optional[Dict[int, LiveEngine]] = None):
    """B3：一键重新探活（不做危险的重启/拉起动作，结果回显人工处置建议）。"""
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return False, (404, "资源不存在")
    engine_key = session_engine(session)
    online, stale = _force_channel_probe(engine_key)
    hint = "" if online else _MANUAL_CHANNEL_HINT.get(
        engine_key, "确认交易终端已启动并登录")
    return True, {
        "session_id": session_id,
        "engine": engine_key,
        "online": online,
        "stale": stale,
        "message": ("通道在线" if online else "通道仍离线"),
        "manual_action": hint,
    }


# ---------------------------------------------------------------------------
# 外部现金流台账（③）
# ---------------------------------------------------------------------------
# 入金/出金/分红/其他。金额落库为**有符号**：>0 流入，<0 流出；调用方对
# deposit/dividend/withdraw 只需给正数（符号由类型决定），other 按给定符号判向。
CASH_FLOW_TYPES = ("deposit", "withdraw", "dividend", "other")
_CASH_FLOW_SIGN = {"deposit": 1, "withdraw": -1, "dividend": 1}


def cash_flow_net_inflow(db: Session, session_id: int) -> Dict[int, Decimal]:
    """各组合累计外部净流入（正=净入金）。recover 与展示共用同一口径。

    迁移未执行（无 live_cash_flows 表）时降级返回空 dict——现金流是增量能力，
    不能让会话详情/起账因缺表整体失败。
    """
    try:
        rows = (
            db.query(LiveCashFlow)
            .filter(LiveCashFlow.live_session_id == session_id)
            .all()
        )
    except Exception:  # noqa: BLE001
        db.rollback()
        logger.warning(
            "cash_flow_net_inflow: live_cash_flows 读取失败（是否未执行迁移？）"
            " → 按无外部现金流处理 (session %s)", session_id)
        return {}
    out: Dict[int, Decimal] = {}
    for r in rows:
        try:
            amt = Decimal(str(r.amount or 0))
        except (TypeError, ValueError):
            continue
        out[r.portfolio_strategy_id] = (
            out.get(r.portfolio_strategy_id, Decimal("0")) + amt)
    return out


def record_cash_flow(
    db: Session,
    session_id: int,
    portfolio_id: int,
    flow_type: str,
    amount,
    flow_date=None,
    note: Optional[str] = None,
    engine: Optional[LiveEngine] = None,
    allow_backfill: bool = False,
) -> Tuple[Optional[dict], Optional[str]]:
    """登记一笔外部现金流并计入虚拟账本（③）。

    虚拟现金随台账增减（入金加、出金减），同时累计净流入同增——净值/收益率/
    回撤按「总市值 − 累计净流入」计算，入金/出金不会污染业绩口径（回测无外部
    现金流，净流入恒 0，两端可比）。

    运行中引擎立即应用（内存态）；未运行只落台账，下次启动 recover 重放。
    返回 (row_dict, error)。error 非 None 时未落库。
    """
    if flow_type not in CASH_FLOW_TYPES:
        return None, "flow_type 非法，仅支持 %s" % " / ".join(CASH_FLOW_TYPES)
    try:
        raw = Decimal(str(amount))
    except (ArithmeticError, TypeError, ValueError):
        return None, "金额非法"
    sign = _CASH_FLOW_SIGN.get(flow_type)
    # deposit/dividend/withdraw：符号由类型决定（调用方给正数即可）；other 保留符号
    amt = abs(raw) * sign if sign is not None else raw
    # 台账落库是 Numeric(15,2)：先量化到分，返回/记账与库内口径一致
    amt = amt.quantize(Decimal("0.01"))
    if amt == 0:
        return None, "金额不能为 0"
    if flow_date is None:
        flow_day = now_shanghai().date()
    elif isinstance(flow_date, date):
        flow_day = flow_date
    else:
        try:
            flow_day = date.fromisoformat(str(flow_date))
        except ValueError:
            return None, "flow_date 须为 YYYY-MM-DD"

    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return None, "资源不存在"
    # C3（2026-11-18）日期边界：未来日期会把外部净流入提前计进今天的净值口径；
    # 早于会话起账日的流水会改写已发生的历史净值（当日/区间收益全部变）——
    # 两者都只在显式 allow_backfill / 合法日期下才允许。
    today = now_shanghai().date()
    if flow_day > today:
        return None, "flow_date 不能晚于今天（%s）" % today.isoformat()
    started = getattr(session, "created_at", None)
    started_day = started.date() if started is not None else None
    if started_day is not None and flow_day < started_day and not allow_backfill:
        return None, ("flow_date 早于会话起账日 %s；补记历史流水需显式传 "
                      "allow_backfill=true（会改写历史净值口径）"
                      % started_day.isoformat())
    link = (
        db.query(LiveSessionPortfolio)
        .filter_by(session_id=session_id, portfolio_strategy_id=portfolio_id)
        .first()
    )
    if link is None:
        return None, "组合未关联本会话"

    row = LiveCashFlow(
        live_session_id=session_id,
        portfolio_strategy_id=portfolio_id,
        flow_type=flow_type,
        amount=amt,
        flow_date=flow_day,
        note=(note or None),
    )
    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except Exception:  # noqa: BLE001 — 多为未执行迁移（无表）
        db.rollback()
        logger.exception("record_cash_flow: 台账写入失败 (session %s)", session_id)
        return None, "现金流台账不可用：请先执行数据库迁移（alembic upgrade head）"
    applied = False
    if engine is not None:
        try:
            applied = engine.apply_cash_flow(portfolio_id, amt)
        except Exception:  # noqa: BLE001 — 账已落库，内存应用失败由重启重放兜底
            logger.exception(
                "cash flow apply to engine failed (session %s, portfolio %s)",
                session_id, portfolio_id)
    net = cash_flow_net_inflow(db, session_id).get(portfolio_id, Decimal("0"))
    logger.info(
        "cash flow: session %s 组合 %s %s %s（累计净流入 %s，engine_applied=%s）",
        session_id, portfolio_id, flow_type, amt, net, applied)
    return {
        "id": row.id,
        "session_id": session_id,
        "portfolio_id": portfolio_id,
        "flow_type": flow_type,
        "amount": str(amt),
        "flow_date": flow_day.isoformat(),
        "note": row.note,
        "net_inflow": str(net),
        "engine_applied": applied,
    }, None


#: C1 差异台账状态常量（模型层定义，service/API/引擎共用一份）
DIFF_STATUS_PENDING = "pending"
DIFF_STATUS_ACCEPTED = "accepted"
DIFF_STATUS_RESOLVED = "resolved"
DIFF_STATUS_IGNORED = "ignored"


def reverse_cash_flow(
    db: Session, session_id: int, flow_id: int,
    note: Optional[str] = None, engine: Optional[LiveEngine] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """C2（2026-11-18）红冲：反向补一笔冲销原流水，禁止物理删除/改数。

    - 冲销行 `flow_type` 与原行一致、`amount` 取反 → Σamount 口径自动归零，
      净值（总市值 − 累计净流入）无需额外处理；
    - 原行 `reversed_by_id` + `voided=1`（展示划线），同一笔只能冲销一次；
    - 红冲行本身不可再被冲销（防套娃）；
    - 运行中引擎立即应用该反向金额，停止时下次启动重放。
    """
    row = db.query(LiveCashFlow).filter_by(
        id=flow_id, live_session_id=session_id).first()
    if row is None:
        return None, "资源不存在"
    if row.reversed_by_id is not None:
        return None, "该流水已被红冲，不能重复冲销"
    if row.reversal_of_id is not None:
        return None, "红冲行不能再被红冲（如需恢复请重新登记一笔流水）"
    if row.flow_date and row.flow_date > now_shanghai().date():
        return None, "原流水日期在未来，拒绝红冲"
    amount = -Decimal(str(row.amount))
    reversal = LiveCashFlow(
        live_session_id=row.live_session_id,
        portfolio_strategy_id=row.portfolio_strategy_id,
        flow_type=row.flow_type,
        amount=amount,
        flow_date=now_shanghai().date(),
        note=(note or ("红冲 #%s" % row.id))[:200],
        reversal_of_id=row.id,
    )
    try:
        db.add(reversal)
        db.flush()
        row.reversed_by_id = reversal.id
        row.voided = True
        db.commit()
        db.refresh(reversal)
    except Exception:  # noqa: BLE001 — 多为未执行迁移（缺列）
        db.rollback()
        logger.exception("reverse_cash_flow: 红冲失败 (session %s, flow %s)",
                         session_id, flow_id)
        return None, "现金流红冲不可用：请先执行数据库迁移（alembic upgrade head）"
    applied = False
    if engine is not None:
        try:
            applied = engine.apply_cash_flow(
                row.portfolio_strategy_id, amount)
        except Exception:  # noqa: BLE001 — 已落库，内存失败由重启重放兜底
            logger.exception("reverse_cash_flow: engine apply 失败 (session %s)",
                             session_id)
    net = cash_flow_net_inflow(db, session_id).get(
        row.portfolio_strategy_id, Decimal("0"))
    logger.info(
        "cash flow reverse: session %s 组合 %s 冲销 #%s %s（累计净流入 %s）",
        session_id, row.portfolio_strategy_id, row.id, amount, net)
    return {
        "id": reversal.id,
        "session_id": session_id,
        "portfolio_id": row.portfolio_strategy_id,
        "flow_type": reversal.flow_type,
        "amount": str(reversal.amount),
        "flow_date": reversal.flow_date.isoformat(),
        "note": reversal.note,
        "reversal_of_id": row.id,
        "net_inflow": str(net),
        "engine_applied": applied,
    }, None


def record_reconciliation_diff(
    db: Session, session_id: int, diff_type: str, *,
    portfolio_id: Optional[int] = None, stock_code: Optional[str] = None,
    expected=None, actual=None, detail: Optional[str] = None,
) -> Optional[dict]:
    """C1 差异台账写入（默认关闭，由 `tq_trade.reconciliation_ledger_enabled` 控制）。

    同一 `(session, diff_type, stock_code)` 的 pending 行复用（更新 actual/delta/
    detail），不为每次账户刷新新起一行——差异未处理期间值会来回变，逐次追加会
    把台账刷成噪声。写库失败只告警（差异告警本身已经发出，台账是补强不是主链）。
    """
    try:
        q = db.query(LiveReconciliationDiff).filter(
            LiveReconciliationDiff.live_session_id == session_id,
            LiveReconciliationDiff.diff_type == diff_type,
            LiveReconciliationDiff.status == DIFF_STATUS_PENDING,
        )
        if stock_code:
            q = q.filter(LiveReconciliationDiff.stock_code == str(stock_code).upper())
        else:
            q = q.filter(LiveReconciliationDiff.stock_code.is_(None))
        row = q.order_by(LiveReconciliationDiff.id.desc()).first()
        delta = None
        try:
            if expected is not None and actual is not None:
                delta = Decimal(str(actual)) - Decimal(str(expected))
        except (ArithmeticError, TypeError, ValueError):
            delta = None
        if row is None:
            row = LiveReconciliationDiff(
                live_session_id=session_id,
                portfolio_strategy_id=portfolio_id,
                diff_type=diff_type,
                stock_code=(str(stock_code).upper() if stock_code else None),
                expected=expected, actual=actual, delta=delta,
                status=DIFF_STATUS_PENDING, detail=(detail or None),
            )
            db.add(row)
        else:
            row.actual = actual
            row.delta = delta
            row.detail = (detail or row.detail)
            if portfolio_id is not None:
                row.portfolio_strategy_id = portfolio_id
        db.commit()
        db.refresh(row)
        return serialize_reconciliation_diff(row)
    except Exception:  # noqa: BLE001 — 多为未执行迁移（无表）
        db.rollback()
        logger.warning("record_reconciliation_diff 写入失败 (session %s, %s)",
                       session_id, diff_type, exc_info=True)
        return None


def serialize_reconciliation_diff(row) -> dict:
    def _s(v):
        return None if v is None else str(v)
    return {
        "id": row.id,
        "diff_type": row.diff_type,
        "portfolio_id": row.portfolio_strategy_id,
        "stock_code": row.stock_code,
        "expected": _s(row.expected),
        "actual": _s(row.actual),
        "delta": _s(row.delta),
        "status": row.status,
        "resolution": row.resolution,
        "detail": row.detail,
        "created_at": (row.created_at.strftime("%Y-%m-%d %H:%M:%S")
                       if row.created_at else None),
        "updated_at": (row.updated_at.strftime("%Y-%m-%d %H:%M:%S")
                       if row.updated_at else None),
    }


def list_reconciliation_diffs(
    db: Session, session_id: int, status: Optional[str] = None,
    limit: int = 200,
) -> Optional[dict]:
    """C1 差异台账列表（按 id 倒序）。session 不存在返回 None。"""
    session = db.query(LiveSession.id).filter(LiveSession.id == session_id).first()
    if session is None:
        return None
    try:
        q = db.query(LiveReconciliationDiff).filter(
            LiveReconciliationDiff.live_session_id == session_id)
        if status:
            q = q.filter(LiveReconciliationDiff.status == status)
        rows = (q.order_by(LiveReconciliationDiff.id.desc())
                .limit(max(1, min(int(limit), 1000))).all())
    except Exception:  # noqa: BLE001 — 未执行迁移 → 空列表
        db.rollback()
        rows = []
    counts: Dict[str, int] = {}
    for r in rows:
        counts[r.status] = counts.get(r.status, 0) + 1
    return {
        "diffs": [serialize_reconciliation_diff(r) for r in rows],
        "status_counts": counts,
    }


def resolve_reconciliation_diff(
    db: Session, session_id: int, diff_id: int, action: str,
    resolution: Optional[str] = None, engine=None, price=None,
) -> Tuple[Optional[dict], Optional[str]]:
    """C1/I2 差异处理：accept（以柜台为准）/ resolve（以平台为准）/ ignore。

    - `position` + accept：写合成成交把虚拟持仓改到柜台量（运行中由引擎同步
      内存，停止时重启重放生效），现金按差额×价格联动；
    - `cash` / `equity` + accept：只确认口径（既有规则明确「现金差只告警不改账」，
      这里不能借差异闭环偷偷改虚拟现金）。
    """
    if action not in ("accept", "resolve", "ignore"):
        return None, "action 仅支持 accept / resolve / ignore"
    row = db.query(LiveReconciliationDiff).filter_by(
        id=diff_id, live_session_id=session_id).first()
    if row is None:
        return None, "资源不存在"
    if row.status != DIFF_STATUS_PENDING:
        return None, "该差异已处理（status=%s）" % row.status
    note = (str(resolution)[:200] if resolution else None)
    if action == "ignore":
        row.status = DIFF_STATUS_IGNORED
        row.resolution = note or "人工忽略"
        db.commit()
        db.refresh(row)
        return serialize_reconciliation_diff(row), None
    if action == "resolve":
        row.status = DIFF_STATUS_RESOLVED
        row.resolution = note or "以平台台账为准"
        db.commit()
        db.refresh(row)
        return serialize_reconciliation_diff(row), None
    # accept
    if row.diff_type == "position":
        if row.stock_code is None or row.actual is None:
            return None, "该差异缺少代码/柜台数量，无法自动改账"
        try:
            actual_qty = int(Decimal(str(row.actual)))
            expected_qty = int(Decimal(str(row.expected or 0)))
        except (ArithmeticError, TypeError, ValueError):
            return None, "差异数量非法"
        delta = actual_qty - expected_qty
        if delta:
            px = _diff_price_hint(row, price)
            if px is None:
                return None, ("无法确定改账价格（该票已无持仓/无行情），"
                              "请改用人工持仓改账接口显式指定 price")
            if engine is not None:
                applied = engine.accept_counter_truth(
                    row.portfolio_strategy_id, row.stock_code, actual_qty,
                    price=px, note=note)
                if applied is None or applied.get("status") == "error":
                    return None, (applied or {}).get("message", "改账失败")
            else:
                strategy_id = _first_strategy_id(db, row.portfolio_strategy_id)
                if strategy_id is None:
                    return None, "组合内没有可用策略，无法归因改账成交"
                data, error = adjust_position_virtual(
                    db, session_id, row.portfolio_strategy_id, strategy_id,
                    row.stock_code, delta, px,
                    note=note or "接受柜台为准（差异台账）")
                if error is not None:
                    return None, error
        row.status = DIFF_STATUS_ACCEPTED
        row.resolution = note or "以柜台为准，虚拟账已按差额改账"
    else:
        row.status = DIFF_STATUS_ACCEPTED
        row.resolution = note or "已确认柜台口径（现金/净值差按既有规则只留痕不改账）"
    db.commit()
    db.refresh(row)
    return serialize_reconciliation_diff(row), None


def _diff_price_hint(row, price=None) -> Optional[Decimal]:
    """差异改账价格：请求显式 price 优先，其次从 detail 的 `price=` 标记解析。

    台账只存差异本身（股数/金额），价格属于「处理时」信息；调用方（API）优先
    传当前价/成本，缺省时退化为写入差异时随 detail 带上的价格提示。
    """
    if price is not None:
        try:
            value = Decimal(str(price))
        except (ArithmeticError, TypeError, ValueError):
            return None
        return value if value > 0 else None
    detail = row.detail or ""
    for marker in ("price=", "px="):
        idx = detail.find(marker)
        if idx >= 0:
            raw = detail[idx + len(marker):].split(",")[0].split(";")[0].strip()
            try:
                value = Decimal(raw)
            except (ArithmeticError, ValueError):
                continue
            if value > 0:
                return value
    return None


def _first_strategy_id(db: Session, portfolio_id: Optional[int]) -> Optional[int]:
    if portfolio_id is None:
        return None
    row = (db.query(Strategy.id)
           .filter(Strategy.portfolio_id == portfolio_id)
           .order_by(Strategy.id).first())
    return None if row is None else int(row[0])


def serialize_reconciliation_report(row) -> dict:
    """C4 日报序列化：金额统一两位小数字符串，detail 还原为对象。"""
    detail = None
    if row.detail:
        try:
            detail = json.loads(row.detail)
        except (TypeError, ValueError):
            detail = {"raw": row.detail}
    return {
        "id": row.id,
        "live_session_id": row.live_session_id,
        "report_date": row.report_date.isoformat() if row.report_date else None,
        "engine": row.engine,
        "positions_total": row.positions_total,
        "positions_matched": row.positions_matched,
        "positions_diff": row.positions_diff,
        "cash_expected": (str(row.cash_expected)
                          if row.cash_expected is not None else None),
        "cash_actual": (str(row.cash_actual)
                        if row.cash_actual is not None else None),
        "cash_diff": (str(row.cash_diff)
                      if row.cash_diff is not None else None),
        "equity_expected": (str(row.equity_expected)
                            if row.equity_expected is not None else None),
        "equity_actual": (str(row.equity_actual)
                          if row.equity_actual is not None else None),
        "equity_deviation_pct": (str(row.equity_deviation_pct)
                                 if row.equity_deviation_pct is not None else None),
        "order_count": row.order_count,
        "trade_count": row.trade_count,
        "detail": detail,
        "created_at": (row.created_at.strftime("%Y-%m-%d %H:%M:%S")
                       if row.created_at else None),
        "updated_at": (row.updated_at.strftime("%Y-%m-%d %H:%M:%S")
                       if row.updated_at else None),
    }


_REPORT_MONEY_FIELDS = ("cash_expected", "cash_actual", "cash_diff",
                        "equity_expected", "equity_actual")
_REPORT_INT_FIELDS = ("positions_total", "positions_matched", "positions_diff",
                      "order_count", "trade_count")


def upsert_reconciliation_report(
    db: Session, session_id: int, payload: dict,
) -> Tuple[Optional[dict], Optional[str]]:
    """C4 每日对账日报：同 `(会话, 日期)` 覆盖更新，不追加重复行。

    payload 由运行中的引擎组装（柜台快照 + 虚拟内存态）；本函数只做落库。
    报表只读聚合、不改任何账，重复生成安全。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return None, "资源不存在"
    report_date = payload.get("report_date") or now_shanghai().date()
    try:
        row = (
            db.query(LiveReconciliationReport)
            .filter(
                LiveReconciliationReport.live_session_id == session_id,
                LiveReconciliationReport.report_date == report_date,
            )
            .first()
        )
        if row is None:
            row = LiveReconciliationReport(
                live_session_id=session_id, report_date=report_date)
            db.add(row)
        row.engine = payload.get("engine") or session_engine(session)
        for field in _REPORT_INT_FIELDS:
            if field in payload:
                setattr(row, field, payload.get(field))
        for field in _REPORT_MONEY_FIELDS:
            value = payload.get(field)
            setattr(row, field,
                    Decimal(str(value)).quantize(Decimal("0.01"))
                    if value is not None else None)
        pct = payload.get("equity_deviation_pct")
        row.equity_deviation_pct = (
            Decimal(str(pct)).quantize(Decimal("0.0001"))
            if pct is not None else None)
        detail = payload.get("detail")
        row.detail = (json.dumps(detail, ensure_ascii=False, default=str)
                      if detail is not None else None)
        db.commit()
        db.refresh(row)
        return serialize_reconciliation_report(row), None
    except Exception:  # noqa: BLE001 — 多为未执行迁移（无表）
        db.rollback()
        logger.exception("upsert_reconciliation_report 写入失败 (session %s)",
                         session_id)
        return None, "对账日报写入失败"


def list_reconciliation_reports(
    db: Session, session_id: int, limit: int = 30,
) -> Optional[list]:
    """C4 日报列表（按报告日倒序）。会话不存在返回 None（路由层映射 404）。"""
    session = db.query(LiveSession.id).filter(
        LiveSession.id == session_id).first()
    if session is None:
        return None
    rows = (
        db.query(LiveReconciliationReport)
        .filter(LiveReconciliationReport.live_session_id == session_id)
        .order_by(LiveReconciliationReport.report_date.desc())
        .limit(max(1, min(int(limit or 30), 365)))
        .all()
    )
    return [serialize_reconciliation_report(r) for r in rows]


def serialize_out_of_pool_decision(row) -> dict:
    return {
        "id": row.id,
        "live_session_id": row.live_session_id,
        "stock_code": row.stock_code,
        "action": row.action,
        "portfolio_strategy_id": row.portfolio_strategy_id,
        "note": row.note,
        "created_at": (row.created_at.strftime("%Y-%m-%d %H:%M:%S")
                       if row.created_at else None),
        "updated_at": (row.updated_at.strftime("%Y-%m-%d %H:%M:%S")
                       if row.updated_at else None),
    }


def record_out_of_pool_decision(
    db: Session, session_id: int, stock_code: str, action: str,
    *, portfolio_id: Optional[int] = None, note: Optional[str] = None,
    engine=None,
) -> Tuple[Optional[dict], Optional[str]]:
    """I3 池外外部仓处置：include（纳入组合池）/ ignore（永久忽略告警）。

    - include：把代码加入该组合当前股票池（`stock_pool_stocks`）；组合开启
      PIT 时同步补一条 `effective_from=今天` 的成员区间。下一轮持仓采集会
      自动按池内继承仓导入虚拟账本（并按既有规则扣减配置资金）。
    - ignore：只抑制 `out_of_pool_position` 告警；仓位仍在展示层可见、仍不
      参与风控/不代卖（默认语义不变）。
    运行中会话额外刷新引擎内存（过滤集合 / 池代码），立即生效。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return None, "资源不存在"
    code = str(stock_code or "").strip().upper()
    if not code:
        return None, "stock_code 不能为空"
    act = str(action or "").strip().lower()
    if act not in ("include", "ignore"):
        return None, "action 仅支持 include / ignore"
    pool_id = None
    ps = None
    if act == "include":
        if portfolio_id is None:
            return None, "include 需要 portfolio_strategy_id"
        link = (
            db.query(LiveSessionPortfolio)
            .filter(LiveSessionPortfolio.session_id == session_id,
                    LiveSessionPortfolio.portfolio_strategy_id == portfolio_id)
            .first()
        )
        ps = (db.query(PortfolioStrategy)
              .filter(PortfolioStrategy.id == portfolio_id).first())
        if link is None or ps is None:
            return None, "组合不在该会话中"
        if ps.stock_pool_id is None:
            return None, "组合未绑定股票池，无法纳入"
        pool_id = ps.stock_pool_id
        exists = (
            db.query(StockPoolStock.id)
            .filter(StockPoolStock.pool_id == pool_id,
                    StockPoolStock.stock_code == code)
            .first()
        )
        if exists is None:
            db.add(StockPoolStock(pool_id=pool_id, stock_code=code))
        if bool(getattr(ps, "pool_pit_enabled", False)):
            open_row = (
                db.query(StockPoolMembership.id)
                .filter(StockPoolMembership.pool_id == pool_id,
                        StockPoolMembership.stock_code == code,
                        StockPoolMembership.effective_to.is_(None))
                .first()
            )
            if open_row is None:
                db.add(StockPoolMembership(
                    pool_id=pool_id, stock_code=code,
                    effective_from=now_shanghai().date(),
                    effective_to=None, source="manual",
                    note=(note or "池外仓纳入")[:200]))
    try:
        row = (
            db.query(LiveOutOfPoolDecision)
            .filter(LiveOutOfPoolDecision.live_session_id == session_id,
                    LiveOutOfPoolDecision.stock_code == code)
            .first()
        )
        if row is None:
            row = LiveOutOfPoolDecision(
                live_session_id=session_id, stock_code=code)
            db.add(row)
        row.action = act
        row.portfolio_strategy_id = portfolio_id if act == "include" else None
        row.note = (str(note)[:200] if note else None)
        db.commit()
        db.refresh(row)
    except Exception:  # noqa: BLE001
        db.rollback()
        logger.exception("out_of_pool decision 写入失败 (session %s, %s)",
                         session_id, code)
        return None, "处置记录写入失败"
    if engine is not None:
        try:
            engine.reload_out_of_pool_decisions(db)
            if act == "include":
                engine.include_pool_code(portfolio_id, code)
        except Exception:  # noqa: BLE001 — 内存刷新失败不影响落库
            logger.warning("out_of_pool engine refresh failed (session %s)",
                           session_id, exc_info=True)
    data = serialize_out_of_pool_decision(row)
    data["pool_id"] = pool_id
    return data, None


def list_out_of_pool_decisions(db: Session, session_id: int) -> Optional[list]:
    session = db.query(LiveSession.id).filter(
        LiveSession.id == session_id).first()
    if session is None:
        return None
    rows = (
        db.query(LiveOutOfPoolDecision)
        .filter(LiveOutOfPoolDecision.live_session_id == session_id)
        .order_by(LiveOutOfPoolDecision.updated_at.desc(),
                  LiveOutOfPoolDecision.id.desc())
        .all()
    )
    return [serialize_out_of_pool_decision(r) for r in rows]


def serialize_live_corporate_action(row) -> dict:
    """C5 实盘公司行为台账序列化（applied_date 升序展示由调用方决定）。"""
    return {
        "id": row.id,
        "live_session_id": row.live_session_id,
        "portfolio_strategy_id": row.portfolio_strategy_id,
        "code": row.code,
        "event_type": row.event_type,
        "ex_date": row.ex_date.isoformat() if row.ex_date else None,
        "applied_date": row.applied_date.isoformat() if row.applied_date else None,
        "amount": str(row.amount) if row.amount is not None else None,
        "qty_delta": row.qty_delta,
        "note": row.note,
        "created_at": (row.created_at.strftime("%Y-%m-%d %H:%M:%S")
                       if row.created_at else None),
    }


def list_live_corporate_actions(
    db: Session, session_id: int, limit: int = 200,
) -> Optional[list]:
    """C5 实盘公司行为台账（手工事件 + 启发式送转识别），按生效日倒序。

    台账已应用的事件唯一键幂等；启发式识别的行 `event_type=split` 且
    `note` 标明来源，便于与回测侧逐条对照。
    """
    session = db.query(LiveSession.id).filter(
        LiveSession.id == session_id).first()
    if session is None:
        return None
    rows = (
        db.query(LiveCorporateAction)
        .filter(LiveCorporateAction.live_session_id == session_id)
        .order_by(LiveCorporateAction.applied_date.desc(),
                  LiveCorporateAction.id.desc())
        .limit(max(1, min(int(limit or 200), 1000)))
        .all()
    )
    return [serialize_live_corporate_action(r) for r in rows]


def record_manual_action(
    db: Session, session_id: int, portfolio_id: int, action_type: str, *,
    stock_code: Optional[str] = None, trade_type: Optional[str] = None,
    quantity: Optional[int] = None, price=None, result: str = "applied",
    live_order_id: Optional[int] = None, cash_delta=None,
    reason: Optional[str] = None,
) -> Optional[dict]:
    """I1 人工操作审计行（改账类动作由 service 直接写；下单类由引擎写）。

    返回行 dict；写库失败返回 None（审计失败绝不改变交易结果）。
    """
    try:
        row = LiveManualAction(
            live_session_id=session_id,
            portfolio_strategy_id=portfolio_id,
            action_type=action_type,
            stock_code=stock_code,
            trade_type=trade_type,
            quantity=quantity,
            price=price,
            result=result,
            live_order_id=live_order_id,
            cash_delta=cash_delta,
            reason=(str(reason)[:200] if reason else None),
        )
        db.add(row)
        db.commit()
        db.refresh(row)
        return serialize_manual_action(row)
    except Exception:  # noqa: BLE001 — 多为未执行迁移（无表）
        db.rollback()
        logger.exception("record_manual_action 写入失败 (session %s)", session_id)
        return None


def serialize_manual_action(row) -> dict:
    return {
        "id": row.id,
        "action_type": row.action_type,
        "portfolio_id": row.portfolio_strategy_id,
        "stock_code": row.stock_code,
        "trade_type": row.trade_type,
        "quantity": row.quantity,
        "price": str(row.price) if row.price is not None else None,
        "cash_delta": (str(row.cash_delta) if row.cash_delta is not None else None),
        "result": row.result,
        "live_order_id": row.live_order_id,
        "reason": row.reason,
        "created_at": (row.created_at.strftime("%Y-%m-%d %H:%M:%S")
                       if row.created_at else None),
    }


def list_manual_actions(db: Session, session_id: int, limit: int = 200) -> Optional[dict]:
    """I1 人工操作审计列表（倒序）。session 不存在返回 None。"""
    session = db.query(LiveSession.id).filter(LiveSession.id == session_id).first()
    if session is None:
        return None
    try:
        rows = (
            db.query(LiveManualAction)
            .filter(LiveManualAction.live_session_id == session_id)
            .order_by(LiveManualAction.id.desc())
            .limit(max(1, min(int(limit), 1000)))
            .all()
        )
    except Exception:  # noqa: BLE001 — 未执行迁移 → 空列表
        db.rollback()
        rows = []
    return {"actions": [serialize_manual_action(r) for r in rows]}


def adjust_position_virtual(
    db: Session, session_id: int, portfolio_id: int, strategy_id: int,
    stock_code: str, quantity_delta: int, price, note: Optional[str] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """I1 持仓改账（仅停止的会话）：写一笔合成成交，重启 recover 重放即生效。

    为什么用合成 `live_trades` 而不是直接改内存/另建表：虚拟持仓与虚拟现金的唯一
    权威来源就是 live_trades 重放（recover），写合成行 = 与真实成交同一条口径，
    净值/回测对账不需要新增读取路径。`live_order_id=None` + commission/stamp=0，
    方向由 quantity_delta 符号决定；BUY 扣现金、SELL 加现金（均按传入价格）。
    """
    code = str(stock_code or "").strip().upper()
    if not code:
        return None, "stock_code 不能为空"
    try:
        delta = int(quantity_delta)
    except (TypeError, ValueError):
        return None, "quantity_delta 须为整数"
    if delta == 0:
        return None, "quantity_delta 不能为 0"
    try:
        px = Decimal(str(price))
    except (ArithmeticError, TypeError, ValueError):
        return None, "price 非法"
    if not px.is_finite() or px <= 0:
        return None, "price 必须为正数"
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return None, "资源不存在"
    link = (
        db.query(LiveSessionPortfolio)
        .filter_by(session_id=session_id, portfolio_strategy_id=portfolio_id)
        .first()
    )
    if link is None:
        return None, "组合未关联本会话"
    if not db.query(Strategy.id).filter(
            Strategy.id == strategy_id,
            Strategy.portfolio_id == portfolio_id).first():
        return None, "strategy_id 不属于该组合"
    qty = abs(delta)
    amount = (px * Decimal(qty)).quantize(Decimal("0.01"))
    direction = "BUY" if delta > 0 else "SELL"
    row = LiveTrade(
        live_session_id=session_id,
        live_order_id=None,
        portfolio_strategy_id=portfolio_id,
        strategy_id=strategy_id,
        stock_code=code,
        trade_type=direction,
        price=px,
        quantity=qty,
        amount=amount,
        commission=Decimal("0"),
        stamp_duty=Decimal("0"),
        trade_time=now_shanghai(),
    )
    db.add(row)
    try:
        db.commit()
        db.refresh(row)
    except Exception:  # noqa: BLE001
        db.rollback()
        logger.exception("adjust_position_virtual: 合成成交写入失败 (session %s)",
                         session_id)
        return None, "持仓改账写入失败：请确认已执行数据库迁移"
    cash_delta = (-amount) if delta > 0 else amount
    action = record_manual_action(
        db, session_id, portfolio_id, "position_adjust",
        stock_code=code, trade_type=direction, quantity=qty, price=px,
        result="applied", cash_delta=cash_delta,
        reason=(note or "人工持仓改账（合成成交，重启生效）"))
    logger.info(
        "position adjust: session %s 组合 %s %s %s %d 股 @ %s（合成成交 id=%s）",
        session_id, portfolio_id, code, direction, qty, px, row.id)
    return {
        "trade_id": row.id,
        "stock_code": code,
        "trade_type": direction,
        "quantity": qty,
        "price": str(px),
        "amount": str(amount),
        "cash_delta": str(cash_delta),
        "manual_action": action,
        "effective": "重启会话后由 live_trades 重放生效",
    }, None


def list_cash_flows(
    db: Session, session_id: int, portfolio_id: Optional[int] = None,
    limit: int = 200,
) -> Optional[dict]:
    """外部现金流台账（按日期倒序）+ 各组合累计净流入。session 不存在返回 None。"""
    session = db.query(LiveSession.id).filter(LiveSession.id == session_id).first()
    if session is None:
        return None
    q = db.query(LiveCashFlow).filter(LiveCashFlow.live_session_id == session_id)
    if portfolio_id is not None:
        q = q.filter(LiveCashFlow.portfolio_strategy_id == portfolio_id)
    try:
        rows = (
            q.order_by(LiveCashFlow.flow_date.desc(), LiveCashFlow.id.desc())
            .limit(max(1, min(int(limit), 1000)))
            .all()
        )
    except Exception:  # noqa: BLE001 — 未执行迁移（无表）→ 空台账 + 提示
        db.rollback()
        rows = []
        logger.warning(
            "list_cash_flows: live_cash_flows 不可读（是否未执行迁移？）(session %s)",
            session_id)
    net = cash_flow_net_inflow(db, session_id)
    return {
        "flows": [
            {
                "id": r.id,
                "portfolio_id": r.portfolio_strategy_id,
                "flow_type": r.flow_type,
                "amount": str(r.amount),
                "flow_date": r.flow_date.isoformat() if r.flow_date else None,
                "note": r.note,
                "reversal_of_id": r.reversal_of_id,
                "reversed_by_id": r.reversed_by_id,
                "voided": bool(r.voided),
                "created_at": (
                    r.created_at.strftime("%Y-%m-%d %H:%M:%S")
                    if r.created_at else None),
            }
            for r in rows
        ],
        "net_inflow": [
            {"portfolio_id": pid, "net_inflow": str(amt)}
            for pid, amt in sorted(net.items())
        ],
        "total_net_inflow": str(sum(net.values(), Decimal("0"))),
    }


def _finite_or_none(value):
    """非有限浮点（inf / -inf / nan）→ None；其它类型原样返回。

    桥/引擎若混入非有限值（QMT 停牌持仓会返回 Infinity），float() 出来仍是
    inf，FastAPI 的 json.dumps 会抛 ValueError → 会话详情整接口 500。
    """
    if value is None or isinstance(value, bool):
        return value
    try:
        num = float(value)
    except (TypeError, ValueError):
        return value
    return num if math.isfinite(num) else None


def _json_safe(value):
    """递归把非有限 float 换成 None（组装处兜底，不引入全局 JSON 编码器）。"""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _latest_account_snapshot(db: Session, session_id: int, engine) -> Optional[dict]:
    """最新账户快照 dict（工作台账户全貌卡）；无任何快照 → None。"""
    snap = (
        db.query(LiveAccountSnapshot)
        .filter(LiveAccountSnapshot.live_session_id == session_id)
        .order_by(LiveAccountSnapshot.captured_at.desc(),
                  LiveAccountSnapshot.id.desc())
        .first()
    )
    if snap is None:
        return None
    return {
        "engine": snap.engine,
        "captured_at": snap.captured_at.strftime("%Y-%m-%d %H:%M:%S")
        if snap.captured_at else None,
        "total_asset": _finite_or_none(snap.total_asset),
        "available_cash": _finite_or_none(snap.available_cash),
        "market_value": _finite_or_none(snap.market_value),
        "positions": [
            {
                "stock_code": p.stock_code,
                "quantity": p.quantity,
                "available_quantity": p.available_quantity,
                "cost_price": _finite_or_none(p.cost_price),
                "current_price": _finite_or_none(p.current_price),
                "market_value": _finite_or_none(p.market_value),
            }
            for p in (snap.positions or [])
        ],
    }


def delete_session(db: Session, session_id: int, engines: Dict[int, LiveEngine]) -> Tuple[bool, Optional[str]]:
    """删除 session。返回 (ok, error_message)。运行中拒绝。"""
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return False, "资源不存在"
    if session.status == "running" or session_id in engines:
        return False, "session 运行中，请先停止再删除"
    db.query(LiveSessionPortfolio).filter(
        LiveSessionPortfolio.session_id == session_id
    ).delete()
    db.delete(session)
    db.commit()
    return True, None


# ---------------------------------------------------------------------------
# start / stop 业务体
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# 并发约束（C1 引擎类型独占 / C2 账户独占 / C3 键鼠软拦 / C4 多账户并行）
# ---------------------------------------------------------------------------

#: 键鼠 UI 自动化引擎（tdx 走通达信交易窗口、ths 走 xiadan.exe）：同时启动有
#: 误点风险，属「建议级」而非硬拦（默认软拦，config 可升级为硬拦）。
KEYBOARD_ENGINES = ("tdx", "ths")

#: stopping 中间态的最长租约（秒）：stop 先置 stopping 并在注册表保留，引擎
#: stop() 收敛后才置 stopped；超过该时长仍未收敛视为僵尸，允许抢占。
STOPPING_STALE_SECONDS = 120

#: 平台同时运行中的实盘会话硬上限（显式常量，不再用「四引擎各一」的隐式推论）。
#: 超过即 409 rule=session_limit；停止一个即可再启。
MAX_ACTIVE_LIVE_SESSIONS = 4


def _occupied_since(session) -> Optional[datetime]:
    return getattr(session, "started_at", None) or getattr(session, "updated_at", None)


def _is_stale_stopping(session) -> bool:
    if getattr(session, "status", None) != "stopping":
        return False
    ts = getattr(session, "stopped_at", None) or getattr(session, "updated_at", None)
    if ts is None:
        return False
    try:
        return (datetime.now() - ts).total_seconds() > STOPPING_STALE_SECONDS
    except Exception:  # noqa: BLE001
        return False


def _occupied_sessions(
    db: Session,
    engines: Dict[int, LiveEngine],
    exclude_id: int,
) -> Dict[int, LiveSession]:
    """当前占用中的其它会话：内存注册表 ∪ DB status ∈ (running, stopping)。

    为什么必须并入 DB：后端重启后注册表为空，但 DB 仍可能是 running/stopping；
    以及 stop 已置 stopping、引擎正在异步收敛的窗口。只有 stopped 才释放占用。
    stopping 超过 ``STOPPING_STALE_SECONDS`` 且无引擎 → 视为僵尸，置 stopped 释放。
    """
    occupied: Dict[int, LiveSession] = {}
    rows = (
        db.query(LiveSession)
        .filter(LiveSession.status.in_(("running", "stopping")))
        .all()
    )
    for row in rows:
        if row.id == exclude_id:
            continue
        # B5 影子模式不占用真实通道/账户/键鼠，也不计入并行会话上限
        if (getattr(row, "execution_mode", None) or "auto") == "shadow":
            continue
        if row.id not in engines and _is_stale_stopping(row):
            set_session_status(
                row, "stopped", caller="_occupied_sessions",
                reason="stopping 超 %ss 未收敛，释放占用" % STOPPING_STALE_SECONDS)
            row.stopped_at = row.stopped_at or datetime.now()
            db.commit()
            logger.warning(
                "实盘会话 #%s「%s」stopping 超 %ss 未收敛，强制释放占用",
                row.id, row.name, STOPPING_STALE_SECONDS,
            )
            continue
        occupied[row.id] = row
    # 注册表里有、DB 尚未落到 running 的极短窗口也纳入占用
    for sid in engines:
        if sid == exclude_id or sid in occupied:
            continue
        row = db.query(LiveSession).filter(LiveSession.id == sid).first()
        if row is not None:
            if (getattr(row, "execution_mode", None) or "auto") == "shadow":
                continue
            occupied[sid] = row
    return occupied


def _occupied_payload(session) -> dict:
    since = _occupied_since(session)
    return {
        "session_id": session.id,
        "name": session.name,
        "engine": session_engine(session),
        "account_key": getattr(session, "account_key", None),
        "account_key_source": getattr(session, "account_key_source", None),
        "status": getattr(session, "status", None),
        "since": since.isoformat() if since is not None else None,
    }


def _record_decision_event(
    db: Session,
    session_id: int,
    *,
    gate: str,
    layer: str,
    action: str,
    param_name: Optional[str] = None,
    actual_value: Optional[float] = None,
    message: Optional[str] = None,
) -> None:
    """轻量决策事件落库（A2 键鼠 ack / A7 参数漂移）：失败只告警，绝不影响主流程。

    不单独 commit——随调用方（路由）的 commit 一并持久化；调用方回滚则该事件
    也不落库，符合「启动失败时无需留痕」的口径。
    """
    try:
        db.add(LiveDecisionEvent(
            live_session_id=session_id,
            gate=gate,
            layer=layer,
            action=action,
            param_name=param_name,
            actual_value=actual_value,
            message=(message or "")[:200] or None,
        ))
    except Exception:  # noqa: BLE001 — 观测失败不得阻断启动
        logger.warning("写入决策事件失败（gate=%s）", gate, exc_info=True)


def _engine_occupancy_warnings(db: Session, session) -> list:
    """A1 建会话软校验（不阻断）：目标引擎已被其它 running/stopping 会话占用。

    仅在创建响应里提示「启动前需先停止该会话」，真正的硬拦仍在 start_session
    （C1 引擎独占）。任何异常都降级为空列表——创建会话不得因提示失败而失败。
    """
    try:
        eng = session_engine(session)
        for other in _occupied_sessions(db, ENGINES, session.id).values():
            if session_engine(other) == eng:
                return [
                    "该引擎（%s）当前被会话 %d「%s」占用，启动前需先停止该会话"
                    % (eng, other.id, other.name)
                ]
    except Exception:  # noqa: BLE001
        logger.warning("会话占用软校验失败（session %s）", getattr(session, "id", None),
                       exc_info=True)
    return []


def _check_channel_conflict(
    db: Session,
    session: LiveSession,
    engines: Dict[int, LiveEngine],
) -> Optional[Tuple[int, str, dict]]:
    """C1 引擎类型独占（硬，跨账号）：同一 engine 全平台最多一个 running。

    物理依据：每个 engine 背后只有一个本地入口——tdx=唯一通达信 TQ 客户端、
    ths=唯一 xiadan.exe、iquant=8790 唯一 QMT 终端、goldminer=7801 唯一掘金终端。
    同引擎双会话 = 两套虚拟账本交易同一真实账户（双份记账/互相吃单/快照失真），
    跨账号也不可行（当前架构无多终端实例/多桥端口支持），故一律 409。
    """
    eng = session_engine(session)
    for other in _occupied_sessions(db, engines, session.id).values():
        if session_engine(other) == eng:
            label = {
                "ths": "同花顺通道键鼠独占",
                "tdx": "通达信 TQ 通道独占",
                "iquant": "QMT 通道独占",
                "goldminer": "掘金通道独占",
            }.get(eng, "通道独占")
            return (
                409,
                "%s（同一引擎全平台只允许一个运行中会话）：会话 %d「%s」正在运行，"
                "请先停止该会话" % (label, other.id, other.name),
                {
                    "rule": "engine_exclusive",
                    "engine": eng,
                    "occupied_by": _occupied_payload(other),
                },
            )
    return None


def resolve_account_key(
    engine_key: str,
    dispatcher=None,
    session=None,
) -> Tuple[str, str]:
    """解析会话的账户身份键 (key, source)。

    - iquant/goldminer：桥 ``/ping`` 返回的 account（心跳成功时缓存到
      ``dispatcher.last_ping``）；取不到再读 config（goldminer.account_id 等）。
    - tdx：``tq_trade.account``；ths：``ths_ui.account``（如配置）。
    - 都取不到 → 降级 ``engine:<key>``（同引擎即同账户，宁可多拦）。
    """
    source = "degraded"
    account = ""
    if engine_key in ("iquant", "goldminer"):
        body = getattr(dispatcher, "last_ping", None) if dispatcher is not None else None
        if isinstance(body, dict):
            account = str(body.get("account") or body.get("account_id") or "").strip()
        if account:
            return "%s:%s" % (engine_key, account), "bridge"
        cfg_section = "iquant_bridge" if engine_key == "iquant" else "goldminer"
        cfg = load_config().get(cfg_section) or {}
        if isinstance(cfg, dict):
            account = str(cfg.get("account_id") or "").strip()
        if account:
            return "%s:%s" % (engine_key, account), "config"
    elif engine_key == "tdx":
        account = str(tq_trade_config().get("account") or "").strip()
        if account:
            return "tdx:%s" % account, "config"
    elif engine_key == "ths":
        ths = load_config().get("ths_ui") or {}
        if isinstance(ths, dict):
            account = str(ths.get("account") or "").strip()
        if account:
            return "ths:%s" % account, "config"
    return "engine:%s" % engine_key, source


def _check_account_conflict(
    db: Session,
    session: LiveSession,
    engines: Dict[int, LiveEngine],
    own_key: str,
) -> Optional[Tuple[int, str, dict]]:
    """C2 账户键独占（硬，跨引擎）：同一账户最多一个 running 会话。

    其它会话的 account_key 缺失时降级为该会话的 ``engine:<key>``（宁可多拦）。
    """
    if not own_key:
        return None
    for other in _occupied_sessions(db, engines, session.id).values():
        other_key = getattr(other, "account_key", None) or (
            "engine:%s" % session_engine(other))
        if other_key == own_key:
            return (
                409,
                "账户独占：账户 %s 已被会话 %d「%s」占用（%s），请先停止该会话"
                % (own_key, other.id, other.name, session_engine(other)),
                {
                    "rule": "account_exclusive",
                    "account_key": own_key,
                    "occupied_by": _occupied_payload(other),
                },
            )
    return None


def _check_keyboard_conflict(
    db: Session,
    session: LiveSession,
    engines: Dict[int, LiveEngine],
    ack_keyboard_risk: bool,
) -> Optional[Tuple[int, str, dict]]:
    """C3 键鼠类互斥（tdx / ths）：默认软拦（409 要求显式 ack），可配置升级硬拦。

    两者都是键鼠 UI 自动化，同时启动有误点风险，但底层已有
    ``win_dialog_scope`` 的 UI 自动化互斥锁防「同时操作」；真正残留的是
    「人看错窗口点错」。软拦语义（2026-11-16 起 ack 真正改变结果）：
    未带 ``ack_keyboard_risk`` → 409 要求确认；带 ack → 放行并留 WARNING +
    决策事件。``tq_trade.keyboard_conflict_hard_block=true`` 时升级为无条件 409
    （ack 也不放行）。
    """
    eng = session_engine(session)
    if eng not in KEYBOARD_ENGINES:
        return None
    peer = None
    for other in _occupied_sessions(db, engines, session.id).values():
        other_eng = session_engine(other)
        if other_eng in KEYBOARD_ENGINES and other_eng != eng:
            peer = other
            break
    if peer is None:
        return None
    hard_block = bool(
        (load_config().get("tq_trade") or {}).get(
            "keyboard_conflict_hard_block", False))
    payload = {
        "rule": "keyboard_exclusive",
        "engine": eng,
        "peer_engine": session_engine(peer),
        "occupied_by": _occupied_payload(peer),
        "hard_block": hard_block,
        "requires_ack": not hard_block,
    }
    message = (
        "键鼠类通道互斥提醒：会话 %d「%s」（%s）正在运行，同时启动 %s 有误点风险，"
        "建议先停止一个，或在多设备上分别操作"
        % (peer.id, peer.name, session_engine(peer), eng)
    )
    if hard_block:
        return 409, message, payload
    if ack_keyboard_risk:
        logger.warning("键鼠互斥已确认放行（ack_keyboard_risk）：%s", message)
        _record_decision_event(
            db, session.id,
            gate="keyboard_exclusive", layer="live_gate", action="ack",
            param_name="peer_session", actual_value=float(peer.id), message=message,
        )
        return None
    return (
        409,
        message + "。如确认风险，请携带 ack_keyboard_risk=true 重试",
        payload,
    )


def recommended_channel_warnings(
    db: Session,
    session: LiveSession,
    engines: Dict[int, LiveEngine],
) -> list:
    """C4 多账户并行提醒（不阻断）：同时运行多个不同账户的会话时给出建议。"""
    occupied = _occupied_sessions(db, engines, session.id)
    accounts = set()
    for other in occupied.values():
        key = getattr(other, "account_key", None) or (
            "engine:%s" % session_engine(other))
        accounts.add(key)
    if len(accounts) <= 1:
        return []
    return [
        "当前有 %d 个不同账户的会话并行运行（%s）。建议尽量在多台设备上分别操作，"
        "避免在同一个终端窗口误操作到其它账户。"
        % (len(accounts), "、".join(sorted(accounts)))
    ]


def params_drift_block_enabled() -> bool:
    """A7 参数漂移阻断开关（默认 false）：仅显式配置才把告警升级为 409。"""
    tq = load_config().get("tq_trade") or {}
    return bool(tq.get("params_drift_block", False)) if isinstance(tq, dict) else False


#: 回测 run 级覆盖字段（只影响那一次回测的成交/费用模拟，不落组合表）：
#: 与组合配置天然不相等，比对它们会制造假漂移，必须跳过。
_RUN_LEVEL_SNAPSHOT_FIELDS = frozenset({
    "fee_reserve_enabled", "bond_t0_enabled", "bond_lot_enabled",
    "daily_close_peak_enabled", "daily_loss_basis", "dividend_mode",
    "data_start_date", "adaptive_slippage_enabled", "slippage_impact_coef",
    "volume_limit_enabled", "volume_limit_ratio", "equity_cap",
})


def _norm_param(value) -> Optional[str]:
    """参数归一：数值统一 Decimal 归一后比较（'5' 与 '5.0' 等价），其余按 str。

    回测快照里数值以 str(repr/Decimal) 形式落库，当前值是 Decimal/float/int；
    直接字符串比较会出现 '5.0' != '5' 的假漂移，故先做数值归一。
    """
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return str(Decimal(text).normalize())
    except (ArithmeticError, TypeError, ValueError):
        return text


def compute_params_drift(db: Session, session) -> Optional[dict]:
    """A7 参数绑定：比对绑定回测记录的 params_snapshot 与当前组合参数。

    返回 None = 会话未绑定回测记录；否则返回
    ``{backtest_record_id, portfolio_strategy_id?, checked, drifted, fields,
    block_enabled, reason}``。

    只比对快照中**存在**的 portfolio 级字段（``PARAMS_SNAPSHOT_FIELDS``），
    数值归一后比较；资金基准重置只改运行时现金/基准，不改组合配置，因此不会
    造成假漂移。**run 级覆盖字段**（回测时临时覆盖、不落组合表，如
    ``adaptive_slippage_enabled`` / ``volume_limit_ratio`` / ``equity_cap`` /
    ``dividend_mode``）一律跳过——它们与组合配置天然不等，比了就是假漂移。
    任何读取异常一律降级为 checked=False（绝不阻断启动）。
    """
    rec_id = getattr(session, "backtest_record_id", None)
    if not rec_id:
        return None
    block_enabled = params_drift_block_enabled()
    base = {"backtest_record_id": int(rec_id), "block_enabled": block_enabled}
    try:
        rec = db.query(BacktestRecord).filter(BacktestRecord.id == rec_id).first()
        if rec is None:
            return dict(base, checked=False, drifted=False, fields=[],
                        reason="绑定的回测记录不存在")
        snapshot_raw = getattr(rec, "params_snapshot", None)
        if not snapshot_raw:
            return dict(base, checked=False, drifted=False, fields=[],
                        reason="回测记录缺少参数快照")
        snapshot = (json.loads(snapshot_raw) if isinstance(snapshot_raw, str)
                    else snapshot_raw)
        if not isinstance(snapshot, dict):
            return dict(base, checked=False, drifted=False, fields=[],
                        reason="参数快照格式异常")
        ps = db.query(PortfolioStrategy).filter_by(
            id=rec.portfolio_strategy_id).first()
        if ps is None:
            return dict(base, checked=False, drifted=False, fields=[],
                        reason="回测记录关联的组合策略不存在")
        try:
            from core.services.backtest_service import PARAMS_SNAPSHOT_FIELDS
        except Exception:  # noqa: BLE001 — 常量缺失时退化为「无可比对字段」
            PARAMS_SNAPSHOT_FIELDS = ()
        fields = []
        for name in PARAMS_SNAPSHOT_FIELDS:
            if name not in snapshot or name in _RUN_LEVEL_SNAPSHOT_FIELDS:
                continue
            if not hasattr(ps, name):
                continue
            old_raw = snapshot.get(name)
            new_raw = getattr(ps, name, None)
            if _norm_param(old_raw) != _norm_param(new_raw):
                fields.append({
                    "field": name,
                    "snapshot": None if old_raw is None else str(old_raw),
                    "current": None if new_raw is None else str(new_raw),
                })
        return dict(base, portfolio_strategy_id=rec.portfolio_strategy_id,
                    checked=True, drifted=bool(fields), fields=fields, reason="")
    except Exception:  # noqa: BLE001 — 比对失败只降级为未检查
        logger.warning("参数漂移比对失败（session %s）",
                       getattr(session, "id", None), exc_info=True)
        return dict(base, checked=False, drifted=False, fields=[],
                    reason="参数漂移比对失败")


def start_session(
    db: Session,
    session_id: int,
    engines: Dict[int, LiveEngine],
    mode: str,
    db_session_factory=SessionLocal,
    dispatcher_cls=None,
    ack_keyboard_risk: bool = False,
) -> Tuple[Optional[LiveEngine], Optional[LiveSession], Optional[Tuple]]:
    """启动 session 的业务体。

    返回 (engine, session, error)：
    - error 为 None 且 engine 非 None 表示成功：engine 已构造、heartbeat 通过、
      recover 已执行但尚未 start()，**未改 session 状态/未 commit**。由路由负责
      asyncio.create_task(engine.start())、engines[session_id]=engine、
      置 session.status=running + commit（注册先于 commit，闭合 B6 窗口）。
    - error 为 None 且 engine 为 None：幂等命中（session 已在跑），路由直接返回 ok running。
    - 否则 error = (http_status, message)。

    多会话并行（M1）：B6 全局限 1 已移除，改为资源互斥——仅 THS 通道键鼠独占
    （_check_channel_conflict），tdx/iquant 会话可并行；同组合挂多会话=复制交易
    （各会话独立虚拟账本，执行偏差用户自担）。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None, None, (404, "资源不存在")
    if session_id in engines:
        # 幂等：已在跑，直接返回（无 engine 新建，路由据此返回 ok running）
        return None, session, None
    # B5：执行模式需在资源冲突检查前确定——影子模式只读行情、不占真实通道/账户/键鼠
    execution_mode = getattr(session, "execution_mode", None) or "auto"
    is_shadow = execution_mode == "shadow"
    # D3（2026-11-18）：建引擎前应用本会话待生效风控参数变更。
    # 无 pending 时行为与改动前逐字一致；应用失败不阻断启动（只告警）。
    try:
        apply_pending_changes(db, session_id)
    except Exception:  # noqa: BLE001
        logger.warning(
            "apply_pending_changes failed before start (session %s)",
            session_id, exc_info=True)
    # A1 运行中会话总量硬上限（显式常量，不用「四引擎各一」隐式推论）：
    # 本会话之外已有 MAX_ACTIVE_LIVE_SESSIONS 个占用 → 409。
    occupied = _occupied_sessions(db, engines, session_id)
    if len(occupied) >= MAX_ACTIVE_LIVE_SESSIONS:
        return None, session, (
            409,
            "同时运行的实盘会话已达上限 %d 个：请先停止不再需要的会话"
            % MAX_ACTIVE_LIVE_SESSIONS,
            {
                "rule": "session_limit",
                "limit": MAX_ACTIVE_LIVE_SESSIONS,
                "occupied_by": [_occupied_payload(o) for o in occupied.values()],
            },
        )
    # A7 参数绑定：与回测参数快照比对。默认只告警（WARNING + 决策事件），
    # 绝不阻断启动；tq_trade.params_drift_block=true 才升级为 409。
    drift = compute_params_drift(db, session)
    if drift and drift.get("drifted"):
        drift_msg = "参数漂移：当前组合参数与回测快照 #%d 不一致（%s）" % (
            drift["backtest_record_id"],
            "、".join(f["field"] for f in drift["fields"]) or "-",
        )
        logger.warning("会话 %s %s（%s）", session_id, drift_msg,
                       "阻断" if drift.get("block_enabled") else "仅告警")
        _record_decision_event(
            db, session_id, gate="params_drift", layer="live_gate",
            action="block" if drift.get("block_enabled") else "notify",
            message=drift_msg,
        )
        if drift.get("block_enabled"):
            return None, session, (
                409,
                drift_msg + "；参数漂移阻断已开启（tq_trade.params_drift_block=true），"
                "请先对齐参数后重试",
                {"rule": "params_drift", "fields": drift["fields"],
                 "backtest_record_id": drift["backtest_record_id"]},
            )
    # B6 资源互斥（M1 多会话并行）：THS 通道键鼠独占，其余通道（tdx/iquant）
    # 可并行；组合重叠允许（复制交易语义，各会话独立虚拟账本）。
    conflict = None if is_shadow else _check_channel_conflict(db, session, engines)
    if conflict is not None:
        return None, session, conflict

    # 空股票池预检（audit ⑯）：空池会让 BarPoller 静默订阅占位 000001.SZ 空转，
    # 无任何告警；alert 与 auto 两条路径都在此拦截（位于 build_engine 之前，
    # 不依赖交易通道在线）。
    links = (
        db.query(LiveSessionPortfolio)
        .filter(LiveSessionPortfolio.session_id == session_id)
        .all()
    )
    pool_codes: set = set()
    for link in links:
        ps = db.query(PortfolioStrategy).filter_by(
            id=link.portfolio_strategy_id).first()
        if ps is not None:
            pool_codes.update(resolve_stock_codes(db, ps.id))
    if not pool_codes:
        return None, session, (
            503,
            "股票池为空：请先在组合策略中配置股票，或检查股票池同步",
        )

    try:
        engine = build_engine(
            session_id, db, mode,
            db_session_factory=db_session_factory, dispatcher_cls=dispatcher_cls,
            engine=session_engine(session),
            execution_mode=execution_mode,
            signal_file_path=getattr(session, "signal_file_path", None),
            advance_close_enabled=bool(
                getattr(session, "advance_close_enabled", False)),
            advance_close_time=getattr(session, "advance_close_time", None),
            session_mismatch_risk=getattr(
                session, "intraday_mismatched_risk_enabled", None),
        )
    except RuntimeError as e:
        # make_dispatcher 对未知引擎键/依赖缺失（pywinauto 等）/路径账号未配置
        # 统一抛 RuntimeError；这里必须转成 503 可操作提示，不能漏成 500。
        return None, session, (503, str(e) or "交易通道未就绪")
    # 启动前探测通道在线：客户端未起/未登录直接拒绝，不建引擎、不置 running，
    # 避免「一点就成功、通道离线却无告警」。heartbeat() 本地 loopback 瞬时失败，
    # 不阻塞事件循环。
    if not engine.dispatcher.heartbeat():
        channel = getattr(engine.dispatcher, "channel", "")
        detail = getattr(engine.dispatcher, "last_error", "") or ""
        if channel == "alert":
            return None, session, (
                503,
                "通达信行情未就绪：请先启动并登录通达信客户端（%s）"
                % (detail or "未取到行情"),
            )
        if channel == "ths_ui":
            ths = ths_ui_config()
            return None, session, (
                503,
                "同花顺交易客户端未就绪：请先启动并登录「%s」（%s）"
                % (ths["window_title"], detail or "未找到交易窗口"),
            )
        if channel == "tdx_tq":
            return None, session, (
                503,
                "通达信交易未就绪：请先启动并登录通达信交易客户端（%s）"
                % (detail or "未获取到资金账号"),
            )
        channel = getattr(engine.dispatcher, "channel", "")
        base_url = getattr(engine.dispatcher, "_base_url", "") or "未知"
        if channel in ("goldminer_bridge", "iquant_bridge"):
            # 分级提示：端口无监听 / 已监听但无响应 / 应答格式不符，三档可区分，
            # 避免把「桥没跑」与「桥卡在查询」混成一句「桥未启动」误导排查方向。
            reason = getattr(engine.dispatcher, "last_probe_error", None)
            return None, session, (
                503, bridge_probe_message(channel, reason, base_url))
        return None, session, (
            503,
            "桥未启动：%s— 请先在对应客户端加载并运行对应桥策略" % base_url,
        )
    # OCR 启动闸仅 direct 模式：file_builtin 下单不做 OCR 前置拒单（OCR 守卫在
    # 执行器 _execute_via_ui 里，失败落 results rejected 回执），无需阻断启动。
    if (getattr(engine.dispatcher, "channel", "") == "ths_ui"
            and getattr(engine.dispatcher, "execute_mode", "direct") == "direct"
            and not ths_ui_config()["captcha_ocr"]):
        return None, session, (
            503,
            "同花顺复制表格会触发验证码：请先在系统配置启用「验证码 OCR」再启动实盘",
        )
    preflight_err = preflight_signal_config(db, session_id, execution_mode)
    if preflight_err is not None:
        return None, session, preflight_err
    # C2 账户键独占（硬，跨引擎）：桥心跳成功后账户号已可确定，按「C1 引擎类型
    # → C2 账户键」顺序校验（C1 已在上方完成）。取不到账号时降级 engine:<key>。
    own_key, own_source = resolve_account_key(
        session_engine(session), engine.dispatcher)
    account_conflict = (
        None if is_shadow
        else _check_account_conflict(db, session, engines, own_key))
    if account_conflict is not None:
        return None, session, account_conflict
    # C3 键鼠类互斥（tdx/ths）：默认软拦——未带 ack 返回 409 要求确认，带
    # ack_keyboard_risk 放行并留痕；config 可升级为硬拦（409，ack 也拒）。
    keyboard_conflict = (
        None if is_shadow
        else _check_keyboard_conflict(db, session, engines, ack_keyboard_risk))
    if keyboard_conflict is not None:
        return None, session, keyboard_conflict
    # 账户键落库（路由 commit 时随 status=running 一并持久化），供后续会话判定。
    # B5 影子模式不绑定真实账户，避免占住账户键。
    session.account_key = None if is_shadow else own_key
    session.account_key_source = None if is_shadow else own_source
    # C4 多账户并行提醒（不阻断）：随启动结果回传给前端做横幅。
    try:
        start_warnings = recommended_channel_warnings(db, session, engines)
    except Exception:  # noqa: BLE001
        start_warnings = []
    # H1 拆单能力一致性：拆单仅通达信 TQ 实现；其它通道静默不拆会让「大额单
    # 一笔打满」与预期不符，这里在启动结果里明确软提示（不阻断启动）。
    channel = getattr(engine.dispatcher, "channel", "")
    for msg in split_channel_warnings(db, session_id, execution_mode, channel):
        start_warnings.append(msg)
        _record_decision_event(
            db, session_id, gate="split_order", layer="live_gate",
            action="unsupported", message=msg,
        )
    # F2 启动前数据体检（只读，不阻断启动）：检查本次会话策略周期在本地是否有
    # 最新数据；非盘中自动重读一次已落盘数据。缺数据时明确提示「需在通达信客户端
    # 补下载」，而不是等盘中新鲜度闸静默跳过。
    try:
        health_periods = sorted({
            getattr(st, "period", None)
            for _pid, st in _session_strategies(db, session_id)
            if getattr(st, "period", None)
        }) or ["1d"]
        health = run_health_check(
            engine.dispatcher, sorted(pool_codes), health_periods,
            auto_fill=True, calendar=getattr(engine, "_calendar", None),
        )["health"]
        if not health.get("healthy"):
            msg = health_summary_text(health)
            start_warnings.append(msg)
            _record_decision_event(
                db, session_id, gate="data_health", layer="live_gate",
                action="notify", message=msg,
            )
            try:
                from core.services.notify_service import notify as _notify
                _notify("warn", "实盘数据体检异常", msg,
                        gate="data_health", trade_date=now_shanghai().date())
            except Exception:  # noqa: BLE001 — 通知失败不影响启动
                logger.debug("data health notify failed", exc_info=True)
    except Exception:  # noqa: BLE001 — 体检失败只告警，绝不阻断启动
        logger.warning("启动前数据体检失败 (session %s)", session_id, exc_info=True)
    setattr(engine, "start_warnings", start_warnings)
    if execution_mode != "alert":
        # 重启恢复：从 live_trades 重放虚拟持仓/虚拟现金（alert 无下单/成交）
        engine.recover(db)
        # 接缝日锚定：首次启动时把该组合的起账状态（虚拟现金/存量仓）显式落库，
        # 供增量回测「同起点同分母」续跑。已写定则不覆盖；失败不阻断启动。
        try:
            from core.services.incremental_backtest import capture_seam
            capture_seam(db, session_id, engine)
        except Exception:  # noqa: BLE001
            logger.warning(
                "incremental backtest: 接缝日捕获失败 (session %s)",
                session_id, exc_info=True)
    return engine, session, None


def stop_session(
    db: Session,
    session_id: int,
    engines: Dict[int, LiveEngine],
) -> Optional[LiveEngine]:
    """停止 session 业务体：从注册表弹出引擎、改状态、commit。

    返回弹出的 engine（供路由 asyncio.create_task(engine.stop())）；session 不存在返回 None。
    **引擎的异步 stop() 由路由调度**（本函数同步，不碰事件循环）。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None
    engine = engines.pop(session_id, None)
    # stopping 中间态（2026-11-16）：有引擎时先置 stopping（仍算占用，防「刚停就
    # 启另一会话」双开），等路由 asyncio 任务里 engine.stop() 收敛后再置 stopped；
    # 无引擎（已在停止/未运行）直接 stopped。只有 stopped 才释放 C1/C2 占用。
    set_session_status(
        session, "stopping" if engine is not None else "stopped",
        caller="stop_session", reason="有引擎→stopping；无引擎→stopped")
    session.stopped_at = datetime.now()
    db.commit()
    # 会话离开 ENGINES 后 watchdog 再也扫不到它：显式清监控态，否则停止前的
    # 告警（如 client_offline）会在 active_alarms 里永久挂红灯（2026-11-13 实证）。
    try:
        from core.engine.live.engine_watchdog import current_watchdog
        watchdog = current_watchdog()
        if watchdog is not None:
            watchdog.clear_session(session_id)
    except Exception:  # noqa: BLE001 — 观测清理失败不得影响停会话
        logger.exception("clear watchdog state failed (session %s)", session_id)
    return engine


def finalize_stopped(session_id: int, db_session_factory=SessionLocal) -> None:
    """引擎 stop() 收敛后的收尾：stopping → stopped，释放 C1/C2 占用。

    由路由的 asyncio 任务在 ``await engine.stop()``（含异常）后调用；用独立
    session，避免请求线程的 Session 已关闭。幂等：只处理 stopping。
    """
    db = db_session_factory()
    try:
        session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
        if session is not None and session.status == "stopping":
            set_session_status(
                session, "stopped", caller="finalize_stopped",
                reason="engine.stop() 已收敛")
            session.stopped_at = session.stopped_at or datetime.now()
            db.commit()
    except Exception:  # noqa: BLE001 — 收尾失败不得抛给调用方（下次启动会兜底）
        logger.exception("finalize_stopped failed (session %s)", session_id)
        db.rollback()
    finally:
        db.close()


_BASELINE_RESET_CALENDAR = None


def _baseline_reset_calendar():
    """重置窗口用的交易日历（懒加载单例）：主源掘金桥 /calendar，失败回退内置休市表。

    只在手工重置基准时调用（低频），不参与主循环；`is_trading_day` 内部按年缓存，
    桥离线时退化到 `BUILTIN_CLOSURES`，未知年份工作日 fail-open。
    """
    global _BASELINE_RESET_CALENDAR
    if _BASELINE_RESET_CALENDAR is None:
        from core.engine.live.calendar import (
            TradingCalendar, goldminer_calendar_provider,
        )
        _BASELINE_RESET_CALENDAR = TradingCalendar(goldminer_calendar_provider)
    return _BASELINE_RESET_CALENDAR


def _baseline_reset_window_allowed(
    now: datetime, calendar=None,
) -> Tuple[bool, str]:
    """资金基准重置时段闸：先判交易日，再判盘中；返回 (是否允许, 拒绝原因)。

    2026-11-16 起加交易日判据：非交易日直接拒绝（此前只说「盘中禁止」，周末/节假日
    会给出误导文案）。交易日仍禁用 09:15–11:30、13:00–15:00（含集合竞价与午盘），
    收盘后/盘前允许。日历不可用（桥离线且年份未收录）时告警并退化为只看时间窗——
    绝不因日历问题把真实交易日误挡。
    """
    cal = calendar if calendar is not None else _baseline_reset_calendar()
    try:
        if not cal.is_trading_day(now.date()):
            return False, "今天不是交易日：资金基准重置仅在交易日收盘后执行"
    except Exception:  # noqa: BLE001 — 日历不可用：告警后按时间窗判定
        logger.warning("重置窗口交易日判定失败（按时间窗继续）", exc_info=True)
    minutes = now.hour * 60 + now.minute
    if (
        (9 * 60 + 15 <= minutes < 11 * 60 + 30)
        or (13 * 60 <= minutes < 15 * 60)
    ):
        return False, "盘中禁止重置资金基准：请收盘后（15:00 之后）再操作"
    return True, ""


def reset_baseline(
    db: Session,
    session_id: int,
    portfolio_id: int,
    value,
    engines: Optional[Dict[int, LiveEngine]] = None,
    note: Optional[str] = None,
) -> Tuple[bool, Optional[Tuple]]:
    """收盘后重置资金基准 + 成套重锚风控锚点（2026-11-16）。

    规则（方案 §3.3/§3.4）：
    - 基准 = 真实账户总资产口径，**只缩不放**：填写值 > 真实总资产 → 400 拒绝；
      未填写 → 取当日真实总资产。
    - 成套重锚 ``peak_value`` / ``prev_close_value`` / ``_last_bar_total_value`` /
      ``_last_bar_date``（DB risk_state 与运行中内存同时更新）。
    - **不清零** ``circuit_breaker_count``、**不解锁** manual_recovery、不改暂停/
      熔断挂起标志——只换基准，不伪装成「恢复」。
    - **绝不**写 ``live_cash_flows`` / 调 ``apply_cash_flow``（会污染
      ``external_net_inflow`` 与 ``risk_equity``）；只调整基准与审计。
    - 复位真实-虚拟偏差告警（``_equity_deviation_warned``），让新基准重新比对。
    - 基准版本 +1，旧 checkpoint 删除、接缝重设，防止旧 checkpoint 覆盖新基准。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return False, (404, "资源不存在")
    now = now_shanghai()
    allowed, reason = _baseline_reset_window_allowed(now)
    if not allowed:
        return False, (403, reason)
    link = (
        db.query(LiveSessionPortfolio)
        .filter(
            LiveSessionPortfolio.session_id == session_id,
            LiveSessionPortfolio.portfolio_strategy_id == portfolio_id,
        )
        .first()
    )
    if link is None:
        return False, (404, "会话未关联该组合")
    total = latest_account_total_asset(db, session_id)
    if value is not None:
        try:
            new_base = Decimal(str(value)).quantize(Decimal("0.01"))
        except (ArithmeticError, TypeError, ValueError):
            return False, (400, "资金基准须为数字")
        if new_base <= 0:
            return False, (400, "资金基准须大于 0")
        if total is not None:
            cap = Decimal(str(total)).quantize(Decimal("0.01"))
            if new_base > cap:
                return False, (
                    400,
                    "资金基准只缩不放：填写值 %s 超过真实总资产 %s，请填写 ≤ %s 的金额"
                    % (new_base, cap, cap),
                )
    else:
        if total is None:
            return False, (
                400, "取不到真实总资产：请显式填写资金基准（≤ 真实总资产）")
        new_base = Decimal(str(total)).quantize(Decimal("0.01"))

    # 当前净值：运行中取组合 risk_equity；否则退真实总资产/新基准。
    engine = (engines or {}).get(session_id)
    port = None
    if engine is not None:
        for p in (getattr(engine, "portfolios", None) or []):
            if getattr(p, "portfolio_id", None) == portfolio_id:
                port = p
                break
    cur_equity = None
    if port is not None:
        try:
            cur_equity = Decimal(str(port.risk_equity())).quantize(Decimal("0.01"))
        except Exception:  # noqa: BLE001
            cur_equity = None
    if cur_equity is None and total is not None:
        cur_equity = Decimal(str(total)).quantize(Decimal("0.01"))
    if cur_equity is None:
        cur_equity = new_base

    old_base = session_baseline_value(session)
    old_state: dict = {}
    try:
        old_state = json.loads(link.risk_state) if link.risk_state else {}
    except (TypeError, ValueError):
        old_state = {}
    if not isinstance(old_state, dict):
        old_state = {}
    old_peak = old_state.get("peak_value")
    old_prev = old_state.get("prev_close_value")
    old_seam_date = link.seam_date

    # 成套锚点：只更新四个基准值，暂停/熔断标志与触发日原样保留。
    new_state = dict(old_state)
    new_state.setdefault("schema", 2)
    new_state["peak_value"] = str(cur_equity)
    new_state["prev_close_value"] = str(cur_equity)
    new_state["last_bar_total_value"] = str(cur_equity)
    new_state["last_bar_date"] = now.date().isoformat()
    link.risk_state = json.dumps(new_state, ensure_ascii=False)

    # 运行中同步内存锚点 + 复位偏差告警，避免重启前继续用旧峰值/旧昨收。
    if port is not None:
        rm = getattr(port, "risk_manager", None)
        if rm is not None:
            rm.peak_value = cur_equity
            rm.prev_close_value = cur_equity
            rm._last_bar_total_value = cur_equity
            rm._last_bar_date = now.date()
        try:
            engine._equity_deviation_warned = False
        except Exception:  # noqa: BLE001
            pass

    # 会话基准（live_capital 兼容列同步写，语义 = 实盘资金基准）。
    session.baseline_value = new_base
    session.live_capital = new_base
    session.baseline_mode = "manual" if value is not None else "total_asset"
    session.baseline_set_at = now
    session.baseline_version = int(session.baseline_version or 0) + 1

    # 顺延单打上基准版本（保留信号原始目标量）；数量/价格不动——放行时
    # _allocate_batch_buys 会按真实可用动态比例缩量。
    deferred = (
        db.query(LiveOrder)
        .filter(
            LiveOrder.live_session_id == session_id,
            LiveOrder.portfolio_strategy_id == portfolio_id,
            LiveOrder.status == "deferred",
        )
        .all()
    )
    for order in deferred:
        if order.orig_quantity is None:
            order.orig_quantity = order.quantity
        order.baseline_version = session.baseline_version

    # 旧 checkpoint 删除 + 接缝重设：确保增量回测不会把新基准覆盖回去。
    (
        db.query(IncrementalBacktestCheckpoint)
        .filter(
            IncrementalBacktestCheckpoint.session_id == session_id,
            IncrementalBacktestCheckpoint.portfolio_strategy_id == portfolio_id,
        )
        .delete(synchronize_session=False)
    )
    new_seam_date = now.date().isoformat()
    if port is not None:
        try:
            from core.services.incremental_backtest import capture_seam
            capture_seam(db, session_id, engine, today=now.date(),
                         force=True, portfolio_id=portfolio_id)
        except Exception:  # noqa: BLE001 — 接缝失败不阻断基准重置
            logger.exception("reset_baseline capture_seam failed (session %s)", session_id)
    else:
        # 未运行：清空接缝，下次启动 recover 后由 capture_seam 重新写定。
        link.seam_date = None
        link.seam_cash = None
        link.seam_positions_json = None
        link.seam_trade_cursor = 0
        new_seam_date = None

    db.add(LiveBaselineReset(
        live_session_id=session_id,
        portfolio_strategy_id=portfolio_id,
        old_baseline=Decimal(str(old_base)).quantize(Decimal("0.01")) if old_base else None,
        new_baseline=new_base,
        old_peak=Decimal(str(old_peak)) if old_peak is not None else None,
        new_peak=cur_equity,
        old_prev_close=Decimal(str(old_prev)) if old_prev is not None else None,
        new_prev_close=cur_equity,
        old_seam_date=old_seam_date,
        new_seam_date=new_seam_date,
        baseline_version=session.baseline_version,
        note=note,
    ))
    db.commit()
    logger.info(
        "资金基准重置：会话 #%s 组合 #%s 基准 %s → %s，锚点重锚到 %s（版本 %s）",
        session_id, portfolio_id, old_base, new_base, cur_equity,
        session.baseline_version,
    )
    return True, None


def account_preview_by_engine(
    db: Session,
    engine_key: str,
    mode: Optional[str] = None,
) -> Tuple[Optional[dict], Optional[Tuple]]:
    """A7 资金基准预览（GET /api/live/account-preview?engine=&mode=）。

    优先实时探测所选通道的账户资产（桥 /account、通达信 query_stock_asset、
    同花顺资金标签）；探测失败回退该引擎最近一次账户快照。返回
    ``{engine,mode,total_asset,available,captured_at,source}``，只读、不下单、不改账。

    2026-11-16 修（A6）：mode 不再固定 ``simulation``。显式传入优先；未传时取该
    引擎最近一条会话的实际 mode（兼容 NULL/历史会话的 `session_engine` 解析）——
    live/simulation 指向不同桥地址时，预览才不会取到错账户。
    """
    if engine_key not in ENGINES_META:
        return None, (400, "engine 非法，仅支持 %s" % " / ".join(ENGINES_META))
    effective_mode = mode if mode in ("simulation", "live") else None
    sessions = (
        db.query(LiveSession).order_by(LiveSession.id.desc()).all()
    )
    if effective_mode is None:
        for s in sessions:
            if session_engine(s) == engine_key:
                effective_mode = getattr(s, "mode", None) or "simulation"
                break
        else:
            effective_mode = "simulation"
    probe_error = ""
    try:
        dispatcher = make_dispatcher(engine_key, effective_mode, 0)
        if engine_key == "ths":
            rows = dispatcher.get_account()
            row = rows[0] if isinstance(rows, list) and rows else {}
        else:
            row = dispatcher.get_asset() or {}
        total = row.get("total_asset")
        if total is not None:
            available = row.get("available")
            return {
                "engine": engine_key,
                "mode": effective_mode,
                "total_asset": float(total),
                "available": float(available) if available is not None else None,
                "captured_at": now_shanghai().isoformat(),
                "source": "live",
            }, None
        probe_error = "通道未返回账户资产（客户端未登录/桥未运行）"
    except Exception as exc:  # noqa: BLE001 — 预览失败回退快照，不抛给前端
        probe_error = str(exc) or exc.__class__.__name__

    # 回退：该引擎最近一次会话的账户快照（事后查看/盘后预览）。
    for session in sessions:
        if session_engine(session) != engine_key:
            continue
        snap = (
            db.query(LiveAccountSnapshot)
            .filter(LiveAccountSnapshot.live_session_id == session.id)
            .order_by(LiveAccountSnapshot.id.desc())
            .first()
        )
        if snap is None:
            continue
        return {
            "engine": engine_key,
            "mode": effective_mode,
            "total_asset": (
                float(snap.total_asset) if snap.total_asset is not None else None),
            "available": (
                float(snap.available_cash)
                if getattr(snap, "available_cash", None) is not None else None),
            "captured_at": (
                snap.captured_at.isoformat() if snap.captured_at else None),
            "source": "snapshot",
            "warning": probe_error,
        }, None
    return None, (503, "取不到账户资产：%s" % (probe_error or "无可用数据"))


# ---------------------------------------------------------------------------
# 熔断手动恢复 / 桥状态
# ---------------------------------------------------------------------------

def recover_breaker(
    db: Session,
    session_id: int,
    portfolio_id: int,
    engines: Dict[int, LiveEngine],
    reason: Optional[str] = None,
    confirm: bool = False,
) -> Tuple[bool, Optional[str]]:
    """手动恢复某组合熔断（3 次转手动恢复后的人工恢复入口）。

    引擎运行中：engine.recover_breaker 负责内存态 + DB 双写；
    引擎未运行：只改 DB（下次 start → recover 读回 count=0 不补挂，组合正常）。
    D5（2026-11-18）：恢复原因写入 live_decision_events；已转手动恢复
    （count>=3 或 risk_state.manual_recovery）时必须显式 confirm=true。
    返回 (ok, error_message)。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return False, "资源不存在"
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=session_id, portfolio_strategy_id=portfolio_id,
    ).first()
    if link is None:
        return False, "该组合不在此 session 中"
    manual_recovery = int(link.circuit_breaker_count or 0) >= 3
    if not manual_recovery and link.risk_state:
        try:
            manual_recovery = bool(json.loads(link.risk_state).get("manual_recovery"))
        except (TypeError, ValueError):
            manual_recovery = False
    if manual_recovery and not confirm:
        return False, "该组合已转手动恢复，需二次确认（confirm=true）"
    engine = engines.get(session_id)
    if engine is not None:
        # 运行中：引擎方法负责内存 + DB 双写
        if not engine.recover_breaker(portfolio_id):
            return False, "该组合不在此 session 引擎中"
    else:
        # 未运行：只改 DB（下次 start → recover 只在 count>=3 转手动；count=0 不补挂）
        link.circuit_breaker_count = 0
        link.status = "active"
        # ⑤ 峰值口径与运行路径一致：默认不重锚（risk_state 里 peak 原样保留，
        # 回撤基准不变）；仅当配置显式开启时把峰值重锚到最近一次估值。
        if manual_reset_reanchor_enabled() and link.risk_state:
            try:
                state = json.loads(link.risk_state)
                last = state.get("last_bar_total_value")
                if last not in (None, 0, "0"):
                    state["peak_value"] = last
                    link.risk_state = json.dumps(state, ensure_ascii=False)
            except (TypeError, ValueError):
                logger.warning(
                    "recover_breaker: risk_state 解析失败，峰值保持原值 (session %s, "
                    "portfolio %s)", session_id, portfolio_id)
    # D5：恢复留痕（原因/时间/组合），本地单用户也保留审计链
    db.add(LiveDecisionEvent(
        live_session_id=session_id, gate="breaker_recover",
        layer="portfolio_risk", action="recover", bar_time=datetime.now(),
        portfolio_id=portfolio_id, param_name="recover",
        message=("手动恢复熔断 portfolio=%s reason=%s" % (
            portfolio_id, reason or ""))[:200],
    ))
    db.commit()
    return True, None


def account_risk_status(session_id: int,
                        engine: Optional[LiveEngine]) -> Optional[dict]:
    """D1/D2 账户级风控状态；引擎存在时带内存实时态。"""
    service = getattr(engine, "_account_risk", None) if engine is not None else None
    if service is None:
        return None
    return {
        "enabled": bool(service.enabled),
        "max_drawdown": (float(service.max_drawdown)
                         if service.max_drawdown is not None else None),
        "daily_loss_limit": (float(service.daily_loss_limit)
                             if service.daily_loss_limit is not None else None),
        "halted": bool(service.halted),
        "breaker_active": bool(service.breaker_active),
        "daily_pause_active": bool(service.daily_pause_active),
        "triggers": int(service.triggers or 0),
        "manual_recovery": bool(service.manual_recovery),
        "peak_value": (float(service.peak_value)
                       if service.peak_value is not None else None),
        "prev_close_value": (float(service.prev_close_value)
                             if service.prev_close_value is not None else None),
        "last_date": (service.last_date.isoformat()
                      if service.last_date is not None else None),
        "breaker_trigger_date": (
            service.breaker_trigger_date.isoformat()
            if service.breaker_trigger_date is not None else None),
    }


def update_account_risk(
    db: Session,
    session_id: int,
    payload: dict,
    engines: Dict[int, LiveEngine],
) -> Tuple[bool, object]:
    """配置账户级风控（D1/D2）。返回 (ok, error_or_data)。"""
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return False, (404, "资源不存在")

    def _ratio(name: str):
        raw = payload.get(name)
        if raw is None or raw == "":
            return None, None
        try:
            value = Decimal(str(raw))
        except Exception:  # noqa: BLE001
            return None, (400, "%s 非法" % name)
        if not (Decimal("0") < value <= Decimal("1")):
            return None, (400, "%s 需在 (0, 1] 区间" % name)
        return value, None

    max_dd, error = _ratio("max_drawdown")
    if error:
        return False, error
    daily_loss, error = _ratio("daily_loss_limit")
    if error:
        return False, error
    enabled = payload.get("enabled")
    if enabled is not None:
        session.account_risk_enabled = bool(enabled)
    if max_dd is not None:
        session.account_max_drawdown = max_dd
    if daily_loss is not None:
        session.account_daily_loss_limit = daily_loss
    db.commit()
    engine = engines.get(session_id)
    if engine is not None:
        account_risk = getattr(engine, "_account_risk", None)
        if account_risk is not None:
            account_risk.apply_config(
                enabled=None if enabled is None else bool(enabled),
                max_drawdown=max_dd, daily_loss_limit=daily_loss)
            account_risk.persist_state()
    return True, account_risk_status(session_id, engine) or {
        "enabled": bool(session.account_risk_enabled),
        "max_drawdown": (float(session.account_max_drawdown)
                         if session.account_max_drawdown is not None else None),
        "daily_loss_limit": (float(session.account_daily_loss_limit)
                             if session.account_daily_loss_limit is not None
                             else None),
    }


def recover_account_risk(
    db: Session,
    session_id: int,
    reason: Optional[str],
    engines: Dict[int, LiveEngine],
) -> Tuple[bool, object]:
    """人工恢复账户级熔断（D1）；未运行会话只清状态列。"""
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return False, (404, "资源不存在")
    engine = engines.get(session_id)
    account_risk = getattr(engine, "_account_risk", None) if engine else None
    if account_risk is not None:
        account_risk.recover(reason=reason)
    else:
        session.account_risk_state = None
        db.commit()
    return True, {"status": "recovered", "reason": reason}


def list_portfolio_daily(db: Session, session_id: int, days: int = 30):
    """A3：组合资金日序列（按日倒序；附账户合计行）。会话不存在返回 None。"""
    exists = db.query(LiveSession.id).filter(LiveSession.id == session_id).first()
    if exists is None:
        return None
    days = max(1, min(int(days or 30), 365))
    cutoff = date.today() - timedelta(days=days)
    rows = (
        db.query(LivePortfolioDaily)
        .filter(LivePortfolioDaily.live_session_id == session_id,
                LivePortfolioDaily.trade_date >= cutoff)
        .order_by(LivePortfolioDaily.trade_date.desc(),
                  LivePortfolioDaily.portfolio_strategy_id.asc())
        .all()
    )
    items = []
    for row in rows:
        items.append({
            "trade_date": row.trade_date.isoformat(),
            "portfolio_strategy_id": row.portfolio_strategy_id,
            "cash": float(row.cash) if row.cash is not None else None,
            "market_value": (float(row.market_value)
                             if row.market_value is not None else None),
            "total_value": (float(row.total_value)
                            if row.total_value is not None else None),
            "risk_value": (float(row.risk_value)
                           if row.risk_value is not None else None),
            "weight": float(row.weight) if row.weight is not None else None,
            "position_count": row.position_count,
            "detail": _loads_or_none(row.detail),
        })
    return items


def _loads_or_none(raw):
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# D3 待生效变更队列（2026-11-18）
# ---------------------------------------------------------------------------
# 组合级参数落在 portfolio_strategies；策略级风控参数落在 strategies。
PENDING_PORTFOLIO_FIELDS = ("max_drawdown", "daily_loss_limit")
PENDING_STRATEGY_FIELDS = ("stop_loss_ratio", "take_profit_ratio",
                           "trailing_stop_ratio")


def _pending_target(db: Session, session_id: int, portfolio_id: int,
                    strategy_id: Optional[int], field: str):
    """解析变更目标对象；返回 (obj, error)。"""
    if field not in PENDING_PORTFOLIO_FIELDS + PENDING_STRATEGY_FIELDS:
        return None, (400, "不支持的风控字段：%s" % field)
    link = db.query(LiveSessionPortfolio).filter_by(
        session_id=session_id, portfolio_strategy_id=portfolio_id).first()
    if link is None:
        return None, (404, "该组合不在此 session 中")
    if field in PENDING_PORTFOLIO_FIELDS:
        obj = db.query(PortfolioStrategy).filter_by(id=portfolio_id).first()
        if obj is None:
            return None, (404, "组合不存在")
        return obj, None
    if strategy_id is None:
        return None, (400, "策略级字段需提供 strategy_id")
    obj = db.query(Strategy).filter_by(
        id=int(strategy_id), portfolio_id=portfolio_id).first()
    if obj is None:
        return None, (404, "该策略不在此组合中")
    return obj, None


def _pending_ratio(raw):
    try:
        value = Decimal(str(raw))
    except Exception:  # noqa: BLE001
        return None, (400, "value 非法")
    if not (Decimal("0") <= value <= Decimal("1")):
        return None, (400, "value 需在 [0, 1] 区间")
    return value, None


def list_pending_changes(db: Session, session_id: int):
    """D3：待生效队列（含已应用/已放弃近 100 条）；会话不存在返回 None。"""
    exists = db.query(LiveSession.id).filter(LiveSession.id == session_id).first()
    if exists is None:
        return None
    rows = (
        db.query(LiveSessionPendingChange)
        .filter(
            LiveSessionPendingChange.live_session_id == session_id,
            # H2 拆单补发行通过 split-remainders 接口展示，不混入参数队列
            LiveSessionPendingChange.field != "split_remainder",
        )
        .order_by(LiveSessionPendingChange.id.desc())
        .limit(100)
        .all()
    )
    return [{
        "id": row.id,
        "scope": "session" if row.portfolio_strategy_id is None else "portfolio",
        "portfolio_strategy_id": row.portfolio_strategy_id,
        "strategy_id": row.strategy_id,
        "field": row.field,
        "old_value": row.old_value,
        "new_value": row.new_value,
        "status": row.status,
        "note": row.note,
        "created_at": _iso_utc(row.created_at),
        "applied_at": _iso_utc(row.applied_at),
    } for row in rows]


def enqueue_pending_change(db: Session, session_id: int, payload: dict,
                           engines: Dict[int, LiveEngine]):
    """入队/立即应用一条风控参数变更；返回 (ok, data_or_error)。"""
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if session is None:
        return False, (404, "资源不存在")
    try:
        portfolio_id = int(payload.get("portfolio_strategy_id"))
    except (TypeError, ValueError):
        return False, (400, "portfolio_strategy_id 非法")
    field = str(payload.get("field") or "").strip()
    strategy_id = payload.get("strategy_id")
    if strategy_id is not None:
        try:
            strategy_id = int(strategy_id)
        except (TypeError, ValueError):
            return False, (400, "strategy_id 非法")
    value, error = _pending_ratio(payload.get("value"))
    if error:
        return False, error
    target, error = _pending_target(
        db, session_id, portfolio_id, strategy_id, field)
    if error:
        return False, error
    old_value = getattr(target, field, None)
    note = payload.get("note")
    running = (session_id in engines
               or session.status in ("running", "stopping"))
    if running:
        query = db.query(LiveSessionPendingChange).filter(
            LiveSessionPendingChange.live_session_id == session_id,
            LiveSessionPendingChange.portfolio_strategy_id == portfolio_id,
            LiveSessionPendingChange.field == field,
            LiveSessionPendingChange.status == "pending",
        )
        query = (query.filter(LiveSessionPendingChange.strategy_id == strategy_id)
                 if strategy_id is not None
                 else query.filter(LiveSessionPendingChange.strategy_id.is_(None)))
        row = query.first()
        if row is None:
            row = LiveSessionPendingChange(
                live_session_id=session_id,
                portfolio_strategy_id=portfolio_id,
                strategy_id=strategy_id, field=field,
            )
            db.add(row)
        row.old_value = None if old_value is None else str(old_value)
        row.new_value = str(value)
        row.note = note
        row.status = "pending"
        row.applied_at = None
        db.commit()
        return True, {
            "id": row.id, "field": field, "old_value": row.old_value,
            "new_value": row.new_value, "status": "pending",
            "effective_at": "next_start",
            "message": "会话运行中：已加入待生效队列，下次启动生效",
        }
    # 未运行：立即应用并留一条 applied 审计
    setattr(target, field, value)
    row = LiveSessionPendingChange(
        live_session_id=session_id, portfolio_strategy_id=portfolio_id,
        strategy_id=strategy_id, field=field,
        old_value=None if old_value is None else str(old_value),
        new_value=str(value), note=note, status="applied",
        applied_at=datetime.now(),
    )
    db.add(row)
    db.commit()
    logger.info("pending change applied immediately (session %s, %s=%s)",
                session_id, field, value)
    return True, {
        "id": row.id, "field": field,
        "old_value": row.old_value, "new_value": row.new_value,
        "status": "applied", "effective_at": "now",
        "message": "会话未运行：已立即生效",
    }


def discard_pending_change(db: Session, session_id: int, change_id: int,
                           engines: Dict[int, LiveEngine]):
    """放弃一条 pending 变更（不改参数，保留审计）。"""
    row = db.query(LiveSessionPendingChange).filter_by(
        id=change_id, live_session_id=session_id).first()
    if row is None:
        return False, (404, "资源不存在")
    if row.status != "pending":
        return False, (400, "仅 pending 状态可放弃")
    row.status = "discarded"
    db.commit()
    return True, {"id": row.id, "status": "discarded"}


def list_split_remainders(db: Session, session_id: int):
    """H2：待补发的拆单剩余快照（停会话时写入，逐条可补发）。"""
    exists = db.query(LiveSession.id).filter(LiveSession.id == session_id).first()
    if exists is None:
        return None
    rows = (
        db.query(LiveSessionPendingChange)
        .filter(
            LiveSessionPendingChange.live_session_id == session_id,
            LiveSessionPendingChange.field == "split_remainder",
            LiveSessionPendingChange.status == "pending",
        )
        .order_by(LiveSessionPendingChange.id.asc())
        .all()
    )
    items = []
    for row in rows:
        code, _, signal_name = (row.old_value or "").partition("|")
        parts = (row.new_value or "").split("|")
        if len(parts) < 2 or not code:
            continue
        try:
            qty = int(parts[1])
        except (TypeError, ValueError):
            continue
        price = None
        if len(parts) > 2 and parts[2]:
            try:
                price = float(Decimal(parts[2]))
            except (ArithmeticError, TypeError, ValueError):
                price = None
        items.append({
            "id": row.id,
            "portfolio_strategy_id": row.portfolio_strategy_id,
            "strategy_id": row.strategy_id,
            "stock_code": code,
            "trade_type": parts[0],
            "quantity": qty,
            "price": price,
            "signal_type": parts[3] if len(parts) > 3 else "",
            "period": parts[4] if len(parts) > 4 else "",
            "signal_name": signal_name,
            "created_at": _iso_utc(row.created_at),
        })
    return items


def list_gate_snapshots(db: Session, session_id: int, days: int = 30):
    """E2：每日门控快照（按日倒序）。会话不存在返回 None。"""
    exists = db.query(LiveSession.id).filter(LiveSession.id == session_id).first()
    if exists is None:
        return None
    days = max(1, min(int(days or 30), 365))
    cutoff = date.today() - timedelta(days=days)
    rows = (
        db.query(LiveGateSnapshot)
        .filter(LiveGateSnapshot.live_session_id == session_id,
                LiveGateSnapshot.snap_date >= cutoff)
        .order_by(LiveGateSnapshot.snap_date.desc(),
                  LiveGateSnapshot.portfolio_strategy_id.asc())
        .all()
    )
    items = []
    for row in rows:
        items.append({
            "id": row.id,
            "snap_date": (row.snap_date.isoformat()
                          if row.snap_date is not None else None),
            "portfolio_id": row.portfolio_strategy_id,
            "benchmark_index": row.benchmark_index,
            "gate_engine": row.gate_engine,
            "direction": row.direction,
            "strength": row.strength,
            "as_of": row.as_of.isoformat() if row.as_of is not None else None,
        })
    return items


def reorder_split_remainders(
    db: Session, session_id: int, engine: LiveEngine,
    change_ids: Optional[list] = None,
):
    """H2：一键补发拆单剩余量（须运行中引擎；逐笔重走完整提交链）。

    返回 ``(data, error)``；成功补发的行置 applied，被闸门拦截/失败的行保留
    pending 便于人工处理后重试。
    """
    items = list_split_remainders(db, session_id)
    if items is None:
        return None, (404, "资源不存在")
    if change_ids:
        try:
            wanted = {int(i) for i in change_ids}
        except (TypeError, ValueError):
            return None, (400, "change_ids 必须为整数数组")
        items = [i for i in items if i["id"] in wanted]
    if not items:
        return None, (400, "没有待补发的拆单剩余量")
    payload = engine.reorder_split_remainders([dict(i) for i in items])
    results = payload.get("results") or []
    applied_ids = [
        r["id"] for r in results
        if r.get("status") == "accepted" and r.get("id") is not None
    ]
    if applied_ids:
        (
            db.query(LiveSessionPendingChange)
            .filter(LiveSessionPendingChange.id.in_(applied_ids))
            .update(
                {LiveSessionPendingChange.status: "applied",
                 LiveSessionPendingChange.applied_at: datetime.now()},
                synchronize_session=False,
            )
        )
    accepted = len(applied_ids)
    _record_decision_event(
        db, session_id, gate="split_reorder", layer="live_gate",
        action="submit" if accepted else "skip",
        message="拆单剩余补发：提交 %d 笔，成功 %d 笔，剩余 %d 笔"
                % (len(results), accepted, len(results) - accepted),
    )
    db.commit()
    remaining = (
        db.query(func.count(LiveSessionPendingChange.id))
        .filter(
            LiveSessionPendingChange.live_session_id == session_id,
            LiveSessionPendingChange.field == "split_remainder",
            LiveSessionPendingChange.status == "pending",
        )
        .scalar() or 0
    )
    return {
        "results": results,
        "accepted": accepted,
        "remaining": int(remaining),
    }, None


def apply_pending_changes(db: Session, session_id: int) -> int:
    """启动建引擎前应用本会话全部 pending 变更；返回应用条数。

    两类行共用同一张队列表：
    - ``portfolio_strategy_id IS NULL``：B1/A4 会话配置变更（engine/execution_mode/
      portfolio_ids/资金池等），值为 JSON；先应用，保证随后建的引擎读到新配置；
    - 非空：D3 风控参数变更（Decimal 比例），沿用原逻辑。
    """
    rows = (
        db.query(LiveSessionPendingChange)
        .filter(
            LiveSessionPendingChange.live_session_id == session_id,
            LiveSessionPendingChange.status == "pending",
            # H2 拆单剩余补发行单独消费，不参与参数应用
            LiveSessionPendingChange.field != "split_remainder",
        )
        .order_by(LiveSessionPendingChange.id.asc())
        .all()
    )
    applied = 0
    session_rows = [r for r in rows if r.portfolio_strategy_id is None]
    param_rows = [r for r in rows if r.portfolio_strategy_id is not None]
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    for row in session_rows:
        if session is None or row.field not in SESSION_NEXT_START_FIELDS:
            row.status = "discarded"
            logger.warning(
                "session pending change discarded (session %s id=%s field=%s)",
                session_id, row.id, row.field)
            continue
        try:
            value = json.loads(row.new_value)
        except (TypeError, ValueError):
            row.status = "discarded"
            logger.warning(
                "session pending change invalid value (session %s id=%s field=%s)",
                session_id, row.id, row.field)
            continue
        try:
            _apply_session_field(db, session, row.field, value)
        except Exception:  # noqa: BLE001 — 单条失败不阻断启动
            row.status = "discarded"
            logger.warning(
                "session pending change apply failed (session %s id=%s field=%s)",
                session_id, row.id, row.field, exc_info=True)
            continue
        row.status = "applied"
        row.applied_at = datetime.now()
        applied += 1
    for row in param_rows:
        target, error = _pending_target(
            db, session_id, row.portfolio_strategy_id,
            row.strategy_id, row.field)
        if error or row.new_value is None:
            row.status = "discarded"
            logger.warning(
                "pending change discarded (session %s id=%s field=%s): %s",
                session_id, row.id, row.field, error)
            continue
        try:
            value = Decimal(str(row.new_value))
        except Exception:  # noqa: BLE001
            row.status = "discarded"
            continue
        setattr(target, row.field, value)
        row.status = "applied"
        row.applied_at = datetime.now()
        applied += 1
    db.commit()
    if applied:
        logger.info("pending changes applied before start (session %s): %d",
                    session_id, applied)
    return applied


# ---------------------------------------------------------------------------
# M2 全局应急开关（2026-11-18）
# ---------------------------------------------------------------------------
def get_emergency(db: Session) -> dict:
    """全局应急开关状态（表/行缺失 = 关闭）。"""
    row = db.query(LiveGlobalSwitch).filter(LiveGlobalSwitch.id == 1).first()
    if row is None:
        return {"halt_new_positions": False, "reason": None, "operator": None,
                "updated_at": None}
    return {
        "halt_new_positions": bool(row.halt_new_positions),
        "reason": row.reason,
        "operator": row.operator,
        "updated_at": _iso_utc(row.updated_at),
    }


def set_global_halt(db: Session, halted: bool, reason: Optional[str],
                    operator: Optional[str],
                    engines: Dict[int, LiveEngine]) -> dict:
    """写入全局开关并即时同步所有运行中引擎（无需等 30s 刷新）。"""
    row = db.query(LiveGlobalSwitch).filter(LiveGlobalSwitch.id == 1).first()
    if row is None:
        row = LiveGlobalSwitch(id=1)
        db.add(row)
    row.halt_new_positions = bool(halted)
    row.reason = reason
    row.operator = operator
    row.updated_at = datetime.now()
    db.commit()
    for engine in (engines or {}).values():
        setter = getattr(engine, "set_global_halt", None)
        if setter is not None:
            try:
                setter(bool(halted))
            except Exception:  # noqa: BLE001
                logger.warning("sync global halt to engine failed", exc_info=True)
    return get_emergency(db)


def liquidate_all(db: Session, confirm: Optional[str], reason: Optional[str],
                  engines: Dict[int, LiveEngine]):
    """M2 一键全平：需显式 confirm=LIQUIDATE；仅运行中会话。"""
    if str(confirm or "") != "LIQUIDATE":
        return False, (400, "高危操作：需 confirm=LIQUIDATE 显式确认")
    results = []
    for session_id, engine in list((engines or {}).items()):
        fn = getattr(engine, "emergency_liquidate_all", None)
        if fn is None:
            continue
        try:
            detail = fn(reason=reason)
        except Exception as exc:  # noqa: BLE001 — 单会话失败不影响其它会话
            logger.exception("emergency liquidate failed (session %s)",
                             session_id)
            detail = {"error": str(exc)}
        results.append(dict(detail, session_id=session_id))
    return True, {"results": results, "reason": reason}


def bridge_status(session_id: int, status: str, engine: Optional[LiveEngine]) -> dict:
    """桥在线状态（§11 切片5 并入）。引擎运行中取最近已知在线态（不新发 /ping）；未运行返回 not_running。"""
    if engine is None:
        return {"online": None, "status": "not_running"}
    return {"online": _live_bridge_online(engine), "status": status}


# ---------------------------------------------------------------------------
# 历史查询 + 序列化
# ---------------------------------------------------------------------------

_SH_TZ = timezone(timedelta(hours=8))


def _iso_cst(dt: Optional[datetime]) -> Optional[str]:
    """上海墙钟 naive（bar_time/trade_time）→ 显式 +08:00。

    前端统一按 Asia/Shanghai 展示，依赖后端显式时区；naive 字符串不再猜测。
    """
    return dt.replace(tzinfo=_SH_TZ).isoformat() if dt else None


def _iso_utc(dt: Optional[datetime]) -> Optional[str]:
    """UTC naive（``server_default=func.now()`` 的 created_at/updated_at）→ +00:00。"""
    return dt.replace(tzinfo=timezone.utc).isoformat() if dt else None


def serialize_order(o: LiveOrder) -> dict:
    return {
        "id": o.id,
        "live_session_id": o.live_session_id,
        "portfolio_strategy_id": o.portfolio_strategy_id,
        "strategy_id": o.strategy_id,
        "stock_code": o.stock_code,
        # 引擎写入 lower（buy/sell），统一大写对齐回测与前端标签（历史行兼容）。
        "trade_type": (o.trade_type or "").upper(),
        "order_type": o.order_type,
        "price": float(o.price) if o.price is not None else None,
        "quantity": o.quantity,
        "filled_quantity": o.filled_quantity,
        "filled_price": float(o.filled_price) if o.filled_price is not None else None,
        "status": o.status,
        "error_message": o.error_message,
        "signal_name": o.signal_name,
        "signal_type": o.signal_type,
        "bar_time": _iso_cst(o.bar_time),
        "order_ref": o.order_ref,
        "bridge_order_id": o.bridge_order_id,
        # H5 进度：拆单子单 x/N、追价第 j 次；普通委托为 None
        "split_group": getattr(o, "split_group", None),
        "split_seq": getattr(o, "split_seq", None),
        "split_total": getattr(o, "split_total", None),
        "requote_attempt": getattr(o, "requote_attempt", None),
        "created_at": _iso_utc(o.created_at),
        "updated_at": _iso_utc(o.updated_at),
    }


def serialize_trade(t: LiveTrade) -> dict:
    return {
        "id": t.id,
        "live_session_id": t.live_session_id,
        "live_order_id": t.live_order_id,
        "portfolio_strategy_id": t.portfolio_strategy_id,
        "strategy_id": t.strategy_id,
        "stock_code": t.stock_code,
        "trade_type": (t.trade_type or "").upper(),
        "price": float(t.price) if t.price is not None else None,
        "quantity": t.quantity,
        "amount": float(t.amount) if t.amount is not None else None,
        "commission": float(t.commission) if t.commission is not None else None,
        "stamp_duty": float(t.stamp_duty) if t.stamp_duty is not None else None,
        "trade_time": _iso_cst(t.trade_time),
        "created_at": _iso_utc(t.created_at),
    }


_OVERSELL_WARNED: set = set()
_OVERSELL_LOCK = threading.Lock()


def _warn_replay_oversell(session_id, code, sell_qty, held_qty) -> None:
    """重放发现超卖流水告警（每 (session, code) 进程内只报一次，避免刷屏）。

    期初外部导入持仓未落 live_trades，重放时「只有卖出」会对空仓卖 ——
    ``Position.sell`` 把数量钳制为 0（不会产生负数），但账实不符必须暴露。
    """
    key = (session_id, code)
    with _OVERSELL_LOCK:
        if key in _OVERSELL_WARNED:
            return
        _OVERSELL_WARNED.add(key)
    logger.warning(
        "重放发现超卖流水（缺少买入/期初导入流水）：session=%s code=%s "
        "卖出=%s 当前持仓=%s——按 0 钳制、不展示负数",
        session_id, code, sell_qty, held_qty)


def _display_price(value) -> float:
    """成本价展示字段：收敛到 3 位小数（计算与存储仍全精度）。"""
    try:
        return float(Decimal(str(value)).quantize(Decimal("0.001")))
    except (ArithmeticError, TypeError, ValueError):
        return 0.0


def _display_money(value) -> float:
    """金额展示字段：收敛到 2 位小数（分的精度；计算与存储仍全精度）。"""
    d = dec_money(value)
    return float(d) if d is not None else 0.0


def aggregate_positions_from_trades(trades: List[LiveTrade]) -> List[dict]:
    """从 live_trades 重放聚合虚拟持仓（与 recover/Position.apply_trade 同口径）。

    按 trade_time 顺序：BUY 加权累加持仓+均价，SELL 减仓均价不变。按 (组合策略, 子策略, 股票)
    分组，同票被多个子策略持有时各成一行。返回按 (股票, 组合, 子策略) 排序。
    """
    agg: Dict[tuple, Position] = {}
    for tr in trades:
        key = (tr.portfolio_strategy_id, tr.strategy_id, tr.stock_code)
        pos = agg.get(key)
        if pos is None:
            pos = Position(tr.stock_code)
            agg[key] = pos
        if (TradeType(tr.trade_type.upper()) == TradeType.SELL
                and pos.quantity <= 0):
            # 无买入/期初导入流水却出现卖出 → 脏数据，告警但不制造负数
            _warn_replay_oversell(
                tr.live_session_id, tr.stock_code, tr.quantity, pos.quantity)
        pos.apply_trade(TradeEvent(
            strategy_id=tr.strategy_id,
            portfolio_id=tr.portfolio_strategy_id,
            stock_code=tr.stock_code,
            trade_type=TradeType(tr.trade_type.upper()),
            price=Decimal(str(tr.price)),
            quantity=tr.quantity,
            amount=Decimal(str(tr.amount)),
            commission=Decimal(str(tr.commission)),
            stamp_duty=Decimal(str(tr.stamp_duty)),
            trade_time=tr.trade_time,
        ))
    rows = []
    for (pid, sid, code), pos in agg.items():
        if pos.quantity <= 0:
            continue  # 已清仓（含超卖被钳制为 0），不展示负数
        rows.append({
            "portfolio_id": pid,
            "strategy_id": sid,
            "stock_code": code,
            "quantity": pos.quantity,
            "avg_cost": _display_price(pos.avg_cost),
            "market_value": _display_money(pos.market_value),
        })
    return sorted(rows, key=lambda r: (r["stock_code"], r["portfolio_id"], r["strategy_id"]))


def engine_virtual_positions(engine: LiveEngine) -> List[dict]:
    """运行中：读引擎内存态虚拟持仓，一行一个 (组合策略, 子策略, 股票)。

    只读引擎内存态，不修改 Position 对象；含当日已成交未落库的变动（更实时）。
    同票被多个子策略持有时各成一行（不再按 code 汇总净仓）。按 (股票, 组合, 子策略) 排序。
    """
    # 组合→股票池代码集（兜底归属标注用）：不在池内却被 _strategy_ctx_for_code
    # 挂到首个组合的仓位，attribution=fallback（前端显示「非池内·兜底归属」）。
    pools = getattr(engine, "_pool_codes_by_portfolio", None) or {}
    rows = []
    for port in engine.portfolios:
        pool_codes = pools.get(port.portfolio_id)
        for ctx in port.strategies:
            for code, pos in ctx.positions.items():
                if pos.quantity <= 0:
                    continue  # 已清仓；负数不应出现（sell 已钳制），防御性跳过
                row = {
                    "portfolio_id": port.portfolio_id,
                    "strategy_id": ctx.strategy_id,
                    "stock_code": code,
                    "quantity": pos.quantity,
                    "avg_cost": _display_price(pos.avg_cost),
                    "market_value": _display_money(pos.market_value),
                    # D6：内存态峰值（可能比峰值表更新，尚未 flush）
                    "highest_price": _display_price(
                        getattr(pos, "highest_price", None) or 0),
                }
                # 池信息缺失（旧引擎/测试）时不臆断，省略字段由前端按 pool 降级
                if pool_codes is not None:
                    row["attribution"] = "pool" if code in pool_codes else "fallback"
                rows.append(row)
    return sorted(rows, key=lambda r: (r["stock_code"], r["portfolio_id"], r["strategy_id"]))


# 外部持仓（真实账户原有持仓）读回缓存 TTL：THS UI 读回慢（约 3~10s，含验证码
# 流水线），60s 内复用上次读回，避免工作台每次刷新都重走 UI 自动化。
_EXTERNAL_POS_TTL_SECONDS = 60.0


def _normalize_external_symbol(code) -> str:
    """外部持仓代码归一：带后缀原样返回；6 位数字按首位推断交易所补后缀
    （92→BJ（北交所新股段）、6/5/9→SH、4/8→BJ、其余→SZ，与 tdx_tq_dispatcher._exchange_for 同口径）。

    无法归一（非 6 位数字且无后缀）返回空串，调用方丢弃该行。
    """
    text = str(code or "").strip()
    if "." in text:
        return text
    digits = re.sub(r"\D", "", text)[:6]
    if len(digits) != 6:
        return ""
    if digits.startswith("92"):
        suffix = "BJ"
    elif digits.startswith("11"):
        suffix = "SH"  # 沪市可转债 110/113/118
    elif digits.startswith(("6", "5", "9")):
        suffix = "SH"
    elif digits.startswith(("4", "8")):
        suffix = "BJ"
    else:
        suffix = "SZ"
    return "%s.%s" % (digits, suffix)


def engine_external_positions(engine: LiveEngine) -> List[dict]:
    """运行中：读 dispatcher 真实账户持仓，剔除策略虚拟持仓已持有的代码，
    剩余即「原有持仓」（非策略买入，仅供监视，不参与策略交易）。

    读回结果（dispatcher 原始行）带 60s TTL 缓存在引擎属性 _external_pos_cache
    （(monotonic_ts, rows)）上——THS UI 读回慢，TDX SDK 读快但同样走缓存统一口径。
    dispatcher 缺 get_positions（iquant 桥）/异常/None → 无外部行，不抛错。
    行内 ``out_of_pool``：代码不在任何组合股票池 → True（池外外部仓，仅展示/对账，
    不进风控、不占槽位、不代卖；前端据此显示「池外·不参与风控」徽章）。
    """
    dispatcher = getattr(engine, "dispatcher", None)
    if dispatcher is None:
        return []
    # 组合→股票池代码集：池外外部仓打 out_of_pool=True（前端「池外·不参与风控」
    # 徽章的唯一判据）。池信息缺失（旧引擎/测试未注入）时不臆断，一律 False。
    pools = getattr(engine, "_pool_codes_by_portfolio", None) or {}
    pool_codes: set = set()
    for codes in pools.values():
        pool_codes |= set(codes or ())
    virtual_codes = {r["stock_code"] for r in engine_virtual_positions(engine)}
    now = time.monotonic()
    real = None
    cached_rows = None
    cache = getattr(engine, "_external_pos_cache", None)
    if cache is not None:
        try:
            ts, rows = cache
            if isinstance(rows, list):
                cached_rows = rows
            if cached_rows is not None and now - ts < _EXTERNAL_POS_TTL_SECONDS:
                real = cached_rows
        except Exception:
            real = None
    if real is None:
        fn = getattr(dispatcher, "get_positions", None)
        if not callable(fn):
            # 桥通道（HttpBridgeDispatcher 只有 query_positions，没有 get_positions）：
            # 用引擎每轮采集写入的外部持仓快照（已归一化 symbol/volume/cost/market_value）。
            # 不读快照的话，iquant/goldminer 的持仓页永远「暂无持仓」——池外仓既不进
            # 虚拟账本、又不在这里返回（2026-11-10 实证：账户持仓市值 30.8 万、表格为空）。
            snap = getattr(engine, "_external_positions", None)
            real = [r for r in (snap or []) if isinstance(r, dict)]
        else:
            try:
                fresh = fn()
            except Exception:
                # 读失败（如 TdxTqDispatcher 的 BridgeUnavailableError）不是「零持仓」：
                # 不刷新缓存、沿用上次成功结果，避免瞬时失败把外部持仓闪成空。
                logger.warning("外部持仓读回失败（dispatcher.get_positions）", exc_info=True)
                real = cached_rows or []
            else:
                if isinstance(fresh, list):
                    real = fresh
                    try:
                        engine._external_pos_cache = (now, real)
                    except Exception:
                        pass
                else:
                    # None（ThsUiDispatcher 读表失败契约）同样不是真空仓：
                    # 不得写成空缓存，否则 60s TTL 内外部持仓会一直闪空。
                    logger.warning("外部持仓读回失败（dispatcher.get_positions 返回 None）")
                    real = cached_rows or []
    out = []
    for r in real:
        if not isinstance(r, dict):
            continue
        code = _normalize_external_symbol(r.get("symbol"))
        if not code:
            # QMT/掘金桥 /positions 行没有 symbol，只有 instrument(601989)+exchange(SH)。
            # 之前这里只看 symbol → 桥行全部被丢弃，持仓页对 iquant/goldminer 永远是
            # 「暂无持仓」（2026-11-10 实证：账户持仓市值 30.8 万、表格为空）。
            inst = str(r.get("instrument") or "").strip()
            exch = str(r.get("exchange") or "").strip()
            if inst and exch:
                code = _normalize_external_symbol("%s.%s" % (inst, exch))
        if not code or code in virtual_codes:
            continue
        try:
            quantity = int(r.get("volume") or 0)
        except (TypeError, ValueError):
            continue
        if quantity <= 0:
            continue
        try:
            avg_cost = float(r.get("cost") or 0)
            market_value = float(r.get("market_value") or 0)
        except (TypeError, ValueError):
            avg_cost, market_value = 0.0, 0.0
        if market_value <= 0:
            # 通达信持仓明细无市值列（market_value=0）→ 按最近已知价重算，
            # 成本价只进「成本价」列、不冒充市值（2026-11-18 口径统一）。
            for port in getattr(engine, "portfolios", None) or []:
                px = (getattr(port, "_price_snapshot", None) or {}).get(code)
                try:
                    if px is not None and float(px) > 0:
                        market_value = round(float(px) * quantity, 2)
                        break
                except (TypeError, ValueError):
                    continue
        out.append({
            "portfolio_id": None,
            "strategy_id": None,
            "stock_code": code,
            "quantity": quantity,
            "avg_cost": _display_price(avg_cost),
            "market_value": _display_money(market_value),
            "source": "external",
            # 池外仓：不在任何组合股票池 → 不参与风控/不占槽位/平台不代卖
            "out_of_pool": bool(pool_codes) and code not in pool_codes,
        })
    return out


def session_orders(db: Session, session_id: int, status: Optional[str] = None) -> Optional[List[dict]]:
    """委托历史。session 不存在返回 None。可选 status 过滤。"""
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None
    q = db.query(LiveOrder).filter(LiveOrder.live_session_id == session_id)
    if status:
        q = q.filter(LiveOrder.status == status)
    rows = q.order_by(LiveOrder.created_at.desc(), LiveOrder.id.desc()).all()
    items = [serialize_order(o) for o in rows]
    # H5 进度汇总：同一 split_group 的 N 笔子单 → 已成交笔数/剩余股数（组级，非单行）
    groups = {o.split_group for o in rows if getattr(o, "split_group", None)}
    if groups:
        members = (
            db.query(LiveOrder)
            .filter(LiveOrder.live_session_id == session_id,
                    LiveOrder.split_group.in_(groups))
            .all()
        )
        summary: Dict[str, dict] = {}
        for g in groups:
            group_rows = [r for r in members if r.split_group == g]
            total = max((r.split_total or 0) for r in group_rows) or len(group_rows)
            summary[g] = {
                "total": total,
                "filled_children": sum(
                    1 for r in group_rows if r.status == "filled"),
                "remaining_quantity": sum(
                    max(0, (r.quantity or 0) - (r.filled_quantity or 0))
                    for r in group_rows),
            }
        for item in items:
            if item.get("split_group"):
                item["split_summary"] = summary.get(item["split_group"])
    return items


# G1：deferred 重排次数标记（与引擎 `_RETRY_MARKER` 同格式，只读解析）
_RETRY_MARKER_TEXT = "deferred_retry:n="


def session_deferred_orders(db: Session, session_id: int) -> Optional[List[dict]]:
    """G1：待放行的顺延委托（会话停止时也可查；in_memory 恒 False）。

    会话运行中优先走引擎 `list_deferred_orders()`（能标记内存队列可达性）；
    本函数只做 DB 权威视图，供停止态/引擎缺失时回退。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None
    rows = (
        db.query(LiveOrder)
        .filter(LiveOrder.live_session_id == session_id,
                LiveOrder.status == "deferred")
        .order_by(LiveOrder.bar_time, LiveOrder.id)
        .all()
    )
    items = []
    for row in rows:
        msg = row.error_message or ""
        retry = 0
        if _RETRY_MARKER_TEXT in msg:
            try:
                retry = int(msg.split(_RETRY_MARKER_TEXT, 1)[1].split(";", 1)[0])
            except (ValueError, IndexError):
                retry = 0
        items.append({
            "id": row.id,
            "portfolio_strategy_id": row.portfolio_strategy_id,
            "strategy_id": row.strategy_id,
            "stock_code": row.stock_code,
            "trade_type": (row.trade_type or "").upper(),
            "price": None if row.price is None else float(row.price),
            "quantity": row.quantity,
            "signal_type": row.signal_type,
            "signal_name": row.signal_name,
            "period": None,
            "bar_time": None if row.bar_time is None else row.bar_time.isoformat(),
            "signal_bar_date": (
                None if row.bar_time is None
                else row.bar_time.date().isoformat()),
            "status": row.status,
            "retry_count": retry,
            "error_message": row.error_message,
            "in_memory": False,
        })
    return items


# G4：盘前效果统计（只读聚合，不改任何账）
_PRE_OPEN_METRICS = (
    "reported", "filled", "cancel_crossed", "requeued", "blocked", "deferred",
)


def _pre_open_bucket_key(day: date, group_by: str) -> str:
    if group_by == "week":
        iso = day.isocalendar()
        return "%04d-W%02d" % (iso[0], iso[1])
    return day.isoformat()


def pre_open_stats(db: Session, session_id: int, start: date, end: date,
                   group_by: str = "day") -> Optional[dict]:
    """G4：按日/周聚合盘前集合竞价的报出、成交、越价撤单、重排与积压。

    数据源只读：
    - `live_decision_events`（gate=pre_open_auction）：release=报出，
      block=提交异常/拦截，cancel_requeue=开盘越价撤单后重排；
    - `live_orders`：bar_time 落在 09:15–09:25 且状态 filled=盘前成交，
      status=deferred 按信号日归桶=仍积压的顺延单。
    会话不存在返回 None；空区间返回各桶零值（不伪造行）。
    """
    if group_by not in ("day", "week"):
        raise ValueError("group_by 仅支持 day / week")
    if start > end:
        raise ValueError("start 不能晚于 end")
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None
    start_dt = datetime.combine(start, datetime.min.time())
    end_dt = datetime.combine(end + timedelta(days=1), datetime.min.time())

    buckets: Dict[str, dict] = {}

    def _bucket(day: date) -> dict:
        key = _pre_open_bucket_key(day, group_by)
        item = buckets.get(key)
        if item is None:
            item = {"bucket": key}
            for name in _PRE_OPEN_METRICS:
                item[name] = 0
            buckets[key] = item
        return item

    decisions = (
        db.query(LiveDecisionEvent)
        .filter(LiveDecisionEvent.live_session_id == session_id,
                LiveDecisionEvent.gate == "pre_open_auction",
                LiveDecisionEvent.bar_time >= start_dt,
                LiveDecisionEvent.bar_time < end_dt)
        .all()
    )
    for event in decisions:
        if event.bar_time is None:
            continue
        item = _bucket(event.bar_time.date())
        if event.action == "release":
            item["reported"] += 1
        elif event.action == "block":
            item["blocked"] += 1
        elif event.action == "cancel_requeue":
            # 越价撤单确认并成功重排：同一次结果同时计入两项
            item["cancel_crossed"] += 1
            item["requeued"] += 1

    orders = (
        db.query(LiveOrder)
        .filter(LiveOrder.live_session_id == session_id,
                LiveOrder.bar_time >= start_dt,
                LiveOrder.bar_time < end_dt)
        .all()
    )
    for row in orders:
        if row.bar_time is None:
            continue
        item = _bucket(row.bar_time.date())
        clock = (row.bar_time.hour, row.bar_time.minute)
        if row.status == "filled" and (9, 15) <= clock < (9, 25):
            item["filled"] += 1
        elif row.status == "deferred":
            item["deferred"] += 1

    items = [buckets[k] for k in sorted(buckets)]
    totals = {name: sum(i[name] for i in items) for name in _PRE_OPEN_METRICS}
    return {
        "session_id": session_id,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "group_by": group_by,
        "items": items,
        "totals": totals,
    }


def session_trades(
    db: Session, session_id: int,
    start_date: Optional[date] = None, end_date: Optional[date] = None,
) -> Optional[List[dict]]:
    """成交历史，按 trade_time 倒序。session 不存在返回 None。

    start_date/end_date（上海自然日，闭区间）只做**查询过滤**，不删除任何记录；
    缺省不过滤，与旧行为一致。trade_time 为上海墙钟 naive，故用同日 00:00 比较。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None
    q = db.query(LiveTrade).filter(LiveTrade.live_session_id == session_id)
    if start_date is not None:
        q = q.filter(LiveTrade.trade_time >= datetime(
            start_date.year, start_date.month, start_date.day))
    if end_date is not None:
        q = q.filter(LiveTrade.trade_time < datetime(
            end_date.year, end_date.month, end_date.day) + timedelta(days=1))
    rows = q.order_by(LiveTrade.trade_time.desc(), LiveTrade.id.desc()).all()
    return [serialize_trade(t) for t in rows]


#: D6 峰值来源中文标签（前端直接展示，避免前端再维护一份枚举）
_PEAK_SOURCE_LABELS = {
    "bar": "逐bar抬高",
    "table": "峰值表",
    "period": "策略周期补种",
    "daily": "日线补种",
    "snapshot": "快照回填",
    "cost": "成本兜底",
    "import": "导入仓起点",
}


def attach_peak_info(db: Session, session_id: int,
                     engine: Optional[LiveEngine], rows: List[dict]) -> None:
    """D6：给策略持仓行补「峰值 / 峰值日期 / 来源标记」（原地修改）。

    运行中：内存峰值（``Position.highest_price``，可含未 flush 的最新值）与
    ``live_position_peaks`` 表值取大者；停止后：只用峰值表（``live_trades``
    重放无法还原历史 bar 峰值）。两者都没有时字段留空，由前端显示「—」。
    """
    strategy_rows = [r for r in rows if r.get("source") == "strategy"]
    if not strategy_rows:
        return
    try:
        table = {
            (int(r.portfolio_strategy_id), str(r.stock_code)): r
            for r in db.query(LivePositionPeak).filter(
                LivePositionPeak.live_session_id == session_id).all()
        }
    except Exception:  # noqa: BLE001 — 观测性字段，读取失败不阻断持仓接口
        table = {}
    buffer = (dict(getattr(engine, "_peak_buffer", {}) or {})
              if engine is not None else {})
    for row in strategy_rows:
        key = (int(row.get("portfolio_id") or 0), str(row.get("stock_code") or ""))
        try:
            peak = Decimal(str(row.get("highest_price") or 0))
        except (ArithmeticError, TypeError, ValueError):
            peak = Decimal("0")
        bar_time = None
        source = None
        rec = table.get(key)
        if rec is not None:
            try:
                peak = max(peak, Decimal(str(rec.highest_price or 0)))
            except (ArithmeticError, TypeError, ValueError):
                pass
            bar_time = rec.peak_bar_time
            source = rec.peak_source
        buf = buffer.get(key)
        if buf is not None and buf[0] >= peak:
            peak, bar_time, source = buf[0], buf[1] or bar_time, buf[2] or source
        if peak <= 0:
            row["peak_bar_time"] = None
            row["peak_source"] = None
            row["peak_source_label"] = None
            continue
        row["highest_price"] = _display_price(peak)
        row["peak_bar_time"] = _iso_cst(bar_time) if bar_time else None
        if source is None:
            # 无持久化来源：峰值不高于成本 → 成本起点；否则视为持仓期逐 bar 抬高
            try:
                cost = Decimal(str(row.get("avg_cost") or 0))
            except (ArithmeticError, TypeError, ValueError):
                cost = Decimal("0")
            source = "cost" if peak <= cost else "bar"
        row["peak_source"] = source
        row["peak_source_label"] = _PEAK_SOURCE_LABELS.get(source, source)


def session_positions(db: Session, session_id: int, engine: Optional[LiveEngine]) -> Optional[List[dict]]:
    """持仓分级（策略持仓/原有持仓）。运行中读引擎内存态虚拟持仓（含未落库当日
    变动，标 source=strategy）+ dispatcher 真实持仓差集（原有持仓，source=external）；
    停止后从 live_trades 重放聚合（无外部数据可得，全部标 source=strategy）。

    session 不存在返回 None。返回 List[dict]，前端按 source 分级渲染。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None
    if engine is not None:
        rows = engine_virtual_positions(engine)
        for r in rows:
            r["source"] = "strategy"
        rows.extend(engine_external_positions(engine))
        attach_peak_info(db, session_id, engine, rows)
        return rows
    trades = (
        db.query(LiveTrade)
        .filter(LiveTrade.live_session_id == session_id)
        .order_by(LiveTrade.trade_time, LiveTrade.id)
        .all()
    )
    rows = aggregate_positions_from_trades(trades)
    for r in rows:
        r["source"] = "strategy"
    attach_peak_info(db, session_id, None, rows)
    return rows


def serialize_decision(d: LiveDecisionEvent) -> dict:
    """决策闸门事件序列化（实盘/回测同字段口径）。"""
    return {
        "id": d.id,
        "gate": d.gate,
        "layer": d.layer,
        "action": d.action,
        "portfolio_id": d.portfolio_id,
        "strategy_id": d.strategy_id,
        "stock_code": d.stock_code,
        "bar_time": _iso_cst(d.bar_time),
        "param_name": d.param_name,
        "param_value": d.param_value,
        "actual_value": d.actual_value,
        "requested_qty": d.requested_qty,
        "final_qty": d.final_qty,
        "message": d.message,
        "created_at": _iso_utc(d.created_at),
        # M4 确认/关闭状态：open 未处理 / acked 已确认（含日终清零）
        "ack_status": getattr(d, "ack_status", None) or "open",
        "ack_note": getattr(d, "ack_note", None),
        "ack_at": _iso_utc(getattr(d, "ack_at", None)),
    }


# 决策事件读取侧年份下限：低于此值的 bar_time 视为历史脏行（epoch 秒误解析），
# 保留在库里以备追溯，但不进列表与统计。bar_time 为 NULL 的事件（peak_seed 审计等）
# 是正常事件，不在此过滤范围内。
_MIN_DECISION_BAR_TIME = datetime(2000, 1, 1)


def session_decisions(db: Session, session_id: int,
                      ack_status: Optional[str] = None) -> Optional[dict]:
    """决策闸门事件：聚合统计 + 原始事件。session 不存在返回 None。

    summary 复用 summarize_decisions（与回测详情同口径），按 (gate, param_name)
    分组计数；events 按 bar_time 正序返回（下钻看逐次触发）。
    ``ack_status`` 可选 open/acked 过滤（M4 事件关闭），ack_summary 始终按全量计。

    读取侧年份下限：历史脏行 ``bar_time = 1970-08-23 20:05:06``（旧
    ``parse_bar_time`` 把 ``20261106`` 当 epoch 秒，2026-11-06 一次性写入会话 4
    共 66 条）按约定保留在库里以备追溯，但不参与展示与统计。``bar_time`` 为
    NULL 的行（如 peak_seed 审计）是正常事件，照常返回。
    """
    session = db.query(LiveSession).filter(LiveSession.id == session_id).first()
    if not session:
        return None
    rows = (
        db.query(LiveDecisionEvent)
        .filter(
            LiveDecisionEvent.live_session_id == session_id,
            or_(
                LiveDecisionEvent.bar_time.is_(None),
                LiveDecisionEvent.bar_time >= _MIN_DECISION_BAR_TIME,
            ),
        )
        .order_by(LiveDecisionEvent.bar_time, LiveDecisionEvent.id)
        .all()
    )
    ack_summary = {"open": 0, "acked": 0}
    for d in rows:
        status = getattr(d, "ack_status", None) or "open"
        if status == "acked":
            ack_summary["acked"] += 1
        else:
            ack_summary["open"] += 1
    if ack_status:
        rows = [d for d in rows
                if (getattr(d, "ack_status", None) or "open") == ack_status]
    return {
        "summary": summarize_decisions(rows),
        "ack_summary": ack_summary,
        "events": [serialize_decision(d) for d in rows],
    }


def ack_decision(db: Session, session_id: int, event_id: int,
                 note: Optional[str] = None) -> Optional[dict]:
    """M4：确认/关闭单条决策事件；session 或事件不存在返回 None。

    重复确认为幂等覆盖（更新备注与时间），便于补记处理说明。
    """
    if not db.query(LiveSession.id).filter(LiveSession.id == session_id).first():
        return None
    event = (
        db.query(LiveDecisionEvent)
        .filter(LiveDecisionEvent.live_session_id == session_id,
                LiveDecisionEvent.id == event_id)
        .first()
    )
    if event is None:
        return None
    event.ack_status = "acked"
    event.ack_note = (str(note).strip()[:200] if note not in (None, "") else None)
    event.ack_at = datetime.now()
    db.commit()
    return serialize_decision(event)


def ack_all_decisions(db: Session, session_id: int,
                      note: Optional[str] = None,
                      day_end: bool = False) -> Optional[dict]:
    """M4：批量确认当前所有 open 事件；session 不存在返回 None。

    ``day_end=true`` 时备注默认写「日终清零」，用于日终把当天的拦截事件一次性
    归档，避免天天堆积；已 acked 的事件不重复处理。
    """
    if not db.query(LiveSession.id).filter(LiveSession.id == session_id).first():
        return None
    rows = (
        db.query(LiveDecisionEvent)
        .filter(LiveDecisionEvent.live_session_id == session_id,
                or_(LiveDecisionEvent.ack_status == "open",
                    LiveDecisionEvent.ack_status.is_(None)))
        .all()
    )
    resolved_note = str(note).strip()[:200] if note not in (None, "") else None
    if resolved_note is None and day_end:
        resolved_note = "日终清零"
    now = datetime.now()
    for event in rows:
        event.ack_status = "acked"
        event.ack_note = resolved_note
        event.ack_at = now
    db.commit()
    return {"acked": len(rows), "ack_status": "acked", "note": resolved_note}


def search_decision_events(db: Session, *, session_id: Optional[int] = None,
                           gate: Optional[str] = None,
                           layer: Optional[str] = None,
                           action: Optional[str] = None,
                           ack_status: Optional[str] = None,
                           keyword: Optional[str] = None,
                           start: Optional[date] = None,
                           end: Optional[date] = None,
                           limit: int = 200,
                           offset: int = 0) -> dict:
    """L4 关键事件检索：跨会话只读查询 ``live_decision_events``。

    过滤维度：会话 / gate / layer / action / ack 状态 / 关键字（message、stock_code、
    param_name）/ ``created_at`` 自然日闭区间（``start``、``end`` 为 Asia/Shanghai
    日期，按数据库本地时间比较）。按 ``created_at``、``id`` 倒序，``limit`` 夹在
    1~1000；bar_time 历史脏行沿用 ``session_decisions`` 同一年份下限过滤。

    纯只读：不写库、不改 ack、不触发任何交易路径。
    """
    try:
        limit = int(limit)
    except (TypeError, ValueError):
        limit = 200
    try:
        offset = int(offset)
    except (TypeError, ValueError):
        offset = 0
    limit = min(max(limit, 1), 1000)
    offset = max(offset, 0)

    query = db.query(LiveDecisionEvent).filter(
        or_(
            LiveDecisionEvent.bar_time.is_(None),
            LiveDecisionEvent.bar_time >= _MIN_DECISION_BAR_TIME,
        )
    )
    if session_id is not None:
        query = query.filter(LiveDecisionEvent.live_session_id == int(session_id))
    if gate:
        query = query.filter(LiveDecisionEvent.gate == str(gate))
    if layer:
        query = query.filter(LiveDecisionEvent.layer == str(layer))
    if action:
        query = query.filter(LiveDecisionEvent.action == str(action))
    if ack_status == "open":
        query = query.filter(or_(LiveDecisionEvent.ack_status == "open",
                                 LiveDecisionEvent.ack_status.is_(None)))
    elif ack_status == "acked":
        query = query.filter(LiveDecisionEvent.ack_status == "acked")
    text = str(keyword or "").strip()
    if text:
        like = "%%%s%%" % text
        query = query.filter(or_(
            LiveDecisionEvent.message.like(like),
            LiveDecisionEvent.stock_code.like(like),
            LiveDecisionEvent.param_name.like(like),
            LiveDecisionEvent.gate.like(like),
        ))
    if start is not None:
        query = query.filter(
            LiveDecisionEvent.created_at >= datetime.combine(start, datetime.min.time()))
    if end is not None:
        query = query.filter(
            LiveDecisionEvent.created_at
            < datetime.combine(end + timedelta(days=1), datetime.min.time()))

    total = query.count()
    rows = (query.order_by(LiveDecisionEvent.created_at.desc(),
                           LiveDecisionEvent.id.desc())
            .offset(offset).limit(limit).all())
    session_ids = {int(d.live_session_id) for d in rows}
    names = {}
    if session_ids:
        names = dict(
            db.query(LiveSession.id, LiveSession.name)
            .filter(LiveSession.id.in_(session_ids)).all())
    items = []
    for d in rows:
        item = serialize_decision(d)
        item["session_id"] = d.live_session_id
        item["session_name"] = names.get(d.live_session_id)
        items.append(item)
    return {"items": items, "total": total, "limit": limit, "offset": offset}


def serialize_signal(s: LiveSignal) -> dict:
    return {
        "id": s.id,
        "portfolio_id": s.portfolio_strategy_id,
        "strategy_id": s.strategy_id,
        "stock_code": s.stock_code,
        "period": s.period,
        "bar_time": _iso_cst(s.bar_time),
        "formula_name": s.formula_name,
        "signal_name": s.signal_name,
        "signal_type": s.signal_type,
        "trigger_value": s.trigger_value,
        "price": str(s.price) if s.price is not None else None,
        "execution_mode": s.execution_mode,
        "status": s.status,
        "alert_status": s.alert_status,
        "alert_error": s.alert_error,
        "tdx_run_id": s.tdx_run_id,
        "created_at": _iso_utc(s.created_at),
        # alerted_at 由 SignalMonitor 用 now_shanghai() 写入（非 server_default），
        # 按上海时区标注；标成 UTC 会让外部消费方偏移 8 小时。
        "alerted_at": _iso_cst(s.alerted_at),
    }


def session_signals(db, session_id, strategy_id=None, stock_code=None,
                    signal_type=None, execution_mode=None, alert_status=None,
                    start=None, end=None, limit=100, offset=0):
    if db.query(LiveSession).filter(LiveSession.id == session_id).first() is None:
        return None
    q = db.query(LiveSignal).filter(LiveSignal.live_session_id == session_id)
    if strategy_id is not None:
        q = q.filter(LiveSignal.strategy_id == strategy_id)
    if stock_code:
        q = q.filter(LiveSignal.stock_code == stock_code)
    if signal_type:
        q = q.filter(LiveSignal.signal_type == signal_type)
    if execution_mode:
        q = q.filter(LiveSignal.execution_mode == execution_mode)
    if alert_status:
        q = q.filter(LiveSignal.alert_status == alert_status)
    if start is not None:
        q = q.filter(LiveSignal.bar_time >= start)
    if end is not None:
        q = q.filter(LiveSignal.bar_time <= end)
    total = q.count()
    rows = (q.order_by(LiveSignal.bar_time.desc(), LiveSignal.id.desc())
            .offset(offset).limit(min(max(limit, 1), 500)).all())
    return {"items": [serialize_signal(r) for r in rows], "total": total}


# ---------------------------------------------------------------------------
# N2 / K7 理论 vs 实际对照
# ---------------------------------------------------------------------------
def _curve_points(rows) -> list:
    """[(date, total_value)] → 首点归一曲线（跨初始资金可叠加）。"""
    points = [(d, float(v)) for d, v in rows if v is not None]
    base = next((v for _, v in points if v > 0), None)
    return [
        {
            "date": d.isoformat(),
            "total_value": round(v, 2),
            "index_value": round(v / base, 6) if base else None,
        }
        for d, v in points
    ]


def _curve_metrics(curve: list) -> dict:
    """曲线口径指标：累计收益 / 最大回撤（基于归一净值，与初始资金无关）。"""
    values = [p["total_value"] for p in curve if p["total_value"] is not None]
    if not values:
        return {"total_return": None, "max_drawdown": None}
    peak = values[0]
    max_dd = 0.0
    for v in values:
        peak = max(peak, v)
        if peak > 0:
            max_dd = min(max_dd, v / peak - 1)
    return {
        "total_return": (round(values[-1] / values[0] - 1, 6)
                         if values[0] else None),
        "max_drawdown": round(max_dd, 6),
    }


def _snapshot_dividend_mode(raw) -> Optional[str]:
    """从回测 params_snapshot 读 dividend_mode（容错：非法 JSON/缺失返回 None）。"""
    if not raw:
        return None
    try:
        data = json.loads(raw) if isinstance(raw, (str, bytes, bytearray)) else raw
    except (TypeError, ValueError):
        return None
    if isinstance(data, dict):
        mode = data.get("dividend_mode")
        return mode if isinstance(mode, str) else None
    return None


def compute_theory_vs_live(
    db: Session, session, portfolio_id: Optional[int] = None,
    start_date: Optional[date] = None, end_date: Optional[date] = None,
) -> dict:
    """N2：绑定回测记录（理论）与实盘日序列/成交（实际）的同窗对照。

    - 理论侧：``backtest_records`` 绑定的组合日快照 + 成交
    - 实际侧：``live_portfolio_daily`` + ``live_trades`` + ``live_orders``
    - 滑移：同股票/同日/同方向的理论均价 vs 实盘均价，买贵卖贱记为正
    - 未绑定回测记录时不报错（bound=False，只回实际侧），便于实盘页常驻展示
    """
    result: dict = {
        "bound": False, "backtest_record_id": None, "backtest_name": None,
        "window": {"start": None, "end": None},
        "theory": {"curve": [], "metrics": {}},
        "live": {"curve": [], "metrics": {}},
        "deviation": None,
        "warnings": [],
    }

    live_q = db.query(LivePortfolioDaily).filter(
        LivePortfolioDaily.live_session_id == session.id)
    if portfolio_id:
        live_q = live_q.filter(
            LivePortfolioDaily.portfolio_strategy_id == portfolio_id)
    if start_date:
        live_q = live_q.filter(LivePortfolioDaily.trade_date >= start_date)
    if end_date:
        live_q = live_q.filter(LivePortfolioDaily.trade_date <= end_date)
    live_rows = live_q.order_by(LivePortfolioDaily.trade_date).all()
    if not live_rows:
        result["warnings"].append(
            "无实盘日序列（live_portfolio_daily 尚未生成），实际侧曲线为空")

    live_curve = _curve_points(
        [(r.trade_date, r.total_value) for r in live_rows])

    trade_q = db.query(LiveTrade).filter(
        LiveTrade.live_session_id == session.id)
    if portfolio_id:
        trade_q = trade_q.filter(
            LiveTrade.portfolio_strategy_id == portfolio_id)
    if start_date:
        trade_q = trade_q.filter(LiveTrade.trade_time >= datetime.combine(
            start_date, datetime.min.time()))
    if end_date:
        trade_q = trade_q.filter(LiveTrade.trade_time <= datetime.combine(
            end_date, datetime.max.time()))
    live_trades = trade_q.all()

    order_q = db.query(LiveOrder).filter(
        LiveOrder.live_session_id == session.id)
    if portfolio_id:
        order_q = order_q.filter(
            LiveOrder.portfolio_strategy_id == portfolio_id)
    if start_date:
        order_q = order_q.filter(LiveOrder.created_at >= datetime.combine(
            start_date, datetime.min.time()))
    if end_date:
        order_q = order_q.filter(LiveOrder.created_at <= datetime.combine(
            end_date, datetime.max.time()))
    live_orders = order_q.all()

    filled = sum(1 for o in live_orders
                 if (o.filled_quantity or 0) >= (o.quantity or 0)
                 and (o.quantity or 0) > 0)
    partial = sum(1 for o in live_orders
                  if 0 < (o.filled_quantity or 0) < (o.quantity or 0))
    live_metrics = _curve_metrics(live_curve)
    live_metrics.update({
        "trades_count": len(live_trades),
        "orders_count": len(live_orders),
        "filled_orders": filled,
        "partial_orders": partial,
        "fill_rate": (round(filled / len(live_orders), 4)
                      if live_orders else None),
    })
    result["live"] = {"curve": live_curve, "metrics": live_metrics}

    rec_id = getattr(session, "backtest_record_id", None)
    if not rec_id:
        result["warnings"].append(
            "会话未绑定回测记录：只展示实际侧，无法做理论对照")
        return result
    rec = db.query(BacktestRecord).filter(BacktestRecord.id == rec_id).first()
    if rec is None:
        result["warnings"].append(f"绑定的回测记录 #{rec_id} 不存在")
        return result
    # J3：显式标注口径差异（实盘桥行情不复权 vs 回测复权），避免把除权日附近的
    # 理论/实际信号不一致误判为策略或执行缺陷。
    bt_mode = _snapshot_dividend_mode(getattr(rec, "params_snapshot", None))
    if bt_mode is not None and bt_mode not in ("none", "exact"):
        result["warnings"].append(
            "口径差异（J3）：实盘 1d/1w/1mon 桥行情不复权（adjust=0），"
            f"回测使用 {bt_mode} 复权；除权日附近理论/实际信号可能天然不一致，"
            "非执行缺陷")

    win_start = start_date or rec.start_date
    win_end = end_date or rec.end_date
    if live_rows:
        win_start = start_date or live_rows[0].trade_date
        win_end = end_date or live_rows[-1].trade_date
    result.update({
        "bound": True,
        "backtest_record_id": int(rec.id),
        "backtest_name": rec.name,
        "window": {
            "start": win_start.isoformat() if win_start else None,
            "end": win_end.isoformat() if win_end else None,
        },
    })

    snap_q = db.query(BacktestDailySnapshot).filter_by(
        backtest_record_id=rec.id, target_type="portfolio")
    if win_start:
        snap_q = snap_q.filter(BacktestDailySnapshot.snap_date >= win_start)
    if win_end:
        snap_q = snap_q.filter(BacktestDailySnapshot.snap_date <= win_end)
    snaps = snap_q.order_by(BacktestDailySnapshot.snap_date).all()
    theory_curve = _curve_points(
        [(s.snap_date, s.total_value) for s in snaps])

    bt_q = db.query(BacktestTrade).filter(
        BacktestTrade.backtest_record_id == rec.id)
    if win_start:
        bt_q = bt_q.filter(BacktestTrade.bar_time >= datetime.combine(
            win_start, datetime.min.time()))
    if win_end:
        bt_q = bt_q.filter(BacktestTrade.bar_time <= datetime.combine(
            win_end, datetime.max.time()))
    theory_trades = bt_q.all()

    theory_metrics = _curve_metrics(theory_curve)
    theory_metrics["trades_count"] = len(theory_trades)
    result["theory"] = {"curve": theory_curve, "metrics": theory_metrics}

    # ---- 成交滑移：同股票/同日/同方向的理论均价 vs 实盘均价 ----
    def _avg_by_key(trades, time_attr):
        acc: Dict[tuple, list] = {}
        for t in trades:
            ts = getattr(t, time_attr, None)
            if ts is None:
                continue
            key = (t.stock_code, ts.date(), t.trade_type)
            amt, qty = acc.get(key, (0.0, 0.0))
            acc[key] = (amt + float(t.amount or 0), qty + float(t.quantity or 0))
        return acc

    theory_px = _avg_by_key(theory_trades, "bar_time")
    live_px = _avg_by_key(live_trades, "trade_time")
    weighted_dev = 0.0
    weight_sum = 0.0
    pairs = []
    for key in set(theory_px) & set(live_px):
        t_amt, t_qty = theory_px[key]
        l_amt, l_qty = live_px[key]
        if t_qty <= 0 or l_qty <= 0:
            continue
        t_px, l_px = t_amt / t_qty, l_amt / l_qty
        if t_px <= 0:
            continue
        sign = 1.0 if key[2] == "BUY" else -1.0
        dev = sign * (l_px - t_px) / t_px
        weighted_dev += dev * l_amt
        weight_sum += l_amt
        pairs.append({
            "stock_code": key[0], "date": key[1].isoformat(),
            "trade_type": key[2], "theory_price": round(t_px, 4),
            "live_price": round(l_px, 4), "deviation_bps": round(dev * 10000, 2),
        })
    pairs.sort(key=lambda p: abs(p["deviation_bps"]), reverse=True)
    result["deviation"] = {
        "return_gap": (
            round(live_metrics["total_return"] - theory_metrics["total_return"], 6)
            if (live_metrics.get("total_return") is not None
                and theory_metrics.get("total_return") is not None) else None),
        "max_drawdown_gap": (
            round(live_metrics["max_drawdown"] - theory_metrics["max_drawdown"], 6)
            if (live_metrics.get("max_drawdown") is not None
                and theory_metrics.get("max_drawdown") is not None) else None),
        "trades_gap": len(live_trades) - len(theory_trades),
        "slippage": {
            "matched_pairs": len(pairs),
            "weighted_deviation_bps": (
                round(weighted_dev / weight_sum * 10000, 2)
                if weight_sum else None),
            "details": pairs[:20],
        },
    }
    if not snaps:
        result["warnings"].append("绑定回测记录在该窗口内无日快照，理论侧曲线为空")
    return result
