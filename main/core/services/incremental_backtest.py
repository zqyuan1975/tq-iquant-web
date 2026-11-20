"""接缝续跑式增量回测服务。

核心思路：某个组合策略在回测里跑出一段历史后调入实盘，回测从「接缝日」
（该组合进入实盘的交易日，显式落库在 ``live_session_portfolios``）接着往后
逐日延展，并与实盘在同一段区间上逐日对照。结果只要求与全量重跑「逐日特征
基本对齐」（特征级容差），换取每天省掉全量重跑的时间。

几条硬约束（漏一项就是静默错账）：
- 每组合虚拟现金、每票持仓（数量/成本/买入日/highest_price）、净值峰值、
  最大回撤计数、当日日亏基准与昨收净值、熔断剩余状态、已处理 bar 标记、
  参数快照哈希、口径版本、数据截止日，全部落 checkpoint。
- 口径版本闸：成本/成交价/复权/T+1/引擎版本任一变化 → 作废 checkpoint，
  从接缝日重跑。
- 参数指纹闸：组合级列 + 策略数值参数/公式/角色/周期 + 策略增删 + 股票池
  成分代码列表任一变化 → 作废 checkpoint 并从接缝日重建（日志注明变化分项）。
- 接缝粒度 = **交易日 + 日内时间游标**：checkpoint 同时记 ``as_of_date``（最后
  已处理交易日）与 ``as_of_bar_time``（最后已处理 bar 时间戳）。跨日不重放整天
  分钟 bar（引擎视野从次一交易日 00:00 起）；同一交易日内续跑只喂游标之后的 bar。
- 分钟级与日线共用同一条续跑路径（引擎按周期路由，订单/持仓状态序列化方式一致）。
  缺口规划只允许「恰好今天一天」走当日交接日线的快路径（交接物只有日线）。
- 双跑校验：从接缝日全量重算并与续跑结果按特征级容差对账。
"""
import hashlib
import json
import logging
import threading
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

import polars as pl
from sqlalchemy import func
from sqlalchemy.orm import Session

from core.config import load_config
from core.db import SessionLocal, MAINTENANCE_GATE
from core.engine.backtest_engine import BacktestEngine, build_prev_close_by_day
from core.engine.bar_poller import parse_bar_time, to_ohlcv
from core.engine.event import OrderEvent
from core.engine.portfolio import Portfolio
from core.engine.portfolio_builder import assemble_portfolio
from core.engine.position import Position
from core.models import (
    Formula,
    FormulaSignal,
    IncrementalBacktestCheckpoint,
    IncrementalBacktestDay,
    IncrementalBacktestValidation,
    LiveAccountSnapshot,
    LiveAccountPositionSnapshot,
    LiveCashFlow,
    LiveDecisionEvent,
    LiveSession,
    LiveSessionPortfolio,
    LiveTrade,
    PortfolioStrategy,
    StockPoolStock,
    Strategy,
)
from core.services.backtest_service import (
    STRATEGY_SNAPSHOT_FIELDS,
    _pool_stock_names,
    _slice_klines_from,
    _strategy_periods,
    build_klines,
    build_gate_tracker,
    build_params_snapshot,
    build_corporate_timeline,
    build_signal_cache,
)
from core.tq.data import no_trade_bar_mask
from core.engine.market_gate import GATE_FORMULA_VERSION
from core.engine.corporate_actions import (
    CORPORATE_ACTION_VERSION, build_timeline_from_rows,
)
from core.engine.pool_timeline import (
    POOL_TIMELINE_VERSION, build_pool_timeline,
)
from core.engine.execution_profile import (
    EXECUTION_PROFILE_VERSION, expand_profile, fingerprint,
)
from core.services.notify_service import notify
from tq_iquant_shared.constants import SignalType, TradeType

logger = logging.getLogger(__name__)

#: 抢 ``MAINTENANCE_GATE`` 的最长等待（秒）：备份 VACUUM INTO 约 15–20s，
#: 等一轮即可，超时就让出本轮（调度器 60s 后重试）。
GATE_WAIT_SECONDS = 30.0

_TZ = ZoneInfo("Asia/Shanghai")

SCHEMA_VERSION = 3
# 口径版本（任一语义变化 → 提升 SCHEMA_VERSION 并在注释里写明原因）
_COST_SCHEMA = "fee_inclusive_v2"    # 成本含佣金/印花税/过户费
_EXEC_SCHEMA = "next_bar_open_v1"    # 成交价 = 下一根 bar 开盘价
_T1_SCHEMA = "t1_forced_v1"          # 回测强制 T+1
# v3（2026-11-16）：资金基准对齐——实盘起账基准改为「真实账户总资产」口径，
# params_fingerprint 新增 sections["baseline"]（baseline_version/baseline_value）；
# 基准重置会重设 seam_date/seam_cash 并删除旧 checkpoint。旧 checkpoint 的
# initial_capital/seam 属于旧基准，恢复会把新基准覆盖回去，故整体作废重建。
_ENGINE_SCHEMA = "backtest_engine_v3"

# SCHEMA_VERSION 3（2026-11-16，P1 核查）：
#   1) checkpoint state 新增 risk_cooldown（风控冷静期不再续跑后清零）；
#   2) 参数指纹新增 sections["formula"]（公式正文哈希/engine/no_leak_cross/信号线）
#      与接缝日游标 seam_bar_time；
#   3) 成本/成交口径变化：无成交假 bar 剔除 + 卖出印花税按成交日分段（2023-08-28
#      前 0.1%）。旧 checkpoint 的 state 缺冷静期、结果按旧成本口径算出 → 一律判废。
#   **不自动迁移/修补旧 checkpoint**：判废后由调用方（用户/调度器）决定何时从接缝日
#   重建（下一轮 run_incremental 会按常规路径重建）。

# 小样本豁免阈值（修复 9）：成交笔数与方向一致率在个位数样本上没有统计意义。
# 取 5 的依据：n=4 时任意 1 笔不一致 → 一致率 ≤ 0.75，直接击穿 0.9 容差，
# 属纯噪声误判；n≥5 时单个离群点最多把一致率压到 0.8，与「允许极少数边界单
# （停牌/涨跌停顺延）跨日漂移」的既有口径同量级。样本量按「两侧成交笔数之和」
# 计（续跑 + 全量重算，只统计参与对账的日期区间）；命中时笔数/方向两项判 None
# 不参与总体结论，净值与回撤偏差照常全量校验。常量独立便于以后调阈值。
SMALL_SAMPLE_MIN_TRADES = 5

# 日线及以上周期（不含分钟 bar 的周期）。仅用于判断「该组合是否需要日内游标」：
# 全为日线及以上时，同一交易日的重复触达一律按「当日已续跑」跳过（日线 bar 只有
# 一根，15:00 之后取到即当日收盘，与历史行为逐字一致）；含任一周期即启用日内游标。
_DAILY_PERIODS = {"1d", "1w", "1mon"}

# 日内游标收敛判据：当日最后一根分钟 bar 的时间戳下界（15:00 收盘，留 5 分钟
# 数据落盘余量）。本段最后一根 bar 的时间戳达到该下界才把 checkpoint 标成
# ``day_complete``——否则 11:00 手工触发的一次续跑会把「只跑到 11:00」的日子
# 误标成已完成，15:10 的调度 / 当日再触发就再也不会补齐余下 bar（静默丢半天）。
_DAY_LAST_BAR_TIME = time(14, 55)

# 同一组合策略的续跑/校验串行化（独立于 backtest 的 _BACKTEST_LOCK，
# 续跑不排队等全量回测）。
_INCR_LOCK = threading.Lock()

# ---------------------------------------------------------------- 当日 bar 交接
# DailyCloser 15:05 已取到的日线（含公式预热窗口），续跑同日常规路径直接复用，
# 不再额外卡通达信行情接口。每会话保留最近 3 个交易日，避免内存无界增长。
_DAILY_BARS: Dict[tuple, Dict[str, list]] = {}
_DAILY_BARS_ORDER: Dict[int, List[date]] = {}
_DAILY_BARS_LOCK = threading.Lock()


def record_daily_bars(session_id: int, trade_date: date,
                      bars_by_code: Dict[str, list]) -> None:
    """登记 15:05 DailyCloser 已取到的日线；供当日续跑复用（零额外 TQ 拉取）。"""
    if not session_id or not trade_date or not bars_by_code:
        return
    with _DAILY_BARS_LOCK:
        _DAILY_BARS[(session_id, trade_date)] = bars_by_code
        order = _DAILY_BARS_ORDER.setdefault(session_id, [])
        if trade_date not in order:
            order.append(trade_date)
        while len(order) > 3:
            old = order.pop(0)
            _DAILY_BARS.pop((session_id, old), None)


def take_daily_bars(session_id: int, trade_date: date) -> Optional[Dict[str, list]]:
    """取当日交接的日线；无则 None（走 build_klines 补跑路径）。"""
    with _DAILY_BARS_LOCK:
        return _DAILY_BARS.get((session_id, trade_date))


def _klines_from_handoff(bars_by_code: Dict[str, list]) -> dict:
    """交接日线 → {code: {"1d": polars.DataFrame(datetime, Open...Volume)}}。"""
    out: dict = {}
    for code, rows in bars_by_code.items():
        data = []
        for row in rows:
            t = parse_bar_time(row)
            o = to_ohlcv(row)
            if t is None:
                continue
            if o["open"] <= 0 or o["close"] <= 0:
                continue  # 伪影/停牌行，与取数层过滤口径一致
            data.append({
                "datetime": datetime(t.year, t.month, t.day),
                "Open": _dec(o["open"]),
                "High": _dec(o["high"]),
                "Low": _dec(o["low"]),
                "Close": _dec(o["close"]),
                "Volume": int(o["volume"]),
            })
        if data:
            # 无成交假 bar 剔除（2026-11-16 P0）：DailyCloser 交接日线同样可能带
            # fill 补出的停牌行（OHLC 全等于前收、Volume=0），与 build_klines 的
            # 取数层同口径剔除，否则快路径会照停牌价成交/估值。
            mask = no_trade_bar_mask(
                [r["Open"] for r in data], [r["High"] for r in data],
                [r["Low"] for r in data], [r["Close"] for r in data],
                [r["Volume"] for r in data])
            data = [r for r, nt in zip(data, mask) if not nt]
        if data:
            out[code] = {"1d": pl.DataFrame(data)}
    return out


# ---------------------------------------------------------------- 序列化工具
def _s(v) -> Optional[str]:
    return None if v is None else str(v)


def _dt(v) -> Optional[str]:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return str(v)


def _pdt(v) -> Optional[datetime]:
    return datetime.fromisoformat(v) if v else None


def _pdate(v) -> Optional[date]:
    return date.fromisoformat(v) if v else None


def _dec(v, default="0") -> Decimal:
    if v is None:
        return Decimal(default)
    return Decimal(str(v))


def _order_to_dict(o: OrderEvent) -> dict:
    return {
        "strategy_id": o.strategy_id,
        "portfolio_id": o.portfolio_id,
        "stock_code": o.stock_code,
        "trade_type": o.trade_type.value,
        "signal_type": o.signal_type.value,
        "quantity": o.quantity,
        "price": _s(o.price),
        "bar_time": _dt(o.bar_time),
        "signal_name": o.signal_name,
        "signal_bar_date": _dt(o.signal_bar_date),
        "period": o.period,
        # 在途单寿命起点（B1）：随 checkpoint 跨段传递，续跑后仍受同一交易日上限约束。
        "pending_since": _dt(getattr(o, "pending_since", None)),
    }


def _order_from_dict(d: dict) -> OrderEvent:
    return OrderEvent(
        strategy_id=int(d["strategy_id"]),
        portfolio_id=int(d.get("portfolio_id") or 0),
        stock_code=d["stock_code"],
        trade_type=TradeType(d["trade_type"]),
        signal_type=SignalType(d["signal_type"]),
        quantity=int(d["quantity"]),
        price=Decimal(str(d["price"])) if d.get("price") is not None else None,
        bar_time=_pdt(d.get("bar_time")),
        signal_name=d.get("signal_name") or "",
        signal_bar_date=_pdate(d.get("signal_bar_date")),
        period=d.get("period"),
        pending_since=_pdate(d.get("pending_since")),
    )


def state_payload(portfolio: Portfolio, as_of_date: date,
                  pending_orders: Optional[List[OrderEvent]] = None,
                  as_of_bar_time: Optional[datetime] = None,
                  day_complete: bool = False) -> dict:
    """把 Portfolio 完整内存态序列化为 JSON 安全 dict（漏项=静默错账）。

    as_of_bar_time：**日内时间游标**——本段最后处理的一根 bar 的时间戳。跨日接缝
    用它之外的交易日做边界（次一交易日 00:00 起），同一交易日内续跑只喂
    ``datetime > as_of_bar_time`` 的 bar，已处理 bar 绝不重放（否则信号/风控/成交
    会被同一根 bar 驱动两次）。None = 旧 checkpoint（无游标，按整日边界处理）。
    day_complete：该交易日是否已收到收盘 bar（见 ``_DAY_LAST_BAR_TIME``）。False
    时同一天再次触达要接着往后跑，而不是直接判「当日已续跑」。
    """
    positions: Dict[str, dict] = {}
    for ctx in portfolio.strategies:
        bucket: Dict[str, dict] = {}
        for code, pos in ctx.positions.items():
            if pos.quantity == 0 and not pos._lots:
                continue
            bucket[code] = {
                "quantity": int(pos.quantity),
                "avg_cost": _s(pos.avg_cost),
                "highest_price": _s(pos.highest_price),
                "buy_time": _dt(pos.buy_time),
                "strategy_quantity": int(pos.strategy_quantity),
                "strategy_cost": _s(pos.strategy_cost),
                "platform_quantity": int(pos.platform_quantity),
                "add_count": int(pos.add_count),
                "same_day_sellable": bool(pos.same_day_sellable),
                "lot_size": int(pos.lot_size),
                "lots": {k.isoformat(): int(v) for k, v in pos._lots.items()},
            }
        positions[str(ctx.strategy_id)] = bucket

    rm = portfolio.risk_manager
    risk = {
        "peak_value": _s(rm.peak_value),
        "prev_close_value": _s(rm.prev_close_value),
        "manual_recovery": bool(rm.manual_recovery),
        "breaker_trigger_date": _dt(rm.breaker_trigger_date),
        "daily_pause_active": bool(rm.daily_pause_active),
        "daily_loss_trigger_date": _dt(rm.daily_loss_trigger_date),
        "consecutive_drawdown_triggers": int(rm.consecutive_drawdown_triggers),
        "circuit_breaker_active": bool(rm.circuit_breaker_active),
        "last_bar_date": _dt(rm._last_bar_date),
        "last_bar_total_value": _s(rm._last_bar_total_value),
        "last_daily_date": _dt(rm._last_daily_date),
        "daily_loss_on_equity_enabled": bool(rm.daily_loss_on_equity_enabled),
    }
    return {
        "schema": SCHEMA_VERSION,
        "as_of_date": as_of_date.isoformat(),
        "cash": _s(portfolio.account.cash),
        "initial_capital": _s(portfolio.account.initial_capital),
        "positions": positions,
        "risk": risk,
        "price_snapshot": {k: _s(v) for k, v in portfolio._price_snapshot.items()},
        "price_history": {
            k: [_s(x) for x in v] for k, v in portfolio._price_history.items()},
        "price_hist_last_t": {
            k: _dt(v) for k, v in portfolio._price_hist_last_t.items()},
        # 风控冷静期（P1，2026-11-16）：{(strategy_id, code): 风控卖出日}。漏存 →
        # 续跑后冷静期清零，全量会拦的回买在续跑段直接成交（两端口径不一致）。
        "risk_cooldown": {
            "%s|%s" % (sid, code): d.isoformat()
            for (sid, code), d in (portfolio._risk_cooldown or {}).items()
        },
        "pending_orders": [_order_to_dict(o) for o in (pending_orders or [])],
        "processed_bar_date": as_of_date.isoformat(),
        # 接缝粒度 = 交易日 + 日内时间游标（见函数 docstring）
        "as_of_bar_time": _dt(as_of_bar_time),
        "day_complete": bool(day_complete),
    }


def restore_portfolio(portfolio: Portfolio, payload: dict) -> List[OrderEvent]:
    """把 checkpoint 内存态灌回 Portfolio；返回待继续处理的在途单。"""
    portfolio.account.cash = _dec(payload.get("cash"))
    # 2026-11-16 资金基准对齐：initial_capital 是「当前配置/基准」而非路径依赖
    # 状态，**不从旧 checkpoint 回灌**（旧基准的 initial_capital 会把新基准覆盖
    # 回去，导致续跑侧净额凭空多/少一截）。保留装配时按现行基准写入的值。

    by_strategy = {ctx.strategy_id: ctx for ctx in portfolio.strategies}
    for sid_str, bucket in (payload.get("positions") or {}).items():
        ctx = by_strategy.get(int(sid_str))
        if ctx is None:
            continue
        for code, d in bucket.items():
            pos = Position(
                code,
                same_day_sellable=bool(d.get("same_day_sellable")),
                lot_size=int(d.get("lot_size") or 100),
            )
            pos.quantity = int(d.get("quantity") or 0)
            pos.avg_cost = _dec(d.get("avg_cost"))
            pos.highest_price = _dec(d.get("highest_price"))
            pos.buy_time = _pdt(d.get("buy_time"))
            pos.strategy_quantity = int(d.get("strategy_quantity") or 0)
            pos.strategy_cost = _dec(d.get("strategy_cost"))
            pos.platform_quantity = int(d.get("platform_quantity") or 0)
            pos.add_count = int(d.get("add_count") or 0)
            pos._lots = {
                date.fromisoformat(k): int(v)
                for k, v in (d.get("lots") or {}).items()
            }
            ctx.positions[code] = pos
        for code in list(ctx.positions):
            if code not in bucket:
                del ctx.positions[code]

    rm = portfolio.risk_manager
    r = payload.get("risk") or {}
    rm.peak_value = _dec(r.get("peak_value"))
    rm.prev_close_value = (
        _dec(r["prev_close_value"]) if r.get("prev_close_value") is not None else None)
    rm.manual_recovery = bool(r.get("manual_recovery"))
    rm.breaker_trigger_date = _pdate(r.get("breaker_trigger_date"))
    rm.daily_pause_active = bool(r.get("daily_pause_active"))
    rm.daily_loss_trigger_date = _pdate(r.get("daily_loss_trigger_date"))
    rm.consecutive_drawdown_triggers = int(
        r.get("consecutive_drawdown_triggers") or 0)
    rm.circuit_breaker_active = bool(r.get("circuit_breaker_active"))
    rm._last_bar_date = _pdate(r.get("last_bar_date"))
    rm._last_bar_total_value = (
        _dec(r["last_bar_total_value"])
        if r.get("last_bar_total_value") is not None else None)
    # 接缝日/段末日已经评估过 daily_loss；恢复后首根 bar 不得重复评估。
    rm._last_daily_date = _pdate(r.get("last_daily_date")) or _pdate(
        payload.get("as_of_date"))
    rm.daily_loss_on_equity_enabled = bool(r.get("daily_loss_on_equity_enabled"))

    portfolio._price_snapshot = {
        k: _dec(v) for k, v in (payload.get("price_snapshot") or {}).items()}
    portfolio._price_history = {
        k: [_dec(x) for x in v]
        for k, v in (payload.get("price_history") or {}).items()}
    portfolio._price_hist_last_t = {
        k: _pdt(v) for k, v in (payload.get("price_hist_last_t") or {}).items()
        if v}

    # 风控冷静期恢复（P1）：键 (strategy_id, code) → 风控卖出日。
    portfolio._risk_cooldown = {}
    for key, val in (payload.get("risk_cooldown") or {}).items():
        sid_str, _, code = str(key).partition("|")
        d = _pdate(val)
        if not code or d is None:
            continue
        try:
            portfolio._risk_cooldown[(int(sid_str), code)] = d
        except (TypeError, ValueError):
            continue

    return [_order_from_dict(d) for d in (payload.get("pending_orders") or [])]


# ---------------------------------------------------------------- 配置 / 口径
def _cfg() -> dict:
    return (load_config() or {}).get("incremental_backtest") or {}


def _resolve_modes(dividend_mode: str) -> tuple:
    """把三档复权模式拆成（记账取数档, 公式信号档）。

    ``dual`` = 信号前复权 + 记账后复权（与 ``backtest_service.run_backtest``
    同口径）。**TQ 不认 "dual"**：直传 ``dividend_type="dual"`` 会返回空表，
    续跑因此判定「无可用日线数据」——必须先拆成两档再分别取数/喂公式。
    """
    if dividend_mode == "dual":
        return "back", "front"
    return dividend_mode, dividend_mode


def _tolerances(cfg: Optional[dict] = None) -> dict:
    c = cfg or _cfg()
    t = c.get("tolerances") or {}
    return {
        "max_net_dev_pct": float(t.get("max_net_dev_pct", 0.5)),
        "mean_net_dev_pct": float(t.get("mean_net_dev_pct", 0.2)),
        "trade_count_match_ratio": float(t.get("trade_count_match_ratio", 0.9)),
        "direction_match_ratio": float(t.get("direction_match_ratio", 0.9)),
        "max_drawdown_dev_pct": float(t.get("max_drawdown_dev_pct", 1.0)),
    }


def schema_parts(dividend_mode: str) -> dict:
    """口径指纹的组成分项（随 checkpoint 落盘，作废时据此定位**具体**哪一项变了）。"""
    return {
        "schema": SCHEMA_VERSION,
        "cost": _COST_SCHEMA,
        "exec": _EXEC_SCHEMA,
        "t1": _T1_SCHEMA,
        "engine": _ENGINE_SCHEMA,
        "dividend_mode": dividend_mode,
        # 市场多空门控公式口径（2026-11-18）：公式/参数任何调整都会改变拦单点，
        # 接缝前后必须同口径，否则续跑结果与全量/实盘不可比。
        "market_gate": GATE_FORMULA_VERSION,
        # 公司行为内核口径（2026-11-18，J1/J2）：判定语义变更同样作废旧接缝。
        "corporate_actions": CORPORATE_ACTION_VERSION,
        # 时点股票池（2026-11-18，J7）与执行档位（N1）口径版本。
        "pool_timeline": POOL_TIMELINE_VERSION,
        "execution": EXECUTION_PROFILE_VERSION,
    }


def schema_fingerprint(dividend_mode: str) -> str:
    payload = json.dumps(schema_parts(dividend_mode), sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def params_hash(ps, dividend_mode: str, db: Optional[Session] = None,
                baseline: Optional[dict] = None) -> str:
    return params_fingerprint(ps, dividend_mode, db, baseline=baseline)["hash"]


# 参数指纹覆盖三类口径来源（任一变化都会让续跑与当前配置不一致）：
# 1) 组合级列（build_params_snapshot：资金/费率/滑点/池 id/复权模式…）；
# 2) 策略行——数值参数（止损/止盈/移动止损/加仓阈值/资金占比…）、公式、角色、
#    周期、策略增删（按 id 升序，重命名不参与）；
# 3) 股票池成分代码列表（组合级 ∪ 各策略独立池）。
# 实盘执行配置（max_order_amount/requote/split_order 等）不影响回测，不参与。
# 复用 params_snapshot 的同一字段口径（2026-11-16）：指纹与快照对「哪些策略
# 字段影响结果」必须一致，避免一处改了、另一处漏判。
# 不参与指纹的字段：name 只影响展示（重命名不该作废 checkpoint）；实盘执行配置
# （max_order_amount/requote/split_order）不影响回测结果（旧口径）。
_FP_EXCLUDED_FIELDS = frozenset({
    "name", "max_order_amount", "requote_enabled", "requote_max_attempts",
    "requote_price_mode", "requote_max_slippage_ticks",
    "split_order_enabled", "split_order_threshold",
    "split_order_count", "split_order_min_qty", "split_order_interval_seconds",
    "split_order_interval_jitter", "split_order_volume_pct",
})
_FP_STRATEGY_FIELDS = tuple(
    f for f in STRATEGY_SNAPSHOT_FIELDS if f not in _FP_EXCLUDED_FIELDS)

_FP_PART_LABELS = {
    "params": "组合级参数（资金/费率/滑点/复权模式等）",
    "strategies": "策略参数/公式/角色/周期或策略增删",
    "formula": "公式正文/引擎/跨周期口径/信号线",
    "pool": "股票池成分代码",
    "corporate_actions": "公司行为事件集合/处置档位",
    "pool_timeline": "时点股票池成分区间",
    "execution": "回测执行档位（成交假设）",
    "baseline": "资金基准（baseline_version / baseline_value）",
}

# schema_parts 各分项的中文名（checkpoint 作废日志用）
_SCHEMA_PART_LABELS = {
    "schema": "口径版本 SCHEMA_VERSION",
    "cost": "成本口径 _COST_SCHEMA",
    "exec": "成交口径 _EXEC_SCHEMA",
    "t1": "T+1 口径 _T1_SCHEMA",
    "engine": "引擎版本 _ENGINE_SCHEMA",
    "dividend_mode": "复权模式",
    "corporate_actions": "公司行为内核口径",
    "pool_timeline": "时点股票池内核口径",
    "execution": "执行档位版本",
}


def _fp_value(v):
    """指纹取值：Decimal/float 统一转字符串，None/bool/int/str 原样。"""
    if v is None or isinstance(v, (str, bool, int)):
        return v
    return str(v)


def _strategy_fp_rows(ps, db: Optional[Session]) -> list:
    if db is None:
        return []
    rows = (
        db.query(Strategy)
        .filter_by(portfolio_id=ps.id)
        .order_by(Strategy.id)
        .all()
    )
    return [
        {f: _fp_value(getattr(s, f, None)) for f in _FP_STRATEGY_FIELDS}
        for s in rows
    ]


def _formula_fp(ps, db: Optional[Session]) -> dict:
    """公式实体指纹：正文哈希 + engine + no_leak_cross + 绑定信号线（2026-11-16 P1）。

    依据：旧指纹只有 ``formula_id``。改公式正文/计算引擎/跨周期口径，或增删改
    绑定的信号线（signal_name / trigger_value）后继续续跑，checkpoint 不判废
    → 旧持仓 + 新信号混算（结果既不是旧公式也不是新公式的完整产物）。
    正文用 sha256（正文可能很长，且历史记录只需判「变没变」）。
    """
    if db is None:
        return {}
    formula_ids = {
        s.formula_id for s in (
            db.query(Strategy).filter_by(portfolio_id=ps.id).all())
        if s.formula_id is not None
    }
    out: Dict[str, object] = {}
    for fid in sorted(formula_ids):
        formula = db.get(Formula, fid)
        if formula is None:
            out[str(fid)] = None
            continue
        signals = (
            db.query(FormulaSignal)
            .filter_by(formula_id=fid)
            .order_by(FormulaSignal.id)
            .all()
        )
        out[str(fid)] = {
            "content_sha256": hashlib.sha256(
                (formula.content or "").encode("utf-8")).hexdigest(),
            "engine": formula.engine,
            "no_leak_cross": bool(formula.no_leak_cross),
            "formula_count": formula.formula_count,
            "signals": [
                {"id": sig.id, "signal_name": sig.signal_name,
                 "signal_type": sig.signal_type,
                 "trigger_value": sig.trigger_value}
                for sig in signals
            ],
        }
    return out


def _pool_fp(ps, db: Optional[Session]) -> dict:
    """股票池成分指纹：{pool_id: 升序代码列表}（组合级 ∪ 各策略独立池）。"""
    if db is None:
        return {}
    pool_ids = set()
    if ps.stock_pool_id is not None:
        pool_ids.add(int(ps.stock_pool_id))
    rows = (
        db.query(Strategy)
        .filter_by(portfolio_id=ps.id)
        .order_by(Strategy.id)
        .all()
    )
    for s in rows:
        if s.stock_pool_id is not None:
            pool_ids.add(int(s.stock_pool_id))
    if not pool_ids:
        return {}
    out = {str(pid): [] for pid in sorted(pool_ids)}
    for r in (
        db.query(StockPoolStock)
        .filter(StockPoolStock.pool_id.in_(sorted(pool_ids)))
        .order_by(StockPoolStock.pool_id, StockPoolStock.stock_code)
        .all()
    ):
        out.setdefault(str(r.pool_id), []).append(r.stock_code)
    return out


def _corporate_actions_fp(ps, db: Optional[Session]) -> str:
    """公司行为指纹（2026-11-18）：开关 + 处置档位 + 事件集合摘要。

    关闭时返回固定串（"disabled"），开启时附内核版本与事件 digest —— 事件内容
    或处置档位一变，旧接缝判废（判废不自动重建，由调度器/用户决定）。
    """
    if not getattr(ps, "corporate_actions_enabled", False):
        return "disabled"
    digest = None
    if db is not None:
        digest = build_corporate_timeline(db).digest()
    return json.dumps({
        "version": CORPORATE_ACTION_VERSION,
        "reinvest": bool(getattr(ps, "dividend_reinvest_enabled", False)),
        "dispose_days": int(getattr(ps, "suspension_dispose_days", 0) or 0),
        "delist_force_close": bool(getattr(ps, "delist_force_close_enabled", False)),
        "digest": digest,
    }, sort_keys=True, ensure_ascii=False)


def _pool_timeline_fp(ps, db: Optional[Session]) -> str:
    """时点股票池指纹（2026-11-18，J7）：关闭=固定串；开启=内核版本 + 区间 digest。"""
    if not getattr(ps, "pool_pit_enabled", False):
        return "disabled"
    digest = None
    if db is not None:
        digest = build_pool_timeline(db, ps).digest()
    return json.dumps({
        "version": POOL_TIMELINE_VERSION,
        "digest": digest,
    }, sort_keys=True, ensure_ascii=False)


def _execution_fp(ps=None) -> str:
    """执行档位指纹（2026-11-18，N1）：增量调度固定 legacy，逐开关入指纹。"""
    return fingerprint(expand_profile(None))


def baseline_fingerprint(session_row) -> dict:
    """资金基准指纹（2026-11-16 基准对齐）：baseline_version + baseline_value。

    基准变更 → ``params_fingerprint`` 的 sections["baseline"] 变 → checkpoint
    作废，防旧 checkpoint 的 initial_capital/seam 把新基准覆盖回去。
    baseline_value 为空时回退 live_capital（旧会话尚无 baseline_value）。
    """
    if session_row is None:
        return {"baseline_version": 0, "baseline_value": None}
    value = getattr(session_row, "baseline_value", None)
    if value is None:
        value = getattr(session_row, "live_capital", None)
    return {
        "baseline_version": int(
            getattr(session_row, "baseline_version", 0) or 0),
        "baseline_value": _fp_value(value),
    }


def params_fingerprint(ps, dividend_mode: str,
                       db: Optional[Session] = None,
                       baseline: Optional[dict] = None) -> dict:
    """参数指纹：总体哈希 + 分项哈希（用于定位「哪一类变化」作废 checkpoint）。

    baseline（2026-11-16 资金基准对齐）：{"baseline_version": int,
    "baseline_value": str|None}。基准变更 → 指纹变 → checkpoint 作废重建，
    避免旧 checkpoint 的 initial_capital/seam 覆盖新基准。
    """
    sections = {
        "params": build_params_snapshot(ps, dividend_mode=dividend_mode),
        "strategies": json.dumps(
            _strategy_fp_rows(ps, db), sort_keys=True, ensure_ascii=False),
        # 公式实体（正文/引擎/跨周期口径/信号线）：只改公式不换 formula_id 时
        # 旧指纹完全无感 → 旧持仓 + 新信号混算，故单列分项。
        "formula": json.dumps(
            _formula_fp(ps, db), sort_keys=True, ensure_ascii=False),
        "pool": json.dumps(_pool_fp(ps, db), sort_keys=True, ensure_ascii=False),
        # 公司行为事件集合（2026-11-18）：开关/事件内容/处置档位任一变化，
        # 接缝前后半段口径就会不一致（历史分红/送转/退市强平无法复现）。
        "corporate_actions": _corporate_actions_fp(ps, db),
        # 时点股票池（2026-11-18，J7）：开关或成分区间变动 → 旧接缝判废
        # （历史某日「在不在池」不同，信号集合就不同）。
        "pool_timeline": _pool_timeline_fp(ps, db),
        # 执行档位（2026-11-18，N1）：legacy 固定串；realistic 或显式覆盖时按
        # 展开后的逐开关入指纹（成交假设变化必须新旧不混比）。
        "execution": _execution_fp(ps),
        "baseline": json.dumps(baseline or {}, sort_keys=True, ensure_ascii=False),
    }
    payload = json.dumps(sections, sort_keys=True, ensure_ascii=False)
    return {
        "hash": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
        "parts": {
            k: hashlib.sha256(v.encode("utf-8")).hexdigest()
            for k, v in sections.items()
        },
    }


def _invalidation_reasons(old_state: Optional[dict], old_schema_fp: str,
                          old_schema_version: int, want_schema_fp: str,
                          want_parts: dict,
                          want_schema_parts: Optional[dict] = None) -> List[str]:
    """定位 checkpoint 作废原因（落到日志，避免「检测到变化但看不到发生了什么」）。"""
    reasons: List[str] = []
    if int(old_schema_version) != SCHEMA_VERSION:
        reasons.append("口径版本 SCHEMA_VERSION 变化（%s → %s）"
                       % (old_schema_version, SCHEMA_VERSION))
    if old_schema_fp != want_schema_fp:
        # 旧 checkpoint 已把口径分项落盘 → 逐项报出具体变化（如引擎版本 v1→v2）；
        # 无分项（更老的 checkpoint）退化为笼统原因，不静默。
        old_sp = (old_state or {}).get("schema_parts")
        want_sp = want_schema_parts or {}
        if isinstance(old_sp, dict) and want_sp:
            changed = [
                "%s（%s → %s）" % (_SCHEMA_PART_LABELS.get(k, k), old_sp.get(k), want_sp.get(k))
                for k in want_sp if old_sp.get(k) != want_sp.get(k)
            ]
            reasons.append(
                "口径/复权模式指纹变化：" + ("；".join(changed) if changed else "分项未定位")
            )
        else:
            reasons.append("口径/复权模式指纹变化（旧 checkpoint 无分项，无法定位具体项）")
    old_parts = (old_state or {}).get("params_fp_parts")
    if not isinstance(old_parts, dict):
        reasons.append("旧 checkpoint 无分项指纹（按参数变化处理）")
    else:
        for key, label in _FP_PART_LABELS.items():
            if old_parts.get(key) != want_parts.get(key):
                reasons.append("%s变化" % label)
    return reasons or ["参数指纹变化（未定位到具体分项）"]



def checkpoint_valid(params_hash: str, schema_fp: str, schema_version: int,
                     expected_params_hash: str,
                     expected_schema_fp: str) -> bool:
    """口径版本闸：任一不匹配 → 作废 checkpoint，从接缝日重跑。"""
    return (
        int(schema_version) == SCHEMA_VERSION
        and params_hash == expected_params_hash
        and schema_fp == expected_schema_fp
    )


# ---------------------------------------------------------------- 交易日 / 补跑
def _weekday_days(start: date, end: date) -> List[date]:
    out: List[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:
            out.append(cur)
        cur += timedelta(days=1)
    return out


def plan_resume(as_of: date, today: date, max_catchup_days: int,
                trading_days: Optional[List[date]] = None) -> dict:
    """规划续跑区间。

    返回：
    - ``dates``：需要续跑的交易日（升序）
    - ``gap_dates``：超过补跑上限、无法逐日补齐的交易日（标红「断档」）
    - ``rebuild_from_seam``：是否需要从接缝日全量重建（断档兜底）
    """
    days = trading_days if trading_days is not None else _weekday_days(
        as_of + timedelta(days=1), today)
    dates = sorted({d for d in days if as_of < d <= today})
    cap = max(1, int(max_catchup_days or 1))
    if len(dates) > cap:
        return {"dates": dates, "gap_dates": list(dates),
                "rebuild_from_seam": True}
    return {"dates": dates, "gap_dates": [], "rebuild_from_seam": False}


def fast_path_allowed(as_of: Optional[date], today: date, periods,
                      handoff, max_catchup_days: int) -> bool:
    """是否允许走「只用当日交接日线」的快路径。

    快路径只拿得到 ``today`` 这一天的 15:05 交接日线，进入前必须先 ``plan_resume``
    规划缺口：**只有缺口恰好只有 today 一天**才允许。连续多日未跑（as_of 早于上一
    交易日）必须退回常规取数路径，由 build_klines + plan_resume 补齐中间交易日或
    标断档；否则中间交易日的 bar 会被静默跳过（旧实现的问题）。
    """
    if as_of is None or handoff is None or as_of >= today:
        return False
    if set(periods) != {"1d"}:
        return False
    plan = plan_resume(as_of, today, max_catchup_days)
    return (not plan["rebuild_from_seam"]) and plan["dates"] == [today]


def _slice_klines_after(klines: dict, cursor: datetime) -> dict:
    """引擎视野切片：只保留 ``datetime > cursor`` 的 bar（同一交易日内的续跑）。

    cursor = checkpoint 里记录的日内时间游标（最后已处理 bar 的时间戳）。同日
    再触发时游标之前的 bar 已经驱动过信号/风控/成交，重放会把同一根 bar 算两次
    （接缝重复计入）。跨日续跑不走此函数（整段都是新 bar）。
    """
    result: dict = {}
    for code, periods in klines.items():
        per: dict = {}
        for period, df in periods.items():
            if "datetime" not in df.columns:
                continue
            sliced = df.filter(pl.col("datetime") > cursor)
            if sliced.height > 0:
                per[period] = sliced
        if per:
            result[code] = per
    return result


def _max_bar_time(klines: dict) -> Optional[datetime]:
    """klines 里最晚的 bar 时间戳（= 本段最后处理的那一根）。"""
    latest: Optional[datetime] = None
    for periods in klines.values():
        for df in periods.values():
            if "datetime" not in df.columns:
                continue
            for t in df["datetime"].to_list():
                if t is not None and (latest is None or t > latest):
                    latest = t
    return latest


def _checkpoint_done(ckpt, today: date) -> bool:
    """checkpoint 是否已推进到 ``today`` 且当日已收满（调度器跳过判据）。

    - 早于今天 → 未完成；
    - 晚于今天 → 已完成（异常态，按完成处理避免空转）；
    - 同一天 → 看 ``day_complete``：分钟级策略盘中续跑过（游标只到 11:00 之类）
      时当日尚未收满，15:10 还必须再跑一次补齐余下 bar。旧/损坏 checkpoint 无该
      字段 → 按已完成处理（等同于改动前行为），由 run_incremental 自行作废重建。
    """
    if ckpt is None:
        return False
    if ckpt.as_of_date > today:
        return True
    if ckpt.as_of_date < today:
        return False
    try:
        state = json.loads(ckpt.state_json or "{}")
    except (TypeError, ValueError):
        return True
    return bool((state or {}).get("day_complete", True))


# ---------------------------------------------------------------- 接缝锚定
def capture_seam(db: Session, session_id: int, engine, today: Optional[date] = None,
                 force: bool = False,
                 portfolio_id: Optional[int] = None) -> None:
    """实盘启动（recover 之后）写入接缝日与起账状态；已写定则不覆盖。

    force=True（资金基准重置用）时覆盖已写定的接缝；portfolio_id 限定只重设
    指定组合的接缝（None=全部）。
    """
    today = today or datetime.now(_TZ).date()
    links = (
        db.query(LiveSessionPortfolio)
        .filter(LiveSessionPortfolio.session_id == session_id)
        .all()
    )
    ports = {p.portfolio_id: p for p in (getattr(engine, "portfolios", None) or [])}
    # 接缝时刻之前的最近一份**含有效持仓**的账户快照：实盘引擎跨重启正是从这里
    # 恢复移动止损峰值（LiveEngine._capture_account_snapshot）。接缝若只用成本
    # 种下，会把平台已掌握的历史高点丢掉 → 续跑侧移动止损基准低于实盘，同一只
    # 票出现「实盘止损、回测不止损」。capture_seam 在引擎首轮 tick 之前执行，
    # 库里最新快照必然来自上一次运行，不存在把接缝后的价格泄漏进起账状态。
    #
    # 不能直接取「最近一份」快照：会话停牌/停用后桥可能只回现金（实测 2026-11-11
    # 12:08 那份 market_value=0、持仓行为空），取到它会得到空 snap_peaks，峰值
    # 退化成成本价，等于峰值种植白做。故用 JOIN 在 SQL 里跳过无有效持仓行的快照，
    # 由近到远取第一份「quantity>0 且 highest_price>0」的快照。
    # 不用「最近 N 份里找」的分页上限：该查询走 live_session_id + captured_at
    # 索引并在 SQL 侧 LIMIT 1，命中即停，不会物化全历史持仓表；若一段长停牌期
    # 每天若干份空快照，固定 N 反而可能漏到成本价退化。
    snap = (
        db.query(LiveAccountSnapshot)
        .join(LiveAccountPositionSnapshot,
              LiveAccountPositionSnapshot.snapshot_id == LiveAccountSnapshot.id)
        .filter(
            LiveAccountSnapshot.live_session_id == session_id,
            LiveAccountPositionSnapshot.quantity > 0,
            LiveAccountPositionSnapshot.highest_price > 0,
        )
        .order_by(LiveAccountSnapshot.captured_at.desc(),
                  LiveAccountSnapshot.id.desc())
        .first()
    )
    snap_at = getattr(snap, "captured_at", None)
    snap_peaks: Dict[str, Decimal] = {}
    if snap is not None:
        for row in (snap.positions or []):
            if row.highest_price is None:
                continue
            hp = Decimal(str(row.highest_price))
            if hp > 0:
                snap_peaks[row.stock_code] = hp
    else:
        # 全历史都没有含持仓的快照 → 峰值只能按成本价种下，必须留痕可审计。
        # 区分「一份快照都没有」与「有快照但都是空仓」两种成因。
        total = (
            db.query(LiveAccountSnapshot)
            .filter(LiveAccountSnapshot.live_session_id == session_id)
            .count()
        )
        latest = (
            db.query(LiveAccountSnapshot)
            .filter(LiveAccountSnapshot.live_session_id == session_id)
            .order_by(LiveAccountSnapshot.captured_at.desc(),
                      LiveAccountSnapshot.id.desc())
            .first()
        )
        logger.warning(
            "[seam] 会话 %s 无任何含有效持仓的账户快照（快照总数=%s，最近一份 "
            "id=%s captured_at=%s market_value=%s），接缝峰值退化为成本价种下",
            session_id, total,
            getattr(latest, "id", None), getattr(latest, "captured_at", None),
            getattr(latest, "market_value", None),
        )
    changed = False
    for link in links:
        if portfolio_id is not None and link.portfolio_strategy_id != portfolio_id:
            continue
        if link.seam_date is not None and not force:
            # 接缝审计（纯日志）：已写定不覆盖时也留一条「锚点保留」，带上
            # 版本（接缝日#成交游标）、现金、持仓数与行 updated_at——用于区分
            # 「行被更新但字段未变」是否由接缝捕获导致（2026-11-19 排查）。
            _kept = _seam_positions_count(link.seam_positions_json)
            logger.info(
                "[seam] 锚点保留（已写定不覆盖）：session=%s portfolio=%s "
                "version=%s#%s seam_date=%s cursor=%s cash=%s positions=%s "
                "row_updated_at=%s",
                session_id, link.portfolio_strategy_id,
                getattr(link.seam_date, "isoformat", lambda: link.seam_date)(),
                getattr(link, "seam_trade_cursor", None),
                getattr(link.seam_date, "isoformat", lambda: link.seam_date)(),
                getattr(link, "seam_trade_cursor", None),
                getattr(link, "seam_cash", None), _kept,
                getattr(link, "updated_at", None),
            )
            continue
        port = ports.get(link.portfolio_strategy_id)
        if port is None:
            continue
        positions = []
        for ctx in port.strategies:
            for code, pos in ctx.positions.items():
                if not pos.quantity or pos.quantity <= 0:
                    continue
                # 起账峰值 = max(账本峰值/成本, 接缝前快照记录的历史最高价)。
                # 快照必须早于本笔持仓建仓（同 live_engine 的 stale-peak 口径：
                # 先清仓再重开时，上一轮持仓的高点不得复活）；建仓时间未知时
                # 仍信快照，与导入路径一致。
                seed_hp = pos.highest_price or pos.avg_cost
                prev_hp = snap_peaks.get(code)
                if (prev_hp is not None
                        and (pos.buy_time is None or snap_at is None
                             or pos.buy_time <= snap_at)):
                    seed_hp = max(seed_hp, prev_hp)
                positions.append({
                    "strategy_id": ctx.strategy_id,
                    "stock_code": code,
                    "quantity": int(pos.quantity),
                    "avg_cost": str(pos.avg_cost),
                    "buy_date": _dt(pos.buy_time.date() if pos.buy_time else None),
                    "highest_price": str(seed_hp),
                })
        link.seam_date = today
        link.seam_cash = port.account.cash
        # 已计入 seam_cash 的现金流水位（2026-11-16 P1）：实盘侧净值重放要并入
        # live_cash_flows，但接缝日的流水可能已包含在 seam_cash 里。按 id 水位
        # 右开最精确（flow_date 只有日期、created_at 是 UTC，都不可靠）。
        flow_cursor = (
            db.query(func.max(LiveCashFlow.id))
            .filter(
                LiveCashFlow.live_session_id == session_id,
                LiveCashFlow.portfolio_strategy_id == link.portfolio_strategy_id,
            )
            .scalar()
        )
        seam_bar_time = datetime.now(_TZ).replace(tzinfo=None).isoformat()
        link.seam_positions_json = json.dumps({
            "positions": positions,
            # 接缝游标（P0，2026-11-16）：引擎恢复完成、首轮 tick 之前的时刻。
            # 首段重放会按它切掉接缝日的分钟 bar —— 接缝现金/持仓取自 recover
            # 之后，已含这些 bar 驱动的成交；再全量重放当日就会重复开仓/扣现金。
            "seam_bar_time": seam_bar_time,
            "flow_cursor": int(flow_cursor) if flow_cursor is not None else 0,
        }, ensure_ascii=False)
        # 起账状态来自 recover 之后的引擎（已含当日成交）；记下已计入的成交
        # 水位，重放只补该水位之后的成交，防同一笔成交被算两次。
        cursor = (
            db.query(func.max(LiveTrade.id))
            .filter(
                LiveTrade.live_session_id == session_id,
                LiveTrade.portfolio_strategy_id == link.portfolio_strategy_id,
            )
            .scalar()
        )
        link.seam_trade_cursor = int(cursor) if cursor is not None else 0
        changed = True
        # 接缝审计（纯日志）：捕获时刻 + 锚点版本（接缝日#成交游标）+ 现金/持仓数。
        logger.info(
            "[seam] 锚点捕获：session=%s portfolio=%s version=%s#%s "
            "seam_date=%s cursor=%s cash=%s positions=%s captured_at=%s",
            session_id, link.portfolio_strategy_id, today.isoformat(),
            link.seam_trade_cursor, today.isoformat(), link.seam_trade_cursor,
            link.seam_cash, len(positions), seam_bar_time)
    if changed:
        db.commit()


def resolve_anchor(link: LiveSessionPortfolio, cfg: Optional[dict] = None) -> bool:
    """接缝锚定开关三态解析：NULL=跟随全局（默认 True）/ 0 / 1。"""
    if link.seam_anchor_enabled is not None:
        return bool(link.seam_anchor_enabled)
    return bool((cfg or _cfg()).get("anchor_on_live_start", True))


def _seam_positions_count(raw) -> int:
    """接缝 JSON 的持仓条数（兼容旧纯 list 与 dict 两种格式，坏数据返回 0）。"""
    try:
        data = json.loads(raw or "[]")
    except (TypeError, ValueError):
        return 0
    if isinstance(data, dict):
        data = data.get("positions") or []
    return len(data) if isinstance(data, list) else 0


def _seam_payload(raw) -> tuple:
    """解析接缝持仓 JSON → ``(positions, seam_bar_time)``。

    兼容两种存储：旧格式为纯 list（无游标 → 返回 None，行为与旧版一致）；
    新格式 ``{"positions": [...], "seam_bar_time": "ISO 本地时间"}``。
    """
    if not raw:
        return [], None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return [], None
    if isinstance(data, dict):
        return list(data.get("positions") or []), _pdt(data.get("seam_bar_time"))
    if isinstance(data, list):
        return data, None
    return [], None


def _seam_flow_cursor(raw) -> Optional[int]:
    """解析接缝 JSON 内的现金流水位 ``flow_cursor``（``live_cash_flows.id`` 右开）。

    为什么不用 ``flow_date``/``created_at``（2026-11-16 P1）：``flow_date`` 只有日期，
    同日流水无法区分先后；``created_at`` 是 SQLite ``func.now()`` 的 UTC 值，而接缝
    游标是本地（Asia/Shanghai）naive 时间，直接比较有 8 小时偏差——接缝日 09:00
    的流水会被判成「已计入」。按自增主键右开两处歧义都不存在。
    旧接缝（无该字段）返回 None，由调用方回退到 ``flow_date > seam_date`` 并告警。
    """
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(data, dict):
        return None
    cursor = data.get("flow_cursor")
    if cursor is None:
        return None
    try:
        return int(cursor)
    except (TypeError, ValueError):
        return None


def _trim_seam_day_intraday(klines: dict, seam_bar_time) -> dict:
    """首段重放：切掉接缝日 ``datetime <= seam_bar_time`` 的**分钟级** bar。

    依据（2026-11-16 P0）：``capture_seam`` 在 ``engine.recover(db)`` 之后取状态，
    接缝现金/持仓已包含当日盘中（如 09:31–10:30）成交；若首段仍从接缝日 00:00
    重放整日 bar，这些成交会进两次（重复开仓、重复扣现金）。日线及以上周期的
    bar 时间戳是**周期起点**（00:00），捕获时 LiveEngine 尚未处理当日日线信号，
    必须保留（否则首段丢掉当日 1d/1w/1mon 信号）；分钟级按游标右开切片。
    """
    if seam_bar_time is None:
        return klines
    out: dict = {}
    for code, periods in (klines or {}).items():
        out[code] = {}
        for period, df in (periods or {}).items():
            if df is None or period in _DAILY_PERIODS or "datetime" not in df.columns:
                out[code][period] = df
                continue
            try:
                sliced = df.filter(pl.col("datetime") > seam_bar_time)
            except Exception:  # noqa: BLE001 - 数据异常时保留原样（宁可重放也不静默丢）
                sliced = df
            out[code][period] = sliced
    return out


def apply_seam_anchor(portfolio: Portfolio, seam_cash, seam_positions: List[dict]) -> None:
    """按实盘起账状态重锚回测（现金=虚拟起账现金，存量仓按成本注入）。"""
    if seam_cash is not None:
        portfolio.account.cash = _dec(seam_cash)
    by_strategy = {ctx.strategy_id: ctx for ctx in portfolio.strategies}
    default_ctx = portfolio.strategies[0] if portfolio.strategies else None
    for item in seam_positions or []:
        code = item.get("stock_code")
        if not code:
            continue
        sid = item.get("strategy_id")
        ctx = by_strategy.get(int(sid)) if sid is not None else default_ctx
        if ctx is None:
            ctx = default_ctx
        if ctx is None:
            continue
        qty = int(item.get("quantity") or 0)
        if qty <= 0:
            continue
        cost = _dec(item.get("avg_cost"))
        pos = Position(code)
        pos.quantity = qty
        pos.avg_cost = cost
        pos.highest_price = _dec(item.get("highest_price"), str(cost)) or cost
        buy_date = _pdate(item.get("buy_date"))
        pos.buy_time = (datetime.combine(buy_date, time(0, 0))
                        if buy_date else None)
        if buy_date:
            pos._lots = {buy_date: qty}
        ctx.positions[code] = pos


# ---------------------------------------------------------------- 对齐评估
def _row_total(row: dict) -> Decimal:
    for key in ("total_value", "inc_total_value"):
        if row.get(key) is not None:
            return Decimal(str(row[key]))
    return Decimal("0")


def _row_trades(row: dict) -> int:
    for key in ("trades_count", "inc_trades_count"):
        if row.get(key) is not None:
            return int(row[key])
    return 0


def _max_drawdown_pct(values: List[Decimal]) -> float:
    peak = Decimal("0")
    worst = Decimal("0")
    for v in values:
        if v > peak:
            peak = v
        if peak > 0:
            dd = (peak - v) / peak
            if dd > worst:
                worst = dd
    return float(worst) * 100.0


def evaluate_alignment(inc_rows: dict, full_rows: dict, tolerances: dict) -> dict:
    """续跑 vs 全量重算的特征级对账。

    判据与依据：
    - 逐日净值相对偏差（最大 ≤ 0.5% / 均值 ≤ 0.2%）：日线收盘取整到分、
      费用与过户费舍入、T+1 分桶边界带来的日级噪声上界远小于 0.5%。
    - 逐日成交笔数一致率 ≥ 0.9、方向一致率 ≥ 0.9：允许极少数边界单
      （停牌/涨跌停顺延）跨日漂移。方向按两侧**各自**的成交明细（``dirs``）
      逐日比较；任一侧缺明细时该判据为 None（不参与判定，绝不恒真通过）。
    - 最大回撤偏差 ≤ 1.0pp：回撤是路径极值，对单日噪声更敏感，故放宽。
    """
    dates = sorted(set(inc_rows) & set(full_rows))
    if not dates:
        return {
            "status": "error", "max_net_dev_pct": None,
            "mean_net_dev_pct": None, "trade_count_match_ratio": None,
            "direction_match_ratio": None, "max_drawdown_dev_pct": None,
            "first_divergence_date": None, "dates_count": 0,
        }
    devs: List[float] = []
    count_match = 0
    direction_match = 0
    direction_eval = 0
    trades_seen = 0
    first_div: Optional[date] = None
    first_net_div: Optional[date] = None
    inc_series: List[Decimal] = []
    full_series: List[Decimal] = []

    for d in dates:
        a = inc_rows[d]
        b = full_rows[d]
        va = _row_total(a)
        vb = _row_total(b)
        inc_series.append(va)
        full_series.append(vb)
        denom = max(abs(vb), Decimal("1"))
        dev = float(abs(va - vb) / denom) * 100.0
        devs.append(dev)

        ta = _row_trades(a)
        tb = _row_trades(b)
        trades_seen += ta + tb
        same_count = ta == tb
        if same_count:
            count_match += 1
        # 方向判据：两侧各自成交明细（dirs）逐日比较；任一侧无明细（旧行/NULL）
        # → 该日不参与判定（返回 None），不再「缺数据即通过」。
        inc_dirs = a.get("dirs")
        full_dirs = b.get("dirs")
        same_dir: Optional[bool] = None
        if inc_dirs is not None and full_dirs is not None:
            same_dir = list(inc_dirs) == list(full_dirs)
            direction_eval += 1
            if same_dir:
                direction_match += 1
        if first_net_div is None and dev > tolerances["max_net_dev_pct"]:
            first_net_div = d
        if first_div is None and (dev > tolerances["max_net_dev_pct"]
                                  or not same_count
                                  or (same_dir is not None and not same_dir)):
            first_div = d

    # 小样本豁免（修复 9）：笔数与方向一致率在个位数样本上无统计意义（见
    # SMALL_SAMPLE_MIN_TRADES 注释）。净值/回撤偏差仍全量参与判定；此时
    # first_divergence_date 只反映净值背离（笔数/方向不参与）。
    small_sample = trades_seen < SMALL_SAMPLE_MIN_TRADES
    if small_sample:
        first_div = first_net_div

    n = len(dates)
    max_dev = max(devs)
    mean_dev = sum(devs) / n
    count_ratio = None if small_sample else count_match / n
    dir_ratio = (
        None if small_sample or not direction_eval
        else direction_match / direction_eval
    )
    dd_dev = abs(_max_drawdown_pct(inc_series) - _max_drawdown_pct(full_series))

    ok = (
        max_dev <= tolerances["max_net_dev_pct"]
        and mean_dev <= tolerances["mean_net_dev_pct"]
        and (count_ratio is None
             or count_ratio >= tolerances["trade_count_match_ratio"])
        and (dir_ratio is None
             or dir_ratio >= tolerances["direction_match_ratio"])
        and dd_dev <= tolerances["max_drawdown_dev_pct"]
    )
    return {
        "status": "pass" if ok else "fail",
        "max_net_dev_pct": round(max_dev, 4),
        "mean_net_dev_pct": round(mean_dev, 4),
        "trade_count_match_ratio": (round(count_ratio, 4)
                                    if count_ratio is not None else None),
        "direction_match_ratio": (round(dir_ratio, 4)
                                  if dir_ratio is not None else None),
        "max_drawdown_dev_pct": round(dd_dev, 4),
        "first_divergence_date": first_div,
        "dates_count": n,
        # 小样本豁免留痕：trades_seen < SMALL_SAMPLE_MIN_TRADES 时笔数/方向两项为 None
        "small_sample": small_sample,
        "trades_seen": trades_seen,
    }


# ---------------------------------------------------------------- 实盘侧重放
def _close_map(klines: dict) -> Dict[str, Dict[date, Decimal]]:
    """每 code 的「逐日收盘价」：优先 ``1d``，缺失时回落到分钟周期当日最后一根。

    分钟级策略的 klines 通常不含 ``1d``（只拉策略自身周期）。实盘侧逐日估值若仍
    只读 ``1d``，会退化成「最近成交价/成本」估值，与续跑侧（日终估值步 = 当日
    最后一根 bar 的 close）产生成倍虚假净值偏差。回落口径与回测日终估值步同源：
    取当日时间戳最晚的一根 bar 的 Close（同时间戳并列时取更细周期）。
    """
    out: Dict[str, Dict[date, Decimal]] = {}
    for code, periods in klines.items():
        df = periods.get("1d")
        if df is not None and "datetime" in df.columns:
            m: Dict[date, Decimal] = {}
            for t, c in zip(df["datetime"].to_list(), df["Close"].to_list()):
                if c is not None and c > 0:
                    m[t.date()] = Decimal(str(c))
            out[code] = m
            continue
        # 无日线：按「当日最晚时间戳」求收盘（细→粗遍历，并列时细周期先占位）
        last_dt: Dict[date, datetime] = {}
        m = {}
        for period in ("1m", "5m", "15m", "30m", "1h", "60m", "1w", "1mon"):
            pdf = periods.get(period)
            if pdf is None or "datetime" not in pdf.columns:
                continue
            for t, c in zip(pdf["datetime"].to_list(), pdf["Close"].to_list()):
                if t is None or c is None or c <= 0:
                    continue
                d = t.date()
                if d not in last_dt or t > last_dt[d]:
                    last_dt[d] = t
                    m[d] = Decimal(str(c))
        if m:
            out[code] = m
    return out


def _seam_trim_cursor(link: 'LiveSessionPortfolio', db: Session,
                      seam_bar_time: Optional[datetime]) -> Optional[datetime]:
    """首次构建时切分「已入账 bar」的游标（2026-11-16 P0）。

    优先用 v3 接缝落盘的 ``seam_bar_time``（捕获时刻）。旧接缝（纯 list 格式）没有
    该字段，退化为「成交游标那笔成交的时间」：recover 后的基线已含游标之前的全部
    成交，切掉此前的分钟 bar 即可避免同一笔成交进两次。两者都缺 → None（不裁剪，
    保持旧行为，由调用方日志留痕）。
    """
    if seam_bar_time is not None:
        return seam_bar_time
    cursor = getattr(link, "seam_trade_cursor", None)
    if cursor is None:
        return None
    trade = db.get(LiveTrade, cursor)
    if trade is None or trade.trade_time is None:
        return None
    return trade.trade_time


def _live_rows(db: Session, link: LiveSessionPortfolio, dates: List[date],
               closes: Dict[str, Dict[date, Decimal]]) -> Dict[date, dict]:
    """实盘侧逐日净值：接缝基线 + live_trades + live_cash_flows 重放，按该日收盘价估值。

    现金流（2026-11-16，P1）：旧实现只重放 ``live_trades``，出入金/分红不进现金，
    有流水时 live 侧净值系统性偏低（进而把正常的 inc_total 判成 ``net_dev``）。
    现金台账与成交一样按「接缝捕获之后」过滤，避免把已计入 ``seam_cash`` 的流水
    再算一次：新接缝按 ``flow_cursor``（``live_cash_flows.id`` 右开）精确过滤；
    旧接缝无游标，只能按 ``flow_date > seam_date`` 保守过滤并留痕。
    """
    base, _seam_bar_time = _seam_payload(link.seam_positions_json)
    qty: Dict[str, Decimal] = {}
    last_price: Dict[str, Decimal] = {}
    for item in base:
        code = item.get("stock_code")
        q = Decimal(str(item.get("quantity") or 0))
        qty[code] = q
        last_price[code] = Decimal(str(item.get("avg_cost") or 0))
    cash = Decimal(str(link.seam_cash or 0))

    trades_q = db.query(LiveTrade).filter(
        LiveTrade.live_session_id == link.session_id,
        LiveTrade.portfolio_strategy_id == link.portfolio_strategy_id,
    )
    cursor = getattr(link, "seam_trade_cursor", None)
    if cursor is not None:
        # 接缝基线的现金/持仓已含捕获时刻之前的成交 → 只重放之后新增的成交。
        trades_q = trades_q.filter(LiveTrade.id > int(cursor))
    else:
        # 旧接缝（改动前捕获）没有水位，只能按接缝日 00:00 重放；若接缝当日
        # 已有成交，则存在把已计入基线的成交再算一次的风险，必须留痕。
        trades_q = trades_q.filter(LiveTrade.trade_time >= datetime.combine(
            link.seam_date, time(0, 0)))
        same_day = (
            db.query(LiveTrade.id)
            .filter(
                LiveTrade.live_session_id == link.session_id,
                LiveTrade.portfolio_strategy_id == link.portfolio_strategy_id,
                LiveTrade.trade_time >= datetime.combine(link.seam_date, time(0, 0)),
                LiveTrade.trade_time < datetime.combine(
                    link.seam_date + timedelta(days=1), time(0, 0)),
            )
            .first()
        )
        if same_day is not None:
            logger.warning(
                "[seam] 会话 %s 组合 %s 的接缝无成交游标（旧版本捕获）且接缝日 "
                "%s 已有成交：按接缝日 00:00 重放，当日成交可能重复计入基线",
                link.session_id, link.portfolio_strategy_id, link.seam_date)
    trades = trades_q.order_by(LiveTrade.trade_time, LiveTrade.id).all()
    # 外部现金流台账（入金/出金/分红）：amount 有符号（>0 流入，<0 流出）。
    flows_q = db.query(LiveCashFlow).filter(
        LiveCashFlow.live_session_id == link.session_id,
        LiveCashFlow.portfolio_strategy_id == link.portfolio_strategy_id,
    )
    flow_cursor = _seam_flow_cursor(link.seam_positions_json)
    if flow_cursor is not None:
        # 接缝基线的现金已含捕获时刻之前的全部流水 → 只重放之后新增的（id 右开）。
        flows_q = flows_q.filter(LiveCashFlow.id > flow_cursor)
    else:
        flows_q = flows_q.filter(LiveCashFlow.flow_date > link.seam_date)
        same_day_flow = (
            db.query(LiveCashFlow.id)
            .filter(
                LiveCashFlow.live_session_id == link.session_id,
                LiveCashFlow.portfolio_strategy_id == link.portfolio_strategy_id,
                LiveCashFlow.flow_date == link.seam_date,
            )
            .first()
        )
        if same_day_flow is not None:
            logger.warning(
                "[seam] 会话 %s 组合 %s 的接缝无流水游标（旧版本）且接缝日 %s "
                "有现金流：按 flow_date > 接缝日重放，当日流水可能漏计",
                link.session_id, link.portfolio_strategy_id, link.seam_date)
    flows = flows_q.order_by(LiveCashFlow.flow_date, LiveCashFlow.id).all()
    ptr = 0
    flow_ptr = 0
    out: Dict[date, dict] = {}
    for d in dates:
        day_trades = 0
        dirs: List[str] = []
        while flow_ptr < len(flows) and flows[flow_ptr].flow_date <= d:
            # 现金流只改虚拟现金，不产生成交（不计入笔数/方向对账）。
            cash += Decimal(str(flows[flow_ptr].amount or 0))
            flow_ptr += 1
        while ptr < len(trades) and trades[ptr].trade_time.date() <= d:
            t = trades[ptr]
            amount = Decimal(str(t.amount or 0))
            fees = Decimal(str(t.commission or 0)) + Decimal(str(t.stamp_duty or 0))
            price = Decimal(str(t.price or 0))
            if t.trade_type == "BUY":
                qty[t.stock_code] = qty.get(t.stock_code, Decimal("0")) + Decimal(t.quantity)
                cash -= amount + fees
            else:
                qty[t.stock_code] = qty.get(t.stock_code, Decimal("0")) - Decimal(t.quantity)
                cash += amount - fees
            last_price[t.stock_code] = price
            if t.trade_time.date() == d:
                day_trades += 1
                dirs.append("%s:%s" % (t.stock_code, t.trade_type))
            ptr += 1

        market_value = Decimal("0")
        for code, q in qty.items():
            if q <= 0:
                continue
            px = closes.get(code, {}).get(d)
            if px is not None and px > 0:
                last_price[code] = px
            market_value += last_price.get(code, Decimal("0")) * q
        out[d] = {
            "trade_date": d,
            "total_value": cash + market_value,
            "cash": cash,
            "market_value": market_value,
            "trades_count": day_trades,
            "dirs": sorted(dirs),
        }
    return out


# ---------------------------------------------------------------- 续跑主流程
def _upsert_day(db: Session, session_id: int, ps_id: int, trade_date: date,
                **fields) -> IncrementalBacktestDay:
    row = (
        db.query(IncrementalBacktestDay)
        .filter_by(session_id=session_id, portfolio_strategy_id=ps_id,
                   trade_date=trade_date)
        .first()
    )
    if row is None:
        row = IncrementalBacktestDay(
            session_id=session_id, portfolio_strategy_id=ps_id,
            trade_date=trade_date)
        db.add(row)
    for k, v in fields.items():
        setattr(row, k, v)
    return row


def _inc_trades_by_day(trades: List, dates: List[date]) -> Dict[date, List[str]]:
    out: Dict[date, List[str]] = {d: [] for d in dates}
    for t in trades:
        d = t.trade_time.date()
        if d in out:
            out[d].append("%s:%s" % (t.stock_code, t.trade_type.value))
    for d in out:
        out[d].sort()
    return out


def run_incremental(db: Session, session_id: int, portfolio_strategy_id: int,
                    today: Optional[date] = None,
                    now: Optional[datetime] = None,
                    force_rebuild: bool = False) -> dict:
    """续跑一个组合策略到 ``today``（默认上海今天）。

    force_rebuild（K3，默认 False）：无视 checkpoint 有效性，强制丢弃旧 checkpoint
    并从接缝日重建（用户在工作台点「立即重建接缝」时使用，仍复用同一把锁）。
    """
    now = now or datetime.now(_TZ)
    today = today or now.date()
    cfg = _cfg()
    # 与自动备份互斥（2026-11-20 15:10 争用）：备份正在 VACUUM INTO 时不进入
    # 计算+写库，本轮直接让出，调度器 60s 后会重试；不阻塞、不丢任务。
    if not MAINTENANCE_GATE.acquire(timeout=GATE_WAIT_SECONDS):
        logger.info(
            "incremental backtest deferred (session %s, ps %s): 自动备份进行中",
            session_id, portfolio_strategy_id)
        return {"status": "deferred",
                "reason": "自动备份进行中，本轮跳过（调度器稍后重试）"}
    try:
        with _INCR_LOCK:
            return _run_incremental_locked(db, session_id, portfolio_strategy_id,
                                           today, cfg, force_rebuild=force_rebuild)
    finally:
        MAINTENANCE_GATE.release()


def _run_incremental_locked(db: Session, session_id: int,
                            ps_id: int, today: date, cfg: dict,
                            force_rebuild: bool = False) -> dict:
    if not cfg.get("enabled", True):
        return {"status": "disabled", "reason": "增量回测已关闭（enabled=false）"}
    link = (
        db.query(LiveSessionPortfolio)
        .filter_by(session_id=session_id, portfolio_strategy_id=ps_id)
        .first()
    )
    if link is None:
        return {"status": "skipped", "reason": "会话未关联该组合策略"}
    ps = db.query(PortfolioStrategy).filter_by(id=ps_id).first()
    if ps is None:
        return {"status": "error", "reason": "组合策略不存在"}

    periods = _strategy_periods(ps, db)
    # 分钟级策略走同一条续跑路径（引擎按周期路由 + 状态序列化与日线一致）。
    # intraday=True 时才启用「同一交易日内按游标续跑」；纯日线组合保持旧语义
    # （同一交易日重复触达一律「当日已续跑」，逐字不变）。
    intraday = any(p not in _DAILY_PERIODS for p in periods)
    strategies = (
        db.query(Strategy).filter_by(portfolio_id=ps.id).order_by(Strategy.id).all()
    )
    if any(getattr(s, "regime_filter_enabled", False) for s in strategies):
        return {
            "status": "unsupported",
            "reason": "市场状态闸（regime_filter_enabled）需要额外基准取数，本期不支持",
        }
    if link.seam_date is None:
        return {"status": "skipped",
                "reason": "尚未建立接缝日（该组合未在实盘启动过）"}
    seam_positions, seam_bar_time = _seam_payload(link.seam_positions_json)

    dividend_mode = cfg.get("dividend_mode", "dual")
    accounting_mode, signal_mode = _resolve_modes(dividend_mode)
    # 资金基准指纹（2026-11-16）：基准变更会让 checkpoint 作废重建，防旧基准覆盖。
    session_row = db.query(LiveSession).filter_by(id=session_id).first()
    baseline_fp = baseline_fingerprint(session_row)
    fp = params_fingerprint(ps, dividend_mode, db, baseline=baseline_fp)
    want_params_hash = fp["hash"]
    want_schema_fp = schema_fingerprint(dividend_mode)

    ckpt = (
        db.query(IncrementalBacktestCheckpoint)
        .filter_by(session_id=session_id, portfolio_strategy_id=ps_id)
        .first()
    )
    state = None
    invalidated: Optional[dict] = None
    #: 判废的旧 checkpoint 与事件文案推迟到写库阶段落库（见「判废落库」块）。
    stale_ckpt = None
    invalidation_message = ""
    if ckpt is not None:
        try:
            state = json.loads(ckpt.state_json)
        except (TypeError, ValueError):
            state = None
        if force_rebuild or not checkpoint_valid(
                ckpt.params_hash, (state or {}).get("schema_fp", ""),
                ckpt.schema_version, want_params_hash, want_schema_fp):
            if force_rebuild:
                reasons = ["用户手动重建接缝"]
            else:
                reasons = _invalidation_reasons(
                    state, (state or {}).get("schema_fp", ""), ckpt.schema_version,
                    want_schema_fp, fp["parts"], schema_parts(dividend_mode))
            invalidated = {"categories": reasons, "from_as_of": _dt(ckpt.as_of_date)}
            logger.warning(
                "[incremental] %s → checkpoint 作废并从接缝日重建 "
                "(session %s, ps %s, 旧 as_of=%s)：%s",
                "用户手动重建接缝" if force_rebuild else "参数/口径指纹变化",
                session_id, ps_id, ckpt.as_of_date, "；".join(reasons))
            # K3：判废即落决策事件（工作台红标 + 事后追溯，随重建事务一并提交；
            # 重建失败回滚时事件同步回滚，避免「未真正作废」的假告警）。
            #
            # 2026-11-20 15:10 争用修复：此处只记文案，事件与旧 checkpoint 删除
            # 都推迟到写库阶段（见下方「判废落库」块）。SQLite 是库级写锁，原实现
            # 在这里 flush 后锁会横跨整个回测计算（数秒到数十秒），与自动备份
            # 同分钟触发时，实盘快照写入 busy_timeout=5s 超时即报 database is locked。
            # 事务边界不变：事件/删除/新 checkpoint/逐日行仍在同一事务内提交。
            invalidation_message = ("；".join(reasons))[:200]
            stale_ckpt = ckpt
            ckpt = None
            state = None

    as_of = date.fromisoformat(state["as_of_date"]) if state else None
    as_of_bar = _pdt((state or {}).get("as_of_bar_time"))
    # 幂等：当日（或更晚）已续跑过就跳过。分钟级例外：``day_complete=False`` 表示
    # 上一轮只跑到盘中某根 bar（如 11:00 手工触发），必须接着补余下 bar，不能跳过。
    done_today = bool((state or {}).get("day_complete", True))
    if as_of is not None and as_of >= today:
        if (as_of > today or not intraday or done_today):
            return {"status": "ok", "as_of_date": as_of.isoformat(),
                    "days": 0, "note": "当日已续跑"}
    # 同一交易日内的游标续跑（只喂 datetime > as_of_bar 的 bar，不重放已处理 bar）。
    # 跨日续跑走整日边界（次一交易日 00:00 起），无需重放当天分钟 bar。
    continue_intraday = (
        state is not None and as_of is not None and as_of_bar is not None
        and as_of >= today and not done_today)

    handoff = take_daily_bars(session_id, today)
    fast_path = fast_path_allowed(
        as_of, today, periods, handoff, int(cfg.get("max_catchup_days", 5)))
    # 只有结束日志时，卡在取数/回测段无法定位（fast=False = 当日没有
    # DailyCloser 15:05 日线交接，本节自己向 TQ 取数）。
    logger.info(
        "incremental backtest start (session %s, ps %s): as_of=%s today=%s "
        "fast=%s", session_id, ps_id, as_of, today, fast_path)

    gap_dates: List[date] = []
    if fast_path:
        klines_all = _klines_from_handoff(handoff)
        run_from = today
    else:
        if state is None:
            range_start = link.seam_date
        elif continue_intraday:
            # 同一交易日续跑：从游标所在交易日重新取数，稍后按游标切掉已处理 bar
            range_start = as_of
        else:
            range_start = as_of + timedelta(days=1)
        klines_all = build_klines(ps, range_start, today, db,
                                  dividend_mode=accounting_mode)
        run_from = range_start
        # 交易日以行情实际日期为准；缺数据时用工作日兜底
        bar_days = sorted({
            t.date()
            for periods_map in klines_all.values()
            for df in periods_map.values()
            if "datetime" in df.columns
            for t in df["datetime"].to_list()
        })
        if state is None:
            # 首次从接缝日起跑：这是预期的初始构建，不是「补跑」，不标断档
            plan = {"dates": bar_days, "gap_dates": [],
                    "rebuild_from_seam": False}
        else:
            plan = plan_resume(
                as_of, today, int(cfg.get("max_catchup_days", 5)),
                trading_days=bar_days if bar_days else None,
            )
        gap_dates = plan["gap_dates"]
        if plan["rebuild_from_seam"]:
            # 超过补跑上限 → 标断档，并从接缝日全量重建，保证 checkpoint 之后可用。
            # 全量重建必须**丢弃旧 checkpoint 状态**（现金/持仓/在途单），只按接缝
            # 锚点重放；否则旧 as_of 状态叠加从接缝日重放的整段 bar = 每次成交算两遍。
            klines_all = build_klines(ps, link.seam_date, today, db,
                                      dividend_mode=accounting_mode)
            run_from = link.seam_date
            state = None

    if not klines_all:
        return {"status": "skipped", "reason": "无可用 K 线数据（今日 bar 未落盘？）"}

    klines = _slice_klines_from(klines_all, run_from)
    if state is None:
        # 首次构建（或断档全量重建）：接缝现金/持仓取自 engine.recover 之后，已含
        # 接缝日盘中成交；切掉游标之前的分钟 bar，防同一笔成交进两次（见
        # _trim_seam_day_intraday / _seam_trim_cursor）。日线及以上周期不受影响
        # （保留当日信号）。
        klines = _trim_seam_day_intraday(
            klines, _seam_trim_cursor(link, db, seam_bar_time))
    if continue_intraday and as_of_bar is not None:
        # 同一交易日续跑：游标之前的 bar 已经驱动过信号/风控/成交，绝不重放
        klines = _slice_klines_after(klines, as_of_bar)
        if not klines:
            return {"status": "ok", "as_of_date": as_of_bar.date().isoformat(),
                    "days": 0, "note": "游标之后无新增 bar"}
    if not klines:
        return {"status": "skipped", "reason": "续跑区间无 K 线"}

    signal_cache = build_signal_cache(
        ps, klines_all, db, start=run_from, dividend_mode=signal_mode)

    portfolio = assemble_portfolio(ps, strategies, db)
    # 公司行为（J1/J2，默认关）：与全量回测同一内核与同一张表，接缝前后同口径
    # 由 schema_fp 的 corporate_actions 版本 + params 分项保证。
    if getattr(ps, "corporate_actions_enabled", False):
        _ca_timeline = build_corporate_timeline(db, run_from, today)
        if len(_ca_timeline):
            portfolio.corporate_timeline = _ca_timeline
            portfolio.corporate_actions_enabled = True
            portfolio.dividend_reinvest_enabled = bool(
                getattr(ps, "dividend_reinvest_enabled", False))
            portfolio.suspension_dispose_days = int(
                getattr(ps, "suspension_dispose_days", 0) or 0)
            portfolio.delist_force_close_enabled = bool(
                getattr(ps, "delist_force_close_enabled", False))
    # 时点股票池（J7，默认关）：与全量回测同一张成员表与同一判定内核，
    # 接缝前后同口径由 schema_fp 的 pool_timeline 版本 + params 分项保证。
    if getattr(ps, "pool_pit_enabled", False):
        _pit_timeline = build_pool_timeline(db, ps, today)
        if len(_pit_timeline):
            portfolio.pool_timeline = _pit_timeline
            portfolio.pool_pit_enabled = True
    if state is None:
        if resolve_anchor(link, cfg):
            apply_seam_anchor(portfolio, link.seam_cash, seam_positions)
        pending: List[OrderEvent] = []
    else:
        pending = restore_portfolio(portfolio, state)

    # 市场多空门控（2026-11-18）：与全量回测同一口径——enabled=None 跟随组合
    # 配置；取数失败抛 MarketGateUnavailableError（调度器逐条 try/except 兜底，
    # 不会中断其余组合）。接缝前后同口径由 schema_fp 的 market_gate 版本保证。
    gate_tracker = build_gate_tracker(ps, run_from, today)

    result = BacktestEngine().run(
        portfolio, klines=klines, signal_cache=signal_cache,
        initial_pending_orders=pending,
        stock_names=_pool_stock_names(ps, db),
        # 涨跌停基准按「(代码, 交易日) 前收」显式传入：续跑首段从接缝日/游标起，
        # 引擎内部按本次 klines 现推会缺前收（首根 fail-open），用全量 klines_all
        # 推出的前收表保证与全量回测同一口径。
        prev_close_by_day=build_prev_close_by_day(klines_all),
        # 在途单最大存活期（交易日；缺键=20，<=0 不限）：与全量回测同一配置项，
        # 防止停牌/连板单跨段无限顺延。
        pending_order_max_days=int(cfg.get("pending_order_max_days", 20)),
        gate_tracker=gate_tracker)

    snaps = result.get("snapshots") or []
    if not snaps:
        return {"status": "skipped", "reason": "本次区间无快照"}

    new_dates = sorted({s["snap_date"] for s in snaps})
    as_of_new = new_dates[-1]
    # 接缝粒度 = 交易日 + 日内游标：游标取本段最后处理的那根 bar 时间戳。
    # day_complete 判定「该交易日是否已收到收盘 bar」：分钟级盘中续跑（如 11:00）
    # 只到 11:00 → False，同一天再触发会从游标接着往后跑；纯日线组合恒 True
    # （日线只有一根 bar，与改动前「当日已续跑」语义逐字一致）。
    last_bar = _max_bar_time(klines)
    day_complete = (not intraday) or bool(
        last_bar is not None and last_bar.date() >= today
        and last_bar.time() >= _DAY_LAST_BAR_TIME)
    payload = state_payload(portfolio, as_of_new,
                            pending_orders=result.get("pending_orders"),
                            as_of_bar_time=last_bar,
                            day_complete=day_complete)
    payload["schema_fp"] = want_schema_fp
    # 口径分项随 checkpoint 落盘：下次作废时据此定位「引擎版本/复权模式/成本口径」
    # 具体哪一项变了（旧 checkpoint 无该字段 → 日志退化为笼统原因）。
    payload["schema_parts"] = schema_parts(dividend_mode)
    # 分项指纹随 checkpoint 落盘：下次作废时据此定位「哪一类变化」（旧 checkpoint
    # 无该字段 → 日志标注按参数变化处理）。
    payload["params_fp_parts"] = fp["parts"]
    # 「判废落库」：计算全部完成后才真正写库，写锁只覆盖本段与末尾 commit。
    # 与判废事件、新 checkpoint、逐日行同一事务；中途失败仍整体回滚（旧 checkpoint
    # 原样保留、事件不落库），与推迟前的语义一致。
    if stale_ckpt is not None:
        db.add(LiveDecisionEvent(
            live_session_id=session_id,
            gate="incremental_checkpoint",
            layer="live_gate",
            action="invalidated",
            param_name="portfolio_strategy_id",
            actual_value=float(ps_id),
            message=invalidation_message,
        ))
        # SQLAlchemy 同 flush 内 INSERT 先于 DELETE，这里先 flush DELETE，
        # 避免新 checkpoint 的 (session, ps) 唯一键与旧行相撞。
        db.delete(stale_ckpt)
        db.flush()
    if ckpt is None:
        ckpt = IncrementalBacktestCheckpoint(
            session_id=session_id, portfolio_strategy_id=ps_id,
            seam_date=link.seam_date)
        db.add(ckpt)
    ckpt.seam_date = link.seam_date
    ckpt.as_of_date = as_of_new
    ckpt.state_json = json.dumps(payload, ensure_ascii=False)
    ckpt.params_hash = want_params_hash
    ckpt.schema_version = SCHEMA_VERSION

    closes = _close_map(klines_all)
    live = _live_rows(db, link, new_dates, closes)
    inc_dirs = _inc_trades_by_day(result.get("trades") or [], new_dates)
    tol = _tolerances(cfg)
    # 同日游标续跑：当日已有「游标之前」的成交方向记录，本段只含游标之后的成交，
    # 直接覆盖会让当日笔数偏少（与实盘侧对不上）。这里与既有行合并成整日口径。
    prev_days: Dict[date, IncrementalBacktestDay] = {}
    if continue_intraday:
        for row in db.query(IncrementalBacktestDay).filter(
                IncrementalBacktestDay.session_id == session_id,
                IncrementalBacktestDay.portfolio_strategy_id == ps_id,
                IncrementalBacktestDay.trade_date.in_(new_dates)).all():
            prev_days[row.trade_date] = row

    for snap in snaps:
        d = snap["snap_date"]
        if d not in new_dates:
            continue
        inc_total = Decimal(str(snap["total_value"]))
        lv = live.get(d, {})
        live_total = Decimal(str(lv.get("total_value", 0)))
        denom = max(abs(live_total), Decimal("1"))
        dev = float(abs(inc_total - live_total) / denom) * 100.0
        inc_d = inc_dirs.get(d, [])
        if d in prev_days:
            try:
                inc_d = list(json.loads(prev_days[d].inc_dirs_json or "[]")) + inc_d
            except (TypeError, ValueError):
                pass
        live_d = lv.get("dirs", [])
        direction_match = inc_d == live_d
        tags: List[str] = []
        if d in gap_dates:
            tags.append("gap")
        if not day_complete and d >= today:
            # 当日收盘 bar 未到：这一行是盘中口径（当日后续续跑会覆盖）
            tags.append("intraday")
        if dev > tol["max_net_dev_pct"]:
            tags.append("net_dev")
        if len(inc_d) != len(live_d):
            tags.append("trade_mismatch")
        if not direction_match:
            tags.append("direction_mismatch")
        status = "gap" if "gap" in tags else ("warn" if tags else "ok")
        _upsert_day(
            db, session_id, ps_id, d,
            inc_total_value=inc_total,
            inc_cash=snap.get("cash"),
            inc_market_value=snap.get("market_value"),
            live_total_value=live_total,
            live_cash=lv.get("cash"),
            live_market_value=lv.get("market_value"),
            net_dev_pct=round(dev, 4),
            inc_trades_count=len(inc_d),
            live_trades_count=len(live_d),
            inc_dirs_json=json.dumps(inc_d, ensure_ascii=False),
            direction_match=direction_match,
            status=status,
            tags_json=json.dumps(tags, ensure_ascii=False),
        )
    db.commit()

    logger.info(
        "incremental backtest done (session %s, ps %s): %s → %s, %d day(s), "
        "gap=%d, fast=%s",
        session_id, ps_id, new_dates[0], as_of_new, len(new_dates),
        len(gap_dates), fast_path)
    return {
        "status": "ok",
        "as_of_date": as_of_new.isoformat(),
        "days": len(new_dates),
        "gap_days": len(gap_dates),
        "fast_path": fast_path,
        "checkpoint_invalidated": invalidated,
    }


# ---------------------------------------------------------------- 双跑校验
def _apply_validation_marks(day_rows: List[IncrementalBacktestDay],
                            status: str,
                            first_div: Optional[date]) -> None:
    """把本轮校验结论写回逐日行。

    先清掉上一轮的 ``validation_fail`` 标记再按本轮结论重标——旧实现只加不删，
    一旦某次校验失败，这些行会永久标红，后续通过也消不掉（误导排查）。
    行状态按 tags 重算（gap 优先，其次有任意 tag 即 warn）。
    """
    if status not in ("pass", "fail"):
        return
    for r in day_rows:
        try:
            tags = json.loads(r.tags_json or "[]")
        except (TypeError, ValueError):
            tags = []
        tags = [t for t in tags if t != "validation_fail"]
        if status == "fail" and first_div is not None and r.trade_date >= first_div:
            tags.append("validation_fail")
        r.tags_json = json.dumps(tags, ensure_ascii=False)
        r.status = "gap" if "gap" in tags else ("warn" if tags else "ok")


def run_validation(db: Session, session_id: int, ps_id: int,
                   from_date: Optional[date] = None,
                   to_date: Optional[date] = None,
                   now: Optional[datetime] = None) -> dict:
    """从接缝日（或 from_date）全量重算，与续跑结果按特征级容差对账。"""
    to_date = to_date or datetime.now(_TZ).date()
    cfg = _cfg()
    with _INCR_LOCK:
        link = (
            db.query(LiveSessionPortfolio)
            .filter_by(session_id=session_id, portfolio_strategy_id=ps_id)
            .first()
        )
        if link is None or link.seam_date is None:
            return {"status": "error", "reason": "尚未建立接缝日"}
        ps = db.query(PortfolioStrategy).filter_by(id=ps_id).first()
        if ps is None:
            return {"status": "error", "reason": "组合策略不存在"}
        from_date = from_date or link.seam_date
        strategies = (
            db.query(Strategy).filter_by(portfolio_id=ps.id)
            .order_by(Strategy.id).all()
        )
        dividend_mode = cfg.get("dividend_mode", "dual")
        accounting_mode, signal_mode = _resolve_modes(dividend_mode)
        try:
            seam_positions, seam_bar_time = _seam_payload(link.seam_positions_json)
            klines_all = build_klines(ps, from_date, to_date, db,
                                      dividend_mode=accounting_mode)
            klines = _slice_klines_from(klines_all, from_date)
            # 与续跑首段同口径：从接缝日全量重算时，接缝日游标之前的分钟 bar 已计入
            # 接缝基线（seam_cash/seam_positions），必须一并切掉，否则对账两侧基线
            # 不同源（一侧含盘中成交、一侧重复成交），永远对不上。
            klines = _trim_seam_day_intraday(
                klines, _seam_trim_cursor(link, db, seam_bar_time))
            if not klines:
                return {"status": "skipped",
                        "reason": "接缝游标已覆盖区间内全部 K 线，无需校验"}
            signal_cache = build_signal_cache(
                ps, klines_all, db, start=from_date,
                dividend_mode=signal_mode)
            portfolio = assemble_portfolio(ps, strategies, db)
            # 校验侧与续跑侧同口径（公司行为，J1/J2）：否则「全量 vs 续跑」对比恒不一致
            if getattr(ps, "corporate_actions_enabled", False):
                _ca_timeline = build_corporate_timeline(db, from_date, to_date)
                if len(_ca_timeline):
                    portfolio.corporate_timeline = _ca_timeline
                    portfolio.corporate_actions_enabled = True
                    portfolio.dividend_reinvest_enabled = bool(
                        getattr(ps, "dividend_reinvest_enabled", False))
                    portfolio.suspension_dispose_days = int(
                        getattr(ps, "suspension_dispose_days", 0) or 0)
                    portfolio.delist_force_close_enabled = bool(
                        getattr(ps, "delist_force_close_enabled", False))
            # 时点股票池（J7）：校验侧与续跑侧同口径
            if getattr(ps, "pool_pit_enabled", False):
                _pit_timeline = build_pool_timeline(db, ps, to_date)
                if len(_pit_timeline):
                    portfolio.pool_timeline = _pit_timeline
                    portfolio.pool_pit_enabled = True
            if resolve_anchor(link, cfg):
                apply_seam_anchor(portfolio, link.seam_cash, seam_positions)
            # 校验侧与续跑侧必须同口径门控（否则「全量 vs 续跑」对比恒不一致）
            gate_tracker = build_gate_tracker(ps, from_date, to_date)
            result = BacktestEngine().run(
                portfolio, klines=klines, signal_cache=signal_cache,
                stock_names=_pool_stock_names(ps, db),
                prev_close_by_day=build_prev_close_by_day(klines_all),
                pending_order_max_days=int(cfg.get("pending_order_max_days", 20)),
                gate_tracker=gate_tracker)
        except Exception as exc:  # noqa: BLE001 — 校验失败要留痕，不吞
            logger.exception("incremental validation failed")
            row = IncrementalBacktestValidation(
                session_id=session_id, portfolio_strategy_id=ps_id,
                from_date=from_date, to_date=to_date, status="error",
                detail_json=json.dumps({"error": str(exc)}, ensure_ascii=False),
            )
            db.add(row)
            db.commit()
            return {"status": "error", "reason": str(exc)}

        full_trades = result.get("trades") or []
        full_dates = [s["snap_date"] for s in (result.get("snapshots") or [])]
        # 全量侧的方向列表必须由它自身的成交生成（旧实现硬编码 True = 该判据恒真）。
        full_dirs = _inc_trades_by_day(full_trades, full_dates)
        full_rows: Dict[date, dict] = {}
        for snap in result.get("snapshots") or []:
            d = snap["snap_date"]
            dirs = full_dirs.get(d, [])
            full_rows[d] = {
                "trade_date": d,
                "total_value": snap["total_value"],
                "trades_count": len(dirs),
                "dirs": dirs,
            }

        day_rows = (
            db.query(IncrementalBacktestDay)
            .filter(
                IncrementalBacktestDay.session_id == session_id,
                IncrementalBacktestDay.portfolio_strategy_id == ps_id,
                IncrementalBacktestDay.trade_date >= from_date,
                IncrementalBacktestDay.trade_date <= to_date,
            )
            .all()
        )
        inc_rows: Dict[date, dict] = {}
        for r in day_rows:
            inc_rows[r.trade_date] = {
                "trade_date": r.trade_date,
                "total_value": r.inc_total_value,
                "trades_count": r.inc_trades_count,
                # 旧行没有方向明细（NULL）→ 该判据不参与判定，而不是恒真通过
                "dirs": (json.loads(r.inc_dirs_json)
                         if r.inc_dirs_json else None),
            }

        tol = _tolerances(cfg)
        metrics = evaluate_alignment(inc_rows, full_rows, tol)
        criteria = {
            "tolerances": tol,
            "basis": {
                "max_net_dev_pct": "日线收盘取整到分 + 费用/过户费舍入 + T+1 分桶边界的日级噪声上界远小于 0.5%",
                "mean_net_dev_pct": "随机噪声不应系统性累积，均值显著小于单日上界",
                "trade_count_match_ratio": "允许极少数边界单（停牌/涨跌停顺延）跨日漂移",
                "direction_match_ratio": "方向错配比笔数错配更严重，但边界单仍可能造成 1 日差异",
                "max_drawdown_dev_pct": "回撤是路径极值，对单日噪声更敏感，放宽到 1 个百分点",
                "small_sample": (
                    "成交笔数（两侧之和）< %d 时笔数/方向一致率判 None 不参与结论："
                    "个位数样本上单个边界单即可击穿 0.9 容差，属噪声误判" %
                    SMALL_SAMPLE_MIN_TRADES),
            },
        }
        row = IncrementalBacktestValidation(
            session_id=session_id, portfolio_strategy_id=ps_id,
            from_date=from_date, to_date=to_date,
            status=metrics["status"],
            max_net_dev_pct=metrics["max_net_dev_pct"],
            mean_net_dev_pct=metrics["mean_net_dev_pct"],
            trade_count_match_ratio=metrics["trade_count_match_ratio"],
            direction_match_ratio=metrics["direction_match_ratio"],
            max_drawdown_dev_pct=metrics["max_drawdown_dev_pct"],
            first_divergence_date=metrics["first_divergence_date"],
            criteria_json=json.dumps(criteria, ensure_ascii=False),
            detail_json=json.dumps(
                {"dates_count": metrics["dates_count"],
                 "small_sample": metrics.get("small_sample"),
                 "trades_seen": metrics.get("trades_seen")},
                ensure_ascii=False),
        )
        db.add(row)
        _apply_validation_marks(
            day_rows, metrics["status"], metrics["first_divergence_date"])
        db.commit()
        return {
            "status": metrics["status"],
            **{k: v for k, v in metrics.items() if k != "status"},
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
        }


# ---------------------------------------------------------------- 查询汇总
def _checkpoint_summary(ckpt) -> dict:
    """checkpoint 的可读摘要（含接缝粒度：交易日 + 日内游标）。"""
    try:
        state = json.loads(ckpt.state_json or "{}") or {}
    except (TypeError, ValueError):
        state = {}
    return {
        "seam_date": ckpt.seam_date.isoformat(),
        "as_of_date": ckpt.as_of_date.isoformat(),
        # 分钟级盘中续跑后能看出跑到哪一根 bar（None = 旧 checkpoint 无游标）
        "as_of_bar_time": state.get("as_of_bar_time"),
        "day_complete": bool(state.get("day_complete", True)),
        "params_hash": ckpt.params_hash,
        "schema_version": ckpt.schema_version,
        "updated_at": _dt(ckpt.updated_at),
    }


def summary(db: Session, session_id: int, ps_id: int) -> dict:
    ps = db.query(PortfolioStrategy).filter_by(id=ps_id).first()
    # 分钟级与日线同一条续跑路径（接缝粒度 = 交易日 + 日内游标），故只看组合是否存在。
    supported = ps is not None
    reason = "" if ps is not None else "组合策略不存在"

    link = (
        db.query(LiveSessionPortfolio)
        .filter_by(session_id=session_id, portfolio_strategy_id=ps_id)
        .first()
    )
    ckpt = (
        db.query(IncrementalBacktestCheckpoint)
        .filter_by(session_id=session_id, portfolio_strategy_id=ps_id)
        .first()
    )
    validation = (
        db.query(IncrementalBacktestValidation)
        .filter_by(session_id=session_id, portfolio_strategy_id=ps_id)
        .order_by(IncrementalBacktestValidation.id.desc())
        .first()
    )
    days = (
        db.query(IncrementalBacktestDay)
        .filter_by(session_id=session_id, portfolio_strategy_id=ps_id)
        .order_by(IncrementalBacktestDay.trade_date.desc())
        .limit(400)
        .all()
    )
    # K3：最近一次接缝判废（工作台红标 + 一键重建入口）
    last_inv = (
        db.query(LiveDecisionEvent)
        .filter_by(
            live_session_id=session_id,
            gate="incremental_checkpoint",
            action="invalidated",
            param_name="portfolio_strategy_id",
            actual_value=float(ps_id),
        )
        .order_by(LiveDecisionEvent.id.desc())
        .first()
    )

    return {
        "supported": supported,
        "reason": reason,
        "seam_date": link.seam_date.isoformat() if link and link.seam_date else None,
        "seam_cash": str(link.seam_cash) if link and link.seam_cash is not None else None,
        "checkpoint": None if ckpt is None else _checkpoint_summary(ckpt),
        "last_invalidation": None if last_inv is None else {
            "at": _dt(last_inv.created_at) or _dt(last_inv.bar_time),
            "message": last_inv.message or "",
        },
        "days": [{
            "trade_date": r.trade_date.isoformat(),
            "inc_total_value": str(r.inc_total_value) if r.inc_total_value is not None else None,
            "inc_cash": str(r.inc_cash) if r.inc_cash is not None else None,
            "inc_market_value": str(r.inc_market_value) if r.inc_market_value is not None else None,
            "live_total_value": str(r.live_total_value) if r.live_total_value is not None else None,
            "live_cash": str(r.live_cash) if r.live_cash is not None else None,
            "live_market_value": str(r.live_market_value) if r.live_market_value is not None else None,
            "net_dev_pct": float(r.net_dev_pct) if r.net_dev_pct is not None else None,
            "inc_trades_count": r.inc_trades_count,
            "live_trades_count": r.live_trades_count,
            "direction_match": bool(r.direction_match) if r.direction_match is not None else None,
            "status": r.status,
            "tags": json.loads(r.tags_json or "[]"),
        } for r in reversed(days)],
        "latest_validation": None if validation is None else {
            "id": validation.id,
            "from_date": validation.from_date.isoformat(),
            "to_date": validation.to_date.isoformat(),
            "status": validation.status,
            "max_net_dev_pct": float(validation.max_net_dev_pct) if validation.max_net_dev_pct is not None else None,
            "mean_net_dev_pct": float(validation.mean_net_dev_pct) if validation.mean_net_dev_pct is not None else None,
            "trade_count_match_ratio": float(validation.trade_count_match_ratio) if validation.trade_count_match_ratio is not None else None,
            "direction_match_ratio": float(validation.direction_match_ratio) if validation.direction_match_ratio is not None else None,
            "max_drawdown_dev_pct": float(validation.max_drawdown_dev_pct) if validation.max_drawdown_dev_pct is not None else None,
            "first_divergence_date": (
                validation.first_divergence_date.isoformat()
                if validation.first_divergence_date else None),
            "criteria": json.loads(validation.criteria_json or "{}"),
            "created_at": _dt(validation.created_at),
        },
        "config": {
            "enabled": bool(_cfg().get("enabled", True)),
            "daily_at": _cfg().get("daily_at", "15:10"),
            "max_catchup_days": _cfg().get("max_catchup_days", 5),
            "tolerances": _tolerances(),
        },
    }


# ---------------------------------------------------------------- 调度器
def _parse_hm(text: str, default: str) -> tuple:
    raw = text or default
    try:
        h, m = raw.split(":")
        return int(h), int(m)
    except (ValueError, AttributeError):
        h, m = default.split(":")
        return int(h), int(m)


def _record_scheduler_failure(db: Session, session_id: int, ps_id: int,
                              gate: str, exc: BaseException) -> None:
    """调度异常落 ``live_decision_events``（审计可见，失败只告警）。

    调用前调用方必须已 ``rollback``。写入失败不得再抛，避免掩盖原始异常。
    """
    try:
        db.add(LiveDecisionEvent(
            live_session_id=session_id,
            gate=gate,
            layer="live_gate",
            action="error",
            param_name="portfolio_strategy_id",
            actual_value=float(ps_id),
            message=(f"{type(exc).__name__}: {exc}")[:200] or None,
        ))
        db.commit()
    except Exception:  # noqa: BLE001 — 观测失败不得阻断调度
        logger.warning("写入调度失败事件失败（gate=%s, ps=%s）", gate, ps_id,
                       exc_info=True)
        try:
            db.rollback()
        except Exception:  # noqa: BLE001
            pass


class IncrementalBacktestScheduler:
    """进程内每日续跑 + 每周双跑校验（daemon 线程，60s 轮询）。

    每日 ``daily_at``（默认 15:10，晚于 DailyCloser 15:05 的日线取数）后对
    所有已建立接缝的（会话, 组合策略）执行续跑；当日 checkpoint 已到今天就跳过
    （幂等）。每周 ``validate_weekday``（默认 5=周六）``validate_at`` 后跑一次
    全量重算对账。
    """

    def __init__(self, *, cfg_provider=None, session_factory=None,
                 now_fn=None, interval_seconds: float = 60.0):
        self._cfg = cfg_provider or _cfg
        self._session_factory = session_factory or SessionLocal
        self._now = now_fn or (lambda: datetime.now(_TZ))
        self._interval = max(float(interval_seconds), 5.0)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_validation_date: Optional[date] = None
        # 失败事件去重：同一 (会话, 组合, 闸门, 日期) 只落一行（调度 60s 一轮重试，
        # 不节流会把 live_decision_events 刷屏）
        self._notified_failures: set = set()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="incremental-backtest-scheduler",
                daemon=True)
            self._thread.start()

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=timeout)
        self._thread = None

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.maybe_run()
            except Exception:  # noqa: BLE001 — 调度失败不得拖垮进程
                logger.exception("增量回测调度异常（下轮重试）")
            if self._stop.wait(self._interval):
                break

    def maybe_run(self) -> Optional[dict]:
        cfg = self._cfg()
        if not cfg.get("enabled", True):
            return None
        now = self._now()
        today = now.date()
        daily_hm = _parse_hm(cfg.get("daily_at", "15:10"), "15:10")
        if (now.hour, now.minute) < daily_hm:
            return None

        with self._session_factory() as db:
            links = (
                db.query(LiveSessionPortfolio)
                .filter(LiveSessionPortfolio.seam_date.isnot(None))
                .all()
            )
            for link in links:
                ckpt = (
                    db.query(IncrementalBacktestCheckpoint)
                    .filter_by(
                        session_id=link.session_id,
                        portfolio_strategy_id=link.portfolio_strategy_id)
                    .first()
                )
                if _checkpoint_done(ckpt, today):
                    continue
                try:
                    run_incremental(
                        db, link.session_id, link.portfolio_strategy_id,
                        today=today, now=now)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "增量回测失败 (session %s, ps %s)",
                        link.session_id, link.portfolio_strategy_id)
                    db.rollback()
                    key = (link.session_id, link.portfolio_strategy_id,
                           "incremental_backtest", today)
                    if key not in self._notified_failures:
                        self._notified_failures.add(key)
                        _record_scheduler_failure(
                            db, link.session_id, link.portfolio_strategy_id,
                            "incremental_backtest", exc)
                        # 告警外发（旁路）：增量回测调度失败（同键每日只发一条）
                        notify(
                            "warn", "增量回测调度失败",
                            "会话 %s 组合 %s 增量续跑失败：%s: %s"
                            % (link.session_id, link.portfolio_strategy_id,
                               type(exc).__name__, exc),
                            gate="incremental_backtest", code=str(link.portfolio_strategy_id),
                            trade_date=today,
                            fields={"会话": link.session_id,
                                    "组合": link.portfolio_strategy_id},
                        )

            # 每周双跑校验
            try:
                weekday = int(cfg.get("validate_weekday", 5))
            except (TypeError, ValueError):
                weekday = 5
            validate_hm = _parse_hm(cfg.get("validate_at", "18:00"), "18:00")
            if (now.weekday() == weekday
                    and (now.hour, now.minute) >= validate_hm
                    and self._last_validation_date != today):
                for link in links:
                    try:
                        run_validation(
                            db, link.session_id, link.portfolio_strategy_id,
                            to_date=today, now=now)
                    except Exception as exc:  # noqa: BLE001
                        logger.exception(
                            "增量回测双跑校验失败 (session %s, ps %s)",
                            link.session_id, link.portfolio_strategy_id)
                        db.rollback()
                        key = (link.session_id, link.portfolio_strategy_id,
                               "incremental_validation", today)
                        if key not in self._notified_failures:
                            self._notified_failures.add(key)
                            _record_scheduler_failure(
                                db, link.session_id,
                                link.portfolio_strategy_id,
                                "incremental_validation", exc)
                            # 告警外发（旁路）：每周双跑校验失败（同键每日一条）
                            notify(
                                "warn", "增量回测双跑校验失败",
                                "会话 %s 组合 %s 双跑校验失败：%s: %s"
                                % (link.session_id, link.portfolio_strategy_id,
                                   type(exc).__name__, exc),
                                gate="incremental_validation",
                                code=str(link.portfolio_strategy_id),
                                trade_date=today,
                                fields={"会话": link.session_id,
                                        "组合": link.portfolio_strategy_id},
                            )
                self._last_validation_date = today
        return {"ran_at": now.isoformat()}
