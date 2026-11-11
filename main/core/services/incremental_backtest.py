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
from sqlalchemy.orm import Session

from core.config import load_config
from core.db import SessionLocal
from core.engine.backtest_engine import BacktestEngine
from core.engine.bar_poller import parse_bar_time, to_ohlcv
from core.engine.event import OrderEvent
from core.engine.portfolio import Portfolio
from core.engine.portfolio_builder import assemble_portfolio
from core.engine.position import Position
from core.models import (
    IncrementalBacktestCheckpoint,
    IncrementalBacktestDay,
    IncrementalBacktestValidation,
    LiveSessionPortfolio,
    LiveTrade,
    PortfolioStrategy,
    Strategy,
)
from core.services.backtest_service import (
    _pool_stock_names,
    _slice_klines_from,
    _strategy_periods,
    build_klines,
    build_params_snapshot,
    build_signal_cache,
)
from tq_iquant_shared.constants import SignalType, TradeType

logger = logging.getLogger(__name__)

_TZ = ZoneInfo("Asia/Shanghai")

SCHEMA_VERSION = 1
# 口径版本（任一语义变化 → 提升 SCHEMA_VERSION 并在注释里写明原因）
_COST_SCHEMA = "fee_inclusive_v2"    # 成本含佣金/印花税/过户费
_EXEC_SCHEMA = "next_bar_open_v1"    # 成交价 = 下一根 bar 开盘价
_T1_SCHEMA = "t1_forced_v1"          # 回测强制 T+1
_ENGINE_SCHEMA = "backtest_engine_v1"

# 仅这些周期可续跑；分钟级续跑语义未定（跨日分段时在途分钟 bar 与
# 成交时点边界未规范化），明确不支持而不是勉强实现。
_DAILY_PERIODS = {"1d", "1w", "1mon"}

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
    )


def state_payload(portfolio: Portfolio, as_of_date: date,
                  pending_orders: Optional[List[OrderEvent]] = None) -> dict:
    """把 Portfolio 完整内存态序列化为 JSON 安全 dict（漏项=静默错账）。"""
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
        "pending_orders": [_order_to_dict(o) for o in (pending_orders or [])],
        "processed_bar_date": as_of_date.isoformat(),
    }


def restore_portfolio(portfolio: Portfolio, payload: dict) -> List[OrderEvent]:
    """把 checkpoint 内存态灌回 Portfolio；返回待继续处理的在途单。"""
    portfolio.account.cash = _dec(payload.get("cash"))
    portfolio.account.initial_capital = _dec(
        payload.get("initial_capital"), str(portfolio.account.initial_capital))

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


def schema_fingerprint(dividend_mode: str) -> str:
    payload = json.dumps({
        "schema": SCHEMA_VERSION,
        "cost": _COST_SCHEMA,
        "exec": _EXEC_SCHEMA,
        "t1": _T1_SCHEMA,
        "engine": _ENGINE_SCHEMA,
        "dividend_mode": dividend_mode,
    }, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def params_hash(ps, dividend_mode: str) -> str:
    snapshot = build_params_snapshot(ps, dividend_mode=dividend_mode)
    return hashlib.sha256(snapshot.encode("utf-8")).hexdigest()


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


# ---------------------------------------------------------------- 接缝锚定
def capture_seam(db: Session, session_id: int, engine, today: Optional[date] = None) -> None:
    """实盘启动（recover 之后）写入接缝日与起账状态；已写定则不覆盖。"""
    today = today or datetime.now(_TZ).date()
    links = (
        db.query(LiveSessionPortfolio)
        .filter(LiveSessionPortfolio.session_id == session_id)
        .all()
    )
    ports = {p.portfolio_id: p for p in (getattr(engine, "portfolios", None) or [])}
    changed = False
    for link in links:
        if link.seam_date is not None:
            continue
        port = ports.get(link.portfolio_strategy_id)
        if port is None:
            continue
        positions = []
        for ctx in port.strategies:
            for code, pos in ctx.positions.items():
                if not pos.quantity or pos.quantity <= 0:
                    continue
                positions.append({
                    "strategy_id": ctx.strategy_id,
                    "stock_code": code,
                    "quantity": int(pos.quantity),
                    "avg_cost": str(pos.avg_cost),
                    "buy_date": _dt(pos.buy_time.date() if pos.buy_time else None),
                    # 起账时 highest_price 按成本种下（无持仓期价格序列可依）
                    "highest_price": str(pos.highest_price or pos.avg_cost),
                })
        link.seam_date = today
        link.seam_cash = port.account.cash
        link.seam_positions_json = json.dumps(positions, ensure_ascii=False)
        changed = True
    if changed:
        db.commit()


def resolve_anchor(link: LiveSessionPortfolio, cfg: Optional[dict] = None) -> bool:
    """接缝锚定开关三态解析：NULL=跟随全局（默认 True）/ 0 / 1。"""
    if link.seam_anchor_enabled is not None:
        return bool(link.seam_anchor_enabled)
    return bool((cfg or _cfg()).get("anchor_on_live_start", True))


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
      （停牌/涨跌停顺延）跨日漂移。
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
    first_div: Optional[date] = None
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
        same_count = ta == tb
        if same_count:
            count_match += 1
        same_dir = bool(a.get("direction_match", True)) and bool(
            b.get("direction_match", True))
        if same_dir:
            direction_match += 1
        if first_div is None and (dev > tolerances["max_net_dev_pct"]
                                  or not same_count or not same_dir):
            first_div = d

    n = len(dates)
    max_dev = max(devs)
    mean_dev = sum(devs) / n
    count_ratio = count_match / n
    dir_ratio = direction_match / n
    dd_dev = abs(_max_drawdown_pct(inc_series) - _max_drawdown_pct(full_series))

    ok = (
        max_dev <= tolerances["max_net_dev_pct"]
        and mean_dev <= tolerances["mean_net_dev_pct"]
        and count_ratio >= tolerances["trade_count_match_ratio"]
        and dir_ratio >= tolerances["direction_match_ratio"]
        and dd_dev <= tolerances["max_drawdown_dev_pct"]
    )
    return {
        "status": "pass" if ok else "fail",
        "max_net_dev_pct": round(max_dev, 4),
        "mean_net_dev_pct": round(mean_dev, 4),
        "trade_count_match_ratio": round(count_ratio, 4),
        "direction_match_ratio": round(dir_ratio, 4),
        "max_drawdown_dev_pct": round(dd_dev, 4),
        "first_divergence_date": first_div,
        "dates_count": n,
    }


# ---------------------------------------------------------------- 实盘侧重放
def _close_map(klines: dict) -> Dict[str, Dict[date, Decimal]]:
    out: Dict[str, Dict[date, Decimal]] = {}
    for code, periods in klines.items():
        df = periods.get("1d")
        if df is None or "datetime" not in df.columns:
            continue
        m: Dict[date, Decimal] = {}
        for t, c in zip(df["datetime"].to_list(), df["Close"].to_list()):
            if c is not None and c > 0:
                m[t.date()] = Decimal(str(c))
        out[code] = m
    return out


def _live_rows(db: Session, link: LiveSessionPortfolio, dates: List[date],
               closes: Dict[str, Dict[date, Decimal]]) -> Dict[date, dict]:
    """实盘侧逐日净值：接缝基线 + live_trades 重放，按该日收盘价估值。"""
    base = json.loads(link.seam_positions_json or "[]")
    qty: Dict[str, Decimal] = {}
    last_price: Dict[str, Decimal] = {}
    for item in base:
        code = item.get("stock_code")
        q = Decimal(str(item.get("quantity") or 0))
        qty[code] = q
        last_price[code] = Decimal(str(item.get("avg_cost") or 0))
    cash = Decimal(str(link.seam_cash or 0))

    trades = (
        db.query(LiveTrade)
        .filter(
            LiveTrade.live_session_id == link.session_id,
            LiveTrade.portfolio_strategy_id == link.portfolio_strategy_id,
            LiveTrade.trade_time >= datetime.combine(
                link.seam_date, time(0, 0)),
        )
        .order_by(LiveTrade.trade_time, LiveTrade.id)
        .all()
    )
    ptr = 0
    out: Dict[date, dict] = {}
    for d in dates:
        day_trades = 0
        dirs: List[str] = []
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
                    now: Optional[datetime] = None) -> dict:
    """续跑一个组合策略到 ``today``（默认上海今天）。"""
    today = today or datetime.now(_TZ).date()
    cfg = _cfg()
    with _INCR_LOCK:
        return _run_incremental_locked(db, session_id, portfolio_strategy_id,
                                       today, cfg)


def _run_incremental_locked(db: Session, session_id: int,
                            ps_id: int, today: date, cfg: dict) -> dict:
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
    unsupported = [p for p in periods if p not in _DAILY_PERIODS]
    if unsupported:
        return {
            "status": "unsupported",
            "reason": "分钟级策略（%s）续跑语义未定，本期不支持"
                      % "/".join(unsupported),
        }
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

    dividend_mode = cfg.get("dividend_mode", "dual")
    accounting_mode, signal_mode = _resolve_modes(dividend_mode)
    want_params_hash = params_hash(ps, dividend_mode)
    want_schema_fp = schema_fingerprint(dividend_mode)

    ckpt = (
        db.query(IncrementalBacktestCheckpoint)
        .filter_by(session_id=session_id, portfolio_strategy_id=ps_id)
        .first()
    )
    state = None
    if ckpt is not None:
        try:
            state = json.loads(ckpt.state_json)
        except (TypeError, ValueError):
            state = None
        if not checkpoint_valid(
                ckpt.params_hash, (state or {}).get("schema_fp", ""),
                ckpt.schema_version, want_params_hash, want_schema_fp):
            db.delete(ckpt)
            db.commit()
            ckpt = None
            state = None

    as_of = date.fromisoformat(state["as_of_date"]) if state else None
    # 幂等：当日已续跑过就跳过
    if as_of is not None and as_of >= today:
        return {"status": "ok", "as_of_date": as_of.isoformat(),
                "days": 0, "note": "当日已续跑"}

    handoff = take_daily_bars(session_id, today)
    fast_path = (
        state is not None and handoff is not None and as_of is not None
        and as_of < today and set(periods) == {"1d"}
    )

    gap_dates: List[date] = []
    if fast_path:
        klines_all = _klines_from_handoff(handoff)
        run_from = today
    else:
        range_start = link.seam_date if state is None else as_of + timedelta(days=1)
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
            # 超过补跑上限 → 标断档，并从接缝日全量重建，保证 checkpoint 之后可用
            klines_all = build_klines(ps, link.seam_date, today, db,
                                      dividend_mode=accounting_mode)
            run_from = link.seam_date

    if not klines_all:
        return {"status": "skipped", "reason": "无可用日线数据（今日 bar 未落盘？）"}

    klines = _slice_klines_from(klines_all, run_from)
    if not klines:
        return {"status": "skipped", "reason": "续跑区间无 K 线"}

    signal_cache = build_signal_cache(
        ps, klines_all, db, start=run_from, dividend_mode=signal_mode)

    portfolio = assemble_portfolio(ps, strategies, db)
    if state is None:
        if resolve_anchor(link, cfg):
            apply_seam_anchor(
                portfolio, link.seam_cash,
                json.loads(link.seam_positions_json or "[]"))
        pending: List[OrderEvent] = []
    else:
        pending = restore_portfolio(portfolio, state)

    result = BacktestEngine().run(
        portfolio, klines=klines, signal_cache=signal_cache,
        initial_pending_orders=pending,
        stock_names=_pool_stock_names(ps, db))

    snaps = result.get("snapshots") or []
    if not snaps:
        return {"status": "skipped", "reason": "本次区间无快照"}

    new_dates = sorted({s["snap_date"] for s in snaps})
    as_of_new = new_dates[-1]
    payload = state_payload(portfolio, as_of_new,
                            pending_orders=result.get("pending_orders"))
    payload["schema_fp"] = want_schema_fp
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
        live_d = lv.get("dirs", [])
        direction_match = inc_d == live_d
        tags: List[str] = []
        if d in gap_dates:
            tags.append("gap")
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
    }


# ---------------------------------------------------------------- 双跑校验
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
        unsupported = [p for p in _strategy_periods(ps, db)
                       if p not in _DAILY_PERIODS]
        if unsupported:
            return {
                "status": "error",
                "reason": "分钟级策略（%s）续跑语义未定，本期不支持双跑校验"
                          % "/".join(unsupported),
            }
        from_date = from_date or link.seam_date
        strategies = (
            db.query(Strategy).filter_by(portfolio_id=ps.id)
            .order_by(Strategy.id).all()
        )
        dividend_mode = cfg.get("dividend_mode", "dual")
        accounting_mode, signal_mode = _resolve_modes(dividend_mode)
        try:
            klines_all = build_klines(ps, from_date, to_date, db,
                                      dividend_mode=accounting_mode)
            klines = _slice_klines_from(klines_all, from_date)
            signal_cache = build_signal_cache(
                ps, klines_all, db, start=from_date,
                dividend_mode=signal_mode)
            portfolio = assemble_portfolio(ps, strategies, db)
            if resolve_anchor(link, cfg):
                apply_seam_anchor(
                    portfolio, link.seam_cash,
                    json.loads(link.seam_positions_json or "[]"))
            result = BacktestEngine().run(
                portfolio, klines=klines, signal_cache=signal_cache,
                stock_names=_pool_stock_names(ps, db))
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

        full_rows: Dict[date, dict] = {}
        for snap in result.get("snapshots") or []:
            d = snap["snap_date"]
            full_rows[d] = {
                "trade_date": d,
                "total_value": snap["total_value"],
                "trades_count": len([
                    t for t in (result.get("trades") or [])
                    if t.trade_time.date() == d]),
                "direction_match": True,
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
                "direction_match": bool(r.direction_match),
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
                {"dates_count": metrics["dates_count"]}, ensure_ascii=False),
        )
        db.add(row)
        # 校验失败 → 超容差的日行标红（warn + validation_fail）
        if metrics["status"] == "fail" and metrics["first_divergence_date"]:
            for r in day_rows:
                if r.trade_date >= metrics["first_divergence_date"]:
                    tags = json.loads(r.tags_json or "[]")
                    if "validation_fail" not in tags:
                        tags.append("validation_fail")
                    r.tags_json = json.dumps(tags, ensure_ascii=False)
                    if r.status == "ok":
                        r.status = "warn"
        db.commit()
        return {
            "status": metrics["status"],
            **{k: v for k, v in metrics.items() if k != "status"},
            "from_date": from_date.isoformat(),
            "to_date": to_date.isoformat(),
        }


# ---------------------------------------------------------------- 查询汇总
def summary(db: Session, session_id: int, ps_id: int) -> dict:
    ps = db.query(PortfolioStrategy).filter_by(id=ps_id).first()
    periods = _strategy_periods(ps, db) if ps is not None else []
    unsupported = [p for p in periods if p not in _DAILY_PERIODS]
    supported = ps is not None and not unsupported
    reason = ""
    if ps is None:
        reason = "组合策略不存在"
    elif unsupported:
        reason = "分钟级策略（%s）续跑语义未定，本期不支持" % "/".join(unsupported)

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

    return {
        "supported": supported,
        "reason": reason,
        "seam_date": link.seam_date.isoformat() if link and link.seam_date else None,
        "seam_cash": str(link.seam_cash) if link and link.seam_cash is not None else None,
        "checkpoint": None if ckpt is None else {
            "seam_date": ckpt.seam_date.isoformat(),
            "as_of_date": ckpt.as_of_date.isoformat(),
            "params_hash": ckpt.params_hash,
            "schema_version": ckpt.schema_version,
            "updated_at": _dt(ckpt.updated_at),
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
                if ckpt is not None and ckpt.as_of_date >= today:
                    continue
                try:
                    run_incremental(
                        db, link.session_id, link.portfolio_strategy_id,
                        today=today, now=now)
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "增量回测失败 (session %s, ps %s)",
                        link.session_id, link.portfolio_strategy_id)
                    db.rollback()

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
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "增量回测双跑校验失败 (session %s, ps %s)",
                            link.session_id, link.portfolio_strategy_id)
                        db.rollback()
                self._last_validation_date = today
        return {"ran_at": now.isoformat()}
