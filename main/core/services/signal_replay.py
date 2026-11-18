"""N3 信号回放校验。

用途：实盘某天「该出的信号没出 / 多出了不该出的」，用同一组合 + 同一区间
重新拉行情、重算公式信号，输出每根 bar 触发的信号；给定会话时再与该会话的
实盘委托（live_orders）按 (日期, 股票, 方向) 对照，快速区分：

- 回放有、实盘无 → 疑似漏单 / 执行链路拦截
- 回放无、实盘有 → 口径不一致（参数/复权/人工单）
- 两边都有 → 正常

只读：不写任何表，不改会话状态。
"""

from datetime import date, datetime
from typing import Optional, Tuple

from sqlalchemy.orm import Session

from core.models import (
    Formula, FormulaSignal, LiveOrder, LiveSession, PortfolioStrategy, Strategy,
)
from core.services import backtest_service as bt

# 公式信号类型 → 实盘委托方向
_SIGNAL_DIRECTION = {"OPEN": "BUY", "CLOSE": "SELL"}
_MAX_DIFF_ROWS = 200


def _signal_type_map(db: Session, portfolio_id: int) -> dict:
    rows = (
        db.query(FormulaSignal.signal_name, FormulaSignal.signal_type)
        .join(Formula, Formula.id == FormulaSignal.formula_id)
        .join(Strategy, Strategy.formula_id == Formula.id)
        .filter(Strategy.portfolio_id == portfolio_id)
        .all()
    )
    out: dict = {}
    for name, sig_type in rows:
        out.setdefault(name, sig_type)
    return out


def _bar_parts(bar_time) -> Tuple[Optional[date], str]:
    if isinstance(bar_time, datetime):
        return bar_time.date(), bar_time.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(bar_time, date):
        return bar_time, bar_time.isoformat()
    return None, str(bar_time)


def replay_signals(
    db: Session, portfolio_strategy_id: int, start_date: date, end_date: date,
    dividend_mode: str = "front", session_id: Optional[int] = None,
) -> Tuple[Optional[dict], Optional[str]]:
    """重算区间信号并（可选）与实盘委托对照。返回 (payload, error)。"""
    if start_date > end_date:
        return None, "开始日期不能晚于结束日期"
    ps = db.query(PortfolioStrategy).filter(
        PortfolioStrategy.id == portfolio_strategy_id).first()
    if ps is None:
        return None, "组合策略不存在"

    payload: dict = {
        "portfolio_strategy_id": portfolio_strategy_id,
        "portfolio_name": ps.name,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "dividend_mode": dividend_mode,
        "session_id": session_id,
        "rows": [],
        "summary": {},
        "comparison": None,
        "warnings": [],
    }

    klines = bt.build_klines(
        ps, start_date, end_date, db=db, dividend_mode=dividend_mode)
    if not klines:
        payload["warnings"].append("区间内无行情数据（TQ 未返回），信号为空")
        payload["summary"] = {
            "signal_bars": 0, "signals": 0, "by_date": [],
            "by_strategy": {}, "by_stock": [],
        }
        return payload, None

    cache = bt.build_signal_cache(
        ps, klines, db=db, start=start_date, dividend_mode=dividend_mode)
    type_map = _signal_type_map(db, portfolio_strategy_id)

    rows = []
    for (strategy_id, stock_code, bar_time), items in cache.items():
        hits = [it for it in (items or []) if it.get("value")]
        if not hits:
            continue
        day, bar_text = _bar_parts(bar_time)
        signals = []
        directions = set()
        for it in hits:
            name = it.get("name")
            sig_type = type_map.get(name)
            direction = _SIGNAL_DIRECTION.get(sig_type)
            if direction:
                directions.add(direction)
            signals.append({
                "name": name, "signal_type": sig_type,
                "value": it.get("value"),
            })
        rows.append({
            "bar_time": bar_text,
            "date": day.isoformat() if day else None,
            "strategy_id": strategy_id,
            "stock_code": stock_code,
            "signals": signals,
            "directions": sorted(directions),
        })
    rows.sort(key=lambda r: (r["bar_time"], r["stock_code"], r["strategy_id"]))
    payload["rows"] = rows

    by_date: dict = {}
    by_strategy: dict = {}
    by_stock: dict = {}
    for r in rows:
        by_date[r["date"]] = by_date.get(r["date"], 0) + 1
        by_strategy[str(r["strategy_id"])] = (
            by_strategy.get(str(r["strategy_id"]), 0) + len(r["signals"]))
        by_stock[r["stock_code"]] = (
            by_stock.get(r["stock_code"], 0) + len(r["signals"]))
    payload["summary"] = {
        "signal_bars": len(rows),
        "signals": sum(len(r["signals"]) for r in rows),
        "by_date": [
            {"date": d, "count": c}
            for d, c in sorted(by_date.items(), key=lambda kv: kv[0] or "")
        ],
        "by_strategy": by_strategy,
        "by_stock": [
            {"stock_code": code, "count": c}
            for code, c in sorted(by_stock.items(),
                                  key=lambda kv: (-kv[1], kv[0]))[:20]
        ],
    }

    if session_id:
        session = db.query(LiveSession).filter(
            LiveSession.id == session_id).first()
        if session is None:
            payload["warnings"].append(f"会话 #{session_id} 不存在，跳过对照")
        else:
            payload["comparison"] = _compare_with_live(
                db, session, rows, start_date, end_date)
    return payload, None


def _compare_with_live(
    db: Session, session, rows: list, start_date: date, end_date: date,
) -> dict:
    """回放信号 vs 实盘委托：按 (日期, 股票, 方向) 聚合后求差集。"""
    replay_keys: dict = {}
    for r in rows:
        if not r.get("date"):
            continue
        for direction in r.get("directions") or []:
            key = (r["date"], r["stock_code"], direction)
            replay_keys[key] = replay_keys.get(key, 0) + 1

    orders = (
        db.query(LiveOrder)
        .filter(
            LiveOrder.live_session_id == session.id,
            LiveOrder.bar_time >= datetime.combine(start_date, datetime.min.time()),
            LiveOrder.bar_time <= datetime.combine(end_date, datetime.max.time()),
        )
        .all()
    )
    live_keys: dict = {}
    for o in orders:
        if o.bar_time is None:
            continue
        key = (o.bar_time.date().isoformat(), o.stock_code, o.trade_type)
        live_keys[key] = live_keys.get(key, 0) + 1

    matched = set(replay_keys) & set(live_keys)
    only_replay = sorted(set(replay_keys) - set(live_keys))
    only_live = sorted(set(live_keys) - set(replay_keys))

    def _rows(keys):
        return [
            {"date": d, "stock_code": code, "trade_type": t}
            for d, code, t in keys[:_MAX_DIFF_ROWS]
        ]

    return {
        "session_id": session.id,
        "replay_keys": len(replay_keys),
        "live_order_keys": len(live_keys),
        "matched": len(matched),
        "only_replay_count": len(only_replay),
        "only_live_count": len(only_live),
        "only_replay": _rows(only_replay),
        "only_live": _rows(only_live),
        "truncated": (len(only_replay) > _MAX_DIFF_ROWS
                      or len(only_live) > _MAX_DIFF_ROWS),
        "note": "only_replay=疑似漏单/被拦截；only_live=实盘口径或人工单",
    }
