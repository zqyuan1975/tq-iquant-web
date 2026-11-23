"""公司行为漏记检测（只观测，2026-11-23）。

背景：``corporate_action_sync`` 从通达信同步现金分红/送转/配股写入
``corporate_actions``，回测 ``exact`` 档与实盘按「不复权真实价 + 公司行为入账」
记账。同步漏一条事件、或某只票因子取不到时，账目会静默出错且没有任何提示。

本模块用**另一条数据链路**做交叉验证：同一只票同时取
- ``dividend_type="none"``（不复权真实成交价）
- ``dividend_type="front"``（前复权，除权日连续）

两者比值 ``adj/raw`` 在除权日发生跳变。逐日计算跳变倍数

    k = (adj_t / raw_t) / (adj_{t-1} / raw_{t-1})

k > 1 代表 raw 相对 adj 下移，即发生了除权/除息。当隐含跌幅
``(1 - 1/k) * 100`` 超过阈值（默认 1%，低于此视为复权价 2 位小数舍入噪声），
就去 ``corporate_actions`` 反查该日附近是否已登记事件；查不到即判定
「未登记事件」，写一条 ``corporate_action_unverified`` 诊断。

硬约束（对应用户明确要求）：
1. **只写诊断表**，绝不自动入账、不改已有事件、不改任何账本数值；
2. 默认开启但只告警（``logger.warning`` + 诊断行），任何异常都不阻断调用方；
3. 支持按股票池 / 最近 N 日限定范围，避免全量扫描拖慢服务；
4. 疑似事件类型是**启发式线索**（dividend/split/rights），仅供排查，不是结论。
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional

from sqlalchemy.orm import Session

from core.models import DIAG_UNVERIFIED_EVENT

logger = logging.getLogger(__name__)

#: 数据来源标识（诊断行 source 字段，便于日后换算法时区分）。
SOURCE_RAW_VS_FRONT = "tq:close_raw_vs_front"

#: 检测开关与阈值默认值（config.yaml 可覆盖 ``corporate_action_audit`` 段）。
#: enabled 默认 True —— 只写诊断/打日志，不参与任何交易或记账判定。
DEFAULT_CONFIG: dict = {
    "enabled": True,
    "lookback_days": 250,
    "min_drop_pct": 1.0,
    "match_tolerance_days": 5,
    "max_codes": 0,
}

#: 常见送转比例（每股送转股数），用于把断点粗分类为 split。
_SPLIT_RATIOS = (0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0, 1.2, 1.5, 2.0)
_SPLIT_TOL = 0.05
#: 隐含跌幅低于该值（%）优先猜现金分红；高于且不吻合送转比例 → 猜配股/混合。
_DIVIDEND_MAX_DROP_PCT = 10.0


def audit_config(overrides: Optional[dict] = None) -> dict:
    """读取检测配置：代码默认值 ← config.yaml ``corporate_action_audit`` ← 入参。"""
    cfg = dict(DEFAULT_CONFIG)
    if overrides is None:
        try:
            from core.config import load_config
            section = (load_config() or {}).get("corporate_action_audit")
        except Exception:  # noqa: BLE001 — 配置读不到就按默认值（只观测，不影响主流程）
            logger.debug("公司行为检测配置读取失败，使用默认值", exc_info=True)
            section = None
        overrides = section if isinstance(section, dict) else {}
    for key, default in DEFAULT_CONFIG.items():
        value = overrides.get(key, default)
        if isinstance(default, bool):
            cfg[key] = bool(value)
        else:
            try:
                cfg[key] = int(default) if isinstance(default, int) else float(value)
            except (TypeError, ValueError):
                cfg[key] = default
    return cfg


# ---------------------------------------------------------------- 纯计算

def _to_float(value) -> Optional[float]:
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out or out in (float("inf"), float("-inf")):  # NaN / inf
        return None
    return out


def _to_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):  # pandas.Timestamp
        try:
            got = value.date()
            return got if isinstance(got, date) else None
        except Exception:  # noqa: BLE001
            return None
    return None


def suspected_event_type(implied_ratio: float) -> str:
    """按跳变倍数启发式猜测事件类型（dividend / split / rights）。

    仅作排查线索：现金分红与低比例送转在价格序列上不可区分，配股与「送转+派现」
    混合事件也只能给方向性猜测。最终以已登记事件/公告为准。
    """
    if implied_ratio <= 0:
        return "dividend"
    drop_pct = (1.0 - 1.0 / implied_ratio) * 100.0
    add = implied_ratio - 1.0
    if drop_pct >= 8.0 and any(abs(add - r) <= _SPLIT_TOL for r in _SPLIT_RATIOS):
        return "split"
    if drop_pct < _DIVIDEND_MAX_DROP_PCT:
        return "dividend"
    return "rights"


def detect_price_breakpoints(
    raw_close: Dict[date, float],
    adj_close: Dict[date, float],
    *,
    min_drop_pct: float = 1.0,
) -> List[dict]:
    """定位复权比跳变断点（纯函数，不碰库/网络）。

    只判定**价格下行方向**（``implied_ratio > 1``）：送转/派现/配股都会让不复权
    价在除权日下移。反方向（缩股）不在本检测范围，避免把数据错误当公司行为。
    """
    dates = sorted(d for d in raw_close if d in adj_close)
    out: List[dict] = []
    for prev, cur in zip(dates, dates[1:]):
        raw_prev = _to_float(raw_close.get(prev))
        raw_cur = _to_float(raw_close.get(cur))
        adj_prev = _to_float(adj_close.get(prev))
        adj_cur = _to_float(adj_close.get(cur))
        if not all(v is not None and v > 0 for v in (raw_prev, raw_cur, adj_prev, adj_cur)):
            continue
        ratio_prev = adj_prev / raw_prev
        ratio_cur = adj_cur / raw_cur
        if ratio_prev <= 0 or ratio_cur <= 0:
            continue
        implied = ratio_cur / ratio_prev
        if implied <= 1.0:
            continue
        drop_pct = (1.0 - 1.0 / implied) * 100.0
        if drop_pct < min_drop_pct:
            continue
        out.append({
            "break_date": cur,
            "prev_trade_date": prev,
            "implied_ratio": round(implied, 6),
            "implied_drop_pct": round(drop_pct, 4),
            "prev_close": round(raw_prev, 6),
            # 若断点完全由除权造成，次日理论参考价 = 前收 / k
            "implied_ex_price": round(raw_prev / implied, 6),
        })
    return out


def _match_events(events: Iterable, break_date: date, tolerance_days: int) -> List:
    lo = break_date - timedelta(days=max(int(tolerance_days), 0))
    hi = break_date + timedelta(days=max(int(tolerance_days), 0))
    return [e for e in events if e.ex_date and lo <= e.ex_date <= hi]


# ---------------------------------------------------------------- 取数

def _extract_close_series(raw: dict, codes: Iterable[str]) -> Dict[str, Dict[date, float]]:
    """TQ 原始行情 ``{period: {field: DataFrame}}`` → ``{code: {date: close}}``。"""
    period = (raw or {}).get("1d") or {}
    close_df = period.get("Close")
    columns = getattr(close_df, "columns", None)
    if close_df is None or columns is None:
        return {}
    columns = list(columns)
    out: Dict[str, Dict[date, float]] = {}
    if not columns:
        return out
    for code in codes:
        if code not in columns:
            continue
        series: Dict[date, float] = {}
        for ts in close_df.index:
            day = _to_date(ts)
            if day is None:
                continue
            value = _to_float(close_df.loc[ts, code])
            if value is None:
                continue
            series[day] = value
        if series:
            out[code] = series
    return out


def _fetch_close(tq, codes: List[str], start: str, end: str,
                 dividend_type: str) -> Dict[str, Dict[date, float]]:
    raw = tq.get_history_raw(
        stocks=list(codes), periods=["1d"], start=start, end=end,
        dividend_type=dividend_type, count=-1,
    )
    return _extract_close_series(raw, codes)


# ---------------------------------------------------------------- 落库

def _serialize(row) -> dict:
    def _f(value):
        return float(value) if value is not None else None

    detail = None
    if row.detail:
        try:
            detail = json.loads(row.detail)
        except ValueError:
            detail = row.detail
    return {
        "id": row.id,
        "diagnostic_type": row.diagnostic_type,
        "code": row.code,
        "break_date": row.break_date.isoformat() if row.break_date else None,
        "prev_trade_date": (row.prev_trade_date.isoformat()
                            if row.prev_trade_date else None),
        "implied_ratio": _f(row.implied_ratio),
        "implied_drop_pct": _f(row.implied_drop_pct),
        "prev_close": _f(row.prev_close),
        "implied_ex_price": _f(row.implied_ex_price),
        "suspected_event_type": row.suspected_event_type,
        "window_start": row.window_start.isoformat() if row.window_start else None,
        "window_end": row.window_end.isoformat() if row.window_end else None,
        "source": row.source,
        "detail": detail,
        "created_at": row.created_at.isoformat() if row.created_at else None,
    }


def _upsert(db: Session, code: str, item: dict, window: tuple,
            threshold_pct: float, tolerance_days: int) -> str:
    """按 (类型, 代码, 断点日) 幂等写入；返回 added/updated/unchanged。"""
    from core.models import CorporateActionDiagnostic

    row = (db.query(CorporateActionDiagnostic)
           .filter(CorporateActionDiagnostic.diagnostic_type == DIAG_UNVERIFIED_EVENT,
                   CorporateActionDiagnostic.code == code,
                   CorporateActionDiagnostic.break_date == item["break_date"])
           .first())
    implied_ratio = item["implied_ratio"]
    detail = {
        "detector": "adj_raw_ratio_jump",
        "threshold_pct": threshold_pct,
        "match_tolerance_days": tolerance_days,
        "implied_share_ratio": round(implied_ratio - 1.0, 6),
        "implied_cash_per_share": round(
            item["prev_close"] * (1.0 - 1.0 / implied_ratio), 6),
        "heuristic_only": True,
    }
    fields = {
        "prev_trade_date": item["prev_trade_date"],
        "implied_ratio": implied_ratio,
        "implied_drop_pct": item["implied_drop_pct"],
        "prev_close": item["prev_close"],
        "implied_ex_price": item["implied_ex_price"],
        "suspected_event_type": suspected_event_type(implied_ratio),
        "window_start": window[0],
        "window_end": window[1],
        "source": SOURCE_RAW_VS_FRONT,
        "detail": json.dumps(detail, ensure_ascii=False, sort_keys=True),
    }
    if row is None:
        db.add(CorporateActionDiagnostic(
            diagnostic_type=DIAG_UNVERIFIED_EVENT, code=code,
            break_date=item["break_date"], **fields))
        return "added"
    changed = False
    for key, value in fields.items():
        if getattr(row, key) != value:
            setattr(row, key, value)
            changed = True
    return "updated" if changed else "unchanged"


def _resolve_stale(db: Session, code: str, window: tuple,
                   keep_dates: set) -> int:
    """删掉本窗口内已不再成立的旧诊断（事件后来补录/数据修正）。"""
    from core.models import CorporateActionDiagnostic

    query = (db.query(CorporateActionDiagnostic)
             .filter(CorporateActionDiagnostic.diagnostic_type == DIAG_UNVERIFIED_EVENT,
                     CorporateActionDiagnostic.code == code))
    if window[0] is not None:
        query = query.filter(CorporateActionDiagnostic.break_date >= window[0])
    if window[1] is not None:
        query = query.filter(CorporateActionDiagnostic.break_date <= window[1])
    stale = [row for row in query.all() if row.break_date not in keep_dates]
    for row in stale:
        db.delete(row)
    return len(stale)


# ---------------------------------------------------------------- 主流程

def audit_corporate_actions(
    db: Session, codes: Iterable[str], start: str = "", end: str = "",
    lookback_days: Optional[int] = None, tq=None,
    config: Optional[dict] = None, write: bool = True,
) -> dict:
    """检测「有显著除权断点但未登记事件」并写诊断（绝不动事件/账本）。

    返回::

        {"enabled", "skipped", "reason", "requested", "scanned", "failed",
         "breakpoints", "matched", "unverified", "written", "resolved",
         "diagnostics": [...], "window": {...}, "source"}

    ``matched`` = 断点已能对上登记事件的个数（不落库）；``unverified`` = 未登记
    事件个数；``written`` 为诊断表实际新增行数，``resolved`` 为本轮清掉的过期诊断。
    """
    cfg = audit_config(config)
    window = _resolve_window(start, end, lookback_days or cfg["lookback_days"])
    wanted = [str(c or "").strip() for c in codes or []]
    wanted = list(dict.fromkeys(c for c in wanted if c))
    base = {"enabled": bool(cfg["enabled"]), "skipped": False, "reason": None,
            "requested": len(wanted), "scanned": 0, "failed": [],
            "breakpoints": 0, "matched": 0, "unverified": 0, "written": 0,
            "resolved": 0, "diagnostics": [], "source": SOURCE_RAW_VS_FRONT,
            "window": {"start": window[0].isoformat() if window[0] else None,
                       "end": window[1].isoformat() if window[1] else None}}
    if not cfg["enabled"]:
        base.update({"skipped": True, "reason": "corporate_action_audit.enabled=false"})
        return base
    if not wanted:
        base.update({"skipped": True, "reason": "股票范围为空"})
        return base

    max_codes = cfg.get("max_codes") or 0
    if max_codes > 0 and len(wanted) > max_codes:
        base["requested"] = len(wanted)
        wanted = wanted[:max_codes]
        base["truncated"] = True

    if tq is None:
        from core.tq.data import TQData
        tq = TQData()

    start_str = window[0].strftime("%Y%m%d")
    end_str = window[1].strftime("%Y%m%d")
    try:
        raw_series = _fetch_close(tq, wanted, start_str, end_str, "none")
        adj_series = _fetch_close(tq, wanted, start_str, end_str, "front")
    except Exception:  # noqa: BLE001 — 只观测，不允许阻断调用方
        logger.warning("公司行为检测取数失败（跳过本轮，不影响主流程）", exc_info=True)
        base.update({"skipped": True, "reason": "行情取数失败", "failed": list(wanted)})
        return base

    from core.models import CorporateAction

    written = resolved = 0
    for code in wanted:
        raw_close = raw_series.get(code) or {}
        adj_close = adj_series.get(code) or {}
        if not raw_close or not adj_close:
            base["failed"].append(code)
            continue
        base["scanned"] += 1
        local_window = (min(raw_close), max(raw_close))
        events = (db.query(CorporateAction)
                  .filter(CorporateAction.code == code,
                          CorporateAction.ex_date >= local_window[0],
                          CorporateAction.ex_date <= local_window[1])
                  .all())
        items = detect_price_breakpoints(
            raw_close, adj_close, min_drop_pct=float(cfg["min_drop_pct"]))
        keep: set = set()
        for item in items:
            base["breakpoints"] += 1
            hits = _match_events(events, item["break_date"],
                                 int(cfg["match_tolerance_days"]))
            if hits:
                base["matched"] += 1
                continue
            base["unverified"] += 1
            keep.add(item["break_date"])
            logger.warning(
                "公司行为疑似漏记 %s 断点日=%s 前收=%.3f 隐含跌幅=%.2f%% 疑似%s",
                code, item["break_date"], item["prev_close"],
                item["implied_drop_pct"],
                suspected_event_type(item["implied_ratio"]))
            if not write:
                continue
            status = _upsert(db, code, item, local_window,
                             float(cfg["min_drop_pct"]),
                             int(cfg["match_tolerance_days"]))
            written += status == "added"
            if status != "unchanged":
                base["diagnostics"].append({
                    "code": code, **_serialize_values(item, local_window)})
        if write:
            resolved += _resolve_stale(db, code, local_window, keep)
    if write:
        try:
            db.commit()
        except Exception:  # noqa: BLE001 — 诊断写失败不能影响业务
            db.rollback()
            logger.warning("公司行为诊断写入失败（忽略，不影响主流程）", exc_info=True)
            base.update({"skipped": True, "reason": "诊断写库失败"})
            return base
    base["written"] = written
    base["resolved"] = resolved
    return base


def _serialize_values(item: dict, window: tuple) -> dict:
    """未落库时的返回视图，字段与 ``_serialize`` 对齐（便于前端直接用）。"""
    return {
        "diagnostic_type": DIAG_UNVERIFIED_EVENT,
        "break_date": item["break_date"].isoformat(),
        "prev_trade_date": item["prev_trade_date"].isoformat(),
        "implied_ratio": item["implied_ratio"],
        "implied_drop_pct": item["implied_drop_pct"],
        "prev_close": item["prev_close"],
        "implied_ex_price": item["implied_ex_price"],
        "suspected_event_type": suspected_event_type(item["implied_ratio"]),
        "window_start": window[0].isoformat(),
        "window_end": window[1].isoformat(),
        "source": SOURCE_RAW_VS_FRONT,
    }


def _resolve_window(start: str, end: str, lookback_days: int) -> tuple:
    start_d = _parse_day(start)
    end_d = _parse_day(end) or date.today()
    if start_d is None:
        start_d = end_d - timedelta(days=max(int(lookback_days or 0), 1))
    if start_d > end_d:
        start_d, end_d = end_d, start_d
    return start_d, end_d


def _parse_day(value: str) -> Optional[date]:
    text = str(value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


# ---------------------------------------------------------------- 查询

def list_diagnostics(db: Session, *, code: Optional[str] = None,
                     start: Optional[date] = None, end: Optional[date] = None,
                     diagnostic_type: str = DIAG_UNVERIFIED_EVENT,
                     page: int = 1, page_size: int = 50) -> dict:
    from core.models import CorporateActionDiagnostic

    query = db.query(CorporateActionDiagnostic)
    if diagnostic_type:
        query = query.filter(
            CorporateActionDiagnostic.diagnostic_type == diagnostic_type)
    if code:
        query = query.filter(
            CorporateActionDiagnostic.code == code.strip().upper())
    if start is not None:
        query = query.filter(CorporateActionDiagnostic.break_date >= start)
    if end is not None:
        query = query.filter(CorporateActionDiagnostic.break_date <= end)
    total = query.count()
    page = max(1, int(page or 1))
    page_size = max(1, min(500, int(page_size or 50)))
    rows = (query.order_by(CorporateActionDiagnostic.break_date.desc(),
                           CorporateActionDiagnostic.id.desc())
            .offset((page - 1) * page_size).limit(page_size).all())
    return {"total": total, "page": page, "page_size": page_size,
            "items": [_serialize(r) for r in rows]}
