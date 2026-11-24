"""公司行为自动同步（F 组，2026-11-22 口径切换）。

从通达信 TQ ``get_divid_factors`` 抽「现金分红 / 送转 / 配股」写入
``corporate_actions``，供 ``exact`` 档（不复权真实价 + 公司行为入账）使用。

设计约束（对应用户批准的四条）：

1. **逐票独立成败**：单只票取数/解析失败只记录进 ``failed``，绝不中断整批
   ——同步不阻塞上线，失败清单留待下次补。
2. **配股不处理但不得静默**：配股落一条 ``event_type="rights"`` 且 ``note``
   带 ``rights_unhandled``；内核 ``apply_day`` 对 rights 只发 notice（放弃认配），
   不改账、不下单。
3. **回填覆盖要可辨识**：返回 ``covered`` / ``failed`` / ``no_events``，调用方
   据此给缺失股票打 ``corporate_actions_missing``，不得把「没取到」当「无事件」。
4. **口径换算**：TDX ``Bonus`` / ``ShareBonus`` / ``Allotment`` 均为**每 10 股**
   口径，落库统一换算成每股（÷10）；``AllotPrice`` 为配股价（元/股）。

不做的事：不写 ``live_corporate_actions`` 台账（由实盘 apply_day 写），
不触发任何下单，不改持仓。
"""
from __future__ import annotations

import json
import logging
import os
import threading
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Set

from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

#: 配股标记：出现即代表「该事件存在但本批不接入账」，供报表/核对检索。
RIGHTS_UNHANDLED = "rights_unhandled"

#: 覆盖清单文件名（与 dev.db 同目录）。记录每只票最近一次同步结果：
#: covered（已取到并落库）/ no_events（取数成功但无除权事件）/ failed（取数失败）。
#: 回测 exact 档据此区分「确实没事件」与「没回填」，缺的标 corporate_actions_missing。
COVERAGE_FILE_NAME = "corporate_actions_coverage.json"

_Q6 = Decimal("0.000001")


def _dec(value) -> Optional[Decimal]:
    if value is None or value == "":
        return None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if d != d:  # NaN
        return None
    return d.quantize(_Q6)


def _as_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    if len(digits) >= 8:
        try:
            return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
        except ValueError:
            return None
    return None


def _iter_factor_rows(frame) -> Iterable[dict]:
    """TQ divid_factors DataFrame → [{ex_date, bonus, share_bonus, allotment, allot_price}]。

    TQ 返回 Date 索引 + 列 ``Type/Bonus/AllotPrice/ShareBonus/Allotment``（实测），
    也兼容 Date 作为列、列名大小写不一的变体。
    """
    if frame is None or getattr(frame, "empty", True):
        return []
    columns = {str(c).strip().lower(): c for c in frame.columns}
    date_col = columns.get("date")
    bonus_col = columns.get("bonus")
    share_col = columns.get("sharebonus")
    allot_qty_col = columns.get("allotment")
    allot_price_col = columns.get("allotprice")
    out: List[dict] = []
    for idx, row in frame.iterrows():
        ex = _as_date(row[date_col] if date_col is not None else idx)
        if ex is None:
            continue
        out.append({
            "ex_date": ex,
            "bonus": _dec(row[bonus_col]) if bonus_col is not None else None,
            "share_bonus": _dec(row[share_col]) if share_col is not None else None,
            "allotment": _dec(row[allot_qty_col]) if allot_qty_col is not None else None,
            "allot_price": _dec(row[allot_price_col]) if allot_price_col is not None else None,
        })
    return out


def _upsert(db: Session, fields: dict) -> str:
    """按 (code, event_type, ex_date) 幂等写入；返回 added/updated/unchanged。"""
    from core.models import CorporateAction

    row = (db.query(CorporateAction)
           .filter(CorporateAction.code == fields["code"],
                   CorporateAction.event_type == fields["event_type"],
                   CorporateAction.ex_date == fields["ex_date"])
           .first())
    if row is None:
        db.add(CorporateAction(**fields))
        return "added"
    changed = False
    for key in ("cash_per_share", "share_ratio", "rights_price", "note"):
        value = fields.get(key)
        if value is not None and getattr(row, key) != value:
            setattr(row, key, value)
            changed = True
    return "updated" if changed else "unchanged"


def sync_corporate_actions(db: Session, codes: Iterable[str],
                           start: str = "", end: str = "") -> dict:
    """批量回填公司行为（只读 TQ、写 ``corporate_actions``）。

    返回::

        {"requested": n, "covered": [..], "failed": [..], "no_events": [..],
         "added": x, "updated": y, "rights": r}

    ``failed`` = 取数/解析抛错的票（留待下次补）；``no_events`` = 取数成功但
    无除权事件（空表）；两者语义不同，调用方不得混用。
    """
    from core.tq.data import TQData

    tq = TQData()
    requested = 0
    covered: List[str] = []
    failed: List[str] = []
    no_events: List[str] = []
    added = updated = rights = 0

    for raw_code in codes or []:
        code = str(raw_code or "").strip()
        if not code:
            continue
        requested += 1
        try:
            frame = tq.get_divid_factors(code, start=start, end=end)
            rows = list(_iter_factor_rows(frame))
        except Exception as exc:  # noqa: BLE001 — 单票失败不阻断整批
            failed.append(code)
            logger.warning("公司行为取数失败 %s：%s", code, exc)
            _mark_coverage(code, "failed", window=(start, end))
            continue
        if not rows:
            no_events.append(code)
            _mark_coverage(code, "no_events", window=(start, end))
            continue
        wrote = 0
        for item in rows:
            ex = item["ex_date"]
            bonus = item["bonus"] or Decimal("0")
            share_bonus = item["share_bonus"] or Decimal("0")
            allotment = item["allotment"] or Decimal("0")
            allot_price = item["allot_price"]
            if bonus > 0:
                status = _upsert(db, {
                    "code": code, "event_type": "dividend", "ex_date": ex,
                    "cash_per_share": (bonus / Decimal("10")).quantize(_Q6),
                    "note": "auto:tdx_divid_factors",
                })
                added += status == "added"
                updated += status == "updated"
                wrote += 1
            if share_bonus > 0:
                status = _upsert(db, {
                    "code": code, "event_type": "split", "ex_date": ex,
                    "share_ratio": (share_bonus / Decimal("10")).quantize(_Q6),
                    "note": "auto:tdx_divid_factors",
                })
                added += status == "added"
                updated += status == "updated"
                wrote += 1
            if allotment > 0 or (allot_price or Decimal("0")) > 0:
                status = _upsert(db, {
                    "code": code, "event_type": "rights", "ex_date": ex,
                    "share_ratio": ((allotment or Decimal("0")) / Decimal("10")).quantize(_Q6)
                    if allotment > 0 else None,
                    "rights_price": allot_price,
                    "note": RIGHTS_UNHANDLED + "（配股本批不接入账，仅标记）",
                })
                added += status == "added"
                updated += status == "updated"
                rights += 1
                wrote += 1
        try:
            db.commit()
        except Exception:  # noqa: BLE001 — 单票写库失败只影响该票
            db.rollback()
            failed.append(code)
            logger.exception("公司行为写入失败 %s", code)
            _mark_coverage(code, "failed", window=(start, end))
            continue
        covered.append(code)
        _mark_coverage(code, "covered" if wrote else "no_events", window=(start, end))
        if not wrote:
            no_events.append(code)

    return {"requested": requested, "covered": covered, "failed": failed,
            "no_events": no_events, "added": added, "updated": updated,
            "rights": rights}


def fetch_ex_date_codes(codes: Iterable[str], day: date,
                        window_days: int = 30) -> dict:
    """直查 TQ 判定「当日除权除息」的股票（不写库，供盘前核对/补数用）。

    返回 ``{"ok": bool, "codes": {code: [event_type, ...]}, "failed": [code]}``。
    ``ok=False`` 表示整批取数不可用（调用方 fail-open 放行 + 记一条
    ``ex_dividend_data_unavailable``）。
    """
    from datetime import timedelta

    from core.tq.data import TQData

    tq = TQData()
    start = (day - timedelta(days=max(int(window_days), 1))).strftime("%Y%m%d")
    end = day.strftime("%Y%m%d")
    codes_out: Dict[str, List[str]] = {}
    failed: List[str] = []
    attempted = 0
    for raw_code in codes or []:
        code = str(raw_code or "").strip()
        if not code:
            continue
        attempted += 1
        try:
            rows = list(_iter_factor_rows(tq.get_divid_factors(code, start=start, end=end)))
        except Exception:  # noqa: BLE001 — 单票失败计入 failed
            failed.append(code)
            continue
        for item in rows:
            if item["ex_date"] != day:
                continue
            kinds = codes_out.setdefault(code, [])
            if (item["bonus"] or Decimal("0")) > 0:
                kinds.append("dividend")
            if (item["share_bonus"] or Decimal("0")) > 0:
                kinds.append("split")
            if (item["allotment"] or Decimal("0")) > 0:
                kinds.append("rights")
    return {"ok": attempted > 0 and len(failed) < attempted,
            "codes": codes_out, "failed": failed}


def load_ex_date_codes(db: Session, day: date) -> dict:
    """从 ``corporate_actions`` 读当日除权事件（实盘闸用，零 TDX 调用）。

    返回 ``{"ok": bool, "codes": {code: [event_type, ...]}}``。
    ``ok=False`` = 事件表为空/读取失败 → 调用方 fail-open 并记一条
    ``ex_dividend_data_unavailable``（每日至多一条）。
    """
    from core.models import CorporateAction

    try:
        rows = (db.query(CorporateAction)
                .filter(CorporateAction.ex_date == day).all())
        total = db.query(CorporateAction.id).first() is not None
    except Exception:  # noqa: BLE001 — 读库失败不能阻断交易
        logger.warning("除权事件表读取失败（fail-open）", exc_info=True)
        return {"ok": False, "codes": {}}
    codes: Dict[str, List[str]] = {}
    for row in rows:
        if row.event_type in ("dividend", "split", "rights"):
            codes.setdefault(row.code, []).append(row.event_type)
    # 表整体为空 = 尚未回填，不能把「没数据」当「无除权」
    return {"ok": bool(total), "codes": codes}


# ---------------------------------------------------------------- 覆盖清单
# 「取数成功但无事件」与「没回填」在表里长得一样（都是没有行），必须靠清单区分，
# 否则 exact 档会把未回填的票当成无除权静默放行。清单与 dev.db 同目录、纯 JSON，
# 不新增表/迁移；每票一条，记录最近一次同步的状态与窗口。

def coverage_path() -> Path:
    from core.db import _db_path
    return _db_path().parent / COVERAGE_FILE_NAME


def _read_coverage(path: Optional[Path] = None) -> dict:
    target = Path(path) if path else coverage_path()
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_coverage(data: dict, path: Optional[Path] = None) -> None:
    target = Path(path) if path else coverage_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True),
                   encoding="utf-8")
    os.replace(tmp, target)


def _mark_coverage(code: str, status: str, window=("", ""),
                   path: Optional[Path] = None) -> None:
    try:
        data = _read_coverage(path)
        data[code] = {
            "status": status,
            "window": list(window or ("", "")),
            "checked_at": datetime.now().isoformat(timespec="seconds"),
        }
        _write_coverage(data, path)
    except OSError:
        # 清单写不进去不影响已落库的事件，但必须留 WARNING（下次回填会重写）
        logger.warning("公司行为覆盖清单写入失败 %s", code, exc_info=True)


def missing_coverage_codes(db: Session, codes: Iterable[str]) -> List[str]:
    """返回「未回填」的代码（回测 exact 档据此标 corporate_actions_missing）。

    已覆盖 = 清单里 covered/no_events，或事件表里有手工/自动录入的任何行。
    两次都查不到 → 视为未回填（宁可显式标记，不静默当无事件）。
    """
    from core.models import CorporateAction

    wanted = [str(c or "").strip() for c in codes or []]
    wanted = list(dict.fromkeys(c for c in wanted if c))
    if not wanted:
        return []
    covered: Set[str] = {
        code for code, item in _read_coverage().items()
        if isinstance(item, dict) and item.get("status") in ("covered", "no_events")
    }
    try:
        rows = (db.query(CorporateAction.code)
                .filter(CorporateAction.code.in_(wanted)).distinct().all())
        covered.update(row[0] for row in rows)
    except Exception:  # noqa: BLE001 — 读事件表失败按未覆盖处理（显式标记）
        logger.warning("公司行为覆盖核对失败（按未覆盖处理）", exc_info=True)
    return sorted(code for code in wanted if code not in covered)


def resolve_backfill_codes(db: Session, pool_ids: Iterable[int] = (1,)) -> dict:
    """回填范围 = 指定池（默认池 1）∪ 回测记录涉及的池 ∪ 各会话最新快照持仓。

    返回 ``{"codes": [...], "by_pool": {id: [...]}, "holdings": [...],
    "pool_ids": [...]}``，``codes`` 为去重排序后的并集。
    """
    from sqlalchemy import func

    from core.models import (
        BacktestRecord, LiveAccountPositionSnapshot, LiveAccountSnapshot,
        StockPoolStock,
    )

    pools = {int(p) for p in pool_ids or () if p is not None}
    for (snapshot,) in db.query(BacktestRecord.params_snapshot).all():
        try:
            payload = json.loads(snapshot or "{}")
        except ValueError:
            continue
        if not isinstance(payload, dict):
            continue
        pid = payload.get("stock_pool_id")
        if pid is not None:
            try:
                pools.add(int(pid))
            except (TypeError, ValueError):
                pass
        for key in (payload.get("stock_pool_codes") or {}):
            try:
                pools.add(int(key))
            except (TypeError, ValueError):
                pass

    by_pool: Dict[int, List[str]] = {}
    if pools:
        rows = (db.query(StockPoolStock.pool_id, StockPoolStock.stock_code)
                .filter(StockPoolStock.pool_id.in_(sorted(pools)))
                .order_by(StockPoolStock.pool_id, StockPoolStock.stock_code).all())
        for pool_id, code in rows:
            by_pool.setdefault(int(pool_id), []).append(code)

    latest_ids = [
        sid for (_, sid) in
        db.query(LiveAccountSnapshot.live_session_id,
                 func.max(LiveAccountSnapshot.id))
        .group_by(LiveAccountSnapshot.live_session_id).all()
    ]
    holdings: List[str] = []
    if latest_ids:
        rows = (db.query(LiveAccountPositionSnapshot.stock_code)
                .filter(LiveAccountPositionSnapshot.snapshot_id.in_(latest_ids),
                        LiveAccountPositionSnapshot.quantity > 0)
                .distinct().all())
        holdings = sorted(row[0] for row in rows)

    codes = sorted({code for bucket in by_pool.values() for code in bucket}
                   | set(holdings))
    return {"codes": codes, "by_pool": by_pool, "holdings": holdings,
            "pool_ids": sorted(pools)}


# ---------------------------------------------------------------- 同步状态
# 定时/手工/CLI 共用一份「最近一次同步」状态，供运维页展示与跨重启当日判重。
# 与覆盖清单同目录、同样纯 JSON 原子写：不新增表/迁移，避免多进程写库风险。
STATE_FILE_NAME = "corporate_actions_sync_state.json"
DEFAULT_SYNC_ENABLED = True
DEFAULT_SYNC_DAILY_AT = "15:20"
DEFAULT_SYNC_RETRY_MINUTES = 30
#: 被 TQ 闸（P0/交易窗口）拒绝时的短退避：不记失败，几分钟后自动再试。
GATE_RETRY_MINUTES = 5

#: 进程内同步互斥：定时与手工同时触发时后来者直接让路（不排队、不重复打 TQ）。
SYNC_LOCK = threading.Lock()


def try_acquire_sync() -> bool:
    """非阻塞抢同步锁；返回 False 表示已有同步在跑。"""
    return SYNC_LOCK.acquire(blocking=False)


def release_sync() -> None:
    try:
        SYNC_LOCK.release()
    except RuntimeError:
        pass


def sync_state_path() -> Path:
    from core.db import _db_path
    return _db_path().parent / STATE_FILE_NAME


def read_sync_state(path: Optional[Path] = None) -> dict:
    target = Path(path) if path else sync_state_path()
    try:
        data = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _write_sync_state(data: dict, path: Optional[Path] = None) -> None:
    target = Path(path) if path else sync_state_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(target.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True),
                   encoding="utf-8")
    os.replace(tmp, target)


def record_sync_state(trigger: str, status: str, result: Optional[dict] = None,
                      scope: Optional[dict] = None,
                      message: Optional[str] = None,
                      path: Optional[Path] = None,
                      now: Optional[datetime] = None) -> dict:
    """记录一次同步结果（失败只告警，不影响同步本身）。

    ``last_success_date`` 仅在「无 failed」的成功/跳过时推进，定时调度据此
    做当日判重；部分失败留待下一轮只补失败票。
    """
    now = now or datetime.now()
    prev = read_sync_state(path)
    result = result or {}
    failed = list(result.get("failed") or [])
    payload = {
        "last_run_at": now.isoformat(timespec="seconds"),
        "last_success_date": prev.get("last_success_date"),
        "trigger": trigger,
        "status": status,
        "result": {
            "requested": int(result.get("requested") or 0),
            "covered": len(result.get("covered") or []),
            "no_events": len(result.get("no_events") or []),
            "failed": failed,
            "added": int(result.get("added") or 0),
            "updated": int(result.get("updated") or 0),
            "rights": int(result.get("rights") or 0),
        },
        "scope": scope or {},
        "message": message,
    }
    if status in ("success", "skipped") and not failed:
        payload["last_success_date"] = now.date().isoformat()
    try:
        _write_sync_state(payload, path)
    except OSError:
        logger.warning("公司行为同步状态写入失败（忽略，不影响同步结果）",
                       exc_info=True)
    return payload


DEFAULT_SYNC_CONFIG: dict = {
    "enabled": DEFAULT_SYNC_ENABLED,
    "daily_at": DEFAULT_SYNC_DAILY_AT,
    "retry_cooldown_minutes": DEFAULT_SYNC_RETRY_MINUTES,
}


def _parse_hm(raw, default: str) -> tuple:
    try:
        hh, mm = str(raw or "").strip().split(":")
        hh, mm = int(hh), int(mm)
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            return hh, mm
    except (TypeError, ValueError):
        pass
    hh, mm = default.split(":")
    return int(hh), int(mm)


def sync_settings(overrides: Optional[dict] = None) -> dict:
    """读取 ``corporate_action_sync`` 配置；缺省/非法回退默认（同 audit_config）。"""
    cfg = dict(DEFAULT_SYNC_CONFIG)
    if overrides is None:
        try:
            from core.config import load_config
            section = (load_config() or {}).get("corporate_action_sync")
        except Exception:  # noqa: BLE001 — 配置读不到就按默认值
            logger.debug("公司行为同步配置读取失败，使用默认值", exc_info=True)
            section = None
        overrides = section if isinstance(section, dict) else {}
    cfg["enabled"] = bool(overrides.get("enabled", DEFAULT_SYNC_ENABLED))
    cfg["daily_at"] = str(overrides.get("daily_at") or DEFAULT_SYNC_DAILY_AT)
    cfg["daily_at_hm"] = _parse_hm(cfg["daily_at"], DEFAULT_SYNC_DAILY_AT)
    try:
        cooldown = int(overrides.get(
            "retry_cooldown_minutes", DEFAULT_SYNC_RETRY_MINUTES))
    except (TypeError, ValueError):
        cooldown = DEFAULT_SYNC_RETRY_MINUTES
    cfg["retry_cooldown_minutes"] = cooldown if cooldown > 0 else DEFAULT_SYNC_RETRY_MINUTES
    return cfg


def all_pool_ids(db: Session) -> List[int]:
    from core.models import StockPool

    return [int(pid) for (pid,) in db.query(StockPool.id).order_by(StockPool.id).all()]


def _coverage_buckets(db: Session) -> tuple:
    """返回 (status_by_code, codes_with_events)，供覆盖统计一次算完。"""
    from core.models import CorporateAction

    status_by_code = {
        code: str(item.get("status") or "")
        for code, item in _read_coverage().items() if isinstance(item, dict)
    }
    try:
        rows = db.query(CorporateAction.code).distinct().all()
        codes_with_events = {row[0] for row in rows}
    except Exception:  # noqa: BLE001 — 读事件表失败按未覆盖处理
        logger.warning("公司行为覆盖统计读取事件表失败（按未覆盖处理）",
                       exc_info=True)
        codes_with_events = set()
    return status_by_code, codes_with_events


def _bucket_counts(codes: Iterable[str], status_by_code: dict,
                   codes_with_events: Set[str]) -> dict:
    out = {"total": 0, "covered": 0, "no_events": 0, "failed": 0, "missing": 0}
    for code in codes:
        out["total"] += 1
        status = status_by_code.get(code) or ""
        if status == "covered" or code in codes_with_events:
            out["covered"] += 1
        elif status == "no_events":
            out["no_events"] += 1
        elif status == "failed":
            out["failed"] += 1
        else:
            out["missing"] += 1
    return out


def coverage_summary(db: Session, pool_ids: Optional[Iterable[int]] = None) -> dict:
    """覆盖统计（只读、零 TQ 调用）：分池 + 持仓 + 最近同步 + 调度状态。

    四类严格分割：有事件行 / status=covered → covered；status=no_events →
    no_events；status=failed → failed；其余 → missing（与
    ``missing_coverage_codes`` 的判定保持一致）。
    """
    from core.models import StockPool, StockPoolStock

    wanted = sorted({int(p) for p in (pool_ids or all_pool_ids(db))})
    status_by_code, codes_with_events = _coverage_buckets(db)

    names = dict(db.query(StockPool.id, StockPool.name).all())
    by_pool: Dict[int, List[str]] = {}
    if wanted:
        rows = (db.query(StockPoolStock.pool_id, StockPoolStock.stock_code)
                .filter(StockPoolStock.pool_id.in_(wanted))
                .order_by(StockPoolStock.pool_id, StockPoolStock.stock_code).all())
        for pool_id, code in rows:
            by_pool.setdefault(int(pool_id), []).append(str(code))

    pools = []
    for pid in wanted:
        counts = _bucket_counts(by_pool.get(pid, []), status_by_code,
                                codes_with_events)
        counts.update({"pool_id": pid, "pool_name": names.get(pid) or "池 %s" % pid})
        pools.append(counts)

    # 持仓（各会话最新快照 quantity>0）：不在池内时单列，避免漏看真实持仓缺口。
    from sqlalchemy import func

    from core.models import LiveAccountPositionSnapshot, LiveAccountSnapshot

    latest_ids = [
        sid for (_, sid) in
        db.query(LiveAccountSnapshot.live_session_id,
                 func.max(LiveAccountSnapshot.id))
        .group_by(LiveAccountSnapshot.live_session_id).all()
    ]
    holdings: List[str] = []
    if latest_ids:
        rows = (db.query(LiveAccountPositionSnapshot.stock_code)
                .filter(LiveAccountPositionSnapshot.snapshot_id.in_(latest_ids),
                        LiveAccountPositionSnapshot.quantity > 0)
                .distinct().all())
        holdings = sorted(str(row[0]) for row in rows)
    in_pools = {code for bucket in by_pool.values() for code in bucket}
    holdings_only = [code for code in holdings if code not in in_pools]

    totals = {"total": 0, "covered": 0, "no_events": 0, "failed": 0, "missing": 0}
    for item in pools:
        for key in totals:
            totals[key] += int(item[key])
    holdings_counts = _bucket_counts(holdings_only, status_by_code,
                                     codes_with_events)
    for key in totals:
        totals[key] += int(holdings_counts[key])

    scheduler = sync_settings()
    try:
        from core.tq import gate as tq_gate
        policy = tq_gate.evaluate_policy()
        next_open = policy.get("next_p2_open_at")
        scheduler_view = {
            "enabled": bool(scheduler["enabled"]),
            "daily_at": scheduler["daily_at"],
            "p2_allowed": bool(policy.get("p2_allowed")),
            "next_p2_open_at": (next_open.isoformat()
                                if hasattr(next_open, "isoformat") else None),
        }
    except Exception:  # noqa: BLE001 — 闸状态读不到不影响只读统计
        logger.debug("TQ 闸状态读取失败（覆盖统计降级）", exc_info=True)
        scheduler_view = {
            "enabled": bool(scheduler["enabled"]),
            "daily_at": scheduler["daily_at"],
            "p2_allowed": None,
            "next_p2_open_at": None,
        }

    return {
        "pools": pools,
        "holdings": dict(holdings_counts, codes=holdings_only),
        "totals": totals,
        "last_sync": read_sync_state(),
        "scheduler": scheduler_view,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


class CorporateActionSyncScheduler:
    """进程内每交易日自动补公司行为（daemon 线程，60s 轮询）。

    只在 P2 放行窗内跑（默认 15:20，晚于收盘放行 15:15）；范围 = 全部股票池
    ∪ 回测涉及池 ∪ 最新快照持仓，再过滤 ``missing_coverage_codes``，因此
    「已覆盖的票跳过、只补缺失」天然幂等。单票失败已在
    ``sync_corporate_actions`` 内记 ``failed`` 且当次不重试，下一轮自然重试；
    被 TQ 闸拒绝按短退避等待，不突破闸门。
    """

    def __init__(self, *, cfg_provider: Optional[Callable[[], dict]] = None,
                 session_factory=None,
                 now_fn: Optional[Callable[[], datetime]] = None,
                 interval_seconds: float = 60.0):
        self._cfg = cfg_provider or _load_sync_section
        if session_factory is None:
            from core.db import SessionLocal
            session_factory = SessionLocal
        self._session_factory = session_factory
        if now_fn is None:
            from core.engine.live.timing import now_shanghai
            now_fn = now_shanghai
        self._now = now_fn
        self._interval = max(float(interval_seconds), 5.0)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._last_attempt_at: Optional[datetime] = None
        self._last_attempt_ok = False
        self._next_attempt_at: Optional[datetime] = None

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="corporate-action-sync-scheduler",
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
                logger.exception("公司行为自动同步调度异常（下轮重试）")
            if self._stop.wait(self._interval):
                break

    def maybe_run(self) -> Optional[dict]:
        """单次检查（测试直接调用）：未启用/未到点/非交易日/已完成 → None。"""
        settings = sync_settings(self._cfg())
        if not settings["enabled"]:
            return None
        now = self._now()
        if (now.hour, now.minute) < settings["daily_at_hm"]:
            return None
        from core.tq.gate import is_trading_day

        if not is_trading_day(now.date()):
            return None
        today = now.date().isoformat()
        if read_sync_state().get("last_success_date") == today:
            return None
        if (self._last_attempt_ok and self._last_attempt_at is not None
                and self._last_attempt_at.date() == now.date()):
            return None
        if self._next_attempt_at is not None and now < self._next_attempt_at:
            return None

        from core.tq import gate as tq_gate

        if not tq_gate.evaluate_policy().get("p2_allowed"):
            self._next_attempt_at = now + timedelta(minutes=GATE_RETRY_MINUTES)
            logger.info("公司行为自动同步等待 P2 放行（%s），%d 分钟后重试",
                        now.strftime("%Y-%m-%d %H:%M"), GATE_RETRY_MINUTES)
            return {"status": "deferred", "reason": "p2_blocked"}

        if not try_acquire_sync():
            self._next_attempt_at = now + timedelta(minutes=GATE_RETRY_MINUTES)
            logger.info("公司行为同步正在进行中，本轮让路，%d 分钟后重试",
                        GATE_RETRY_MINUTES)
            return {"status": "deferred", "reason": "busy"}

        from core.tq.gate import P2, TdxTaskBlocked, tdx_priority

        try:
            with self._session_factory() as db:
                scope = resolve_backfill_codes(db, pool_ids=all_pool_ids(db))
                missing = missing_coverage_codes(db, scope["codes"])
                scope_view = {
                    "pool_ids": scope["pool_ids"],
                    "pool_counts": {str(k): len(v)
                                    for k, v in scope["by_pool"].items()},
                    "holdings": len(scope["holdings"]),
                    "missing": len(missing),
                }
                if not missing:
                    record_sync_state("scheduled", "skipped", scope=scope_view,
                                      message="无缺失代码", now=now)
                    self._last_attempt_at = now
                    self._last_attempt_ok = True
                    return {"status": "skipped", "missing": 0}
                logger.info("开始公司行为自动同步：缺失 %d 只（%s）",
                            len(missing), now.strftime("%Y-%m-%d %H:%M"))
                try:
                    with tdx_priority(P2):
                        result = sync_corporate_actions(db, missing)
                except TdxTaskBlocked:
                    self._next_attempt_at = now + timedelta(
                        minutes=GATE_RETRY_MINUTES)
                    logger.info("公司行为自动同步被 TQ 闸拒绝（交易时段），"
                                "%d 分钟后重试", GATE_RETRY_MINUTES)
                    return {"status": "deferred", "reason": "gate_blocked"}
                status = "failed" if result["failed"] else "success"
                record_sync_state("scheduled", status, result=result,
                                  scope=scope_view, now=now)
                self._last_attempt_at = now
                self._last_attempt_ok = status == "success"
                if status == "failed":
                    self._next_attempt_at = now + timedelta(
                        minutes=settings["retry_cooldown_minutes"])
                logger.info("公司行为自动同步完成：requested=%d added=%d "
                            "updated=%d failed=%d", result["requested"],
                            result["added"], result["updated"],
                            len(result["failed"]))
                return dict(result, status=status)
        finally:
            release_sync()


def _load_config() -> dict:
    from core.config import load_config
    return load_config()


def _load_sync_section() -> dict:
    """调度默认配置来源：只取 ``corporate_action_sync`` 段（缺省回退默认值）。"""
    try:
        cfg = _load_config()
    except Exception:  # noqa: BLE001 — 配置读不到按默认值跑
        return {}
    section = cfg.get("corporate_action_sync") if isinstance(cfg, dict) else None
    return section if isinstance(section, dict) else {}

