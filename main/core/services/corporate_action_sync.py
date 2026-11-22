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
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

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
