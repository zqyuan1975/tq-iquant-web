# -*- coding: utf-8 -*-
"""内置宽库构建（阶段二）· 编排层。

阶段机（incremental / full 共用）：

``universe → profile → fin → dividend → sector → daily → publish``

* 每个批次独立事务：数据行 + ``selection_wh_build_progress`` 一起提交；
  失败批次整批回滚，已完成的批次保留，续跑只重做非 ``done`` 批次。
* ``publish`` 是唯一改变快照可见性的步骤：重算 close/波动率/回撤，然后
  单事务切换 ``selection_wh_meta.as_of``。因此失败时旧 as_of 的快照语义
  不变（新增行只补缺口或追加未来日线，不改写旧 as_of 已覆盖代码的既有值）。
* 所有 TQ 取数都在 P2 优先级下执行；P2 未放行会抛 ``TdxTaskBlocked``，
  运行标记 blocked，由调度器在 15:45/16:15/16:45 退避重试。
* 日志落 ``<app_root>/logs/selection_wide_build.log``，不写请求线程。
"""
from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from sqlalchemy import update
from sqlalchemy.orm import Session

from core.config import app_root
from core.models.selection import SelectionWarehouseStock
from core.selection import wide_sources as sources
from core.selection import wide_store as store

DEFAULT_DAILY_START = date(2023, 12, 1)
DEFAULT_MODE = "incremental"
STATE_FILE_NAME = "selection_wide_build_state.json"
LOG_FILE_NAME = "selection_wide_build.log"
SOURCE_PATH = "builtin:selection-wide-build"

PHASES = ("universe", "profile", "fin", "dividend", "sector", "daily", "publish")
CHUNK_CODES = 200

logger = logging.getLogger("selection_wide_build")
if not logger.handlers:
    try:
        log_dir = app_root() / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_dir / LOG_FILE_NAME, encoding="utf-8")
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    except OSError:
        logger.addHandler(logging.NullHandler())

_BUILD_LOCK = threading.Lock()


# ------------------------------------------------------------------ helpers
def _slot_batches(universe: Sequence[str], need: Callable[[str], bool],
                  prefix: str) -> List[tuple]:
    """按**宇宙固定分片位**生成批次 ``(key, codes)``。

    键里的编号是分片在 universe 中的固定位置（``i // CHUNK_CODES``），
    不随本轮「待补票数」变化。这样中断续跑时剩余票仍落在原分片号上，
    不会因为重新编号撞上已 done 的键而被误跳过。
    """
    out = []
    for i in range(0, len(universe), CHUNK_CODES):
        batch = [code for code in universe[i:i + CHUNK_CODES] if need(code)]
        if batch:
            out.append(("%s:%04d" % (prefix, i // CHUNK_CODES), batch))
    return out


def today_run_id(now: Optional[datetime] = None) -> str:
    from core.tq.gate import now_shanghai

    moment = now or now_shanghai()
    return "wide-%s" % moment.strftime("%Y%m%d")


def resolve_as_of(tq, explicit: Optional[date] = None) -> date:
    if explicit is not None:
        return explicit
    from core.tq.gate import now_shanghai

    today = now_shanghai().date()
    try:
        days = tq.get_trading_dates(
            "SH", start=(today - timedelta(days=20)).strftime("%Y%m%d"),
            end=today.strftime("%Y%m%d")) or []
    except Exception:  # noqa: BLE001 — 日历取不到时按自然日兜底
        return today
    parsed = sorted(d for d in (sources.to_date(x) for x in days) if d and d <= today)
    return parsed[-1] if parsed else today


def state_path() -> Path:
    return app_root() / "data" / STATE_FILE_NAME


def read_state(path: Optional[Path] = None) -> Dict[str, Any]:
    target = Path(path) if path else state_path()
    try:
        return json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def _write_state(data: Dict[str, Any], path: Optional[Path] = None) -> None:
    target = Path(path) if path else state_path()
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=1, sort_keys=True),
                   encoding="utf-8")
    tmp.replace(target)


def record_state(*, run_id: str, mode: str, status: str, as_of: Optional[date],
                 result: Optional[Dict[str, Any]] = None,
                 trigger: str = "manual", message: str = "",
                 slot: Optional[str] = None,
                 now: Optional[datetime] = None,
                 path: Optional[Path] = None) -> Dict[str, Any]:
    from core.tq.gate import now_shanghai

    moment = now or now_shanghai()
    prev = read_state(path)
    attempts = dict(prev.get("attempts") or {})
    day_key = moment.date().isoformat()
    #: 调度触发按**计划档位**记账（15:30/15:45/16:15/16:45），不用实际墙钟。
    #: 否则 16:50 执行的 16:45 档会记成 "16:50"，下一分钟再判 16:45 不在
    #: attempts 里 → 每分钟重试一次，退避语义失效。
    slot_key = slot or moment.strftime("%H:%M")
    slots = list(attempts.get(day_key) or [])
    if slot_key not in slots:
        slots.append(slot_key)
        attempts[day_key] = slots
    attempts = {k: v for k, v in sorted(attempts.items())[-7:]}
    payload: Dict[str, Any] = {
        **prev,
        "run_id": run_id,
        "mode": mode,
        "status": status,
        "trigger": trigger,
        "message": message,
        "last_run_at": moment.isoformat(timespec="seconds"),
        "attempts": attempts,
    }
    if as_of is not None:
        payload["as_of"] = as_of.isoformat()
    if result is not None:
        payload["result"] = result
    if status == "succeeded":
        payload["last_success_date"] = moment.date().isoformat()
    _write_state(payload, path)
    return payload


def try_acquire_build() -> bool:
    return _BUILD_LOCK.acquire(blocking=False)


def release_build() -> None:
    if _BUILD_LOCK.locked():
        try:
            _BUILD_LOCK.release()
        except RuntimeError:
            pass


def build_running() -> bool:
    return _BUILD_LOCK.locked()


# ------------------------------------------------------------------ build
def run_build(*, mode: str = DEFAULT_MODE,
              run_id: Optional[str] = None,
              as_of: Optional[date] = None,
              daily_start: Optional[date] = None,
              session_factory: Optional[Callable[[], Session]] = None,
              tq: Any = None,
              trigger: str = "manual",
              slot: Optional[str] = None) -> Dict[str, Any]:
    """执行一次构建；返回结构化结果（不抛业务异常，P2 阻塞也返回状态）。"""
    if mode not in ("incremental", "full"):
        raise ValueError("mode 只能是 incremental 或 full")
    from core.db import SessionLocal
    from core.tq.gate import P2, TdxTaskBlocked, tdx_priority
    from core.tq.data import TQData

    factory = session_factory or SessionLocal
    client = tq or TQData()
    now = datetime.now()
    run_id = run_id or today_run_id()
    as_of = resolve_as_of(client, as_of)
    start_day = daily_start or DEFAULT_DAILY_START
    db = factory()
    started = time.monotonic()
    result: Dict[str, Any] = {
        "run_id": run_id, "mode": mode, "trigger": trigger,
        "as_of": as_of.isoformat(), "daily_start": start_day.isoformat(),
        "status": "running", "phases": {}, "counts": {},
    }
    logger.info("宽库构建开始 run=%s mode=%s as_of=%s", run_id, mode, as_of)
    try:
        done = store.done_batches(db, run_id)
        universe: List[str] = []

        def _phase(name: str, fn: Callable[[], Dict[str, Any]]) -> None:
            t0 = time.monotonic()
            try:
                info = fn()
            except Exception as exc:  # noqa: BLE001 — 统一落阶段信息后上抛
                result["phases"][name] = {
                    "status": "failed", "error": str(exc)[:500],
                    "seconds": round(time.monotonic() - t0, 2)}
                raise
            result["phases"][name] = {
                "status": "done", "seconds": round(time.monotonic() - t0, 2),
                **info}
            logger.info("宽库构建阶段完成 %s %s", name, info)

        with tdx_priority(P2):
            def phase_universe() -> Dict[str, Any]:
                nonlocal universe
                universe = sources.fetch_universe(client)
                existing = store.existing_codes(db)
                missing = [c for c in universe if c not in existing]
                store.record_progress(
                    db, run_id=run_id, mode=mode, phase="universe",
                    batch_key="all", status="done", total=len(universe),
                    done=len(missing),
                    payload={"universe": len(universe),
                             "existing": len(existing),
                             "missing": len(missing)})
                db.commit()
                return {"universe": len(universe), "existing": len(existing),
                        "missing": len(missing)}

            def phase_profile() -> Dict[str, Any]:
                have = set() if mode == "full" else store.existing_codes(db)
                slots = _slot_batches(universe, lambda c: c not in have, "profile")
                if slots:
                    store.plan_batches(
                        db, run_id=run_id, mode=mode, phase="profile",
                        batches=[(key, len(b)) for key, b in slots])
                    db.commit()
                fetched = 0
                for key, batch in slots:
                    if ("profile", key) in done:
                        continue
                    profiles = sources.fetch_profiles(client, batch)
                    gp = sources.fetch_gp16(client, batch, as_of)
                    rows = []
                    for item in profiles:
                        code = item["code"]
                        rows.append({
                            "code": code,
                            "name": item.get("name"),
                            "exchange": code.split(".")[-1] if "." in code else None,
                            "more": item.get("more"),
                            "info": item.get("info"),
                            "gp": gp.get(code),
                        })
                    store.upsert_stocks(db, rows)
                    store.record_progress(
                        db, run_id=run_id, mode=mode, phase="profile",
                        batch_key=key, status="done", total=len(batch),
                        done=len(rows),
                        payload={"codes": len(batch), "rows": len(rows)})
                    db.commit()
                    fetched += len(rows)
                return {"batches": len(slots), "rows": fetched}

            def phase_fin() -> Dict[str, Any]:
                have = set() if mode == "full" else store.fin_codes(db)
                slots = _slot_batches(universe, lambda c: c not in have, "fin")
                if slots:
                    store.plan_batches(
                        db, run_id=run_id, mode=mode, phase="fin",
                        batches=[(key, len(b)) for key, b in slots])
                    db.commit()
                fetched = 0
                for key, batch in slots:
                    if ("fin", key) in done:
                        continue
                    rows = sources.fetch_financials(client, batch)
                    store.upsert_fin(db, rows)
                    for row in rows:
                        db.execute(update(SelectionWarehouseStock)
                                   .where(SelectionWarehouseStock.code == row["code"])
                                   .values(fin_periods=row["n_periods"],
                                           fin_last_tag=row["last_tag_time"]))
                    store.record_progress(
                        db, run_id=run_id, mode=mode, phase="fin",
                        batch_key=key, status="done", total=len(batch),
                        done=len(rows),
                        payload={"codes": len(batch), "rows": len(rows)})
                    db.commit()
                    fetched += len(rows)
                return {"batches": len(slots), "rows": fetched}

            def phase_dividend() -> Dict[str, Any]:
                have = set() if mode == "full" else store.dividend_codes(db)
                slots = _slot_batches(universe, lambda c: c not in have, "dividend")
                if slots:
                    store.plan_batches(
                        db, run_id=run_id, mode=mode, phase="dividend",
                        batches=[(key, len(b)) for key, b in slots])
                    db.commit()
                fetched = 0
                for key, batch in slots:
                    if ("dividend", key) in done:
                        continue
                    rows = sources.fetch_dividends_from_actions(db, batch, as_of)
                    store.upsert_dividend(db, rows)
                    store.record_progress(
                        db, run_id=run_id, mode=mode, phase="dividend",
                        batch_key=key, status="done", total=len(batch),
                        done=len(rows),
                        payload={"codes": len(batch), "rows": len(rows)})
                    db.commit()
                    fetched += len(rows)
                return {"batches": len(slots), "rows": fetched}

            def phase_sector() -> Dict[str, Any]:
                new_codes = [code for (code,) in
                             db.query(SelectionWarehouseStock.code)
                             .filter(SelectionWarehouseStock.sector_name.is_(None))
                             .all()]
                key = "sector:all"
                if ("sector", key) in done:
                    return {"patched": 0, "membership": 0}
                mapping = sources.fetch_sector(client, new_codes)
                for code, item in mapping.items():
                    db.execute(update(SelectionWarehouseStock)
                               .where(SelectionWarehouseStock.code == code)
                               .values(sector_code=item.get("sector_code"),
                                       sector_name=item.get("sector_name")))
                db.commit()
                membership = store.refresh_sector_membership(db, as_of)
                store.record_progress(
                    db, run_id=run_id, mode=mode, phase="sector",
                    batch_key=key, status="done", total=len(new_codes),
                    done=len(mapping),
                    payload={"patched": len(mapping), "membership": membership})
                db.commit()
                return {"patched": len(mapping), "membership": membership}

            def phase_daily() -> Dict[str, Any]:
                before = store.table_counts(db)["daily_count"]
                codes = [code for (code,) in
                         db.query(SelectionWarehouseStock.code)
                         .order_by(SelectionWarehouseStock.code).all()]
                marks = store.daily_watermarks(db)
                planned = []
                work = []
                for i in range(0, len(codes), CHUNK_CODES):
                    slot = i // CHUNK_CODES
                    by_begin: Dict[date, List[str]] = {}
                    for code in codes[i:i + CHUNK_CODES]:
                        if mode == "full":
                            begin = start_day
                        else:
                            last = marks.get(code)
                            begin = (last + timedelta(days=1)) if last else start_day
                        if begin > as_of:
                            continue
                        by_begin.setdefault(begin, []).append(code)
                    for begin, batch in sorted(by_begin.items()):
                        key = "daily:%s:%04d" % (begin.strftime("%Y%m%d"), slot)
                        planned.append((key, len(batch)))
                        work.append((key, batch, begin))
                if planned:
                    store.plan_batches(db, run_id=run_id, mode=mode,
                                       phase="daily", batches=planned)
                    db.commit()
                fetched = 0
                batches = 0
                for key, batch, begin in work:
                    if ("daily", key) in done:
                        continue
                    rows = sources.fetch_daily(client, batch, begin, as_of)
                    inserted = store.insert_daily(db, rows)
                    store.record_progress(
                        db, run_id=run_id, mode=mode, phase="daily",
                        batch_key=key, status="done", total=len(batch),
                        done=len(rows),
                        payload={"codes": len(batch), "fetched": len(rows),
                                 "inserted": inserted})
                    db.commit()
                    fetched += len(rows)
                    batches += 1
                after = store.table_counts(db)["daily_count"]
                return {"groups": len({begin for _, _, begin in work}),
                        "batches": batches,
                        "rows_fetched": fetched, "rows_added": after - before}

            _phase("universe", phase_universe)
            _phase("profile", phase_profile)
            _phase("fin", phase_fin)
            _phase("dividend", phase_dividend)
            _phase("sector", phase_sector)
            _phase("daily", phase_daily)

        def phase_publish() -> Dict[str, Any]:
            metrics = store.recompute_metrics(db, as_of)
            counts = store.table_counts(db)
            store.publish_meta(db, as_of=as_of, source_path=SOURCE_PATH,
                               counts=counts)
            store.record_progress(
                db, run_id=run_id, mode=mode, phase="publish",
                batch_key="meta", status="done", total=1, done=1,
                payload={**counts, "metrics": metrics,
                         "daily": store.daily_bounds(db)})
            db.commit()
            return {**counts, "metrics": metrics}

        _phase("publish", phase_publish)
        result["status"] = "succeeded"
        result["counts"] = store.table_counts(db)
        result["daily"] = store.daily_bounds(db)
    except Exception as exc:  # noqa: BLE001 — 失败也要落状态给页面
        db.rollback()
        name = type(exc).__name__
        blocked = name == "TdxTaskBlocked"
        result["status"] = "blocked" if blocked else "failed"
        result["error"] = "%s: %s" % (name, str(exc)[:300])
        logger.warning("宽库构建结束 run=%s status=%s error=%s",
                       run_id, result["status"], result["error"])
    finally:
        result["seconds"] = round(time.monotonic() - started, 2)
        try:
            record_state(run_id=run_id, mode=mode, status=result["status"],
                         as_of=as_of, result=result, trigger=trigger,
                         slot=slot, message=result.get("error", ""))
        except Exception:  # noqa: BLE001 — 状态文件失败不掩盖构建结果
            logger.exception("宽库构建状态落盘失败")
        try:
            db.close()
        except Exception:  # noqa: BLE001
            pass
    logger.info("宽库构建完成 run=%s status=%s seconds=%s",
                run_id, result["status"], result["seconds"])
    return result


# ------------------------------------------------------------------ status
def build_status(db: Optional[Session] = None) -> Dict[str, Any]:
    from core.db import SessionLocal

    own = db is None
    session = db or SessionLocal()
    try:
        meta = store.meta_row(session)
        latest = store.latest_run_id(session)
        run_id = latest or today_run_id()
        rows = store.run_progress(session, run_id)
        phases: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            item = phases.setdefault(row.phase, {
                "phase": row.phase, "batches": 0, "done_batches": 0,
                "failed_batches": 0, "running_batches": 0,
                "rows_done": 0, "errors": [],
            })
            item["batches"] += 1
            if row.status == "done":
                item["done_batches"] += 1
                item["rows_done"] += int(row.done or 0)
            elif row.status == "failed":
                item["failed_batches"] += 1
                if row.error and len(item["errors"]) < 3:
                    item["errors"].append(row.error)
            elif row.status == "running":
                item["running_batches"] += 1
        for name, item in phases.items():
            item["percent"] = round(
                100.0 * item["done_batches"] / max(item["batches"], 1), 1)
        counts = store.table_counts(session)
        return {
            "ready": bool(meta and meta.as_of),
            "as_of": meta.as_of.isoformat() if meta and meta.as_of else None,
            "source_path": meta.source_path if meta else None,
            "imported_at": meta.imported_at.isoformat(timespec="seconds")
            if meta and meta.imported_at else None,
            "stock_count": meta.stock_count if meta else counts["stock_count"],
            "fin_count": meta.fin_count if meta else counts["fin_count"],
            "dividend_count": meta.dividend_count if meta else counts["dividend_count"],
            "counts": counts,
            "daily": store.daily_bounds(session),
            "running": build_running(),
            "run_id": run_id,
            "latest_run_id": latest,
            "phases": list(phases.values()),
            "last_build": read_state(),
        }
    finally:
        if own:
            session.close()


# ------------------------------------------------------------------ scheduler
def build_settings(cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if cfg is None:
        from core.config import load_config

        cfg = load_config()
    raw = (cfg or {}).get("selection_wide_build") or {}

    def _hm(value: Any, default: str) -> int:
        text = str(value or default)
        try:
            hour, minute = text.split(":")[:2]
            return int(hour) * 60 + int(minute)
        except (TypeError, ValueError):
            hour, minute = default.split(":")[:2]
            return int(hour) * 60 + int(minute)

    slots = raw.get("retry_slots") or ["15:45", "16:15", "16:45"]
    return {
        "enabled": bool(raw.get("enabled", False)),
        "daily_at_hm": _hm(raw.get("daily_at"), "15:30"),
        "retry_hm": sorted({_hm(v, "15:45") for v in slots}),
        "daily_start": str(raw.get("daily_start") or DEFAULT_DAILY_START),
    }


class SelectionWideBuildScheduler:
    """交易日 15:30 触发；P2 未放行时按 15:45/16:15/16:45 退避。

    ``run_id=wide-YYYYMMDD`` 跨重启判重：状态文件记录 ``last_success_date``，
    数据库 progress 中 ``publish`` 批次 done 也视为当日已完成。
    """

    def __init__(self, cfg_provider: Optional[Callable[[], Dict[str, Any]]] = None,
                 session_factory: Optional[Callable[[], Session]] = None,
                 now_fn: Optional[Callable[[], datetime]] = None,
                 interval_seconds: float = 60.0):
        from core.db import SessionLocal

        if cfg_provider is None:
            from core.config import load_config

            cfg_provider = load_config
        if now_fn is None:
            from core.tq.gate import now_shanghai

            now_fn = now_shanghai
        self._cfg = cfg_provider
        self._session_factory = session_factory or SessionLocal
        self._now = now_fn
        self._interval = max(float(interval_seconds), 10.0)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(
                target=self._loop, name="selection-wide-build-scheduler",
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
            except Exception:  # noqa: BLE001 — 调度异常不得拖垮进程
                logger.exception("宽库构建调度异常（下轮重试）")
            if self._stop.wait(self._interval):
                break

    def maybe_run(self) -> Optional[Dict[str, Any]]:
        from core.tq.gate import is_trading_day

        settings = build_settings(self._cfg())
        if not settings["enabled"]:
            return None
        now = self._now()
        if not is_trading_day(now.date()):
            return None
        minutes = now.hour * 60 + now.minute
        slot = None
        for candidate in [settings["daily_at_hm"], *settings["retry_hm"]]:
            if minutes >= candidate:
                slot = candidate
        if slot is None:
            return None
        slot_text = "%02d:%02d" % divmod(slot, 60)
        run_id = today_run_id(now)
        state = read_state()
        if state.get("last_success_date") == now.date().isoformat():
            return None
        if slot_text in (state.get("attempts", {}).get(now.date().isoformat()) or []):
            return None
        db = self._session_factory()
        try:
            done = store.done_batches(db, run_id)
        finally:
            db.close()
        if ("publish", "meta") in done:
            return None
        if not try_acquire_build():
            return None
        try:
            result = run_build(
                mode="incremental", run_id=run_id, trigger="scheduled",
                slot=slot_text)
            return result
        finally:
            release_build()
