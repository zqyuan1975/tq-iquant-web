"""盘中分钟数据下载器（7709 直连，只服务当前实盘会话的标的）。

职责：计划（缺日）→ 分页拉取 → 写自有存储 → 登记覆盖台账；带全局限频、
失败退避重试、主站隔离切换与可随时停止的后台线程。

生命周期：由实盘会话 ``start_session`` 启动、``stop_session`` 停止（接线在
下一步落地）；本模块自身也可独立 ``start()/stop()``。

增量与缺日口径复用 ``kline_coverage_ledger``（source=``tdx7709``）。注意该
台账以文件指纹判断可信度，而分钟存储没有「单一来源文件」语义，因此这里
只复用其**日期段存储/合并**规则，缺日由 :func:`planned_missing_days` 按
覆盖段与 ``trading_days`` 计算，不依赖指纹。

今天的覆盖不在盘中登记：每次轮询都会抓最新一页，收盘后（>=15:00）才把
当日记为已覆盖，避免把「只抓到下午」误当成整日完整。
"""
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from core.services import kline_coverage_ledger as ledger

from .hosts import HostPool, load_host_pool
from .protocol import (
    MAX_PAGE, BarRecord, normalize_code, records_to_bars,
)
from .store import MinuteStore

logger = logging.getLogger(__name__)

SOURCE = "tdx7709"
PERIOD = "1m"
DEFAULT_LOOKBACK_TRADING_DAYS = 15
#: 收盘后（含边界）才把当天登记为已覆盖。
_CLOSE_TIME = dt_time(15, 0)
#: 默认拉取窗口：收盘后仍多留 5 分钟，保证「收盘后那一轮」能落在 15:00 之后。
DEFAULT_WINDOW_START = "09:15"
DEFAULT_WINDOW_END = "15:05"


class DownloadError(Exception):
    """单只标的本轮下载失败（进入下一轮重试）。"""


class DownloadCancelled(Exception):
    """线程收到停止信号。"""


@dataclass
class DownloadConfig:
    lookback_trading_days: int = DEFAULT_LOOKBACK_TRADING_DAYS
    period: str = PERIOD
    interval_seconds: float = 60.0
    page_size: int = MAX_PAGE
    #: 全局请求速率（不是每连接），所有连接共用同一个限频器。
    #: ``None`` = 按池规模自适应；显式值 = 人工覆盖（0 仍表示不限频）。
    max_req_per_second: Optional[float] = None
    #: 并发连接数；`None` = 按池规模自适应；1 = 单线程顺序。
    connections: Optional[int] = None
    #: 按池规模自适应开关；关闭时退回「显式值或保守默认 4/12」。
    adaptive_enabled: bool = True
    #: 自适应上下限夹取（防止映射把连接/限频推出可接受范围）。
    connections_max: int = 12
    max_req_per_second_max: float = 36.0
    #: 利用率观测与自动抬档：连续 N 轮顶格/超时 → 告警一次并抬一档。
    high_streak_threshold: int = 3
    high_watermark: float = 0.9
    max_raise_tiers: int = 1
    retry_times: int = 2
    retry_backoff_seconds: float = 1.5
    request_timeout_seconds: float = 5.0
    failure_threshold: int = 3
    isolation_seconds: float = 60.0
    #: 每个交易日的拉取窗口（含两端，``HH:MM``）。窗口外不请求、不写库。
    window_start: str = DEFAULT_WINDOW_START
    window_end: str = DEFAULT_WINDOW_END

    @classmethod
    def from_section(cls, section: Optional[dict]) -> "DownloadConfig":
        section = section if isinstance(section, dict) else {}
        return cls(
            lookback_trading_days=_int(
                section.get("lookback_trading_days"),
                DEFAULT_LOOKBACK_TRADING_DAYS),
            period=str(section.get("period") or PERIOD),
            interval_seconds=_float(section.get("interval_seconds"), 60.0),
            page_size=min(max(_int(section.get("page_size"), MAX_PAGE), 1),
                          MAX_PAGE),
            max_req_per_second=_optional_float(
                section.get("max_req_per_second")),
            connections=_clamp_connections(section.get("connections")),
            adaptive_enabled=_bool(section.get("adaptive_enabled"), True),
            connections_max=min(max(
                _int(section.get("connections_max"), 12), 1), 16),
            max_req_per_second_max=_float(
                section.get("max_req_per_second_max"), 36.0),
            high_streak_threshold=max(
                _int(section.get("high_streak_threshold"), 3), 1),
            high_watermark=_ratio(section.get("high_watermark"), 0.9),
            max_raise_tiers=max(_int(section.get("max_raise_tiers"), 1), 0),
            retry_times=max(_int(section.get("retry_times"), 2), 0),
            # 0 = 不等待退避 / 不隔离，均为合法语义（见 _wait 与 HostPool），
            # 用 _float0 保留；缺省/None/空串才回落默认。
            retry_backoff_seconds=_float0(
                section.get("retry_backoff_seconds"), 1.5),
            request_timeout_seconds=_float(
                section.get("request_timeout_seconds"), 5.0),
            failure_threshold=max(_int(section.get("failure_threshold"), 3), 1),
            # HostPool 对 isolation_seconds 做 max(..., 0.0)，0 = 不隔离是合法值。
            isolation_seconds=_float0(
                section.get("isolation_seconds"), 60.0),
            window_start=str(
                section.get("window_start") or DEFAULT_WINDOW_START),
            window_end=str(section.get("window_end") or DEFAULT_WINDOW_END),
        )


#: 池规模 → (connections, rps) 确定性映射。每档按「单连接约 3 req/s」的实测
#: 吞吐外推；``single_host_sustainable`` 指 60s 轮询间隔内能否跑完全池。
SIZING_TIERS: Tuple[dict, ...] = (
    {"max_pool": 150, "label": "≤150", "connections": 2,
     "max_req_per_second": 6.0, "single_host_sustainable": True,
     "note": "小池不浪费连接"},
    {"max_pool": 400, "label": "151–400", "connections": 4,
     "max_req_per_second": 12.0, "single_host_sustainable": True,
     "note": "251 只实测档（11.95 req/s 顶格）"},
    {"max_pool": 800, "label": "401–800", "connections": 6,
     "max_req_per_second": 18.0, "single_host_sustainable": True,
     "note": "621 只约 35s/轮，余量 <20s"},
    {"max_pool": 2000, "label": "801–2000", "connections": 8,
     "max_req_per_second": 24.0, "single_host_sustainable": False,
     "note": "2000 只约 83s/轮，单主站 60s 不可持续；建议 interval≥120s 或 2 主站分片"},
    {"max_pool": 6000, "label": "2001–6000", "connections": 10,
     "max_req_per_second": 30.0, "single_host_sustainable": False,
     "note": "5581 只约 186s/轮，必须多主站分片（≥3–4）"},
    {"max_pool": None, "label": ">6000", "connections": 12,
     "max_req_per_second": 36.0, "single_host_sustainable": False,
     "note": "已达连接/限频上限，需分片+扩间隔"},
)


def _tier_for_pool(pool_size: int) -> int:
    size = max(int(pool_size or 0), 0)
    for idx, tier in enumerate(SIZING_TIERS):
        limit = tier["max_pool"]
        if limit is None or size <= limit:
            return idx
    return len(SIZING_TIERS) - 1


def sizing_for_pool(pool_size: int, *, connections_max: int = 12,
                    max_req_per_second_max: float = 36.0,
                    tier_bonus: int = 0) -> dict:
    """确定性「池规模 → connections / rps」映射（含上下限夹取）。"""
    base_idx = _tier_for_pool(pool_size)
    bonus = max(int(tier_bonus or 0), 0)
    idx = min(base_idx + bonus, len(SIZING_TIERS) - 1)
    tier = SIZING_TIERS[idx]
    conn = min(max(int(tier["connections"]), 1),
               max(int(connections_max), 1), 16)
    rps = min(max(float(tier["max_req_per_second"]), 1.0),
              max(float(max_req_per_second_max), 1.0))
    return {
        "tier_index": idx,
        "base_tier_index": base_idx,
        "tier": tier["label"],
        "connections": conn,
        "max_req_per_second": rps,
        "single_host_sustainable": bool(tier["single_host_sustainable"]),
        "note": tier["note"],
    }


def sizing_table(*, connections_max: int = 12,
                 max_req_per_second_max: float = 36.0) -> List[dict]:
    """完整建议配置表（供前端按池规模对照）。"""
    out: List[dict] = []
    for idx, tier in enumerate(SIZING_TIERS):
        out.append({
            "tier_index": idx,
            "tier": tier["label"],
            "max_pool": tier["max_pool"],
            "connections": min(max(int(tier["connections"]), 1),
                               max(int(connections_max), 1), 16),
            "max_req_per_second": min(
                max(float(tier["max_req_per_second"]), 1.0),
                max(float(max_req_per_second_max), 1.0)),
            "single_host_sustainable": bool(tier["single_host_sustainable"]),
            "note": tier["note"],
        })
    return out


class RateLimiter:
    """最简令牌间隔限频：保证相邻请求间隔 >= 1/qps。"""

    def __init__(self, qps: float, clock=time.monotonic,
                 sleep: Optional[Callable[[float], None]] = time.sleep) -> None:
        self.qps = float(qps or 0)
        self._clock = clock
        self._sleep = sleep
        self._interval = 1.0 / self.qps if self.qps > 0 else 0.0
        self._next = 0.0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        if self._interval <= 0:
            return
        with self._lock:
            now = self._clock()
            wait = self._next - now
            if wait > 0 and self._sleep is not None:
                self._sleep(wait)
                now += wait
            self._next = max(now, self._next) + self._interval


def planned_missing_days(code: str, start: date, end: date, *,
                         db_path=None) -> List[date]:
    """按台账覆盖段计算缺日（不依赖文件指纹）。

    ``plan_from_ledger`` 在无指纹时会把记录判为不可信且不给缺日；这里直接
    读它的 ``ranges`` / ``empty_days`` 结果，配合 ``trading_days`` 求差集。
    """
    if end < start:
        return []
    plan = ledger.plan_from_ledger(
        code, PERIOD, start, end, source=SOURCE, field=ledger.FIELD_ALL,
        db_path=db_path)
    known = set()
    expected = ledger.trading_days(start, end)
    # plan_from_ledger 返回的 ranges 是 ISO 字符串对，需要转回 date 再比较。
    ranges: List[Tuple[date, date]] = []
    for item in plan.get("ranges") or []:
        try:
            a, b = item
            ranges.append((date.fromisoformat(str(a)[:10]),
                           date.fromisoformat(str(b)[:10])))
        except (TypeError, ValueError):
            continue
    for d in expected:
        if any(a <= d <= b for a, b in ranges):
            known.add(d)
    for text in plan.get("empty_days") or []:
        try:
            known.add(date.fromisoformat(str(text)[:10]))
        except ValueError:
            continue
    return [d for d in expected if d not in known]


class MinuteDownloadService:
    """盘中循环下载器（daemon 线程，可停止）。"""

    def __init__(
        self,
        store: MinuteStore,
        config: Optional[DownloadConfig] = None,
        host_pool: Optional[HostPool] = None,
        codes_provider: Optional[Callable[[], Iterable[str]]] = None,
        ledger_db_path=None,
        request_fn: Optional[Callable] = None,
        clock: Optional[Callable[[], datetime]] = None,
        on_event: Optional[Callable[[dict], None]] = None,
    ) -> None:
        self.store = store
        self.config = config or DownloadConfig()
        self.host_pool = host_pool or load_host_pool(
            failure_threshold=self.config.failure_threshold,
            isolation_seconds=self.config.isolation_seconds)
        self.codes_provider = codes_provider
        self.ledger_db_path = ledger_db_path
        self._request = request_fn
        self._clock = clock
        self._on_event = on_event
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._round_lock = threading.Lock()
        # 自适应生效值（初始保守 4/12；首个 active 轮按池规模重算）。
        self._effective_connections = min(
            max(_int(self.config.connections, 4), 1), 16)
        initial_rps = self.config.max_req_per_second
        self._effective_rps = float(initial_rps) if initial_rps is not None \
            else 12.0
        self._raise_bonus = 0
        self._limiter = RateLimiter(self._effective_rps)
        # 客户端按「工作线程 × 主站」缓存：同一 socket 绝不被两个线程同时使用。
        self._clients: Dict[Tuple[int, str, int], object] = {}
        self._clients_lock = threading.Lock()
        self._stats_lock = threading.Lock()
        self._pool: Optional[ThreadPoolExecutor] = None
        self._pool_size = 0
        self._codes: List[str] = []
        self._last_empty_warn = 0.0
        self._stats = {"requests": 0, "failed_requests": 0, "bars_written": 0}
        self._rounds = 0
        self._last_round_at: Optional[str] = None
        self._last_summary: dict = {}
        self._last_error = ""
        # 利用率观测（每轮 actual_rps/上限、轮次耗时/interval）与自动抬档状态。
        self._util_lock = threading.Lock()
        self._util: dict = {
            "actual_rps": None,
            "rps_cap": self._effective_rps,
            "rps_ratio": None,
            "round_seconds": None,
            "interval_seconds": float(self.config.interval_seconds or 0),
            "elapsed_ratio": None,
            "high_streak": 0,
            "saturated": False,
            "alerts": 0,
            "last_alert_at": None,
            "last_alert_reason": "",
            "raise_exhausted": False,
            "last_round_at": None,
        }
        # 线程监督（会话 tick 据此判断是否需要重建）：启动时刻、异常退出原因。
        self._started_at: Optional[datetime] = None
        self._thread_error = ""
        # 交易时段门控：窗口含两端；state 为 None 时首轮评估必打一行日志。
        self._window_start = _parse_hhmm(
            self.config.window_start, dt_time(9, 15))
        self._window_end = _parse_hhmm(
            self.config.window_end, dt_time(15, 5))
        self._last_gate_state: Optional[str] = None
        # 交易日历可用性："calendar" / "fallback"（仅状态切换时记日志）。
        self._calendar_mode: Optional[str] = None
        if self._window_end < _CLOSE_TIME:
            logger.warning(
                "分钟下载窗口结束 %s 早于收盘登记时刻 %s：当日覆盖要等到"
                "次日才会登记（默认 %s 即为此保留）",
                self.config.window_end, _CLOSE_TIME,
                DEFAULT_WINDOW_END)

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    @property
    def pool_codes(self) -> List[str]:
        """下载池当前标的（6 位代码）；供覆盖缺口比对，只读快照。"""
        return list(self._codes)

    def start(self) -> bool:
        if self.running:
            return False
        try:
            self.store.ensure_schema()
        except Exception:
            logger.exception("分钟存储初始化失败，下载器不启动")
            return False
        self._stop.clear()
        self._thread_error = ""
        self._started_at = self._now()
        self._thread = threading.Thread(
            target=self._loop, name="minute-download", daemon=True)
        self._thread.start()
        logger.info(
            "分钟下载器已启动：lookback=%d 交易日 interval=%.0fs qps=%.1f "
            "connections=%d 自适应=%s 窗口=%s-%s",
            self.config.lookback_trading_days, self.config.interval_seconds,
            self._effective_rps, self._connections(),
            "on" if self.config.adaptive_enabled else "off",
            self._window_start.strftime("%H:%M"),
            self._window_end.strftime("%H:%M"))
        return True

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=max(float(timeout), 0.0))
        self._shutdown_pool()
        self._close_clients()
        self._thread = None
        logger.info("分钟下载器已停止")

    def _close_clients(self) -> None:
        with self._clients_lock:
            clients = list(self._clients.values())
            self._clients.clear()
        for client in clients:
            try:
                client.close()
            except Exception:  # pragma: no cover - 关闭失败不影响退出
                pass

    # ------------------------------------------------------------------
    # 主循环
    # ------------------------------------------------------------------

    def _loop(self) -> None:
        try:
            while not self._stop.is_set():
                started = time.monotonic()
                try:
                    summary = self.run_once()
                except DownloadCancelled:
                    break
                except Exception:
                    logger.exception("分钟下载轮次异常（下一轮继续）")
                else:
                    elapsed = time.monotonic() - started
                    if summary.get("codes"):
                        # 每轮一行：用于人工核对「首轮回溯 → 次轮增量」是否按预期。
                        logger.info(
                            "分钟下载轮次 #%d：codes=%d requests=%d bars=%d "
                            "failures=%d empty_days=%d 用时=%.1fs",
                            self._rounds, summary["codes"], summary["requests"],
                            summary["bars"], summary["failures"],
                            summary["empty_days"], elapsed)
                elapsed = time.monotonic() - started
                self._wait(self.config.interval_seconds - elapsed)
        except BaseException as exc:  # noqa: BLE001 — 线程退出必须可观测
            self._thread_error = "%s: %s" % (type(exc).__name__, exc)
            logger.error(
                "分钟下载线程异常终止（会话监督将重建）：%s",
                self._thread_error, exc_info=True)
        finally:
            # 非 stop() 主动停止的退出，一律留一条 error，避免线程静默消失。
            if not self._stop.is_set() and not self._thread_error:
                self._thread_error = "thread exited without stop signal"
                logger.error("分钟下载线程意外退出（未收到停止信号）")

    def _wait(self, seconds: float) -> None:
        if seconds and seconds > 0:
            self._stop.wait(seconds)

    def run_once(self) -> dict:
        """执行一轮抓取（供循环与测试直接调用）。

        先过交易时段门控：窗口外/非交易日直接返回空轮（不取标的、不发请求、
        不写库），仅在状态切换时打一行日志。

        标的清单在本函数内去重（``_refresh_codes``），每只票只提交一次任务；
        单票的多页仍在该工作线程内顺序拉取，不会重复抓取或乱序覆盖。
        ``connections<=1`` 走单线程顺序路径，行为与旧实现逐字一致。
        """
        with self._round_lock:
            now = self._now()
            state = self._gate_state_at(now)
            self._note_gate(state)
            codes = self._refresh_codes() if state == "active" else []
            summary = {
                "codes": len(codes), "bars": 0, "requests": 0,
                "failures": 0, "empty_days": 0,
            }
            if codes:
                self._apply_adaptive(len(codes))
                started = time.monotonic()
                if self._connections() <= 1:
                    self._run_sequential(codes, summary)
                else:
                    self._run_pool(codes, summary)
                self._observe_utilization(
                    summary, time.monotonic() - started, state)
            self._rounds += 1
            self._last_round_at = now.strftime("%Y-%m-%d %H:%M:%S")
            self._last_summary = summary
            return summary

    # ------------------------------------------------------------------
    # 交易时段门控
    # ------------------------------------------------------------------

    def _gate_state_at(self, now: datetime) -> str:
        """返回当前的拉取状态（窗口含两端：09:15 与 15:05 都算窗口内）。"""
        if not self._is_trading_day(now.date()):
            return "non_trading_day"
        t = now.time()
        if t < self._window_start:
            return "before_window"
        if t > self._window_end:
            return "after_window"
        return "active"

    def _is_trading_day(self, day: date) -> bool:
        """交易日判定：周末硬规则 + 项目内置休市表；拿不到日历退化为工作日。

        口径与 ``kline_coverage_ledger.trading_days`` 一致（同一份
        ``BUILTIN_CLOSURES``）。该表只收录已核对年份，未收录年份或导入失败时
        按工作日放行，并只在「日历 ↔ 退化」切换时记一行日志，避免每分钟刷屏。
        """
        if day.weekday() >= 5:
            return False
        try:
            from core.engine.live.calendar import BUILTIN_CLOSURES
        except Exception as exc:  # pragma: no cover - 日历模块异常环境
            self._note_calendar_mode("fallback", "导入失败：%s" % exc)
            return True
        closed = BUILTIN_CLOSURES.get(day.year)
        if closed is None:
            self._note_calendar_mode(
                "fallback", "%d 年无内置休市表" % day.year)
            return True
        self._note_calendar_mode("calendar", "")
        return day not in closed

    def _note_calendar_mode(self, mode: str, detail: str) -> None:
        if mode == self._calendar_mode:
            return
        if mode == "fallback":
            logger.warning(
                "分钟下载器交易日历不可用（%s），退化为工作日判定", detail)
        elif self._calendar_mode == "fallback":
            logger.info("分钟下载器交易日历已恢复，按休市表判定")
        self._calendar_mode = mode

    def _note_gate(self, state: str) -> None:
        """只在进入/离开窗口时各打一行 info，窗口内不重复。"""
        if state == self._last_gate_state:
            return
        self._last_gate_state = state
        window = "%s-%s" % (self._window_start.strftime("%H:%M"),
                            self._window_end.strftime("%H:%M"))
        if state == "active":
            logger.info("分钟下载器已进入交易时段（%s），开始拉取", window)
        elif state == "before_window":
            logger.info("分钟下载器已就绪，等待开盘窗口（%s）", window)
        elif state == "after_window":
            logger.info("分钟下载器已离开交易时段（%s），暂停拉取", window)
        else:
            logger.info("分钟下载器今日非交易日，暂停拉取")

    def _run_sequential(self, codes: Sequence[str], summary: dict) -> None:
        for code in codes:
            if self._stop.is_set():
                break
            try:
                result = self._download_code(code)
            except DownloadCancelled:
                raise
            except Exception as exc:  # noqa: BLE001 - 单票失败不拖垮整轮
                self._note_failure(code, exc, summary)
                continue
            self._merge_result(result, summary)

    def _run_pool(self, codes: Sequence[str], summary: dict) -> None:
        """连接池路径：每只票一个任务，工作线程各自持有独立连接。"""
        pool = self._ensure_pool()
        futures = {pool.submit(self._download_code, code): code
                   for code in codes}
        cancelled = self._stop.is_set()
        for fut in as_completed(futures):
            code = futures[fut]
            if self._stop.is_set() and not cancelled:
                cancelled = True
                for pending in futures:
                    pending.cancel()
            try:
                result = fut.result()
            except DownloadCancelled:
                cancelled = True
                continue
            except Exception as exc:  # noqa: BLE001 - 单票失败不拖垮整轮
                self._note_failure(code, exc, summary)
                continue
            self._merge_result(result, summary)
        if cancelled or self._stop.is_set():
            raise DownloadCancelled()

    def _merge_result(self, result: dict, summary: dict) -> None:
        summary["bars"] += result["bars"]
        summary["requests"] += result["requests"]
        summary["empty_days"] += result["empty_days"]

    def _note_failure(self, code: str, exc: Exception, summary: dict) -> None:
        summary["failures"] += 1
        self._set_last_error("%s: %s" % (code, exc))
        logger.warning("分钟下载失败 code=%s：%s", code, exc)

    def _set_last_error(self, text: str) -> None:
        with self._stats_lock:
            self._last_error = text

    def _connections(self) -> int:
        return min(max(_int(getattr(self, "_effective_connections", 1), 1), 1),
                   16)

    # ------------------------------------------------------------------
    # 自适应（池规模 → connections/rps）+ 利用率观测
    # ------------------------------------------------------------------

    def _apply_adaptive(self, pool_size: int) -> None:
        """按当前池规模重算生效连接/限频；仅在轮次边界调用（无 worker 在跑）。"""
        cfg = self.config
        explicit_conn = cfg.connections is not None
        explicit_rps = cfg.max_req_per_second is not None
        if cfg.adaptive_enabled:
            sizing = sizing_for_pool(
                pool_size,
                connections_max=cfg.connections_max,
                max_req_per_second_max=cfg.max_req_per_second_max,
                tier_bonus=self._raise_bonus)
            conn = sizing["connections"]
            rps = sizing["max_req_per_second"]
        else:
            conn = _int(cfg.connections, 4)
            rps = float(cfg.max_req_per_second) \
                if cfg.max_req_per_second is not None else 12.0
        if explicit_conn:
            conn = int(cfg.connections)
        if explicit_rps:
            rps = float(cfg.max_req_per_second)
        conn = min(max(int(conn), 1), max(int(cfg.connections_max), 1), 16)
        rps_cap = max(float(cfg.max_req_per_second_max), 1.0)
        rps = 0.0 if rps == 0 else min(max(float(rps), 1.0), rps_cap)
        changed = (conn != self._effective_connections
                   or rps != self._effective_rps)
        self._effective_connections = conn
        self._effective_rps = rps
        if changed:
            self._limiter = RateLimiter(rps)
            logger.info(
                "分钟下载自适应：池=%d 生效 connections=%d rps=%.1f（档位 +%d）",
                pool_size, conn, rps, self._raise_bonus)

    def _observe_utilization(self, summary: dict, elapsed: float,
                             state: str) -> None:
        """记录本轮 actual_rps/上限、耗时/interval；连续顶格则告警并抬档。"""
        if state != "active" or elapsed <= 0:
            return
        requests = int(summary.get("requests") or 0)
        actual_rps = requests / elapsed if requests > 0 else 0.0
        cap = float(self._effective_rps)
        rps_ratio = (actual_rps / cap) if cap > 0 else 0.0
        interval = float(self.config.interval_seconds or 0)
        elapsed_ratio = (elapsed / interval) if interval > 0 else 0.0
        saturated = (
            rps_ratio >= float(self.config.high_watermark)
            or elapsed_ratio >= 1.0)
        now_text = self._now().strftime("%Y-%m-%d %H:%M:%S")
        with self._util_lock:
            self._util.update({
                "actual_rps": round(actual_rps, 3),
                "rps_cap": cap,
                "rps_ratio": round(rps_ratio, 4),
                "round_seconds": round(elapsed, 3),
                "interval_seconds": interval,
                "elapsed_ratio": round(elapsed_ratio, 4),
                "saturated": bool(saturated),
                "last_round_at": now_text,
            })
            if not saturated:
                self._util["high_streak"] = 0
                return
            streak = int(self._util.get("high_streak") or 0) + 1
            self._util["high_streak"] = streak
            threshold = max(int(self.config.high_streak_threshold or 1), 1)
            if streak < threshold:
                return
            self._util["high_streak"] = 0
            self._util["alerts"] = int(self._util.get("alerts") or 0) + 1
            self._util["last_alert_at"] = now_text
            self._util["last_alert_reason"] = (
                "round_timeout" if elapsed_ratio >= 1.0 else "rps_saturated")
            alerts = self._util["alerts"]
            reason = self._util["last_alert_reason"]
        explicit = (self.config.connections is not None
                    or self.config.max_req_per_second is not None)
        can_raise = (self.config.adaptive_enabled and not explicit
                     and self._raise_bonus < int(self.config.max_raise_tiers))
        message = (
            "分钟下载利用率告警：actual_rps=%.2f/%.1f(%.0f%%) 轮次=%.1fs/"
            "%.0fs(%.0f%%) 连续%d轮顶格；%s"
            % (actual_rps, cap, rps_ratio * 100, elapsed, interval,
               elapsed_ratio * 100, threshold,
               ("自动抬一档" if can_raise else "已达上限，仅告警")))
        logger.warning("%s", message)
        self._emit_event({
            "gate": "minute_download",
            "rule": "adaptive_utilization",
            "message": message,
            "reason": reason,
            "actual_rps": round(actual_rps, 3),
            "rps_cap": cap,
            "rps_ratio": round(rps_ratio, 4),
            "round_seconds": round(elapsed, 3),
            "interval_seconds": interval,
            "elapsed_ratio": round(elapsed_ratio, 4),
            "alerts": alerts,
            "at": now_text,
        })
        if can_raise:
            self._raise_bonus += 1
            # 轮次已结束，安全地换到下一档（下一个 active 轮生效）。
            self._apply_adaptive(len(self._codes))
        else:
            with self._util_lock:
                self._util["raise_exhausted"] = True

    def _emit_event(self, payload: dict) -> None:
        if self._on_event is None:
            return
        try:
            self._on_event(payload)
        except Exception:  # noqa: BLE001 — 告警投递失败不影响下载
            logger.debug("分钟下载事件投递失败", exc_info=True)

    def _ensure_pool(self) -> ThreadPoolExecutor:
        size = self._connections()
        if self._pool is not None and self._pool_size == size:
            return self._pool
        self._shutdown_pool()
        self._pool = ThreadPoolExecutor(
            max_workers=size, thread_name_prefix="minute-dl")
        self._pool_size = size
        logger.info("分钟下载并发池已启用：%d 个连接", size)
        return self._pool

    def _shutdown_pool(self) -> None:
        pool, self._pool = self._pool, None
        self._pool_size = 0
        if pool is not None:
            pool.shutdown(wait=False, cancel_futures=True)

    def _refresh_codes(self) -> List[str]:
        if self.codes_provider is None:
            return list(self._codes)
        try:
            raw = list(self.codes_provider() or [])
        except Exception:
            logger.warning("取会话标的清单失败，沿用上一轮清单", exc_info=True)
            return list(self._codes)
        seen = set()
        codes = []
        for item in raw:
            try:
                _, code6 = normalize_code(item)
            except ValueError:
                continue
            if code6 not in seen:
                seen.add(code6)
                codes.append(code6)
        self._codes = codes
        if not codes:
            self._warn_empty_codes()
        return list(codes)

    def _warn_empty_codes(self) -> None:
        now = time.monotonic()
        if now - self._last_empty_warn < 3600:
            return
        self._last_empty_warn = now
        logger.warning("分钟下载器标的清单为空（组合/备选池未就绪），本轮空转")

    # ------------------------------------------------------------------
    # 单票下载
    # ------------------------------------------------------------------

    def _download_code(self, code: str) -> dict:
        market, code6 = normalize_code(code)
        now = self._now()
        today = now.date()
        window_start = today - timedelta(
            days=max(self.config.lookback_trading_days, 1) * 2)
        missing = planned_missing_days(
            code6, window_start, today - timedelta(days=1),
            db_path=self.ledger_db_path)
        pages = self._pages_needed(missing) if missing else 1

        written = 0
        requests = 0
        fetched_days = set()
        earliest: Optional[date] = None
        latest: Optional[date] = None
        reached_end = False
        for page in range(pages):
            if self._stop.is_set():
                raise DownloadCancelled()
            offset = page * self.config.page_size
            records = self._query(market, code6, offset, self.config.page_size)
            requests += 1
            if not records:
                reached_end = True
                break
            bars = records_to_bars(records)
            written += self.store.upsert_bars(code6, PERIOD, bars)
            days = {rec.day for rec in records}
            fetched_days.update(days)
            page_min, page_max = min(days), max(days)
            earliest = page_min if earliest is None else min(earliest, page_min)
            latest = page_max if latest is None else max(latest, page_max)
            if len(records) < self.config.page_size:
                reached_end = True
                break

        with self._stats_lock:
            self._stats["bars_written"] += written
        empty = 0
        if written:
            covered = self._coverable_days(fetched_days, today, now)
            self._record_coverage(code6, covered)
            empty = self._record_empty(code6, missing, covered, reached_end)
        return {"bars": written, "requests": requests, "empty_days": empty}

    def _pages_needed(self, missing: Sequence[date]) -> int:
        """按缺日跨度估算分页数（1m 约 240 根/日，向上取整后 +1 兜底）。"""
        if not missing:
            return 1
        span_days = max((max(missing) - min(missing)).days + 1, 1)
        bars = span_days * 240
        pages = (bars + self.config.page_size - 1) // self.config.page_size
        # 交易日占比约 5/7，再放宽 1 页，避免节假日把页数估少
        return max(min(pages + 1, 20), 1)

    def _coverable_days(self, fetched: Iterable[date], today: date,
                        now: datetime) -> List[date]:
        """可登记为「已覆盖」的日期：历史日全算；当日仅在收盘后算。"""
        out = []
        closed = now.time() >= _CLOSE_TIME
        for d in sorted(set(fetched)):
            if d < today or (d == today and closed):
                out.append(d)
        return out

    def _record_coverage(self, code6: str, days: Sequence[date]) -> None:
        if not days:
            return
        try:
            ledger.record_coverage(
                code6, PERIOD, ledger.FIELD_ALL, [(d, d) for d in days],
                source=SOURCE, db_path=self.ledger_db_path)
        except Exception:
            logger.warning("分钟覆盖台账登记失败 code=%s", code6, exc_info=True)

    def _record_empty(self, code6: str, missing: Sequence[date],
                      covered: Sequence[date], reached_end: bool) -> int:
        if not missing or not reached_end or not covered:
            return 0
        newest = max(covered)
        harmless = [d for d in missing if d <= newest and d not in set(covered)]
        if not harmless:
            return 0
        try:
            ledger.record_empty_days(
                code6, PERIOD, harmless, source=SOURCE,
                db_path=self.ledger_db_path)
        except Exception:
            logger.warning("分钟空日台账登记失败 code=%s", code6, exc_info=True)
            return 0
        return len(harmless)

    # ------------------------------------------------------------------
    # 网络
    # ------------------------------------------------------------------

    def _query(self, market: int, code6: str, start: int,
               count: int) -> List[BarRecord]:
        attempts = self.config.retry_times + 1
        errors: List[str] = []
        for attempt in range(attempts):
            for host, port in self.host_pool.candidates():
                if self._stop.is_set():
                    raise DownloadCancelled()
                self._limiter.acquire()
                t0 = time.monotonic()
                try:
                    records = self._request_bars(
                        host, port, market, code6, start, count)
                except Exception as exc:  # noqa: BLE001 - 换主站重试
                    self.host_pool.report_failure(host, port, str(exc))
                    with self._stats_lock:
                        self._stats["failed_requests"] += 1
                    errors.append("%s:%d %s" % (host, port, exc))
                    continue
                self.host_pool.report_success(
                    host, port, (time.monotonic() - t0) * 1000.0)
                with self._stats_lock:
                    self._stats["requests"] += 1
                return records
            if attempt < attempts - 1:
                self._wait(self.config.retry_backoff_seconds * (2 ** attempt))
        self._set_last_error("全部主站失败: %s" % "; ".join(errors[-3:]))
        raise DownloadError(self._last_error)

    def _request_bars(self, host: str, port: int, market: int, code6: str,
                      start: int, count: int) -> List[BarRecord]:
        if self._request is not None:
            return list(self._request(
                host, port, market, code6, start, count) or [])
        # 同一工作线程对同一主站复用一个 socket；线程间绝不共享连接。
        key = (threading.get_ident(), host, port)
        with self._clients_lock:
            client = self._clients.get(key)
        if client is None:
            from .protocol import TdxClient

            client = TdxClient(
                host, port,
                timeout=self.config.request_timeout_seconds)
            client.connect()
            with self._clients_lock:
                self._clients[key] = client
        try:
            return client.query_bars(market, code6, start, count)
        except Exception:
            try:
                client.close()
            finally:
                with self._clients_lock:
                    self._clients.pop(key, None)
            raise

    # ------------------------------------------------------------------
    # 状态
    # ------------------------------------------------------------------

    def status(self) -> dict:
        with self._stats_lock:
            stats = dict(self._stats)
            last_error = self._last_error
        with self._util_lock:
            util = dict(self._util)
        alive = self.running
        now = self._now()
        last_round_age = self._seconds_since_last_round(now)
        interval = float(self.config.interval_seconds or 0)
        # 存活但超过 3 个轮询间隔没有产出 → 视为卡死（区别于线程已死）。
        stalled = bool(
            alive and last_round_age is not None
            and last_round_age > max(3.0 * interval, 30.0))
        pool_size = len(self._codes)
        cfg = self.config
        sizing = sizing_for_pool(
            pool_size, connections_max=cfg.connections_max,
            max_req_per_second_max=cfg.max_req_per_second_max,
            tier_bonus=self._raise_bonus) if cfg.adaptive_enabled else {
                "tier_index": None, "base_tier_index": None, "tier": "manual",
                "connections": self._effective_connections,
                "max_req_per_second": self._effective_rps,
                "single_host_sustainable": None,
                "note": "adaptive_enabled=false（人工配置）",
            }
        util.update({
            "rps_cap": self._effective_rps,
            "interval_seconds": interval,
            "effective_connections": self._effective_connections,
            "effective_max_req_per_second": self._effective_rps,
            "tier_index": sizing.get("tier_index"),
            "auto_raised_tiers": self._raise_bonus,
            "max_raise_tiers": int(cfg.max_raise_tiers),
            "explicit_override": (cfg.connections is not None
                                  or cfg.max_req_per_second is not None),
        })
        return {
            "running": alive,
            "alive": alive,
            "stalled": stalled,
            "rounds": self._rounds,
            "last_round_at": self._last_round_at,
            "last_round_age_seconds": (
                int(last_round_age) if last_round_age is not None else None),
            "thread_error": self._thread_error or None,
            "codes": len(self._codes),
            "pool_codes": list(self._codes),
            "last_summary": dict(self._last_summary),
            "store": str(self.store.path),
            "ledger": str(self.ledger_db_path) if self.ledger_db_path else None,
            "requests": stats["requests"],
            "failed_requests": stats["failed_requests"],
            "bars_written": stats["bars_written"],
            "last_error": last_error,
            "hosts": self.host_pool.snapshot(),
            # 交易时段门控：state 为 active 之外时本轮不请求、不写库。
            "gate": {
                "state": self._last_gate_state,
                "window": [self._window_start.strftime("%H:%M"),
                           self._window_end.strftime("%H:%M")],
                "calendar": self._calendar_mode,
            },
            "config": {
                "lookback_trading_days": cfg.lookback_trading_days,
                "interval_seconds": cfg.interval_seconds,
                "page_size": cfg.page_size,
                "adaptive_enabled": bool(cfg.adaptive_enabled),
                # 生效值（自适应或人工覆盖后的真实值）。
                "max_req_per_second": self._effective_rps,
                "connections": self._effective_connections,
                # 原始配置值（None = 交给自适应）。
                "configured_max_req_per_second": cfg.max_req_per_second,
                "configured_connections": cfg.connections,
                "connections_max": cfg.connections_max,
                "max_req_per_second_max": cfg.max_req_per_second_max,
                "window_start": cfg.window_start,
                "window_end": cfg.window_end,
            },
            "utilization": util,
            "sizing": {
                "pool_size": pool_size,
                **sizing,
                "table": sizing_table(
                    connections_max=cfg.connections_max,
                    max_req_per_second_max=cfg.max_req_per_second_max),
            },
        }

    def _seconds_since_last_round(self, now: datetime) -> Optional[float]:
        """距最近一轮完成（未完成过则距启动）的秒数；无启动时刻 → None。"""
        ref_text = self._last_round_at
        ref: Optional[datetime] = None
        if ref_text:
            try:
                ref = datetime.strptime(ref_text, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                ref = None
        if ref is None:
            ref = self._started_at
        if ref is None:
            return None
        return max((now - ref).total_seconds(), 0.0)

    # ------------------------------------------------------------------
    # 工具
    # ------------------------------------------------------------------

    def _now(self) -> datetime:
        if self._clock is not None:
            return self._clock()
        from core.engine.live.timing import now_shanghai

        return now_shanghai()


def _parse_hhmm(value, default: dt_time) -> dt_time:
    """解析 ``HH:MM``（兼容 ``HHMM``）；非法值回落到默认窗口。"""
    text = str(value or "").strip()
    for fmt in ("%H:%M", "%H%M"):
        try:
            return datetime.strptime(text, fmt).time()
        except ValueError:
            continue
    return default


def _int(value, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float(value, default: float) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if v > 0 else default


def _float0(value, default: float) -> float:
    """同 :func:`_float`，但保留显式 0（0 有明确语义时用：0=不等待退避/不隔离）。"""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return v if v >= 0 else default


def _optional_int(value) -> Optional[int]:
    if value is None or str(value).strip() == "":
        return None
    try:
        v = int(value)
    except (TypeError, ValueError):
        return None
    return v if v >= 0 else None


def _clamp_connections(value) -> Optional[int]:
    """显式连接数夹取到 [1, 16]；``None`` 保留给「按池规模自适应」。"""
    v = _optional_int(value)
    if v is None:
        return None
    return min(max(v, 1), 16)


def _optional_float(value) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    return v if v >= 0 else None


def _bool(value, default: bool) -> bool:
    if value is None or str(value).strip() == "":
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    return default


def _ratio(value, default: float) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    return min(max(v, 0.0), 1.0)


__all__ = [
    "DownloadConfig", "MinuteDownloadService", "RateLimiter",
    "planned_missing_days", "sizing_for_pool", "sizing_table",
    "SIZING_TIERS", "SOURCE", "PERIOD",
]
