"""交易日历与交易总开关（0010 后续：迁入 live 子包）。

权威交易日由桥 ``xtdata.get_trading_dates`` 提供（GET /calendar），Core 按自然年
缓存一次。判定原则：

- 周末（周六/周日）永远非交易日——A 股硬规则，不依赖数据，也不 fail-open。
- 工作日是否交易日由权威集合判定（覆盖节假日/调休）。
- **多源回退链**：主源（当前通道 ``dispatcher.query_calendar``）→ 备用源（掘金桥
  ``/calendar``，见 ``goldminer_calendar_provider``）→ 内置官方休市表（``BUILTIN_CLOSURES``）
  → fail-open（工作日默认交易日）。
- fail-open 语义保留（绝不因日历过期误挡真实交易日），但必须**可见**：WARNING
  日志 + 一条 risk 事件（每个年份最多一次，不刷屏），并经 ``status()`` 暴露到
  会话 API（``GET /api/live/sessions/{id}`` 的 ``trading_calendar`` 字段）。
- **空结果不永久缓存**：全部源失败时按 ``retry_after_seconds``（默认 900s）后再试，
  某源恢复即自动升级回权威集合，避免"空日历写进缓存后整年不复用"。
- provider 只返回"截至今天"的部分年份数据时，超出其覆盖范围（未来日期）不按
  "不在集合=休市"判定，退回内置休市表/工作日放行——防止日历截断把未来交易日
  误判为休市。
- **半日市（P2-10）**：仅上午 09:30–11:30 交易、下午休市。来源优先级
  ``half_day_provider``（可注入，按年拉取）→ 内置 ``BUILTIN_HALF_DAYS`` → 无数据时
  fail-open（按全天交易处理）+ WARNING/risk 事件，每年最多一次。半日市当天
  ``is_trading_allowed`` 在 11:30（含）之后判不可交易，09:15–09:20 盘前集合竞价窗口
  不受影响（该窗口不依赖本开关的时段判定）。
- ``is_trading_allowed(now)`` = 交易日 且 09:30 ≤ 时刻 < 收盘（常态 15:00 / 半日市
  11:30），作为下单总闸。

注意：这里只负责"是否允许下单"。引擎生命周期（start/stop）、14:30 估值、15:05 收盘
清扫各有自己的调度判定，不被本开关替代。

本模块为 live 专属（仅 LiveEngine 及其协作者使用，回测不依赖），故归入
core.engine.live 子包。原路径 core.engine.trading_calendar 保留 re-export shim，
外部 import 路径与测试直连无需改动。
"""
from __future__ import annotations

import inspect
import logging
import re
import time as _time
from datetime import date, datetime, time, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

logger = logging.getLogger("core.engine.live.calendar")

# A 股连续竞价时段（下单总闸用，与 live_engine 的收盘守卫一致）
_MARKET_OPEN = time(9, 30)
_MARKET_CLOSE = time(15, 0)
# 半日市收盘（P2-10）：仅上午交易，11:30 起全天收市（左闭右开，11:30 边界即不可报）
_HALF_DAY_CLOSE = time(11, 30)
# 午休（P2-5）：11:30–13:00 交易所不受理委托。旧实现只判 09:30–15:00，
# 11:30 边界 bar 触发的单会报进午休（TDX 排队到 13:00、QMT 可能直接拒单）。
_LUNCH_START = time(11, 30)
_LUNCH_END = time(13, 0)

_SHANGHAI_TZ = timezone(timedelta(hours=8))

# provider 契约：无参（主源）或接受自然年参数（可按年精确拉取）；元素为
# date / datetime / 'YYYYMMDD' / 'YYYY-MM-DD' / 'YYYY/MM/DD' / 毫秒时间戳。
CalendarProvider = Callable[..., Iterable]

# 内置官方休市表（仅列**工作日**休市；周末由硬规则覆盖，不重复收录）。
#
# 数据来源：上海证券交易所「休市安排（2026年）」
#   https://www.sse.com.cn/disclosure/dealinstruc/closed/
#   公告原文（2026 年）：
#     元旦 1/1(四)–1/3(六)；春节 2/15(日)–2/23(一)；清明 4/4(六)–4/6(一)；
#     劳动节 5/1(五)–5/5(二)；端午 6/19(五)–6/21(日)；中秋 9/25(五)–9/27(日)；
#     国庆 10/1(四)–10/7(三)。
#   与掘金桥 /calendar 实测返回值逐日比对一致（2026-11-05 取证）。
#
# 更新方式：每年 12 月沪深交易所发布次年全年休市安排后，在此追加对应年份；
# 未收录年份不做"凭记忆编造"，自动退化为 fail-open（工作日放行）并告警一次。
BUILTIN_CLOSURES: Dict[int, frozenset] = {
    2026: frozenset({
        date(2026, 1, 1), date(2026, 1, 2),                       # 元旦
        date(2026, 2, 16), date(2026, 2, 17), date(2026, 2, 18),
        date(2026, 2, 19), date(2026, 2, 20), date(2026, 2, 23),   # 春节
        date(2026, 4, 6),                                         # 清明
        date(2026, 5, 1), date(2026, 5, 4), date(2026, 5, 5),     # 劳动节
        date(2026, 6, 19),                                        # 端午
        date(2026, 9, 25),                                        # 中秋
        date(2026, 10, 1), date(2026, 10, 2), date(2026, 10, 5),
        date(2026, 10, 6), date(2026, 10, 7),                     # 国庆
    }),
    # 2027 年（数据来源：《国务院办公厅关于2027年部分节假日安排的通知》
    #   国办发明电〔2026〕10号，2026-11-05 发布，
    #   https://www.gov.cn/zhengce/content/202611/content_7085101.htm ；本表
    #   2026-11-11 录入）。仅收**工作日**休市；周末由硬规则覆盖。原文对照：
    #     元旦   1/1(五)–1/3(日)              → 工作日休市 1/1
    #     春节   2/5(五)–2/13(六)；1/31(日)、2/14(日) 上班
    #                                        → 工作日休市 2/5、2/8–2/12
    #     清明   4/3(六)–4/5(一)              → 工作日休市 4/5
    #     劳动节 5/1(六)–5/5(三)；5/8(六) 上班 → 工作日休市 5/3、5/4、5/5
    #     端午   6/9(三)                      → 工作日休市 6/9
    #     中秋   9/15(三)                     → 工作日休市 9/15
    #     国庆   10/1(五)–10/7(四)；9/26(日)、10/9(六) 上班
    #                                        → 工作日休市 10/1、10/4–10/7
    #   **调休上班日全部落在周六/周日**（1/31、2/14、5/8、9/26、10/9），按交易所
    #   「周末休市」规则仍非交易日，故 2027 年不存在"周末调休成交易日"的日期。
    #   台账：上交所 2027 年休市安排公告截至 2026-11-11 尚未发布，本表按国办通知 +
    #   「周末休市」推导，公告发布后应逐日复核（不一致以交易所公告为准）。
    2027: frozenset({
        date(2027, 1, 1),                                         # 元旦
        date(2027, 2, 5), date(2027, 2, 8), date(2027, 2, 9),
        date(2027, 2, 10), date(2027, 2, 11), date(2027, 2, 12),   # 春节
        date(2027, 4, 5),                                         # 清明
        date(2027, 5, 3), date(2027, 5, 4), date(2027, 5, 5),     # 劳动节
        date(2027, 6, 9),                                         # 端午
        date(2027, 9, 15),                                        # 中秋
        date(2027, 10, 1), date(2027, 10, 4), date(2027, 10, 5),
        date(2027, 10, 6), date(2027, 10, 7),                     # 国庆
    }),
}

# 内置半日市表（P2-10）：仅列**工作日**半天交易（09:30–11:30，下午休市）。
#
# 现状（2026-11-11 核对）：A 股现行休市安排中**已无半日市**——国办发明电〔2026〕10号
# 覆盖的 2027 年七个节假日全部为整日休市（春节前最后一个交易日 2027-02-04 周四为
# 全天交易），元旦前、春节前均无半天交易日。历史上（2007 年以前）曾有元旦/春节前
# 半日市，现已取消。
#
# 因此本表对已核对年份录入**空集合**（语义 = "该年已知没有半日市"，不告警），
# 机制保留以便：①交易所将来恢复半日市时按年追加；②``TradingCalendar(half_day_provider=)``
# 由通道/配置注入半日市日期（优先于本表）。未收录年份一律 fail-open（按全天交易，
# 绝不因半日市数据缺失而 fail-closed 拒绝交易），并告警一次/年。
BUILTIN_HALF_DAYS: Dict[int, frozenset] = {
    2026: frozenset(),
    2027: frozenset(),
}


def _accepts_year(fn: Callable) -> bool:
    """provider 是否接受自然年参数（避免用 TypeError 试探吞掉真实异常）。"""
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    for p in sig.parameters.values():
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD, p.VAR_POSITIONAL):
            return True
    return False


def _from_epoch(value: float) -> Optional[date]:
    """秒/毫秒时间戳 → 上海时区自然日。"""
    try:
        ts = float(value)
    except (TypeError, ValueError):
        return None
    if ts > 1e11:  # 毫秒
        ts /= 1000.0
    try:
        return datetime.fromtimestamp(ts, tz=_SHANGHAI_TZ).date()
    except (OverflowError, OSError, ValueError):
        return None


def goldminer_calendar_provider(year: int) -> List[str]:
    """备用日历源：掘金桥 ``GET /calendar?start=YYYY-01-01&end=YYYY-12-31``。

    桥返回 ``["2026-11-02", ...]``（ISO 格式，``TradingCalendar._coerce`` 兼容）。
    桥离线/非 JSON/``ok=false`` 一律抛异常，由 TradingCalendar 继续向后回退。
    """
    from core.config import load_config
    import httpx

    cfg = load_config().get("goldminer") or {}
    base_url = str(cfg.get("base_url") or "http://127.0.0.1:7801").rstrip("/")
    timeout = float(cfg.get("request_timeout_seconds") or 30.0)
    with httpx.Client(timeout=timeout) as client:
        resp = client.get(
            base_url + "/calendar",
            params={"start": "%d-01-01" % year, "end": "%d-12-31" % year},
        )
        resp.raise_for_status()
        body = resp.json() or {}
    if not body.get("ok"):
        raise RuntimeError("goldminer calendar failed: %s" % body.get("error"))
    return list(body.get("data") or [])


class TradingCalendar:
    def __init__(
        self,
        provider: CalendarProvider,
        fallback_providers: Optional[List[Tuple[str, CalendarProvider]]] = None,
        retry_after_seconds: float = 900.0,
        clock: Optional[Callable[[], float]] = None,
        half_day_provider: Optional[CalendarProvider] = None,
    ):
        # (label, fn, takes_year)
        self._sources: List[Tuple[str, CalendarProvider, bool]] = [
            ("primary", provider, _accepts_year(provider))
        ]
        for label, fn in (fallback_providers or []):
            self._sources.append((label, fn, _accepts_year(fn)))
        self._retry_after = float(retry_after_seconds)
        self._clock = clock or _time.monotonic
        # year -> (交易日集合, 覆盖起, 覆盖止, 来源标签)
        self._by_year: Dict[int, Tuple[Set[date], date, date, str]] = {}
        # year -> 退化来源（'builtin' / 'fail_open'）
        self._degraded: Dict[int, str] = {}
        self._retry_at: Dict[int, float] = {}
        self._warned_year: Set[int] = set()
        # 半日市（P2-10）：year -> 半日市日期集合（空集合=已确认无半日市）。
        # 只有"确认来源"才进 _half_by_year；无数据时宁可不缓存（退避后重试）并 fail-open。
        self._half_provider = half_day_provider
        self._half_provider_takes_year = (
            _accepts_year(half_day_provider) if half_day_provider is not None else False
        )
        self._half_by_year: Dict[int, Set[date]] = {}
        self._half_retry_at: Dict[int, float] = {}
        self._half_warned_year: Set[int] = set()
        # 退化告警事件回调（LiveEngine 接线后 → risk 事件；每自然年最多触发一次）
        self.on_degraded: Optional[Callable[[dict], None]] = None

    # ---------------- 加载 / 回退链 ----------------

    def _load_year(self, year: int) -> Optional[Tuple[Set[date], date, date, str]]:
        """返回某自然年权威交易日集合；None 表示已退化（见 ``_degraded[year]``）。"""
        if year in self._by_year:
            return self._by_year[year]
        now = self._clock()
        if year in self._degraded and now < self._retry_at.get(year, 0.0):
            return None  # 退避窗口内，不重复打源（避免慢桥拖主循环）

        reasons: List[str] = []
        for label, fn, takes_year in self._sources:
            try:
                raw = list((fn(year) if takes_year else fn()) or [])
            except Exception as exc:  # 桥离线/超时/HTTP 非 200/非 JSON
                reasons.append("%s: %s" % (label, exc))
                continue
            dates = {
                d for d in (self._coerce(item) for item in raw)
                if d is not None and d.year == year
            }
            if dates:
                self._by_year[year] = (dates, min(dates), max(dates), label)
                self._degraded.pop(year, None)
                self._retry_at.pop(year, None)
                if label != "primary":
                    logger.info(
                        "trading calendar for %s resolved by fallback source '%s' "
                        "(%d trading days)", year, label, len(dates))
                return self._by_year[year]
            reasons.append("%s: empty" % label)

        # 全部源失败/为空 → 退化：内置休市表（已收录年份）优先，否则 fail-open
        closed = BUILTIN_CLOSURES.get(year)
        source = "builtin" if closed is not None else "fail_open"
        self._degraded[year] = source
        self._retry_at[year] = now + self._retry_after
        self._warn_once(year, source, reasons)
        return None

    def _warn_once(self, year: int, source: str, reasons: List[str]) -> None:
        """退化告警：WARNING + risk 事件，每个自然年最多一次（不刷屏）。"""
        detail = "；".join(reasons) if reasons else "no provider configured"
        if source == "builtin":
            message = (
                "交易日历全部数据源不可用（%s）；%d 年退化为内置官方休市表（工作日"
                "非节假日照常放行，节假日休市），请检查桥/行情通道；恢复后自动重试"
                % (detail, year))
        else:
            message = (
                "交易日历全部数据源不可用（%s）；%d 年无内置休市表，fail-open："
                "工作日一律视为交易日（节假日可能误放行，下单由柜台拒单兜底）"
                % (detail, year))
        if year in self._warned_year:
            logger.debug("%s (year %s, already reported)", message, year)
            return
        self._warned_year.add(year)
        logger.warning("%s", message)
        cb = self.on_degraded
        if cb is not None:
            try:
                cb({
                    "rule": "trading_calendar_degraded",
                    "year": year,
                    "source": source,
                    "reasons": reasons,
                    "message": message,
                })
            except Exception:  # noqa: BLE001  告警回调失败不得影响日历判定
                logger.warning("calendar degraded callback failed", exc_info=True)

    # ---------------- 判定 ----------------

    @staticmethod
    def _coerce(item) -> Optional[date]:
        # datetime 是 date 的子类，必须先判 datetime，否则 datetime 会被原样返回，
        # dates 集合里存 datetime 导致 d(date) in dates 永为 False（交易日被误判非交易日）。
        if isinstance(item, datetime):
            return item.date()
        if isinstance(item, date):
            return item
        if isinstance(item, (int, float)) and not isinstance(item, bool):
            return _from_epoch(item)
        text = str(item).strip()
        if not text:
            return None
        # 'YYYY-MM-DD' / 'YYYY/MM/DD' / 'YYYY.MM.DD'（掘金桥口径；可带时间后缀）
        m = re.match(r"^(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})", text)
        if m:
            try:
                return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            except ValueError:
                return None
        digits = re.sub(r"\D", "", text)
        if len(digits) == 8:  # 通达信/QMT 桥口径 'YYYYMMDD'
            try:
                return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
            except ValueError:
                return None
        if len(digits) >= 14:  # 'YYYYMMDDHHMMSS'
            try:
                return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
            except ValueError:
                return None
        return None

    # ---------------- 半日市（P2-10）----------------

    def _load_half_days(self, year: int) -> Optional[Set[date]]:
        """某自然年的半日市日期集合；None = 无确认来源（fail-open：按全天交易）。

        来源优先级：``half_day_provider``（按年拉取，可跨通道注入）→ 内置
        ``BUILTIN_HALF_DAYS``（存在即权威，空集合 = 该年确认无半日市）→ fail-open
        + WARNING/risk 事件（每年一次）。provider 失败不缓存，``retry_after_seconds``
        后重试（与交易日历同一退避语义，避免慢桥拖主循环）。
        """
        if year in self._half_by_year:
            return self._half_by_year[year]
        now = self._clock()
        if year in self._half_retry_at and now < self._half_retry_at[year]:
            return None  # 退避窗内，不重复打源

        reasons: List[str] = []
        provider = self._half_provider
        if provider is not None:
            try:
                raw = list((provider(year) if self._half_provider_takes_year else provider()) or [])
            except Exception as exc:  # 通道不支持/离线/超时 → 退回内置表
                reasons.append("half_day_provider: %s" % exc)
            else:
                days = {
                    d for d in (self._coerce(item) for item in raw)
                    if d is not None and d.year == year
                }
                if days:
                    self._half_by_year[year] = days
                    self._half_retry_at.pop(year, None)
                    return days
                reasons.append("half_day_provider: empty")

        builtin = BUILTIN_HALF_DAYS.get(year)
        if builtin is not None:
            # 已核对年份：内置表权威（可为空集合 = 确认无半日市），不告警。
            self._half_by_year[year] = set(builtin)
            self._half_retry_at.pop(year, None)
            return self._half_by_year[year]

        # 无任何确认来源 → fail-open（按全天交易），每年告警一次，退避后重试。
        self._half_retry_at[year] = now + self._retry_after
        self._warn_half_day_once(year, reasons)
        return None

    def _warn_half_day_once(self, year: int, reasons: List[str]) -> None:
        """半日市数据缺失告警（WARNING + risk 事件，每年最多一次，不刷屏）。"""
        detail = "；".join(reasons) if reasons else "no half-day provider configured"
        message = (
            "半日市数据不可用（%s）；%d 年按全天交易处理（fail-open，不因半日市数据"
            "缺失拒绝交易）；如该年存在半日市，下午 11:30–15:00 的单会被柜台拒单兜底"
            % (detail, year))
        if year in self._half_warned_year:
            logger.debug("%s (year %s, already reported)", message, year)
            return
        self._half_warned_year.add(year)
        logger.warning("%s", message)
        cb = self.on_degraded
        if cb is not None:
            try:
                cb({
                    "rule": "half_day_calendar_degraded",
                    "year": year,
                    "source": "fail_open",
                    "reasons": reasons,
                    "message": message,
                })
            except Exception:  # noqa: BLE001  告警回调失败不得影响日历判定
                logger.warning("half-day degraded callback failed", exc_info=True)

    def is_half_day(self, d: date) -> bool:
        """d 是否为半日市（仅上午 09:30–11:30 交易）。

        非交易日/周末恒 False（半日市只在交易日成立）；数据不可用时 fail-open
        （返回 False = 按全天交易），绝不因此拒绝交易。
        """
        if d.weekday() >= 5 or not self.is_trading_day(d):
            return False
        days = self._load_half_days(d.year)
        return bool(days) and d in days

    def session_close_time(self, d: date) -> time:
        """d 当天的收盘时刻：半日市 11:30，常态 15:00（左闭右开判定用）。"""
        return _HALF_DAY_CLOSE if self.is_half_day(d) else _MARKET_CLOSE

    def is_trading_day(self, d: date) -> bool:
        # 周末硬规则（weekday(): Monday=0 .. Sunday=6）
        if d.weekday() >= 5:
            return False
        loaded = self._load_year(d.year)
        if loaded is not None:
            trading, cover_from, cover_to, _source = loaded
            if cover_from <= d <= cover_to:
                return d in trading
            # 超出数据源覆盖范围（多数桥只给到"今天"）：不按"不在集合=休市"判定，
            # 否则日历截断会把未来交易日误判为休市（甚至整月不下单）。
        closed = BUILTIN_CLOSURES.get(d.year)
        if closed is not None:
            return d not in closed  # 内置官方休市表：工作日 - 节假日
        return True  # fail-open：工作日无权威数据，放行

    def is_fail_open(self, d: date) -> bool:
        """该日所属年份是否处于 fail-open 退化（无权威数据且无内置休市表）。

        fail-open 下工作日一律视为交易日：长假中的工作日会被误判为交易日，
        ``_next_trading_day`` 因而把「节后首日」算早。调用方据此放宽 deferred 单
        的过期窗口（见 LiveEngine._DEFERRED_RELEASE_GRACE_DAYS）。
        """
        if d.year in self._by_year:
            return False
        self._load_year(d.year)
        return self._degraded.get(d.year) == "fail_open"

    def is_last_trading_day_of_week(self, d: date) -> bool:
        """d 是否为「本周最后一个交易日」。

        从 d+1 到本周日逐日判定，全为非交易日则 d 是期末（周五休市自动落到周四，
        长假同理）。d 本身须为交易日。
        """
        if not self.is_trading_day(d):
            return False
        for offset in range(1, 7 - d.weekday()):
            if self.is_trading_day(d + timedelta(days=offset)):
                return False
        return True

    def is_last_trading_day_of_month(self, d: date) -> bool:
        """d 是否为「本月最后一个交易日」（月末休市自动前移到前一交易日）。"""
        if not self.is_trading_day(d):
            return False
        if d.month == 12:
            next_month = date(d.year + 1, 1, 1)
        else:
            next_month = date(d.year, d.month + 1, 1)
        cur = d + timedelta(days=1)
        while cur < next_month:
            if self.is_trading_day(cur):
                return False
            cur += timedelta(days=1)
        return True

    def is_trading_allowed(self, now: datetime) -> bool:
        """是否允许在 ``now`` 时刻下单：交易日 且 09:30–11:30 / 13:00–15:00（左闭右开）。

        P2-5：午休 11:30–13:00 禁报（交易所不受理；11:30 边界 bar 的单不再排进
        TDX / 被 QMT 拒）。13:00 起正常放行，不影响下午派发。
        P2-10：半日市（仅上午交易）当天收盘时刻前移到 11:30，11:30 之后一律禁报
        （含原下午时段）。盘前集合竞价 09:15–09:20 走独立路径，不受本开关影响。
        """
        if not self.is_trading_day(now.date()):
            return False
        t = now.time()
        if not (_MARKET_OPEN <= t < self.session_close_time(now.date())):
            return False
        return not (_LUNCH_START <= t < _LUNCH_END)

    def is_lunch_break(self, now: datetime) -> bool:
        """是否处于午休 11:30–13:00（左闭右开）。

        与下单总闸 ``is_trading_allowed`` 共用同一组午休常量（_LUNCH_START /
        _LUNCH_END），不另造时间常量。午休交易所无新 bar，行情轮询可整段暂停；
        11:30 收线那根由 13:00 首轮一次取回并派发（见 LiveEngine._quote_poll_active）。
        """
        return _LUNCH_START <= now.time() < _LUNCH_END

    def lunch_end_at(self, now: datetime) -> datetime:
        """``now`` 当日的午休结束时刻（13:00:00），供暂停状态回报「下次恢复」。"""
        return now.replace(hour=_LUNCH_END.hour, minute=_LUNCH_END.minute,
                           second=0, microsecond=0)

    # ---------------- 可观测性 ----------------

    def status(self) -> dict:
        """日历来源/退化状态（会话 API 暴露"是否在用退化日历"）。"""
        years: Dict[str, dict] = {}
        for y in sorted(set(self._by_year) | set(self._degraded)):
            if y in self._by_year:
                trading, cover_from, cover_to, source = self._by_year[y]
                years[str(y)] = {
                    "source": source,
                    "degraded": False,
                    "trading_days": len(trading),
                    "coverage": [cover_from.isoformat(), cover_to.isoformat()],
                }
            else:
                years[str(y)] = {"source": self._degraded[y], "degraded": True}
        return {
            "degraded": bool(self._degraded),
            "years": years,
            "builtin_years": sorted(BUILTIN_CLOSURES),
            "retry_after_seconds": self._retry_after,
            # 半日市（P2-10）：已确认年份的日期数 + 内置表收录年份（未收录年份
            # 走 fail-open，按全天交易，见 _load_half_days）。
            "half_days": {
                str(y): len(days) for y, days in sorted(self._half_by_year.items())
            },
            "builtin_half_day_years": sorted(BUILTIN_HALF_DAYS),
        }
