"""DailyCloser — 日终/收盘三件套的"时点判断 + 编排"（0010 步骤 5，纯搬移）。

把原 LiveEngine 的 _maybe_daily_close / _maybe_daily_bars / _maybe_close_sweep
收敛到此处。本服务只负责"**到点 + 编排**"：
- 14:30 调 breaker.on_daily_update（daily_loss 推进/恢复，逻辑在 BreakerService）；
- 15:05 日线级别轮次（1d/1w/1mon）：等周期收盘价落定后拉完成 bar，经 on_bar 回调驱动
  （行情/信号缓存与 bar 驱动归 MarketDataService/引擎，本服务只编排时点）；
- 14:50 提前收盘价（B，仅 1d，可选）：14:50 合成 1m bar 收盘价定信号，委托价取
  14:52 起始 1m bar 开盘价（14:51 兜底），当日尾盘成交；
- 15:05 收盘清扫：成交回填 + 终态同步 + 剩余未成交标 canceled（订单状态机逻辑在 OrderStateMachine）。

幂等：last_daily_date/last_daily_bar_date/last_close_sweep_date 记录当日已执行，
避免每轮 60s tick 重复触发。时间走注入 clock（默认 now_shanghai，上海时区实盘固有时点）。
非交易日（周末/节假日）三件套全部跳过，避免陈旧 bar/快照误估值或误触熔断。

**半日市（P2-10）时点结论：14:30/15:05 均不前移。**
- 14:30 日终估值：半日市当天 11:30 后已无新价，14:30 估的即当日最终价（最后已知价
  跨日持久），提前到 11:30 反而与「日终」语义冲突且无收益，故不动；
- 15:05 日线轮次：半日市当天 1d bar 在 11:30 就定型，但行情源（TQ/桥）对当日日线的
  落盘/校验在 15:00 后统一完成，15:05 取数**仍满足「最新日线日期 == 今日」新鲜度
  校验**；前移到 11:35 会撞上数据未落盘（触发 daily_bar_freshness 跳过）风险，故不前移；
- 15:05 收盘清扫：清扫是「收盘后权威收口」，只比较柜台 /orders、/deals 与本地状态，
  不依赖行情仍在跳动；半日市当天 11:30 后不会再有新成交，15:05 执行仍然正确（只是
  兜底时点偏晚 3.5 小时，不影响终态可收敛）。前移到 11:35 则可能撞上柜台当日委托
  状态尚未收敛（券商日终处理在 15:00 后），故保持 15:05；
- 唯一必须动的是 14:52「提前收盘价」轮次（B）：半日市当天该时点已收盘，属收盘后
  下单，故直接跳过（见 maybe_advance_daily_close），1d 信号交由 15:05 轮次补跑。
- 盘前集合竞价（09:15–09:20，LiveEngine 路径）在半日市当天照常，不经过本服务。

本服务不反向 import LiveEngine：经 EngineContext 读 portfolios/session_id/dispatcher/db 工厂，
协作者（market_data/breaker/orders）与 on_bar、set_bridge_offline、emit 回调由构造注入。
逻辑原样搬移，仅 self.xxx → 注入对象。
"""
from __future__ import annotations

import logging
from datetime import date, datetime, time, timedelta
from typing import Callable, Dict, Optional, Set

from ..bar_poller import parse_bar_time, to_ohlcv
from ..event import BarEvent
from ..http_bridge_dispatcher import BridgeUnavailableError
from ...models import LiveDecisionEvent, LiveOrder, LiveSession, LiveTrade
from .context import EngineContext

logger = logging.getLogger("core.engine.live.daily_closer")


def _positive_num(value) -> Optional[float]:
    """转正数价格；None/0/负/非数字 → None（停牌、缺失、伪影一律不采用）。"""
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None
    return num if num > 0 else None


# 上海 14:30 日终估值驱动（E5/E6 update_daily：daily_loss 推进/恢复、熔断次日恢复）。
_DAILY_CLOSE_TIME = (14, 30)
# 15:05 日线级别（1d/1w/1mon）信号轮次：等周期收盘价落定后用**完整 bar**计算。
# 与 14:30 的日终估值独立（幂等标记也各自独立）。
_DAILY_BARS_TIME = (15, 5)
# 14:50 分钟 bar 的 stime（快照合成器标签=分钟终点）：14:50:00–14:50:59 的收盘价。
_ADVANCE_BAR_LABEL = (14, 51)
# B 执行价：信号 bar（14:50 分钟）之后「再下一根」1m bar 的起始分钟，
# 取其开盘价作为委托价（考虑 14:51 报单的报价时延）。
_ADVANCE_EXEC_START = (14, 52)
# 14:52 bar 取不到时的兜底：14:51 起始 1m bar 的开盘价（此时已闭合、稳定）。
_ADVANCE_EXEC_FALLBACK_START = (14, 51)
# 允许多久的「等 14:52 bar 成形」窗口：14:53 起才开始用 14:51 兜底。
_ADVANCE_EXEC_FALLBACK_TIME = (14, 53)
# 提前收盘价轮次的执行窗口上限（15:00 收盘后不再补跑，避免用非盘中价交易）。
_ADVANCE_CLOSE_DEADLINE = (15, 0)
# 收盘清扫时点（15:05）：交易所已收盘、QMT 自动撤单已落定后做确定性兜底。
_MARKET_CLOSE_SWEEP_TIME = (15, 5)


class DailyCloser:
    """日终/收盘时点编排：估值推进、日线驱动、收盘清扫。"""

    def __init__(
        self,
        ctx: EngineContext,
        market_data,
        breaker,
        orders,
        on_bar: Callable[..., None],
        calendar_provider: Callable[[], object],
        emit: Callable[[str, dict], None],
        set_bridge_offline: Callable[[], None],
        external_signals_only: bool = False,
        advance_close_enabled: bool = False,
        advance_close_time: str = "14:51",
        snapshot_synth=None,
        on_daily_snapshot: Optional[Callable[[date], None]] = None,
        on_daily_gap_report: Optional[Callable[[date], None]] = None,
        on_daily_cross_check: Optional[Callable[[date], None]] = None,
    ) -> None:
        self._ctx = ctx
        self._market_data = market_data
        self._breaker = breaker
        self._orders = orders
        self._on_bar = on_bar
        # 日历以 provider 注入（而非对象引用）：测试在构造后会替换 engine._calendar
        # （_make_engine 后 engine._calendar = 假日历），本服务需经 provider 读到最新值。
        self._get_calendar = calendar_provider
        self._emit = emit
        self._set_bridge_offline = set_bridge_offline
        # 外部信号文件模式：停用日终 1w/1mon 通达信补注入（信号来源只有预警文件）。
        # 1d/1w/1mon 的 bar 驱动与风控照常执行。
        self._external_signals_only = bool(external_signals_only)
        # 14:50 提前收盘价（仅 1d）：开关 + 执行时点 + 快照合成器（拿 14:50 1m bar）。
        # 会话级开关默认关；开启后 1d 日线轮次由 15:05 移到该时点（周线/月线不动）。
        self._advance_enabled = bool(advance_close_enabled)
        self._advance_time = self._parse_hhmm(advance_close_time, _ADVANCE_BAR_LABEL)
        # B 的委托价取「14:52 起始 1m bar 的开盘价」，故有效轮次不早于 14:52：
        # 配置仍允许 14:51（语义=轮次不早于该时点），这里只把下限抬到 14:52。
        if self._advance_time < _ADVANCE_EXEC_START:
            self._advance_time = _ADVANCE_EXEC_START
        self._advance_synth = snapshot_synth
        # A3（2026-11-18）：14:30 日终估值后写组合资金日序列；None=不启用。
        self._on_daily_snapshot = on_daily_snapshot
        # F3（2026-11-18）：收盘清扫完成后输出当日缺失分钟段清单（只观测）。None=不启用。
        self._on_daily_gap_report = on_daily_gap_report
        # F4（2026-11-18）：收盘清扫后抽样交叉校验收盘价（只观测）。None=不启用。
        self._on_daily_cross_check = on_daily_cross_check
        self._clock = ctx.clock
        # R4：幂等日期标记移入 __init__（原在 _calendar property 的 return 之后，死代码）。
        # E5/E6：14:30 日终已算过的日期标记（每日一次，避免每轮重复调 update_daily）
        self.last_daily_date = None
        # 15:05 日线级别（1d/1w/1mon）轮次已驱动的日期标记（内存 + DB
        # daily_bars_done_date 跨重启幂等）
        self.last_daily_bar_date = None
        self._daily_bars_done_loaded = False
        # 本轮已成功驱动的 (交易日, 周期, 组合) 集合（P1，2026-11-16）：整轮未完成
        # （有组合因桥离线跳过）时不标 done、下轮 60s tick 重试，这里用来跳过已
        # 驱动成功的组合，避免重试时对同一根 bar 重复驱动（幂等门虽兜底，但会产生
        # 成串 bar_processed 噪音事件）。
        self._daily_driven_keys: Set[tuple] = set()
        # P2-11：日线取数非当日 / 取数为空 的 risk 事件节流（每交易日一条）
        self._daily_freshness_noted_date = None
        # 15:05 收盘清扫已执行的日期标记（每日一次，确定性兜底未成交单）
        self.last_close_sweep_date = None
        # 14:52 提前收盘价轮次已执行日期（内存 + DB advance_close_done_date 跨重启幂等）
        self.last_advance_close_date = None
        self._advance_done_loaded = False
        # 14:50 价不可得的告警节流（重试提示/失败告警各每交易日一条）
        self._advance_retry_warned_date = None
        self._advance_fail_date = None
        # B 执行价（14:52/14:51 1m bar open）告警节流：等成形 / 用兜底 / 双缺失
        self._advance_exec_wait_warned_date = None
        self._advance_exec_fallback_warned_date = None
        self._advance_exec_missing_date = None

    @staticmethod
    def _parse_hhmm(raw, default):
        """解析 HH:MM；非法/缺省回退 default（时点自由度收敛到分钟）。"""
        try:
            hh, mm = str(raw or "").strip().split(":")
            hh, mm = int(hh), int(mm)
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return (hh, mm)
        except (TypeError, ValueError):
            pass
        return default

    @property
    def _calendar(self):
        return self._get_calendar()

    def _next_trading_day(self, today: date) -> date:
        """次一交易日（跳过周末 + 日历非交易日，最多探 15 天）。

        仅用于 P1-1 日亏暂停解除门槛：日历离线时 fail-open 按工作日判，
        最坏退化为「跳过周末」，不会因日历缺失把暂停无限延长。
        """
        d = today + timedelta(days=1)
        for _ in range(15):
            if d.weekday() < 5 and self._calendar.is_trading_day(d):
                return d
            d += timedelta(days=1)
        return today + timedelta(days=1)

    # ---------------- 日终估值（E5/E6）----------------
    def maybe_daily_close(self, now: Optional[datetime] = None) -> None:
        """本机时间 ≥ 14:30 且当日未算过 → 对每个组合调一次 update_daily。

        日终一次：日内盈亏 daily_pnl = 当前总市值 - prev_close（昨日收盘，update_peak 跨日刷新），
        检测 daily_loss 暂停 + 次日恢复。用上海时间（实盘固有时点，同 C6 1d 快照时点）。
        幂等：last_daily_date 记录当日已算，避免每轮循环重复触发。
        """
        if now is None:
            now = self._clock()
        if (now.hour, now.minute) < _DAILY_CLOSE_TIME:
            return
        today = now.date()
        # 非交易日（周末/节假日）不算日终：避免周末/假期 14:30 用陈旧 bar 重复估值、
        # 误触 daily_loss 或错误刷新 prev_close。桥日历离线时 fail-open（工作日照常）。
        if not self._calendar.is_trading_day(today):
            return
        if self.last_daily_date == today:
            return
        self.last_daily_date = today
        # P1-1：14:30 触发的日亏暂停延长到**次一交易日**整日，D+2 首 bar 才恢复
        # （回测是「上一日收盘净值判日亏 → 暂停次日全天」，实盘 14:30 当场判若仍按
        # 「触发日次日恢复」就只停了当日 30 分钟）。见 RiskManager.update_daily。
        resume_after = self._next_trading_day(today)
        for portfolio in self._ctx.portfolios:
            # per-portfolio 日终估值 + update_daily + daily_loss 触发/恢复 + risk 事件
            # 归 BreakerService.on_daily_update（0010 步骤 4）；时点判断/幂等留本方法。
            self._breaker.on_daily_update(portfolio, today, resume_after=resume_after)
            # W2 现金管理增强（预留触发点，默认关）：命中阈值仅记决策事件，不产生订单。
            # 设计定案：实盘在 14:30 日终（与 daily_loss 同点）触发。估值口径与
            # on_daily_update 一致（现金 + 持仓按最近已知价市值），bar_time 取日终时点
            # 供决策事件时间戳与日内去重（_cash_enhance_last_date）。
            total = portfolio.account.cash + portfolio._holdings_value()
            portfolio.maybe_cash_enhancement(
                BarEvent(stocks={}, bar_time=datetime.combine(today, datetime.min.time()),
                         period="1d"),
                total,
            )
        logger.info(
            "daily close done (session %s, %s): %d portfolio(s) valuated",
            self._ctx.session_id, today, len(self._ctx.portfolios),
        )
        # A3：日终组合资金日序列（14:30 每交易日一次；失败只告警）
        if self._on_daily_snapshot is not None:
            try:
                self._on_daily_snapshot(today)
            except Exception:  # noqa: BLE001 — 观测快照失败不影响日终估值
                logger.warning(
                    "daily portfolio snapshot failed (session %s, %s)",
                    self._ctx.session_id, today, exc_info=True)

    # ---------------- 15:05 日线级别轮次（1d/1w/1mon，收盘落定后）----------------
    def _note_daily_freshness_skip(self, today, *, stale: int, total: int) -> None:
        """P2-11：日线轮次因「取数为空 / 最新日线非当日」跳过时记 risk 事件。

        旧实现只打一行 WARNING，工作台上完全看不出「今天日线策略没跑」（既没有
        信号也没有风控）。这里每交易日只发一条 ``gate=daily_bar_freshness``，
        后续分钟级重试成功则照常驱动。
        """
        if self._daily_freshness_noted_date == today:
            return
        self._daily_freshness_noted_date = today
        if total == 0:
            message = "日线取数为空（桥/TQ 未就绪或通道不支持日线），本交易日日线轮次暂未执行"
        else:
            message = ("最新日线非当日 %d/%d 只（日线未落盘），本交易日日线轮次暂未执行；"
                       "不会用昨日 bar 冒充今日收盘，等待落盘后重试" % (stale, total))
        logger.warning("daily bar freshness skip: %s (session %s)",
                       message, self._ctx.session_id)
        try:
            self._emit("risk", {
                "session_id": self._ctx.session_id,
                "gate": "daily_bar_freshness",
                "action": "skip",
                "rule": "daily_bar_not_today",
                "period": "1d",
                "stale_count": stale,
                "total_count": total,
                "message": message,
            })
        except Exception:  # noqa: BLE001 — 事件推送失败不影响轮次重试
            logger.warning("daily_bar_freshness 事件推送失败 (session %s)",
                           self._ctx.session_id, exc_info=True)
        # P1#3（2026-11-16）：该事件原只推 SSE，刷新页面即消失（注释却写「工作台
        # 可见」）。补落 live_decision_events；节流沿用上面每交易日一条，不会刷表。
        db = None
        try:
            db = self._ctx.db_session_factory()
            db.add(LiveDecisionEvent(
                live_session_id=self._ctx.session_id,
                gate="daily_bar_freshness",
                layer="risk",
                action="skip",
                bar_time=datetime.combine(today, datetime.min.time()),
                param_name="daily_bar_not_today",
                message=message[:200],
            ))
            db.commit()
        except Exception:  # noqa: BLE001
            if db is not None:
                db.rollback()
            logger.warning("daily_bar_freshness 落库失败 (session %s)",
                           self._ctx.session_id, exc_info=True)
        finally:
            if db is not None:
                db.close()

    def maybe_daily_bars(self, now: Optional[datetime] = None) -> None:
        """15:05 日线级别轮次：等周期收盘价落定后，用**完整 bar**驱动 1d/1w/1mon。

        默认口径（advance_close_enabled=0）：
          - 1d    → 每交易日 15:05 跑一次，用当日**已完成**日线（收盘价）算信号；
            订单由 _signal_bar_date_for 打 signal_bar_date=今日 → defer 到次一交易日
            首 bar 提交（与回测「信号 bar 收盘定规模、次日开盘成交」一致）。
          - 1w/1mon → 仅该周期**最后一个交易日**（周五 / 月末最后交易日）跑一次，
            用已完成周期 bar 算信号，同样次日执行；非周期末不驱动，绝不用未完成
            周/月 bar 产生信号。
        B（advance_close_enabled=1）：1d 若已在 14:52 提前轮次（maybe_advance_daily_close）
        成功执行则此处跳过；提前轮次失败（14:50 价/执行价不可得）时本方法按 post_close
        口径补跑 1d，避免勾了 B 反而当天无信号。1w/1mon 不受 B 影响，仍走本方法。

        数据源：_fetch_1d_bars → dispatcher.query_quote(period="1d")（tdx/ths/预警走
        通达信 TQ 日线，iquant 走 QMT 桥 /quote，goldminer 走掘金桥）。15:05 之后最后
        一根即已完成日线；另做「最新 bar 日期必须等于今天」校验，桥端未落盘时跳过该
        code，绝不拿昨日完成 bar 冒充今日收盘。
        1w/1mon 的 bar 桥端拉不到，信号由 inject_startup_periods 预填 signal_cache，
        本方法负责日切补注入与驱动。
        幂等：last_daily_bar_date + LiveSession.daily_bars_done_date（跨重启）。
        """
        if now is None:
            now = self._clock()
        if (now.hour, now.minute) < _DAILY_BARS_TIME:
            return
        today = now.date()
        # 非交易日不驱动日线（周末/假期不用陈旧快照触发 1d/1w/1mon 信号）。
        if not self._calendar.is_trading_day(today):
            return
        if self._daily_bars_done(today):
            return
        # 跨日残留的驱动记录不参与判定（幂等键含交易日，这里只做清理）
        if self._daily_driven_keys:
            self._daily_driven_keys = {
                k for k in self._daily_driven_keys if k[0] == today}
        cal = self._calendar
        periods: list = []
        # 1d：默认每交易日收盘落定后跑；B 开启且当日 14:52 提前轮次**已成功执行**时
        # 跳过（避免同日重复定信号）。若提前轮次失败（14:50 价或执行价均不可得，
        # advance_close_done_date 未落），这里按 post_close 口径补跑：用收盘价定信号、
        # 次日开盘执行，绝不能因为勾了 B 就整天没有 1d 信号与风控。
        if (not self._advance_enabled) or (not self._advance_done(today)):
            periods.append("1d")
        # 1w/1mon：仅周期最后一个交易日（周五 / 月末最后交易日）跑，其余日子跳过。
        if cal.is_last_trading_day_of_week(today):
            periods.append("1w")
        if cal.is_last_trading_day_of_month(today):
            periods.append("1mon")
        if not periods:
            # 只可能出现在「B 开启 + 今日非周期末」：标记当日已完成，避免每 60s 重复拉日线。
            self._mark_daily_bars_done(today)
            return
        # 拉 1d 快照（同时作为 1d 信号源与 1w/1mon 的 daily_time/stocks 载体）。
        # 长度/去重规则 **同启动预热（preheat）**。
        md = self._market_data
        bars_by_code = self._fetch_1d_bars()
        # 停止中：本次拉取只是被中断的部分结果，绝不拿残缺池驱动信号/下单
        if self._stop_requested():
            logger.info(
                "stop requested, skip daily bar drive (session %s)",
                self._ctx.session_id)
            return
        if not bars_by_code:
            # P2-11：取数为空同样意味着「今天日线策略没跑」，补事件让工作台可见。
            self._note_daily_freshness_skip(today, stale=0, total=0)
            return
        # 完成性校验：最后一根 1d bar 必须是「今天」，否则说明日线还没落盘
        # （桥/TQ 滞后）——跳过该 code，绝不用昨日完成 bar 冒充今日收盘。
        stale = 0
        total = len(bars_by_code)
        fresh: Dict[str, list] = {}
        for code, rows in bars_by_code.items():
            last_st = parse_bar_time(rows[-1]) if rows else None
            if last_st is None or last_st.date() != today:
                stale += 1
                continue
            fresh[code] = rows
        if stale:
            logger.warning(
                "daily bars: %d code(s) 最新日线不是今日（日线未落盘？），本次跳过 "
                "(session %s)", stale, self._ctx.session_id,
            )
            # P2-11：旧实现只打 WARNING → 工作台看不出「今天日线策略没跑」。
            self._note_daily_freshness_skip(today, stale=stale, total=total)
        bars_by_code = fresh
        if not bars_by_code:
            return
        # 增量回测交接：把本轮已取到的完整日线（含公式预热窗口）留给 15:10 的
        # 接缝续跑复用，避免为续跑额外卡通达信行情。交接失败不影响实盘。
        try:
            from core.services.incremental_backtest import record_daily_bars
            record_daily_bars(self._ctx.session_id, today, bars_by_code)
        except Exception:  # noqa: BLE001
            logger.warning(
                "incremental backtest: 日线交接失败 (session %s)",
                self._ctx.session_id, exc_info=True)
        # daily_time = 任一 code 最新 1d bar 的 stime（交易日 00:00）；解析失败用今日零点兜底
        daily_time = parse_bar_time(next(iter(bars_by_code.values()))[-1])
        if daily_time is None:
            daily_time = datetime.combine(today, datetime.min.time())
        # P1（2026-11-16）：幂等标记**延后到整轮驱动成功之后**（原实现在驱动前就写
        # daily_bars_done_date 并 commit）。旧实现下多组合会话一次短暂掉线（驱动循环
        # 遇 BridgeUnavailableError 直接 return）或进程在第一个组合驱动后崩溃，都会
        # 留下「已标记完成但剩余组合/周期从未驱动」的状态，当天永久不再重试——剩余
        # 组合的 1d/1w/1mon 信号与日线风控整体缺失。现在：离线组合跳过继续驱动其余
        # 组合，只要本轮有组合因桥离线未驱动就不标完成，由既有 60s tick 重试（已驱动
        # 成功的 (周期, 组合) 记在 _daily_driven_keys 里，重试不重复驱动）。
        # 日切检测：新 daily_time 的 1w/1mon cache miss → 通达信补注入
        # （外部信号文件模式：不补注入，信号只来自预警文件；bar 驱动/风控不变）
        reinjected = False
        if not self._external_signals_only:
            reinjected = md.startup_periods_missing(daily_time)
            if reinjected:
                md.inject_startup_periods(daily_time)
        # 收盘落定后最后一根 1d bar 即完成 bar（Close=15:00 收盘价）
        stocks = {code: to_ohlcv(bars[-1]) for code, bars in bars_by_code.items()}
        # C4(#28)：跨周期共享去重缓存（key 含 period，1d/1w/1mon 互不干扰）
        df_cache: Dict = {}
        raw_cache: Dict = {}
        undriven = 0
        for period in periods:
            # post_close=True：收盘后报不进交易所，immediate 策略也必须 defer 到次日。
            bar_event = BarEvent(stocks=stocks, bar_time=daily_time, period=period,
                                 post_close=True)
            for portfolio in self._ctx.portfolios:
                done_key = (today, period, portfolio.portfolio_id)
                if done_key in self._daily_driven_keys:
                    continue
                try:
                    self._on_bar(
                        portfolio, bar_event, bars_by_code=bars_by_code,
                        df_cache=df_cache, raw_cache=raw_cache,
                    )
                except BridgeUnavailableError as e:
                    # 掉线只跳过本组合（不中断整轮）：其余组合/周期照常驱动，
                    # 本轮的 done 标记在循环后统一决定（有 undriven 就不标，靠
                    # 60s tick 重试）。绝不留「已标记完成但未驱动」的状态。
                    self._set_bridge_offline()
                    undriven += 1
                    logger.warning(
                        "bridge unavailable on daily %s bar %s (portfolio %s): %s",
                        period, daily_time, portfolio.portfolio_id, e,
                    )
                    continue
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "daily %s bar error (portfolio %s, time %s)",
                        period, portfolio.portfolio_id, daily_time,
                    )
                    continue
                # 停止请求可能在驱动途中到达（2026-11-19 实证：15:06:22 停止会话，
                # _on_bar 因停止闸直接短路、并未做信号/风控判定，但旧实现仍记
                # 已驱动并标 daily_bars_done_date）。此时立即收尾，绝不标记完成，
                # 让账面如实反映「本轮未跑」而不是「已跑过」。
                if self._stop_requested():
                    logger.warning(
                        "daily bar drive truncated by stop request (session %s, %s, "
                        "portfolio %s): 不标记日线完成，留待重启后重跑",
                        self._ctx.session_id, daily_time, portfolio.portfolio_id)
                    return
                self._daily_driven_keys.add(done_key)
        if undriven:
            logger.warning(
                "daily bar drive incomplete (session %s, %s): %d portfolio(s) "
                "skipped by bridge offline, 不标记完成，下一轮 60s tick 重试",
                self._ctx.session_id, daily_time, undriven,
            )
            return
        # 收尾前再确认一次：停止请求若在最后一个组合驱动后、标记前到达，同样不能
        # 落「已完成」——否则风控被短路却看起来跑过了。
        if self._stop_requested():
            logger.warning(
                "daily bar drive truncated by stop request before mark-done "
                "(session %s, %s): 不标记日线完成", self._ctx.session_id, daily_time)
            return
        self._mark_daily_bars_done(today)
        logger.info(
            "daily bar driven (session %s, %s): %s completed bar over %d code(s), "
            "1w/1mon reinject=%s",
            self._ctx.session_id, daily_time, "+".join(periods), len(bars_by_code),
            reinjected,
        )

    # ---------------- 14:50 提前收盘价（仅 1d）----------------
    def _stop_requested(self) -> bool:
        """停止请求探针：长循环里定期检查，命中即收尾。"""
        fn = getattr(self._ctx, "stop_requested", None)
        if fn is None:
            return False
        try:
            return bool(fn())
        except Exception:  # noqa: BLE001 — 探针异常不得影响日终流程
            return False

    def _fetch_1d_bars(self) -> Dict[str, list]:
        """拉股票池 1d 快照（长度/去重同启动预热）。桥离线返回 {}（已标记离线）。"""
        md = self._market_data
        bars_by_code: Dict[str, list] = {}
        codes = list(md._bar_poller.stock_codes)
        for idx, code in enumerate(codes):
            # 停止中不再遍历全池：单只 /quote 最坏可等满桥超时（掘金 ~8s/只），
            # 496 只全池会让 engine.stop() 排队数十分钟、状态永远停在 stopping。
            if self._stop_requested():
                logger.info(
                    "stop requested, abort daily bars fetch (session %s): %d/%d code(s)",
                    self._ctx.session_id, idx, len(codes))
                break
            count = self._ctx.code_period_count.get(
                (code, "1d"), md._period_count.get("1d", md._formula_count)
            )
            try:
                bars = self._ctx.dispatcher.query_quote(
                    code, period="1d", count=count
                )
            except BridgeUnavailableError:
                self._set_bridge_offline()
                logger.warning("bridge offline on daily bars (session %s)", self._ctx.session_id)
                return {}
            if bars:
                bars_by_code[code] = md._sort_and_cap(bars, count)
        return bars_by_code

    def maybe_advance_daily_close(self, now: Optional[datetime] = None) -> None:
        """14:50 提前收盘价轮次（仅 1d，会话级开关，B 口径）。

        - 时点：≥ max(advance_close_time, 14:52) 且 < 15:00；
        - 信号价：快照合成器 14:50 分钟 bar（stime=14:51:00）的收盘价；
        - 委托价：**信号出现后下一根 1m bar 的再下一根**的开盘价（14:52 起始 bar，
          考虑 14:51 报单的报价时延）；14:53 起主取仍缺失则回退 14:51 起始 bar 的
          开盘价；两者都取不到 → 该 code 本日不下单（fail-closed，绝不退化成
          市价/合成价）；
        - 驱动：与收盘落定轮次完全同一套 on_bar/Portfolio/risk_manager 代码路径，
          period="1d"、mismatched_risk=False（1w/1mon 不参与、不提前）；
        - 执行：该轮 BarEvent 带 advance_close=True，信号**当日提交**（尾盘成交），
          不受策略 risk_timing=next_day 的次日顺延；
        - 幂等：内存标记 + LiveSession.advance_close_done_date 跨重启；
        - 取不到 14:50 价 → 本轮不执行（不伪造价格），下一轮重试，14:59 后 ERROR+risk 事件。
        """
        if not self._advance_enabled:
            return
        if now is None:
            now = self._clock()
        hhmm = (now.hour, now.minute)
        if hhmm < self._advance_time or hhmm >= _ADVANCE_CLOSE_DEADLINE:
            return
        today = now.date()
        if not self._calendar.is_trading_day(today):
            return
        # 半日市（P2-10）：当日 11:30 已收盘，14:52 提前轮次属"收盘后下单"，必须跳过。
        # 此时 1d 信号改由 15:05 收盘落定轮次按完整日线补跑（次日开盘执行），
        # 与 B 开启但提前轮次未执行时的口径一致，不丢信号。
        if self._calendar.is_half_day(today):
            return
        if self._advance_done(today):
            return
        bars_by_code = self._fetch_1d_bars()
        if not bars_by_code:
            return
        daily_time = parse_bar_time(next(iter(bars_by_code.values()))[-1])
        if daily_time is None:
            daily_time = datetime.combine(today, datetime.min.time())
        prices = self._advance_close_prices(list(bars_by_code), today)
        if not prices:
            if hhmm >= (14, 59):
                if self._advance_fail_date != today:
                    self._advance_fail_date = today
                    msg = ("14:50 快照价不可得，1d 提前轮次本日未执行；"
                           "15:05 收盘落定轮次将按完整日线补跑（次日开盘执行），"
                           "不伪造价格")
                    logger.error("%s (session %s)", msg, self._ctx.session_id)
                    self._emit("risk", {
                        "rule": "advance_close_unavailable", "message": msg,
                    })
            elif self._advance_retry_warned_date != today:
                self._advance_retry_warned_date = today
                logger.info(
                    "advance close: 14:50 1m bar 尚未闭合/未就绪，下一轮重试 (session %s)",
                    self._ctx.session_id,
                )
            return
        # 委托价（B 口径）：信号出现后「再下一根」1m bar 的开盘价（14:52 起始 bar）；
        # 14:53 起主取仍缺失则回退 14:51 起始 bar；两者都缺失的 code 本日不下单。
        exec_prices, used_fallback, missing = self._advance_exec_prices(
            list(prices), today, hhmm)
        if self._stop_requested():
            logger.info(
                "stop requested, skip advance-close order drive (session %s)",
                self._ctx.session_id)
            return
        if hhmm < _ADVANCE_EXEC_FALLBACK_TIME and set(exec_prices) != set(prices):
            # 14:52 分钟 bar 刚起步（forming 尚未建立）：等下一轮 60s，不告警。
            if self._advance_exec_wait_warned_date != today:
                self._advance_exec_wait_warned_date = today
                logger.info(
                    "advance close: 14:52 起始 1m bar 尚未成形，下一轮再报单 (session %s)",
                    self._ctx.session_id,
                )
            return
        if not exec_prices and not missing:
            return
        if used_fallback and self._advance_exec_fallback_warned_date != today:
            self._advance_exec_fallback_warned_date = today
            msg = ("14:52 起始 1m bar 开盘价不可得，已用 14:51 开盘价作委托价；"
                   "实际成交时点已接近收盘，请注意滑点")
            logger.warning("%s (session %s)", msg, self._ctx.session_id)
            self._emit("risk", {
                "rule": "advance_close_exec_price_fallback", "message": msg,
            })
        if (missing and hhmm >= _ADVANCE_EXEC_FALLBACK_TIME
                and self._advance_exec_missing_date != today):
            self._advance_exec_missing_date = today
            head = ",".join(missing[:10])
            if len(missing) > 10:
                head += "...(+%d)" % (len(missing) - 10)
            msg = ("14:52/14:51 起始 1m bar 开盘价均不可得，以下 code 本日不下单：%s"
                   % head)
            logger.error("%s (session %s)", msg, self._ctx.session_id)
            self._emit("risk", {
                "rule": "advance_close_exec_price_missing", "message": msg,
            })
        adv_bars: Dict[str, list] = {}
        adv_stocks: Dict[str, dict] = {}
        adv_exec: Dict[str, float] = {}
        skipped = 0
        for code, close in prices.items():
            px = exec_prices.get(code)
            if px is None:
                # fail-closed：两处开盘价都取不到 → 该 code 本轮不下单（不伪造价格）
                skipped += 1
                continue
            rows = bars_by_code.get(code) or []
            if not rows:
                skipped += 1
                continue
            last_st = parse_bar_time(rows[-1])
            # 只覆盖「今日」的 forming 1d bar；陈旧 bar 不伪造（跳过，不误发昨日信号）
            if last_st is None or last_st.date() != today:
                skipped += 1
                continue
            last = dict(rows[-1])
            last["close"] = close
            hi = _positive_num(last.get("high"))
            lo = _positive_num(last.get("low"))
            if hi is not None:
                last["high"] = max(hi, close)
            if lo is not None:
                last["low"] = min(lo, close)
            adv_bars[code] = list(rows[:-1]) + [last]
            adv_stocks[code] = to_ohlcv(last)
            adv_exec[code] = px
        if not adv_stocks:
            return
        # advance_close=True：该 1d bar 的信号当日提交（尾盘成交），不受 risk_timing
        # next_day 的 B-1 未来函数闸门延迟（见 portfolio._signal_bar_date_for）。
        # exec_prices：委托价用 14:52/14:51 起始 1m bar 的开盘价（数量/资金仍按
        # 14:50 收盘价定规模；成本价由成交回填/券商快照决定，与此无关）。
        bar_event = BarEvent(stocks=adv_stocks, bar_time=daily_time, period="1d",
                             advance_close=True, exec_prices=adv_exec)
        df_cache: Dict = {}
        raw_cache: Dict = {}
        for portfolio in self._ctx.portfolios:
            try:
                # mismatched_risk=False：周线/月线策略不参与本提前轮次（要求不提前）
                self._on_bar(
                    portfolio, bar_event, bars_by_code=adv_bars,
                    df_cache=df_cache, raw_cache=raw_cache,
                    mismatched_risk=False,
                )
            except BridgeUnavailableError as e:
                self._set_bridge_offline()
                logger.warning(
                    "bridge unavailable on advance 1d bar %s: %s", daily_time, e)
                return
            except Exception:  # noqa: BLE001
                logger.exception(
                    "advance 1d bar error (portfolio %s, time %s)",
                    portfolio.portfolio_id, daily_time,
                )
        self.last_advance_close_date = today
        self._persist_advance_done(today)
        logger.info(
            "advance close driven (session %s, %s): %d code(s), skipped=%d, at=%s",
            self._ctx.session_id, daily_time, len(adv_stocks), skipped,
            now.strftime("%H:%M:%S"),
        )

    def _advance_close_prices(self, codes, today) -> Dict[str, float]:
        """14:50 分钟 bar 收盘价（stime=14:51:00）。

        有快照合成器（tdx/ths/预警/goldminer）→ 取已完成 1m bar 中 stime 精确匹配；
        无合成器（iquant/QMT 桥自带实时分钟线）→ 通道 1m 快照，接受 stime 落在
        [14:49:30, 14:51:00] 的最后一根（兼容桥端 bar 起点/终点标注差异）。
        取不到的 code 不返回（调用方重试，绝不伪造价格）。
        """
        target = datetime.combine(today, time(*_ADVANCE_BAR_LABEL))
        prices: Dict[str, float] = {}
        synth = self._advance_synth
        if synth is not None:
            try:
                completed = synth.completed_bars(codes, count=30)
            except Exception:  # noqa: BLE001
                logger.exception(
                    "advance close: completed_bars failed (session %s)",
                    self._ctx.session_id)
                completed = {}
            for code, rows in (completed or {}).items():
                for row in reversed(rows or []):
                    st = parse_bar_time(row)
                    if st is None:
                        continue
                    if st == target:
                        px = _positive_num(row.get("close"))
                        if px is not None:
                            prices[code] = px
                        break
                    if st < target:
                        break
            return prices
        lo = target - timedelta(seconds=90)
        for code in codes:
            if self._stop_requested():
                logger.info(
                    "stop requested, abort advance-close quote fetch (session %s)",
                    self._ctx.session_id)
                break
            try:
                rows = self._ctx.dispatcher.query_quote(code, period="1m", count=10)
            except BridgeUnavailableError:
                self._set_bridge_offline()
                return prices
            except Exception:  # noqa: BLE001
                logger.warning(
                    "advance close: 1m quote failed for %s", code, exc_info=True)
                continue
            for row in reversed(rows or []):
                st = parse_bar_time(row)
                if st is None:
                    continue
                if lo <= st <= target:
                    px = _positive_num(row.get("close"))
                    if px is not None:
                        prices[code] = px
                    break
                if st < lo:
                    break
        return prices

    def _advance_exec_prices(self, codes, today, hhmm):
        """B 轮次委托价：14:52 起始 1m bar 的 open（14:53 起回退 14:51 起始 bar）。

        返回 ``(prices, used_fallback, missing)``：
          - prices：{code: open}，仅含取到的 code；
          - used_fallback：是否有 code 用了 14:51 兜底（调用方每交易日告警一次）；
          - missing：两处都取不到的 code（调用方 fail-closed，不下单，绝不伪造价格）。

        快照合成器路径（tdx/ths/预警/goldminer）：标签语义固定为「分钟终点」，
        直接精确匹配 label = start + 1min（14:52 → 14:53:00，14:51 → 14:52:00）。

        无合成器路径（iquant / QMT 桥）：桥端「分钟起点/终点」标注约定不确定，
        因此按**信号 bar 的序号**定位（与 _advance_close_prices 同口径，先取
        「stime ≤ 14:51:00 的最后一根」= 信号 bar），再顺延 1 根 / 2 根取 open：
          - 顺延 2 根 = 信号出现后的「再下一根」1m bar（用户口径的主执行价）；
          - 顺延 1 根 = 紧随信号 bar 的下一根（14:51 兜底）。
        这与用户口径「下一根、再下一根」严格对应，不依赖桥端的标注约定。
        """
        primary_start = datetime.combine(today, time(*_ADVANCE_EXEC_START))
        fallback_start = datetime.combine(today, time(*_ADVANCE_EXEC_FALLBACK_START))
        target = datetime.combine(today, time(*_ADVANCE_BAR_LABEL))
        use_fallback = tuple(hhmm) >= _ADVANCE_EXEC_FALLBACK_TIME
        prices: Dict[str, float] = {}
        used_fallback = False
        missing = []
        synth = self._advance_synth
        if synth is not None:
            try:
                prices = dict(synth.open_at(codes, primary_start) or {})
            except Exception:  # noqa: BLE001
                logger.exception(
                    "advance exec: open_at failed (session %s)", self._ctx.session_id)
                prices = {}
            if use_fallback:
                lacks = [c for c in codes if c not in prices]
                if lacks:
                    try:
                        fb = dict(synth.open_at(lacks, fallback_start) or {})
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "advance exec: fallback open_at failed (session %s)",
                            self._ctx.session_id)
                        fb = {}
                    if fb:
                        prices.update(fb)
                        used_fallback = True
            missing = [c for c in codes if c not in prices]
            return prices, used_fallback, missing
        for code in codes:
            try:
                rows = self._ctx.dispatcher.query_quote(code, period="1m", count=10)
            except BridgeUnavailableError:
                self._set_bridge_offline()
                missing.extend([c for c in codes if c not in prices])
                return prices, used_fallback, missing
            except Exception:  # noqa: BLE001
                logger.warning(
                    "advance exec: 1m quote failed for %s", code, exc_info=True)
                missing.append(code)
                continue
            parsed = []
            for row in rows or []:
                st = parse_bar_time(row)
                if st is None or st.date() != today:
                    continue
                parsed.append((st, row))
            parsed.sort(key=lambda item: item[0])
            idx = -1
            for i, (st, _row) in enumerate(parsed):
                if st <= target:
                    idx = i
                else:
                    break
            if idx >= 0 and idx + 2 < len(parsed):
                px = _positive_num(parsed[idx + 2][1].get("open"))
                if px is not None:
                    prices[code] = px
                    continue
            if use_fallback and idx >= 0 and idx + 1 < len(parsed):
                px = _positive_num(parsed[idx + 1][1].get("open"))
                if px is not None:
                    prices[code] = px
                    used_fallback = True
                    continue
            missing.append(code)
        return prices, used_fallback, missing

    def _daily_bars_done(self, today) -> bool:
        """15:05 日线轮次当日是否已执行（内存 → DB 一次性加载，重启后不重复跑）。"""
        if self.last_daily_bar_date == today:
            return True
        if not self._daily_bars_done_loaded:
            self._daily_bars_done_loaded = True
            db = None
            try:
                db = self._ctx.db_session_factory()
                row = db.query(LiveSession).filter(
                    LiveSession.id == self._ctx.session_id).first()
                raw = getattr(row, "daily_bars_done_date", None) if row else None
                if raw:
                    try:
                        self.last_daily_bar_date = date.fromisoformat(str(raw)[:10])
                    except ValueError:
                        pass
            except Exception:  # noqa: BLE001
                logger.warning(
                    "daily bars state load failed (session %s)",
                    self._ctx.session_id, exc_info=True)
            finally:
                if db is not None:
                    db.close()
        return self.last_daily_bar_date == today

    def _mark_daily_bars_done(self, today) -> None:
        """内存 + DB 落库标记当日 15:05 日线轮次已执行（跨重启幂等）。"""
        self.last_daily_bar_date = today
        db = None
        try:
            db = self._ctx.db_session_factory()
            row = db.query(LiveSession).filter(
                LiveSession.id == self._ctx.session_id).first()
            if row is not None:
                row.daily_bars_done_date = today.isoformat()
            db.commit()
        except Exception:  # noqa: BLE001
            if db is not None:
                db.rollback()
            logger.warning(
                "daily bars state persist failed (session %s)",
                self._ctx.session_id, exc_info=True)
        finally:
            if db is not None:
                db.close()

    def _advance_done(self, today) -> bool:
        """当日是否已执行（内存 → DB 一次性加载，重启后不重复跑）。"""
        if self.last_advance_close_date == today:
            return True
        if not self._advance_done_loaded:
            self._advance_done_loaded = True
            db = None
            try:
                db = self._ctx.db_session_factory()
                row = db.query(LiveSession).filter(
                    LiveSession.id == self._ctx.session_id).first()
                raw = getattr(row, "advance_close_done_date", None) if row else None
                if raw:
                    try:
                        self.last_advance_close_date = date.fromisoformat(str(raw)[:10])
                    except ValueError:
                        pass
            except Exception:  # noqa: BLE001
                logger.warning(
                    "advance close state load failed (session %s)",
                    self._ctx.session_id, exc_info=True)
            finally:
                if db is not None:
                    db.close()
        return self.last_advance_close_date == today

    def _persist_advance_done(self, today) -> None:
        db = None
        try:
            db = self._ctx.db_session_factory()
            row = db.query(LiveSession).filter(
                LiveSession.id == self._ctx.session_id).first()
            if row is not None:
                row.advance_close_done_date = today.isoformat()
            db.commit()
        except Exception:  # noqa: BLE001
            if db is not None:
                db.rollback()
            logger.warning(
                "advance close state persist failed (session %s)",
                self._ctx.session_id, exc_info=True)
        finally:
            if db is not None:
                db.close()

    # ---------------- 收盘清扫（15:05 确定性兜底）----------------
    def maybe_close_sweep(self, now: Optional[datetime] = None) -> None:
        """交易日 15:05 后对当日仍未完结单做一次确定性兜底（幂等）。

        实时轮询（orders.poll_deals，5s）是「尽力而为」：桥抖动/被饿死/ORDER 实时表移除单
        都可能让单残留 submitted/partial。收盘后状态已全部收敛，做一次权威清扫：
          1) /orders + /deals 各查一次，补 order_ref 定位 + 按 order_ref/remark 回填成交；
          2) sync_terminal_order_status 处理 53部撤/54全撤/57废单（含 partial 先 apply 再 cancel）；
          3) A 股规则：收盘仍未成交（含 48-52 在途/55 部成/实时表缺席）的单一律标 canceled
             ——收盘后不会再有成交，未成交部分被交易所/券商自动撤。partial 已回填的部分成交
             先 apply 落持仓再 cancel（与 53 部撤同处理，不丢成交）。
        与实时轮询的关系：清扫是「权威收口」，实时是「近实时反馈」；清扫幂等可重复执行。
        非交易日/桥离线：跳过（下轮或下个交易日重试，不误标）。
        """
        if now is None:
            now = self._clock()
        if (now.hour, now.minute) < _MARKET_CLOSE_SWEEP_TIME:
            return
        today = now.date()
        if not self._calendar.is_trading_day(today):
            return
        if self.last_close_sweep_date == today:
            return
        db = self._ctx.db_session_factory()
        try:
            pending = (
                db.query(LiveOrder)
                .filter(
                    LiveOrder.live_session_id == self._ctx.session_id,
                    LiveOrder.status.in_(["submitted", "partial"]),
                )
                .all()
            )
            if not pending:
                self.last_close_sweep_date = today
                logger.info(
                    "close sweep: no pending orders, nothing to do (session %s, %s)",
                    self._ctx.session_id, today,
                )
                self._report_daily_gap(today)
                self._report_daily_cross_check(today)
                return
            logger.info(
                "close sweep start (session %s, %s): %d pending order(s)",
                self._ctx.session_id, today, len(pending),
            )
            # 1) 一次 /orders + /deals：补 ref + 回填成交（复用实时轮询同套逻辑）。
            try:
                orders = self._ctx.dispatcher.query_orders()
            except BridgeUnavailableError:
                logger.warning("close sweep: bridge offline (/orders), skip")
                return
            claimed_refs = set(
                r[0] for r in db.query(LiveOrder.order_ref).filter(
                    LiveOrder.live_session_id == self._ctx.session_id,
                    LiveOrder.order_ref.isnot(None),
                ).all()
            )
            for lo in pending:
                if lo.order_ref is None:
                    self._orders.try_match_order_ref(lo, claimed_refs, orders=orders)
                    if lo.order_ref is not None:
                        claimed_refs.add(lo.order_ref)
            db.commit()

            try:
                deals = self._ctx.dispatcher.query_deals()
            except BridgeUnavailableError:
                deals = None
            if deals is not None:
                for lo in pending:
                    if lo.status not in ("submitted", "partial"):
                        continue
                    if lo.order_ref is not None:
                        matched = [d for d in deals if d.get("order_ref") == lo.order_ref]
                        if matched:
                            self._orders.backfill_order(db, lo, matched)
                            continue
                    if lo.bridge_order_id:
                        expected_remark = lo.bridge_order_id[:20]
                        matched = [d for d in deals if d.get("remark") == expected_remark]
                        if matched:
                            self._orders.backfill_order(db, lo, matched)
                db.commit()

            # 2) 终态同步：53/54 → canceled（partial 先 apply）、57 → rejected。
            self._orders.sync_terminal_order_status(db, pending, orders, deals)
            db.commit()

            # 3) A 股收盘兜底：仍在 submitted/partial 的单 = 收盘未成交 → canceled。
            #    实时表缺席（已撤单被移除）或状态仍在途（48-52/55/255）都落此：收盘后
            #    不会再成交，未成交即撤。已有部分成交（filled_quantity>0）先 apply 再 cancel。
            #
            #    资金安全：若 /orders 报 traded_volume > filled_quantity，说明 /deals 成交回报
            #    滞后（成交价/量/金额尚未回填齐），此时绝不能 cancel——会丢真实成交。延后本轮
            #    （不置 last_close_sweep_date），下轮 60s tick 等 /deals 追上再清扫，与
            #    sync_terminal_order_status 同一守卫。仅对 /orders 里**确实查到**的单可比
            #    traded_volume；实时表缺席的单无此字段，按 A 股规则直接 cancel（不会再有成交）。
            by_ref = {}
            for o in (orders or []):
                ref = o.get("order_ref")
                if ref is not None:
                    by_ref[ref] = o
            deferred = 0
            for lo in pending:
                if lo.status not in ("submitted", "partial"):
                    continue
                o = by_ref.get(lo.order_ref) if lo.order_ref is not None else None
                if o is not None:
                    traded_volume = int(o.get("traded_volume") or 0)
                    if traded_volume > 0 and int(lo.filled_quantity or 0) < traded_volume:
                        deferred += 1
                        logger.warning(
                            "close sweep: order %s %s %s deferring — /orders traded_volume=%s "
                            "ahead of filled=%s, waiting for /deals backfill",
                            lo.id, lo.trade_type, lo.stock_code,
                            traded_volume, int(lo.filled_quantity or 0),
                        )
                        continue
                if int(lo.filled_quantity or 0) > 0:
                    # 已有部分成交（partial，或 submitted 带成交的不一致态）：先 apply 落持仓
                    # 再 cancel——撤单后不会再有成交，这是最后的 apply 时机。
                    trade = (
                        db.query(LiveTrade)
                        .filter(LiveTrade.live_order_id == lo.id)
                        .first()
                    )
                    if trade is not None:
                        self._orders.apply_filled_trade(
                            lo, trade.price, trade.quantity, trade.amount, trade.commission
                        )
                # A-3：标 canceled 前 best-effort 撤真实单（通道无 cancel_order 跳过；
                # 异常/失败仅 warning，不阻断清扫终态——收盘后当日单自然失效）。
                if lo.order_ref is not None:
                    self._best_effort_cancel(lo)
                lo.status = "canceled"
                if not lo.error_message:
                    lo.error_message = "close sweep: unfilled remainder canceled after market close"
                self._orders.pending_orders.pop(lo.id, None)
                logger.info(
                    "close sweep: order %s %s %s marked canceled (filled=%s/%s)",
                    lo.id, lo.trade_type, lo.stock_code,
                    int(lo.filled_quantity or 0), lo.quantity,
                )
                self._emit("order", {
                    "portfolio_id": lo.portfolio_strategy_id,
                    "order_id": lo.id,
                    "status": "canceled",
                    "stock_code": lo.stock_code,
                    "trade_type": (lo.trade_type or "").upper(),
                    "quantity": lo.quantity,
                    "filled_quantity": int(lo.filled_quantity or 0),
                    "error_message": lo.error_message,
                })
            db.commit()
            self._orders.sync_pending_orders(db)
            if deferred:
                # 有成交回报滞后的单：本轮不标记完成，下轮 60s tick 重试，等 /deals 回填齐。
                logger.warning(
                    "close sweep: %d order(s) deferred for /deals backfill, will retry "
                    "(session %s, %s)", deferred, self._ctx.session_id, today,
                )
                return
            self.last_close_sweep_date = today
            logger.info("close sweep done (session %s, %s)", self._ctx.session_id, today)
            self._report_daily_gap(today)
            self._report_daily_cross_check(today)
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.exception("close sweep error (session %s)", self._ctx.session_id)
        finally:
            db.close()

    def _report_daily_gap(self, today: date) -> None:
        """F3：收盘清扫完成后回调引擎输出当日缺失分钟段清单（只观测，失败只告警）。"""
        cb = self._on_daily_gap_report
        if not callable(cb):
            return
        try:
            cb(today)
        except Exception:  # noqa: BLE001 — 缺失清单失败不影响日终
            logger.warning(
                "daily gap report failed (session %s, %s)",
                self._ctx.session_id, today, exc_info=True,
            )

    def _report_daily_cross_check(self, today: date) -> None:
        """F4：收盘清扫完成后回调引擎做抽样交叉校验（只观测，失败只告警）。"""
        cb = self._on_daily_cross_check
        if not callable(cb):
            return
        try:
            cb(today)
        except Exception:  # noqa: BLE001 — 交叉校验失败不影响日终
            logger.warning(
                "daily cross check failed (session %s, %s)",
                self._ctx.session_id, today, exc_info=True,
            )

    def _best_effort_cancel(self, lo) -> None:
        """A-3：清扫终态前 best-effort 撤真实委托。

        通道无 cancel_order（如 QMT 桥）→ 跳过；异常/失败仅 warning——收盘后
        交易所/券商对当日单自动失效，撤单失败不阻断清扫终态。
        """
        cancel = getattr(self._ctx.dispatcher, "cancel_order", None)
        if not callable(cancel) or getattr(
            self._ctx.dispatcher, "supports_cancel", True
        ) is False:
            return
        try:
            if cancel(lo.order_ref, lo.stock_code):
                logger.info(
                    "close sweep: cancel_order ok (order %s ref %s)",
                    lo.id, lo.order_ref,
                )
            else:
                logger.warning(
                    "close sweep: cancel_order failed (order %s ref %s): %s",
                    lo.id, lo.order_ref,
                    getattr(self._ctx.dispatcher, "last_error", ""),
                )
        except Exception as e:  # noqa: BLE001
            logger.warning(
                "close sweep: cancel_order error (order %s ref %s): %s",
                lo.id, lo.order_ref, e,
            )
