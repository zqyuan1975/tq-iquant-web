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
import re
from datetime import date, datetime, time, timedelta
from typing import Callable, Dict, Optional, Set

from ..bar_poller import parse_bar_time, to_ohlcv
from ..event import BarEvent
from ..http_bridge_dispatcher import BridgeUnavailableError
from ...models import LiveDecisionEvent, LiveOrder, LiveSession, LiveSignal, LiveTrade
from ...tq.gate import P0, tdx_priority
from .. import period_semantics as ps
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
        period_signal_mode: str = ps.PERIOD_CLOSE,
        on_period_alert: Optional[Callable[[object, BarEvent], None]] = None,
        on_reversal_close: Optional[Callable[[object, BarEvent, dict], None]] = None,
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
        # 信号判定维度（规范 2026-11-23）：period_close / dynamic。pre_close_1450
        # 由 _advance_enabled 表达（仅 1d）。三者互斥，统一走 period_semantics。
        self._period_mode = ps.normalize_mode(period_signal_mode)
        # 动态 K 线的预警回调（引擎注入；只进 SignalMonitor，不进下单链）。
        self._on_period_alert = on_period_alert
        # close_next_open 反转平仓回调（引擎注入；生成次日 deferred 卖单）。
        self._on_reversal_close = on_reversal_close
        # 14:50 收盘复核幂等标记（每日一次）。
        self._advance_recheck_done_date: Optional[date] = None
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
            message = "日线取数为空（桥/TQ 未就绪或通道不支持日线），本交易日无可用日线"
        else:
            message = ("最新日线非当日 %d/%d 只按陈旧剔除，其余继续执行；"
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

    def _note_period_stale_alerts(self, today, stale_codes: Set[str],
                                  periods: Optional[list]) -> None:
        """B3：1w/1mon 轮次下，因陈旧/停牌被剔除的票逐票补 alert_only 决策事件。

        这些票本轮不参与周期信号判定，既不产生信号也不产生委托；但工作台需要
        看得见「哪只票因为日线没落盘/停牌被剔除」，故逐票写一条
        ``action=alert_only`` 的决策事件（不产生订单）。
        """
        targets = [p for p in ("1w", "1mon") if p in (periods or ())]
        if not targets or not stale_codes:
            return
        bar_time = datetime.combine(today, datetime.min.time())
        for code in sorted(stale_codes):
            for period in targets:
                message = ("%s 因陈旧/停牌被剔除，%s 当轮不参与判定，不产生委托"
                           % (code, period))
                try:
                    self._emit("risk", {
                        "session_id": self._ctx.session_id,
                        "gate": "daily_bar_freshness",
                        "action": "alert_only",
                        "rule": "daily_bar_not_today",
                        "period": period,
                        "stock_code": code,
                        "message": message,
                    })
                except Exception:  # noqa: BLE001 — 事件推送失败不影响轮次
                    logger.warning(
                        "period stale alert 推送失败 (session %s, %s %s)",
                        self._ctx.session_id, code, period, exc_info=True)
                db = None
                try:
                    db = self._ctx.db_session_factory()
                    db.add(LiveDecisionEvent(
                        live_session_id=self._ctx.session_id,
                        gate="daily_bar_freshness",
                        layer="risk",
                        action="alert_only",
                        bar_time=bar_time,
                        stock_code=code,
                        param_name="daily_bar_not_today",
                        message=message[:200],
                    ))
                    db.commit()
                except Exception:  # noqa: BLE001
                    if db is not None:
                        db.rollback()
                    logger.warning(
                        "period stale alert 落库失败 (session %s, %s %s)",
                        self._ctx.session_id, code, period, exc_info=True)
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
        # 1w/1mon：默认只在周期最后一个交易日驱动（交易日历判定，周五休市自动前移）。
        # dynamic 模式下只做预警：不进 periods（不下单），改由动态预警段逐日滚动。
        for period in ps.DYNAMIC_PERIODS:
            if ps.is_dynamic(period, self._period_mode):
                continue
            if ps.is_period_end_day(cal, period, today):
                periods.append(period)
        dynamic_periods: list = []
        if not self._external_signals_only:
            dynamic_periods = [
                p for p in ps.DYNAMIC_PERIODS
                if ps.is_dynamic(p, self._period_mode)
                and any(st.period == p for pf in self._ctx.portfolios
                        for st in pf.strategies)
            ]
        # 14:50 收盘复核：只对当日确有 14:50 判定留档的代码重算最终收盘信号。
        advance_codes = self._advance_judged_codes(today)
        if not periods and not dynamic_periods and not advance_codes:
            # 只可能出现在「B 开启 + 今日非周期末 + 无 14:50 判定」：标记当日已完成。
            self._mark_daily_bars_done(today)
            return
        # 拉 1d 快照（同时作为 1d 信号源与 1w/1mon 的 daily_time/stocks 载体）。
        # 长度/去重规则 **同启动预热（preheat）**。仅收盘复核时只取留档代码。
        md = self._market_data
        fetch_codes = None
        if not periods and not dynamic_periods:
            fetch_codes = sorted(advance_codes)
        bars_by_code = self._fetch_1d_bars(codes=fetch_codes)
        # 停止中：本次拉取只是被中断的部分结果，绝不拿残缺池驱动信号/下单
        if self._stop_requested():
            logger.info(
                "stop requested, skip daily bar drive (session %s)",
                self._ctx.session_id)
            return
        if not bars_by_code:
            if not periods and not dynamic_periods and advance_codes:
                # 仅收盘复核：14:50 留档代码当天无可用日线 → final_close 未知，
                # 仍写复核记录（reversed=None），并标记完成避免每 60s 重试。
                self._reconcile_advance_close(
                    today, datetime.combine(today, datetime.min.time()), {}, advance_codes)
                self._advance_recheck_done_date = today
                self._mark_daily_bars_done(today)
                return
            # P2-11：取数为空同样意味着「今天日线策略没跑」，补事件让工作台可见。
            self._note_daily_freshness_skip(today, stale=0, total=0)
            return
        # 完成性校验：最后一根 1d bar 必须是「今天」，否则说明日线还没落盘
        # （桥/TQ 滞后）——跳过该 code，绝不用昨日完成 bar 冒充今日收盘。
        stale = 0
        stale_codes: Set[str] = set()
        total = len(bars_by_code)
        fresh: Dict[str, list] = {}
        for code, rows in bars_by_code.items():
            last_st = parse_bar_time(rows[-1]) if rows else None
            if last_st is None or last_st.date() != today:
                stale += 1
                stale_codes.add(code)
                continue
            fresh[code] = rows
        if stale:
            logger.warning(
                "daily bars: %d code(s) 最新日线不是今日（日线未落盘？），本次跳过 "
                "(session %s)", stale, self._ctx.session_id,
            )
            # P2-11：旧实现只打 WARNING → 工作台看不出「今天日线策略没跑」。
            self._note_daily_freshness_skip(today, stale=stale, total=total)
            # B3：1w/1mon 轮次下，因陈旧/停牌剔除的票逐票补 alert_only 事件
            # （只告警，不产生委托）。
            self._note_period_stale_alerts(today, stale_codes, periods)
        bars_by_code = fresh
        if not bars_by_code:
            if not periods and not dynamic_periods and advance_codes:
                self._reconcile_advance_close(
                    today, datetime.combine(today, datetime.min.time()), {}, advance_codes)
                self._advance_recheck_done_date = today
                self._mark_daily_bars_done(today)
                return
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
        daily_time = parse_bar_time(next(iter(bars_by_code.values()))[-1]) \
            if bars_by_code else None
        if daily_time is None:
            daily_time = datetime.combine(today, datetime.min.time())
        # P1（2026-11-16）：幂等标记**延后到整轮驱动成功之后**（原实现在驱动前就写
        # daily_bars_done_date 并 commit）。旧实现下多组合会话一次短暂掉线（驱动循环
        # 遇 BridgeUnavailableError 直接 return）或进程在第一个组合驱动后崩溃，都会
        # 留下「已标记完成但剩余组合/周期从未驱动」的状态，当天永久不再重试——剩余
        # 组合的 1d/1w/1mon 信号与日线风控整体缺失。现在：离线组合跳过继续驱动其余
        # 组合，只要本轮有组合因桥离线未驱动就不标完成，由既有 60s tick 重试（已驱动
        # 成功的 (周期, 组合) 记在 _daily_driven_keys 里，重试不重复驱动）。
        # 周期末信号注入（规范 2026-11-23）：
        # - period_close：只对当轮要驱动的 1w/1mon **强制重注入**，用完成周期 bar
        #   覆盖启动时可能写入的滚动值（旧实现只看 cache miss → 同日启动会漏刷新）；
        # - dynamic：每交易日强制重注入滚动值，仅用于预警；
        # - 外部信号文件模式：不注入（信号只来自预警文件）。
        period_end_periods = [p for p in periods if p in ps.DYNAMIC_PERIODS]
        inject_periods = period_end_periods + dynamic_periods
        reinjected = False
        if not self._external_signals_only and inject_periods:
            written = md.inject_startup_periods(
                daily_time, periods=inject_periods, force=True)
            reinjected = written > 0
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
        # dynamic（仅 1w/1mon）：滚动值只进预警通道，绝不进入 _on_bar/下单链。
        if dynamic_periods and self._on_period_alert is not None:
            for period in dynamic_periods:
                alert_bar = BarEvent(stocks=stocks, bar_time=daily_time, period=period,
                                     post_close=True)
                for portfolio in self._ctx.portfolios:
                    try:
                        self._on_period_alert(portfolio, alert_bar)
                    except Exception:  # noqa: BLE001 — 预警失败不影响日终收尾
                        logger.exception(
                            "dynamic period alert error (portfolio %s, %s)",
                            portfolio.portfolio_id, period)
            logger.info(
                "dynamic period alerts (session %s, %s): %s（仅预警，不下单）",
                self._ctx.session_id, daily_time, "+".join(dynamic_periods))
        # 14:50 收盘复核：用最终日线收盘价重判并标记反转（每日一次，幂等）。
        if advance_codes and self._advance_recheck_done_date != today:
            self._reconcile_advance_close(today, daily_time, bars_by_code, advance_codes)
            self._advance_recheck_done_date = today
        self._mark_daily_bars_done(today)
        # P0-1（2026-11-23）：15:05 日线轮次后触发一次公司行为漏记检测。
        # 后台线程、只写诊断表；失败只告警，不影响日终收尾与实盘下单语义。
        try:
            from core.services.data_observability import trigger_auto_audit

            trigger_auto_audit(list(bars_by_code.keys()), reason="daily_close")
        except Exception:  # noqa: BLE001 — 观测触发失败不影响日终收尾
            logger.warning("日终自动审计触发失败（忽略）", exc_info=True)
        logger.info(
            "daily bar driven (session %s, %s): %s completed bar over %d code(s), "
            "1w/1mon reinject=%s",
            self._ctx.session_id, daily_time, "+".join(periods) or "-",
            len(bars_by_code), reinjected,
        )

    # ---------------- 14:50 提前收盘价（仅 1d）----------------
    def _advance_judged_codes(self, today: date) -> Set[str]:
        """当日 14:50 判定留档涉及的代码（供 15:05 收盘复核）。

        只读查询；开启 B 且当日确有留档才返回非空。查询失败按空处理（复核跳过，
        不影响日终主链）。
        """
        if not self._advance_enabled:
            return set()
        start = datetime.combine(today, time.min)
        end = start + timedelta(days=1)
        db = self._ctx.db_session_factory()
        try:
            rows = (
                db.query(LiveDecisionEvent.stock_code)
                .filter(
                    LiveDecisionEvent.live_session_id == self._ctx.session_id,
                    LiveDecisionEvent.gate == "advance_close_judgement",
                    LiveDecisionEvent.bar_time >= start,
                    LiveDecisionEvent.bar_time < end,
                )
                .distinct()
                .all()
            )
            return {r[0] for r in rows if r[0]}
        except Exception:  # noqa: BLE001 — 复核是observability，不阻断日终
            logger.warning("advance_close judgement query failed", exc_info=True)
            return set()
        finally:
            db.close()

    def _record_advance_judgements(
        self, today: date, daily_time: datetime, prices: Dict[str, float],
        judge_sources: Optional[Dict[str, str]] = None,
        judge_lags: Optional[Dict[str, Optional[float]]] = None,
        exec_sources: Optional[Dict[str, str]] = None,
        exec_prices: Optional[Dict[str, float]] = None,
    ) -> None:
        """14:50 判定留档：判定价 + 信号方向 + 判定价/执行价双来源（逐只）。

        写 LiveDecisionEvent(gate="advance_close_judgement")，同日幂等。
        ``judge_sources``（``native_1m``/``snapshot_synth``/``none``）与
        ``exec_sources``（``bar_1m``/``snapshot_synth``/``fallback_1451``）
        **分别记录，不合并**；``judge_lags`` 为原生判定价滞后秒数。
        """
        start = datetime.combine(today, time.min)
        end = start + timedelta(days=1)
        db = self._ctx.db_session_factory()
        try:
            exists = (
                db.query(LiveDecisionEvent.id)
                .filter(
                    LiveDecisionEvent.live_session_id == self._ctx.session_id,
                    LiveDecisionEvent.gate == "advance_close_judgement",
                    LiveDecisionEvent.bar_time >= start,
                    LiveDecisionEvent.bar_time < end,
                )
                .first()
            )
            if exists:
                return
            rows = (
                db.query(LiveSignal)
                .filter(
                    LiveSignal.live_session_id == self._ctx.session_id,
                    LiveSignal.period == "1d",
                    LiveSignal.bar_time == daily_time,
                )
                .all()
            )
            pid_by_strategy = {
                st.strategy_id: pf.portfolio_id
                for pf in self._ctx.portfolios for st in pf.strategies
            }
            written = 0
            for r in rows:
                px = prices.get(r.stock_code)
                if px is None:
                    continue
                db.add(LiveDecisionEvent(
                    live_session_id=self._ctx.session_id,
                    gate="advance_close_judgement",
                    layer="signal",
                    action="judge",
                    portfolio_id=pid_by_strategy.get(r.strategy_id),
                    strategy_id=r.strategy_id,
                    stock_code=r.stock_code,
                    bar_time=daily_time,
                    param_name="advance_close",
                    param_value=float(px),
                    actual_value=None,
                    message=(
                        "judge=%.4f at 14:50 judge_src=%s lag=%s signal=%s/%s "
                        "exec=%s exec_src=%s final_close=None reversed=None"
                        % (
                            float(px),
                            (judge_sources or {}).get(r.stock_code, "unknown"),
                            (judge_lags or {}).get(r.stock_code),
                            r.signal_type, r.signal_name,
                            (exec_prices or {}).get(r.stock_code),
                            (exec_sources or {}).get(r.stock_code, "unknown"),
                        )
                    )[:200],
                ))
                written += 1
            if written:
                db.commit()
        except Exception:  # noqa: BLE001 — 留档失败不得影响已提交的委托
            db.rollback()
            logger.warning("advance_close judgement persist failed", exc_info=True)
        finally:
            db.close()

    def _reconcile_advance_close(
        self, today: date, daily_time: datetime, bars_by_code: Dict[str, list],
        codes: Set[str],
    ) -> None:
        """15:05 收盘复核：用最终日线收盘价重判，记录 final_close 与 reversed。

        判定信号集合来自 14:50 的 LiveSignal；最终信号集合用 TQ 公式按完成日线
        重算。写 LiveDecisionEvent(gate="advance_close_recheck")，同日幂等。
        重算失败（无缓存）→ reversed=None（未知），绝不当成「未反转」。
        """
        start = datetime.combine(today, time.min)
        end = start + timedelta(days=1)
        db = self._ctx.db_session_factory()
        # 规范 3.6.3：待平仓队列（portfolio_id → strategy_id → codes），库提交后统一驱动。
        pending: Dict[int, Dict[int, Set[str]]] = {}
        try:
            exists = (
                db.query(LiveDecisionEvent.id)
                .filter(
                    LiveDecisionEvent.live_session_id == self._ctx.session_id,
                    LiveDecisionEvent.gate == "advance_close_recheck",
                    LiveDecisionEvent.bar_time >= start,
                    LiveDecisionEvent.bar_time < end,
                )
                .first()
            )
            if exists:
                return
            judged = (
                db.query(LiveDecisionEvent)
                .filter(
                    LiveDecisionEvent.live_session_id == self._ctx.session_id,
                    LiveDecisionEvent.gate == "advance_close_judgement",
                    LiveDecisionEvent.bar_time >= start,
                    LiveDecisionEvent.bar_time < end,
                )
                .all()
            )
            if not judged:
                return
            judged_names: Dict[tuple, Set[str]] = {}
            sig_rows = (
                db.query(LiveSignal)
                .filter(
                    LiveSignal.live_session_id == self._ctx.session_id,
                    LiveSignal.period == "1d",
                    LiveSignal.bar_time == daily_time,
                )
                .all()
            )
            for r in sig_rows:
                judged_names.setdefault(
                    (r.strategy_id, r.stock_code), set()).add(r.signal_name)
            # 用完成日线重算最终信号（只算留档代码；force 覆盖滚动缓存）
            md = self._market_data
            code_list = sorted({c for c in codes if c})
            if code_list:
                # 复核与 15:05/14:50 同级走 P0（规范 3.4 / 3.6.5）。
                with tdx_priority(P0):
                    md.inject_startup_periods(
                        daily_time, periods=("1d",), force=True, codes=code_list)
            ctx_by_strategy = {
                st.strategy_id: st for pf in self._ctx.portfolios for st in pf.strategies
            }
            # 成交价（留档第 2 项）：当日同标的 BUY 成交均价；无成交置 None。
            fills: Dict[str, Optional[float]] = {}
            for code in code_list:
                trades = (
                    db.query(LiveTrade)
                    .filter(
                        LiveTrade.live_session_id == self._ctx.session_id,
                        LiveTrade.stock_code == code,
                        LiveTrade.trade_type == "BUY",
                        LiveTrade.trade_time >= start,
                        LiveTrade.trade_time < end,
                    )
                    .all()
                )
                qty = sum(int(t.quantity or 0) for t in trades)
                if qty > 0:
                    amt = sum(float(t.price) * int(t.quantity or 0) for t in trades)
                    fills[code] = amt / qty
                else:
                    fills[code] = None
            for ev in judged:
                code = ev.stock_code
                rows = bars_by_code.get(code) or []
                final_close = None
                if rows:
                    try:
                        final_close = float(rows[-1].get("close"))
                    except Exception:  # noqa: BLE001
                        final_close = None
                key = (ev.strategy_id, code)
                cache_key = (ev.strategy_id, code, daily_time)
                outputs = md.signal_cache.get(cache_key)
                st = ctx_by_strategy.get(ev.strategy_id)
                if outputs is None or st is None:
                    final_names: Optional[Set[str]] = None
                else:
                    out_map = {o.get("name"): o.get("value") for o in outputs}
                    final_names = {
                        cfg["signal_name"] for cfg in st.formula_signals
                        if cfg["signal_name"] in out_map
                        and out_map[cfg["signal_name"]] == cfg["trigger_value"]
                    }
                old_names = judged_names.get(key, set())
                reversed_flag = None if final_names is None else (final_names != old_names)
                # 判定价来源（与执行价来源分开）：从判定事件 message 提取，复核时带上。
                _m = re.search(r"judge_src=(\w+)", ev.message or "")
                judge_src = _m.group(1) if _m else "unknown"
                # 处置档（留档第 5 项）：未反转=none；反转按策略档位（默认 hold）。
                disposition = "none"
                if reversed_flag:
                    disposition = (
                        getattr(st, "reversal_disposition", None)
                        or "close_next_open")
                    if disposition == "close_next_open":
                        # 三源去重（规范 3.6.3）：同日已有该标的 SELL 单 →
                        # 反转单不再生成（风控 > 策略 > 反转；同标同日仅一条卖出）。
                        if not self._has_sell_order_today(db, code, start, end):
                            pending.setdefault(ev.portfolio_id, {}).setdefault(
                                ev.strategy_id, set()).add(code)
                        else:
                            disposition = "dedupe_existing_sell"
                db.add(LiveDecisionEvent(
                    live_session_id=self._ctx.session_id,
                    gate="advance_close_recheck",
                    layer="signal",
                    action="recheck",
                    portfolio_id=ev.portfolio_id,
                    strategy_id=ev.strategy_id,
                    stock_code=code,
                    bar_time=ev.bar_time,
                    param_name="advance_close",
                    param_value=ev.param_value,
                    actual_value=final_close,
                    message=(
                        "judge=%s judge_src=%s fill=%s final_close=%s judged=%s "
                        "final=%s reversed=%s disposition=%s"
                        % (ev.param_value, judge_src, fills.get(code), final_close,
                           ",".join(sorted(old_names)) or "-",
                           ",".join(sorted(final_names)) if final_names is not None else "?",
                           reversed_flag, disposition)
                    )[:200],
                ))
            db.commit()
        except Exception:  # noqa: BLE001
            db.rollback()
            logger.warning("advance_close recheck persist failed", exc_info=True)
        finally:
            db.close()
        # 反转处置（close_next_open）：库已提交后在同一 15:05 P0 轮次内驱动，
        # 生成次日 deferred 卖单（T+1，次日首根 bar 放行），不依赖人工。
        if pending:
            self._dispatch_reversal_closures(
                pending, today, daily_time, bars_by_code)

    @staticmethod
    def _has_sell_order_today(db, code: str, start: datetime, end: datetime) -> bool:
        """同日该标的是否已有有效 SELL 委托（去重键含 direction=SELL）。

        终态失败（canceled/rejected/failed/error）不算占用，避免一笔废单永久
        阻止反转平仓；其余状态（含 deferred/submitted/partial/filled）均视为已占用。
        """
        row = (
            db.query(LiveOrder.id)
            .filter(
                LiveOrder.stock_code == code,
                LiveOrder.trade_type == "SELL",
                LiveOrder.bar_time >= start,
                LiveOrder.bar_time < end,
                ~LiveOrder.status.in_(
                    ("canceled", "rejected", "failed", "error", "submit_error")),
            )
            .first()
        )
        return row is not None

    def _dispatch_reversal_closures(
        self, pending: Dict[int, Dict[int, Set[str]]], today: date,
        daily_time: datetime, bars_by_code: Dict[str, list],
    ) -> None:
        """按组合驱动反转平仓（只走既有 deferred 提交链，不另建下单路径）。"""
        if self._on_reversal_close is None:
            return
        by_id = {pf.portfolio_id: pf for pf in self._ctx.portfolios}
        for portfolio_id, mapping in pending.items():
            portfolio = by_id.get(portfolio_id)
            if portfolio is None:
                continue
            stocks: Dict[str, dict] = {}
            for codes in mapping.values():
                for code in codes:
                    rows = bars_by_code.get(code) or []
                    if not rows:
                        continue
                    try:
                        stocks[code] = to_ohlcv(rows[-1])
                    except Exception:  # noqa: BLE001
                        continue
            if not stocks:
                continue
            bar = BarEvent(stocks=stocks, bar_time=daily_time, period="1d",
                           post_close=True)
            try:
                with tdx_priority(P0):
                    self._on_reversal_close(portfolio, bar, mapping)
            except Exception:  # noqa: BLE001 — 单组合失败不影响其它组合
                logger.exception(
                    "reversal close dispatch failed (session %s, portfolio %s)",
                    self._ctx.session_id, portfolio_id)

    def _stop_requested(self) -> bool:
        """停止请求探针：长循环里定期检查，命中即收尾。"""
        fn = getattr(self._ctx, "stop_requested", None)
        if fn is None:
            return False
        try:
            return bool(fn())
        except Exception:  # noqa: BLE001 — 探针异常不得影响日终流程
            return False

    def _fetch_1d_bars(self, codes=None) -> Dict[str, list]:
        """拉股票池 1d 快照（长度/去重同启动预热）。桥离线返回 {}（已标记离线）。

        ``codes``：限定代码（None=全池）；14:50 收盘复核只需留档代码。
        口径：走 dispatcher.query_quote → 各通道均显式**不复权**（真实价），
        与日终信号公式注入同口径；不得在此另取前复权。

        优先级：显式 **P0**（实盘主链）。15:05 日线轮次落在 14:55–15:10 收盘
        保护窗内，dispatcher.query_quote 默认 P1 会被自家闸拒绝（2026-11-23
        15:05:26 实测「日线取数为空」），本轮回合因此整天不执行。保护窗只应
        挡看板/回测类任务，不挡实盘自己的日线轮次。
        """
        md = self._market_data
        bars_by_code: Dict[str, list] = {}
        if codes is None:
            codes = list(md._bar_poller.stock_codes)
        else:
            codes = list(codes)
        with tdx_priority(P0):
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
        with tdx_priority(P0):
            prices, judge_sources, judge_lags = self._advance_close_prices(
                list(bars_by_code), today)
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
        with tdx_priority(P0):
            exec_prices, used_fallback, missing, exec_sources = (
                self._advance_exec_prices(list(prices), today, hhmm))
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
                # 14:50 轮次与 15:05 同级走 P0：避免紧邻 14:55–15:10 保护窗被挤掉。
                with tdx_priority(P0):
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
        # 双留档：判定价 + 信号方向 + 执行价来源（final_close/反转由 15:05 复核补写）。
        self._record_advance_judgements(
            today, daily_time, prices,
            judge_sources=judge_sources, judge_lags=judge_lags,
            exec_sources=exec_sources, exec_prices=exec_prices)
        self.last_advance_close_date = today
        self._persist_advance_done(today)
        logger.info(
            "advance close driven (session %s, %s): %d code(s), skipped=%d, at=%s",
            self._ctx.session_id, daily_time, len(adv_stocks), skipped,
            now.strftime("%H:%M:%S"),
        )

    def _native_snapshot(self):
        """返回 ``(native, live)``：原生 1m 调度器与当前是否可用。

        ``native`` 是 ``BarPoller._dispatcher``（``NativeMinuteDispatcher``），
        不是 ``MarketDataService._native_minute``——后者只是 ``minute_health``
        健康态对象（见 ``market_data.py`` 的构造注释），没有取数方法。
        ``live=False`` 表示未启用（非原生分发器 / 无 health）、健康态为
        stalled/fallback 或读健康失败；此时该 code 必须回落快照合成（逐只判定，
        不整批降级）。
        """
        poller = getattr(self._market_data, "_bar_poller", None)
        native = getattr(poller, "_dispatcher", None)
        health = getattr(native, "minute_health", None) if native is not None else None
        if health is None or not hasattr(native, "query_quotes_batch"):
            return None, False
        try:
            return native, not health.is_fallback(self._clock())
        except Exception:  # noqa: BLE001
            return native, False

    def _advance_close_prices(self, codes, today):
        """14:50 判定价：原生 1m 优先，不足逐只回落快照合成，最后 fail-closed。

        返回 ``(prices, sources, lags)``：
          - prices：{code: close}；
          - sources：{code: ``native_1m`` / ``snapshot_synth`` / ``none``}（逐只）；
          - lags：{code: 滞后秒数或 None}——原生最后一根标签相对 14:51 的差距，
            用于说明为何回落（未追平）；非原生来源为 None。

        回落条件（逐 code，命中任一即回落合成）：
          1. ``live.native_minute.enabled`` 未开（``native is None``）；
          2. 原生源 stalled / fallback 健康态；
          3. 取回的最后一根标签 ≠ 14:51（不得用 14:50/14:52 顶替）；
          4. 价格无效（0 / 负 / 缺失）。
        全部复用既有 NativeMinuteDispatcher 与 SnapshotBarSynthesizer，不另写取价。
        取不到的 code 不返回价格（调用方重试，绝不伪造）。
        """
        target = datetime.combine(today, time(*_ADVANCE_BAR_LABEL))
        prices: Dict[str, float] = {}
        sources: Dict[str, str] = {}
        lags: Dict[str, Optional[float]] = {}
        synth = self._advance_synth
        native, live = self._native_snapshot()

        # 1) 原生 1m 优先（逐只）
        if native is not None and live:
            try:
                batch = native.query_quotes_batch(codes, "1m", 20) or {}
            except Exception:  # noqa: BLE001
                logger.warning(
                    "advance close: native 1m quote failed (session %s)",
                    self._ctx.session_id, exc_info=True)
                batch = {}
            for code in codes:
                parsed = []
                for row in batch.get(code) or []:
                    st = parse_bar_time(row)
                    if st is not None:
                        parsed.append((st, row))
                if not parsed:
                    lags[code] = None
                    continue
                parsed.sort(key=lambda item: item[0])
                last_label, last_row = parsed[-1]
                # 滞后秒数：最后一根标签落后 14:51 多少秒（0=已追平）
                lags[code] = max(0.0, (target - last_label).total_seconds())
                if last_label != target:
                    continue  # 未追平 → 回落合成
                px = _positive_num(last_row.get("close"))
                if px is None:
                    continue  # 价格无效 → 回落合成
                prices[code] = px
                sources[code] = "native_1m"

        lacks = [c for c in codes if c not in prices]

        # 2) 回落快照合成（既有 completed_bars 精确匹配 14:51:00）
        if lacks and synth is not None:
            try:
                completed = synth.completed_bars(lacks, count=30)
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
                            sources[code] = "snapshot_synth"
                            # 保留原生滞后秒数（回落原因留痕）；原生未参与过才补 None
                            lags.setdefault(code, None)
                        break
                    if st < target:
                        break

        # 3) 无合成器（iquant/QMT 桥）：通道 1m，接受 [14:49:30, 14:51:00]
        if lacks and synth is None:
            lo = target - timedelta(seconds=90)
            for code in lacks:
                if self._stop_requested():
                    logger.info(
                        "stop requested, abort advance-close quote fetch (session %s)",
                        self._ctx.session_id)
                    break
                try:
                    rows = self._ctx.dispatcher.query_quote(code, period="1m", count=10)
                except BridgeUnavailableError:
                    self._set_bridge_offline()
                    break
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
                            sources[code] = "native_1m"
                            lags[code] = max(0.0, (target - st).total_seconds())
                        break
                    if st < lo:
                        break

        # 4) fail-closed：逐只标记，调用方重试/告警
        for code in codes:
            sources.setdefault(code, "none")
            lags.setdefault(code, None)
        return prices, sources, lags

    def _advance_exec_prices(self, codes, today, hhmm):
        """B 轮次委托价：14:52 起始 1m bar 的 open（14:53 起回退 14:51 起始 bar）。

        返回 ``(prices, used_fallback, missing, sources)``：
          - prices：{code: open}，仅含取到的 code；
          - used_fallback：是否有 code 用了 14:51 兜底（调用方每交易日告警一次）；
          - missing：两处都取不到的 code（调用方 fail-closed，不下单，绝不伪造价格）；
          - sources：{code: source}，``bar_1m`` / ``snapshot_synth`` / ``fallback_1451``
            （规范 3.3.2 / D14；逐只留痕，供留档五项审计）。

        优先级（规范 3.3.2）：
          1. ``bar_1m``：原生 1m（``native_minute`` 开启、未停滞）标签 14:53 的 open；
          2. ``snapshot_synth``：既有 ``SnapshotBarSynthesizer.open_at(14:52)``；
          3. ``fallback_1451``：14:53 起先取原生标签 14:52，再回退合成 ``open_at(14:51)``。
        三级都取不到 → ``missing``（调用方 fail-closed，不下单，绝不伪造价格）。

        返回 ``(prices, used_fallback, missing, sources)``：
          - sources：{code: source}，逐只留痕（D14），供留档五项审计。

        2026-11-23 实测：TQ ``get_history_raw`` 的 1m/5m 最后一根停在上一交易日
        15:00（11-20），1d/1w 到当日；外置「数据下载工具」在运行时会盘中写入
        ``vipdoc\\*\\minline\\*.lc1``，但今日未运行。因此 ``bar_1m`` 级真实存在，
        但**条件可用**：仅当下载工具在跑且 ``live.native_minute.enabled=true``；
        否则唯一当日 1m 来源就是快照合成器。
        """
        primary_start = datetime.combine(today, time(*_ADVANCE_EXEC_START))
        fallback_start = datetime.combine(today, time(*_ADVANCE_EXEC_FALLBACK_START))
        target = datetime.combine(today, time(*_ADVANCE_BAR_LABEL))
        use_fallback = tuple(hhmm) >= _ADVANCE_EXEC_FALLBACK_TIME
        prices: Dict[str, float] = {}
        sources: Dict[str, str] = {}
        used_fallback = False
        missing = []
        synth = self._advance_synth
        native, native_live = (
            self._native_snapshot() if synth is not None else (None, False))
        primary_label = primary_start + timedelta(minutes=1)
        fallback_label = fallback_start + timedelta(minutes=1)

        def _native_open(codes_wanted, label):
            """从原生 1m（native_minute）取指定标签 bar 的 open。"""
            if not native_live or not codes_wanted:
                return {}
            try:
                batch = native.query_quotes_batch(codes_wanted, "1m", 20) or {}
            except Exception:  # noqa: BLE001
                logger.warning(
                    "advance exec: native 1m quote failed (session %s)",
                    self._ctx.session_id, exc_info=True)
                return {}
            out: Dict[str, float] = {}
            for code in codes_wanted:
                row = None
                for item in batch.get(code) or []:
                    if parse_bar_time(item) == label:
                        row = item
                        break
                px = _positive_num(row.get("open")) if row else None
                if px is not None:
                    out[code] = px
            return out

        if synth is not None:
            prices = {}
            sources = {}
            # 1) 真实 1m bar（native_minute 开启且未停滞）：标签 14:53 的 open。
            got = _native_open(list(codes), primary_label)
            prices.update(got)
            for code in got:
                sources[code] = "bar_1m"
            # 2) 快照合成（既有合成器 open_at(14:52)，不另写合成）。
            lacks = [c for c in codes if c not in prices]
            if lacks:
                try:
                    got = dict(synth.open_at(lacks, primary_start) or {})
                except Exception:  # noqa: BLE001
                    logger.exception(
                        "advance exec: open_at failed (session %s)",
                        self._ctx.session_id)
                    got = {}
                prices.update(got)
                for code in got:
                    sources[code] = "snapshot_synth"
            # 3) 14:53 起回退 14:51 起始 bar：真实 1m（标签 14:52）优先后合成。
            if use_fallback:
                lacks = [c for c in codes if c not in prices]
                got = _native_open(lacks, fallback_label)
                prices.update(got)
                for code in got:
                    sources[code] = "fallback_1451"
                if got:
                    used_fallback = True
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
                        for code in fb:
                            sources[code] = "fallback_1451"
                        used_fallback = True
            missing = [c for c in codes if c not in prices]
            for code in missing:
                sources[code] = "none"
            return prices, used_fallback, missing, sources
        for code in codes:
            try:
                rows = self._ctx.dispatcher.query_quote(code, period="1m", count=10)
            except BridgeUnavailableError:
                self._set_bridge_offline()
                missing.extend([c for c in codes if c not in prices])
                for code in codes:
                    sources.setdefault(code, "none")
                return prices, used_fallback, missing, sources
            except Exception:  # noqa: BLE001
                logger.warning(
                    "advance exec: 1m quote failed for %s", code, exc_info=True)
                missing.append(code)
                sources[code] = "none"
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
                    sources[code] = "bar_1m"
                    continue
            if use_fallback and idx >= 0 and idx + 1 < len(parsed):
                px = _positive_num(parsed[idx + 1][1].get("open"))
                if px is not None:
                    prices[code] = px
                    sources[code] = "fallback_1451"
                    used_fallback = True
                    continue
            missing.append(code)
            sources[code] = "none"
        return prices, used_fallback, missing, sources

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
