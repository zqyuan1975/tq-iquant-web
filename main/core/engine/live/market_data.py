"""MarketDataService — 行情/信号缓存与周期 bar 分发（0010 步骤 2b）。

从 LiveEngine 抽出的行情数据协作者，独占：
- 预热/增量 bar 缓存（_preheat_cache）与公式信号缓存（signal_cache）；
- 公式注入配置（tq_formula / formula_by_strategy / formula_count* / 周期集合）；
- 启动预热、逐 bar 公式信号注入、周期边界 bar 分发、1w/1mon 通达信启动注入。

边界（铁律：协作者不反向 import LiveEngine）：
- 本服务不直接下单、不持有引擎引用。周期 bar 需驱动 on_bar 管线时，经构造注入的
  ``on_bar`` 回调（引擎传入其 _handle_bar 绑定方法），由引擎侧完成下单/风控。
- 共享状态（portfolios/dispatcher/code_period_count）经 EngineContext 读取。
- F5 桥可用持仓刷新后需写回 T+1 检查器，经注入的 ``set_available_map`` 窄回调，
  不直接抓执行层对象。

本步为纯搬移：方法体逻辑不变，仅把 self._dispatcher/self.portfolios 等改为
self.ctx.*、self._handle_bar 改为 self._on_bar；LiveEngine 保留同名薄委托方法/
property，既有调用点与测试直连（engine.signal_cache={...}、engine._preheat()、
LiveEngine._bars_to_formula_df(...) 等）全部穿透到本服务，行为不变。
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Callable, Dict, List, Optional

from ..event import BarEvent
from ..http_bridge_dispatcher import BridgeUnavailableError
from ...tq.utils import TDXLockTimeout
from ...tq.gate import P0, tdx_priority
from ..bar_poller import parse_bar_time, to_ohlcv, latest_completed_bar
from .native_minute import latest_closed_bar
from ..portfolio import Portfolio
from .context import EngineContext
from .timing import _to_int

logger = logging.getLogger("core.engine.live.market_data")

# TQ 公式输出中需跳过的非变量键（同 backtest._FORMULA_META_KEYS）
_FORMULA_META_KEYS = ("Date", "ErrorId", "Error", "Time")

# C6(C)：1w/1mon 走通达信启动/日终注入（桥端 xtdata 拉不到），_fill_signal_cache 跳过不拉桥
_STARTUP_ONLY_PERIODS = ("1w", "1mon")

# 周期边界新鲜度闸覆盖的直取周期：手册（TQ 1.3.1）get_market_data 本地模式
# 「周期数据个数=客户端盘后下载的数据个数」「分钟线需 Level2」，且 refresh_kline
# 「盘中下载 1m/5m 只能到上个交易日」——盘中直取可能拿到上一交易日的完成 bar。
# 1m 走快照合成（deferred 放行时钟依赖，不在此闸）；1d 由 daily_closer 同口径校验；
# 1w/1mon 属启动注入，均不改。
_FRESHNESS_GATED_PERIODS = ("5m", "15m", "30m", "1h")


class MarketDataService:
    def __init__(
        self,
        ctx: EngineContext,
        bar_poller,
        *,
        on_bar: Callable[..., None],
        set_available_map: Callable[[Dict[str, int]], None],
        tq_formula=None,
        formula_by_strategy: Optional[Dict[int, str]] = None,
        formula_count: int = 200,
        formula_count_by_name: Optional[Dict[str, int]] = None,
        emit_risk: Optional[Callable[[dict], None]] = None,
        snapshot_synth=None,
    ) -> None:
        self.ctx = ctx
        self._bar_poller = bar_poller
        # 快照合成器（1m → 5m/15m/30m/1h 聚合兜底）：TQ 本地分钟线盘中只到上一交易日，
        # 新鲜度闸会把多分钟策略整日拦死。显式传入优先；未传时从 BarPoller 的合成适配器
        # 自动发现（tdx/ths/alert/goldminer 的 bar_poller._dispatcher 就是
        # SnapshotQuoteDispatcher）——因此 LiveEngine 无需改动即可启用兜底。
        if snapshot_synth is None:
            _poller_dispatcher = getattr(bar_poller, "_dispatcher", None)
            snapshot_synth = getattr(_poller_dispatcher, "synthesizer", None)
        self._snapshot_synth = snapshot_synth
        # 原生分钟数据源健康态（开关 live.native_minute.enabled 打开时由
        # NativeMinuteDispatcher 挂出）：周期派发据此决定「用原生还是回落聚合」。
        # 关闭时 dispatcher 为 SnapshotQuoteDispatcher，无该属性 → None，行为不变。
        self._native_minute = getattr(
            getattr(bar_poller, "_dispatcher", None), "minute_health", None)
        # 驱动一根 bar 的 on_bar 管线（引擎传入 self._handle_bar 绑定方法）；本服务
        # 不反向 import LiveEngine，周期/日终 bar 经此回调交回引擎完成下单/风控。
        self._on_bar = on_bar
        # F5：桥可用持仓刷新后写回 T+1 检查器的窄回调（LiveT1Checker.set_available_map）。
        self._set_available_map = set_available_map
        # risk 事件出口（引擎传入 self._emit_risk_safe）：新鲜度闸跳过时发可观测事件，
        # 避免静默丢边界；为空（单测直连/无引擎）时只记日志。
        self._emit_risk = emit_risk

        # 公式注入（0010）：tq_formula 封装内存注入链路；formula_by_strategy 预加载
        # {strategy_id: formula_name}，避免每 bar 查库；formula_count 为注入历史根数
        # （1m/5m 默认 200，够均线预热；不足时调大）。
        self._tq_formula = tq_formula
        self._formula_by_strategy: Dict[int, str] = formula_by_strategy or {}
        self._formula_count = formula_count
        # #27→#28：count 按公式配（Formula.formula_count），同公式恒定 → C4 去重 key
        # (code, period, formula) 无需 count 进 key。_formula_count 作全局兜底（老调用不破）。
        self._formula_count_by_name: Dict[str, int] = formula_count_by_name or {}
        # 每周期预拉最大 count：该周期策略所用公式的最大 formula_count——边界/日终分发
        # 预拉的 bars 够该周期最长公式（count 不够时注入会缺历史，信号 NaN 静默失效）。
        self._period_count: Dict[str, int] = {}
        # 实例所有策略的周期集合（含无公式策略，含 1d/1w/1mon）——_dispatch_period_bar
        # 边界分发 guard 用：只拉实例确有策略的周期，挡掉 periods_on_boundary 纯算术
        # 带出但实例无人用的周期（如 14:30 的 15m——minute%15==0 触发，却无 15m 策略，
        # 白拉 17 只 count=200 后 period 过滤全跳过）。比 _period_count 更宽：后者只收
        # 有公式映射的策略周期，无公式的 30m 策略靠此集合保住风控单的边界驱动。
        self._strategy_periods: set = set()
        for _p in ctx.portfolios:
            for _ctx in _p.strategies:
                self._strategy_periods.add(_ctx.period)
                _name = self._formula_by_strategy.get(_ctx.strategy_id)
                if not _name:
                    continue
                _cnt = self._formula_count_by_name.get(_name, self._formula_count)
                if _cnt > self._period_count.get(_ctx.period, 0):
                    self._period_count[_ctx.period] = _cnt

        # 预热缓存：(code, period) -> {"bars": [...], "last_stime": datetime, "count": int}
        # 启动 preheat() 填充（拉 code_period_count[(code,period)] 根历史）；
        # 运行期 _get_bars_with_increment 读它 + 增量拉新 bar 拼接（省去每 bar 全量重拉）；
        # 离线恢复 _tick_main 清空 → 下次走全量重建。跨 bar 生命周期（不像 df_cache 每 bar 重建）。
        self._preheat_cache: Dict[tuple, dict] = {}

        # 信号缓存：(strategy_id, stock_code, bar_time) -> [{"name": str, "value": int}]
        # 风控信号（止损/止盈/移动止损）由 Portfolio._check_risks 直接生成，无需缓存；
        # 公式信号（OPEN/ADD/REDUCE/CLOSE）需缓存命中才触发——fill_signal_cache 在
        # 每根 bar 前拉历史 → 内存注入算公式 → 填此 dict。测试可直接预置以验证下单链路。
        self.signal_cache: Dict = {}

    # ---------------- 预热 + 增量拼接（拉取优化）----------------
    def preheat(self, only_missing: bool = False) -> None:
        """启动预热：对每个 (code, period) 拉 code_period_count 根历史存 _preheat_cache。

        只预热 ctx.code_period_count 里的 (code,period)（实例真实有策略的，按需不浪费），
        跳过 1d/1w/1mon（1d 不预热走日终 _maybe_daily_bars；1w/1mon 走通达信 inject_startup_periods）。
        单 (code,period) 拉取失败不阻断启动（log warn，运行期该 key 走 _get_bars_with_increment
        的"缓存未命中全量补"自愈）。启动一次性同步调用，在 start() 里 inject_startup_periods 之后。

        only_missing=True（收盘后停轮询 → 次日恢复时的预热）：跳过已有缓存的
        (code, period)，只补缺失项。暂停期不产生 bar（无交易无 bar），已有缓存
        可直接续接，全量重拉会在开盘前对桥/TQ 形成数百次突发请求（与"收盘降压"
        目标相反）；缺失项（暂停期扩池的新代码）仍按 code_period_count 全量补齐，
        保证开盘第一根 bar 公式历史不缺。
        """
        n_pull = 0
        for (code, period), count in self.ctx.code_period_count.items():
            if period in ("1d", "1w", "1mon"):
                continue
            if only_missing and (code, period) in self._preheat_cache:
                continue
            # 节流：预热是大批量连续 TQ 拉（数百 (code,period)），同步调用会冻结
            # 通达信客户端 UI——逐笔小睡让出 UI 线程（总时长 +~2 分钟，可接受）
            n_pull += 1
            if n_pull % 5 == 0:
                time.sleep(0.2)
            try:
                bars = self.ctx.dispatcher.query_quote(code, period=period, count=count)
            except BridgeUnavailableError:
                logger.warning("preheat failed (bridge unavailable) %s %s", code, period)
                continue
            except Exception:  # noqa: BLE001
                logger.exception("preheat failed %s %s", code, period)
                continue
            if not bars:
                continue
            self._preheat_cache[(code, period)] = self._make_cache_entry(bars, count)

    def _make_cache_entry(self, bars: list, count: int) -> dict:
        """bars → 排序截断到 count 根 + 算 last_stime，构造 _preheat_cache 条目。"""
        bars = self._sort_and_cap(bars, count)
        return {"bars": bars, "last_stime": self._max_stime(bars), "count": count}

    def get_bars_with_increment(self, code: str, period: str, count: int) -> list:
        """读预热缓存历史 + 增量拉新 bar 拼接，返回 count 根窗口（拉取优化核心）。

        1) 缓存命中：增量拉 query_quote(count=INCREMENT_COUNT) 筛 stime > cache.last_stime
           的新 bar，拼到 cache.bars 末尾，排序截断保持 count 长；无新 bar 直接返缓存（最省）。
        2) 缓存未命中（预热失败/离线清缓存后）：全量拉 count 根回填缓存（异常/首次路径，
           不背正常增量的负担）。
        桥拉取抛 BridgeUnavailableError 向上传播（交 on_bar/dispatch_period_bar 置离线）。
        """
        INCREMENT_COUNT = 10  # 增量拉取根数，够覆盖正常单边界增量（1-2 根）；离线恢复走清缓存全量重建
        cache = self._preheat_cache.get((code, period))
        if cache is None:
            bars = self.ctx.dispatcher.query_quote(code, period=period, count=count)
            if bars:
                self._preheat_cache[(code, period)] = self._make_cache_entry(bars, count)
            return bars
        # 缓存存在但请求 count > 缓存 count：缓存历史不够长公式的窗口 → 升级全量拉 count 根
        # （同 _fetch_cached_bars 升级语义：count 不够会缺历史，长均线 NaN 静默失效）。
        if count > cache["count"]:
            bars = self.ctx.dispatcher.query_quote(code, period=period, count=count)
            if bars:
                self._preheat_cache[(code, period)] = self._make_cache_entry(bars, count)
            return bars
        new_bars = self.ctx.dispatcher.query_quote(code, period=period, count=INCREMENT_COUNT)
        last = cache["last_stime"]
        fresh = [b for b in new_bars if self._bar_stime(b) is not None
                 and (last is None or self._bar_stime(b) > last)]
        if not fresh:
            return cache["bars"]
        merged = self._sort_and_cap(cache["bars"] + fresh, count)
        cache["bars"] = merged
        cache["last_stime"] = self._max_stime(merged)
        return merged

    @staticmethod
    def _bar_stime(bar: dict) -> Optional[datetime]:
        """bar → 结束时间 datetime（复用 parse_bar_time，兼容 stime/time/index）。"""
        return parse_bar_time(bar)

    @staticmethod
    def _sort_and_cap(bars: list, count: int) -> list:
        """按 bar stime 升序排序，截断保留最新 count 根（去重同 stime）。"""
        timed = [(parse_bar_time(b), b) for b in bars]
        seen: Dict[datetime, dict] = {}
        for bt, b in timed:
            if bt is not None and bt not in seen:
                seen[bt] = b
        ordered = [b for _, b in sorted(seen.items())]
        return ordered[-count:] if count > 0 else ordered

    @staticmethod
    def _max_stime(bars: list) -> Optional[datetime]:
        """bars 中最新 bar 的 stime（空 → None）。"""
        stimes = [parse_bar_time(b) for b in bars]
        stimes = [t for t in stimes if t is not None]
        return max(stimes) if stimes else None

    def dispatch_period_bar(self, period: str, boundary_time: datetime) -> str:
        """C6(A) 边界分发：period 边界到（1m stime 判定）→ 拉该周期 bar → 注入 → 驱动该周期策略。

        对每 code 拉 query_quote(period, count=formula_count) 一次，既供公式注入又供 BarEvent。
        取每 code「最新已完成 bar」（stime < 本批 latest）的 OHLCV 构造 BarEvent
        （bar_time=boundary_time，与 1m 节拍对齐）——不用 forming 最新一根（未来函数）。
        桥拉取抛 BridgeUnavailableError → 向上传播由 _on_bar 置离线。无完成 bar 的 code 跳过。

        新鲜度闸（5m/15m/30m/1h）：手册 TQ 1.3.1 允许本地模式分钟线停在上一交易日
        （refresh_kline：盘中下载 1m/5m 只能到上个交易日），直取拿到的「最新完成 bar」
        可能不是当日。此处校验其 stime 交易日 == boundary_time 当日（1m 边界由上海时间
        驱动，实盘即今日），不等则**跳过该 code 不派发**（fail-safe，绝不用旧 bar 触发），
        记 WARNING + risk 事件（gate=period_bar_freshness）；全部 code 被拦返回 "stale"，
        由引擎登记待补偿，后续拿到当日 bar 再派发。
        1m（快照合成）/1d（daily_closer 同口径闸）/1w·1mon（启动注入）不走此闸。

        聚合兜底：被闸拦下的 code 若有当前当日快照合成的 1m bar，用
        ``SnapshotBarSynthesizer.aggregated_bars`` 聚合出本周期 bar 派发（open=首根、
        close=末根、high/low=极值、volume=累加，只取完整桶），并在 risk 事件
        ``gate=period_bar_fallback`` 中标注 ``source=aggregated_1m``；仍拿不到当日
        完整桶的 code 继续按 ``period_bar_freshness`` 告警。

        返回："dispatched"（至少一只 code 派发成功）/ "stale"（全部被新鲜度闸拦下，可补偿）/
        "empty"（有该周期策略但一只 code 都取不到可用 bar，可补偿）/ "no_strategy"
        （实例无该周期策略，无事可做，不进补偿）。

        周期 guard：periods_on_boundary 是纯算术（minute%15==0 等），不查实例有无该周期策略，
        会带出实例无人用的周期（如 14:30 的 15m）。此处按 _strategy_periods 过滤——实例无该
        周期策略直接 return，避免白拉（拉完 period 过滤全跳过，不出单纯浪费）。

        P1#6（2026-11-16）：旧实现把「无该周期策略」与「全部 code 取不到 bar」都返回
        "empty"，引擎只对 "stale" 挂待补偿 → TQ 偶发返回空时该 5m/15m/30m/1h 边界当日
        永久丢失。现在无策略返回 "no_strategy"，"empty" 只表示可补偿的「暂无数据」。
        """
        if period not in self._strategy_periods:
            return "no_strategy"
        gate_freshness = period in _FRESHNESS_GATED_PERIODS
        today = boundary_time.date()
        native = self._native_minute
        # 原生分钟源已判定停滞（下载工具未开/被关/清单未更新）→ 本轮不派发
        # 原生周期 bar，直接走下方快照聚合兜底；留痕由 NativeMinuteDispatcher
        # 的状态机统一发出（回落/恢复都写 WARNING/INFO + risk 事件）。
        native_stalled = native is not None and native.is_fallback(boundary_time)
        bars_by_code: Dict[str, list] = {}
        stocks: Dict[str, dict] = {}
        stale: List[tuple] = []  # [(code, bar_time)]：最新完成 bar 非当日，被闸拦下
        cp_counts: Dict[str, int] = {}
        # C4(#28)：预拉 count = 该周期策略最大 formula_count（够最长公式，注入不欠历史）
        count = self._period_count.get(period, self._formula_count)
        for code in self._bar_poller.stock_codes:
            # 按 (code, period) 取该股票该周期所需根数（比全局 _period_count 更细，按需）。
            # 走预热缓存 + 增量拼接（省去每边界全量重拉 count 根）。
            cp_count = self.ctx.code_period_count.get((code, period), count)
            cp_counts[code] = cp_count
            bars = self.get_bars_with_increment(code, period, cp_count)
            if not bars:
                if native_stalled:
                    stale.append((code, None))
                continue
            bars_by_code[code] = bars
            if native_stalled:
                stale.append((code, None))
                continue
            # 原生开关打开：按墙钟只取已闭合 bar（标签 <= 边界时刻），未完成桶
            # 不进入信号/风控；关闭时保持旧语义（最新一根一律当 forming 丢掉）。
            cb = (latest_closed_bar(bars, boundary_time)
                  if native is not None else latest_completed_bar(bars))
            if cb is None:
                continue
            bt = parse_bar_time(cb)
            if gate_freshness and (bt is None or bt.date() != today):
                stale.append((code, bt))
                continue
            stocks[code] = to_ohlcv(cb)
        if stale:
            # 兜底：TQ 原生分钟线只到上一交易日时，改用已合成的 1m bar 聚合出本周期
            # 当日 bar（同一进程内存，不触 TQ）。聚合 bar 全部已闭合，不能再用
            # latest_completed_bar（它会把最新一根当 forming 丢掉）——直接取末根。
            recovered: List[tuple] = []  # [(code, agg_bar, bar_time)]
            still_stale: List[tuple] = []
            if self._snapshot_synth is not None:
                try:
                    agg = self._snapshot_synth.aggregated_bars(
                        [code for code, _ in stale], period=period, count=count,
                    )
                except Exception:  # noqa: BLE001
                    logger.warning(
                        "aggregated fallback failed %s %s", period, boundary_time,
                        exc_info=True)
                    agg = {}
            else:
                agg = {}
            for code, old_bt in stale:
                agg_bars = agg.get(code) or []
                if not agg_bars:
                    still_stale.append((code, old_bt))
                    continue
                last = agg_bars[-1]
                bt = parse_bar_time(last)
                if bt is None or bt.date() != today:
                    still_stale.append((code, old_bt))
                    continue
                # 原生历史（更早交易日）补足公式预热窗口；同日同 stime 以聚合为准
                # （聚合在前 → _sort_and_cap 去重时先到先得）。
                cp_count = cp_counts.get(code, count)
                bars_by_code[code] = self._sort_and_cap(
                    agg_bars + (bars_by_code.get(code) or []), cp_count)
                stocks[code] = to_ohlcv(last)
                recovered.append((code, last, bt))
            if recovered:
                self._report_aggregated_fallback(
                    period, boundary_time, recovered, len(still_stale))
            stale = still_stale
            if stale:
                self._report_stale_period_bars(period, boundary_time, stale)
        if not stocks:
            return "stale" if stale else "empty"
        bar_event = BarEvent(stocks=stocks, bar_time=boundary_time, period=period)
        df_cache: Dict = {}
        raw_cache: Dict = {}
        for portfolio in self.ctx.portfolios:
            self._on_bar(
                portfolio, bar_event, bars_by_code=bars_by_code,
                df_cache=df_cache, raw_cache=raw_cache,
            )
        return "dispatched"

    def _report_stale_period_bars(
        self, period: str, boundary_time: datetime, stale: List[tuple],
    ) -> None:
        """新鲜度闸拦截告警：WARNING + risk 事件（gate=period_bar_freshness）。

        每 (period, boundary) 聚合一条（codes 截断 5 只），避免整池停更时刷屏；
        事件字段保留 gate/period/boundary_time/bar_date/codes/stale_count 供排查。
        """
        codes = [code for code, _ in stale]
        sample = codes[:5]
        dates = sorted({bt.date().isoformat() for _, bt in stale if bt is not None})
        bar_date = dates[0] if dates else None
        bar_dates = ",".join(dates) if dates else "未知"
        more = " 等" if len(codes) > len(sample) else ""
        logger.warning(
            "周期边界新鲜度闸跳过：period=%s boundary=%s 最新完成 bar 日期=%s"
            "（非当日 %s），code=%s%s（共 %d 只）",
            period, boundary_time.strftime("%Y-%m-%d %H:%M"), bar_dates,
            boundary_time.date().isoformat(), ",".join(sample), more, len(codes),
        )
        if self._emit_risk is None:
            return
        try:
            self._emit_risk({
                "rule": "period_bar_freshness_skip",
                "gate": "period_bar_freshness",
                "level": "warning",
                "period": period,
                "boundary_time": boundary_time.strftime("%Y-%m-%d %H:%M:%S"),
                "bar_date": bar_date,
                "codes": sample,
                "stale_count": len(codes),
                "message": "周期边界 %s 的 %s bar 停在 %s（非当日），已跳过派发，等当日数据后补偿"
                           % (boundary_time.strftime("%H:%M"), period, bar_dates),
            })
        except Exception:  # noqa: BLE001
            logger.warning("emit period_bar_freshness risk failed", exc_info=True)

    def _report_aggregated_fallback(
        self, period: str, boundary_time: datetime,
        recovered: List[tuple], stale_count: int,
    ) -> None:
        """聚合兜底可观测事件：INFO + risk（gate=period_bar_fallback）。

        ``source=aggregated_1m`` 明确标注这批 bar 来自快照 1m 聚合、不是 TQ 原生
        分钟线（事后复盘/业绩对照时可判别数据来源）。每 (period, boundary) 一条。
        """
        codes = [code for code, _, _ in recovered]
        sample = codes[:5]
        bar_dates = sorted({bt.date().isoformat() for _, _, bt in recovered})
        more = " 等" if len(codes) > len(sample) else ""
        logger.info(
            "周期边界聚合兜底：period=%s boundary=%s 原生数据非当日，改用合成 1m 聚合"
            "（source=aggregated_1m），code=%s%s（%d 只，仍有 %d 只无当日数据）",
            period, boundary_time.strftime("%Y-%m-%d %H:%M"),
            ",".join(sample), more, len(codes), stale_count,
        )
        if self._emit_risk is None:
            return
        try:
            self._emit_risk({
                "rule": "period_bar_fallback_aggregated",
                "gate": "period_bar_fallback",
                "level": "info",
                "period": period,
                "boundary_time": boundary_time.strftime("%Y-%m-%d %H:%M:%S"),
                "bar_date": bar_dates[-1] if bar_dates else None,
                "codes": sample,
                "recovered_count": len(codes),
                "stale_count": stale_count,
                "source": "aggregated_1m",
                "message": "周期边界 %s 的 %s 原生分钟线停在上一交易日，已改用快照合成 1m "
                           "聚合 bar 派发 %d 只（source=aggregated_1m）"
                           % (boundary_time.strftime("%H:%M"), period, len(codes)),
            })
        except Exception:  # noqa: BLE001
            logger.warning("emit period_bar_fallback risk failed", exc_info=True)

    # ---------------- F5：桥可用持仓（SELL 减仓上限）----------------
    def refresh_available_map(self) -> None:
        """F5：拉桥 /positions，按 code(instrument.exchange) 聚合 m_nCanUseVolume。

        桥无该仓/拉取失败（离线）→ 空表 → get_available_shares 全量放行（券商端
        T+1 兜底，避免误伤正常卖出；G6 处理券商拒单）。
        """
        try:
            rows = self.ctx.dispatcher.query_positions()
        except (BridgeUnavailableError, TDXLockTimeout):
            # P2-7：TQ 锁等待超时同「桥离线」处理——空表 → 全量放行（券商端
            # T+1 兜底），绝不因取数被占用而中断本根 bar 的风控评估。
            self._set_available_map({})
            return
        m: Dict[str, int] = {}
        for r in rows or []:
            inst = r.get("instrument")
            exch = r.get("exchange")
            avail = r.get("available")
            if inst and exch and avail is not None:
                m["%s.%s" % (inst, exch)] = int(avail)
        self._set_available_map(m)

    # ---------------- 公式信号注入（0010 + C4 #28 三维去重）----------------
    def fill_signal_cache(
        self,
        portfolio: Portfolio,
        bar: BarEvent,
        bars_by_code: Optional[Dict[str, list]] = None,
        df_cache: Optional[Dict] = None,
        raw_cache: Optional[Dict] = None,
    ) -> None:
        """实盘逐 bar 算公式信号填 signal_cache。预填模式（不改 Portfolio）。

        对每个策略 × bar.stocks 每只股票：
          bridge query_quote(code, period, count=N) 拉历史+实时 bar
          → _bars_to_formula_df 转 OHLCV DataFrame
          → TQFormula.compute_injected_batch 批量内存注入算公式
          → _extract_latest_signal 取最后一条（当前 bar 信号）
          → 填 signal_cache[(strategy_id, code, bar.bar_time)]
        C6 节拍过滤：bar.period 非 None 时只注入匹配周期的策略（5m 边界 bar 不注入 1m 策略）；
        1w/1mon（_STARTUP_ONLY_PERIODS）走通达信启动/日终注入，不拉桥。
        bars_by_code：调用方已预拉好的 bars（边界/日终分发），避免二次拉桥。
        C4(#28) 三维去重（单 bar 生命周期，跨组合共享）：
          df_cache[(code, period)]   → 同 key 只 query_quote 一次（count 更大时升级重拉）
          raw_cache[(code,period,formula)] → 同 key 只算一次（TQ 计算最贵）；同组
          (formula,period) 的多 code 合并成一次 compute_injected_batch（process 只 1 次）
          count 不进 key 的前提：count 是 Formula.formula_count 公式级字段（#27），
          同公式 count 恒定 → 同 (code,period,formula) 的 count 必然相同。
        signal_cache key 仍带 strategy_id（隔离不变，值相同各自存一份）。
        无 tq_formula / 策略无公式映射 / 拉取为空 / 算失败 → 跳过（该股该 bar 无公式信号）。
        """
        if self._tq_formula is None or not self._formula_by_strategy:
            return
        if df_cache is None:
            df_cache = {}
        if raw_cache is None:
            raw_cache = {}
        # 节拍诊断：累计本根 bar 的 tqcenter 公式耗时 / 拉桥耗时，末尾汇总一条，
        # 实测每分钟公式求值是否随会话累积变慢（午后 quote 断档根因定位）。
        t_start = time.perf_counter()
        tq_elapsed = 0.0
        fetch_elapsed = 0.0
        n_inject = 0
        # 两阶段批量（process 批量化，治 1m 全池 111 只/分钟 ~83s 超节拍）：
        #   Phase A 按 (formula, period) 收集本根 bar 待算的 {code: 单列 df}，
        #   Phase B 每组只调一次 compute_injected_batch（N 次 set + 1 次 process，
        #           而非逐只 2N 次 DLL 往返；喂入每只的数据与逐只路径完全一致），
        #   Phase C 按 ctx 回填 signal_cache。
        # 去重：raw_cache 跨组合/本 bar 已算的 key 不再收集；seen_pending 防本次调用内
        #   （同组合多策略）重复收集。count 是公式级字段，同 (formula,period) 组内恒定。
        pending: Dict[tuple, Dict[str, dict]] = {}
        seen_pending: set = set()
        signal_targets: List[tuple] = []  # (strategy_id, raw_key)
        for ctx in portfolio.strategies:
            formula_name = self._formula_by_strategy.get(ctx.strategy_id)
            if not formula_name:
                continue
            # C6：该 bar 只注入匹配周期的策略
            if bar.period is not None and ctx.period != bar.period:
                continue
            # C6(C)：1w/1mon 走通达信启动/日终注入，不拉桥
            if ctx.period in _STARTUP_ONLY_PERIODS:
                continue
            period = ctx.period
            # #27→#28：注入 count 来自 Formula.formula_count（公式级），非全局 200
            count = self._formula_count_by_name.get(formula_name, self._formula_count)
            for code in bar.stocks:
                # 股票池过滤：池外股票不拉公式（多组合共享行情 bar，各策略只算自己池内）
                if ctx.stock_pool is not None and code not in ctx.stock_pool:
                    continue
                raw_key = (code, period, formula_name)
                signal_targets.append((ctx.strategy_id, raw_key))
                # 三维去重：跨组合已算（raw_cache）或本次已收集（seen_pending）→ 不重复
                if raw_key in raw_cache or raw_key in seen_pending:
                    continue
                _tf = time.perf_counter()
                try:
                    bars = self._fetch_cached_bars(
                        df_cache, bars_by_code, code, period, count
                    )
                except BridgeUnavailableError:
                    fetch_elapsed += time.perf_counter() - _tf
                    # 拉历史失败：跳过该股（不阻断 on_bar，风控信号仍可触发）
                    logger.warning("quote failed for formula inject %s %s", code, period)
                    continue
                fetch_elapsed += time.perf_counter() - _tf
                seen_pending.add(raw_key)
                df = self._bars_to_formula_df(bars, code)
                if df is None:
                    # 无有效 bar：空信号占位（免下轮/跨组合重算）
                    raw_cache[raw_key] = []
                    continue
                pending.setdefault((formula_name, period), {})[code] = df
        # Phase B：每组 (formula, period) 批量 process（分片节流：日线全池 250 只
        # 一组的 ~10s 级 TDX 公式计算整段占住通达信 UI 线程 → 客户端冻结。按
        # 30 只分片、片间让 0.4s，单段冻结降到亚秒级；总耗时略增可接受），
        # 逐 code 拆结果填 raw_cache。
        for (formula_name, period), df_by_code in pending.items():
            codes = list(df_by_code.keys())
            chunk_size = 30
            for i in range(0, len(codes), chunk_size):
                chunk_codes = codes[i:i + chunk_size]
                if i > 0:
                    time.sleep(0.4)
                chunk = {c: df_by_code[c] for c in chunk_codes}
                _tt = time.perf_counter()
                # P0：实盘信号计算属主链，任何时段可拿通道（后台 P2 分片让出）
                with tdx_priority(P0):
                    raw = self._tq_formula.compute_injected_batch(
                        formula_name=formula_name, ohlcv_by_code=chunk, period=period,
                        # 显式不复权（0）：喂入的 bars 已是 query_quote 的不复权真实价，
                        # process 端若沿用默认前复权会二次调整 → 信号口径再次分裂。
                        dividend_type=0,
                    )
                tq_elapsed += time.perf_counter() - _tt
                n_inject += len(chunk)
                for code in chunk:
                    raw_cache[(code, period, formula_name)] = \
                        self._extract_latest_signal(raw, code)
        # Phase C：回填 signal_cache（含本轮批量算的与 raw_cache 已命中的）。
        # 已有条目优先（audit Fix2）：外部信号注入（_process_external_signal）先
        # 写入同 key (strategy_id, code, bar.bar_time) 条目再经 _handle_bar 走到
        # 本 Phase——TQ 重算不得覆盖注入信号（1m TQ 全空本就写不进，5m/15m/1d
        # 等周期重算有值时会吞掉注入信号）；同 key 重算同输入同结果，跳过无损。
        for strategy_id, raw_key in signal_targets:
            outputs = raw_cache.get(raw_key)
            if not outputs:
                continue
            key = (strategy_id, raw_key[0], bar.bar_time)
            if key in self.signal_cache:
                continue
            self.signal_cache[key] = outputs
        if n_inject:
            # 每根 bar 每个有注入的组合一条（1m 每分钟一条）：tq= 为 tqcenter 公式
            # 累计耗时、fetch= 为拉桥累计，据此区分瓶颈在通达信公式还是桥，观察 tq=
            # 是否随会话累积增大（午后变慢机理验证）。
            logger.info(
                "signal inject: portfolio=%s period=%s bar=%s codes=%d "
                "tq=%.2fs fetch=%.2fs total=%.2fs",
                portfolio.portfolio_id, bar.period, bar.bar_time, n_inject,
                tq_elapsed, fetch_elapsed, time.perf_counter() - t_start,
            )

    def _fetch_cached_bars(
        self,
        df_cache: Dict,
        bars_by_code: Optional[Dict[str, list]],
        code: str,
        period: str,
        count: int,
    ) -> list:
        """拉取去重：df_cache[(code, period)] 同 key 只实际拉取一次（单 bar 生命周期）。

        口径：信号取数统一为**不复权**（dispatcher.query_quote 写死 none）——本进程
        所有行情源（TQ/同花顺/桥）都只提供这一种口径，故 df_cache 键无需再带口径维度；
        若将来引入第二种口径的取数，必须把它加进 key，否则会跨口径复用 bars。

        缓存值 (bars, used_count)。同 code+period 的公式 count 更大 → 升级重拉（过小会缺
        历史，公式长均线 NaN 静默失效）；bars_by_code 已按该周期最大 count 预拉 → 直接复用，
        记 used=(code,period) 最大 count，避免无谓升级重拉。
        bars_by_code 提供的 bars **不足 count**（BarPoller 本轮拉的 count 窗口，如 10 根）
        → 不能直接复用（拿 10 根喂 200 根窗口公式 = 长均线 NaN 静默失效），改走
        _reuse_provided_with_cache：并入预热缓存，缓存覆盖 count 则复用、否则增量补齐。
        底层实际拉取走 get_bars_with_increment（预热缓存 + 增量拼接），不再直接 query_quote
        全量——1m 算公式（bars_by_code=None）与升级重拉都受益。
        """
        key = (code, period)
        cached = df_cache.get(key)
        if cached is not None:
            bars, used = cached
            if count <= used:
                return bars
            bars = self.get_bars_with_increment(code, period, count)
            df_cache[key] = (bars, count)
            return bars
        if bars_by_code is not None and code in bars_by_code:
            provided = bars_by_code[code]
            # 提供 bars 已覆盖公式窗口 → 直接复用（used 记该 (code,period) 最大 count，
            # 同 bar 内更大 count 公式也直接复用不重拉）。
            if len(provided) >= count:
                used = max(count, self.ctx.code_period_count.get((code, period), self._formula_count))
                df_cache[key] = (provided, used)
                return provided
            # 提供 bars 不足 count：
            #   非轮询周期（5m/1d...边界分发预拉，本就按 code_period_count 全量，桥只返
            #   这么多历史）→ 直接复用（历史就这么多，不能无中生有）。
            #   轮询周期（1m，BarPoller 透传，count 窗口仅判完成用）→ 并入预热缓存复用/补齐。
            if period != self._bar_poller.period:
                used = max(count, self.ctx.code_period_count.get((code, period), self._formula_count))
                df_cache[key] = (provided, used)
                return provided
            bars = self._reuse_provided_with_cache(code, period, provided, count)
            df_cache[key] = (bars, count)
            return bars
        bars = self.get_bars_with_increment(code, period, count)
        df_cache[key] = (bars, count)
        return bars

    def _reuse_provided_with_cache(self, code: str, period: str, provided: list, count: int) -> list:
        """把调用方本轮已拉到的 bars（BarPoller 透传）并入预热缓存复用，零额外拉取。

        BarPoller 每轮已拉 1m（count 窗口，如 10 根），注入若再走 get_bars_with_increment
        增量拉（同样 count 窗口）就是同一批 bars 的双份冗余。把本轮已拉的并入缓存后：
          缓存历史已够 count 根（启动预热 code_period_count 根）→ 直接返回，零拉桥；
          缓存历史不够（未预热/离线清空/请求 count 更大）→ 回退 get_bars_with_increment
          全量/增量补齐（同原路径，冷启动安全）。
        """
        cache = self._preheat_cache.get((code, period))
        if cache is None:
            # 冷启动/离线清空：提供 bars 量小不足公式窗口，走全量拉补缓存（含这些 bars）。
            return self.get_bars_with_increment(code, period, count)
        merged = self._sort_and_cap(cache["bars"] + provided, count)
        cache["bars"] = merged
        cache["last_stime"] = self._max_stime(merged)
        if len(merged) >= count:
            return merged
        return self.get_bars_with_increment(code, period, count)

    @staticmethod
    def _bars_to_formula_df(bars: list, code: str) -> Optional[dict]:
        """桥 bar dict 列表 → {Amount/Volume/Close/Open/High/Low: pandas.DataFrame}。

        桥 bar 字段：stime(yyyymmddHHMMSS)/time(时间戳)/index(历史工具) + 小写 OHLCV。
        时间统一用 parse_bar_time（与 BarPoller 同规则），兼容 stime/time/index 各来源。
        输出：每字段单列 DataFrame（列=[code]，行=DatetimeIndex）。
        空 bars / 无有效时间 → None（调用方跳过）。
        """
        if not bars:
            return None
        # pandas 在此函数内首次按需 import（非顶部）：pandas 较重，且本函数仅在
        # 公式注入路径调用；避免模块导入期无条件加载（审计 #31：math 已提顶，pandas 刻意保留 lazy）。
        import pandas as pd

        times, o, h, l, c, v, a = [], [], [], [], [], [], []
        for b in bars:
            t = parse_bar_time(b)
            if t is None:
                continue

            def _num(key):
                val = b.get(key)
                if val is None or val == "":
                    return 0.0
                try:
                    return float(val)
                except (TypeError, ValueError):
                    return 0.0

            times.append(t)
            o.append(_num("open"))
            h.append(_num("high"))
            l.append(_num("low"))
            c.append(_num("close"))
            v.append(int(_num("volume")))
            a.append(_num("amount"))
        if not times:
            return None
        idx = pd.DatetimeIndex(times)
        return {
            "Open": pd.DataFrame({"open": o}, index=idx).rename(columns={"open": code}),
            "High": pd.DataFrame({"high": h}, index=idx).rename(columns={"high": code}),
            "Low": pd.DataFrame({"low": l}, index=idx).rename(columns={"low": code}),
            "Close": pd.DataFrame({"close": c}, index=idx).rename(columns={"close": code}),
            "Volume": pd.DataFrame({"volume": v}, index=idx).rename(columns={"volume": code}),
            "Amount": pd.DataFrame({"amount": a}, index=idx).rename(columns={"amount": code}),
        }

    @staticmethod
    def _extract_latest_signal(raw: Optional[dict], code: str) -> List[dict]:
        """从 formula_process_mul_zb 返回取最后一条 bar 的信号 → [{"name", "value"}]。

        raw: {stock_code: {var_name: [{"Date","Value"}, ...]}, "ErrorId", ...}
        实盘逐 bar 算，注入 N 根算出 N 条输出，取最后一条即当前 bar 信号
        （避开回测的索引对齐全段逻辑）。ErrorId 非 0/19 → 空。
        """
        if not isinstance(raw, dict) or not raw:
            return []
        err = raw.get("ErrorId")
        if err is not None and str(err) not in ("0", "19"):
            return []
        stock_data = raw.get(code)
        if not isinstance(stock_data, dict) or not stock_data:
            return []
        outputs: List[dict] = []
        for var_name, val_list in stock_data.items():
            if var_name in _FORMULA_META_KEYS:
                continue
            if not isinstance(val_list, list) or not val_list:
                continue
            last = val_list[-1]
            if not isinstance(last, dict):
                continue
            v = last.get("Value")
            if v is None:
                continue
            outputs.append({"name": var_name, "value": _to_int(v)})
        return outputs

    def inject_startup_periods(self, daily_time: datetime) -> None:
        """C6(C)：1w/1mon 策略通达信注入——TQFormula.compute 自取历史 → 最新信号填 signal_cache。

        key=(strategy_id, stock_code, daily_time)，与日终 _maybe_daily_bars 驱动用的
        bar_time 一致，驱动时命中预填信号。桥端 xtdata 拉不到 1w/1mon，通达信是唯一通路。
        start() 启动调一次；_maybe_daily_bars 检测日切 cache miss 时补调。
        单策略/单股 compute 失败 → 跳过（不阻断其余）。
        """
        if self._tq_formula is None or not self._formula_by_strategy:
            return
        codes = list(self._bar_poller.stock_codes)
        for portfolio in self.ctx.portfolios:
            for ctx in portfolio.strategies:
                if ctx.period not in _STARTUP_ONLY_PERIODS:
                    continue
                formula_name = self._formula_by_strategy.get(ctx.strategy_id)
                if not formula_name:
                    continue
                for code in codes:
                    # 节流：逐股 TDX 公式 compute（1w/1mon 全池）同步占 UI，逐笔小睡
                    time.sleep(0.2)
                    try:
                        # 1w/1mon 信号预填同属实盘信号生成：按 P0 取通道，
                        # 避免在 14:55–15:10 保护窗内被 P1 规则拒绝而丢信号。
                        with tdx_priority(P0):
                            raw = self._tq_formula.compute(
                                formula_name, "", [code], period=ctx.period, count=-1,
                                # 不复权（0）：与 query_quote 信号口径一致
                                dividend_type=0,
                            )
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "startup period compute failed %s %s", ctx.period, code
                        )
                        continue
                    outputs = self._extract_latest_signal(raw, code)
                    if outputs:
                        self.signal_cache[(ctx.strategy_id, code, daily_time)] = outputs

    def startup_periods_missing(self, daily_time: datetime) -> bool:
        """1w/1mon 策略在 daily_time 的信号是否全部已预填（cache miss → 需补注入）。

        _maybe_daily_bars 日切检测用：新交易日的 daily_time 尚无 cache 键 → True。
        """
        for portfolio in self.ctx.portfolios:
            for ctx in portfolio.strategies:
                if ctx.period not in _STARTUP_ONLY_PERIODS:
                    continue
                for code in self._bar_poller.stock_codes:
                    if (ctx.strategy_id, code, daily_time) not in self.signal_cache:
                        return True
        return False
